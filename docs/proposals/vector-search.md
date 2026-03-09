# Proposal: Vector Search for SNKV (Python Layer)

**Status:** Draft
**Author:** SNKV Contributors
**Target version:** 0.6.0

---

## 1. Motivation

SNKV is a crash-safe embedded KV store. Many real-world workloads that use embedded KV stores also need **approximate nearest-neighbour (ANN) search** over dense float vectors:

- Semantic search over document embeddings (OpenAI, sentence-transformers)
- Image / audio similarity
- Recommendation engines
- RAG (Retrieval-Augmented Generation) pipelines

Today users must maintain two separate stores: SNKV for structured data and a separate vector index (FAISS, hnswlib, ChromaDB) for embeddings. This creates consistency problems — a key can be deleted from SNKV but remain in the vector index, or vice versa.

**Goal:** Add first-class vector search to SNKV so that a single store can hold both the raw data and the vector index, with consistent deletes, TTL, and column-family scoping.

---

## 2. Scope

| In scope | Out of scope |
|---|---|
| Python-layer `VectorStore` class | C/C++ API changes |
| HNSW index via `chroma-hnswlib` | Exact (brute-force) search |
| Per-CF vector indexes | Distributed / multi-process index sync |
| Vector persistence inside SNKV (single file) | GPU-accelerated search |
| TTL-aware vector expiry | Multi-vector-per-key |
| Thread-safe reads | Cross-CF vector search |

---

## 3. Dependencies

### 3.1 Required for vector features

```
chroma-hnswlib >= 0.7.3
numpy >= 1.21
```

`chroma-hnswlib` is Chroma's maintained fork of `hnswlib`:
- C++ header-only, compiled at install time via pybind11
- Available on PyPI for Linux (x86\_64, aarch64), macOS (x86\_64, arm64), Windows (AMD64)
- Apache 2.0 licensed

`numpy` is used for float32 vector encoding/decoding and as the input/output format for `add_items` and `knn_query`.

### 3.2 Installation

`snkv` does **not** hard-depend on these packages. The `VectorStore` class lives in `snkv.vector` and raises `ImportError` with a clear message if they are absent:

```
pip install snkv[vector]
```

`pyproject.toml`:

```toml
[project.optional-dependencies]
vector = ["chroma-hnswlib>=0.7.3", "numpy>=1.21"]
```

---

## 4. Architecture

```
┌─────────────────────────────────────────────────────┐
│                    VectorStore                      │
│                                                     │
│  ┌──────────────┐      ┌─────────────────────────┐  │
│  │  KVStore     │      │  hnswlib.Index (HNSW)   │  │
│  │  (snkv)      │      │  (in-memory)            │  │
│  │              │      │                         │  │
│  │  default CF  │      │  int_id → float32 vec   │  │
│  │  user key →  │      │  ANN graph              │  │
│  │  user value  │      │                         │  │
│  │              │      └─────────────────────────┘  │
│  │  __vec__ CF  │                ↑                  │
│  │  user key →  │────── rebuilt on open             │
│  │  float32[]   │                                   │
│  │              │                                   │
│  │  __vec_meta__│                                   │
│  │  dim, space, │                                   │
│  │  next_id     │                                   │
│  │              │                                   │
│  │  __vec_id__k │                                   │
│  │  key → int   │                                   │
│  │              │                                   │
│  │  __vec_id__i │                                   │
│  │  int → key   │                                   │
│  └──────────────┘                                   │
└─────────────────────────────────────────────────────┘
```

### 4.1 Why Python layer, not C layer

- `hnswlib` is **C++** (STL, templates). Integrating C++ into SNKV's pure-C codebase requires a C++ compiler on every platform, `extern "C"` wrappers, and complicates the single-header build.
- At Python layer, `VectorStore` composes two independent libraries cleanly.
- Users who do not need vector search pay zero overhead — `chroma_hnswlib` is never imported.
- Faster to ship and iterate.

### 4.2 Single-file design

All vector data is stored **inside** the `.db` file using dedicated SNKV column families. There is no separate `.idx` file. Benefits:

- One file to backup, copy, or delete
- Crash safety and atomicity inherited from SNKV / SQLite WAL
- `drop_vector_index()` is a single CF drop — no orphaned files

---

## 5. `chroma-hnswlib` API Reference

This section documents every `chroma_hnswlib` call used internally by `VectorStore`, providing a clear contract with the library.

```python
import chroma_hnswlib as hnswlib
import numpy as np
```

### 5.1 Create an index

```python
index = hnswlib.Index(space="l2", dim=128)
# space: "l2" | "cosine" | "ip"
# dim: fixed vector dimension for the lifetime of the index
```

### 5.2 Initialise (must be called before first add)

```python
index.init_index(
    max_elements=1024,          # initial capacity; grown automatically by VectorStore
    M=16,                       # HNSW M — graph connectivity (8–64 typical)
    ef_construction=200,        # build-time search width (higher = better recall, slower build)
    allow_replace_deleted=True, # allow reuse of deleted slots — must be True for SNKV
)
```

### 5.3 Add vectors

```python
# Single vector
index.add_items(
    np.array([v], dtype=np.float32),   # shape (1, dim)
    [int_id],                           # list of integer IDs, must be unique
)

# Batch (used during index rebuild on open)
vectors = np.frombuffer(raw_bytes, dtype=np.float32).reshape(-1, dim)
ids = [id0, id1, id2, ...]
index.add_items(vectors, ids)
```

### 5.4 Query (ANN search)

```python
labels, distances = index.knn_query(
    np.array([query], dtype=np.float32),  # shape (1, dim)
    k=top_k,
)
# labels:    np.ndarray shape (1, top_k) — integer IDs of nearest neighbours
# distances: np.ndarray shape (1, top_k)
#   l2:     squared Euclidean distance (lower = more similar)
#   cosine: 1 - cosine_similarity      (lower = more similar)
#   ip:     negated inner product       (lower = more similar, i.e. higher dot product)
```

### 5.5 Soft-delete (called after KV delete)

```python
index.mark_deleted(int_id)
# O(1) — marks the slot as deleted; excluded from future knn_query results
# Slot is recycled on next add_items because allow_replace_deleted=True
```

### 5.6 Grow capacity

```python
current = index.get_max_elements()
index.resize_index(current * 2)
# Called automatically by VectorStore when count approaches max_elements (>= 90%)
```

### 5.7 Inspect

```python
index.get_current_count()       # number of active (non-deleted) vectors
index.get_max_elements()        # current capacity
index.get_items([id0, id1])     # returns np.ndarray shape (n, dim) — raw vectors
```

### 5.8 ef_search (query-time quality/speed trade-off)

```python
index.set_ef(50)   # higher = better recall, slower query; default 10
```

Called once at open time from the `ef_search` constructor parameter.

### 5.9 Persist index snapshot (Phase 2 — fast startup)

```python
# Save
index.save_index("/path/to/index.bin")

# Load (must call init_index first with matching space/dim)
index.load_index("/path/to/index.bin", max_elements=1024)
```

In Phase 1 the index is always rebuilt from raw vectors stored in SNKV (see §7.1).
In Phase 2 the serialised binary is stored in `__snkv_vec_snap__` CF for instant startup.

---

## 6. Storage Layout

For each `VectorStore` (default CF) or `VectorColumnFamily`, four internal CFs are created lazily on the first `vector_put`:

| CF name | Key | Value | Purpose |
|---|---|---|---|
| `__snkv_vec__` | user key (bytes) | float32 array as raw bytes | Raw vector storage |
| `__snkv_vec_id__k` | user key (bytes) | 8-byte big-endian int64 (hnsw_id) | key → HNSW integer ID |
| `__snkv_vec_id__i` | 8-byte big-endian int64 (hnsw_id) | user key (bytes) | HNSW integer ID → key |
| `__snkv_vec_meta__` | `b"dim"` / `b"space"` / `b"next_id"` / `b"ef_construction"` / `b"M"` | encoded value | Index configuration |

For column families, the CF name is appended: `__snkv_vec__users`, `__snkv_vec_id__k_users`, etc.

### 6.1 Float32 encoding

Vectors are stored as **raw little-endian IEEE 754 float32 bytes**:

```
4 bytes × dim  (e.g. 6144 bytes for dim=1536)
```

No compression. Decode/encode is a single `np.frombuffer` / `ndarray.tobytes()` call.

### 6.2 HNSW integer ID

hnswlib requires a unique non-negative integer ID per vector. SNKV maintains a monotonically increasing `next_id` counter stored in `__snkv_vec_meta__`. IDs are never reused — deleted IDs are soft-deleted in the HNSW index via `mark_deleted`, and recycled automatically by hnswlib when `allow_replace_deleted=True`.

---

## 7. Index Lifecycle

### 7.1 Open (`VectorStore.__init__`)

```
1. Open KVStore at path
2. Read __snkv_vec_meta__: dim, space, M, ef_construction
   → If absent: new store; index not initialised until first vector_put
3. Create hnswlib.Index(space, dim)
4. index.init_index(max_elements=max(1024, count*2), M=M,
                    ef_construction=ef_c, allow_replace_deleted=True)
5. index.set_ef(ef_search)
6. Batch-load all vectors from __snkv_vec__ CF:
   - Collect all (key, vec_bytes) pairs via iterator
   - Resolve int_ids from __snkv_vec_id__k
   - Call index.add_items(all_vectors, all_ids) in one batch
7. Index ready
```

Rebuild is O(n × d) — linear in the number of vectors.
For 100k vectors at dim=1536: ~2–5 s on a modern CPU.
Phase 2 (§16) adds a snapshot CF for instant startup.

### 7.2 `vector_put`

```
1. Validate len(vector) == dim; raise ValueError if not
2. If key already exists in __snkv_vec_id__k:
   a. Read old_int_id
   b. Begin write transaction
   c. KVStore.put(key, value)                         → update user data
   d. CF put(__snkv_vec__, key, vector.tobytes())      → update raw vector
   e. int_id = next_id; next_id += 1
   f. CF put(__snkv_vec_id__k, key, pack_int64(int_id))
   g. CF put(__snkv_vec_id__i, pack_int64(int_id), key)
   h. CF delete(__snkv_vec_id__i, pack_int64(old_int_id))
   i. Update next_id in __snkv_vec_meta__
   j. Commit
   k. index.mark_deleted(old_int_id)
   l. index.add_items(vector, [int_id])
3. Else (new key):
   a. Begin write transaction
   b. KVStore.put(key, value)
   c. CF put(__snkv_vec__, key, vector.tobytes())
   d. int_id = next_id; next_id += 1
   e. CF put(__snkv_vec_id__k, key, pack_int64(int_id))
   f. CF put(__snkv_vec_id__i, pack_int64(int_id), key)
   g. Update next_id in __snkv_vec_meta__
   h. Commit
   i. index.add_items(vector, [int_id])
   j. If count >= 0.9 * max_elements: index.resize_index(max_elements * 2)
```

Steps (a)–(j) / (a)–(h) are a single SNKV transaction — atomic. HNSW update happens after commit (in-memory only; SNKV is the source of truth).

### 7.3 `delete`

```
1. Lookup int_id = __snkv_vec_id__k[key]; raise NotFoundError if absent
2. Begin write transaction
3. KVStore.delete(key)
4. CF delete(__snkv_vec__, key)
5. CF delete(__snkv_vec_id__k, key)
6. CF delete(__snkv_vec_id__i, pack_int64(int_id))
7. Commit
8. index.mark_deleted(int_id)   → HNSW soft-delete, O(1)
```

### 7.4 `search`

```
1. If index not initialised: raise VectorIndexError
2. labels, distances = index.knn_query(np.array([query], dtype=np.float32), k=top_k)
3. For each int_id in labels[0]:
   key   = CF get(__snkv_vec_id__i, pack_int64(int_id))
   value = KVStore.get(key)        # None if key expired between index build and search
   if value is not None:
       append SearchResult(key, value, distance)
4. Return list of SearchResult (may be shorter than top_k if some keys expired)
```

All in-memory after step 2. Steps 3+ are single B-tree lookups per result.

### 7.5 `vector_purge_expired`

```
1. Call KVStore.purge_expired() to get all expired keys
2. For each expired key:
   int_id = __snkv_vec_id__k[key]  (if present)
   Delete from __snkv_vec__, __snkv_vec_id__k, __snkv_vec_id__i
   index.mark_deleted(int_id)
3. Return count of vectors removed
```

---

## 8. Python API

### 8.1 `VectorStore`

```python
from snkv.vector import VectorStore

# Create / open
vs = VectorStore(
    path,                       # str or None (in-memory)
    dim,                        # int — vector dimension, required
    space="l2",                 # "l2" | "cosine" | "ip"
    M=16,                       # HNSW M parameter (graph connectivity)
    ef_construction=200,        # HNSW ef_construction (build quality)
    ef_search=50,               # HNSW ef at query time
    **kv_kwargs,                # passed to KVStore (journal_mode, cache_size, etc.)
)

# Write — stores KV pair + vector, atomic
vs.vector_put(key, value, vector)             # vector: list[float] | np.ndarray
vs.vector_put(key, value, vector, ttl=3600)   # with TTL (seconds)

# Read value (no vector involved)
value = vs.get(key)                # returns bytes or None
value = vs[key]                    # raises KeyError if missing

# Read vector
vec = vs.vector_get(key)           # returns np.ndarray(dim,), raises NotFoundError if absent

# Delete — removes from KV + HNSW index, atomic
vs.delete(key)
del vs[key]

# Existence check
"key" in vs                        # True / False

# ANN search — returns up to top_k results (fewer if some keys expired)
results = vs.search(query_vector, top_k=10)
# list[SearchResult(key: bytes, value: bytes, distance: float)]

# Search keys only — no value fetch (faster)
results = vs.search_keys(query_vector, top_k=10)
# list[tuple[bytes, float]]   — (key, distance)

# Index metadata
info = vs.vector_info()
# {"dim": 1536, "space": "cosine", "count": 42000, "M": 16,
#  "ef_construction": 200, "ef_search": 50, "max_elements": 84000}

# Purge expired vectors (call after KVStore TTL expiry)
n = vs.vector_purge_expired()      # returns count removed

# Drop vector index (KV data kept, all __snkv_vec*__ CFs removed)
vs.drop_vector_index()

# Column families with independent vector indexes
with vs.create_vector_column_family("images", dim=512, space="cosine") as vcf:
    vcf.vector_put(b"img:001", b"cat.jpg", cat_embedding)
    results = vcf.search(query_embedding, top_k=5)

with vs.open_vector_column_family("images") as vcf:
    ...

# Context manager
with VectorStore("mydb.db", dim=1536) as vs:
    ...

# Explicit close
vs.close()
```

### 8.2 `SearchResult`

```python
from snkv.vector import SearchResult

result.key       # bytes  — the KV key
result.value     # bytes  — the KV value
result.distance  # float  — distance metric value
                 #   l2:     squared Euclidean (lower = more similar)
                 #   cosine: 1 - cosine_similarity (lower = more similar)
                 #   ip:     negated inner product  (lower = more similar)
```

### 8.3 `VectorColumnFamily`

Has the same methods as `VectorStore` (scoped to its CF):
`vector_put`, `vector_get`, `get`, `delete`, `search`, `search_keys`,
`vector_info`, `vector_purge_expired`, `drop_vector_index`,
`__getitem__`, `__setitem__`, `__delitem__`, `__contains__`,
`close`, `__enter__`, `__exit__`.

---

## 9. TTL Integration

`vector_put(key, value, vector, ttl=seconds)` sets TTL on the KV entry via SNKV's existing TTL mechanism. The HNSW index entry is cleaned up lazily:

- **On `search()`**: if a result key's value is `None` (expired), it is silently filtered from results. The HNSW entry is left until `vector_purge_expired()` is called.
- **On `vector_purge_expired()`**: scans the TTL index, deletes all expired vector entries from HNSW and all `__snkv_vec*__` CFs atomically.

This matches SNKV's existing lazy-expiry design and avoids background threads.

---

## 10. Thread Safety

Same rules as `KVStore`: each thread must use its own `VectorStore` instance.

The HNSW index is **not** thread-safe for concurrent writes. Do not share a `VectorStore` instance across threads.

For concurrent **reads** (search only), hnswlib supports concurrent `knn_query` calls when `allow_replace_deleted=True` (which is always set by SNKV). SNKV's WAL mode also allows concurrent readers at the KV layer.

---

## 11. Error Handling

| Condition | Exception |
|---|---|
| `chroma_hnswlib` or `numpy` not installed | `ImportError` with `pip install snkv[vector]` hint |
| `dim` mismatch on open vs stored | `ValueError` with message |
| `space` mismatch on open vs stored | `ValueError` with message |
| Vector length != dim | `ValueError` with message |
| Key not found (`get`, `delete`, `vector_get`) | `snkv.NotFoundError` |
| `search` / `search_keys` before any `vector_put` | `snkv.vector.VectorIndexError` |
| Negative or zero `top_k` | `ValueError` |

---

## 12. Design Decisions

| # | Decision | Choice | Rationale |
|---|---|---|---|
| 1 | `vector_put` on existing key | Overwrite: mark old ID deleted, assign new ID | Keeps API simple; HNSW slot recycled via `allow_replace_deleted=True` |
| 2 | `VectorStore` vs subclass | Composition — wraps `KVStore` | Cleaner API boundary; avoids MRO complexity |
| 3 | `vs[key] = value` (no vector) | Allowed — falls through to plain KV put | Lets `VectorStore` be used as a drop-in KV store |
| 4 | `search` when key expired mid-search | Filter silently, return fewer than top_k | Matches SNKV lazy-expiry design; no exception spam |
| 5 | HNSW capacity management | Auto-resize when count >= 90% of max_elements | No manual tuning needed by user |
| 6 | HNSW snapshot on `close()` | Not in Phase 1 — rebuild from raw vectors | Simpler; snapshot added in Phase 2 when rebuild latency becomes a concern |

---

## 13. File Layout

```
python/
  snkv/
    __init__.py          (unchanged)
    _snkv.pyi            (unchanged)
    vector.py            (new — VectorStore, VectorColumnFamily, SearchResult, VectorIndexError)
  tests/
    test_vector.py       (new — 30 tests)
  examples/
    vector_search.py     (new — semantic search demo)
```

No changes to `snkv_module.c`, `kvstore.c`, or `kvstore.h`.

---

## 14. `vector.py` Module Structure

```python
# snkv/vector.py

try:
    import chroma_hnswlib as hnswlib
    import numpy as np
except ImportError:
    raise ImportError(
        "Vector search requires: pip install snkv[vector]"
    )

from dataclasses import dataclass
from typing import Optional, List, Tuple
from snkv import KVStore, NotFoundError


class VectorIndexError(Exception):
    """Raised when the HNSW index is not initialised or is in an invalid state."""


@dataclass
class SearchResult:
    key:      bytes
    value:    bytes
    distance: float


class VectorStore:
    def __init__(self, path: Optional[str], dim: int, space: str = "l2",
                 M: int = 16, ef_construction: int = 200,
                 ef_search: int = 50, **kv_kwargs): ...

    def vector_put(self, key, value, vector, ttl: Optional[float] = None) -> None: ...
    def vector_get(self, key) -> "np.ndarray": ...
    def get(self, key, default=None) -> Optional[bytes]: ...
    def delete(self, key) -> None: ...
    def search(self, query, top_k: int = 10) -> List[SearchResult]: ...
    def search_keys(self, query, top_k: int = 10) -> List[Tuple[bytes, float]]: ...
    def vector_info(self) -> dict: ...
    def vector_purge_expired(self) -> int: ...
    def drop_vector_index(self) -> None: ...
    def create_vector_column_family(self, name: str, dim: int,
                                    space: str = "l2", **kwargs) -> "VectorColumnFamily": ...
    def open_vector_column_family(self, name: str) -> "VectorColumnFamily": ...
    def close(self) -> None: ...
    def __enter__(self) -> "VectorStore": ...
    def __exit__(self, *args) -> None: ...
    def __getitem__(self, key) -> bytes: ...
    def __setitem__(self, key, value) -> None: ...
    def __delitem__(self, key) -> None: ...
    def __contains__(self, key) -> bool: ...


class VectorColumnFamily:
    """Same interface as VectorStore, scoped to one column family."""
    # identical method signatures to VectorStore
    ...
```

---

## 15. Test Plan (`tests/test_vector.py`)

| # | Test |
|---|---|
| 1 | `vector_put` + `get` → correct value returned |
| 2 | `vector_put` + `vector_get` → correct vector returned (allclose) |
| 3 | `search` → nearest neighbour is correct (l2) |
| 4 | `search` top_k=5 → 5 results, sorted by distance ascending |
| 5 | `delete` → key absent from search results |
| 6 | `delete` → `get` raises NotFoundError |
| 7 | Close + reopen → index rebuilt, search still correct |
| 8 | dim mismatch on reopen → ValueError |
| 9 | space mismatch on reopen → ValueError |
| 10 | vector length != dim → ValueError |
| 11 | `search` before any `vector_put` → VectorIndexError |
| 12 | `vector_put` overwrite → new vector searchable, old vector gone |
| 13 | TTL: expired key filtered from `search` results |
| 14 | `vector_purge_expired` → expired vectors removed from index + CFs |
| 15 | `vector_info` → correct dim / space / count |
| 16 | `drop_vector_index` → `search` raises VectorIndexError |
| 17 | `drop_vector_index` → KV data still accessible via `get` |
| 18 | `search_keys` → returns (key, distance), no value fetch |
| 19 | `vs[key]` → correct value |
| 20 | `key in vs` → True / False |
| 21 | `del vs[key]` → key absent |
| 22 | `vs[key] = value` (no vector) → KV stored, not in vector index |
| 23 | VectorColumnFamily: index independent from default CF |
| 24 | VectorColumnFamily: different dim than default CF allowed |
| 25 | VectorColumnFamily: search scoped to CF only |
| 26 | VectorColumnFamily: close + reopen → correct |
| 27 | cosine space: nearest neighbour correct |
| 28 | ip (inner product) space: nearest neighbour correct |
| 29 | 1000 vectors → search top-10 recall >= 0.9 |
| 30 | in-memory store (path=None) → works correctly |

---

## 16. Example: Semantic Search Demo

```python
# python/examples/vector_search.py
from snkv.vector import VectorStore
import numpy as np

DIM = 64   # use 1536 for real OpenAI embeddings

def random_embedding(seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    v = rng.random(DIM).astype(np.float32)
    return v / np.linalg.norm(v)

docs = [
    (b"doc:rust",   b"Rust is a systems programming language"),
    (b"doc:python", b"Python is great for data science"),
    (b"doc:go",     b"Go excels at concurrent network services"),
    (b"doc:c",      b"C gives you direct hardware access"),
    (b"doc:sql",    b"SQL is the language of relational databases"),
]

with VectorStore(None, dim=DIM, space="cosine") as vs:
    for i, (key, value) in enumerate(docs):
        vs.vector_put(key, value, random_embedding(i))

    query = random_embedding(0)   # most similar to doc:rust embedding
    results = vs.search(query, top_k=3)
    print("Top 3 results:")
    for r in results:
        print(f"  {r.key.decode():15s}  dist={r.distance:.4f}  {r.value.decode()}")
```

---

## 17. Implementation Phases

| Phase | Scope | Version |
|---|---|---|
| 1 | `VectorStore` (default CF): `vector_put`, `get`, `vector_get`, `delete`, `search`, `search_keys`, `vector_info`, `drop_vector_index`, close/reopen, dict interface | 0.6.0 |
| 2 | `VectorColumnFamily` support | 0.6.0 |
| 3 | TTL integration (`vector_put(ttl=)`, `vector_purge_expired`) | 0.6.0 |
| 4 | HNSW snapshot for fast startup (serialized index in `__snkv_vec_snap__` CF) | 0.7.0 |
| 5 | Bulk insert `vector_put_batch(items)` for efficient initial load | 0.7.0 |
