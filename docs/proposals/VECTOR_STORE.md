# SNKV Vector Store — Design Proposal

**Status**: Proposed
**Author**: SNKV Contributors
**Date**: 2026-03-07

---

## 1. Overview

This document proposes integrating approximate nearest-neighbour (ANN) vector search
into SNKV using [usearch](https://github.com/unum-cloud/usearch) as the underlying
HNSW index engine.

The goal is to allow SNKV to serve as a self-contained embedded vector database —
no separate Chroma, Qdrant, or FAISS process required. All data lives in a single
`.db` file alongside a small sidecar index file per vector space.

### 1.1 Motivation

Applications that use SNKV for structured key-value storage often also need semantic
search (RAG pipelines, LLM response caches, recommendation systems). Today they must
run a separate vector database. Adding vector search to SNKV eliminates that dependency
while preserving SNKV's "single embedded file" contract.

### 1.2 Why usearch

| Criterion | Flat (scratch) | **usearch** | FAISS |
|---|---|---|---|
| Production-ready | No | **Yes** | Yes |
| Scale | <500K vectors | **Billions** | Billions |
| Dependencies | None | **Header-only C++** | BLAS/LAPACK |
| C API | Custom | **Built-in** | Yes |
| Quantization | No | **f32/f16/int8/b1** | Yes |
| License | — | **Apache 2.0** | MIT |
| Active maintenance | — | **Yes (Cloudflare, Unum)** | Yes |

usearch is header-only C++17 (single `index_dense.hpp`). SNKV's public API remains
pure C — the C++ is confined to `src/kvvec.cpp` and never surfaces to callers.

---

## 2. Design

### 2.1 Concepts

A **vector space** (`KVVecSpace`) is the user-facing handle, analogous to a
`KVColumnFamily`. Each vector space has:

- A **name** (e.g., `"docs"`, `"embeddings"`)
- A fixed **dimension** (e.g., 1536 for OpenAI `text-embedding-3-small`)
- A **metric** (cosine / L2 / dot product)
- A **quantization level** (f32 / f16 / int8 / b1)

Multiple vector spaces can coexist in the same `KVStore`.

### 2.2 Storage Strategy

Two storage layers work together:

```
mydb.db                         ← SNKV B-tree (crash-safe)
mydb.db.vec/
  docs.usvec                    ← usearch HNSW sidecar (rebuildable)
  embeddings.usvec
```

**Inside the B-tree**, three internal column families are created per vector space
(the `__` prefix is reserved and rejected by user-facing CF APIs):

| CF Name | Key | Value |
|---|---|---|
| `__snkv_vec_cfg__<space>` | `"cfg"` | `dim(4B) + metric(1B) + quant(1B) + next_label(8B)` |
| `__snkv_vec_raw__<space>` | user key | `label(8B) + nDim(4B) + float32×nDim` |
| `__snkv_vec_lbl__<space>` | `label(8B BE)` | user key bytes |

**The HNSW index** is loaded from / saved to the sidecar file. The B-tree is ground
truth — if the sidecar is missing, the index is rebuilt from `__snkv_vec_raw__` on
open (one-time cost).

### 2.3 Key→Label Mapping

usearch identifies vectors by `uint64` labels internally. SNKV maintains:

- `__snkv_vec_raw__`: `user_key → [label(8B)][raw float32 vector]`
  Used for: vector retrieval, label lookup on delete, index rebuild
- `__snkv_vec_lbl__`: `label(8B BE) → user_key`
  Used for: resolving search results back to user keys
- `cfg.next_label`: monotonically increasing `uint64` counter, persisted in B-tree

### 2.4 Operation Flows

**`kvvec_put(key, vec)`**
1. Look up key in `raw` CF → if found, get old label, mark it removed in usearch
2. Assign `next_label++`, persist to `cfg` CF
3. Write `[label][nDim][vec]` to `raw` CF (B-tree commit)
4. Write `label → key` to `lbl` CF (same transaction)
5. Add vector to in-memory usearch index with new label

**`kvvec_search(query, k)`**
1. Query usearch HNSW index → get `(label, distance)` pairs
2. For each label, look up user key in `lbl` CF
3. Return `(key, distance)` results sorted by distance

**`kvvec_delete(key)`**
1. Look up key in `raw` CF → get label
2. Remove label from usearch index
3. Delete from `raw` CF and `lbl` CF (one transaction)

**`kvvec_flush()`**
1. Serialize usearch index to buffer
2. Write to `<dbpath>.vec/<space>.usvec`

**`kvvec_space_open()` with existing space**
1. Read config from `cfg` CF
2. If sidecar exists → `index.load(sidecar_path)`
3. If sidecar missing → iterate `raw` CF, re-add all vectors to fresh index

### 2.5 Crash Safety

Raw vectors are committed to the B-tree before any in-memory HNSW update.
If the process crashes:
- Data in B-tree is intact (WAL guarantees)
- Sidecar may be stale or missing
- On next open, SNKV detects the mismatch via `next_label` in `cfg` and rebuilds
  the HNSW index from `raw` CF

No data loss. Worst case: one-time rebuild cost on restart.

---

## 3. API

### 3.1 C API — `include/kvvec.h`

```c
/* Distance metrics */
#define KVVEC_METRIC_COSINE  0
#define KVVEC_METRIC_L2      1
#define KVVEC_METRIC_DOT     2

/* Quantization levels */
#define KVVEC_QUANT_F32   0   /* full precision float32 (default) */
#define KVVEC_QUANT_F16   1   /* half precision — ~2x smaller index */
#define KVVEC_QUANT_INT8  2   /* 8-bit quantized — ~4x smaller index */
#define KVVEC_QUANT_B1    3   /* binary — ~32x smaller index */

typedef struct KVVecSpace KVVecSpace;

typedef struct {
  int    dimensions;       /* REQUIRED: embedding dimension */
  int    metric;           /* KVVEC_METRIC_* (default: COSINE) */
  int    quantization;     /* KVVEC_QUANT_*  (default: F32) */
  size_t capacity;         /* initial capacity hint (default: 1024) */
  int    connectivity;     /* HNSW M — edges per node (default: 16) */
  int    expansion_add;    /* HNSW ef_construction (default: 128) */
  int    expansion_search; /* HNSW ef at query time (default: 64) */
} KVVecConfig;

typedef struct {
  void  *pKey;      /* caller must snkv_free() */
  int    nKey;
  float  distance;
} KVVecResult;

typedef struct {
  uint64_t nVectors;
  uint64_t capacity;
  int      dimensions;
  int      metric;
  int      quantization;
  size_t   index_bytes;
} KVVecStats;

/* Lifecycle */
int  kvvec_space_open(KVStore *pKV, const char *zName,
                      const KVVecConfig *pConfig, KVVecSpace **ppSpace);
int  kvvec_space_close(KVVecSpace *pSpace);
int  kvvec_space_drop(KVStore *pKV, const char *zName);

/* CRUD */
int  kvvec_put(KVVecSpace *pSpace,
               const void *pKey, int nKey,
               const float *pVec, int nDim);
int  kvvec_delete(KVVecSpace *pSpace, const void *pKey, int nKey);
int  kvvec_get(KVVecSpace *pSpace,
               const void *pKey, int nKey,
               float **ppVec, int *pnDim);

/* Search */
int  kvvec_search(KVVecSpace *pSpace,
                  const float *pQuery, int nDim, int k,
                  KVVecResult **ppResults, int *pnResults);
void kvvec_results_free(KVVecResult *pResults, int nResults);

/* Persistence */
int  kvvec_flush(KVVecSpace *pSpace);

/* Stats */
int  kvvec_stats(KVVecSpace *pSpace, KVVecStats *pStats);
```

#### Return codes

All functions return standard `KVSTORE_*` error codes:

| Code | Meaning |
|---|---|
| `KVSTORE_OK` | Success |
| `KVSTORE_NOTFOUND` | Key does not exist |
| `KVSTORE_ERROR` | Dimension mismatch, OOM, I/O error |
| `KVSTORE_NOMEM` | Allocation failure |

### 3.2 Python API

```python
class VectorSpace:
    def put(self, key: bytes | str, vector: np.ndarray) -> None:
        """Insert or update. vector must be float32, shape (dim,)."""

    def delete(self, key: bytes | str) -> None:
        """Remove a vector. Raises NotFoundError if missing."""

    def get(self, key: bytes | str) -> np.ndarray:
        """Return stored float32 vector for key."""

    def search(self, query: np.ndarray, k: int) -> list[tuple[bytes, float]]:
        """Return up to k (key, distance) pairs, sorted by distance ascending."""

    def flush(self) -> None:
        """Persist HNSW index to sidecar file."""

    def stats(self) -> dict:
        """Return n_vectors, capacity, dimensions, metric, index_bytes."""

    def __enter__(self): return self
    def __exit__(self, *_): self.flush()


class KVStore:
    def vector_space(
        self,
        name: str,
        *,
        dimensions: int = 0,
        metric: str = "cosine",       # "cosine" | "l2" | "dot"
        quantization: str = "f32",    # "f32" | "f16" | "int8" | "b1"
        capacity: int = 1024,
        connectivity: int = 16,
        expansion_add: int = 128,
        expansion_search: int = 64,
    ) -> VectorSpace:
        """Open or create a vector space. Existing spaces ignore config."""
```

#### Python usage example

```python
import snkv
import numpy as np

db = snkv.KVStore("mydb.db")

# Create space (first time only)
vs = db.vector_space("docs", dimensions=1536, metric="cosine", quantization="int8")

# Index documents
for doc_id, embedding in zip(doc_ids, embeddings):
    vs.put(doc_id.encode(), embedding.astype(np.float32))

vs.flush()   # save HNSW index to sidecar

# Semantic search
query_vec = embed_model.encode("how to reset password?")
results = vs.search(query_vec, k=5)

for key, distance in results:
    print(key.decode(), f"  dist={distance:.4f}")

# Reuse across sessions
db2 = snkv.KVStore("mydb.db")
vs2 = db2.vector_space("docs")           # loads sidecar automatically
results2 = vs2.search(query_vec, k=5)   # works immediately
```

---

## 4. Files

### New files

| File | Purpose |
|---|---|
| `include/kvvec.h` | Public C API (pure C, `extern "C"`) |
| `src/kvvec.cpp` | usearch wrapper + all implementation |
| `third_party/usearch/` | Git submodule — only `include/usearch/index_dense.hpp` used |
| `tests/test_vec.c` | 12 C unit tests |
| `python/snkv_vec_module.c` | CPython extension for `VectorSpace` |
| `python/tests/test_vec.py` | Python unit tests |
| `examples/vec.c` | C example: RAG pipeline |
| `python/examples/vec.py` | Python example: semantic cache + RAG |
| `docs/api/VEC_API.md` | Public API reference |

### Modified files

| File | Change |
|---|---|
| `Makefile` | Add CXX build rule for `src/kvvec.cpp`, link `-lstdc++ -lm`, add usearch submodule target |
| `python/snkv/__init__.py` | Add `VectorSpace` class, `KVStore.vector_space()` method |
| `python/pyproject.toml` | Add `numpy` as optional dependency under `[project.optional-dependencies] vec` |
| `docs/api/API_SPECIFICATION.md` | Reference to VEC_API.md |

---

## 5. Build System

### Makefile additions

```makefile
CXX        ?= g++
CXXFLAGS   += -std=c++17 -O2
USEARCH_DIR = third_party/usearch

# Include usearch headers
CXXFLAGS   += -I$(USEARCH_DIR)/include

# Compile kvvec.cpp separately with C++ compiler
src/kvvec.o: src/kvvec.cpp include/kvvec.h include/kvstore.h
	$(CXX) $(CXXFLAGS) $(CFLAGS_COMMON) -c -o $@ $<

# Link with C++ stdlib
LDFLAGS += -lstdc++ -lm

# Submodule init
third_party/usearch:
	git submodule add \
	  https://github.com/unum-cloud/usearch third_party/usearch
	git submodule update --init --recursive
```

### Build guard (optional, off by default)

```makefile
# Compile without vector support by default
# Enable with: make SNKV_VECTOR=1
ifeq ($(SNKV_VECTOR),1)
  CFLAGS  += -DSNKV_VECTOR_SUPPORT
  OBJ     += src/kvvec.o
  LDFLAGS += -lstdc++ -lm
endif
```

---

## 6. Implementation Notes

### `src/kvvec.cpp` internal structure

```
KVVecSpace {
  KVStore         *pKV;
  KVColumnFamily  *pCfgCF;   /* __snkv_vec_cfg__<name> */
  KVColumnFamily  *pRawCF;   /* __snkv_vec_raw__<name> */
  KVColumnFamily  *pLblCF;   /* __snkv_vec_lbl__<name> */
  char            *zName;
  char            *zSidecarPath;
  unum::usearch::index_dense_t *pIndex;
  sqlite3_mutex   *pMutex;
  int              nDim;
  int              metric;
  int              quant;
  uint64_t         nextLabel;
}
```

Key implementation decisions:

- **Update = remove + reinsert**: usearch HNSW does not support in-place update.
  On `kvvec_put` for an existing key, the old label is marked removed via
  `index.remove(old_label)`, then a new label is assigned and inserted.

- **Mutex**: `sqlite3_mutex_alloc(SQLITE_MUTEX_FAST)` — same pattern as `KVStore`.
  The mutex is held for the duration of each public function call.

- **Sidecar path**: derived from the KVStore filename.
  `mydb.db` → `mydb.db.vec/` directory, `<space>.usvec` file per space.
  For in-memory KVStores (filename=NULL), sidecar is skipped; index lives only
  in memory for the session.

- **Index rebuild**: triggered when sidecar is absent on open.
  Iterates `__snkv_vec_raw__<space>` CF, re-adds all `(label, vec)` pairs.
  Progress is logged via `fprintf(stderr, ...)` for observability.

- **usearch version pin**: The submodule is pinned to a specific commit hash
  in `.gitmodules` to prevent unexpected API changes from breaking the build.

### Thread safety

`KVVecSpace` is **not** safe to use from multiple threads concurrently.
Concurrent access from different threads requires external locking by the caller,
same as `KVColumnFamily`. Multiple `KVVecSpace` handles may be used from
different threads simultaneously (each has its own mutex).

---

## 7. Test Plan

### C tests — `tests/test_vec.c` (12 tests)

| # | Test |
|---|---|
| 1 | Create space, put one vector, search k=1 → self returned, distance ≈ 0 |
| 2 | Put 1000 vectors, search k=10 → 10 results, sorted by distance ascending |
| 3 | `kvvec_get` → returned float32 values match what was stored |
| 4 | Re-put same key with different vector → search returns updated vector |
| 5 | `kvvec_delete` → key absent from search and `kvvec_get` returns NOTFOUND |
| 6 | `kvvec_flush`, close, reopen → sidecar loaded, search still correct |
| 7 | Crash sim (no flush), reopen → index rebuilt from raw CF, search correct |
| 8 | Dimension mismatch on `kvvec_put` → KVSTORE_ERROR |
| 9 | Search on empty space → 0 results, KVSTORE_OK |
| 10 | L2 metric → distances are Euclidean (verified against brute force) |
| 11 | `kvvec_space_drop` → all internal CFs gone, sidecar deleted |
| 12 | Two spaces in same KVStore → isolated, no cross-contamination |

### Python tests — `python/tests/test_vec.py`

Mirror of C tests via Python bindings, plus:
- numpy array in / numpy array out roundtrip
- Context manager (`with db.vector_space(...) as vs:`) auto-flushes
- `NotFoundError` raised correctly on missing key

### Valgrind

```bash
valgrind --leak-check=full ./tests/test_vec
```

Expected: zero leaks. All `KVVecResult.pKey` pointers are freed by
`kvvec_results_free()`.

---

## 8. Performance Expectations

Benchmarked against 1M float32 vectors, dim=128, cosine metric, int8 quantization,
on Linux x86-64 (AVX2):

| Operation | Expected throughput |
|---|---|
| `kvvec_put` (bulk load) | ~150K–300K vectors/sec |
| `kvvec_search` k=10 | ~5K–20K queries/sec |
| `kvvec_search` k=10 (f32, no quant) | ~2K–8K queries/sec |
| Index rebuild from raw (1M vecs) | ~30–60 seconds |
| Sidecar load (1M vecs, int8) | ~1–3 seconds |

For dim=1536 (OpenAI embeddings), throughput is proportionally lower.
Use `KVVEC_QUANT_INT8` for production deployments at high dimension.

---

## 9. Risks and Mitigations

| Risk | Mitigation |
|---|---|
| usearch API changes (active project) | Pin to specific commit in `.gitmodules` |
| Large indexes exceed available RAM | `KVVEC_QUANT_INT8` / `KVVEC_QUANT_B1` — 4–32x smaller |
| Sidecar out of sync after crash | B-tree is ground truth; sidecar always rebuildable |
| C++ in a C codebase | `kvvec.h` is pure C; `kvvec.cpp` compiled separately; C++ never leaks into public API |
| `KVSTORE_MAX_VALUE_SIZE` (10MB) | Vectors stored per-key — each entry is `dim*4` bytes, well within 10MB |
| Windows build (`-lstdc++`) | Use `-lc++` on MSVC/Clang; guard in Makefile with `ifeq ($(OS),Windows_NT)` |

---

## 10. Delivery Milestones

### Phase 1 — C layer (2–3 weeks)
- [ ] Add usearch as git submodule, pin commit
- [ ] `include/kvvec.h` — final public API
- [ ] `src/kvvec.cpp` — full implementation
- [ ] `tests/test_vec.c` — 12/12 passing
- [ ] Valgrind clean
- [ ] `examples/vec.c`

### Phase 2 — Python bindings (1 week)
- [ ] `python/snkv_vec_module.c`
- [ ] `VectorSpace` class in `python/snkv/__init__.py`
- [ ] `python/tests/test_vec.py` — all passing
- [ ] `python/examples/vec.py` (semantic cache + RAG demo)

### Phase 3 — Docs + polish (2–3 days)
- [ ] `docs/api/VEC_API.md`
- [ ] `docs/api/API_SPECIFICATION.md` — add vector section
- [ ] Update `README.md` with vector store section
- [ ] PyPI release with `snkv[vec]` optional dependency

---

## 11. Future Work (out of scope for this proposal)

- **Filtered search**: `kvvec_search_filtered(space, query, k, filter_fn)` —
  skip vectors by metadata predicate during HNSW traversal
- **Batch insert API**: `kvvec_put_batch()` — amortize B-tree transaction overhead
  across many inserts
- **Multi-vector keys**: store multiple embeddings per key (e.g., multi-modal)
- **HNSW parameter tuning API**: expose `index.change_expansion_search()` at runtime
- **SNKV Cloud**: sidecar stored in object storage for serverless deployments
