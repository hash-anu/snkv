# Design Decisions: usearch Integration in SNKV

**Status:** Accepted
**Author:** SNKV Contributors
**Target version:** 0.6.0
**Related proposal:** `vector-search.md`

---

## Overview

This document records every non-obvious design decision made while integrating
[usearch](https://github.com/unum-cloud/usearch) (Apache-2.0) into SNKV as an optional
vector-search layer. Each decision states the choice, the alternatives considered, and the
reasoning.

---

## Decision 1 — Library: usearch over hnswlib / chroma-hnswlib / FAISS

**Choice:** `usearch >= 2.9`

| Library | Why rejected |
|---|---|
| `hnswlib` | Unmaintained since 2023; no pre-built wheels for recent Python/platform combos |
| `chroma-hnswlib` | Chroma's fork — carries Chroma-specific patches; ties SNKV to another project's release cadence |
| `faiss-cpu` | 200 MB binary; overkill for embedded use; requires MKL or OpenBLAS on some platforms |
| `annoy` | Read-only after build; no incremental add/delete — unusable for a mutable KV store |

**Why usearch:**
- Header-only C++ library (`usearch/index.hpp` as main include) — easy to vendor if needed
- Pre-built wheels on PyPI for Linux (x86_64, aarch64), macOS (x86_64, arm64), Windows (AMD64)
- Apache-2.0 — no licensing friction alongside SNKV's Apache-2.0 C core
- Native support for `l2`, `cosine`, and `ip` spaces; `int8` and `float16` quantisation available
- `Index.add(keys, vectors)` accepts numpy arrays directly; no extra conversion
- Integer external keys are first-class within usearch (no internal ID↔label indirection inside the library; SNKV still maintains a bytes↔int map because its user keys are arbitrary bytes)
- Active maintenance and responsive upstream

---

## Decision 2 — Integration layer: Python, not C

**Choice:** `python/snkv/vector.py` wraps both `KVStore` (Python) and `usearch.Index`

**Alternative:** Write a C shim that calls into the usearch C++ API and expose it through `snkv_module.c`.

**Rejected because:**
- usearch is C++ (templates, STL). Calling C++ from SNKV's pure-C core requires `extern "C"` wrappers, a C++ compiler on every build host, and complicates the single-header amalgamation.
- The Python layer is already the right abstraction boundary: `KVStore` is already a Python class; composing two Python objects is clean and testable.
- Users who never use vector search pay zero overhead — usearch is never imported unless `snkv[vector]` is installed.
- Iteration speed for the index rebuild (§ Decision 8) is dominated by the HNSW graph construction, not Python overhead.

---

## Decision 3 — Optional dependency, not hard dependency

**Choice:** `pip install snkv[vector]` installs `usearch >= 2.9` and `numpy >= 1.21`.
The base `pip install snkv` does not pull in either.

**Why:** SNKV is used as a general-purpose embedded KV store. Most users never need vector
search. Adding 20 MB of compiled C++ to every install would be wrong.

`snkv/vector.py` raises `ImportError` with a clear pip hint if usearch is absent:
```python
try:
    import usearch.index
    import numpy as np
except ImportError:
    raise ImportError("Vector search requires: pip install snkv[vector]")
```

---

## Decision 4 — Storage: SNKV column families, not a separate `.usearch` file

**Choice:** All vector metadata and raw vectors live inside the `.db` file using dedicated
SNKV column families (`__snkv_vec__`, `__snkv_vec_id__k`, `__snkv_vec_id__i`,
`__snkv_vec_meta__`). The in-memory usearch `Index` object is rebuilt from these CFs on
every open.

**Alternative:** Persist the usearch index as a sidecar file (`store.usearch`) using
`index.save()` / `index.load()`.

**Rejected because:**
- Two files means two things to back up, copy, move, and keep in sync. Loss of the sidecar
  means re-embedding from scratch if raw vectors aren't stored.
- The sidecar is not crash-safe relative to the SNKV WAL. A crash between a KV commit and
  `index.save()` leaves them inconsistent.
- SNKV's WAL atomicity applies naturally when vectors are stored as CFs: the KV write and
  the raw-vector write are a single transaction.

**Trade-off accepted:** Rebuilding the index on every open is O(n × d). For 100k vectors
at dim=1536 this takes 2–5 s. Phase 2 mitigates this with a snapshot CF
(`__snkv_vec_snap__`) that stores the serialised usearch index binary for instant load
(see Decision 8).

---

## Decision 5 — Column family naming convention

**Choice:** Internal CFs are named with a `_snkv_vec` prefix (single leading underscore):

| CF | Purpose |
|---|---|
| `_snkv_vec_` | user key → float32 bytes (raw vector) |
| `_snkv_vec_idk_` | user key → 8-byte big-endian int64 (usearch label) |
| `_snkv_vec_idi_` | 8-byte big-endian int64 → user key |
| `_snkv_vec_meta_` | configuration (`ndim`, `metric`, `connectivity`, `expansion_add`, `expansion_search`, `next_id`, `dtype`) |

**Why single underscore, not double:**
SNKV's C layer reserves CF names starting with `__` for its own internal CFs
(`__snkv_ttl_k__`, `__snkv_ttl_e__`, `__snkv_auth__`). The user-facing
`kvstore_cf_create` and `kvstore_cf_open` APIs reject any name starting with `__`.
Vector CFs use a single leading underscore to stay out of user namespace while
remaining allowed by the API.

**Why four CFs instead of one:**
- `__snkv_vec__` is the source of truth for index rebuild. Separating it from the user's
  data CF keeps user iteration clean.
- The two ID maps allow O(log n) lookup in both directions without scanning. usearch
  requires non-negative integer labels; SNKV keys are arbitrary bytes. A bidirectional
  map is the minimal structure to translate between them.
- Metadata is tiny (≤5 keys) and rarely written; sharing a CF with vectors would mix
  high-frequency writes with rare config reads.

---

## Decision 6 — Integer label strategy: monotonically increasing counter, never reused

**Choice:** `next_id` is stored in `__snkv_vec_meta__` and incremented on every
`vector_put`. Deleted labels are soft-deleted in usearch (`index.remove(label)`) but
their integer values are never recycled in SNKV's ID maps.

**Why not recycle IDs:**
- Reusing an integer ID that was soft-deleted in usearch would require confirming the slot
  is truly free in the in-memory index, adding a race window. Monotonic counters are
  simpler and provably correct.
- usearch marks deleted slots as available for reuse internally; capacity is not wasted
  even when SNKV-level IDs are not recycled. SNKV-side IDs are distinct from usearch's
  internal slot management.
- The counter is a 64-bit int — 9 × 10¹⁸ inserts before overflow.

---

## Decision 7 — Atomicity boundary: KV commit first, HNSW update second

**Choice:** Every mutating operation (`vector_put`, `delete`) commits to SNKV first, then
updates the in-memory usearch index.

```
begin SNKV transaction
  put user_key → user_value         (default CF)
  put user_key → float32_bytes      (__snkv_vec__)
  put user_key → int_id             (__snkv_vec_id__k)
  put int_id   → user_key           (__snkv_vec_id__i)
  put "next_id" → new_next_id       (__snkv_vec_meta__)
commit SNKV transaction
index.add(int_id, vector)           ← in-memory only
```

**Why SNKV first:**
- SNKV is the source of truth. If the process crashes after the SNKV commit but before the
  usearch update, the next open rebuilds the index from SNKV data — fully consistent.
- The reverse order (usearch first, then SNKV commit fails) would leave a stale entry in
  the in-memory index that persists until restart.

**Consequence:** A search immediately after a crash-recovery open will not return results
that were added to SNKV but not yet replayed into usearch — but this window is zero in
normal operation (index is rebuilt synchronously on open).

---

## Decision 8 — Index rebuild on open (Phase 1) vs snapshot (Phase 2)

**Phase 1 (0.6.0):** On every `VectorStore.__init__`, scan all `__snkv_vec__` and
`__snkv_vec_id__k` entries, batch-add to a fresh `Index`, then set `expansion_search`
(the usearch query-time search width, equivalent to hnswlib's `ef`).

**Phase 2 (0.7.0):** Serialize the usearch index binary to `__snkv_vec_snap__` CF on
`close()`. On open, load the snapshot if present and consistent (matches stored `next_id`
and item count); fall back to full rebuild if not.

**Why defer Phase 2:**
- Rebuild is fast enough for typical embedded workloads (< 50k vectors).
- Phase 2 adds complexity: snapshot invalidation logic, partial-snapshot detection after
  crash, and the snapshot binary is not encrypted (raw float32 data). These concerns are
  better solved with more usage feedback.
- Keeping Phase 1 simple reduces the test matrix for the initial release.

---

## Decision 9 — Encryption interaction: raw vectors are encrypted, snapshot is not (Phase 2 concern)

**Phase 1:** Raw float32 bytes stored in `__snkv_vec__` CF are encrypted by SNKV's
XChaCha20-Poly1305 layer just like any other value. The in-memory usearch index holds
plaintext vectors (necessary for similarity computation).

**Phase 2 consideration:** The usearch snapshot binary stored in `__snkv_vec_snap__` would
contain plaintext float32 data. Options when Phase 2 ships:
1. Encrypt the snapshot blob before storing it as a CF value (same AEAD, ≤ 40 bytes overhead).
2. Always rebuild from encrypted `__snkv_vec__` entries (skip the snapshot for encrypted stores).

Option 1 is preferred but deferred. For Phase 1 (rebuild-only) there is no plaintext-at-rest concern because the index is never serialized.

---

## Decision 10 — TTL: lazy expiry in search, explicit purge for HNSW cleanup

**Choice:** Expired vectors are not removed from the usearch index until
`vector_purge_expired()` is called. `search()` silently skips result keys whose KV value
is `None` (expired under SNKV lazy-expiry).

**Why not background thread / eager expiry:**
- SNKV has no background threads. Adding one for vector expiry would break the
  single-threaded embedding model.
- Lazy-skip in `search()` is already safe because SNKV's lazy-expiry guarantees that an
  expired key returns `None` on get.
- `vector_purge_expired()` gives the caller full control over when to pay the cleanup cost
  (e.g. once per minute in a cron-like call).

**Known limitation:** Expired vectors remain in the HNSW graph and affect `index.search()`
recall slightly. In practice, expired entries are a small fraction of the index at any
moment; the impact on recall is negligible.

---

## Decision 11 — Thread safety: one instance per thread, no internal locking

**Choice:** `VectorStore` inherits SNKV's threading model — each thread must use its own
instance. No mutex is added around the usearch `Index`.

**Rationale:**
- Adding a Python-level lock around every `search()` call would serialize all ANN queries,
  defeating the purpose of parallel read workloads.
- usearch `Index` supports concurrent reads natively when the index is not being mutated.
  Users who need concurrent read + write must either serialize at application level or use
  separate processes (SNKV WAL supports multi-process readers).
- SNKV's existing documentation already sets this expectation. Deviating for VectorStore
  would create confusing asymmetry.

---

## Decision 12 — `space` is fixed at creation; cannot be changed

**Choice:** `space` (`l2`, `cosine`, `ip`) is written to `__snkv_vec_meta__` on the first
`vector_put` and cannot be changed on reopen. A mismatch raises `ValueError`.

**Why:** Changing the distance metric on an existing index would make all stored distances
and rankings meaningless. The correct migration path is `drop_vector_index()` followed by
re-insertion with the new space. This is an intentional sharp edge.

**Implementation note — usearch metric name mapping:**
The public SNKV API uses intuitive names; they must be translated when constructing
`usearch.index.Index(metric=...)`:

| SNKV `space=` | usearch `metric=` | Notes |
|---|---|---|
| `"l2"` | `"l2sq"` | usearch computes squared L2; distances are comparable, not Euclidean |
| `"cosine"` | `"cos"` | usearch uses `"cos"`, not `"cosine"` |
| `"ip"` | `"ip"` | identical |

The stored meta value is the SNKV name (`"l2"`, `"cosine"`, `"ip"`); translation happens
in `VectorStore.__init__` before the usearch `Index` is constructed.

---

## Decision 13 — `dim` is fixed at creation; cannot be changed

Same rationale as Decision 12. Embedding models produce fixed-dimension vectors. A dim
mismatch means the wrong model was used, which is always a programming error.

---

## Decision 14 — `search()` may return fewer than `top_k` results

**Choice:** If some of the HNSW nearest neighbours have expired (lazy TTL) or were deleted
between index build and search, they are silently dropped. No exception is raised.

**Alternative:** Pad the usearch query with `top_k + buffer` and trim to `top_k` after
filtering.

**Rejected because:** Choosing a buffer size is a guess. For use cases with high TTL
churn the buffer would need to be large, inflating query cost. Silent under-delivery is
the standard behaviour in every other ANN library (FAISS, Weaviate) and is documented
clearly.

---

## Decision 15 — `vs[key] = value` stores KV only, not in vector index

**Choice:** `__setitem__` is a plain KV put. It does not touch the vector index.

**Rationale:** `VectorStore` is a strict superset of `KVStore`. Plain KV semantics must
be preserved so callers can store metadata, config, or non-vectorised documents in the
same store without polluting the vector index.

`vector_put(key, value, vector)` is the explicit entry point for indexed writes.

---

## Decision 16 — `vector_put` on existing key: new integer label, mark old deleted

**Choice:** When `vector_put` is called for a key that already has a vector:
1. The old usearch integer label is read from `__snkv_vec_id__k`.
2. A new `next_id` is assigned.
3. The SNKV transaction updates all four CFs atomically.
4. Post-commit: `index.remove(old_id)` then `index.add(new_id, new_vector)`.

**Alternative:** Reuse the same integer label and call `index.update(id, new_vector)`.

**Rejected because:** usearch has no `update` method. The only supported path for changing
a vector is remove + add. Using a new label avoids any ambiguity about whether the old
graph edges have been cleaned up.

---

## Decision 17 — `drop_vector_index()` removes CFs but keeps KV data

**Choice:** `drop_vector_index()` deletes all `__snkv_vec*__` CFs. The user's data in the
default CF is untouched and accessible via `get()`.

**Why:** Vector indexes can be rebuilt by re-inserting vectors. Destroying user data
because the caller wanted to rebuild the index would be catastrophic. The asymmetry is
intentional and documented.

---

## Decision 18 — Encryption: optional `password` parameter; `AuthError` on mismatch

**Choice:** `VectorStore` accepts an optional `password` parameter:

```python
# Unencrypted (default)
vs = VectorStore("store.db", dim=1536)

# Encrypted — all CF values (vectors, IDs, metadata) encrypted transparently
vs = VectorStore("store.db", dim=1536, password=b"my-secret")
```

Internally:
- `password=None` → `KVStore(path, ...)` — plain open
- `password=bytes` → `KVStore.open_encrypted(path, password, ...)` — encrypted open

SNKV's XChaCha20-Poly1305 encryption is transparent: all CF reads and writes go through
the encryption layer automatically. The in-memory usearch index always holds plaintext
vectors (required for ANN computation) — only the on-disk representation is encrypted.

**Error behaviour:**
- Opening a plain store with `password=` → **encrypts the store** (first-time encryption via
  Argon2id key derivation + XChaCha20-Poly1305). This is intentional: `open_encrypted` on a
  plain store is the standard way to add encryption after the fact.
- Opening an encrypted store without `password=` → `AuthError` (SNKV auth guard in
  `kvstore_open_v2` detects the `__snkv_auth__` CF and rejects the plain open)
- Wrong password → `AuthError` (Argon2id verify token mismatch)

The two `AuthError` cases propagate unchanged to the caller — no extra wrapping.

**Why not auto-detect encryption and prompt for password:**
- Requiring an explicit `password=` argument makes the security contract visible at the
  call site. Silent auto-detection would hide the fact that the store is encrypted.
- Consistent with how `KVStore.open_encrypted` works in the base API.

---

## Decision 19 — Parallelism: spawn processes, not threads

**Choice:** Concurrent access to a `VectorStore` is achieved by spawning multiple
processes, each opening their own `VectorStore` instance against the same `.db` file.
Multi-threading within a single `VectorStore` instance is not supported for writes
(Decision 11).

**Why processes work:**
- SNKV uses SQLite WAL mode, which supports multiple concurrent readers and one writer
  across processes — no extra coordination needed at the KV layer.
- Each process holds its own in-memory usearch index and its own file descriptor. There
  is no shared mutable state between processes.
- This matches the multi-process pattern already demonstrated by SNKV's existing
  `multiprocess_writer.py` / `multiprocess_reader.py` examples.

**Recommended patterns:**

```
# Read-heavy: N reader processes, 1 writer process
writer process  →  opens VectorStore, calls vector_put
reader process  →  opens VectorStore (read-only view), calls search
```

```
# Write-heavy: serialize writes through one process; broadcast reads
coordinator  →  receives write requests, performs vector_put
workers      →  each opens own VectorStore, performs search
```

**Known limitation — stale in-memory index across processes:**
Each process builds its usearch index independently on open from the shared SNKV file.
If process A adds a vector after process B has already opened, process B's in-memory
index does not see the new entry until it reopens.

This is expected behaviour for embedded stores. Mitigations:
- Reader processes that need up-to-date results should reopen (or rebuild) periodically.
- For workloads where readers must see all writes immediately, funnel both reads and writes
  through a single process.

**Encrypted multi-process:** each process passes the same `password=` to `VectorStore`;
SNKV handles per-process key derivation and decryption independently.

---

## Decision 20 — Vector quantization: `dtype` parameter; on-disk always `float32`

**Choice:** `VectorStore` accepts a `dtype` parameter controlling the in-memory usearch
index precision:

| `dtype=` | usearch dtype | Memory vs f32 | Typical recall |
|---|---|---|---|
| `"f32"` (default) | `"f32"` | 1× (baseline) | 100% |
| `"f16"` | `"f16"` | 0.5× | ~99% |
| `"i8"` | `"i8"` | 0.25× | ~95–98% |

**On-disk representation is always `float32` regardless of `dtype`.**

The `__snkv_vec__` CF stores raw `float32` bytes. The usearch `Index` is constructed
with the given `dtype` and receives float32 inputs; usearch converts internally during
`add`. This design means:
- Crash recovery can always rebuild the index at any `dtype` from the stored float32 data.
- Two-stage reranking (Decision 23) can use exact float32 distances regardless of index dtype.
- The user can change `dtype` by calling `drop_vector_index()` and re-inserting (same path
  as changing `space`).

`dtype` is stored in `__snkv_vec_meta__` under key `b"dtype"`. A mismatch on reopen
raises `ValueError` to prevent silent quality degradation.

**Why not store quantized bytes on disk:**
Storing int8 bytes would save disk space but lose information permanently. If the user
later wants to rebuild with f32 precision, the original vectors are gone. Float32 on
disk is the safe choice for an embedded store where disk is cheap and vectors are
typically ≤ 100k.

---

## Decision 21 — Metadata filtering: new CF + post-filter

**Choice:** An optional `metadata` dict can be attached to each vector entry. Search
results can be filtered by metadata using either an equality dict or a callable predicate.

**Storage:**

New CF: `_snkv_vec_tags_` stores JSON-encoded metadata per key:

| CF | Key | Value |
|---|---|---|
| `_snkv_vec_tags_` | user key (bytes) | JSON-encoded dict (bytes) |

The CF is created lazily on the first `vector_put(metadata=...)`. Stores that never use
metadata have no `_snkv_vec_tags_` CF (zero overhead).

**API:**

```python
# Write with metadata
vs.vector_put(b"doc:1", b"content", vec, metadata={"category": "science", "year": 2024})

# Filter by equality dict — only results where ALL fields match
results = vs.search(query, top_k=5, filter={"category": "science"})

# Filter by callable — arbitrary predicate on the metadata dict
results = vs.search(query, top_k=5, filter=lambda m: m.get("year", 0) > 2020)

# Get stored metadata for a key
meta = vs.get_metadata(b"doc:1")   # returns dict or None
```

**Implementation — post-filter with oversampling:**

usearch has no native predicate pushdown. Filtering is applied after ANN search:

```
1. Compute oversample_k = min(count, top_k * oversample_factor)
2. index.search(query, oversample_k)  → candidate labels
3. For each candidate: fetch metadata from __snkv_vec_tags__
4. Apply filter predicate
5. Return first top_k matching results
```

`oversample_factor` defaults to `3`; the user can override with `oversample=N` in
`search()`. If fewer than `top_k` results pass the filter, the returned list is shorter
(same silent-under-delivery rule as Decision 14).

**Why post-filter, not pre-filter:**
Pre-filtering (build a candidate set from the metadata CF, then search within it) would
require HNSW to support arbitrary entry-point restriction, which usearch does not expose.
Post-filtering is the standard approach (used by Weaviate, Qdrant in dense-only mode).

**Metadata on overwrite:** if `vector_put` is called on an existing key without
`metadata=`, the existing metadata entry is preserved. Passing `metadata={}` clears it.

**Metadata on delete:** `delete()` removes the `__snkv_vec_tags__` entry within the same
transaction as the other vec CF deletes.

---

## Decision 22 — Batch insert: `vector_put_batch`

**Choice:** `vector_put_batch(items, ttl=None)` accepts an iterable of
`(key, value, vector)` or `(key, value, vector, metadata)` tuples and writes all of them
in a single SNKV transaction, then updates the usearch index in one `index.add` call.

**Why batch is faster than N individual `vector_put` calls:**
- One transaction: one WAL commit, one fsync, one checkpoint trigger check.
- One `index.add(all_ids, all_vectors)`: usearch can parallelise the graph construction
  across vectors in the batch.
- For initial load of 100k vectors, batch is typically 5–20× faster than individual puts.

**Atomicity:**
The entire batch is one SNKV transaction. If any item fails (invalid vector shape, disk
full, etc.), the transaction rolls back and zero items are written. The usearch index is
only updated after a successful commit — no partial index state.

**Behaviour on overwrite within a batch:**
If the same key appears twice in the batch, the last occurrence wins (same semantics as
inserting the same key twice in a transaction).

**TTL in batch:**
A single `ttl=seconds` applies uniformly to all items in the batch. Per-item TTL is not
supported in `vector_put_batch`; use individual `vector_put` calls for mixed TTLs.

---

## Decision 23 — Two-stage search: ANN candidate fetch + exact float32 rerank

**Choice:** `search(query, top_k=10, rerank=False, oversample=3)`.

When `rerank=True`:
1. **Stage 1 (fast):** `index.search(query, top_k * oversample)` — ANN candidates using
   the index dtype (may be quantized f16 or i8).
2. **Stage 2 (exact):** Fetch stored float32 vectors from `__snkv_vec__` for each
   candidate. Compute exact distances in Python/numpy. Re-sort by exact distance.
3. Return top_k results ordered by exact distance.

**When to use `rerank=True`:**
- When `dtype="i8"` or `dtype="f16"` — quantization degrades ANN precision; reranking
  restores exact ordering in the final top_k.
- When the application requires the nearest neighbour to be exactly correct (not just
  approximately correct).

**When `rerank=False` (default):**
Backwards-compatible — Stage 2 is entirely skipped. For `dtype="f32"`, reranking
produces identical results to no reranking (ANN distances are already exact at f32).

**`oversample` parameter:**
Controls Stage 1 candidate pool size. Higher oversample = higher final recall at the
cost of more KV reads in Stage 2. Default `3` is a good balance for i8/f16.

**Distance computation in Stage 2:**
| `space=` | Stage 2 formula |
|---|---|
| `"l2"` | `np.sum((a - b) ** 2, axis=1)` (squared L2, consistent with usearch l2sq) |
| `"cosine"` | `1 - (a @ b) / (norm(a) * norm(b))` |
| `"ip"` | `-np.dot(a, b)` (negated, lower = more similar, consistent with usearch ip) |

**Filter + rerank interaction:**
If both `filter=` and `rerank=True` are provided: Stage 1 fetches `top_k * oversample`
candidates, Stage 2 fetches vectors + metadata, applies filter and rerank simultaneously,
returns first top_k passing the filter ordered by exact distance.

---

## Decision 24 — Index statistics: `vector_stats()`

**Choice:** `vector_stats()` returns a comprehensive dict covering both configuration and
runtime state:

```python
{
    # Configuration (from meta CF)
    "dim":              1536,
    "space":            "cosine",
    "dtype":            "f32",
    "connectivity":     16,
    "expansion_add":    128,
    "expansion_search": 64,

    # Runtime state
    "count":            42_000,     # active (non-deleted) vectors in usearch index
    "capacity":         84_000,     # max_elements allocated in usearch index
    "fill_ratio":       0.50,       # count / capacity
    "vec_cf_count":     42_100,     # entries in __snkv_vec__ (may include expired)
    "has_metadata":     True,       # whether __snkv_vec_tags__ CF exists
}
```

**Why `vec_cf_count` vs `count` may differ:**
Expired keys are removed from the KV default CF by lazy expiry but may still have
entries in `__snkv_vec__` until `vector_purge_expired()` is called. The gap between
`vec_cf_count` and `count` indicates how many orphaned vector entries are waiting to
be purged.

**`fill_ratio`:**
When `fill_ratio >= 0.9`, the next `vector_put` calls `index.reserve(capacity * 2)`
to pre-allocate capacity before the add (usearch 2.x API). `vector_stats()` makes this visible so callers can
pre-resize if desired.

**`has_metadata`:**
True if `__snkv_vec_tags__` CF exists (i.e., at least one `vector_put(metadata=...)` was
ever called). Lets callers know whether metadata filtering is available.

**`__len__`:**
`len(vs)` returns the active vector count (same as `vector_stats()["count"]`). Standard
Python protocol — callers should not need to call `vector_stats()` just to check size.

---

## Decision 25 — `expansion_search` persistence: stored in meta, restored on reopen

**Choice:** `expansion_search` is stored in `__snkv_vec_meta__` under key
`b"expansion_search"` and restored from meta on every `VectorStore` open.

**Why this is a bug without persistence:**
`expansion_search` is the usearch query-time beam width (`ef` in hnswlib terms). Higher
values give better recall at the cost of latency. If a user opens with
`expansion_search=128` (high recall), closes, and reopens without passing the parameter,
the index silently falls back to the default (`64`). For `dtype="i8"` stores where high
`expansion_search` compensates for quantization loss, this is a silent correctness
regression.

**Implementation:**
- Constructor signature: `expansion_search: Optional[int] = None` (sentinel, **not** `int = 64`).
  If `None`, the open logic falls back to the stored value, then to the hard default (`64`).
  If an integer is passed, it overrides the stored value.
- `_write_full_meta()` adds: `self._meta_cf.put(b"expansion_search", _pack_i64(self._expansion_search))`
- `_rebuild_index()` logic:
  ```python
  expsch_raw = self._meta_cf.get(b"expansion_search")
  if self._expansion_search is None:
      # caller did not pass a value — use stored, fall back to 64
      self._expansion_search = _unpack_i64(expsch_raw) if expsch_raw else 64
  # else: caller passed an explicit value — keep it, stored value is ignored
  self._index.expansion_search = self._expansion_search
  ```
- This differs from `connectivity` and `expansion_add`, where the stored value always wins
  because those parameters affect the HNSW graph structure built at insert time.
  `expansion_search` is query-time only and does not affect the graph, so the caller's
  intent at open time is authoritative when explicitly supplied.

**Note:** Unlike `dim`, `space`, and `dtype`, `expansion_search` is not immutable — users
may legitimately open the same store with different `expansion_search` values at different
times (trading recall for speed). The stored value is the last-used value, serving as a
sensible default on the next open.

---

## Decision 26 — `max_distance` filter in `search()`

**Choice:** `search(query, top_k=10, *, max_distance=None, ...)` adds an optional
distance threshold. Results with distance strictly greater than `max_distance` are
dropped before returning.

```python
# Only return results within distance 0.3 (cosine)
results = vs.search(query, top_k=10, max_distance=0.3)

# Combine with filter and rerank
results = vs.search(query, top_k=5, max_distance=0.5,
                    filter={"category": "news"}, rerank=True)
```

**Why in the library, not caller-side:**
Every production semantic search call applies a distance cutoff. Without it, a query
against a nearly empty store returns distant, irrelevant results. The caller can
post-filter, but a built-in threshold is cleaner and documents intent.

**Application order:**
```
ANN candidates → [rerank if requested] → [filter if requested] → drop > max_distance → return
```
`max_distance` is applied last so it operates on the final (exact, if reranked) distances.

**`max_distance` and silent under-delivery:**
If all candidates exceed `max_distance`, the returned list is empty. This is consistent
with Decision 14 (silent under-delivery). No exception is raised.

**Distance scale:**
`max_distance` uses the same scale as the distances returned in `SearchResult.distance`
— squared L2 for `space="l2"`, cosine distance (0–2) for `space="cosine"`, negative
inner product for `space="ip"`.

---

## Known limitations

**One vector space per `.db` file.**
All internal CFs use fixed names (`_snkv_vec_`, `_snkv_vec_idk_`, etc.). Opening
two `VectorStore` instances against the same `.db` file with different `dim` or `space`
values will corrupt both indexes — the second open will fail at the dim/space mismatch
check, but if somehow bypassed the CF data would interleave.

**Correct pattern for multiple embedding models (e.g. text + image):**
```python
# Use separate files
text_vs  = VectorStore("store_text.db",  dim=384,  space="cosine")
image_vs = VectorStore("store_image.db", dim=512,  space="l2")
```

Multi-space support in a single file (CF name namespacing) is a candidate for a future
decision (D27).

---

## Test plan additions (tests/test_vector.py)

### Vector quantization (Decision 20) — 4 tests

| # | Test |
|---|---|
| T1 | `dtype="f16"`: insert 10 vectors, search, nearest neighbour correct |
| T2 | `dtype="i8"`: insert 10 vectors, search, nearest neighbour correct |
| T3 | `dtype` stored in meta, survives close + reopen |
| T4 | `dtype` mismatch on reopen → `ValueError` |

### Metadata filtering (Decision 21) — 6 tests

| # | Test |
|---|---|
| T5 | `vector_put(metadata=…)` → `get_metadata()` returns same dict |
| T6 | `search(filter={"k": "v"})` — only matching docs returned |
| T7 | `search(filter=callable)` — predicate applied correctly |
| T8 | Overwrite without `metadata=` preserves existing metadata |
| T9 | `delete(key)` removes entry from `__snkv_vec_tags__` |
| T10 | `search(filter=…)` with highly selective filter returns < top_k (no exception) |

### Batch insert (Decision 22) — 4 tests

| # | Test |
|---|---|
| T11 | `vector_put_batch(items)` — all items searchable after call |
| T12 | Correct `vector_stats()["count"]` after batch |
| T13 | Empty batch — no-op, no exception |
| T14 | Batch with invalid vector shape — entire batch rolls back, count unchanged |

### Two-stage search (Decision 23) — 4 tests

| # | Test |
|---|---|
| T15 | `search(rerank=True)` with `dtype="f32"` — same top result as `rerank=False` |
| T16 | `search(rerank=True)` with `dtype="i8"` — top-1 result matches brute-force exact |
| T17 | `search(rerank=True, oversample=5)` — result list length ≤ top_k |
| T18 | `search(filter=…, rerank=True)` — filter and rerank compose correctly |

### Index statistics (Decision 24) — 3 tests

| # | Test |
|---|---|
| T19 | `vector_stats()` returns correct `count` and `dim` after inserts |
| T20 | `vector_stats()["fill_ratio"]` between 0.0 and 1.0 |
| T21 | `vector_stats()["has_metadata"]` False before any metadata put, True after |

### `expansion_search` persistence (Decision 25) — 3 tests

| # | Test |
|---|---|
| T22 | Open with `expansion_search=128`, close, reopen without passing param → `expansion_search` restored to `128` |
| T23 | `vector_stats()["expansion_search"]` reflects stored value after reopen |
| T24 | Open with `expansion_search=128`, close, reopen with explicit `expansion_search=32` → stored value overridden to `32` |

### `max_distance` filter (Decision 26) — 3 tests

| # | Test |
|---|---|
| T25 | `search(max_distance=0.0)` on non-empty index → empty result, no exception |
| T26 | `search(max_distance=large)` → same results as no threshold |
| T27 | `search(filter=…, rerank=True, max_distance=X)` — all three compose correctly |

---

## Summary table

| # | Decision | Choice |
|---|---|---|
| 1 | Vector library | usearch (Apache-2.0) |
| 2 | Integration layer | Python wrapper |
| 3 | Dependency model | Optional extra (`snkv[vector]`) |
| 4 | Persistence | SNKV column families (single file) |
| 5 | CF naming | `__snkv_vec__` prefix + per-CF suffix |
| 6 | Integer labels | Monotonically increasing, never reused |
| 7 | Atomicity | SNKV commit first, usearch update second |
| 8 | Index startup | Rebuild from CFs (Phase 1); snapshot (Phase 2) |
| 9 | Encryption of vectors | CF values encrypted; snapshot deferred |
| 10 | TTL expiry | Lazy-skip in search; explicit `vector_purge_expired()` |
| 11 | Thread safety | One instance per thread; no internal lock |
| 12 | `space` mutability | Fixed at creation; mismatch = ValueError |
| 13 | `dim` mutability | Fixed at creation; mismatch = ValueError |
| 14 | Partial search results | Silent under-delivery allowed |
| 15 | `vs[key] = value` | KV only; does not touch vector index |
| 16 | Overwrite existing vector | New label + old label removed |
| 17 | `drop_vector_index` | Removes CFs; KV data preserved |
| 18 | Encryption | Optional `password=`; `AuthError` on mismatch |
| 19 | Parallelism | Spawn processes; each holds own usearch index |
| 20 | Vector quantization | `dtype="f32/f16/i8"`; on-disk always f32 |
| 21 | Metadata filtering | `__snkv_vec_tags__` CF; post-filter with oversample |
| 22 | Batch insert | Single transaction + single `index.add`; all-or-nothing |
| 23 | Two-stage search | ANN candidates → exact f32 rerank; `rerank=False` default |
| 24 | Index statistics | `vector_stats()` returns config + runtime counters; `__len__` |
| 25 | `expansion_search` persistence | Stored in meta; restored on reopen; caller arg overrides |
| 26 | `max_distance` filter | Applied last after rerank; silent empty list if none pass |
| — | Known limitations | One vector space per `.db` file; multi-space = separate files |
