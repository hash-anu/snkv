# SNKV Vector Store — Production Integration Design

**Status**: Proposed
**Author**: SNKV Contributors
**Date**: 2026-03-07
**Revision**: 3

---

## Table of Contents

1. [Overview](#1-overview)
2. [Design](#2-design)
3. [Security and Input Validation](#3-security-and-input-validation)
4. [API](#4-api)
5. [Error Handling Contract](#5-error-handling-contract)
6. [Files](#6-files)
7. [Build System](#7-build-system)
8. [Implementation Notes](#8-implementation-notes)
9. [CI/CD Pipeline](#9-cicd-pipeline)
10. [Observability](#10-observability)
11. [Test Plan](#11-test-plan)
12. [Performance Validation](#12-performance-validation)
13. [Backwards Compatibility and Rollback](#13-backwards-compatibility-and-rollback)
14. [Packaging and Release](#14-packaging-and-release)
15. [Risks and Mitigations](#15-risks-and-mitigations)
16. [Release Checklist](#16-release-checklist)
17. [Delivery Milestones](#17-delivery-milestones)
18. [Future Work](#18-future-work)

---

## 1. Overview

This document is the complete specification for integrating approximate nearest-neighbour
(ANN) vector search into SNKV using [usearch](https://github.com/unum-cloud/usearch).
Following this document end-to-end produces production-ready code.

### 1.1 Goal

Allow SNKV to serve as a self-contained embedded vector database with no separate
Chroma, Qdrant, or FAISS process. All data lives in a single `.db` file alongside
a rebuildable sidecar index file per vector space.

### 1.2 Why usearch

| Criterion | Flat (scratch) | **usearch** | FAISS |
|---|---|---|---|
| Production-ready | No | **Yes** | Yes |
| Scale | <500K vectors | **Billions** | Billions |
| Dependencies | None | **Header-only C++** | BLAS/LAPACK |
| C API | Custom | **Built-in** | Yes |
| Quantization | No | **f32/f16/int8/b1** | Yes |
| License | — | **Apache 2.0** | MIT |
| Actively maintained | — | **Yes (Cloudflare, Unum)** | Yes |

usearch is header-only C++17 (`index_dense.hpp`). SNKV's public API remains pure C.
All C++ is confined to `src/kvvec.cpp`.

---

## 2. Design

### 2.1 Concepts

A **vector space** (`KVVecSpace`) is the user-facing handle, analogous to `KVColumnFamily`.
Each vector space has a name, fixed dimension, metric, and quantization level.
Multiple vector spaces coexist in the same `KVStore`.

### 2.2 Storage Layout

```
mydb.db                         ← SNKV B-tree (crash-safe, ground truth)
mydb.db.vec/
  docs.usvec                    ← usearch HNSW sidecar (rebuildable)
  docs.usvec.tmp                ← atomic staging file (renamed on success)
  embeddings.usvec
```

Three internal column families per vector space (reserved `__` prefix):

| CF Name | Key | Value |
|---|---|---|
| `__snkv_vec_cfg__<space>` | `"cfg"` | `version(1B) + dim(4B) + metric(1B) + quant(1B) + next_label(8B)` |
| `__snkv_vec_raw__<space>` | user key | `label(8B) + nDim(4B) + float32×nDim` |
| `__snkv_vec_lbl__<space>` | `label(8B BE)` | user key bytes |

The HNSW index is loaded from / saved to the sidecar. If the sidecar is missing or
corrupt, the index is rebuilt from `__snkv_vec_raw__` (one-time cost, no data loss).

### 2.3 Storage Format Versioning

The first byte of the `cfg` blob is a format version, currently `0x01`.

On open, if the stored version exceeds the compiled-in version:
```
KVSTORE_ERROR: "vector space format version 2 is newer than this build (max: 1)"
```

This allows future layout changes without silent data corruption.

### 2.4 Key→Label Mapping

usearch uses `uint64` labels internally. SNKV maintains:

- `raw` CF: `user_key → [label(8B)][nDim(4B)][float32 vector]` — retrieval + rebuild
- `lbl` CF: `[label 8B BE] → user_key` — resolving search results to user keys
- `cfg.next_label`: monotonically increasing `uint64`, persisted in B-tree

Labels are never reused after delete. `uint64` overflow is not a practical concern
(580,000 years at 1M inserts/sec).

### 2.5 Name Length Constraint

```c
#define KVVEC_MAX_SPACE_NAME  239   /* 255 (KVSTORE_MAX_CF_NAME) - 16 (longest prefix) */
```

`kvvec_space_open` returns `KVSTORE_ERROR` if `strlen(zName) > KVVEC_MAX_SPACE_NAME`
or if `zName` contains path-traversal characters (see §3).

### 2.6 Operation Flows

**`kvvec_put(key, vec)`**
1. Validate inputs: `nKey > 0`, `nDim == space->nDim`, all float values finite (§3)
2. Lock mutex
3. Look up key in `raw` CF — if found, read old label, call `index.remove(old_label)`,
   delete from `lbl` CF
4. If `index.size() >= 0.8 * index.capacity()`, call `index.reserve(capacity * 2)`
5. Assign `next_label++`
6. Write `[label][nDim][vec]` to `raw` CF + `label→key` to `lbl` CF in one
   B-tree write transaction (committed before HNSW update)
7. Call `index.add(next_label, vec)` — if this throws (OOM), rollback the B-tree write
   and return `KVSTORE_NOMEM`
8. Unlock mutex

**`kvvec_search(query, k)`**
1. Validate: `nDim == space->nDim`, all query values finite, `k > 0`
2. Lock mutex
3. `index.search(query, k)` → `(label, distance)` pairs
4. For each label, look up user key in `lbl` CF
5. Allocate `KVVecResult[]`, copy keys, unlock mutex
6. Return results sorted by distance ascending

**`kvvec_delete(key)`**
1. Lock mutex
2. Look up key in `raw` CF — if missing, unlock and return `KVSTORE_NOTFOUND`
3. Read label; call `index.remove(label)`
4. Delete from `raw` CF and `lbl` CF in one transaction
5. Unlock mutex

**`kvvec_flush()`**
1. No-op for in-memory KVStores (return `KVSTORE_OK`)
2. Lock mutex
3. Ensure sidecar directory exists; create if needed
4. `index.save(tmp_path)` — write to `<space>.usvec.tmp`
5. If save fails: delete `.tmp`, unlock, return `KVSTORE_ERROR`
6. If disk full (`errno == ENOSPC`): delete `.tmp`, return `KVSTORE_ERROR` with
   message `"disk full: could not flush vector index for space '<name>'"`
7. `rename(tmp_path, sidecar_path)` — atomic on POSIX;
   `MoveFileExW(..., MOVEFILE_REPLACE_EXISTING)` on Windows
8. Unlock mutex

**`kvvec_compact()`**
1. Lock mutex; iterate `raw` CF, collect all `(label, vec)` pairs
2. Build a fresh `index_dense_t` with same config into a temp pointer
3. Re-add all vectors (calling `progress_cb` every 10,000 vectors)
4. On OOM during rebuild: free temp index, return `KVSTORE_NOMEM` (original index preserved)
5. On success: swap in new index, call `kvvec_flush()`
6. Unlock mutex

**`kvvec_space_open()` — existing space**
1. Validate `zName` (length, characters)
2. Read `cfg` CF — version check, read dim/metric/quant
3. If caller supplied `pConfig->dimensions > 0` and differs from stored → `KVSTORE_ERROR`:
   `"dimension mismatch: space 'docs' has 1536, caller specified 768"`
4. If sidecar exists and loads cleanly: use it
5. If sidecar missing or corrupt (`index.load()` throws): rebuild from `raw` CF
6. Mismatch detection: if `index.size() + index.removed_count() != next_label`, rebuild

**`kvvec_space_open()` — new space**
1. Validate `zName` and `pConfig->dimensions > 0`
2. Write `cfg` blob (version=1, next_label=0) to B-tree
3. Create sidecar directory; create fresh usearch index

### 2.7 Crash Safety

| Scenario | Result on next open |
|---|---|
| Crash before B-tree commit | Key not in store — consistent |
| Crash after B-tree commit, before `index.add` | `next_label` mismatch triggers rebuild |
| Crash during `kvvec_flush` | `.tmp` left behind; original `.usvec` intact; `.tmp` cleaned up on next open |
| Sidecar corrupt | `index.load()` throws → rebuild from `raw` CF |

On open, any `.usvec.tmp` files in the sidecar directory are deleted before loading.

### 2.8 Capacity Auto-Resize

- Initial capacity: `max(pConfig->capacity, 1024)`
- Resize trigger: `index.size() >= 0.8 * index.capacity()`
- Resize amount: `index.reserve(current_capacity * 2)`
- Memory formula for user guidance: `nVectors × dim × bytes_per_element`
  where `bytes_per_element` = 4 (f32), 2 (f16), 1 (int8), 0.125 (b1)

### 2.9 Compaction Policy

usearch tombstones deleted entries but does not compact automatically.
Recommended caller pattern:

```c
KVVecStats st;
kvvec_stats(space, &st);
if (st.nRemoved > st.nVectors / 5) {   /* >20% tombstoned */
    kvvec_compact(space, NULL, NULL);
}
```

The `nRemoved` counter is exposed in `KVVecStats`.

### 2.10 Multi-Process Access

- Each process loads its own in-memory usearch index from the sidecar on open.
- **Contract**: only one process writes to a given vector space at a time.
- Reader processes call `kvvec_reload()` to pick up writes from the writer.
- Multi-writer is undefined behaviour — callers must use an external file lock.

### 2.11 In-Memory KVStore

When opened with `zFilename = NULL`:
- No sidecar is created; `kvvec_flush()` is a no-op.
- The HNSW index lives in memory for the session only.
- `kvvec_space_open` always creates a new space (nothing to load).

---

## 3. Security and Input Validation

All validation is performed before the mutex is acquired.

### 3.1 Space Name Validation

```c
static int kvvecValidateName(const char *zName) {
  int n = (int)strlen(zName);
  if (n == 0 || n > KVVEC_MAX_SPACE_NAME) return KVSTORE_ERROR;
  /* Reject path traversal and shell-unsafe characters */
  for (int i = 0; i < n; i++) {
    char c = zName[i];
    int ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
             (c >= '0' && c <= '9') || c == '_' || c == '-';
    if (!ok) return KVSTORE_ERROR;
  }
  /* Reject reserved prefix */
  if (strncmp(zName, "__snkv_", 7) == 0) return KVSTORE_ERROR;
  return KVSTORE_OK;
}
```

Allowed characters: `[a-zA-Z0-9_-]`. Rejects `.`, `/`, `\`, `..`, null bytes,
and the internal `__snkv_` prefix.

Error message: `"invalid vector space name: only [a-zA-Z0-9_-] are allowed"`

### 3.2 Vector Value Validation

All vectors (on put and on search) are checked for non-finite values before
being passed to usearch:

```c
static int kvvecValidateVec(const float *pVec, int nDim) {
  for (int i = 0; i < nDim; i++) {
    if (!isfinite(pVec[i])) return KVSTORE_ERROR;
  }
  return KVSTORE_OK;
}
```

Error message: `"vector contains NaN or Inf at index %d"`.

**Why**: NaN/Inf in vectors causes usearch HNSW distance computations to produce
undefined results, silently corrupting the graph structure.

### 3.3 Dimension Bounds

```c
#define KVVEC_MAX_DIM  65536   /* 65536 * 4 bytes = 256KB per vector, within 10MB value limit */
#define KVVEC_MIN_DIM  1
```

`kvvec_space_open` returns `KVSTORE_ERROR` if `dimensions < KVVEC_MIN_DIM` or
`dimensions > KVVEC_MAX_DIM`.

### 3.4 Key and k Bounds

- `nKey <= 0` or `nKey > KVSTORE_MAX_KEY_SIZE`: `KVSTORE_ERROR`
- `k <= 0`: `KVSTORE_ERROR` with message `"k must be >= 1"`
- `k > 10000`: `KVSTORE_ERROR` with message `"k must be <= 10000"`

### 3.5 OOM Handling in usearch

usearch throws `std::bad_alloc` on OOM. All calls to `index.add()`,
`index.reserve()`, and `index.search()` are wrapped in `try/catch`:

```cpp
try {
  pSpace->pIndex->add(label, pVec);
} catch (const std::bad_alloc &) {
  /* rollback B-tree write */
  sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
  sqlite3_mutex_leave(pSpace->pMutex);
  return KVSTORE_NOMEM;
} catch (const std::exception &e) {
  /* log e.what() via SNKV internal logging */
  sqlite3_mutex_leave(pSpace->pMutex);
  return KVSTORE_ERROR;
}
```

No C++ exception ever propagates across the `extern "C"` boundary.

---

## 4. API

### 4.1 C API — `include/kvvec.h`

```c
/* SPDX-License-Identifier: Apache-2.0 */
#ifndef _KVVEC_H_
#define _KVVEC_H_

#include "kvstore.h"
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct KVVecSpace KVVecSpace;

/* Name length limit */
#define KVVEC_MAX_SPACE_NAME  239
#define KVVEC_MAX_DIM         65536
#define KVVEC_MIN_DIM         1

/* Distance metrics */
#define KVVEC_METRIC_COSINE  0   /* normalized dot product (default) */
#define KVVEC_METRIC_L2      1   /* Euclidean distance */
#define KVVEC_METRIC_DOT     2   /* inner product */

/* Quantization */
#define KVVEC_QUANT_F32   0   /* float32 — full precision (default) */
#define KVVEC_QUANT_F16   1   /* float16 — ~2x smaller index */
#define KVVEC_QUANT_INT8  2   /* int8    — ~4x smaller index */
#define KVVEC_QUANT_B1    3   /* binary  — ~32x smaller index */

typedef struct {
  int    dimensions;         /* REQUIRED for new spaces (ignored for existing) */
  int    metric;             /* KVVEC_METRIC_* (default: COSINE) */
  int    quantization;       /* KVVEC_QUANT_*  (default: F32) */
  size_t capacity;           /* initial capacity hint (default: 1024) */
  int    connectivity;       /* HNSW M (default: 16) */
  int    expansion_add;      /* HNSW ef_construction (default: 128) */
  int    expansion_search;   /* HNSW ef at query time (default: 64) */
} KVVecConfig;

/*
** Progress callback for rebuild/compact operations.
** Called approximately every 10,000 vectors. May be NULL.
*/
typedef void (*KVVecProgressFn)(uint64_t done, uint64_t total, void *ctx);

typedef struct {
  void  *pKey;      /* caller must snkv_free() */
  int    nKey;
  float  distance;  /* lower = more similar */
} KVVecResult;

typedef struct {
  uint64_t nVectors;       /* live vector count */
  uint64_t nRemoved;       /* tombstoned entries not yet compacted */
  uint64_t capacity;       /* current usearch index capacity */
  int      dimensions;
  int      metric;
  int      quantization;
  size_t   index_bytes;    /* in-memory HNSW index size in bytes */
} KVVecStats;

/*
** Open or create a vector space.
**
** zName: max KVVEC_MAX_SPACE_NAME chars, [a-zA-Z0-9_-] only.
**
** Existing space:
**   - Stored config is used; pConfig is ignored except for dimensions
**     conflict check (returns KVSTORE_ERROR with descriptive message).
**   - Sidecar loaded if valid; otherwise rebuilt from raw vectors.
**   - progress_cb called during rebuild (may be NULL).
**
** New space:
**   - pConfig->dimensions required (> 0).
**   - Sidecar directory created automatically.
**
** Returns KVSTORE_OK on success.
*/
int kvvec_space_open(
  KVStore           *pKV,
  const char        *zName,
  const KVVecConfig *pConfig,
  KVVecProgressFn    progress_cb,
  void              *progress_ctx,
  KVVecSpace       **ppSpace
);

/*
** Flush HNSW index to sidecar (atomic write) and free all resources.
** If flush fails, resources are still freed; KVSTORE_ERROR is returned.
** Data in B-tree is never affected by flush failure.
*/
int kvvec_space_close(KVVecSpace *pSpace);

/*
** Delete all data for this space and remove the sidecar.
** Any open handle for this space must be closed first.
*/
int kvvec_space_drop(KVStore *pKV, const char *zName);

/*
** Insert or update a vector.
**
** - nDim must match space dimensions (KVSTORE_ERROR otherwise).
** - All float values must be finite (KVSTORE_ERROR if NaN/Inf present).
** - B-tree write is committed before HNSW update (crash-safe).
** - Index is auto-resized at 80% capacity.
** - Returns KVSTORE_NOMEM if usearch allocation fails (B-tree write rolled back).
*/
int kvvec_put(
  KVVecSpace  *pSpace,
  const void  *pKey, int nKey,
  const float *pVec, int nDim
);

/*
** Remove a vector by key.
** Returns KVSTORE_NOTFOUND if key does not exist.
*/
int kvvec_delete(
  KVVecSpace *pSpace,
  const void *pKey, int nKey
);

/*
** Retrieve the raw float32 vector for a key (B-tree lookup, no HNSW).
** *ppVec is heap-allocated; caller must snkv_free(*ppVec).
*/
int kvvec_get(
  KVVecSpace  *pSpace,
  const void  *pKey, int nKey,
  float      **ppVec, int *pnDim
);

/*
** Approximate nearest-neighbour search.
**
** - All query values must be finite (KVSTORE_ERROR if NaN/Inf present).
** - k must be in [1, 10000].
** - *pnResults may be < k if fewer vectors exist.
** - Each result->pKey must be snkv_free()'d; use kvvec_results_free() for bulk.
** - Results sorted by distance ascending.
*/
int kvvec_search(
  KVVecSpace      *pSpace,
  const float     *pQuery, int nDim, int k,
  KVVecResult    **ppResults, int *pnResults
);

/* Free all pKey pointers and the results array itself. */
void kvvec_results_free(KVVecResult *pResults, int nResults);

/*
** Serialize HNSW index to sidecar via atomic temp+rename.
** No-op for in-memory KVStores.
** Returns KVSTORE_ERROR on disk full or I/O failure (with descriptive errmsg).
*/
int kvvec_flush(KVVecSpace *pSpace);

/*
** Rebuild HNSW index from raw vectors, eliminating tombstoned entries.
** Recommended when nRemoved > nVectors / 5.
** On OOM: original index is preserved, KVSTORE_NOMEM returned.
** On success: compacted index is flushed to sidecar.
*/
int kvvec_compact(
  KVVecSpace      *pSpace,
  KVVecProgressFn  progress_cb,
  void            *progress_ctx
);

/*
** Reload HNSW index from sidecar (multi-process reader pattern).
** Returns KVSTORE_ERROR if sidecar missing or corrupt (index unchanged).
*/
int kvvec_reload(KVVecSpace *pSpace);

/* Populate pStats with current metrics. */
int kvvec_stats(KVVecSpace *pSpace, KVVecStats *pStats);

#ifdef __cplusplus
}
#endif

#endif /* _KVVEC_H_ */
```

### 4.2 Python API

```python
import numpy as np
from typing import Optional, Callable

class VectorSpace:
    """
    Approximate nearest-neighbour vector space backed by SNKV + usearch.

    Thread safety:
      A single VectorSpace instance must not be used from multiple threads
      concurrently. Use separate instances per thread.

    GIL:
      put(), search(), compact(), flush(), and reload() release the GIL
      during CPU-bound or I/O-bound usearch operations.
    """

    def put(self, key: bytes | str, vector: np.ndarray) -> None:
        """
        Insert or update a vector.
        vector must be dtype=float32, shape (dim,).
        Raises ValueError for wrong dtype, wrong shape, or NaN/Inf values.
        Raises MemoryError if the index cannot be grown (OOM).
        """

    def delete(self, key: bytes | str) -> None:
        """Remove a vector. Raises NotFoundError if key does not exist."""

    def get(self, key: bytes | str) -> np.ndarray:
        """
        Return stored float32 vector for key.
        Raises NotFoundError if key does not exist.
        """

    def search(self, query: np.ndarray, k: int) -> list[tuple[bytes, float]]:
        """
        Return up to k (key, distance) pairs closest to query, sorted ascending.
        Raises ValueError for wrong dtype, shape, NaN/Inf, or k out of range.
        """

    def flush(self) -> None:
        """
        Persist HNSW index to sidecar (atomic).
        Raises OSError on disk full or I/O failure.
        No-op for in-memory stores.
        """

    def compact(self, progress: Optional[Callable[[int, int], None]] = None) -> None:
        """
        Rebuild index removing tombstoned entries. Releases GIL.
        progress(done, total) called every 10,000 vectors.
        Raises MemoryError on OOM (original index preserved).
        """

    def reload(self) -> None:
        """Reload HNSW index from sidecar (multi-process reader pattern)."""

    def stats(self) -> dict:
        """
        Return dict:
          n_vectors, n_removed, capacity, dimensions,
          metric, quantization, index_bytes
        """

    def __enter__(self): return self
    def __exit__(self, *_): self.flush()   # flushes even on exception


class KVStore:
    def vector_space(
        self,
        name: str,
        *,
        dimensions: int = 0,
        metric: str = "cosine",      # "cosine" | "l2" | "dot"
        quantization: str = "f32",   # "f32" | "f16" | "int8" | "b1"
        capacity: int = 1024,
        connectivity: int = 16,
        expansion_add: int = 128,
        expansion_search: int = 64,
        progress: Optional[Callable[[int, int], None]] = None,
    ) -> VectorSpace:
        """
        Open or create a vector space.
        Existing spaces: config ignored (dimensions conflict raises ValueError).
        name: [a-zA-Z0-9_-], max 239 chars.
        """
```

#### Python dtype enforcement

```python
def put(self, key, vector):
    if not isinstance(vector, np.ndarray):
        raise TypeError(f"vector must be np.ndarray, got {type(vector).__name__}")
    if vector.dtype != np.float32:
        raise ValueError(f"vector must be float32, got {vector.dtype} — use vector.astype(np.float32)")
    if vector.ndim != 1 or len(vector) != self._dim:
        raise ValueError(f"vector must have shape ({self._dim},), got {vector.shape}")
    # NaN/Inf check done in C layer
```

#### Python usage example

```python
import snkv
import numpy as np

db = snkv.KVStore("mydb.db")

# Open/create space
vs = db.vector_space("docs", dimensions=1536, metric="cosine", quantization="int8")

# Bulk insert with progress
for doc_id, emb in zip(doc_ids, embeddings):
    vs.put(doc_id.encode(), emb.astype(np.float32))

vs.flush()   # atomic persist

# Search
results = vs.search(query_vec, k=5)
for key, dist in results:
    print(key.decode(), f"dist={dist:.4f}")

# Compact after many deletes
st = vs.stats()
if st["n_removed"] > st["n_vectors"] // 5:
    vs.compact(progress=lambda done, total: print(f"{done}/{total}"))

# Context manager — flushes on exit
with db.vector_space("cache", dimensions=768) as cache:
    cache.put(b"q1", query_vec)
```

---

## 5. Error Handling Contract

Every public function returns a `KVSTORE_*` code. `KVSTORE_ERROR` is always
accompanied by a message retrievable via `kvstore_errmsg(pKV)`.

| Situation | Code | Message |
|---|---|---|
| Name too long | `KVSTORE_ERROR` | `"vector space name exceeds 239 characters"` |
| Invalid name chars | `KVSTORE_ERROR` | `"invalid vector space name: only [a-zA-Z0-9_-] are allowed"` |
| dim=0 for new space | `KVSTORE_ERROR` | `"dimensions must be > 0 for new vector space"` |
| dim > 65536 | `KVSTORE_ERROR` | `"dimensions %d exceeds maximum (%d)"` |
| Dimension mismatch on open | `KVSTORE_ERROR` | `"dimension mismatch: space '%s' has %d, caller specified %d"` |
| Dimension mismatch on put | `KVSTORE_ERROR` | `"put dimension %d does not match space dimension %d"` |
| NaN/Inf in vector | `KVSTORE_ERROR` | `"vector contains non-finite value at index %d"` |
| k out of range | `KVSTORE_ERROR` | `"k must be in [1, 10000], got %d"` |
| Disk full on flush | `KVSTORE_ERROR` | `"disk full: could not flush vector index for space '%s'"` |
| usearch OOM | `KVSTORE_NOMEM` | `"out of memory growing vector index for space '%s'"` |
| Format version newer | `KVSTORE_ERROR` | `"vector space format version %d is newer than this build (max: 1)"` |

Python layer maps codes to exceptions:
- `KVSTORE_NOTFOUND` → `NotFoundError`
- `KVSTORE_NOMEM` → `MemoryError`
- `KVSTORE_ERROR` → `RuntimeError` with the C message text

---

## 6. Files

### New files

| File | Purpose |
|---|---|
| `include/kvvec.h` | Public C API |
| `src/kvvec.cpp` | usearch wrapper + full implementation |
| `third_party/usearch/` | Git submodule (pinned commit) |
| `tests/test_vec.c` | 20 C unit tests |
| `tests/test_vec_stress.c` | Stress + recall validation |
| `python/snkv_vec_module.c` | CPython extension |
| `python/tests/test_vec.py` | Python unit tests |
| `examples/vec.c` | C example: RAG pipeline |
| `python/examples/vec.py` | Python example: semantic cache |
| `docs/api/VEC_API.md` | Public API reference |

### Modified files

| File | Change |
|---|---|
| `Makefile` | CXX rule, `-lstdc++ -lm`, `SNKV_VECTOR=1` guard, usearch submodule target |
| `python/snkv/__init__.py` | `VectorSpace` class, `KVStore.vector_space()` |
| `python/pyproject.toml` | `[project.optional-dependencies] vec = ["numpy>=1.21"]` |
| `docs/api/API_SPECIFICATION.md` | Link to `VEC_API.md` |
| `.github/workflows/publish.yml` | Build `snkv[vec]` wheels, run vector tests |
| `.gitmodules` | usearch submodule with pinned commit |

---

## 7. Build System

### Makefile additions

```makefile
CXX        ?= g++
USEARCH_DIR = third_party/usearch
CXXFLAGS   += -std=c++17 -O2 -I$(USEARCH_DIR)/include

src/kvvec.o: src/kvvec.cpp include/kvvec.h include/kvstore.h
	$(CXX) $(CXXFLAGS) $(CFLAGS_COMMON) -c -o $@ $<

# Platform-aware C++ stdlib
ifeq ($(OS),Windows_NT)
  LDFLAGS += -lc++ -lm
else
  LDFLAGS += -lstdc++ -lm
endif

# Optional build guard — vector support off by default
ifeq ($(SNKV_VECTOR),1)
  CFLAGS  += -DSNKV_VECTOR_SUPPORT
  OBJ     += src/kvvec.o
endif

third_party/usearch:
	git submodule add https://github.com/unum-cloud/usearch $@
	git -C $@ checkout <pinned-commit-hash>
	git submodule update --init
```

When `SNKV_VECTOR_SUPPORT` is not defined, `kvvec.h` exposes stub functions that
return `KVSTORE_ERROR` with message `"built without SNKV_VECTOR_SUPPORT"`. This
allows callers to link against SNKV without the vector library and get a clear
runtime error rather than a link error.

---

## 8. Implementation Notes

### Internal struct

```cpp
struct KVVecSpace {
  KVStore                       *pKV;
  KVColumnFamily                *pCfgCF;
  KVColumnFamily                *pRawCF;
  KVColumnFamily                *pLblCF;
  char                          *zName;
  char                          *zSidecarPath;   /* NULL for in-memory */
  unum::usearch::index_dense_t  *pIndex;
  sqlite3_mutex                 *pMutex;
  int                            nDim;
  int                            metric;
  int                            quant;
  uint64_t                       nextLabel;
};
```

### Sidecar path construction (platform-safe)

```c
static void kvvecSidecarPath(char *buf, size_t sz,
                              const char *dbPath, const char *spaceName) {
#ifdef _WIN32
  snprintf(buf, sz, "%s.vec\\%s.usvec", dbPath, spaceName);
#else
  snprintf(buf, sz, "%s.vec/%s.usvec",  dbPath, spaceName);
#endif
}
```

### usearch version pin in `.gitmodules`

```ini
[submodule "third_party/usearch"]
    path = third_party/usearch
    url  = https://github.com/unum-cloud/usearch
    # Pinned — update only after reviewing usearch changelog
    # Verified: <version>, commit <hash>, date <date>
```

CI fails if the submodule HEAD differs from the pinned hash.

### Exception boundary

No C++ exception crosses the `extern "C"` boundary. Every usearch call is
wrapped in `try { ... } catch (const std::bad_alloc &) { return KVSTORE_NOMEM; }
catch (const std::exception &e) { /* log */ return KVSTORE_ERROR; }`.

### Thread safety

`KVVecSpace` acquires `pSpace->pMutex` for the full duration of every public call.
Multiple independent `KVVecSpace` instances can be used from different threads
simultaneously (each has its own mutex and index copy).

---

## 9. CI/CD Pipeline

### GitHub Actions — `.github/workflows/ci.yml`

```yaml
name: CI

on: [push, pull_request]

jobs:
  build-and-test:
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
        python: ["3.9", "3.10", "3.11", "3.12", "3.13"]

    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4
        with:
          submodules: recursive   # pulls usearch

      - name: Verify usearch pin
        run: |
          EXPECTED="<pinned-commit-hash>"
          ACTUAL=$(git -C third_party/usearch rev-parse HEAD)
          if [ "$ACTUAL" != "$EXPECTED" ]; then
            echo "usearch submodule is not at pinned commit!" && exit 1
          fi

      - name: Build C (with vector support)
        run: make SNKV_VECTOR=1 BUILD=release all

      - name: C unit tests
        run: make SNKV_VECTOR=1 test

      - name: C stress test (recall validation)
        run: ./tests/test_vec_stress   # fails if recall@10 < 0.95

      - name: Valgrind (Linux only)
        if: runner.os == 'Linux'
        run: valgrind --leak-check=full --error-exitcode=1 ./tests/test_vec

      - name: Python build
        run: pip install -e "python/.[vec]"

      - name: Python tests
        run: pytest python/tests/ -v

      - name: Build without vector support (regression)
        run: make SNKV_VECTOR=0 BUILD=release all test
```

### Wheel publishing — `.github/workflows/publish.yml`

Add to existing cibuildwheel config:
- Pass `SNKV_VECTOR=1` to the wheel build command
- Test wheel: `python -c "import snkv; db = snkv.KVStore(':memory:'); vs = db.vector_space('t', dimensions=4); vs.put(b'k', __import__('numpy').array([1,0,0,0], dtype='float32'))"`
- Only publish if all platform builds pass

---

## 10. Observability

### Structured log messages

`kvvec.cpp` uses a single internal log function that writes to `stderr` in
structured format, controllable by `KVVEC_LOG_LEVEL` environment variable
(`0`=off, `1`=errors only, `2`=info, `3`=debug; default: `1`):

```
[KVVEC ERROR] space=docs msg="disk full: could not flush vector index"
[KVVEC INFO]  space=docs msg="sidecar missing, rebuilding from 1000000 raw vectors"
[KVVEC INFO]  space=docs msg="rebuild complete" vectors=1000000 elapsed_ms=45231
[KVVEC DEBUG] space=docs msg="auto-resize" old_capacity=1024 new_capacity=2048
```

### `kvvec_stats` fields for monitoring

| Field | What to alert on |
|---|---|
| `nRemoved / nVectors > 0.2` | Call `kvvec_compact()` |
| `nVectors >= capacity * 0.9` | Index is near-full (should auto-resize, but worth monitoring) |
| `index_bytes > available_ram * 0.5` | Consider switching to `KVVEC_QUANT_INT8` |

### Python stats integration

```python
st = vs.stats()
# Push to your metrics system, e.g.:
statsd.gauge("snkv.vec.n_vectors",  st["n_vectors"],  tags={"space": name})
statsd.gauge("snkv.vec.n_removed",  st["n_removed"],  tags={"space": name})
statsd.gauge("snkv.vec.index_bytes", st["index_bytes"], tags={"space": name})
```

---

## 11. Test Plan

### C unit tests — `tests/test_vec.c` (20 tests)

| # | Test |
|---|---|
| 1 | Create space, put one vector, search k=1 → self returned, distance ≈ 0 |
| 2 | Put 1000 vectors, search k=10 → 10 results, sorted ascending |
| 3 | `kvvec_get` → returned float32 values exactly match stored values |
| 4 | Re-put same key with different vector → search returns updated vector |
| 5 | `kvvec_delete` → absent from search; `kvvec_get` returns NOTFOUND |
| 6 | `kvvec_flush`, close, reopen → sidecar loaded, search still correct |
| 7 | Delete sidecar manually, reopen → rebuild from raw CF, search correct |
| 8 | Dimension mismatch on `kvvec_put` → KVSTORE_ERROR with message |
| 9 | Dimension mismatch on `kvvec_space_open` (existing) → KVSTORE_ERROR with message |
| 10 | NaN in put vector → KVSTORE_ERROR with index of bad value |
| 11 | Inf in search query → KVSTORE_ERROR |
| 12 | k=0 → KVSTORE_ERROR; k=10001 → KVSTORE_ERROR |
| 13 | Search on empty space → 0 results, KVSTORE_OK |
| 14 | L2 metric → distances verified against brute-force Euclidean |
| 15 | `kvvec_space_drop` → all internal CFs removed, sidecar deleted |
| 16 | Two spaces in same KVStore → isolated, no cross-contamination |
| 17 | `kvvec_compact` → `nRemoved` resets to 0, search accuracy preserved |
| 18 | Name > 239 chars → KVSTORE_ERROR |
| 19 | Name with `/` or `..` → KVSTORE_ERROR |
| 20 | No `kvvec_flush`, close, reopen → `.tmp` cleaned up, sidecar rebuilt |

### C stress tests — `tests/test_vec_stress.c`

- **Recall test**: 100K random float32 vectors, dim=128, cosine. Compare HNSW
  top-10 against brute-force top-10. Assert recall@10 >= 0.95. Fail the test
  (and CI) if below threshold.
- **Capacity test**: Insert past initial capacity (auto-resize path). Assert
  no crash, no data loss, `nVectors` correct.
- **High-delete test**: Insert 50K, delete 40K, compact, search. Assert accuracy.
- **Disk full simulation**: Use a tmpfs with limited size. Assert flush returns
  `KVSTORE_ERROR`, not a crash, and the original sidecar is intact.

### Python tests — `python/tests/test_vec.py`

Mirror of all 20 C tests via Python API, plus:
- `vector.astype(np.float64)` → `ValueError` with clear dtype message
- Wrong shape `(dim, 1)` → `ValueError`
- Context manager flushes on normal and exceptional exit
- `NotFoundError` raised correctly on missing key
- `stats()` returns correct `n_removed` after deletes
- `compact(progress=cb)` — callback receives monotonically increasing `done`
- `reload()` on in-memory store → no error (no-op)

### Valgrind

```bash
valgrind --leak-check=full --error-exitcode=1 ./tests/test_vec
valgrind --leak-check=full --error-exitcode=1 ./tests/test_vec_stress
```

Zero leaks expected. usearch internal allocations delegated to
`sqlite3Malloc`/`sqlite3_free` via custom allocator wrapper.

---

## 12. Performance Validation

Before shipping, validate on the target platform (Linux x86-64, AVX2):

| Metric | Minimum acceptable |
|---|---|
| `kvvec_put` throughput (dim=128, int8) | >= 100K vectors/sec |
| `kvvec_search` k=10 (dim=128, int8) | >= 3K queries/sec |
| `kvvec_search` k=10 (dim=1536, int8) | >= 300 queries/sec |
| Recall@10 (dim=128, cosine, default params) | >= 0.95 |
| Sidecar load time (1M vecs, int8) | <= 5 seconds |
| Index rebuild time (1M vecs, int8) | <= 120 seconds |
| Memory (1M vecs, dim=128, int8) | <= 200 MB |

If any metric fails, investigate before release — do not ship with known regressions.

**Memory formula** (document in `VEC_API.md`):

```
RAM (MB) ≈ nVectors × dim × bytes_per_element / 1,048,576

bytes_per_element: f32=4, f16=2, int8=1, b1=0.125
Example: 1M × 1536 × 1 (int8) ≈ 1,536 MB
```

---

## 13. Backwards Compatibility and Rollback

### Opening a new-format DB with an old SNKV build

If a DB was written with vector support and is then opened with a SNKV build
compiled without `SNKV_VECTOR_SUPPORT`:

- The internal `__snkv_vec_*` CFs exist in the B-tree but are never accessed
  (they are hidden behind the reserved `__` prefix check)
- Regular KV operations (`kvstore_put`, `kvstore_get`, etc.) are unaffected
- The DB is fully readable and writable for non-vector keys

**No data loss. No corruption. Full backwards compatibility for non-vector operations.**

### Downgrading SNKV version

If a DB was written with vector format version 1 and opened with a future build
that only supports format version 2: the newer build detects the older format via
the version byte and migrates in-place (or rejects with a clear error — defined
per future release).

If a DB was written with vector format version 2 and opened with an old build
(max version 1): `KVSTORE_ERROR` is returned with a clear message. No silent
corruption.

### Rollback procedure

If vector support is shipped and must be rolled back:

1. Rebuild SNKV without `SNKV_VECTOR_SUPPORT` — existing non-vector data is intact
2. The `__snkv_vec_*` CFs remain in the DB file but are inert
3. Optionally: call `kvvec_space_drop` before downgrading to clean up internal CFs
4. Delete the `<dbpath>.vec/` sidecar directory manually

---

## 14. Packaging and Release

### `python/pyproject.toml`

```toml
[project.optional-dependencies]
vec = ["numpy>=1.21"]

[project]
# Existing fields unchanged
```

Install options:
```bash
pip install snkv          # no vector support (numpy not required)
pip install snkv[vec]     # with vector support (numpy required)
```

### Import guard in Python

```python
# python/snkv/__init__.py
def vector_space(self, name, *, dimensions=0, **kwargs):
    try:
        import numpy as np
    except ImportError:
        raise ImportError(
            "vector_space() requires numpy. Install it with: pip install snkv[vec]"
        )
    ...
```

### Wheel build matrix

Add to cibuildwheel:
- All existing platforms (Linux x86-64, Linux aarch64, macOS x86-64, macOS arm64, Windows x86-64)
- Pass `SNKV_VECTOR=1` in `CIBW_ENVIRONMENT`
- Include usearch submodule in sdist via `MANIFEST.in`

### Version bump

Vector support ships in version `0.5.0` (minor bump — new feature, no breaking change).
`KVVEC_VERSION` constant defined in `kvvec.h`:

```c
#define KVVEC_VERSION_MAJOR 0
#define KVVEC_VERSION_MINOR 5
#define KVVEC_VERSION_PATCH 0
```

---

## 15. Risks and Mitigations

| Risk | Mitigation |
|---|---|
| usearch API changes | Pinned commit; CI enforces pin; changelog reviewed on any update |
| Index exceeds RAM | Memory formula documented; `KVVEC_QUANT_INT8`/`B1` recommended; `index_bytes` in stats |
| Sidecar corruption on crash | Atomic temp+rename; `.tmp` cleanup on open; rebuild always possible |
| CF name overflow | `KVVEC_MAX_SPACE_NAME=239` enforced; validated before any CF is opened |
| NaN/Inf corrupts HNSW graph | `isfinite()` check on every put and search before usearch is called |
| C++ exceptions across C boundary | All usearch calls wrapped in `try/catch`; no exception escapes `extern "C"` |
| Windows path separator | Platform-guarded `kvvecSidecarPath()` helper |
| Multi-writer races | Documented as unsupported; `kvvec_reload()` for reader processes |
| Index degrades after many deletes | `nRemoved` in stats; `kvvec_compact()` with threshold guidance |
| Python GIL stall | `Py_BEGIN/END_ALLOW_THREADS` in all CPU-bound methods |
| Disk full during flush | Detected via `errno == ENOSPC`; `.tmp` cleaned; descriptive error returned |
| Old SNKV opens new-format DB | Format version byte; `KVSTORE_ERROR` with clear message |
| Wrong numpy dtype | Validated in Python layer before C call; clear `ValueError` message |
| `KVSTORE_MAX_VALUE_SIZE` (10MB) | Per-key raw vector is `dim*4` bytes; max at dim=65536 is 256KB |

---

## 16. Release Checklist

All items must pass before tagging a release.

### Code
- [ ] `include/kvvec.h` API frozen — no breaking changes after this point
- [ ] All `KVSTORE_ERROR` paths return a message via `kvstore_errmsg()`
- [ ] No C++ exception escapes `extern "C"` boundary (verified by code review)
- [ ] `kvvecValidateName` and `kvvecValidateVec` called in every public function

### Tests
- [ ] `tests/test_vec.c` — 20/20 passing on Linux, macOS, Windows
- [ ] `tests/test_vec_stress.c` — recall@10 >= 0.95 on Linux x86-64
- [ ] Valgrind — zero leaks on `test_vec` and `test_vec_stress`
- [ ] `python/tests/test_vec.py` — all passing on Python 3.9–3.13
- [ ] Build without `SNKV_VECTOR=1` — all existing tests still pass (no regression)

### Performance
- [ ] `kvvec_put` >= 100K vecs/sec (dim=128, int8)
- [ ] `kvvec_search` k=10 >= 3K q/sec (dim=128, int8)
- [ ] Recall@10 >= 0.95 (dim=128, cosine)

### Security
- [ ] Path traversal test passes (name with `/`, `..`, null byte → KVSTORE_ERROR)
- [ ] NaN/Inf test passes
- [ ] Fuzz: 10,000 random names → no crash (only KVSTORE_OK or KVSTORE_ERROR)

### Compatibility
- [ ] Existing DB with no vector spaces: all non-vector operations unaffected
- [ ] DB written with vector support, opened with `SNKV_VECTOR=0` build: no crash
- [ ] `pip install snkv` (without `[vec]`): imports cleanly, `vector_space()` raises ImportError with helpful message

### Docs
- [ ] `docs/api/VEC_API.md` complete with all function signatures and examples
- [ ] Memory formula documented
- [ ] Compaction guidance documented
- [ ] Multi-process contract documented

### Release
- [ ] `KVVEC_VERSION` constants set to `0.5.0`
- [ ] `python/pyproject.toml` version bumped to `0.5.0`
- [ ] Wheels built and smoke-tested on all platforms
- [ ] `CHANGELOG.md` updated with vector store section
- [ ] GitHub release notes written

---

## 17. Delivery Milestones

### Phase 1 — C layer (2–3 weeks)
- [ ] Add usearch submodule, pin commit, CI hash-check
- [ ] `include/kvvec.h` — final API
- [ ] `src/kvvec.cpp` — full implementation including all validation, OOM handling,
      atomic flush, compact, reload
- [ ] `tests/test_vec.c` — 20/20 passing
- [ ] `tests/test_vec_stress.c` — recall and capacity tests passing
- [ ] Valgrind clean
- [ ] `examples/vec.c`

### Phase 2 — Python bindings (1 week)
- [ ] `python/snkv_vec_module.c` with GIL release on all CPU-bound calls
- [ ] `VectorSpace` class and `KVStore.vector_space()` in `__init__.py`
- [ ] dtype/shape validation in Python layer
- [ ] Import guard for missing numpy
- [ ] `python/tests/test_vec.py` — all passing, Python 3.9–3.13
- [ ] `python/examples/vec.py`

### Phase 3 — Docs, CI, packaging (2–3 days)
- [ ] `docs/api/VEC_API.md`
- [ ] CI matrix updated (Linux/macOS/Windows × Python 3.9–3.13)
- [ ] Publish workflow updated for `snkv[vec]` wheels
- [ ] Release checklist completed
- [ ] Version bumped to `0.5.0`

---

## 18. Future Work (out of scope for this proposal)

- **Filtered search**: `kvvec_search_filtered(space, query, k, filter_fn)` — skip
  vectors by metadata predicate during HNSW traversal
- **Batch insert API**: `kvvec_put_batch()` — one B-tree transaction for N vectors,
  significantly faster bulk load
- **Multi-vector keys**: multiple embeddings per key for multi-modal use cases
- **Runtime ef tuning**: `kvvec_set_expansion_search()` — adjust recall/latency
  without reopening the space
- **Streaming index**: mmap-backed index that exceeds RAM via OS paging
- **SNKV Cloud**: sidecar stored in object storage for serverless deployments
