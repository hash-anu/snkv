/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright 2025 SNKV Contributors */
/*
** Vector Store — HNSW vector search on top of SNKV
**
** KVVecStore wraps a KVStore and adds an in-memory usearch HNSW index.
** All vector data is persisted inside the .db file using dedicated column
** families; the HNSW graph is saved as a sidecar file on close and
** reloaded on the next open (unencrypted file-backed stores only).
**
** Quick start:
**
**   KVVecStore *vs = NULL;
**   kvstore_vec_open("store.db", 128, KVVEC_SPACE_COSINE,
**                    0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs);
**
**   float vec[128] = { ... };
**   kvstore_vec_put(vs, "doc:1", 5, "hello", 5, vec, 0, NULL, 0);
**
**   KVVecSearchResult *res = NULL;
**   int nRes = 0;
**   kvstore_vec_search(vs, query, 5, 0, 3, 0.0f, &res, &nRes);
**   for(int i = 0; i < nRes; i++)
**       printf("%.*s  dist=%.4f\n", res[i].nKey, (char*)res[i].pKey, res[i].distance);
**   kvstore_vec_free_results(res, nRes);
**
**   kvstore_vec_close(vs);
*/

#ifndef _KVSTORE_VEC_H_
#define _KVSTORE_VEC_H_

#include "kvstore.h"   /* KVStore, KVSTORE_* error codes, snkv_malloc/snkv_free */
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* -----------------------------------------------------------------------
** Distance spaces (maps to usearch metric_kind)
** ----------------------------------------------------------------------- */
#define KVVEC_SPACE_L2      0   /* squared Euclidean (||a-b||²) — NOT sqrt; default */
#define KVVEC_SPACE_COSINE  1   /* cosine distance (1 - dot(a,b)/(|a||b|)) */
#define KVVEC_SPACE_IP      2   /* inner product (negative dot product) */

/* -----------------------------------------------------------------------
** In-memory index precision (on-disk storage is always float32)
** ----------------------------------------------------------------------- */
#define KVVEC_DTYPE_F32     0   /* float32 — default */
#define KVVEC_DTYPE_F16     1   /* float16 — half RAM, negligible recall loss */
#define KVVEC_DTYPE_I8      2   /* int8    — quarter RAM, cosine-like metrics only */

/* -----------------------------------------------------------------------
** Extra error codes (extend KVSTORE_* range)
** ----------------------------------------------------------------------- */
#define KVVEC_DIM_MISMATCH   20  /* opened with wrong dim for existing store */
#define KVVEC_DTYPE_MISMATCH 21  /* opened with wrong dtype for existing store */
#define KVVEC_SPACE_MISMATCH 22  /* opened with wrong space for existing store */
#define KVVEC_INDEX_DROPPED  23  /* vector index has been dropped */
#define KVVEC_INDEX_EMPTY    24  /* index is empty; call vector_put first */
#define KVVEC_BAD_VECTOR     25  /* vector shape or value is invalid */

/* -----------------------------------------------------------------------
** KVVecStore — opaque handle returned by kvstore_vec_open
** ----------------------------------------------------------------------- */
typedef struct KVVecStore KVVecStore;

/* -----------------------------------------------------------------------
** KVVecSearchResult — one result entry from kvstore_vec_search
**
** Ownership:
**   - pKey, pValue, pMetadata are heap-allocated; caller must NOT free
**     them individually.  Pass the entire array to kvstore_vec_free_results.
** ----------------------------------------------------------------------- */
typedef struct KVVecSearchResult KVVecSearchResult;
struct KVVecSearchResult {
    void  *pKey;       /* key bytes */
    int    nKey;
    void  *pValue;     /* value bytes */
    int    nValue;
    float  distance;
    void  *pMetadata;  /* JSON bytes, or NULL if no metadata */
    int    nMetadata;
};

/* -----------------------------------------------------------------------
** KVVecKeyResult — one result entry from kvstore_vec_search_keys
** ----------------------------------------------------------------------- */
typedef struct KVVecKeyResult KVVecKeyResult;
struct KVVecKeyResult {
    void  *pKey;   /* key bytes; freed by kvstore_vec_free_key_results */
    int    nKey;
    float  distance;
};

/* -----------------------------------------------------------------------
** KVVecStats — returned by kvstore_vec_stats
** ----------------------------------------------------------------------- */
typedef struct KVVecStats KVVecStats;
struct KVVecStats {
    int     dim;
    int     space;            /* KVVEC_SPACE_* */
    int     dtype;            /* KVVEC_DTYPE_* */
    int     connectivity;
    int     expansion_add;
    int     expansion_search;
    int64_t count;            /* active (non-deleted) vectors in HNSW index */
    int64_t capacity;         /* allocated capacity */
    double  fill_ratio;       /* count / capacity */
    int64_t vec_cf_count;     /* entries in _snkv_vec_ CF (may include expired) */
    int     has_metadata;     /* 1 if _snkv_vec_tags_ CF exists */
    int     sidecar_enabled;  /* 1 if sidecar persistence is active */
};

/* -----------------------------------------------------------------------
** KVVecItem — one item for kvstore_vec_put_batch
** ----------------------------------------------------------------------- */
typedef struct KVVecItem KVVecItem;
struct KVVecItem {
    const void  *pKey;    int nKey;
    const void  *pVal;    int nVal;
    const float *pVec;    /* dim float32 values */
    const void  *pMeta;   int nMeta;   /* JSON bytes; NULL = no metadata */
};

/* =======================================================================
** Lifecycle
** ======================================================================= */

/*
** Open (or create) a vector store.
**
**   zPath          - path to .db file; NULL for in-memory store
**   dim            - vector dimension (fixed for store lifetime)
**   space          - KVVEC_SPACE_L2 / COSINE / IP
**   connectivity   - HNSW M parameter; 0 → default (16)
**   expansion_add  - HNSW ef_construction; 0 → default (128)
**   expansion_search - HNSW ef at query time; 0 → stored value or 64
**   dtype          - KVVEC_DTYPE_F32 / F16 / I8
**   pPassword/nPassword - encryption password; NULL/0 for plain store
**   ppVS           - output handle
**
** Returns KVSTORE_OK on success, or one of:
**   KVVEC_DIM_MISMATCH, KVVEC_SPACE_MISMATCH, KVVEC_DTYPE_MISMATCH
**   if the file was created with different immutable parameters.
*/
int kvstore_vec_open(
    const char    *zPath,
    int            dim,
    int            space,
    int            connectivity,
    int            expansion_add,
    int            expansion_search,
    int            dtype,
    const uint8_t *pPassword,
    int            nPassword,
    KVVecStore   **ppVS
);

/*
** Close the vector store and free all resources.
** For unencrypted file-backed stores the HNSW index is saved to
** {zPath}.usearch so the next open can skip the O(n*dim) rebuild.
*/
void kvstore_vec_close(KVVecStore *pVS);

/* =======================================================================
** Writes
** ======================================================================= */

/*
** Insert or update a key/value pair and add its vector to the HNSW index.
**
**   pKey / nKey    - key bytes
**   pVal / nVal    - value bytes
**   pVec           - float32 array of length dim
**   expire_ms      - absolute expiry (ms since epoch); 0 = no TTL
**   pMeta / nMeta  - optional JSON metadata bytes; NULL/0 = preserve existing
**                    pass "" (empty string, nMeta=0) to clear metadata
**
** The SNKV write (all CFs) is one atomic transaction.
** The usearch index update happens after commit.
*/
int kvstore_vec_put(
    KVVecStore  *pVS,
    const void  *pKey,   int nKey,
    const void  *pVal,   int nVal,
    const float *pVec,
    int64_t      expire_ms,
    const void  *pMeta,  int nMeta
);

/*
** Batch insert/update. All items are written in one atomic transaction.
** expire_ms is applied uniformly to all items; 0 = no TTL.
**
** Last-write-wins for duplicate keys within the batch.
*/
int kvstore_vec_put_batch(
    KVVecStore       *pVS,
    const KVVecItem  *pItems,
    int               nItems,
    int64_t           expire_ms
);

/* =======================================================================
** Reads
** ======================================================================= */

/*
** Get value bytes for key. Caller must snkv_free(*ppVal).
** Returns KVSTORE_NOTFOUND if missing or expired.
*/
int kvstore_vec_get(
    KVVecStore  *pVS,
    const void  *pKey,  int nKey,
    void       **ppVal, int *pnVal
);

/*
** Get stored float32 vector. *ppVec is a flat array of dim floats.
** Caller must snkv_free(*ppVec).
** Returns KVSTORE_NOTFOUND if the key has no vector.
*/
int kvstore_vec_get_vector(
    KVVecStore  *pVS,
    const void  *pKey,  int nKey,
    float      **ppVec, int *pnFloats
);

/*
** Get metadata JSON bytes. Caller must snkv_free(*ppMeta).
** Returns KVSTORE_NOTFOUND if the key has no metadata (not an error).
** *ppMeta is NULL and *pnMeta is 0 in that case.
*/
int kvstore_vec_get_metadata(
    KVVecStore  *pVS,
    const void  *pKey,  int nKey,
    void       **ppMeta, int *pnMeta
);

/*
** Plain KV get — no vector index involved.
** Alias for kvstore_vec_get; included for completeness.
*/
#define kvstore_vec_kv_get(vs, pk, nk, ppv, pnv) \
    kvstore_vec_get((vs),(pk),(nk),(ppv),(pnv))

/*
** Plain KV put — does NOT add to vector index.
** Useful for storing non-vector metadata alongside vector keys.
*/
int kvstore_vec_kv_put(
    KVVecStore  *pVS,
    const void  *pKey, int nKey,
    const void  *pVal, int nVal
);

/*
** Returns 1 if key exists (and is not expired), 0 otherwise.
*/
int kvstore_vec_contains(
    KVVecStore  *pVS,
    const void  *pKey, int nKey
);

/*
** Returns the number of active vectors in the HNSW index.
*/
int64_t kvstore_vec_count(KVVecStore *pVS);

/* =======================================================================
** Delete
** ======================================================================= */

/*
** Delete key from KV store, all vector CFs, and HNSW index.
** Returns KVSTORE_NOTFOUND if key does not exist.
*/
int kvstore_vec_delete(
    KVVecStore  *pVS,
    const void  *pKey, int nKey
);

/* =======================================================================
** Search
** ======================================================================= */

/*
** Approximate nearest-neighbour search.
**
**   pQuery         - float32 query vector of length dim
**   top_k          - maximum results to return
**   rerank         - 1 = exact rerank from stored float32 vectors
**   oversample     - candidate pool = top_k * oversample (used when
**                    rerank != 0); 0 → default (3)
**   max_distance   - drop results with distance > threshold; 0.0 = no limit
**   ppResults      - output array of KVVecSearchResult (heap-allocated)
**   pnResults      - number of results returned
**
** Caller must pass the array to kvstore_vec_free_results when done.
** Returns KVVEC_INDEX_EMPTY if index has no vectors.
*/
int kvstore_vec_search(
    KVVecStore          *pVS,
    const float         *pQuery,
    int                  top_k,
    int                  rerank,
    int                  oversample,
    float                max_distance,
    KVVecSearchResult  **ppResults,
    int                 *pnResults
);

/*
** ANN search returning (key, distance) pairs only — no value fetch.
**
**   ppResults / pnResults - output array of KVVecKeyResult (heap-allocated)
**
** Caller must pass the array to kvstore_vec_free_key_results when done.
*/
int kvstore_vec_search_keys(
    KVVecStore       *pVS,
    const float      *pQuery,
    int               top_k,
    KVVecKeyResult  **ppResults,
    int              *pnResults
);

/* Free the array returned by kvstore_vec_search. */
void kvstore_vec_free_results(KVVecSearchResult *pResults, int nResults);

/* Free the array returned by kvstore_vec_search_keys. */
void kvstore_vec_free_key_results(KVVecKeyResult *pResults, int nResults);

/* =======================================================================
** Statistics
** ======================================================================= */

/*
** Fill *pStats with index configuration and runtime state.
** Returns KVSTORE_OK always (fields default to 0 if index is dropped).
*/
int kvstore_vec_stats(KVVecStore *pVS, KVVecStats *pStats);

/* =======================================================================
** Maintenance
** ======================================================================= */

/*
** Delete all expired vectors from the HNSW index and all _snkv_vec*_ CFs.
** *pnDeleted (may be NULL) — number of vectors removed.
** Use this instead of kvstore_purge_expired to keep vector CFs in sync.
*/
int kvstore_vec_purge_expired(KVVecStore *pVS, int *pnDeleted);

/*
** Drop all _snkv_vec*_ CFs and free the in-memory index.
** KV data in the default CF is preserved and accessible via kvstore_vec_get.
** After this call, kvstore_vec_search returns KVVEC_INDEX_DROPPED.
*/
int kvstore_vec_drop_index(KVVecStore *pVS);

#ifdef __cplusplus
}
#endif

#endif /* _KVSTORE_VEC_H_ */
