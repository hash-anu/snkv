/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright 2025 SNKV Contributors */
/*
** kvstore_vec.c — HNSW vector search built on top of KVStore
**
** Data layout (five internal CFs):
**
**   _snkv_vec_      user key  → float32 bytes (dim × 4)
**   _snkv_vec_idk_  user key  → 8-byte big-endian int64  (usearch label)
**   _snkv_vec_idi_  8-byte BE int64 → user key
**   _snkv_vec_meta_ config keys (ndim, metric, connectivity, …)
**   _snkv_vec_tags_ user key  → JSON metadata bytes  (lazy, created on first write)
**
** The usearch HNSW index (in-memory) is rebuilt from the CFs on every open,
** or loaded from a sidecar file ({path}.usearch + {path}.usearch.nid) when
** available (unencrypted, file-backed stores only).
**
** All writes go through a single KVStore transaction for atomicity.
** The usearch index is updated AFTER a successful commit.
*/

#include "kvstore_vec.h"
#include "usearch.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>

#ifdef _WIN32
#  include <windows.h>
#else
#  include <time.h>
#endif

/* -----------------------------------------------------------------------
** Stub for usearch's Go-binding extern declaration in usearch.h.
** Not called by our code; provided to satisfy the linker.
** ----------------------------------------------------------------------- */
int goFilteredSearchCallback(usearch_key_t key, void *state) {
    (void)key; (void)state; return 0;
}

/* -----------------------------------------------------------------------
** Internal CF names
** ----------------------------------------------------------------------- */
#define CF_VEC   "_snkv_vec_"
#define CF_IDK   "_snkv_vec_idk_"
#define CF_IDI   "_snkv_vec_idi_"
#define CF_META  "_snkv_vec_meta_"
#define CF_TAGS  "_snkv_vec_tags_"

/* Meta keys */
static const char META_NDIM[]    = "ndim";
static const char META_METRIC[]  = "metric";
static const char META_CONN[]    = "connectivity";
static const char META_EXPADD[]  = "expansion_add";
static const char META_EXPSRCH[] = "expansion_search";
static const char META_DTYPE[]   = "dtype";
static const char META_NEXT_ID[] = "next_id";

/* Default HNSW parameters */
#define DEFAULT_CONNECTIVITY     16
#define DEFAULT_EXPANSION_ADD   128
#define DEFAULT_EXPANSION_SEARCH 64

/* Auto-reserve threshold: reserve capacity*2 when fill >= this */
#define RESERVE_THRESHOLD  0.9

/* -----------------------------------------------------------------------
** KVVecStore struct
** ----------------------------------------------------------------------- */
struct KVVecStore {
    KVStore        *pKV;
    usearch_index_t pIdx;      /* NULL if index dropped */
    sqlite3_mutex  *pMutex;    /* Recursive mutex protecting pIdx, nextId, pTagsCF */

    int     dim;
    int     space;             /* KVVEC_SPACE_* */
    int     dtype;             /* KVVEC_DTYPE_* */
    int     connectivity;
    int     expansion_add;
    int     expansion_search;
    int64_t nextId;

    char *zSidecarPath;        /* "{path}.usearch"; NULL if disabled */

    KVColumnFamily *pVecCF;    /* _snkv_vec_ */
    KVColumnFamily *pIdkCF;    /* _snkv_vec_idk_ */
    KVColumnFamily *pIdiCF;    /* _snkv_vec_idi_ */
    KVColumnFamily *pMetaCF;   /* _snkv_vec_meta_ */
    KVColumnFamily *pTagsCF;   /* _snkv_vec_tags_ — lazy */
};

/* -----------------------------------------------------------------------
** Utility: big-endian int64 encode/decode
** ----------------------------------------------------------------------- */
static void packI64(unsigned char *buf, int64_t v) {
    buf[0] = (unsigned char)((uint64_t)v >> 56);
    buf[1] = (unsigned char)((uint64_t)v >> 48);
    buf[2] = (unsigned char)((uint64_t)v >> 40);
    buf[3] = (unsigned char)((uint64_t)v >> 32);
    buf[4] = (unsigned char)((uint64_t)v >> 24);
    buf[5] = (unsigned char)((uint64_t)v >> 16);
    buf[6] = (unsigned char)((uint64_t)v >>  8);
    buf[7] = (unsigned char)((uint64_t)v      );
}

static int64_t unpackI64(const unsigned char *buf) {
    return (int64_t)(
        ((uint64_t)buf[0] << 56) | ((uint64_t)buf[1] << 48) |
        ((uint64_t)buf[2] << 40) | ((uint64_t)buf[3] << 32) |
        ((uint64_t)buf[4] << 24) | ((uint64_t)buf[5] << 16) |
        ((uint64_t)buf[6] <<  8) |  (uint64_t)buf[7]
    );
}

/* -----------------------------------------------------------------------
** Map KVVEC_SPACE_* → usearch_metric_kind_t
** ----------------------------------------------------------------------- */
static usearch_metric_kind_t spaceToMetric(int space) {
    switch (space) {
        case KVVEC_SPACE_COSINE: return usearch_metric_cos_k;
        case KVVEC_SPACE_IP:     return usearch_metric_ip_k;
        default:                 return usearch_metric_l2sq_k;
    }
}

/* -----------------------------------------------------------------------
** Map KVVEC_DTYPE_* → usearch_scalar_kind_t
** ----------------------------------------------------------------------- */
static usearch_scalar_kind_t dtypeToScalar(int dtype) {
    switch (dtype) {
        case KVVEC_DTYPE_F16: return usearch_scalar_f16_k;
        case KVVEC_DTYPE_I8:  return usearch_scalar_i8_k;
        default:              return usearch_scalar_f32_k;
    }
}

/* -----------------------------------------------------------------------
** Map space name string → KVVEC_SPACE_*
** ----------------------------------------------------------------------- */
static int spaceFromStr(const char *s) {
    if (strcmp(s, "cosine") == 0) return KVVEC_SPACE_COSINE;
    if (strcmp(s, "ip")     == 0) return KVVEC_SPACE_IP;
    return KVVEC_SPACE_L2;
}

static const char *spaceToStr(int space) {
    switch (space) {
        case KVVEC_SPACE_COSINE: return "cosine";
        case KVVEC_SPACE_IP:     return "ip";
        default:                 return "l2";
    }
}

static int dtypeFromStr(const char *s) {
    if (strcmp(s, "f16") == 0) return KVVEC_DTYPE_F16;
    if (strcmp(s, "i8")  == 0) return KVVEC_DTYPE_I8;
    return KVVEC_DTYPE_F32;
}

static const char *dtypeToStr(int dtype) {
    switch (dtype) {
        case KVVEC_DTYPE_F16: return "f16";
        case KVVEC_DTYPE_I8:  return "i8";
        default:              return "f32";
    }
}

/* -----------------------------------------------------------------------
** Open-or-create a column family
** ----------------------------------------------------------------------- */
static int getOrCreateCF(KVStore *pKV, const char *zName, KVColumnFamily **ppCF) {
    int rc = kvstore_cf_open(pKV, zName, ppCF);
    if (rc == KVSTORE_NOTFOUND) {
        rc = kvstore_cf_create(pKV, zName, ppCF);
    }
    return rc;
}

/* -----------------------------------------------------------------------
** Get (or lazily create) the tags CF
** ----------------------------------------------------------------------- */
static int getTagsCF(KVVecStore *pVS, KVColumnFamily **ppTags) {
    if (pVS->pTagsCF) {
        *ppTags = pVS->pTagsCF;
        return KVSTORE_OK;
    }
    int rc = getOrCreateCF(pVS->pKV, CF_TAGS, &pVS->pTagsCF);
    *ppTags = pVS->pTagsCF;
    return rc;
}

/* -----------------------------------------------------------------------
** Build (or restore) the usearch index from stored CFs
** ----------------------------------------------------------------------- */
static int rebuildIndex(KVVecStore *pVS) {
    usearch_error_t err = NULL;

    /* Read stored metadata */
    void *pNdimRaw    = NULL; int nNdimRaw    = 0;
    void *pMetricRaw  = NULL; int nMetricRaw  = 0;
    void *pConnRaw    = NULL; int nConnRaw    = 0;
    void *pExpAddRaw  = NULL; int nExpAddRaw  = 0;
    void *pExpSrchRaw = NULL; int nExpSrchRaw = 0;
    void *pDtypeRaw   = NULL; int nDtypeRaw   = 0;
    void *pNextIdRaw  = NULL; int nNextIdRaw  = 0;

    kvstore_cf_get(pVS->pMetaCF, META_NDIM,    (int)strlen(META_NDIM),
                   &pNdimRaw, &nNdimRaw);
    kvstore_cf_get(pVS->pMetaCF, META_METRIC,  (int)strlen(META_METRIC),
                   &pMetricRaw, &nMetricRaw);
    kvstore_cf_get(pVS->pMetaCF, META_CONN,    (int)strlen(META_CONN),
                   &pConnRaw, &nConnRaw);
    kvstore_cf_get(pVS->pMetaCF, META_EXPADD,  (int)strlen(META_EXPADD),
                   &pExpAddRaw, &nExpAddRaw);
    kvstore_cf_get(pVS->pMetaCF, META_EXPSRCH, (int)strlen(META_EXPSRCH),
                   &pExpSrchRaw, &nExpSrchRaw);
    kvstore_cf_get(pVS->pMetaCF, META_DTYPE,   (int)strlen(META_DTYPE),
                   &pDtypeRaw, &nDtypeRaw);
    kvstore_cf_get(pVS->pMetaCF, META_NEXT_ID, (int)strlen(META_NEXT_ID),
                   &pNextIdRaw, &nNextIdRaw);

    int existingStore = (pNdimRaw != NULL);
    int rc = KVSTORE_OK;

    if (existingStore) {
        /* Validate immutable fields */
        if (nNdimRaw == 8) {
            int storedDim = (int)unpackI64((const unsigned char*)pNdimRaw);
            if (storedDim != pVS->dim) {
                rc = KVVEC_DIM_MISMATCH;
                goto cleanup_meta;
            }
        }
        if (pMetricRaw && nMetricRaw > 0) {
            char metricBuf[32] = {0};
            int n = nMetricRaw < 31 ? nMetricRaw : 31;
            memcpy(metricBuf, pMetricRaw, n);
            int storedSpace = spaceFromStr(metricBuf);
            if (storedSpace != pVS->space) {
                rc = KVVEC_SPACE_MISMATCH;
                goto cleanup_meta;
            }
        }
        if (pDtypeRaw && nDtypeRaw > 0) {
            char dtypeBuf[8] = {0};
            int n = nDtypeRaw < 7 ? nDtypeRaw : 7;
            memcpy(dtypeBuf, pDtypeRaw, n);
            int storedDtype = dtypeFromStr(dtypeBuf);
            if (storedDtype != pVS->dtype) {
                rc = KVVEC_DTYPE_MISMATCH;
                goto cleanup_meta;
            }
        }
        /* connectivity and expansion_add: stored value wins */
        if (pConnRaw   && nConnRaw   == 8) pVS->connectivity   = (int)unpackI64((const unsigned char*)pConnRaw);
        if (pExpAddRaw && nExpAddRaw == 8) pVS->expansion_add  = (int)unpackI64((const unsigned char*)pExpAddRaw);
        /* expansion_search: only override if caller passed 0 (sentinel) */
        if (pVS->expansion_search == 0) {
            pVS->expansion_search = (pExpSrchRaw && nExpSrchRaw == 8)
                ? (int)unpackI64((const unsigned char*)pExpSrchRaw)
                : DEFAULT_EXPANSION_SEARCH;
        }
        pVS->nextId = (pNextIdRaw && nNextIdRaw == 8)
            ? unpackI64((const unsigned char*)pNextIdRaw) : 0;
    } else {
        /* New store: apply defaults */
        if (pVS->connectivity    == 0) pVS->connectivity    = DEFAULT_CONNECTIVITY;
        if (pVS->expansion_add   == 0) pVS->expansion_add   = DEFAULT_EXPANSION_ADD;
        if (pVS->expansion_search == 0) pVS->expansion_search = DEFAULT_EXPANSION_SEARCH;
    }

    /* Try to open tags CF if it already exists */
    {
        KVColumnFamily *pTmp = NULL;
        if (kvstore_cf_open(pVS->pKV, CF_TAGS, &pTmp) == KVSTORE_OK)
            pVS->pTagsCF = pTmp;
    }

    /* Count existing vectors */
    int64_t count = 0;
    kvstore_cf_count(pVS->pVecCF, &count);

    /* Initialise usearch index */
    usearch_init_options_t opts;
    memset(&opts, 0, sizeof(opts));
    opts.metric_kind  = spaceToMetric(pVS->space);
    opts.quantization = dtypeToScalar(pVS->dtype);
    opts.dimensions   = (size_t)pVS->dim;
    opts.connectivity = (size_t)pVS->connectivity;
    opts.expansion_add    = (size_t)pVS->expansion_add;
    opts.expansion_search = (size_t)pVS->expansion_search;
    opts.multi = 0;

    pVS->pIdx = usearch_init(&opts, &err);
    if (!pVS->pIdx || err) { rc = KVSTORE_ERROR; goto cleanup_meta; }

    /* Try sidecar fast-path (unencrypted, file-backed stores only) */
    if (pVS->zSidecarPath) {
        char nidPath[4096];
        snprintf(nidPath, sizeof(nidPath), "%s.nid", pVS->zSidecarPath);

        FILE *f = fopen(nidPath, "rb");
        if (f) {
            unsigned char nidBuf[8];
            int ok = (fread(nidBuf, 1, 8, f) == 8);
            fclose(f);
            if (ok) {
                int64_t sidecarNid = unpackI64(nidBuf);
                if (sidecarNid == pVS->nextId) {
                    /* Load into a fresh index handle */
                    usearch_init_options_t lopts = opts;
                    usearch_index_t candidate = usearch_init(&lopts, &err);
                    if (candidate && !err) {
                        usearch_load(candidate, pVS->zSidecarPath, &err);
                        if (!err && (int)usearch_dimensions(candidate, &err) == pVS->dim && !err) {
                            usearch_change_expansion_search(candidate,
                                (size_t)pVS->expansion_search, &err);
                            if (!err) {
                                /* Sidecar loaded successfully — discard temp index */
                                usearch_free(pVS->pIdx, &err);
                                pVS->pIdx = candidate;
                                goto cleanup_meta; /* fast path */
                            }
                        }
                        usearch_free(candidate, &err);
                        err = NULL;
                    }
                }
            }
            /* Stale or corrupt sidecar — remove both files */
            remove(pVS->zSidecarPath);
            remove(nidPath);
        }
    }

    /* Full rebuild from CFs */
    if (count > 0) {
        usearch_reserve(pVS->pIdx, (size_t)(count + 1), &err);
        if (err) { err = NULL; }

        KVIterator *pIter = NULL;
        if (kvstore_cf_iterator_create(pVS->pVecCF, &pIter) == KVSTORE_OK) {
            int iterRc = kvstore_iterator_first(pIter);
            while (iterRc == KVSTORE_OK && !kvstore_iterator_eof(pIter)) {
                void *pKey = NULL; int nKey = 0;
                void *pVec = NULL; int nVec = 0;
                kvstore_iterator_key(pIter, &pKey, &nKey);
                kvstore_iterator_value(pIter, &pVec, &nVec);

                if (pKey && pVec && nVec == pVS->dim * (int)sizeof(float)) {
                    /* Look up usearch label for this key */
                    void *pIdRaw = NULL; int nIdRaw = 0;
                    if (kvstore_cf_get(pVS->pIdkCF, pKey, nKey, &pIdRaw, &nIdRaw) == KVSTORE_OK
                            && nIdRaw == 8) {
                        uint64_t label = (uint64_t)unpackI64((const unsigned char*)pIdRaw);
                        usearch_add(pVS->pIdx, (usearch_key_t)label,
                                    pVec, usearch_scalar_f32_k, &err);
                        if (err) err = NULL; /* non-fatal; continue rebuild */
                        snkv_free(pIdRaw);
                    }
                    /* pKey and pVec are internal iterator buffers — do NOT free */
                }
                iterRc = kvstore_iterator_next(pIter);
            }
            kvstore_iterator_close(pIter);
        }
        usearch_change_expansion_search(pVS->pIdx,
            (size_t)pVS->expansion_search, &err);
        if (err) err = NULL;
    }

cleanup_meta:
    snkv_free(pNdimRaw);
    snkv_free(pMetricRaw);
    snkv_free(pConnRaw);
    snkv_free(pExpAddRaw);
    snkv_free(pExpSrchRaw);
    snkv_free(pDtypeRaw);
    snkv_free(pNextIdRaw);
    return rc;
}

/* -----------------------------------------------------------------------
** Write full metadata to meta CF (called within an active transaction)
** ----------------------------------------------------------------------- */
static int writeFullMeta(KVVecStore *pVS) {
    unsigned char buf[8];
    int rc = KVSTORE_OK;

    packI64(buf, (int64_t)pVS->dim);
    rc = kvstore_cf_put(pVS->pMetaCF, META_NDIM, (int)strlen(META_NDIM), buf, 8);
    if (rc) return rc;

    const char *space = spaceToStr(pVS->space);
    rc = kvstore_cf_put(pVS->pMetaCF, META_METRIC, (int)strlen(META_METRIC),
                        space, (int)strlen(space));
    if (rc) return rc;

    packI64(buf, (int64_t)pVS->connectivity);
    rc = kvstore_cf_put(pVS->pMetaCF, META_CONN, (int)strlen(META_CONN), buf, 8);
    if (rc) return rc;

    packI64(buf, (int64_t)pVS->expansion_add);
    rc = kvstore_cf_put(pVS->pMetaCF, META_EXPADD, (int)strlen(META_EXPADD), buf, 8);
    if (rc) return rc;

    packI64(buf, (int64_t)pVS->expansion_search);
    rc = kvstore_cf_put(pVS->pMetaCF, META_EXPSRCH, (int)strlen(META_EXPSRCH), buf, 8);
    if (rc) return rc;

    const char *dtype = dtypeToStr(pVS->dtype);
    rc = kvstore_cf_put(pVS->pMetaCF, META_DTYPE, (int)strlen(META_DTYPE),
                        dtype, (int)strlen(dtype));
    if (rc) return rc;

    packI64(buf, pVS->nextId);
    rc = kvstore_cf_put(pVS->pMetaCF, META_NEXT_ID, (int)strlen(META_NEXT_ID), buf, 8);
    return rc;
}

/* -----------------------------------------------------------------------
** Write just the next_id (after each put)
** ----------------------------------------------------------------------- */
static int writeNextId(KVVecStore *pVS) {
    unsigned char buf[8];
    packI64(buf, pVS->nextId);
    return kvstore_cf_put(pVS->pMetaCF, META_NEXT_ID, (int)strlen(META_NEXT_ID), buf, 8);
}

/* =======================================================================
** kvstore_vec_open
** ======================================================================= */
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
) {
    if (!ppVS) return KVSTORE_ERROR;
    *ppVS = NULL;
    if (dim < 1) return KVSTORE_ERROR;

    /* Open KVStore first — this calls sqlite3_initialize(), which sets up the
    ** memory allocator that snkv_malloc (sqlite3MallocZero) depends on. */
    KVStore *pKV = NULL;
    int rc;
    if (pPassword && nPassword > 0) {
        rc = kvstore_open_encrypted(zPath, pPassword, nPassword, &pKV, NULL);
    } else {
        rc = kvstore_open(zPath, &pKV, KVSTORE_JOURNAL_WAL);
    }
    if (rc != KVSTORE_OK) return rc;

    KVVecStore *pVS = (KVVecStore*)snkv_malloc(sizeof(KVVecStore));
    if (!pVS) { kvstore_close(pKV); return KVSTORE_NOMEM; }

    pVS->pKV           = pKV;
    pVS->pIdx          = NULL;
    pVS->pMutex        = sqlite3_mutex_alloc(SQLITE_MUTEX_RECURSIVE);
    pVS->dim           = dim;
    pVS->space         = space;
    pVS->dtype         = dtype;
    pVS->connectivity  = connectivity;
    pVS->expansion_add = expansion_add;
    pVS->expansion_search = expansion_search;
    pVS->nextId        = 0;
    pVS->zSidecarPath  = NULL;
    pVS->pVecCF        = NULL;
    pVS->pIdkCF        = NULL;
    pVS->pIdiCF        = NULL;
    pVS->pMetaCF       = NULL;
    pVS->pTagsCF       = NULL;
    if (!pVS->pMutex) { kvstore_close(pKV); snkv_free(pVS); return KVSTORE_ERROR; }

    /* Sidecar enabled for unencrypted, file-backed stores */
    if (zPath && !(pPassword && nPassword > 0)) {
        size_t pathLen = strlen(zPath);
        pVS->zSidecarPath = (char*)snkv_malloc(pathLen + 16);
        if (!pVS->zSidecarPath) { rc = KVSTORE_NOMEM; goto fail; }
        memcpy(pVS->zSidecarPath, zPath, pathLen);
        memcpy(pVS->zSidecarPath + pathLen, ".usearch", 9); /* includes NUL */
    }

    /* Open four core CFs */
    rc = getOrCreateCF(pVS->pKV, CF_VEC,  &pVS->pVecCF);  if (rc) goto fail;
    rc = getOrCreateCF(pVS->pKV, CF_IDK,  &pVS->pIdkCF);  if (rc) goto fail;
    rc = getOrCreateCF(pVS->pKV, CF_IDI,  &pVS->pIdiCF);  if (rc) goto fail;
    rc = getOrCreateCF(pVS->pKV, CF_META, &pVS->pMetaCF); if (rc) goto fail;

    /* Build index from CFs (or sidecar) */
    rc = rebuildIndex(pVS);
    if (rc != KVSTORE_OK) goto fail;

    *ppVS = pVS;
    return KVSTORE_OK;

fail:
    if (pVS->pVecCF)  kvstore_cf_close(pVS->pVecCF);
    if (pVS->pIdkCF)  kvstore_cf_close(pVS->pIdkCF);
    if (pVS->pIdiCF)  kvstore_cf_close(pVS->pIdiCF);
    if (pVS->pMetaCF) kvstore_cf_close(pVS->pMetaCF);
    if (pVS->pTagsCF) kvstore_cf_close(pVS->pTagsCF);
    if (pVS->pIdx)    { usearch_error_t e = NULL; usearch_free(pVS->pIdx, &e); }
    if (pVS->pKV)     kvstore_close(pVS->pKV);
    if (pVS->pMutex)  sqlite3_mutex_free(pVS->pMutex);
    snkv_free(pVS->zSidecarPath);
    snkv_free(pVS);
    return rc;
}

/* =======================================================================
** kvstore_vec_close
** ======================================================================= */
void kvstore_vec_close(KVVecStore *pVS) {
    if (!pVS) return;
    usearch_error_t err = NULL;

    /* Save sidecar + next_id stamp */
    if (pVS->zSidecarPath && pVS->pIdx) {
        char nidPath[4096];
        snprintf(nidPath, sizeof(nidPath), "%s.nid", pVS->zSidecarPath);

        usearch_save(pVS->pIdx, pVS->zSidecarPath, &err);
        if (!err) {
            FILE *f = fopen(nidPath, "wb");
            if (f) {
                unsigned char buf[8];
                packI64(buf, pVS->nextId);
                if (fwrite(buf, 1, 8, f) != 8) {
                    /* partial write — remove both */
                    fclose(f);
                    remove(pVS->zSidecarPath);
                    remove(nidPath);
                } else {
                    fclose(f);
                }
            } else {
                remove(pVS->zSidecarPath);
            }
        } else {
            err = NULL;
            remove(pVS->zSidecarPath);
            remove(nidPath);
        }
    }

    /* Close CFs */
    KVColumnFamily *cfs[] = {
        pVS->pVecCF, pVS->pIdkCF, pVS->pIdiCF, pVS->pMetaCF, pVS->pTagsCF
    };
    for (int i = 0; i < 5; i++) {
        if (cfs[i]) kvstore_cf_close(cfs[i]);
    }

    if (pVS->pIdx)   usearch_free(pVS->pIdx, &err);
    if (pVS->pKV)    kvstore_close(pVS->pKV);
    if (pVS->pMutex) sqlite3_mutex_free(pVS->pMutex);
    snkv_free(pVS->zSidecarPath);
    snkv_free(pVS);
}

/* =======================================================================
** kvstore_vec_put
** ======================================================================= */
int kvstore_vec_put(
    KVVecStore  *pVS,
    const void  *pKey,  int nKey,
    const void  *pVal,  int nVal,
    const float *pVec,
    int64_t      expire_ms,
    const void  *pMeta, int nMeta
) {
    if (!pVS || !pKey || !pVal) return KVSTORE_ERROR;
    if (!pVec) return KVVEC_BAD_VECTOR;
    if (!pVS->pIdx) return KVVEC_INDEX_DROPPED;

    sqlite3_mutex_enter(pVS->pMutex);
    usearch_error_t err = NULL;
    unsigned char idBuf[8];

    /* Check if key already has a vector (need to remove old usearch entry) */
    void *pOldId = NULL; int nOldId = 0;
    kvstore_cf_get(pVS->pIdkCF, pKey, nKey, &pOldId, &nOldId);
    int64_t oldLabel = -1;
    if (pOldId && nOldId == 8) {
        oldLabel = unpackI64((const unsigned char*)pOldId);
    }
    snkv_free(pOldId);

    int isFirst = 0;
    {
        void *pNdimChk = NULL; int nNdimChk = 0;
        kvstore_cf_get(pVS->pMetaCF, META_NDIM, (int)strlen(META_NDIM),
                       &pNdimChk, &nNdimChk);
        isFirst = (pNdimChk == NULL);
        snkv_free(pNdimChk);
    }

    /* Ensure tags CF exists before opening write transaction (DDL cannot be
       nested inside DML in SQLite) */
    if (pMeta && nMeta > 0) {
        KVColumnFamily *pTmp = NULL;
        int rc2 = getTagsCF(pVS, &pTmp);
        if (rc2 != KVSTORE_OK) {
            sqlite3_mutex_leave(pVS->pMutex);
            return rc2;
        }
    }

    int64_t newLabel = pVS->nextId;
    packI64(idBuf, newLabel);

    /* Begin atomic write transaction */
    int rc = kvstore_begin(pVS->pKV, 1);
    if (rc != KVSTORE_OK) { sqlite3_mutex_leave(pVS->pMutex); return rc; }

    /* Write value (with or without TTL) */
    if (expire_ms > 0) {
        rc = kvstore_put_ttl(pVS->pKV, pKey, nKey, pVal, nVal, expire_ms);
    } else {
        rc = kvstore_put(pVS->pKV, pKey, nKey, pVal, nVal);
    }
    if (rc != KVSTORE_OK) goto rollback;

    /* Write vector bytes */
    rc = kvstore_cf_put(pVS->pVecCF, pKey, nKey,
                        pVec, pVS->dim * (int)sizeof(float));
    if (rc != KVSTORE_OK) goto rollback;

    /* Write key→label mapping */
    rc = kvstore_cf_put(pVS->pIdkCF, pKey, nKey, idBuf, 8);
    if (rc != KVSTORE_OK) goto rollback;

    /* Write label→key mapping */
    rc = kvstore_cf_put(pVS->pIdiCF, idBuf, 8, pKey, nKey);
    if (rc != KVSTORE_OK) goto rollback;

    /* Remove old label→key entry if overwriting */
    if (oldLabel >= 0) {
        unsigned char oldBuf[8]; packI64(oldBuf, oldLabel);
        kvstore_cf_delete(pVS->pIdiCF, oldBuf, 8); /* ignore NOTFOUND */
    }

    /* Metadata */
    if (pMeta != NULL) {
        if (nMeta > 0) {
            rc = kvstore_cf_put(pVS->pTagsCF, pKey, nKey, pMeta, nMeta);
            if (rc != KVSTORE_OK) goto rollback;
        } else {
            /* nMeta==0 with non-NULL pMeta → clear existing metadata */
            if (pVS->pTagsCF) {
                kvstore_cf_delete(pVS->pTagsCF, pKey, nKey); /* ignore NOTFOUND */
            }
        }
    }

    /* Update next_id in meta */
    pVS->nextId = newLabel + 1;
    if (isFirst) {
        rc = writeFullMeta(pVS);
    } else {
        rc = writeNextId(pVS);
    }
    if (rc != KVSTORE_OK) { pVS->nextId = newLabel; goto rollback; }

    rc = kvstore_commit(pVS->pKV);
    if (rc != KVSTORE_OK) {
        pVS->nextId = newLabel;
        sqlite3_mutex_leave(pVS->pMutex);
        return rc;
    }

    /* Post-commit usearch update */
    if (oldLabel >= 0) {
        usearch_remove(pVS->pIdx, (usearch_key_t)oldLabel, &err);
        err = NULL;
    }
    /* Auto-reserve when capacity is 0 (fresh index) or fill_ratio >= RESERVE_THRESHOLD */
    {
        size_t cap  = usearch_capacity(pVS->pIdx, &err); err = NULL;
        size_t sz   = usearch_size(pVS->pIdx, &err);     err = NULL;
        if (cap == 0 || (double)sz / (double)cap >= RESERVE_THRESHOLD) {
            size_t newCap = (cap == 0) ? 64 : cap * 2;
            usearch_reserve(pVS->pIdx, newCap, &err); err = NULL;
        }
    }
    usearch_add(pVS->pIdx, (usearch_key_t)newLabel, pVec, usearch_scalar_f32_k, &err);
    err = NULL;
    sqlite3_mutex_leave(pVS->pMutex);
    return KVSTORE_OK;

rollback:
    kvstore_rollback(pVS->pKV);
    sqlite3_mutex_leave(pVS->pMutex);
    return rc;
}

/* =======================================================================
** kvstore_vec_put_batch
** ======================================================================= */
int kvstore_vec_put_batch(
    KVVecStore       *pVS,
    const KVVecItem  *pItems,
    int               nItems,
    int64_t           expire_ms
) {
    if (!pVS || !pItems || nItems <= 0) return KVSTORE_ERROR;
    if (!pVS->pIdx) return KVVEC_INDEX_DROPPED;
    if (nItems == 0) return KVSTORE_OK;
    /* Validate per-item vectors before taking any locks */
    for (int i = 0; i < nItems; i++) {
        if (!pItems[i].pVec) return KVVEC_BAD_VECTOR;
    }

    sqlite3_mutex_enter(pVS->pMutex);
    usearch_error_t err = NULL;

    /* Ensure tags CF before DDL/DML boundary */
    for (int i = 0; i < nItems; i++) {
        if (pItems[i].pMeta && pItems[i].nMeta > 0) {
            KVColumnFamily *pTmp = NULL;
            int rc2 = getTagsCF(pVS, &pTmp);
            if (rc2 != KVSTORE_OK) {
                sqlite3_mutex_leave(pVS->pMutex);
                return rc2;
            }
            break;
        }
    }

    int isFirst = 0;
    {
        void *pNdimChk = NULL; int nNdimChk = 0;
        kvstore_cf_get(pVS->pMetaCF, META_NDIM, (int)strlen(META_NDIM),
                       &pNdimChk, &nNdimChk);
        isFirst = (pNdimChk == NULL);
        snkv_free(pNdimChk);
    }

    /* Snapshot pre-existing labels */
    int64_t *oldLabels = (int64_t*)snkv_malloc(nItems * sizeof(int64_t));
    if (!oldLabels) { sqlite3_mutex_leave(pVS->pMutex); return KVSTORE_NOMEM; }
    for (int i = 0; i < nItems; i++) {
        oldLabels[i] = -1;
        void *pOldId = NULL; int nOldId = 0;
        kvstore_cf_get(pVS->pIdkCF, pItems[i].pKey, pItems[i].nKey, &pOldId, &nOldId);
        if (pOldId && nOldId == 8) {
            oldLabels[i] = unpackI64((const unsigned char*)pOldId);
        }
        snkv_free(pOldId);
    }

    int64_t baseId = pVS->nextId;
    int rc = kvstore_begin(pVS->pKV, 1);
    if (rc != KVSTORE_OK) {
        snkv_free(oldLabels);
        sqlite3_mutex_leave(pVS->pMutex);
        return rc;
    }

    for (int i = 0; i < nItems; i++) {
        const KVVecItem *it = &pItems[i];
        int64_t newLabel = baseId + i;
        unsigned char idBuf[8]; packI64(idBuf, newLabel);

        if (expire_ms > 0) {
            rc = kvstore_put_ttl(pVS->pKV, it->pKey, it->nKey,
                                 it->pVal, it->nVal, expire_ms);
        } else {
            rc = kvstore_put(pVS->pKV, it->pKey, it->nKey, it->pVal, it->nVal);
        }
        if (rc != KVSTORE_OK) goto batch_rollback;

        rc = kvstore_cf_put(pVS->pVecCF, it->pKey, it->nKey,
                            it->pVec, pVS->dim * (int)sizeof(float));
        if (rc != KVSTORE_OK) goto batch_rollback;

        rc = kvstore_cf_put(pVS->pIdkCF, it->pKey, it->nKey, idBuf, 8);
        if (rc != KVSTORE_OK) goto batch_rollback;

        rc = kvstore_cf_put(pVS->pIdiCF, idBuf, 8, it->pKey, it->nKey);
        if (rc != KVSTORE_OK) goto batch_rollback;

        if (oldLabels[i] >= 0) {
            unsigned char oldBuf[8]; packI64(oldBuf, oldLabels[i]);
            kvstore_cf_delete(pVS->pIdiCF, oldBuf, 8);
        }

        if (it->pMeta != NULL) {
            if (it->nMeta > 0) {
                rc = kvstore_cf_put(pVS->pTagsCF,
                                    it->pKey, it->nKey,
                                    it->pMeta, it->nMeta);
                if (rc != KVSTORE_OK) goto batch_rollback;
            } else if (pVS->pTagsCF) {
                kvstore_cf_delete(pVS->pTagsCF, it->pKey, it->nKey);
            }
        }
    }

    pVS->nextId = baseId + nItems;
    if (isFirst) {
        rc = writeFullMeta(pVS);
    } else {
        rc = writeNextId(pVS);
    }
    if (rc != KVSTORE_OK) { pVS->nextId = baseId; goto batch_rollback; }

    rc = kvstore_commit(pVS->pKV);
    if (rc != KVSTORE_OK) {
        pVS->nextId = baseId;
        snkv_free(oldLabels);
        sqlite3_mutex_leave(pVS->pMutex);
        return rc;
    }

    /* Post-commit usearch: remove old entries, auto-reserve, batch add */
    for (int i = 0; i < nItems; i++) {
        if (oldLabels[i] >= 0) {
            usearch_remove(pVS->pIdx, (usearch_key_t)oldLabels[i], &err);
            err = NULL;
        }
    }
    {
        size_t cap = usearch_capacity(pVS->pIdx, &err); err = NULL;
        size_t sz  = usearch_size(pVS->pIdx, &err);     err = NULL;
        size_t needed = sz + (size_t)nItems;
        if (cap == 0 || (double)sz / (double)cap >= RESERVE_THRESHOLD || cap < needed) {
            size_t newCap = cap * 2 > needed ? cap * 2 : needed;
            if (newCap < 64) newCap = 64;
            usearch_reserve(pVS->pIdx, newCap, &err); err = NULL;
        }
    }
    for (int i = 0; i < nItems; i++) {
        usearch_add(pVS->pIdx, (usearch_key_t)(baseId + i),
                    pItems[i].pVec, usearch_scalar_f32_k, &err);
        err = NULL;
    }
    snkv_free(oldLabels);
    sqlite3_mutex_leave(pVS->pMutex);
    return KVSTORE_OK;

batch_rollback:
    kvstore_rollback(pVS->pKV);
    snkv_free(oldLabels);
    sqlite3_mutex_leave(pVS->pMutex);
    return rc;
}

/* =======================================================================
** kvstore_vec_get
** ======================================================================= */
int kvstore_vec_get(
    KVVecStore  *pVS,
    const void  *pKey,  int nKey,
    void       **ppVal, int *pnVal
) {
    if (!pVS || !pKey || !ppVal || !pnVal) return KVSTORE_ERROR;
    return kvstore_get(pVS->pKV, pKey, nKey, ppVal, pnVal);
}

/* =======================================================================
** kvstore_vec_get_vector
** ======================================================================= */
int kvstore_vec_get_vector(
    KVVecStore  *pVS,
    const void  *pKey,  int nKey,
    float      **ppVec, int *pnFloats
) {
    if (!pVS || !pKey || !ppVec || !pnFloats) return KVSTORE_ERROR;
    if (!pVS->pVecCF) return KVVEC_INDEX_DROPPED;

    void *pRaw = NULL; int nRaw = 0;
    int rc = kvstore_cf_get(pVS->pVecCF, pKey, nKey, &pRaw, &nRaw);
    if (rc != KVSTORE_OK) return rc;

    *ppVec    = (float*)pRaw;
    *pnFloats = nRaw / (int)sizeof(float);
    return KVSTORE_OK;
}

/* =======================================================================
** kvstore_vec_get_metadata
** ======================================================================= */
int kvstore_vec_get_metadata(
    KVVecStore  *pVS,
    const void  *pKey,  int nKey,
    void       **ppMeta, int *pnMeta
) {
    if (!pVS || !pKey || !ppMeta || !pnMeta) return KVSTORE_ERROR;
    *ppMeta = NULL; *pnMeta = 0;
    sqlite3_mutex_enter(pVS->pMutex);
    int rc;
    if (!pVS->pTagsCF) {
        rc = KVSTORE_OK; /* no metadata stored — not an error */
    } else {
        rc = kvstore_cf_get(pVS->pTagsCF, pKey, nKey, ppMeta, pnMeta);
    }
    sqlite3_mutex_leave(pVS->pMutex);
    return rc;
}

/* =======================================================================
** kvstore_vec_kv_put
** ======================================================================= */
int kvstore_vec_kv_put(
    KVVecStore  *pVS,
    const void  *pKey, int nKey,
    const void  *pVal, int nVal
) {
    if (!pVS) return KVSTORE_ERROR;
    return kvstore_put(pVS->pKV, pKey, nKey, pVal, nVal);
}

/* =======================================================================
** kvstore_vec_contains
** ======================================================================= */
int kvstore_vec_contains(
    KVVecStore  *pVS,
    const void  *pKey, int nKey
) {
    if (!pVS || !pKey) return 0;
    void *pVal = NULL; int nVal = 0;
    int rc = kvstore_get(pVS->pKV, pKey, nKey, &pVal, &nVal);
    if (rc == KVSTORE_OK) { snkv_free(pVal); return 1; }
    return 0;
}

/* =======================================================================
** kvstore_vec_count
** ======================================================================= */
int64_t kvstore_vec_count(KVVecStore *pVS) {
    if (!pVS || !pVS->pIdx) return 0;
    sqlite3_mutex_enter(pVS->pMutex);
    usearch_error_t err = NULL;
    int64_t n = (int64_t)usearch_size(pVS->pIdx, &err);
    sqlite3_mutex_leave(pVS->pMutex);
    return n;
}

/* =======================================================================
** kvstore_vec_delete
** ======================================================================= */
int kvstore_vec_delete(
    KVVecStore  *pVS,
    const void  *pKey, int nKey
) {
    if (!pVS || !pKey) return KVSTORE_ERROR;

    sqlite3_mutex_enter(pVS->pMutex);

    /* If index was dropped, plain KV delete */
    if (!pVS->pIdkCF) {
        int rc2 = kvstore_delete(pVS->pKV, pKey, nKey);
        sqlite3_mutex_leave(pVS->pMutex);
        return rc2;
    }

    void *pIdRaw = NULL; int nIdRaw = 0;
    kvstore_cf_get(pVS->pIdkCF, pKey, nKey, &pIdRaw, &nIdRaw);
    int64_t intId = -1;
    if (pIdRaw && nIdRaw == 8) {
        intId = unpackI64((const unsigned char*)pIdRaw);
    }
    snkv_free(pIdRaw);

    if (intId < 0) {
        /* Key exists only in default CF (no vector) */
        int rc2 = kvstore_delete(pVS->pKV, pKey, nKey);
        sqlite3_mutex_leave(pVS->pMutex);
        return rc2;
    }

    int rc = kvstore_begin(pVS->pKV, 1);
    if (rc != KVSTORE_OK) { sqlite3_mutex_leave(pVS->pMutex); return rc; }

    rc = kvstore_delete(pVS->pKV, pKey, nKey);
    if (rc != KVSTORE_OK) goto del_rollback;

    kvstore_cf_delete(pVS->pVecCF, pKey, nKey);
    kvstore_cf_delete(pVS->pIdkCF, pKey, nKey);

    {
        unsigned char idBuf[8]; packI64(idBuf, intId);
        kvstore_cf_delete(pVS->pIdiCF, idBuf, 8);
    }

    if (pVS->pTagsCF) {
        kvstore_cf_delete(pVS->pTagsCF, pKey, nKey);
    }

    rc = kvstore_commit(pVS->pKV);
    if (rc != KVSTORE_OK) { sqlite3_mutex_leave(pVS->pMutex); return rc; }

    if (pVS->pIdx) {
        usearch_error_t err = NULL;
        usearch_remove(pVS->pIdx, (usearch_key_t)intId, &err);
    }
    sqlite3_mutex_leave(pVS->pMutex);
    return KVSTORE_OK;

del_rollback:
    kvstore_rollback(pVS->pKV);
    sqlite3_mutex_leave(pVS->pMutex);
    return rc;
}

/* =======================================================================
** Exact-rerank helpers
** ======================================================================= */
static float exactDistL2(const float *a, const float *b, int dim) {
    float s = 0.0f;
    for (int i = 0; i < dim; i++) { float d = a[i]-b[i]; s += d*d; }
    return s;
}

static float exactDistCosine(const float *a, const float *b, int dim) {
    float dot = 0.0f, na = 0.0f, nb = 0.0f;
    for (int i = 0; i < dim; i++) {
        dot += a[i]*b[i]; na += a[i]*a[i]; nb += b[i]*b[i];
    }
    float denom = sqrtf(na) * sqrtf(nb) + 1e-10f;
    return 1.0f - dot / denom;
}

static float exactDistIP(const float *a, const float *b, int dim) {
    float dot = 0.0f;
    for (int i = 0; i < dim; i++) dot += a[i]*b[i];
    return -dot;
}

static float exactDist(const float *q, const float *v, int dim, int space) {
    switch (space) {
        case KVVEC_SPACE_COSINE: return exactDistCosine(q, v, dim);
        case KVVEC_SPACE_IP:     return exactDistIP(q, v, dim);
        default:                 return exactDistL2(q, v, dim);
    }
}

/* =======================================================================
** kvstore_vec_search
** ======================================================================= */
int kvstore_vec_search(
    KVVecStore          *pVS,
    const float         *pQuery,
    int                  top_k,
    int                  rerank,
    int                  oversample,
    float                max_distance,
    KVVecSearchResult  **ppResults,
    int                 *pnResults
) {
    if (!pVS || !pQuery || !ppResults || !pnResults) return KVSTORE_ERROR;
    *ppResults = NULL; *pnResults = 0;
    if (!pVS->pIdx) return KVVEC_INDEX_DROPPED;
    if (top_k <= 0) return KVSTORE_ERROR;
    if (oversample < 1) oversample = 3;

    sqlite3_mutex_enter(pVS->pMutex);
    usearch_error_t err = NULL;
    size_t sz = usearch_size(pVS->pIdx, &err);
    if (sz == 0) { sqlite3_mutex_leave(pVS->pMutex); return KVVEC_INDEX_EMPTY; }

    int fetch_k = (rerank) ? top_k * oversample : top_k;
    if ((size_t)fetch_k > sz) fetch_k = (int)sz;
    if (fetch_k < 1) fetch_k = 1;

    int src = KVSTORE_OK;
    usearch_key_t      *keys  = (usearch_key_t*)snkv_malloc(fetch_k * sizeof(usearch_key_t));
    usearch_distance_t *dists = (usearch_distance_t*)snkv_malloc(fetch_k * sizeof(usearch_distance_t));
    if (!keys || !dists) {
        snkv_free(keys); snkv_free(dists);
        src = KVSTORE_NOMEM; goto search_done;
    }

    {
    size_t found = usearch_search(pVS->pIdx, pQuery, usearch_scalar_f32_k,
                                  (size_t)fetch_k, keys, dists, &err);
    if (err) {
        snkv_free(keys); snkv_free(dists);
        src = KVSTORE_ERROR; goto search_done;
    }
    if (found == 0) { snkv_free(keys); snkv_free(dists); goto search_done; }

    /* Allocate candidate array (upper bound: found) */
    KVVecSearchResult *cands = (KVVecSearchResult*)snkv_malloc(
        found * sizeof(KVVecSearchResult));
    if (!cands) {
        snkv_free(keys); snkv_free(dists);
        src = KVSTORE_NOMEM; goto search_done;
    }
    int nCands = 0;

    /* For rerank: collect vec bytes */
    float **vecBufs = NULL;
    if (rerank) {
        vecBufs = (float**)snkv_malloc(found * sizeof(float*));
        if (!vecBufs) {
            snkv_free(cands); snkv_free(keys); snkv_free(dists);
            src = KVSTORE_NOMEM; goto search_done;
        }
        for (size_t i = 0; i < found; i++) vecBufs[i] = NULL;
    }

    for (size_t i = 0; i < found; i++) {
        unsigned char idBuf[8]; packI64(idBuf, (int64_t)keys[i]);
        void *pKey = NULL; int nKey = 0;
        if (kvstore_cf_get(pVS->pIdiCF, idBuf, 8, &pKey, &nKey) != KVSTORE_OK) continue;

        void *pVal = NULL; int nVal = 0;
        if (kvstore_get(pVS->pKV, pKey, nKey, &pVal, &nVal) != KVSTORE_OK) {
            snkv_free(pKey); continue; /* expired */
        }

        float *pVecBuf = NULL;
        if (rerank) {
            void *pRawVec = NULL; int nRawVec = 0;
            if (kvstore_cf_get(pVS->pVecCF, pKey, nKey, &pRawVec, &nRawVec) != KVSTORE_OK
                    || nRawVec != pVS->dim * (int)sizeof(float)) {
                snkv_free(pKey); snkv_free(pVal); snkv_free(pRawVec);
                continue;
            }
            pVecBuf = (float*)pRawVec;
        }

        KVVecSearchResult *c = &cands[nCands];
        c->pKey      = pKey;
        c->nKey      = nKey;
        c->pValue    = pVal;
        c->nValue    = nVal;
        c->distance  = dists[i];
        c->pMetadata = NULL;
        c->nMetadata = 0;
        if (vecBufs) vecBufs[nCands] = pVecBuf;
        nCands++;
    }

    snkv_free(keys); snkv_free(dists);

    /* Exact rerank */
    if (rerank && nCands > 0) {
        for (int i = 0; i < nCands; i++) {
            cands[i].distance = exactDist(pQuery, vecBufs[i], pVS->dim, pVS->space);
            snkv_free(vecBufs[i]);
            vecBufs[i] = NULL;
        }
        /* Simple insertion sort for small nCands */
        for (int i = 1; i < nCands; i++) {
            KVVecSearchResult tmp = cands[i];
            int j = i - 1;
            while (j >= 0 && cands[j].distance > tmp.distance) {
                cands[j+1] = cands[j]; j--;
            }
            cands[j+1] = tmp;
        }
    }
    snkv_free(vecBufs);

    /* All candidates expired or filtered out */
    if (nCands == 0) { snkv_free(cands); goto search_done; }

    /* Apply max_distance + top_k filter */
    KVVecSearchResult *out = (KVVecSearchResult*)snkv_malloc(
        nCands * sizeof(KVVecSearchResult));
    if (!out) {
        for (int i = 0; i < nCands; i++) {
            snkv_free(cands[i].pKey); snkv_free(cands[i].pValue);
            snkv_free(cands[i].pMetadata);
        }
        snkv_free(cands);
        src = KVSTORE_NOMEM; goto search_done;
    }
    int nOut = 0;
    for (int i = 0; i < nCands; i++) {
        if (nOut < top_k && (max_distance <= 0.0f || cands[i].distance <= max_distance)) {
            out[nOut++] = cands[i];
        } else {
            snkv_free(cands[i].pKey); snkv_free(cands[i].pValue);
            snkv_free(cands[i].pMetadata);
        }
    }
    snkv_free(cands);

    *ppResults = out;
    *pnResults = nOut;
    }

search_done:
    sqlite3_mutex_leave(pVS->pMutex);
    return src;
}

/* =======================================================================
** kvstore_vec_search_keys
** ======================================================================= */
int kvstore_vec_search_keys(
    KVVecStore       *pVS,
    const float      *pQuery,
    int               top_k,
    KVVecKeyResult  **ppResults,
    int              *pnResults
) {
    if (!pVS || !pQuery || !ppResults || !pnResults) return KVSTORE_ERROR;
    *ppResults = NULL; *pnResults = 0;
    if (!pVS->pIdx) return KVVEC_INDEX_DROPPED;
    if (top_k <= 0) return KVSTORE_ERROR;

    sqlite3_mutex_enter(pVS->pMutex);
    usearch_error_t err = NULL;
    size_t sz = usearch_size(pVS->pIdx, &err);
    if (sz == 0) { sqlite3_mutex_leave(pVS->pMutex); return KVVEC_INDEX_EMPTY; }

    int fetch_k = top_k;
    if ((size_t)fetch_k > sz) fetch_k = (int)sz;

    int skrc = KVSTORE_OK;
    usearch_key_t      *keys  = (usearch_key_t*)snkv_malloc(fetch_k * sizeof(usearch_key_t));
    usearch_distance_t *dists = (usearch_distance_t*)snkv_malloc(fetch_k * sizeof(usearch_distance_t));
    if (!keys || !dists) {
        snkv_free(keys); snkv_free(dists);
        skrc = KVSTORE_NOMEM; goto skeys_done;
    }

    {
    size_t found = usearch_search(pVS->pIdx, pQuery, usearch_scalar_f32_k,
                                  (size_t)fetch_k, keys, dists, &err);
    if (err) {
        snkv_free(keys); snkv_free(dists);
        skrc = KVSTORE_ERROR; goto skeys_done;
    }
    if (found == 0) { snkv_free(keys); snkv_free(dists); goto skeys_done; }

    KVVecKeyResult *out = (KVVecKeyResult*)snkv_malloc(found * sizeof(KVVecKeyResult));
    if (!out) {
        snkv_free(keys); snkv_free(dists);
        skrc = KVSTORE_NOMEM; goto skeys_done;
    }

    int nOut = 0;
    for (size_t i = 0; i < found; i++) {
        unsigned char idBuf[8]; packI64(idBuf, (int64_t)keys[i]);
        void *pKey = NULL; int nKey = 0;
        if (kvstore_cf_get(pVS->pIdiCF, idBuf, 8, &pKey, &nKey) != KVSTORE_OK) continue;
        /* Skip expired */
        void *pTmp = NULL; int nTmp = 0;
        if (kvstore_get(pVS->pKV, pKey, nKey, &pTmp, &nTmp) != KVSTORE_OK) {
            snkv_free(pKey); continue;
        }
        snkv_free(pTmp);
        out[nOut].pKey     = pKey;
        out[nOut].nKey     = nKey;
        out[nOut].distance = dists[i];
        nOut++;
    }
    snkv_free(keys); snkv_free(dists);

    *ppResults = out;
    *pnResults = nOut;
    }

skeys_done:
    sqlite3_mutex_leave(pVS->pMutex);
    return skrc;
}

/* =======================================================================
** kvstore_vec_free_results / kvstore_vec_free_key_results
** ======================================================================= */
void kvstore_vec_free_results(KVVecSearchResult *pResults, int nResults) {
    if (!pResults) return;
    for (int i = 0; i < nResults; i++) {
        snkv_free(pResults[i].pKey);
        snkv_free(pResults[i].pValue);
        snkv_free(pResults[i].pMetadata);
    }
    snkv_free(pResults);
}

void kvstore_vec_free_key_results(KVVecKeyResult *pResults, int nResults) {
    if (!pResults) return;
    for (int i = 0; i < nResults; i++) {
        snkv_free(pResults[i].pKey);
    }
    snkv_free(pResults);
}

/* =======================================================================
** kvstore_vec_stats
** ======================================================================= */
int kvstore_vec_stats(KVVecStore *pVS, KVVecStats *pStats) {
    if (!pVS || !pStats) return KVSTORE_ERROR;
    memset(pStats, 0, sizeof(*pStats));

    sqlite3_mutex_enter(pVS->pMutex);
    pStats->dim              = pVS->dim;
    pStats->space            = pVS->space;
    pStats->dtype            = pVS->dtype;
    pStats->connectivity     = pVS->connectivity;
    pStats->expansion_add    = pVS->expansion_add;
    pStats->expansion_search = pVS->expansion_search;

    if (pVS->pIdx) {
        usearch_error_t err = NULL;
        pStats->count    = (int64_t)usearch_size(pVS->pIdx, &err);
        pStats->capacity = (int64_t)usearch_capacity(pVS->pIdx, &err);
        pStats->fill_ratio = pStats->capacity > 0
            ? (double)pStats->count / (double)pStats->capacity : 0.0;
    }
    if (pVS->pVecCF) {
        kvstore_cf_count(pVS->pVecCF, &pStats->vec_cf_count);
    }
    pStats->has_metadata    = (pVS->pTagsCF != NULL) ? 1 : 0;
    pStats->sidecar_enabled = (pVS->zSidecarPath != NULL) ? 1 : 0;
    sqlite3_mutex_leave(pVS->pMutex);
    return KVSTORE_OK;
}

/* =======================================================================
** kvstore_vec_purge_expired
** ======================================================================= */
int kvstore_vec_purge_expired(KVVecStore *pVS, int *pnDeleted) {
    if (pnDeleted) *pnDeleted = 0;
    if (!pVS || !pVS->pIdx || !pVS->pVecCF) return KVSTORE_OK;

    sqlite3_mutex_enter(pVS->pMutex);
    usearch_error_t err = NULL;

    /* Collect expired keys by scanning _snkv_vec_ and checking default CF */
    typedef struct { void *pKey; int nKey; int64_t intId; } Expired;
    Expired *expired = NULL;
    int nExpired = 0, capExpired = 0;

    KVIterator *pIter = NULL;
    if (kvstore_cf_iterator_create(pVS->pVecCF, &pIter) != KVSTORE_OK) {
        sqlite3_mutex_leave(pVS->pMutex);
        return KVSTORE_OK;
    }

    int iterRc = kvstore_iterator_first(pIter);
    while (iterRc == KVSTORE_OK && !kvstore_iterator_eof(pIter)) {
        void *pKey = NULL; int nKey = 0;
        kvstore_iterator_key(pIter, &pKey, &nKey);

        void *pVal = NULL; int nVal = 0;
        if (kvstore_get(pVS->pKV, pKey, nKey, &pVal, &nVal) != KVSTORE_OK) {
            /* expired or missing — collect for deletion */
            void *pIdRaw = NULL; int nIdRaw = 0;
            kvstore_cf_get(pVS->pIdkCF, pKey, nKey, &pIdRaw, &nIdRaw);
            int64_t intId = (pIdRaw && nIdRaw == 8)
                ? unpackI64((const unsigned char*)pIdRaw) : -1;
            snkv_free(pIdRaw);

            if (nExpired >= capExpired) {
                int newCap = capExpired == 0 ? 16 : capExpired * 2;
                Expired *tmp = (Expired*)snkv_malloc(newCap * sizeof(Expired));
                if (!tmp) break; /* pKey is iterator-internal — do NOT free */
                memcpy(tmp, expired, nExpired * sizeof(Expired));
                snkv_free(expired);
                expired = tmp;
                capExpired = newCap;
            }
            /* Must copy the key (iterator buffer is internal) */
            void *keyCopy = snkv_malloc(nKey);
            if (!keyCopy) break; /* pKey is iterator-internal — do NOT free */
            memcpy(keyCopy, pKey, nKey);
            expired[nExpired].pKey  = keyCopy;
            expired[nExpired].nKey  = nKey;
            expired[nExpired].intId = intId;
            nExpired++;
        } else {
            snkv_free(pVal);
        }
        /* pKey is an internal buffer — do NOT free */
        iterRc = kvstore_iterator_next(pIter);
    }
    kvstore_iterator_close(pIter);

    if (nExpired == 0) {
        snkv_free(expired);
        sqlite3_mutex_leave(pVS->pMutex);
        return KVSTORE_OK;
    }

    int rc = kvstore_begin(pVS->pKV, 1);
    if (rc != KVSTORE_OK) { goto purge_done; }

    for (int i = 0; i < nExpired; i++) {
        kvstore_cf_delete(pVS->pVecCF, expired[i].pKey, expired[i].nKey);
        kvstore_cf_delete(pVS->pIdkCF, expired[i].pKey, expired[i].nKey);
        if (expired[i].intId >= 0) {
            unsigned char idBuf[8]; packI64(idBuf, expired[i].intId);
            kvstore_cf_delete(pVS->pIdiCF, idBuf, 8);
        }
        if (pVS->pTagsCF) {
            kvstore_cf_delete(pVS->pTagsCF, expired[i].pKey, expired[i].nKey);
        }
    }
    rc = kvstore_commit(pVS->pKV);
    if (rc == KVSTORE_OK) {
        for (int i = 0; i < nExpired; i++) {
            if (expired[i].intId >= 0) {
                usearch_remove(pVS->pIdx, (usearch_key_t)expired[i].intId, &err);
                err = NULL;
            }
        }
        if (pnDeleted) *pnDeleted = nExpired;
    } else {
        kvstore_rollback(pVS->pKV);
    }

purge_done:
    for (int i = 0; i < nExpired; i++) snkv_free(expired[i].pKey);
    snkv_free(expired);
    sqlite3_mutex_leave(pVS->pMutex);
    return rc;
}

/* =======================================================================
** kvstore_vec_drop_index
** ======================================================================= */
int kvstore_vec_drop_index(KVVecStore *pVS) {
    if (!pVS) return KVSTORE_ERROR;
    sqlite3_mutex_enter(pVS->pMutex);

    KVColumnFamily *cfs[] = {
        pVS->pVecCF, pVS->pIdkCF, pVS->pIdiCF, pVS->pMetaCF, pVS->pTagsCF
    };
    const char *cfNames[] = { CF_VEC, CF_IDK, CF_IDI, CF_META, CF_TAGS };

    for (int i = 0; i < 5; i++) {
        if (cfs[i]) {
            kvstore_cf_close(cfs[i]);
            kvstore_cf_drop(pVS->pKV, cfNames[i]); /* ignore NOTFOUND */
        }
    }

    pVS->pVecCF  = NULL;
    pVS->pIdkCF  = NULL;
    pVS->pIdiCF  = NULL;
    pVS->pMetaCF = NULL;
    pVS->pTagsCF = NULL;
    pVS->nextId  = 0;

    if (pVS->pIdx) {
        usearch_error_t err = NULL;
        usearch_free(pVS->pIdx, &err);
        pVS->pIdx = NULL;
    }

    /* Remove sidecar so next open won't load stale index */
    if (pVS->zSidecarPath) {
        char nidPath[4096];
        snprintf(nidPath, sizeof(nidPath), "%s.nid", pVS->zSidecarPath);
        remove(pVS->zSidecarPath);
        remove(nidPath);
        snkv_free(pVS->zSidecarPath);
        pVS->zSidecarPath = NULL;
    }
    sqlite3_mutex_leave(pVS->pMutex);
    return KVSTORE_OK;
}
