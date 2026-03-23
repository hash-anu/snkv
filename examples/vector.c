/* SPDX-License-Identifier: Apache-2.0 */
/*
** examples/vector.c — SNKV Vector Store: comprehensive C API walkthrough
**
** Build:
**   make vector             # builds libsnkv_vec.a
**   make vector-examples    # compiles this file and links it
**
** Run:
**   ./examples/vector
**
** Covers every public API in kvstore_vec.h:
**   Lifecycle        — open, close
**   Writes           — put, put_batch, kv_put
**   Reads            — get, get_vector, get_metadata, contains, count
**   Search           — search, search_keys (ANN + rerank + max_distance)
**   Delete           — delete
**   TTL              — put with expire_ms, purge_expired
**   Stats            — kvstore_vec_stats
**   Drop index       — drop_index (KV data survives)
**   Sidecar          — fast reload from .usearch / .usearch.nid on reopen
**   Encryption       — encrypted store (sidecar disabled automatically)
*/

#include "kvstore_vec.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>

#define DB   "examples/vec_demo.db"
#define DIM  8

/* -----------------------------------------------------------------------
** Tiny helpers
** ----------------------------------------------------------------------- */
static void die(const char *msg, int rc) {
    fprintf(stderr, "FATAL: %s  (rc=%d)\n", msg, rc);
    exit(1);
}

static void fill_vec(float *v, float base) {
    for (int i = 0; i < DIM; i++) v[i] = base + i * 0.1f;
}

static void print_vec(const float *v, int n) {
    printf("[");
    for (int i = 0; i < n; i++) printf("%.2f%s", v[i], i+1<n?", ":"");
    printf("]");
}

static void rm_db(void) {
    remove(DB); remove(DB "-wal"); remove(DB "-shm");
    remove(DB ".usearch"); remove(DB ".usearch.nid");
}

/* ====================================================================
** 1. Lifecycle — open and close
** ==================================================================== */
static void section_lifecycle(void) {
    printf("\n--- 1. Lifecycle ---\n");
    rm_db();

    KVVecStore *vs = NULL;
    int rc = kvstore_vec_open(
        DB,
        DIM,               /* vector dimension — fixed for the lifetime of the store */
        KVVEC_SPACE_L2,    /* distance metric: L2 squared Euclidean */
        0,                 /* HNSW connectivity M (0 = default 16) */
        0,                 /* ef_construction (0 = default 128) */
        0,                 /* ef_search (0 = default 64) */
        KVVEC_DTYPE_F32,   /* in-memory index precision */
        NULL, 0,           /* no password → plain store */
        &vs
    );
    if (rc != KVSTORE_OK) die("open", rc);
    printf("Opened store: dim=%d  space=L2  dtype=f32\n", DIM);

    kvstore_vec_close(vs);
    printf("Closed — sidecar saved to %s.usearch\n", DB);
}

/* ====================================================================
** 2. Writes — put, put_batch, kv_put
** ==================================================================== */
static void section_writes(KVVecStore *vs) {
    printf("\n--- 2. Writes ---\n");
    int rc;

    /* Single put ---------------------------------------------------- */
    float v1[DIM]; fill_vec(v1, 1.0f);
    rc = kvstore_vec_put(vs,
        "doc:1", 5,          /* key */
        "hello world", 11,   /* value */
        v1,                  /* float32 vector */
        0,                   /* expire_ms = 0 → no TTL */
        "{\"topic\":\"ai\"}", 15  /* JSON metadata */
    );
    if (rc != KVSTORE_OK) die("put doc:1", rc);
    printf("Put doc:1  vec="); print_vec(v1, DIM); printf("\n");

    /* Single put with no metadata ------------------------------------ */
    float v2[DIM]; fill_vec(v2, 2.0f);
    rc = kvstore_vec_put(vs, "doc:2", 5, "world peace", 11,
                         v2, 0, NULL, 0);
    if (rc != KVSTORE_OK) die("put doc:2", rc);
    printf("Put doc:2  vec="); print_vec(v2, DIM); printf("\n");

    /* Batch put ------------------------------------------------------- */
    float vecs[3][DIM];
    fill_vec(vecs[0], 3.0f);
    fill_vec(vecs[1], 4.0f);
    fill_vec(vecs[2], 5.0f);

    KVVecItem batch[3] = {
        { "doc:3", 5, "item three",  10, vecs[0], NULL, 0 },
        { "doc:4", 5, "item four",   9,  vecs[1], NULL, 0 },
        { "doc:5", 5, "item five",   9,  vecs[2], NULL, 0 },
    };
    rc = kvstore_vec_put_batch(vs, batch, 3, 0 /* no TTL */);
    if (rc != KVSTORE_OK) die("put_batch", rc);
    printf("Batch put doc:3 doc:4 doc:5 (3 items, one transaction)\n");

    /* Plain KV put (no vector index update) -------------------------- */
    rc = kvstore_vec_kv_put(vs,
        "config:dim", 10, "8", 1);
    if (rc != KVSTORE_OK) die("kv_put", rc);
    printf("kv_put config:dim (KV only — not in vector index)\n");
}

/* ====================================================================
** 3. Reads — get, get_vector, get_metadata, contains, count
** ==================================================================== */
static void section_reads(KVVecStore *vs) {
    printf("\n--- 3. Reads ---\n");

    /* get value ------------------------------------------------------ */
    void *pVal = NULL; int nVal = 0;
    int rc = kvstore_vec_get(vs, "doc:1", 5, &pVal, &nVal);
    if (rc != KVSTORE_OK) die("get doc:1", rc);
    printf("get doc:1  →  \"%.*s\"\n", nVal, (char*)pVal);
    snkv_free(pVal);

    /* get_vector ----------------------------------------------------- */
    float *pVec = NULL; int nFloats = 0;
    rc = kvstore_vec_get_vector(vs, "doc:1", 5, &pVec, &nFloats);
    if (rc != KVSTORE_OK) die("get_vector doc:1", rc);
    printf("get_vector doc:1  →  "); print_vec(pVec, nFloats); printf("\n");
    snkv_free(pVec);

    /* get_metadata --------------------------------------------------- */
    void *pMeta = NULL; int nMeta = 0;
    rc = kvstore_vec_get_metadata(vs, "doc:1", 5, &pMeta, &nMeta);
    if (rc == KVSTORE_OK && pMeta) {
        printf("get_metadata doc:1  →  %.*s\n", nMeta, (char*)pMeta);
        snkv_free(pMeta);
    }

    /* contains ------------------------------------------------------- */
    printf("contains doc:2      →  %d\n", kvstore_vec_contains(vs, "doc:2", 5));
    printf("contains doc:99     →  %d\n", kvstore_vec_contains(vs, "doc:99", 6));

    /* count ---------------------------------------------------------- */
    printf("count               →  %lld vectors in HNSW index\n",
           (long long)kvstore_vec_count(vs));
}

/* ====================================================================
** 4. Search — ANN, search_keys, rerank, max_distance
** ==================================================================== */
static void section_search(KVVecStore *vs) {
    printf("\n--- 4. Search ---\n");
    int rc;

    /* query close to doc:3 (base 3.0) */
    float q[DIM]; fill_vec(q, 3.05f);

    /* Basic ANN search ----------------------------------------------- */
    KVVecSearchResult *res = NULL; int n = 0;
    rc = kvstore_vec_search(vs, q,
        3,      /* top_k */
        0,      /* rerank = 0 → use HNSW distances */
        0,      /* oversample (only used with rerank) */
        0.0f,   /* max_distance = 0 → no limit */
        &res, &n);
    if (rc != KVSTORE_OK) die("search", rc);
    printf("ANN search (top_k=3, no rerank):\n");
    for (int i = 0; i < n; i++) {
        printf("  [%d]  key=%-8.*s  dist=%.4f  val=%.*s\n",
               i, res[i].nKey, (char*)res[i].pKey,
               res[i].distance,
               res[i].nValue, (char*)res[i].pValue);
    }
    kvstore_vec_free_results(res, n);

    /* search_keys — returns (key, distance) only, no value fetch ----- */
    KVVecKeyResult *kres = NULL; int kn = 0;
    rc = kvstore_vec_search_keys(vs, q, 2, &kres, &kn);
    if (rc != KVSTORE_OK) die("search_keys", rc);
    printf("search_keys (top_k=2):\n");
    for (int i = 0; i < kn; i++)
        printf("  [%d]  key=%-8.*s  dist=%.4f\n",
               i, kres[i].nKey, (char*)kres[i].pKey, kres[i].distance);
    kvstore_vec_free_key_results(kres, kn);

    /* search with rerank --------------------------------------------- */
    res = NULL; n = 0;
    rc = kvstore_vec_search(vs, q, 2,
        1,   /* rerank = 1: fetch oversample×top_k candidates, exact rerank */
        3,   /* oversample factor */
        0.0f, &res, &n);
    if (rc != KVSTORE_OK) die("search rerank", rc);
    printf("search with exact rerank (oversample=3):\n");
    for (int i = 0; i < n; i++)
        printf("  [%d]  key=%-8.*s  dist(exact)=%.4f\n",
               i, res[i].nKey, (char*)res[i].pKey, res[i].distance);
    kvstore_vec_free_results(res, n);

    /* search with max_distance --------------------------------------- */
    res = NULL; n = 0;
    rc = kvstore_vec_search(vs, q, 5,
        0, 0,
        0.05f,  /* only return results within distance 0.05 */
        &res, &n);
    if (rc != KVSTORE_OK) die("search max_dist", rc);
    printf("search with max_distance=0.05  →  %d result(s)\n", n);
    for (int i = 0; i < n; i++)
        printf("  [%d]  key=%-8.*s  dist=%.4f\n",
               i, res[i].nKey, (char*)res[i].pKey, res[i].distance);
    kvstore_vec_free_results(res, n);
}

/* ====================================================================
** 5. Delete
** ==================================================================== */
static void section_delete(KVVecStore *vs) {
    printf("\n--- 5. Delete ---\n");

    int rc = kvstore_vec_delete(vs, "doc:5", 5);
    if (rc != KVSTORE_OK) die("delete doc:5", rc);
    printf("Deleted doc:5  →  count now %lld\n",
           (long long)kvstore_vec_count(vs));

    /* delete missing key */
    rc = kvstore_vec_delete(vs, "doc:99", 6);
    printf("Delete doc:99 (missing)  →  rc=%d (NOTFOUND=%d)\n",
           rc, KVSTORE_NOTFOUND);
}

/* ====================================================================
** 6. TTL — put with expiry, purge_expired
** ==================================================================== */
static void section_ttl(KVVecStore *vs) {
    printf("\n--- 6. TTL ---\n");

    float v[DIM]; fill_vec(v, 9.0f);

    /* Expired in the past -------------------------------------------- */
    int64_t past_ms = kvstore_now_ms() - 1000; /* 1 s ago */
    int rc = kvstore_vec_put(vs, "tmp:sess", 8, "bearer-xyz", 10,
                             v, past_ms, NULL, 0);
    if (rc != KVSTORE_OK) die("put ttl", rc);
    printf("Put tmp:sess with past expire_ms (%lld ms ago)\n",
           (long long)(kvstore_now_ms() - past_ms));

    /* Lazy expiry on get --------------------------------------------- */
    void *pVal = NULL; int nVal = 0;
    rc = kvstore_vec_get(vs, "tmp:sess", 8, &pVal, &nVal);
    printf("get tmp:sess  →  rc=%d (NOTFOUND=%d — lazily expired)\n",
           rc, KVSTORE_NOTFOUND);
    snkv_free(pVal);

    /* Bulk purge ------------------------------------------------------ */
    int nPurged = 0;
    rc = kvstore_vec_purge_expired(vs, &nPurged);
    printf("purge_expired  →  rc=%d  nPurged=%d\n", rc, nPurged);
}

/* ====================================================================
** 7. Stats
** ==================================================================== */
static void section_stats(KVVecStore *vs) {
    printf("\n--- 7. Stats ---\n");

    KVVecStats st;
    kvstore_vec_stats(vs, &st);
    printf("dim=%d  space=%d  dtype=%d\n", st.dim, st.space, st.dtype);
    printf("connectivity=%d  ef_add=%d  ef_search=%d\n",
           st.connectivity, st.expansion_add, st.expansion_search);
    printf("count=%lld  capacity=%lld  fill=%.1f%%\n",
           (long long)st.count, (long long)st.capacity, st.fill_ratio * 100.0);
    printf("vec_cf_count=%lld  has_metadata=%d  sidecar=%d\n",
           (long long)st.vec_cf_count, st.has_metadata, st.sidecar_enabled);
}

/* ====================================================================
** 8. Drop vector index — KV data survives
** ==================================================================== */
static void section_drop(KVVecStore *vs) {
    printf("\n--- 8. Drop vector index ---\n");

    /* Store a key before dropping */
    float v[DIM]; fill_vec(v, 1.5f);
    kvstore_vec_put(vs, "keep:me", 7, "still here", 10, v, 0, NULL, 0);

    int rc = kvstore_vec_drop_index(vs);
    printf("drop_index  →  rc=%d\n", rc);
    printf("count after drop  →  %lld (HNSW freed)\n",
           (long long)kvstore_vec_count(vs));

    /* KV value still accessible */
    void *pVal = NULL; int nVal = 0;
    rc = kvstore_vec_get(vs, "keep:me", 7, &pVal, &nVal);
    if (rc == KVSTORE_OK)
        printf("get keep:me after drop  →  \"%.*s\"  (KV data intact)\n",
               nVal, (char*)pVal);
    snkv_free(pVal);

    /* Search returns DROPPED */
    KVVecSearchResult *res = NULL; int n = 0;
    rc = kvstore_vec_search(vs, v, 1, 0, 0, 0.0f, &res, &n);
    printf("search after drop  →  rc=%d (DROPPED=%d)\n",
           rc, KVVEC_INDEX_DROPPED);
    kvstore_vec_free_results(res, n);
}

/* ====================================================================
** 9. Sidecar fast reload
** ==================================================================== */
static void section_sidecar(void) {
    printf("\n--- 9. Sidecar fast reload ---\n");
    rm_db();

    KVVecStore *vs = NULL;
    kvstore_vec_open(DB, DIM, KVVEC_SPACE_COSINE, 0, 0, 0,
                     KVVEC_DTYPE_F32, NULL, 0, &vs);

    float v[DIM]; fill_vec(v, 1.0f);
    kvstore_vec_put(vs, "s:1", 3, "sidecar test", 12, v, 0, NULL, 0);

    kvstore_vec_close(vs);
    vs = NULL;
    printf("Closed — .usearch and .usearch.nid written\n");

    /* Reopen: sidecar loaded → O(1) instead of O(n·dim) rebuild */
    int rc = kvstore_vec_open(DB, DIM, KVVEC_SPACE_COSINE, 0, 0, 0,
                              KVVEC_DTYPE_F32, NULL, 0, &vs);
    if (rc != KVSTORE_OK) die("sidecar reopen", rc);
    printf("Reopened via sidecar  →  count=%lld\n",
           (long long)kvstore_vec_count(vs));

    KVVecSearchResult *res = NULL; int n = 0;
    rc = kvstore_vec_search(vs, v, 1, 0, 0, 0.0f, &res, &n);
    if (rc == KVSTORE_OK && n > 0)
        printf("Search after reload  →  key=%.*s  dist=%.4f\n",
               res[0].nKey, (char*)res[0].pKey, res[0].distance);
    kvstore_vec_free_results(res, n);

    kvstore_vec_close(vs);
    rm_db();
}

/* ====================================================================
** 10. Encrypted vector store
** ==================================================================== */
static void section_encryption(void) {
    printf("\n--- 10. Encryption ---\n");
    rm_db();

    const uint8_t *pw = (const uint8_t *)"hunter2";
    int npw = 7;

    KVVecStore *vs = NULL;
    int rc = kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2, 0, 0, 0,
                              KVVEC_DTYPE_F32, pw, npw, &vs);
    if (rc != KVSTORE_OK) die("open encrypted", rc);

    KVVecStats st;
    kvstore_vec_stats(vs, &st);
    printf("Encrypted store  →  sidecar_enabled=%d (0 = disabled)\n",
           st.sidecar_enabled);

    float v[DIM]; fill_vec(v, 1.0f);
    kvstore_vec_put(vs, "secret:1", 8, "top secret value", 16,
                    v, 0, NULL, 0);
    printf("Put secret:1 (value encrypted on disk)\n");

    kvstore_vec_close(vs);
    vs = NULL;

    /* Correct password — works */
    rc = kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2, 0, 0, 0,
                          KVVEC_DTYPE_F32, pw, npw, &vs);
    printf("Reopen correct pw  →  rc=%d (OK=%d)\n", rc, KVSTORE_OK);
    if (rc == KVSTORE_OK) {
        void *pVal = NULL; int nVal = 0;
        kvstore_vec_get(vs, "secret:1", 8, &pVal, &nVal);
        printf("get secret:1  →  \"%.*s\"\n", nVal, (char*)pVal);
        snkv_free(pVal);
        kvstore_vec_close(vs);
    }

    /* Wrong password — rejected */
    vs = NULL;
    rc = kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2, 0, 0, 0,
                          KVVEC_DTYPE_F32,
                          (const uint8_t*)"wrong", 5, &vs);
    printf("Reopen wrong pw  →  rc=%d (AUTH_FAILED=%d)\n",
           rc, KVSTORE_AUTH_FAILED);
    kvstore_vec_close(vs); /* NULL-safe */
    rm_db();
}

/* ====================================================================
** main
** ==================================================================== */
int main(void) {
    printf("=== SNKV Vector Store Example ===\n");
    rm_db();

    /* Open a fresh store for sections 2–8 */
    KVVecStore *vs = NULL;
    int rc = kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2, 0, 0, 0,
                              KVVEC_DTYPE_F32, NULL, 0, &vs);
    if (rc != KVSTORE_OK) die("initial open", rc);

    section_writes(vs);
    section_reads(vs);
    section_search(vs);
    section_delete(vs);
    section_ttl(vs);
    section_stats(vs);
    section_drop(vs);

    kvstore_vec_close(vs);
    rm_db();

    /* Standalone sections that manage their own store */
    section_lifecycle();
    section_sidecar();
    section_encryption();

    printf("\n[OK] vector.c example complete.\n");
    return 0;
}
