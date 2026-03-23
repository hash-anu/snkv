/* SPDX-License-Identifier: Apache-2.0 */
/* tests/test_vec.c — comprehensive tests for kvstore_vec */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <stdint.h>

#include "kvstore_vec.h"

/* -----------------------------------------------------------------------
** Test harness
** ----------------------------------------------------------------------- */
static int g_pass = 0, g_fail = 0;

#define ASSERT(cond, msg) do { \
    if (cond) { printf("  [PASS] %s\n", msg); g_pass++; } \
    else       { printf("  [FAIL] %s  (line %d)\n", msg, __LINE__); g_fail++; } \
} while(0)

#define ASSERT_EQ(a,b,msg) ASSERT((a)==(b), msg)
#define ASSERT_OK(rc, msg) ASSERT((rc)==KVSTORE_OK, msg)
#define ASSERT_NE(rc, val, msg) ASSERT((rc)!=(val), msg)

static void section(const char *name) {
    printf("\n=== %s ===\n", name);
}

/* -----------------------------------------------------------------------
** Helpers
** ----------------------------------------------------------------------- */
#define DIM 8

static void fill_vec(float *v, float base) {
    for (int i = 0; i < DIM; i++) v[i] = base + i * 0.1f;
}

/* unit-normalise a vector */
static void normalize(float *v, int dim) {
    float s = 0.0f;
    for (int i = 0; i < dim; i++) s += v[i]*v[i];
    s = sqrtf(s);
    if (s > 0) for (int i = 0; i < dim; i++) v[i] /= s;
}

#define DB "tests/test_vec_tmp.db"

static void rm_db(void) {
    remove(DB);
    remove(DB "-wal");
    remove(DB "-shm");
    remove(DB ".usearch");
    remove(DB ".usearch.nid");
}

/* -----------------------------------------------------------------------
** T1: open / close empty store
** ----------------------------------------------------------------------- */
static void test_open_close(void) {
    section("T1: open / close");
    rm_db();

    KVVecStore *vs = NULL;
    int rc = kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                              0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs);
    ASSERT_OK(rc, "open new store");
    ASSERT(vs != NULL, "handle not NULL");

    KVVecStats st;
    kvstore_vec_stats(vs, &st);
    ASSERT_EQ(st.dim,   DIM,             "dim");
    ASSERT_EQ(st.space, KVVEC_SPACE_L2,  "space L2");
    ASSERT_EQ(st.dtype, KVVEC_DTYPE_F32, "dtype F32");
    ASSERT_EQ(st.count, 0,               "count=0");
    ASSERT_EQ(st.sidecar_enabled, 1,     "sidecar enabled");

    kvstore_vec_close(vs);

    /* sidecar should exist now (empty store — usearch saves even 0-vec index) */
    /* just confirm close doesn't crash */
    ASSERT(1, "close without crash");
    rm_db();
}

/* -----------------------------------------------------------------------
** T2: put / get / contains / count
** ----------------------------------------------------------------------- */
static void test_put_get(void) {
    section("T2: put / get / contains / count");
    rm_db();

    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                               0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs), "open");

    float v1[DIM]; fill_vec(v1, 1.0f);
    float v2[DIM]; fill_vec(v2, 2.0f);

    ASSERT_OK(kvstore_vec_put(vs, "k1", 2, "val1", 4, v1, 0, NULL, 0), "put k1");
    ASSERT_OK(kvstore_vec_put(vs, "k2", 2, "val2", 4, v2, 0, NULL, 0), "put k2");

    ASSERT_EQ(kvstore_vec_count(vs), 2, "count=2");
    ASSERT_EQ(kvstore_vec_contains(vs, "k1", 2), 1, "contains k1");
    ASSERT_EQ(kvstore_vec_contains(vs, "k2", 2), 1, "contains k2");
    ASSERT_EQ(kvstore_vec_contains(vs, "k3", 2), 0, "not contains k3");

    void *val = NULL; int nval = 0;
    ASSERT_OK(kvstore_vec_get(vs, "k1", 2, &val, &nval), "get k1");
    ASSERT(val && nval == 4 && memcmp(val, "val1", 4) == 0, "k1 value correct");
    snkv_free(val);

    float *vec = NULL; int nf = 0;
    ASSERT_OK(kvstore_vec_get_vector(vs, "k1", 2, &vec, &nf), "get_vector k1");
    ASSERT(vec && nf == DIM, "vector dim");
    ASSERT(fabsf(vec[0] - v1[0]) < 1e-6f, "vector[0] correct");
    snkv_free(vec);

    kvstore_vec_close(vs);
    rm_db();
}

/* -----------------------------------------------------------------------
** T3: overwrite — old usearch label removed, new label added
** ----------------------------------------------------------------------- */
static void test_overwrite(void) {
    section("T3: overwrite");
    rm_db();

    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                               0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs), "open");

    float v1[DIM]; fill_vec(v1, 1.0f);
    float v2[DIM]; fill_vec(v2, 9.9f);

    ASSERT_OK(kvstore_vec_put(vs, "k1", 2, "val1", 4, v1, 0, NULL, 0), "first put");
    ASSERT_EQ(kvstore_vec_count(vs), 1, "count=1 after first put");

    ASSERT_OK(kvstore_vec_put(vs, "k1", 2, "val2", 4, v2, 0, NULL, 0), "overwrite");
    ASSERT_EQ(kvstore_vec_count(vs), 1, "count still 1 after overwrite");

    void *val = NULL; int nval = 0;
    ASSERT_OK(kvstore_vec_get(vs, "k1", 2, &val, &nval), "get after overwrite");
    ASSERT(val && memcmp(val, "val2", 4) == 0, "new value correct");
    snkv_free(val);

    float *vec = NULL; int nf = 0;
    ASSERT_OK(kvstore_vec_get_vector(vs, "k1", 2, &vec, &nf), "get_vector after overwrite");
    ASSERT(vec && fabsf(vec[0] - v2[0]) < 1e-6f, "new vector correct");
    snkv_free(vec);

    kvstore_vec_close(vs);
    rm_db();
}

/* -----------------------------------------------------------------------
** T4: delete
** ----------------------------------------------------------------------- */
static void test_delete(void) {
    section("T4: delete");
    rm_db();

    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                               0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs), "open");

    float v[DIM]; fill_vec(v, 1.0f);
    ASSERT_OK(kvstore_vec_put(vs, "k1", 2, "v1", 2, v, 0, NULL, 0), "put");
    ASSERT_EQ(kvstore_vec_count(vs), 1, "count=1");

    ASSERT_OK(kvstore_vec_delete(vs, "k1", 2), "delete k1");
    ASSERT_EQ(kvstore_vec_count(vs), 0, "count=0 after delete");
    ASSERT_EQ(kvstore_vec_contains(vs, "k1", 2), 0, "k1 gone");

    /* Delete non-existent key returns NOTFOUND */
    int rc = kvstore_vec_delete(vs, "k1", 2);
    ASSERT_EQ(rc, KVSTORE_NOTFOUND, "delete missing key → NOTFOUND");

    kvstore_vec_close(vs);
    rm_db();
}

/* -----------------------------------------------------------------------
** T5: search — basic ANN, results ordered by distance
** ----------------------------------------------------------------------- */
static void test_search(void) {
    section("T5: search basic ANN");
    rm_db();

    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                               0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs), "open");

    /* Insert 10 vectors, each shifted by 1.0 along dim0 */
    for (int i = 0; i < 10; i++) {
        float v[DIM];
        for (int d = 0; d < DIM; d++) v[d] = (d == 0) ? (float)i : 0.0f;
        char key[4]; snprintf(key, sizeof(key), "k%d", i);
        kvstore_vec_put(vs, key, (int)strlen(key), "val", 3, v, 0, NULL, 0);
    }

    /* Query close to k3 (v[0]=3.0) */
    float q[DIM];
    for (int d = 0; d < DIM; d++) q[d] = (d == 0) ? 3.05f : 0.0f;

    KVVecSearchResult *res = NULL; int n = 0;
    ASSERT_OK(kvstore_vec_search(vs, q, 3, 0, 3, 0.0f, &res, &n), "search ok");
    ASSERT(n > 0, "got results");
    ASSERT(n <= 3, "at most 3 results");

    /* first result should be k3 */
    ASSERT(res && res[0].nKey == 2 && memcmp(res[0].pKey, "k3", 2) == 0,
           "nearest is k3");
    /* results ordered ascending by distance */
    if (n >= 2) {
        ASSERT(res[0].distance <= res[1].distance, "dist[0] <= dist[1]");
    }
    kvstore_vec_free_results(res, n);

    kvstore_vec_close(vs);
    rm_db();
}

/* -----------------------------------------------------------------------
** T6: search_keys
** ----------------------------------------------------------------------- */
static void test_search_keys(void) {
    section("T6: search_keys");
    rm_db();

    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                               0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs), "open");

    float v[DIM]; fill_vec(v, 1.0f);
    ASSERT_OK(kvstore_vec_put(vs, "key", 3, "val", 3, v, 0, NULL, 0), "put");

    KVVecKeyResult *res = NULL; int n = 0;
    ASSERT_OK(kvstore_vec_search_keys(vs, v, 1, &res, &n), "search_keys ok");
    ASSERT_EQ(n, 1, "one result");
    ASSERT(res && res[0].nKey == 3 && memcmp(res[0].pKey, "key", 3) == 0,
           "key correct");
    ASSERT(res[0].distance < 0.01f, "distance near 0");
    kvstore_vec_free_key_results(res, n);

    /* Empty index returns OK with 0 results (after deleting the key) */
    kvstore_vec_delete(vs, "key", 3);
    int rc = kvstore_vec_search_keys(vs, v, 1, &res, &n);
    ASSERT_EQ(rc, KVVEC_INDEX_EMPTY, "empty index → INDEX_EMPTY");

    kvstore_vec_close(vs);
    rm_db();
}

/* -----------------------------------------------------------------------
** T7: search with rerank
** ----------------------------------------------------------------------- */
static void test_search_rerank(void) {
    section("T7: search with rerank");
    rm_db();

    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_COSINE,
                               0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs), "open cosine");

    for (int i = 0; i < 5; i++) {
        float v[DIM];
        fill_vec(v, (float)(i + 1));
        normalize(v, DIM);
        char key[4]; snprintf(key, sizeof(key), "k%d", i);
        kvstore_vec_put(vs, key, (int)strlen(key), "v", 1, v, 0, NULL, 0);
    }

    float q[DIM]; fill_vec(q, 1.0f); normalize(q, DIM);

    KVVecSearchResult *res = NULL; int n = 0;
    ASSERT_OK(kvstore_vec_search(vs, q, 3, 1, 3, 0.0f, &res, &n),
              "search with rerank ok");
    ASSERT(n > 0, "rerank returns results");
    if (n >= 2) {
        ASSERT(res[0].distance <= res[1].distance, "rerank: dist[0] <= dist[1]");
    }
    kvstore_vec_free_results(res, n);

    kvstore_vec_close(vs);
    rm_db();
}

/* -----------------------------------------------------------------------
** T8: search with max_distance
** ----------------------------------------------------------------------- */
static void test_search_max_distance(void) {
    section("T8: search max_distance");
    rm_db();

    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                               0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs), "open");

    /* Two vectors: near (dist≈0) and far (dist≈100) from origin */
    float near_v[DIM]; for (int i = 0; i < DIM; i++) near_v[i] = 0.01f;
    float far_v[DIM];  for (int i = 0; i < DIM; i++) far_v[i]  = 10.0f;

    kvstore_vec_put(vs, "near", 4, "n", 1, near_v, 0, NULL, 0);
    kvstore_vec_put(vs, "far",  3, "f", 1, far_v,  0, NULL, 0);

    float q[DIM]; memset(q, 0, sizeof(q));

    /* max_distance=1.0 should drop the far vector */
    KVVecSearchResult *res = NULL; int n = 0;
    ASSERT_OK(kvstore_vec_search(vs, q, 5, 0, 3, 1.0f, &res, &n), "search ok");
    ASSERT(n == 1, "only near result returned");
    if (n == 1) {
        ASSERT(memcmp(res[0].pKey, "near", 4) == 0, "near key returned");
    }
    kvstore_vec_free_results(res, n);

    kvstore_vec_close(vs);
    rm_db();
}

/* -----------------------------------------------------------------------
** T9: metadata put / get
** ----------------------------------------------------------------------- */
static void test_metadata(void) {
    section("T9: metadata");
    rm_db();

    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                               0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs), "open");

    float v[DIM]; fill_vec(v, 1.0f);
    const char *meta = "{\"topic\":\"ml\"}";
    ASSERT_OK(kvstore_vec_put(vs, "k1", 2, "val", 3, v, 0, meta, (int)strlen(meta)),
              "put with metadata");

    void *m = NULL; int nm = 0;
    ASSERT_OK(kvstore_vec_get_metadata(vs, "k1", 2, &m, &nm), "get_metadata ok");
    ASSERT(m && nm == (int)strlen(meta), "metadata length");
    ASSERT(memcmp(m, meta, nm) == 0, "metadata bytes correct");
    snkv_free(m);

    /* Overwrite with empty metadata (clear) */
    ASSERT_OK(kvstore_vec_put(vs, "k1", 2, "val", 3, v, 0, "", 0), "clear metadata");
    m = NULL; nm = 0;
    /* After clear, get_metadata returns NOTFOUND or OK with NULL */
    int rc = kvstore_vec_get_metadata(vs, "k1", 2, &m, &nm);
    ASSERT(rc == KVSTORE_OK || rc == KVSTORE_NOTFOUND, "clear: ok or notfound");
    ASSERT(m == NULL || nm == 0, "cleared metadata is empty");
    snkv_free(m);

    /* Key with no metadata → KVSTORE_OK or KVSTORE_NOTFOUND, pMeta=NULL */
    float v2[DIM]; fill_vec(v2, 2.0f);
    kvstore_vec_put(vs, "k2", 2, "val", 3, v2, 0, NULL, 0);
    m = NULL; nm = 0;
    {
        int rc2 = kvstore_vec_get_metadata(vs, "k2", 2, &m, &nm);
        ASSERT(rc2 == KVSTORE_OK || rc2 == KVSTORE_NOTFOUND, "no-metadata key ok");
    }
    ASSERT(m == NULL && nm == 0, "no metadata returns NULL");

    kvstore_vec_close(vs);
    rm_db();
}

/* -----------------------------------------------------------------------
** T10: batch put
** ----------------------------------------------------------------------- */
static void test_batch(void) {
    section("T10: batch put");
    rm_db();

    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                               0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs), "open");

    float vecs[5][DIM];
    KVVecItem items[5];
    char keys[5][4];
    char vals[5][4];
    for (int i = 0; i < 5; i++) {
        fill_vec(vecs[i], (float)(i + 1));
        snprintf(keys[i], sizeof(keys[i]), "k%d", i);
        snprintf(vals[i], sizeof(vals[i]), "v%d", i);
        items[i].pKey  = keys[i];
        items[i].nKey  = (int)strlen(keys[i]);
        items[i].pVal  = vals[i];
        items[i].nVal  = (int)strlen(vals[i]);
        items[i].pVec  = vecs[i];
        items[i].pMeta = NULL;
        items[i].nMeta = 0;
    }

    ASSERT_OK(kvstore_vec_put_batch(vs, items, 5, 0), "batch put ok");
    ASSERT_EQ(kvstore_vec_count(vs), 5, "count=5");

    void *val = NULL; int nval = 0;
    ASSERT_OK(kvstore_vec_get(vs, "k3", 2, &val, &nval), "get k3 from batch");
    ASSERT(val && memcmp(val, "v3", 2) == 0, "batch value correct");
    snkv_free(val);

    kvstore_vec_close(vs);
    rm_db();
}

/* -----------------------------------------------------------------------
** T11: sidecar round-trip — close and reopen fast path
** ----------------------------------------------------------------------- */
static void test_sidecar(void) {
    section("T11: sidecar round-trip");
    rm_db();

    /* Open, insert, close (sidecar saved) */
    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                               0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs), "open");

    float v[DIM]; fill_vec(v, 1.5f);
    ASSERT_OK(kvstore_vec_put(vs, "sc", 2, "sval", 4, v, 0, NULL, 0), "put");
    kvstore_vec_close(vs);

    /* Sidecar files should exist */
    FILE *f1 = fopen(DB ".usearch",     "rb");
    FILE *f2 = fopen(DB ".usearch.nid", "rb");
    ASSERT(f1 != NULL, "sidecar .usearch exists");
    ASSERT(f2 != NULL, "sidecar .usearch.nid exists");
    if (f1) fclose(f1);
    if (f2) fclose(f2);

    /* Reopen — should load from sidecar */
    vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                               0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs), "reopen");

    ASSERT_EQ(kvstore_vec_count(vs), 1, "count=1 after sidecar reload");

    float q[DIM]; fill_vec(q, 1.5f);
    KVVecSearchResult *res = NULL; int n = 0;
    ASSERT_OK(kvstore_vec_search(vs, q, 1, 0, 1, 0.0f, &res, &n), "search after reload");
    ASSERT_EQ(n, 1, "1 result");
    ASSERT(res && res[0].nKey == 2 && memcmp(res[0].pKey, "sc", 2) == 0,
           "correct key after reload");
    kvstore_vec_free_results(res, n);

    kvstore_vec_close(vs);
    rm_db();
}

/* -----------------------------------------------------------------------
** T12: dim mismatch on reopen → KVVEC_DIM_MISMATCH
** ----------------------------------------------------------------------- */
static void test_dim_mismatch(void) {
    section("T12: dim mismatch");
    rm_db();

    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                               0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs), "open dim=8");
    float v[DIM]; fill_vec(v, 1.0f);
    kvstore_vec_put(vs, "k", 1, "v", 1, v, 0, NULL, 0);
    kvstore_vec_close(vs);

    vs = NULL;
    int rc = kvstore_vec_open(DB, 16 /* wrong dim */, KVVEC_SPACE_L2,
                              0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs);
    ASSERT_EQ(rc, KVVEC_DIM_MISMATCH, "dim mismatch detected");
    ASSERT(vs == NULL, "handle is NULL on mismatch");

    rm_db();
}

/* -----------------------------------------------------------------------
** T13: space mismatch on reopen → KVVEC_SPACE_MISMATCH
** ----------------------------------------------------------------------- */
static void test_space_mismatch(void) {
    section("T13: space mismatch");
    rm_db();

    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                               0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs), "open L2");
    float v[DIM]; fill_vec(v, 1.0f);
    kvstore_vec_put(vs, "k", 1, "v", 1, v, 0, NULL, 0);
    kvstore_vec_close(vs);

    vs = NULL;
    int rc = kvstore_vec_open(DB, DIM, KVVEC_SPACE_COSINE,
                              0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs);
    ASSERT_EQ(rc, KVVEC_SPACE_MISMATCH, "space mismatch detected");
    ASSERT(vs == NULL, "handle NULL on mismatch");

    rm_db();
}

/* -----------------------------------------------------------------------
** T14: plain KV put (kv_put / kv_get / contains)
** ----------------------------------------------------------------------- */
static void test_kv_put(void) {
    section("T14: plain kv_put");
    rm_db();

    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                               0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs), "open");

    ASSERT_OK(kvstore_vec_kv_put(vs, "plain", 5, "data", 4), "kv_put");
    ASSERT_EQ(kvstore_vec_contains(vs, "plain", 5), 1, "contains plain key");
    ASSERT_EQ(kvstore_vec_count(vs), 0, "vector count unchanged");

    void *val = NULL; int nval = 0;
    ASSERT_OK(kvstore_vec_get(vs, "plain", 5, &val, &nval), "get plain");
    ASSERT(val && memcmp(val, "data", 4) == 0, "plain value correct");
    snkv_free(val);

    kvstore_vec_close(vs);
    rm_db();
}

/* -----------------------------------------------------------------------
** T15: TTL — expired key absent from search
** ----------------------------------------------------------------------- */
static void test_ttl(void) {
    section("T15: TTL");
    rm_db();

    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                               0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs), "open");

    float v1[DIM]; fill_vec(v1, 1.0f);
    float v2[DIM]; fill_vec(v2, 10.0f);

    /* k1: expire in the past (already expired) */
    int64_t past_ms = kvstore_now_ms() - 5000;
    ASSERT_OK(kvstore_vec_put(vs, "k1", 2, "v1", 2, v1, past_ms, NULL, 0), "put expired");

    /* k2: no TTL */
    ASSERT_OK(kvstore_vec_put(vs, "k2", 2, "v2", 2, v2, 0, NULL, 0), "put permanent");

    /* k1 should be gone from get */
    void *val = NULL; int nval = 0;
    int rc = kvstore_vec_get(vs, "k1", 2, &val, &nval);
    ASSERT(rc == KVSTORE_NOTFOUND || val == NULL, "expired key not returned by get");
    snkv_free(val);

    /* Purge — should remove k1 from vector CFs */
    int ndel = 0;
    ASSERT_OK(kvstore_vec_purge_expired(vs, &ndel), "purge_expired ok");
    ASSERT_EQ(ndel, 1, "purged 1 key");

    /* After purge, count should be 1 (k2 only) */
    KVVecStats st;
    kvstore_vec_stats(vs, &st);
    ASSERT_EQ(st.vec_cf_count, 1, "vec_cf_count=1 after purge");

    /* Second purge: nothing to delete */
    ndel = 0;
    ASSERT_OK(kvstore_vec_purge_expired(vs, &ndel), "second purge ok");
    ASSERT_EQ(ndel, 0, "nothing deleted on second purge");

    kvstore_vec_close(vs);
    rm_db();
}

/* -----------------------------------------------------------------------
** T16: drop_vector_index — KV data survives, search fails
** ----------------------------------------------------------------------- */
static void test_drop_index(void) {
    section("T16: drop_vector_index");
    rm_db();

    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                               0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs), "open");

    float v[DIM]; fill_vec(v, 1.0f);
    ASSERT_OK(kvstore_vec_put(vs, "k1", 2, "val1", 4, v, 0, NULL, 0), "put");

    ASSERT_OK(kvstore_vec_drop_index(vs), "drop_index ok");
    ASSERT_EQ(kvstore_vec_count(vs), 0, "count=0 after drop");

    /* KV data still accessible */
    void *val = NULL; int nval = 0;
    ASSERT_OK(kvstore_vec_get(vs, "k1", 2, &val, &nval), "get after drop");
    ASSERT(val && memcmp(val, "val1", 4) == 0, "value survives drop");
    snkv_free(val);

    /* Search returns INDEX_DROPPED */
    KVVecSearchResult *res = NULL; int n = 0;
    int rc = kvstore_vec_search(vs, v, 1, 0, 1, 0.0f, &res, &n);
    ASSERT_EQ(rc, KVVEC_INDEX_DROPPED, "search → INDEX_DROPPED");

    kvstore_vec_close(vs);
    rm_db();
}

/* -----------------------------------------------------------------------
** T17: search on empty index → INDEX_EMPTY (not crash)
** ----------------------------------------------------------------------- */
static void test_search_empty(void) {
    section("T17: search empty index");
    rm_db();

    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                               0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs), "open");

    float q[DIM]; fill_vec(q, 1.0f);
    KVVecSearchResult *res = NULL; int n = 0;
    int rc = kvstore_vec_search(vs, q, 5, 0, 3, 0.0f, &res, &n);
    ASSERT_EQ(rc, KVVEC_INDEX_EMPTY, "empty → INDEX_EMPTY");
    ASSERT(res == NULL, "res NULL on empty");

    kvstore_vec_close(vs);
    rm_db();
}

/* -----------------------------------------------------------------------
** T18: stats — fill_ratio, vec_cf_count, has_metadata
** ----------------------------------------------------------------------- */
static void test_stats(void) {
    section("T18: stats");
    rm_db();

    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                               0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs), "open");

    float v[DIM]; fill_vec(v, 1.0f);
    kvstore_vec_put(vs, "k", 1, "v", 1, v, 0,
                    "{\"x\":1}", (int)strlen("{\"x\":1}"));

    KVVecStats st;
    ASSERT_OK(kvstore_vec_stats(vs, &st), "stats ok");
    ASSERT_EQ(st.count,         1,          "count=1");
    ASSERT_EQ(st.vec_cf_count,  1,          "vec_cf_count=1");
    ASSERT_EQ(st.has_metadata,  1,          "has_metadata=1");
    ASSERT_EQ(st.connectivity,  16,         "connectivity default=16");
    ASSERT_EQ(st.expansion_add, 128,        "expansion_add default=128");
    ASSERT_EQ(st.expansion_search, 64,      "expansion_search default=64");
    ASSERT(st.capacity > 0,                 "capacity > 0");
    ASSERT(st.fill_ratio >= 0.0,            "fill_ratio >= 0");

    kvstore_vec_close(vs);
    rm_db();
}

/* -----------------------------------------------------------------------
** T19: rebuild from CFs (no sidecar) — delete sidecar files manually
** ----------------------------------------------------------------------- */
static void test_rebuild_from_cfs(void) {
    section("T19: rebuild from CFs");
    rm_db();

    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                               0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs), "open");

    for (int i = 0; i < 5; i++) {
        float v[DIM]; fill_vec(v, (float)(i + 1));
        char key[4]; snprintf(key, sizeof(key), "k%d", i);
        kvstore_vec_put(vs, key, (int)strlen(key), "v", 1, v, 0, NULL, 0);
    }
    kvstore_vec_close(vs);

    /* Delete sidecar — force full CF rebuild */
    remove(DB ".usearch");
    remove(DB ".usearch.nid");

    vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                               0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs), "reopen no sidecar");
    ASSERT_EQ(kvstore_vec_count(vs), 5, "count=5 after CF rebuild");

    float q[DIM]; fill_vec(q, 1.0f);
    KVVecSearchResult *res = NULL; int n = 0;
    ASSERT_OK(kvstore_vec_search(vs, q, 1, 0, 1, 0.0f, &res, &n), "search after rebuild");
    ASSERT_EQ(n, 1, "one result");
    kvstore_vec_free_results(res, n);

    kvstore_vec_close(vs);
    rm_db();
}

/* -----------------------------------------------------------------------
** T20: encrypted store — sidecar disabled, opens with password
** ----------------------------------------------------------------------- */
static void test_encrypted(void) {
    section("T20: encrypted store");
    rm_db();

    const uint8_t pw[] = "testpassword";
    int npw = (int)strlen((char*)pw);

    KVVecStore *vs = NULL;
    int rc = kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                              0, 0, 0, KVVEC_DTYPE_F32, pw, npw, &vs);
    ASSERT_OK(rc, "open encrypted");
    if (rc != KVSTORE_OK) return;

    KVVecStats st;
    kvstore_vec_stats(vs, &st);
    ASSERT_EQ(st.sidecar_enabled, 0, "sidecar disabled for encrypted");

    float v[DIM]; fill_vec(v, 1.0f);
    ASSERT_OK(kvstore_vec_put(vs, "sec", 3, "secret", 6, v, 0, NULL, 0), "put encrypted");
    kvstore_vec_close(vs);

    /* Reopen with correct password */
    vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                               0, 0, 0, KVVEC_DTYPE_F32, pw, npw, &vs), "reopen encrypted");
    ASSERT_EQ(kvstore_vec_count(vs), 1, "count=1 after encrypted reopen");

    void *val = NULL; int nval = 0;
    ASSERT_OK(kvstore_vec_get(vs, "sec", 3, &val, &nval), "get from encrypted");
    ASSERT(val && memcmp(val, "secret", 6) == 0, "encrypted value correct");
    snkv_free(val);
    kvstore_vec_close(vs);

    /* Wrong password → AUTH_FAILED */
    vs = NULL;
    rc = kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                          0, 0, 0, KVVEC_DTYPE_F32,
                          (uint8_t*)"wrongpw", 7, &vs);
    ASSERT_EQ(rc, KVSTORE_AUTH_FAILED, "wrong password → AUTH_FAILED");
    ASSERT(vs == NULL, "handle NULL on auth fail");

    rm_db();
}

/* -----------------------------------------------------------------------
** T21: dtype F16 — store and search
** ----------------------------------------------------------------------- */
static void test_dtype_f16(void) {
    section("T21: dtype F16");
    rm_db();

    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                               0, 0, 0, KVVEC_DTYPE_F16, NULL, 0, &vs), "open F16");

    float v[DIM]; fill_vec(v, 1.0f);
    ASSERT_OK(kvstore_vec_put(vs, "k", 1, "v", 1, v, 0, NULL, 0), "put F16");
    ASSERT_EQ(kvstore_vec_count(vs), 1, "count=1");

    KVVecSearchResult *res = NULL; int n = 0;
    ASSERT_OK(kvstore_vec_search(vs, v, 1, 0, 1, 0.0f, &res, &n), "search F16");
    ASSERT_EQ(n, 1, "one result");
    kvstore_vec_free_results(res, n);

    kvstore_vec_close(vs);
    rm_db();
}

/* -----------------------------------------------------------------------
** T22: purge_expired on non-TTL store → 0 deleted, no crash
** ----------------------------------------------------------------------- */
static void test_purge_no_ttl(void) {
    section("T22: purge_expired no TTL");
    rm_db();

    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                               0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs), "open");

    float v[DIM]; fill_vec(v, 1.0f);
    kvstore_vec_put(vs, "k", 1, "v", 1, v, 0, NULL, 0);

    int ndel = -1;
    ASSERT_OK(kvstore_vec_purge_expired(vs, &ndel), "purge no TTL ok");
    ASSERT_EQ(ndel, 0, "0 deleted when no TTL");

    kvstore_vec_close(vs);
    rm_db();
}

/* -----------------------------------------------------------------------
** T23: double close safety (NULL guard)
** ----------------------------------------------------------------------- */
static void test_null_safety(void) {
    section("T23: NULL safety");
    kvstore_vec_close(NULL);  /* must not crash */
    ASSERT(1, "close(NULL) safe");

    KVVecSearchResult *r = NULL;
    kvstore_vec_free_results(NULL, 0);
    kvstore_vec_free_results(r, 0);
    ASSERT(1, "free_results(NULL) safe");

    kvstore_vec_free_key_results(NULL, 0);
    ASSERT(1, "free_key_results(NULL) safe");

    /* NULL pVec → KVVEC_BAD_VECTOR */
    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2,
                               0, 0, 0, KVVEC_DTYPE_F32, NULL, 0, &vs), "open for null vec test");
    int rc = kvstore_vec_put(vs, "k", 1, "v", 1, NULL, 0, NULL, 0);
    ASSERT_EQ(rc, KVVEC_BAD_VECTOR, "NULL pVec → BAD_VECTOR");

    KVVecItem items[1] = {{ "k", 1, "v", 1, NULL, NULL, 0 }};
    int rc2 = kvstore_vec_put_batch(vs, items, 1, 0);
    ASSERT_EQ(rc2, KVVEC_BAD_VECTOR, "NULL item.pVec → BAD_VECTOR");

    kvstore_vec_close(vs);
    rm_db();
}

/* -----------------------------------------------------------------------
** main
** ----------------------------------------------------------------------- */
int main(void) {
    printf("=== SNKV Vector Store Tests ===\n");

    test_open_close();
    test_put_get();
    test_overwrite();
    test_delete();
    test_search();
    test_search_keys();
    test_search_rerank();
    test_search_max_distance();
    test_metadata();
    test_batch();
    test_sidecar();
    test_dim_mismatch();
    test_space_mismatch();
    test_kv_put();
    test_ttl();
    test_drop_index();
    test_search_empty();
    test_stats();
    test_rebuild_from_cfs();
    test_encrypted();
    test_dtype_f16();
    test_purge_no_ttl();
    test_null_safety();

    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
