/* SPDX-License-Identifier: Apache-2.0 */
/* tests/test_vec.c — comprehensive tests for kvstore_vec */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <stdint.h>

#include "kvstore_vec.h"
#include "platform_compat.h"   /* pthread_create / pthread_join (T32) */

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
/* -----------------------------------------------------------------------
** T24: big values (overflow pages) — contains / search_keys /
** purge_expired only need "is the key live?", so they must not read the
** values.  SQLITE_STATUS_MALLOC_SIZE records the largest single SQLite
** allocation; it must stay far below the value size.
** ----------------------------------------------------------------------- */
#define BIGV      50000
#define NBIGV     30
static int  big_max_alloc(void) { int c = 0, h = 0; sqlite3_status(SQLITE_STATUS_MALLOC_SIZE, &c, &h, 0); return h; }
static void big_reset(void)     { int c = 0, h = 0; sqlite3_status(SQLITE_STATUS_MALLOC_SIZE, &c, &h, 1); }

static void test_big_values_liveness(void) {
    section("T24: big values — liveness checks do not read values");
    rm_db();
    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2, 0, 0, 0,
                               KVVEC_DTYPE_F32, NULL, 0, &vs), "open");
    unsigned char *val = (unsigned char *)malloc(BIGV);
    memset(val, 'b', BIGV);
    float v[DIM];
    char key[32];
    int putOk = 1;
    int64_t past = kvstore_now_ms() - 1000;
    for (int i = 0; i < NBIGV; i++) {
        int n = snprintf(key, sizeof key, "big:%03d", i);
        fill_vec(v, (float)i);
        /* odd keys are already expired */
        if (kvstore_vec_put(vs, key, n, val, BIGV, v, (i % 2) ? past : 0, NULL, 0) != KVSTORE_OK)
            putOk = 0;
    }
    ASSERT(putOk, "put 30 vectors with 50 KB values (15 already expired)");

    /* contains */
    big_reset();
    int cOk = 1;
    for (int i = 0; i < NBIGV; i++) {
        int n = snprintf(key, sizeof key, "big:%03d", i);
        if (kvstore_vec_contains(vs, key, n) != !(i % 2)) cOk = 0;
    }
    ASSERT(cOk, "contains: live keys 1, expired keys 0");
    ASSERT(kvstore_vec_contains(vs, "big:999", 7) == 0, "contains: missing key 0");
    printf("        largest allocation during contains: %d bytes\n", big_max_alloc());
    ASSERT(big_max_alloc() < 16 * 1024, "contains does not read values");

    /* search_keys: expired keys are skipped */
    big_reset();
    KVVecKeyResult *kr = NULL; int nkr = 0;
    fill_vec(v, 0.0f);
    ASSERT_OK(kvstore_vec_search_keys(vs, v, NBIGV, &kr, &nkr), "search_keys");
    int allLive = 1;
    for (int i = 0; i < nkr; i++) {
        /* keys are not NUL-terminated: copy before parsing */
        char kb[32] = {0};
        int nk = kr[i].nKey < (int)sizeof kb - 1 ? kr[i].nKey : (int)sizeof kb - 1;
        memcpy(kb, kr[i].pKey, nk);
        if (atoi(kb + 4) % 2) allLive = 0;
    }
    if (nkr != NBIGV / 2 || !allLive)
        printf("        search_keys returned %d keys, allLive=%d\n", nkr, allLive);
    ASSERT(nkr == NBIGV / 2 && allLive, "search_keys returns exactly the 15 live keys");
    kvstore_vec_free_key_results(kr, nkr);
    printf("        largest allocation during search_keys: %d bytes\n", big_max_alloc());
    ASSERT(big_max_alloc() < 16 * 1024, "search_keys does not read values");

    /* purge_expired removes the expired vectors (their values are already
    ** lazily deleted by the checks above; the vector rows remain) */
    big_reset();
    int nDel = -1;
    ASSERT_OK(kvstore_vec_purge_expired(vs, &nDel), "purge_expired");
    ASSERT_EQ(nDel, NBIGV / 2, "purge_expired removed 15 vectors");
    ASSERT_EQ(kvstore_vec_count(vs), (int64_t)(NBIGV / 2), "15 vectors left in the index");
    printf("        largest allocation during purge_expired: %d bytes\n", big_max_alloc());
    ASSERT(big_max_alloc() < 16 * 1024, "purge_expired does not read values");

    /* the live values are intact */
    void *pv = NULL; int npv = 0;
    ASSERT_OK(kvstore_vec_get(vs, "big:000", 7, &pv, &npv), "get a live big value");
    ASSERT(npv == BIGV && ((unsigned char *)pv)[BIGV - 1] == 'b', "live big value intact");
    snkv_free(pv);

    kvstore_vec_close(vs);
    free(val);
    rm_db();
}

/* -----------------------------------------------------------------------
** T25: drop_index removes the vector data from the file, not just memory.
** Bug: the internal CFs have reserved names, which kvstore_cf_drop rejects;
** the errors were ignored, so after a reopen every vector came back.
** ----------------------------------------------------------------------- */
static void test_drop_index_persists(void) {
    section("T25: drop_index survives a reopen");
    rm_db();
    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2, 0, 0, 0,
                               KVVEC_DTYPE_F32, NULL, 0, &vs), "open");
    float v[DIM];
    char key[16];
    for (int i = 0; i < 3; i++) {
        int n = snprintf(key, sizeof key, "d%d", i);
        fill_vec(v, (float)i);
        ASSERT_OK(kvstore_vec_put(vs, key, n, "val", 3, v, 0, "{\"a\":1}", 7), "put");
    }
    ASSERT_OK(kvstore_vec_drop_index(vs), "drop_index");
    kvstore_vec_close(vs);

    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2, 0, 0, 0,
                               KVVEC_DTYPE_F32, NULL, 0, &vs), "reopen");
    ASSERT_EQ(kvstore_vec_count(vs), 0, "no vectors after reopen");
    KVVecStats st;
    kvstore_vec_stats(vs, &st);
    ASSERT_EQ(st.vec_cf_count, 0, "vector CF is empty after reopen");
    ASSERT_EQ(st.has_metadata, 0, "metadata CF was dropped too");
    float *pv = NULL; int nf = 0;
    ASSERT_EQ(kvstore_vec_get_vector(vs, "d0", 2, &pv, &nf), KVSTORE_NOTFOUND,
              "stored vector is gone");
    snkv_free(pv);
    void *val = NULL; int nval = 0;
    ASSERT_OK(kvstore_vec_get(vs, "d0", 2, &val, &nval), "value kept");
    snkv_free(val);

    /* The store is usable again: a new vector goes in and is found. */
    fill_vec(v, 9.0f);
    ASSERT_OK(kvstore_vec_put(vs, "new", 3, "nv", 2, v, 0, NULL, 0), "put after drop+reopen");
    ASSERT_EQ(kvstore_vec_count(vs), 1, "count 1");
    kvstore_vec_close(vs);
    rm_db();
}

/* -----------------------------------------------------------------------
** T26: a plain vector store reopened with a password stays readable.
** Bug: the plain->encrypted conversion skipped every "__snkv_" CF, so the
** vector CFs stayed plaintext while encryption was on and every read of
** them failed (AUTH_FAILED / empty index).
** ----------------------------------------------------------------------- */
static void test_plain_to_encrypted(void) {
    section("T26: plain vector store reopened with a password");
    rm_db();
    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, 16, KVVEC_SPACE_L2, 0, 0, 0,
                               KVVEC_DTYPE_F32, NULL, 0, &vs), "open plain");
    float v[16];
    char key[16];
    for (int i = 0; i < 3; i++) {
        for (int j = 0; j < 16; j++) v[j] = (float)(i * 16 + j);
        int n = snprintf(key, sizeof key, "p%d", i);
        ASSERT_OK(kvstore_vec_put(vs, key, n, "val", 3, v, 0, "{\"m\":1}", 7), "put");
    }
    kvstore_vec_close(vs);
    remove(DB ".usearch"); remove(DB ".usearch.nid");   /* force a rebuild */

    const uint8_t pw[] = "secret";
    ASSERT_OK(kvstore_vec_open(DB, 16, KVVEC_SPACE_L2, 0, 0, 0,
                               KVVEC_DTYPE_F32, pw, 6, &vs), "reopen with password");
    ASSERT_EQ(kvstore_vec_count(vs), 3, "index rebuilt with 3 vectors");
    float *pv = NULL; int nf = 0;
    ASSERT_OK(kvstore_vec_get_vector(vs, "p1", 2, &pv, &nf), "get_vector after encryption");
    ASSERT(nf == 16 && pv && pv[0] == 16.0f, "vector content intact");
    snkv_free(pv);
    void *meta = NULL; int nmeta = 0;
    ASSERT_OK(kvstore_vec_get_metadata(vs, "p2", 2, &meta, &nmeta), "metadata after encryption");
    ASSERT(nmeta == 7 && meta && memcmp(meta, "{\"m\":1}", 7) == 0, "metadata intact");
    snkv_free(meta);
    for (int j = 0; j < 16; j++) v[j] = (float)(16 + j);
    KVVecSearchResult *res = NULL; int nres = 0;
    ASSERT_OK(kvstore_vec_search(vs, v, 1, 0, 0, 0.0f, &res, &nres), "search");
    ASSERT(nres == 1 && res[0].nKey == 2 && memcmp(res[0].pKey, "p1", 2) == 0,
           "nearest is p1");
    kvstore_vec_free_results(res, nres);
    kvstore_vec_close(vs);

    /* And it opens again with the password (already encrypted). */
    ASSERT_OK(kvstore_vec_open(DB, 16, KVVEC_SPACE_L2, 0, 0, 0,
                               KVVEC_DTYPE_F32, pw, 6, &vs), "second encrypted open");
    ASSERT_EQ(kvstore_vec_count(vs), 3, "still 3 vectors");
    kvstore_vec_close(vs);
    rm_db();
}

/* -----------------------------------------------------------------------
** T27: stored metadata is visible after a reopen.
** Bug: rebuildIndex opened the tags CF with kvstore_cf_open, which rejects
** reserved names, so get_metadata returned "no metadata" after a reopen.
** ----------------------------------------------------------------------- */
static void test_metadata_after_reopen(void) {
    section("T27: metadata visible after reopen");
    rm_db();
    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2, 0, 0, 0,
                               KVVEC_DTYPE_F32, NULL, 0, &vs), "open");
    float v[DIM]; fill_vec(v, 1.0f);
    ASSERT_OK(kvstore_vec_put(vs, "m1", 2, "val", 3, v, 0, "{\"tag\":\"x\"}", 11), "put with metadata");
    kvstore_vec_close(vs);

    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2, 0, 0, 0,
                               KVVEC_DTYPE_F32, NULL, 0, &vs), "reopen");
    void *meta = NULL; int nmeta = 0;
    ASSERT_OK(kvstore_vec_get_metadata(vs, "m1", 2, &meta, &nmeta), "get_metadata");
    ASSERT(nmeta == 11 && meta && memcmp(meta, "{\"tag\":\"x\"}", 11) == 0,
           "metadata returned after reopen");
    snkv_free(meta);
    KVVecStats st; kvstore_vec_stats(vs, &st);
    ASSERT_EQ(st.has_metadata, 1, "stats report metadata");
    kvstore_vec_close(vs);
    rm_db();
}

/* -----------------------------------------------------------------------
** T28: a batch that repeats a key adds only its last vector to the index
** Bug: every item's label was added to the in-memory HNSW index, so the
** superseded label stayed there: count was too high and the stale label
** had no key mapping.
** ----------------------------------------------------------------------- */
static void test_batch_duplicate_keys(void) {
    section("T28: put_batch with a repeated key");
    rm_db();
    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2, 0, 0, 0,
                               KVVEC_DTYPE_F32, NULL, 0, &vs), "open");
    float a[DIM], b1[DIM], b2[DIM], b3[DIM];
    fill_vec(a, 1.0f); fill_vec(b1, 50.0f); fill_vec(b2, 60.0f); fill_vec(b3, 2.0f);
    KVVecItem items[4] = {
        { "a", 1, "va",  2, a,  NULL, 0 },
        { "b", 1, "vb1", 3, b1, NULL, 0 },
        { "b", 1, "vb2", 3, b2, NULL, 0 },
        { "b", 1, "vb3", 3, b3, NULL, 0 },
    };
    ASSERT_OK(kvstore_vec_put_batch(vs, items, 4, 0), "batch with key b three times");
    ASSERT_EQ(kvstore_vec_count(vs), 2, "index holds 2 vectors (a, b)");

    float *got = NULL; int n = 0;
    ASSERT_OK(kvstore_vec_get_vector(vs, "b", 1, &got, &n), "get_vector b");
    ASSERT(n == DIM && got && got[0] == b3[0], "b has the last batch vector");
    snkv_free(got);

    /* Query at b1: the superseded vector must not come back as a hit. */
    KVVecSearchResult *res = NULL; int nRes = 0;
    ASSERT_OK(kvstore_vec_search(vs, b1, 5, 0, 0, 0.0f, &res, &nRes), "search");
    ASSERT_EQ(nRes, 2, "search sees exactly 2 vectors");
    kvstore_vec_free_results(res, nRes);

    /* Same again after a full rebuild (no sidecar). */
    kvstore_vec_close(vs);
    remove(DB ".usearch"); remove(DB ".usearch.nid");
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2, 0, 0, 0,
                               KVVEC_DTYPE_F32, NULL, 0, &vs), "reopen");
    ASSERT_EQ(kvstore_vec_count(vs), 2, "count 2 after rebuild");
    kvstore_vec_close(vs);
    rm_db();
}

/* Copy a file byte for byte; returns 0 on success. */
static int copy_file(const char *from, const char *to) {
    FILE *in = fopen(from, "rb");
    if (!in) return -1;
    FILE *out = fopen(to, "wb");
    if (!out) { fclose(in); return -1; }
    char buf[4096]; size_t n;
    while ((n = fread(buf, 1, sizeof buf, in)) > 0) fwrite(buf, 1, n, out);
    fclose(in); fclose(out);
    return 0;
}

static int file_exists(const char *p) {
    FILE *f = fopen(p, "rb");
    if (f) { fclose(f); return 1; }
    return 0;
}

/* -----------------------------------------------------------------------
** T29: a stale sidecar is not loaded
** Bug: the sidecar was trusted whenever its next_id matched.  A delete does
** not change next_id, so a sidecar left by an earlier session (e.g. the
** process died before close rewrote it) brought deleted vectors back.
** ----------------------------------------------------------------------- */
static void test_stale_sidecar(void) {
    section("T29: stale sidecar after a delete");
    rm_db();
    remove(DB ".old"); remove(DB ".old.nid");
    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2, 0, 0, 0,
                               KVVEC_DTYPE_F32, NULL, 0, &vs), "open");
    float v[DIM];
    fill_vec(v, 1.0f);  kvstore_vec_put(vs, "a", 1, "1", 1, v, 0, NULL, 0);
    fill_vec(v, 20.0f); kvstore_vec_put(vs, "b", 1, "2", 1, v, 0, NULL, 0);
    fill_vec(v, 40.0f); kvstore_vec_put(vs, "c", 1, "3", 1, v, 0, NULL, 0);
    kvstore_vec_close(vs);                         /* sidecar: next_id 3, 3 vectors */
    ASSERT(copy_file(DB ".usearch", DB ".old") == 0 &&
           copy_file(DB ".usearch.nid", DB ".old.nid") == 0, "saved sidecar copy");

    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2, 0, 0, 0,
                               KVVEC_DTYPE_F32, NULL, 0, &vs), "reopen (loads sidecar)");
    ASSERT(!file_exists(DB ".usearch"), "sidecar removed while the store is open");
    ASSERT_OK(kvstore_vec_delete(vs, "b", 1), "delete b");
    kvstore_vec_close(vs);
    ASSERT(file_exists(DB ".usearch"), "close writes the sidecar again");

    /* Put the old sidecar back, as if the second close never happened. */
    ASSERT(copy_file(DB ".old", DB ".usearch") == 0 &&
           copy_file(DB ".old.nid", DB ".usearch.nid") == 0, "restore stale sidecar");
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2, 0, 0, 0,
                               KVVEC_DTYPE_F32, NULL, 0, &vs), "reopen with stale sidecar");
    ASSERT_EQ(kvstore_vec_count(vs), 2, "deleted vector not resurrected (count 2)");
    fill_vec(v, 20.0f);
    KVVecSearchResult *res = NULL; int nRes = 0;
    ASSERT_OK(kvstore_vec_search(vs, v, 3, 0, 0, 0.0f, &res, &nRes), "search near b");
    int sawB = 0;
    for (int i = 0; i < nRes; i++)
        if (res[i].nKey == 1 && memcmp(res[i].pKey, "b", 1) == 0) sawB = 1;
    ASSERT(nRes == 2 && !sawB, "search does not return deleted b");
    kvstore_vec_free_results(res, nRes);
    kvstore_vec_close(vs);

    /* An encrypted open must remove a sidecar left by a plain session. */
    ASSERT(file_exists(DB ".usearch"), "plain close left a sidecar");
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2, 0, 0, 0,
                               KVVEC_DTYPE_F32, (const uint8_t*)"pw", 2, &vs),
              "encrypted open");
    ASSERT(!file_exists(DB ".usearch") && !file_exists(DB ".usearch.nid"),
           "encrypted open removed the plain sidecar");
    kvstore_vec_close(vs);
    remove(DB ".old"); remove(DB ".old.nid");
    rm_db();
}

/* -----------------------------------------------------------------------
** T30: get_vector / get_metadata respect TTL
** Bug: both read the hidden CFs directly, so an expired key still returned
** its vector and metadata although kvstore_vec_get said NOTFOUND.
** ----------------------------------------------------------------------- */
static void test_get_vector_ttl(void) {
    section("T30: get_vector / get_metadata of an expired key");
    rm_db();
    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2, 0, 0, 0,
                               KVVEC_DTYPE_F32, NULL, 0, &vs), "open");
    float v[DIM]; fill_vec(v, 1.0f);
    int64_t past = kvstore_now_ms() - 5000;
    ASSERT_OK(kvstore_vec_put(vs, "old", 3, "v", 1, v, past, "{\"t\":1}", 7), "put expired");
    ASSERT_OK(kvstore_vec_put(vs, "new", 3, "v", 1, v, 0, "{\"t\":2}", 7), "put live");

    float *got = NULL; int n = 0;
    ASSERT_EQ(kvstore_vec_get_vector(vs, "old", 3, &got, &n), KVSTORE_NOTFOUND,
              "get_vector(expired) -> NOTFOUND");
    ASSERT(got == NULL, "no vector returned");
    void *meta = NULL; int nm = 0;
    ASSERT_EQ(kvstore_vec_get_metadata(vs, "old", 3, &meta, &nm), KVSTORE_NOTFOUND,
              "get_metadata(expired) -> NOTFOUND");
    ASSERT(meta == NULL && nm == 0, "no metadata returned");

    ASSERT_OK(kvstore_vec_get_vector(vs, "new", 3, &got, &n), "get_vector(live) ok");
    snkv_free(got);
    ASSERT_OK(kvstore_vec_get_metadata(vs, "new", 3, &meta, &nm), "get_metadata(live) ok");
    ASSERT(nm == 7, "live metadata returned");
    snkv_free(meta);
    kvstore_vec_close(vs);
    rm_db();
}

/* -----------------------------------------------------------------------
** T31: purge_expired works in fixed-size batches
** Before, every expired key was collected in one growing array before
** anything was deleted (memory proportional to the number expired).  Now
** it deletes KVVEC_PURGE_BATCH (64) at a time and resumes after the last
** deleted key; this checks that more than two batches, with live keys
** interleaved, are all handled.
** ----------------------------------------------------------------------- */
static void test_purge_batches(void) {
    section("T31: purge_expired across several batches");
    rm_db();
    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2, 0, 0, 0,
                               KVVEC_DTYPE_F32, NULL, 0, &vs), "open");
    int64_t past = kvstore_now_ms() - 5000;
    char key[16];
    float v[DIM];
    int nOk = 0;
    for (int i = 0; i < 200; i++) {
        snprintf(key, sizeof key, "p%03d", i);
        fill_vec(v, (float)i);
        /* every 10th key stays live */
        int64_t exp = (i % 10 == 0) ? 0 : past;
        if (kvstore_vec_put(vs, key, (int)strlen(key), "v", 1, v, exp, NULL, 0) == KVSTORE_OK) nOk++;
    }
    ASSERT_EQ(nOk, 200, "200 puts (180 already expired)");
    int ndel = 0;
    ASSERT_OK(kvstore_vec_purge_expired(vs, &ndel), "purge_expired");
    ASSERT_EQ(ndel, 180, "all 180 expired items purged (3 batches)");
    ASSERT_EQ(kvstore_vec_count(vs), 20, "20 live vectors left in the index");
    KVVecStats st; kvstore_vec_stats(vs, &st);
    ASSERT_EQ(st.vec_cf_count, 20, "20 rows left in the vector CF");
    float *got = NULL; int n = 0;
    ASSERT_OK(kvstore_vec_get_vector(vs, "p190", 4, &got, &n), "live key still has its vector");
    snkv_free(got);
    ASSERT_OK(kvstore_vec_purge_expired(vs, &ndel), "second purge");
    ASSERT_EQ(ndel, 0, "nothing left to purge");

    /* Search with rerank still returns values (read only for the results). */
    fill_vec(v, 190.0f);
    KVVecSearchResult *res = NULL; int nRes = 0;
    ASSERT_OK(kvstore_vec_search(vs, v, 3, 1, 3, 0.0f, &res, &nRes), "rerank search");
    ASSERT(nRes == 3 && res[0].nKey == 4 && memcmp(res[0].pKey, "p190", 4) == 0
           && res[0].nValue == 1 && memcmp(res[0].pValue, "v", 1) == 0,
           "nearest is p190 with its value");
    kvstore_vec_free_results(res, nRes);
    kvstore_vec_close(vs);
    rm_db();
}

/* -----------------------------------------------------------------------
** T32: one KVVecStore shared by several threads.  get / get_vector /
** contains / kv_put used to skip the store mutex and could touch the
** B-tree at the same time as a put on another thread.
** ----------------------------------------------------------------------- */
#define T32_THREADS 4
#define T32_KEYS    60

typedef struct { KVVecStore *vs; int id; int errors; } T32Arg;

static void *t32_worker(void *arg) {
    T32Arg *a = (T32Arg*)arg;
    char key[32]; float v[DIM];
    for (int i = 0; i < T32_KEYS; i++) {
        snprintf(key, sizeof key, "t%d_%03d", a->id, i);
        fill_vec(v, (float)(a->id * 1000 + i));
        if (kvstore_vec_put(a->vs, key, (int)strlen(key), "val", 3, v, 0, NULL, 0) != KVSTORE_OK) a->errors++;
        void *pv = NULL; int nv = 0;
        if (kvstore_vec_get(a->vs, key, (int)strlen(key), &pv, &nv) != KVSTORE_OK || nv != 3) a->errors++;
        snkv_free(pv);
        float *pf = NULL; int nf = 0;
        if (kvstore_vec_get_vector(a->vs, key, (int)strlen(key), &pf, &nf) != KVSTORE_OK || nf != DIM) a->errors++;
        snkv_free(pf);
        if (!kvstore_vec_contains(a->vs, key, (int)strlen(key))) a->errors++;
        if (kvstore_vec_kv_put(a->vs, "plain", 5, "p", 1) != KVSTORE_OK) a->errors++;
        KVVecSearchResult *res = NULL; int nRes = 0;
        int rc = kvstore_vec_search(a->vs, v, 2, (i & 1), 3, 0.0f, &res, &nRes);
        if (rc != KVSTORE_OK && rc != KVVEC_INDEX_EMPTY) a->errors++;
        kvstore_vec_free_results(res, nRes);
        (void)kvstore_vec_count(a->vs);
    }
    for (int i = 0; i < T32_KEYS; i += 2) {             /* delete every other key */
        snprintf(key, sizeof key, "t%d_%03d", a->id, i);
        if (kvstore_vec_delete(a->vs, key, (int)strlen(key)) != KVSTORE_OK) a->errors++;
    }
    return NULL;
}

static void test_shared_threads(void) {
    section("T32: one vector store shared by several threads");
    rm_db();
    KVVecStore *vs = NULL;
    ASSERT_OK(kvstore_vec_open(DB, DIM, KVVEC_SPACE_L2, 0, 0, 0,
                               KVVEC_DTYPE_F32, NULL, 0, &vs), "open");
    pthread_t th[T32_THREADS]; T32Arg args[T32_THREADS];
    for (int i = 0; i < T32_THREADS; i++) {
        args[i].vs = vs; args[i].id = i; args[i].errors = 0;
        pthread_create(&th[i], NULL, t32_worker, &args[i]);
    }
    int errors = 0;
    for (int i = 0; i < T32_THREADS; i++) { pthread_join(th[i], NULL); errors += args[i].errors; }
    ASSERT_EQ(errors, 0, "no errors from put/get/get_vector/contains/kv_put/search/delete");
    ASSERT_EQ(kvstore_vec_count(vs), T32_THREADS * T32_KEYS / 2, "index holds exactly the undeleted vectors");
    KVVecStats st; kvstore_vec_stats(vs, &st);
    ASSERT_EQ(st.vec_cf_count, T32_THREADS * T32_KEYS / 2, "vector CF agrees with the index");
    ASSERT(!kvstore_vec_contains(vs, "t2_010", 6) && kvstore_vec_contains(vs, "t2_011", 6),
           "deleted key gone, kept key present");
    kvstore_vec_close(vs);
    rm_db();
}

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
    test_big_values_liveness();
    test_drop_index_persists();
    test_plain_to_encrypted();
    test_metadata_after_reopen();
    test_batch_duplicate_keys();
    test_stale_sidecar();
    test_get_vector_ttl();
    test_purge_batches();
    test_shared_threads();

    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
