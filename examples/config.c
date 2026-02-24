/* SPDX-License-Identifier: Apache-2.0 */
/*
** Configuration Examples
** Demonstrates: keyvaluestore_open_v2 with KeyValueStoreConfig options
*/

#define SNKV_IMPLEMENTATION
#include "snkv.h"
#include <stdio.h>
#include <string.h>

/* ------------------------------------------------------------------ */
static void example_default_config(void)
{
    printf("=== Default Config (NULL) ===\n");

    KeyValueStore *db;
    /* NULL config -- uses WAL, SYNC_NORMAL, 2000-page cache */
    int rc = keyvaluestore_open_v2("cfg_default.db", &db, NULL);
    printf("  keyvaluestore_open_v2(NULL): %s\n", rc == KEYVALUESTORE_OK ? "OK" : "FAIL");

    keyvaluestore_put(db, "hello", 5, "world", 5);

    void *val; int len;
    keyvaluestore_get(db, "hello", 5, &val, &len);
    printf("  get 'hello' -> '%.*s'\n", len, (char *)val);
    snkv_free(val);

    keyvaluestore_close(db);
    remove("cfg_default.db");
    printf("\n");
}

/* ------------------------------------------------------------------ */
static void example_explicit_wal(void)
{
    printf("=== Explicit WAL + SYNC_NORMAL ===\n");

    KeyValueStoreConfig cfg = {0};
    cfg.journalMode = KEYVALUESTORE_JOURNAL_WAL;
    cfg.syncLevel   = KEYVALUESTORE_SYNC_NORMAL;

    KeyValueStore *db;
    int rc = keyvaluestore_open_v2("cfg_wal.db", &db, &cfg);
    printf("  open: %s\n", rc == KEYVALUESTORE_OK ? "OK" : "FAIL");

    keyvaluestore_put(db, "key", 3, "val", 3);

    void *val; int len;
    keyvaluestore_get(db, "key", 3, &val, &len);
    printf("  get 'key' -> '%.*s'\n", len, (char *)val);
    snkv_free(val);

    keyvaluestore_close(db);
    remove("cfg_wal.db");
    printf("\n");
}

/* ------------------------------------------------------------------ */
static void example_large_cache(void)
{
    printf("=== Large Cache (4000 pages ~16 MB) ===\n");

    KeyValueStoreConfig cfg = {0};
    cfg.journalMode = KEYVALUESTORE_JOURNAL_WAL;
    cfg.cacheSize   = 4000;   /* ~16 MB with 4096-byte pages */

    KeyValueStore *db;
    int rc = keyvaluestore_open_v2("cfg_cache.db", &db, &cfg);
    printf("  open: %s\n", rc == KEYVALUESTORE_OK ? "OK" : "FAIL");

    /* Insert a batch of records */
    int i;
    for (i = 0; i < 1000; i++) {
        char key[32], val[32];
        int  nk = snprintf(key, sizeof(key), "key_%04d", i);
        int  nv = snprintf(val, sizeof(val), "val_%04d", i);
        keyvaluestore_put(db, key, nk, val, nv);
    }

    /* Verify a few */
    void *v; int n;
    keyvaluestore_get(db, "key_0000", 8, &v, &n);
    printf("  key_0000 -> '%.*s'\n", n, (char *)v);
    snkv_free(v);

    keyvaluestore_get(db, "key_0999", 8, &v, &n);
    printf("  key_0999 -> '%.*s'\n", n, (char *)v);
    snkv_free(v);

    keyvaluestore_close(db);
    remove("cfg_cache.db");
    printf("\n");
}

/* ------------------------------------------------------------------ */
static void example_full_sync(void)
{
    printf("=== Power-safe: SYNC_FULL ===\n");

    KeyValueStoreConfig cfg = {0};
    cfg.journalMode = KEYVALUESTORE_JOURNAL_WAL;
    cfg.syncLevel   = KEYVALUESTORE_SYNC_FULL;   /* fsync on every commit */

    KeyValueStore *db;
    int rc = keyvaluestore_open_v2("cfg_full.db", &db, &cfg);
    printf("  open: %s\n", rc == KEYVALUESTORE_OK ? "OK" : "FAIL");

    keyvaluestore_put(db, "safe", 4, "data", 4);

    void *val; int len;
    keyvaluestore_get(db, "safe", 4, &val, &len);
    printf("  get 'safe' -> '%.*s'\n", len, (char *)val);
    snkv_free(val);

    keyvaluestore_close(db);
    remove("cfg_full.db");
    printf("\n");
}

/* ------------------------------------------------------------------ */
static void example_read_only(void)
{
    printf("=== Read-Only Open ===\n");

    /* First create a database with some data */
    {
        KeyValueStore *db;
        keyvaluestore_open("cfg_ro.db", &db, KEYVALUESTORE_JOURNAL_WAL);
        keyvaluestore_put(db, "info", 4, "snkv", 4);
        keyvaluestore_close(db);
    }

    /* Now open it read-only */
    KeyValueStoreConfig ro = {0};
    ro.readOnly = 1;

    KeyValueStore *db;
    int rc = keyvaluestore_open_v2("cfg_ro.db", &db, &ro);
    printf("  read-only open: %s\n", rc == KEYVALUESTORE_OK ? "OK" : "FAIL");

    void *val; int len;
    rc = keyvaluestore_get(db, "info", 4, &val, &len);
    printf("  get 'info' -> '%.*s'\n", len, (char *)val);
    snkv_free(val);

    /* Write attempt must fail */
    rc = keyvaluestore_put(db, "new", 3, "x", 1);
    printf("  put on read-only db: %s (expected FAIL)\n",
           rc != KEYVALUESTORE_OK ? "correctly rejected" : "UNEXPECTED OK");

    keyvaluestore_close(db);
    remove("cfg_ro.db");
    remove("cfg_ro.db-wal");
    remove("cfg_ro.db-shm");
    printf("\n");
}

/* ------------------------------------------------------------------ */
static void example_delete_journal(void)
{
    printf("=== Rollback Journal Mode ===\n");

    KeyValueStoreConfig cfg = {0};
    cfg.journalMode = KEYVALUESTORE_JOURNAL_DELETE;

    KeyValueStore *db;
    int rc = keyvaluestore_open_v2("cfg_delete.db", &db, &cfg);
    printf("  open: %s\n", rc == KEYVALUESTORE_OK ? "OK" : "FAIL");

    keyvaluestore_put(db, "k", 1, "v", 1);

    void *val; int len;
    keyvaluestore_get(db, "k", 1, &val, &len);
    printf("  get 'k' -> '%.*s'\n", len, (char *)val);
    snkv_free(val);

    keyvaluestore_close(db);
    remove("cfg_delete.db");
    printf("\n");
}

/* ------------------------------------------------------------------ */
int main(void)
{
    example_default_config();
    example_explicit_wal();
    example_large_cache();
    example_full_sync();
    example_read_only();
    example_delete_journal();
    printf("All config examples passed.\n");
    return 0;
}
