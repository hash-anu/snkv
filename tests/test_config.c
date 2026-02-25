/* SPDX-License-Identifier: Apache-2.0 */
/*
** test_config.c -- Tests for keyvaluestore_open_v2 / KeyValueStoreConfig
**
** Verifies:
**   1. NULL config (all defaults)
**   2. Explicit WAL + SYNC_NORMAL
**   3. SYNC_OFF
**   4. SYNC_FULL
**   5. Custom page size (new DB)
**   6. Custom cache size
**   7. Read-only open: reads succeed, writes rejected
**   8. Read-only open of empty DB is rejected
**   9. DELETE journal mode
**  10. busyTimeout field stored (open succeeds)
**  11. keyvaluestore_open backward-compatibility (delegates to open_v2)
*/

#include "keyvaluestore.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* ------------------------------------------------------------------ */
static int passed = 0;
static int failed = 0;

#define CHECK(cond, msg) do {                                   \
    if (cond) {                                                 \
        printf("  [PASS] %s\n", msg);                          \
        passed++;                                               \
    } else {                                                    \
        printf("  [FAIL] %s\n", msg);                          \
        failed++;                                               \
    }                                                           \
} while(0)

/* ------------------------------------------------------------------ */
static void test_null_config(void)
{
    printf("\n--- Test 1: NULL config (all defaults) ---\n");
    KeyValueStore *db = NULL;
    int rc = keyvaluestore_open_v2("tc_null.db", &db, NULL);
    CHECK(rc == KEYVALUESTORE_OK && db != NULL, "open with NULL config");

    if (db) {
        rc = keyvaluestore_put(db, "k", 1, "v", 1);
        CHECK(rc == KEYVALUESTORE_OK, "put succeeds");

        void *val; int len;
        rc = keyvaluestore_get(db, "k", 1, &val, &len);
        CHECK(rc == KEYVALUESTORE_OK && len == 1 && *(char*)val == 'v', "get returns correct value");
        if (rc == KEYVALUESTORE_OK) sqliteFree(val);

        keyvaluestore_close(db);
    }
    remove("tc_null.db");
}

/* ------------------------------------------------------------------ */
static void test_wal_normal(void)
{
    printf("\n--- Test 2: Explicit WAL + SYNC_NORMAL ---\n");
    KeyValueStoreConfig cfg = {0};
    cfg.journalMode = KEYVALUESTORE_JOURNAL_WAL;
    cfg.syncLevel   = KEYVALUESTORE_SYNC_NORMAL;

    KeyValueStore *db = NULL;
    int rc = keyvaluestore_open_v2("tc_wal.db", &db, &cfg);
    CHECK(rc == KEYVALUESTORE_OK && db != NULL, "open WAL+NORMAL");

    if (db) {
        rc = keyvaluestore_put(db, "key", 3, "value", 5);
        CHECK(rc == KEYVALUESTORE_OK, "put OK");

        void *val; int len;
        rc = keyvaluestore_get(db, "key", 3, &val, &len);
        CHECK(rc == KEYVALUESTORE_OK && len == 5, "get OK");
        if (rc == KEYVALUESTORE_OK) sqliteFree(val);

        keyvaluestore_close(db);
    }
    remove("tc_wal.db");
}

/* ------------------------------------------------------------------ */
static void test_sync_off(void)
{
    printf("\n--- Test 3: SYNC_OFF ---\n");
    KeyValueStoreConfig cfg = {0};
    cfg.journalMode = KEYVALUESTORE_JOURNAL_WAL;
    cfg.syncLevel   = KEYVALUESTORE_SYNC_OFF;

    KeyValueStore *db = NULL;
    int rc = keyvaluestore_open_v2("tc_off.db", &db, &cfg);
    CHECK(rc == KEYVALUESTORE_OK && db != NULL, "open SYNC_OFF");

    if (db) {
        rc = keyvaluestore_put(db, "a", 1, "b", 1);
        CHECK(rc == KEYVALUESTORE_OK, "put OK");

        void *val; int len;
        rc = keyvaluestore_get(db, "a", 1, &val, &len);
        CHECK(rc == KEYVALUESTORE_OK, "get OK");
        if (rc == KEYVALUESTORE_OK) sqliteFree(val);

        keyvaluestore_close(db);
    }
    remove("tc_off.db");
}

/* ------------------------------------------------------------------ */
static void test_sync_full(void)
{
    printf("\n--- Test 4: SYNC_FULL ---\n");
    KeyValueStoreConfig cfg = {0};
    cfg.journalMode = KEYVALUESTORE_JOURNAL_WAL;
    cfg.syncLevel   = KEYVALUESTORE_SYNC_FULL;

    KeyValueStore *db = NULL;
    int rc = keyvaluestore_open_v2("tc_full.db", &db, &cfg);
    CHECK(rc == KEYVALUESTORE_OK && db != NULL, "open SYNC_FULL");

    if (db) {
        rc = keyvaluestore_put(db, "safe", 4, "data", 4);
        CHECK(rc == KEYVALUESTORE_OK, "put OK");

        void *val; int len;
        rc = keyvaluestore_get(db, "safe", 4, &val, &len);
        CHECK(rc == KEYVALUESTORE_OK && len == 4, "get OK");
        if (rc == KEYVALUESTORE_OK) sqliteFree(val);

        keyvaluestore_close(db);
    }
    remove("tc_full.db");
}

/* ------------------------------------------------------------------ */
static void test_custom_page_size(void)
{
    printf("\n--- Test 5: Custom page size (8192) for new DB ---\n");
    KeyValueStoreConfig cfg = {0};
    cfg.journalMode = KEYVALUESTORE_JOURNAL_WAL;
    cfg.pageSize    = 8192;

    KeyValueStore *db = NULL;
    int rc = keyvaluestore_open_v2("tc_page.db", &db, &cfg);
    CHECK(rc == KEYVALUESTORE_OK && db != NULL, "open with pageSize=8192");

    if (db) {
        rc = keyvaluestore_put(db, "pg", 2, "ok", 2);
        CHECK(rc == KEYVALUESTORE_OK, "put OK");

        void *val; int len;
        rc = keyvaluestore_get(db, "pg", 2, &val, &len);
        CHECK(rc == KEYVALUESTORE_OK, "get OK");
        if (rc == KEYVALUESTORE_OK) sqliteFree(val);

        keyvaluestore_close(db);
    }
    remove("tc_page.db");
}

/* ------------------------------------------------------------------ */
static void test_custom_cache_size(void)
{
    printf("\n--- Test 6: Custom cache size (500 pages) ---\n");
    KeyValueStoreConfig cfg = {0};
    cfg.journalMode = KEYVALUESTORE_JOURNAL_WAL;
    cfg.cacheSize   = 500;

    KeyValueStore *db = NULL;
    int rc = keyvaluestore_open_v2("tc_cache.db", &db, &cfg);
    CHECK(rc == KEYVALUESTORE_OK && db != NULL, "open with cacheSize=500");

    if (db) {
        int i;
        for (i = 0; i < 200; i++) {
            char key[16]; int nk = snprintf(key, sizeof(key), "k%d", i);
            keyvaluestore_put(db, key, nk, "v", 1);
        }
        void *val; int len;
        rc = keyvaluestore_get(db, "k0", 2, &val, &len);
        CHECK(rc == KEYVALUESTORE_OK, "get after 200 puts OK");
        if (rc == KEYVALUESTORE_OK) sqliteFree(val);

        keyvaluestore_close(db);
    }
    remove("tc_cache.db");
}

/* ------------------------------------------------------------------ */
static void test_read_only(void)
{
    printf("\n--- Test 7: Read-only open ---\n");

    /* Create DB first */
    {
        KeyValueStore *db;
        keyvaluestore_open("tc_ro.db", &db, KEYVALUESTORE_JOURNAL_WAL);
        keyvaluestore_put(db, "ro_key", 6, "ro_val", 6);
        keyvaluestore_close(db);
    }

    KeyValueStoreConfig ro = {0};
    ro.readOnly = 1;

    KeyValueStore *db = NULL;
    int rc = keyvaluestore_open_v2("tc_ro.db", &db, &ro);
    CHECK(rc == KEYVALUESTORE_OK && db != NULL, "read-only open succeeds");

    if (db) {
        void *val; int len;
        rc = keyvaluestore_get(db, "ro_key", 6, &val, &len);
        CHECK(rc == KEYVALUESTORE_OK && len == 6, "read succeeds");
        if (rc == KEYVALUESTORE_OK) sqliteFree(val);

        /* Write attempt must fail */
        rc = keyvaluestore_put(db, "new", 3, "x", 1);
        CHECK(rc != KEYVALUESTORE_OK, "write correctly rejected on read-only DB");

        keyvaluestore_close(db);
    }
    remove("tc_ro.db");
    remove("tc_ro.db-wal");
    remove("tc_ro.db-shm");
}

/* ------------------------------------------------------------------ */
static void test_read_only_empty_db(void)
{
    printf("\n--- Test 8: Read-only open of empty/non-existent DB fails ---\n");
    remove("tc_ro_empty.db");   /* make sure it doesn't exist */

    KeyValueStoreConfig ro = {0};
    ro.readOnly = 1;

    KeyValueStore *db = NULL;
    int rc = keyvaluestore_open_v2("tc_ro_empty.db", &db, &ro);
    CHECK(rc != KEYVALUESTORE_OK, "opening empty DB read-only is rejected");
    CHECK(db == NULL, "handle is NULL on failure");
    if (db) keyvaluestore_close(db);
    remove("tc_ro_empty.db");
}

/* ------------------------------------------------------------------ */
static void test_delete_journal(void)
{
    printf("\n--- Test 9: DELETE journal mode ---\n");
    KeyValueStoreConfig cfg = {0};
    cfg.journalMode = KEYVALUESTORE_JOURNAL_DELETE;

    KeyValueStore *db = NULL;
    int rc = keyvaluestore_open_v2("tc_del.db", &db, &cfg);
    CHECK(rc == KEYVALUESTORE_OK && db != NULL, "open DELETE journal");

    if (db) {
        rc = keyvaluestore_put(db, "j", 1, "m", 1);
        CHECK(rc == KEYVALUESTORE_OK, "put OK");

        void *val; int len;
        rc = keyvaluestore_get(db, "j", 1, &val, &len);
        CHECK(rc == KEYVALUESTORE_OK, "get OK");
        if (rc == KEYVALUESTORE_OK) sqliteFree(val);

        keyvaluestore_close(db);
    }
    remove("tc_del.db");
}

/* ------------------------------------------------------------------ */
static void test_busy_timeout_field(void)
{
    printf("\n--- Test 10: busyTimeout field (open succeeds) ---\n");
    KeyValueStoreConfig cfg = {0};
    cfg.journalMode = KEYVALUESTORE_JOURNAL_WAL;
    cfg.busyTimeout = 500;   /* 500 ms retry */

    KeyValueStore *db = NULL;
    int rc = keyvaluestore_open_v2("tc_busy.db", &db, &cfg);
    CHECK(rc == KEYVALUESTORE_OK && db != NULL, "open with busyTimeout=500 succeeds");

    if (db) {
        rc = keyvaluestore_put(db, "bt", 2, "ok", 2);
        CHECK(rc == KEYVALUESTORE_OK, "put OK");
        keyvaluestore_close(db);
    }
    remove("tc_busy.db");
}

/* ------------------------------------------------------------------ */
static void test_backward_compat(void)
{
    printf("\n--- Test 11: keyvaluestore_open backward compatibility ---\n");
    KeyValueStore *db = NULL;
    int rc = keyvaluestore_open("tc_compat.db", &db, KEYVALUESTORE_JOURNAL_WAL);
    CHECK(rc == KEYVALUESTORE_OK && db != NULL, "keyvaluestore_open works");

    if (db) {
        rc = keyvaluestore_put(db, "compat", 6, "yes", 3);
        CHECK(rc == KEYVALUESTORE_OK, "put OK");

        void *val; int len;
        rc = keyvaluestore_get(db, "compat", 6, &val, &len);
        CHECK(rc == KEYVALUESTORE_OK && len == 3, "get OK");
        if (rc == KEYVALUESTORE_OK) sqliteFree(val);

        keyvaluestore_close(db);
    }
    remove("tc_compat.db");
}

/* ------------------------------------------------------------------ */
int main(void)
{
    printf("=== SNKV Config Test Suite ===\n");

    test_null_config();
    test_wal_normal();
    test_sync_off();
    test_sync_full();
    test_custom_page_size();
    test_custom_cache_size();
    test_read_only();
    test_read_only_empty_db();
    test_delete_journal();
    test_busy_timeout_field();
    test_backward_compat();

    printf("\n=== Results: %d passed, %d failed ===\n", passed, failed);
    return failed ? 1 : 0;
}
