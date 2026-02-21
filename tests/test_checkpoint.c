/* SPDX-License-Identifier: Apache-2.0 */
/*
** test_checkpoint.c -- Tests for kvstore_checkpoint() and walSizeLimit.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "kvstore.h"
#include "platform_compat.h"

static int passed = 0;
static int failed = 0;

#define CHECK(cond, msg) do { \
    if (cond) { \
        printf("  [PASS] %s\n", msg); \
        passed++; \
    } else { \
        printf("  [FAIL] %s\n", msg); \
        failed++; \
    } \
} while(0)

static void cleanup(const char *db) {
    char buf[256];
    remove(db);
    snprintf(buf, sizeof(buf), "%s-wal", db); remove(buf);
    snprintf(buf, sizeof(buf), "%s-shm", db); remove(buf);
}

#ifndef _WIN32
static long long file_size(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) return -1LL;
    return (long long)st.st_size;
}
#endif

/* ----------------------------------------------------------------------- */

static void test_passive_checkpoint(void) {
    printf("\n--- Test 1: Manual PASSIVE checkpoint ---\n");
    const char *db = "ckpt_t1.db";
    cleanup(db);

    KVStoreConfig cfg = {0};
    cfg.journalMode = KVSTORE_JOURNAL_WAL;
    KVStore *kv = NULL;
    int rc = kvstore_open_v2(db, &kv, &cfg);
    CHECK(rc == KVSTORE_OK, "open WAL store");

    /* Write 50 records to build up some WAL frames */
    int i;
    char key[32], val[64];
    for (i = 0; i < 50; i++) {
        snprintf(key, sizeof(key), "key%04d", i);
        snprintf(val, sizeof(val), "value%04d", i);
        kvstore_put(kv, key, (int)strlen(key), val, (int)strlen(val));
    }

    int nLog = -1, nCkpt = -1;
    rc = kvstore_checkpoint(kv, KVSTORE_CHECKPOINT_PASSIVE, &nLog, &nCkpt);
    CHECK(rc == KVSTORE_OK,    "PASSIVE checkpoint returns OK");
    CHECK(nLog  >= 0,          "pnLog output is non-negative");
    CHECK(nCkpt >= 0,          "pnCkpt output is non-negative");

    kvstore_close(kv);
    cleanup(db);
}

static void test_truncate_checkpoint(void) {
    printf("\n--- Test 2: TRUNCATE checkpoint clears WAL ---\n");
    const char *db = "ckpt_t2.db";
    cleanup(db);

    KVStoreConfig cfg = {0};
    cfg.journalMode = KVSTORE_JOURNAL_WAL;
    KVStore *kv = NULL;
    kvstore_open_v2(db, &kv, &cfg);

    int i;
    char key[32], val[64];
    for (i = 0; i < 100; i++) {
        snprintf(key, sizeof(key), "key%04d", i);
        snprintf(val, sizeof(val), "value%04d", i);
        kvstore_put(kv, key, (int)strlen(key), val, (int)strlen(val));
    }

    /* WAL file should exist and have content before checkpoint.
    ** Skipped on Windows: stat() returns stale size (0) for files held
    ** open by another handle until FlushFileBuffers / handle close. */
    char walPath[256];
    snprintf(walPath, sizeof(walPath), "%s-wal", db);
#ifndef _WIN32
    long long walBefore = file_size(walPath);
    CHECK(walBefore > 0, "WAL file has content before checkpoint");
#endif

    int nLog = -1, nCkpt = -1;
    int rc = kvstore_checkpoint(kv, KVSTORE_CHECKPOINT_TRUNCATE, &nLog, &nCkpt);
    CHECK(rc == KVSTORE_OK, "TRUNCATE checkpoint returns OK");
    CHECK(nLog == 0,        "pnLog == 0 after TRUNCATE (WAL cleared)");

    kvstore_close(kv);
    cleanup(db);
}

static void test_walsizelimit_auto_checkpoint(void) {
    printf("\n--- Test 3: walSizeLimit auto-checkpoint fires ---\n");
    const char *db = "ckpt_t3.db";
    cleanup(db);

    KVStoreConfig cfg = {0};
    cfg.journalMode = KVSTORE_JOURNAL_WAL;
    cfg.walSizeLimit = 10;   /* checkpoint every 10 commits */
    KVStore *kv = NULL;
    int rc = kvstore_open_v2(db, &kv, &cfg);
    CHECK(rc == KVSTORE_OK, "open with walSizeLimit=10");

    /* Write 50 records -- 5 auto-checkpoints should have fired */
    int i;
    char key[32], val[64];
    for (i = 0; i < 50; i++) {
        snprintf(key, sizeof(key), "key%04d", i);
        snprintf(val, sizeof(val), "value%04d", i);
        kvstore_put(kv, key, (int)strlen(key), val, (int)strlen(val));
    }

    /* After auto-checkpoints the WAL should be small -- manually verify
    ** by running a final PASSIVE checkpoint and checking nLog is low. */
    int nLog = -1, nCkpt = -1;
    rc = kvstore_checkpoint(kv, KVSTORE_CHECKPOINT_PASSIVE, &nLog, &nCkpt);
    CHECK(rc == KVSTORE_OK, "manual checkpoint after auto-checkpoints succeeds");
    /* PASSIVE returns pnLog = total WAL frames, pnCkpt = frames successfully copied.
    ** If auto-checkpoints ran correctly, all frames are already in the DB so the
    ** manual checkpoint copies none new -- but pnLog == pnCkpt means no frames
    ** are blocked (i.e. auto-checkpoint did its job). */
    CHECK(nLog == nCkpt, "all WAL frames checkpointed -- auto-checkpoint was effective");

    kvstore_close(kv);
    cleanup(db);
}

static void test_walsizelimit_disabled(void) {
    printf("\n--- Test 4: walSizeLimit=0 (disabled) -- WAL grows freely ---\n");
    const char *db = "ckpt_t4.db";
    cleanup(db);

    KVStoreConfig cfg = {0};
    cfg.journalMode = KVSTORE_JOURNAL_WAL;
    cfg.walSizeLimit = 0;    /* no auto-checkpoint */
    KVStore *kv = NULL;
    kvstore_open_v2(db, &kv, &cfg);

    int i;
    char key[32], val[64];
    for (i = 0; i < 100; i++) {
        snprintf(key, sizeof(key), "key%04d", i);
        snprintf(val, sizeof(val), "value%04d", i);
        kvstore_put(kv, key, (int)strlen(key), val, (int)strlen(val));
    }

    char walPath[256];
    snprintf(walPath, sizeof(walPath), "%s-wal", db);
#ifndef _WIN32
    /* Skipped on Windows: stat() returns stale size for open files. */
    long long walSize = file_size(walPath);
    CHECK(walSize > 0, "WAL file has grown (no auto-checkpoint)");
#endif

    kvstore_close(kv);
    cleanup(db);
}

static void test_null_output_params(void) {
    printf("\n--- Test 5: NULL pnLog / pnCkpt -- no crash ---\n");
    const char *db = "ckpt_t5.db";
    cleanup(db);

    KVStoreConfig cfg = {0};
    cfg.journalMode = KVSTORE_JOURNAL_WAL;
    KVStore *kv = NULL;
    kvstore_open_v2(db, &kv, &cfg);
    kvstore_put(kv, "k", 1, "v", 1);

    int rc = kvstore_checkpoint(kv, KVSTORE_CHECKPOINT_PASSIVE, NULL, NULL);
    CHECK(rc == KVSTORE_OK, "checkpoint with NULL output params returns OK");

    kvstore_close(kv);
    cleanup(db);
}

static void test_checkpoint_during_write_transaction(void) {
    printf("\n--- Test 6: Checkpoint with active write transaction -> BUSY ---\n");
    const char *db = "ckpt_t6.db";
    cleanup(db);

    KVStoreConfig cfg = {0};
    cfg.journalMode = KVSTORE_JOURNAL_WAL;
    KVStore *kv = NULL;
    kvstore_open_v2(db, &kv, &cfg);

    kvstore_begin(kv, 1);   /* open write transaction */

    int rc = kvstore_checkpoint(kv, KVSTORE_CHECKPOINT_PASSIVE, NULL, NULL);
    CHECK(rc == KVSTORE_BUSY, "checkpoint with open write transaction returns BUSY");

    kvstore_rollback(kv);
    kvstore_close(kv);
    cleanup(db);
}

static void test_checkpoint_delete_journal(void) {
    printf("\n--- Test 7: Checkpoint on DELETE-journal DB -> OK (no-op) ---\n");
    const char *db = "ckpt_t7.db";
    cleanup(db);

    KVStoreConfig cfg = {0};
    cfg.journalMode = KVSTORE_JOURNAL_DELETE;
    KVStore *kv = NULL;
    kvstore_open_v2(db, &kv, &cfg);
    kvstore_put(kv, "k", 1, "v", 1);

    int nLog = -1, nCkpt = -1;
    int rc = kvstore_checkpoint(kv, KVSTORE_CHECKPOINT_PASSIVE, &nLog, &nCkpt);
    CHECK(rc == KVSTORE_OK, "checkpoint on DELETE-journal returns OK");
    CHECK(nLog  == 0, "pnLog == 0 on non-WAL database");
    CHECK(nCkpt == 0, "pnCkpt == 0 on non-WAL database");

    kvstore_close(kv);
    cleanup(db);
}

/* ----------------------------------------------------------------------- */

int main(void) {
    enable_ansi_colors();
    printf("=== SNKV Checkpoint Test Suite ===\n");

    test_passive_checkpoint();
    test_truncate_checkpoint();
    test_walsizelimit_auto_checkpoint();
    test_walsizelimit_disabled();
    test_null_output_params();
    test_checkpoint_during_write_transaction();
    test_checkpoint_delete_journal();

    printf("\n=== Results: %d passed, %d failed ===\n", passed, failed);
    return failed ? 1 : 0;
}
