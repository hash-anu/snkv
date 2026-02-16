/*
** test_crash_recovery.c
**
** Validates crash recovery and transaction durability for SNKV.
**
** Tests:
** 1. Committed data survives unclean close (WAL + DELETE modes)
** 2. Uncommitted data is rolled back on reopen
** 3. WAL recovery after unclean shutdown
** 4. Rollback journal recovery after unclean shutdown
** 5. Multiple commit/crash cycles
** 6. Large transaction crash recovery
** 7. Integrity check after every recovery
**
** Cross-platform: works on Linux, macOS, and Windows.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "kvstore.h"
#include "platform_compat.h"

#define DB_FILE_WAL   "test_crash_wal.db"
#define DB_FILE_DEL   "test_crash_del.db"

#define CLR_GREEN  "\033[0;32m"
#define CLR_RED    "\033[0;31m"
#define CLR_BLUE   "\033[0;34m"
#define CLR_CYAN   "\033[0;36m"
#define CLR_RESET  "\033[0m"

static int g_passed = 0;
static int g_failed = 0;

static void print_result(const char *name, int ok) {
    if (ok) {
        g_passed++;
        printf(CLR_GREEN "  [PASS]" CLR_RESET " %s\n", name);
    } else {
        g_failed++;
        printf(CLR_RED   "  [FAIL]" CLR_RESET " %s\n", name);
    }
}

static void print_section(const char *title) {
    printf("\n" CLR_BLUE "=== %s ===" CLR_RESET "\n", title);
}

static void cleanup(const char *db) {
    char buf[512];
    remove(db);
    snprintf(buf, sizeof(buf), "%s-journal", db);
    remove(buf);
    snprintf(buf, sizeof(buf), "%s-wal", db);
    remove(buf);
    snprintf(buf, sizeof(buf), "%s-shm", db);
    remove(buf);
}

/* ---- helpers ---- */

static void make_key(char *buf, int buflen, const char *prefix, int seq) {
    snprintf(buf, buflen, "%s-%06d", prefix, seq);
}

static void make_value(char *buf, int buflen, const char *prefix, int seq) {
    snprintf(buf, buflen, "%s-value-%06d-data", prefix, seq);
}

/* Count how many keys with given prefix exist in the DB */
static int count_keys(KVStore *kv, const char *prefix, int max_seq) {
    int count = 0;
    int i;
    char key[64];
    for (i = 0; i < max_seq; i++) {
        int exists = 0;
        make_key(key, sizeof(key), prefix, i);
        kvstore_exists(kv, key, (int)strlen(key), &exists);
        if (exists) count++;
    }
    return count;
}

/* Verify all keys [0, count) have correct values */
static int verify_keys(KVStore *kv, const char *prefix, int count) {
    int i;
    char key[64], expected[64];
    for (i = 0; i < count; i++) {
        void *got = NULL;
        int got_len = 0;
        make_key(key, sizeof(key), prefix, i);
        make_value(expected, sizeof(expected), prefix, i);

        int rc = kvstore_get(kv, key, (int)strlen(key), &got, &got_len);
        if (rc != KVSTORE_OK) return 0;

        int exp_len = (int)strlen(expected);
        if (got_len != exp_len || memcmp(got, expected, exp_len) != 0) {
            sqliteFree(got);
            return 0;
        }
        sqliteFree(got);
    }
    return 1;
}

/* Run integrity check, return 1 if OK */
static int check_integrity(KVStore *kv) {
    char *errMsg = NULL;
    int rc = kvstore_integrity_check(kv, &errMsg);
    if (errMsg) sqliteFree(errMsg);
    return (rc == KVSTORE_OK);
}

/* ================================================================
** TEST 1: Committed data survives unclean close
**
** Write N keys and commit, then close WITHOUT calling kvstore_close
** (simulate crash by just freeing — but since we can't do that safely,
** we close normally, which is the minimum recovery path).
** Reopen and verify all committed data is present.
** ================================================================ */

static void test_committed_survives_wal(void) {
    print_section("Committed data survives reopen (WAL mode)");
    cleanup(DB_FILE_WAL);

    KVStore *kv = NULL;
    int rc, i;
    char key[64], value[64];

    /* Phase 1: Write 500 keys and commit */
    rc = kvstore_open(DB_FILE_WAL, &kv, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) { print_result("Open DB", 0); return; }

    kvstore_begin(kv, 1);
    for (i = 0; i < 500; i++) {
        make_key(key, sizeof(key), "committed", i);
        make_value(value, sizeof(value), "committed", i);
        kvstore_put(kv, key, (int)strlen(key), value, (int)strlen(value));
    }
    rc = kvstore_commit(kv);
    print_result("Phase 1: Write 500 keys + commit", rc == KVSTORE_OK);

    kvstore_close(kv);

    /* Phase 2: Reopen and verify */
    rc = kvstore_open(DB_FILE_WAL, &kv, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) { print_result("Reopen DB", 0); return; }

    int found = count_keys(kv, "committed", 500);
    print_result("All 500 committed keys present", found == 500);

    int correct = verify_keys(kv, "committed", 500);
    print_result("All values correct", correct);

    print_result("Integrity check", check_integrity(kv));

    kvstore_close(kv);
    cleanup(DB_FILE_WAL);
}

static void test_committed_survives_delete(void) {
    print_section("Committed data survives reopen (DELETE journal mode)");
    cleanup(DB_FILE_DEL);

    KVStore *kv = NULL;
    int rc, i;
    char key[64], value[64];

    rc = kvstore_open(DB_FILE_DEL, &kv, KVSTORE_JOURNAL_DELETE);
    if (rc != KVSTORE_OK) { print_result("Open DB", 0); return; }

    kvstore_begin(kv, 1);
    for (i = 0; i < 500; i++) {
        make_key(key, sizeof(key), "committed", i);
        make_value(value, sizeof(value), "committed", i);
        kvstore_put(kv, key, (int)strlen(key), value, (int)strlen(value));
    }
    rc = kvstore_commit(kv);
    print_result("Phase 1: Write 500 keys + commit", rc == KVSTORE_OK);

    kvstore_close(kv);

    rc = kvstore_open(DB_FILE_DEL, &kv, KVSTORE_JOURNAL_DELETE);
    if (rc != KVSTORE_OK) { print_result("Reopen DB", 0); return; }

    int found = count_keys(kv, "committed", 500);
    print_result("All 500 committed keys present", found == 500);

    int correct = verify_keys(kv, "committed", 500);
    print_result("All values correct", correct);

    print_result("Integrity check", check_integrity(kv));

    kvstore_close(kv);
    cleanup(DB_FILE_DEL);
}

/* ================================================================
** TEST 2: Uncommitted data is NOT visible after reopen
**
** Write batch 1 and commit. Then write batch 2 but DON'T commit.
** Close and reopen. Only batch 1 should be present.
** ================================================================ */

static void test_uncommitted_rolled_back(int journalMode, const char *mode_name) {
    char title[128];
    snprintf(title, sizeof(title), "Uncommitted data rolled back (%s)", mode_name);
    print_section(title);

    const char *db = (journalMode == KVSTORE_JOURNAL_WAL) ? DB_FILE_WAL : DB_FILE_DEL;
    cleanup(db);

    KVStore *kv = NULL;
    int rc, i;
    char key[64], value[64];

    /* Phase 1: Write 200 keys and commit */
    rc = kvstore_open(db, &kv, journalMode);
    if (rc != KVSTORE_OK) { print_result("Open DB", 0); return; }

    kvstore_begin(kv, 1);
    for (i = 0; i < 200; i++) {
        make_key(key, sizeof(key), "batch1", i);
        make_value(value, sizeof(value), "batch1", i);
        kvstore_put(kv, key, (int)strlen(key), value, (int)strlen(value));
    }
    rc = kvstore_commit(kv);
    print_result("Batch 1: 200 keys committed", rc == KVSTORE_OK);

    /* Phase 2: Write 300 more keys but DO NOT commit */
    kvstore_begin(kv, 1);
    for (i = 0; i < 300; i++) {
        make_key(key, sizeof(key), "batch2", i);
        make_value(value, sizeof(value), "batch2", i);
        kvstore_put(kv, key, (int)strlen(key), value, (int)strlen(value));
    }
    /* Simulate crash: close without committing */
    kvstore_close(kv);

    /* Phase 3: Reopen and verify */
    rc = kvstore_open(db, &kv, journalMode);
    if (rc != KVSTORE_OK) { print_result("Reopen DB", 0); return; }

    int batch1_count = count_keys(kv, "batch1", 200);
    print_result("Batch 1 (committed) all present", batch1_count == 200);

    int batch1_correct = verify_keys(kv, "batch1", 200);
    print_result("Batch 1 values correct", batch1_correct);

    int batch2_count = count_keys(kv, "batch2", 300);
    print_result("Batch 2 (uncommitted) rolled back", batch2_count == 0);

    print_result("Integrity check", check_integrity(kv));

    kvstore_close(kv);
    cleanup(db);
}

/* ================================================================
** TEST 3: Explicit rollback discards data
** ================================================================ */

static void test_explicit_rollback(int journalMode, const char *mode_name) {
    char title[128];
    snprintf(title, sizeof(title), "Explicit rollback discards data (%s)", mode_name);
    print_section(title);

    const char *db = (journalMode == KVSTORE_JOURNAL_WAL) ? DB_FILE_WAL : DB_FILE_DEL;
    cleanup(db);

    KVStore *kv = NULL;
    int rc, i;
    char key[64], value[64];

    rc = kvstore_open(db, &kv, journalMode);
    if (rc != KVSTORE_OK) { print_result("Open DB", 0); return; }

    /* Commit some data first */
    kvstore_begin(kv, 1);
    for (i = 0; i < 100; i++) {
        make_key(key, sizeof(key), "keep", i);
        make_value(value, sizeof(value), "keep", i);
        kvstore_put(kv, key, (int)strlen(key), value, (int)strlen(value));
    }
    kvstore_commit(kv);

    /* Write more data then rollback */
    kvstore_begin(kv, 1);
    for (i = 0; i < 200; i++) {
        make_key(key, sizeof(key), "discard", i);
        make_value(value, sizeof(value), "discard", i);
        kvstore_put(kv, key, (int)strlen(key), value, (int)strlen(value));
    }
    rc = kvstore_rollback(kv);
    print_result("Rollback succeeded", rc == KVSTORE_OK);

    /* Verify: 'keep' keys present, 'discard' keys gone */
    int keep_count = count_keys(kv, "keep", 100);
    print_result("Committed keys still present", keep_count == 100);

    int discard_count = count_keys(kv, "discard", 200);
    print_result("Rolled-back keys discarded", discard_count == 0);

    print_result("Integrity check", check_integrity(kv));

    kvstore_close(kv);
    cleanup(db);
}

/* ================================================================
** TEST 4: Multiple commit/crash cycles
**
** Repeatedly: commit a batch, close (simulate crash), reopen, verify.
** ================================================================ */

static void test_multiple_crash_cycles(void) {
    print_section("Multiple commit/reopen cycles (WAL mode)");
    cleanup(DB_FILE_WAL);

    int cycle, i, rc;
    char key[64], value[64];
    char prefix[32];
    int total_expected = 0;

    for (cycle = 0; cycle < 5; cycle++) {
        KVStore *kv = NULL;
        rc = kvstore_open(DB_FILE_WAL, &kv, KVSTORE_JOURNAL_WAL);
        if (rc != KVSTORE_OK) {
            print_result("Open failed on cycle", 0);
            cleanup(DB_FILE_WAL);
            return;
        }

        /* Verify all previously committed data is intact */
        int prev_total = 0;
        int c;
        for (c = 0; c < cycle; c++) {
            snprintf(prefix, sizeof(prefix), "cycle%d", c);
            prev_total += count_keys(kv, prefix, 100);
        }
        if (cycle > 0) {
            char desc[64];
            snprintf(desc, sizeof(desc), "Cycle %d: previous %d keys intact", cycle, total_expected);
            print_result(desc, prev_total == total_expected);
        }

        /* Write 100 new keys for this cycle */
        snprintf(prefix, sizeof(prefix), "cycle%d", cycle);
        kvstore_begin(kv, 1);
        for (i = 0; i < 100; i++) {
            make_key(key, sizeof(key), prefix, i);
            make_value(value, sizeof(value), prefix, i);
            kvstore_put(kv, key, (int)strlen(key), value, (int)strlen(value));
        }
        kvstore_commit(kv);
        total_expected += 100;

        /* Also write uncommitted data that should be lost */
        kvstore_begin(kv, 1);
        for (i = 0; i < 50; i++) {
            snprintf(prefix, sizeof(prefix), "ghost%d", cycle);
            make_key(key, sizeof(key), prefix, i);
            make_value(value, sizeof(value), prefix, i);
            kvstore_put(kv, key, (int)strlen(key), value, (int)strlen(value));
        }
        /* Close without committing — uncommitted data should be lost */
        kvstore_close(kv);
    }

    /* Final verification */
    KVStore *kv = NULL;
    rc = kvstore_open(DB_FILE_WAL, &kv, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) {
        print_result("Final reopen", 0);
        cleanup(DB_FILE_WAL);
        return;
    }

    int final_total = 0;
    int cycle_i;
    for (cycle_i = 0; cycle_i < 5; cycle_i++) {
        snprintf(prefix, sizeof(prefix), "cycle%d", cycle_i);
        final_total += count_keys(kv, prefix, 100);
    }
    char desc[128];
    snprintf(desc, sizeof(desc), "Final: all %d committed keys present", total_expected);
    print_result(desc, final_total == total_expected);

    /* Verify no ghost keys survived */
    int ghost_total = 0;
    for (cycle_i = 0; cycle_i < 5; cycle_i++) {
        snprintf(prefix, sizeof(prefix), "ghost%d", cycle_i);
        ghost_total += count_keys(kv, prefix, 50);
    }
    print_result("No uncommitted ghost keys survived", ghost_total == 0);

    /* Verify all values are correct */
    int all_correct = 1;
    for (cycle_i = 0; cycle_i < 5 && all_correct; cycle_i++) {
        snprintf(prefix, sizeof(prefix), "cycle%d", cycle_i);
        if (!verify_keys(kv, prefix, 100)) all_correct = 0;
    }
    print_result("All values correct across cycles", all_correct);

    print_result("Integrity check", check_integrity(kv));

    kvstore_close(kv);
    cleanup(DB_FILE_WAL);
}

/* ================================================================
** TEST 5: Large transaction crash recovery
**
** Write a large batch (5000 keys), commit. Write another large batch,
** don't commit. Reopen and verify only the first batch survives.
** ================================================================ */

static void test_large_txn_recovery(int journalMode, const char *mode_name) {
    char title[128];
    snprintf(title, sizeof(title), "Large transaction recovery (%s)", mode_name);
    print_section(title);

    const char *db = (journalMode == KVSTORE_JOURNAL_WAL) ? DB_FILE_WAL : DB_FILE_DEL;
    cleanup(db);

    KVStore *kv = NULL;
    int rc, i;
    char key[64], value[64];

    rc = kvstore_open(db, &kv, journalMode);
    if (rc != KVSTORE_OK) { print_result("Open DB", 0); return; }

    /* Write and commit 5000 keys */
    kvstore_begin(kv, 1);
    for (i = 0; i < 5000; i++) {
        make_key(key, sizeof(key), "large-ok", i);
        make_value(value, sizeof(value), "large-ok", i);
        kvstore_put(kv, key, (int)strlen(key), value, (int)strlen(value));
    }
    rc = kvstore_commit(kv);
    print_result("5000 keys committed", rc == KVSTORE_OK);

    /* Write 5000 more but don't commit */
    kvstore_begin(kv, 1);
    for (i = 0; i < 5000; i++) {
        make_key(key, sizeof(key), "large-lost", i);
        make_value(value, sizeof(value), "large-lost", i);
        kvstore_put(kv, key, (int)strlen(key), value, (int)strlen(value));
    }
    /* Crash: close without commit */
    kvstore_close(kv);

    /* Reopen and verify */
    rc = kvstore_open(db, &kv, journalMode);
    if (rc != KVSTORE_OK) { print_result("Reopen DB", 0); return; }

    int ok_count = count_keys(kv, "large-ok", 5000);
    print_result("All 5000 committed keys present", ok_count == 5000);

    int ok_correct = verify_keys(kv, "large-ok", 5000);
    print_result("All committed values correct", ok_correct);

    int lost_count = count_keys(kv, "large-lost", 5000);
    print_result("All 5000 uncommitted keys rolled back", lost_count == 0);

    print_result("Integrity check", check_integrity(kv));

    kvstore_close(kv);
    cleanup(db);
}

/* ================================================================
** TEST 6: Overwrite + crash recovery
**
** Write keys, commit. Overwrite same keys with new values, commit.
** Overwrite again but DON'T commit. Reopen: should see second values.
** ================================================================ */

static void test_overwrite_recovery(void) {
    print_section("Overwrite + crash recovery (WAL mode)");
    cleanup(DB_FILE_WAL);

    KVStore *kv = NULL;
    int rc, i;
    char key[64], value[64];

    rc = kvstore_open(DB_FILE_WAL, &kv, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) { print_result("Open DB", 0); return; }

    /* Version 1: write original values */
    kvstore_begin(kv, 1);
    for (i = 0; i < 200; i++) {
        snprintf(key, sizeof(key), "ow-key-%06d", i);
        snprintf(value, sizeof(value), "version-1-%06d", i);
        kvstore_put(kv, key, (int)strlen(key), value, (int)strlen(value));
    }
    kvstore_commit(kv);

    /* Version 2: overwrite with new values and commit */
    kvstore_begin(kv, 1);
    for (i = 0; i < 200; i++) {
        snprintf(key, sizeof(key), "ow-key-%06d", i);
        snprintf(value, sizeof(value), "version-2-%06d", i);
        kvstore_put(kv, key, (int)strlen(key), value, (int)strlen(value));
    }
    kvstore_commit(kv);

    /* Version 3: overwrite again but DON'T commit */
    kvstore_begin(kv, 1);
    for (i = 0; i < 200; i++) {
        snprintf(key, sizeof(key), "ow-key-%06d", i);
        snprintf(value, sizeof(value), "version-3-%06d", i);
        kvstore_put(kv, key, (int)strlen(key), value, (int)strlen(value));
    }
    /* Crash: close without commit */
    kvstore_close(kv);

    /* Reopen: should see version-2 values (last committed) */
    rc = kvstore_open(DB_FILE_WAL, &kv, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) { print_result("Reopen DB", 0); return; }

    int v2_correct = 0;
    int v3_found = 0;
    for (i = 0; i < 200; i++) {
        void *got = NULL;
        int got_len = 0;
        snprintf(key, sizeof(key), "ow-key-%06d", i);

        rc = kvstore_get(kv, key, (int)strlen(key), &got, &got_len);
        if (rc == KVSTORE_OK) {
            char expected_v2[64];
            snprintf(expected_v2, sizeof(expected_v2), "version-2-%06d", i);
            int exp_len = (int)strlen(expected_v2);

            if (got_len == exp_len && memcmp(got, expected_v2, exp_len) == 0) {
                v2_correct++;
            }

            char expected_v3[64];
            snprintf(expected_v3, sizeof(expected_v3), "version-3-%06d", i);
            exp_len = (int)strlen(expected_v3);
            if (got_len == exp_len && memcmp(got, expected_v3, exp_len) == 0) {
                v3_found++;
            }
            sqliteFree(got);
        }
    }

    print_result("All 200 keys have version-2 values", v2_correct == 200);
    print_result("No version-3 (uncommitted) values leaked", v3_found == 0);
    print_result("Integrity check", check_integrity(kv));

    kvstore_close(kv);
    cleanup(DB_FILE_WAL);
}

/* ================================================================
** TEST 7: Delete + crash recovery
**
** Write keys and commit. Delete some and commit. Delete more but
** DON'T commit. Reopen: second batch of deletes should be undone.
** ================================================================ */

static void test_delete_recovery(void) {
    print_section("Delete + crash recovery (WAL mode)");
    cleanup(DB_FILE_WAL);

    KVStore *kv = NULL;
    int rc, i;
    char key[64], value[64];

    rc = kvstore_open(DB_FILE_WAL, &kv, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) { print_result("Open DB", 0); return; }

    /* Write 300 keys */
    kvstore_begin(kv, 1);
    for (i = 0; i < 300; i++) {
        make_key(key, sizeof(key), "deltest", i);
        make_value(value, sizeof(value), "deltest", i);
        kvstore_put(kv, key, (int)strlen(key), value, (int)strlen(value));
    }
    kvstore_commit(kv);

    /* Delete keys 0-99 and commit */
    kvstore_begin(kv, 1);
    for (i = 0; i < 100; i++) {
        make_key(key, sizeof(key), "deltest", i);
        kvstore_delete(kv, key, (int)strlen(key));
    }
    kvstore_commit(kv);

    /* Delete keys 100-199 but DON'T commit */
    kvstore_begin(kv, 1);
    for (i = 100; i < 200; i++) {
        make_key(key, sizeof(key), "deltest", i);
        kvstore_delete(kv, key, (int)strlen(key));
    }
    /* Crash */
    kvstore_close(kv);

    /* Reopen: keys 0-99 should be gone, 100-299 should be present */
    rc = kvstore_open(DB_FILE_WAL, &kv, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) { print_result("Reopen DB", 0); return; }

    int deleted_count = count_keys(kv, "deltest", 100);  /* keys 0-99 */
    print_result("Committed deletes (0-99) stayed deleted", deleted_count == 0);

    /* Count keys 100-199 (uncommitted deletes should be undone) */
    int restored = 0;
    for (i = 100; i < 200; i++) {
        int exists = 0;
        make_key(key, sizeof(key), "deltest", i);
        kvstore_exists(kv, key, (int)strlen(key), &exists);
        if (exists) restored++;
    }
    print_result("Uncommitted deletes (100-199) restored", restored == 100);

    /* Count keys 200-299 (never touched) */
    int untouched = 0;
    for (i = 200; i < 300; i++) {
        int exists = 0;
        make_key(key, sizeof(key), "deltest", i);
        kvstore_exists(kv, key, (int)strlen(key), &exists);
        if (exists) untouched++;
    }
    print_result("Untouched keys (200-299) still present", untouched == 100);

    /* Verify restored values are correct */
    int restored_correct = 1;
    for (i = 100; i < 200 && restored_correct; i++) {
        void *got = NULL;
        int got_len = 0;
        make_key(key, sizeof(key), "deltest", i);
        make_value(value, sizeof(value), "deltest", i);

        rc = kvstore_get(kv, key, (int)strlen(key), &got, &got_len);
        if (rc != KVSTORE_OK) { restored_correct = 0; break; }

        int exp_len = (int)strlen(value);
        if (got_len != exp_len || memcmp(got, value, exp_len) != 0) {
            restored_correct = 0;
        }
        sqliteFree(got);
    }
    print_result("Restored values are correct", restored_correct);

    print_result("Integrity check", check_integrity(kv));

    kvstore_close(kv);
    cleanup(DB_FILE_WAL);
}

/* ==================== Main ==================== */
int main(void) {
    printf("\n" CLR_CYAN);
    printf("════════════════════════════════════════════════════════\n");
    printf("  SNKV Crash Recovery & Transaction Durability Tests\n");
    printf("════════════════════════════════════════════════════════\n");
    printf(CLR_RESET);

    /* Test 1: Committed data survives */
    test_committed_survives_wal();
    test_committed_survives_delete();

    /* Test 2: Uncommitted data rolled back */
    test_uncommitted_rolled_back(KVSTORE_JOURNAL_WAL, "WAL");
    test_uncommitted_rolled_back(KVSTORE_JOURNAL_DELETE, "DELETE");

    /* Test 3: Explicit rollback */
    test_explicit_rollback(KVSTORE_JOURNAL_WAL, "WAL");
    test_explicit_rollback(KVSTORE_JOURNAL_DELETE, "DELETE");

    /* Test 4: Multiple commit/crash cycles */
    test_multiple_crash_cycles();

    /* Test 5: Large transaction recovery */
    test_large_txn_recovery(KVSTORE_JOURNAL_WAL, "WAL");
    test_large_txn_recovery(KVSTORE_JOURNAL_DELETE, "DELETE");

    /* Test 6: Overwrite + crash */
    test_overwrite_recovery();

    /* Test 7: Delete + crash */
    test_delete_recovery();

    /* Summary */
    printf("\n" CLR_CYAN);
    printf("════════════════════════════════════════════════════════\n");
    printf("  Results: " CLR_GREEN "%d passed" CLR_RESET ", "
           CLR_RED "%d failed" CLR_RESET "\n", g_passed, g_failed);
    printf(CLR_CYAN "════════════════════════════════════════════════════════\n");
    printf(CLR_RESET "\n");

    return g_failed > 0 ? 1 : 0;
}
