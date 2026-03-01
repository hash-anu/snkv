/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright 2025 SNKV Contributors */
/*
** Auto-Vacuum Test Suite for KVStore
**
** All databases are opened with incremental auto-vacuum by default.
** Users call kvstore_incremental_vacuum() to reclaim unused pages.
**
** Tests:
** 1. Incremental vacuum reclaims space after deletes
** 2. Partial vacuum steps (nPage > 0)
** 3. Incremental vacuum with WAL journal mode
** 4. Data integrity preserved after vacuum
** 5. Multiple vacuum cycles
*/

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include "kvstore.h"

#define TEST_DB   "test_autovacuum.db"

/* Test result tracking */
static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define COLOR_RED     "\x1b[31m"
#define COLOR_GREEN   "\x1b[32m"
#define COLOR_BLUE    "\x1b[34m"
#define COLOR_RESET   "\x1b[0m"

#define TEST_START(name) \
  do { \
    tests_run++; \
    printf(COLOR_BLUE "TEST %d: %s" COLOR_RESET "\n", tests_run, name); \
  } while(0)

#define TEST_PASS() \
  do { \
    tests_passed++; \
    printf(COLOR_GREEN "  PASSED" COLOR_RESET "\n\n"); \
  } while(0)

#define TEST_FAIL(msg) \
  do { \
    tests_failed++; \
    printf(COLOR_RED "  FAILED: %s" COLOR_RESET "\n\n", msg); \
    return; \
  } while(0)

#define ASSERT_OK(rc, msg) \
  if( (rc) != KVSTORE_OK ) { \
    printf("  Error at line %d: %s (code=%d)\n", __LINE__, msg, rc); \
    TEST_FAIL(msg); \
  }

/* Get file size in bytes */
static long get_file_size(const char *path){
  FILE *f = fopen(path, "rb");
  long sz;
  if( !f ) return 0;
  fseek(f, 0, SEEK_END);
  sz = ftell(f);
  fclose(f);
  return sz;
}

/* Remove database and associated files */
static void cleanup_db(const char *path){
  char buf[256];
  remove(path);
  snprintf(buf, sizeof(buf), "%s-journal", path);
  remove(buf);
  snprintf(buf, sizeof(buf), "%s-wal", path);
  remove(buf);
  snprintf(buf, sizeof(buf), "%s-shm", path);
  remove(buf);
}

/* Insert N key-value pairs with ~100 byte values */
static int insert_records(KVStore *kv, int start, int count){
  int rc;
  char key[32];
  char value[128];

  rc = kvstore_begin(kv, 1);
  if( rc != KVSTORE_OK ) return rc;

  for( int i = start; i < start + count; i++ ){
    snprintf(key, sizeof(key), "key-%06d", i);
    memset(value, 'A' + (i % 26), 100);
    value[100] = '\0';
    rc = kvstore_put(kv, key, (int)strlen(key), value, 101);
    if( rc != KVSTORE_OK ){
      kvstore_rollback(kv);
      return rc;
    }
  }

  return kvstore_commit(kv);
}

/* Delete N key-value pairs */
static int delete_records(KVStore *kv, int start, int count){
  int rc;
  char key[32];

  rc = kvstore_begin(kv, 1);
  if( rc != KVSTORE_OK ) return rc;

  for( int i = start; i < start + count; i++ ){
    snprintf(key, sizeof(key), "key-%06d", i);
    rc = kvstore_delete(kv, key, (int)strlen(key));
    if( rc != KVSTORE_OK && rc != KVSTORE_NOTFOUND ){
      kvstore_rollback(kv);
      return rc;
    }
  }

  return kvstore_commit(kv);
}

/*
** Test 1: Incremental vacuum reclaims space after deletes
*/
static void test_incremental_vacuum(void){
  KVStore *kv = NULL;
  int rc;
  long size_after_insert, size_after_delete, size_after_vacuum;

  TEST_START("Incremental vacuum reclaims space after deletes");
  cleanup_db(TEST_DB);

  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "open");

  /* Insert 2000 records */
  rc = insert_records(kv, 0, 2000);
  ASSERT_OK(rc, "insert 2000 records");
  kvstore_close(kv);

  size_after_insert = get_file_size(TEST_DB);
  printf("  File size after insert: %ld bytes\n", size_after_insert);

  /* Delete most records -- file should NOT shrink yet (incremental mode) */
  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "reopen");

  rc = delete_records(kv, 0, 1800);
  ASSERT_OK(rc, "delete 1800 records");
  kvstore_close(kv);

  size_after_delete = get_file_size(TEST_DB);
  printf("  File size after delete (no vacuum): %ld bytes\n", size_after_delete);

  /* Now run incremental vacuum to reclaim space */
  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "reopen for vacuum");

  rc = kvstore_incremental_vacuum(kv, 0);  /* 0 = free all */
  ASSERT_OK(rc, "incremental vacuum");
  kvstore_close(kv);

  size_after_vacuum = get_file_size(TEST_DB);
  printf("  File size after vacuum: %ld bytes\n", size_after_vacuum);

  if( size_after_vacuum >= size_after_delete ){
    printf("  Vacuum did not shrink file!\n");
    TEST_FAIL("incremental vacuum did not reclaim pages");
  }

  printf("  Reclaimed: %ld bytes (%.0f%%)\n",
         size_after_delete - size_after_vacuum,
         100.0 * (size_after_delete - size_after_vacuum) / size_after_delete);

  cleanup_db(TEST_DB);
  TEST_PASS();
}

/*
** Test 2: Partial vacuum steps (nPage > 0)
*/
static void test_partial_vacuum(void){
  KVStore *kv = NULL;
  int rc;
  long size_before, size_partial, size_full;

  TEST_START("Partial vacuum steps (nPage > 0)");
  cleanup_db(TEST_DB);

  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "open");

  rc = insert_records(kv, 0, 2000);
  ASSERT_OK(rc, "insert");

  rc = delete_records(kv, 0, 1800);
  ASSERT_OK(rc, "delete");
  kvstore_close(kv);

  size_before = get_file_size(TEST_DB);
  printf("  File size before vacuum: %ld bytes\n", size_before);

  /* Vacuum only 10 pages */
  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "reopen");

  rc = kvstore_incremental_vacuum(kv, 10);
  ASSERT_OK(rc, "vacuum 10 pages");
  kvstore_close(kv);

  size_partial = get_file_size(TEST_DB);
  printf("  File size after 10-page vacuum: %ld bytes\n", size_partial);

  /* Vacuum remaining */
  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "reopen");

  rc = kvstore_incremental_vacuum(kv, 0);
  ASSERT_OK(rc, "vacuum all remaining");
  kvstore_close(kv);

  size_full = get_file_size(TEST_DB);
  printf("  File size after full vacuum: %ld bytes\n", size_full);

  if( size_partial >= size_before ){
    TEST_FAIL("partial vacuum did not shrink file");
  }
  if( size_full >= size_partial ){
    TEST_FAIL("full vacuum did not shrink further");
  }

  printf("  Partial reclaimed: %ld bytes, Full reclaimed: %ld bytes total\n",
         size_before - size_partial, size_before - size_full);

  cleanup_db(TEST_DB);
  TEST_PASS();
}

/*
** Test 3: Incremental vacuum with WAL journal mode
*/
static void test_vacuum_wal_mode(void){
  KVStore *kv = NULL;
  int rc;
  long size_after_insert, size_after_vacuum;

  TEST_START("Incremental vacuum with WAL journal mode");
  cleanup_db(TEST_DB);

  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_WAL);
  ASSERT_OK(rc, "open WAL");

  rc = insert_records(kv, 0, 2000);
  ASSERT_OK(rc, "insert 2000");
  kvstore_close(kv);

  size_after_insert = get_file_size(TEST_DB);
  printf("  File size after insert: %ld bytes\n", size_after_insert);

  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_WAL);
  ASSERT_OK(rc, "reopen");

  rc = delete_records(kv, 0, 1800);
  ASSERT_OK(rc, "delete 1800");

  rc = kvstore_incremental_vacuum(kv, 0);
  ASSERT_OK(rc, "incremental vacuum");
  kvstore_close(kv);

  size_after_vacuum = get_file_size(TEST_DB);
  printf("  File size after vacuum: %ld bytes\n", size_after_vacuum);

  if( size_after_vacuum >= size_after_insert ){
    printf("  File did not shrink with WAL+vacuum\n");
    cleanup_db(TEST_DB);
    TEST_FAIL("WAL vacuum did not shrink file");
  }

  printf("  Reclaimed: %ld bytes (%.0f%%)\n",
         size_after_insert - size_after_vacuum,
         100.0 * (size_after_insert - size_after_vacuum) / size_after_insert);

  cleanup_db(TEST_DB);
  TEST_PASS();
}

/*
** Test 4: Data integrity preserved after vacuum
*/
static void test_vacuum_integrity(void){
  KVStore *kv = NULL;
  int rc;

  TEST_START("Data integrity preserved after vacuum");
  cleanup_db(TEST_DB);

  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "open");

  rc = insert_records(kv, 0, 2000);
  ASSERT_OK(rc, "insert 2000 records");

  rc = delete_records(kv, 0, 1800);
  ASSERT_OK(rc, "delete 1800 records");

  rc = kvstore_incremental_vacuum(kv, 0);
  ASSERT_OK(rc, "vacuum");
  kvstore_close(kv);

  /* Reopen and verify remaining 200 records */
  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "reopen for verify");

  for( int i = 1800; i < 2000; i++ ){
    char key[32];
    void *pVal = NULL;
    int nVal = 0;
    snprintf(key, sizeof(key), "key-%06d", i);
    rc = kvstore_get(kv, key, (int)strlen(key), &pVal, &nVal);
    if( rc != KVSTORE_OK ){
      printf("  Missing key: %s\n", key);
      kvstore_close(kv);
      TEST_FAIL("data lost after vacuum");
    }
    sqliteFree(pVal);
  }

  /* Deleted keys should not exist */
  for( int i = 0; i < 1800; i += 300 ){
    char key[32];
    void *pVal = NULL;
    int nVal = 0;
    snprintf(key, sizeof(key), "key-%06d", i);
    rc = kvstore_get(kv, key, (int)strlen(key), &pVal, &nVal);
    if( rc != KVSTORE_NOTFOUND ){
      printf("  Deleted key still present: %s\n", key);
      if( pVal ) sqliteFree(pVal);
      kvstore_close(kv);
      TEST_FAIL("deleted key still exists after vacuum");
    }
  }

  /* Integrity check */
  char *zErr = NULL;
  rc = kvstore_integrity_check(kv, &zErr);
  if( rc != KVSTORE_OK ){
    printf("  Integrity check failed: %s\n", zErr ? zErr : "unknown");
    if( zErr ) sqliteFree(zErr);
    kvstore_close(kv);
    TEST_FAIL("integrity check failed after vacuum");
  }
  if( zErr ) sqliteFree(zErr);
  printf("  All 200 remaining records intact, deleted keys confirmed gone\n");
  printf("  Integrity check passed\n");

  kvstore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/*
** Test 5: Multiple vacuum cycles (insert/delete/vacuum repeated)
*/
static void test_multiple_vacuum_cycles(void){
  KVStore *kv = NULL;
  int rc;
  long sizes[4];

  TEST_START("Multiple vacuum cycles");
  cleanup_db(TEST_DB);

  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "open");

  for( int cycle = 0; cycle < 3; cycle++ ){
    int base = cycle * 1000;

    rc = insert_records(kv, base, 1000);
    ASSERT_OK(rc, "insert");

    rc = delete_records(kv, base, 800);
    ASSERT_OK(rc, "delete");

    rc = kvstore_incremental_vacuum(kv, 0);
    ASSERT_OK(rc, "vacuum");

    kvstore_close(kv);
    sizes[cycle] = get_file_size(TEST_DB);
    printf("  Cycle %d: file size = %ld bytes\n", cycle + 1, sizes[cycle]);

    rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
    ASSERT_OK(rc, "reopen");
  }

  /* Verify data from all cycles: keys base+800..base+999 should exist */
  for( int cycle = 0; cycle < 3; cycle++ ){
    int base = cycle * 1000;
    for( int i = base + 800; i < base + 1000; i++ ){
      char key[32];
      void *pVal = NULL;
      int nVal = 0;
      snprintf(key, sizeof(key), "key-%06d", i);
      rc = kvstore_get(kv, key, (int)strlen(key), &pVal, &nVal);
      if( rc != KVSTORE_OK ){
        printf("  Missing key from cycle %d: %s\n", cycle + 1, key);
        kvstore_close(kv);
        TEST_FAIL("data lost across vacuum cycles");
      }
      sqliteFree(pVal);
    }
  }

  printf("  All 600 records (200 per cycle) intact across 3 vacuum cycles\n");

  char *zErr = NULL;
  rc = kvstore_integrity_check(kv, &zErr);
  if( rc != KVSTORE_OK ){
    printf("  Integrity check failed: %s\n", zErr ? zErr : "unknown");
    if( zErr ) sqliteFree(zErr);
    kvstore_close(kv);
    TEST_FAIL("integrity check failed");
  }
  if( zErr ) sqliteFree(zErr);

  kvstore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

int main(void){
  printf("=== SNKV Auto-Vacuum Test Suite ===\n\n");

  test_incremental_vacuum();
  test_partial_vacuum();
  test_vacuum_wal_mode();
  test_vacuum_integrity();
  test_multiple_vacuum_cycles();

  printf("=== Results: %d/%d passed", tests_passed, tests_run);
  if( tests_failed > 0 ){
    printf(", " COLOR_RED "%d failed" COLOR_RESET, tests_failed);
  }
  printf(" ===\n");

  cleanup_db(TEST_DB);

  return tests_failed > 0 ? 1 : 0;
}
