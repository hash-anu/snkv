/*
** Auto-Vacuum Test Suite for KeyValueStore
**
** All databases are opened with incremental auto-vacuum by default.
** Users call keyvaluestore_incremental_vacuum() to reclaim unused pages.
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
#include "keyvaluestore.h"

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
  if( (rc) != KEYVALUESTORE_OK ) { \
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
static int insert_records(KeyValueStore *kv, int start, int count){
  int rc;
  char key[32];
  char value[128];

  rc = keyvaluestore_begin(kv, 1);
  if( rc != KEYVALUESTORE_OK ) return rc;

  for( int i = start; i < start + count; i++ ){
    snprintf(key, sizeof(key), "key-%06d", i);
    memset(value, 'A' + (i % 26), 100);
    value[100] = '\0';
    rc = keyvaluestore_put(kv, key, (int)strlen(key), value, 101);
    if( rc != KEYVALUESTORE_OK ){
      keyvaluestore_rollback(kv);
      return rc;
    }
  }

  return keyvaluestore_commit(kv);
}

/* Delete N key-value pairs */
static int delete_records(KeyValueStore *kv, int start, int count){
  int rc;
  char key[32];

  rc = keyvaluestore_begin(kv, 1);
  if( rc != KEYVALUESTORE_OK ) return rc;

  for( int i = start; i < start + count; i++ ){
    snprintf(key, sizeof(key), "key-%06d", i);
    rc = keyvaluestore_delete(kv, key, (int)strlen(key));
    if( rc != KEYVALUESTORE_OK && rc != KEYVALUESTORE_NOTFOUND ){
      keyvaluestore_rollback(kv);
      return rc;
    }
  }

  return keyvaluestore_commit(kv);
}

/*
** Test 1: Incremental vacuum reclaims space after deletes
*/
static void test_incremental_vacuum(void){
  KeyValueStore *kv = NULL;
  int rc;
  long size_after_insert, size_after_delete, size_after_vacuum;

  TEST_START("Incremental vacuum reclaims space after deletes");
  cleanup_db(TEST_DB);

  rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "open");

  /* Insert 2000 records */
  rc = insert_records(kv, 0, 2000);
  ASSERT_OK(rc, "insert 2000 records");
  keyvaluestore_close(kv);

  size_after_insert = get_file_size(TEST_DB);
  printf("  File size after insert: %ld bytes\n", size_after_insert);

  /* Delete most records -- file should NOT shrink yet (incremental mode) */
  rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "reopen");

  rc = delete_records(kv, 0, 1800);
  ASSERT_OK(rc, "delete 1800 records");
  keyvaluestore_close(kv);

  size_after_delete = get_file_size(TEST_DB);
  printf("  File size after delete (no vacuum): %ld bytes\n", size_after_delete);

  /* Now run incremental vacuum to reclaim space */
  rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "reopen for vacuum");

  rc = keyvaluestore_incremental_vacuum(kv, 0);  /* 0 = free all */
  ASSERT_OK(rc, "incremental vacuum");
  keyvaluestore_close(kv);

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
  KeyValueStore *kv = NULL;
  int rc;
  long size_before, size_partial, size_full;

  TEST_START("Partial vacuum steps (nPage > 0)");
  cleanup_db(TEST_DB);

  rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "open");

  rc = insert_records(kv, 0, 2000);
  ASSERT_OK(rc, "insert");

  rc = delete_records(kv, 0, 1800);
  ASSERT_OK(rc, "delete");
  keyvaluestore_close(kv);

  size_before = get_file_size(TEST_DB);
  printf("  File size before vacuum: %ld bytes\n", size_before);

  /* Vacuum only 10 pages */
  rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "reopen");

  rc = keyvaluestore_incremental_vacuum(kv, 10);
  ASSERT_OK(rc, "vacuum 10 pages");
  keyvaluestore_close(kv);

  size_partial = get_file_size(TEST_DB);
  printf("  File size after 10-page vacuum: %ld bytes\n", size_partial);

  /* Vacuum remaining */
  rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "reopen");

  rc = keyvaluestore_incremental_vacuum(kv, 0);
  ASSERT_OK(rc, "vacuum all remaining");
  keyvaluestore_close(kv);

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
  KeyValueStore *kv = NULL;
  int rc;
  long size_after_insert, size_after_vacuum;

  TEST_START("Incremental vacuum with WAL journal mode");
  cleanup_db(TEST_DB);

  rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_WAL);
  ASSERT_OK(rc, "open WAL");

  rc = insert_records(kv, 0, 2000);
  ASSERT_OK(rc, "insert 2000");
  keyvaluestore_close(kv);

  size_after_insert = get_file_size(TEST_DB);
  printf("  File size after insert: %ld bytes\n", size_after_insert);

  rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_WAL);
  ASSERT_OK(rc, "reopen");

  rc = delete_records(kv, 0, 1800);
  ASSERT_OK(rc, "delete 1800");

  rc = keyvaluestore_incremental_vacuum(kv, 0);
  ASSERT_OK(rc, "incremental vacuum");
  keyvaluestore_close(kv);

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
  KeyValueStore *kv = NULL;
  int rc;

  TEST_START("Data integrity preserved after vacuum");
  cleanup_db(TEST_DB);

  rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "open");

  rc = insert_records(kv, 0, 2000);
  ASSERT_OK(rc, "insert 2000 records");

  rc = delete_records(kv, 0, 1800);
  ASSERT_OK(rc, "delete 1800 records");

  rc = keyvaluestore_incremental_vacuum(kv, 0);
  ASSERT_OK(rc, "vacuum");
  keyvaluestore_close(kv);

  /* Reopen and verify remaining 200 records */
  rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "reopen for verify");

  for( int i = 1800; i < 2000; i++ ){
    char key[32];
    void *pVal = NULL;
    int nVal = 0;
    snprintf(key, sizeof(key), "key-%06d", i);
    rc = keyvaluestore_get(kv, key, (int)strlen(key), &pVal, &nVal);
    if( rc != KEYVALUESTORE_OK ){
      printf("  Missing key: %s\n", key);
      keyvaluestore_close(kv);
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
    rc = keyvaluestore_get(kv, key, (int)strlen(key), &pVal, &nVal);
    if( rc != KEYVALUESTORE_NOTFOUND ){
      printf("  Deleted key still present: %s\n", key);
      if( pVal ) sqliteFree(pVal);
      keyvaluestore_close(kv);
      TEST_FAIL("deleted key still exists after vacuum");
    }
  }

  /* Integrity check */
  char *zErr = NULL;
  rc = keyvaluestore_integrity_check(kv, &zErr);
  if( rc != KEYVALUESTORE_OK ){
    printf("  Integrity check failed: %s\n", zErr ? zErr : "unknown");
    if( zErr ) sqliteFree(zErr);
    keyvaluestore_close(kv);
    TEST_FAIL("integrity check failed after vacuum");
  }
  if( zErr ) sqliteFree(zErr);
  printf("  All 200 remaining records intact, deleted keys confirmed gone\n");
  printf("  Integrity check passed\n");

  keyvaluestore_close(kv);
  cleanup_db(TEST_DB);
  TEST_PASS();
}

/*
** Test 5: Multiple vacuum cycles (insert/delete/vacuum repeated)
*/
static void test_multiple_vacuum_cycles(void){
  KeyValueStore *kv = NULL;
  int rc;
  long sizes[4];

  TEST_START("Multiple vacuum cycles");
  cleanup_db(TEST_DB);

  rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "open");

  for( int cycle = 0; cycle < 3; cycle++ ){
    int base = cycle * 1000;

    rc = insert_records(kv, base, 1000);
    ASSERT_OK(rc, "insert");

    rc = delete_records(kv, base, 800);
    ASSERT_OK(rc, "delete");

    rc = keyvaluestore_incremental_vacuum(kv, 0);
    ASSERT_OK(rc, "vacuum");

    keyvaluestore_close(kv);
    sizes[cycle] = get_file_size(TEST_DB);
    printf("  Cycle %d: file size = %ld bytes\n", cycle + 1, sizes[cycle]);

    rc = keyvaluestore_open(TEST_DB, &kv, KEYVALUESTORE_JOURNAL_DELETE);
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
      rc = keyvaluestore_get(kv, key, (int)strlen(key), &pVal, &nVal);
      if( rc != KEYVALUESTORE_OK ){
        printf("  Missing key from cycle %d: %s\n", cycle + 1, key);
        keyvaluestore_close(kv);
        TEST_FAIL("data lost across vacuum cycles");
      }
      sqliteFree(pVal);
    }
  }

  printf("  All 600 records (200 per cycle) intact across 3 vacuum cycles\n");

  char *zErr = NULL;
  rc = keyvaluestore_integrity_check(kv, &zErr);
  if( rc != KEYVALUESTORE_OK ){
    printf("  Integrity check failed: %s\n", zErr ? zErr : "unknown");
    if( zErr ) sqliteFree(zErr);
    keyvaluestore_close(kv);
    TEST_FAIL("integrity check failed");
  }
  if( zErr ) sqliteFree(zErr);

  keyvaluestore_close(kv);
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
