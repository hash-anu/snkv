/*
** Column Family Test Suite for KVStore
** 
** Demonstrates and tests column family functionality
*/

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "kvstore.h"

#define TEST_DB "cf_test.db"

#define COLOR_GREEN  "\x1b[32m"
#define COLOR_RED    "\x1b[31m"
#define COLOR_BLUE   "\x1b[34m"
#define COLOR_RESET  "\x1b[0m"

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) \
  printf(COLOR_BLUE "\nTEST: %s" COLOR_RESET "\n", name)

#define PASS() \
  do { \
    tests_passed++; \
    printf(COLOR_GREEN "  ✓ PASSED" COLOR_RESET "\n"); \
  } while(0)

#define FAIL(msg) \
  do { \
    tests_failed++; \
    printf(COLOR_RED "  ✗ FAILED: %s" COLOR_RESET "\n", msg); \
    return; \
  } while(0)

#define ASSERT_OK(rc, msg) \
  if( (rc) != KVSTORE_OK ) { \
    printf("  Error: %s (code=%d)\n", msg, rc); \
    if( kv ) printf("  Details: %s\n", kvstore_errmsg(kv)); \
    FAIL(msg); \
  }

/*
** Test 1: Create and open column families
*/
static void test_cf_create_open(void) {
  TEST("Create and open column families");
  KVStore *kv = NULL;
  KVColumnFamily *cf_users = NULL;
  KVColumnFamily *cf_sessions = NULL;
  KVColumnFamily *cf_default = NULL;
  int rc;
  
  remove(TEST_DB);
  
  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "Failed to open database");
  
  /* Get default CF */
  rc = kvstore_cf_get_default(kv, &cf_default);
  ASSERT_OK(rc, "Failed to get default CF");
  printf("  ✓ Default CF opened\n");
  
  /* Create users CF */
  rc = kvstore_cf_create(kv, "users", &cf_users);
  ASSERT_OK(rc, "Failed to create users CF");
  printf("  ✓ Created 'users' CF\n");
  
  /* Create sessions CF */
  rc = kvstore_cf_create(kv, "sessions", &cf_sessions);
  ASSERT_OK(rc, "Failed to create sessions CF");
  printf("  ✓ Created 'sessions' CF\n");
  
  /* Try to create duplicate */
  KVColumnFamily *cf_dup = NULL;
  rc = kvstore_cf_create(kv, "users", &cf_dup);
  if( rc == KVSTORE_OK ){
    FAIL("Should not allow duplicate CF");
  }
  printf("  ✓ Duplicate CF prevented\n");
  
  /* Clean up */
  kvstore_cf_close(cf_users);
  kvstore_cf_close(cf_sessions);
  kvstore_cf_close(cf_default);
  kvstore_close(kv);
  
  /* Reopen and verify persistence */
  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "Failed to reopen database");
  
  rc = kvstore_cf_open(kv, "users", &cf_users);
  ASSERT_OK(rc, "Failed to open persisted users CF");
  printf("  ✓ CF persisted across sessions\n");
  
  kvstore_cf_close(cf_users);
  kvstore_close(kv);
  remove(TEST_DB);
  
  PASS();
}

/*
** Test 2: Put/Get in different CFs
*/
static void test_cf_isolation(void) {
  TEST("Column family data isolation");
  KVStore *kv = NULL;
  KVColumnFamily *cf_users = NULL;
  KVColumnFamily *cf_sessions = NULL;
  int rc, exists;
  void *val;
  int vlen;
  
  remove(TEST_DB);
  
  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "Failed to open database");
  
  rc = kvstore_cf_create(kv, "users", &cf_users);
  ASSERT_OK(rc, "Failed to create users CF");
  
  rc = kvstore_cf_create(kv, "sessions", &cf_sessions);
  ASSERT_OK(rc, "Failed to create sessions CF");
  
  /* Put same key in different CFs */
  rc = kvstore_cf_put(cf_users, "key1", 4, "user_value", 10);
  ASSERT_OK(rc, "Failed to put in users CF");
  
  rc = kvstore_cf_put(cf_sessions, "key1", 4, "session_value", 13);
  ASSERT_OK(rc, "Failed to put in sessions CF");
  
  /* Verify values are independent */
  rc = kvstore_cf_get(cf_users, "key1", 4, &val, &vlen);
  ASSERT_OK(rc, "Failed to get from users CF");
  if( vlen != 10 || memcmp(val, "user_value", 10) != 0 ){
    sqliteFree(val);
    FAIL("Wrong value from users CF");
  }
  printf("  ✓ users CF: key1 = user_value\n");
  sqliteFree(val);
  
  rc = kvstore_cf_get(cf_sessions, "key1", 4, &val, &vlen);
  ASSERT_OK(rc, "Failed to get from sessions CF");
  if( vlen != 13 || memcmp(val, "session_value", 13) != 0 ){
    sqliteFree(val);
    FAIL("Wrong value from sessions CF");
  }
  printf("  ✓ sessions CF: key1 = session_value\n");
  sqliteFree(val);
  
  /* Verify key doesn't exist in wrong CF */
  rc = kvstore_cf_put(cf_users, "users_only", 10, "data", 4);
  ASSERT_OK(rc, "Failed to put users_only");
  
  rc = kvstore_cf_exists(cf_users, "users_only", 10, &exists);
  ASSERT_OK(rc, "Failed exists check");
  if( !exists ){
    FAIL("Key should exist in users CF");
  }
  
  rc = kvstore_cf_exists(cf_sessions, "users_only", 10, &exists);
  ASSERT_OK(rc, "Failed exists check");
  if( exists ){
    FAIL("Key should not exist in sessions CF");
  }
  printf("  ✓ Keys properly isolated between CFs\n");
  
  kvstore_cf_close(cf_users);
  kvstore_cf_close(cf_sessions);
  kvstore_close(kv);
  remove(TEST_DB);
  
  PASS();
}

/*
** Test 3: List column families
*/
static void test_cf_list(void) {
  TEST("List column families");
  KVStore *kv = NULL;
  KVColumnFamily *cf;
  char **names = NULL;
  int count, rc, i;
  int found_logs = 0;
  
  remove(TEST_DB);
  
  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "Failed to open database");
  
  /* Create multiple CFs */
  const char *cf_names[] = {"analytics", "cache", "logs", "metrics"};
  for(i = 0; i < 4; i++){
    rc = kvstore_cf_create(kv, cf_names[i], &cf);
    ASSERT_OK(rc, "Failed to create CF");
    kvstore_cf_close(cf);
  }
  
  /* List all CFs */
  rc = kvstore_cf_list(kv, &names, &count);
  ASSERT_OK(rc, "Failed to list CFs");
  
  if( count != 4 ){
    FAIL("Expected 4 CFs before drop");
  }
  
  for(i = 0; i < count; i++){
    sqliteFree(names[i]);
  }
  sqliteFree(names);
  names = NULL;
  
  /* Drop one CF */
  rc = kvstore_cf_drop(kv, "logs");
  ASSERT_OK(rc, "Failed to drop CF 'logs'");
  
  /* List CFs again */
  rc = kvstore_cf_list(kv, &names, &count);
  ASSERT_OK(rc, "Failed to list CFs after drop");
  
  printf("  Found %d column families after drop:\n", count);
  
  for(i = 0; i < count; i++){
    printf("    - %s\n", names[i]);
    if( strcmp(names[i], "logs") == 0 ){
      found_logs = 1;
    }
    sqliteFree(names[i]);
  }
  sqliteFree(names);
  
  if( count != 3 ){
    FAIL("Expected 3 CFs after drop");
  }
  
  if( found_logs ){
    FAIL("Dropped CF 'logs' still present in list");
  }
  
  kvstore_close(kv);
  remove(TEST_DB);
  
  PASS();
}


/*
** Test 4: Iterator per CF
*/
static void test_cf_iterators(void) {
  TEST("Iterators per column family");
  KVStore *kv = NULL;
  KVColumnFamily *cf_a = NULL, *cf_b = NULL;
  KVIterator *it;
  int rc, i, count_a, count_b;
  char key[32], val[32];
  void *k, *v;
  int klen, vlen;
  
  remove(TEST_DB);
  
  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "Failed to open database");
  
  rc = kvstore_cf_create(kv, "cf_a", &cf_a);
  ASSERT_OK(rc, "Failed to create cf_a");
  
  rc = kvstore_cf_create(kv, "cf_b", &cf_b);
  ASSERT_OK(rc, "Failed to create cf_b");
  
  /* Populate cf_a with 5 items */
  for(i = 0; i < 5; i++){
    snprintf(key, sizeof(key), "a_key_%d", i);
    snprintf(val, sizeof(val), "a_val_%d", i);
    rc = kvstore_cf_put(cf_a, key, strlen(key), val, strlen(val));
    ASSERT_OK(rc, "Failed to put in cf_a");
  }
  
  /* Populate cf_b with 3 items */
  for(i = 0; i < 3; i++){
    snprintf(key, sizeof(key), "b_key_%d", i);
    snprintf(val, sizeof(val), "b_val_%d", i);
    rc = kvstore_cf_put(cf_b, key, strlen(key), val, strlen(val));
    ASSERT_OK(rc, "Failed to put in cf_b");
  }
  
  /* Iterate cf_a */
  rc = kvstore_cf_iterator_create(cf_a, &it);
  ASSERT_OK(rc, "Failed to create iterator for cf_a");
  
  rc = kvstore_iterator_first(it);
  ASSERT_OK(rc, "Failed to move to first");
  
  count_a = 0;
  while( !kvstore_iterator_eof(it) ){
    rc = kvstore_iterator_key(it, &k, &klen);
    ASSERT_OK(rc, "Failed to get key");
    rc = kvstore_iterator_value(it, &v, &vlen);
    ASSERT_OK(rc, "Failed to get value");
    count_a++;
    kvstore_iterator_next(it);
  }
  kvstore_iterator_close(it);
  
  if( count_a != 5 ){
    printf("  Expected 5 items in cf_a, got %d\n", count_a);
    FAIL("Wrong count in cf_a");
  }
  printf("  ✓ cf_a has %d items\n", count_a);
  
  /* Iterate cf_b */
  rc = kvstore_cf_iterator_create(cf_b, &it);
  ASSERT_OK(rc, "Failed to create iterator for cf_b");
  
  rc = kvstore_iterator_first(it);
  ASSERT_OK(rc, "Failed to move to first");
  
  count_b = 0;
  while( !kvstore_iterator_eof(it) ){
    kvstore_iterator_key(it, &k, &klen);
    kvstore_iterator_value(it, &v, &vlen);
    count_b++;
    kvstore_iterator_next(it);
  }
  kvstore_iterator_close(it);
  
  if( count_b != 3 ){
    printf("  Expected 3 items in cf_b, got %d\n", count_b);
    FAIL("Wrong count in cf_b");
  }
  printf("  ✓ cf_b has %d items\n", count_b);
  
  kvstore_cf_close(cf_a);
  kvstore_cf_close(cf_b);
  kvstore_close(kv);
  remove(TEST_DB);
  
  PASS();
}

/*
** Test 5: Transactions across CFs
*/
static void test_cf_transactions(void) {
  TEST("Transactions across column families");
  KVStore *kv = NULL;
  KVColumnFamily *cf1 = NULL, *cf2 = NULL;
  int rc, exists;
  
  remove(TEST_DB);
  
  rc = kvstore_open(TEST_DB, &kv, KVSTORE_JOURNAL_DELETE);
  ASSERT_OK(rc, "Failed to open database");
  
  rc = kvstore_cf_create(kv, "cf1", &cf1);
  ASSERT_OK(rc, "Failed to create cf1");
  
  rc = kvstore_cf_create(kv, "cf2", &cf2);
  ASSERT_OK(rc, "Failed to create cf2");
  
  /* Begin transaction and write to both CFs */
  rc = kvstore_begin(kv, 1);
  ASSERT_OK(rc, "Failed to begin transaction");
  
  rc = kvstore_cf_put(cf1, "key1", 4, "val1", 4);
  ASSERT_OK(rc, "Failed to put in cf1");
  
  rc = kvstore_cf_put(cf2, "key2", 4, "val2", 4);
  ASSERT_OK(rc, "Failed to put in cf2");
  
  /* Rollback */
  rc = kvstore_rollback(kv);
  ASSERT_OK(rc, "Failed to rollback");
  
  /* Verify nothing was written */
  rc = kvstore_cf_exists(cf1, "key1", 4, &exists);
  ASSERT_OK(rc, "Failed exists check");
  if( exists ){
    FAIL("key1 should not exist after rollback");
  }
  
  rc = kvstore_cf_exists(cf2, "key2", 4, &exists);
  ASSERT_OK(rc, "Failed exists check");
  if( exists ){
    FAIL("key2 should not exist after rollback");
  }
  printf("  ✓ Rollback works across CFs\n");
  
  /* Now commit */
  rc = kvstore_begin(kv, 1);
  ASSERT_OK(rc, "Failed to begin transaction");
  
  rc = kvstore_cf_put(cf1, "key1", 4, "val1", 4);
  ASSERT_OK(rc, "Failed to put in cf1");
  
  rc = kvstore_cf_put(cf2, "key2", 4, "val2", 4);
  ASSERT_OK(rc, "Failed to put in cf2");
  
  rc = kvstore_commit(kv);
  ASSERT_OK(rc, "Failed to commit");
  
  /* Verify both exist */
  rc = kvstore_cf_exists(cf1, "key1", 4, &exists);
  ASSERT_OK(rc, "Failed exists check");
  if( !exists ){
    FAIL("key1 should exist after commit");
  }
  
  rc = kvstore_cf_exists(cf2, "key2", 4, &exists);
  ASSERT_OK(rc, "Failed exists check");
  if( !exists ){
    FAIL("key2 should exist after commit");
  }
  printf("  ✓ Commit works across CFs\n");
  
  kvstore_cf_close(cf1);
  kvstore_cf_close(cf2);
  kvstore_close(kv);
  remove(TEST_DB);
  
  PASS();
}

/*
** Main
*/
int main(void) {
  printf("\n");
  printf("========================================\n");
  printf("Column Family Test Suite\n");
  printf("========================================\n");
  
  test_cf_create_open();
  test_cf_isolation();
  test_cf_list();
  test_cf_iterators();
  test_cf_transactions();
  
  printf("\n========================================\n");
  printf("Test Summary\n");
  printf("========================================\n");
  printf("Passed: " COLOR_GREEN "%d" COLOR_RESET "\n", tests_passed);
  if( tests_failed > 0 ){
    printf("Failed: " COLOR_RED "%d" COLOR_RESET "\n", tests_failed);
  }else{
    printf("Failed: 0\n");
  }
  printf("========================================\n\n");
  
  if( tests_failed == 0 ){
    printf(COLOR_GREEN "✓ All column family tests passed!" COLOR_RESET "\n\n");
    return 0;
  }else{
    printf(COLOR_RED "✗ Some tests failed!" COLOR_RESET "\n\n");
    return 1;
  }
}
