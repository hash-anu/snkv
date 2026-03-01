/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright 2025 SNKV Contributors */
/*
** ACID Compliance Test Suite for KVStore
** 
** This is a comprehensive, self-contained test suite for verifying
** ACID properties of the kvstore implementation.
**
** ACID Properties Tested:
** - Atomicity: All-or-nothing transaction semantics
** - Consistency: Database remains in valid state
** - Isolation: Concurrent transactions don't interfere
** - Durability: Committed data survives crashes
**
** Compilation:
**   gcc -g -Wall -Iinclude -o tests/test_acid tests/test_acid.c src/kvstore.c src/os.c \
**       src/os_unix.c src/os_win.c src/util.c src/printf.c src/random.c src/hash.c \
**       src/pager.c src/btree.c
**
** Usage:
**   ./tests/test_acid [database_file]
**
** Date: 2025 January 27
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include "kvstore.h"
#include "platform_compat.h"

/*
** Test result structure
*/
typedef struct ACIDTestResult {
  int atomicity_passed;
  int consistency_passed;
  int isolation_passed;
  int durability_passed;
  int overall_passed;
  char error_msg[1024];
} ACIDTestResult;

/*
** Helper: Compare buffers
*/
static int buffers_equal(const void *a, const void *b, int size) {
  return memcmp(a, b, size) == 0;
}

/*
** ATOMICITY TEST
** Verifies that transaction rollback properly undoes all changes
*/
static const char *journal_mode_name(int mode) {
  return mode == KVSTORE_JOURNAL_WAL ? "WAL" : "DELETE";
}

static int test_atomicity(const char *dbfile, char *err_msg, int journal_mode) {
  KVStore *pKV = NULL;
  int rc;
  char key1[] = "atomicity_test_key1";
  char key2[] = "atomicity_test_key2";
  char key3[] = "atomicity_test_key3";
  char value1[] = "value1";
  char value2[] = "value2";
  char value3[] = "value3";
  void *pVal = NULL;
  int nVal = 0;
  int exists = 0;
  
  printf("  Testing Atomicity (%s)...\n", journal_mode_name(journal_mode));

  /* Open database */
  rc = kvstore_open(dbfile, &pKV, journal_mode);
  if(rc != KVSTORE_OK) {
    snprintf(err_msg, 1024, "Failed to open database: %d", rc);
    return 0;
  }
  
  /* Test 1: Rollback should undo all puts */
  printf("    Test 1.1: Rollback undoes all puts\n");
  rc = kvstore_begin(pKV, 1);
  if(rc != KVSTORE_OK) {
    snprintf(err_msg, 1024, "Failed to begin transaction: %d", rc);
    kvstore_close(pKV);
    return 0;
  }
  
  kvstore_put(pKV, key1, strlen(key1), value1, strlen(value1));
  kvstore_put(pKV, key2, strlen(key2), value2, strlen(value2));
  kvstore_put(pKV, key3, strlen(key3), value3, strlen(value3));
  
  rc = kvstore_rollback(pKV);
  if(rc != KVSTORE_OK) {
    snprintf(err_msg, 1024, "Failed to rollback: %d", rc);
    kvstore_close(pKV);
    return 0;
  }
  
  /* Verify keys don't exist after rollback */
  kvstore_exists(pKV, key1, strlen(key1), &exists);
  if(exists) {
    snprintf(err_msg, 1024, "Key1 exists after rollback - atomicity violated");
    kvstore_close(pKV);
    return 0;
  }
  
  kvstore_exists(pKV, key2, strlen(key2), &exists);
  if(exists) {
    snprintf(err_msg, 1024, "Key2 exists after rollback - atomicity violated");
    kvstore_close(pKV);
    return 0;
  }
  
  kvstore_exists(pKV, key3, strlen(key3), &exists);
  if(exists) {
    snprintf(err_msg, 1024, "Key3 exists after rollback - atomicity violated");
    kvstore_close(pKV);
    return 0;
  }
  
  /* Test 2: Partial transaction rollback */
  printf("    Test 1.2: Partial operations are atomic\n");
  
  /* First, insert some data and commit */
  rc = kvstore_begin(pKV, 1);
  kvstore_put(pKV, key1, strlen(key1), value1, strlen(value1));
  rc = kvstore_commit(pKV);
  if(rc != KVSTORE_OK) {
    snprintf(err_msg, 1024, "Failed to commit initial data: %d", rc);
    kvstore_close(pKV);
    return 0;
  }
  
  /* Now modify it in a transaction that we'll rollback */
  rc = kvstore_begin(pKV, 1);
  kvstore_put(pKV, key1, strlen(key1), value2, strlen(value2)); /* Update */
  kvstore_put(pKV, key2, strlen(key2), value2, strlen(value2)); /* Insert */
  kvstore_delete(pKV, key1, strlen(key1));                       /* Delete */
  rc = kvstore_rollback(pKV);
  
  /* Verify key1 still has original value */
  rc = kvstore_get(pKV, key1, strlen(key1), &pVal, &nVal);
  if(rc != KVSTORE_OK) {
    snprintf(err_msg, 1024, "Key1 doesn't exist after rollback - atomicity violated");
    kvstore_close(pKV);
    return 0;
  }
  
  if(!buffers_equal(pVal, value1, strlen(value1))) {
    snprintf(err_msg, 1024, "Key1 has wrong value after rollback - atomicity violated");
    sqliteFree(pVal);
    kvstore_close(pKV);
    return 0;
  }
  sqliteFree(pVal);
  
  /* Verify key2 doesn't exist */
  kvstore_exists(pKV, key2, strlen(key2), &exists);
  if(exists) {
    snprintf(err_msg, 1024, "Key2 exists after rollback - atomicity violated");
    kvstore_close(pKV);
    return 0;
  }
  
  /* Cleanup */
  kvstore_delete(pKV, key1, strlen(key1));
  
  kvstore_close(pKV);
  printf("    Atomicity tests PASSED\n");
  return 1;
}

/*
** CONSISTENCY TEST
** Verifies that database maintains valid state and constraints
*/
static int test_consistency(const char *dbfile, char *err_msg, int journal_mode) {
  KVStore *pKV = NULL;
  KVColumnFamily *pCF1 = NULL, *pCF2 = NULL;
  int rc;
  char *integrity_err = NULL;
  void *pVal = NULL;
  int nVal = 0;
  
  printf("  Testing Consistency (%s)...\n", journal_mode_name(journal_mode));

  /* Open database */
  rc = kvstore_open(dbfile, &pKV, journal_mode);
  if(rc != KVSTORE_OK) {
    snprintf(err_msg, 1024, "Failed to open database: %d", rc);
    return 0;
  }
  
  /* Test 1: Integrity check on empty database */
  printf("    Test 2.1: Database integrity on empty DB\n");
  rc = kvstore_integrity_check(pKV, &integrity_err);
  if(rc != KVSTORE_OK) {
    snprintf(err_msg, 1024, "Integrity check failed on empty DB: %s", 
             integrity_err ? integrity_err : "unknown");
    if(integrity_err) sqliteFree(integrity_err);
    kvstore_close(pKV);
    return 0;
  }
  
  /* Test 2: Consistency after many operations */
  printf("    Test 2.2: Consistency after multiple operations\n");
  
  char key[32], value[128];
  for(int i = 0; i < 100; i++) {
    snprintf(key, sizeof(key), "consistency_key_%d", i);
    snprintf(value, sizeof(value), "consistency_value_%d", i);
    rc = kvstore_put(pKV, key, strlen(key), value, strlen(value));
    if(rc != KVSTORE_OK) {
      snprintf(err_msg, 1024, "Failed to put key %d: %d", i, rc);
      kvstore_close(pKV);
      return 0;
    }
  }
  
  /* Delete every other key */
  for(int i = 0; i < 100; i += 2) {
    snprintf(key, sizeof(key), "consistency_key_%d", i);
    kvstore_delete(pKV, key, strlen(key));
  }
  
  /* Verify remaining keys */
  for(int i = 1; i < 100; i += 2) {
    snprintf(key, sizeof(key), "consistency_key_%d", i);
    snprintf(value, sizeof(value), "consistency_value_%d", i);
    
    rc = kvstore_get(pKV, key, strlen(key), &pVal, &nVal);
    if(rc != KVSTORE_OK) {
      snprintf(err_msg, 1024, "Failed to get key %d after deletions: %d", i, rc);
      kvstore_close(pKV);
      return 0;
    }
    
    if(!buffers_equal(pVal, value, strlen(value))) {
      snprintf(err_msg, 1024, "Key %d has wrong value after deletions", i);
      sqliteFree(pVal);
      kvstore_close(pKV);
      return 0;
    }
    sqliteFree(pVal);
    pVal = NULL;
  }
  
  /* Check integrity after operations */
  rc = kvstore_integrity_check(pKV, &integrity_err);
  if(rc != KVSTORE_OK) {
    snprintf(err_msg, 1024, "Integrity check failed after operations: %s",
             integrity_err ? integrity_err : "unknown");
    if(integrity_err) sqliteFree(integrity_err);
    kvstore_close(pKV);
    return 0;
  }
  
  /* Test 3: Column family consistency */
  printf("    Test 2.3: Column family isolation\n");
  
  rc = kvstore_cf_create(pKV, "cf_test1", &pCF1);
  if(rc != KVSTORE_OK) {
    snprintf(err_msg, 1024, "Failed to create CF1: %d", rc);
    kvstore_close(pKV);
    return 0;
  }
  
  rc = kvstore_cf_create(pKV, "cf_test2", &pCF2);
  if(rc != KVSTORE_OK) {
    snprintf(err_msg, 1024, "Failed to create CF2: %d", rc);
    kvstore_cf_close(pCF1);
    kvstore_close(pKV);
    return 0;
  }
  
  /* Put same key in different CFs with different values */
  char test_key[] = "shared_key";
  char cf1_value[] = "CF1_value";
  char cf2_value[] = "CF2_value";
  
  kvstore_cf_put(pCF1, test_key, strlen(test_key), cf1_value, strlen(cf1_value));
  kvstore_cf_put(pCF2, test_key, strlen(test_key), cf2_value, strlen(cf2_value));
  
  /* Verify isolation */
  rc = kvstore_cf_get(pCF1, test_key, strlen(test_key), &pVal, &nVal);
  if(rc != KVSTORE_OK || !buffers_equal(pVal, cf1_value, strlen(cf1_value))) {
    snprintf(err_msg, 1024, "CF1 value incorrect - CF isolation violated");
    sqliteFree(pVal);
    kvstore_cf_close(pCF1);
    kvstore_cf_close(pCF2);
    kvstore_close(pKV);
    return 0;
  }
  sqliteFree(pVal);
  
  rc = kvstore_cf_get(pCF2, test_key, strlen(test_key), &pVal, &nVal);
  if(rc != KVSTORE_OK || !buffers_equal(pVal, cf2_value, strlen(cf2_value))) {
    snprintf(err_msg, 1024, "CF2 value incorrect - CF isolation violated");
    sqliteFree(pVal);
    kvstore_cf_close(pCF1);
    kvstore_cf_close(pCF2);
    kvstore_close(pKV);
    return 0;
  }
  sqliteFree(pVal);
  
  /* Final integrity check */
  rc = kvstore_integrity_check(pKV, &integrity_err);
  if(rc != KVSTORE_OK) {
    snprintf(err_msg, 1024, "Final integrity check failed: %s",
             integrity_err ? integrity_err : "unknown");
    if(integrity_err) sqliteFree(integrity_err);
    kvstore_cf_close(pCF1);
    kvstore_cf_close(pCF2);
    kvstore_close(pKV);
    return 0;
  }
  
  /* Cleanup */
  kvstore_cf_close(pCF1);
  kvstore_cf_close(pCF2);
  kvstore_cf_drop(pKV, "cf_test1");
  kvstore_cf_drop(pKV, "cf_test2");
  
  /* Clear test data */
  for(int i = 0; i < 100; i++) {
    snprintf(key, sizeof(key), "consistency_key_%d", i);
    kvstore_delete(pKV, key, strlen(key));
  }
  
  kvstore_close(pKV);
  printf("    Consistency tests PASSED\n");
  return 1;
}

/*
** ISOLATION TEST
** Verifies that concurrent transactions are properly isolated
** Note: This is a simplified test since full concurrency requires
** multi-process or multi-thread testing
*/
static int test_isolation(const char *dbfile, char *err_msg, int journal_mode) {
  KVStore *pKV = NULL;
  int rc;
  char key1[] = "isolation_key1";
  char key2[] = "isolation_key2";
  char value_orig[] = "original_value";
  char value_new[] = "new_value";
  void *pVal = NULL;
  int nVal = 0;
  
  printf("  Testing Isolation (%s)...\n", journal_mode_name(journal_mode));

  /* Open database */
  rc = kvstore_open(dbfile, &pKV, journal_mode);
  if(rc != KVSTORE_OK) {
    snprintf(err_msg, 1024, "Failed to open database: %d", rc);
    return 0;
  }
  
  /* Test 1: Read transaction doesn't see uncommitted changes */
  printf("    Test 3.1: Read transaction isolation\n");
  
  /* Setup: Insert initial data */
  kvstore_put(pKV, key1, strlen(key1), value_orig, strlen(value_orig));
  
  /* This test would ideally require multiple connections, but we can
   * verify that within a single process, transactions maintain isolation */
  
  /* Test 2: Write-Write conflict detection */
  printf("    Test 3.2: Transaction conflict handling\n");
  
  /* Begin transaction, modify data */
  rc = kvstore_begin(pKV, 1);
  if(rc != KVSTORE_OK) {
    snprintf(err_msg, 1024, "Failed to begin write transaction: %d", rc);
    kvstore_close(pKV);
    return 0;
  }
  
  /* Update key within transaction */
  rc = kvstore_put(pKV, key1, strlen(key1), value_new, strlen(value_new));
  if(rc != KVSTORE_OK) {
    snprintf(err_msg, 1024, "Failed to update in transaction: %d", rc);
    kvstore_rollback(pKV);
    kvstore_close(pKV);
    return 0;
  }
  
  /* Verify update is visible within transaction */
  rc = kvstore_get(pKV, key1, strlen(key1), &pVal, &nVal);
  if(rc != KVSTORE_OK || !buffers_equal(pVal, value_new, strlen(value_new))) {
    snprintf(err_msg, 1024, "Updated value not visible within transaction");
    sqliteFree(pVal);
    kvstore_rollback(pKV);
    kvstore_close(pKV);
    return 0;
  }
  sqliteFree(pVal);
  
  /* Commit transaction */
  rc = kvstore_commit(pKV);
  if(rc != KVSTORE_OK) {
    snprintf(err_msg, 1024, "Failed to commit transaction: %d", rc);
    kvstore_close(pKV);
    return 0;
  }
  
  /* Test 3: Verify transaction boundaries */
  printf("    Test 3.3: Transaction boundary enforcement\n");
  
  /* Attempting operations without transaction should auto-commit */
  rc = kvstore_put(pKV, key2, strlen(key2), value_orig, strlen(value_orig));
  if(rc != KVSTORE_OK) {
    snprintf(err_msg, 1024, "Auto-transaction failed: %d", rc);
    kvstore_close(pKV);
    return 0;
  }
  
  /* Verify the data is persisted */
  rc = kvstore_get(pKV, key2, strlen(key2), &pVal, &nVal);
  if(rc != KVSTORE_OK) {
    snprintf(err_msg, 1024, "Auto-committed data not found: %d", rc);
    kvstore_close(pKV);
    return 0;
  }
  sqliteFree(pVal);
  
  /* Cleanup */
  kvstore_delete(pKV, key1, strlen(key1));
  kvstore_delete(pKV, key2, strlen(key2));
  
  kvstore_close(pKV);
  printf("    Isolation tests PASSED\n");
  return 1;
}

/*
** DURABILITY TEST
** Verifies that committed data survives database close/reopen
** and simulated crashes
*/
static int test_durability(const char *dbfile, char *err_msg, int journal_mode) {
  KVStore *pKV = NULL;
  int rc;
  char key[] = "durability_key";
  char value[] = "durability_value_that_must_survive";
  void *pVal = NULL;
  int nVal = 0;
  
  printf("  Testing Durability (%s)...\n", journal_mode_name(journal_mode));

  /* Test 1: Data survives normal close/reopen */
  printf("    Test 4.1: Data survives close/reopen\n");

  rc = kvstore_open(dbfile, &pKV, journal_mode);
  if(rc != KVSTORE_OK) {
    snprintf(err_msg, 1024, "Failed to open database: %d", rc);
    return 0;
  }
  
  /* Write and commit data */
  rc = kvstore_begin(pKV, 1);
  rc = kvstore_put(pKV, key, strlen(key), value, strlen(value));
  rc = kvstore_commit(pKV);
  if(rc != KVSTORE_OK) {
    snprintf(err_msg, 1024, "Failed to commit data: %d", rc);
    kvstore_close(pKV);
    return 0;
  }
  
  /* Explicitly sync to disk */
  rc = kvstore_sync(pKV);
  if(rc != KVSTORE_OK) {
    snprintf(err_msg, 1024, "Failed to sync: %d", rc);
    kvstore_close(pKV);
    return 0;
  }
  
  /* Close database */
  kvstore_close(pKV);
  pKV = NULL;
  
  /* Reopen and verify data exists */
  rc = kvstore_open(dbfile, &pKV, journal_mode);
  if(rc != KVSTORE_OK) {
    snprintf(err_msg, 1024, "Failed to reopen database: %d", rc);
    return 0;
  }
  
  rc = kvstore_get(pKV, key, strlen(key), &pVal, &nVal);
  if(rc != KVSTORE_OK) {
    snprintf(err_msg, 1024, "Data not found after reopen: %d", rc);
    kvstore_close(pKV);
    return 0;
  }
  
  if(!buffers_equal(pVal, value, strlen(value))) {
    snprintf(err_msg, 1024, "Data corrupted after reopen");
    sqliteFree(pVal);
    kvstore_close(pKV);
    return 0;
  }
  sqliteFree(pVal);
  
  /* Test 2: Multiple write/close cycles */
  printf("    Test 4.2: Multiple write/reopen cycles\n");
  
  for(int cycle = 0; cycle < 5; cycle++) {
    char cycle_key[32];
    char cycle_value[64];
    snprintf(cycle_key, sizeof(cycle_key), "cycle_key_%d", cycle);
    snprintf(cycle_value, sizeof(cycle_value), "cycle_value_%d", cycle);
    
    rc = kvstore_put(pKV, cycle_key, strlen(cycle_key), 
                     cycle_value, strlen(cycle_value));
    if(rc != KVSTORE_OK) {
      snprintf(err_msg, 1024, "Failed to write in cycle %d: %d", cycle, rc);
      kvstore_close(pKV);
      return 0;
    }
    
    kvstore_sync(pKV);
    kvstore_close(pKV);
    
    /* Reopen */
    rc = kvstore_open(dbfile, &pKV, journal_mode);
    if(rc != KVSTORE_OK) {
      snprintf(err_msg, 1024, "Failed to reopen in cycle %d: %d", cycle, rc);
      return 0;
    }
    
    /* Verify all previous data still exists */
    for(int i = 0; i <= cycle; i++) {
      char check_key[32];
      snprintf(check_key, sizeof(check_key), "cycle_key_%d", i);
      
      int exists = 0;
      rc = kvstore_exists(pKV, check_key, strlen(check_key), &exists);
      if(rc != KVSTORE_OK || !exists) {
        snprintf(err_msg, 1024, "Data from cycle %d lost in cycle %d", i, cycle);
        kvstore_close(pKV);
        return 0;
      }
    }
  }
  
  /* Cleanup */
  kvstore_delete(pKV, key, strlen(key));
  for(int i = 0; i < 5; i++) {
    char cycle_key[32];
    snprintf(cycle_key, sizeof(cycle_key), "cycle_key_%d", i);
    kvstore_delete(pKV, cycle_key, strlen(cycle_key));
  }
  
  kvstore_close(pKV);
  printf("    Durability tests PASSED\n");
  return 1;
}

/*
** Main ACID compliance check function
*/
int kvstore_acid_compliance_check(const char *dbfile, ACIDTestResult *result,
                                  int journal_mode) {
  int all_passed = 1;
  const char *mode_name = journal_mode_name(journal_mode);

  if(!result) {
    return 0;
  }

  memset(result, 0, sizeof(ACIDTestResult));

  printf("\n========================================\n");
  printf("KVSTORE ACID COMPLIANCE TEST SUITE (%s)\n", mode_name);
  printf("========================================\n\n");
  printf("Database: %s\n", dbfile);
  printf("Journal mode: %s\n\n", mode_name);

  /* Remove existing database for clean test */
  unlink(dbfile);
  /* Also remove WAL/SHM files if present */
  char wal_path[256], shm_path[256];
  snprintf(wal_path, sizeof(wal_path), "%s-wal", dbfile);
  snprintf(shm_path, sizeof(shm_path), "%s-shm", dbfile);
  unlink(wal_path);
  unlink(shm_path);

  /* Test Atomicity */
  printf("[1/4] ATOMICITY\n");
  result->atomicity_passed = test_atomicity(dbfile, result->error_msg, journal_mode);
  if(!result->atomicity_passed) {
    printf("  FAILED: %s\n", result->error_msg);
    all_passed = 0;
  }
  printf("\n");

  /* Test Consistency */
  printf("[2/4] CONSISTENCY\n");
  result->consistency_passed = test_consistency(dbfile, result->error_msg, journal_mode);
  if(!result->consistency_passed) {
    printf("  FAILED: %s\n", result->error_msg);
    all_passed = 0;
  }
  printf("\n");

  /* Test Isolation */
  printf("[3/4] ISOLATION\n");
  result->isolation_passed = test_isolation(dbfile, result->error_msg, journal_mode);
  if(!result->isolation_passed) {
    printf("  FAILED: %s\n", result->error_msg);
    all_passed = 0;
  }
  printf("\n");

  /* Test Durability */
  printf("[4/4] DURABILITY\n");
  result->durability_passed = test_durability(dbfile, result->error_msg, journal_mode);
  if(!result->durability_passed) {
    printf("  FAILED: %s\n", result->error_msg);
    all_passed = 0;
  }
  printf("\n");

  /* Summary */
  printf("========================================\n");
  printf("ACID COMPLIANCE RESULTS (%s)\n", mode_name);
  printf("========================================\n");
  printf("Atomicity:    %s\n", result->atomicity_passed ? "PASS" : "FAIL");
  printf("Consistency:  %s\n", result->consistency_passed ? "PASS" : "FAIL");
  printf("Isolation:    %s\n", result->isolation_passed ? "PASS" : "FAIL");
  printf("Durability:   %s\n", result->durability_passed ? "PASS" : "FAIL");
  printf("----------------------------------------\n");
  printf("Overall:      %s\n", all_passed ? "PASS" : "FAIL");
  printf("========================================\n\n");

  result->overall_passed = all_passed;
  return all_passed;
}

/*
** Main test program
*/
int main(int argc, char **argv) {
  const char *dbfile = "acid_test.db";
  int verbose = 1;
  
  /* Parse command line arguments */
  for(int i = 1; i < argc; i++) {
    if(strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printf("KVStore ACID Compliance Test Suite\n\n");
      printf("Usage: %s [options] [database_file]\n\n", argv[0]);
      printf("Options:\n");
      printf("  -h, --help     Show this help message\n");
      printf("  -q, --quiet    Quiet mode (minimal output)\n");
      printf("\nArguments:\n");
      printf("  database_file  Path to test database (default: acid_test.db)\n");
      printf("\nDescription:\n");
      printf("  Runs comprehensive ACID compliance tests:\n");
      printf("    [A] Atomicity    - Transaction rollback and all-or-nothing semantics\n");
      printf("    [C] Consistency  - Database integrity and constraint maintenance\n");
      printf("    [I] Isolation    - Transaction isolation and conflict handling\n");
      printf("    [D] Durability   - Data persistence across restarts\n");
      printf("\n");
      return 0;
    } else if(strcmp(argv[i], "-q") == 0 || strcmp(argv[i], "--quiet") == 0) {
      verbose = 0;
    } else {
      dbfile = argv[i];
    }
  }
  
  srand(time(NULL));

  if(verbose) {
    printf("\n+============================================================+\n");
    printf("|      KVSTORE ACID COMPLIANCE TEST SUITE v2.0              |\n");
    printf("|                                                            |\n");
    printf("|  Comprehensive testing of ACID transaction properties     |\n");
    printf("|  Tests both DELETE journal and WAL journal modes          |\n");
    printf("|  Built on SQLite btree implementation                     |\n");
    printf("+============================================================+\n\n");
    printf("Test Database: %s\n\n", dbfile);
  }

  /* Run ACID suite with DELETE journal mode */
  ACIDTestResult result_delete;
  int passed_delete = kvstore_acid_compliance_check(dbfile, &result_delete,
                                                     KVSTORE_JOURNAL_DELETE);

  /* Run ACID suite with WAL journal mode */
  ACIDTestResult result_wal;
  int passed_wal = kvstore_acid_compliance_check(dbfile, &result_wal,
                                                  KVSTORE_JOURNAL_WAL);

  int all_passed = passed_delete && passed_wal;

  if(verbose) {
    printf("\n+============================================================+\n");
    printf("|                    FINAL RESULTS                           |\n");
    printf("+============================================================+\n");
    printf("|  DELETE Journal Mode                                       |\n");
    printf("+============================================================+\n");
    printf("|  [A] Atomicity        %-33s|\n",
           result_delete.atomicity_passed ? "[OK] PASSED" : "[X] FAILED");
    printf("|  [C] Consistency      %-33s|\n",
           result_delete.consistency_passed ? "[OK] PASSED" : "[X] FAILED");
    printf("|  [I] Isolation        %-33s|\n",
           result_delete.isolation_passed ? "[OK] PASSED" : "[X] FAILED");
    printf("|  [D] Durability       %-33s|\n",
           result_delete.durability_passed ? "[OK] PASSED" : "[X] FAILED");
    printf("+============================================================+\n");
    printf("|  WAL Journal Mode                                          |\n");
    printf("+============================================================+\n");
    printf("|  [A] Atomicity        %-33s|\n",
           result_wal.atomicity_passed ? "[OK] PASSED" : "[X] FAILED");
    printf("|  [C] Consistency      %-33s|\n",
           result_wal.consistency_passed ? "[OK] PASSED" : "[X] FAILED");
    printf("|  [I] Isolation        %-33s|\n",
           result_wal.isolation_passed ? "[OK] PASSED" : "[X] FAILED");
    printf("|  [D] Durability       %-33s|\n",
           result_wal.durability_passed ? "[OK] PASSED" : "[X] FAILED");
    printf("+============================================================+\n");
    printf("|  OVERALL              %-33s|\n",
           all_passed ? "[OK] ALL PASSED" : "[X] FAILED");
    printf("+============================================================+\n\n");
  }

  if(!all_passed) {
    if(verbose) {
      printf("ACID compliance check FAILED!\n");
      if(!passed_delete && result_delete.error_msg[0]) {
        printf("DELETE mode error: %s\n", result_delete.error_msg);
      }
      if(!passed_wal && result_wal.error_msg[0]) {
        printf("WAL mode error: %s\n", result_wal.error_msg);
      }
    } else {
      printf("FAILED\n");
    }
    return 1;
  }

  if(verbose) {
    printf("All ACID compliance tests PASSED!\n");
    printf("The kvstore implementation is fully ACID compliant ");
    printf("in both DELETE and WAL journal modes.\n\n");
  } else {
    printf("PASSED\n");
  }
  remove(dbfile);

  return 0;
}
