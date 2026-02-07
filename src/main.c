/*
** Example Usage of Production-Ready KVStore
** 
** This file demonstrates common usage patterns and best practices.
*/

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "kvstore.h"
#include "sqliteInt.h"

/*
** Example 1: Simple key-value operations
*/
void example_basic_usage(void) {
    KVStore *kv = NULL;
    int rc;
    void *value = NULL;
    int value_len;
    
    printf("\n=== Example 1: Basic Usage ===\n");
    
    /* Open database */
    rc = kvstore_open("example.db", &kv, 0, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Failed to open: %s\n", kvstore_errmsg(kv));
        return;
    }
    
    /* Store some data */
    rc = kvstore_put(kv, "username", 8, "john_doe", 8);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Put failed: %s\n", kvstore_errmsg(kv));
        kvstore_close(kv);
        return;
    }
    printf("Stored: username = john_doe\n");
    
    /* Retrieve data */
    rc = kvstore_get(kv, "username", 8, &value, &value_len);
    if (rc == KVSTORE_OK) {
        printf("Retrieved: username = %.*s\n", value_len, (char*)value);
        sqliteFree(value);
    } else if (rc == KVSTORE_NOTFOUND) {
        printf("Key not found\n");
    } else {
        fprintf(stderr, "Get failed: %s\n", kvstore_errmsg(kv));
    }
    
    /* Clean up */
    kvstore_close(kv);
}

/*
** Example 2: Using transactions for batch operations
*/
void example_transactions(void) {
    KVStore *kv = NULL;
    int rc, i;
    char key[32], value[64];
    
    printf("\n=== Example 2: Transactions ===\n");
    
    rc = kvstore_open("example.db", &kv, 0, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) return;
    
    /* Begin transaction */
    rc = kvstore_begin(kv, 1);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Begin failed: %s\n", kvstore_errmsg(kv));
        kvstore_close(kv);
        return;
    }
    
    /* Insert multiple records atomically */
    printf("Inserting 100 records in a transaction...\n");
    for (i = 0; i < 100; i++) {
        snprintf(key, sizeof(key), "item_%04d", i);
        snprintf(value, sizeof(value), "Item number %d", i);
        
        rc = kvstore_put(kv, key, strlen(key), value, strlen(value));
        if (rc != KVSTORE_OK) {
            fprintf(stderr, "Put failed, rolling back\n");
            kvstore_rollback(kv);
            kvstore_close(kv);
            return;
        }
    }
    
    /* Commit all changes */
    rc = kvstore_commit(kv);
    if (rc == KVSTORE_OK) {
        printf("Successfully committed 100 records\n");
    } else {
        fprintf(stderr, "Commit failed: %s\n", kvstore_errmsg(kv));
    }
    
    kvstore_close(kv);
}

/*
** Example 3: Iterating over all entries
*/
void example_iteration(void) {
    KVStore *kv = NULL;
    KVIterator *it = NULL;
    int rc, count = 0;
    void *key, *value;
    int key_len, value_len;
    
    printf("\n=== Example 3: Iteration ===\n");
    
    rc = kvstore_open("example.db", &kv, 0, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) return;
    
    /* Create iterator */
    rc = kvstore_iterator_create(kv, &it);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Iterator creation failed: %s\n", kvstore_errmsg(kv));
        kvstore_close(kv);
        return;
    }
    
    /* Move to first entry */
    rc = kvstore_iterator_first(it);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Iterator first failed\n");
        kvstore_iterator_close(it);
        kvstore_close(kv);
        return;
    }
    
    /* Iterate through all entries */
    printf("Listing first 10 entries:\n");
    while (!kvstore_iterator_eof(it) && count < 10) {
        rc = kvstore_iterator_key(it, &key, &key_len);
        if (rc != KVSTORE_OK) break;
        
        rc = kvstore_iterator_value(it, &value, &value_len);
        if (rc != KVSTORE_OK) break;
        
        printf("  %.*s = %.*s\n", key_len, (char*)key, value_len, (char*)value);
        
        count++;
        kvstore_iterator_next(it);
    }
    
    printf("Total entries shown: %d\n", count);
    
    /* Clean up iterator */
    kvstore_iterator_close(it);
    kvstore_close(kv);
}

/*
** Example 4: Error handling and statistics
*/
void example_error_handling(void) {
    KVStore *kv = NULL;
    KVStoreStats stats;
    int rc;
    void *value;
    int value_len;
    
    printf("\n=== Example 4: Error Handling & Statistics ===\n");
    
    rc = kvstore_open("example.db", &kv, 0, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) return;
    
    /* Try to get non-existent key */
    rc = kvstore_get(kv, "nonexistent_key", 15, &value, &value_len);
    if (rc == KVSTORE_NOTFOUND) {
        printf("Key not found (expected)\n");
    } else if (rc != KVSTORE_OK) {
        fprintf(stderr, "Unexpected error: %s\n", kvstore_errmsg(kv));
    }
    
    /* Try invalid operation */
    rc = kvstore_put(kv, NULL, 0, "value", 5);
    if (rc != KVSTORE_OK) {
        printf("Invalid key rejected (expected): %s\n", kvstore_errmsg(kv));
    }
    
    /* Get statistics */
    rc = kvstore_stats(kv, &stats);
    if (rc == KVSTORE_OK) {
        printf("\nDatabase Statistics:\n");
        printf("  Total puts:    %llu\n", (unsigned long long)stats.nPuts);
        printf("  Total gets:    %llu\n", (unsigned long long)stats.nGets);
        printf("  Total deletes: %llu\n", (unsigned long long)stats.nDeletes);
        printf("  Iterations:    %llu\n", (unsigned long long)stats.nIterations);
        printf("  Errors:        %llu\n", (unsigned long long)stats.nErrors);
    }
    
    kvstore_close(kv);
}

/*
** Example 5: Data persistence verification
*/
void example_persistence(void) {
    KVStore *kv = NULL;
    int rc;
    void *value = NULL;
    int value_len;
    
    printf("\n=== Example 5: Persistence ===\n");
    
    /* Session 1: Write data */
    printf("Session 1: Writing data...\n");
    rc = kvstore_open("persist_test.db", &kv, 0, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) return;
    
    kvstore_put(kv, "persistent_key", 14, "persistent_value", 16);
    printf("  Stored: persistent_key = persistent_value\n");
    
    /* Explicit sync to ensure data is on disk */
    kvstore_sync(kv);
    kvstore_close(kv);
    
    /* Session 2: Read data */
    printf("Session 2: Reading data...\n");
    rc = kvstore_open("persist_test.db", &kv, 0, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) return;
    
    rc = kvstore_get(kv, "persistent_key", 14, &value, &value_len);
    if (rc == KVSTORE_OK) {
        printf("  Retrieved: persistent_key = %.*s\n", value_len, (char*)value);
        printf("  ✓ Data persisted successfully!\n");
        sqliteFree(value);
    } else {
        printf("  ✗ Data not found after restart\n");
    }
    
    kvstore_close(kv);
    remove("persist_test.db");
}

/*
** Example 6: Integrity checking
*/
void example_integrity_check(void) {
    KVStore *kv = NULL;
    int rc, i;
    char key[32], value[64];
    char *err_msg = NULL;
    
    printf("\n=== Example 6: Integrity Check ===\n");
    
    rc = kvstore_open("example.db", &kv, 0, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) return;
    
    /* Add some data */
    printf("Adding test data...\n");
    for (i = 0; i < 50; i++) {
        snprintf(key, sizeof(key), "check_%d", i);
        snprintf(value, sizeof(value), "value_%d", i);
        kvstore_put(kv, key, strlen(key), value, strlen(value));
    }
    
    /* Perform integrity check */
    printf("Performing integrity check...\n");
    rc = kvstore_integrity_check(kv, &err_msg);
    
    if (rc == KVSTORE_OK) {
        printf("✓ Database integrity verified\n");
    } else {
        printf("✗ Integrity check failed: %s\n", 
               err_msg ? err_msg : "unknown error");
        if (err_msg) sqliteFree(err_msg);
    }
    
    kvstore_close(kv);
}

/*
** Example 7: Duplicate key handling (updates)
*/
void example_duplicate_keys(void) {
    KVStore *kv = NULL;
    int rc;
    void *value = NULL;
    int value_len;
    int i;
    char update_value[32];
    
    printf("\n=== Example 7: Duplicate Key Handling ===\n");
    
    rc = kvstore_open("example.db", &kv, 0, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) return;
    
    /* Initial insert */
    printf("Initial insert: key1 = value1\n");
    rc = kvstore_put(kv, "key1", 4, "value1", 6);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Initial put failed: %s\n", kvstore_errmsg(kv));
        kvstore_close(kv);
        return;
    }
    
    /* Verify initial value */
    rc = kvstore_get(kv, "key1", 4, &value, &value_len);
    if (rc == KVSTORE_OK) {
        printf("Retrieved: key1 = %.*s\n", value_len, (char*)value);
        sqliteFree(value);
        value = NULL;
    }
    
    /* Update with duplicate key - should replace */
    printf("\nUpdating: key1 = value2 (replacing value1)\n");
    rc = kvstore_put(kv, "key1", 4, "value2", 6);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Update put failed: %s\n", kvstore_errmsg(kv));
        kvstore_close(kv);
        return;
    }
    
    /* Verify updated value */
    rc = kvstore_get(kv, "key1", 4, &value, &value_len);
    if (rc == KVSTORE_OK) {
        printf("Retrieved: key1 = %.*s\n", value_len, (char*)value);
        if (memcmp(value, "value2", 6) == 0) {
            printf("✓ Key successfully updated (old value replaced)\n");
        } else {
            printf("✗ Unexpected value retrieved\n");
        }
        sqliteFree(value);
        value = NULL;
    }
    
    /* Multiple rapid updates */
    printf("\nPerforming 5 rapid updates to same key...\n");
    for (i = 1; i <= 5; i++) {
        snprintf(update_value, sizeof(update_value), "update_%d", i);
        rc = kvstore_put(kv, "key1", 4, update_value, strlen(update_value));
        if (rc != KVSTORE_OK) {
            fprintf(stderr, "Update %d failed\n", i);
            break;
        }
        printf("  Update %d: key1 = %s\n", i, update_value);
    }
    value = NULL;
    /* Verify final value */
    rc = kvstore_get(kv, "key1", 4, &value, &value_len);
    if (rc == KVSTORE_OK) {
        printf("Final value: key1 = %.*s\n", value_len, (char*)value);
        if (memcmp(value, "update_5", 8) == 0) {
            printf("✓ All updates applied correctly\n");
        }
        sqliteFree(value);
        value = NULL;
    }
    
    /* Test duplicate keys in transaction */
    printf("\nTesting duplicate keys in transaction...\n");
    rc = kvstore_begin(kv, 1);
    if (rc == KVSTORE_OK) {
        /* Insert new key */
        kvstore_put(kv, "txn_key", 7, "original", 8);
        
        /* Update same key in same transaction */
        kvstore_put(kv, "txn_key", 7, "modified", 8);
        
        /* Update again */
        kvstore_put(kv, "txn_key", 7, "final", 5);
        
        /* Commit */
        rc = kvstore_commit(kv);
        if (rc == KVSTORE_OK) {
            printf("Transaction committed\n");
            
            /* Verify only final value persisted */
            rc = kvstore_get(kv, "txn_key", 7, &value, &value_len);
            if (rc == KVSTORE_OK) {
                printf("After transaction: txn_key = %.*s\n", value_len, (char*)value);
                if (memcmp(value, "final", 5) == 0) {
                    printf("✓ Only final update in transaction persisted\n");
                } else {
                    printf("✗ Unexpected transaction result\n");
                }
                sqliteFree(value);
                value = NULL;
            }
        }
    }
    
    /* Test rollback of duplicate key update */
    printf("\nTesting rollback of update...\n");
    
    /* Get current value */
    rc = kvstore_get(kv, "txn_key", 7, &value, &value_len);
    if (rc == KVSTORE_OK) {
        printf("Before transaction: txn_key = %.*s\n", value_len, (char*)value);
        sqliteFree(value);
        value = NULL;
    }
    
    /* Start transaction and update */
    rc = kvstore_begin(kv, 1);
    if (rc == KVSTORE_OK) {
        kvstore_put(kv, "txn_key", 7, "should_rollback", 15);
        
        /* Rollback instead of commit */
        kvstore_rollback(kv);
        printf("Transaction rolled back\n");
        
        /* Verify old value is still there */
        rc = kvstore_get(kv, "txn_key", 7, &value, &value_len);
        if (rc == KVSTORE_OK) {
            printf("After rollback: txn_key = %.*s\n", value_len, (char*)value);
            if (memcmp(value, "final", 5) == 0) {
                printf("✓ Rollback successful - original value preserved\n");
            } else {
                printf("✗ Rollback failed - value was modified\n");
            }
            sqliteFree(value);
            value = NULL;
        }
    }
    
    /* Test size changes with duplicate keys */
    printf("\nTesting updates with different value sizes...\n");
    
    /* Small value */
    kvstore_put(kv, "size_test", 9, "tiny", 4);
    rc = kvstore_get(kv, "size_test", 9, &value, &value_len);
    if (rc == KVSTORE_OK) {
        printf("Small value: size_test = %.*s (length: %d)\n", 
               value_len, (char*)value, value_len);
        sqliteFree(value);
        value = NULL;
    }
    
    /* Medium value */
    char medium_value[256];
    memset(medium_value, 'M', sizeof(medium_value));
    kvstore_put(kv, "size_test", 9, medium_value, sizeof(medium_value));
    rc = kvstore_get(kv, "size_test", 9, &value, &value_len);
    if (rc == KVSTORE_OK) {
        printf("Medium value: size_test = [%d bytes of data]\n", value_len);
        sqliteFree(value);
        value = NULL;
    }
    
    /* Large value */
    char *large_value = malloc(10000);
    if (large_value) {
        memset(large_value, 'L', 10000);
        kvstore_put(kv, "size_test", 9, large_value, 10000);
        rc = kvstore_get(kv, "size_test", 9, &value, &value_len);
        if (rc == KVSTORE_OK) {
            printf("Large value: size_test = [%d bytes of data]\n", value_len);
            if (value_len == 10000) {
                printf("✓ Large value update successful\n");
            }
            sqliteFree(value);
            value = NULL;
        }
        free(large_value);
    }
    
    /* Back to small value */
    kvstore_put(kv, "size_test", 9, "small_again", 11);
    rc = kvstore_get(kv, "size_test", 9, &value, &value_len);
    if (rc == KVSTORE_OK) {
        printf("Small again: size_test = %.*s (length: %d)\n", 
               value_len, (char*)value, value_len);
        printf("✓ Successfully handled varying value sizes\n");
        sqliteFree(value);
        value = NULL;
    }
    
    kvstore_close(kv);
}

/*
** Main function
*/
int main(void) {
    printf("========================================\n");
    printf("KVStore Usage Examples\n");
    printf("========================================\n");
    
    /* Clean up any existing database */
    remove("example.db");
    
    /* Run examples */
    example_basic_usage();
    example_transactions();
    example_iteration();
    example_error_handling();
    example_persistence();
    example_integrity_check();
    example_duplicate_keys();
    
    /* Clean up */
    remove("example.db");
    
    printf("\n========================================\n");
    printf("Examples completed successfully!\n");
    printf("========================================\n\n");
    
    return 0;
}