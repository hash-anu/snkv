# KVStore Example Guide

A comprehensive collection of practical examples demonstrating how to use the KVStore library for various use cases.

## Building and Running Examples

First, build the SNKV static library:

```bash
make
```

Then compile your example against `libsnkv.a`:

```bash
gcc -Iinclude -o example example.c libsnkv.a
./example
```

Or use the built-in targets to build and run all examples:

```bash
make examples       # build all examples in examples/
make run-examples   # build and run all examples
```

## Getting Started

### Example 1: Hello World

The simplest possible example - store and retrieve a value.

```
#include "kvstore.h"
#include <stdio.h>
#include <string.h>

int main() {
    KVStore *pKV = NULL;
    int rc;
    
    // Open or create database
    rc = kvstore_open("hello.db", &pKV, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Failed to open database\n");
        return 1;
    }
    
    // Store a greeting
    const char *key = "greeting";
    const char *value = "Hello, World!";
    rc = kvstore_put(pKV, key, strlen(key), value, strlen(value));
    
    if (rc == KVSTORE_OK) {
        printf("Stored: %s = %s\n", key, value);
    }
    
    // Retrieve the greeting
    void *pValue = NULL;
    int nValue = 0;
    rc = kvstore_get(pKV, key, strlen(key), &pValue, &nValue);
    
    if (rc == KVSTORE_OK) {
        printf("Retrieved: %s = %.*s\n", key, nValue, (char*)pValue);
        sqliteFree(pValue);
    }
    
    // Clean up
    kvstore_close(pKV);
    return 0;
}
```

**Output:**
```
Stored: greeting = Hello, World!
Retrieved: greeting = Hello, World!
```

---

## Basic Operations

### Example 2: CRUD Operations

Complete Create, Read, Update, Delete example.

```c
#include "kvstore.h"
#include <stdio.h>
#include <string.h>

void print_user(const char *user_id, KVStore *pKV) {
    void *pValue;
    int nValue;
    
    int rc = kvstore_get(pKV, user_id, strlen(user_id), &pValue, &nValue);
    if (rc == KVSTORE_OK) {
        printf("User %s: %.*s\n", user_id, nValue, (char*)pValue);
        sqliteFree(pValue);
    } else if (rc == KVSTORE_NOTFOUND) {
        printf("User %s: Not found\n", user_id);
    }
}

int main() {
    KVStore *pKV;
    kvstore_open("users.db", &pKV, KVSTORE_JOURNAL_WAL);
    
    // CREATE - Add new user
    printf("=== CREATE ===\n");
    kvstore_put(pKV, "user:1", 6, "Alice Smith", 11);
    print_user("user:1", pKV);
    
    // READ - Retrieve user
    printf("\n=== READ ===\n");
    print_user("user:1", pKV);
    
    // UPDATE - Modify existing user
    printf("\n=== UPDATE ===\n");
    kvstore_put(pKV, "user:1", 6, "Alice Johnson", 13);
    print_user("user:1", pKV);
    
    // DELETE - Remove user
    printf("\n=== DELETE ===\n");
    kvstore_delete(pKV, "user:1", 6);
    print_user("user:1", pKV);
    
    kvstore_close(pKV);
    return 0;
}
```

**Output:**
```
=== CREATE ===
User user:1: Alice Smith

=== READ ===
User user:1: Alice Smith

=== UPDATE ===
User user:1: Alice Johnson

=== DELETE ===
User user:1: Not found
```

---

### Example 3: Checking Existence

Efficiently check if keys exist without retrieving values.

```c
#include "kvstore.h"
#include <stdio.h>
#include <string.h>

int main() {
    KVStore *pKV;
    kvstore_open("inventory.db", &pKV, KVSTORE_JOURNAL_WAL);
    
    // Add some items
    kvstore_put(pKV, "item:laptop", 11, "In Stock", 8);
    kvstore_put(pKV, "item:mouse", 10, "Out of Stock", 12);
    
    // Check existence
    const char *items[] = {"item:laptop", "item:mouse", "item:keyboard"};
    int num_items = 3;
    
    for (int i = 0; i < num_items; i++) {
        int exists = 0;
        kvstore_exists(pKV, items[i], strlen(items[i]), &exists);
        
        printf("%s: %s\n", items[i], exists ? "EXISTS" : "NOT FOUND");
    }
    
    kvstore_close(pKV);
    return 0;
}
```

**Output:**
```
item:laptop: EXISTS
item:mouse: EXISTS
item:keyboard: NOT FOUND
```

---

## Transaction Examples

### Example 4: Basic Transaction

Atomic batch of operations.

```c
#include "kvstore.h"
#include <stdio.h>
#include <string.h>

int transfer_funds(KVStore *pKV, const char *from, const char *to, int amount) {
    int rc;
    
    // Begin write transaction
    rc = kvstore_begin(pKV, 1);
    if (rc != KVSTORE_OK) {
        return rc;
    }
    
    // Get source balance
    void *pValue;
    int nValue;
    rc = kvstore_get(pKV, from, strlen(from), &pValue, &nValue);
    if (rc != KVSTORE_OK) {
        kvstore_rollback(pKV);
        return rc;
    }
    
    int from_balance = atoi((char*)pValue);
    sqliteFree(pValue);
    
    // Check sufficient funds
    if (from_balance < amount) {
        printf("Insufficient funds!\n");
        kvstore_rollback(pKV);
        return KVSTORE_ERROR;
    }
    
    // Get destination balance
    rc = kvstore_get(pKV, to, strlen(to), &pValue, &nValue);
    if (rc != KVSTORE_OK) {
        kvstore_rollback(pKV);
        return rc;
    }
    
    int to_balance = atoi((char*)pValue);
    sqliteFree(pValue);
    
    // Update balances
    char balance_str[32];
    
    sprintf(balance_str, "%d", from_balance - amount);
    kvstore_put(pKV, from, strlen(from), balance_str, strlen(balance_str));
    
    sprintf(balance_str, "%d", to_balance + amount);
    kvstore_put(pKV, to, strlen(to), balance_str, strlen(balance_str));
    
    // Commit transaction
    rc = kvstore_commit(pKV);
    if (rc == KVSTORE_OK) {
        printf("Transfer successful: %s -> %s ($%d)\n", from, to, amount);
    } else {
        kvstore_rollback(pKV);
    }
    
    return rc;
}

int main() {
    KVStore *pKV;
    kvstore_open("bank.db", &pKV, KVSTORE_JOURNAL_WAL);
    
    // Initialize accounts
    kvstore_put(pKV, "account:alice", 13, "1000", 4);
    kvstore_put(pKV, "account:bob", 11, "500", 3);
    
    // Perform transfer
    transfer_funds(pKV, "account:alice", "account:bob", 200);
    
    // Check final balances
    void *pValue;
    int nValue;
    
    kvstore_get(pKV, "account:alice", 13, &pValue, &nValue);
    printf("Alice's balance: $%.*s\n", nValue, (char*)pValue);
    sqliteFree(pValue);
    
    kvstore_get(pKV, "account:bob", 11, &pValue, &nValue);
    printf("Bob's balance: $%.*s\n", nValue, (char*)pValue);
    sqliteFree(pValue);
    
    kvstore_close(pKV);
    return 0;
}
```

**Output:**
```
Transfer successful: account:alice -> account:bob ($200)
Alice's balance: $800
Bob's balance: $700
```

---

### Example 5: Transaction Rollback

Handling errors with automatic rollback.

```c
#include "kvstore.h"
#include <stdio.h>
#include <string.h>

int batch_insert(KVStore *pKV, const char **keys, const char **values, int count) {
    int rc;
    
    rc = kvstore_begin(pKV, 1);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Failed to begin transaction\n");
        return rc;
    }
    
    printf("Starting batch insert of %d items...\n", count);
    
    for (int i = 0; i < count; i++) {
        rc = kvstore_put(pKV, keys[i], strlen(keys[i]), 
                        values[i], strlen(values[i]));
        
        if (rc != KVSTORE_OK) {
            fprintf(stderr, "Error inserting key %s: %s\n", 
                    keys[i], kvstore_errmsg(pKV));
            printf("Rolling back transaction...\n");
            kvstore_rollback(pKV);
            return rc;
        }
        
        printf("  Inserted: %s = %s\n", keys[i], values[i]);
    }
    
    rc = kvstore_commit(pKV);
    if (rc == KVSTORE_OK) {
        printf("Transaction committed successfully!\n");
    } else {
        fprintf(stderr, "Commit failed, rolling back...\n");
        kvstore_rollback(pKV);
    }
    
    return rc;
}

int main() {
    KVStore *pKV;
    kvstore_open("config.db", &pKV, KVSTORE_JOURNAL_WAL);
    
    const char *keys[] = {"server.host", "server.port", "server.timeout"};
    const char *values[] = {"localhost", "8080", "30"};
    
    batch_insert(pKV, keys, values, 3);
    
    kvstore_close(pKV);
    return 0;
}
```
**Output:**
```

Starting batch insert of 3 items...
  Inserted: server.host = localhost
  Inserted: server.port = 8080
  Inserted: server.timeout = 30
Transaction committed successfully!
```

---

## Column Family Examples

### Example 6: Organizing Data with Column Families

Using column families to separate different data types.

```c
#include "kvstore.h"
#include <stdio.h>
#include <string.h>

int main() {
    KVStore *pKV;
    KVColumnFamily *pUsersCF, *pProductsCF, *pOrdersCF;
    
    // Open database
    kvstore_open("ecommerce.db", &pKV, KVSTORE_JOURNAL_WAL);
    
    // Create column families
    printf("Creating column families...\n");
    kvstore_cf_create(pKV, "users", &pUsersCF);
    kvstore_cf_create(pKV, "products", &pProductsCF);
    kvstore_cf_create(pKV, "orders", &pOrdersCF);
    
    // Store user data
    printf("\n=== Users ===\n");
    kvstore_cf_put(pUsersCF, "user:1", 6, "alice@example.com", 17);
    kvstore_cf_put(pUsersCF, "user:2", 6, "bob@example.com", 15);
    
    // Store product data
    printf("=== Products ===\n");
    kvstore_cf_put(pProductsCF, "prod:100", 8, "Laptop:$999", 11);
    kvstore_cf_put(pProductsCF, "prod:101", 8, "Mouse:$29", 9);
    
    // Store order data
    printf("=== Orders ===\n");
    kvstore_cf_put(pOrdersCF, "order:1", 7, "user:1,prod:100", 15);
    kvstore_cf_put(pOrdersCF, "order:2", 7, "user:2,prod:101", 15);
    
    // Retrieve data from different CFs
    void *pValue;
    int nValue;
    
    printf("\n=== Retrieval ===\n");
    
    kvstore_cf_get(pUsersCF, "user:1", 6, &pValue, &nValue);
    printf("User 1: %.*s\n", nValue, (char*)pValue);
    sqliteFree(pValue);
    
    kvstore_cf_get(pProductsCF, "prod:100", 8, &pValue, &nValue);
    printf("Product 100: %.*s\n", nValue, (char*)pValue);
    sqliteFree(pValue);
    
    kvstore_cf_get(pOrdersCF, "order:1", 7, &pValue, &nValue);
    printf("Order 1: %.*s\n", nValue, (char*)pValue);
    sqliteFree(pValue);
    
    // Clean up
    kvstore_cf_close(pUsersCF);
    kvstore_cf_close(pProductsCF);
    kvstore_cf_close(pOrdersCF);
    kvstore_close(pKV);
    
    return 0;
}
```

**Output:**
```
Creating column families...

=== Users ===
=== Products ===
=== Orders ===

=== Retrieval ===
User 1: alice@example.com
Product 100: Laptop:$999
Order 1: user:1,prod:100
```

---

### Example 7: Listing and Managing Column Families

Enumerate and manage column families dynamically.

```c
#include "kvstore.h"
#include <stdio.h>
#include <string.h>

void list_column_families(KVStore *pKV) {
    char **azNames;
    int nCount;
    
    int rc = kvstore_cf_list(pKV, &azNames, &nCount);
    if (rc == KVSTORE_OK) {
        printf("Column Families (%d total):\n", nCount);
        for (int i = 0; i < nCount; i++) {
            printf("  - %s\n", azNames[i]);
            sqliteFree(azNames[i]);
        }
        sqliteFree(azNames);
    }
}

int main() {
    KVStore *pKV;
    kvstore_open("multi_cf.db", &pKV, KVSTORE_JOURNAL_WAL);
    
    printf("=== Initial State ===\n");
    list_column_families(pKV);
    
    // Create several column families
    printf("\n=== Creating CFs ===\n");
    KVColumnFamily *pCF1, *pCF2, *pCF3;
    kvstore_cf_create(pKV, "logs", &pCF1);
    kvstore_cf_create(pKV, "metrics", &pCF2);
    kvstore_cf_create(pKV, "cache", &pCF3);
    printf("Created: logs, metrics, cache\n");
    
    printf("\n=== After Creation ===\n");
    list_column_families(pKV);
    
    // Drop one column family
    printf("\n=== Dropping 'cache' CF ===\n");
    kvstore_cf_close(pCF3);
    kvstore_cf_drop(pKV, "cache");
    
    printf("\n=== After Drop ===\n");
    list_column_families(pKV);
    
    // Clean up
    kvstore_cf_close(pCF1);
    kvstore_cf_close(pCF2);
    kvstore_close(pKV);
    
    return 0;
}
```

**Output:**
```
=== Initial State ===
Column Families (0 total):

=== Creating CFs ===
Created: logs, metrics, cache

=== After Creation ===
Column Families (3 total):
  - cache
  - logs
  - metrics

=== Dropping 'cache' CF ===

=== After Drop ===
Column Families (2 total):
  - logs
  - metrics
```

---

## Iterator Examples

### Example 8: Basic Iteration

Scan all key-value pairs in order.

```c
#include "kvstore.h"
#include <stdio.h>
#include <string.h>

int main() {
    KVStore *pKV;
    KVIterator *pIter;
    
    kvstore_open("inventory.db", &pKV, KVSTORE_JOURNAL_WAL);
    
    // Add sample data
    kvstore_put(pKV, "apple", 5, "50", 2);
    kvstore_put(pKV, "banana", 6, "30", 2);
    kvstore_put(pKV, "orange", 6, "40", 2);
    kvstore_put(pKV, "grape", 5, "60", 2);
    
    // Create iterator
    kvstore_iterator_create(pKV, &pIter);
    
    printf("Inventory:\n");
    printf("%-10s %s\n", "Item", "Quantity");
    printf("------------------------\n");
    
    // Iterate through all items
    for (kvstore_iterator_first(pIter);
         !kvstore_iterator_eof(pIter);
         kvstore_iterator_next(pIter)) {
        
        void *pKey, *pValue;
        int nKey, nValue;
        
        kvstore_iterator_key(pIter, &pKey, &nKey);
        kvstore_iterator_value(pIter, &pValue, &nValue);
        
        printf("%-10.*s %.*s\n", nKey, (char*)pKey, nValue, (char*)pValue);
    }
    
    kvstore_iterator_close(pIter);
    kvstore_close(pKV);
    
    return 0;
}
```

**Output:**
```
Inventory:
Item       Quantity
------------------------
apple      50
banana     30
grape      60
orange     40
```

---

### Example 9: Filtered Iteration

Process only specific keys matching a pattern.

```c
#include "kvstore.h"
#include <stdio.h>
#include <string.h>

int main() {
    KVStore *pKV;
    KVIterator *pIter;
    
    kvstore_open("users.db", &pKV, KVSTORE_JOURNAL_WAL);
    
    // Add users and admins
    kvstore_put(pKV, "user:alice", 10, "Regular User", 12);
    kvstore_put(pKV, "user:bob", 8, "Regular User", 12);
    kvstore_put(pKV, "admin:charlie", 13, "Administrator", 13);
    kvstore_put(pKV, "admin:diana", 11, "Administrator", 13);
    kvstore_put(pKV, "user:eve", 8, "Regular User", 12);
    
    kvstore_iterator_create(pKV, &pIter);
    
    // Filter for admin keys only
    printf("Administrators:\n");
    for (kvstore_iterator_first(pIter);
         !kvstore_iterator_eof(pIter);
         kvstore_iterator_next(pIter)) {
        
        void *pKey, *pValue;
        int nKey, nValue;
        
        kvstore_iterator_key(pIter, &pKey, &nKey);
        
        // Check if key starts with "admin:"
        if (nKey >= 6 && memcmp(pKey, "admin:", 6) == 0) {
            kvstore_iterator_value(pIter, &pValue, &nValue);
            printf("  %.*s: %.*s\n", nKey, (char*)pKey, nValue, (char*)pValue);
        }
    }
    
    kvstore_iterator_close(pIter);
    kvstore_close(pKV);
    
    return 0;
}
```

**Output:**
```
Administrators:
  admin:charlie: Administrator
  admin:diana: Administrator
```

---

### Example 10: Counting and Statistics

Use iterators to gather statistics.

```c
#include "kvstore.h"
#include <stdio.h>
#include <string.h>

typedef struct {
    int total_keys;
    size_t total_key_bytes;
    size_t total_value_bytes;
    int max_key_size;
    int max_value_size;
} StoreStats;

void calculate_stats(KVStore *pKV, StoreStats *stats) {
    KVIterator *pIter;
    
    memset(stats, 0, sizeof(StoreStats));
    
    kvstore_iterator_create(pKV, &pIter);
    
    for (kvstore_iterator_first(pIter);
         !kvstore_iterator_eof(pIter);
         kvstore_iterator_next(pIter)) {
        
        void *pKey, *pValue;
        int nKey, nValue;
        
        kvstore_iterator_key(pIter, &pKey, &nKey);
        kvstore_iterator_value(pIter, &pValue, &nValue);
        
        stats->total_keys++;
        stats->total_key_bytes += nKey;
        stats->total_value_bytes += nValue;
        
        if (nKey > stats->max_key_size) {
            stats->max_key_size = nKey;
        }
        if (nValue > stats->max_value_size) {
            stats->max_value_size = nValue;
        }
    }
    
    kvstore_iterator_close(pIter);
}

int main() {
    KVStore *pKV;
    kvstore_open("data.db", &pKV, KVSTORE_JOURNAL_WAL);
    
    // Add sample data
    kvstore_put(pKV, "a", 1, "short", 5);
    kvstore_put(pKV, "longer_key", 10, "medium value", 12);
    kvstore_put(pKV, "k", 1, "very long value string here", 27);
    
    // Calculate statistics
    StoreStats stats;
    calculate_stats(pKV, &stats);
    
    printf("Database Statistics:\n");
    printf("  Total keys:        %d\n", stats.total_keys);
    printf("  Total key bytes:   %zu\n", stats.total_key_bytes);
    printf("  Total value bytes: %zu\n", stats.total_value_bytes);
    printf("  Max key size:      %d\n", stats.max_key_size);
    printf("  Max value size:    %d\n", stats.max_value_size);
    printf("  Avg key size:      %.2f\n", 
           (double)stats.total_key_bytes / stats.total_keys);
    printf("  Avg value size:    %.2f\n", 
           (double)stats.total_value_bytes / stats.total_keys);
    
    kvstore_close(pKV);
    return 0;
}
```

**Output:**
```
Database Statistics:
  Total keys:        3
  Total key bytes:   12
  Total value bytes: 44
  Max key size:      10
  Max value size:    27
  Avg key size:      4.00
  Avg value size:    14.67
```

---

## Error Handling Patterns

### Example 11: Comprehensive Error Handling

Best practices for handling all error conditions.

```c
#include "kvstore.h"
#include <stdio.h>
#include <string.h>

int safe_get(KVStore *pKV, const char *key, char *buffer, int buf_size) {
    void *pValue;
    int nValue;
    int rc;
    
    rc = kvstore_get(pKV, key, strlen(key), &pValue, &nValue);
    
    switch (rc) {
        case KVSTORE_OK:
            if (nValue < buf_size) {
                memcpy(buffer, pValue, nValue);
                buffer[nValue] = '\0';
                sqliteFree(pValue);
                return nValue;
            } else {
                fprintf(stderr, "Buffer too small for key '%s'\n", key);
                sqliteFree(pValue);
                return -1;
            }
            
        case KVSTORE_NOTFOUND:
            fprintf(stderr, "Key '%s' not found\n", key);
            return -1;
            
        case KVSTORE_NOMEM:
            fprintf(stderr, "Out of memory retrieving '%s'\n", key);
            return -1;
            
        case KVSTORE_CORRUPT:
            fprintf(stderr, "Database corruption detected!\n");
            return -1;
            
        default:
            fprintf(stderr, "Unexpected error for key '%s': %s\n", 
                    key, kvstore_errmsg(pKV));
            return -1;
    }
}

int main() {
    KVStore *pKV;
    char buffer[256];
    int rc;
    
    rc = kvstore_open("test.db", &pKV, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Cannot open database: %s\n", 
                kvstore_errmsg(pKV));
        return 1;
    }
    
    // Store a value
    kvstore_put(pKV, "config", 6, "production", 10);
    
    // Safe retrieval
    if (safe_get(pKV, "config", buffer, sizeof(buffer)) > 0) {
        printf("Config: %s\n", buffer);
    }
    
    // Try to get non-existent key
    safe_get(pKV, "missing", buffer, sizeof(buffer));
    
    // Check integrity
    char *zErrMsg = NULL;
    rc = kvstore_integrity_check(pKV, &zErrMsg);
    if (rc == KVSTORE_OK) {
        printf("Database integrity: OK\n");
    } else {
        fprintf(stderr, "Integrity check failed: %s\n", zErrMsg);
        sqliteFree(zErrMsg);
    }
    
    kvstore_close(pKV);
    return 0;
}
```

**Output:**
```
Config: production
Key 'missing' not found
Database integrity: OK
```

---

## Real-World Use Cases

### Example 12: Session Store

Implementing a web session storage system.

```c
#include "kvstore.h"
#include <stdio.h>
#include <string.h>
#include <time.h>

typedef struct {
    char user_id[64];
    time_t created_at;
    time_t last_access;
    int visit_count;
} Session;

int session_create(KVStore *pKV, const char *session_id, const char *user_id) {
    Session sess;
    
    strncpy(sess.user_id, user_id, sizeof(sess.user_id) - 1);
    sess.created_at = time(NULL);
    sess.last_access = sess.created_at;
    sess.visit_count = 1;
    
    return kvstore_put(pKV, session_id, strlen(session_id), 
                      &sess, sizeof(Session));
}

int session_get(KVStore *pKV, const char *session_id, Session *pSess) {
    void *pValue;
    int nValue;
    int rc;
    
    rc = kvstore_get(pKV, session_id, strlen(session_id), &pValue, &nValue);
    
    if (rc == KVSTORE_OK) {
        memcpy(pSess, pValue, sizeof(Session));
        sqliteFree(pValue);
        
        // Update last access time
        pSess->last_access = time(NULL);
        pSess->visit_count++;
        
        kvstore_put(pKV, session_id, strlen(session_id), 
                   pSess, sizeof(Session));
    }
    
    return rc;
}

void session_delete(KVStore *pKV, const char *session_id) {
    kvstore_delete(pKV, session_id, strlen(session_id));
}

int session_cleanup_expired(KVStore *pKV, int max_age_seconds) {
    KVIterator *pIter;
    time_t now = time(NULL);
    int deleted = 0;
    
    kvstore_iterator_create(pKV, &pIter);
    
    for (kvstore_iterator_first(pIter);
         !kvstore_iterator_eof(pIter);
         kvstore_iterator_next(pIter)) {
        
        void *pKey, *pValue;
        int nKey, nValue;
        
        kvstore_iterator_key(pIter, &pKey, &nKey);
        kvstore_iterator_value(pIter, &pValue, &nValue);
        
        Session *pSess = (Session*)pValue;
        
        if (now - pSess->last_access > max_age_seconds) {
            char *key_copy = malloc(nKey + 1);
            memcpy(key_copy, pKey, nKey);
            key_copy[nKey] = '\0';
            
            kvstore_delete(pKV, key_copy, nKey);
            free(key_copy);
            deleted++;
        }
    }
    
    kvstore_iterator_close(pIter);
    return deleted;
}

int main() {
    KVStore *pKV;
    kvstore_open("sessions.db", &pKV, KVSTORE_JOURNAL_WAL);
    
    // Create sessions
    printf("Creating sessions...\n");
    session_create(pKV, "sess_abc123", "user_alice");
    session_create(pKV, "sess_def456", "user_bob");
    session_create(pKV, "sess_ghi789", "user_charlie");
    
    // Access a session
    printf("\nAccessing session...\n");
    Session sess;
    if (session_get(pKV, "sess_abc123", &sess) == KVSTORE_OK) {
        printf("Session for user: %s\n", sess.user_id);
        printf("Visit count: %d\n", sess.visit_count);
        printf("Last access: %s", ctime(&sess.last_access));
    }
    
    // Clean up old sessions (older than 1 hour)
    printf("\nCleaning up expired sessions...\n");
    int deleted = session_cleanup_expired(pKV, 3600);
    printf("Deleted %d expired sessions\n", deleted);
    
    kvstore_close(pKV);
    return 0;
}
```

**Output:**
```
Creating sessions...

Accessing session...
Session for user: user_alice
Visit count: 2
Last access: Wed Feb  4 10:15:32 2026

Cleaning up expired sessions...
Deleted 0 expired sessions
```

---

### Example 13: Cache Implementation

Simple LRU-style cache with TTL support.

```c
#include "kvstore.h"
#include <stdio.h>
#include <string.h>
#include <time.h>

typedef struct {
    char data[256];
    time_t expires_at;
} CacheEntry;

int cache_set(KVStore *pKV, const char *key, const char *value, int ttl_seconds) {
    CacheEntry entry;
    
    strncpy(entry.data, value, sizeof(entry.data) - 1);
    entry.data[sizeof(entry.data) - 1] = '\0';
    entry.expires_at = time(NULL) + ttl_seconds;
    
    return kvstore_put(pKV, key, strlen(key), &entry, sizeof(CacheEntry));
}

int cache_get(KVStore *pKV, const char *key, char *buffer, int buf_size) {
    void *pValue;
    int nValue;
    int rc;
    
    rc = kvstore_get(pKV, key, strlen(key), &pValue, &nValue);
    
    if (rc == KVSTORE_OK) {
        CacheEntry *entry = (CacheEntry*)pValue;
        
        // Check if expired
        if (time(NULL) > entry->expires_at) {
            sqliteFree(pValue);
            kvstore_delete(pKV, key, strlen(key));
            return KVSTORE_NOTFOUND;
        }
        
        strncpy(buffer, entry->data, buf_size - 1);
        buffer[buf_size - 1] = '\0';
        sqliteFree(pValue);
        
        return KVSTORE_OK;
    }
    
    return rc;
}

void cache_evict_expired(KVStore *pKV) {
    KVIterator *pIter;
    time_t now = time(NULL);
    
    kvstore_iterator_create(pKV, &pIter);
    
    for (kvstore_iterator_first(pIter);
         !kvstore_iterator_eof(pIter);
         kvstore_iterator_next(pIter)) {
        
        void *pKey, *pValue;
        int nKey, nValue;
        
        kvstore_iterator_key(pIter, &pKey, &nKey);
        kvstore_iterator_value(pIter, &pValue, &nValue);
        
        CacheEntry *entry = (CacheEntry*)pValue;
        
        if (now > entry->expires_at) {
            char *key_copy = malloc(nKey + 1);
            memcpy(key_copy, pKey, nKey);
            key_copy[nKey] = '\0';
            
            kvstore_delete(pKV, key_copy, nKey);
            printf("Evicted expired key: %s\n", key_copy);
            free(key_copy);
        }
    }
    
    kvstore_iterator_close(pIter);
}

int main() {
    KVStore *pKV;
    char buffer[256];
    
    kvstore_open("cache.db", &pKV, KVSTORE_JOURNAL_WAL);
    
    // Set cache entries with different TTLs
    printf("Setting cache entries...\n");
    cache_set(pKV, "api:user:1", "Alice", 10);  // 10 seconds
    cache_set(pKV, "api:user:2", "Bob", 60);    // 60 seconds
    cache_set(pKV, "api:user:3", "Charlie", 5); // 5 seconds
    
    // Get immediately
    printf("\nGetting cache entries...\n");
    if (cache_get(pKV, "api:user:1", buffer, sizeof(buffer)) == KVSTORE_OK) {
        printf("Cache hit: api:user:1 = %s\n", buffer);
    }
    
    // Simulate time passing
    printf("\nWaiting 6 seconds...\n");
    sleep(6);
    
    // Try to get expired entry
    if (cache_get(pKV, "api:user:3", buffer, sizeof(buffer)) == KVSTORE_NOTFOUND) {
        printf("Cache miss: api:user:3 (expired)\n");
    }
    
    // Get still-valid entry
    if (cache_get(pKV, "api:user:2", buffer, sizeof(buffer)) == KVSTORE_OK) {
        printf("Cache hit: api:user:2 = %s\n", buffer);
    }
    
    // Evict all expired entries
    printf("\nEvicting expired entries...\n");
    cache_evict_expired(pKV);
    
    kvstore_close(pKV);
    return 0;
}
```

**Output:**
```
Setting cache entries...

Getting cache entries...
Cache hit: api:user:1 = Alice

Waiting 6 seconds...
Cache miss: api:user:3 (expired)
Cache hit: api:user:2 = Bob

Evicting expired entries...
```

---

### Example 14: Configuration Manager

Hierarchical configuration storage.

```c
#include "kvstore.h"
#include <stdio.h>
#include <string.h>

typedef struct {
    KVStore *pKV;
    KVColumnFamily *pCF;
} ConfigManager;

int config_init(ConfigManager *mgr, const char *env) {
    int rc = kvstore_open("config.db", &mgr->pKV, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) {
        return rc;
    }
    
    // Each environment gets its own column family
    rc = kvstore_cf_create(mgr->pKV, env, &mgr->pCF);
    if (rc != KVSTORE_OK) {
        // CF might already exist, try to open it
        rc = kvstore_cf_open(mgr->pKV, env, &mgr->pCF);
    }
    
    return rc;
}

int config_set(ConfigManager *mgr, const char *key, const char *value) {
    return kvstore_cf_put(mgr->pCF, key, strlen(key), value, strlen(value));
}

int config_get(ConfigManager *mgr, const char *key, char *buffer, int buf_size) {
    void *pValue;
    int nValue;
    int rc;
    
    rc = kvstore_cf_get(mgr->pCF, key, strlen(key), &pValue, &nValue);
    
    if (rc == KVSTORE_OK) {
        int copy_len = (nValue < buf_size - 1) ? nValue : buf_size - 1;
        memcpy(buffer, pValue, copy_len);
        buffer[copy_len] = '\0';
        sqliteFree(pValue);
    }
    
    return rc;
}

void config_list(ConfigManager *mgr, const char *prefix) {
    KVIterator *pIter;
    int prefix_len = prefix ? strlen(prefix) : 0;
    
    kvstore_cf_iterator_create(mgr->pCF, &pIter);
    
    printf("Configuration (prefix: %s):\n", prefix ? prefix : "all");
    
    for (kvstore_iterator_first(pIter);
         !kvstore_iterator_eof(pIter);
         kvstore_iterator_next(pIter)) {
        
        void *pKey, *pValue;
        int nKey, nValue;
        
        kvstore_iterator_key(pIter, &pKey, &nKey);
        
        if (!prefix || (nKey >= prefix_len && 
            memcmp(pKey, prefix, prefix_len) == 0)) {
            
            kvstore_iterator_value(pIter, &pValue, &nValue);
            printf("  %.*s = %.*s\n", nKey, (char*)pKey, nValue, (char*)pValue);
        }
    }
    
    kvstore_iterator_close(pIter);
}

void config_close(ConfigManager *mgr) {
    kvstore_cf_close(mgr->pCF);
    kvstore_close(mgr->pKV);
}

int main() {
    ConfigManager dev_mgr, prod_mgr;
    char buffer[256];
    
    // Initialize separate configs for dev and production
    printf("=== Development Environment ===\n");
    config_init(&dev_mgr, "development");
    config_set(&dev_mgr, "database.host", "localhost");
    config_set(&dev_mgr, "database.port", "5432");
    config_set(&dev_mgr, "api.debug", "true");
    config_set(&dev_mgr, "api.timeout", "30");
    
    printf("\n=== Production Environment ===\n");
    config_init(&prod_mgr, "production");
    config_set(&prod_mgr, "database.host", "db.example.com");
    config_set(&prod_mgr, "database.port", "5432");
    config_set(&prod_mgr, "api.debug", "false");
    config_set(&prod_mgr, "api.timeout", "60");
    
    // Read configs
    printf("\n=== Development Config ===\n");
    config_list(&dev_mgr, "database");
    
    printf("\n=== Production Config ===\n");
    config_list(&prod_mgr, "database");
    
    // Get specific value
    if (config_get(&prod_mgr, "api.timeout", buffer, sizeof(buffer)) == KVSTORE_OK) {
        printf("\nProduction API timeout: %s seconds\n", buffer);
    }
    
    config_close(&dev_mgr);
    config_close(&prod_mgr);
    
    return 0;
}
```

**Output:**
```
=== Development Environment ===

=== Production Environment ===

=== Development Config ===
Configuration (prefix: database):
  database.host = localhost
  database.port = 5432

=== Production Config ===
Configuration (prefix: database):
  database.host = db.example.com
  database.port = 5432

Production API timeout: 60 seconds
```

---

## Performance Optimization

### Example 15: Batch Operations

Optimizing bulk inserts with transactions.

```c
#include "kvstore.h"
#include <stdio.h>
#include <string.h>
#include <time.h>

double benchmark_inserts(KVStore *pKV, int count, int use_transaction) {
    char key[32], value[64];
    clock_t start, end;
    
    start = clock();
    
    if (use_transaction) {
        kvstore_begin(pKV, 1);
    }
    
    for (int i = 0; i < count; i++) {
        snprintf(key, sizeof(key), "key_%d", i);
        snprintf(value, sizeof(value), "value_for_key_%d", i);
        
        kvstore_put(pKV, key, strlen(key), value, strlen(value));
    }
    
    if (use_transaction) {
        kvstore_commit(pKV);
    }
    
    end = clock();
    
    return ((double)(end - start)) / CLOCKS_PER_SEC;
}

int main() {
    KVStore *pKV;
    double time_no_tx, time_with_tx;
    int num_ops = 10000;
    
    printf("Benchmark: %d insert operations\n\n", num_ops);
    
    // Test without transaction (auto-commit each operation)
    printf("Without transaction (auto-commit):\n");
    kvstore_open("bench1.db", &pKV, KVSTORE_JOURNAL_WAL);
    time_no_tx = benchmark_inserts(pKV, num_ops, 0);
    printf("  Time: %.3f seconds\n", time_no_tx);
    printf("  Rate: %.0f ops/sec\n", num_ops / time_no_tx);
    kvstore_close(pKV);
    
    // Test with single transaction
    printf("\nWith transaction (batch commit):\n");
    kvstore_open("bench2.db", &pKV, KVSTORE_JOURNAL_WAL);
    time_with_tx = benchmark_inserts(pKV, num_ops, 1);
    printf("  Time: %.3f seconds\n", time_with_tx);
    printf("  Rate: %.0f ops/sec\n", num_ops / time_with_tx);
    kvstore_close(pKV);
    
    printf("\nSpeedup: %.1fx faster\n", time_no_tx / time_with_tx);
    
    return 0;
}
```

**Output:**
```
Benchmark: 10000 insert operations

Without transaction (auto-commit):
  Time: 12.456 seconds
  Rate: 803 ops/sec

With transaction (batch commit):
  Time: 0.234 seconds
  Rate: 42735 ops/sec

Speedup: 53.2x faster
```

**Note:** Actual times will vary based on your hardware and disk speed.

---

## Thread-Safe Operations

### Example 16: Multi-threaded Access

Safe concurrent access from multiple threads.

```c
#include "kvstore.h"
#include <stdio.h>
#include <pthread.h>
#include <string.h>

typedef struct {
    KVStore *pKV;
    int thread_id;
    int num_ops;
} KVThreadData;

void* writer_thread(void *arg) {
    KVThreadData *data = (KVThreadData*)arg;
    char key[32], value[64];
    
    for (int i = 0; i < data->num_ops; i++) {
        snprintf(key, sizeof(key), "thread_%d_key_%d", data->thread_id, i);
        snprintf(value, sizeof(value), "Thread %d, item %d", data->thread_id, i);
        
        kvstore_put(data->pKV, key, strlen(key), value, strlen(value));
    }
    
    printf("Writer thread %d completed %d operations\n", 
           data->thread_id, data->num_ops);
    
    return NULL;
}

void* reader_thread(void *arg) {
    KVThreadData *data = (KVThreadData*)arg;
    char key[32];
    void *pValue;
    int nValue;
    int successful_reads = 0;
    
    for (int i = 0; i < data->num_ops; i++) {
        // Try to read from various threads
        int target_thread = i % 4;
        snprintf(key, sizeof(key), "thread_%d_key_%d", target_thread, i / 4);
        
        if (kvstore_get(data->pKV, key, strlen(key), &pValue, &nValue) == KVSTORE_OK) {
            successful_reads++;
            sqliteFree(pValue);
        }
    }
    
    printf("Reader thread %d completed %d reads (%d successful)\n", 
           data->thread_id, data->num_ops, successful_reads);
    
    return NULL;
}

int main() {
    KVStore *pKV;
    pthread_t threads[8];
    KVThreadData thread_data[8];
    
    kvstore_open("multithreaded.db", &pKV, KVSTORE_JOURNAL_WAL);
    
    printf("Starting multi-threaded test...\n\n");
    
    // Create 4 writer threads
    for (int i = 0; i < 4; i++) {
        thread_data[i].pKV = pKV;
        thread_data[i].thread_id = i;
        thread_data[i].num_ops = 100;
        
        pthread_create(&threads[i], NULL, writer_thread, &thread_data[i]);
    }
    
    // Create 4 reader threads
    for (int i = 4; i < 8; i++) {
        thread_data[i].pKV = pKV;
        thread_data[i].thread_id = i;
        thread_data[i].num_ops = 100;
        
        pthread_create(&threads[i], NULL, reader_thread, &thread_data[i]);
    }
    
    // Wait for all threads
    for (int i = 0; i < 8; i++) {
        pthread_join(threads[i], NULL);
    }
    
    printf("\nAll threads completed successfully!\n");
    
    // Print statistics
    KVStoreStats stats;
    kvstore_stats(pKV, &stats);
    printf("\nFinal Statistics:\n");
    printf("  Puts: %llu\n", stats.nPuts);
    printf("  Gets: %llu\n", stats.nGets);
    printf("  Errors: %llu\n", stats.nErrors);
    
    kvstore_close(pKV);
    return 0;
}
```

**Output:**
```
Starting multi-threaded test...

Writer thread 0 completed 100 operations
Writer thread 2 completed 100 operations
Writer thread 1 completed 100 operations
Writer thread 3 completed 100 operations
Reader thread 4 completed 100 reads (78 successful)
Reader thread 5 completed 100 reads (82 successful)
Reader thread 6 completed 100 reads (85 successful)
Reader thread 7 completed 100 reads (79 successful)

All threads completed successfully!

Final Statistics:
  Puts: 400
  Gets: 400
  Errors: 0
```

**Note:** The order of thread completion may vary, and successful read counts depend on timing.

---

## Advanced Patterns

### Example 17: Versioned Key-Value Store

Implementing simple versioning.

```c
#include "kvstore.h"
#include <stdio.h>
#include <string.h>
#include <time.h>

typedef struct {
    char data[256];
    int version;
    time_t timestamp;
} VersionedValue;

int versioned_put(KVStore *pKV, const char *key, const char *value) {
    VersionedValue vval;
    void *pOldValue;
    int nOldValue;
    int old_version = 0;
    
    // Get current version if exists
    if (kvstore_get(pKV, key, strlen(key), &pOldValue, &nOldValue) == KVSTORE_OK) {
        VersionedValue *old = (VersionedValue*)pOldValue;
        old_version = old->version;
        
        // Save old version to history
        char history_key[128];
        snprintf(history_key, sizeof(history_key), "%s:v%d", key, old_version);
        kvstore_put(pKV, history_key, strlen(history_key), pOldValue, nOldValue);
        
        sqliteFree(pOldValue);
    }
    
    // Create new version
    strncpy(vval.data, value, sizeof(vval.data) - 1);
    vval.data[sizeof(vval.data) - 1] = '\0';
    vval.version = old_version + 1;
    vval.timestamp = time(NULL);
    
    return kvstore_put(pKV, key, strlen(key), &vval, sizeof(VersionedValue));
}

int versioned_get(KVStore *pKV, const char *key, int version, char *buffer, int buf_size) {
    void *pValue;
    int nValue;
    int rc;
    char lookup_key[128];
    
    if (version < 0) {
        // Get current version
        snprintf(lookup_key, sizeof(lookup_key), "%s", key);
    } else {
        // Get specific version
        snprintf(lookup_key, sizeof(lookup_key), "%s:v%d", key, version);
    }
    
    rc = kvstore_get(pKV, lookup_key, strlen(lookup_key), &pValue, &nValue);
    
    if (rc == KVSTORE_OK) {
        VersionedValue *vval = (VersionedValue*)pValue;
        strncpy(buffer, vval->data, buf_size - 1);
        buffer[buf_size - 1] = '\0';
        
        printf("  [Version %d, %s]\n", vval->version, ctime(&vval->timestamp));
        
        sqliteFree(pValue);
    }
    
    return rc;
}

int main() {
    KVStore *pKV;
    char buffer[256];
    
    kvstore_open("versioned.db", &pKV, KVSTORE_JOURNAL_WAL);
    
    printf("Creating versioned document...\n\n");
    
    // Version 1
    printf("Version 1:\n");
    versioned_put(pKV, "document", "Initial draft");
    versioned_get(pKV, "document", -1, buffer, sizeof(buffer));
    printf("  Content: %s\n\n", buffer);
    
    sleep(1);
    
    // Version 2
    printf("Version 2:\n");
    versioned_put(pKV, "document", "Added introduction");
    versioned_get(pKV, "document", -1, buffer, sizeof(buffer));
    printf("  Content: %s\n\n", buffer);
    
    sleep(1);
    
    // Version 3
    printf("Version 3:\n");
    versioned_put(pKV, "document", "Added conclusion");
    versioned_get(pKV, "document", -1, buffer, sizeof(buffer));
    printf("  Content: %s\n\n", buffer);
    
    // Retrieve old versions
    printf("=== Version History ===\n");
    
    printf("Version 1:\n");
    versioned_get(pKV, "document", 1, buffer, sizeof(buffer));
    printf("  Content: %s\n\n", buffer);
    
    printf("Version 2:\n");
    versioned_get(pKV, "document", 2, buffer, sizeof(buffer));
    printf("  Content: %s\n\n", buffer);
    
    printf("Current version:\n");
    versioned_get(pKV, "document", -1, buffer, sizeof(buffer));
    printf("  Content: %s\n", buffer);
    
    kvstore_close(pKV);
    return 0;
}
```

**Output:**
```
Creating versioned document...

Version 1:
  [Version 1, Wed Feb  4 10:20:00 2026]
  Content: Initial draft

Version 2:
  [Version 2, Wed Feb  4 10:20:01 2026]
  Content: Added introduction

Version 3:
  [Version 3, Wed Feb  4 10:20:02 2026]
  Content: Added conclusion

=== Version History ===
Version 1:
  [Version 1, Wed Feb  4 10:20:00 2026]
  Content: Initial draft

Version 2:
  [Version 2, Wed Feb  4 10:20:01 2026]
  Content: Added introduction

Current version:
  [Version 3, Wed Feb  4 10:20:02 2026]
  Content: Added conclusion
```

---

### Example 18: Secondary Indexes

Implementing simple secondary indexes.

```c
#include "kvstore.h"
#include <stdio.h>
#include <string.h>

typedef struct {
    char name[64];
    char email[64];
    int age;
} User;

int user_create(KVStore *pKV, const char *user_id, const User *user) {
    int rc;
    
    // Begin transaction
    kvstore_begin(pKV, 1);
    
    // Store primary record (by user ID)
    char primary_key[128];
    snprintf(primary_key, sizeof(primary_key), "user:%s", user_id);
    rc = kvstore_put(pKV, primary_key, strlen(primary_key), user, sizeof(User));
    
    if (rc != KVSTORE_OK) {
        kvstore_rollback(pKV);
        return rc;
    }
    
    // Create email index
    char email_index[128];
    snprintf(email_index, sizeof(email_index), "email_idx:%s", user->email);
    rc = kvstore_put(pKV, email_index, strlen(email_index), 
                    user_id, strlen(user_id));
    
    if (rc != KVSTORE_OK) {
        kvstore_rollback(pKV);
        return rc;
    }
    
    kvstore_commit(pKV);
    return KVSTORE_OK;
}

int user_find_by_email(KVStore *pKV, const char *email, User *user) {
    void *pValue;
    int nValue;
    int rc;
    
    // Look up email index
    char email_index[128];
    snprintf(email_index, sizeof(email_index), "email_idx:%s", email);
    
    rc = kvstore_get(pKV, email_index, strlen(email_index), &pValue, &nValue);
    if (rc != KVSTORE_OK) {
        return rc;
    }
    
    // Get user ID from index
    char user_id[64];
    memcpy(user_id, pValue, nValue);
    user_id[nValue] = '\0';
    sqliteFree(pValue);
    
    // Look up user record
    char primary_key[128];
    snprintf(primary_key, sizeof(primary_key), "user:%s", user_id);
    
    rc = kvstore_get(pKV, primary_key, strlen(primary_key), &pValue, &nValue);
    if (rc == KVSTORE_OK) {
        memcpy(user, pValue, sizeof(User));
        sqliteFree(pValue);
    }
    
    return rc;
}

int main() {
    KVStore *pKV;
    User user, found_user;
    
    kvstore_open("users_indexed.db", &pKV, KVSTORE_JOURNAL_WAL);
    
    // Create users
    printf("Creating users...\n");
    
    strcpy(user.name, "Alice Smith");
    strcpy(user.email, "alice@example.com");
    user.age = 30;
    user_create(pKV, "001", &user);
    
    strcpy(user.name, "Bob Jones");
    strcpy(user.email, "bob@example.com");
    user.age = 25;
    user_create(pKV, "002", &user);
    
    strcpy(user.name, "Charlie Brown");
    strcpy(user.email, "charlie@example.com");
    user.age = 35;
    user_create(pKV, "003", &user);
    
    // Find user by email
    printf("\n=== Finding user by email ===\n");
    if (user_find_by_email(pKV, "bob@example.com", &found_user) == KVSTORE_OK) {
        printf("Found: %s (age %d)\n", found_user.name, found_user.age);
    }
    
    if (user_find_by_email(pKV, "alice@example.com", &found_user) == KVSTORE_OK) {
        printf("Found: %s (age %d)\n", found_user.name, found_user.age);
    }
    
    // Try non-existent email
    if (user_find_by_email(pKV, "nobody@example.com", &found_user) == KVSTORE_NOTFOUND) {
        printf("User with email 'nobody@example.com' not found\n");
    }
    
    kvstore_close(pKV);
    return 0;
}
```

**Output:**
```
Creating users...

=== Finding user by email ===
Found: Bob Jones (age 25)
Found: Alice Smith (age 30)
User with email 'nobody@example.com' not found
```

---

## Summary

This guide demonstrates:

1. **Basic operations** - Simple CRUD patterns
2. **Transactions** - Atomic operations and error recovery
3. **Column families** - Data organization and isolation
4. **Iterators** - Sequential access and filtering
5. **Error handling** - Robust error management
6. **Real-world use cases** - Session stores, caches, configuration
7. **Performance** - Optimization techniques
8. **Thread safety** - Concurrent access patterns
9. **Advanced patterns** - Versioning and indexing

---


## Additional Resources

- Source Code: `kvstore.c`, `kvstore.h`

For questions or issues, please refer to the project repository.
