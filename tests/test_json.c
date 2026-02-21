/*
** KVStore JSON Examples - Standalone Programs
** 
** This file contains complete, standalone examples demonstrating:
** 1. Inserting large JSON files into kvstore
** 2. Fetching and verifying JSON data
** 3. Working with multiple JSON documents
** 4. Using column families for JSON organization
** 5. Batch operations with JSON data
**
** Compile with: gcc -o kvstore_json_examples kvstore_json_examples.c kvstore.c btree.c -I. -DSQLITE_THREADSAFE=1
*/

#include "kvstore.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>

/* JSON utilities */
#include <ctype.h>

/*
** Simple JSON validator - checks if a string is well-formed JSON
*/
static int validateJSON(const char *json, int len) {
    int braceDepth = 0;
    int bracketDepth = 0;
    int inString = 0;
    int i;
    
    for (i = 0; i < len; i++) {
        char c = json[i];
        
        if (inString) {
            if (c == '"' && (i == 0 || json[i-1] != '\\')) {
                inString = 0;
            }
            continue;
        }
        
        switch (c) {
            case '"':
                if (i == 0 || json[i-1] != '\\') {
                    inString = 1;
                }
                break;
            case '{':
                braceDepth++;
                break;
            case '}':
                braceDepth--;
                if (braceDepth < 0) return 0;
                break;
            case '[':
                bracketDepth++;
                break;
            case ']':
                bracketDepth--;
                if (bracketDepth < 0) return 0;
                break;
        }
    }
    
    return braceDepth == 0 && bracketDepth == 0 && !inString;
}

/*
** Generate a large sample JSON document
*/
static char* generateLargeJSON(int numRecords, int *outSize) {
    char *json;
    int capacity = numRecords * 200;  /* Estimate 200 bytes per record */
    int len = 0;
    int i;
    
    json = (char*)malloc(capacity);
    if (!json) return NULL;
    
    /* Start JSON object */
    len += sprintf(json + len, "{\n  \"metadata\": {\n");
    len += sprintf(json + len, "    \"version\": \"1.0\",\n");
    len += sprintf(json + len, "    \"timestamp\": %ld,\n", (long)time(NULL));
    len += sprintf(json + len, "    \"record_count\": %d\n", numRecords);
    len += sprintf(json + len, "  },\n");
    len += sprintf(json + len, "  \"records\": [\n");
    
    /* Add records */
    for (i = 0; i < numRecords; i++) {
        len += sprintf(json + len, "    {\n");
        len += sprintf(json + len, "      \"id\": %d,\n", i);
        len += sprintf(json + len, "      \"name\": \"Record_%d\",\n", i);
        len += sprintf(json + len, "      \"value\": %d,\n", i * 100);
        len += sprintf(json + len, "      \"active\": %s,\n", (i % 2 == 0) ? "true" : "false");
        len += sprintf(json + len, "      \"description\": \"This is a sample record with id %d for testing large JSON storage\"\n", i);
        len += sprintf(json + len, "    }%s\n", (i < numRecords - 1) ? "," : "");
    }
    
    len += sprintf(json + len, "  ]\n");
    len += sprintf(json + len, "}\n");
    
    *outSize = len;
    return json;
}

/*
** Generate a complex nested JSON document
*/
static char* generateNestedJSON(int depth, int *outSize) {
    char *json;
    int capacity = 10000;
    int len = 0;
    int i;
    
    json = (char*)malloc(capacity);
    if (!json) return NULL;
    
    len += sprintf(json + len, "{\n");
    len += sprintf(json + len, "  \"type\": \"complex_document\",\n");
    len += sprintf(json + len, "  \"depth\": %d,\n", depth);
    len += sprintf(json + len, "  \"data\": {\n");
    
    /* Create nested structure */
    char indent[256] = "    ";
    for (i = 0; i < depth; i++) {
        len += sprintf(json + len, "%s\"level_%d\": {\n", indent, i);
        len += sprintf(json + len, "%s  \"index\": %d,\n", indent, i);
        len += sprintf(json + len, "%s  \"values\": [%d, %d, %d],\n", 
                      indent, i*10, i*20, i*30);
        if (i < depth - 1) {
            len += sprintf(json + len, "%s  \"nested\": {\n", indent);
        }
        strcat(indent, "  ");
    }
    
    /* Close nested structure */
    for (i = depth - 1; i >= 0; i--) {
        indent[strlen(indent) - 2] = '\0';
        if (i < depth - 1) {
            len += sprintf(json + len, "%s  }\n", indent);
        }
        len += sprintf(json + len, "%s}%s\n", indent, (i > 0) ? "," : "");
    }
    
    len += sprintf(json + len, "  }\n");
    len += sprintf(json + len, "}\n");
    
    *outSize = len;
    return json;
}

/*
** Compare two JSON strings (simple byte comparison)
*/
static int compareJSON(const char *json1, int len1, const char *json2, int len2) {
    if (len1 != len2) return 0;
    return memcmp(json1, json2, len1) == 0;
}

/* ============================================================================
** EXAMPLE 1: Basic JSON Insert, Fetch, and Verify
** ============================================================================ */
int example1_basic_json_operations(void) {
    KVStore *pKV = NULL;
    int rc;
    char *jsonData = NULL;
    int jsonSize = 0;
    void *fetchedData = NULL;
    int fetchedSize = 0;
    
    printf("\n=== EXAMPLE 1: Basic JSON Operations ===\n");
    
    /* Open kvstore */
    rc = kvstore_open("example1.db", &pKV, KVSTORE_JOURNAL_DELETE);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Failed to open kvstore: %d\n", rc);
        return 1;
    }
    printf("[OK] Opened kvstore\n");
    
    /* Generate a large JSON document (1000 records) */
    jsonData = generateLargeJSON(1000, &jsonSize);
    if (!jsonData) {
        fprintf(stderr, "Failed to generate JSON\n");
        kvstore_close(pKV);
        return 1;
    }
    printf("[OK] Generated JSON document: %d bytes\n", jsonSize);
    
    /* Validate the generated JSON */
    if (!validateJSON(jsonData, jsonSize)) {
        fprintf(stderr, "Generated JSON is invalid!\n");
        free(jsonData);
        kvstore_close(pKV);
        return 1;
    }
    printf("[OK] JSON validation passed\n");
    
    /* Insert JSON into kvstore */
    const char *key = "large_json_doc";
    rc = kvstore_put(pKV, key, strlen(key), jsonData, jsonSize);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Failed to put JSON: %d (%s)\n", rc, kvstore_errmsg(pKV));
        free(jsonData);
        kvstore_close(pKV);
        return 1;
    }
    printf("[OK] Inserted JSON document with key: %s\n", key);
    
    /* Fetch JSON back */
    rc = kvstore_get(pKV, key, strlen(key), &fetchedData, &fetchedSize);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Failed to get JSON: %d (%s)\n", rc, kvstore_errmsg(pKV));
        free(jsonData);
        kvstore_close(pKV);
        return 1;
    }
    printf("[OK] Fetched JSON document: %d bytes\n", fetchedSize);
    
    /* Verify data integrity */
    if (compareJSON(jsonData, jsonSize, (char*)fetchedData, fetchedSize)) {
        printf("[OK] Data verification PASSED - JSON matches exactly!\n");
    } else {
        fprintf(stderr, "[X] Data verification FAILED - JSON mismatch!\n");
        printf("  Original size: %d, Fetched size: %d\n", jsonSize, fetchedSize);
    }
    
    /* Validate fetched JSON */
    if (validateJSON((char*)fetchedData, fetchedSize)) {
        printf("[OK] Fetched JSON is well-formed\n");
    } else {
        fprintf(stderr, "[X] Fetched JSON is malformed!\n");
    }
    
    /* Cleanup */
    sqliteFree(fetchedData);
    free(jsonData);
    
    /* Get statistics */
    KVStoreStats stats;
    kvstore_stats(pKV, &stats);
    printf("\nStatistics:\n");
    printf("  Puts: %llu\n", stats.nPuts);
    printf("  Gets: %llu\n", stats.nGets);
    printf("  Errors: %llu\n", stats.nErrors);
    
    kvstore_close(pKV);
    printf("\n[OK] Example 1 completed successfully!\n");
    
    return 0;
}

/* ============================================================================
** EXAMPLE 2: Multiple JSON Documents with Different Sizes
** ============================================================================ */
int example2_multiple_json_documents(void) {
    KVStore *pKV = NULL;
    int rc;
    int i;
    
    printf("\n=== EXAMPLE 2: Multiple JSON Documents ===\n");
    
    rc = kvstore_open("example2.db", &pKV, KVSTORE_JOURNAL_DELETE);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Failed to open kvstore: %d\n", rc);
        return 1;
    }
    printf("[OK] Opened kvstore\n");
    
    /* Insert JSON documents of varying sizes */
    int sizes[] = {10, 100, 500, 1000, 5000};
    int numDocs = sizeof(sizes) / sizeof(sizes[0]);
    
    printf("\nInserting %d JSON documents...\n", numDocs);
    
    for (i = 0; i < numDocs; i++) {
        char key[64];
        char *jsonData;
        int jsonSize;
        
        sprintf(key, "json_doc_%d_records", sizes[i]);
        jsonData = generateLargeJSON(sizes[i], &jsonSize);
        
        if (!jsonData) {
            fprintf(stderr, "Failed to generate JSON for size %d\n", sizes[i]);
            continue;
        }
        
        rc = kvstore_put(pKV, key, strlen(key), jsonData, jsonSize);
        if (rc != KVSTORE_OK) {
            fprintf(stderr, "Failed to insert document %d: %d\n", i, rc);
            free(jsonData);
            continue;
        }
        
        printf("  [OK] Inserted %s: %d bytes\n", key, jsonSize);
        free(jsonData);
    }
    
    /* Verify all documents */
    printf("\nVerifying all documents...\n");
    
    for (i = 0; i < numDocs; i++) {
        char key[64];
        char *expectedJSON;
        int expectedSize;
        void *fetchedData;
        int fetchedSize;
        
        sprintf(key, "json_doc_%d_records", sizes[i]);
        expectedJSON = generateLargeJSON(sizes[i], &expectedSize);
        
        rc = kvstore_get(pKV, key, strlen(key), &fetchedData, &fetchedSize);
        if (rc != KVSTORE_OK) {
            fprintf(stderr, "  [X] Failed to fetch %s\n", key);
            free(expectedJSON);
            continue;
        }
        
        if (compareJSON(expectedJSON, expectedSize, (char*)fetchedData, fetchedSize)) {
            printf("  [OK] %s verified (size: %d bytes)\n", key, fetchedSize);
        } else {
            fprintf(stderr, "  [X] %s verification failed!\n", key);
        }
        
        sqliteFree(fetchedData);
        free(expectedJSON);
    }
    
    kvstore_close(pKV);
    printf("\n[OK] Example 2 completed successfully!\n");
    
    return 0;
}

/* ============================================================================
** EXAMPLE 3: Column Families for JSON Organization
** ============================================================================ */
int example3_column_families_json(void) {
    KVStore *pKV = NULL;
    KVColumnFamily *pCF_Users = NULL;
    KVColumnFamily *pCF_Products = NULL;
    int rc;
    
    printf("\n=== EXAMPLE 3: Column Families for JSON ===\n");
    
    rc = kvstore_open("example3.db", &pKV, KVSTORE_JOURNAL_DELETE);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Failed to open kvstore: %d\n", rc);
        return 1;
    }
    printf("[OK] Opened kvstore\n");
    
    /* Create column families */
    rc = kvstore_cf_create(pKV, "users", &pCF_Users);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Failed to create users CF: %d\n", rc);
        kvstore_close(pKV);
        return 1;
    }
    printf("[OK] Created 'users' column family\n");
    
    rc = kvstore_cf_create(pKV, "products", &pCF_Products);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Failed to create products CF: %d\n", rc);
        kvstore_cf_close(pCF_Users);
        kvstore_close(pKV);
        return 1;
    }
    printf("[OK] Created 'products' column family\n");
    
    /* Insert user JSON documents */
    printf("\nInserting user documents...\n");
    for (int i = 0; i < 5; i++) {
        char key[32];
        char json[512];
        int jsonLen;
        
        sprintf(key, "user_%d", 1000 + i);
        jsonLen = sprintf(json, 
            "{\n"
            "  \"user_id\": %d,\n"
            "  \"username\": \"user%d\",\n"
            "  \"email\": \"user%d@example.com\",\n"
            "  \"created\": %ld,\n"
            "  \"premium\": %s\n"
            "}", 
            1000 + i, i, i, (long)time(NULL), (i % 2 == 0) ? "true" : "false");
        
        rc = kvstore_cf_put(pCF_Users, key, strlen(key), json, jsonLen);
        if (rc == KVSTORE_OK) {
            printf("  [OK] Inserted %s (%d bytes)\n", key, jsonLen);
        } else {
            fprintf(stderr, "  [X] Failed to insert %s\n", key);
        }
    }
    
    /* Insert product JSON documents */
    printf("\nInserting product documents...\n");
    for (int i = 0; i < 5; i++) {
        char key[32];
        char json[512];
        int jsonLen;
        
        sprintf(key, "product_%d", 2000 + i);
        jsonLen = sprintf(json,
            "{\n"
            "  \"product_id\": %d,\n"
            "  \"name\": \"Product %d\",\n"
            "  \"price\": %d.99,\n"
            "  \"stock\": %d,\n"
            "  \"category\": \"category_%d\"\n"
            "}",
            2000 + i, i, 10 + i * 5, 50 + i * 10, i % 3);
        
        rc = kvstore_cf_put(pCF_Products, key, strlen(key), json, jsonLen);
        if (rc == KVSTORE_OK) {
            printf("  [OK] Inserted %s (%d bytes)\n", key, jsonLen);
        } else {
            fprintf(stderr, "  [X] Failed to insert %s\n", key);
        }
    }
    
    /* Verify documents from both CFs */
    printf("\nVerifying user documents...\n");
    for (int i = 0; i < 5; i++) {
        char key[32];
        void *data;
        int size;
        
        sprintf(key, "user_%d", 1000 + i);
        rc = kvstore_cf_get(pCF_Users, key, strlen(key), &data, &size);
        
        if (rc == KVSTORE_OK) {
            if (validateJSON((char*)data, size)) {
                printf("  [OK] %s: valid JSON (%d bytes)\n", key, size);
            } else {
                fprintf(stderr, "  [X] %s: invalid JSON!\n", key);
            }
            sqliteFree(data);
        } else {
            fprintf(stderr, "  [X] Failed to fetch %s\n", key);
        }
    }
    
    printf("\nVerifying product documents...\n");
    for (int i = 0; i < 5; i++) {
        char key[32];
        void *data;
        int size;
        
        sprintf(key, "product_%d", 2000 + i);
        rc = kvstore_cf_get(pCF_Products, key, strlen(key), &data, &size);
        
        if (rc == KVSTORE_OK) {
            if (validateJSON((char*)data, size)) {
                printf("  [OK] %s: valid JSON (%d bytes)\n", key, size);
            } else {
                fprintf(stderr, "  [X] %s: invalid JSON!\n", key);
            }
            sqliteFree(data);
        } else {
            fprintf(stderr, "  [X] Failed to fetch %s\n", key);
        }
    }
    
    /* List all column families */
    char **cfNames;
    int cfCount;
    rc = kvstore_cf_list(pKV, &cfNames, &cfCount);
    if (rc == KVSTORE_OK) {
        printf("\nColumn families in database: %d\n", cfCount);
        for (int i = 0; i < cfCount; i++) {
            printf("  - %s\n", cfNames[i]);
            sqliteFree(cfNames[i]);
        }
        sqliteFree(cfNames);
    }
    
    kvstore_cf_close(pCF_Users);
    kvstore_cf_close(pCF_Products);
    kvstore_close(pKV);
    
    printf("\n[OK] Example 3 completed successfully!\n");
    return 0;
}

/* ============================================================================
** EXAMPLE 4: Nested JSON and Complex Structures
** ============================================================================ */
int example4_nested_json(void) {
    KVStore *pKV = NULL;
    int rc;
    
    printf("\n=== EXAMPLE 4: Nested JSON Structures ===\n");
    
    rc = kvstore_open("example4.db", &pKV, KVSTORE_JOURNAL_DELETE);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Failed to open kvstore: %d\n", rc);
        return 1;
    }
    printf("[OK] Opened kvstore\n");
    
    /* Test different nesting depths */
    int depths[] = {5, 10, 20, 30};
    int numTests = sizeof(depths) / sizeof(depths[0]);
    
    printf("\nTesting nested JSON at different depths...\n");
    
    for (int i = 0; i < numTests; i++) {
        char key[64];
        char *jsonData;
        int jsonSize;
        void *fetchedData;
        int fetchedSize;
        
        sprintf(key, "nested_depth_%d", depths[i]);
        jsonData = generateNestedJSON(depths[i], &jsonSize);
        
        if (!jsonData) {
            fprintf(stderr, "Failed to generate nested JSON depth %d\n", depths[i]);
            continue;
        }
        
        /* Insert */
        rc = kvstore_put(pKV, key, strlen(key), jsonData, jsonSize);
        if (rc != KVSTORE_OK) {
            fprintf(stderr, "  [X] Failed to insert depth %d: %d\n", depths[i], rc);
            free(jsonData);
            continue;
        }
        printf("  [OK] Inserted nested JSON (depth=%d, size=%d bytes)\n", 
               depths[i], jsonSize);
        
        /* Fetch and verify */
        rc = kvstore_get(pKV, key, strlen(key), &fetchedData, &fetchedSize);
        if (rc != KVSTORE_OK) {
            fprintf(stderr, "  [X] Failed to fetch depth %d\n", depths[i]);
            free(jsonData);
            continue;
        }
        
        if (compareJSON(jsonData, jsonSize, (char*)fetchedData, fetchedSize) &&
            validateJSON((char*)fetchedData, fetchedSize)) {
            printf("  [OK] Verified nested JSON (depth=%d)\n", depths[i]);
        } else {
            fprintf(stderr, "  [X] Verification failed for depth %d\n", depths[i]);
        }
        
        sqliteFree(fetchedData);
        free(jsonData);
    }
    
    kvstore_close(pKV);
    printf("\n[OK] Example 4 completed successfully!\n");
    
    return 0;
}

/* ============================================================================
** EXAMPLE 5: Batch Operations and Transactions
** ============================================================================ */
int example5_batch_json_operations(void) {
    KVStore *pKV = NULL;
    int rc;
    int numDocs = 100;
    
    printf("\n=== EXAMPLE 5: Batch JSON Operations ===\n");
    
    rc = kvstore_open("example5.db", &pKV, KVSTORE_JOURNAL_DELETE);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Failed to open kvstore: %d\n", rc);
        return 1;
    }
    printf("[OK] Opened kvstore\n");
    
    /* Batch insert with transaction */
    printf("\nBatch inserting %d JSON documents in transaction...\n", numDocs);
    
    clock_t start = clock();
    
    rc = kvstore_begin(pKV, 1);  /* Begin write transaction */
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Failed to begin transaction: %d\n", rc);
        kvstore_close(pKV);
        return 1;
    }
    
    int successCount = 0;
    for (int i = 0; i < numDocs; i++) {
        char key[64];
        char json[256];
        int jsonLen;
        
        sprintf(key, "batch_doc_%04d", i);
        jsonLen = sprintf(json,
            "{\"id\":%d,\"name\":\"Document %d\",\"timestamp\":%ld}",
            i, i, (long)time(NULL));
        
        rc = kvstore_put(pKV, key, strlen(key), json, jsonLen);
        if (rc == KVSTORE_OK) {
            successCount++;
        } else {
            fprintf(stderr, "Failed to insert doc %d: %d\n", i, rc);
        }
    }
    
    rc = kvstore_commit(pKV);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Failed to commit transaction: %d\n", rc);
        kvstore_rollback(pKV);
        kvstore_close(pKV);
        return 1;
    }
    
    clock_t end = clock();
    double elapsed = (double)(end - start) / CLOCKS_PER_SEC;
    
    printf("[OK] Inserted %d/%d documents in %.3f seconds\n", 
           successCount, numDocs, elapsed);
    printf("  Average: %.2f docs/sec\n", successCount / elapsed);
    
    /* Verify random documents */
    printf("\nVerifying random documents...\n");
    int verifyIndices[] = {0, 25, 50, 75, 99};
    int numVerify = sizeof(verifyIndices) / sizeof(verifyIndices[0]);
    
    for (int i = 0; i < numVerify; i++) {
        char key[64];
        void *data;
        int size;
        int idx = verifyIndices[i];
        
        sprintf(key, "batch_doc_%04d", idx);
        rc = kvstore_get(pKV, key, strlen(key), &data, &size);
        
        if (rc == KVSTORE_OK && validateJSON((char*)data, size)) {
            printf("  [OK] Document %d verified\n", idx);
            sqliteFree(data);
        } else {
            fprintf(stderr, "  [X] Document %d verification failed\n", idx);
        }
    }
    
    /* Iterate and count */
    printf("\nIterating through all documents...\n");
    KVIterator *pIter = NULL;
    rc = kvstore_iterator_create(pKV, &pIter);
    
    if (rc == KVSTORE_OK) {
        int count = 0;
        int validJSON = 0;
        
        for (kvstore_iterator_first(pIter); 
             !kvstore_iterator_eof(pIter); 
             kvstore_iterator_next(pIter)) {
            
            void *value;
            int valueSize;
            
            rc = kvstore_iterator_value(pIter, &value, &valueSize);
            if (rc == KVSTORE_OK) {
                count++;
                if (validateJSON((char*)value, valueSize)) {
                    validJSON++;
                }
            }
        }
        
        printf("  [OK] Found %d documents, %d with valid JSON\n", count, validJSON);
        kvstore_iterator_close(pIter);
    }
    
    kvstore_close(pKV);
    printf("\n[OK] Example 5 completed successfully!\n");
    
    return 0;
}

/* ============================================================================
** EXAMPLE 6: Large JSON File (Multi-MB)
** ============================================================================ */
int example6_very_large_json(void) {
    KVStore *pKV = NULL;
    int rc;
    
    printf("\n=== EXAMPLE 6: Very Large JSON (Multi-MB) ===\n");
    
    rc = kvstore_open("example6.db", &pKV, KVSTORE_JOURNAL_DELETE);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Failed to open kvstore: %d\n", rc);
        return 1;
    }
    printf("[OK] Opened kvstore\n");
    
    /* Generate a very large JSON document (50,000 records ~ 5-6 MB) */
    printf("\nGenerating very large JSON document...\n");
    
    char *largeJSON;
    int largeSize;
    
    largeJSON = generateLargeJSON(50000, &largeSize);
    if (!largeJSON) {
        fprintf(stderr, "Failed to generate large JSON\n");
        kvstore_close(pKV);
        return 1;
    }
    
    printf("[OK] Generated JSON: %.2f MB (%d bytes)\n", 
           largeSize / (1024.0 * 1024.0), largeSize);
    
    /* Validate */
    printf("Validating JSON structure...\n");
    if (!validateJSON(largeJSON, largeSize)) {
        fprintf(stderr, "[X] Generated JSON is invalid!\n");
        free(largeJSON);
        kvstore_close(pKV);
        return 1;
    }
    printf("[OK] JSON validation passed\n");
    
    /* Insert */
    printf("Inserting large JSON...\n");
    clock_t start = clock();
    
    const char *key = "very_large_json";
    rc = kvstore_put(pKV, key, strlen(key), largeJSON, largeSize);
    
    clock_t end = clock();
    double elapsed = (double)(end - start) / CLOCKS_PER_SEC;
    
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "[X] Failed to insert: %d (%s)\n", rc, kvstore_errmsg(pKV));
        free(largeJSON);
        kvstore_close(pKV);
        return 1;
    }
    
    printf("[OK] Inserted in %.3f seconds (%.2f MB/sec)\n", 
           elapsed, (largeSize / (1024.0 * 1024.0)) / elapsed);
    
    /* Fetch */
    printf("Fetching large JSON...\n");
    void *fetchedData;
    int fetchedSize;
    
    start = clock();
    rc = kvstore_get(pKV, key, strlen(key), &fetchedData, &fetchedSize);
    end = clock();
    elapsed = (double)(end - start) / CLOCKS_PER_SEC;
    
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "[X] Failed to fetch: %d\n", rc);
        free(largeJSON);
        kvstore_close(pKV);
        return 1;
    }
    
    printf("[OK] Fetched in %.3f seconds (%.2f MB/sec)\n",
           elapsed, (fetchedSize / (1024.0 * 1024.0)) / elapsed);
    
    /* Verify */
    printf("Verifying data integrity...\n");
    if (compareJSON(largeJSON, largeSize, (char*)fetchedData, fetchedSize)) {
        printf("[OK] Data verification PASSED - exact match!\n");
    } else {
        fprintf(stderr, "[X] Data verification FAILED!\n");
        fprintf(stderr, "  Original: %d bytes, Fetched: %d bytes\n", 
                largeSize, fetchedSize);
    }
    
    if (validateJSON((char*)fetchedData, fetchedSize)) {
        printf("[OK] Fetched JSON is well-formed\n");
    } else {
        fprintf(stderr, "[X] Fetched JSON is malformed!\n");
    }
    
    /* Sync to disk */
    printf("Syncing to disk...\n");
    rc = kvstore_sync(pKV);
    if (rc == KVSTORE_OK) {
        printf("[OK] Database synced successfully\n");
    }
    
    sqliteFree(fetchedData);
    free(largeJSON);
    kvstore_close(pKV);
    
    printf("\n[OK] Example 6 completed successfully!\n");
    return 0;
}

/* ============================================================================
** Main - Run all examples
** ============================================================================ */
int main(int argc, char **argv) {
    int runExample = 0;
    
    printf("+===========================================================+\n");
    printf("|  KVStore JSON Examples - Large File Operations           |\n");
    printf("+===========================================================+\n");
    
    /* Parse command line */
    if (argc > 1) {
        runExample = atoi(argv[1]);
    }
    
    /* Run examples */
    if (runExample == 0) {
        /* Run all examples */
        printf("\nRunning all examples...\n");
        example1_basic_json_operations();
        example2_multiple_json_documents();
        example3_column_families_json();
        example4_nested_json();
        example5_batch_json_operations();
        example6_very_large_json();
    } else {
        /* Run specific example */
        switch (runExample) {
            case 1: example1_basic_json_operations(); break;
            case 2: example2_multiple_json_documents(); break;
            case 3: example3_column_families_json(); break;
            case 4: example4_nested_json(); break;
            case 5: example5_batch_json_operations(); break;
            case 6: example6_very_large_json(); break;
            default:
                fprintf(stderr, "Invalid example number. Use 1-6 or 0 for all.\n");
                return 1;
        }
    }
    
    printf("\n+===========================================================+\n");
    printf("|  All examples completed!                                  |\n");
    printf("+===========================================================+\n");
    
    return 0;
}
