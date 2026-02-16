/* SPDX-License-Identifier: Apache-2.0 */
/*
** Column Family Examples
** Demonstrates: Creating CFs, storing data in separate namespaces,
**               listing and dropping column families
*/

#define SNKV_IMPLEMENTATION
#include "snkv.h"
#include <stdio.h>
#include <string.h>

static void list_column_families(KVStore *pKV) {
    char **azNames;
    int nCount;

    int rc = kvstore_cf_list(pKV, &azNames, &nCount);
    if (rc == KVSTORE_OK) {
        printf("Column Families (%d total):\n", nCount);
        for (int i = 0; i < nCount; i++) {
            printf("  - %s\n", azNames[i]);
            snkv_free(azNames[i]);
        }
        snkv_free(azNames);
    }
}

static void example_data_organization(void) {
    KVStore *pKV;
    KVColumnFamily *pUsersCF, *pProductsCF, *pOrdersCF;
    void *pValue;
    int nValue;

    printf("=== Organizing Data with Column Families ===\n");

    kvstore_open("ecommerce.db", &pKV, KVSTORE_JOURNAL_WAL);

    /* Create column families */
    printf("Creating column families...\n");
    kvstore_cf_create(pKV, "users", &pUsersCF);
    kvstore_cf_create(pKV, "products", &pProductsCF);
    kvstore_cf_create(pKV, "orders", &pOrdersCF);

    /* Store user data */
    kvstore_cf_put(pUsersCF, "user:1", 6, "alice@example.com", 17);
    kvstore_cf_put(pUsersCF, "user:2", 6, "bob@example.com", 15);

    /* Store product data */
    kvstore_cf_put(pProductsCF, "prod:100", 8, "Laptop:$999", 11);
    kvstore_cf_put(pProductsCF, "prod:101", 8, "Mouse:$29", 9);

    /* Store order data */
    kvstore_cf_put(pOrdersCF, "order:1", 7, "user:1,prod:100", 15);
    kvstore_cf_put(pOrdersCF, "order:2", 7, "user:2,prod:101", 15);

    /* Retrieve data from different CFs */
    printf("\n--- Retrieval ---\n");

    kvstore_cf_get(pUsersCF, "user:1", 6, &pValue, &nValue);
    printf("User 1: %.*s\n", nValue, (char*)pValue);
    snkv_free(pValue);

    kvstore_cf_get(pProductsCF, "prod:100", 8, &pValue, &nValue);
    printf("Product 100: %.*s\n", nValue, (char*)pValue);
    snkv_free(pValue);

    kvstore_cf_get(pOrdersCF, "order:1", 7, &pValue, &nValue);
    printf("Order 1: %.*s\n", nValue, (char*)pValue);
    snkv_free(pValue);

    kvstore_cf_close(pUsersCF);
    kvstore_cf_close(pProductsCF);
    kvstore_cf_close(pOrdersCF);
    kvstore_close(pKV);
    remove("ecommerce.db");
    printf("\n");
}

static void example_cf_management(void) {
    KVStore *pKV;

    printf("=== Listing and Managing Column Families ===\n");

    kvstore_open("multi_cf.db", &pKV, KVSTORE_JOURNAL_WAL);

    printf("--- Initial State ---\n");
    list_column_families(pKV);

    /* Create several column families */
    KVColumnFamily *pCF1, *pCF2, *pCF3;
    kvstore_cf_create(pKV, "logs", &pCF1);
    kvstore_cf_create(pKV, "metrics", &pCF2);
    kvstore_cf_create(pKV, "cache", &pCF3);
    printf("Created: logs, metrics, cache\n");

    printf("\n--- After Creation ---\n");
    list_column_families(pKV);

    /* Drop one column family */
    printf("\n--- Dropping 'cache' CF ---\n");
    kvstore_cf_close(pCF3);
    kvstore_cf_drop(pKV, "cache");

    printf("\n--- After Drop ---\n");
    list_column_families(pKV);

    kvstore_cf_close(pCF1);
    kvstore_cf_close(pCF2);
    kvstore_close(pKV);
    remove("multi_cf.db");
    printf("\n");
}

int main(void) {
    example_data_organization();
    example_cf_management();
    printf("All column family examples passed.\n");
    return 0;
}
