/* SPDX-License-Identifier: Apache-2.0 */
/*
** Basic KeyValueStore Examples
** Demonstrates: Hello World, CRUD operations, existence checks
*/

#define SNKV_IMPLEMENTATION
#include "snkv.h"
#include <stdio.h>
#include <string.h>

static void print_user(const char *user_id, KeyValueStore *pKV) {
    void *pValue;
    int nValue;

    int rc = keyvaluestore_get(pKV, user_id, strlen(user_id), &pValue, &nValue);
    if (rc == KEYVALUESTORE_OK) {
        printf("User %s: %.*s\n", user_id, nValue, (char*)pValue);
        snkv_free(pValue);
    } else if (rc == KEYVALUESTORE_NOTFOUND) {
        printf("User %s: Not found\n", user_id);
    }
}

static void example_hello_world(void) {
    KeyValueStore *pKV = NULL;
    int rc;

    printf("=== Hello World ===\n");

    rc = keyvaluestore_open("hello.db", &pKV, KEYVALUESTORE_JOURNAL_WAL);
    if (rc != KEYVALUESTORE_OK) {
        fprintf(stderr, "Failed to open database\n");
        return;
    }

    /* Store a greeting */
    const char *key = "greeting";
    const char *value = "Hello, World!";
    rc = keyvaluestore_put(pKV, key, strlen(key), value, strlen(value));
    if (rc == KEYVALUESTORE_OK) {
        printf("Stored: %s = %s\n", key, value);
    }

    /* Retrieve the greeting */
    void *pValue = NULL;
    int nValue = 0;
    rc = keyvaluestore_get(pKV, key, strlen(key), &pValue, &nValue);
    if (rc == KEYVALUESTORE_OK) {
        printf("Retrieved: %s = %.*s\n", key, nValue, (char*)pValue);
        snkv_free(pValue);
    }

    keyvaluestore_close(pKV);
    remove("hello.db");
    printf("\n");
}

static void example_crud(void) {
    KeyValueStore *pKV;

    printf("=== CRUD Operations ===\n");

    keyvaluestore_open("users.db", &pKV, KEYVALUESTORE_JOURNAL_WAL);

    /* CREATE */
    printf("--- CREATE ---\n");
    keyvaluestore_put(pKV, "user:1", 6, "Alice Smith", 11);
    print_user("user:1", pKV);

    /* READ */
    printf("--- READ ---\n");
    print_user("user:1", pKV);

    /* UPDATE */
    printf("--- UPDATE ---\n");
    keyvaluestore_put(pKV, "user:1", 6, "Alice Johnson", 13);
    print_user("user:1", pKV);

    /* DELETE */
    printf("--- DELETE ---\n");
    keyvaluestore_delete(pKV, "user:1", 6);
    print_user("user:1", pKV);

    keyvaluestore_close(pKV);
    remove("users.db");
    printf("\n");
}

static void example_existence(void) {
    KeyValueStore *pKV;

    printf("=== Existence Check ===\n");

    keyvaluestore_open("inventory.db", &pKV, KEYVALUESTORE_JOURNAL_WAL);

    keyvaluestore_put(pKV, "item:laptop", 11, "In Stock", 8);
    keyvaluestore_put(pKV, "item:mouse", 10, "Out of Stock", 12);

    const char *items[] = {"item:laptop", "item:mouse", "item:keyboard"};
    int num_items = 3;

    for (int i = 0; i < num_items; i++) {
        int exists = 0;
        keyvaluestore_exists(pKV, items[i], strlen(items[i]), &exists);
        printf("%s: %s\n", items[i], exists ? "EXISTS" : "NOT FOUND");
    }

    keyvaluestore_close(pKV);
    remove("inventory.db");
    printf("\n");
}

int main(void) {
    example_hello_world();
    example_crud();
    example_existence();
    printf("All basic examples passed.\n");
    return 0;
}
