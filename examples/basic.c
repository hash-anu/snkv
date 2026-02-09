/* SPDX-License-Identifier: Apache-2.0 */
/*
** Basic KVStore Examples
** Demonstrates: Hello World, CRUD operations, existence checks
*/

#include "kvstore.h"
#include <stdio.h>
#include <string.h>

static void print_user(const char *user_id, KVStore *pKV) {
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

static void example_hello_world(void) {
    KVStore *pKV = NULL;
    int rc;

    printf("=== Hello World ===\n");

    rc = kvstore_open("hello.db", &pKV, 0, KVSTORE_JOURNAL_WAL);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Failed to open database\n");
        return;
    }

    /* Store a greeting */
    const char *key = "greeting";
    const char *value = "Hello, World!";
    rc = kvstore_put(pKV, key, strlen(key), value, strlen(value));
    if (rc == KVSTORE_OK) {
        printf("Stored: %s = %s\n", key, value);
    }

    /* Retrieve the greeting */
    void *pValue = NULL;
    int nValue = 0;
    rc = kvstore_get(pKV, key, strlen(key), &pValue, &nValue);
    if (rc == KVSTORE_OK) {
        printf("Retrieved: %s = %.*s\n", key, nValue, (char*)pValue);
        sqliteFree(pValue);
    }

    kvstore_close(pKV);
    remove("hello.db");
    printf("\n");
}

static void example_crud(void) {
    KVStore *pKV;

    printf("=== CRUD Operations ===\n");

    kvstore_open("users.db", &pKV, 0, KVSTORE_JOURNAL_WAL);

    /* CREATE */
    printf("--- CREATE ---\n");
    kvstore_put(pKV, "user:1", 6, "Alice Smith", 11);
    print_user("user:1", pKV);

    /* READ */
    printf("--- READ ---\n");
    print_user("user:1", pKV);

    /* UPDATE */
    printf("--- UPDATE ---\n");
    kvstore_put(pKV, "user:1", 6, "Alice Johnson", 13);
    print_user("user:1", pKV);

    /* DELETE */
    printf("--- DELETE ---\n");
    kvstore_delete(pKV, "user:1", 6);
    print_user("user:1", pKV);

    kvstore_close(pKV);
    remove("users.db");
    printf("\n");
}

static void example_existence(void) {
    KVStore *pKV;

    printf("=== Existence Check ===\n");

    kvstore_open("inventory.db", &pKV, 0, KVSTORE_JOURNAL_WAL);

    kvstore_put(pKV, "item:laptop", 11, "In Stock", 8);
    kvstore_put(pKV, "item:mouse", 10, "Out of Stock", 12);

    const char *items[] = {"item:laptop", "item:mouse", "item:keyboard"};
    int num_items = 3;

    for (int i = 0; i < num_items; i++) {
        int exists = 0;
        kvstore_exists(pKV, items[i], strlen(items[i]), &exists);
        printf("%s: %s\n", items[i], exists ? "EXISTS" : "NOT FOUND");
    }

    kvstore_close(pKV);
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
