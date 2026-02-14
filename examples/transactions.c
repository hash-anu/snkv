/* SPDX-License-Identifier: Apache-2.0 */
/*
** Transaction Examples
** Demonstrates: Atomic batch operations, rollback
*/

#define SNKV_IMPLEMENTATION
#include "snkv.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static int transfer_funds(KVStore *pKV, const char *from, const char *to,
                          int amount) {
    int rc;
    void *pValue;
    int nValue;

    rc = kvstore_begin(pKV, 1);
    if (rc != KVSTORE_OK) return rc;

    /* Get source balance */
    rc = kvstore_get(pKV, from, strlen(from), &pValue, &nValue);
    if (rc != KVSTORE_OK) {
        kvstore_rollback(pKV);
        return rc;
    }
    char tmp[32];
    memcpy(tmp, pValue, nValue);
    tmp[nValue] = '\0';
    int from_balance = atoi(tmp);
    snkv_free(pValue);

    if (from_balance < amount) {
        printf("Insufficient funds!\n");
        kvstore_rollback(pKV);
        return KVSTORE_ERROR;
    }

    /* Get destination balance */
    rc = kvstore_get(pKV, to, strlen(to), &pValue, &nValue);
    if (rc != KVSTORE_OK) {
        kvstore_rollback(pKV);
        return rc;
    }
    memcpy(tmp, pValue, nValue);
    tmp[nValue] = '\0';
    int to_balance = atoi(tmp);
    snkv_free(pValue);

    /* Update balances */
    char balance_str[32];

    sprintf(balance_str, "%d", from_balance - amount);
    kvstore_put(pKV, from, strlen(from), balance_str, strlen(balance_str));

    sprintf(balance_str, "%d", to_balance + amount);
    kvstore_put(pKV, to, strlen(to), balance_str, strlen(balance_str));

    rc = kvstore_commit(pKV);
    if (rc == KVSTORE_OK) {
        printf("Transfer successful: %s -> %s ($%d)\n", from, to, amount);
    } else {
        kvstore_rollback(pKV);
    }
    return rc;
}

static void example_atomic_transfer(void) {
    KVStore *pKV;
    void *pValue;
    int nValue;

    printf("=== Atomic Transfer ===\n");

    kvstore_open("bank.db", &pKV, 0, KVSTORE_JOURNAL_WAL);

    /* Initialize accounts */
    kvstore_put(pKV, "account:alice", 13, "1000", 4);
    kvstore_put(pKV, "account:bob", 11, "500", 3);

    /* Perform transfer */
    transfer_funds(pKV, "account:alice", "account:bob", 200);

    /* Check final balances */
    kvstore_get(pKV, "account:alice", 13, &pValue, &nValue);
    printf("Alice's balance: $%.*s\n", nValue, (char*)pValue);
    snkv_free(pValue);

    kvstore_get(pKV, "account:bob", 11, &pValue, &nValue);
    printf("Bob's balance: $%.*s\n", nValue, (char*)pValue);
    snkv_free(pValue);

    kvstore_close(pKV);
    remove("bank.db");
    printf("\n");
}

static void example_batch_insert(void) {
    KVStore *pKV;
    int rc;

    printf("=== Batch Insert with Rollback ===\n");

    kvstore_open("config.db", &pKV, 0, KVSTORE_JOURNAL_WAL);

    const char *keys[] = {"server.host", "server.port", "server.timeout"};
    const char *values[] = {"localhost", "8080", "30"};
    int count = 3;

    rc = kvstore_begin(pKV, 1);
    if (rc != KVSTORE_OK) {
        fprintf(stderr, "Failed to begin transaction\n");
        kvstore_close(pKV);
        return;
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
            kvstore_close(pKV);
            return;
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

    kvstore_close(pKV);
    remove("config.db");
    printf("\n");
}

int main(void) {
    example_atomic_transfer();
    example_batch_insert();
    printf("All transaction examples passed.\n");
    return 0;
}
