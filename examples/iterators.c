/* SPDX-License-Identifier: Apache-2.0 */
/*
** Iterator Examples
** Demonstrates: Basic scan, filtered iteration, store statistics
*/

#include "kvstore.h"
#include <stdio.h>
#include <string.h>

static void example_basic_scan(void) {
    KVStore *pKV;
    KVIterator *pIter;

    printf("=== Basic Iteration ===\n");

    kvstore_open("inventory.db", &pKV, 0, KVSTORE_JOURNAL_WAL);

    kvstore_put(pKV, "apple", 5, "50", 2);
    kvstore_put(pKV, "banana", 6, "30", 2);
    kvstore_put(pKV, "orange", 6, "40", 2);
    kvstore_put(pKV, "grape", 5, "60", 2);

    kvstore_iterator_create(pKV, &pIter);

    printf("%-10s %s\n", "Item", "Quantity");
    printf("------------------------\n");

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
    remove("inventory.db");
    printf("\n");
}

static void example_filtered_iteration(void) {
    KVStore *pKV;
    KVIterator *pIter;

    printf("=== Filtered Iteration ===\n");

    kvstore_open("roles.db", &pKV, 0, KVSTORE_JOURNAL_WAL);

    kvstore_put(pKV, "user:alice", 10, "Regular User", 12);
    kvstore_put(pKV, "user:bob", 8, "Regular User", 12);
    kvstore_put(pKV, "admin:charlie", 13, "Administrator", 13);
    kvstore_put(pKV, "admin:diana", 11, "Administrator", 13);
    kvstore_put(pKV, "user:eve", 8, "Regular User", 12);

    kvstore_iterator_create(pKV, &pIter);

    printf("Administrators:\n");
    for (kvstore_iterator_first(pIter);
         !kvstore_iterator_eof(pIter);
         kvstore_iterator_next(pIter)) {

        void *pKey, *pValue;
        int nKey, nValue;

        kvstore_iterator_key(pIter, &pKey, &nKey);

        if (nKey >= 6 && memcmp(pKey, "admin:", 6) == 0) {
            kvstore_iterator_value(pIter, &pValue, &nValue);
            printf("  %.*s: %.*s\n", nKey, (char*)pKey, nValue, (char*)pValue);
        }
    }

    kvstore_iterator_close(pIter);
    kvstore_close(pKV);
    remove("roles.db");
    printf("\n");
}

typedef struct {
    int total_keys;
    int total_key_bytes;
    int total_value_bytes;
    int max_key_size;
    int max_value_size;
} StoreStats;

static void calculate_stats(KVStore *pKV, StoreStats *stats) {
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

        if (nKey > stats->max_key_size) stats->max_key_size = nKey;
        if (nValue > stats->max_value_size) stats->max_value_size = nValue;
    }

    kvstore_iterator_close(pIter);
}

static void example_statistics(void) {
    KVStore *pKV;

    printf("=== Store Statistics ===\n");

    kvstore_open("data.db", &pKV, 0, KVSTORE_JOURNAL_WAL);

    kvstore_put(pKV, "a", 1, "short", 5);
    kvstore_put(pKV, "longer_key", 10, "medium value", 12);
    kvstore_put(pKV, "k", 1, "very long value string here", 27);

    StoreStats stats;
    calculate_stats(pKV, &stats);

    printf("  Total keys:        %d\n", stats.total_keys);
    printf("  Total key bytes:   %d\n", stats.total_key_bytes);
    printf("  Total value bytes: %d\n", stats.total_value_bytes);
    printf("  Max key size:      %d\n", stats.max_key_size);
    printf("  Max value size:    %d\n", stats.max_value_size);
    printf("  Avg key size:      %.2f\n",
           (double)stats.total_key_bytes / stats.total_keys);
    printf("  Avg value size:    %.2f\n",
           (double)stats.total_value_bytes / stats.total_keys);

    /* Also show the built-in kvstore stats */
    KVStoreStats kstats;
    kvstore_stats(pKV, &kstats);
    printf("\n  Built-in Stats:\n");
    printf("    Puts: %llu\n", (unsigned long long)kstats.nPuts);
    printf("    Gets: %llu\n", (unsigned long long)kstats.nGets);
    printf("    Iterations: %llu\n", (unsigned long long)kstats.nIterations);

    kvstore_close(pKV);
    remove("data.db");
    printf("\n");
}

static void example_prefix_iteration(void) {
    KVStore *pKV;
    KVIterator *pIter;

    printf("=== Prefix Iteration ===\n");

    kvstore_open("prefix.db", &pKV, 0, KVSTORE_JOURNAL_WAL);

    /* Populate store */
    kvstore_put(pKV, "user:alice", 10, "online", 6);
    kvstore_put(pKV, "user:bob", 8, "offline", 7);
    kvstore_put(pKV, "user:charlie", 12, "online", 6);
    kvstore_put(pKV, "admin:root", 10, "active", 6);
    kvstore_put(pKV, "admin:dba", 9, "inactive", 8);

    /* Create prefix iterator for "user:" */
    kvstore_prefix_iterator_create(pKV, "user:", 5, &pIter);

    printf("%-15s %s\n", "Key", "Value");
    printf("-------------------------------\n");

    for (kvstore_iterator_first(pIter);
         !kvstore_iterator_eof(pIter);
         kvstore_iterator_next(pIter)) {

        void *pKey, *pValue;
        int nKey, nValue;

        kvstore_iterator_key(pIter, &pKey, &nKey);
        kvstore_iterator_value(pIter, &pValue, &nValue);

        printf("%-15.*s %.*s\n",
               nKey, (char*)pKey,
               nValue, (char*)pValue);
    }

    kvstore_iterator_close(pIter);
    kvstore_close(pKV);
    remove("prefix.db");

    printf("\n");
}

int main(void) {
    example_basic_scan();
    example_filtered_iteration();
    example_statistics();
    example_prefix_iteration();
    printf("All iterator examples passed.\n");
    return 0;
}
