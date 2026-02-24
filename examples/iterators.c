/* SPDX-License-Identifier: Apache-2.0 */
/*
** Iterator Examples
** Demonstrates: Basic scan, filtered iteration, store statistics
*/

#define SNKV_IMPLEMENTATION
#include "snkv.h"
#include <stdio.h>
#include <string.h>
#include <inttypes.h>

static void example_basic_scan(void) {
    KeyValueStore *pKV;
    KeyValueIterator *pIter;

    printf("=== Basic Iteration ===\n");

    keyvaluestore_open("inventory.db", &pKV, KEYVALUESTORE_JOURNAL_WAL);

    keyvaluestore_put(pKV, "apple", 5, "50", 2);
    keyvaluestore_put(pKV, "banana", 6, "30", 2);
    keyvaluestore_put(pKV, "orange", 6, "40", 2);
    keyvaluestore_put(pKV, "grape", 5, "60", 2);

    keyvaluestore_iterator_create(pKV, &pIter);

    printf("%-10s %s\n", "Item", "Quantity");
    printf("------------------------\n");

    for (keyvaluestore_iterator_first(pIter);
         !keyvaluestore_iterator_eof(pIter);
         keyvaluestore_iterator_next(pIter)) {

        void *pKey, *pValue;
        int nKey, nValue;

        keyvaluestore_iterator_key(pIter, &pKey, &nKey);
        keyvaluestore_iterator_value(pIter, &pValue, &nValue);

        printf("%-10.*s %.*s\n", nKey, (char*)pKey, nValue, (char*)pValue);
    }

    keyvaluestore_iterator_close(pIter);
    keyvaluestore_close(pKV);
    remove("inventory.db");
    printf("\n");
}

static void example_filtered_iteration(void) {
    KeyValueStore *pKV;
    KeyValueIterator *pIter;

    printf("=== Filtered Iteration ===\n");

    keyvaluestore_open("roles.db", &pKV, KEYVALUESTORE_JOURNAL_WAL);

    keyvaluestore_put(pKV, "user:alice", 10, "Regular User", 12);
    keyvaluestore_put(pKV, "user:bob", 8, "Regular User", 12);
    keyvaluestore_put(pKV, "admin:charlie", 13, "Administrator", 13);
    keyvaluestore_put(pKV, "admin:diana", 11, "Administrator", 13);
    keyvaluestore_put(pKV, "user:eve", 8, "Regular User", 12);

    keyvaluestore_iterator_create(pKV, &pIter);

    printf("Administrators:\n");
    for (keyvaluestore_iterator_first(pIter);
         !keyvaluestore_iterator_eof(pIter);
         keyvaluestore_iterator_next(pIter)) {

        void *pKey, *pValue;
        int nKey, nValue;

        keyvaluestore_iterator_key(pIter, &pKey, &nKey);

        if (nKey >= 6 && memcmp(pKey, "admin:", 6) == 0) {
            keyvaluestore_iterator_value(pIter, &pValue, &nValue);
            printf("  %.*s: %.*s\n", nKey, (char*)pKey, nValue, (char*)pValue);
        }
    }

    keyvaluestore_iterator_close(pIter);
    keyvaluestore_close(pKV);
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

static void calculate_stats(KeyValueStore *pKV, StoreStats *stats) {
    KeyValueIterator *pIter;

    memset(stats, 0, sizeof(StoreStats));
    keyvaluestore_iterator_create(pKV, &pIter);

    for (keyvaluestore_iterator_first(pIter);
         !keyvaluestore_iterator_eof(pIter);
         keyvaluestore_iterator_next(pIter)) {

        void *pKey, *pValue;
        int nKey, nValue;

        keyvaluestore_iterator_key(pIter, &pKey, &nKey);
        keyvaluestore_iterator_value(pIter, &pValue, &nValue);

        stats->total_keys++;
        stats->total_key_bytes += nKey;
        stats->total_value_bytes += nValue;

        if (nKey > stats->max_key_size) stats->max_key_size = nKey;
        if (nValue > stats->max_value_size) stats->max_value_size = nValue;
    }

    keyvaluestore_iterator_close(pIter);
}

static void example_statistics(void) {
    KeyValueStore *pKV;

    printf("=== Store Statistics ===\n");

    keyvaluestore_open("data.db", &pKV, KEYVALUESTORE_JOURNAL_WAL);

    keyvaluestore_put(pKV, "a", 1, "short", 5);
    keyvaluestore_put(pKV, "longer_key", 10, "medium value", 12);
    keyvaluestore_put(pKV, "k", 1, "very long value string here", 27);

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
    KeyValueStoreStats kstats;
    keyvaluestore_stats(pKV, &kstats);
    printf("\n  Built-in Stats:\n");
    printf("    Puts: %" PRIu64 "\n", kstats.nPuts);
    printf("    Gets: %" PRIu64 "\n", kstats.nGets);
    printf("    Iterations: %" PRIu64 "\n", kstats.nIterations);

    keyvaluestore_close(pKV);
    remove("data.db");
    printf("\n");
}

static void example_prefix_iteration(void) {
    KeyValueStore *pKV;
    KeyValueIterator *pIter;

    printf("=== Prefix Iteration ===\n");

    keyvaluestore_open("prefix.db", &pKV, KEYVALUESTORE_JOURNAL_WAL);

    /* Populate store */
    keyvaluestore_put(pKV, "user:alice", 10, "online", 6);
    keyvaluestore_put(pKV, "user:bob", 8, "offline", 7);
    keyvaluestore_put(pKV, "user:charlie", 12, "online", 6);
    keyvaluestore_put(pKV, "admin:root", 10, "active", 6);
    keyvaluestore_put(pKV, "admin:dba", 9, "inactive", 8);

    /* Create prefix iterator for "user:" */
    keyvaluestore_prefix_iterator_create(pKV, "user:", 5, &pIter);

    printf("%-15s %s\n", "Key", "Value");
    printf("-------------------------------\n");

    for (keyvaluestore_iterator_first(pIter);
         !keyvaluestore_iterator_eof(pIter);
         keyvaluestore_iterator_next(pIter)) {

        void *pKey, *pValue;
        int nKey, nValue;

        keyvaluestore_iterator_key(pIter, &pKey, &nKey);
        keyvaluestore_iterator_value(pIter, &pValue, &nValue);

        printf("%-15.*s %.*s\n",
               nKey, (char*)pKey,
               nValue, (char*)pValue);
    }

    keyvaluestore_iterator_close(pIter);
    keyvaluestore_close(pKV);
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
