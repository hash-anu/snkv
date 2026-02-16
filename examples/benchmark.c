/* SPDX-License-Identifier: Apache-2.0 */
/*
** Benchmark Example
** Demonstrates: Auto-commit vs batch transaction performance
*/

#define SNKV_IMPLEMENTATION
#include "snkv.h"
#include <stdio.h>
#include <string.h>
#include <time.h>

static double benchmark_inserts(KVStore *pKV, int count, int use_transaction) {
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

int main(void) {
    KVStore *pKV;
    double time_no_tx, time_with_tx;
    int num_ops = 10000;

    printf("=== Benchmark: %d insert operations ===\n\n", num_ops);

    /* Test without transaction (auto-commit each operation) */
    printf("Without transaction (auto-commit):\n");
    kvstore_open("bench_auto.db", &pKV, KVSTORE_JOURNAL_WAL);
    time_no_tx = benchmark_inserts(pKV, num_ops, 0);
    printf("  Time: %.3f seconds\n", time_no_tx);
    if (time_no_tx > 0) {
        printf("  Rate: %.0f ops/sec\n", num_ops / time_no_tx);
    }
    kvstore_close(pKV);

    /* Test with single transaction */
    printf("\nWith transaction (batch commit):\n");
    kvstore_open("bench_batch.db", &pKV, KVSTORE_JOURNAL_WAL);
    time_with_tx = benchmark_inserts(pKV, num_ops, 1);
    printf("  Time: %.3f seconds\n", time_with_tx);
    if (time_with_tx > 0) {
        printf("  Rate: %.0f ops/sec\n", num_ops / time_with_tx);
    }
    kvstore_close(pKV);

    if (time_with_tx > 0 && time_no_tx > 0) {
        printf("\nSpeedup: %.1fx faster\n", time_no_tx / time_with_tx);
    }

    remove("bench_auto.db");
    remove("bench_batch.db");

    printf("\nBenchmark example completed.\n");
    return 0;
}
