/*
** KVStore Performance Benchmark
** 
** Tests: Sequential writes, random reads, sequential scan, 
**        random updates, random deletes, bulk operations
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include "kvstore.h"

#define DB_FILE      "benchmark_kv.db"
#define NUM_RECORDS  50000
#define BATCH_SIZE   1000
#define NUM_READS    50000
#define NUM_UPDATES  10000
#define NUM_DELETES  5000

#define COLOR_BLUE   "\x1b[34m"
#define COLOR_GREEN  "\x1b[32m"
#define COLOR_YELLOW "\x1b[33m"
#define COLOR_CYAN   "\x1b[36m"
#define COLOR_RESET  "\x1b[0m"

/* High-resolution timer */
static double get_time(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

/* Format numbers with commas */
static void format_number(long long num, char *buf, size_t size) {
    if (num >= 1000000) {
        snprintf(buf, size, "%lld,%03lld,%03lld", 
                 num/1000000, (num/1000)%1000, num%1000);
    } else if (num >= 1000) {
        snprintf(buf, size, "%lld,%03lld", num/1000, num%1000);
    } else {
        snprintf(buf, size, "%lld", num);
    }
}

static void print_result(const char *test, double elapsed, int ops) {
    double ops_per_sec = ops / elapsed;
    char buf[32];
    format_number((long long)ops_per_sec, buf, sizeof(buf));
    
    printf("  %-30s: ", test);
    printf(COLOR_GREEN "%s ops/sec" COLOR_RESET " ", buf);
    printf("(%.3f seconds for %d ops)\n", elapsed, ops);
}

static void print_header(const char *title) {
    printf("\n" COLOR_CYAN);
    printf("════════════════════════════════════════════════════════\n");
    printf("  %s\n", title);
    printf("════════════════════════════════════════════════════════\n");
    printf(COLOR_RESET);
}

/* ==================== BENCHMARK 1: Sequential Writes ==================== */
static void bench_sequential_writes(KVStore *kv) {
    print_header("BENCHMARK 1: Sequential Writes");
    printf("  Writing %d records in batches of %d...\n\n", NUM_RECORDS, BATCH_SIZE);
    
    char key[32], value[128];
    int i;
    double start, end;
    
    start = get_time();
    
    for (i = 0; i < NUM_RECORDS; i++) {
        if (i % BATCH_SIZE == 0) {
            if (i > 0) kvstore_commit(kv);
            kvstore_begin(kv, 1);
        }
        
        snprintf(key, sizeof(key), "key_%08d", i);
        snprintf(value, sizeof(value), 
                 "value_%08d_with_some_additional_data_to_make_it_realistic", i);
        
        kvstore_put(kv, key, strlen(key), value, strlen(value));
    }
    kvstore_commit(kv);
    
    end = get_time();
    
    print_result("Sequential writes", end - start, NUM_RECORDS);
}

/* ==================== BENCHMARK 2: Random Reads ==================== */
static void bench_random_reads(KVStore *kv) {
    print_header("BENCHMARK 2: Random Reads");
    printf("  Reading %d random records...\n\n", NUM_READS);
    
    char key[32];
    void *value;
    int vlen, i;
    double start, end;
    
    start = get_time();
    
    for (i = 0; i < NUM_READS; i++) {
        int idx = rand() % NUM_RECORDS;
        snprintf(key, sizeof(key), "key_%08d", idx);
        
        if (kvstore_get(kv, key, strlen(key), &value, &vlen) == KVSTORE_OK) {
            sqliteFree(value);
        }
    }
    
    end = get_time();
    
    print_result("Random reads", end - start, NUM_READS);
}

/* ==================== BENCHMARK 3: Sequential Scan ==================== */
static void bench_sequential_scan(KVStore *kv) {
    print_header("BENCHMARK 3: Sequential Scan");
    printf("  Scanning all records...\n\n");
    
    KVIterator *it = NULL;
    void *key, *value;
    int klen, vlen, count = 0;
    double start, end;
    
    kvstore_iterator_create(kv, &it);
    
    start = get_time();
    
    kvstore_iterator_first(it);
    while (!kvstore_iterator_eof(it)) {
        kvstore_iterator_key(it, &key, &klen);
        kvstore_iterator_value(it, &value, &vlen);
        count++;
        kvstore_iterator_next(it);
    }
    
    end = get_time();
    
    kvstore_iterator_close(it);
    
    print_result("Sequential scan", end - start, count);
}

/* ==================== BENCHMARK 4: Random Updates ==================== */
static void bench_random_updates(KVStore *kv) {
    print_header("BENCHMARK 4: Random Updates");
    printf("  Updating %d random records...\n\n", NUM_UPDATES);
    
    char key[32], value[128];
    int i;
    double start, end;
    
    kvstore_begin(kv, 1);
    
    start = get_time();
    
    for (i = 0; i < NUM_UPDATES; i++) {
        int idx = rand() % NUM_RECORDS;
        snprintf(key, sizeof(key), "key_%08d", idx);
        snprintf(value, sizeof(value), "updated_value_%08d", idx);
        
        kvstore_put(kv, key, strlen(key), value, strlen(value));
    }
    
    kvstore_commit(kv);
    
    end = get_time();
    
    print_result("Random updates", end - start, NUM_UPDATES);
}

/* ==================== BENCHMARK 5: Random Deletes ==================== */
static void bench_random_deletes(KVStore *kv) {
    print_header("BENCHMARK 5: Random Deletes");
    printf("  Deleting %d random records...\n\n", NUM_DELETES);
    
    char key[32];
    int i;
    double start, end;
    
    kvstore_begin(kv, 1);
    
    start = get_time();
    
    for (i = 0; i < NUM_DELETES; i++) {
        int idx = rand() % NUM_RECORDS;
        snprintf(key, sizeof(key), "key_%08d", idx);
        kvstore_delete(kv, key, strlen(key));
    }
    
    kvstore_commit(kv);
    
    end = get_time();
    
    print_result("Random deletes", end - start, NUM_DELETES);
}

/* ==================== BENCHMARK 6: Exists Checks ==================== */
static void bench_exists_checks(KVStore *kv) {
    print_header("BENCHMARK 6: Exists Checks");
    printf("  Checking existence of %d keys...\n\n", NUM_READS);
    
    char key[32];
    int i, exists;
    double start, end;
    
    start = get_time();
    
    for (i = 0; i < NUM_READS; i++) {
        int idx = rand() % NUM_RECORDS;
        snprintf(key, sizeof(key), "key_%08d", idx);
        kvstore_exists(kv, key, strlen(key), &exists);
    }
    
    end = get_time();
    
    print_result("Exists checks", end - start, NUM_READS);
}

/* ==================== BENCHMARK 7: Mixed Workload ==================== */
static void bench_mixed_workload(KVStore *kv) {
    print_header("BENCHMARK 7: Mixed Workload");
    printf("  70%% reads, 20%% writes, 10%% deletes...\n\n");
    
    int total_ops = 20000;
    char key[32], value[128];
    void *val;
    int vlen, i;
    double start, end;
    
    kvstore_begin(kv, 1);
    
    start = get_time();
    
    for (i = 0; i < total_ops; i++) {
        int idx = rand() % NUM_RECORDS;
        int op = rand() % 100;
        
        snprintf(key, sizeof(key), "key_%08d", idx);
        
        if (op < 70) {
            /* Read */
            if (kvstore_get(kv, key, strlen(key), &val, &vlen) == KVSTORE_OK) {
                sqliteFree(val);
            }
        } else if (op < 90) {
            /* Write */
            snprintf(value, sizeof(value), "mixed_value_%08d", idx);
            kvstore_put(kv, key, strlen(key), value, strlen(value));
        } else {
            /* Delete */
            kvstore_delete(kv, key, strlen(key));
        }
    }
    
    kvstore_commit(kv);
    
    end = get_time();
    
    print_result("Mixed workload", end - start, total_ops);
}

/* ==================== BENCHMARK 8: Bulk Insert ==================== */
static void bench_bulk_insert(void) {
    print_header("BENCHMARK 8: Bulk Insert (Single Transaction)");
    printf("  Inserting %d records in one transaction...\n\n", NUM_RECORDS);
    
    KVStore *kv = NULL;
    char key[32], value[128];
    int i;
    double start, end;
    
    remove("benchmark_bulk.db");
    kvstore_open("benchmark_bulk.db", &kv, 0, KVSTORE_JOURNAL_DELETE);
    
    kvstore_begin(kv, 1);
    
    start = get_time();
    
    for (i = 0; i < NUM_RECORDS; i++) {
        snprintf(key, sizeof(key), "bulk_key_%08d", i);
        snprintf(value, sizeof(value), "bulk_value_%08d", i);
        kvstore_put(kv, key, strlen(key), value, strlen(value));
    }
    
    kvstore_commit(kv);
    
    end = get_time();
    
    kvstore_close(kv);
    remove("benchmark_bulk.db");
    
    print_result("Bulk insert", end - start, NUM_RECORDS);
}

/* ==================== Main ==================== */
int main(void) {
    KVStore *kv = NULL;
    double total_start, total_end;
    
    printf("\n");
    printf(COLOR_BLUE "╔══════════════════════════════════════════════════════════════╗\n");
    printf("║                  KVStore Performance Benchmark               ║\n");
    printf("║                                                              ║\n");
    printf("║  Database: %-50s║\n", DB_FILE);
    printf("║  Records:  %-50d║\n", NUM_RECORDS);
    printf("╚══════════════════════════════════════════════════════════════╝\n");
    printf(COLOR_RESET);
    
    srand(time(NULL));
    
    /* Initialize database */
    printf("\n" COLOR_YELLOW "Initializing database..." COLOR_RESET "\n");
    remove(DB_FILE);
    
    if (kvstore_open(DB_FILE, &kv, 0, KVSTORE_JOURNAL_WAL) != KVSTORE_OK) {
        fprintf(stderr, "Failed to open KVStore\n");
        return 1;
    }
    
    total_start = get_time();
    
    /* Run benchmarks */
    bench_sequential_writes(kv);
    bench_random_reads(kv);
    bench_sequential_scan(kv);
    bench_random_updates(kv);
    bench_random_deletes(kv);
    bench_exists_checks(kv);
    bench_mixed_workload(kv);
    
    KVStoreStats stats;
    kvstore_stats(kv, &stats);
    printf("  Total operations:\n");
    printf("    - Puts:    %llu\n", stats.nPuts);
    printf("    - Gets:    %llu\n", stats.nGets);
    printf("    - Deletes: %llu\n", stats.nDeletes);


    kvstore_close(kv);
    
    bench_bulk_insert();
    
    total_end = get_time();
    
    /* Summary */
    printf("\n" COLOR_CYAN);
    printf("════════════════════════════════════════════════════════\n");
    printf("  SUMMARY\n");
    printf("════════════════════════════════════════════════════════\n");
    printf(COLOR_RESET);
    printf("  Total benchmark time: " COLOR_GREEN "%.2f seconds" COLOR_RESET "\n", 
           total_end - total_start);
        
    printf("\n" COLOR_GREEN "✓ Benchmark complete!" COLOR_RESET "\n\n");
    
    /* Cleanup */
    remove(DB_FILE);
    
    return 0;
}
