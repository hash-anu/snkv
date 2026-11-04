/* SPDX-License-Identifier: Apache-2.0 */
/*
** Maintenance: Durability, Integrity, Reclaiming Space
** Demonstrates: kvstore_sync, kvstore_integrity_check,
**               kvstore_incremental_vacuum
**
** kvstore_sync        — force everything committed so far to disk.  Useful
**                       with KVSTORE_SYNC_OFF (fast, no fsync per commit,
**                       common on flash-wear-sensitive devices): call it at
**                       the points where data must survive a power cut.
** kvstore_integrity_check — walk every B-tree and report damage.  Run it
**                       after an unclean shutdown, before a backup, or in a
**                       periodic health check.
** kvstore_incremental_vacuum — deleting keys frees pages inside the file
**                       but does not shrink it; this returns them to the
**                       file system.
*/

#define SNKV_IMPLEMENTATION
#include "snkv.h"
#include <stdio.h>
#include <string.h>

#define DB_FILE "maintenance.db"

static void cleanup(void) {
    remove(DB_FILE);
    remove(DB_FILE "-wal");
    remove(DB_FILE "-shm");
    remove(DB_FILE "-journal");
}

static long file_size(const char *path) {
    FILE *f = fopen(path, "rb");
    long n = -1;
    if (f) { fseek(f, 0, SEEK_END); n = ftell(f); fclose(f); }
    return n;
}

/* 1. kvstore_sync with fsync switched off for ordinary commits. */
static void example_sync(void) {
    KVStore *pKV = NULL;
    KVStoreConfig cfg;
    char key[32];

    printf("=== kvstore_sync: choose when data reaches the disk ===\n");
    memset(&cfg, 0, sizeof cfg);
    cfg.journalMode = KVSTORE_JOURNAL_WAL;
    cfg.syncLevel   = KVSTORE_SYNC_OFF;          /* no fsync on commit */
    kvstore_open_v2(DB_FILE, &pKV, &cfg);

    for (int i = 0; i < 100; i++) {
        snprintf(key, sizeof key, "reading:%03d", i);
        kvstore_put(pKV, key, (int)strlen(key), "21.5C", 5);
    }
    printf("100 commits with SYNC_OFF: fast, but a power cut could lose them.\n");

    int rc = kvstore_sync(pKV);                  /* the durability point */
    printf("kvstore_sync -> %s: everything committed so far is now on disk.\n",
           rc == KVSTORE_OK ? "OK" : "error");

    /* Not allowed inside a write transaction: commit first. */
    kvstore_begin(pKV, 1);
    rc = kvstore_sync(pKV);
    printf("kvstore_sync inside a transaction -> %s\n\n",
           rc == KVSTORE_OK ? "OK (unexpected)" : "refused: commit or roll back first");
    kvstore_rollback(pKV);
    kvstore_close(pKV);
    cleanup();
}

/* 2. kvstore_integrity_check. */
static void example_integrity(void) {
    KVStore *pKV = NULL;
    char *zErr = NULL;

    printf("=== kvstore_integrity_check: verify the file ===\n");
    kvstore_open(DB_FILE, &pKV, KVSTORE_JOURNAL_WAL);
    kvstore_put(pKV, "a", 1, "1", 1);

    int rc = kvstore_integrity_check(pKV, &zErr);
    if (rc == KVSTORE_OK) {
        printf("integrity check -> OK\n\n");
    } else {
        /* KVSTORE_CORRUPT: zErr describes what is wrong. */
        printf("integrity check -> rc=%d: %s\n\n", rc, zErr ? zErr : "");
    }
    snkv_free(zErr);                             /* always free (may be NULL) */
    kvstore_close(pKV);
    cleanup();
}

/* 3. kvstore_incremental_vacuum: give freed pages back to the disk. */
static void example_vacuum(void) {
    KVStore *pKV = NULL;
    char key[32], value[200];

    printf("=== kvstore_incremental_vacuum: shrink the file after deletes ===\n");
    /* Rollback journal so all data lives in the main file and its size is
    ** easy to see (in WAL mode recent pages sit in the -wal file). */
    kvstore_open(DB_FILE, &pKV, KVSTORE_JOURNAL_DELETE);
    memset(value, 'x', sizeof value);

    kvstore_begin(pKV, 1);
    for (int i = 0; i < 2000; i++) {
        snprintf(key, sizeof key, "item:%05d", i);
        kvstore_put(pKV, key, (int)strlen(key), value, (int)sizeof value);
    }
    kvstore_commit(pKV);
    printf("after 2000 puts:          %7ld bytes\n", file_size(DB_FILE));

    kvstore_begin(pKV, 1);
    for (int i = 0; i < 1800; i++) {
        snprintf(key, sizeof key, "item:%05d", i);
        kvstore_delete(pKV, key, (int)strlen(key));
    }
    kvstore_commit(pKV);
    printf("after deleting 1800:      %7ld bytes  (freed pages stay in the file)\n",
           file_size(DB_FILE));

    kvstore_incremental_vacuum(pKV, 0);          /* 0 = free every unused page */
    printf("after incremental_vacuum: %7ld bytes\n", file_size(DB_FILE));
    printf("(pass nPage > 0 to free at most that many pages per call, e.g. a\n"
           " little at a time on a device that must stay responsive)\n");

    kvstore_close(pKV);
    cleanup();
}

int main(void) {
    cleanup();
    example_sync();
    example_integrity();
    example_vacuum();
    printf("\n[OK] maintenance.c example complete.\n");
    return 0;
}
