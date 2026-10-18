/* SPDX-License-Identifier: Apache-2.0 */
/*
** checkpoint_demo.c
** Demonstrates: walAutoCheckpoint (page-based auto-checkpoint, on by
** default at 1000 pages) and manual kvstore_checkpoint()
*/

#define SNKV_IMPLEMENTATION
#include "snkv.h"
#include <stdio.h>
#include <string.h>

/* ------------------------------------------------------------------ */
static void example_auto_checkpoint(void)
{
    printf("=== Auto-Checkpoint via walAutoCheckpoint ===\n");

    KVStoreConfig cfg = {0};
    cfg.journalMode = KVSTORE_JOURNAL_WAL;
    cfg.walAutoCheckpoint = 20;   /* checkpoint once the WAL holds 20 pages
                                  ** (0 = default 1000 pages, -1 = off) */

    KVStore *db;
    int rc = kvstore_open_v2("ckpt_auto.db", &db, &cfg);
    printf("  open (walAutoCheckpoint=20 pages): %s\n", rc == KVSTORE_OK ? "OK" : "FAIL");

    /* Write 60 records, one commit each -- the WAL passes 20 pages
    ** repeatedly, so auto-checkpoints fire automatically */
    int i;
    for (i = 0; i < 60; i++) {
        char key[32], val[32];
        int nk = snprintf(key, sizeof(key), "key_%04d", i);
        int nv = snprintf(val, sizeof(val), "val_%04d", i);
        kvstore_put(db, key, nk, val, nv);
    }
    KVStoreStats st;
    kvstore_stats(db, &st);
    printf("  wrote 60 records (%llu auto-checkpoints fired)\n",
           (unsigned long long)st.nCheckpoints);

    /* Manual PASSIVE checkpoint -- all frames already copied by auto-checkpoints */
    int nLog = 0, nCkpt = 0;
    rc = kvstore_checkpoint(db, KVSTORE_CHECKPOINT_PASSIVE, &nLog, &nCkpt);
    printf("  PASSIVE checkpoint: rc=%d  nLog=%d  nCkpt=%d\n", rc, nLog, nCkpt);
    printf("  (nLog==nCkpt means no frames are stuck -- WAL is fully copied)\n");

    kvstore_close(db);
    remove("ckpt_auto.db");
    remove("ckpt_auto.db-wal");
    remove("ckpt_auto.db-shm");
    printf("\n");
}

/* ------------------------------------------------------------------ */
static void example_manual_checkpoint(void)
{
    printf("=== Manual Checkpoint (PASSIVE then TRUNCATE) ===\n");

    KVStoreConfig cfg = {0};
    cfg.journalMode  = KVSTORE_JOURNAL_WAL;
    cfg.walAutoCheckpoint = -1;   /* no auto-checkpoint -- we control it manually */

    KVStore *db;
    kvstore_open_v2("ckpt_manual.db", &db, &cfg);

    /* Write 50 records -- WAL grows without auto-checkpoint */
    int i;
    for (i = 0; i < 50; i++) {
        char key[32], val[32];
        int nk = snprintf(key, sizeof(key), "key_%04d", i);
        int nv = snprintf(val, sizeof(val), "val_%04d", i);
        kvstore_put(db, key, nk, val, nv);
    }
    printf("  wrote 50 records (no auto-checkpoint)\n");

    /* PASSIVE checkpoint: copy frames without blocking readers/writers */
    int nLog = 0, nCkpt = 0;
    int rc = kvstore_checkpoint(db, KVSTORE_CHECKPOINT_PASSIVE, &nLog, &nCkpt);
    printf("  PASSIVE:  rc=%d  nLog=%d  nCkpt=%d\n", rc, nLog, nCkpt);

    /* Write 50 more records */
    for (i = 50; i < 100; i++) {
        char key[32], val[32];
        int nk = snprintf(key, sizeof(key), "key_%04d", i);
        int nv = snprintf(val, sizeof(val), "val_%04d", i);
        kvstore_put(db, key, nk, val, nv);
    }
    printf("  wrote 50 more records\n");

    /* TRUNCATE checkpoint: copy all frames AND truncate WAL file to zero */
    rc = kvstore_checkpoint(db, KVSTORE_CHECKPOINT_TRUNCATE, &nLog, &nCkpt);
    printf("  TRUNCATE: rc=%d  nLog=%d  nCkpt=%d\n", rc, nLog, nCkpt);
    printf("  (nLog==0 means WAL file has been truncated to zero bytes)\n");

    kvstore_close(db);
    remove("ckpt_manual.db");
    remove("ckpt_manual.db-wal");
    remove("ckpt_manual.db-shm");
    printf("\n");
}

/* ------------------------------------------------------------------ */
static void example_busy_guard(void)
{
    printf("=== Checkpoint Rejected During Write Transaction ===\n");

    KVStoreConfig cfg = {0};
    cfg.journalMode = KVSTORE_JOURNAL_WAL;

    KVStore *db;
    kvstore_open_v2("ckpt_busy.db", &db, &cfg);

    kvstore_begin(db, 1);   /* open explicit write transaction */
    kvstore_put(db, "k", 1, "v", 1);

    int rc = kvstore_checkpoint(db, KVSTORE_CHECKPOINT_PASSIVE, NULL, NULL);
    printf("  checkpoint during write txn: rc=%d (%s)\n",
           rc, rc == KVSTORE_BUSY ? "KVSTORE_BUSY -- correctly rejected" : "unexpected");

    kvstore_rollback(db);
    kvstore_close(db);
    remove("ckpt_busy.db");
    remove("ckpt_busy.db-wal");
    remove("ckpt_busy.db-shm");
    printf("\n");
}

/* ------------------------------------------------------------------ */
int main(void)
{
    example_auto_checkpoint();
    example_manual_checkpoint();
    example_busy_guard();
    printf("All checkpoint examples completed.\n");
    return 0;
}
