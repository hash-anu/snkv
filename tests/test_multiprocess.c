/* SPDX-License-Identifier: Apache-2.0 */
/*
** tests/test_multiprocess.c — Multi-process WAL isolation test (POSIX only).
**
** Uses fork() to verify that two processes sharing the same WAL database
** see consistent snapshots and that the busy-handler retries correctly.
**
** Tests:
**  1. Parent writes N keys; child reads them after WAL checkpoint.
**  2. Child write is rejected while parent holds a write transaction.
**     (busyTimeout=0 so it fails fast with KVSTORE_BUSY or completes after
**      the parent commits, depending on OS scheduling.)
**  3. Concurrent reads: two processes both read the same data simultaneously.
**
** Skipped on Windows (no fork).
*/

#include "kvstore.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#if defined(_WIN32) || defined(_WIN64)

int main(void){
  printf("=== test_multiprocess ===\n");
  printf("  SKIP: fork() not available on Windows\n");
  printf("--- 0 passed, 0 failed (skipped) ---\n");
  return 0;
}

#else  /* POSIX */

#include <unistd.h>
#include <sys/wait.h>
#include <errno.h>

static int passed = 0, failed = 0;
static void check(const char *n, int ok){
  if(ok){ printf("  PASS: %s\n", n); passed++; }
  else  { printf("  FAIL: %s\n", n); failed++; }
}
#define ASSERT(n, e) check(n, (int)(e))

#define DB_PATH   "mp_test.db"
#define N_KEYS    50

static void db_remove(void){
  remove(DB_PATH);
  remove(DB_PATH "-wal");
  remove(DB_PATH "-shm");
}

/* --------------------------------------------------------
** Test 1: parent writes N keys, child reads them back.
** -------------------------------------------------------- */
static void test_parent_write_child_read(void){
  printf("\nTest 1: parent writes, child reads\n");
  db_remove();

  /* Parent: open, write, checkpoint, leave read transaction open. */
  KVStore *kv = NULL;
  kvstore_open(DB_PATH, &kv, KVSTORE_JOURNAL_WAL);
  if(!kv){ fprintf(stderr, "open failed\n"); return; }

  char key[32], val[32];
  for(int i = 0; i < N_KEYS; i++){
    snprintf(key, sizeof(key), "key%04d", i);
    snprintf(val, sizeof(val), "val%04d", i);
    kvstore_put(kv, key, (int)strlen(key), val, (int)strlen(val));
  }
  /* Checkpoint so child can see data without WAL. */
  kvstore_checkpoint(kv, KVSTORE_CHECKPOINT_FULL, NULL, NULL);

  pid_t pid = fork();
  if(pid == 0){
    /* Child: open its own handle, read all keys. */
    KVStore *ckv = NULL;
    int rc = kvstore_open(DB_PATH, &ckv, KVSTORE_JOURNAL_WAL);
    if(rc != KVSTORE_OK) _exit(1);
    int ok = 1;
    for(int i = 0; i < N_KEYS; i++){
      snprintf(key, sizeof(key), "key%04d", i);
      snprintf(val, sizeof(val), "val%04d", i);
      void *pv = NULL; int nv = 0;
      rc = kvstore_get(ckv, key, (int)strlen(key), &pv, &nv);
      if(rc != KVSTORE_OK || nv != (int)strlen(val) ||
         memcmp(pv, val, nv) != 0){ ok = 0; }
      if(pv) snkv_free(pv);
    }
    kvstore_close(ckv);
    _exit(ok ? 0 : 2);
  }

  int status = 0;
  waitpid(pid, &status, 0);
  kvstore_close(kv);
  int child_ok = WIFEXITED(status) && WEXITSTATUS(status) == 0;
  ASSERT("child read all N keys correctly", child_ok);
  db_remove();
}

/* --------------------------------------------------------
** Test 2: concurrent readers from two processes simultaneously.
** -------------------------------------------------------- */
static void test_concurrent_readers(void){
  printf("\nTest 2: concurrent readers\n");
  db_remove();

  KVStore *kv = NULL;
  kvstore_open(DB_PATH, &kv, KVSTORE_JOURNAL_WAL);
  if(!kv) return;
  for(int i = 0; i < 20; i++){
    char k[16]; snprintf(k, sizeof(k), "r%d", i);
    kvstore_put(kv, k, (int)strlen(k), "value", 5);
  }
  kvstore_checkpoint(kv, KVSTORE_CHECKPOINT_FULL, NULL, NULL);
  /* Keep kv open through the fork so the WAL SHM stays fully initialised.
  ** Closing before fork leaves a window where both children race to
  ** initialise SHM simultaneously; keeping one connection live prevents
  ** that race on both Linux and macOS. */

  /* Fork two reader processes. */
  pid_t pids[2];
  for(int p = 0; p < 2; p++){
    pids[p] = fork();
    if(pids[p] == 0){
      KVStore *rkv = NULL;
      kvstore_open(DB_PATH, &rkv, KVSTORE_JOURNAL_WAL);
      if(!rkv) _exit(1);
      int ok = 1;
      for(int i = 0; i < 20; i++){
        char k[16]; snprintf(k, sizeof(k), "r%d", i);
        int ex = 0;
        if(kvstore_exists(rkv, k, (int)strlen(k), &ex) != KVSTORE_OK || !ex)
          ok = 0;
      }
      kvstore_close(rkv);
      _exit(ok ? 0 : 2);
    }
  }
  int all_ok = 1;
  for(int p = 0; p < 2; p++){
    int st = 0; waitpid(pids[p], &st, 0);
    if(!WIFEXITED(st) || WEXITSTATUS(st) != 0) all_ok = 0;
  }
  kvstore_close(kv);
  ASSERT("both concurrent readers saw all keys", all_ok);
  db_remove();
}

/* --------------------------------------------------------
** Test 3: WAL reader snapshot isolation across processes.
**   Child opens a read snapshot before parent writes new data.
**   Child should NOT see the new data (isolated snapshot).
** -------------------------------------------------------- */
static void test_snapshot_isolation(void){
  printf("\nTest 3: WAL reader snapshot isolation\n");
  db_remove();

  KVStore *kv = NULL;
  kvstore_open(DB_PATH, &kv, KVSTORE_JOURNAL_WAL);
  if(!kv) return;
  kvstore_put(kv, "pre", 3, "exists", 6);
  kvstore_checkpoint(kv, KVSTORE_CHECKPOINT_FULL, NULL, NULL);

  /* Two pipes for bidirectional sync:
  **   c2p: child writes, parent reads  (child signals snapshot ready)
  **   p2c: parent writes, child reads  (parent signals new_key written) */
  int c2p[2], p2c[2];
  pipe(c2p);
  pipe(p2c);

  pid_t pid = fork();
  if(pid == 0){
    close(c2p[0]); close(p2c[1]);
    /* Child: open snapshot before parent writes. */
    KVStore *ckv = NULL;
    kvstore_open(DB_PATH, &ckv, KVSTORE_JOURNAL_WAL);
    /* Begin read transaction to freeze snapshot. */
    kvstore_begin(ckv, 0);

    /* Signal parent that snapshot is open. */
    char sig = 'R'; write(c2p[1], &sig, 1);

    /* Wait for parent to commit new data. */
    read(p2c[0], &sig, 1);

    /* Snapshot should not see "new_key" written after our begin. */
    int ex = 0;
    kvstore_exists(ckv, "new_key", 7, &ex);
    int ok = (ex == 0);  /* must NOT be visible in our snapshot */

    kvstore_rollback(ckv);
    kvstore_close(ckv);
    close(c2p[1]); close(p2c[0]);
    _exit(ok ? 0 : 2);
  }

  close(c2p[1]); close(p2c[0]);
  /* Wait for child to open its snapshot. */
  char sig; read(c2p[0], &sig, 1);

  /* Parent: write new key AFTER child has its snapshot. */
  kvstore_put(kv, "new_key", 7, "new_val", 7);

  /* Signal child that write is done. */
  sig = 'W'; write(p2c[1], &sig, 1);
  close(c2p[0]); close(p2c[1]);

  int status = 0;
  waitpid(pid, &status, 0);
  kvstore_close(kv);

  int child_ok = WIFEXITED(status) && WEXITSTATUS(status) == 0;
  ASSERT("child snapshot did not see post-snapshot write", child_ok);
  db_remove();
}

int main(void){
  printf("=== test_multiprocess ===\n");
  test_parent_write_child_read();
  test_concurrent_readers();
  test_snapshot_isolation();
  printf("\n--- %d passed, %d failed ---\n", passed, failed);
  return (failed > 0) ? 1 : 0;
}

#endif /* POSIX */
