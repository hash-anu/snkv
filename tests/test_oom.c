/* SPDX-License-Identifier: Apache-2.0 */
/*
** tests/test_oom.c — out-of-memory sweep (embedded / low-memory targets).
**
** Installs a counting allocator with SQLITE_CONFIG_MALLOC, then runs one
** workload over and over, failing exactly the Nth allocation for
** N = 1, 2, 3, ... until a run completes without reaching N.  After every
** run (with failures switched off again) it checks that:
**
**   1. nothing crashed (the process is still here),
**   2. every byte allocated during the run was freed (no leak on any
**      error path),
**   3. the database reopens and passes kvstore_integrity_check, and the
**      data written by completed calls is readable.
**
** On a device with a few hundred KB of RAM an allocation can fail at any
** point, so every one of these paths matters.  Among others this covers
** kvstoreRegisterCF failing to grow the handle list: the open must fail
** with KVSTORE_NOMEM instead of returning an untracked handle.
**
** The sweep does not use encryption (each run would derive a 64 MiB Argon2
** key).  A separate section checks encrypted stores on a small heap: the
** Argon2 work area comes from this allocator and fails cleanly, and an
** encrypted put / get / iterator read holds one copy of a value, not two.
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "kvstore.h"
#include "sqlite3.h"

/* ---------------------------------------------------------------- allocator */
static long long g_nCalls;      /* allocation calls since the run started */
static long long g_failAt;      /* fail the call with this number; 0 = never */
static long long g_nFailed;     /* failures injected in this run */
static long long g_nLive;       /* outstanding allocations */
static long long g_nLiveBytes;  /* outstanding bytes */
static long long g_nPeakBytes;  /* high-water mark of g_nLiveBytes */
static long long g_failAbove;   /* fail any request larger than this; 0 = off */

#define HDR 16                  /* keeps 16-byte alignment of user memory */

static void notePeak(void){ if( g_nLiveBytes > g_nPeakBytes ) g_nPeakBytes = g_nLiveBytes; }
static int shouldFail(void){
  g_nCalls++;
  if( g_failAt && g_nCalls == g_failAt ){ g_nFailed++; return 1; }
  return 0;
}
static void *oomMalloc(int n){
  if( shouldFail() ) return 0;
  if( g_failAbove && n > g_failAbove ){ g_nFailed++; return 0; }
  char *p = (char*)malloc((size_t)n + HDR);
  if( !p ) return 0;
  *(int*)p = n;
  g_nLive++; g_nLiveBytes += n; notePeak();
  return p + HDR;
}
static void oomFree(void *p){
  if( !p ) return;
  char *b = (char*)p - HDR;
  g_nLive--; g_nLiveBytes -= *(int*)b;
  free(b);
}
static void *oomRealloc(void *p, int n){
  if( !p ) return oomMalloc(n);
  if( shouldFail() ) return 0;
  if( g_failAbove && n > g_failAbove ){ g_nFailed++; return 0; }
  char *b = (char*)p - HDR;
  int old = *(int*)b;
  char *nb = (char*)realloc(b, (size_t)n + HDR);
  if( !nb ) return 0;
  *(int*)nb = n;
  g_nLiveBytes += n - old; notePeak();
  return nb + HDR;
}
static int oomSize(void *p){ return p ? *(int*)((char*)p - HDR) : 0; }
static int oomRoundup(int n){ return (n + 7) & ~7; }
static int oomInit(void *x){ (void)x; return SQLITE_OK; }
static void oomShutdown(void *x){ (void)x; }

/* ---------------------------------------------------------------- workload */
#define DB "t_oom.db"

static void rmdb(void){
  remove(DB); remove(DB "-wal"); remove(DB "-shm"); remove(DB "-journal");
}

/* Every call may fail; results are ignored, outputs are always freed.
** Returns the number of user-visible writes that reported success, so the
** verify step knows which keys must exist. */
static unsigned workload(void){
  unsigned ok = 0;
  KVStore *kv = NULL;
  KVColumnFamily *a = NULL, *a2 = NULL, *b = NULL;
  void *v = NULL; int n = 0;

  if( kvstore_open(DB, &kv, KVSTORE_JOURNAL_WAL) != KVSTORE_OK ){
    kvstore_close(kv);          /* NULL-safe */
    return 0;
  }
  if( kvstore_put(kv, "d1", 2, "default", 7) == KVSTORE_OK ) ok |= 1;

  /* "b" gets the lowest root page; it is dropped below. */
  if( kvstore_cf_create(kv, "b", &b) == KVSTORE_OK ){
    kvstore_cf_put(b, "x", 1, "y", 1);
    kvstore_cf_close(b); b = NULL;
  }
  /* Four open handles fill the handle list's first block, so registering
  ** "a" must grow it: that realloc is one of the injected failures. */
  KVColumnFamily *c[4] = {0, 0, 0, 0};
  if( kvstore_cf_create(kv, "c", &c[0]) == KVSTORE_OK ){
    for( int i = 1; i < 4; i++ ) kvstore_cf_open(kv, "c", &c[i]);
  }
  if( kvstore_cf_create(kv, "a", &a) == KVSTORE_OK ){
    char key[8];
    for( int i = 0; i < 20; i++ ){
      snprintf(key, sizeof key, "a%02d", i);
      kvstore_cf_put(a, key, 3, "value-of-a", 10);
    }
  }
  for( int i = 0; i < 4; i++ ) if( c[i] ) kvstore_cf_close(c[i]);
  /* Dropping "b" moves the highest root page ("a") into b's slot.  Only a
  ** registered handle is re-pointed; an untracked one would now write
  ** into a freed page. */
  kvstore_cf_drop(kv, "b");
  if( a && kvstore_cf_put(a, "keep", 4, "kept", 4) == KVSTORE_OK ) ok |= 2;
  /* A second handle to the same CF: goes through kvstoreRegisterCF. */
  if( kvstore_cf_open(kv, "a", &a2) == KVSTORE_OK ){
    kvstore_cf_put_ttl(a2, "ttl", 3, "t", 1, kvstore_now_ms() + 600000);
    kvstore_cf_get(a2, "a05", 3, &v, &n); snkv_free(v); v = NULL;
  }
  if( a ){ kvstore_cf_get(a, "ttl", 3, &v, &n); snkv_free(v); v = NULL; }

  KVIterator *it = NULL;
  if( a && kvstore_cf_iterator_create(a, &it) == KVSTORE_OK ){
    for( kvstore_iterator_first(it); !kvstore_iterator_eof(it); kvstore_iterator_next(it) ){
      void *k = NULL; int nk = 0;
      kvstore_iterator_key(it, &k, &nk);
    }
    kvstore_iterator_close(it);
  }

  if( kvstore_begin(kv, 1) == KVSTORE_OK ){
    kvstore_put(kv, "t1", 2, "in-txn", 6);
    if( kvstore_commit(kv) != KVSTORE_OK ) kvstore_rollback(kv);
  }

  int nDel = 0;
  kvstore_purge_expired(kv, &nDel);
  kvstore_incremental_vacuum(kv, 0);
  kvstore_sync(kv);

  if( a2 ) kvstore_cf_close(a2);
  if( a )  kvstore_cf_close(a);
  kvstore_close(kv);
  return ok;
}

/* Reopen without failures; returns 0 if the store is healthy. */
static int verify(unsigned ok){
  KVStore *kv = NULL;
  int bad = 0;
  if( kvstore_open(DB, &kv, KVSTORE_JOURNAL_WAL) != KVSTORE_OK ) return 1;
  if( kvstore_integrity_check(kv, NULL) != KVSTORE_OK ) bad = 1;
  void *v = NULL; int n = 0;
  if( (ok & 1) ){
    if( kvstore_get(kv, "d1", 2, &v, &n) != KVSTORE_OK || n != 7 ) bad = 1;
    snkv_free(v); v = NULL;
  }
  if( (ok & 2) ){
    KVColumnFamily *a = NULL;
    if( kvstore_cf_open(kv, "a", &a) != KVSTORE_OK ) bad = 1;
    else{
      if( kvstore_cf_get(a, "keep", 4, &v, &n) != KVSTORE_OK || n != 4 ) bad = 1;
      snkv_free(v); v = NULL;
      kvstore_cf_close(a);
    }
  }
  kvstore_close(kv);
  return bad;
}

/* ------------------------------------------------------- encrypted memory */
static int g_nPass, g_nFail;
#define ECHECK(c, msg) do{ \
  if( c ){ printf("  [PASS] %s\n", msg); g_nPass++; } \
  else   { printf("  [FAIL] %s\n", msg); g_nFail++; } \
}while(0)

/*
** Encrypted stores on a small heap:
**   - the Argon2 work area is allocated through SQLITE_CONFIG_MALLOC (so a
**     heap limit applies to it and an allocation failure is clean),
**   - an encrypted put / get / iterator read of a value holds one copy of
**     it, not two (ciphertext + plaintext, or ciphertext + blob).
** A 64 KiB page cache (cacheSize = -64) keeps cached pages out of the
** measurement.
*/
static void encrypted_memory(void){
  const int VAL = 1 << 20;                     /* 1 MiB value */
  KVStoreConfig cfg; memset(&cfg, 0, sizeof cfg);
  cfg.journalMode = KVSTORE_JOURNAL_WAL;
  cfg.cacheSize   = -64;
  KVStore *kv = NULL;
  char msg[160];

  printf("=== encrypted store memory ===\n");
  rmdb();
  long long live0 = g_nLive;
  g_failAbove = 1 << 20; g_nFailed = 0;        /* only the 64 MiB work area is this big */
  int rc = kvstore_open_encrypted(DB, "pw", 2, &kv, &cfg);
  g_failAbove = 0;
  ECHECK(rc == KVSTORE_NOMEM && kv == NULL && g_nFailed > 0,
         "key derivation memory comes from the configured allocator; failure -> KVSTORE_NOMEM");
  ECHECK(g_nLive == live0, "nothing leaked by the failed open");

  rmdb(); kv = NULL;
  rc = kvstore_open_encrypted(DB, "pw", 2, &kv, &cfg);
  ECHECK(rc == KVSTORE_OK, "open encrypted with a 64 KiB cache");
  if( rc != KVSTORE_OK ){ rmdb(); return; }
  char *val = (char*)malloc((size_t)VAL);      /* libc: not counted */
  for( int i = 0; i < VAL; i++ ) val[i] = (char)(i * 7);

  long long base = g_nLiveBytes; g_nPeakBytes = base;
  rc = kvstore_put(kv, "big", 3, val, VAL);
  long long peak = g_nPeakBytes - base;
  snprintf(msg, sizeof msg, "encrypted put of 1 MiB peaks at %.2f MiB (< 1.5; two copies before)",
           peak / 1048576.0);
  ECHECK(rc == KVSTORE_OK && peak < VAL + VAL / 2, msg);

  void *v = NULL; int n = 0;
  base = g_nLiveBytes; g_nPeakBytes = base;
  rc = kvstore_get(kv, "big", 3, &v, &n);
  peak = g_nPeakBytes - base;
  snprintf(msg, sizeof msg, "encrypted get of 1 MiB peaks at %.2f MiB incl. the result (< 1.5)",
           peak / 1048576.0);
  ECHECK(rc == KVSTORE_OK && n == VAL && v && memcmp(v, val, VAL) == 0
         && peak < VAL + VAL / 2, msg);
  snkv_free(v);

  KVIterator *it = NULL;
  if( kvstore_iterator_create(kv, &it) == KVSTORE_OK ){
    kvstore_iterator_first(it);
    base = g_nLiveBytes; g_nPeakBytes = base;
    rc = kvstore_iterator_value(it, &v, &n);
    peak = g_nPeakBytes - base;
    snprintf(msg, sizeof msg, "encrypted iterator read of 1 MiB peaks at %.2f MiB (< 1.5)",
             peak / 1048576.0);
    ECHECK(rc == KVSTORE_OK && n == VAL && memcmp(v, val, VAL) == 0
           && peak < VAL + VAL / 2, msg);
    kvstore_iterator_close(it);
  }
  free(val);
  kvstore_close(kv);
  ECHECK(g_nLive == live0, "everything freed after close");
  rmdb();
}

int main(void){
  sqlite3_mem_methods m = {
    oomMalloc, oomFree, oomRealloc, oomSize, oomRoundup,
    oomInit, oomShutdown, 0
  };
  if( sqlite3_config(SQLITE_CONFIG_MALLOC, &m) != SQLITE_OK ){
    printf("sqlite3_config(MALLOC) failed\n");
    return 1;
  }
  printf("=== test_oom: fail the Nth allocation, N = 1, 2, ... ===\n");

  /* Warm-up run: lets the library make its one-time global allocations. */
  rmdb();
  workload();
  rmdb();
  long long baseLive = g_nLive, baseBytes = g_nLiveBytes;
  g_nCalls = 0;
  workload();
  long long total = g_nCalls;
  printf("  workload makes %lld allocation calls; %lld live after warm-up\n",
         total, baseLive);

  int nLeak = 0, nBad = 0, nRuns = 0;
  for( long long N = 1; ; N++ ){
    rmdb();
    g_nCalls = 0; g_nFailed = 0; g_failAt = N;
    unsigned ok = workload();
    g_failAt = 0;
    if( g_nFailed == 0 ) break;                 /* N past the last call */
    nRuns++;
    if( g_nLive != baseLive ){
      if( nLeak < 10 )
        printf("  [FAIL] N=%lld: %lld allocation(s), %lld byte(s) leaked\n",
               N, g_nLive - baseLive, g_nLiveBytes - baseBytes);
      nLeak++;
      baseLive = g_nLive; baseBytes = g_nLiveBytes;   /* report each once */
    }
    if( verify(ok) ){
      if( nBad < 10 ) printf("  [FAIL] N=%lld: store unhealthy after the run\n", N);
      nBad++;
    }
    if( g_nLive != baseLive ){                  /* verify itself must not leak */
      baseLive = g_nLive; baseBytes = g_nLiveBytes;
    }
  }
  rmdb();

  printf("  %d runs, each with one injected allocation failure\n", nRuns);
  printf("  %s runs with a leak: %d\n", nLeak ? "[FAIL]" : "[PASS]", nLeak);
  printf("  %s runs leaving an unhealthy store: %d\n", nBad ? "[FAIL]" : "[PASS]", nBad);
  encrypted_memory();
  printf("=== Results: %d passed, %d failed ===\n",
         (nLeak == 0) + (nBad == 0) + g_nPass, (nLeak != 0) + (nBad != 0) + g_nFail);
  return (nLeak || nBad || g_nFail) ? 1 : 0;
}
