/* SPDX-License-Identifier: Apache-2.0 */
/*
**
** Key-Value Store Implementation - Column Families
** Built on top of SQLite v3.51.200 btree implementation
**
** This implementation provides a robust key-value store
** with column family support, proper error handling, logging, safety checks,
** and thread-safety via mutexes.
**
** Storage model:
**   User data tables use BTREE_BLOBKEY.  Each entry is stored as a
**   single blob in the btree:
**     [key_len (4 bytes BE)] [key_bytes] [value_bytes]
**   A custom comparison function (sqlite3VdbeRecordCompare in btree.c)
**   compares only the key portion, so entries are kept in lexicographic
**   key order.  This enables O(log n) point lookups, efficient prefix
**   search via cursor seek, and sorted iteration.
**
**   The internal CF metadata table remains BTREE_INTKEY with FNV-1a
**   hash as rowid (only used for column family name → root page mapping).
*/

#include "kvstore.h"
#include "pager.h"
#include <string.h>
#include <assert.h>
#include <stdio.h>
#ifdef _WIN32
#  include <windows.h>
#else
#  include <sys/time.h>
#endif

/*
** Compile-time configuration
*/
#ifndef KVSTORE_MAX_KEY_SIZE
# define KVSTORE_MAX_KEY_SIZE   (64 * 1024)  /* 64KB max key size */
#endif

#ifndef KVSTORE_MAX_VALUE_SIZE
# define KVSTORE_MAX_VALUE_SIZE (10 * 1024 * 1024)  /* 10MB max value size */
#endif

#ifndef KVSTORE_DEFAULT_CACHE_SIZE
# define KVSTORE_DEFAULT_CACHE_SIZE 2000  /* 2000 pages default cache */
#endif

#ifndef KVSTORE_MAX_CF_NAME
# define KVSTORE_MAX_CF_NAME 255  /* Maximum column family name length */
#endif

/* (collision probes removed — BLOBKEY storage uses direct key comparison) */

/*
** Column family metadata stored in meta table
** Meta layout:
**   meta[1] = Default CF table root (backward compatibility)
**   meta[2] = Number of column families
**   meta[3] = CF metadata table root
*/
#define META_DEFAULT_CF_ROOT  1
#define META_CF_COUNT         2
#define META_CF_METADATA_ROOT 3

/*
** Default column family name
*/
#define DEFAULT_CF_NAME "default"

/*
** Per-CF TTL index CF name prefixes.
** Names starting with "__" are reserved; user-facing APIs reject them.
*/
#define SNKV_TTL_KEY_PREFIX  "__snkv_ttl_k__"
#define SNKV_TTL_EXP_PREFIX  "__snkv_ttl_e__"
#define SNKV_TTL_NAME_BUFLEN 270  /* 14 + 255 (max CF name) + 1 NUL */

/* ======================================================================
** BLOBKEY encoding helpers.
**
** Every BLOBKEY cell stores its data as a single blob:
**     [key_len  4 bytes, big-endian] [key_bytes ...] [value_bytes ...]
**
** The btree comparison function (sqlite3VdbeRecordCompare in btree.c)
** compares only the key portion, so entries are ordered lexicographically
** by key.  Two entries with the same key compare as equal; BtreeInsert
** will overwrite the old cell automatically (upsert).
** ====================================================================== */
/*
** Encode [key_len (4 bytes BE)][key_bytes][value_bytes] into *ppOut.
**
** If the total fits within the caller-supplied stack buffer (pStack, nStack),
** *ppOut is set to pStack — no heap allocation occurs.  Otherwise a heap
** buffer is allocated and *ppOut is set to that buffer.
**
** Returns the total encoded length on success, or -1 on OOM.
** Caller must call sqlite3_free(*ppOut) only when *ppOut != pStack.
*/
static int kvstoreEncodeBlob(
  const void *pKey, int nKey,
  const void *pVal, int nVal,
  unsigned char *pStack, int nStack,
  unsigned char **ppOut
){
  int nTotal = 4 + nKey + nVal;
  unsigned char *pOut;
  if( nTotal <= nStack ){
    pOut = pStack;
  }else{
    pOut = (unsigned char *)sqlite3Malloc(nTotal);
    if( pOut == 0 ){ *ppOut = 0; return -1; }
  }
  pOut[0] = (unsigned char)((nKey >> 24) & 0xFF);
  pOut[1] = (unsigned char)((nKey >> 16) & 0xFF);
  pOut[2] = (unsigned char)((nKey >>  8) & 0xFF);
  pOut[3] = (unsigned char)( nKey        & 0xFF);
  memcpy(pOut + 4, pKey, nKey);
  if( nVal > 0 && pVal ){
    memcpy(pOut + 4 + nKey, pVal, nVal);
  }
  *ppOut = pOut;
  return nTotal;
}

/* ======================================================================
** Internal cursor helpers – wraps the new callerallocs cursor model
** ====================================================================== */
static BtCursor *kvstoreAllocCursor(void){
  int n = sqlite3BtreeCursorSize();
  BtCursor *p = (BtCursor*)sqlite3MallocZero(n);
  if( p ) sqlite3BtreeCursorZero(p);
  return p;
}

static void kvstoreFreeCursor(BtCursor *pCur){
  if( pCur ){
    sqlite3BtreeCloseCursor(pCur);
    sqlite3_free(pCur);
  }
}

/* ======================================================================
** Current time in milliseconds since the Unix epoch.
** ====================================================================== */
int64_t kvstore_now_ms(void){
#ifdef _WIN32
  FILETIME ft;
  ULARGE_INTEGER ul;
  GetSystemTimeAsFileTime(&ft);
  ul.LowPart  = ft.dwLowDateTime;
  ul.HighPart = ft.dwHighDateTime;
  /* Windows FILETIME epoch is 1601-01-01; subtract 116444736000000000 * 100ns
  ** intervals to reach Unix epoch, then divide by 10000 to get ms. */
  return (int64_t)((ul.QuadPart - 116444736000000000ULL) / 10000);
#else
  struct timeval tv;
  gettimeofday(&tv, 0);
  return (int64_t)tv.tv_sec * 1000 + (int64_t)(tv.tv_usec / 1000);
#endif
}

/* ======================================================================
** Big-endian 64-bit encode/decode helpers for TTL timestamps.
** ====================================================================== */
static void kvstoreEncodeBE64(unsigned char buf[8], int64_t v){
  buf[0] = (unsigned char)((v >> 56) & 0xFF);
  buf[1] = (unsigned char)((v >> 48) & 0xFF);
  buf[2] = (unsigned char)((v >> 40) & 0xFF);
  buf[3] = (unsigned char)((v >> 32) & 0xFF);
  buf[4] = (unsigned char)((v >> 24) & 0xFF);
  buf[5] = (unsigned char)((v >> 16) & 0xFF);
  buf[6] = (unsigned char)((v >>  8) & 0xFF);
  buf[7] = (unsigned char)( v        & 0xFF);
}

static int64_t kvstoreDecodeBE64(const unsigned char buf[8]){
  return ((int64_t)buf[0] << 56) | ((int64_t)buf[1] << 48) |
         ((int64_t)buf[2] << 40) | ((int64_t)buf[3] << 32) |
         ((int64_t)buf[4] << 24) | ((int64_t)buf[5] << 16) |
         ((int64_t)buf[6] <<  8) |  (int64_t)buf[7];
}

/*
** Column Family structure
*/
struct KVColumnFamily {
  KVStore *pKV;      /* Parent store */
  char *zName;       /* Column family name */
  int iTable;        /* Root page of this CF's btree */
  int refCount;      /* Reference count */
  sqlite3_mutex *pMutex;  /* Recursive mutex for this CF (SQLITE_MUTEX_RECURSIVE) */
  BtCursor *pReadCur;     /* Cached read-only cursor; NULL = not open yet */
  int hasTtl;                   /* 1 if TTL index CFs are open for this CF */
  KVColumnFamily *pTtlKeyCF;    /* __snkv_ttl_k__<name>; NULL until first TTL use */
  KVColumnFamily *pTtlExpiryCF; /* __snkv_ttl_e__<name>; NULL until first TTL use */
};

/*
** KVStore structure - represents an open key-value store
*/
struct KVStore {
  Btree *pBt;        /* Underlying btree handle */
  sqlite3 *db;       /* Database connection (required by btree) */
  KeyInfo *pKeyInfo; /* Shared KeyInfo for BLOBKEY cursor comparisons */
  int iMetaTable;    /* Root page of CF metadata table */
  int inTrans;       /* Transaction state: 0=none, 1=read, 2=write */
  int isCorrupted;   /* Set if database corruption is detected */
  char *zErrMsg;     /* Last error message */

  /* Thread safety */
  sqlite3_mutex *pMutex;  /* Recursive mutex protecting store operations (SQLITE_MUTEX_RECURSIVE) */
  int closing;            /* Set to 1 when kvstore_close() is in progress */

  /* WAL auto-checkpoint */
  int walSizeLimit;       /* Checkpoint every N commits (0 = disabled) */
  int walCommits;         /* Write commits since last auto-checkpoint */

  /* Column families */
  KVColumnFamily *pDefaultCF;  /* Default column family */
  KVColumnFamily **apCF;       /* Array of open column families */
  int nCF;                     /* Number of open column families */
  int nCFAlloc;                /* Allocated size of apCF array */

  /* Statistics */
  struct {
    u64 nPuts;       /* Number of put operations */
    u64 nGets;       /* Number of get operations */
    u64 nDeletes;    /* Number of delete operations */
    u64 nIterations; /* Number of iterations */
    u64 nErrors;     /* Number of errors encountered */
  } stats;
};

/*
** Iterator structure for traversing the store
*/
struct KVIterator {
  KVColumnFamily *pCF;  /* Column family being iterated */
  BtCursor *pCur;    /* Btree cursor */
  int eof;           /* End-of-file flag */
  int ownsTrans;     /* 1 if iterator started its own transaction */
  void *pKeyBuf;     /* Buffer for current key */
  int nKeyBuf;       /* Size of key buffer */
  void *pValBuf;     /* Buffer for current value */
  int nValBuf;       /* Size of value buffer */
  int isValid;       /* Iterator validity flag */
  void *pPrefix;     /* Prefix filter (NULL = no filter) */
  int nPrefix;       /* Length of prefix */
};

/*
** Return the cached read cursor for a column family, opening it on first
** use.  The cursor remains open across calls (SQLite moves it to
** CURSOR_REQUIRESSEEK when pages are modified, restoring it on next seek).
** Caller must hold both pCF->pMutex and pKV->pMutex.
*/
static BtCursor *kvstoreGetReadCursor(KVColumnFamily *pCF){
  if( pCF->pReadCur ) return pCF->pReadCur;
  BtCursor *pCur = kvstoreAllocCursor();
  if( !pCur ) return NULL;
  int rc = sqlite3BtreeCursor(pCF->pKV->pBt, pCF->iTable, 0,
                               pCF->pKV->pKeyInfo, pCur);
  if( rc != SQLITE_OK ){
    sqlite3_free(pCur);
    return NULL;
  }
  pCF->pReadCur = pCur;
  return pCur;
}

/*
** Set error message (caller must hold mutex)
*/
static void kvstoreSetError(KVStore *pKV, const char *zFormat, ...){
  va_list ap;
  if( pKV->zErrMsg ){
    sqlite3_free(pKV->zErrMsg);
    pKV->zErrMsg = 0;
  }
  if( zFormat ){
    va_start(ap, zFormat);
    pKV->zErrMsg = sqlite3VMPrintf(pKV->db, zFormat, ap);
    va_end(ap);
  }
  pKV->stats.nErrors++;
}

/*
** Get last error message
*/
const char *kvstore_errmsg(KVStore *pKV){
  const char *zMsg;

  if( !pKV ){
    return "no error";
  }

  sqlite3_mutex_enter(pKV->pMutex);
  if( pKV->zErrMsg ){
    zMsg = pKV->zErrMsg;
  }else{
    zMsg = "no error";
  }
  sqlite3_mutex_leave(pKV->pMutex);

  return zMsg;
}

/*
** Validate key and value sizes (caller must hold mutex)
*/
static int kvstoreValidateKeyValue(
  KVStore *pKV,
  const void *pKey,
  int nKey,
  const void *pValue,
  int nValue
){
  if( pKey == NULL || nKey <= 0 ){
    kvstoreSetError(pKV, "invalid key: null or zero length");
    return KVSTORE_ERROR;
  }

  if( nKey > KVSTORE_MAX_KEY_SIZE ){
    kvstoreSetError(pKV, "key too large: %d bytes (max %d)",
                    nKey, KVSTORE_MAX_KEY_SIZE);
    return KVSTORE_ERROR;
  }

  if( pValue == NULL && nValue != 0 ){
    kvstoreSetError(pKV, "invalid value: null pointer with non-zero length");
    return KVSTORE_ERROR;
  }

  if( nValue > KVSTORE_MAX_VALUE_SIZE ){
    kvstoreSetError(pKV, "value too large: %d bytes (max %d)",
                    nValue, KVSTORE_MAX_VALUE_SIZE);
    return KVSTORE_ERROR;
  }

  return KVSTORE_OK;
}

/*
** Check if store is corrupted (caller must hold mutex)
*/
static int kvstoreCheckCorruption(KVStore *pKV, int rc){
  if( rc == SQLITE_CORRUPT || rc == SQLITE_NOTADB ){
    pKV->isCorrupted = 1;
    kvstoreSetError(pKV, "database corruption detected");
    return 1;
  }
  return 0;
}

/* ======================================================================
** BLOBKEY seek helper.
**
** Positions the cursor on the entry whose key matches (pKey, nKey).
** Uses BtreeIndexMoveto which goes through our custom comparison
** function.  Returns SQLITE_OK and sets *pFound=1 if an exact
** match was found; the cursor is left pointing at the entry.
** ====================================================================== */
static int kvstoreSeekKey(
  BtCursor *pCur,
  KeyInfo *pKeyInfo,
  const void *pKey, int nKey,
  int *pFound
){
  int rc;
  int res = 0;
  UnpackedRecord idxKey;  /* stack-allocated: avoids one heap malloc+free per seek */
  Mem memField;           /* placeholder aMem[0] required by comparison function */

  *pFound = 0;

  memset(&idxKey, 0, sizeof(idxKey));
  memset(&memField, 0, sizeof(memField));

  /* Point UnpackedRecord directly at the raw key bytes (not the header) */
  idxKey.pKeyInfo   = pKeyInfo;
  idxKey.aMem       = &memField;
  idxKey.u.z        = (char *)pKey;
  idxKey.n          = nKey;
  idxKey.nField     = 1;
  idxKey.default_rc = 0;  /* exact match */

  rc = sqlite3BtreeIndexMoveto(pCur, &idxKey, &res);

  if( rc == SQLITE_OK && res == 0 ){
    *pFound = 1;
  }
  return rc;
}

/* ======================================================================
** INTKEY helpers for the CF metadata table (internal only).
** The metadata table stores CF name → root page mappings using INTKEY
** with FNV-1a hash as rowid.
** ====================================================================== */
#ifndef KVSTORE_MAX_COLLISION_PROBES
# define KVSTORE_MAX_COLLISION_PROBES 64
#endif

static i64 kvstoreMetaHashKey(const void *pKey, int nKey){
  const unsigned char *p = (const unsigned char *)pKey;
  u64 hash = (u64)14695981039346656037ULL;
  int i;
  for(i = 0; i < nKey; i++){
    hash ^= (u64)p[i];
    hash *= (u64)1099511628211ULL;
  }
  hash &= 0x7FFFFFFFFFFFFFFFULL;
  if( hash == 0 ) hash = 1;
  return (i64)hash;
}

static int kvstoreMetaSeekKey(
  BtCursor *pCur,
  const void *pKey, int nKey,
  int *pFound,
  i64 *pRowid
){
  i64 baseHash = kvstoreMetaHashKey(pKey, nKey);
  int probe;
  *pFound = 0;
  for(probe = 0; probe < KVSTORE_MAX_COLLISION_PROBES; probe++){
    i64 tryRowid = baseHash + probe;
    int res = 0;
    int rc = sqlite3BtreeTableMoveto(pCur, tryRowid, 0, &res);
    if( rc != SQLITE_OK ) return rc;
    if( res != 0 ) return SQLITE_OK;
    u32 payloadSz = sqlite3BtreePayloadSize(pCur);
    if( (int)payloadSz >= 4 + nKey ){
      unsigned char hdr[4];
      rc = sqlite3BtreePayload(pCur, 0, 4, hdr);
      if( rc != SQLITE_OK ) return rc;
      int storedKeyLen = (hdr[0]<<24)|(hdr[1]<<16)|(hdr[2]<<8)|hdr[3];
      if( storedKeyLen == nKey ){
        void *storedKey = sqlite3Malloc(nKey);
        if( storedKey == 0 ) return SQLITE_NOMEM;
        rc = sqlite3BtreePayload(pCur, 4, nKey, storedKey);
        if( rc != SQLITE_OK ){ sqlite3_free(storedKey); return rc; }
        int match = (memcmp(storedKey, pKey, nKey) == 0);
        sqlite3_free(storedKey);
        if( match ){
          *pFound = 1;
          if( pRowid ) *pRowid = tryRowid;
          return SQLITE_OK;
        }
      }
    }
  }
  return SQLITE_OK;
}

static int kvstoreMetaFindSlot(
  BtCursor *pCur,
  const void *pKey, int nKey,
  i64 *pRowid
){
  i64 baseHash = kvstoreMetaHashKey(pKey, nKey);
  int probe;
  for(probe = 0; probe < KVSTORE_MAX_COLLISION_PROBES; probe++){
    i64 tryRowid = baseHash + probe;
    int res = 0;
    int rc = sqlite3BtreeTableMoveto(pCur, tryRowid, 0, &res);
    if( rc != SQLITE_OK ) return rc;
    if( res != 0 ){
      *pRowid = tryRowid;
      return SQLITE_OK;
    }
  }
  return SQLITE_FULL;
}

/*
** Initialize CF metadata table (caller must hold mutex)
*/
static int initCFMetadataTable(KVStore *pKV){
  int rc;
  Pgno pgno;

  rc = sqlite3BtreeBeginTrans(pKV->pBt, 1, 0);
  if( rc != SQLITE_OK ) return rc;

  /* Create metadata table (INTKEY) */
  rc = sqlite3BtreeCreateTable(pKV->pBt, &pgno, BTREE_INTKEY);
  if( rc != SQLITE_OK ){
    sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
    return rc;
  }
  pKV->iMetaTable = (int)pgno;

  /* Store metadata table root in meta[3] */
  rc = sqlite3BtreeUpdateMeta(pKV->pBt, META_CF_METADATA_ROOT, pgno);
  if( rc != SQLITE_OK ){
    sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
    return rc;
  }

  /* Initialize CF count to 1 (default CF) */
  rc = sqlite3BtreeUpdateMeta(pKV->pBt, META_CF_COUNT, 1);
  if( rc != SQLITE_OK ){
    sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
    return rc;
  }

  rc = sqlite3BtreeCommit(pKV->pBt);
  return rc;
}

/*
** Create default column family (caller must hold mutex)
*/
static int createDefaultCF(KVStore *pKV){
  int rc;
  KVColumnFamily *pCF;
  Pgno pgno;

  rc = sqlite3BtreeBeginTrans(pKV->pBt, 1, 0);
  if( rc != SQLITE_OK ) return rc;

  /* Create default CF table (BLOBKEY — keys stored lexicographically) */
  rc = sqlite3BtreeCreateTable(pKV->pBt, &pgno, BTREE_BLOBKEY);
  if( rc != SQLITE_OK ){
    sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
    return rc;
  }

  /* Store in meta[1] for backward compatibility */
  rc = sqlite3BtreeUpdateMeta(pKV->pBt, META_DEFAULT_CF_ROOT, pgno);
  if( rc != SQLITE_OK ){
    sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
    return rc;
  }

  rc = sqlite3BtreeCommit(pKV->pBt);
  if( rc != SQLITE_OK ) return rc;

  /* Create CF structure */
  pCF = (KVColumnFamily*)sqlite3MallocZero(sizeof(KVColumnFamily));
  if( !pCF ) return KVSTORE_NOMEM;

  pCF->pKV = pKV;
  pCF->zName = sqliteStrDup(DEFAULT_CF_NAME);
  pCF->iTable = (int)pgno;
  pCF->refCount = 1;

  /* Create mutex for default CF */
  pCF->pMutex = sqlite3MutexAlloc(SQLITE_MUTEX_RECURSIVE);
  if( !pCF->pMutex ){
    sqlite3_free(pCF->zName);
    sqlite3_free(pCF);
    return KVSTORE_NOMEM;
  }

  pKV->pDefaultCF = pCF;
  return KVSTORE_OK;
}

/* ======================================================================
** Busy-handler callback for kvstore_open_v2(busyTimeout > 0).
**
** SQLite calls this every time a lock cannot be acquired.  We sleep 1 ms
** per call and give up once the total elapsed time exceeds busyTimeout.
**
** pArg — points to a KVBusyCtx embedded in the KVStore allocation.
** nBusy — number of previous busy calls for this operation.
** Returns 1 to retry, 0 to surface SQLITE_BUSY to the caller.
** ====================================================================== */
typedef struct KVBusyCtx KVBusyCtx;
struct KVBusyCtx {
  int timeoutMs;    /* maximum wait in milliseconds */
  sqlite3_vfs *pVfs;
};

static int kvstoreBusyHandler(void *pArg, int nBusy){
  KVBusyCtx *ctx = (KVBusyCtx *)pArg;
  /* Each call represents ~1 ms of waiting (one 1000-µs sleep below). */
  if( nBusy * 1 >= ctx->timeoutMs ) return 0;  /* give up */
  sqlite3OsSleep(ctx->pVfs, 1000);              /* sleep 1 ms */
  return 1;                                     /* retry */
}

/* ======================================================================
** Shared teardown helper — releases all resources owned by a partially or
** fully initialised KVStore.  Used by both kvstore_open_v2 error paths
** and kvstore_close.
** ====================================================================== */
/* Free all resources owned by one KVColumnFamily struct (no lock needed). */
static void kvstoreFreeCFStruct(KVColumnFamily *pCF){
  if( !pCF ) return;
  if( pCF->pTtlKeyCF ){
    KVColumnFamily *p = pCF->pTtlKeyCF;
    if( p->pReadCur ) kvstoreFreeCursor(p->pReadCur);
    if( p->pMutex )   sqlite3_mutex_free(p->pMutex);
    sqlite3_free(p->zName); sqlite3_free(p);
  }
  if( pCF->pTtlExpiryCF ){
    KVColumnFamily *p = pCF->pTtlExpiryCF;
    if( p->pReadCur ) kvstoreFreeCursor(p->pReadCur);
    if( p->pMutex )   sqlite3_mutex_free(p->pMutex);
    sqlite3_free(p->zName); sqlite3_free(p);
  }
  if( pCF->pReadCur ) kvstoreFreeCursor(pCF->pReadCur);
  if( pCF->pMutex )   sqlite3_mutex_free(pCF->pMutex);
  sqlite3_free(pCF->zName);
  sqlite3_free(pCF);
}

static void kvstoreTeardownNoLock(KVStore *pKV){
  int i;
  if( pKV->pDefaultCF ){
    kvstoreFreeCFStruct(pKV->pDefaultCF);
    pKV->pDefaultCF = 0;
  }
  for( i = 0; i < pKV->nCF; i++ ){
    if( pKV->apCF[i] ) kvstoreFreeCFStruct(pKV->apCF[i]);
  }
  sqlite3_free(pKV->apCF);
  if( pKV->pBt ) sqlite3BtreeClose(pKV->pBt);
  sqlite3_free(pKV->pKeyInfo);
  if( pKV->zErrMsg ) sqlite3_free(pKV->zErrMsg);
  /* Free the busy-handler context if we allocated one */
  if( pKV->db ){
    sqlite3_free(pKV->db->busyHandler.pBusyArg);
    if( pKV->db->mutex ) sqlite3_mutex_free(pKV->db->mutex);
    sqlite3_free(pKV->db);
  }
}

/*
** Increment the commit counter and run a PASSIVE checkpoint when the
** walSizeLimit threshold is reached. Called in the TRANS_NONE window
** (after sqlite3BtreeCommit, before the read transaction is restored).
*/
static void kvstoreAutoCheckpoint(KVStore *pKV){
  if( pKV->walSizeLimit > 0 ){
    pKV->walCommits++;
    if( pKV->walCommits >= pKV->walSizeLimit ){
      pKV->walCommits = 0;
      sqlite3BtreeCheckpoint(pKV->pBt, SQLITE_CHECKPOINT_PASSIVE, NULL, NULL);
    }
  }
}

/* Early forward declaration needed by kvstore_open_v2 TTL probe. */
static int kvstoreCfOpenInternal(KVStore*, const char*, KVColumnFamily**);

/*
** Open a key-value store with full configuration control.
*/
int kvstore_open_v2(
  const char *zFilename,
  KVStore **ppKV,
  const KVStoreConfig *pConfig
){
  KVStore *pKV;
  int rc;

  /* Resolve configuration — use defaults for any zero/unset field */
  int journalMode  = pConfig ? pConfig->journalMode  : KVSTORE_JOURNAL_WAL;
  int syncLevel    = pConfig ? pConfig->syncLevel    : KVSTORE_SYNC_NORMAL;
  int cacheSize    = pConfig ? pConfig->cacheSize    : 0;
  int pageSize     = pConfig ? pConfig->pageSize     : 0;
  int readOnly     = pConfig ? pConfig->readOnly     : 0;
  int busyTimeout  = pConfig ? pConfig->busyTimeout  : 0;

  if( cacheSize <= 0 ) cacheSize = KVSTORE_DEFAULT_CACHE_SIZE;

  if( ppKV == NULL ){
    return KVSTORE_ERROR;
  }
  *ppKV = NULL;

  /* One-time SQLite library initialisation */
  rc = sqlite3_initialize();
  if( rc != SQLITE_OK ) return rc;

  /* Allocate KVStore structure */
  pKV = (KVStore*)sqlite3MallocZero(sizeof(KVStore));
  if( pKV == NULL ){
    return KVSTORE_NOMEM;
  }

  /* Create main mutex */
  pKV->pMutex = sqlite3MutexAlloc(SQLITE_MUTEX_RECURSIVE);
  if( !pKV->pMutex ){
    sqlite3_free(pKV);
    return KVSTORE_NOMEM;
  }

  /* Create a minimal sqlite3 structure (required by btree) */
  pKV->db = (sqlite3*)sqlite3MallocZero(sizeof(sqlite3));
  if( pKV->db == NULL ){
    sqlite3_mutex_free(pKV->pMutex);
    sqlite3_free(pKV);
    return KVSTORE_NOMEM;
  }
  pKV->db->mutex = sqlite3MutexAlloc(SQLITE_MUTEX_RECURSIVE);
  pKV->db->aLimit[SQLITE_LIMIT_LENGTH] = SQLITE_MAX_LENGTH;

  /* Install busy handler if a positive timeout was requested */
  if( busyTimeout > 0 ){
    KVBusyCtx *ctx = (KVBusyCtx*)sqlite3MallocZero(sizeof(KVBusyCtx));
    if( !ctx ){
      sqlite3_mutex_free(pKV->db->mutex);
      sqlite3_free(pKV->db);
      sqlite3_mutex_free(pKV->pMutex);
      sqlite3_free(pKV);
      return KVSTORE_NOMEM;
    }
    ctx->timeoutMs = busyTimeout;
    ctx->pVfs      = sqlite3_vfs_find(0);
    pKV->db->busyHandler.xBusyHandler = kvstoreBusyHandler;
    pKV->db->busyHandler.pBusyArg     = ctx;
    pKV->db->busyTimeout              = busyTimeout;
  }

  /* Choose VFS flags based on readOnly */
  int vfsFlags = readOnly
    ? (SQLITE_OPEN_READONLY  | SQLITE_OPEN_MAIN_DB)
    : (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB);

  /* Open the btree */
  rc = sqlite3BtreeOpen(
    sqlite3_vfs_find(0),
    zFilename,
    pKV->db,
    &pKV->pBt,
    0,           /* no btree flags */
    vfsFlags
  );
  if( rc != SQLITE_OK ){
    kvstoreSetError(pKV, "failed to open btree: error %d", rc);
    kvstoreTeardownNoLock(pKV);
    sqlite3_mutex_free(pKV->pMutex);
    sqlite3_free(pKV);
    return rc;
  }

  /* Set page size for new databases (must be before any write transaction).
  ** sqlite3BtreeSetPageSize returns SQLITE_READONLY for existing databases
  ** (page size already fixed) — silently ignore that. */
  if( pageSize > 0 ){
    int psRc = sqlite3BtreeSetPageSize(pKV->pBt, pageSize, -1, 0);
    if( psRc != SQLITE_OK && psRc != SQLITE_READONLY ){
      kvstoreSetError(pKV, "failed to set page size: error %d", psRc);
      kvstoreTeardownNoLock(pKV);
      sqlite3_mutex_free(pKV->pMutex);
      sqlite3_free(pKV);
      return psRc;
    }
  }

  /* Set cache size */
  sqlite3BtreeSetCacheSize(pKV->pBt, cacheSize);

  /* Set synchronous level via pager flags.
  ** KVSTORE_SYNC_* maps to PAGER_SYNCHRONOUS_* with a +1 offset:
  **   KVSTORE_SYNC_OFF    (0) → PAGER_SYNCHRONOUS_OFF    (0x01)
  **   KVSTORE_SYNC_NORMAL (1) → PAGER_SYNCHRONOUS_NORMAL (0x02)
  **   KVSTORE_SYNC_FULL   (2) → PAGER_SYNCHRONOUS_FULL   (0x03)
  ** PAGER_CACHESPILL allows dirty pages to spill under memory pressure. */
  {
    unsigned syncFlag = (unsigned)(syncLevel + 1) | PAGER_CACHESPILL;
    sqlite3_mutex_enter(pKV->db->mutex);
    sqlite3BtreeSetPagerFlags(pKV->pBt, syncFlag);
    sqlite3_mutex_leave(pKV->db->mutex);
  }

  if( !readOnly ){
    /* Enable incremental auto-vacuum.  Must happen before SetVersion.
    ** SQLITE_READONLY returned for existing databases — that is expected. */
    int avRc = sqlite3BtreeSetAutoVacuum(pKV->pBt, BTREE_AUTOVACUUM_INCR);
    if( avRc != SQLITE_OK && avRc != SQLITE_READONLY ){
      kvstoreSetError(pKV, "failed to set auto-vacuum mode: error %d", avRc);
      kvstoreTeardownNoLock(pKV);
      sqlite3_mutex_free(pKV->pMutex);
      sqlite3_free(pKV);
      return avRc;
    }

    /* Set journal mode (WAL or delete-journal).
    ** sqlite3BtreeSetVersion(2) writes the WAL marker into the header.
    ** sqlite3BtreeSetVersion(1) reverts to rollback-journal mode. */
    {
      int ver = (journalMode == KVSTORE_JOURNAL_WAL) ? 2 : 1;
      rc = sqlite3BtreeSetVersion(pKV->pBt, ver);
      if( rc == SQLITE_OK ){
        rc = sqlite3BtreeCommit(pKV->pBt);  /* SetVersion leaves a txn open */
      }
      if( rc != SQLITE_OK ){
        kvstoreSetError(pKV, "failed to set journal mode: error %d", rc);
        kvstoreTeardownNoLock(pKV);
        sqlite3_mutex_free(pKV->pMutex);
        sqlite3_free(pKV);
        return rc;
      }
    }
  }

  /* Check whether this is a new or existing database */
  u32 defaultCFRoot = 0;
  u32 cfMetaRoot    = 0;
  int needsInit     = 0;

  rc = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
  if( rc != SQLITE_OK ){
    kvstoreSetError(pKV, "failed to begin read transaction: error %d", rc);
    kvstoreTeardownNoLock(pKV);
    sqlite3_mutex_free(pKV->pMutex);
    sqlite3_free(pKV);
    return rc;
  }
  sqlite3BtreeGetMeta(pKV->pBt, META_DEFAULT_CF_ROOT, &defaultCFRoot);
  sqlite3BtreeGetMeta(pKV->pBt, META_CF_METADATA_ROOT, &cfMetaRoot);
  sqlite3BtreeCommit(pKV->pBt);

  if( defaultCFRoot == 0 ){
    needsInit = 1;
  }

  if( needsInit ){
    if( readOnly ){
      /* Cannot initialise a brand-new database in read-only mode */
      kvstoreSetError(pKV, "cannot open empty database read-only");
      kvstoreTeardownNoLock(pKV);
      sqlite3_mutex_free(pKV->pMutex);
      sqlite3_free(pKV);
      return KVSTORE_READONLY;
    }
    rc = createDefaultCF(pKV);
    if( rc != SQLITE_OK ){
      kvstoreTeardownNoLock(pKV);
      sqlite3_mutex_free(pKV->pMutex);
      sqlite3_free(pKV);
      return rc;
    }
    rc = initCFMetadataTable(pKV);
    if( rc != SQLITE_OK ){
      kvstoreTeardownNoLock(pKV);
      sqlite3_mutex_free(pKV->pMutex);
      sqlite3_free(pKV);
      return rc;
    }
  }else{
    /* Existing database — open the default CF */
    KVColumnFamily *pCF = (KVColumnFamily*)sqlite3MallocZero(sizeof(KVColumnFamily));
    if( !pCF ){
      kvstoreTeardownNoLock(pKV);
      sqlite3_mutex_free(pKV->pMutex);
      sqlite3_free(pKV);
      return KVSTORE_NOMEM;
    }
    pCF->pKV    = pKV;
    pCF->zName  = sqliteStrDup(DEFAULT_CF_NAME);
    pCF->iTable = (int)defaultCFRoot;
    pCF->refCount = 1;
    pCF->pMutex = sqlite3MutexAlloc(SQLITE_MUTEX_RECURSIVE);
    if( !pCF->pMutex ){
      sqlite3_free(pCF->zName);
      sqlite3_free(pCF);
      kvstoreTeardownNoLock(pKV);
      sqlite3_mutex_free(pKV->pMutex);
      sqlite3_free(pKV);
      return KVSTORE_NOMEM;
    }
    pKV->pDefaultCF = pCF;
    pKV->iMetaTable = (int)cfMetaRoot;
  }

  /* Allocate a shared KeyInfo used by all BLOBKEY data-table cursors */
  {
    KeyInfo *pKI = (KeyInfo*)sqlite3MallocZero(SZ_KEYINFO(1));
    if( !pKI ){
      kvstoreTeardownNoLock(pKV);
      sqlite3_mutex_free(pKV->pMutex);
      sqlite3_free(pKV);
      return KVSTORE_NOMEM;
    }
    pKI->nRef      = 1;
    pKI->enc       = SQLITE_UTF8;
    pKI->nKeyField = 1;
    pKI->nAllField = 1;
    pKI->db        = pKV->db;
    pKI->aSortFlags = 0;
    pKI->aColl[0]  = 0;
    pKV->pKeyInfo  = pKI;
  }

  /* Keep a persistent read transaction open to avoid per-operation
  ** BeginTrans/Commit overhead on the hot read path. */
  {
    int rcR = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
    if( rcR == SQLITE_OK ) pKV->inTrans = 1;
    /* Non-fatal if it fails; auto-trans will handle individual operations. */
  }

  /* WAL auto-checkpoint config (walCommits starts at 0 via sqlite3MallocZero). */
  pKV->walSizeLimit = pConfig ? pConfig->walSizeLimit : 0;

  /* Probe for existing TTL index CFs from a previous session (default CF). */
  {
    char zKeyName[SNKV_TTL_NAME_BUFLEN], zExpName[SNKV_TTL_NAME_BUFLEN];
    snprintf(zKeyName, SNKV_TTL_NAME_BUFLEN, "%s%s", SNKV_TTL_KEY_PREFIX, DEFAULT_CF_NAME);
    snprintf(zExpName, SNKV_TTL_NAME_BUFLEN, "%s%s", SNKV_TTL_EXP_PREFIX, DEFAULT_CF_NAME);
    KVColumnFamily *pKeyCF = NULL, *pExpCF = NULL;
    sqlite3_mutex_enter(pKV->pMutex);
    if( kvstoreCfOpenInternal(pKV, zKeyName, &pKeyCF) == KVSTORE_OK &&
        kvstoreCfOpenInternal(pKV, zExpName, &pExpCF) == KVSTORE_OK ){
      pKV->pDefaultCF->pTtlKeyCF    = pKeyCF;
      pKV->pDefaultCF->pTtlExpiryCF = pExpCF;
      pKV->pDefaultCF->hasTtl       = 1;
    } else {
      if( pKeyCF ) kvstoreFreeCFStruct(pKeyCF);
      if( pExpCF ) kvstoreFreeCFStruct(pExpCF);
    }
    sqlite3_mutex_leave(pKV->pMutex);
  }

  *ppKV = pKV;
  return KVSTORE_OK;
}

/*
** Open a key-value store database file (simplified interface).
** Delegates to kvstore_open_v2 with journalMode set and all other
** fields at their defaults.
*/
int kvstore_open(
  const char *zFilename,
  KVStore **ppKV,
  int journalMode
){
  KVStoreConfig cfg;
  memset(&cfg, 0, sizeof(cfg));
  cfg.journalMode = journalMode;
  return kvstore_open_v2(zFilename, ppKV, &cfg);
}

/*
** Close a key-value store.
*/
int kvstore_close(KVStore *pKV){
  int rc, i;

  if( pKV == NULL ){
    return KVSTORE_OK;
  }

  /* Acquire mutex and signal closure so racing threads bail out cleanly */
  sqlite3_mutex_enter(pKV->pMutex);
  pKV->closing = 1;

  /* Rollback any active transaction */
  if( pKV->inTrans ){
    sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
    pKV->inTrans = 0;
  }

  /* Close all open column families (including TTL index CFs) */
  kvstoreFreeCFStruct(pKV->pDefaultCF);
  pKV->pDefaultCF = NULL;

  for(i = 0; i < pKV->nCF; i++){
    kvstoreFreeCFStruct(pKV->apCF[i]);
  }
  sqlite3_free(pKV->apCF);

  /* Close the btree */
  rc = sqlite3BtreeClose(pKV->pBt);

  /* Free shared KeyInfo */
  sqlite3_free(pKV->pKeyInfo);

  /* Free error message */
  if( pKV->zErrMsg ){
    sqlite3_free(pKV->zErrMsg);
  }

  /* Free db resources (including busy-handler context if installed) */
  if( pKV->db->busyHandler.pBusyArg ){
    sqlite3_free(pKV->db->busyHandler.pBusyArg);
  }
  if( pKV->db->mutex ){
    sqlite3_mutex_free(pKV->db->mutex);
  }
  sqlite3_free(pKV->db);

  /* Release and free mutex */
  sqlite3_mutex_leave(pKV->pMutex);
  sqlite3_mutex_free(pKV->pMutex);

  sqlite3_free(pKV);

  return rc;
}

/*
** Begin a transaction.
*/
int kvstore_begin(KVStore *pKV, int wrflag){
  int rc;

  if( pKV == NULL ){
    return KVSTORE_ERROR;
  }

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot begin transaction: database is corrupted");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_CORRUPT;
  }

  if( pKV->inTrans == 2 ){
    /* Write transaction already active — reject */
    kvstoreSetError(pKV, "transaction already active");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  if( pKV->inTrans == 1 && !wrflag ){
    /* Persistent read is already open; just reuse it */
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_OK;
  }

  /* inTrans==1 && wrflag: SQLite forbids upgrading a read that holds the
  ** WAL checkpoint slot (readLock==0) — always SQLITE_BUSY.  Release the
  ** persistent read cleanly and then open a fresh write transaction instead.
  ** inTrans==0          : open a fresh read or write transaction directly. */
  if( pKV->inTrans == 1 ){
    sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
  }

  rc = sqlite3BtreeBeginTrans(pKV->pBt, wrflag, 0);
  if( rc == SQLITE_OK ){
    pKV->inTrans = wrflag ? 2 : 1;
  }else{
    kvstoreCheckCorruption(pKV, rc);
    kvstoreSetError(pKV, "failed to begin transaction: error %d", rc);
  }

  sqlite3_mutex_leave(pKV->pMutex);
  return rc;
}

/*
** Commit the current transaction.
*/
int kvstore_commit(KVStore *pKV){
  int rc;

  if( pKV == NULL ){
    return KVSTORE_ERROR;
  }

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  if( !pKV->inTrans ){
    kvstoreSetError(pKV, "no active transaction to commit");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  rc = sqlite3BtreeCommit(pKV->pBt);
  if( rc == SQLITE_OK ){
    pKV->inTrans = 0;
    kvstoreAutoCheckpoint(pKV);
    /* Restore the persistent read transaction so that subsequent get/exists
    ** calls avoid per-call BeginTrans/Commit overhead. */
    if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ){
      pKV->inTrans = 1;
    }
  }else{
    kvstoreCheckCorruption(pKV, rc);
    kvstoreSetError(pKV, "failed to commit transaction: error %d", rc);
  }

  sqlite3_mutex_leave(pKV->pMutex);
  return rc;
}

/*
** Rollback the current transaction.
*/
int kvstore_rollback(KVStore *pKV){
  int rc;

  if( pKV == NULL ){
    return KVSTORE_ERROR;
  }

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  if( !pKV->inTrans ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_OK; /* No transaction to rollback */
  }

  rc = sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
  pKV->inTrans = 0;

  if( rc != SQLITE_OK ){
    kvstoreCheckCorruption(pKV, rc);
    kvstoreSetError(pKV, "failed to rollback transaction: error %d", rc);
  }

  /* Restore the persistent read transaction after rollback. */
  if( !pKV->closing ){
    if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ){
      pKV->inTrans = 1;
    }
  }

  sqlite3_mutex_leave(pKV->pMutex);
  return rc;
}

/*
** Run a WAL checkpoint on the database.
*/
int kvstore_checkpoint(KVStore *pKV, int mode, int *pnLog, int *pnCkpt){
  int rc;
  if( pKV == NULL ) return KVSTORE_ERROR;

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  if( pKV->inTrans == 2 ){
    kvstoreSetError(pKV, "commit or rollback the write transaction first");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_BUSY;
  }

  if( pnLog  ) *pnLog  = 0;
  if( pnCkpt ) *pnCkpt = 0;

  /* Release the persistent read transaction — sqlite3BtreeCheckpoint
  ** requires TRANS_NONE (returns SQLITE_LOCKED otherwise). */
  if( pKV->inTrans == 1 ){
    sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
  }

#ifndef SQLITE_OMIT_WAL
  rc = sqlite3BtreeCheckpoint(pKV->pBt, mode, pnLog, pnCkpt);
#else
  rc = SQLITE_OK;
#endif

  /* Restore the persistent read transaction. */
  if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ){
    pKV->inTrans = 1;
  }

  if( rc != SQLITE_OK ){
    kvstoreSetError(pKV, "checkpoint failed: error %d", rc);
  }

  sqlite3_mutex_leave(pKV->pMutex);
  return rc;
}

/* Forward declarations for functions defined later in this file. */
static int kvstore_cf_put_internal(
  KVColumnFamily *pCF,
  const void *pKey, int nKey,
  const void *pValue, int nValue
);

/* ======================================================================
** Raw btree helpers.
** These operate directly on a btree table (iTable) without acquiring any
** mutexes or managing transactions.  The caller MUST:
**   - hold pKV->pMutex
**   - be in the correct transaction state (write for put/delete, read for get)
**
** They are used exclusively by TTL internals to perform TTL CF writes
** within the same transaction as the data CF write.
** ====================================================================== */

/*
** Raw btree put: insert/overwrite (pKey,nKey) → (pVal,nVal) in table iTable.
** Uses BLOBKEY encoding.  Does not increment stats.
*/
static int kvstoreRawBtreePut(
  KVStore *pKV, int iTable,
  const void *pKey, int nKey,
  const void *pVal, int nVal
){
  BtCursor *pCur = kvstoreAllocCursor();
  if( !pCur ) return SQLITE_NOMEM;

  int rc = sqlite3BtreeCursor(pKV->pBt, iTable, 1, pKV->pKeyInfo, pCur);
  if( rc != SQLITE_OK ){ kvstoreFreeCursor(pCur); return rc; }

  unsigned char stackBuf[512];
  unsigned char *pEncoded;
  int nEncoded = kvstoreEncodeBlob(pKey, nKey, pVal, nVal,
                                   stackBuf, (int)sizeof(stackBuf), &pEncoded);
  if( nEncoded < 0 ){ kvstoreFreeCursor(pCur); return SQLITE_NOMEM; }

  BtreePayload payload;
  memset(&payload, 0, sizeof(payload));
  payload.pKey = pEncoded;
  payload.nKey = nEncoded;
  rc = sqlite3BtreeInsert(pCur, &payload, 0, 0);

  if( pEncoded != stackBuf ) sqlite3_free(pEncoded);
  kvstoreFreeCursor(pCur);
  return rc;
}

/*
** Raw btree delete: remove the entry with the given key from table iTable.
** Returns KVSTORE_NOTFOUND if not present (not an error for TTL cleanup).
*/
static int kvstoreRawBtreeDelete(
  KVStore *pKV, int iTable,
  const void *pKey, int nKey
){
  BtCursor *pCur = kvstoreAllocCursor();
  if( !pCur ) return SQLITE_NOMEM;

  int rc = sqlite3BtreeCursor(pKV->pBt, iTable, 1, pKV->pKeyInfo, pCur);
  if( rc != SQLITE_OK ){ kvstoreFreeCursor(pCur); return rc; }

  int found = 0;
  rc = kvstoreSeekKey(pCur, pKV->pKeyInfo, pKey, nKey, &found);
  if( rc == SQLITE_OK && found ){
    rc = sqlite3BtreeDelete(pCur, 0);
  } else if( rc == SQLITE_OK && !found ){
    rc = KVSTORE_NOTFOUND;
  }

  kvstoreFreeCursor(pCur);
  return rc;
}

/*
** Raw btree get: read the value for key from table iTable.
** On success *ppVal / *pnVal are set; caller must sqlite3_free(*ppVal).
** Returns KVSTORE_NOTFOUND if not present.
** Caller must be in at least a read transaction.
*/
static int kvstoreRawBtreeGet(
  KVStore *pKV, int iTable,
  const void *pKey, int nKey,
  void **ppVal, int *pnVal
){
  BtCursor *pCur = kvstoreAllocCursor();
  if( !pCur ) return SQLITE_NOMEM;

  int rc = sqlite3BtreeCursor(pKV->pBt, iTable, 0, pKV->pKeyInfo, pCur);
  if( rc != SQLITE_OK ){ kvstoreFreeCursor(pCur); return rc; }

  int found = 0;
  rc = kvstoreSeekKey(pCur, pKV->pKeyInfo, pKey, nKey, &found);
  if( rc != SQLITE_OK || !found ){
    kvstoreFreeCursor(pCur);
    return (rc == SQLITE_OK) ? KVSTORE_NOTFOUND : rc;
  }

  u32 payloadSz = sqlite3BtreePayloadSize(pCur);
  unsigned char hdr[4];
  rc = sqlite3BtreePayload(pCur, 0, 4, hdr);
  if( rc != SQLITE_OK ){ kvstoreFreeCursor(pCur); return rc; }

  int storedKeyLen = (hdr[0]<<24)|(hdr[1]<<16)|(hdr[2]<<8)|hdr[3];
  int valueLen = (int)payloadSz - 4 - storedKeyLen;
  if( valueLen < 0 ){ kvstoreFreeCursor(pCur); return KVSTORE_CORRUPT; }

  void *pValue = NULL;
  if( valueLen > 0 ){
    pValue = sqlite3Malloc(valueLen);
    if( !pValue ){ kvstoreFreeCursor(pCur); return SQLITE_NOMEM; }
    rc = sqlite3BtreePayload(pCur, 4 + storedKeyLen, valueLen, pValue);
    if( rc != SQLITE_OK ){
      sqlite3_free(pValue);
      kvstoreFreeCursor(pCur);
      return rc;
    }
  }

  kvstoreFreeCursor(pCur);
  *ppVal = pValue;
  *pnVal = valueLen;
  return SQLITE_OK;
}

/* ======================================================================
** Internal CF open: open an existing CF by name without public-API
** restrictions (accepts names starting with "__").
** Caller must hold pKV->pMutex.  Uses the existing transaction.
** ====================================================================== */
static int kvstoreCfOpenInternal(
  KVStore *pKV,
  const char *zName,
  KVColumnFamily **ppCF
){
  int rc;
  BtCursor *pCur = NULL;
  int autoTrans = 0;
  i64 nNameLen = (i64)strlen(zName);
  int iTable;

  *ppCF = NULL;

  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
    if( rc != SQLITE_OK ) return rc;
    autoTrans = 1;
    pKV->inTrans = 1;
  }

  pCur = kvstoreAllocCursor();
  if( !pCur ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    return SQLITE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 0, 0, pCur);
  if( rc != SQLITE_OK ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    return rc;
  }

  int found = 0;
  i64 foundRowid = 0;
  rc = kvstoreMetaSeekKey(pCur, zName, (int)nNameLen, &found, &foundRowid);
  if( rc != SQLITE_OK || !found ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
    return (rc == SQLITE_OK) ? KVSTORE_NOTFOUND : rc;
  }

  int res = 0;
  rc = sqlite3BtreeTableMoveto(pCur, foundRowid, 0, &res);
  if( rc != SQLITE_OK || res != 0 ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
    return KVSTORE_CORRUPT;
  }

  u32 payloadSz = sqlite3BtreePayloadSize(pCur);
  if( (int)payloadSz < 4 + (int)nNameLen + 4 ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
    return KVSTORE_CORRUPT;
  }

  unsigned char tableRootBytes[4];
  rc = sqlite3BtreePayload(pCur, 4 + (int)nNameLen, 4, tableRootBytes);
  kvstoreFreeCursor(pCur);
  if( autoTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
  if( rc != SQLITE_OK ) return rc;

  iTable = (tableRootBytes[0]<<24)|(tableRootBytes[1]<<16)|
           (tableRootBytes[2]<<8)|tableRootBytes[3];

  KVColumnFamily *pCF = (KVColumnFamily*)sqlite3MallocZero(sizeof(KVColumnFamily));
  if( !pCF ) return KVSTORE_NOMEM;

  char *zNameCopy = sqliteStrDup(zName);
  if( !zNameCopy ){ sqlite3_free(pCF); return KVSTORE_NOMEM; }

  pCF->pKV     = pKV;
  pCF->zName   = zNameCopy;
  pCF->iTable  = iTable;
  pCF->refCount = 1;
  pCF->pMutex  = sqlite3MutexAlloc(SQLITE_MUTEX_RECURSIVE);
  if( !pCF->pMutex ){
    sqlite3_free(zNameCopy);
    sqlite3_free(pCF);
    return KVSTORE_NOMEM;
  }

  *ppCF = pCF;
  return KVSTORE_OK;
}

/* ======================================================================
** kvstoreCreateOrOpenHiddenCF — open or create a hidden CF by name.
**
** Caller MUST hold pKV->pMutex and MUST be in a write transaction
** (pKV->inTrans == 2) because creation modifies the metadata table.
**
** Returns KVSTORE_OK and sets *ppCF on success.
** ====================================================================== */
static int kvstoreCreateOrOpenHiddenCF(
  KVStore *pKV,
  const char *zName,
  KVColumnFamily **ppCF
){
  /* Try to open an already-existing CF (e.g. from a previous session). */
  KVColumnFamily *pCF = NULL;
  int rc = kvstoreCfOpenInternal(pKV, zName, &pCF);
  if( rc == KVSTORE_OK ){ *ppCF = pCF; return KVSTORE_OK; }
  if( rc != KVSTORE_NOTFOUND ) return rc;  /* real I/O error */

  /* Not found — create it within the caller's write transaction. */
  Pgno pgno = 0;
  rc = sqlite3BtreeCreateTable(pKV->pBt, &pgno, BTREE_BLOBKEY);
  if( rc != SQLITE_OK ) return rc;

  int nNameLen = (int)strlen(zName);
  int iTable   = (int)pgno;
  unsigned char tableRootBytes[4];
  tableRootBytes[0] = (iTable >> 24) & 0xFF;
  tableRootBytes[1] = (iTable >> 16) & 0xFF;
  tableRootBytes[2] = (iTable >>  8) & 0xFF;
  tableRootBytes[3] = iTable & 0xFF;

  unsigned char metaStack[320];
  unsigned char *pEncoded;
  int nEncoded = kvstoreEncodeBlob(zName, nNameLen, tableRootBytes, 4,
                                   metaStack, (int)sizeof(metaStack), &pEncoded);
  if( nEncoded < 0 ) return KVSTORE_NOMEM;

  BtCursor *pCur = kvstoreAllocCursor();
  if( !pCur ){
    if( pEncoded != metaStack ) sqlite3_free(pEncoded);
    return KVSTORE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 1, 0, pCur);
  if( rc == SQLITE_OK ){
    i64 slot;
    rc = kvstoreMetaFindSlot(pCur, zName, nNameLen, &slot);
    if( rc == SQLITE_OK ){
      BtreePayload payload;
      memset(&payload, 0, sizeof(payload));
      payload.nKey  = slot;
      payload.pData = pEncoded;
      payload.nData = nEncoded;
      rc = sqlite3BtreeInsert(pCur, &payload, 0, 0);
    }
  }
  if( pEncoded != metaStack ) sqlite3_free(pEncoded);
  kvstoreFreeCursor(pCur);
  if( rc != SQLITE_OK ) return rc;

  u32 cfCount = 0;
  sqlite3BtreeGetMeta(pKV->pBt, META_CF_COUNT, &cfCount);
  cfCount++;
  sqlite3BtreeUpdateMeta(pKV->pBt, META_CF_COUNT, cfCount);

  /* Build CF struct. */
  pCF = (KVColumnFamily*)sqlite3MallocZero(sizeof(KVColumnFamily));
  if( !pCF ) return KVSTORE_NOMEM;

  char *zNameCopy = sqliteStrDup(zName);
  if( !zNameCopy ){ sqlite3_free(pCF); return KVSTORE_NOMEM; }

  pCF->pKV      = pKV;
  pCF->zName    = zNameCopy;
  pCF->iTable   = iTable;
  pCF->refCount = 1;
  pCF->pMutex   = sqlite3MutexAlloc(SQLITE_MUTEX_RECURSIVE);
  if( !pCF->pMutex ){
    sqlite3_free(zNameCopy);
    sqlite3_free(pCF);
    return KVSTORE_NOMEM;
  }

  *ppCF = pCF;
  return KVSTORE_OK;
}

/* ======================================================================
** kvstoreGetOrCreateTtlCFs — get (or lazily create) both TTL index CFs
** for a user column family.
**
** Caller MUST hold pCF->pMutex + pKV->pMutex and MUST be in a write
** transaction (pKV->inTrans == 2).
**
** On success sets pCF->hasTtl = 1 and fills pCF->pTtlKeyCF /
** pCF->pTtlExpiryCF.  Returns KVSTORE_OK.
** ====================================================================== */
static int kvstoreGetOrCreateTtlCFs(KVColumnFamily *pCF){
  if( pCF->hasTtl ) return KVSTORE_OK;
  KVStore *pKV = pCF->pKV;

  char zKeyName[SNKV_TTL_NAME_BUFLEN];
  char zExpName[SNKV_TTL_NAME_BUFLEN];
  snprintf(zKeyName, SNKV_TTL_NAME_BUFLEN, "%s%s", SNKV_TTL_KEY_PREFIX, pCF->zName);
  snprintf(zExpName, SNKV_TTL_NAME_BUFLEN, "%s%s", SNKV_TTL_EXP_PREFIX, pCF->zName);

  KVColumnFamily *pKeyCF = NULL, *pExpCF = NULL;
  int rc = kvstoreCreateOrOpenHiddenCF(pKV, zKeyName, &pKeyCF);
  if( rc != KVSTORE_OK ) return rc;

  rc = kvstoreCreateOrOpenHiddenCF(pKV, zExpName, &pExpCF);
  if( rc != KVSTORE_OK ){
    kvstoreFreeCFStruct(pKeyCF);
    return rc;
  }

  pCF->pTtlKeyCF    = pKeyCF;
  pCF->pTtlExpiryCF = pExpCF;
  pCF->hasTtl       = 1;
  return KVSTORE_OK;
}
static int kvstore_cf_get_internal(
  KVColumnFamily *pCF,
  const void *pKey, int nKey,
  void **ppValue, int *pnValue
);
static int kvstore_cf_delete_internal(
  KVColumnFamily *pCF,
  const void *pKey, int nKey
);
static int kvstore_cf_exists_internal(
  KVColumnFamily *pCF,
  const void *pKey, int nKey,
  int *pExists
);

/*
** Default CF operations (wrappers)
*/
int kvstore_put(KVStore *pKV, const void *pKey, int nKey,
                const void *pValue, int nValue){
  if( !pKV || !pKV->pDefaultCF ) return KVSTORE_ERROR;
  return kvstore_cf_put_internal(pKV->pDefaultCF, pKey, nKey, pValue, nValue);
}

int kvstore_get(KVStore *pKV, const void *pKey, int nKey,
                void **ppValue, int *pnValue){
  if( !pKV || !pKV->pDefaultCF ) return KVSTORE_ERROR;
  return kvstore_cf_get_internal(pKV->pDefaultCF, pKey, nKey, ppValue, pnValue);
}

int kvstore_delete(KVStore *pKV, const void *pKey, int nKey){
  if( !pKV || !pKV->pDefaultCF ) return KVSTORE_ERROR;
  return kvstore_cf_delete_internal(pKV->pDefaultCF, pKey, nKey);
}

int kvstore_exists(KVStore *pKV, const void *pKey, int nKey, int *pExists){
  if( !pKV || !pKV->pDefaultCF ) return KVSTORE_ERROR;
  return kvstore_cf_exists_internal(pKV->pDefaultCF, pKey, nKey, pExists);
}

/*
** Get default column family
*/
int kvstore_cf_get_default(KVStore *pKV, KVColumnFamily **ppCF){
  if( !pKV || !ppCF ){
    return KVSTORE_ERROR;
  }

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  if( !pKV->pDefaultCF ){
    kvstoreSetError(pKV, "default column family not initialized");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  pKV->pDefaultCF->refCount++;
  *ppCF = pKV->pDefaultCF;

  sqlite3_mutex_leave(pKV->pMutex);
  return KVSTORE_OK;
}

/*
** Create a new column family
*/
int kvstore_cf_create(KVStore *pKV, const char *zName, KVColumnFamily **ppCF){
  int rc;
  BtCursor *pCur = NULL;
  KVColumnFamily *pCF = NULL;
  int autoTrans = 0;
  Pgno pgno;
  u32 cfCount;
  i64 nNameLen;
  char *zNameCopy = NULL;

  if( !pKV || !zName || !ppCF ){
    if( pKV ) kvstoreSetError(pKV, "invalid parameters to cf_create");
    return KVSTORE_ERROR;
  }

  *ppCF = NULL;

  nNameLen = strlen(zName);
  if( nNameLen == 0 || nNameLen > KVSTORE_MAX_CF_NAME ){
    sqlite3_mutex_enter(pKV->pMutex);
    kvstoreSetError(pKV, "invalid column family name length");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  /* Names starting with "__" are reserved for internal use. */
  if( nNameLen >= 2 && zName[0] == '_' && zName[1] == '_' ){
    sqlite3_mutex_enter(pKV->pMutex);
    kvstoreSetError(pKV, "column family names starting with \"__\" are reserved");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  /* Check if default CF */
  if( strcmp(zName, DEFAULT_CF_NAME) == 0 ){
    return kvstore_cf_get_default(pKV, ppCF);
  }

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  /* Start write transaction (release persistent read first if active). */
  if( pKV->inTrans != 2 ){
    if( pKV->inTrans == 1 ){
      sqlite3BtreeCommit(pKV->pBt);
      pKV->inTrans = 0;
    }
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 1, 0);
    if( rc != SQLITE_OK ){
      sqlite3_mutex_leave(pKV->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 2;
  }

  /* Check if CF already exists – search metadata table */
  pCur = kvstoreAllocCursor();
  if( !pCur ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return SQLITE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 0, 0, pCur);
  if( rc != SQLITE_OK ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return rc;
  }

  int found = 0;
  rc = kvstoreMetaSeekKey(pCur, zName, (int)nNameLen, &found, NULL);
  kvstoreFreeCursor(pCur);
  pCur = NULL;

  if( rc != SQLITE_OK ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return rc;
  }
  if( found ){
    if( autoTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
    kvstoreSetError(pKV, "column family already exists: %s", zName);
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  /* Create new btree table for this CF (BLOBKEY) */
  rc = sqlite3BtreeCreateTable(pKV->pBt, &pgno, BTREE_BLOBKEY);
  if( rc != SQLITE_OK ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return rc;
  }

  /* Store CF metadata in the metadata table.
  ** The metadata table is also INTKEY.  We hash the CF name to get
  ** a rowid and store [name_len | name | table_root (4 bytes BE)] as data.
  */
  {
    unsigned char tableRootBytes[4];
    int iTable = (int)pgno;
    tableRootBytes[0] = (iTable >> 24) & 0xFF;
    tableRootBytes[1] = (iTable >> 16) & 0xFF;
    tableRootBytes[2] = (iTable >>  8) & 0xFF;
    tableRootBytes[3] = iTable & 0xFF;

    unsigned char metaStack[320]; /* enough for max CF name (255) + 4 + 4 + header */
    unsigned char *pEncoded;
    int nEncoded = kvstoreEncodeBlob(zName, (int)nNameLen,
                                     tableRootBytes, 4,
                                     metaStack, (int)sizeof(metaStack),
                                     &pEncoded);
    if( nEncoded < 0 ){
      if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
      sqlite3_mutex_leave(pKV->pMutex);
      return SQLITE_NOMEM;
    }

    pCur = kvstoreAllocCursor();
    if( !pCur ){
      if( pEncoded != metaStack ) sqlite3_free(pEncoded);
      if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
      sqlite3_mutex_leave(pKV->pMutex);
      return SQLITE_NOMEM;
    }
    rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 1, 0, pCur);
    if( rc != SQLITE_OK ){
      if( pEncoded != metaStack ) sqlite3_free(pEncoded);
      kvstoreFreeCursor(pCur);
      if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
      sqlite3_mutex_leave(pKV->pMutex);
      return rc;
    }

    i64 slot;
    rc = kvstoreMetaFindSlot(pCur, zName, (int)nNameLen, &slot);
    if( rc != SQLITE_OK ){
      if( pEncoded != metaStack ) sqlite3_free(pEncoded);
      kvstoreFreeCursor(pCur);
      if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
      sqlite3_mutex_leave(pKV->pMutex);
      return rc;
    }

    BtreePayload payload;
    memset(&payload, 0, sizeof(payload));
    payload.nKey  = slot;
    payload.pData = pEncoded;
    payload.nData = nEncoded;

    rc = sqlite3BtreeInsert(pCur, &payload, 0, 0);
    if( pEncoded != metaStack ) sqlite3_free(pEncoded);
    kvstoreFreeCursor(pCur);
    pCur = NULL;

    if( rc != SQLITE_OK ){
      if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
      sqlite3_mutex_leave(pKV->pMutex);
      return rc;
    }
  }

  /* Update CF count */
  sqlite3BtreeGetMeta(pKV->pBt, META_CF_COUNT, &cfCount);
  cfCount++;
  sqlite3BtreeUpdateMeta(pKV->pBt, META_CF_COUNT, cfCount);

  /* Commit if we started the transaction, then restore persistent read. */
  if( autoTrans ){
    rc = sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
    if( rc != SQLITE_OK ){
      sqlite3_mutex_leave(pKV->pMutex);
      return rc;
    }
    if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ){
      pKV->inTrans = 1;
    }
  }

  /* Create CF structure */
  pCF = (KVColumnFamily*)sqlite3MallocZero(sizeof(KVColumnFamily));
  if( !pCF ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }

  zNameCopy = sqliteStrDup(zName);
  if( !zNameCopy ){
    sqlite3_free(pCF);
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }

  pCF->pKV = pKV;
  pCF->zName = zNameCopy;
  pCF->iTable = (int)pgno;
  pCF->refCount = 1;

  /* Create mutex for this CF */
  pCF->pMutex = sqlite3MutexAlloc(SQLITE_MUTEX_RECURSIVE);
  if( !pCF->pMutex ){
    sqlite3_free(zNameCopy);
    sqlite3_free(pCF);
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }

  sqlite3_mutex_leave(pKV->pMutex);

  *ppCF = pCF;
  return KVSTORE_OK;
}

/*
** Open existing column family
*/
int kvstore_cf_open(KVStore *pKV, const char *zName, KVColumnFamily **ppCF){
  int rc;
  BtCursor *pCur = NULL;
  KVColumnFamily *pCF = NULL;
  int autoTrans = 0;
  i64 nNameLen;
  int iTable;
  char *zNameCopy = NULL;

  if( !pKV || !zName || !ppCF ){
    if( pKV ){
      sqlite3_mutex_enter(pKV->pMutex);
      kvstoreSetError(pKV, "invalid parameters to cf_open");
      sqlite3_mutex_leave(pKV->pMutex);
    }
    return KVSTORE_ERROR;
  }

  *ppCF = NULL;
  nNameLen = strlen(zName);

  /* Check if default CF */
  if( strcmp(zName, DEFAULT_CF_NAME) == 0 ){
    return kvstore_cf_get_default(pKV, ppCF);
  }

  /* Names starting with "__" are reserved for internal use. */
  if( nNameLen >= 2 && zName[0] == '_' && zName[1] == '_' ){
    sqlite3_mutex_enter(pKV->pMutex);
    kvstoreSetError(pKV, "column family names starting with \"__\" are reserved");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  /* Start read transaction if needed */
  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
    if( rc != SQLITE_OK ){
      sqlite3_mutex_leave(pKV->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 1;
  }

  /* Look up CF in metadata table */
  pCur = kvstoreAllocCursor();
  if( !pCur ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return SQLITE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 0, 0, pCur);
  if( rc != SQLITE_OK ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return rc;
  }

  int found = 0;
  i64 foundRowid = 0;
  rc = kvstoreMetaSeekKey(pCur, zName, (int)nNameLen, &found, &foundRowid);
  if( rc != SQLITE_OK || !found ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return (rc == SQLITE_OK) ? KVSTORE_NOTFOUND : rc;
  }

  /* Re-position cursor on the found rowid and read data */
  {
    int res = 0;
    rc = sqlite3BtreeTableMoveto(pCur, foundRowid, 0, &res);
    if( rc != SQLITE_OK || res != 0 ){
      kvstoreFreeCursor(pCur);
      if( autoTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
      sqlite3_mutex_leave(pKV->pMutex);
      return KVSTORE_CORRUPT;
    }
  }

  /* Read the payload: [name_len(4) | name | table_root(4)] */
  u32 payloadSz = sqlite3BtreePayloadSize(pCur);
  if( (int)payloadSz < 4 + (int)nNameLen + 4 ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_CORRUPT;
  }

  /* Read table root bytes (last 4 bytes of value portion) */
  unsigned char tableRootBytes[4];
  rc = sqlite3BtreePayload(pCur, 4 + (int)nNameLen, 4, tableRootBytes);
  kvstoreFreeCursor(pCur);
  pCur = NULL;

  if( autoTrans ){
    sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
  }

  if( rc != SQLITE_OK ){
    sqlite3_mutex_leave(pKV->pMutex);
    return rc;
  }

  iTable = (tableRootBytes[0] << 24) | (tableRootBytes[1] << 16) |
           (tableRootBytes[2] << 8) | tableRootBytes[3];

  /* Create CF structure */
  pCF = (KVColumnFamily*)sqlite3MallocZero(sizeof(KVColumnFamily));
  if( !pCF ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }

  zNameCopy = sqliteStrDup(zName);
  if( !zNameCopy ){
    sqlite3_free(pCF);
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }

  pCF->pKV = pKV;
  pCF->zName = zNameCopy;
  pCF->iTable = iTable;
  pCF->refCount = 1;

  /* Create mutex for this CF */
  pCF->pMutex = sqlite3MutexAlloc(SQLITE_MUTEX_RECURSIVE);
  if( !pCF->pMutex ){
    sqlite3_free(zNameCopy);
    sqlite3_free(pCF);
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }

  /* Probe for existing TTL index CFs from a previous session. */
  {
    char zKeyName[SNKV_TTL_NAME_BUFLEN], zExpName[SNKV_TTL_NAME_BUFLEN];
    snprintf(zKeyName, SNKV_TTL_NAME_BUFLEN, "%s%s", SNKV_TTL_KEY_PREFIX, zName);
    snprintf(zExpName, SNKV_TTL_NAME_BUFLEN, "%s%s", SNKV_TTL_EXP_PREFIX, zName);
    KVColumnFamily *pKeyCF2 = NULL, *pExpCF2 = NULL;
    if( kvstoreCfOpenInternal(pKV, zKeyName, &pKeyCF2) == KVSTORE_OK &&
        kvstoreCfOpenInternal(pKV, zExpName, &pExpCF2) == KVSTORE_OK ){
      pCF->pTtlKeyCF    = pKeyCF2;
      pCF->pTtlExpiryCF = pExpCF2;
      pCF->hasTtl       = 1;
    } else {
      if( pKeyCF2 ) kvstoreFreeCFStruct(pKeyCF2);
      if( pExpCF2 ) kvstoreFreeCFStruct(pExpCF2);
    }
  }

  sqlite3_mutex_leave(pKV->pMutex);

  *ppCF = pCF;
  return KVSTORE_OK;
}

/*
** Close column family handle
*/
void kvstore_cf_close(KVColumnFamily *pCF){
  KVStore *pKV;
  int shouldFree = 0;

  if( !pCF ) return;

  pKV = pCF->pKV;

  sqlite3_mutex_enter(pKV->pMutex);

  pCF->refCount--;
  if( pCF->refCount <= 0 && pCF != pKV->pDefaultCF ){
    shouldFree = 1;
  }

  sqlite3_mutex_leave(pKV->pMutex);

  if( shouldFree ){
    kvstoreFreeCFStruct(pCF);
  }
}

/*
** Internal implementation: Put to column family
*/
static int kvstore_cf_put_internal(
  KVColumnFamily *pCF,
  const void *pKey, int nKey,
  const void *pValue, int nValue
){
  BtCursor *pCur = NULL;
  int rc;
  int autoTrans = 0;
  KVStore *pKV = pCF->pKV;

  sqlite3_mutex_enter(pCF->pMutex);
  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_ERROR;
  }

  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot put: database is corrupted");
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_CORRUPT;
  }

  rc = kvstoreValidateKeyValue(pKV, pKey, nKey, pValue, nValue);
  if( rc != KVSTORE_OK ){
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return rc;
  }

  if( pKV->inTrans != 2 ){
    /* Release persistent read (if any) before starting a write — SQLite
    ** forbids upgrading when readLock==0 (WAL checkpoint slot). */
    if( pKV->inTrans == 1 ){
      sqlite3BtreeCommit(pKV->pBt);
      pKV->inTrans = 0;
    }
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 1, 0);
    if( rc != SQLITE_OK ){
      sqlite3_mutex_leave(pKV->pMutex);
      sqlite3_mutex_leave(pCF->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 2;
  }

  pCur = kvstoreAllocCursor();
  if( !pCur ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return SQLITE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pCF->iTable, 1, pKV->pKeyInfo, pCur);
  if( rc != SQLITE_OK ){
    kvstoreCheckCorruption(pKV, rc);
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return rc;
  }

  /* Encode [key_len | key | value] as a single BLOBKEY entry.
  ** Use a stack buffer for small payloads to avoid a heap allocation. */
  {
    unsigned char stackBuf[512];
    unsigned char *pEncoded;
    int nEncoded = kvstoreEncodeBlob(pKey, nKey, pValue, nValue,
                                     stackBuf, (int)sizeof(stackBuf),
                                     &pEncoded);
    if( nEncoded < 0 ){
      kvstoreFreeCursor(pCur);
      if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
      sqlite3_mutex_leave(pKV->pMutex);
      sqlite3_mutex_leave(pCF->pMutex);
      return SQLITE_NOMEM;
    }

    BtreePayload payload;
    memset(&payload, 0, sizeof(payload));
    payload.pKey  = pEncoded;
    payload.nKey  = nEncoded;
    /* pData and nData are 0 for index/BLOBKEY tables */

    rc = sqlite3BtreeInsert(pCur, &payload, 0, 0);
    if( pEncoded != stackBuf ) sqlite3_free(pEncoded);
  }
  kvstoreFreeCursor(pCur);

  /* TTL cleanup: if this CF has TTL index CFs and the write succeeded,
  ** remove any existing TTL entries for this key so that a previous
  ** put_ttl on the same key cannot cause a future expiry on the new value.
  */
  if( rc == SQLITE_OK && pCF->hasTtl && pCF->pTtlKeyCF ){
    void *pOldTtl = NULL; int nOldTtl = 0;
    int rck = kvstoreRawBtreeGet(pKV, pCF->pTtlKeyCF->iTable,
                                  pKey, nKey, &pOldTtl, &nOldTtl);
    if( rck == SQLITE_OK && nOldTtl == 8 ){
      unsigned char *pExpKey = (unsigned char*)sqlite3Malloc(8 + nKey);
      if( pExpKey ){
        memcpy(pExpKey, pOldTtl, 8);
        memcpy(pExpKey + 8, pKey, nKey);
        kvstoreRawBtreeDelete(pKV, pCF->pTtlExpiryCF->iTable, pExpKey, 8 + nKey);
        sqlite3_free(pExpKey);
      }
      kvstoreRawBtreeDelete(pKV, pCF->pTtlKeyCF->iTable, pKey, nKey);
    }
    if( pOldTtl ) sqlite3_free(pOldTtl);
  }

  if( rc == SQLITE_OK ){
    pKV->stats.nPuts++;
    if( autoTrans ){
      rc = sqlite3BtreeCommit(pKV->pBt);
      pKV->inTrans = 0;
      /* Restore persistent read transaction */
      if( rc == SQLITE_OK ){
        kvstoreAutoCheckpoint(pKV);
        if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ){
          pKV->inTrans = 1;
        }
      }
    }
  }else{
    kvstoreCheckCorruption(pKV, rc);
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
      pKV->inTrans = 0;
      /* Restore persistent read transaction */
      if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ){
        pKV->inTrans = 1;
      }
    }
  }

  sqlite3_mutex_leave(pKV->pMutex);
  sqlite3_mutex_leave(pCF->pMutex);

  return rc;
}

/*
** Internal implementation: Get from column family
*/
static int kvstore_cf_get_internal(
  KVColumnFamily *pCF,
  const void *pKey, int nKey,
  void **ppValue, int *pnValue
){
  BtCursor *pCur = NULL;
  int rc;
  KVStore *pKV = pCF->pKV;

  if( !ppValue || !pnValue ){
    sqlite3_mutex_enter(pKV->pMutex);
    kvstoreSetError(pKV, "invalid parameters to get");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  *ppValue = NULL;
  *pnValue = 0;

  sqlite3_mutex_enter(pCF->pMutex);
  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_ERROR;
  }

  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot get: database is corrupted");
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_CORRUPT;
  }

  if( !pKey || nKey <= 0 ){
    kvstoreSetError(pKV, "invalid key");
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_ERROR;
  }

  if( !pKV->inTrans ){
    /* Persistent read transaction normally keeps inTrans>=1; fall back if not. */
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
    if( rc != SQLITE_OK ){
      sqlite3_mutex_leave(pKV->pMutex);
      sqlite3_mutex_leave(pCF->pMutex);
      return rc;
    }
    pKV->inTrans = 1;
  }

  /* Use the cached read cursor — no malloc/free per call. */
  pCur = kvstoreGetReadCursor(pCF);
  if( !pCur ){
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return SQLITE_NOMEM;
  }

  int found = 0;
  rc = kvstoreSeekKey(pCur, pKV->pKeyInfo, pKey, nKey, &found);
  if( rc != SQLITE_OK || !found ){
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return (rc == SQLITE_OK) ? KVSTORE_NOTFOUND : rc;
  }

  /* TTL lazy-expiry check.  kvstoreRawBtreeGet on the key CF (a different
  ** table) does not disturb pCur which is positioned on the data CF. */
  if( pCF->hasTtl && pCF->pTtlKeyCF ){
    void *pTtlVal = NULL; int nTtlVal = 0;
    int rck = kvstoreRawBtreeGet(pKV, pCF->pTtlKeyCF->iTable,
                                  pKey, nKey, &pTtlVal, &nTtlVal);
    if( rck == SQLITE_OK && nTtlVal == 8 ){
      int64_t expireMs = kvstoreDecodeBE64((const unsigned char*)pTtlVal);
      sqlite3_free(pTtlVal);
      if( kvstore_now_ms() >= expireMs ){
        /* Expired — invalidate cached cursor, upgrade tx, lazy-delete. */
        kvstoreFreeCursor(pCF->pReadCur);
        pCF->pReadCur = NULL;
        if( pKV->inTrans == 1 ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
        if( sqlite3BtreeBeginTrans(pKV->pBt, 1, 0) == SQLITE_OK ){
          pKV->inTrans = 2;
          kvstoreRawBtreeDelete(pKV, pCF->iTable, pKey, nKey);
          kvstoreRawBtreeDelete(pKV, pCF->pTtlKeyCF->iTable, pKey, nKey);
          unsigned char expBuf[8]; kvstoreEncodeBE64(expBuf, expireMs);
          unsigned char *pExpKey = (unsigned char*)sqlite3Malloc(8 + nKey);
          if( pExpKey ){
            memcpy(pExpKey, expBuf, 8);
            memcpy(pExpKey + 8, pKey, nKey);
            kvstoreRawBtreeDelete(pKV, pCF->pTtlExpiryCF->iTable, pExpKey, 8 + nKey);
            sqlite3_free(pExpKey);
          }
          sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0;
          kvstoreAutoCheckpoint(pKV);
          if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ) pKV->inTrans = 1;
        }
        sqlite3_mutex_leave(pKV->pMutex);
        sqlite3_mutex_leave(pCF->pMutex);
        return KVSTORE_NOTFOUND;
      }
    } else {
      if( pTtlVal ) sqlite3_free(pTtlVal);
    }
  }

  /* Cursor is positioned on the matching entry — read the value portion. */
  u32 payloadSz = sqlite3BtreePayloadSize(pCur);
  int storedKeyLen = 0;
  {
    unsigned char hdr[4];
    rc = sqlite3BtreePayload(pCur, 0, 4, hdr);
    if( rc == SQLITE_OK ){
      storedKeyLen = (hdr[0]<<24)|(hdr[1]<<16)|(hdr[2]<<8)|hdr[3];
    }
  }

  int valueLen = (int)payloadSz - 4 - storedKeyLen;
  void *pValue = NULL;
  if( rc == SQLITE_OK && valueLen > 0 ){
    pValue = sqlite3Malloc(valueLen);
    if( !pValue ){
      rc = SQLITE_NOMEM;
    }else{
      rc = sqlite3BtreePayload(pCur, 4 + storedKeyLen, valueLen, pValue);
      if( rc != SQLITE_OK ){
        sqlite3_free(pValue);
        pValue = NULL;
      }
    }
  }

  if( rc == SQLITE_OK ){
    pKV->stats.nGets++;
    *ppValue = pValue;
    *pnValue = (valueLen > 0) ? valueLen : 0;
  }

  sqlite3_mutex_leave(pKV->pMutex);
  sqlite3_mutex_leave(pCF->pMutex);

  return rc;
}

/*
** Internal implementation: Delete from column family
*/
static int kvstore_cf_delete_internal(
  KVColumnFamily *pCF,
  const void *pKey, int nKey
){
  BtCursor *pCur = NULL;
  int rc;
  int autoTrans = 0;
  KVStore *pKV = pCF->pKV;

  sqlite3_mutex_enter(pCF->pMutex);
  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_ERROR;
  }

  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot delete: database is corrupted");
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_CORRUPT;
  }

  if( !pKey || nKey <= 0 ){
    kvstoreSetError(pKV, "invalid key");
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_ERROR;
  }

  if( pKV->inTrans != 2 ){
    /* Release persistent read (if any) before starting a write. */
    if( pKV->inTrans == 1 ){
      sqlite3BtreeCommit(pKV->pBt);
      pKV->inTrans = 0;
    }
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 1, 0);
    if( rc != SQLITE_OK ){
      sqlite3_mutex_leave(pKV->pMutex);
      sqlite3_mutex_leave(pCF->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 2;
  }

  pCur = kvstoreAllocCursor();
  if( !pCur ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return SQLITE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pCF->iTable, 1, pKV->pKeyInfo, pCur);
  if( rc != SQLITE_OK ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return rc;
  }

  int found = 0;
  rc = kvstoreSeekKey(pCur, pKV->pKeyInfo, pKey, nKey, &found);
  if( rc != SQLITE_OK || !found ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return (rc == SQLITE_OK) ? KVSTORE_NOTFOUND : rc;
  }

  /* Pre-read TTL for cleanup (need expireMs to delete from expiry CF). */
  void *pOldTtl = NULL; int nOldTtl = 0;
  if( pCF->hasTtl && pCF->pTtlKeyCF ){
    kvstoreRawBtreeGet(pKV, pCF->pTtlKeyCF->iTable, pKey, nKey,
                       &pOldTtl, &nOldTtl);
  }

  /* Cursor is already positioned — delete the entry */
  rc = sqlite3BtreeDelete(pCur, 0);
  kvstoreFreeCursor(pCur);

  /* TTL cleanup — runs only when the data delete succeeded. */
  if( rc == SQLITE_OK && pOldTtl && nOldTtl == 8 ){
    unsigned char *pExpKey = (unsigned char*)sqlite3Malloc(8 + nKey);
    if( pExpKey ){
      memcpy(pExpKey, pOldTtl, 8);
      memcpy(pExpKey + 8, pKey, nKey);
      kvstoreRawBtreeDelete(pKV, pCF->pTtlExpiryCF->iTable, pExpKey, 8 + nKey);
      sqlite3_free(pExpKey);
    }
    kvstoreRawBtreeDelete(pKV, pCF->pTtlKeyCF->iTable, pKey, nKey);
  }
  if( pOldTtl ) sqlite3_free(pOldTtl);

  if( rc == SQLITE_OK ){
    pKV->stats.nDeletes++;
    if( autoTrans ){
      rc = sqlite3BtreeCommit(pKV->pBt);
      pKV->inTrans = 0;
      /* Restore persistent read transaction */
      if( rc == SQLITE_OK ){
        kvstoreAutoCheckpoint(pKV);
        if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ){
          pKV->inTrans = 1;
        }
      }
    }
  }else{
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
      pKV->inTrans = 0;
      /* Restore persistent read transaction */
      if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ){
        pKV->inTrans = 1;
      }
    }
  }

  sqlite3_mutex_leave(pKV->pMutex);
  sqlite3_mutex_leave(pCF->pMutex);

  return rc;
}

/*
** Internal implementation: Check existence in column family
*/
static int kvstore_cf_exists_internal(
  KVColumnFamily *pCF,
  const void *pKey, int nKey,
  int *pExists
){
  BtCursor *pCur = NULL;
  int rc;
  KVStore *pKV = pCF->pKV;

  if( !pExists ){
    sqlite3_mutex_enter(pKV->pMutex);
    kvstoreSetError(pKV, "invalid parameters to exists");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  *pExists = 0;

  sqlite3_mutex_enter(pCF->pMutex);
  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_ERROR;
  }

  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot check existence: database is corrupted");
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_CORRUPT;
  }

  if( !pKey || nKey <= 0 ){
    kvstoreSetError(pKV, "invalid key");
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_ERROR;
  }

  if( !pKV->inTrans ){
    /* Persistent read transaction normally keeps inTrans>=1; fall back if not. */
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
    if( rc != SQLITE_OK ){
      sqlite3_mutex_leave(pKV->pMutex);
      sqlite3_mutex_leave(pCF->pMutex);
      return rc;
    }
    pKV->inTrans = 1;
  }

  /* Use the cached read cursor — no malloc/free per call. */
  pCur = kvstoreGetReadCursor(pCF);
  if( !pCur ){
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return SQLITE_NOMEM;
  }

  int found = 0;
  rc = kvstoreSeekKey(pCur, pKV->pKeyInfo, pKey, nKey, &found);

  if( rc == SQLITE_OK ){
    *pExists = found;
  }

  sqlite3_mutex_leave(pKV->pMutex);
  sqlite3_mutex_leave(pCF->pMutex);

  return rc;
}

/*
** Public CF operations - just call internal versions
*/
int kvstore_cf_put(KVColumnFamily *pCF, const void *pKey, int nKey,
                   const void *pValue, int nValue){
  if( !pCF ) return KVSTORE_ERROR;
  return kvstore_cf_put_internal(pCF, pKey, nKey, pValue, nValue);
}

int kvstore_cf_get(KVColumnFamily *pCF, const void *pKey, int nKey,
                   void **ppValue, int *pnValue){
  if( !pCF ) return KVSTORE_ERROR;
  return kvstore_cf_get_internal(pCF, pKey, nKey, ppValue, pnValue);
}

int kvstore_cf_delete(KVColumnFamily *pCF, const void *pKey, int nKey){
  if( !pCF ) return KVSTORE_ERROR;
  return kvstore_cf_delete_internal(pCF, pKey, nKey);
}

int kvstore_cf_exists(KVColumnFamily *pCF, const void *pKey, int nKey, int *pExists){
  if( !pCF ) return KVSTORE_ERROR;
  return kvstore_cf_exists_internal(pCF, pKey, nKey, pExists);
}

/*
** Create an iterator for default CF
*/
int kvstore_iterator_create(KVStore *pKV, KVIterator **ppIter){
  if( !pKV || !pKV->pDefaultCF ){
    return KVSTORE_ERROR;
  }
  return kvstore_cf_iterator_create(pKV->pDefaultCF, ppIter);
}

/*
** Create an iterator for specific CF
*/
int kvstore_cf_iterator_create(KVColumnFamily *pCF, KVIterator **ppIter){
  KVIterator *pIter;
  int rc;
  KVStore *pKV;

  if( !pCF || !ppIter ){
    return KVSTORE_ERROR;
  }

  pKV = pCF->pKV;
  *ppIter = NULL;

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot create iterator: database is corrupted");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_CORRUPT;
  }

  pIter = (KVIterator*)sqlite3MallocZero(sizeof(KVIterator));
  if( !pIter ){
    kvstoreSetError(pKV, "out of memory allocating iterator");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }

  pIter->pCF = pCF;
  pCF->refCount++;  /* Hold reference to CF */
  pIter->eof = 1;
  pIter->ownsTrans = 0;
  pIter->isValid = 1;

  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
    if( rc != SQLITE_OK ){
      sqlite3_free(pIter);
      pCF->refCount--;
      sqlite3_mutex_leave(pKV->pMutex);
      return rc;
    }
    pIter->ownsTrans = 1;
    pKV->inTrans = 1;
  }

  pIter->pCur = kvstoreAllocCursor();
  if( !pIter->pCur ){
    if( pIter->ownsTrans ){
      sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
      pKV->inTrans = 0;
    }
    pCF->refCount--;
    sqlite3_free(pIter);
    sqlite3_mutex_leave(pKV->pMutex);
    return SQLITE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pCF->iTable, 0, pKV->pKeyInfo, pIter->pCur);
  if( rc != SQLITE_OK ){
    kvstoreCheckCorruption(pKV, rc);
    if( pIter->ownsTrans ){
      sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
      pKV->inTrans = 0;
    }
    pCF->refCount--;
    sqlite3_free(pIter->pCur);
    sqlite3_free(pIter);
    sqlite3_mutex_leave(pKV->pMutex);
    return rc;
  }

  pKV->stats.nIterations++;
  sqlite3_mutex_leave(pKV->pMutex);

  *ppIter = pIter;
  return KVSTORE_OK;
}

/*
** Create a prefix iterator for a specific column family.
** The iterator is positioned at the first key >= prefix.
** Subsequent calls to kvstore_iterator_next will stop when keys
** no longer start with the prefix.
*/
int kvstore_cf_prefix_iterator_create(
  KVColumnFamily *pCF,
  const void *pPrefix, int nPrefix,
  KVIterator **ppIter
){
  int rc;
  KVIterator *pIter;

  if( !pCF || !ppIter || !pPrefix || nPrefix <= 0 ){
    return KVSTORE_ERROR;
  }

  /* Create a normal iterator first */
  rc = kvstore_cf_iterator_create(pCF, ppIter);
  if( rc != KVSTORE_OK ) return rc;

  pIter = *ppIter;

  /* Store a copy of the prefix */
  pIter->pPrefix = sqlite3Malloc(nPrefix);
  if( !pIter->pPrefix ){
    kvstore_iterator_close(pIter);
    *ppIter = NULL;
    return KVSTORE_NOMEM;
  }
  memcpy(pIter->pPrefix, pPrefix, nPrefix);
  pIter->nPrefix = nPrefix;

  /* Position iterator at the first matching key */
  rc = kvstore_iterator_first(pIter);
  if( rc != KVSTORE_OK ){
    kvstore_iterator_close(pIter);
    *ppIter = NULL;
    return rc;
  }

  return KVSTORE_OK;
}

/*
** Create a prefix iterator for the default column family.
*/
int kvstore_prefix_iterator_create(
  KVStore *pKV,
  const void *pPrefix, int nPrefix,
  KVIterator **ppIter
){
  if( !pKV || !pKV->pDefaultCF ){
    return KVSTORE_ERROR;
  }
  return kvstore_cf_prefix_iterator_create(pKV->pDefaultCF, pPrefix, nPrefix, ppIter);
}

/*
** Check if the current cursor entry's key starts with the iterator's prefix.
** Returns 1 if match (or no prefix filter), 0 otherwise.
*/
static int kvstoreIterCheckPrefix(KVIterator *pIter){
  unsigned char hdr[4];
  int rc, keyLen;

  if( !pIter->pPrefix ) return 1;
  if( pIter->eof ) return 0;

  u32 payloadSz = sqlite3BtreePayloadSize(pIter->pCur);
  if( payloadSz < (u32)(4 + pIter->nPrefix) ) return 0;

  rc = sqlite3BtreePayload(pIter->pCur, 0, 4, hdr);
  if( rc != SQLITE_OK ) return 0;
  keyLen = (hdr[0]<<24)|(hdr[1]<<16)|(hdr[2]<<8)|hdr[3];
  if( keyLen < pIter->nPrefix ) return 0;

  /* Read just the prefix-length portion of the stored key and compare */
  {
    unsigned char stackBuf[256];
    void *pBuf = (pIter->nPrefix <= (int)sizeof(stackBuf))
                   ? stackBuf
                   : sqlite3Malloc(pIter->nPrefix);
    int match;
    if( !pBuf ) return 0;
    rc = sqlite3BtreePayload(pIter->pCur, 4, pIter->nPrefix, pBuf);
    match = (rc == SQLITE_OK && memcmp(pBuf, pIter->pPrefix, pIter->nPrefix) == 0);
    if( pBuf != stackBuf ) sqlite3_free(pBuf);
    return match;
  }
}

/*
** Move iterator to first entry
*/
int kvstore_iterator_first(KVIterator *pIter){
  int rc, res;

  if( !pIter || !pIter->isValid ){
    return KVSTORE_ERROR;
  }

  if( pIter->pPrefix ){
    /* Prefix iterator: seek to the first key >= prefix */
    KVStore *pKV = pIter->pCF->pKV;
    UnpackedRecord *pIdxKey = sqlite3VdbeAllocUnpackedRecord(pKV->pKeyInfo);
    if( !pIdxKey ) return SQLITE_NOMEM;

    pIdxKey->u.z       = (char *)pIter->pPrefix;
    pIdxKey->n         = pIter->nPrefix;
    pIdxKey->nField    = 1;
    pIdxKey->default_rc = 0;

    rc = sqlite3BtreeIndexMoveto(pIter->pCur, pIdxKey, &res);
    sqlite3DbFree(pKV->pKeyInfo->db, pIdxKey);
    if( rc != SQLITE_OK ) return rc;

    if( res < 0 ){
      /* Cursor is before the first matching key — advance */
      rc = sqlite3BtreeNext(pIter->pCur, 0);
      if( rc == SQLITE_DONE ){
        pIter->eof = 1;
        return SQLITE_OK;
      }
      if( rc != SQLITE_OK ) return rc;
    }
    pIter->eof = 0;
    if( !kvstoreIterCheckPrefix(pIter) ){
      pIter->eof = 1;
    }
  }else{
    rc = sqlite3BtreeFirst(pIter->pCur, &res);
    if( rc == SQLITE_OK ){
      pIter->eof = res;
    }
  }

  return rc;
}

/*
** Move iterator to next entry
*/
int kvstore_iterator_next(KVIterator *pIter){
  int rc;

  if( !pIter || !pIter->isValid ){
    return KVSTORE_ERROR;
  }

  if( pIter->eof ){
    return KVSTORE_OK;
  }

  rc = sqlite3BtreeNext(pIter->pCur, 0);
  if( rc == SQLITE_DONE ){
    pIter->eof = 1;
    rc = SQLITE_OK;
  }else if( rc == SQLITE_OK && pIter->pPrefix ){
    /* Check prefix bound — keys are sorted, so first mismatch means done */
    if( !kvstoreIterCheckPrefix(pIter) ){
      pIter->eof = 1;
    }
  }

  return rc;
}

/*
** Check if iterator is at end
*/
int kvstore_iterator_eof(KVIterator *pIter){
  if( !pIter || !pIter->isValid ){
    return 1;
  }
  return pIter->eof;
}

/*
** Get current key from iterator.
** Reads the encoded payload and extracts the key portion.
*/
int kvstore_iterator_key(KVIterator *pIter, void **ppKey, int *pnKey){
  int rc;

  if( !pIter || !pIter->isValid || !ppKey || !pnKey ){
    return KVSTORE_ERROR;
  }

  if( pIter->eof ){
    return KVSTORE_ERROR;
  }

  u32 payloadSz = sqlite3BtreePayloadSize(pIter->pCur);
  if( payloadSz < 4 ) return KVSTORE_CORRUPT;

  unsigned char hdr[4];
  rc = sqlite3BtreePayload(pIter->pCur, 0, 4, hdr);
  if( rc != SQLITE_OK ) return rc;

  int keyLen = (hdr[0]<<24)|(hdr[1]<<16)|(hdr[2]<<8)|hdr[3];
  if( keyLen <= 0 || 4 + keyLen > (int)payloadSz ) return KVSTORE_CORRUPT;

  if( pIter->nKeyBuf < keyLen ){
    void *pNew = sqlite3Realloc(pIter->pKeyBuf, keyLen);
    if( !pNew ) return KVSTORE_NOMEM;
    pIter->pKeyBuf = pNew;
    pIter->nKeyBuf = keyLen;
  }

  rc = sqlite3BtreePayload(pIter->pCur, 4, keyLen, pIter->pKeyBuf);
  if( rc != SQLITE_OK ) return rc;

  *ppKey = pIter->pKeyBuf;
  *pnKey = keyLen;

  return KVSTORE_OK;
}

/*
** Get current value from iterator.
** Reads the encoded payload and extracts the value portion.
*/
int kvstore_iterator_value(KVIterator *pIter, void **ppValue, int *pnValue){
  int rc;

  if( !pIter || !pIter->isValid || !ppValue || !pnValue ){
    return KVSTORE_ERROR;
  }

  if( pIter->eof ){
    return KVSTORE_ERROR;
  }

  u32 payloadSz = sqlite3BtreePayloadSize(pIter->pCur);
  if( payloadSz < 4 ) return KVSTORE_CORRUPT;

  unsigned char hdr[4];
  rc = sqlite3BtreePayload(pIter->pCur, 0, 4, hdr);
  if( rc != SQLITE_OK ) return rc;

  int keyLen = (hdr[0]<<24)|(hdr[1]<<16)|(hdr[2]<<8)|hdr[3];
  int valLen = (int)payloadSz - 4 - keyLen;
  if( valLen < 0 ) return KVSTORE_CORRUPT;

  if( valLen == 0 ){
    *ppValue = NULL;
    *pnValue = 0;
    return KVSTORE_OK;
  }

  if( pIter->nValBuf < valLen ){
    void *pNew = sqlite3Realloc(pIter->pValBuf, valLen);
    if( !pNew ) return KVSTORE_NOMEM;
    pIter->pValBuf = pNew;
    pIter->nValBuf = valLen;
  }

  rc = sqlite3BtreePayload(pIter->pCur, 4 + keyLen, valLen, pIter->pValBuf);
  if( rc != SQLITE_OK ) return rc;

  *ppValue = pIter->pValBuf;
  *pnValue = valLen;

  return KVSTORE_OK;
}

/*
** Close an iterator
*/
void kvstore_iterator_close(KVIterator *pIter){
  KVStore *pKV;

  if( !pIter ){
    return;
  }

  pIter->isValid = 0;
  pKV = pIter->pCF ? pIter->pCF->pKV : NULL;

  if( pIter->pCur ){
    kvstoreFreeCursor(pIter->pCur);
    pIter->pCur = NULL;
  }

  if( pIter->ownsTrans && pKV ){
    sqlite3_mutex_enter(pKV->pMutex);
    sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
    sqlite3_mutex_leave(pKV->pMutex);
  }

  if( pIter->pCF ){
    kvstore_cf_close(pIter->pCF);
  }

  if( pIter->pKeyBuf ){
    sqlite3_free(pIter->pKeyBuf);
  }

  if( pIter->pValBuf ){
    sqlite3_free(pIter->pValBuf);
  }

  if( pIter->pPrefix ){
    sqlite3_free(pIter->pPrefix);
  }

  sqlite3_free(pIter);
}

/*
** Get statistics
*/
int kvstore_stats(KVStore *pKV, KVStoreStats *pStats){
  if( !pKV || !pStats ){
    return KVSTORE_ERROR;
  }

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  pStats->nPuts = pKV->stats.nPuts;
  pStats->nGets = pKV->stats.nGets;
  pStats->nDeletes = pKV->stats.nDeletes;
  pStats->nIterations = pKV->stats.nIterations;
  pStats->nErrors = pKV->stats.nErrors;

  sqlite3_mutex_leave(pKV->pMutex);

  return KVSTORE_OK;
}

/*
** Perform database integrity check
*/
int kvstore_integrity_check(KVStore *pKV, char **pzErrMsg){
  int rc;
  Pgno *aRoot;
  int nRoot;
  int hadTrans = 0;
  int nErr = 0;
  char *zErr = NULL;

  if( !pKV ){
    return KVSTORE_ERROR;
  }

  if( pzErrMsg ){
    *pzErrMsg = NULL;
  }

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
    if( rc != SQLITE_OK ){
      sqlite3_mutex_leave(pKV->pMutex);
      return rc;
    }
    hadTrans = 1;
    pKV->inTrans = 1;
  }

  /* Build array of all root pages to check.
  ** Include page 1 (master/schema root), default CF root, metadata table root,
  ** plus every CF root page stored in the metadata table.  This prevents
  ** false "Page N: never used" warnings from the integrity checker.
  */
  {
    int nAlloc = 16;
    nRoot = 0;
    aRoot = (Pgno*)sqlite3MallocZero(nAlloc * sizeof(Pgno));
    if( !aRoot ){
      if( hadTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
      sqlite3_mutex_leave(pKV->pMutex);
      return KVSTORE_NOMEM;
    }

    /* Page 1 is always used (file header / master page) */
    aRoot[nRoot++] = 1;
    aRoot[nRoot++] = (Pgno)pKV->pDefaultCF->iTable;
    if( pKV->iMetaTable ){
      aRoot[nRoot++] = (Pgno)pKV->iMetaTable;
    }

    /* Scan metadata table to find all CF root pages */
    if( pKV->iMetaTable ){
      BtCursor *pCur = kvstoreAllocCursor();
      if( pCur ){
        rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 0, 0, pCur);
        if( rc == SQLITE_OK ){
          int res;
          rc = sqlite3BtreeFirst(pCur, &res);
          while( rc == SQLITE_OK && !res ){
            u32 payloadSz = sqlite3BtreePayloadSize(pCur);
            if( payloadSz >= 8 ){
              unsigned char hdr[4];
              rc = sqlite3BtreePayload(pCur, 0, 4, hdr);
              if( rc == SQLITE_OK ){
                int nameLen = (hdr[0]<<24)|(hdr[1]<<16)|(hdr[2]<<8)|hdr[3];
                int rootOff = 4 + nameLen;
                if( rootOff + 4 <= (int)payloadSz ){
                  unsigned char rootBytes[4];
                  rc = sqlite3BtreePayload(pCur, rootOff, 4, rootBytes);
                  if( rc == SQLITE_OK ){
                    Pgno cfRoot = (rootBytes[0]<<24)|(rootBytes[1]<<16)
                                 |(rootBytes[2]<<8)|rootBytes[3];
                    if( cfRoot > 0 ){
                      if( nRoot >= nAlloc ){
                        nAlloc *= 2;
                        Pgno *aNew = sqlite3Realloc(aRoot, nAlloc*sizeof(Pgno));
                        if( !aNew ){ rc = SQLITE_NOMEM; break; }
                        aRoot = aNew;
                      }
                      aRoot[nRoot++] = cfRoot;
                    }
                  }
                }
              }
            }
            rc = sqlite3BtreeNext(pCur, 0);
            if( rc == SQLITE_DONE ){ rc = SQLITE_OK; break; }
          }
        }
        kvstoreFreeCursor(pCur);
      }
    }
  }

  /* Run the integrity check */
  rc = sqlite3BtreeIntegrityCheck(
    pKV->db,
    pKV->pBt,
    aRoot,       /* root pages */
    0,           /* aCnt (entry count output – not needed) */
    nRoot,       /* number of roots */
    100,         /* max errors to report */
    &nErr,       /* OUT: error count */
    &zErr        /* OUT: error message */
  );

  sqlite3_free(aRoot);

  if( hadTrans ){
    sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
  }

  if( nErr > 0 && zErr ){
    pKV->isCorrupted = 1;
    kvstoreSetError(pKV, "integrity check failed: %s", zErr);
    if( pzErrMsg ){
      *pzErrMsg = zErr;
    }else{
      sqlite3_free(zErr);
    }
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_CORRUPT;
  }

  if( zErr ) sqlite3_free(zErr);

  sqlite3_mutex_leave(pKV->pMutex);
  return KVSTORE_OK;
}

/*
** Synchronize database to disk.
** In SQLite 3.51.200 there is no sqlite3BtreeSync.
** We do a commit+begin cycle to flush to disk if a write transaction
** is active, otherwise this is a no-op (reads are already consistent).
*/
int kvstore_sync(KVStore *pKV){
  int rc = SQLITE_OK;

  if( !pKV ){
    return KVSTORE_ERROR;
  }

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot sync: database is corrupted");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_CORRUPT;
  }

  if( pKV->inTrans == 2 ){
    /* Write transaction active – commit and re-open */
    rc = sqlite3BtreeCommit(pKV->pBt);
    if( rc == SQLITE_OK ){
      pKV->inTrans = 0;
    }else{
      kvstoreCheckCorruption(pKV, rc);
      kvstoreSetError(pKV, "failed to sync database: error %d", rc);
    }
  }

  sqlite3_mutex_leave(pKV->pMutex);
  return rc;
}

/*
** Run incremental vacuum, freeing up to nPage pages.
** Pass nPage=0 to free all unused pages.
*/
int kvstore_incremental_vacuum(KVStore *pKV, int nPage){
  int rc;

  if( !pKV ){
    return KVSTORE_ERROR;
  }

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot vacuum: database is corrupted");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_CORRUPT;
  }

  /* Begin a write transaction if none is active */
  int autoTrans = 0;
  if( pKV->inTrans == 0 ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 2, 0);
    if( rc != SQLITE_OK ){
      kvstoreCheckCorruption(pKV, rc);
      kvstoreSetError(pKV, "failed to begin transaction for vacuum: error %d", rc);
      sqlite3_mutex_leave(pKV->pMutex);
      return rc;
    }
    pKV->inTrans = 2;
    autoTrans = 1;
  }else if( pKV->inTrans == 1 ){
    /* Upgrade read transaction to write */
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 2, 0);
    if( rc != SQLITE_OK ){
      kvstoreCheckCorruption(pKV, rc);
      kvstoreSetError(pKV, "failed to upgrade transaction for vacuum: error %d", rc);
      sqlite3_mutex_leave(pKV->pMutex);
      return rc;
    }
    pKV->inTrans = 2;
    autoTrans = 1;
  }

  /* Run incremental vacuum steps */
  if( nPage <= 0 ){
    /* Free all unused pages */
    do {
      rc = sqlite3BtreeIncrVacuum(pKV->pBt);
    } while( rc == SQLITE_OK );
    if( rc == SQLITE_DONE ) rc = SQLITE_OK;
  }else{
    int i;
    for( i = 0; i < nPage; i++ ){
      rc = sqlite3BtreeIncrVacuum(pKV->pBt);
      if( rc != SQLITE_OK ) break;
    }
    if( rc == SQLITE_DONE ) rc = SQLITE_OK;
  }

  if( rc != SQLITE_OK ){
    kvstoreCheckCorruption(pKV, rc);
    kvstoreSetError(pKV, "incremental vacuum failed: error %d", rc);
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
      pKV->inTrans = 0;
    }
    sqlite3_mutex_leave(pKV->pMutex);
    return rc;
  }

  if( autoTrans ){
    rc = sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
    if( rc != SQLITE_OK ){
      kvstoreCheckCorruption(pKV, rc);
      kvstoreSetError(pKV, "failed to commit vacuum: error %d", rc);
    }
  }

  sqlite3_mutex_leave(pKV->pMutex);
  return rc;
}

/*
** List all column families
*/
int kvstore_cf_list(KVStore *pKV, char ***pazNames, int *pnCount){
  int rc;
  BtCursor *pCur = NULL;
  int autoTrans = 0;
  char **azNames = NULL;
  int nCount = 0;
  int nAlloc = 8;

  if( !pKV || !pazNames || !pnCount ){
    return KVSTORE_ERROR;
  }

  *pazNames = NULL;
  *pnCount = 0;

  azNames = (char**)sqlite3MallocZero(nAlloc * sizeof(char*));
  if( !azNames ){
    return KVSTORE_NOMEM;
  }

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_free(azNames);
    return KVSTORE_ERROR;
  }

  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
    if( rc != SQLITE_OK ){
      sqlite3_mutex_leave(pKV->pMutex);
      sqlite3_free(azNames);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 1;
  }

  pCur = kvstoreAllocCursor();
  if( !pCur ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_free(azNames);
    return SQLITE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 0, 0, pCur);
  if( rc != SQLITE_OK ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_free(azNames);
    return rc;
  }

  int res;
  rc = sqlite3BtreeFirst(pCur, &res);

  while( rc == SQLITE_OK && !res ){
    /* Read payload to extract name */
    u32 payloadSz = sqlite3BtreePayloadSize(pCur);
    if( payloadSz >= 4 ){
      unsigned char hdr[4];
      rc = sqlite3BtreePayload(pCur, 0, 4, hdr);
      if( rc != SQLITE_OK ) break;

      int nameLen = (hdr[0]<<24)|(hdr[1]<<16)|(hdr[2]<<8)|hdr[3];
      if( nameLen > 0 && 4 + nameLen <= (int)payloadSz ){
        char *name = (char*)sqlite3Malloc(nameLen + 1);
        if( !name ){ rc = KVSTORE_NOMEM; break; }

        rc = sqlite3BtreePayload(pCur, 4, nameLen, name);
        if( rc != SQLITE_OK ){ sqlite3_free(name); break; }
        name[nameLen] = '\0';

        /* Skip internal reserved CFs (names starting with "__"). */
        if( nameLen >= 2 && name[0] == '_' && name[1] == '_' ){
          sqlite3_free(name);
        } else {
          if( nCount >= nAlloc ){
            nAlloc *= 2;
            char **azNew = (char**)sqlite3Realloc(azNames, nAlloc * sizeof(char*));
            if( !azNew ){ sqlite3_free(name); rc = KVSTORE_NOMEM; break; }
            azNames = azNew;
          }
          azNames[nCount++] = name;
        }
      }
    }

    rc = sqlite3BtreeNext(pCur, 0);
    if( rc == SQLITE_DONE ){
      rc = SQLITE_OK;
      break;
    }
  }

  kvstoreFreeCursor(pCur);

  if( autoTrans ){
    sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
  }

  sqlite3_mutex_leave(pKV->pMutex);

  if( rc != KVSTORE_OK ){
    int i;
    for(i = 0; i < nCount; i++){
      sqlite3_free(azNames[i]);
    }
    sqlite3_free(azNames);
    return rc;
  }

  *pazNames = azNames;
  *pnCount = nCount;

  return KVSTORE_OK;
}

/*
** Drop a single hidden CF by name within an existing write transaction.
** Caller must hold pKV->pMutex and be in a write transaction (inTrans==2).
** Silently ignores KVSTORE_NOTFOUND.
*/
static void kvstoreDropHiddenCFNoLock(KVStore *pKV, const char *zName){
  int nNameLen = (int)strlen(zName);
  BtCursor *pCur = kvstoreAllocCursor();
  if( !pCur ) return;

  int rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 1, 0, pCur);
  if( rc != SQLITE_OK ){ kvstoreFreeCursor(pCur); return; }

  int found = 0;
  i64 foundRowid = 0;
  rc = kvstoreMetaSeekKey(pCur, zName, nNameLen, &found, &foundRowid);
  if( rc != SQLITE_OK || !found ){ kvstoreFreeCursor(pCur); return; }

  int res = 0;
  rc = sqlite3BtreeTableMoveto(pCur, foundRowid, 0, &res);
  if( rc != SQLITE_OK || res != 0 ){ kvstoreFreeCursor(pCur); return; }

  u32 payloadSz = sqlite3BtreePayloadSize(pCur);
  if( (int)payloadSz < 4 + nNameLen + 4 ){ kvstoreFreeCursor(pCur); return; }

  unsigned char tableRootBytes[4];
  rc = sqlite3BtreePayload(pCur, 4 + nNameLen, 4, tableRootBytes);
  if( rc != SQLITE_OK ){ kvstoreFreeCursor(pCur); return; }

  int iTable = (tableRootBytes[0]<<24)|(tableRootBytes[1]<<16)|
               (tableRootBytes[2]<<8)|tableRootBytes[3];

  sqlite3BtreeDelete(pCur, 0);
  kvstoreFreeCursor(pCur);

  int iMoved = 0;
  sqlite3BtreeDropTable(pKV->pBt, iTable, &iMoved);

  u32 cfCount = 0;
  sqlite3BtreeGetMeta(pKV->pBt, META_CF_COUNT, &cfCount);
  if( cfCount > 0 ) cfCount--;
  sqlite3BtreeUpdateMeta(pKV->pBt, META_CF_COUNT, cfCount);
}

/*
** Drop a column family
*/
int kvstore_cf_drop(KVStore *pKV, const char *zName){
  int rc;
  BtCursor *pCur = NULL;
  int autoTrans = 0;
  i64 nNameLen;
  u32 cfCount;
  int iTable;
  int iMoved = 0;

  if( !pKV || !zName ){
    if( pKV ){
      sqlite3_mutex_enter(pKV->pMutex);
      kvstoreSetError(pKV, "invalid parameters to cf_drop");
      sqlite3_mutex_leave(pKV->pMutex);
    }
    return KVSTORE_ERROR;
  }

  /* Cannot drop default CF */
  if( strcmp(zName, DEFAULT_CF_NAME) == 0 ){
    sqlite3_mutex_enter(pKV->pMutex);
    kvstoreSetError(pKV, "cannot drop default column family");
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  nNameLen = strlen(zName);

  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  /* Start write transaction (release persistent read first if active). */
  if( pKV->inTrans != 2 ){
    if( pKV->inTrans == 1 ){
      sqlite3BtreeCommit(pKV->pBt);
      pKV->inTrans = 0;
    }
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 1, 0);
    if( rc != SQLITE_OK ){
      sqlite3_mutex_leave(pKV->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 2;
  }

  /* Find CF in metadata table */
  pCur = kvstoreAllocCursor();
  if( !pCur ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return SQLITE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 1, 0, pCur);
  if( rc != SQLITE_OK ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return rc;
  }

  int found = 0;
  i64 foundRowid = 0;
  rc = kvstoreMetaSeekKey(pCur, zName, (int)nNameLen, &found, &foundRowid);
  if( rc != SQLITE_OK || !found ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return (rc == SQLITE_OK) ? KVSTORE_NOTFOUND : rc;
  }

  /* Read the table root from payload */
  {
    int res = 0;
    rc = sqlite3BtreeTableMoveto(pCur, foundRowid, 0, &res);
    if( rc != SQLITE_OK || res != 0 ){
      kvstoreFreeCursor(pCur);
      if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
      sqlite3_mutex_leave(pKV->pMutex);
      return KVSTORE_CORRUPT;
    }
  }

  u32 payloadSz = sqlite3BtreePayloadSize(pCur);
  if( (int)payloadSz < 4 + (int)nNameLen + 4 ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return KVSTORE_CORRUPT;
  }

  unsigned char tableRootBytes[4];
  rc = sqlite3BtreePayload(pCur, 4 + (int)nNameLen, 4, tableRootBytes);
  if( rc != SQLITE_OK ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    sqlite3_mutex_leave(pKV->pMutex);
    return rc;
  }

  iTable = (tableRootBytes[0] << 24) | (tableRootBytes[1] << 16) |
           (tableRootBytes[2] << 8) | tableRootBytes[3];

  /* Delete from metadata table */
  rc = sqlite3BtreeDelete(pCur, 0);
  kvstoreFreeCursor(pCur);
  pCur = NULL;

  if( rc != SQLITE_OK ){
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
      pKV->inTrans = 0;
      if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ) pKV->inTrans = 1;
    }
    sqlite3_mutex_leave(pKV->pMutex);
    return rc;
  }

  /* Drop the actual table */
  rc = sqlite3BtreeDropTable(pKV->pBt, iTable, &iMoved);
  if( rc != SQLITE_OK ){
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
      pKV->inTrans = 0;
      if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ) pKV->inTrans = 1;
    }
    kvstoreSetError(pKV, "failed to drop table: error %d", rc);
    sqlite3_mutex_leave(pKV->pMutex);
    return rc;
  }

  /* Drop hidden TTL index CFs if they exist (best-effort). */
  {
    char zKeyName[SNKV_TTL_NAME_BUFLEN], zExpName[SNKV_TTL_NAME_BUFLEN];
    snprintf(zKeyName, SNKV_TTL_NAME_BUFLEN, "%s%s", SNKV_TTL_KEY_PREFIX, zName);
    snprintf(zExpName, SNKV_TTL_NAME_BUFLEN, "%s%s", SNKV_TTL_EXP_PREFIX, zName);
    kvstoreDropHiddenCFNoLock(pKV, zKeyName);
    kvstoreDropHiddenCFNoLock(pKV, zExpName);
  }

  /* Update CF count */
  sqlite3BtreeGetMeta(pKV->pBt, META_CF_COUNT, &cfCount);
  if( cfCount > 0 ) cfCount--;
  sqlite3BtreeUpdateMeta(pKV->pBt, META_CF_COUNT, cfCount);

  if( autoTrans ){
    rc = sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
    if( rc == SQLITE_OK && sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ){
      pKV->inTrans = 1;
    }
  }

  sqlite3_mutex_leave(pKV->pMutex);

  return rc;
}

/* ======================================================================
** TTL public API — per-CF dual-index design
**
** Storage layout per user CF <X>:
**   __snkv_ttl_k__<X>:  user_key → 8-byte BE int64 expire_ms
**   __snkv_ttl_e__<X>:  [8-byte BE expire_ms][user_key] → empty
**
** The TTL index CFs for <X> are created lazily on the first
** kvstore_cf_put_ttl() call on CF <X>.  CFs that never use TTL pay
** zero overhead (hasTtl == 0, pTtlKeyCF == NULL, pTtlExpiryCF == NULL).
**
** The expiry CF's sort order (8-byte BE expire_ms prefix) lets
** kvstore_cf_purge_expired stop at the first unexpired entry — O(expired).
** ====================================================================== */

/*
** kvstore_cf_put_ttl — insert or update a key with an expiry time in CF pCF.
**   expire_ms > 0  — absolute expiry in ms since Unix epoch.
**   expire_ms == 0 — write key without TTL (removes any existing TTL entry).
** Both the data write and the TTL index writes are in one atomic transaction.
*/
int kvstore_cf_put_ttl(
  KVColumnFamily *pCF,
  const void *pKey, int nKey,
  const void *pValue, int nValue,
  int64_t expire_ms
){
  int rc, autoTrans = 0;
  if( !pCF || !pCF->pKV ) return KVSTORE_ERROR;
  KVStore *pKV = pCF->pKV;

  sqlite3_mutex_enter(pCF->pMutex);
  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex); sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_ERROR;
  }
  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot put_ttl: database is corrupted");
    sqlite3_mutex_leave(pKV->pMutex); sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_CORRUPT;
  }
  rc = kvstoreValidateKeyValue(pKV, pKey, nKey, pValue, nValue);
  if( rc != KVSTORE_OK ){
    sqlite3_mutex_leave(pKV->pMutex); sqlite3_mutex_leave(pCF->pMutex);
    return rc;
  }

  if( pKV->inTrans != 2 ){
    if( pKV->inTrans == 1 ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 1, 0);
    if( rc != SQLITE_OK ){
      sqlite3_mutex_leave(pKV->pMutex); sqlite3_mutex_leave(pCF->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 2;
  }

  /* Lazily create TTL index CFs (within the current write tx). */
  rc = kvstoreGetOrCreateTtlCFs(pCF);
  if( rc != KVSTORE_OK ){
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0;
      if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ) pKV->inTrans = 1;
    }
    sqlite3_mutex_leave(pKV->pMutex); sqlite3_mutex_leave(pCF->pMutex);
    return rc;
  }

  /* Write data. */
  rc = kvstoreRawBtreePut(pKV, pCF->iTable, pKey, nKey, pValue, nValue);
  if( rc == SQLITE_OK ){
    pKV->stats.nPuts++;

    /* Remove old TTL entries (need old expireMs to delete from expiry CF). */
    void *pOldTtl = NULL; int nOldTtl = 0;
    kvstoreRawBtreeGet(pKV, pCF->pTtlKeyCF->iTable,
                       pKey, nKey, &pOldTtl, &nOldTtl);
    if( pOldTtl && nOldTtl == 8 ){
      unsigned char *pExpKey = (unsigned char*)sqlite3Malloc(8 + nKey);
      if( pExpKey ){
        memcpy(pExpKey, pOldTtl, 8);
        memcpy(pExpKey + 8, pKey, nKey);
        kvstoreRawBtreeDelete(pKV, pCF->pTtlExpiryCF->iTable, pExpKey, 8 + nKey);
        sqlite3_free(pExpKey);
      }
    }
    if( pOldTtl ) sqlite3_free(pOldTtl);
    kvstoreRawBtreeDelete(pKV, pCF->pTtlKeyCF->iTable, pKey, nKey);

    if( expire_ms > 0 ){
      /* Write new key CF entry. */
      unsigned char ttlBuf[8];
      kvstoreEncodeBE64(ttlBuf, expire_ms);
      rc = kvstoreRawBtreePut(pKV, pCF->pTtlKeyCF->iTable, pKey, nKey, ttlBuf, 8);
      if( rc == SQLITE_OK ){
        /* Write new expiry CF entry: key=[8-byte expireMs][user_key], value=empty. */
        int nExpKey = 8 + nKey;
        unsigned char *pExpKey = (unsigned char*)sqlite3Malloc(nExpKey);
        if( !pExpKey ){
          rc = KVSTORE_NOMEM;
        } else {
          memcpy(pExpKey, ttlBuf, 8);
          memcpy(pExpKey + 8, pKey, nKey);
          rc = kvstoreRawBtreePut(pKV, pCF->pTtlExpiryCF->iTable,
                                  pExpKey, nExpKey, NULL, 0);
          sqlite3_free(pExpKey);
        }
      }
    }
    /* expire_ms == 0: TTL entries already cleared above — nothing more needed. */
  }

  if( autoTrans ){
    if( rc == SQLITE_OK ){
      rc = sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0;
      if( rc == SQLITE_OK ){
        kvstoreAutoCheckpoint(pKV);
        if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ) pKV->inTrans = 1;
      }
    } else {
      sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0;
      if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ) pKV->inTrans = 1;
    }
  }

  sqlite3_mutex_leave(pKV->pMutex);
  sqlite3_mutex_leave(pCF->pMutex);
  return rc;
}

int kvstore_put_ttl(
  KVStore *pKV,
  const void *pKey, int nKey,
  const void *pValue, int nValue,
  int64_t expire_ms
){
  if( !pKV || !pKV->pDefaultCF ) return KVSTORE_ERROR;
  return kvstore_cf_put_ttl(pKV->pDefaultCF, pKey, nKey, pValue, nValue, expire_ms);
}

/*
** kvstore_cf_get_ttl — retrieve value with lazy TTL expiry for CF pCF.
** If expired: deletes key+TTL entries, returns KVSTORE_NOTFOUND, *pnRemaining=0.
** If valid:   *ppValue/ *pnValue set; caller must snkv_free(*ppValue).
**             *pnRemaining = remaining ms, or KVSTORE_NO_TTL if no TTL.
** pnRemaining may be NULL.
*/
int kvstore_cf_get_ttl(
  KVColumnFamily *pCF,
  const void *pKey, int nKey,
  void **ppValue, int *pnValue,
  int64_t *pnRemaining
){
  int rc;
  if( !pCF || !pCF->pKV || !ppValue || !pnValue ) return KVSTORE_ERROR;
  *ppValue = NULL; *pnValue = 0;
  if( pnRemaining ) *pnRemaining = KVSTORE_NO_TTL;
  KVStore *pKV = pCF->pKV;

  sqlite3_mutex_enter(pCF->pMutex);
  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing ){
    sqlite3_mutex_leave(pKV->pMutex); sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_ERROR;
  }
  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot get_ttl: database is corrupted");
    sqlite3_mutex_leave(pKV->pMutex); sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_CORRUPT;
  }
  if( !pKey || nKey <= 0 ){
    kvstoreSetError(pKV, "invalid key");
    sqlite3_mutex_leave(pKV->pMutex); sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_ERROR;
  }

  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
    if( rc != SQLITE_OK ){
      sqlite3_mutex_leave(pKV->pMutex); sqlite3_mutex_leave(pCF->pMutex);
      return rc;
    }
    pKV->inTrans = 1;
  }

  /* Step 1: check TTL key CF (if hasTtl). */
  int64_t remaining = KVSTORE_NO_TTL;
  if( pCF->hasTtl && pCF->pTtlKeyCF ){
    void *pTtlVal = NULL; int nTtlVal = 0;
    if( kvstoreRawBtreeGet(pKV, pCF->pTtlKeyCF->iTable,
                            pKey, nKey, &pTtlVal, &nTtlVal) == SQLITE_OK
        && nTtlVal == 8 ){
      int64_t expireMs = kvstoreDecodeBE64((const unsigned char*)pTtlVal);
      sqlite3_free(pTtlVal);
      int64_t nowMs = kvstore_now_ms();
      if( nowMs >= expireMs ){
        /* Expired: lazy-delete from data CF, key CF, and expiry CF. */
        if( pCF->pReadCur ){ kvstoreFreeCursor(pCF->pReadCur); pCF->pReadCur = NULL; }
        if( pnRemaining ) *pnRemaining = 0;
        if( pKV->inTrans == 1 ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
        if( sqlite3BtreeBeginTrans(pKV->pBt, 1, 0) == SQLITE_OK ){
          pKV->inTrans = 2;
          kvstoreRawBtreeDelete(pKV, pCF->iTable, pKey, nKey);
          kvstoreRawBtreeDelete(pKV, pCF->pTtlKeyCF->iTable, pKey, nKey);
          unsigned char expBuf[8]; kvstoreEncodeBE64(expBuf, expireMs);
          unsigned char *pExpKey = (unsigned char*)sqlite3Malloc(8 + nKey);
          if( pExpKey ){
            memcpy(pExpKey, expBuf, 8);
            memcpy(pExpKey + 8, pKey, nKey);
            kvstoreRawBtreeDelete(pKV, pCF->pTtlExpiryCF->iTable, pExpKey, 8 + nKey);
            sqlite3_free(pExpKey);
          }
          sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0;
          kvstoreAutoCheckpoint(pKV);
          if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ) pKV->inTrans = 1;
        }
        sqlite3_mutex_leave(pKV->pMutex); sqlite3_mutex_leave(pCF->pMutex);
        return KVSTORE_NOTFOUND;
      }
      remaining = expireMs - nowMs;
    } else {
      if( pTtlVal ) sqlite3_free(pTtlVal);
    }
  }

  /* Step 2: read value from data CF. */
  void *pValue = NULL; int nValue = 0;
  rc = kvstoreRawBtreeGet(pKV, pCF->iTable, pKey, nKey, &pValue, &nValue);
  if( rc != SQLITE_OK ){
    sqlite3_mutex_leave(pKV->pMutex); sqlite3_mutex_leave(pCF->pMutex);
    return rc;
  }
  pKV->stats.nGets++;
  *ppValue = pValue; *pnValue = nValue;
  if( pnRemaining ) *pnRemaining = remaining;

  sqlite3_mutex_leave(pKV->pMutex);
  sqlite3_mutex_leave(pCF->pMutex);
  return KVSTORE_OK;
}

int kvstore_get_ttl(
  KVStore *pKV,
  const void *pKey, int nKey,
  void **ppValue, int *pnValue,
  int64_t *pnRemaining
){
  if( !pKV || !pKV->pDefaultCF ) return KVSTORE_ERROR;
  return kvstore_cf_get_ttl(pKV->pDefaultCF, pKey, nKey, ppValue, pnValue, pnRemaining);
}

/*
** kvstore_cf_ttl_remaining — return remaining TTL in ms for key in CF pCF.
**   KVSTORE_NO_TTL (-1) — key exists, no expiry.
**   0                   — key just expired (lazy delete performed).
**   N > 0               — N ms remain.
** Returns KVSTORE_NOTFOUND if the key does not exist.
*/
int kvstore_cf_ttl_remaining(
  KVColumnFamily *pCF,
  const void *pKey, int nKey,
  int64_t *pnRemaining
){
  void *pVal = NULL; int nVal = 0;
  if( !pnRemaining ) return KVSTORE_ERROR;
  *pnRemaining = KVSTORE_NO_TTL;
  int rc = kvstore_cf_get_ttl(pCF, pKey, nKey, &pVal, &nVal, pnRemaining);
  if( pVal ) sqlite3_free(pVal);
  /* "just expired": get_ttl sets *pnRemaining=0 then returns NOTFOUND.
  ** "truly absent":  *pnRemaining stays KVSTORE_NO_TTL, also returns NOTFOUND.
  ** Return OK with remaining=0 so callers can distinguish the two cases. */
  if( rc == KVSTORE_NOTFOUND && *pnRemaining == 0 ){
    return KVSTORE_OK;
  }
  return rc;
}

int kvstore_ttl_remaining(
  KVStore *pKV,
  const void *pKey, int nKey,
  int64_t *pnRemaining
){
  if( !pKV || !pKV->pDefaultCF ) return KVSTORE_ERROR;
  return kvstore_cf_ttl_remaining(pKV->pDefaultCF, pKey, nKey, pnRemaining);
}

/*
** kvstore_cf_purge_expired — scan the expiry index CF and delete all
** expired entries.  Uses the 8-byte BE expire_ms prefix sort order to
** stop at the first unexpired entry — O(expired keys), not O(all TTL keys).
** *pnDeleted (may be NULL) — set to number of data keys deleted.
*/
int kvstore_cf_purge_expired(KVColumnFamily *pCF, int *pnDeleted){
  int rc = SQLITE_OK;
  int nDeleted = 0;
  int nExpired = 0, nAlloc = 32;
  void **apExpKeys = NULL;  /* full composite keys [expireMs][userKey] */
  int  *anExpKeys  = NULL;
  BtCursor *pCur   = NULL;
  int autoTrans    = 0;

  if( !pCF || !pCF->pKV ) return KVSTORE_ERROR;
  if( pnDeleted ) *pnDeleted = 0;
  KVStore *pKV = pCF->pKV;

  sqlite3_mutex_enter(pCF->pMutex);
  sqlite3_mutex_enter(pKV->pMutex);

  if( pKV->closing || !pCF->hasTtl || !pCF->pTtlExpiryCF ){
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_OK;
  }
  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot purge_expired: database is corrupted");
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_leave(pCF->pMutex);
    return KVSTORE_CORRUPT;
  }

  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
    if( rc != SQLITE_OK ){
      sqlite3_mutex_leave(pKV->pMutex);
      sqlite3_mutex_leave(pCF->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 1;
  }

  /* Pass 1 — scan expiry CF (sorted by 8-byte BE expire_ms prefix).
  ** Entries with expire_ms <= nowMs come first; stop on first unexpired. */
  int64_t nowMs = kvstore_now_ms();

  apExpKeys = (void**)sqlite3MallocZero(nAlloc * sizeof(void*));
  anExpKeys = (int*)  sqlite3MallocZero(nAlloc * sizeof(int));
  if( !apExpKeys || !anExpKeys ){ rc = SQLITE_NOMEM; goto purge_cleanup; }

  pCur = kvstoreAllocCursor();
  if( !pCur ){ rc = SQLITE_NOMEM; goto purge_cleanup; }

  rc = sqlite3BtreeCursor(pKV->pBt, pCF->pTtlExpiryCF->iTable,
                           0, pKV->pKeyInfo, pCur);
  if( rc != SQLITE_OK ) goto purge_cleanup;

  {
    int res = 0;
    rc = sqlite3BtreeFirst(pCur, &res);
    while( rc == SQLITE_OK && !res ){
      u32 payloadSz = sqlite3BtreePayloadSize(pCur);
      /* Expiry CF cell: [4-byte key_len BE][8-byte BE expireMs][user_key][empty value]
      ** key_len = 8 + len(user_key) >= 8. */
      if( (int)payloadSz >= 4 + 8 ){
        unsigned char hdr[4];
        rc = sqlite3BtreePayload(pCur, 0, 4, hdr);
        if( rc != SQLITE_OK ) break;
        int storedKeyLen = (hdr[0]<<24)|(hdr[1]<<16)|(hdr[2]<<8)|hdr[3];
        if( storedKeyLen >= 8 ){
          /* Read just the first 8 bytes (expireMs). */
          unsigned char timeBuf[8];
          rc = sqlite3BtreePayload(pCur, 4, 8, timeBuf);
          if( rc != SQLITE_OK ) break;
          int64_t expireMs = kvstoreDecodeBE64(timeBuf);
          if( nowMs < expireMs ) break;  /* all remaining entries are unexpired */

          /* Collect the full composite key [expireMs(8)][user_key]. */
          void *pExpKey = sqlite3Malloc(storedKeyLen);
          if( !pExpKey ){ rc = SQLITE_NOMEM; break; }
          rc = sqlite3BtreePayload(pCur, 4, storedKeyLen, pExpKey);
          if( rc != SQLITE_OK ){ sqlite3_free(pExpKey); break; }

          if( nExpired >= nAlloc ){
            nAlloc *= 2;
            void **apN = (void**)sqlite3Realloc(apExpKeys, nAlloc * sizeof(void*));
            int   *anN = (int*)  sqlite3Realloc(anExpKeys,  nAlloc * sizeof(int));
            if( !apN || !anN ){
              sqlite3_free(pExpKey);
              if( apN ) apExpKeys = apN;
              if( anN ) anExpKeys = anN;
              rc = SQLITE_NOMEM; break;
            }
            apExpKeys = apN; anExpKeys = anN;
          }
          apExpKeys[nExpired] = pExpKey;
          anExpKeys[nExpired] = storedKeyLen;
          nExpired++;
        }
      }
      rc = sqlite3BtreeNext(pCur, 0);
      if( rc == SQLITE_DONE ){ rc = SQLITE_OK; break; }
    }
  }

purge_cleanup:
  kvstoreFreeCursor(pCur);
  pCur = NULL;

  if( autoTrans ){
    sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0;
    autoTrans = 0;
  }

  /* Pass 2 — delete collected keys in one write transaction. */
  if( rc == SQLITE_OK && nExpired > 0 ){
    if( pCF->pReadCur ){ kvstoreFreeCursor(pCF->pReadCur); pCF->pReadCur = NULL; }
    if( pKV->inTrans == 1 ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 1, 0);
    if( rc == SQLITE_OK ){
      int i;
      pKV->inTrans = 2;
      for( i = 0; i < nExpired; i++ ){
        /* apExpKeys[i] = [8-byte expireMs][user_key], length anExpKeys[i] */
        int nUserKey   = anExpKeys[i] - 8;
        const void *pUserKey = (const char*)apExpKeys[i] + 8;
        /* Delete from data CF (ignore NOTFOUND — might already be gone). */
        int rcd = kvstoreRawBtreeDelete(pKV, pCF->iTable, pUserKey, nUserKey);
        if( rcd == SQLITE_OK ) nDeleted++;
        /* Delete from key CF. */
        kvstoreRawBtreeDelete(pKV, pCF->pTtlKeyCF->iTable, pUserKey, nUserKey);
        /* Delete from expiry CF. */
        kvstoreRawBtreeDelete(pKV, pCF->pTtlExpiryCF->iTable,
                              apExpKeys[i], anExpKeys[i]);
      }
      rc = sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0;
      if( rc == SQLITE_OK ){
        kvstoreAutoCheckpoint(pKV);
        if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ) pKV->inTrans = 1;
      } else {
        sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0;
        if( sqlite3BtreeBeginTrans(pKV->pBt, 0, 0) == SQLITE_OK ) pKV->inTrans = 1;
      }
    }
  }

  /* Free collected composite keys. */
  if( apExpKeys ){
    int i;
    for( i = 0; i < nExpired; i++ ) sqlite3_free(apExpKeys[i]);
    sqlite3_free(apExpKeys);
  }
  sqlite3_free(anExpKeys);

  sqlite3_mutex_leave(pKV->pMutex);
  sqlite3_mutex_leave(pCF->pMutex);
  if( rc == SQLITE_OK && pnDeleted ) *pnDeleted = nDeleted;
  return (rc == SQLITE_OK) ? KVSTORE_OK : rc;
}

int kvstore_purge_expired(KVStore *pKV, int *pnDeleted){
  if( !pKV || !pKV->pDefaultCF ) return KVSTORE_ERROR;
  return kvstore_cf_purge_expired(pKV->pDefaultCF, pnDeleted);
}

/* ===== SNKV compatibility functions ===== */

int sqlite3CorruptError(int lineno){
  sqlite3_log(SQLITE_CORRUPT,
    "database corruption at line %d of [%.10s]",
    lineno, SQLITE_SOURCE_ID);
  return SQLITE_CORRUPT;
}

int sqlite3CantopenError(int lineno){
  sqlite3_log(SQLITE_CANTOPEN,
    "cannot open file at line %d of [%.10s]",
    lineno, SQLITE_SOURCE_ID);
  return SQLITE_CANTOPEN;
}

int sqlite3MisuseError(int lineno){
  sqlite3_log(SQLITE_MISUSE,
    "misuse at line %d of [%.10s]",
    lineno, SQLITE_SOURCE_ID);
  return SQLITE_MISUSE;
}

#if defined(SQLITE_DEBUG) || defined(SQLITE_ENABLE_CORRUPT_PGNO)
int sqlite3CorruptPgnoError(int lineno, Pgno pgno){
  sqlite3_log(SQLITE_CORRUPT,
    "database corruption page %u at line %d of [%.10s]",
    pgno, lineno, SQLITE_SOURCE_ID);
  return SQLITE_CORRUPT;
}
#endif

#ifdef SQLITE_DEBUG
int sqlite3NomemError(int lineno){
  return SQLITE_NOMEM;
}
int sqlite3IoerrnomemError(int lineno){
  return SQLITE_IOERR_NOMEM;
}
#endif

/*
** Minimal sqlite3_initialize – sets up mutexes, memory allocator,
** page cache, and OS (VFS).  Skips SQL-layer registration.
*/
int sqlite3_initialize(void){
  int rc;

  if( sqlite3GlobalConfig.isInit ){
    return SQLITE_OK;
  }
  if( sqlite3GlobalConfig.inProgress ){
    return SQLITE_OK;
  }
  sqlite3GlobalConfig.inProgress = 1;

  rc = sqlite3MutexInit();
  if( rc ) goto done;

  sqlite3GlobalConfig.isMutexInit = 1;
  if( !sqlite3GlobalConfig.isMallocInit ){
    rc = sqlite3MallocInit();
    if( rc ) goto done;
    sqlite3GlobalConfig.isMallocInit = 1;
  }

  if( !sqlite3GlobalConfig.isPCacheInit ){
    rc = sqlite3PcacheInitialize();
    if( rc ) goto done;
    sqlite3GlobalConfig.isPCacheInit = 1;
  }

  rc = sqlite3OsInit();
  if( rc ) goto done;

  sqlite3PCacheBufferSetup(
    sqlite3GlobalConfig.pPage,
    sqlite3GlobalConfig.szPage,
    sqlite3GlobalConfig.nPage
  );

  sqlite3MemoryBarrier();
  sqlite3GlobalConfig.isInit = 1;

done:
  sqlite3GlobalConfig.inProgress = 0;
  return rc;
}

/*
** Minimal sqlite3_config – only handle the most common options.
*/
int sqlite3_config(int op, ...){
  int rc = SQLITE_OK;
  if( sqlite3GlobalConfig.isInit ) return SQLITE_MISUSE_BKPT;

  va_list ap;
  va_start(ap, op);
  switch( op ){
    case SQLITE_CONFIG_SINGLETHREAD:
      sqlite3GlobalConfig.bCoreMutex = 0;
      sqlite3GlobalConfig.bFullMutex = 0;
      break;
    case SQLITE_CONFIG_MULTITHREAD:
      sqlite3GlobalConfig.bCoreMutex = 1;
      sqlite3GlobalConfig.bFullMutex = 0;
      break;
    case SQLITE_CONFIG_SERIALIZED:
      sqlite3GlobalConfig.bCoreMutex = 1;
      sqlite3GlobalConfig.bFullMutex = 1;
      break;
    case SQLITE_CONFIG_MEMSTATUS:
      sqlite3GlobalConfig.bMemstat = va_arg(ap, int);
      break;
    case SQLITE_CONFIG_SMALL_MALLOC:
      sqlite3GlobalConfig.bSmallMalloc = va_arg(ap, int);
      break;
    case SQLITE_CONFIG_MALLOC: {
      sqlite3_mem_methods *p = va_arg(ap, sqlite3_mem_methods*);
      if( p ) sqlite3GlobalConfig.m = *p;
      break;
    }
    case SQLITE_CONFIG_GETMALLOC: {
      sqlite3_mem_methods *p = va_arg(ap, sqlite3_mem_methods*);
      if( p ) *p = sqlite3GlobalConfig.m;
      break;
    }
    case SQLITE_CONFIG_MUTEX: {
      sqlite3_mutex_methods *p = va_arg(ap, sqlite3_mutex_methods*);
      if( p ) sqlite3GlobalConfig.mutex = *p;
      break;
    }
    case SQLITE_CONFIG_GETMUTEX: {
      sqlite3_mutex_methods *p = va_arg(ap, sqlite3_mutex_methods*);
      if( p ) *p = sqlite3GlobalConfig.mutex;
      break;
    }
    case SQLITE_CONFIG_PCACHE2: {
      sqlite3_pcache_methods2 *p = va_arg(ap, sqlite3_pcache_methods2*);
      if( p ) sqlite3GlobalConfig.pcache2 = *p;
      break;
    }
    case SQLITE_CONFIG_GETPCACHE2: {
      sqlite3_pcache_methods2 *p = va_arg(ap, sqlite3_pcache_methods2*);
      if( p ) *p = sqlite3GlobalConfig.pcache2;
      break;
    }
    default:
      rc = SQLITE_OK;  /* Silently ignore unknown options */
      break;
  }
  va_end(ap);
  return rc;
}
