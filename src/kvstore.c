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
** Architecture note (3.51.200 migration):
**   SQLite 3.51.200 only supports BTREE_INTKEY (integer key + data) or
**   BTREE_BLOBKEY (blob key, no data).  The old flags=0 mode (blob key +
**   separate data) no longer exists.
**
**   We use BTREE_INTKEY tables.  For each key-value pair:
**     rowid  = FNV-1a 64-bit hash of the blob key
**     data   = [key_len (4 bytes BE) | key_bytes | value_bytes]
**   Lookups hash the key, then verify the stored key matches.
**   Collisions are handled with linear probing (hash+1, hash+2, ...).
*/

#include "kvstore.h"
#include "pager.h"
#include <string.h>
#include <assert.h>
#include <stdio.h>

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

/* Maximum number of linear probes on hash collision */
#ifndef KVSTORE_MAX_COLLISION_PROBES
# define KVSTORE_MAX_COLLISION_PROBES 64
#endif

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

/* ======================================================================
** FNV-1a 64-bit hash used to map blob keys → integer rowids
** ====================================================================== */
static i64 kvstoreHashKey(const void *pKey, int nKey){
  const unsigned char *p = (const unsigned char *)pKey;
  u64 hash = (u64)14695981039346656037ULL;
  int i;
  for(i = 0; i < nKey; i++){
    hash ^= (u64)p[i];
    hash *= (u64)1099511628211ULL;
  }
  /* Ensure positive i64 and non-zero */
  hash &= 0x7FFFFFFFFFFFFFFFULL;
  if( hash == 0 ) hash = 1;
  return (i64)hash;
}

/* ======================================================================
** Encode / decode helpers for the data payload stored with each rowid.
**   Format:  [key_len (4 bytes BE)] [key_data] [value_data]
** ====================================================================== */
static void *kvstoreEncodeData(
  const void *pKey, int nKey,
  const void *pVal, int nVal,
  int *pnTotal
){
  int nTotal = 4 + nKey + nVal;
  unsigned char *pOut = (unsigned char *)sqlite3Malloc(nTotal);
  if( pOut == 0 ) return 0;
  pOut[0] = (unsigned char)((nKey >> 24) & 0xFF);
  pOut[1] = (unsigned char)((nKey >> 16) & 0xFF);
  pOut[2] = (unsigned char)((nKey >>  8) & 0xFF);
  pOut[3] = (unsigned char)( nKey        & 0xFF);
  memcpy(pOut + 4, pKey, nKey);
  if( nVal > 0 && pVal ){
    memcpy(pOut + 4 + nKey, pVal, nVal);
  }
  *pnTotal = nTotal;
  return pOut;
}

/* Extract key length from encoded data.  Returns -1 on error. */
static int kvstoreDecodeKeyLen(const void *pData, int nData){
  const unsigned char *d = (const unsigned char *)pData;
  if( nData < 4 ) return -1;
  return (d[0]<<24) | (d[1]<<16) | (d[2]<<8) | d[3];
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

/*
** Column Family structure
*/
struct KVColumnFamily {
  KVStore *pKV;      /* Parent store */
  char *zName;       /* Column family name */
  int iTable;        /* Root page of this CF's btree */
  int refCount;      /* Reference count */
  kvstore_mutex *pMutex; /* Mutex for this CF */
};

/*
** KVStore structure - represents an open key-value store
*/
struct KVStore {
  Btree *pBt;        /* Underlying btree handle */
  sqlite3 *db;       /* Database connection (required by btree) */
  int iMetaTable;    /* Root page of CF metadata table */
  int inTrans;       /* Transaction state: 0=none, 1=read, 2=write */
  int isCorrupted;   /* Set if database corruption is detected */
  char *zErrMsg;     /* Last error message */

  /* Thread safety */
  kvstore_mutex *pMutex;  /* Main mutex protecting store operations */

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
};

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

  kvstore_mutex_enter(pKV->pMutex);
  if( pKV->zErrMsg ){
    zMsg = pKV->zErrMsg;
  }else{
    zMsg = "no error";
  }
  kvstore_mutex_leave(pKV->pMutex);

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
** Seek helpers for INTKEY tables with hashed blob keys.
**
**   kvstoreSeekKey  – positions cursor on the entry whose stored key
**                     matches (pKey, nKey).  Uses linear probing on
**                     hash collisions.
**                     Returns SQLITE_OK and sets *pFound=1 if found.
**
**   kvstoreSeekSlot – finds the first empty rowid slot starting at
**                     the hash of (pKey, nKey).  Used for insertion
**                     when the key does NOT yet exist.
** ====================================================================== */
static int kvstoreSeekKey(
  BtCursor *pCur,
  const void *pKey, int nKey,
  int *pFound,
  i64 *pRowid         /* OUT: rowid where key was found */
){
  i64 baseHash = kvstoreHashKey(pKey, nKey);
  int probe;
  *pFound = 0;

  for(probe = 0; probe < KVSTORE_MAX_COLLISION_PROBES; probe++){
    i64 tryRowid = baseHash + probe;
    int res = 0;
    int rc = sqlite3BtreeTableMoveto(pCur, tryRowid, 0, &res);
    if( rc != SQLITE_OK ) return rc;
    if( res != 0 ){
      /* No entry at this rowid → key does not exist */
      return SQLITE_OK;
    }
    /* Entry exists – verify stored key */
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
        if( rc != SQLITE_OK ){
          sqlite3_free(storedKey);
          return rc;
        }
        int match = (memcmp(storedKey, pKey, nKey) == 0);
        sqlite3_free(storedKey);
        if( match ){
          *pFound = 1;
          if( pRowid ) *pRowid = tryRowid;
          return SQLITE_OK;
        }
      }
    }
    /* Key at this slot doesn't match – continue probing */
  }
  /* Exhausted probes without finding key */
  return SQLITE_OK;
}

/* Find an empty slot for inserting a new key (starting from its hash) */
static int kvstoreFindSlot(
  BtCursor *pCur,
  const void *pKey, int nKey,
  i64 *pRowid          /* OUT: rowid of the empty slot */
){
  i64 baseHash = kvstoreHashKey(pKey, nKey);
  int probe;

  for(probe = 0; probe < KVSTORE_MAX_COLLISION_PROBES; probe++){
    i64 tryRowid = baseHash + probe;
    int res = 0;
    int rc = sqlite3BtreeTableMoveto(pCur, tryRowid, 0, &res);
    if( rc != SQLITE_OK ) return rc;
    if( res != 0 ){
      /* Empty slot */
      *pRowid = tryRowid;
      return SQLITE_OK;
    }
    /* Slot occupied – check if it's our key (update case handled by caller) */
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

  /* Create default CF table (INTKEY) */
  rc = sqlite3BtreeCreateTable(pKV->pBt, &pgno, BTREE_INTKEY);
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
  pCF->pMutex = kvstore_mutex_alloc();
  if( !pCF->pMutex ){
    sqlite3_free(pCF->zName);
    sqlite3_free(pCF);
    return KVSTORE_NOMEM;
  }

  pKV->pDefaultCF = pCF;
  return KVSTORE_OK;
}

/*
** Open a key-value store database file.
*/
int kvstore_open(
  const char *zFilename,
  KVStore **ppKV,
  int flags,
  int journalMode
){
  KVStore *pKV;
  int rc;

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

  /* Create main mutex first */
  pKV->pMutex = kvstore_mutex_alloc();
  if( !pKV->pMutex ){
    sqlite3_free(pKV);
    return KVSTORE_NOMEM;
  }

  /* Create a minimal sqlite3 structure (required by btree) */
  pKV->db = (sqlite3*)sqlite3MallocZero(sizeof(sqlite3));
  if( pKV->db == NULL ){
    kvstore_mutex_free(pKV->pMutex);
    sqlite3_free(pKV);
    return KVSTORE_NOMEM;
  }
  pKV->db->mutex = sqlite3MutexAlloc(SQLITE_MUTEX_RECURSIVE);
  pKV->db->aLimit[SQLITE_LIMIT_LENGTH] = SQLITE_MAX_LENGTH;

  /* Open the btree (new signature: pVfs, filename, db, ppBt, flags, vfsFlags) */
  rc = sqlite3BtreeOpen(
    sqlite3_vfs_find(0),       /* default VFS */
    zFilename,
    pKV->db,
    &pKV->pBt,
    flags,                     /* btree flags */
    SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB
  );
  if( rc != SQLITE_OK ){
    kvstoreSetError(pKV, "failed to open btree: error %d", rc);
    sqlite3_mutex_free(pKV->db->mutex);
    sqlite3_free(pKV->db);
    kvstore_mutex_free(pKV->pMutex);
    sqlite3_free(pKV);
    return rc;
  }

  /* Set cache size for better performance */
  sqlite3BtreeSetCacheSize(pKV->pBt, KVSTORE_DEFAULT_CACHE_SIZE);

  /* Set journal mode (rollback-delete or WAL).
  ** sqlite3BtreeSetVersion(pBt, 2) writes the WAL format marker into the
  ** database header (bytes 18-19).  On the next transaction the pager will
  ** detect this and open the WAL file automatically.
  ** sqlite3BtreeSetVersion(pBt, 1) reverts to rollback-journal mode.
  */
  {
    int ver = (journalMode == KVSTORE_JOURNAL_WAL) ? 2 : 1;
    rc = sqlite3BtreeSetVersion(pKV->pBt, ver);
    if( rc == SQLITE_OK ){
      /* SetVersion leaves a transaction open – commit it */
      rc = sqlite3BtreeCommit(pKV->pBt);
    }
    if( rc != SQLITE_OK ){
      kvstoreSetError(pKV, "failed to set journal mode: error %d", rc);
      sqlite3BtreeClose(pKV->pBt);
      sqlite3_mutex_free(pKV->db->mutex);
      sqlite3_free(pKV->db);
      kvstore_mutex_free(pKV->pMutex);
      sqlite3_free(pKV);
      return rc;
    }
  }

  /* Check if this is a new database or existing one */
  u32 defaultCFRoot = 0;
  u32 cfMetaRoot = 0;
  int needsInit = 0;

  /* Begin a read transaction so BtreeGetMeta can access page 1 */
  rc = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
  if( rc != SQLITE_OK ){
    kvstoreSetError(pKV, "failed to begin read transaction: error %d", rc);
    sqlite3BtreeClose(pKV->pBt);
    sqlite3_mutex_free(pKV->db->mutex);
    sqlite3_free(pKV->db);
    kvstore_mutex_free(pKV->pMutex);
    sqlite3_free(pKV);
    return rc;
  }

  /* Try to read existing metadata */
  sqlite3BtreeGetMeta(pKV->pBt, META_DEFAULT_CF_ROOT, &defaultCFRoot);
  sqlite3BtreeGetMeta(pKV->pBt, META_CF_METADATA_ROOT, &cfMetaRoot);

  sqlite3BtreeCommit(pKV->pBt);

  if( defaultCFRoot == 0 ){
    needsInit = 1;
  }

  if( needsInit ){
    /* New database - initialize */
    rc = createDefaultCF(pKV);
    if( rc != SQLITE_OK ){
      sqlite3BtreeClose(pKV->pBt);
      sqlite3_mutex_free(pKV->db->mutex);
      sqlite3_free(pKV->db);
      kvstore_mutex_free(pKV->pMutex);
      sqlite3_free(pKV);
      return rc;
    }

    rc = initCFMetadataTable(pKV);
    if( rc != SQLITE_OK ){
      if( pKV->pDefaultCF ){
        if( pKV->pDefaultCF->pMutex ) kvstore_mutex_free(pKV->pDefaultCF->pMutex);
        sqlite3_free(pKV->pDefaultCF->zName);
        sqlite3_free(pKV->pDefaultCF);
      }
      sqlite3BtreeClose(pKV->pBt);
      sqlite3_mutex_free(pKV->db->mutex);
      sqlite3_free(pKV->db);
      kvstore_mutex_free(pKV->pMutex);
      sqlite3_free(pKV);
      return rc;
    }
  }else{
    /* Existing database - open default CF */
    KVColumnFamily *pCF = (KVColumnFamily*)sqlite3MallocZero(sizeof(KVColumnFamily));
    if( !pCF ){
      sqlite3BtreeClose(pKV->pBt);
      sqlite3_mutex_free(pKV->db->mutex);
      sqlite3_free(pKV->db);
      kvstore_mutex_free(pKV->pMutex);
      sqlite3_free(pKV);
      return KVSTORE_NOMEM;
    }

    pCF->pKV = pKV;
    pCF->zName = sqliteStrDup(DEFAULT_CF_NAME);
    pCF->iTable = (int)defaultCFRoot;
    pCF->refCount = 1;

    /* Create mutex for default CF */
    pCF->pMutex = kvstore_mutex_alloc();
    if( !pCF->pMutex ){
      sqlite3_free(pCF->zName);
      sqlite3_free(pCF);
      sqlite3BtreeClose(pKV->pBt);
      sqlite3_mutex_free(pKV->db->mutex);
      sqlite3_free(pKV->db);
      kvstore_mutex_free(pKV->pMutex);
      sqlite3_free(pKV);
      return KVSTORE_NOMEM;
    }

    pKV->pDefaultCF = pCF;
    pKV->iMetaTable = (int)cfMetaRoot;
  }

  pKV->inTrans = 0;
  *ppKV = pKV;
  return KVSTORE_OK;
}

/*
** Close a key-value store.
*/
int kvstore_close(KVStore *pKV){
  int rc, i;

  if( pKV == NULL ){
    return KVSTORE_OK;
  }

  /* Acquire mutex before closing */
  kvstore_mutex_enter(pKV->pMutex);

  /* Rollback any active transaction */
  if( pKV->inTrans ){
    sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
    pKV->inTrans = 0;
  }

  /* Close all open column families */
  if( pKV->pDefaultCF ){
    if( pKV->pDefaultCF->pMutex ){
      kvstore_mutex_free(pKV->pDefaultCF->pMutex);
    }
    sqlite3_free(pKV->pDefaultCF->zName);
    sqlite3_free(pKV->pDefaultCF);
  }

  for(i = 0; i < pKV->nCF; i++){
    if( pKV->apCF[i] ){
      if( pKV->apCF[i]->pMutex ){
        kvstore_mutex_free(pKV->apCF[i]->pMutex);
      }
      sqlite3_free(pKV->apCF[i]->zName);
      sqlite3_free(pKV->apCF[i]);
    }
  }
  sqlite3_free(pKV->apCF);

  /* Close the btree */
  rc = sqlite3BtreeClose(pKV->pBt);

  /* Free error message */
  if( pKV->zErrMsg ){
    sqlite3_free(pKV->zErrMsg);
  }

  /* Free db resources */
  if( pKV->db->mutex ){
    sqlite3_mutex_free(pKV->db->mutex);
  }
  sqlite3_free(pKV->db);

  /* Release and free mutex */
  kvstore_mutex_leave(pKV->pMutex);
  kvstore_mutex_free(pKV->pMutex);

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

  kvstore_mutex_enter(pKV->pMutex);

  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot begin transaction: database is corrupted");
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_CORRUPT;
  }

  if( pKV->inTrans ){
    kvstoreSetError(pKV, "transaction already active");
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  rc = sqlite3BtreeBeginTrans(pKV->pBt, wrflag, 0);
  if( rc == SQLITE_OK ){
    pKV->inTrans = wrflag ? 2 : 1;
  }else{
    kvstoreCheckCorruption(pKV, rc);
    kvstoreSetError(pKV, "failed to begin transaction: error %d", rc);
  }

  kvstore_mutex_leave(pKV->pMutex);
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

  kvstore_mutex_enter(pKV->pMutex);

  if( !pKV->inTrans ){
    kvstoreSetError(pKV, "no active transaction to commit");
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  rc = sqlite3BtreeCommit(pKV->pBt);
  if( rc == SQLITE_OK ){
    pKV->inTrans = 0;
  }else{
    kvstoreCheckCorruption(pKV, rc);
    kvstoreSetError(pKV, "failed to commit transaction: error %d", rc);
  }

  kvstore_mutex_leave(pKV->pMutex);
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

  kvstore_mutex_enter(pKV->pMutex);

  if( !pKV->inTrans ){
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_OK; /* No transaction to rollback */
  }

  rc = sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
  pKV->inTrans = 0;

  if( rc != SQLITE_OK ){
    kvstoreCheckCorruption(pKV, rc);
    kvstoreSetError(pKV, "failed to rollback transaction: error %d", rc);
  }

  kvstore_mutex_leave(pKV->pMutex);
  return rc;
}

/* Forward declaration for CF operations */
static int kvstore_cf_put_internal(
  KVColumnFamily *pCF,
  const void *pKey, int nKey,
  const void *pValue, int nValue
);
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

  kvstore_mutex_enter(pKV->pMutex);

  if( !pKV->pDefaultCF ){
    kvstoreSetError(pKV, "default column family not initialized");
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  pKV->pDefaultCF->refCount++;
  *ppCF = pKV->pDefaultCF;

  kvstore_mutex_leave(pKV->pMutex);
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
    kvstore_mutex_enter(pKV->pMutex);
    kvstoreSetError(pKV, "invalid column family name length");
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  /* Check if default CF */
  if( strcmp(zName, DEFAULT_CF_NAME) == 0 ){
    return kvstore_cf_get_default(pKV, ppCF);
  }

  kvstore_mutex_enter(pKV->pMutex);

  /* Start transaction */
  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 1, 0);
    if( rc != SQLITE_OK ){
      kvstore_mutex_leave(pKV->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 2;
  }else if( pKV->inTrans != 2 ){
    kvstoreSetError(pKV, "cannot create CF: read-only transaction");
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_READONLY;
  }

  /* Check if CF already exists – search metadata table */
  pCur = kvstoreAllocCursor();
  if( !pCur ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    return SQLITE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 0, 0, pCur);
  if( rc != SQLITE_OK ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    return rc;
  }

  int found = 0;
  rc = kvstoreSeekKey(pCur, zName, (int)nNameLen, &found, NULL);
  kvstoreFreeCursor(pCur);
  pCur = NULL;

  if( rc != SQLITE_OK ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    return rc;
  }
  if( found ){
    if( autoTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
    kvstoreSetError(pKV, "column family already exists: %s", zName);
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  /* Create new btree table for this CF */
  rc = sqlite3BtreeCreateTable(pKV->pBt, &pgno, BTREE_INTKEY);
  if( rc != SQLITE_OK ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
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

    int nEncoded;
    void *pEncoded = kvstoreEncodeData(zName, (int)nNameLen,
                                       tableRootBytes, 4, &nEncoded);
    if( !pEncoded ){
      if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
      kvstore_mutex_leave(pKV->pMutex);
      return SQLITE_NOMEM;
    }

    pCur = kvstoreAllocCursor();
    if( !pCur ){
      sqlite3_free(pEncoded);
      if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
      kvstore_mutex_leave(pKV->pMutex);
      return SQLITE_NOMEM;
    }
    rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 1, 0, pCur);
    if( rc != SQLITE_OK ){
      sqlite3_free(pEncoded);
      kvstoreFreeCursor(pCur);
      if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
      kvstore_mutex_leave(pKV->pMutex);
      return rc;
    }

    i64 slot;
    rc = kvstoreFindSlot(pCur, zName, (int)nNameLen, &slot);
    if( rc != SQLITE_OK ){
      sqlite3_free(pEncoded);
      kvstoreFreeCursor(pCur);
      if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
      kvstore_mutex_leave(pKV->pMutex);
      return rc;
    }

    BtreePayload payload;
    memset(&payload, 0, sizeof(payload));
    payload.nKey  = slot;
    payload.pData = pEncoded;
    payload.nData = nEncoded;

    rc = sqlite3BtreeInsert(pCur, &payload, 0, 0);
    sqlite3_free(pEncoded);
    kvstoreFreeCursor(pCur);
    pCur = NULL;

    if( rc != SQLITE_OK ){
      if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
      kvstore_mutex_leave(pKV->pMutex);
      return rc;
    }
  }

  /* Update CF count */
  sqlite3BtreeGetMeta(pKV->pBt, META_CF_COUNT, &cfCount);
  cfCount++;
  sqlite3BtreeUpdateMeta(pKV->pBt, META_CF_COUNT, cfCount);

  /* Commit if we started the transaction */
  if( autoTrans ){
    rc = sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
    if( rc != SQLITE_OK ){
      kvstore_mutex_leave(pKV->pMutex);
      return rc;
    }
  }

  /* Create CF structure */
  pCF = (KVColumnFamily*)sqlite3MallocZero(sizeof(KVColumnFamily));
  if( !pCF ){
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }

  zNameCopy = sqliteStrDup(zName);
  if( !zNameCopy ){
    sqlite3_free(pCF);
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }

  pCF->pKV = pKV;
  pCF->zName = zNameCopy;
  pCF->iTable = (int)pgno;
  pCF->refCount = 1;

  /* Create mutex for this CF */
  pCF->pMutex = kvstore_mutex_alloc();
  if( !pCF->pMutex ){
    sqlite3_free(zNameCopy);
    sqlite3_free(pCF);
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }

  kvstore_mutex_leave(pKV->pMutex);

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
      kvstore_mutex_enter(pKV->pMutex);
      kvstoreSetError(pKV, "invalid parameters to cf_open");
      kvstore_mutex_leave(pKV->pMutex);
    }
    return KVSTORE_ERROR;
  }

  *ppCF = NULL;
  nNameLen = strlen(zName);

  /* Check if default CF */
  if( strcmp(zName, DEFAULT_CF_NAME) == 0 ){
    return kvstore_cf_get_default(pKV, ppCF);
  }

  kvstore_mutex_enter(pKV->pMutex);

  /* Start read transaction if needed */
  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
    if( rc != SQLITE_OK ){
      kvstore_mutex_leave(pKV->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 1;
  }

  /* Look up CF in metadata table */
  pCur = kvstoreAllocCursor();
  if( !pCur ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    return SQLITE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 0, 0, pCur);
  if( rc != SQLITE_OK ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    return rc;
  }

  int found = 0;
  i64 foundRowid = 0;
  rc = kvstoreSeekKey(pCur, zName, (int)nNameLen, &found, &foundRowid);
  if( rc != SQLITE_OK || !found ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    return (rc == SQLITE_OK) ? KVSTORE_NOTFOUND : rc;
  }

  /* Re-position cursor on the found rowid and read data */
  {
    int res = 0;
    rc = sqlite3BtreeTableMoveto(pCur, foundRowid, 0, &res);
    if( rc != SQLITE_OK || res != 0 ){
      kvstoreFreeCursor(pCur);
      if( autoTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
      kvstore_mutex_leave(pKV->pMutex);
      return KVSTORE_CORRUPT;
    }
  }

  /* Read the payload: [name_len(4) | name | table_root(4)] */
  u32 payloadSz = sqlite3BtreePayloadSize(pCur);
  if( (int)payloadSz < 4 + (int)nNameLen + 4 ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
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
    kvstore_mutex_leave(pKV->pMutex);
    return rc;
  }

  iTable = (tableRootBytes[0] << 24) | (tableRootBytes[1] << 16) |
           (tableRootBytes[2] << 8) | tableRootBytes[3];

  /* Create CF structure */
  pCF = (KVColumnFamily*)sqlite3MallocZero(sizeof(KVColumnFamily));
  if( !pCF ){
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }

  zNameCopy = sqliteStrDup(zName);
  if( !zNameCopy ){
    sqlite3_free(pCF);
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }

  pCF->pKV = pKV;
  pCF->zName = zNameCopy;
  pCF->iTable = iTable;
  pCF->refCount = 1;

  /* Create mutex for this CF */
  pCF->pMutex = kvstore_mutex_alloc();
  if( !pCF->pMutex ){
    sqlite3_free(zNameCopy);
    sqlite3_free(pCF);
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_NOMEM;
  }

  kvstore_mutex_leave(pKV->pMutex);

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

  kvstore_mutex_enter(pKV->pMutex);

  pCF->refCount--;
  if( pCF->refCount <= 0 && pCF != pKV->pDefaultCF ){
    shouldFree = 1;
  }

  kvstore_mutex_leave(pKV->pMutex);

  if( shouldFree ){
    if( pCF->pMutex ){
      kvstore_mutex_free(pCF->pMutex);
    }
    sqlite3_free(pCF->zName);
    sqlite3_free(pCF);
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

  kvstore_mutex_enter(pCF->pMutex);
  kvstore_mutex_enter(pKV->pMutex);

  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot put: database is corrupted");
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return KVSTORE_CORRUPT;
  }

  rc = kvstoreValidateKeyValue(pKV, pKey, nKey, pValue, nValue);
  if( rc != KVSTORE_OK ){
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return rc;
  }

  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 1, 0);
    if( rc != SQLITE_OK ){
      kvstore_mutex_leave(pKV->pMutex);
      kvstore_mutex_leave(pCF->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 2;
  }else if( pKV->inTrans != 2 ){
    kvstoreSetError(pKV, "cannot put: read-only transaction active");
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return KVSTORE_READONLY;
  }

  pCur = kvstoreAllocCursor();
  if( !pCur ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return SQLITE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pCF->iTable, 1, 0, pCur);
  if( rc != SQLITE_OK ){
    kvstoreCheckCorruption(pKV, rc);
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return rc;
  }

  /* Check if key already exists (for update) */
  int found = 0;
  i64 existingRowid = 0;
  rc = kvstoreSeekKey(pCur, pKey, nKey, &found, &existingRowid);
  if( rc != SQLITE_OK ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return rc;
  }

  /* Determine the rowid to use */
  i64 rowid;
  if( found ){
    /* Update existing entry – delete the old one first */
    int res = 0;
    rc = sqlite3BtreeTableMoveto(pCur, existingRowid, 0, &res);
    if( rc == SQLITE_OK && res == 0 ){
      rc = sqlite3BtreeDelete(pCur, 0);
    }
    if( rc != SQLITE_OK ){
      kvstoreFreeCursor(pCur);
      if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
      kvstore_mutex_leave(pKV->pMutex);
      kvstore_mutex_leave(pCF->pMutex);
      return rc;
    }
    rowid = existingRowid;
  }else{
    /* New key – find empty slot */
    rc = kvstoreFindSlot(pCur, pKey, nKey, &rowid);
    if( rc != SQLITE_OK ){
      kvstoreFreeCursor(pCur);
      if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
      kvstore_mutex_leave(pKV->pMutex);
      kvstore_mutex_leave(pCF->pMutex);
      return rc;
    }
  }

  /* Encode and insert */
  int nEncoded;
  void *pEncoded = kvstoreEncodeData(pKey, nKey, pValue, nValue, &nEncoded);
  if( !pEncoded ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return SQLITE_NOMEM;
  }

  BtreePayload payload;
  memset(&payload, 0, sizeof(payload));
  payload.nKey  = rowid;
  payload.pData = pEncoded;
  payload.nData = nEncoded;

  rc = sqlite3BtreeInsert(pCur, &payload, 0, 0);
  sqlite3_free(pEncoded);
  kvstoreFreeCursor(pCur);

  if( rc == SQLITE_OK ){
    pKV->stats.nPuts++;
    if( autoTrans ){
      rc = sqlite3BtreeCommit(pKV->pBt);
      pKV->inTrans = 0;
    }
  }else{
    kvstoreCheckCorruption(pKV, rc);
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
      pKV->inTrans = 0;
    }
  }

  kvstore_mutex_leave(pKV->pMutex);
  kvstore_mutex_leave(pCF->pMutex);

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
  int autoTrans = 0;
  KVStore *pKV = pCF->pKV;

  if( !ppValue || !pnValue ){
    kvstore_mutex_enter(pKV->pMutex);
    kvstoreSetError(pKV, "invalid parameters to get");
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  *ppValue = NULL;
  *pnValue = 0;

  kvstore_mutex_enter(pCF->pMutex);
  kvstore_mutex_enter(pKV->pMutex);

  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot get: database is corrupted");
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return KVSTORE_CORRUPT;
  }

  if( !pKey || nKey <= 0 ){
    kvstoreSetError(pKV, "invalid key");
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return KVSTORE_ERROR;
  }

  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
    if( rc != SQLITE_OK ){
      kvstore_mutex_leave(pKV->pMutex);
      kvstore_mutex_leave(pCF->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 1;
  }

  pCur = kvstoreAllocCursor();
  if( !pCur ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return SQLITE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pCF->iTable, 0, 0, pCur);
  if( rc != SQLITE_OK ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return rc;
  }

  int found = 0;
  i64 foundRowid = 0;
  rc = kvstoreSeekKey(pCur, pKey, nKey, &found, &foundRowid);
  if( rc != SQLITE_OK || !found ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return (rc == SQLITE_OK) ? KVSTORE_NOTFOUND : rc;
  }

  /* Re-position and read value */
  {
    int res = 0;
    rc = sqlite3BtreeTableMoveto(pCur, foundRowid, 0, &res);
    if( rc != SQLITE_OK || res != 0 ){
      kvstoreFreeCursor(pCur);
      if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
      kvstore_mutex_leave(pKV->pMutex);
      kvstore_mutex_leave(pCF->pMutex);
      return (rc == SQLITE_OK) ? KVSTORE_CORRUPT : rc;
    }
  }

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

  kvstoreFreeCursor(pCur);
  if( autoTrans ){
    sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
  }

  if( rc == SQLITE_OK ){
    pKV->stats.nGets++;
    *ppValue = pValue;
    *pnValue = (valueLen > 0) ? valueLen : 0;
  }

  kvstore_mutex_leave(pKV->pMutex);
  kvstore_mutex_leave(pCF->pMutex);

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

  kvstore_mutex_enter(pCF->pMutex);
  kvstore_mutex_enter(pKV->pMutex);

  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot delete: database is corrupted");
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return KVSTORE_CORRUPT;
  }

  if( !pKey || nKey <= 0 ){
    kvstoreSetError(pKV, "invalid key");
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return KVSTORE_ERROR;
  }

  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 1, 0);
    if( rc != SQLITE_OK ){
      kvstore_mutex_leave(pKV->pMutex);
      kvstore_mutex_leave(pCF->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 2;
  }else if( pKV->inTrans != 2 ){
    kvstoreSetError(pKV, "cannot delete: read-only transaction");
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return KVSTORE_READONLY;
  }

  pCur = kvstoreAllocCursor();
  if( !pCur ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return SQLITE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pCF->iTable, 1, 0, pCur);
  if( rc != SQLITE_OK ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return rc;
  }

  int found = 0;
  i64 foundRowid = 0;
  rc = kvstoreSeekKey(pCur, pKey, nKey, &found, &foundRowid);
  if( rc != SQLITE_OK || !found ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return (rc == SQLITE_OK) ? KVSTORE_NOTFOUND : rc;
  }

  /* Position on exact rowid and delete */
  {
    int res = 0;
    rc = sqlite3BtreeTableMoveto(pCur, foundRowid, 0, &res);
    if( rc == SQLITE_OK && res == 0 ){
      rc = sqlite3BtreeDelete(pCur, 0);
    }
  }
  kvstoreFreeCursor(pCur);

  if( rc == SQLITE_OK ){
    pKV->stats.nDeletes++;
    if( autoTrans ){
      rc = sqlite3BtreeCommit(pKV->pBt);
      pKV->inTrans = 0;
    }
  }else{
    if( autoTrans ){
      sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
      pKV->inTrans = 0;
    }
  }

  kvstore_mutex_leave(pKV->pMutex);
  kvstore_mutex_leave(pCF->pMutex);

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
  int autoTrans = 0;
  KVStore *pKV = pCF->pKV;

  if( !pExists ){
    kvstore_mutex_enter(pKV->pMutex);
    kvstoreSetError(pKV, "invalid parameters to exists");
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  *pExists = 0;

  kvstore_mutex_enter(pCF->pMutex);
  kvstore_mutex_enter(pKV->pMutex);

  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot check existence: database is corrupted");
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return KVSTORE_CORRUPT;
  }

  if( !pKey || nKey <= 0 ){
    kvstoreSetError(pKV, "invalid key");
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return KVSTORE_ERROR;
  }

  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
    if( rc != SQLITE_OK ){
      kvstore_mutex_leave(pKV->pMutex);
      kvstore_mutex_leave(pCF->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 1;
  }

  pCur = kvstoreAllocCursor();
  if( !pCur ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return SQLITE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pCF->iTable, 0, 0, pCur);
  if( rc != SQLITE_OK ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    kvstore_mutex_leave(pCF->pMutex);
    return rc;
  }

  int found = 0;
  rc = kvstoreSeekKey(pCur, pKey, nKey, &found, NULL);
  kvstoreFreeCursor(pCur);

  if( autoTrans ){
    sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
  }

  if( rc == SQLITE_OK ){
    *pExists = found;
  }

  kvstore_mutex_leave(pKV->pMutex);
  kvstore_mutex_leave(pCF->pMutex);

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

  kvstore_mutex_enter(pKV->pMutex);

  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot create iterator: database is corrupted");
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_CORRUPT;
  }

  pIter = (KVIterator*)sqlite3MallocZero(sizeof(KVIterator));
  if( !pIter ){
    kvstoreSetError(pKV, "out of memory allocating iterator");
    kvstore_mutex_leave(pKV->pMutex);
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
      kvstore_mutex_leave(pKV->pMutex);
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
    kvstore_mutex_leave(pKV->pMutex);
    return SQLITE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pCF->iTable, 0, 0, pIter->pCur);
  if( rc != SQLITE_OK ){
    kvstoreCheckCorruption(pKV, rc);
    if( pIter->ownsTrans ){
      sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0);
      pKV->inTrans = 0;
    }
    pCF->refCount--;
    sqlite3_free(pIter->pCur);
    sqlite3_free(pIter);
    kvstore_mutex_leave(pKV->pMutex);
    return rc;
  }

  pKV->stats.nIterations++;
  kvstore_mutex_leave(pKV->pMutex);

  *ppIter = pIter;
  return KVSTORE_OK;
}

/*
** Move iterator to first entry
*/
int kvstore_iterator_first(KVIterator *pIter){
  int rc, res;

  if( !pIter || !pIter->isValid ){
    return KVSTORE_ERROR;
  }

  rc = sqlite3BtreeFirst(pIter->pCur, &res);
  if( rc == SQLITE_OK ){
    pIter->eof = res;
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
    kvstore_mutex_enter(pKV->pMutex);
    sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
    kvstore_mutex_leave(pKV->pMutex);
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

  sqlite3_free(pIter);
}

/*
** Get statistics
*/
int kvstore_stats(KVStore *pKV, KVStoreStats *pStats){
  if( !pKV || !pStats ){
    return KVSTORE_ERROR;
  }

  kvstore_mutex_enter(pKV->pMutex);

  pStats->nPuts = pKV->stats.nPuts;
  pStats->nGets = pKV->stats.nGets;
  pStats->nDeletes = pKV->stats.nDeletes;
  pStats->nIterations = pKV->stats.nIterations;
  pStats->nErrors = pKV->stats.nErrors;

  kvstore_mutex_leave(pKV->pMutex);

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

  kvstore_mutex_enter(pKV->pMutex);

  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
    if( rc != SQLITE_OK ){
      kvstore_mutex_leave(pKV->pMutex);
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
      kvstore_mutex_leave(pKV->pMutex);
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
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_CORRUPT;
  }

  if( zErr ) sqlite3_free(zErr);

  kvstore_mutex_leave(pKV->pMutex);
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

  kvstore_mutex_enter(pKV->pMutex);

  if( pKV->isCorrupted ){
    kvstoreSetError(pKV, "cannot sync: database is corrupted");
    kvstore_mutex_leave(pKV->pMutex);
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

  kvstore_mutex_leave(pKV->pMutex);
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

  kvstore_mutex_enter(pKV->pMutex);

  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 0, 0);
    if( rc != SQLITE_OK ){
      kvstore_mutex_leave(pKV->pMutex);
      sqlite3_free(azNames);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 1;
  }

  pCur = kvstoreAllocCursor();
  if( !pCur ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    sqlite3_free(azNames);
    return SQLITE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 0, 0, pCur);
  if( rc != SQLITE_OK ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
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

        if( nCount >= nAlloc ){
          nAlloc *= 2;
          char **azNew = (char**)sqlite3Realloc(azNames, nAlloc * sizeof(char*));
          if( !azNew ){ sqlite3_free(name); rc = KVSTORE_NOMEM; break; }
          azNames = azNew;
        }

        azNames[nCount++] = name;
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

  kvstore_mutex_leave(pKV->pMutex);

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
      kvstore_mutex_enter(pKV->pMutex);
      kvstoreSetError(pKV, "invalid parameters to cf_drop");
      kvstore_mutex_leave(pKV->pMutex);
    }
    return KVSTORE_ERROR;
  }

  /* Cannot drop default CF */
  if( strcmp(zName, DEFAULT_CF_NAME) == 0 ){
    kvstore_mutex_enter(pKV->pMutex);
    kvstoreSetError(pKV, "cannot drop default column family");
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_ERROR;
  }

  nNameLen = strlen(zName);

  kvstore_mutex_enter(pKV->pMutex);

  if( !pKV->inTrans ){
    rc = sqlite3BtreeBeginTrans(pKV->pBt, 1, 0);
    if( rc != SQLITE_OK ){
      kvstore_mutex_leave(pKV->pMutex);
      return rc;
    }
    autoTrans = 1;
    pKV->inTrans = 2;
  }else if( pKV->inTrans != 2 ){
    kvstoreSetError(pKV, "cannot drop CF: read-only transaction");
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_READONLY;
  }

  /* Find CF in metadata table */
  pCur = kvstoreAllocCursor();
  if( !pCur ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    return SQLITE_NOMEM;
  }
  rc = sqlite3BtreeCursor(pKV->pBt, pKV->iMetaTable, 1, 0, pCur);
  if( rc != SQLITE_OK ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    return rc;
  }

  int found = 0;
  i64 foundRowid = 0;
  rc = kvstoreSeekKey(pCur, zName, (int)nNameLen, &found, &foundRowid);
  if( rc != SQLITE_OK || !found ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeCommit(pKV->pBt); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    return (rc == SQLITE_OK) ? KVSTORE_NOTFOUND : rc;
  }

  /* Read the table root from payload */
  {
    int res = 0;
    rc = sqlite3BtreeTableMoveto(pCur, foundRowid, 0, &res);
    if( rc != SQLITE_OK || res != 0 ){
      kvstoreFreeCursor(pCur);
      if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
      kvstore_mutex_leave(pKV->pMutex);
      return KVSTORE_CORRUPT;
    }
  }

  u32 payloadSz = sqlite3BtreePayloadSize(pCur);
  if( (int)payloadSz < 4 + (int)nNameLen + 4 ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    return KVSTORE_CORRUPT;
  }

  unsigned char tableRootBytes[4];
  rc = sqlite3BtreePayload(pCur, 4 + (int)nNameLen, 4, tableRootBytes);
  if( rc != SQLITE_OK ){
    kvstoreFreeCursor(pCur);
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    return rc;
  }

  iTable = (tableRootBytes[0] << 24) | (tableRootBytes[1] << 16) |
           (tableRootBytes[2] << 8) | tableRootBytes[3];

  /* Delete from metadata table */
  rc = sqlite3BtreeDelete(pCur, 0);
  kvstoreFreeCursor(pCur);
  pCur = NULL;

  if( rc != SQLITE_OK ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    kvstore_mutex_leave(pKV->pMutex);
    return rc;
  }

  /* Drop the actual table */
  rc = sqlite3BtreeDropTable(pKV->pBt, iTable, &iMoved);
  if( rc != SQLITE_OK ){
    if( autoTrans ){ sqlite3BtreeRollback(pKV->pBt, SQLITE_OK, 0); pKV->inTrans = 0; }
    kvstoreSetError(pKV, "failed to drop table: error %d", rc);
    kvstore_mutex_leave(pKV->pMutex);
    return rc;
  }

  /* Update CF count */
  sqlite3BtreeGetMeta(pKV->pBt, META_CF_COUNT, &cfCount);
  if( cfCount > 0 ) cfCount--;
  sqlite3BtreeUpdateMeta(pKV->pBt, META_CF_COUNT, cfCount);

  if( autoTrans ){
    rc = sqlite3BtreeCommit(pKV->pBt);
    pKV->inTrans = 0;
  }

  kvstore_mutex_leave(pKV->pMutex);

  return rc;
}
