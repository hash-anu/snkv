/*
** 2025 January 26
**
** Key-Value Store Implementation
** Built on top of SQLite v3.3.0 btree implementation
**
** This implementation provides a simple key-value store interface
** using the underlying btree structure from SQLite.
*/

#include "kvstore.h"
#include <string.h>
#include <assert.h>

/*
** KVStore structure - represents an open key-value store
*/
struct KVStore {
  Btree *pBt;        /* Underlying btree handle */
  sqlite3 *db;       /* Database connection (required by btree) */
  int iTable;        /* Root page of the key-value table */
  int inTrans;       /* Transaction state: 0=none, 1=read, 2=write */
};

/*
** Iterator structure for traversing the store
*/
struct KVIterator {
  KVStore *pKV;      /* Parent KVStore */
  BtCursor *pCur;    /* Btree cursor */
  int eof;           /* End-of-file flag */
  void *pKeyBuf;     /* Buffer for current key */
  int nKeyBuf;       /* Size of key buffer */
  void *pValBuf;     /* Buffer for current value */
  int nValBuf;       /* Size of value buffer */
};

/*
** Memory comparison function for key ordering
*/
static int keyCompare(
  void *NotUsed,
  int n1, const void *p1,
  int n2, const void *p2
){
  int c = memcmp(p1, p2, n1 < n2 ? n1 : n2);
  if( c == 0 ){
    c = n1 - n2;
  }
  return c;
}

/*
** Open a key-value store database file.
*/
int kvstore_open(
  const char *zFilename,
  KVStore **ppKV,
  int flags
){
  KVStore *pKV;
  int rc;
  
  /* Allocate KVStore structure */
  pKV = (KVStore*)sqliteMalloc(sizeof(KVStore));
  if( pKV == NULL ){
    return KVSTORE_NOMEM;
  }
  memset(pKV, 0, sizeof(KVStore));
  
  /* Create a minimal sqlite3 structure (required by btree) */
  pKV->db = (sqlite3*)sqliteMalloc(sizeof(sqlite3));
  if( pKV->db == NULL ){
    sqliteFree(pKV);
    return KVSTORE_NOMEM;
  }
  memset(pKV->db, 0, sizeof(sqlite3));
  
  /* Open the btree */
  rc = sqlite3BtreeOpen(zFilename, pKV->db, &pKV->pBt, flags);
  if( rc != SQLITE_OK ){
    sqliteFree(pKV->db);
    sqliteFree(pKV);
    return rc;
  }
  
  /* Begin a write transaction to set up the database */
  rc = sqlite3BtreeBeginTrans(pKV->pBt, 1);
  if( rc != SQLITE_OK ){
    sqlite3BtreeClose(pKV->pBt);
    sqliteFree(pKV->db);
    sqliteFree(pKV);
    return rc;
  }
  
  /* Create the main key-value table if it doesn't exist */
  rc = sqlite3BtreeCreateTable(pKV->pBt, &pKV->iTable, 
                                BTREE_ZERODATA);
  if( rc != SQLITE_OK ){
    sqlite3BtreeRollback(pKV->pBt);
    sqlite3BtreeClose(pKV->pBt);
    sqliteFree(pKV->db);
    sqliteFree(pKV);
    return rc;
  }
  
  /* Commit the setup transaction */
  rc = sqlite3BtreeCommit(pKV->pBt);
  if( rc != SQLITE_OK ){
    sqlite3BtreeClose(pKV->pBt);
    sqliteFree(pKV->db);
    sqliteFree(pKV);
    return rc;
  }
  
  pKV->inTrans = 0;
  *ppKV = pKV;
  return KVSTORE_OK;
}

/*
** Close a key-value store.
*/
int kvstore_close(KVStore *pKV){
  int rc;
  
  if( pKV == NULL ){
    return KVSTORE_OK;
  }
  
  /* Rollback any active transaction */
  if( pKV->inTrans ){
    sqlite3BtreeRollback(pKV->pBt);
  }
  
  /* Close the btree */
  rc = sqlite3BtreeClose(pKV->pBt);
  
  /* Free resources */
  sqliteFree(pKV->db);
  sqliteFree(pKV);
  
  return rc;
}

/*
** Begin a transaction.
*/
int kvstore_begin(KVStore *pKV, int wrflag){
  int rc;
  
  if( pKV->inTrans ){
    return KVSTORE_ERROR; /* Already in transaction */
  }
  
  rc = sqlite3BtreeBeginTrans(pKV->pBt, wrflag);
  if( rc == SQLITE_OK ){
    pKV->inTrans = wrflag ? 2 : 1;
  }
  
  return rc;
}

/*
** Commit the current transaction.
*/
int kvstore_commit(KVStore *pKV){
  int rc;
  
  if( !pKV->inTrans ){
    return KVSTORE_ERROR; /* No active transaction */
  }
  
  rc = sqlite3BtreeCommit(pKV->pBt);
  if( rc == SQLITE_OK ){
    pKV->inTrans = 0;
  }
  
  return rc;
}

/*
** Rollback the current transaction.
*/
int kvstore_rollback(KVStore *pKV){
  int rc;
  
  if( !pKV->inTrans ){
    return KVSTORE_ERROR; /* No active transaction */
  }
  
  rc = sqlite3BtreeRollback(pKV->pBt);
  pKV->inTrans = 0;
  
  return rc;
}

/*
** Insert or update a key-value pair.
*/
int kvstore_put(
  KVStore *pKV,
  const void *pKey,
  int nKey,
  const void *pValue,
  int nValue
){
  BtCursor *pCur;
  int rc;
  int autoTrans = 0;
  
  /* Start transaction if not already in one */
  if( !pKV->inTrans ){
    rc = kvstore_begin(pKV, 1);
    if( rc != SQLITE_OK ) return rc;
    autoTrans = 1;
  }else if( pKV->inTrans != 2 ){
    return KVSTORE_READONLY; /* Read-only transaction */
  }
  
  /* Create a cursor */
  rc = sqlite3BtreeCursor(pKV->pBt, pKV->iTable, 1, 
                          keyCompare, NULL, &pCur);
  if( rc != SQLITE_OK ){
    if( autoTrans ) kvstore_rollback(pKV);
    return rc;
  }
  
  /* Insert the key-value pair */
  rc = sqlite3BtreeInsert(pCur, pKey, nKey, pValue, nValue);
  
  /* Close cursor */
  sqlite3BtreeCloseCursor(pCur);
  
  /* Auto-commit if we started the transaction */
  if( autoTrans ){
    if( rc == SQLITE_OK ){
      rc = kvstore_commit(pKV);
    }else{
      kvstore_rollback(pKV);
    }
  }
  
  return rc;
}

/*
** Retrieve a value by key.
*/
int kvstore_get(
  KVStore *pKV,
  const void *pKey,
  int nKey,
  void **ppValue,
  int *pnValue
){
  BtCursor *pCur;
  int rc;
  int loc;
  u32 dataSize;
  void *pValue;
  int autoTrans = 0;
  
  /* Start read transaction if not already in one */
  if( !pKV->inTrans ){
    rc = kvstore_begin(pKV, 0);
    if( rc != SQLITE_OK ) return rc;
    autoTrans = 1;
  }
  
  /* Create a cursor */
  rc = sqlite3BtreeCursor(pKV->pBt, pKV->iTable, 0,
                          keyCompare, NULL, &pCur);
  if( rc != SQLITE_OK ){
    if( autoTrans ) kvstore_rollback(pKV);
    return rc;
  }
  
  /* Search for the key */
  rc = sqlite3BtreeMoveto(pCur, pKey, nKey, &loc);
  if( rc != SQLITE_OK ){
    sqlite3BtreeCloseCursor(pCur);
    if( autoTrans ) kvstore_rollback(pKV);
    return rc;
  }
  
  /* Check if key was found */
  if( loc != 0 ){
    sqlite3BtreeCloseCursor(pCur);
    if( autoTrans ) kvstore_rollback(pKV);
    return KVSTORE_NOTFOUND;
  }
  
  /* Get the data size */
  rc = sqlite3BtreeDataSize(pCur, &dataSize);
  if( rc != SQLITE_OK ){
    sqlite3BtreeCloseCursor(pCur);
    if( autoTrans ) kvstore_rollback(pKV);
    return rc;
  }
  
  /* Allocate buffer for value */
  pValue = sqliteMalloc(dataSize);
  if( pValue == NULL ){
    sqlite3BtreeCloseCursor(pCur);
    if( autoTrans ) kvstore_rollback(pKV);
    return KVSTORE_NOMEM;
  }
  
  /* Read the value */
  rc = sqlite3BtreeData(pCur, 0, dataSize, pValue);
  if( rc != SQLITE_OK ){
    sqliteFree(pValue);
    sqlite3BtreeCloseCursor(pCur);
    if( autoTrans ) kvstore_rollback(pKV);
    return rc;
  }
  
  /* Close cursor */
  sqlite3BtreeCloseCursor(pCur);
  
  /* Auto-commit if we started the transaction */
  if( autoTrans ){
    kvstore_commit(pKV);
  }
  
  *ppValue = pValue;
  *pnValue = (int)dataSize;
  
  return KVSTORE_OK;
}

/*
** Delete a key-value pair.
*/
int kvstore_delete(
  KVStore *pKV,
  const void *pKey,
  int nKey
){
  BtCursor *pCur;
  int rc;
  int loc;
  int autoTrans = 0;
  
  /* Start write transaction if not already in one */
  if( !pKV->inTrans ){
    rc = kvstore_begin(pKV, 1);
    if( rc != SQLITE_OK ) return rc;
    autoTrans = 1;
  }else if( pKV->inTrans != 2 ){
    return KVSTORE_READONLY;
  }
  
  /* Create a cursor */
  rc = sqlite3BtreeCursor(pKV->pBt, pKV->iTable, 1,
                          keyCompare, NULL, &pCur);
  if( rc != SQLITE_OK ){
    if( autoTrans ) kvstore_rollback(pKV);
    return rc;
  }
  
  /* Search for the key */
  rc = sqlite3BtreeMoveto(pCur, pKey, nKey, &loc);
  if( rc != SQLITE_OK ){
    sqlite3BtreeCloseCursor(pCur);
    if( autoTrans ) kvstore_rollback(pKV);
    return rc;
  }
  
  /* Check if key was found */
  if( loc != 0 ){
    sqlite3BtreeCloseCursor(pCur);
    if( autoTrans ) kvstore_rollback(pKV);
    return KVSTORE_NOTFOUND;
  }
  
  /* Delete the entry */
  rc = sqlite3BtreeDelete(pCur);
  
  /* Close cursor */
  sqlite3BtreeCloseCursor(pCur);
  
  /* Auto-commit if we started the transaction */
  if( autoTrans ){
    if( rc == SQLITE_OK ){
      rc = kvstore_commit(pKV);
    }else{
      kvstore_rollback(pKV);
    }
  }
  
  return rc;
}

/*
** Check if a key exists.
*/
int kvstore_exists(
  KVStore *pKV,
  const void *pKey,
  int nKey,
  int *pExists
){
  BtCursor *pCur;
  int rc;
  int loc;
  int autoTrans = 0;
  
  /* Start read transaction if not already in one */
  if( !pKV->inTrans ){
    rc = kvstore_begin(pKV, 0);
    if( rc != SQLITE_OK ) return rc;
    autoTrans = 1;
  }
  
  /* Create a cursor */
  rc = sqlite3BtreeCursor(pKV->pBt, pKV->iTable, 0,
                          keyCompare, NULL, &pCur);
  if( rc != SQLITE_OK ){
    if( autoTrans ) kvstore_rollback(pKV);
    return rc;
  }
  
  /* Search for the key */
  rc = sqlite3BtreeMoveto(pCur, pKey, nKey, &loc);
  
  /* Close cursor */
  sqlite3BtreeCloseCursor(pCur);
  
  /* Auto-commit if we started the transaction */
  if( autoTrans ){
    kvstore_commit(pKV);
  }
  
  if( rc == SQLITE_OK ){
    *pExists = (loc == 0) ? 1 : 0;
  }
  
  return rc;
}

/*
** Create an iterator.
*/
int kvstore_iterator_create(
  KVStore *pKV,
  KVIterator **ppIter
){
  KVIterator *pIter;
  int rc;
  
  /* Allocate iterator structure */
  pIter = (KVIterator*)sqliteMalloc(sizeof(KVIterator));
  if( pIter == NULL ){
    return KVSTORE_NOMEM;
  }
  memset(pIter, 0, sizeof(KVIterator));
  
  pIter->pKV = pKV;
  pIter->eof = 1;
  
  /* Start read transaction if not already in one */
  if( !pKV->inTrans ){
    rc = kvstore_begin(pKV, 0);
    if( rc != SQLITE_OK ){
      sqliteFree(pIter);
      return rc;
    }
  }
  
  /* Create a cursor */
  rc = sqlite3BtreeCursor(pKV->pBt, pKV->iTable, 0,
                          keyCompare, NULL, &pIter->pCur);
  if( rc != SQLITE_OK ){
    sqliteFree(pIter);
    return rc;
  }
  
  *ppIter = pIter;
  return KVSTORE_OK;
}

/*
** Move iterator to first entry.
*/
int kvstore_iterator_first(KVIterator *pIter){
  int rc;
  int res;
  
  rc = sqlite3BtreeFirst(pIter->pCur, &res);
  if( rc == SQLITE_OK ){
    pIter->eof = res;
  }
  
  return rc;
}

/*
** Move iterator to next entry.
*/
int kvstore_iterator_next(KVIterator *pIter){
  int rc;
  int res;
  
  if( pIter->eof ){
    return KVSTORE_OK;
  }
  
  rc = sqlite3BtreeNext(pIter->pCur, &res);
  if( rc == SQLITE_OK ){
    pIter->eof = res;
  }
  
  return rc;
}

/*
** Check if iterator is at end.
*/
int kvstore_iterator_eof(KVIterator *pIter){
  return pIter->eof;
}

/*
** Get current key from iterator.
*/
int kvstore_iterator_key(
  KVIterator *pIter,
  void **ppKey,
  int *pnKey
){
  int rc;
  i64 keySize;
  
  if( pIter->eof ){
    return KVSTORE_ERROR;
  }
  
  /* Get key size */
  rc = sqlite3BtreeKeySize(pIter->pCur, &keySize);
  if( rc != SQLITE_OK ){
    return rc;
  }
  
  /* Resize buffer if needed */
  if( pIter->nKeyBuf < (int)keySize ){
    void *pNew = sqliteRealloc(pIter->pKeyBuf, (int)keySize);
    if( pNew == NULL ){
      return KVSTORE_NOMEM;
    }
    pIter->pKeyBuf = pNew;
    pIter->nKeyBuf = (int)keySize;
  }
  
  /* Read the key */
  rc = sqlite3BtreeKey(pIter->pCur, 0, (int)keySize, pIter->pKeyBuf);
  if( rc != SQLITE_OK ){
    return rc;
  }
  
  *ppKey = pIter->pKeyBuf;
  *pnKey = (int)keySize;
  
  return KVSTORE_OK;
}

/*
** Get current value from iterator.
*/
int kvstore_iterator_value(
  KVIterator *pIter,
  void **ppValue,
  int *pnValue
){
  int rc;
  u32 dataSize;
  
  if( pIter->eof ){
    return KVSTORE_ERROR;
  }
  
  /* Get data size */
  rc = sqlite3BtreeDataSize(pIter->pCur, &dataSize);
  if( rc != SQLITE_OK ){
    return rc;
  }
  
  /* Resize buffer if needed */
  if( pIter->nValBuf < (int)dataSize ){
    void *pNew = sqliteRealloc(pIter->pValBuf, (int)dataSize);
    if( pNew == NULL ){
      return KVSTORE_NOMEM;
    }
    pIter->pValBuf = pNew;
    pIter->nValBuf = (int)dataSize;
  }
  
  /* Read the data */
  rc = sqlite3BtreeData(pIter->pCur, 0, dataSize, pIter->pValBuf);
  if( rc != SQLITE_OK ){
    return rc;
  }
  
  *ppValue = pIter->pValBuf;
  *pnValue = (int)dataSize;
  
  return KVSTORE_OK;
}

/*
** Close an iterator.
*/
void kvstore_iterator_close(KVIterator *pIter){
  if( pIter == NULL ){
    return;
  }
  
  if( pIter->pCur ){
    sqlite3BtreeCloseCursor(pIter->pCur);
  }
  
  if( pIter->pKeyBuf ){
    sqliteFree(pIter->pKeyBuf);
  }
  
  if( pIter->pValBuf ){
    sqliteFree(pIter->pValBuf);
  }
  
  sqliteFree(pIter);
}