/* SPDX-License-Identifier: Apache-2.0 */
/*
**
** Key-Value Store API Header
** Built on top of SQLite v3.3.0 btree implementation
**
** This header provides a simple key-value store interface using
** the underlying btree structure from SQLite, with support for
** column families (multiple logical namespaces).
*/

#ifndef _KVSTORE_H_
#define _KVSTORE_H_

#include "sqliteInt.h"
#include "btree.h"
#include "kvstore_mutex.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
** KVStore handle - represents an open key-value store
*/
typedef struct KVStore KVStore;

/*
** Column Family handle - represents a logical namespace
*/
typedef struct KVColumnFamily KVColumnFamily;

/*
** Error codes (in addition to standard SQLite error codes)
*/
#define KVSTORE_OK           SQLITE_OK
#define KVSTORE_ERROR        SQLITE_ERROR
#define KVSTORE_NOTFOUND     SQLITE_NOTFOUND
#define KVSTORE_NOMEM        SQLITE_NOMEM
#define KVSTORE_READONLY     SQLITE_READONLY
#define KVSTORE_LOCKED       SQLITE_LOCKED
#define KVSTORE_CORRUPT      SQLITE_CORRUPT

/*
** Maximum number of column families
*/
#define KVSTORE_MAX_COLUMN_FAMILIES 64

/*
** Open a key-value store database file.
**
** Parameters:
**   zFilename - Path to the database file (NULL for in-memory)
**   ppKV      - Output pointer to KVStore handle
**   flags     - Same flags as sqlite3BtreeOpen (BTREE_OMIT_JOURNAL, etc.)
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_open(
  const char *zFilename,
  KVStore **ppKV,
  int flags
);

/*
** Close a key-value store and free all associated resources.
**
** Parameters:
**   pKV - KVStore handle to close
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_close(KVStore *pKV);

/* ========== COLUMN FAMILY OPERATIONS ========== */

/*
** Create a new column family.
**
** Parameters:
**   pKV    - KVStore handle
**   zName  - Column family name (must be unique)
**   ppCF   - Output pointer to column family handle
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
**
** Note: Column family names are limited to 255 characters
*/
int kvstore_cf_create(
  KVStore *pKV,
  const char *zName,
  KVColumnFamily **ppCF
);

/*
** Open an existing column family.
**
** Parameters:
**   pKV    - KVStore handle
**   zName  - Column family name
**   ppCF   - Output pointer to column family handle
**
** Returns:
**   KVSTORE_OK on success
**   KVSTORE_NOTFOUND if column family doesn't exist
*/
int kvstore_cf_open(
  KVStore *pKV,
  const char *zName,
  KVColumnFamily **ppCF
);

/*
** Get the default column family (always exists).
**
** Parameters:
**   pKV  - KVStore handle
**   ppCF - Output pointer to column family handle
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
**
** Note: The default column family is created automatically on database open
*/
int kvstore_cf_get_default(
  KVStore *pKV,
  KVColumnFamily **ppCF
);

/*
** Drop a column family (delete all data and metadata).
**
** Parameters:
**   pKV   - KVStore handle
**   zName - Column family name
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
**
** Note: Cannot drop the default column family
*/
int kvstore_cf_drop(
  KVStore *pKV,
  const char *zName
);

/*
** List all column families in the database.
**
** Parameters:
**   pKV      - KVStore handle
**   pazNames - Output array of column family names (caller must free)
**   pnCount  - Output count of column families
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
**
** Note: Caller must free each name with sqliteFree(), then free the array
*/
int kvstore_cf_list(
  KVStore *pKV,
  char ***pazNames,
  int *pnCount
);

/*
** Close a column family handle (does not delete the column family).
**
** Parameters:
**   pCF - Column family handle
*/
void kvstore_cf_close(KVColumnFamily *pCF);

/* ========== KEY-VALUE OPERATIONS (DEFAULT CF) ========== */

/*
** Insert or update a key-value pair in the default column family.
**
** Parameters:
**   pKV      - KVStore handle
**   pKey     - Pointer to key data
**   nKey     - Length of key in bytes
**   pValue   - Pointer to value data
**   nValue   - Length of value in bytes
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
**
** Note: If the key already exists, its value will be updated.
*/
int kvstore_put(
  KVStore *pKV,
  const void *pKey,
  int nKey,
  const void *pValue,
  int nValue
);

/*
** Retrieve a value by key from the default column family.
**
** Parameters:
**   pKV      - KVStore handle
**   pKey     - Pointer to key data
**   nKey     - Length of key in bytes
**   ppValue  - Output pointer to value data (caller must free)
**   pnValue  - Output pointer to value length
**
** Returns:
**   KVSTORE_OK on success
**   KVSTORE_NOTFOUND if key doesn't exist
**   Other error codes on failure
**
** Note: Caller is responsible for freeing *ppValue with sqliteFree()
*/
int kvstore_get(
  KVStore *pKV,
  const void *pKey,
  int nKey,
  void **ppValue,
  int *pnValue
);

/*
** Delete a key-value pair from the default column family.
**
** Parameters:
**   pKV  - KVStore handle
**   pKey - Pointer to key data
**   nKey - Length of key in bytes
**
** Returns:
**   KVSTORE_OK on success
**   KVSTORE_NOTFOUND if key doesn't exist
**   Other error codes on failure
*/
int kvstore_delete(
  KVStore *pKV,
  const void *pKey,
  int nKey
);

/*
** Check if a key exists in the default column family.
**
** Parameters:
**   pKV     - KVStore handle
**   pKey    - Pointer to key data
**   nKey    - Length of key in bytes
**   pExists - Output: 1 if exists, 0 if not
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_exists(
  KVStore *pKV,
  const void *pKey,
  int nKey,
  int *pExists
);

/* ========== KEY-VALUE OPERATIONS (SPECIFIC CF) ========== */

/*
** Insert or update a key-value pair in a specific column family.
**
** Parameters:
**   pCF      - Column family handle
**   pKey     - Pointer to key data
**   nKey     - Length of key in bytes
**   pValue   - Pointer to value data
**   nValue   - Length of value in bytes
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_cf_put(
  KVColumnFamily *pCF,
  const void *pKey,
  int nKey,
  const void *pValue,
  int nValue
);

/*
** Retrieve a value by key from a specific column family.
**
** Parameters:
**   pCF      - Column family handle
**   pKey     - Pointer to key data
**   nKey     - Length of key in bytes
**   ppValue  - Output pointer to value data (caller must free)
**   pnValue  - Output pointer to value length
**
** Returns:
**   KVSTORE_OK on success
**   KVSTORE_NOTFOUND if key doesn't exist
*/
int kvstore_cf_get(
  KVColumnFamily *pCF,
  const void *pKey,
  int nKey,
  void **ppValue,
  int *pnValue
);

/*
** Delete a key-value pair from a specific column family.
**
** Parameters:
**   pCF  - Column family handle
**   pKey - Pointer to key data
**   nKey - Length of key in bytes
**
** Returns:
**   KVSTORE_OK on success
**   KVSTORE_NOTFOUND if key doesn't exist
*/
int kvstore_cf_delete(
  KVColumnFamily *pCF,
  const void *pKey,
  int nKey
);

/*
** Check if a key exists in a specific column family.
**
** Parameters:
**   pCF     - Column family handle
**   pKey    - Pointer to key data
**   nKey    - Length of key in bytes
**   pExists - Output: 1 if exists, 0 if not
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_cf_exists(
  KVColumnFamily *pCF,
  const void *pKey,
  int nKey,
  int *pExists
);

/*
** Iterator structure for traversing the key-value store
*/
typedef struct KVIterator KVIterator;

/*
** Create an iterator for the default column family.
**
** Parameters:
**   pKV   - KVStore handle
**   ppIter - Output pointer to iterator
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_iterator_create(
  KVStore *pKV,
  KVIterator **ppIter
);

/*
** Create an iterator for a specific column family.
**
** Parameters:
**   pCF    - Column family handle
**   ppIter - Output pointer to iterator
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_cf_iterator_create(
  KVColumnFamily *pCF,
  KVIterator **ppIter
);

/*
** Move iterator to the first key-value pair.
**
** Parameters:
**   pIter - Iterator handle
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_iterator_first(KVIterator *pIter);

/*
** Move iterator to the next key-value pair.
**
** Parameters:
**   pIter - Iterator handle
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_iterator_next(KVIterator *pIter);

/*
** Check if iterator has reached the end.
**
** Parameters:
**   pIter - Iterator handle
**
** Returns:
**   1 if at end, 0 otherwise
*/
int kvstore_iterator_eof(KVIterator *pIter);

/*
** Get current key from iterator.
**
** Parameters:
**   pIter   - Iterator handle
**   ppKey   - Output pointer to key data (do not free)
**   pnKey   - Output pointer to key length
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_iterator_key(
  KVIterator *pIter,
  void **ppKey,
  int *pnKey
);

/*
** Get current value from iterator.
**
** Parameters:
**   pIter    - Iterator handle
**   ppValue  - Output pointer to value data (do not free)
**   pnValue  - Output pointer to value length
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_iterator_value(
  KVIterator *pIter,
  void **ppValue,
  int *pnValue
);

/*
** Close and free an iterator.
**
** Parameters:
**   pIter - Iterator handle
*/
void kvstore_iterator_close(KVIterator *pIter);

/*
** Begin a transaction.
**
** Parameters:
**   pKV      - KVStore handle
**   wrflag   - 1 for write transaction, 0 for read-only
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_begin(KVStore *pKV, int wrflag);

/*
** Commit the current transaction.
**
** Parameters:
**   pKV - KVStore handle
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_commit(KVStore *pKV);

/*
** Rollback the current transaction.
**
** Parameters:
**   pKV - KVStore handle
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_rollback(KVStore *pKV);

/*
** Get the last error message from a KVStore handle.
**
** Parameters:
**   pKV - KVStore handle
**
** Returns:
**   Error message string (do not free)
*/
const char *kvstore_errmsg(KVStore *pKV);

/*
** Statistics structure
*/
typedef struct KVStoreStats KVStoreStats;
struct KVStoreStats {
  u64 nPuts;       /* Number of put operations */
  u64 nGets;       /* Number of get operations */
  u64 nDeletes;    /* Number of delete operations */
  u64 nIterations; /* Number of iterations created */
  u64 nErrors;     /* Number of errors encountered */
};

/*
** Get statistics from the key-value store.
**
** Parameters:
**   pKV    - KVStore handle
**   pStats - Output statistics structure
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_stats(KVStore *pKV, KVStoreStats *pStats);

/*
** Perform an integrity check on the database.
**
** Parameters:
**   pKV      - KVStore handle
**   pzErrMsg - Output pointer to error message (caller must free with sqliteFree)
**
** Returns:
**   KVSTORE_OK if database is ok
**   KVSTORE_CORRUPT if corruption detected
*/
int kvstore_integrity_check(KVStore *pKV, char **pzErrMsg);

/*
** Synchronize the database to disk.
**
** Parameters:
**   pKV - KVStore handle
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_sync(KVStore *pKV);

#ifdef __cplusplus
}
#endif

#endif /* _KVSTORE_H_ */
