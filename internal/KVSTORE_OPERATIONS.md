# KVStore Layer Operations

All `kvstore_*` functions that make up the SNKV public API.
The kvstore layer sits on top of the B-tree layer, providing a simple
key-value interface with column families, iterators, and transactions.

---

## Architecture Context

```
 ┌──────────────────┐
 │  kvstore.c (API)  │  ◄── THIS DOCUMENT
 └──────────────────┘
          │
   btree.c  (B-tree)
          │
   pager.c (Page Cache / Journal)
          │
   os.c   (VFS / Disk I/O)
          │
        Disk
```

---

## Table of Contents

1.  [Data Structures](#1-data-structures)
2.  [Storage Model: BLOBKEY Encoding](#2-storage-model-blobkey-encoding)
3.  [kvstore_open](#3-kvstore_open)
4.  [kvstore_close](#4-kvstore_close)
5.  [kvstore_begin](#5-kvstore_begin)
6.  [kvstore_commit](#6-kvstore_commit)
7.  [kvstore_rollback](#7-kvstore_rollback)
8.  [kvstore_put](#8-kvstore_put)
9.  [kvstore_get](#9-kvstore_get)
10. [kvstore_delete](#10-kvstore_delete)
11. [kvstore_exists](#11-kvstore_exists)
12. [Column Family: kvstore_cf_create](#12-kvstore_cf_create)
13. [Column Family: kvstore_cf_open](#13-kvstore_cf_open)
14. [Column Family: kvstore_cf_drop](#14-kvstore_cf_drop)
15. [Column Family: kvstore_cf_list](#15-kvstore_cf_list)
16. [kvstore_iterator_create](#16-kvstore_iterator_create)
17. [kvstore_iterator_first / next / eof](#17-kvstore_iterator_first--next--eof)
18. [kvstore_iterator_key / value](#18-kvstore_iterator_key--value)
19. [kvstore_iterator_close](#19-kvstore_iterator_close)
20. [kvstore_prefix_iterator_create](#20-kvstore_prefix_iterator_create)
21. [kvstore_incremental_vacuum](#21-kvstore_incremental_vacuum)
22. [kvstore_integrity_check](#22-kvstore_integrity_check)
23. [kvstore_sync](#23-kvstore_sync)
24. [kvstore_stats](#24-kvstore_stats)
25. [kvstore_errmsg](#25-kvstore_errmsg)
26. [Thread Safety Model](#26-thread-safety-model)
27. [Auto-Transaction Pattern](#27-auto-transaction-pattern)

---

## 1. Data Structures

The kvstore layer has four main structures:

```
  ┌────────────────────────────────────────────────────────┐
  │ KVStore                                                │
  │   pBt          → Btree*           (B-tree handle)      │
  │   db           → sqlite3*         (required by btree)  │
  │   pKeyInfo     → KeyInfo*         (shared BLOBKEY cmp) │
  │   iMetaTable   → int              (CF metadata root)   │
  │   inTrans      → int              (0/1/2)              │
  │   isCorrupted  → int              (corruption flag)    │
  │   zErrMsg      → char*            (last error msg)     │
  │   pMutex       → kvstore_mutex*   (main lock)          │
  │   pDefaultCF   → KVColumnFamily*  (default CF)         │
  │   apCF[]       → KVColumnFamily** (open CFs)           │
  │   stats        → {nPuts, nGets, nDeletes, ...}         │
  └────────────────────────────────────────────────────────┘
           │
           │ pDefaultCF / apCF[i]
           ▼
  ┌────────────────────────────────────────────────────────┐
  │ KVColumnFamily                                         │
  │   pKV          → KVStore*         (parent store)       │
  │   zName        → char*            (CF name)            │
  │   iTable       → int              (btree root page)    │
  │   refCount     → int              (reference count)    │
  │   pMutex       → kvstore_mutex*   (per-CF lock)        │
  └────────────────────────────────────────────────────────┘

  ┌────────────────────────────────────────────────────────┐
  │ KVIterator                                             │
  │   pCF          → KVColumnFamily*  (column family)      │
  │   pCur         → BtCursor*        (btree cursor)       │
  │   eof          → int              (end-of-data flag)   │
  │   ownsTrans    → int              (owns read txn?)     │
  │   pKeyBuf      → void*            (reusable key buf)   │
  │   pValBuf      → void*            (reusable val buf)   │
  │   pPrefix      → void*            (prefix filter)      │
  │   nPrefix      → int              (prefix length)      │
  │   isValid      → int              (validity flag)      │
  └────────────────────────────────────────────────────────┘

  ┌────────────────────────────────────────────────────────┐
  │ KVStoreStats                                           │
  │   nPuts        → u64              (put count)          │
  │   nGets        → u64              (get count)          │
  │   nDeletes     → u64              (delete count)       │
  │   nIterations  → u64              (iterator count)     │
  │   nErrors      → u64              (error count)        │
  └────────────────────────────────────────────────────────┘
```

---

## 2. Storage Model: BLOBKEY Encoding

All user data is stored in BLOBKEY btree tables.  Each key-value pair
is encoded as a single blob entry:

```
  ┌─────────────────────────────────────────────────────────────┐
  │ BLOBKEY Cell Layout                                         │
  │                                                             │
  │  ┌────────────┬──────────────────┬───────────────────────┐  │
  │  │ key_len    │ key_bytes        │ value_bytes           │  │
  │  │ (4B, BE)   │ (nKey bytes)     │ (nValue bytes)        │  │
  │  └────────────┴──────────────────┴───────────────────────┘  │
  │  │◄── 4 ──►│◄───── nKey ──────►│◄────── nValue ───────►│  │
  │                                                             │
  │  Total size = 4 + nKey + nValue                             │
  │                                                             │
  │  The btree comparison function compares only the key        │
  │  portion (bytes 4..4+nKey), so entries are sorted by        │
  │  key in lexicographic order.  Two entries with the same     │
  │  key compare as equal → BtreeInsert overwrites (upsert).   │
  └─────────────────────────────────────────────────────────────┘

  Example: key="user:1", value="Alice"

  ┌──────────────┬──────────────────┬─────────────────────┐
  │ 00 00 00 06  │ u s e r : 1      │ A l i c e           │
  │ (key_len=6)  │ (6 bytes)        │ (5 bytes)           │
  └──────────────┴──────────────────┴─────────────────────┘
```

### CF Metadata Table (Internal)

Column family metadata uses INTKEY tables with FNV-1a hash as rowid:

```
  CF Metadata Table (INTKEY, root page stored in meta[3])
  ┌─────────────────────────────────────────────────────┐
  │ rowid = FNV-1a("logs")                              │
  │ data  = [00 00 00 04][l][o][g][s][root page 4B BE] │
  │          name_len     name_bytes   table_root       │
  ├─────────────────────────────────────────────────────┤
  │ rowid = FNV-1a("metrics")                           │
  │ data  = [00 00 00 07][m][e][t][r][i][c][s][root]   │
  └─────────────────────────────────────────────────────┘

  Database Meta Pages (page 1 header):
    meta[1] = Default CF root page
    meta[2] = CF count
    meta[3] = CF metadata table root page
```

---

## 3. kvstore_open

Opens a key-value store database file.  Handles both new and existing databases.

```
  kvstore_open("test.db", &kv, KVSTORE_JOURNAL_WAL)
          │
          ├── Step 1: Initialize SQLite library
          │     sqlite3_initialize()
          │
          ├── Step 2: Allocate KVStore structure
          │     ┌──────────────────────────────────┐
          │     │ KVStore                           │
          │     │   pBt      = NULL                 │
          │     │   db       = alloc sqlite3{}      │
          │     │   pMutex   = kvstore_mutex_alloc() │
          │     │   inTrans  = 0                    │
          │     └──────────────────────────────────┘
          │
          ├── Step 3: Open B-tree
          │     sqlite3BtreeOpen(vfs, "test.db", db, &pBt, flags=0, ...)
          │       │
          │       └── PagerOpen → OsOpen → disk file
          │
          ├── Step 4: Configure cache
          │     sqlite3BtreeSetCacheSize(pBt, 2000)
          │
          ├── Step 5: Enable incremental auto-vacuum
          │     sqlite3BtreeSetAutoVacuum(pBt, BTREE_AUTOVACUUM_INCR)
          │     ┌─────────────────────────────────────────────┐
          │     │ New DB:  Sets mode in header → SQLITE_OK    │
          │     │ Old DB:  Mode already fixed → SQLITE_READONLY │
          │     │          (silently ignored — header wins)    │
          │     └─────────────────────────────────────────────┘
          │
          ├── Step 6: Set journal mode
          │     sqlite3BtreeSetVersion(pBt, ver)
          │     ┌───────────────────────────────────────┐
          │     │ ver=1 → Rollback journal (DELETE mode) │
          │     │ ver=2 → WAL mode                       │
          │     └───────────────────────────────────────┘
          │     sqlite3BtreeCommit(pBt)
          │
          ├── Step 7: Detect new vs existing database
          │     sqlite3BtreeBeginTrans(pBt, 0, 0)  ← read txn
          │     sqlite3BtreeGetMeta(pBt, META_DEFAULT_CF_ROOT)
          │     sqlite3BtreeGetMeta(pBt, META_CF_METADATA_ROOT)
          │     sqlite3BtreeCommit(pBt)
          │
          │     ┌────────────────────────────────────────────┐
          │     │ defaultCFRoot == 0?                        │
          │     │   YES → New database (needs initialization) │
          │     │   NO  → Existing database (open CF)        │
          │     └────────────────────────────────────────────┘
          │
          ├── Step 7a: [NEW DB] Initialize
          │     │
          │     ├── createDefaultCF()
          │     │     BtreeBeginTrans(wrflag=1)
          │     │     BtreeCreateTable(&pgno, BTREE_BLOBKEY)
          │     │     BtreeUpdateMeta(META_DEFAULT_CF_ROOT, pgno)
          │     │     BtreeCommit()
          │     │     Allocate KVColumnFamily{iTable=pgno}
          │     │
          │     └── initCFMetadataTable()
          │           BtreeBeginTrans(wrflag=1)
          │           BtreeCreateTable(&pgno, BTREE_INTKEY)
          │           BtreeUpdateMeta(META_CF_METADATA_ROOT, pgno)
          │           BtreeUpdateMeta(META_CF_COUNT, 1)
          │           BtreeCommit()
          │
          ├── Step 7b: [EXISTING DB] Restore state
          │     Allocate KVColumnFamily{iTable=defaultCFRoot}
          │     pKV->iMetaTable = cfMetaRoot
          │
          ├── Step 8: Allocate shared KeyInfo
          │     ┌─────────────────────────────────────────┐
          │     │ KeyInfo                                  │
          │     │   enc       = SQLITE_UTF8                │
          │     │   nKeyField = 1                          │
          │     │   db        = pKV->db                    │
          │     │   Used by all BLOBKEY cursor comparisons  │
          │     └─────────────────────────────────────────┘
          │
          └── Return KVSTORE_OK, *ppKV = pKV

  New database on disk after open:
  ┌────────────────────────────────────────────────┐
  │ Page 1: File header + schema root (master page) │
  │ Page 2: Default CF table root (BLOBKEY, empty)  │
  │ Page 3: CF metadata table root (INTKEY, empty)  │
  │ Page 4: Pointer-map page (auto-vacuum)          │
  └────────────────────────────────────────────────┘
```

---

## 4. kvstore_close

Closes a key-value store and frees all resources.

```
  kvstore_close(kv)
          │
          ├── kvstore_mutex_enter(pKV->pMutex)
          │
          ├── Rollback any active transaction
          │     if inTrans:
          │       sqlite3BtreeRollback(pBt, SQLITE_OK, 0)
          │       pKV->inTrans = 0
          │
          ├── Close default column family
          │     kvstore_mutex_free(pDefaultCF->pMutex)
          │     free(pDefaultCF->zName)
          │     free(pDefaultCF)
          │
          ├── Close all open column families
          │     for each apCF[i]:
          │       kvstore_mutex_free(apCF[i]->pMutex)
          │       free(apCF[i]->zName)
          │       free(apCF[i])
          │     free(apCF)
          │
          ├── sqlite3BtreeClose(pBt)
          │     │
          │     ├── PagerClose → OsClose → close fd
          │     └── Free B-tree memory
          │
          ├── free(pKeyInfo)         ── shared KeyInfo
          ├── free(zErrMsg)          ── error message
          ├── sqlite3_mutex_free(db->mutex)
          ├── free(db)               ── sqlite3 struct
          │
          ├── kvstore_mutex_leave(pKV->pMutex)
          ├── kvstore_mutex_free(pKV->pMutex)
          └── free(pKV)
```

---

## 5. kvstore_begin

Begins a read or write transaction.

```
  kvstore_begin(kv, wrflag=1)
          │
          ├── kvstore_mutex_enter(pKV->pMutex)
          │
          ├── Check: not corrupted
          ├── Check: no active transaction (inTrans == 0)
          │
          ├── sqlite3BtreeBeginTrans(pBt, wrflag, 0)
          │     │
          │     ├── [wrflag=0] Read transaction
          │     │     PagerSharedLock → SHARED lock or WAL read lock
          │     │
          │     └── [wrflag=1] Write transaction
          │           PagerSharedLock → PagerBegin
          │           → RESERVED lock or WAL write lock
          │
          ├── pKV->inTrans = (wrflag ? 2 : 1)
          │     ┌─────────────────────────────┐
          │     │ Transaction States:          │
          │     │   0 = no transaction         │
          │     │   1 = read transaction       │
          │     │   2 = write transaction      │
          │     └─────────────────────────────┘
          │
          └── kvstore_mutex_leave(pKV->pMutex)
```

---

## 6. kvstore_commit

Commits the current transaction, flushing changes to disk.

```
  kvstore_commit(kv)
          │
          ├── kvstore_mutex_enter(pKV->pMutex)
          │
          ├── Check: inTrans > 0
          │
          ├── sqlite3BtreeCommit(pBt)
          │     │
          │     ├── [Rollback mode]
          │     │     CommitPhaseOne:
          │     │       sync journal → write dirty pages → sync DB
          │     │     CommitPhaseTwo:
          │     │       delete journal → downgrade lock
          │     │
          │     └── [WAL mode]
          │           CommitPhaseOne:
          │             write dirty pages as WAL frames → sync WAL
          │           CommitPhaseTwo:
          │             release WAL write lock
          │
          ├── pKV->inTrans = 0
          │
          └── kvstore_mutex_leave(pKV->pMutex)

  Commit data flow (Rollback mode):
  ┌──────────┐    ┌───────────┐    ┌──────────┐
  │ Modified  │───►│  Journal   │───►│ Database  │
  │ Pages in  │    │  (backup)  │    │  File     │
  │ Cache     │    └───────────┘    └──────────┘
  └──────────┘         sync            sync
                                    then delete journal

  Commit data flow (WAL mode):
  ┌──────────┐    ┌──────────┐
  │ Modified  │───►│ WAL File  │
  │ Pages in  │    │ (append)  │
  │ Cache     │    └──────────┘
  └──────────┘       sync
```

---

## 7. kvstore_rollback

Rolls back the current transaction, discarding all uncommitted changes.

```
  kvstore_rollback(kv)
          │
          ├── kvstore_mutex_enter(pKV->pMutex)
          │
          ├── if !inTrans → return OK (no-op)
          │
          ├── sqlite3BtreeRollback(pBt, SQLITE_OK, 0)
          │     │
          │     ├── [Rollback mode]
          │     │     Replay journal → restore original pages
          │     │     Delete journal → release lock
          │     │
          │     └── [WAL mode]
          │           Discard WAL frames → release write lock
          │
          ├── pKV->inTrans = 0
          │
          └── kvstore_mutex_leave(pKV->pMutex)
```

---

## 8. kvstore_put

Inserts or updates a key-value pair.  If the key exists, the value is
overwritten (upsert semantics).

```
  kvstore_put(kv, "user:1", 6, "Alice", 5)
          │
          │  (delegates to kvstore_cf_put_internal on default CF)
          │
          ├── Acquire locks: CF mutex → KV mutex
          │
          ├── Validate key/value sizes
          │     ┌──────────────────────────────────┐
          │     │ key:  non-null, 1..64KB           │
          │     │ value: null OK if nValue==0, ≤10MB │
          │     └──────────────────────────────────┘
          │
          ├── Auto-transaction if needed
          │     if !inTrans: BtreeBeginTrans(wrflag=1)
          │
          ├── Open cursor on CF's btree table
          │     kvstoreAllocCursor()
          │     sqlite3BtreeCursor(pBt, iTable, wrflag=1, pKeyInfo, pCur)
          │
          ├── Encode key-value as BLOBKEY blob
          │     kvstoreEncodeBlob("user:1", 6, "Alice", 5)
          │     ┌──────────────────────────────────────────┐
          │     │ [00 00 00 06][user:1][Alice]              │
          │     │  key_len=6    key     value               │
          │     │  total = 4 + 6 + 5 = 15 bytes            │
          │     └──────────────────────────────────────────┘
          │
          ├── Insert into btree
          │     BtreePayload { pKey=blob, nKey=15 }
          │     sqlite3BtreeInsert(pCur, &payload, 0, 0)
          │     │
          │     ├── IndexMoveto → find correct leaf position
          │     │     ┌───────────────────────────────────────┐
          │     │     │ B-tree (sorted by key bytes)           │
          │     │     │                [root]                   │
          │     │     │              /        \                 │
          │     │     │        [internal]    [internal]         │
          │     │     │        /    \         /    \            │
          │     │     │   [leaf]  [leaf]  [leaf]  [leaf]       │
          │     │     │    ▲                                   │
          │     │     │    Insert here (key-ordered position)  │
          │     │     └───────────────────────────────────────┘
          │     │
          │     ├── Key exists?
          │     │     YES → overwrite cell (upsert)
          │     │     NO  → insert new cell, rebalance if needed
          │     │
          │     └── PagerWrite() on modified pages
          │
          ├── Close cursor
          │     kvstoreFreeCursor(pCur)
          │
          ├── pKV->stats.nPuts++
          │
          ├── Auto-commit if auto-transaction
          │     BtreeCommit(pBt)
          │
          └── Release locks: KV mutex → CF mutex
```

---

## 9. kvstore_get

Retrieves a value by key.  Returns KVSTORE_NOTFOUND if the key doesn't exist.

```
  kvstore_get(kv, "user:1", 6, &val, &nVal)
          │
          │  (delegates to kvstore_cf_get_internal on default CF)
          │
          ├── Acquire locks: CF mutex → KV mutex
          │
          ├── Validate key
          │
          ├── Auto-transaction if needed (read-only)
          │     if !inTrans: BtreeBeginTrans(wrflag=0)
          │
          ├── Open read cursor on CF's btree table
          │     sqlite3BtreeCursor(pBt, iTable, wrflag=0, pKeyInfo, pCur)
          │
          ├── Seek to key using custom comparator
          │     kvstoreSeekKey(pCur, pKeyInfo, "user:1", 6, &found)
          │     │
          │     ├── Allocate UnpackedRecord{u.z="user:1", n=6}
          │     ├── sqlite3BtreeIndexMoveto(pCur, pIdxKey, &res)
          │     │     │
          │     │     │  Traverses B-tree from root → leaf
          │     │     │  At each node, compares search key with
          │     │     │  cell keys (key bytes only, skipping header)
          │     │     │
          │     │     └── res=0: exact match found
          │     │         res<0: cursor before nearest key
          │     │         res>0: cursor after nearest key
          │     │
          │     └── found = (res == 0)
          │
          ├── [NOT FOUND] → return KVSTORE_NOTFOUND
          │
          ├── [FOUND] Read value from cursor payload
          │     ┌───────────────────────────────────────────┐
          │     │ Payload on disk:                           │
          │     │  [00 00 00 06][user:1][Alice]              │
          │     │   ▲            ▲       ▲                   │
          │     │   offset 0     4       10                  │
          │     │                                            │
          │     │ 1. Read header (4 bytes at offset 0)       │
          │     │    → key_len = 6                           │
          │     │                                            │
          │     │ 2. value_len = payloadSize - 4 - 6 = 5    │
          │     │                                            │
          │     │ 3. BtreePayload(pCur, offset=10, len=5)   │
          │     │    → copies "Alice" into new buffer        │
          │     └───────────────────────────────────────────┘
          │
          ├── Close cursor
          ├── Auto-commit if auto-transaction
          │
          ├── pKV->stats.nGets++
          ├── *ppValue = "Alice" (caller must free)
          ├── *pnValue = 5
          │
          └── Release locks: KV mutex → CF mutex
```

---

## 10. kvstore_delete

Deletes a key-value pair.  Returns KVSTORE_NOTFOUND if key doesn't exist.

```
  kvstore_delete(kv, "user:1", 6)
          │
          │  (delegates to kvstore_cf_delete_internal on default CF)
          │
          ├── Acquire locks: CF mutex → KV mutex
          │
          ├── Validate key
          │
          ├── Auto-transaction if needed (write)
          │     if !inTrans: BtreeBeginTrans(wrflag=1)
          │
          ├── Open write cursor
          │     sqlite3BtreeCursor(pBt, iTable, wrflag=1, pKeyInfo, pCur)
          │
          ├── Seek to key
          │     kvstoreSeekKey(pCur, pKeyInfo, "user:1", 6, &found)
          │
          ├── [NOT FOUND] → return KVSTORE_NOTFOUND
          │
          ├── [FOUND] Delete the cell
          │     sqlite3BtreeDelete(pCur, 0)
          │     │
          │     ├── Remove cell from leaf page
          │     │     ┌──────────────────────────────────────┐
          │     │     │ Leaf Page (before)                    │
          │     │     │  [cell: user:0][cell: user:1][cell: user:2] │
          │     │     │                 ▲                     │
          │     │     │              delete this              │
          │     │     │                                      │
          │     │     │ Leaf Page (after)                     │
          │     │     │  [cell: user:0][cell: user:2]         │
          │     │     └──────────────────────────────────────┘
          │     │
          │     ├── If page underflows → rebalance B-tree
          │     │
          │     └── Freed pages → freelist
          │           (NOT returned to OS until vacuum)
          │
          ├── Close cursor
          ├── pKV->stats.nDeletes++
          ├── Auto-commit if auto-transaction
          │
          └── Release locks: KV mutex → CF mutex
```

---

## 11. kvstore_exists

Checks if a key exists without reading the value.

```
  kvstore_exists(kv, "user:1", 6, &exists)
          │
          ├── Acquire locks: CF mutex → KV mutex
          │
          ├── Auto-transaction if needed (read-only)
          │
          ├── Open read cursor
          │     sqlite3BtreeCursor(pBt, iTable, 0, pKeyInfo, pCur)
          │
          ├── Seek to key
          │     kvstoreSeekKey(pCur, pKeyInfo, "user:1", 6, &found)
          │
          ├── Close cursor
          ├── Auto-commit if auto-transaction
          │
          ├── *pExists = found  (1 or 0)
          │
          └── Release locks

  Note: More efficient than kvstore_get because no value
  data is copied — only the B-tree traversal is performed.
```

---

## 12. kvstore_cf_create

Creates a new column family (logical namespace).

```
  kvstore_cf_create(kv, "logs", &cf)
          │
          ├── Validate name: 1..255 chars, not "default"
          │
          ├── kvstore_mutex_enter(pKV->pMutex)
          │
          ├── Auto-transaction if needed (write)
          │
          ├── Check if CF already exists
          │     Open cursor on metadata table (INTKEY)
          │     kvstoreMetaSeekKey("logs", 4, &found, NULL)
          │     │
          │     └── Hash "logs" → FNV-1a rowid
          │         Probe metadata table for matching entry
          │         (linear probing for collisions)
          │
          ├── [EXISTS] → return error
          │
          ├── Create new BLOBKEY table for this CF
          │     sqlite3BtreeCreateTable(pBt, &pgno, BTREE_BLOBKEY)
          │     ┌────────────────────────────────────────────┐
          │     │ Before:                                     │
          │     │  Page 1: header  │ Page 2: default CF       │
          │     │  Page 3: meta    │ Page 4: ptrmap           │
          │     │                                             │
          │     │ After:                                      │
          │     │  Page 1: header  │ Page 2: default CF       │
          │     │  Page 3: meta    │ Page 4: ptrmap           │
          │     │  Page 5: "logs" CF root (new, empty)        │
          │     └────────────────────────────────────────────┘
          │
          ├── Store CF metadata in metadata table
          │     Encode: [name_len|"logs"|table_root_4B_BE]
          │     Find empty slot: kvstoreMetaFindSlot()
          │     BtreeInsert(pCur, payload)
          │
          ├── Update CF count in meta
          │     BtreeGetMeta(META_CF_COUNT)  → cfCount
          │     cfCount++
          │     BtreeUpdateMeta(META_CF_COUNT, cfCount)
          │
          ├── Auto-commit
          │
          ├── Allocate KVColumnFamily structure
          │     ┌────────────────────────────────┐
          │     │ KVColumnFamily                  │
          │     │   pKV    = kv                   │
          │     │   zName  = "logs"               │
          │     │   iTable = pgno (new root page) │
          │     │   refCount = 1                  │
          │     │   pMutex = new mutex             │
          │     └────────────────────────────────┘
          │
          └── Return KVSTORE_OK, *ppCF = cf
```

---

## 13. kvstore_cf_open

Opens an existing column family by name.

```
  kvstore_cf_open(kv, "logs", &cf)
          │
          ├── If name == "default" → kvstore_cf_get_default()
          │
          ├── kvstore_mutex_enter(pKV->pMutex)
          │
          ├── Auto-transaction (read-only)
          │
          ├── Look up CF in metadata table
          │     Open cursor on metadata table
          │     kvstoreMetaSeekKey("logs", 4, &found, &foundRowid)
          │     │
          │     └── Hash "logs" → FNV-1a rowid
          │         Probe entries, compare stored name bytes
          │
          ├── [NOT FOUND] → return KVSTORE_NOTFOUND
          │
          ├── Read table root from metadata payload
          │     BtreePayload(pCur, offset=4+nameLen, len=4, rootBytes)
          │     iTable = decode_BE(rootBytes)
          │
          ├── Auto-commit
          │
          ├── Allocate KVColumnFamily{iTable=iTable, zName="logs"}
          │
          └── Return KVSTORE_OK, *ppCF = cf
```

---

## 14. kvstore_cf_drop

Drops a column family, deleting all its data and metadata.

```
  kvstore_cf_drop(kv, "logs")
          │
          ├── Cannot drop "default" → error
          │
          ├── kvstore_mutex_enter(pKV->pMutex)
          │
          ├── Auto-transaction (write)
          │
          ├── Find CF in metadata table
          │     kvstoreMetaSeekKey("logs") → found, foundRowid
          │     Read table root from payload → iTable
          │
          ├── Drop the CF's btree table
          │     sqlite3BtreeDropTable(pBt, iTable, &iMoved)
          │     ┌────────────────────────────────────────────┐
          │     │ All data pages for "logs" CF are freed      │
          │     │ Root page released back to freelist         │
          │     │ (file does NOT shrink — use vacuum)         │
          │     └────────────────────────────────────────────┘
          │
          ├── Delete metadata row
          │     BtreeTableMoveto(pCur, foundRowid)
          │     sqlite3BtreeDelete(pCur, 0)
          │
          ├── Decrement CF count
          │     BtreeGetMeta(META_CF_COUNT) → cfCount
          │     cfCount--
          │     BtreeUpdateMeta(META_CF_COUNT, cfCount)
          │
          ├── Auto-commit
          │
          └── Return KVSTORE_OK
```

---

## 15. kvstore_cf_list

Lists all column families in the database.

```
  kvstore_cf_list(kv, &names, &count)
          │
          ├── kvstore_mutex_enter(pKV->pMutex)
          │
          ├── Auto-transaction (read-only)
          │
          ├── Open cursor on metadata table
          │     sqlite3BtreeCursor(pBt, iMetaTable, 0, NULL, pCur)
          │
          ├── Scan all entries
          │     sqlite3BtreeFirst(pCur, &res)
          │     while !res:
          │       ┌────────────────────────────────────────┐
          │       │ Read payload header (4 bytes) → nameLen │
          │       │ Read name bytes (nameLen bytes)         │
          │       │ Append name to output array             │
          │       └────────────────────────────────────────┘
          │       sqlite3BtreeNext(pCur, 0)
          │
          ├── Close cursor, auto-commit
          │
          ├── *pazNames = ["logs", "metrics", ...]
          │   *pnCount  = N
          │
          └── Return KVSTORE_OK
```

---

## 16. kvstore_iterator_create

Creates an iterator for traversing all key-value pairs.

```
  kvstore_iterator_create(kv, &iter)
          │
          │  (delegates to kvstore_cf_iterator_create on default CF)
          │
          ├── kvstore_mutex_enter(pKV->pMutex)
          │
          ├── Allocate KVIterator structure
          │     ┌──────────────────────────────────┐
          │     │ KVIterator                        │
          │     │   pCF      = defaultCF            │
          │     │   eof      = 1  (not positioned)  │
          │     │   isValid  = 1                    │
          │     │   ownsTrans = ?                   │
          │     │   pPrefix  = NULL (no filter)     │
          │     └──────────────────────────────────┘
          │
          ├── Auto-transaction if needed (read-only)
          │     if !inTrans:
          │       BtreeBeginTrans(wrflag=0)
          │       iter->ownsTrans = 1
          │
          ├── Allocate B-tree cursor
          │     kvstoreAllocCursor()
          │     sqlite3BtreeCursor(pBt, iTable, 0, pKeyInfo, pCur)
          │
          ├── pKV->stats.nIterations++
          │
          ├── kvstore_mutex_leave(pKV->pMutex)
          │
          └── Return KVSTORE_OK, *ppIter = iter

  Note: The iterator is NOT yet positioned.
  Call kvstore_iterator_first() to move to the first entry.
```

---

## 17. kvstore_iterator_first / next / eof

Positions and advances the iterator through key-value pairs.

```
  kvstore_iterator_first(iter)
          │
          ├── [No prefix filter]
          │     sqlite3BtreeFirst(pCur, &res)
          │     iter->eof = res  (1 if table empty, 0 otherwise)
          │     ┌──────────────────────────────────────────┐
          │     │ B-tree leaf pages (sorted order):        │
          │     │  [aaa][bbb][ccc][ddd][eee]...            │
          │     │   ▲                                      │
          │     │   cursor positioned here                 │
          │     └──────────────────────────────────────────┘
          │
          └── [With prefix filter]  (see 20)
                Seek to first key >= prefix
                Check if key starts with prefix
                iter->eof = !match


  kvstore_iterator_next(iter)
          │
          ├── if iter->eof → return OK (no-op)
          │
          ├── sqlite3BtreeNext(pCur, 0)
          │     │
          │     ├── SQLITE_OK   → moved to next entry
          │     │     ┌──────────────────────────────────┐
          │     │     │ [aaa][bbb][ccc][ddd][eee]...     │
          │     │     │        ▲                         │
          │     │     │        cursor moves right        │
          │     │     └──────────────────────────────────┘
          │     │
          │     └── SQLITE_DONE → no more entries
          │           iter->eof = 1
          │
          └── [With prefix] Check prefix bound
                if key doesn't start with prefix:
                  iter->eof = 1  (stop early)


  kvstore_iterator_eof(iter)
          │
          └── return iter->eof  (1 = at end, 0 = has data)
```

---

## 18. kvstore_iterator_key / value

Reads the current key or value from the iterator's cursor position.

```
  kvstore_iterator_key(iter, &key, &nKey)
          │
          ├── Get payload size from cursor
          │     payloadSz = sqlite3BtreePayloadSize(pCur)
          │
          ├── Read key_len header (4 bytes at offset 0)
          │     BtreePayload(pCur, 0, 4, hdr)
          │     keyLen = decode_BE(hdr)
          │
          ├── Grow reusable key buffer if needed
          │     if nKeyBuf < keyLen:
          │       pKeyBuf = realloc(pKeyBuf, keyLen)
          │
          ├── Read key bytes into buffer
          │     BtreePayload(pCur, 4, keyLen, pKeyBuf)
          │     ┌────────────────────────────────────────────┐
          │     │ Cell:  [00 00 00 06][user:1][Alice]        │
          │     │                      ▲──────▲              │
          │     │                      read these 6 bytes    │
          │     └────────────────────────────────────────────┘
          │
          ├── *ppKey = pKeyBuf  (do NOT free — owned by iterator)
          └── *pnKey = keyLen


  kvstore_iterator_value(iter, &val, &nVal)
          │
          ├── Read key_len header (4 bytes)
          │     keyLen = decode_BE(hdr)
          │     valLen = payloadSz - 4 - keyLen
          │
          ├── Grow reusable value buffer if needed
          │
          ├── Read value bytes into buffer
          │     BtreePayload(pCur, 4 + keyLen, valLen, pValBuf)
          │     ┌────────────────────────────────────────────┐
          │     │ Cell:  [00 00 00 06][user:1][Alice]        │
          │     │                              ▲─────▲       │
          │     │                              read 5 bytes  │
          │     └────────────────────────────────────────────┘
          │
          ├── *ppValue = pValBuf  (do NOT free — owned by iterator)
          └── *pnValue = valLen
```

---

## 19. kvstore_iterator_close

Closes an iterator and frees all associated resources.

```
  kvstore_iterator_close(iter)
          │
          ├── iter->isValid = 0
          │
          ├── Close B-tree cursor
          │     sqlite3BtreeCloseCursor(pCur)
          │     free(pCur)
          │
          ├── End auto-transaction if owned
          │     if iter->ownsTrans:
          │       kvstore_mutex_enter(pKV->pMutex)
          │       sqlite3BtreeCommit(pBt)
          │       pKV->inTrans = 0
          │       kvstore_mutex_leave(pKV->pMutex)
          │
          ├── Release CF reference
          │     kvstore_cf_close(pCF)  → refCount--
          │
          ├── Free buffers
          │     free(pKeyBuf)
          │     free(pValBuf)
          │     free(pPrefix)
          │
          └── free(iter)
```

---

## 20. kvstore_prefix_iterator_create

Creates an iterator that only returns keys starting with a given prefix.

```
  kvstore_prefix_iterator_create(kv, "user:", 5, &iter)
          │
          ├── Create normal iterator via kvstore_cf_iterator_create()
          │
          ├── Store prefix copy in iterator
          │     iter->pPrefix = malloc(5)
          │     memcpy(iter->pPrefix, "user:", 5)
          │     iter->nPrefix = 5
          │
          ├── Position at first matching key
          │     kvstore_iterator_first(iter)
          │     │
          │     ├── Build UnpackedRecord for prefix
          │     │     pIdxKey = { u.z="user:", n=5 }
          │     │
          │     ├── sqlite3BtreeIndexMoveto(pCur, pIdxKey, &res)
          │     │     ┌───────────────────────────────────────────┐
          │     │     │ B-tree entries (sorted):                   │
          │     │     │  [admin:1][admin:2][user:1][user:2][zzz]  │
          │     │     │                    ▲                       │
          │     │     │                    cursor lands here       │
          │     │     └───────────────────────────────────────────┘
          │     │
          │     ├── if res < 0: BtreeNext() to advance past
          │     │
          │     └── Check prefix match
          │           kvstoreIterCheckPrefix(iter)
          │           Read key_len header → read prefix bytes
          │           memcmp(stored_key, "user:", 5) == 0?
          │
          └── Return iterator (already positioned)

  Subsequent next() calls:
  ┌───────────────────────────────────────────────────────┐
  │ kvstore_iterator_next(iter)                           │
  │   BtreeNext(pCur)                                     │
  │   kvstoreIterCheckPrefix(iter)                        │
  │     key starts with "user:"?                          │
  │       YES → continue iteration                        │
  │       NO  → iter->eof = 1 (stop)                     │
  │                                                       │
  │ Keys returned: [user:1][user:2]                       │
  │ Keys skipped:  [zzz] (prefix mismatch → EOF)          │
  └───────────────────────────────────────────────────────┘
```

---

## 21. kvstore_incremental_vacuum

Reclaims unused pages and shrinks the database file.
Databases are always opened with incremental auto-vacuum enabled.

```
  kvstore_incremental_vacuum(kv, nPage)
          │
          ├── kvstore_mutex_enter(pKV->pMutex)
          │
          ├── Check: not corrupted
          │
          ├── Ensure write transaction
          │     if !inTrans:  BtreeBeginTrans(wrflag=2)
          │     if inTrans==1: upgrade to write txn
          │
          ├── Run vacuum steps
          │     │
          │     ├── [nPage <= 0] Free ALL unused pages
          │     │     do {
          │     │       rc = sqlite3BtreeIncrVacuum(pBt)
          │     │     } while (rc == SQLITE_OK)
          │     │     // SQLITE_DONE means no more pages to free
          │     │
          │     └── [nPage > 0] Free up to nPage pages
          │           for i = 0..nPage-1:
          │             rc = sqlite3BtreeIncrVacuum(pBt)
          │             if rc != SQLITE_OK: break
          │
          │     Each BtreeIncrVacuum step:
          │     ┌──────────────────────────────────────────────┐
          │     │                                              │
          │     │  Database file BEFORE vacuum step:           │
          │     │  [P1][P2][P3][P4][P5][P6][P7][P8]           │
          │     │              free        ▲ last page         │
          │     │                          │                   │
          │     │  1. Find last page in file (P8)              │
          │     │  2. If P8 is a free page → just truncate     │
          │     │  3. If P8 has data:                          │
          │     │     a. Move P8's content → free page (P4)    │
          │     │     b. Update pointer-map references         │
          │     │     c. Decrement page count                  │
          │     │                                              │
          │     │  Database file AFTER vacuum step:            │
          │     │  [P1][P2][P3][P8'][P5][P6][P7]              │
          │     │              ▲ moved    truncated ───►       │
          │     │                                              │
          │     │  File physically shrinks by 1 page on commit │
          │     └──────────────────────────────────────────────┘
          │
          ├── Auto-commit if auto-transaction
          │     BtreeCommit(pBt)
          │     → PagerCommitPhaseOne → truncate file
          │     → PagerCommitPhaseTwo → delete journal
          │
          ├── kvstore_mutex_leave(pKV->pMutex)
          │
          └── Return KVSTORE_OK

  File size timeline:
  ┌───────────────────────────────────────────────────────┐
  │                                                       │
  │  insert 2000 records → file grows to ~500KB           │
  │  delete 1800 records → file stays ~500KB              │
  │                         (freed pages on freelist)     │
  │  kvstore_incremental_vacuum(kv, 0)                    │
  │                       → file shrinks to ~50KB          │
  │                                                       │
  │  Without vacuum: freed pages are reused for future    │
  │  inserts but the file NEVER shrinks.                  │
  └───────────────────────────────────────────────────────┘
```

---

## 22. kvstore_integrity_check

Performs a deep integrity check on the entire database.

```
  kvstore_integrity_check(kv, &errMsg)
          │
          ├── kvstore_mutex_enter(pKV->pMutex)
          │
          ├── Auto-transaction (read-only)
          │
          ├── Build list of all root pages to check
          │     ┌──────────────────────────────────────────┐
          │     │ aRoot[] = {                               │
          │     │   1,              // page 1 (file header) │
          │     │   defaultCF.root, // default CF table     │
          │     │   iMetaTable,     // CF metadata table    │
          │     │   cf1.root,       // from metadata scan   │
          │     │   cf2.root,       // from metadata scan   │
          │     │   ...                                     │
          │     │ }                                         │
          │     └──────────────────────────────────────────┘
          │     (Scans metadata table cursor to find all CF roots)
          │
          ├── Run integrity check
          │     sqlite3BtreeIntegrityCheck(db, pBt, aRoot, aCnt,
          │                                nRoot, 100, &nErr, &zErr)
          │     │
          │     ├── Verify every page in the file:
          │     │     ┌────────────────────────────────────┐
          │     │     │ For each page:                      │
          │     │     │  - Check page header format         │
          │     │     │  - Verify cell pointers             │
          │     │     │  - Check free block list            │
          │     │     │  - Verify parent pointers (autovac) │
          │     │     │  - Ensure all pages accounted for   │
          │     │     │  - Validate btree ordering          │
          │     │     └────────────────────────────────────┘
          │     │
          │     └── Returns nErr=0 if OK, nErr>0 if corrupt
          │
          ├── Auto-commit
          │
          ├── [nErr > 0]
          │     pKV->isCorrupted = 1
          │     *pzErrMsg = error description (caller must free)
          │     return KVSTORE_CORRUPT
          │
          ├── [nErr == 0]
          │     return KVSTORE_OK
          │
          └── kvstore_mutex_leave(pKV->pMutex)
```

---

## 23. kvstore_sync

Synchronizes the database to disk.

```
  kvstore_sync(kv)
          │
          ├── kvstore_mutex_enter(pKV->pMutex)
          │
          ├── Check: not corrupted
          │
          ├── if inTrans == 2 (write transaction active):
          │     sqlite3BtreeCommit(pBt)
          │     │
          │     ├── [Rollback] sync journal → write pages → sync DB
          │     └── [WAL]     append frames → sync WAL
          │     │
          │     pKV->inTrans = 0
          │
          ├── if inTrans != 2:
          │     No-op (reads are already consistent)
          │
          └── kvstore_mutex_leave(pKV->pMutex)

  Note: This performs a commit, not just an fsync.
  After sync, there is no active transaction.
```

---

## 24. kvstore_stats

Returns operation statistics from the key-value store.

```
  kvstore_stats(kv, &stats)
          │
          ├── kvstore_mutex_enter(pKV->pMutex)
          │
          ├── Copy counters:
          │     stats.nPuts       = pKV->stats.nPuts
          │     stats.nGets       = pKV->stats.nGets
          │     stats.nDeletes    = pKV->stats.nDeletes
          │     stats.nIterations = pKV->stats.nIterations
          │     stats.nErrors     = pKV->stats.nErrors
          │
          └── kvstore_mutex_leave(pKV->pMutex)

  Counters are incremented atomically under the main mutex
  on each successful operation.
```

---

## 25. kvstore_errmsg

Returns the last error message.

```
  kvstore_errmsg(kv)
          │
          ├── kvstore_mutex_enter(pKV->pMutex)
          │
          ├── return pKV->zErrMsg ? pKV->zErrMsg : "no error"
          │
          └── kvstore_mutex_leave(pKV->pMutex)

  Note: The returned string is owned by KVStore — do NOT free it.
  The error message is overwritten on the next error.
```

---

## 26. Thread Safety Model

```
  ┌──────────────────────────────────────────────────────────┐
  │ Lock Hierarchy (always acquire in this order):           │
  │                                                          │
  │   1. CF mutex  (kvstore_mutex on KVColumnFamily)         │
  │   2. KV mutex  (kvstore_mutex on KVStore)                │
  │                                                          │
  │ Thread 1                    Thread 2                     │
  │ ─────────                   ─────────                    │
  │ cf_put("logs", k, v)       cf_put("metrics", k, v)      │
  │   lock(logs.mutex)           lock(metrics.mutex)         │
  │   lock(kv.mutex)             lock(kv.mutex)  ← waits    │
  │   BtreeInsert(...)           ...                         │
  │   unlock(kv.mutex)           lock(kv.mutex)  ← acquired │
  │   unlock(logs.mutex)         BtreeInsert(...)            │
  │                              unlock(kv.mutex)            │
  │                              unlock(metrics.mutex)       │
  │                                                          │
  │ Note: The underlying B-tree has its own mutex as well.   │
  │ KVStore mutexes protect KVStore-level state (inTrans,    │
  │ stats, error messages, CF list).                         │
  └──────────────────────────────────────────────────────────┘
```

---

## 27. Auto-Transaction Pattern

Most kvstore operations use an "auto-transaction" pattern: if no
transaction is active, the operation starts one automatically and
commits/rolls back when done.

```
  ┌──────────────────────────────────────────────────────────┐
  │ Auto-Transaction Flow                                    │
  │                                                          │
  │  kvstore_put/get/delete/exists(...)                      │
  │          │                                               │
  │          ├── inTrans == 0?                                │
  │          │     YES → BtreeBeginTrans(wrflag)             │
  │          │           autoTrans = 1                        │
  │          │           inTrans = 1 or 2                     │
  │          │                                               │
  │          │     NO  → use existing transaction             │
  │          │           autoTrans = 0                        │
  │          │                                               │
  │          ├── ... perform operation ...                    │
  │          │                                               │
  │          ├── [SUCCESS + autoTrans]                        │
  │          │     BtreeCommit(pBt)                           │
  │          │     inTrans = 0                                │
  │          │                                               │
  │          ├── [FAILURE + autoTrans]                        │
  │          │     BtreeRollback(pBt)                         │
  │          │     inTrans = 0                                │
  │          │                                               │
  │          └── [autoTrans == 0]                             │
  │                Transaction remains open for caller        │
  │                                                          │
  │  Benefit: Users can do single operations without          │
  │  explicit begin/commit, OR batch operations in a          │
  │  single transaction for atomicity + performance.          │
  │                                                          │
  │  Example — single operation (auto-transaction):          │
  │    kvstore_put(kv, "k", 1, "v", 1);                     │
  │    // internally: begin → insert → commit                │
  │                                                          │
  │  Example — batched operations (explicit transaction):    │
  │    kvstore_begin(kv, 1);                                 │
  │    kvstore_put(kv, "k1", 2, "v1", 2);  // no auto-txn   │
  │    kvstore_put(kv, "k2", 2, "v2", 2);  // no auto-txn   │
  │    kvstore_commit(kv);       // single commit for all    │
  └──────────────────────────────────────────────────────────┘
```

---


## Quick Reference: kvstore → btree mapping

| kvstore function                | btree calls                                          |
|---------------------------------|------------------------------------------------------|
| `kvstore_open`                  | `BtreeOpen`, `SetCacheSize`, `SetAutoVacuum`, `SetVersion`, `GetMeta`, `CreateTable`, `UpdateMeta` |
| `kvstore_close`                 | `BtreeRollback` (if txn), `BtreeClose`               |
| `kvstore_begin`                 | `BtreeBeginTrans(wrflag)`                            |
| `kvstore_commit`                | `BtreeCommit`                                        |
| `kvstore_rollback`              | `BtreeRollback`                                      |
| `kvstore_put`                   | `BtreeCursor`, `BtreeInsert`, `BtreeCloseCursor`     |
| `kvstore_get`                   | `BtreeCursor`, `BtreeIndexMoveto`, `BtreePayload`, `BtreeCloseCursor` |
| `kvstore_delete`                | `BtreeCursor`, `BtreeIndexMoveto`, `BtreeDelete`, `BtreeCloseCursor` |
| `kvstore_exists`                | `BtreeCursor`, `BtreeIndexMoveto`, `BtreeCloseCursor` |
| `kvstore_cf_create`             | `BtreeCreateTable(BLOBKEY)`, `BtreeInsert` (meta), `UpdateMeta` |
| `kvstore_cf_open`               | `BtreeCursor` (meta), `BtreeTableMoveto`, `BtreePayload` |
| `kvstore_cf_drop`               | `BtreeDropTable`, `BtreeDelete` (meta), `UpdateMeta` |
| `kvstore_cf_list`               | `BtreeCursor` (meta), `BtreeFirst`, `BtreeNext`, `BtreePayload` |
| `kvstore_iterator_create`       | `BtreeCursor`                                        |
| `kvstore_iterator_first`        | `BtreeFirst` or `BtreeIndexMoveto` (prefix)          |
| `kvstore_iterator_next`         | `BtreeNext`                                          |
| `kvstore_iterator_key/value`    | `BtreePayloadSize`, `BtreePayload`                   |
| `kvstore_iterator_close`        | `BtreeCloseCursor`, `BtreeCommit` (if owned txn)     |
| `kvstore_incremental_vacuum`    | `BtreeIncrVacuum` (loop)                             |
| `kvstore_integrity_check`       | `BtreeIntegrityCheck`                                |
| `kvstore_sync`                  | `BtreeCommit`                                        |

---

## Full Stack Trace Example

A complete `kvstore_put` showing all four layers:

```
  kvstore_put(kv, "hello", 5, "world", 5)
  │
  │ LAYER 4: kvstore.c
  ├── lock CF mutex → lock KV mutex
  ├── validate key/value
  ├── auto-begin write transaction
  ├── encode BLOBKEY: [00 00 00 05][hello][world]
  │
  │ LAYER 3: btree.c
  ├── sqlite3BtreeCursor(pBt, rootPage, 1, pKeyInfo, pCur)
  │     Opens cursor on CF's B-tree table
  ├── sqlite3BtreeInsert(pCur, {pKey=blob, nKey=15}, 0, 0)
  │   ├── BtreeIndexMoveto → traverse root → internal → leaf
  │   ├── insertCell → write cell into leaf page
  │   ├── balance_nonroot → split if page full
  │   └── return SQLITE_OK
  │
  │ LAYER 2: pager.c
  ├── sqlite3PagerGet(pgno) → fetch page from cache or disk
  ├── sqlite3PagerWrite(pDbPage) → mark dirty, journal backup
  ├── sqlite3PagerCommitPhaseOne → sync journal, write pages
  ├── sqlite3PagerCommitPhaseTwo → delete journal, release lock
  │
  │ LAYER 1: os.c / os_unix.c
  ├── sqlite3OsRead()  → read() syscall
  ├── sqlite3OsWrite() → write() syscall
  ├── sqlite3OsSync()  → fsync() syscall
  ├── sqlite3OsLock()  → fcntl() file lock
  └── sqlite3OsDelete() → unlink() journal
```
