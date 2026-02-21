# KVStore Layer Operations

All `kvstore_*` functions that make up the SNKV public API.
The kvstore layer sits on top of the B-tree layer, providing a simple
key-value interface with column families, iterators, and transactions.

---

## Architecture Context

```
 ┌─────────────────────────────────────────┐
 │           Application Code              │
 │  kvstore_open / put / get / close ...   │
 └────────────────────┬────────────────────┘
                      │ public API
 ┌────────────────────▼────────────────────┐
 │         kvstore.c  (THIS DOCUMENT)      │
 │                                         │
 │  Persistent read transaction (inTrans)  │
 │  Cached read cursor per CF (pReadCur)   │
 │  Stack-alloc UnpackedRecord per seek    │
 │  Stack blob encoding for small puts     │
 │  Two-level recursive mutex locking      │
 └────────────────────┬────────────────────┘
                      │ BtreeInsert / IndexMoveto / ...
 ┌────────────────────▼────────────────────┐
 │         btree.c  (B-tree Engine)        │
 └────────────────────┬────────────────────┘
                      │ PagerGet / PagerWrite / ...
 ┌────────────────────▼────────────────────┐
 │     pager.c  (Page Cache / Journal)     │
 └────────────────────┬────────────────────┘
                      │ OsRead / OsWrite / OsSync / ...
 ┌────────────────────▼────────────────────┐
 │        os.c / os_unix.c / os_windows.c (VFS) │
 └────────────────────┬────────────────────┘
                      │ read() / write() / fsync()
                   [ Disk ]
```

---

## Table of Contents

1.  [Data Structures](#1-data-structures)
2.  [Storage Model: BLOBKEY Encoding](#2-storage-model-blobkey-encoding)
3.  [kvstore_open_v2 / kvstore_open](#3-kvstore_open_v2--kvstore_open)
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
26. [Thread Safety Model & Mutex Migration](#26-thread-safety-model)
27. [Persistent Read Transaction & Auto-Transaction Pattern](#27-persistent-read-transaction--auto-transaction-pattern)
28. [kvstore_checkpoint + WAL Auto-Checkpoint](#28-kvstore_checkpoint--wal-auto-checkpoint)

---

## 1. Data Structures

```
  ┌──────────────────────────────────────────────────────────┐
  │ KVStore                                                  │
  │   pBt          → Btree*            B-tree handle         │
  │   db           → sqlite3*          required by btree     │
  │   pKeyInfo     → KeyInfo*          shared BLOBKEY cmp    │
  │   iMetaTable   → int               CF metadata root page │
  │   inTrans      → int               0=none 1=read 2=write │
  │   isCorrupted  → int               corruption flag       │
  │   closing      → int               1 while close() runs  │
  │   zErrMsg      → char*             last error message    │
  │   pMutex       → sqlite3_mutex*    SQLITE_MUTEX_RECURSIVE│
  │   pDefaultCF   → KVColumnFamily*   default column family │
  │   apCF[]       → KVColumnFamily**  open named CFs        │
  │   stats        → KVStoreStats      {nPuts nGets nDel ...}│
  │   walSizeLimit → int               auto-ckpt every N commits (0=off)│
  │   walCommits   → int               commits since last auto-ckpt     │
  │                                                          │
  │   db->busyHandler.xBusyHandler → kvstoreBusyHandler()   │
  │   db->busyHandler.pBusyArg     → KVBusyCtx* (or NULL)   │
  │   db->busyTimeout              → int (ms, or 0)         │
  └──────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────┐
  │ KVBusyCtx  (heap-allocated; set when busyTimeout > 0)   │
  │   timeoutMs    → int               max wait in ms        │
  │   pVfs         → sqlite3_vfs*      VFS for OsSleep       │
  └──────────────────────────────────────────────────────────┘
           │ pDefaultCF / apCF[i]
           ▼
  ┌──────────────────────────────────────────────────────────┐
  │ KVColumnFamily                                           │
  │   pKV          → KVStore*          parent store          │
  │   zName        → char*             CF name (heap)        │
  │   iTable       → int               B-tree root page      │
  │   refCount     → int               reference count       │
  │   pMutex       → sqlite3_mutex*    SQLITE_MUTEX_RECURSIVE│
  │   pReadCur     → BtCursor*         cached read cursor    │
  │                                    NULL = not open yet   │
  └──────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────┐
  │ KVIterator                                               │
  │   pCF          → KVColumnFamily*   column family         │
  │   pCur         → BtCursor*         B-tree cursor (own)   │
  │   eof          → int               1 = past last entry   │
  │   isValid      → int               1 = usable            │
  │   ownsTrans    → int               1 = started own txn   │
  │   pKeyBuf      → void*             reusable key buffer   │
  │   nKeyBuf      → int               key buffer size       │
  │   pValBuf      → void*             reusable value buffer │
  │   nValBuf      → int               value buffer size     │
  │   pPrefix      → void*             prefix filter (heap)  │
  │   nPrefix      → int               prefix length         │
  └──────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────┐
  │ KVStoreStats                                             │
  │   nPuts        → u64               put count             │
  │   nGets        → u64               get count             │
  │   nDeletes     → u64               delete count          │
  │   nIterations  → u64               iterator count        │
  │   nErrors      → u64               error count           │
  └──────────────────────────────────────────────────────────┘
```

---

## 2. Storage Model: BLOBKEY Encoding

All user data is stored in BLOBKEY B-tree tables. Each key-value pair
is encoded as a single blob entry:

```
  ┌─────────────────────────────────────────────────────────────┐
  │ BLOBKEY Cell Layout                                         │
  │                                                             │
  │  ┌─────────────┬──────────────────┬──────────────────────┐  │
  │  │  key_len    │    key_bytes     │    value_bytes       │  │
  │  │  (4 bytes   │   (nKey bytes)   │   (nValue bytes)     │  │
  │  │   big-endian│                  │                      │  │
  │  └─────────────┴──────────────────┴──────────────────────┘  │
  │   ◄── 4B ────►◄──── nKey ────────►◄──── nValue ──────────►  │
  │                                                             │
  │  Total encoded size = 4 + nKey + nValue                     │
  │                                                             │
  │  The B-tree comparison function reads only bytes [4..4+nKey]│
  │  so entries are sorted lexicographically by key.            │
  │  Same key → BtreeInsert overwrites (upsert semantics).      │
  │                                                             │
  │  Encoding is done by kvstoreEncodeBlob():                   │
  │    • Uses a caller-supplied stack buffer for small payloads │
  │    • Falls back to sqlite3Malloc only when payload > 512B   │
  │    • Returns total encoded length, or -1 on OOM             │
  └─────────────────────────────────────────────────────────────┘

  Example: put("user:1", 6, "Alice", 5)

  ┌──────────────┬──────────────────┬─────────────────────┐
  │ 00 00 00 06  │  u  s  e  r  :  1│  A  l  i  c  e     │
  │  key_len=6   │  (6 bytes)       │  (5 bytes)          │
  └──────────────┴──────────────────┴─────────────────────┘
   offset: 0      4                  10                    15
```

### CF Metadata Table (Internal)

Column family metadata uses an INTKEY B-tree. The rowid is the FNV-1a
hash of the CF name (with linear probing for collisions):

```
  CF Metadata Table (INTKEY, root page stored in page-1 meta[3])

  ┌──────────────────────────────────────────────────────────────┐
  │ rowid = FNV-1a("logs")                                       │
  │ data  = [00 00 00 04][l][o][g][s][root_page_4B_BE]          │
  │          name_len=4   name bytes    table root               │
  ├──────────────────────────────────────────────────────────────┤
  │ rowid = FNV-1a("metrics")                                    │
  │ data  = [00 00 00 07][m][e][t][r][i][c][s][root_4B_BE]      │
  └──────────────────────────────────────────────────────────────┘

  Page-1 header meta slots used by kvstore:
    meta[1] = Default CF root page number
    meta[2] = CF count
    meta[3] = CF metadata table root page number
```

---

## 3. kvstore_open_v2 / kvstore_open

`kvstore_open_v2` is the primary open function. It accepts a `KVStoreConfig`
struct to control journal mode, sync level, cache size, page size, read-only
access, and busy-retry timeout. `kvstore_open` is a thin backward-compatible
wrapper that delegates to `kvstore_open_v2`.

```
  kvstore_open_v2("test.db", &kv, &cfg)
          │
          ├── Step 1: sqlite3_initialize()
          │
          ├── Step 2: Resolve config defaults (zero-value → default)
          │     ┌────────────────────────────────────────────────────┐
          │     │ cfg == NULL        → all-zero KVStoreConfig        │
          │     │ journalMode == 0   → KVSTORE_JOURNAL_WAL           │
          │     │ syncLevel   == 0   → KVSTORE_SYNC_NORMAL           │
          │     │ cacheSize   == 0   → 2000 pages (~8 MB)            │
          │     │ pageSize    == 0   → 4096 bytes (B-tree default)   │
          │     │ readOnly    == 0   → read-write                    │
          │     │ busyTimeout == 0   → no busy retry                 │
          │     │ walSizeLimit== 0   → no WAL auto-checkpoint        │
          │     └────────────────────────────────────────────────────┘
          │
          ├── Step 3: Allocate structures
          │     ┌────────────────────────────────────────────────────┐
          │     │ KVStore {                                           │
          │     │   db      = alloc sqlite3{}                         │
          │     │   pMutex  = sqlite3_mutex_alloc(RECURSIVE)          │
          │     │   closing = 0                                       │
          │     │   inTrans = 0                                       │
          │     │ }                                                   │
          │     └────────────────────────────────────────────────────┘
          │
          ├── Step 4: Open B-tree
          │     flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
          │     if cfg.readOnly:
          │       flags = SQLITE_OPEN_READONLY  (no CREATE)
          │     sqlite3BtreeOpen(vfs, "test.db", db, &pBt, 0, flags)
          │       └── PagerOpen → OsOpen → open or create file
          │
          ├── Step 5: Set page size  (new databases only)
          │     if cfg.pageSize != 0:
          │       sqlite3BtreeSetPageSize(pBt, pageSize, -1, 0)
          │       ┌───────────────────────────────────────────────┐
          │       │ New DB  → sets page size before first write   │
          │       │ Old DB  → returns SQLITE_READONLY (silently   │
          │       │           ignored; existing page size wins)   │
          │       └───────────────────────────────────────────────┘
          │
          ├── Step 6: Set cache size
          │     sqlite3BtreeSetCacheSize(pBt, cacheSize)
          │
          ├── Step 7: Set pager sync flags  (skipped if readOnly)
          │     syncFlag = (syncLevel + 1) | PAGER_CACHESPILL
          │     ┌─────────────────────────────────────────────────────┐
          │     │ SYNC_OFF    (0) → flag = 0x01  SYNCHRONOUS_OFF      │
          │     │ SYNC_NORMAL (1) → flag = 0x02  SYNCHRONOUS_NORMAL   │
          │     │ SYNC_FULL   (2) → flag = 0x03  SYNCHRONOUS_FULL     │
          │     └─────────────────────────────────────────────────────┘
          │     sqlite3_mutex_enter(pKV->db->mutex)
          │     sqlite3BtreeSetPagerFlags(pBt, syncFlag)
          │     sqlite3_mutex_leave(pKV->db->mutex)
          │
          ├── Step 8: Install busy handler  (if busyTimeout > 0)
          │     ┌────────────────────────────────────────────────────┐
          │     │ KVBusyCtx {                                         │
          │     │   timeoutMs = cfg.busyTimeout                       │
          │     │   pVfs      = sqlite3_vfs_find(NULL)                │
          │     │ }  (heap-allocated via sqlite3_malloc)              │
          │     │                                                     │
          │     │ db->busyHandler.xBusyHandler = kvstoreBusyHandler   │
          │     │ db->busyHandler.pBusyArg     = ctx                  │
          │     │                                                     │
          │     │ kvstoreBusyHandler(ctx, nBusy):                     │
          │     │   if nBusy * 1ms >= timeoutMs → return 0 (give up)  │
          │     │   sqlite3OsSleep(pVfs, 1000 µs)  → sleep 1 ms      │
          │     │   return 1  (retry)                                 │
          │     └────────────────────────────────────────────────────┘
          │
          ├── Step 9: Enable auto-vacuum  (skipped if readOnly)
          │     sqlite3BtreeSetAutoVacuum(pBt, BTREE_AUTOVACUUM_INCR)
          │     ┌───────────────────────────────────────────┐
          │     │ New DB  → stores mode in header (OK)       │
          │     │ Old DB  → mode already fixed; silently     │
          │     │           ignored (header wins)            │
          │     └───────────────────────────────────────────┘
          │
          ├── Step 10: Set journal mode  (skipped if readOnly)
          │     sqlite3BtreeSetVersion(pBt, ver)
          │     ┌──────────────────────────────────────┐
          │     │ ver=1 → Rollback journal (DELETE)     │
          │     │ ver=2 → WAL mode                      │
          │     └──────────────────────────────────────┘
          │     sqlite3BtreeCommit(pBt)
          │
          ├── Step 11: Detect new vs existing database
          │     sqlite3BtreeBeginTrans(pBt, 0, 0)
          │     sqlite3BtreeGetMeta(pBt, META_DEFAULT_CF_ROOT, &root)
          │     sqlite3BtreeGetMeta(pBt, META_CF_METADATA_ROOT, &meta)
          │     sqlite3BtreeCommit(pBt)
          │
          │     ┌──────────────────────────────────────┐
          │     │ root == 0 → NEW database              │
          │     │ root != 0 → EXISTING database         │
          │     └──────────────────────────────────────┘
          │
          ├── Step 11a: [NEW DB] Initialize tables
          │     if readOnly:
          │       kvstoreTeardownNoLock(pKV)
          │       return KVSTORE_READONLY
          │               "cannot open empty database read-only"
          │     │
          │     ├── createDefaultCF()
          │     │     BtreeBeginTrans(wrflag=1)
          │     │     BtreeCreateTable(&pgno, BTREE_BLOBKEY)
          │     │     BtreeUpdateMeta(META_DEFAULT_CF_ROOT, pgno)
          │     │     BtreeCommit()
          │     │     Allocate KVColumnFamily{iTable=pgno, pReadCur=NULL}
          │     │
          │     └── initCFMetadataTable()
          │           BtreeBeginTrans(wrflag=1)
          │           BtreeCreateTable(&pgno, BTREE_INTKEY)
          │           BtreeUpdateMeta(META_CF_METADATA_ROOT, pgno)
          │           BtreeUpdateMeta(META_CF_COUNT, 1)
          │           BtreeCommit()
          │
          ├── Step 11b: [EXISTING DB] Restore handles
          │     Allocate KVColumnFamily{iTable=root, pReadCur=NULL}
          │     pKV->iMetaTable = meta
          │
          ├── Step 12: Allocate shared KeyInfo
          │     ┌────────────────────────────────────────┐
          │     │ KeyInfo {                               │
          │     │   enc       = SQLITE_UTF8               │
          │     │   nKeyField = 1                         │
          │     │   db        = pKV->db                   │
          │     │ }                                       │
          │     │ Used by ALL BLOBKEY cursor comparisons  │
          │     └────────────────────────────────────────┘
          │
          ├── Step 13: Init WAL auto-checkpoint config
          │     pKV->walSizeLimit = pConfig ? pConfig->walSizeLimit : 0
          │     pKV->walCommits   = 0
          │     ┌────────────────────────────────────────────────────┐
          │     │ walSizeLimit == 0 → auto-checkpoint disabled       │
          │     │ walSizeLimit == N → checkpoint every N commits     │
          │     │   (only effective in WAL journal mode)             │
          │     └────────────────────────────────────────────────────┘
          │
          └── Step 14: Open persistent read transaction
                sqlite3BtreeBeginTrans(pBt, 0, 0)
                pKV->inTrans = 1   ← held for lifetime of store
                *ppKV = pKV
                return KVSTORE_OK

  kvstore_open — thin backward-compatible wrapper:
  ┌──────────────────────────────────────────────────────────┐
  │ int kvstore_open(path, ppKV, journalMode) {              │
  │   KVStoreConfig cfg;                                     │
  │   memset(&cfg, 0, sizeof(cfg));                          │
  │   cfg.journalMode = journalMode;                         │
  │   return kvstore_open_v2(path, ppKV, &cfg);              │
  │ }                                                        │
  └──────────────────────────────────────────────────────────┘
  All other fields default: syncLevel=NORMAL, cacheSize=2000,
  pageSize=4096 (new DBs), readOnly=0, busyTimeout=0.

  Disk layout after first open (new database):
  ┌────────────────────────────────────────────────┐
  │ Page 1: SQLite file header + schema root        │
  │ Page 2: Default CF table root (BLOBKEY, empty)  │
  │ Page 3: CF metadata table root (INTKEY, empty)  │
  │ Page 4: Pointer-map page (auto-vacuum)          │
  └────────────────────────────────────────────────┘
```

---

## 4. kvstore_close

Closes a key-value store and frees all resources. Sets `closing=1`
under the mutex so any racing API call returns early safely.

```
  kvstore_close(kv)
          │
          ├── sqlite3_mutex_enter(pKV->pMutex)
          ├── pKV->closing = 1          ← racing threads see this and bail
          │
          ├── Rollback any active transaction
          │     if inTrans > 0:
          │       sqlite3BtreeRollback(pBt, SQLITE_OK, 0)
          │       pKV->inTrans = 0
          │
          ├── Free default CF
          │     if pDefaultCF->pReadCur:           ← free cached cursor
          │       kvstoreFreeCursor(pDefaultCF->pReadCur)
          │     sqlite3_mutex_free(pDefaultCF->pMutex)
          │     free(pDefaultCF->zName)
          │     free(pDefaultCF)
          │
          ├── Free all open named CFs
          │     for each apCF[i]:
          │       if apCF[i]->pReadCur:            ← free cached cursor
          │         kvstoreFreeCursor(apCF[i]->pReadCur)
          │       sqlite3_mutex_free(apCF[i]->pMutex)
          │       free(apCF[i]->zName)
          │       free(apCF[i])
          │     free(apCF)
          │
          ├── sqlite3BtreeClose(pBt)
          │     └── PagerClose → OsClose → close fd
          │
          ├── if db->busyHandler.pBusyArg:
          │     sqlite3_free(db->busyHandler.pBusyArg)  ← KVBusyCtx
          ├── free(pKeyInfo)         ← shared KeyInfo
          ├── free(zErrMsg)
          ├── sqlite3_mutex_free(db->mutex)
          ├── free(db)               ← sqlite3 struct
          │
          ├── sqlite3_mutex_leave(pKV->pMutex)
          ├── sqlite3_mutex_free(pKV->pMutex)
          └── free(pKV)
```

---

## 5. kvstore_begin

Begins a read or write transaction. The logic is more nuanced than a
simple `BeginTrans` call because of the **persistent read transaction**
that `kvstore_open` holds:

```
  kvstore_begin(kv, wrflag)
          │
          ├── sqlite3_mutex_enter(pKV->pMutex)
          ├── if closing → return error
          │
          ├── ┌─────────────────────────────────────────────────────┐
          │   │ State machine (inTrans × wrflag):                   │
          │   │                                                     │
          │   │  inTrans=2          → error: write already active   │
          │   │                                                     │
          │   │  inTrans=1, wrflag=0 → no-op: persistent read is   │
          │   │                        already open; return OK      │
          │   │                                                     │
          │   │  inTrans=1, wrflag=1 → must commit read FIRST:      │
          │   │    sqlite3BtreeCommit(pBt)  (releases readLock)     │
          │   │    inTrans = 0                                      │
          │   │    → fall through to fresh BeginTrans(write)        │
          │   │                                                     │
          │   │  inTrans=0, wrflag=0 → BtreeBeginTrans(0,0)        │
          │   │                        inTrans = 1                  │
          │   │                                                     │
          │   │  inTrans=0, wrflag=1 → BtreeBeginTrans(1,0)        │
          │   │                        inTrans = 2                  │
          │   └─────────────────────────────────────────────────────┘
          │
          │   WHY commit before upgrading to write?
          │   ┌────────────────────────────────────────────────────┐
          │   │ In WAL mode, SQLite assigns a "read slot" when a   │
          │   │ read transaction opens. Slot 0 is the checkpoint    │
          │   │ slot — used when the WAL is empty (e.g. right after │
          │   │ database creation). SQLite forbids upgrading a      │
          │   │ connection holding slot 0 to a write transaction    │
          │   │ (walBeginWriteTransaction returns SQLITE_BUSY).     │
          │   │                                                     │
          │   │ Committing the read releases the slot, then a fresh │
          │   │ BtreeBeginTrans(wrflag=1) succeeds unconditionally. │
          │   └────────────────────────────────────────────────────┘
          │
          └── sqlite3_mutex_leave(pKV->pMutex)

  Transaction state diagram:

      kvstore_open()
            │
            ▼
       inTrans = 1  ──────── get/exists ──────────► inTrans = 1
     (persistent read)                               (unchanged)
            │
            │ begin(write)
            │  commit(read) → inTrans=0
            │  BeginTrans(1) → inTrans=2
            ▼
       inTrans = 2
     (write active)
            │
            │ commit() or rollback()
            │  BtreeCommit/Rollback
            │  BeginTrans(0) → inTrans=1
            ▼
       inTrans = 1  (persistent read restored)
```

---

## 6. kvstore_commit

Commits the current transaction, flushing changes to disk. After commit,
immediately restores the persistent read transaction.

```
  kvstore_commit(kv)
          │
          ├── sqlite3_mutex_enter(pKV->pMutex)
          ├── if closing → return error
          ├── if !inTrans → error (no active transaction)
          │
          ├── sqlite3BtreeCommit(pBt)
          │     │
          │     ├── [Rollback mode]
          │     │     CommitPhaseOne:
          │     │       sync journal → write dirty pages → sync DB
          │     │     CommitPhaseTwo:
          │     │       delete journal → release lock
          │     │
          │     └── [WAL mode]
          │           CommitPhaseOne:
          │             append dirty pages as WAL frames → sync WAL
          │           CommitPhaseTwo:
          │             release WAL write lock
          │
          ├── pKV->inTrans = 0        ← TRANS_NONE window begins
          │
          ├── WAL auto-checkpoint (TRANS_NONE window)
          │     kvstoreAutoCheckpoint(pKV)
          │     ┌────────────────────────────────────────────────────┐
          │     │ if walSizeLimit == 0 → skip (disabled)             │
          │     │ walCommits++                                        │
          │     │ if walCommits < walSizeLimit → skip                │
          │     │ walCommits = 0                                      │
          │     │ sqlite3BtreeCheckpoint(PASSIVE, NULL, NULL)        │
          │     │   copies committed WAL frames into DB file         │
          │     │   safe here: write lock already released by commit │
          │     └────────────────────────────────────────────────────┘
          │
          ├── Restore persistent read transaction ← TRANS_NONE window ends
          │     if sqlite3BtreeBeginTrans(pBt, 0, 0) == SQLITE_OK:
          │       pKV->inTrans = 1
          │
          └── sqlite3_mutex_leave(pKV->pMutex)

  Commit data flow (WAL mode):
  ┌──────────┐    ┌─────────────────┐    ┌────────────┐
  │ Dirty     │───►│  WAL File       │    │  Database  │
  │ Pages in  │    │  (frames appended│   │  (unchanged │
  │ Cache     │    │   then synced)  │    │   during   │
  └──────────┘    └─────────────────┘    │   write)   │
                                          └────────────┘
              Later, WAL checkpoint copies frames → DB file

  Commit data flow (Rollback mode):
  ┌──────────┐    ┌─────────────────┐    ┌────────────┐
  │ Dirty     │    │  Journal        │    │  Database  │
  │ Pages in  │───►│  (original pages│───►│  File      │
  │ Cache     │    │   already there)│    │  (written) │
  └──────────┘    └─────────────────┘    └────────────┘
                   sync              sync  then delete journal
```

---

## 7. kvstore_rollback

Rolls back the current transaction, discarding all uncommitted changes.
Restores the persistent read transaction afterward.

```
  kvstore_rollback(kv)
          │
          ├── sqlite3_mutex_enter(pKV->pMutex)
          ├── if closing → return error
          ├── if !inTrans → return OK (no-op)
          │
          ├── sqlite3BtreeRollback(pBt, SQLITE_OK, 0)
          │     │
          │     ├── [Rollback mode]
          │     │     Replay journal → restore original pages
          │     │     Delete journal → release lock
          │     │
          │     └── [WAL mode]
          │           Discard uncommitted WAL frames
          │           Release WAL write lock
          │
          ├── pKV->inTrans = 0
          │
          ├── Restore persistent read transaction (if not closing)
          │     if !closing:
          │       if sqlite3BtreeBeginTrans(pBt, 0, 0) == SQLITE_OK:
          │         pKV->inTrans = 1
          │
          └── sqlite3_mutex_leave(pKV->pMutex)
```

---

## 8. kvstore_put

Inserts or updates a key-value pair (upsert semantics).

```
  kvstore_put(kv, "user:1", 6, "Alice", 5)
          │
          │  delegates to kvstore_cf_put_internal(pDefaultCF, ...)
          │
          ├── sqlite3_mutex_enter(pCF->pMutex)    ← CF lock first
          ├── sqlite3_mutex_enter(pKV->pMutex)    ← then store lock
          ├── if closing → return error
          │
          ├── Validate key/value sizes
          │     key:   non-null, 1..64 KB
          │     value: NULL OK if nValue==0, max 10 MB
          │
          ├── Ensure write transaction
          │     ┌─────────────────────────────────────────────────┐
          │     │ inTrans == 2 → already in write txn, proceed   │
          │     │                autoTrans = 0                    │
          │     │                                                 │
          │     │ inTrans == 1 → persistent read active:          │
          │     │   sqlite3BtreeCommit(pBt)    ← release read     │
          │     │   inTrans = 0                                   │
          │     │   → fall through                                │
          │     │                                                 │
          │     │ inTrans == 0 → begin fresh write:               │
          │     │   sqlite3BtreeBeginTrans(pBt, 1, 0)            │
          │     │   autoTrans = 1                                 │
          │     │   inTrans = 2                                   │
          │     └─────────────────────────────────────────────────┘
          │
          ├── Encode key+value into BLOBKEY blob (stack-first)
          │     unsigned char stackBuf[512];
          │     kvstoreEncodeBlob(key, 6, val, 5,
          │                       stackBuf, 512, &pEncoded)
          │     ┌───────────────────────────────────────────────┐
          │     │ 4 + 6 + 5 = 15 bytes → fits stackBuf          │
          │     │ pEncoded == stackBuf → no heap alloc needed    │
          │     │ [00 00 00 06][user:1][Alice]                   │
          │     └───────────────────────────────────────────────┘
          │
          ├── Open write cursor (separate from cached read cursor)
          │     kvstoreAllocCursor()
          │     sqlite3BtreeCursor(pBt, iTable, wrflag=1, pKeyInfo, pCur)
          │
          ├── Insert into B-tree
          │     BtreePayload { pKey=pEncoded, nKey=15 }
          │     sqlite3BtreeInsert(pCur, &payload, 0, 0)
          │     │
          │     ├── IndexMoveto → find insertion point in leaf
          │     │     ┌─────────────────────────────────────────┐
          │     │     │            [root page]                   │
          │     │     │           /           \                  │
          │     │     │    [internal]       [internal]           │
          │     │     │    /       \         /       \           │
          │     │     │ [leaf]  [leaf]   [leaf]   [leaf]        │
          │     │     │                   ▲                      │
          │     │     │                   cursor position        │
          │     │     └─────────────────────────────────────────┘
          │     │
          │     ├── Key exists?  YES → overwrite cell (upsert)
          │     │               NO  → insert, rebalance if needed
          │     └── PagerWrite() on modified page(s)
          │
          ├── kvstoreFreeCursor(pCur)       ← write cursor freed
          │                                   pCF->pReadCur untouched
          ├── pKV->stats.nPuts++
          │
          ├── if autoTrans:
          │     sqlite3BtreeCommit(pBt)
          │     pKV->inTrans = 0
          │     Restore persistent read:
          │       if BtreeBeginTrans(pBt, 0, 0) == OK:
          │         pKV->inTrans = 1
          │
          ├── sqlite3_mutex_leave(pKV->pMutex)
          └── sqlite3_mutex_leave(pCF->pMutex)
```

---

## 9. kvstore_get

Retrieves a value by key. Uses the **cached read cursor** (`pReadCur`) —
no cursor malloc/free per call. The persistent read transaction normally
makes `BeginTrans`/`Commit` a no-op as well.

```
  kvstore_get(kv, "user:1", 6, &val, &nVal)
          │
          │  delegates to kvstore_cf_get_internal(pDefaultCF, ...)
          │
          ├── sqlite3_mutex_enter(pCF->pMutex)
          ├── sqlite3_mutex_enter(pKV->pMutex)
          ├── if closing → return error
          │
          ├── Transaction check (fallback only)
          │     inTrans >= 1 → persistent read already active,
          │                    no BeginTrans needed
          │     inTrans == 0 → rare; start fresh read:
          │                    BtreeBeginTrans(pBt, 0, 0)
          │                    inTrans = 1
          │
          ├── Get (or open) cached read cursor
          │     kvstoreGetReadCursor(pCF)
          │     ┌──────────────────────────────────────────────┐
          │     │ pCF->pReadCur != NULL → return it directly   │
          │     │                        (zero malloc per call) │
          │     │                                              │
          │     │ pCF->pReadCur == NULL → first call only:     │
          │     │   kvstoreAllocCursor()                       │
          │     │   BtreeCursor(pBt, iTable, 0, pKeyInfo, pCur)│
          │     │   pCF->pReadCur = pCur                       │
          │     └──────────────────────────────────────────────┘
          │
          ├── Seek to key (stack-allocated record, no heap alloc)
          │     kvstoreSeekKey(pCur, pKeyInfo, "user:1", 6, &found)
          │     │
          │     ├── UnpackedRecord idxKey;  ← stack, not heap
          │     │   Mem memField;           ← stack, not heap
          │     │   idxKey.u.z = "user:1"
          │     │   idxKey.n   = 6
          │     │
          │     ├── sqlite3BtreeIndexMoveto(pCur, &idxKey, &res)
          │     │     Traverses root → internal pages → leaf
          │     │     Compares search key against cell keys
          │     │     res=0: exact match
          │     │     res≠0: not found
          │     │
          │     └── found = (res == 0)
          │
          ├── [NOT FOUND] → return KVSTORE_NOTFOUND
          │
          ├── [FOUND] Read value from cursor
          │     ┌───────────────────────────────────────────────┐
          │     │ Payload: [00 00 00 06][user:1][Alice]         │
          │     │           offset 0     4       10             │
          │     │                                               │
          │     │ 1. BtreePayload(pCur, 0, 4, hdr)             │
          │     │    → storedKeyLen = 6                         │
          │     │                                               │
          │     │ 2. valLen = payloadSz - 4 - 6 = 5            │
          │     │                                               │
          │     │ 3. BtreePayload(pCur, 4+6, 5, buf)           │
          │     │    → "Alice"                                  │
          │     └───────────────────────────────────────────────┘
          │
          ├── pCur stays open as pCF->pReadCur  ← NO close/free
          ├── pKV->stats.nGets++
          ├── *ppValue = heap copy of "Alice" (caller must free)
          ├── *pnValue = 5
          │
          ├── sqlite3_mutex_leave(pKV->pMutex)
          └── sqlite3_mutex_leave(pCF->pMutex)

  Per-operation cost (before → after):
  ┌──────────────────────────────┬───────────────────────────────┐
  │ BEFORE                       │ AFTER                         │
  ├──────────────────────────────┼───────────────────────────────┤
  │ sqlite3MallocZero(cursorSz)  │ (cursor already open)         │
  │ sqlite3BtreeCursor()         │ (no open)                     │
  │ VdbeAllocUnpackedRecord()    │ UnpackedRecord on stack        │
  │ sqlite3BtreeIndexMoveto()    │ sqlite3BtreeIndexMoveto()     │
  │ sqlite3DbFree(record)        │ (no free — stack)             │
  │ sqlite3BtreePayload() ×2     │ sqlite3BtreePayload() ×2      │
  │ sqlite3Malloc(valueLen)      │ sqlite3Malloc(valueLen)       │
  │ sqlite3BtreeCloseCursor()    │ (cursor stays open)           │
  │ sqlite3_free(pCur)           │ (no free)                     │
  │ sqlite3BtreeBeginTrans()     │ (no-op: inTrans=1 already)    │
  │ sqlite3BtreeCommit()         │ (no-op: no autoTrans)         │
  └──────────────────────────────┴───────────────────────────────┘
```

---

## 10. kvstore_delete

Deletes a key-value pair. Returns `KVSTORE_NOTFOUND` if the key does
not exist.

```
  kvstore_delete(kv, "user:1", 6)
          │
          │  delegates to kvstore_cf_delete_internal(pDefaultCF, ...)
          │
          ├── sqlite3_mutex_enter(pCF->pMutex)
          ├── sqlite3_mutex_enter(pKV->pMutex)
          ├── if closing → return error
          │
          ├── Ensure write transaction (same as put)
          │     if inTrans == 1: commit read → inTrans=0
          │     if inTrans != 2: BeginTrans(1), autoTrans=1, inTrans=2
          │
          ├── Open write cursor
          │     BtreeCursor(pBt, iTable, 1, pKeyInfo, pCur)
          │
          ├── Seek to key
          │     kvstoreSeekKey(pCur, pKeyInfo, "user:1", 6, &found)
          │
          ├── [NOT FOUND] → rollback autoTrans, return KVSTORE_NOTFOUND
          │
          ├── [FOUND] Delete cell
          │     sqlite3BtreeDelete(pCur, 0)
          │     ┌──────────────────────────────────────────────┐
          │     │ Leaf (before): [user:0][user:1][user:2]      │
          │     │                         ▲ delete             │
          │     │ Leaf (after) : [user:0][user:2]              │
          │     │                                              │
          │     │ If page underflows → btree rebalance         │
          │     │ Freed pages → internal freelist              │
          │     │ (file does not shrink until vacuum)          │
          │     └──────────────────────────────────────────────┘
          │
          ├── kvstoreFreeCursor(pCur)
          ├── pKV->stats.nDeletes++
          │
          ├── if autoTrans:
          │     BtreeCommit(pBt)
          │     pKV->inTrans = 0
          │     Restore persistent read:
          │       if BtreeBeginTrans(0) == OK: inTrans = 1
          │
          ├── sqlite3_mutex_leave(pKV->pMutex)
          └── sqlite3_mutex_leave(pCF->pMutex)
```

---

## 11. kvstore_exists

Checks if a key exists without reading or copying the value.
Cheapest point lookup — only B-tree traversal, no payload read.

```
  kvstore_exists(kv, "user:1", 6, &exists)
          │
          │  delegates to kvstore_cf_exists_internal(pDefaultCF, ...)
          │
          ├── sqlite3_mutex_enter(pCF->pMutex)
          ├── sqlite3_mutex_enter(pKV->pMutex)
          ├── if closing → return error
          │
          ├── Transaction: inTrans>=1 → use persistent read
          │               inTrans==0 → begin fresh read (fallback)
          │
          ├── Get cached read cursor → kvstoreGetReadCursor(pCF)
          │
          ├── kvstoreSeekKey(pCur, pKeyInfo, "user:1", 6, &found)
          │     (stack-allocated UnpackedRecord, no heap)
          │
          ├── pCur stays open as pCF->pReadCur
          ├── *pExists = found  (1 or 0)
          │
          ├── sqlite3_mutex_leave(pKV->pMutex)
          └── sqlite3_mutex_leave(pCF->pMutex)

  Note: No payload is read — only the B-tree traversal and
  the key comparison at the leaf node.
```

---

## 12. kvstore_cf_create

Creates a new named column family (a separate BLOBKEY B-tree table).

```
  kvstore_cf_create(kv, "logs", &cf)
          │
          ├── Validate name: non-null, 1..255 chars, not "default"
          │
          ├── sqlite3_mutex_enter(pKV->pMutex)
          ├── if closing → return error
          │
          ├── Ensure write transaction
          │     if inTrans == 1: BtreeCommit() → inTrans=0  ← release read
          │     if inTrans != 2: BtreeBeginTrans(1,0)
          │                      autoTrans=1, inTrans=2
          │
          ├── Check CF does not already exist
          │     kvstoreMetaSeekKey("logs") → found?
          │     YES → autoTrans rollback, return error
          │
          ├── Create new BLOBKEY table
          │     sqlite3BtreeCreateTable(pBt, &pgno, BTREE_BLOBKEY)
          │     ┌──────────────────────────────────────────────┐
          │     │ Before: pages [1][2-default][3-meta][4-ptrmap]│
          │     │ After:  pages [1][2][3][4][5-"logs" root]    │
          │     │          pgno = 5 (example)                  │
          │     └──────────────────────────────────────────────┘
          │
          ├── Encode metadata payload (stack buffer, no heap)
          │     unsigned char metaStack[320];
          │     kvstoreEncodeBlob("logs", 4, rootBytes, 4,
          │                       metaStack, 320, &pEncoded)
          │     ┌──────────────────────────────────────────────┐
          │     │ [00 00 00 04][l][o][g][s][00 00 00 05]       │
          │     │  name_len=4   name          root_pgno=5      │
          │     └──────────────────────────────────────────────┘
          │
          ├── Insert metadata row
          │     slot = kvstoreMetaFindSlot("logs")   ← FNV-1a hash
          │     BtreeInsert(metaCur, {key=slot, data=pEncoded})
          │
          ├── Update CF count
          │     BtreeGetMeta(META_CF_COUNT) → N
          │     BtreeUpdateMeta(META_CF_COUNT, N+1)
          │
          ├── if autoTrans:
          │     BtreeCommit(pBt), inTrans=0
          │     Restore persistent read: BtreeBeginTrans(0) → inTrans=1
          │
          ├── Allocate KVColumnFamily
          │     ┌───────────────────────────────────────┐
          │     │ KVColumnFamily {                       │
          │     │   pKV      = kv                        │
          │     │   zName    = "logs"                    │
          │     │   iTable   = 5    (new root page)      │
          │     │   refCount = 1                         │
          │     │   pMutex   = sqlite3_mutex_alloc(REC)  │
          │     │   pReadCur = NULL  ← opened on first get│
          │     │ }                                      │
          │     └───────────────────────────────────────┘
          │
          └── sqlite3_mutex_leave(pKV->pMutex)
              return KVSTORE_OK, *ppCF = cf
```

---

## 13. kvstore_cf_open

Opens an existing column family by name.

```
  kvstore_cf_open(kv, "logs", &cf)
          │
          ├── If name == "default" → kvstore_cf_get_default()
          │
          ├── sqlite3_mutex_enter(pKV->pMutex)
          ├── if closing → return error
          │
          ├── Transaction: persistent read (inTrans=1) is used as-is;
          │               no transaction change needed for a read
          │
          ├── Look up CF in metadata table
          │     Open cursor on iMetaTable
          │     kvstoreMetaSeekKey("logs", 4, &found, &foundRowid)
          │     │
          │     ├── hash = FNV-1a("logs")
          │     ├── BtreeTableMoveto(pCur, hash, 0, &res)
          │     └── Linear probe if hash collision
          │         Compare stored name bytes at each slot
          │
          ├── [NOT FOUND] → return KVSTORE_NOTFOUND
          │
          ├── Read table root from metadata payload
          │     BtreePayload(pCur, 4+nameLen, 4, rootBytes)
          │     iTable = decode_BE(rootBytes)
          │
          ├── Allocate KVColumnFamily
          │     ┌───────────────────────────────────────┐
          │     │ KVColumnFamily {                       │
          │     │   iTable   = iTable                   │
          │     │   zName    = "logs"                   │
          │     │   refCount = 1                        │
          │     │   pMutex   = sqlite3_mutex_alloc(REC) │
          │     │   pReadCur = NULL  ← opened on first get│
          │     │ }                                      │
          │     └───────────────────────────────────────┘
          │
          └── sqlite3_mutex_leave(pKV->pMutex)
              return KVSTORE_OK, *ppCF = cf
```

---

## 14. kvstore_cf_drop

Drops a column family, deleting all its data and metadata.

```
  kvstore_cf_drop(kv, "logs")
          │
          ├── Cannot drop "default" → error
          │
          ├── sqlite3_mutex_enter(pKV->pMutex)
          ├── if closing → return error
          │
          ├── Ensure write transaction (same pattern as put)
          │     if inTrans == 1: BtreeCommit() → inTrans=0
          │     if inTrans != 2: BeginTrans(1), autoTrans=1, inTrans=2
          │
          ├── Find CF in metadata table
          │     kvstoreMetaSeekKey("logs") → foundRowid, iTable
          │
          ├── Drop the CF's B-tree table
          │     sqlite3BtreeDropTable(pBt, iTable, &iMoved)
          │     ┌───────────────────────────────────────────┐
          │     │ All data pages for "logs" → freelist      │
          │     │ Root page released                        │
          │     │ File does NOT shrink until vacuum         │
          │     └───────────────────────────────────────────┘
          │
          ├── Delete metadata row
          │     BtreeTableMoveto(pCur, foundRowid)
          │     sqlite3BtreeDelete(pCur, 0)
          │
          ├── Decrement CF count
          │     BtreeGetMeta(META_CF_COUNT) → N
          │     BtreeUpdateMeta(META_CF_COUNT, N-1)
          │
          ├── if autoTrans:
          │     BtreeCommit(pBt), inTrans=0
          │     Restore persistent read: BtreeBeginTrans(0) → inTrans=1
          │
          └── sqlite3_mutex_leave(pKV->pMutex)
              return KVSTORE_OK
```

---

## 15. kvstore_cf_list

Lists all column families in the database.

```
  kvstore_cf_list(kv, &names, &count)
          │
          ├── sqlite3_mutex_enter(pKV->pMutex)
          ├── if closing → return error
          │
          ├── Open cursor on metadata table (uses persistent read)
          │     sqlite3BtreeCursor(pBt, iMetaTable, 0, NULL, pCur)
          │
          ├── Scan all entries
          │     sqlite3BtreeFirst(pCur, &res)
          │     while !res:
          │       ┌──────────────────────────────────────────┐
          │       │ Read 4-byte header → nameLen             │
          │       │ Read nameLen bytes → name string         │
          │       │ Append to output array                   │
          │       └──────────────────────────────────────────┘
          │       sqlite3BtreeNext(pCur, 0)
          │
          ├── Close cursor
          ├── *pazNames = ["logs", "metrics", ...]
          │   *pnCount  = N
          │
          └── sqlite3_mutex_leave(pKV->pMutex)
              return KVSTORE_OK
```

---

## 16. kvstore_iterator_create

Creates an iterator for traversing all key-value pairs.

```
  kvstore_iterator_create(kv, &iter)
          │
          │  delegates to kvstore_cf_iterator_create(pDefaultCF, ...)
          │
          ├── sqlite3_mutex_enter(pKV->pMutex)
          ├── if closing → return error
          │
          ├── Allocate KVIterator
          │     ┌──────────────────────────────────────────┐
          │     │ KVIterator {                              │
          │     │   pCF       = defaultCF                  │
          │     │   eof       = 1   (not positioned yet)   │
          │     │   isValid   = 1                          │
          │     │   ownsTrans = 0   (set below if needed)  │
          │     │   pPrefix   = NULL  (no filter)          │
          │     │   pCur      = NULL  (set below)          │
          │     │ }                                        │
          │     └──────────────────────────────────────────┘
          │
          ├── Transaction for iterator
          │     ┌──────────────────────────────────────────┐
          │     │ inTrans >= 1 → use existing transaction  │
          │     │               ownsTrans = 0              │
          │     │                                          │
          │     │ inTrans == 0 → start fresh read:         │
          │     │   BtreeBeginTrans(pBt, 0, 0)             │
          │     │   ownsTrans = 1                          │
          │     │   inTrans = 1                            │
          │     └──────────────────────────────────────────┘
          │
          ├── Allocate iterator's own B-tree cursor
          │     kvstoreAllocCursor()
          │     BtreeCursor(pBt, iTable, 0, pKeyInfo, pCur)
          │     pIter->pCur = pCur    ← iterator owns this cursor
          │                             (separate from pCF->pReadCur)
          │
          ├── pKV->stats.nIterations++
          ├── sqlite3_mutex_leave(pKV->pMutex)
          └── return KVSTORE_OK, *ppIter = iter

  Note: Iterator has its OWN cursor (not pCF->pReadCur).
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
          │     iter->eof = (res != 0)
          │     ┌──────────────────────────────────────────────┐
          │     │ B-tree leaf pages (sorted order):            │
          │     │   [aaa:1][bbb:2][ccc:3][ddd:4][eee:5] ...   │
          │     │    ▲                                         │
          │     │    cursor lands on first entry               │
          │     └──────────────────────────────────────────────┘
          │
          └── [With prefix filter]  (see §20)
                Seek to first key >= prefix
                Check if key starts with prefix
                iter->eof = !match


  kvstore_iterator_next(iter)
          │
          ├── if iter->eof → return OK (no-op)
          │
          ├── sqlite3BtreeNext(pCur, 0)
          │     SQLITE_OK   → moved to next entry
          │     SQLITE_DONE → no more entries → iter->eof = 1
          │
          │     ┌──────────────────────────────────────────────┐
          │     │   [aaa:1][bbb:2][ccc:3][ddd:4][eee:5] ...   │
          │     │            ▲                                 │
          │     │            cursor moves one step right       │
          │     └──────────────────────────────────────────────┘
          │
          └── [With prefix] Check prefix bound
                kvstoreIterCheckPrefix(iter)
                  key still starts with prefix? → continue
                  key past prefix?              → iter->eof = 1


  kvstore_iterator_eof(iter)
          │
          └── return iter->eof   (1 = past last entry, 0 = has data)
```

---

## 18. kvstore_iterator_key / value

Reads the current key or value from the iterator's cursor position.
Buffers are reused across calls (grown with realloc if needed).

```
  kvstore_iterator_key(iter, &key, &nKey)
          │
          ├── payloadSz = sqlite3BtreePayloadSize(pCur)
          │
          ├── BtreePayload(pCur, 0, 4, hdr)
          │     keyLen = decode_BE(hdr)
          │
          ├── Grow reusable key buffer if needed
          │     if nKeyBuf < keyLen:
          │       pKeyBuf = sqlite3_realloc(pKeyBuf, keyLen)
          │       nKeyBuf = keyLen
          │
          ├── BtreePayload(pCur, 4, keyLen, pKeyBuf)
          │     ┌─────────────────────────────────────────────┐
          │     │ Cell: [00 00 00 06][user:1][Alice]          │
          │     │                     ▲──────▲                │
          │     │                     read 6 bytes            │
          │     └─────────────────────────────────────────────┘
          │
          ├── *ppKey = pKeyBuf   ← do NOT free (owned by iterator)
          └── *pnKey = keyLen


  kvstore_iterator_value(iter, &val, &nVal)
          │
          ├── BtreePayload(pCur, 0, 4, hdr) → keyLen
          │     valLen = payloadSz - 4 - keyLen
          │
          ├── Grow reusable value buffer if needed
          │
          ├── BtreePayload(pCur, 4+keyLen, valLen, pValBuf)
          │     ┌─────────────────────────────────────────────┐
          │     │ Cell: [00 00 00 06][user:1][Alice]          │
          │     │                            ▲─────▲          │
          │     │                            read 5 bytes     │
          │     └─────────────────────────────────────────────┘
          │
          ├── *ppValue = pValBuf  ← do NOT free (owned by iterator)
          └── *pnValue = valLen
```

---

## 19. kvstore_iterator_close

Closes an iterator and frees all associated resources. Handles the
persistent read transaction correctly based on `ownsTrans`.

```
  kvstore_iterator_close(iter)
          │
          ├── iter->isValid = 0
          │
          ├── Close iterator's own cursor
          │     kvstoreFreeCursor(pIter->pCur)   ← NOT pCF->pReadCur
          │     pIter->pCur = NULL
          │
          ├── Transaction cleanup
          │     ┌──────────────────────────────────────────────┐
          │     │ ownsTrans == 0 → iterator used the store's   │
          │     │   existing transaction (persistent read or   │
          │     │   caller's explicit txn).  Leave inTrans     │
          │     │   unchanged — nothing to commit.             │
          │     │                                              │
          │     │ ownsTrans == 1 → iterator started its own   │
          │     │   read transaction (inTrans was 0 at create) │
          │     │   sqlite3_mutex_enter(pKV->pMutex)          │
          │     │   sqlite3BtreeCommit(pBt)                   │
          │     │   pKV->inTrans = 0                          │
          │     │   sqlite3_mutex_leave(pKV->pMutex)          │
          │     │   (persistent read not restored here —      │
          │     │    the store had no persistent read before   │
          │     │    the iterator was created)                 │
          │     └──────────────────────────────────────────────┘
          │
          ├── Release CF reference
          │     kvstore_cf_close(pCF)   → refCount--
          │
          ├── Free buffers
          │     sqlite3_free(pKeyBuf)
          │     sqlite3_free(pValBuf)
          │     sqlite3_free(pPrefix)
          │
          └── sqlite3_free(iter)
```

---

## 20. kvstore_prefix_iterator_create

Creates an iterator that only returns keys starting with a given prefix.
Uses a direct B-tree seek to the first matching key (O(log n), not O(n)).

```
  kvstore_prefix_iterator_create(kv, "user:", 5, &iter)
          │
          ├── Create normal iterator via kvstore_cf_iterator_create()
          │
          ├── Store prefix copy in iterator
          │     pIter->pPrefix = sqlite3_malloc(5)
          │     memcpy(pIter->pPrefix, "user:", 5)
          │     pIter->nPrefix = 5
          │
          ├── Position at first matching key
          │     kvstore_iterator_first(iter)
          │     │
          │     ├── Build UnpackedRecord on STACK for prefix
          │     │     UnpackedRecord idxKey;  ← stack
          │     │     Mem memField;           ← stack
          │     │     idxKey.u.z = "user:"
          │     │     idxKey.n   = 5
          │     │
          │     ├── sqlite3BtreeIndexMoveto(pCur, &idxKey, &res)
          │     │     ┌──────────────────────────────────────────────┐
          │     │     │ B-tree entries (sorted):                     │
          │     │     │  [admin:1][admin:2][user:1][user:2][zzz:1]  │
          │     │     │                    ▲                         │
          │     │     │                    cursor lands here         │
          │     │     └──────────────────────────────────────────────┘
          │     │     res=0: cursor on exact prefix match
          │     │     res<0: cursor before nearest key → BtreeNext()
          │     │
          │     └── kvstoreIterCheckPrefix(iter)
          │           Read key_len header → read first 5 bytes
          │           memcmp(key[0..4], "user:", 5) == 0?
          │           YES → eof=0, iterator ready
          │           NO  → eof=1 (no matching keys)
          │
          └── return iterator (already positioned at first match)

  Subsequent next() calls:
  ┌────────────────────────────────────────────────────────────┐
  │ kvstore_iterator_next(iter)                                │
  │   sqlite3BtreeNext(pCur)                                  │
  │   kvstoreIterCheckPrefix(iter)                            │
  │     key = "user:2" → starts with "user:" → continue      │
  │     key = "zzz:1"  → prefix mismatch     → eof = 1       │
  │                                                           │
  │ Iteration produces (sorted order):                        │
  │   "user:1" → ...                                          │
  │   "user:2" → ...                                          │
  │   (stops before "zzz:1", never scans beyond prefix range) │
  └────────────────────────────────────────────────────────────┘
```

---

## 21. kvstore_incremental_vacuum

Reclaims unused pages from the freelist and shrinks the database file.
Databases always open with incremental auto-vacuum enabled.

```
  kvstore_incremental_vacuum(kv, nPage)
          │
          ├── sqlite3_mutex_enter(pKV->pMutex)
          ├── if closing or isCorrupted → return error
          │
          ├── Ensure write transaction
          │     if inTrans == 1: BtreeCommit() → inTrans=0
          │     if inTrans != 2: BeginTrans(2), autoTrans=1, inTrans=2
          │
          ├── Run vacuum steps
          │     ┌───────────────────────────────────────────────────┐
          │     │ nPage <= 0: free ALL unused pages                 │
          │     │   do {                                            │
          │     │     rc = sqlite3BtreeIncrVacuum(pBt)             │
          │     │   } while (rc == SQLITE_OK)                      │
          │     │   // SQLITE_DONE = no more free pages            │
          │     │                                                   │
          │     │ nPage > 0: free up to nPage pages                │
          │     │   for i = 0..nPage-1:                            │
          │     │     rc = sqlite3BtreeIncrVacuum(pBt)             │
          │     │     if rc != SQLITE_OK: break                    │
          │     └───────────────────────────────────────────────────┘
          │
          │     Each BtreeIncrVacuum step:
          │     ┌───────────────────────────────────────────────────┐
          │     │ File BEFORE: [P1][P2][P3][P4][P5][P6][P7][P8]   │
          │     │                       ↑free           ↑last page │
          │     │                                                   │
          │     │ 1. Find last page (P8)                           │
          │     │ 2. If P8 is free → just truncate                 │
          │     │ 3. If P8 has data:                               │
          │     │    a. Copy P8 content → free slot (P4)           │
          │     │    b. Update pointer-map references              │
          │     │    c. Page count --                              │
          │     │                                                   │
          │     │ File AFTER:  [P1][P2][P3][P8'][P5][P6][P7]      │
          │     │                       ↑moved   ↑ truncated ──►  │
          │     │ File physically shrinks by 1 page on commit      │
          │     └───────────────────────────────────────────────────┘
          │
          ├── if autoTrans:
          │     BtreeCommit(pBt)
          │     pKV->inTrans = 0
          │     Restore persistent read: BtreeBeginTrans(0) → inTrans=1
          │
          └── sqlite3_mutex_leave(pKV->pMutex)
              return KVSTORE_OK

  File size timeline:
  ┌────────────────────────────────────────────────────────────┐
  │  insert 2000 records  → file ~500 KB                       │
  │  delete 1800 records  → file still ~500 KB                 │
  │                         (freed pages sit on freelist)      │
  │  kvstore_incremental_vacuum(kv, 0)                         │
  │                       → file shrinks to ~50 KB             │
  │                                                            │
  │  Without vacuum: freed pages are reused for future inserts │
  │  but the file NEVER shrinks on its own.                    │
  └────────────────────────────────────────────────────────────┘
```

---

## 22. kvstore_integrity_check

Performs a deep integrity check on the entire database.

```
  kvstore_integrity_check(kv, &errMsg)
          │
          ├── sqlite3_mutex_enter(pKV->pMutex)
          ├── if closing → return error
          │
          ├── Build list of all root pages to check
          │     ┌──────────────────────────────────────────┐
          │     │ aRoot[] = {                               │
          │     │   1,              // file header page     │
          │     │   defaultCF.root, // default CF table     │
          │     │   iMetaTable,     // CF metadata table    │
          │     │   cf1.root,       // from metadata scan   │
          │     │   cf2.root,       // from metadata scan   │
          │     │   ...                                     │
          │     │ }                                         │
          │     └──────────────────────────────────────────┘
          │
          ├── sqlite3BtreeIntegrityCheck(db, pBt, aRoot, aCnt,
          │                              nRoot, 100, &nErr, &zErr)
          │     ┌────────────────────────────────────────────┐
          │     │ For every page in the database:            │
          │     │   • Valid page header format               │
          │     │   • Cell pointers in range                 │
          │     │   • Free block list consistent             │
          │     │   • Parent pointers correct (auto-vacuum)  │
          │     │   • All pages accounted for                │
          │     │   • B-tree key ordering valid              │
          │     └────────────────────────────────────────────┘
          │
          ├── [nErr > 0]
          │     pKV->isCorrupted = 1
          │     *pzErrMsg = zErr   (caller must free with sqliteFree)
          │     return KVSTORE_CORRUPT
          │
          ├── [nErr == 0]
          │     return KVSTORE_OK
          │
          └── sqlite3_mutex_leave(pKV->pMutex)
```

---

## 23. kvstore_sync

Synchronizes the database to disk by committing any active write
transaction. After sync there is no active write transaction.

```
  kvstore_sync(kv)
          │
          ├── sqlite3_mutex_enter(pKV->pMutex)
          ├── if closing or isCorrupted → return error
          │
          ├── if inTrans == 2 (active write):
          │     sqlite3BtreeCommit(pBt)
          │       ├── [Rollback] sync journal → write pages → sync DB
          │       └── [WAL]      append frames → sync WAL
          │     pKV->inTrans = 0
          │     Restore persistent read: BtreeBeginTrans(0) → inTrans=1
          │
          ├── if inTrans <= 1: no-op (reads are already consistent)
          │
          └── sqlite3_mutex_leave(pKV->pMutex)
              return KVSTORE_OK
```

---

## 24. kvstore_stats

Returns a snapshot of operation counters. Thread-safe copy under mutex.

```
  kvstore_stats(kv, &stats)
          │
          ├── sqlite3_mutex_enter(pKV->pMutex)
          │
          ├── stats.nPuts       = pKV->stats.nPuts
          │   stats.nGets       = pKV->stats.nGets
          │   stats.nDeletes    = pKV->stats.nDeletes
          │   stats.nIterations = pKV->stats.nIterations
          │   stats.nErrors     = pKV->stats.nErrors
          │
          └── sqlite3_mutex_leave(pKV->pMutex)
              return KVSTORE_OK
```

---

## 25. kvstore_errmsg

Returns the last error message string. String is owned by KVStore —
do NOT free the returned pointer.

```
  kvstore_errmsg(kv)
          │
          ├── sqlite3_mutex_enter(pKV->pMutex)
          ├── return pKV->zErrMsg ? pKV->zErrMsg : "no error"
          └── sqlite3_mutex_leave(pKV->pMutex)

  Note: The message is overwritten on the next error.
  Copy it if you need it to persist.
```

---

## 26. Thread Safety Model & Mutex Migration

```
  Two levels of recursive mutexes (SQLITE_MUTEX_RECURSIVE):

  ┌──────────────────────────────────────────────────────────────┐
  │ Level 1: Per-CF mutex  (pCF->pMutex)                        │
  │   Protects: pCF->refCount, pCF->pReadCur                    │
  │   Held by: all CRUD ops (entered BEFORE store mutex)         │
  │                                                              │
  │ Level 2: Store mutex   (pKV->pMutex)                        │
  │   Protects: inTrans, closing, stats, zErrMsg,               │
  │             isCorrupted, pDefaultCF, apCF[]                  │
  │   Held by: all public API functions                          │
  │                                                              │
  │ Lock order: CF mutex first, then store mutex — always.       │
  │ Consistent ordering prevents deadlock.                       │
  └──────────────────────────────────────────────────────────────┘

  Example: two threads writing to different CFs concurrently

  Thread 1: cf_put("logs", k, v)    Thread 2: cf_put("metrics", k, v)
  ─────────────────────────────     ────────────────────────────────
  lock(logs.pMutex)                 lock(metrics.pMutex)
  lock(pKV->pMutex)  ← waits ─┐    lock(pKV->pMutex)   acquired ┐
  BtreeInsert(...)             │    BtreeInsert(...)              │
  unlock(pKV->pMutex)         └──► acquired                      │
  unlock(logs.pMutex)               unlock(pKV->pMutex) ─────────┘
                                    unlock(metrics.pMutex)

  Shutdown safety (closing flag):

  ┌──────────────────────────────────────────────────────────────┐
  │ kvstore_close():                                             │
  │   lock(pKV->pMutex)                                         │
  │   pKV->closing = 1           ← all API calls see this       │
  │   ... free resources ...                                     │
  │   unlock + free pKV->pMutex                                  │
  │                                                              │
  │ Every public API:                                            │
  │   lock(pKV->pMutex)                                         │
  │   if (pKV->closing) { unlock; return KVSTORE_ERROR; }       │
  │                                                              │
  │ Guarantees: no thread can use the store after close starts.  │
  │ No use-after-free possible.                                  │
  └──────────────────────────────────────────────────────────────┘

  Recursive mutex (SQLITE_MUTEX_RECURSIVE):

  ┌──────────────────────────────────────────────────────────────┐
  │ Allows the same thread to re-acquire its own lock.           │
  │                                                              │
  │ Example: kvstore_iterator_close()                            │
  │   → calls kvstore_cf_close()                                 │
  │       → may call sqlite3_mutex_enter(pKV->pMutex)           │
  │         while pKV->pMutex is already held by the caller.     │
  │         Without recursive, this would deadlock.              │
  └──────────────────────────────────────────────────────────────┘
```

### Mutex Migration: kvstore_mutex → sqlite3_mutex

The original implementation had a custom `kvstore_mutex` abstraction
backed by `kvstore_mutex.c`. That file has been **deleted**. All locking
now uses SQLite's native `sqlite3_mutex` API directly.

```
  BEFORE (removed):                   AFTER (current):
  ─────────────────                   ────────────────
  kvstore_mutex.c  ← deleted          (no separate file)
  kvstore_mutex.h  ← deleted          (no separate header)

  struct KVStore {                     struct KVStore {
    kvstore_mutex *pMutex;               sqlite3_mutex *pMutex;
  };                                   };

  struct KVColumnFamily {              struct KVColumnFamily {
    kvstore_mutex *pMutex;               sqlite3_mutex *pMutex;
  };                                   };

  kvstore_mutex_alloc()                sqlite3_mutex_alloc(
    pthread_mutex_init(&m, NULL)         SQLITE_MUTEX_RECURSIVE)
    // default mutex — NOT recursive    // recursive by type flag

  kvstore_mutex_enter(m)               sqlite3_mutex_enter(m)
  kvstore_mutex_leave(m)               sqlite3_mutex_leave(m)
  kvstore_mutex_free(m)                sqlite3_mutex_free(m)
```

**Why the change?**

```
  ┌──────────────────────────────────────────────────────────────┐
  │ Problem 1: Non-recursive default mutex                       │
  │   pthread_mutex_init(NULL) creates a DEFAULT mutex.          │
  │   A thread locking it twice (same thread) is undefined       │
  │   behavior — either deadlock or silent corruption.           │
  │                                                              │
  │   FIXED: sqlite3_mutex_alloc(SQLITE_MUTEX_RECURSIVE) uses   │
  │   PTHREAD_MUTEX_RECURSIVE on POSIX / CRITICAL_SECTION with  │
  │   spin-count on Windows. Same-thread re-entry is safe.       │
  ├──────────────────────────────────────────────────────────────┤
  │ Problem 2: Use-after-free on close                           │
  │   Old code: released then freed pMutex while another thread  │
  │   might be about to wake and dereference it.                 │
  │                                                              │
  │   FIXED: pKV->closing = 1 is set under pKV->pMutex before   │
  │   any memory is freed. All public API functions check        │
  │   closing after acquiring the mutex and return early.        │
  ├──────────────────────────────────────────────────────────────┤
  │ Problem 3: Code duplication                                  │
  │   kvstore_mutex.c reimplemented what SQLite already provides │
  │   (platform-aware, well-tested mutex primitives).            │
  │                                                              │
  │   FIXED: use sqlite3_mutex_alloc/enter/leave/free directly.  │
  │   SQLite handles POSIX/Windows/no-op variants automatically. │
  └──────────────────────────────────────────────────────────────┘
```

**Allocation site:**

```
  kvstore_open_v2():
    pKV->pMutex = sqlite3_mutex_alloc(SQLITE_MUTEX_RECURSIVE);

  kvstore_cf_create() / kvstore_cf_open():
    pCF->pMutex = sqlite3_mutex_alloc(SQLITE_MUTEX_RECURSIVE);

  kvstore_close():
    sqlite3_mutex_leave(pKV->pMutex);
    sqlite3_mutex_free(pKV->pMutex);   ← freed LAST, after all
                                          other resources are gone
```

---

## 27. Persistent Read Transaction & Auto-Transaction Pattern

### Persistent Read Transaction

`kvstore_open` ends by opening a read transaction that stays open for
the lifetime of the connection (`inTrans=1`). This means every
`kvstore_get` and `kvstore_exists` — without an explicit `kvstore_begin`
— pays zero `BeginTrans`/`Commit` overhead.

```
  ┌──────────────────────────────────────────────────────────────┐
  │ KVStore connection lifetime                                  │
  │                                                              │
  │  open()          ──► BeginTrans(read)  ──► inTrans = 1      │
  │                                                              │
  │  get / exists    ──► [use inTrans=1 directly]               │
  │                       no BeginTrans, no Commit              │
  │                       inTrans stays 1                        │
  │                                                              │
  │  begin(write)    ──► Commit(read)       ──► inTrans = 0     │
  │                  ──► BeginTrans(write)  ──► inTrans = 2     │
  │                                                              │
  │  put / delete    ──► [if inTrans==1: Commit(read) → 0]     │
  │   (autoTrans)    ──► BeginTrans(write)  ──► inTrans = 2     │
  │                  ──► ... operation ...                       │
  │                  ──► Commit(write)      ──► inTrans = 0     │
  │                  ──► BeginTrans(read)   ──► inTrans = 1     │
  │                                                              │
  │  commit()        ──► BtreeCommit        ──► inTrans = 0     │
  │                  ──► BeginTrans(read)   ──► inTrans = 1     │
  │                                                              │
  │  rollback()      ──► BtreeRollback      ──► inTrans = 0     │
  │                  ──► BeginTrans(read)   ──► inTrans = 1     │
  │                                                              │
  │  close()         ──► BtreeRollback (any) ──► BtreeClose     │
  └──────────────────────────────────────────────────────────────┘
```

### Cached Read Cursor per CF

Each `KVColumnFamily` has a `pReadCur` — a `BtCursor*` that stays open
across all `get` and `exists` calls on that CF. It is opened lazily on
the first call and reused on every subsequent call.

```
  ┌──────────────────────────────────────────────────────────────┐
  │ KVColumnFamily                                               │
  │   iTable   = 5                                               │
  │   pReadCur ──────────────────────────────────────────────►  │
  │             BtCursor (open, held permanently)                │
  │               │                                              │
  │               └──► sqlite3BtreeIndexMoveto(pCur, key, &res) │
  │                    called on every get/exists                │
  └──────────────────────────────────────────────────────────────┘

  SQLite cursor state machine (why this is safe):

  ┌───────────────────────────────────────────────────────────┐
  │                                                           │
  │   [VALID] ◄────────────── seek ──────────────────────────┐│
  │     │                                                     ││
  │     │ page modified by write                              ││
  │     ▼                                                     ││
  │  [REQUIRESSEEK]  ───── next BtreeIndexMoveto() ──────────┘│
  │                         (transparently restores cursor)    │
  │                                                           │
  │  Result: cached cursor is ALWAYS safe to reuse even       │
  │  after writes to the same CF's B-tree pages.              │
  └───────────────────────────────────────────────────────────┘
```

### Auto-Transaction for Write Paths

Write operations (put, delete, cf_create, cf_drop) that have no
explicit `kvstore_begin` start their own transaction and restore the
persistent read afterward:

```
  kvstore_put / delete / cf_create / cf_drop
          │
          ├── [inTrans == 2]  write already active → no autoTrans
          │
          ├── [inTrans == 1]  persistent read active:
          │     sqlite3BtreeCommit(pBt)    ← release read (frees slot)
          │     inTrans = 0
          │     → fall through to begin write
          │
          ├── [inTrans == 0]  no transaction:
          │     sqlite3BtreeBeginTrans(pBt, 1, 0)
          │     autoTrans = 1
          │     inTrans = 2
          │
          ├── ... perform operation (insert/delete/create/drop) ...
          │
          ├── [SUCCESS + autoTrans]:
          │     sqlite3BtreeCommit(pBt)
          │     inTrans = 0                ← TRANS_NONE window
          │     kvstoreAutoCheckpoint(pKV) ← optional PASSIVE ckpt in window
          │     Restore persistent read:
          │       sqlite3BtreeBeginTrans(pBt, 0, 0) → inTrans = 1
          │
          └── [FAILURE + autoTrans]:
                sqlite3BtreeRollback(pBt, SQLITE_OK, 0)
                inTrans = 0
                (persistent read NOT restored on error path —
                 the next successful write or explicit begin will
                 set it up again)

  Benefit: single operations need no explicit begin/commit:
    kvstore_put(kv, "k", 1, "v", 1)
    // internally: commit_read → begin_write → insert → commit
    //             → [auto-ckpt if walSizeLimit hit] → begin_read

  Batched operations use explicit transaction (one commit for all):
    kvstore_begin(kv, 1)                // commit_read → begin_write
    kvstore_put(kv, "k1", 2, "v1", 2)  // no autoTrans
    kvstore_put(kv, "k2", 2, "v2", 2)  // no autoTrans
    kvstore_commit(kv)                  // commit → begin_read
```

---

## 28. kvstore_checkpoint + WAL Auto-Checkpoint

### Background: Why the WAL grows unboundedly by default

SNKV opens the B-tree layer directly via `sqlite3BtreeOpen` and never calls
`sqlite3Open`. SQLite's default 1000-frame auto-checkpoint hook is registered
inside `sqlite3OpenTail` → `sqlite3_wal_hook`. Because `sqlite3Open` is never
called, that hook is never installed. The WAL grows without bound until
`kvstore_close()` triggers `sqlite3WalClose` on the last connection, which
performs a final checkpoint.

```
  Default SQLite (via sqlite3Open):          SNKV (via sqlite3BtreeOpen):
  ┌───────────────────────────────┐          ┌───────────────────────────────┐
  │ sqlite3Open()                 │          │ sqlite3BtreeOpen()            │
  │  └── sqlite3OpenTail()        │          │   (no OpenTail, no wal hook)  │
  │        └── sqlite3_wal_hook() │          │                               │
  │              registers auto-  │          │  WAL grows freely until       │
  │              checkpoint at    │          │  kvstore_close() calls        │
  │              1000 frames      │          │  sqlite3WalClose()            │
  └───────────────────────────────┘          └───────────────────────────────┘
```

### kvstoreAutoCheckpoint() — static helper

Called in the TRANS_NONE window after every committed write transaction.
The write lock is already released by `sqlite3BtreeCommit` at this point,
so `sqlite3BtreeCheckpoint` can acquire it safely.

Why commit counter (not WAL frame count)?
- `Btree` is an opaque typedef in `kvstore.c` (only `btree.h` is included,
  not `btreeInt.h`). Accessing `pKV->pBt->pBt->pPager` would be a compile
  error ("invalid use of incomplete typedef 'Btree'").
- A commit counter is sufficient: `walCommits` cannot overflow because it
  resets to 0 whenever a checkpoint fires (max value is `walSizeLimit - 1`).

```
  kvstoreAutoCheckpoint(pKV)
          │
          ├── if walSizeLimit == 0 → return immediately (disabled)
          │
          ├── walCommits++
          │     ┌──────────────────────────────────────────────────┐
          │     │  walCommits tracks committed write transactions   │
          │     │  since the last auto-checkpoint.                  │
          │     │  Range: 0 .. walSizeLimit-1  (never overflows)   │
          │     └──────────────────────────────────────────────────┘
          │
          ├── if walCommits < walSizeLimit → return (threshold not reached)
          │
          ├── walCommits = 0         ← reset counter
          │
          └── sqlite3BtreeCheckpoint(pKV->pBt,
                  SQLITE_CHECKPOINT_PASSIVE, NULL, NULL)
                │
                ├── Acquire WAL write lock (non-blocking)
                ├── Copy committed WAL frames → DB file pages
                ├── Advance the checkpoint watermark
                └── Release WAL write lock

  Timeline with walSizeLimit=3:

  commit #1 → walCommits=1  (no checkpoint)
  commit #2 → walCommits=2  (no checkpoint)
  commit #3 → walCommits=3  ≥ limit → PASSIVE checkpoint → walCommits=0
  commit #4 → walCommits=1  (no checkpoint)
  ...
```

### kvstore_checkpoint() — public API

Exposes manual checkpoint control. Callers can use any of the four SQLite
checkpoint modes. An active write transaction causes an immediate `BUSY`
return; the persistent read transaction is silently released and restored
around the checkpoint operation (required by `sqlite3BtreeCheckpoint`).

```
  kvstore_checkpoint(pKV, mode, pnLog, pnCkpt)
          │
          ├── sqlite3_mutex_enter(pKV->pMutex)
          │
          ├── if closing → return KVSTORE_ERROR
          │
          ├── if inTrans == 2  (write transaction active)
          │     kvstoreSetError("commit or rollback the write transaction first")
          │     return KVSTORE_BUSY
          │
          ├── zero-init output params
          │     if pnLog  → *pnLog  = 0
          │     if pnCkpt → *pnCkpt = 0
          │
          ├── Release persistent read transaction (required for TRANS_NONE)
          │     if inTrans == 1:
          │       sqlite3BtreeCommit(pBt)
          │       inTrans = 0
          │
          ├── Run checkpoint
          │     ┌──────────────────────────────────────────────────────────────┐
          │     │ #ifndef SQLITE_OMIT_WAL                                      │
          │     │   sqlite3BtreeCheckpoint(pBt, mode, pnLog, pnCkpt)          │
          │     │ #else                                                        │
          │     │   rc = SQLITE_OK  (no-op on non-WAL build)                  │
          │     │ #endif                                                       │
          │     │                                                              │
          │     │ mode meanings:                                               │
          │     │   PASSIVE  (0) — copy frames w/o blocking readers/writers   │
          │     │                  may not copy all if a reader is active      │
          │     │   FULL     (1) — wait for all readers, then copy all frames  │
          │     │   RESTART  (2) — like FULL + reset WAL write position        │
          │     │                  WAL file not truncated                      │
          │     │   TRUNCATE (3) — like RESTART + truncate WAL file to 0 bytes│
          │     │                                                              │
          │     │ Output semantics (WAL mode):                                │
          │     │   pnLog  = mxFrame (total frames written to WAL ever)       │
          │     │   pnCkpt = number of frames successfully copied to DB file  │
          │     │   pnLog == pnCkpt → no frames stuck, WAL fully checkpointed │
          │     │   pnLog == 0      → WAL was truncated (TRUNCATE mode only)  │
          │     │                                                              │
          │     │ Non-WAL database (DELETE journal mode):                     │
          │     │   returns KVSTORE_OK, pnLog=0, pnCkpt=0  (no-op)           │
          │     └──────────────────────────────────────────────────────────────┘
          │
          ├── Restore persistent read transaction
          │     if sqlite3BtreeBeginTrans(pBt, 0, 0) == SQLITE_OK:
          │       inTrans = 1
          │
          ├── if rc != SQLITE_OK:
          │     kvstoreSetError("checkpoint failed: error %d", rc)
          │
          ├── sqlite3_mutex_leave(pKV->pMutex)
          └── return rc  (KVSTORE_OK | KVSTORE_BUSY | KVSTORE_ERROR)

  State diagram around checkpoint:

    inTrans=1                     inTrans=0               inTrans=1
  (persistent read)    BtreeCommit    │   BtreeCheckpoint    │   BtreeBeginTrans(0)
       ─────────────────────────────►─┼──────────────────────┼─────────────────────►
                                      │   TRANS_NONE window   │

  Write transaction guard:

    kvstore_begin(kv, 1)  →  inTrans=2
    kvstore_checkpoint(kv, PASSIVE, NULL, NULL)
        → returns KVSTORE_BUSY immediately (write txn not committed)
    kvstore_rollback(kv)  →  inTrans=1
    kvstore_checkpoint(kv, PASSIVE, NULL, NULL)
        → succeeds: releases read, checkpoints, restores read
```

### Choosing between walSizeLimit and kvstore_checkpoint

```
  ┌──────────────────────┬──────────────────────────────────────────────────┐
  │ Mechanism            │ Use when                                          │
  ├──────────────────────┼──────────────────────────────────────────────────┤
  │ walSizeLimit=N       │ Set-and-forget. Every N committed writes trigger  │
  │ (auto, PASSIVE)      │ a PASSIVE checkpoint automatically. No caller     │
  │                      │ involvement needed. Suitable for most workloads.  │
  ├──────────────────────┼──────────────────────────────────────────────────┤
  │ kvstore_checkpoint() │ Explicit control: run at shutdown, after a large  │
  │ (any mode)           │ batch, or to shrink the WAL (TRUNCATE mode).      │
  │                      │ Combine with walSizeLimit=0 for full manual       │
  │                      │ control of when checkpoints occur.                │
  └──────────────────────┴──────────────────────────────────────────────────┘
```

---

## Quick Reference: kvstore → btree mapping

| kvstore function             | btree calls                                                                             |
|------------------------------|-----------------------------------------------------------------------------------------|
| `kvstore_open_v2`            | `BtreeOpen`, `SetPageSize`, `SetCacheSize`, `SetPagerFlags`, `SetAutoVacuum`, `SetVersion`, `GetMeta`, `CreateTable`, `UpdateMeta`, `BeginTrans(0)` |
| `kvstore_open`               | delegates to `kvstore_open_v2` (sets `journalMode`; all other fields default) |
| `kvstore_close`              | `BtreeRollback` (if txn active), `BtreeClose`                                          |
| `kvstore_begin`              | `BtreeCommit` (if inTrans=1) + `BtreeBeginTrans(wrflag)`                               |
| `kvstore_commit`             | `BtreeCommit` + `kvstoreAutoCheckpoint` (TRANS_NONE window) + `BtreeBeginTrans(0)`     |
| `kvstore_rollback`           | `BtreeRollback` + `BtreeBeginTrans(0)` (restore persistent read)                       |
| `kvstore_put`                | `BtreeCursor(write)`, `BtreeInsert`, `BtreeCloseCursor`                                |
| `kvstore_get`                | `kvstoreGetReadCursor` (cached, no open/close), `BtreeIndexMoveto`, `BtreePayload` ×2  |
| `kvstore_delete`             | `BtreeCursor(write)`, `BtreeIndexMoveto`, `BtreeDelete`, `BtreeCloseCursor`            |
| `kvstore_exists`             | `kvstoreGetReadCursor` (cached, no open/close), `BtreeIndexMoveto`                     |
| `kvstore_cf_create`          | `BtreeCreateTable(BLOBKEY)`, `BtreeInsert` (meta), `UpdateMeta`                        |
| `kvstore_cf_open`            | `BtreeCursor` (meta), `BtreeTableMoveto`, `BtreePayload`                               |
| `kvstore_cf_drop`            | `BtreeDropTable`, `BtreeDelete` (meta), `UpdateMeta`                                   |
| `kvstore_cf_list`            | `BtreeCursor` (meta), `BtreeFirst`, `BtreeNext`, `BtreePayload`                        |
| `kvstore_iterator_create`    | `BtreeCursor` (own cursor, not pReadCur)                                               |
| `kvstore_iterator_first`     | `BtreeFirst` or `BtreeIndexMoveto` (prefix seek)                                       |
| `kvstore_iterator_next`      | `BtreeNext`                                                                             |
| `kvstore_iterator_key/value` | `BtreePayloadSize`, `BtreePayload`                                                     |
| `kvstore_iterator_close`     | `BtreeCloseCursor`, `BtreeCommit` (only if ownsTrans)                                  |
| `kvstore_incremental_vacuum` | `BtreeIncrVacuum` (loop)                                                               |
| `kvstore_integrity_check`    | `BtreeIntegrityCheck`                                                                   |
| `kvstore_sync`               | `BtreeCommit` + `BtreeBeginTrans(0)`                                                   |
| `kvstore_checkpoint`         | `BtreeCommit` (release read) + `BtreeCheckpoint(mode)` + `BtreeBeginTrans(0)` (restore read) |
| `kvstoreAutoCheckpoint`      | `BtreeCheckpoint(PASSIVE)` — called in TRANS_NONE window after each committed write    |

---

## Full Stack Trace Example

A complete `kvstore_put` showing all four layers:

```
  kvstore_put(kv, "hello", 5, "world", 5)
  │
  │ LAYER 4: kvstore.c
  ├── lock CF mutex → lock KV mutex
  ├── if inTrans==1: BtreeCommit(read) → inTrans=0
  ├── BtreeBeginTrans(1,0)             → inTrans=2  (autoTrans)
  ├── kvstoreEncodeBlob()  [stack buf] → [00 00 00 05][hello][world]
  ├── BtreeCursor(write)
  │
  │ LAYER 3: btree.c
  ├── sqlite3BtreeInsert(pCur, {pKey=blob, nKey=15})
  │   ├── BtreeIndexMoveto() → traverse root → internal → leaf
  │   │     sqlite3VdbeRecordCompare() at each level
  │   ├── insertCell() → write cell to leaf page
  │   ├── balance_nonroot() → split/merge pages if needed
  │   └── PagerWrite(pPage) → mark page dirty
  │
  │ LAYER 2: pager.c
  ├── pager_write()
  │   ├── [Rollback] writeJournalHdr() → OsWrite(journal)
  │   └── [WAL]      walWriteLock()    → append WAL frame
  │
  │ LAYER 1: os.c / os_unix.c
  ├── sqlite3OsWrite() → pwrite(fd, buf, amt, offset)
  ├── sqlite3OsSync()  → fsync(fd)   (on commit)
  └── sqlite3OsLock()  → fcntl(fd, F_SETLK, ...)

  Back in LAYER 4:
  ├── BtreeCloseCursor(write cursor)
  ├── BtreeCommit()                   → autoTrans commit
  ├── BtreeBeginTrans(0,0)            → inTrans=1  (persistent read restored)
  └── unlock KV mutex → unlock CF mutex
```
