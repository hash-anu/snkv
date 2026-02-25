# Pager Layer Operations

All `sqlite3Pager*` functions that `btree.c` calls during SNKV operations.
The pager sits between the B-tree and the OS/VFS layer, managing page cache,
transactions, journaling, and WAL.

---

## Architecture Context

```
  keyvaluestore.c  (API)
      │
  btree.c    (B-tree)
      │
      ▼
 ┌──────────┐
 │  pager.c  │  ◄── THIS DOCUMENT
 └──────────┘
      │
  os.c / os_unix.c / os_win.c  (VFS)
      │
    Disk
```

---

## Pager State Machine

The pager has 7 states. Every B-tree operation triggers state transitions:

```
                          OPEN ◄──────┬──────┐
                            │         │      │
                            ▼         │      │
             ┌──────────► READER ─────┘      │
             │              │                │
             │              ▼                │
             │◄───── WRITER_LOCKED ───────► ERROR
             │              │                ▲
             │              ▼                │
             │◄──── WRITER_CACHEMOD ────────►│
             │              │                │
             │              ▼                │
             │◄───── WRITER_DBMOD ──────────►│
             │              │                │
             │              ▼                │
             └◄──── WRITER_FINISHED ───────►─┘

  Transitions:
    OPEN  ──► READER            sqlite3PagerSharedLock()
    READER ──► WRITER_LOCKED    sqlite3PagerBegin()
    WRITER_LOCKED ──► CACHEMOD  pager_open_journal()  [internal]
    CACHEMOD ──► DBMOD          syncJournal()          [internal]
    DBMOD ──► FINISHED          sqlite3PagerCommitPhaseOne()
    WRITER_* ──► READER         sqlite3PagerCommitPhaseTwo()
    WRITER_* ──► READER         sqlite3PagerRollback()
    WRITER_* ──► ERROR          pager_error()          [on failure]
    ERROR ──► OPEN              pager_unlock()         [internal]
```

---

## 1. sqlite3PagerOpen

Opens a pager and associates it with a database file. Called by `sqlite3BtreeOpen`.

```
  sqlite3BtreeOpen("test.db")
          │
          ▼
  sqlite3PagerOpen(pVfs, &pPager, "test.db", flags)
          │
          ├── Allocate Pager struct
          │     ┌─────────────────────────────────┐
          │     │ Pager                            │
          │     │   fd      → sqlite3_file (main)  │
          │     │   jfd     → sqlite3_file (jrnl)  │
          │     │   sjfd    → sqlite3_file (sub)   │
          │     │   pPCache → PCache               │
          │     │   pVfs    → sqlite3_vfs          │
          │     │   eState  = PAGER_OPEN            │
          │     │   eLock   = NO_LOCK               │
          │     └─────────────────────────────────┘
          │
          ├── sqlite3OsOpen(pVfs, "test.db", fd)
          │     Opens the database file handle
          │
          ├── sqlite3PcacheOpen(&pPager->pPCache)
          │     Initialize page cache (default 2000 pages)
          │
          ├── Allocate temp space (1 page for scratch)
          │
          └── State: PAGER_OPEN
```

---

## 2. sqlite3PagerClose

Closes pager, rolling back any active transaction first. Called by `sqlite3BtreeClose`.

```
  sqlite3BtreeClose()
          │
          ▼
  sqlite3PagerClose(pPager)
          │
          ├── if eState >= WRITER_LOCKED:
          │     sqlite3PagerRollback()     ── rollback active txn
          │
          ├── if WAL mode:
          │     sqlite3WalClose()          ── close WAL connection
          │
          ├── sqlite3OsClose(pPager->jfd)  ── close journal file
          │
          ├── sqlite3OsClose(pPager->fd)   ── close database file
          │
          ├── sqlite3PcacheClose(pPCache)  ── free page cache
          │
          ├── sqlite3PageFree(pTmpSpace)   ── free scratch buffer
          │
          └── sqlite3_free(pPager)         ── free pager struct
```

---

## 3. sqlite3PagerReadFileheader

Reads the first N bytes of the database file (typically 100 bytes for the SQLite header).
Called during `sqlite3BtreeOpen` to detect page size, version, etc.

```
  sqlite3BtreeOpen()
          │
          ▼
  sqlite3PagerReadFileheader(pPager, 100, zDbHeader)
          │
          ├── sqlite3OsRead(fd, zDbHeader, 100, offset=0)
          │     ┌────────────────────────────────────────┐
          │     │ DB File Header (first 100 bytes)       │
          │     │  [16-17] page size (big-endian)        │
          │     │  [18]    file format write version      │
          │     │  [19]    file format read version       │
          │     │  [20]    reserved space per page        │
          │     │  [28-31] total pages in file            │
          │     │  [32-35] first freelist trunk page      │
          │     │  [92-95] schema version cookie          │
          │     └────────────────────────────────────────┘
          │
          └── Return header bytes to btree.c for parsing
```

---

## 4. sqlite3PagerSetPagesize

Sets or changes the page size. Called during btree open after reading the header.

```
  sqlite3BtreeOpen()  /  sqlite3BtreeSetPageSize()
          │
          ▼
  sqlite3PagerSetPagesize(pPager, &pageSize, nReserve)
          │
          ├── Validate: 512 ≤ pageSize ≤ 65536, power of 2
          │
          ├── if pageSize changed:
          │     ├── Flush and discard all cached pages
          │     │     sqlite3PcacheSetPageSize(pPCache, pageSize)
          │     │
          │     ├── Reallocate temp space for new page size
          │     │
          │     └── Update pPager->pageSize = pageSize
          │
          └── pPager->nReserve = nReserve
                (reserved bytes at end of each page)
```

---

## 5. sqlite3PagerSetCachesize

Sets the maximum number of pages to hold in the page cache.
Called by `sqlite3BtreeSetCacheSize`.

```
  sqlite3BtreeSetCacheSize(pBt, 2000)
          │
          ▼
  sqlite3PagerSetCachesize(pPager, 2000)
          │
          └── sqlite3PcacheSetCachesize(pPCache, 2000)
                │
                ├── nMax = 2000 (max cached pages)
                │
                └── if nPage > nMax:
                      Evict LRU pages until nPage ≤ nMax
                      ┌──────────────────────────┐
                      │ Page Cache (LRU list)     │
                      │  [page5] ── oldest, evict │
                      │  [page2]                  │
                      │  [page8]                  │
                      │  [page1] ── newest, keep  │
                      └──────────────────────────┘
```

---

## 6. sqlite3PagerSharedLock

Acquires a SHARED lock on the database for reading. Transitions OPEN → READER.
Called by `sqlite3BtreeBeginTrans`.

```
  sqlite3BtreeBeginTrans(pBt, wrflag=0)
          │
          ▼
  sqlite3PagerSharedLock(pPager)
          │
          ├── [Rollback mode]
          │     ├── sqlite3OsLock(fd, SHARED_LOCK)
          │     │     ┌──────────────────────────────────┐
          │     │     │ Lock Levels (file locks)         │
          │     │     │  NO_LOCK       (0) - no lock     │
          │     │     │  SHARED_LOCK   (1) - read ok     │
          │     │     │  RESERVED_LOCK (2) - will write  │
          │     │     │  PENDING_LOCK  (3) - upgrading   │
          │     │     │  EXCLUSIVE_LOCK(4) - writing     │
          │     │     └──────────────────────────────────┘
          │     │
          │     ├── Check for hot journal:
          │     │     sqlite3OsAccess(pVfs, zJournal, EXISTS)
          │     │     if hot journal found:
          │     │       sqlite3OsLock(fd, EXCLUSIVE_LOCK)
          │     │       pager_playback()   ── replay journal
          │     │       sqlite3OsUnlock(fd, SHARED_LOCK)
          │     │
          │     └── Read page 1 to get dbSize, version cookie
          │
          ├── [WAL mode]
          │     ├── sqlite3WalBeginReadTransaction()
          │     │     ├── sqlite3OsShmLock(SHARED on read-mark)
          │     │     └── Read WAL-index to find latest snapshot
          │     │
          │     └── No file lock needed (WAL uses shm locks)
          │
          └── State: PAGER_READER
```

---

## 7. sqlite3PagerGet

Fetches a page by page number. If not in cache, reads from disk (or WAL).
The most frequently called pager function — used for every B-tree node access.

```
  sqlite3BtreeInsert() / sqlite3BtreeNext() / ...
          │
          ▼
  sqlite3PagerGet(pPager, pgno=5, &pDbPage, clrFlag)
          │
          ├── Step 1: Check page cache
          │     sqlite3PcacheFetch(pPCache, pgno)
          │     ┌──────────────────────────────┐
          │     │ Page Cache (hash table)       │
          │     │  pgno=1 → [page1]            │
          │     │  pgno=3 → [page3]            │
          │     │  pgno=5 → ???                │
          │     └──────────────────────────────┘
          │
          ├── Step 2a: Cache HIT
          │     └── Return cached page immediately
          │         (no disk I/O!)
          │
          └── Step 2b: Cache MISS
                │
                ├── Allocate page in cache
                │     sqlite3PcacheFetchStress() if cache full
                │       └── pagerStress() → spill dirty page to disk
                │
                ├── [WAL mode] Check WAL first:
                │     sqlite3WalFindFrame(pWal, pgno, &iFrame)
                │     if iFrame > 0:
                │       sqlite3WalReadFrame(pWal, iFrame, pData)
                │       ┌─────────────────────────────────┐
                │       │ WAL File                         │
                │       │  frame 1: pgno=3 [data...]      │
                │       │  frame 2: pgno=5 [data...] ◄──  │
                │       │  frame 3: pgno=1 [data...]      │
                │       └─────────────────────────────────┘
                │       └── Return page from WAL
                │
                ├── [No WAL frame] Read from database file:
                │     sqlite3OsRead(fd, pData, pageSize, offset)
                │     offset = (pgno - 1) * pageSize
                │     ┌─────────────────────────────────┐
                │     │ Database File                    │
                │     │  [page1][page2][page3][page4]    │
                │     │     0    4096   8192  12288      │
                │     │  [page5] ◄── read at 16384      │
                │     └─────────────────────────────────┘
                │
                └── Insert page into cache, return pDbPage
```

---

## 8. sqlite3PagerLookup

Looks up a page in the cache **without reading from disk**. Returns NULL if not cached.
Used by btree.c to check if a page is already in memory.

```
  btree.c (optimistic check)
          │
          ▼
  sqlite3PagerLookup(pPager, pgno=7)
          │
          ├── sqlite3PcacheFetch(pPCache, pgno, createFlag=0)
          │
          ├── Found in cache → return page pointer
          │
          └── Not in cache → return NULL
              (no disk I/O, no allocation)
```

---

## 9. sqlite3PagerWrite

Marks a page as dirty (writable). This triggers journal writing in rollback mode.
Called before ANY modification to a page's data.

```
  btree.c: modify cell data on page 5
          │
          ▼
  sqlite3PagerWrite(pDbPage)
          │
          ├── if page already dirty:
          │     └── return OK (nothing to do)
          │
          ├── [Rollback mode - first write to this page in txn]
          │     ├── Read original page content
          │     ├── Write original to journal:
          │     │     sqlite3OsWrite(jfd, origData, pageSize, jrnlOff)
          │     │     ┌──────────────────────────────────┐
          │     │     │ Journal File                      │
          │     │     │  [header: page count, etc.]       │
          │     │     │  [pgno=5][original page5 data]    │
          │     │     │  [checksum]                       │
          │     │     └──────────────────────────────────┘
          │     │
          │     └── If sector-size > page-size:
          │           Journal all pages in same sector
          │           (prevents partial sector writes)
          │
          ├── [WAL mode]
          │     └── No journal write needed here
          │         (dirty pages written to WAL at commit)
          │
          ├── sqlite3PcacheMakeDirty(pDbPage)
          │     ┌──────────────────────────────┐
          │     │ Dirty Page List               │
          │     │  page3 → page5 → page9 → ... │
          │     └──────────────────────────────┘
          │
          └── State: ≥ WRITER_CACHEMOD
```

---

## 10. sqlite3PagerDontWrite

Tells the pager that a page's modifications can be discarded.
Used by btree.c when a newly allocated page doesn't need to be committed.

```
  btree.c: allocated page but don't need it
          │
          ▼
  sqlite3PagerDontWrite(pDbPage)
          │
          ├── if page is dirty AND pgno ≤ dbOrigSize:
          │     └── Clear dirty flag
          │         sqlite3PcacheMakeClean(pDbPage)
          │
          └── Optimization: avoids writing unused pages
              to journal or WAL
```

---

## 11. sqlite3PagerMovepage

Moves a page from one page number to another. Used during auto-vacuum/incremental-vacuum
and by B-tree balance operations to relocate overflow pages.

```
  btree.c: relocate overflow page 12 → page 5
          │
          ▼
  sqlite3PagerMovepage(pPager, pDbPage, pgno=5, isCommit)
          │
          ├── [Rollback mode]
          │     Write original page 5 to journal (if not done)
          │
          ├── Update page cache:
          │     ┌──────────────────────────────────┐
          │     │ Before:                           │
          │     │  cache[12] = pDbPage              │
          │     │                                   │
          │     │ After:                            │
          │     │  cache[5]  = pDbPage              │
          │     │  (pgno 12 entry removed)          │
          │     └──────────────────────────────────┘
          │     sqlite3PagerRekey(pDbPage, 12→5)
          │
          ├── Mark page 5 as dirty
          │
          └── If page 12 was journaled, add pgno=5
              to the "need not write" list
```

---

## 12. sqlite3PagerRef / sqlite3PagerUnref

Reference counting for pages. Prevents a cached page from being evicted while in use.

```
  sqlite3PagerRef(pDbPage)
          │
          └── pDbPage->nRef++
              Page pinned in cache

  sqlite3PagerUnref(pDbPage)    /   sqlite3PagerUnrefNotNull(pDbPage)
          │
          ├── pDbPage->nRef--
          │
          └── if nRef == 0:
                Page is unpinned (eligible for eviction)
                ┌──────────────────────────────────┐
                │ Page Cache                        │
                │  [page1] nRef=1 - PINNED          │
                │  [page3] nRef=0 - can evict (LRU) │
                │  [page5] nRef=2 - PINNED          │
                │  [page9] nRef=0 - can evict (LRU) │
                └──────────────────────────────────┘

  sqlite3PagerUnrefPageOne(pDbPage)
          │
          └── Special fast-path for page 1
              (most frequently accessed page)
```

---

## 13. sqlite3PagerBegin

Begins a write transaction. Acquires RESERVED lock.
Transitions READER → WRITER_LOCKED.

```
  sqlite3BtreeBeginTrans(pBt, wrflag=1)
          │
          ▼
  sqlite3PagerBegin(pPager, exFlag=0, subjInMemory=0)
          │
          ├── Assert: eState == PAGER_READER
          │
          ├── [Rollback mode]
          │     ├── sqlite3OsLock(fd, RESERVED_LOCK)
          │     │     ┌──────────────────────────┐
          │     │     │ File Lock Upgrade         │
          │     │     │  SHARED → RESERVED        │
          │     │     │  (other readers still OK)  │
          │     │     │  (no other writers)         │
          │     │     └──────────────────────────┘
          │     │
          │     └── Journal not opened yet
          │         (deferred until first write)
          │
          ├── [WAL mode]
          │     ├── sqlite3WalBeginWriteTransaction()
          │     │     sqlite3OsShmLock(EXCLUSIVE on WAL_WRITE_LOCK)
          │     │     ┌──────────────────────────────┐
          │     │     │ WAL Lock Slots (shm)          │
          │     │     │  [0] WRITE  = EXCLUSIVE ◄──   │
          │     │     │  [1] CKPT   = unlocked        │
          │     │     │  [2] RECOVER= unlocked        │
          │     │     │  [3] READ0  = shared          │
          │     │     │  [4] READ1  = shared          │
          │     │     └──────────────────────────────┘
          │     │
          │     └── No file-level lock needed
          │
          └── State: PAGER_WRITER_LOCKED
```

---

## 14. sqlite3PagerCommitPhaseOne

Phase 1 of commit: write all dirty pages, sync. This is the "point of no return."
Transitions WRITER_DBMOD → WRITER_FINISHED.

```
  sqlite3BtreeCommit()
          │
          ▼
  sqlite3PagerCommitPhaseOne(pPager, zSuper=NULL, noSync=0)
          │
          ├── [Rollback mode]
          │     │
          │     ├── Step 1: Sync journal file
          │     │     sqlite3OsSync(jfd, syncFlags)
          │     │     ┌─────────────────────────────────┐
          │     │     │ Journal File (complete)          │
          │     │     │  [header: nRec, dbSize, cksum]   │
          │     │     │  [pgno=3][orig data][checksum]   │
          │     │     │  [pgno=5][orig data][checksum]   │
          │     │     │  [pgno=9][orig data][checksum]   │
          │     │     │  ───────── fsync ─────────────   │
          │     │     └─────────────────────────────────┘
          │     │
          │     ├── Step 2: Write dirty pages to database
          │     │     for each dirty page in cache:
          │     │       sqlite3OsWrite(fd, pageData, pageSize, offset)
          │     │     ┌─────────────────────────────────┐
          │     │     │ Database File                    │
          │     │     │  [page1][page2][page3'][page4]   │
          │     │     │              ▲ new data          │
          │     │     │  [page5'][...][...][page9']      │
          │     │     │     ▲                  ▲         │
          │     │     │   modified          modified     │
          │     │     └─────────────────────────────────┘
          │     │
          │     ├── Step 3: Sync database file
          │     │     sqlite3OsSync(fd, syncFlags)
          │     │
          │     └── Step 4: Truncate DB if needed
          │           sqlite3OsTruncate(fd, newSize)
          │
          ├── [WAL mode]
          │     │
          │     ├── Step 1: Collect dirty pages
          │     │     sqlite3PcacheDirtyList(pPCache)
          │     │     ┌──────────────────────────┐
          │     │     │ Dirty list:               │
          │     │     │  page3 → page5 → page9    │
          │     │     └──────────────────────────┘
          │     │
          │     ├── Step 2: Write frames to WAL
          │     │     sqlite3WalFrames(pWal, pageSize, pList, ...)
          │     │     ┌─────────────────────────────────────┐
          │     │     │ WAL File (append)                    │
          │     │     │  [WAL header: magic, version, ...]   │
          │     │     │  ...existing frames...               │
          │     │     │  [frame N+1: pgno=3][page3 data]     │
          │     │     │  [frame N+2: pgno=5][page5 data]     │
          │     │     │  [frame N+3: pgno=9][page9 data]     │
          │     │     │  ─────────── fsync ───────────────   │
          │     │     └─────────────────────────────────────┘
          │     │
          │     └── Step 3: Update WAL-index (shm)
          │           ┌──────────────────────────────┐
          │           │ WAL-Index (shared memory)     │
          │           │  hash table: pgno → frame#    │
          │           │  mxFrame = N+3                │
          │           │  nBackfill = ...              │
          │           └──────────────────────────────┘
          │
          └── State: PAGER_WRITER_FINISHED
```

---

## 15. sqlite3PagerCommitPhaseTwo

Phase 2 of commit: finalize the transaction and release locks.
Transitions WRITER_FINISHED → READER.

```
  sqlite3BtreeCommit() (continued)
          │
          ▼
  sqlite3PagerCommitPhaseTwo(pPager)
          │
          ├── [Rollback mode]
          │     ├── Delete or truncate journal file
          │     │     sqlite3OsDelete(pVfs, zJournal)
          │     │     ┌─────────────────────┐
          │     │     │ Journal File         │
          │     │     │  DELETED ✗           │
          │     │     └─────────────────────┘
          │     │     (this is the atomic commit point —
          │     │      no journal = transaction committed)
          │     │
          │     └── Downgrade lock:
          │           sqlite3OsUnlock(fd, SHARED_LOCK)
          │
          ├── [WAL mode]
          │     ├── sqlite3WalEndWriteTransaction()
          │     │     sqlite3OsShmLock(unlock WAL_WRITE_LOCK)
          │     │
          │     └── No file lock change needed
          │
          ├── Clear dirty page list
          │     sqlite3PcacheCleanAll(pPCache)
          │
          └── State: PAGER_READER
```

---

## 16. sqlite3PagerRollback

Rolls back the current write transaction. Restores original page content.
Transitions WRITER_* → READER.

```
  sqlite3BtreeRollback()
          │
          ▼
  sqlite3PagerRollback(pPager)
          │
          ├── [Rollback mode]
          │     │
          │     ├── Step 1: Replay journal (pager_playback)
          │     │     ┌──────────────────────────────────┐
          │     │     │ Journal File                      │
          │     │     │  [pgno=3][orig page3][cksum] ──►  │
          │     │     │  [pgno=5][orig page5][cksum] ──►  │
          │     │     │  [pgno=9][orig page9][cksum] ──►  │
          │     │     └──────────────────────────────────┘
          │     │         │         │         │
          │     │         ▼         ▼         ▼
          │     │     ┌──────────────────────────────────┐
          │     │     │ Database File (restored)          │
          │     │     │  [page3 orig][page5 orig][page9]  │
          │     │     └──────────────────────────────────┘
          │     │
          │     ├── Step 2: Delete journal
          │     │     sqlite3OsDelete(pVfs, zJournal)
          │     │
          │     ├── Step 3: Truncate DB to original size
          │     │     sqlite3OsTruncate(fd, dbOrigSize * pageSize)
          │     │
          │     └── Step 4: Release lock
          │           sqlite3OsUnlock(fd, SHARED_LOCK)
          │
          ├── [WAL mode]
          │     ├── Discard WAL frames written by this txn
          │     │     sqlite3WalUndo(pWal, callback)
          │     │     ┌──────────────────────────────────┐
          │     │     │ WAL File                          │
          │     │     │  [frame 1-N]  ── keep (prior)    │
          │     │     │  [frame N+1]  ── discard ✗       │
          │     │     │  [frame N+2]  ── discard ✗       │
          │     │     │  mxFrame reset to N               │
          │     │     └──────────────────────────────────┘
          │     │
          │     └── Release write lock:
          │           sqlite3WalEndWriteTransaction()
          │
          ├── Discard all dirty pages from cache
          │     sqlite3PcacheCleanAll(pPCache)
          │     sqlite3PcacheTruncate(pPCache, dbOrigSize+1)
          │
          └── State: PAGER_READER
```

---

## 17. sqlite3PagerOpenSavepoint / sqlite3PagerSavepoint

Manages savepoints (nested transactions). Used by `sqlite3BtreeBeginTrans`
for nested savepoints.

```
  sqlite3PagerOpenSavepoint(pPager, nSavepoint=2)
          │
          ├── Allocate savepoint records
          │     ┌────────────────────────────────────────┐
          │     │ Savepoint Array                         │
          │     │  [0] SP1: iOffset=1024, iSubRec=0      │
          │     │  [1] SP2: iOffset=2048, iSubRec=3      │
          │     │                                        │
          │     │  iOffset  = journal file position       │
          │     │  iSubRec  = sub-journal record count    │
          │     │  iHdrOffset = journal header position   │
          │     └────────────────────────────────────────┘
          │
          └── Each savepoint remembers the journal state
              so we can roll back to that exact point


  sqlite3PagerSavepoint(pPager, op=SAVEPOINT_ROLLBACK, iSavepoint=1)
          │
          ├── [Rollback mode]
          │     ├── Replay sub-journal from SP2's iSubRec
          │     │     to current position
          │     │     ┌──────────────────────────────┐
          │     │     │ Sub-Journal                   │
          │     │     │  [rec 0] page7 orig ── keep  │
          │     │     │  [rec 1] page2 orig ── keep  │
          │     │     │  [rec 2] page8 orig ── keep  │
          │     │     │  [rec 3] page4 orig ── undo  │
          │     │     │  [rec 4] page6 orig ── undo  │
          │     │     └──────────────────────────────┘
          │     │
          │     └── Replay main journal from SP2's iOffset
          │
          ├── [WAL mode]
          │     └── sqlite3WalSavepointUndo(pWal, ...)
          │           Reset WAL mxFrame to savepoint's value
          │
          └── Discard pages modified after savepoint


  sqlite3PagerSavepoint(pPager, op=SAVEPOINT_RELEASE, iSavepoint=1)
          │
          └── Drop savepoint (merge into parent)
              Truncate savepoint array to iSavepoint
```

---

## 18. sqlite3PagerOpenWal

Switches from rollback journal mode to WAL mode.
Called when `sqlite3BtreeOpen` detects a WAL file, or when WAL is explicitly set.

```
  sqlite3BtreeBeginTrans() / sqlite3BtreeSetVersion()
          │
          ▼
  sqlite3PagerOpenWal(pPager, &isWal)
          │
          ├── sqlite3OsAccess(pVfs, zWal, EXISTS)
          │     Check if "test.db-wal" exists
          │
          ├── sqlite3WalOpen(pVfs, fd, zWal, &pWal)
          │     ├── sqlite3OsOpen(pVfs, "test.db-wal", pWalFd)
          │     │
          │     └── Allocate Wal struct:
          │           ┌────────────────────────────────┐
          │           │ Wal                             │
          │           │   pDbFd   → database file       │
          │           │   pWalFd  → WAL file            │
          │           │   pVfs    → VFS                  │
          │           │   szPage  = 4096                 │
          │           │   mxFrame = 0                    │
          │           │   hdr     → WAL-index header     │
          │           └────────────────────────────────┘
          │
          ├── pPager->pWal = pWal
          │
          └── Mode: WAL (concurrent readers + single writer)
```

---

## 19. sqlite3PagerCheckpoint

Runs a WAL checkpoint — copies WAL frames back to the main database file.
Called by `sqlite3BtreeCheckpoint`.

```
  sqlite3BtreeCheckpoint(pBt, eMode)
          │
          ▼
  sqlite3PagerCheckpoint(pPager, db, eMode, pnLog, pnCkpt)
          │
          ▼
  sqlite3WalCheckpoint(pWal, db, eMode, ...)
          │
          ├── Step 1: Acquire checkpoint lock
          │     sqlite3OsShmLock(EXCLUSIVE on CKPT slot)
          │
          ├── Step 2: Copy WAL frames → database file
          │     for each frame from nBackfill+1 to mxFrame:
          │       ┌──────────────────────────────────────┐
          │       │ WAL File                              │
          │       │  [frame i: pgno=P] ───────────────►  │
          │       │                                      │
          │       │ Database File                        │
          │       │  [page P] ◄── overwritten            │
          │       └──────────────────────────────────────┘
          │       sqlite3OsRead(pWalFd, ...)
          │       sqlite3OsWrite(fd, ..., (P-1)*pageSize)
          │
          ├── Step 3: Sync database file
          │     sqlite3OsSync(fd, syncFlags)
          │
          ├── Step 4: Update WAL-index
          │     nBackfill = mxFrame (all frames checkpointed)
          │
          ├── Step 5: [TRUNCATE mode] Reset WAL
          │     sqlite3OsTruncate(pWalFd, 0)
          │     mxFrame = 0
          │
          ├── Checkpoint modes:
          │     ┌─────────────────────────────────────────┐
          │     │ PASSIVE   - checkpoint what we can,     │
          │     │             don't block readers          │
          │     │ FULL      - wait for readers, then       │
          │     │             checkpoint everything        │
          │     │ RESTART   - like FULL + reset WAL        │
          │     │ TRUNCATE  - like RESTART + truncate WAL  │
          │     └─────────────────────────────────────────┘
          │
          └── Release checkpoint lock
```

---

## 20. sqlite3PagerWalWriteLock / sqlite3PagerWalDb

Helper functions for WAL write lock management.

```
  sqlite3PagerWalWriteLock(pPager, lock=1)
          │
          └── sqlite3WalWriteLock(pWal, 1)
                sqlite3OsShmLock(EXCLUSIVE, WAL_WRITE_LOCK)
                ┌───────────────────────────────┐
                │ Acquire exclusive write lock   │
                │ Only 1 writer at a time        │
                └───────────────────────────────┘

  sqlite3PagerWalWriteLock(pPager, lock=0)
          │
          └── sqlite3WalWriteLock(pWal, 0)
                sqlite3OsShmLock(UNLOCK, WAL_WRITE_LOCK)

  sqlite3PagerWalDb(pPager, db)
          │
          └── sqlite3WalDb(pWal, db)
                Set database connection for WAL
                (used for busy-handler callbacks)
```

---

## 21. sqlite3PagerSetFlags

Sets pager behavior flags (sync mode, fullfsync, etc.).
Called by btree.c after setting synchronous pragma.

```
  sqlite3BtreeSetSafetyLevel(pBt, level, fullSync, ckptFullSync)
          │
          ▼
  sqlite3PagerSetFlags(pPager, flags)
          │
          ├── Decode safety level:
          │     ┌──────────────────────────────────────┐
          │     │ PAGER_SYNCHRONOUS_OFF   (0x01)       │
          │     │   No sync at all (fast, unsafe)       │
          │     │                                      │
          │     │ PAGER_SYNCHRONOUS_NORMAL (0x02)       │
          │     │   Sync at critical moments            │
          │     │                                      │
          │     │ PAGER_SYNCHRONOUS_FULL  (0x03)  ◄──  │
          │     │   Sync on every commit (default)      │
          │     │                                      │
          │     │ PAGER_SYNCHRONOUS_EXTRA (0x04)        │
          │     │   Extra sync for belt-and-suspenders  │
          │     └──────────────────────────────────────┘
          │
          ├── Set sync flags for OsSync calls:
          │     SQLITE_SYNC_NORMAL  or  SQLITE_SYNC_FULL
          │
          └── Set fullFsync / ckptFullFsync options
```

---

## 22. sqlite3PagerMaxPageCount

Gets or sets the maximum page count for the database.

```
  sqlite3BtreeMaxPageCount(pBt, mxPage)
          │
          ▼
  sqlite3PagerMaxPageCount(pPager, mxPage)
          │
          ├── if mxPage > 0:
          │     pPager->mxPgno = mxPage
          │     (limit DB growth)
          │
          └── return pPager->mxPgno
              Default: 0xFFFFFFFE (4,294,967,294)

              Max DB size = mxPgno × pageSize
              = 4,294,967,294 × 4096
              = ~16 TiB (default page size)
```

---

## 23. sqlite3PagerPagecount

Returns the current number of pages in the database file.

```
  sqlite3BtreeBeginTrans() / btreeGetPage()
          │
          ▼
  sqlite3PagerPagecount(pPager, &nPage)
          │
          ├── [WAL mode]
          │     nPage = max(nPage from file, nPage from WAL)
          │     (WAL may have pages beyond file end)
          │
          ├── [Rollback mode]
          │     sqlite3OsFileSize(fd, &fileSize)
          │     nPage = fileSize / pageSize
          │
          └── return nPage
```

---

## 24. sqlite3PagerTruncateImage

Sets the expected database size after commit. Used by btree.c during
free-page operations and auto-vacuum.

```
  btree.c: freeing trailing pages
          │
          ▼
  sqlite3PagerTruncateImage(pPager, nPage)
          │
          └── pPager->dbSize = nPage
              (actual truncation happens during commit)
              ┌──────────────────────────────────┐
              │ Before: DB has 100 pages          │
              │ sqlite3PagerTruncateImage(90)     │
              │ After commit: DB truncated to 90  │
              │  Pages 91-100 freed               │
              └──────────────────────────────────┘
```

---

## 25. sqlite3PagerSetBusyHandler

Installs a busy-handler callback. When the pager cannot acquire a lock,
it calls this function to wait/retry.

```
  sqlite3BtreeOpen()
          │
          ▼
  sqlite3PagerSetBusyHandler(pPager, btreeInvokeBusyHandler, pBt)
          │
          └── pPager->xBusyHandler = btreeInvokeBusyHandler
              pPager->pBusyHandlerArg = pBt

              When lock fails:
              ┌─────────────────────────────────┐
              │ sqlite3OsLock() → SQLITE_BUSY   │
              │         │                        │
              │         ▼                        │
              │ xBusyHandler(pArg)               │
              │   └── return 1: retry lock       │
              │       return 0: give up          │
              └─────────────────────────────────┘
```

---

## 26. sqlite3PagerSetMmapLimit / sqlite3PagerSetSpillsize

Configuration helpers.

```
  sqlite3PagerSetMmapLimit(pPager, szMmap)
          │
          └── pPager->szMmap = szMmap
              Memory-mapped I/O limit
              (0 = disable mmap, use read/write I/O)
              ┌────────────────────────────────────┐
              │ With mmap: OS maps file into memory │
              │   sqlite3OsFetch() → pointer        │
              │   No read() syscall needed          │
              │                                    │
              │ Without mmap: traditional I/O      │
              │   sqlite3OsRead() → copy into buf   │
              └────────────────────────────────────┘

  sqlite3PagerSetSpillsize(pPager, nSpill)
          │
          └── pPager->nSpill = nSpill
              Min dirty pages before spilling to disk
              (spill = write dirty page to DB early
               to free cache memory)
```

---

## 27. sqlite3PagerDataVersion

Returns a version counter that increments when the database changes.
Used by btree.c to detect if the schema needs reloading.

```
  sqlite3BtreeBeginTrans()
          │
          ▼
  sqlite3PagerDataVersion(pPager)
          │
          ├── [WAL mode]
          │     Return WAL data version
          │     (changes when any writer commits)
          │
          ├── [Rollback mode]
          │     Return file change counter from page 1
          │
          └── btree.c compares with cached version
              if different → schema may have changed
```

---

## 28. sqlite3PagerDirectReadOk

Checks if it's safe to read directly from the database file
(bypassing the page cache). Used for memory-mapped I/O optimization.

```
  btree.c: reading overflow page
          │
          ▼
  sqlite3PagerDirectReadOk(pPager, pgno)
          │
          ├── Conditions for direct read:
          │     1. mmap is enabled
          │     2. Page is within mmap'd region
          │     3. Page is not in WAL (or WAL-index says DB copy is current)
          │
          ├── true  → read directly from mmap'd memory
          │            (zero-copy, fastest path)
          │
          └── false → use sqlite3PagerGet()
                       (read through page cache)
```

---

## 29. Accessor Functions

Simple property getters used by btree.c.

```
  sqlite3PagerGetData(pDbPage)
          └── return pDbPage->pData
              (pointer to raw page bytes)

  sqlite3PagerGetExtra(pDbPage)
          └── return pDbPage->pExtra
              (pointer to MemPage struct)

  sqlite3PagerPagenumber(pDbPage)
          └── return pDbPage->pgno

  sqlite3PagerPageRefcount(pDbPage)
          └── return pDbPage->nRef

  sqlite3PagerRefcount(pPager)
          └── return total ref count across all pages

  sqlite3PagerIsreadonly(pPager)
          └── return pPager->readOnly

  sqlite3PagerFile(pPager)
          └── return pPager->fd  (database file handle)

  sqlite3PagerJrnlFile(pPager)
          └── return pPager->jfd  (journal file handle)

  sqlite3PagerVfs(pPager)
          └── return pPager->pVfs

  sqlite3PagerFilename(pPager)
          └── return pPager->zFilename

  sqlite3PagerJournalname(pPager)
          └── return pPager->zJournal

  sqlite3PagerTempSpace(pPager)
          └── return pPager->pTmpSpace
              (one page of scratch memory)

  sqlite3PagerIswriteable(pDbPage)
          └── return (pDbPage->flags & PGHDR_DIRTY) != 0

  sqlite3PagerClearCache(pPager)
          └── sqlite3PcacheClear(pPCache)
              Evict all pages from cache
```

---

## 30. sqlite3PagerRekey

Changes the page number of a cached page. Used during B-tree balance
to rearrange pages without disk I/O.

```
  balance_nonroot() in btree.c
          │
          ▼
  sqlite3PagerRekey(pDbPage, newPgno, flags)
          │
          ├── Update cache hash table:
          │     ┌─────────────────────────────────┐
          │     │ hash[oldPgno] → remove           │
          │     │ hash[newPgno] → pDbPage           │
          │     │                                  │
          │     │ pDbPage->pgno = newPgno           │
          │     │ pDbPage->flags = flags            │
          │     └─────────────────────────────────┘
          │
          └── No disk I/O (cache-only operation)
```

---

## Quick Reference: kvstore → btree → pager mapping

| kvstore function       | btree function          | Pager operations                              |
|------------------------|-------------------------|-----------------------------------------------|
| `keyvaluestore_open`         | `BtreeOpen`             | `PagerOpen`, `ReadFileheader`, `SetPagesize`  |
| `keyvaluestore_close`        | `BtreeClose`            | `PagerClose`, `PagerRollback` (if needed)     |
| `keyvaluestore_begin`        | `BtreeBeginTrans`       | `SharedLock`, `Begin`, `OpenSavepoint`        |
| `keyvaluestore_put`          | `BtreeInsert`           | `Get`, `Write` (multiple pages)               |
| `keyvaluestore_get`          | `BtreePayload`          | `Get`, `GetData`                              |
| `keyvaluestore_delete`       | `BtreeDelete`           | `Get`, `Write` (rebalance pages)              |
| `keyvaluestore_commit`       | `BtreeCommit`           | `CommitPhaseOne`, `CommitPhaseTwo`            |
| `keyvaluestore_rollback`     | `BtreeRollback`         | `PagerRollback`                               |
| `keyvaluestore_iterator_*`   | `BtreeFirst/Next`       | `Get`, `GetData` (sequential page access)     |
| `keyvaluestore_sync`         | `BtreeCheckpoint`       | `PagerCheckpoint`                             |
| `keyvaluestore_integrity_*`  | `BtreeIntegrityCheck`   | `Get` (reads every page)                      |
