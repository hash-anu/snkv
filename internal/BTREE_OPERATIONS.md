# SNKV B-tree Operations Diagram Reference

Every B-tree operation that `kvstore.c` invokes, with diagrams showing what happens inside the tree.

---

## Table of Contents

- [1. BtreeOpen](#1-btreeopen)
- [2. BtreeClose](#2-btreeclose)
- [3. BtreeBeginTrans](#3-btreebegintrans)
- [4. BtreeCommit](#4-btreecommit)
- [5. BtreeRollback](#5-btreerollback)
- [6. BtreeCreateTable](#6-btreecreatetable)
- [7. BtreeDropTable](#7-btreedroptable)
- [8. BtreeCursor](#8-btreecursor)
- [9. BtreeIndexMoveto](#9-btreeindexmoveto)
- [10. BtreeTableMoveto](#10-bttreetablemoveto)
- [11. BtreeInsert](#11-btreeinsert)
- [12. BtreeDelete](#12-btreedelete)
- [13. BtreeFirst](#13-btreefirst)
- [14. BtreeNext](#14-btreenext)
- [15. BtreePayload / BtreePayloadSize](#15-btreepayload--btreepayloadsize)
- [16. BtreeGetMeta / BtreeUpdateMeta](#16-btreegetmeta--btreeupdatemeta)
- [17. BtreeSetCacheSize](#17-btreesetcachesize)
- [18. BtreeSetVersion](#18-btreesetversion)
- [19. BtreeIntegrityCheck](#19-btreeintegritycheck)
- [20. BtreeSetAutoVacuum](#20-btreesetautovacuum)
- [21. BtreeIncrVacuum](#21-btreeincrvacuum)

---

## 1. BtreeOpen

**Used by:** `kvstore_open()`

Opens or creates a database file and initializes the B-tree subsystem.

```
  sqlite3BtreeOpen(vfs, "mydata.db", db, &pBt, flags, vfsFlags)

  ┌──────────────────────────────────────────────────────┐
  │                    BtreeOpen                         │
  │                                                      │
  │  1. Allocate Btree + BtShared structs                │
  │  2. Open the Pager (file handle, page cache)         │
  │  3. Read page 1 header (100-byte DB header)          │
  │  4. Determine page size, encoding, version           │
  │  5. Initialize page cache with configured size       │
  └──────────────────────────────────────────────────────┘

  Disk state after open:

  mydata.db (new file):
  ┌──────────────────────────────────────┐
  │ Page 1 (4096 bytes)                  │
  │ ┌──────────────────────────────────┐ │
  │ │ 100-byte DB header               │ │
  │ │  magic: "SQLite format 3\000"    │ │
  │ │  page_size: 4096                 │ │
  │ │  file_format: 4                  │ │
  │ │  reserved_space: 0               │ │
  │ │  meta[1..15]: 0 (user metadata)  │ │
  │ └──────────────────────────────────┘ │
  │ (rest of page 1: empty leaf)         │
  └──────────────────────────────────────┘

  Memory:
  ┌─────────┐     ┌──────────┐     ┌─────────┐
  │  Btree  │────▶│ BtShared │────▶│  Pager  │───▶ file descriptor
  │ (conn)  │     │ (shared) │     │ (cache) │
  └─────────┘     └──────────┘     └─────────┘
```

---

## 2. BtreeClose

**Used by:** `kvstore_close()`

Closes the B-tree, flushes dirty pages, releases file locks.

```
  sqlite3BtreeClose(pBt)

  ┌──────────────────────────────────────────────────────┐
  │                    BtreeClose                        │
  │                                                      │
  │  1. Close all open cursors                           │
  │  2. Rollback any active transaction                  │
  │  3. In WAL mode: run checkpoint (WAL → DB)           │
  │  4. Close Pager (flush cache, close file)            │
  │  5. Free BtShared if last connection                 │
  │  6. Free Btree struct                                │
  └──────────────────────────────────────────────────────┘

  Before close:
  ┌─────────┐     ┌──────────┐     ┌─────────┐
  │  Btree  │────▶│ BtShared │────▶│  Pager  │───▶ fd
  └─────────┘     │ cursors[]│     │ cache[] │
                  └──────────┘     └─────────┘

  After close:
  (all freed, file descriptors closed)
```

---

## 3. BtreeBeginTrans

**Used by:** `kvstore_begin()`, auto-transactions in put/get/delete

Acquires a read or write lock on the database.

```
  sqlite3BtreeBeginTrans(pBt, wrflag=1, 0)

  READ TRANSACTION (wrflag=0):
  ┌──────────────────────────────────────────────────────┐
  │  1. Acquire SHARED lock on database                  │
  │  2. In WAL mode: snapshot the WAL-index              │
  │     (reader sees consistent point-in-time view)      │
  │  3. Pager state: READER                              │
  └──────────────────────────────────────────────────────┘

  WAL readers:
  ┌────────────┐  ┌────────────┐  ┌────────────┐
  │ Reader 1   │  │ Reader 2   │  │ Reader 3   │
  │ snapshot@5 │  │ snapshot@7 │  │ snapshot@7 │
  └────────────┘  └────────────┘  └────────────┘
        │               │               │
        ▼               ▼               ▼
  ┌─────────────────────────────────────────┐
  │  WAL file: frame 1, 2, 3, 4, 5, 6, 7  │
  └─────────────────────────────────────────┘
  Each reader sees up to their snapshot frame.


  WRITE TRANSACTION (wrflag=1):
  ┌──────────────────────────────────────────────────────┐
  │  1. Acquire SHARED lock (if not held)                │
  │  2. Acquire RESERVED lock (only one writer)          │
  │  3. In WAL mode: ready to append frames              │
  │  4. In DELETE mode: open rollback journal             │
  │  5. Pager state: WRITER_LOCKED                       │
  └──────────────────────────────────────────────────────┘

  Lock progression:
  ┌─────────┐    ┌──────────┐    ┌──────────┐
  │ NONE    │───▶│ SHARED   │───▶│ RESERVED │
  │         │    │ (read ok)│    │(write ok)│
  └─────────┘    └──────────┘    └──────────┘
                  ▲ many readers   ▲ one writer
```

---

## 4. BtreeCommit

**Used by:** `kvstore_commit()`

Makes all changes in the current write transaction durable.

```
  sqlite3BtreeCommit(pBt)

  WAL MODE:
  ┌──────────────────────────────────────────────────────┐
  │  Phase 1 (CommitPhaseOne):                           │
  │  1. Collect all dirty pages from page cache          │
  │  2. Write each dirty page as a WAL frame             │
  │  3. Mark last frame as "commit frame"                │
  │  4. fsync(WAL file)  ← data is now durable          │
  │                                                      │
  │  Phase 2 (CommitPhaseTwo):                           │
  │  5. Clear dirty flags in page cache                  │
  │  6. Release RESERVED lock → SHARED                   │
  │  7. Pager state: READER                              │
  └──────────────────────────────────────────────────────┘

  Page cache                    WAL file
  ┌────────┐                    ┌─────────────────┐
  │ Page 1 │──DIRTY──write────▶│ Frame N: Page 1  │
  │ Page 5 │──DIRTY──write────▶│ Frame N+1: Page 5│
  │ Page 8 │──DIRTY──write────▶│ Frame N+2: Page 8│ ← commit=true
  └────────┘                    └─────────────────┘
       │                               │
   mark clean                      fsync()
                                  (durable)


  DELETE JOURNAL MODE:
  ┌──────────────────────────────────────────────────────┐
  │  Phase 1:                                            │
  │  1. fsync(journal file)  ← old data preserved       │
  │  2. Write dirty pages to main DB file                │
  │  3. fsync(DB file)       ← new data durable         │
  │                                                      │
  │  Phase 2:                                            │
  │  4. Delete journal file  ← commit point              │
  │  5. Release locks                                    │
  └──────────────────────────────────────────────────────┘
```

---

## 5. BtreeRollback

**Used by:** `kvstore_rollback()`, error recovery paths

Discards all changes from the current write transaction.

```
  sqlite3BtreeRollback(pBt, SQLITE_OK, 0)

  WAL MODE:
  ┌──────────────────────────────────────────────────────┐
  │  1. Discard all dirty pages in cache                 │
  │  2. WAL frames NOT written (no commit frame)         │
  │  3. Release RESERVED lock                            │
  │  4. Reload pages from WAL/DB on next access          │
  └──────────────────────────────────────────────────────┘

  Page cache before rollback:     After rollback:
  ┌────────────────────┐          ┌────────────────────┐
  │ Page 1: DIRTY (new)│          │ Page 1: (evicted)  │
  │ Page 5: DIRTY (new)│   ───▶   │ Page 5: (evicted)  │
  │ Page 3: clean      │          │ Page 3: clean      │
  └────────────────────┘          └────────────────────┘
  Dirty pages discarded.           Next read reloads from
  WAL unchanged.                   WAL/DB (old data).


  DELETE JOURNAL MODE:
  ┌──────────────────────────────────────────────────────┐
  │  1. Read original pages from journal file            │
  │  2. Write them BACK to main DB file                  │
  │  3. fsync(DB file)                                   │
  │  4. Delete journal file                              │
  │  5. Release locks                                    │
  └──────────────────────────────────────────────────────┘
```

---

## 6. BtreeCreateTable

**Used by:** `kvstore_open()` (create default CF), `kvstore_cf_create()`

Allocates a new B-tree root page for a new table (column family).

```
  sqlite3BtreeCreateTable(pBt, &pgno, BTREE_BLOBKEY)

  ┌──────────────────────────────────────────────────────┐
  │  1. Allocate a new page from the freelist            │
  │     (or extend the file if freelist empty)           │
  │  2. Initialize as empty leaf page                    │
  │  3. Set page type: BLOBKEY (index-style, raw bytes)  │
  │  4. Return the page number in *pgno                  │
  └──────────────────────────────────────────────────────┘

  Before:                         After:
  ┌─────────┐                     ┌─────────┐
  │ Page 1  │ (DB header + meta)  │ Page 1  │
  │ Page 2  │ (default CF root)   │ Page 2  │
  └─────────┘                     │ Page 3  │ ← NEW (empty leaf)
                                  └─────────┘
                                  pgno = 3

  New page layout:
  ┌──────────────────────────────────────────┐
  │ Page 3 (New BLOBKEY leaf)                │
  │ Header: flags=0x0A (idx leaf) nCells=0   │
  │                                          │
  │          (entirely free space)            │
  │                                          │
  └──────────────────────────────────────────┘
```

---

## 7. BtreeDropTable

**Used by:** `kvstore_cf_drop()`

Deletes a B-tree and all its pages, returning them to the freelist.

```
  sqlite3BtreeDropTable(pBt, iTable, &iMoved)

  Before:
  ┌─────────────────────────┐
  │     Root (Page 5)       │   ← iTable=5
  │    ┌─────┬─────┐        │
  │    ▼     ▼     ▼        │
  │  Pg 10  Pg 11  Pg 12   │  (leaf pages)
  └─────────────────────────┘

  ┌──────────────────────────────────────────────────────┐
  │  1. Walk entire B-tree starting from root page       │
  │  2. Free every leaf page → add to freelist           │
  │  3. Free every interior page → add to freelist       │
  │  4. Free the root page itself                        │
  │  5. Return freed page count                          │
  └──────────────────────────────────────────────────────┘

  After:
  Freelist: [Pg 5] → [Pg 10] → [Pg 11] → [Pg 12]
  (pages reusable by future BtreeCreateTable)
```

---

## 8. BtreeCursor

**Used by:** Every read/write operation (get, put, delete, iterate)

Opens a cursor positioned on a specific B-tree. The cursor navigates through pages.

```
  sqlite3BtreeCursor(pBt, iTable, wrFlag, pKeyInfo, pCur)

  ┌──────────────────────────────────────────────────────┐
  │  1. Allocate cursor struct (pre-allocated by SNKV)   │
  │  2. Attach to B-tree with root page = iTable         │
  │  3. Set wrFlag (0=read, 1=read-write)                │
  │  4. Set KeyInfo (comparison function for BLOBKEY)     │
  │  5. Cursor is INVALID (not positioned yet)           │
  └──────────────────────────────────────────────────────┘

  Cursor structure:
  ┌──────────────────────────────────┐
  │ BtCursor                        │
  │  pBt ──────▶ BtShared           │
  │  pgnoRoot = iTable              │
  │  wrFlag = 1                     │
  │  pKeyInfo ──▶ comparison func   │
  │  state = CURSOR_INVALID         │
  │  aPage[0..N] = page stack       │
  │  ix = cell index on current pg  │
  └──────────────────────────────────┘

  Page stack (after positioning):
  ┌──────────┐
  │ aPage[0] │──▶ Root (interior)     ix=2
  │ aPage[1] │──▶ Child (interior)    ix=0
  │ aPage[2] │──▶ Leaf page           ix=7  ← current cell
  └──────────┘
```

---

## 9. BtreeIndexMoveto

**Used by:** `kvstore_cf_put()`, `kvstore_cf_get()`, `kvstore_cf_delete()`, `kvstore_cf_exists()`, prefix iterators

Positions cursor on or near a key in a BLOBKEY tree using the custom comparator.

```
  sqlite3BtreeIndexMoveto(pCur, pIdxKey, &res)

  pIdxKey contains: [key_len=4B] [key_bytes]

  ┌──────────────────────────────────────────────────────┐
  │  1. Start at root page                               │
  │  2. Binary search cell pointers using comparator:    │
  │     sqlite3VdbeRecordCompare() compares BLOBKEY      │
  │     cells by extracting 4-byte key_len header        │
  │     and comparing only the key portion               │
  │  3. If interior page: follow child pointer, repeat   │
  │  4. If leaf page: stop at cell position              │
  │  5. res = 0 (exact match), -1 (cursor before),      │
  │          +1 (cursor after)                           │
  └──────────────────────────────────────────────────────┘

  Example: IndexMoveto(cursor, "dog")

  Root (interior):
  ┌───────────────────────────────────┐
  │  [ptr→2] "cat" [ptr→3] "fox" [ptr→4] │
  │           ▲                       │
  │     "dog" > "cat" → go right      │
  │     "dog" < "fox" → child ptr→3   │
  └───────────────────────────────────┘
                │
                ▼
  Page 3 (leaf):
  ┌──────────────────────────────────────────┐
  │  "cow"  "deer"  "dog"  "elk"             │
  │                  ▲                        │
  │          binary search finds "dog"        │
  │          res = 0 (exact match)            │
  └──────────────────────────────────────────┘
```

---

## 10. BtreeTableMoveto

**Used by:** CF metadata lookups (mapping CF names to root page numbers)

Positions cursor on a row in an INTKEY table by integer rowid. Used internally for the column family metadata table.

```
  sqlite3BtreeTableMoveto(pCur, rowid, 0, &res)

  ┌──────────────────────────────────────────────────────┐
  │  1. Start at root page of INTKEY table               │
  │  2. Interior nodes store child pointers + rowids     │
  │  3. Compare target rowid vs stored rowids             │
  │  4. Follow child pointer for correct range            │
  │  5. Leaf: find cell with matching rowid               │
  └──────────────────────────────────────────────────────┘

  INTKEY interior:
  ┌──────────────────────────────────┐
  │  [ptr→2] rowid=50 [ptr→3]       │
  │     ▲                            │
  │  target=30 < 50 → go left (pg2) │
  └──────────────────────────────────┘
           │
           ▼
  Page 2 (leaf):
  ┌────────────────────────────────────┐
  │ rowid=10: "users" payload          │
  │ rowid=20: "sessions" payload       │
  │ rowid=30: "cache" payload   ← HIT  │
  └────────────────────────────────────┘
```

---

## 11. BtreeInsert

**Used by:** `kvstore_cf_put()`, `kvstore_cf_create()` (metadata insert)

Inserts or replaces a cell at the cursor position. May trigger page splits.

```
  sqlite3BtreeInsert(pCur, &payload, 0, 0)

  CASE 1: Page has room — simple insert
  ┌──────────────────────────────────────────────────────┐
  │  1. Cursor is positioned (from IndexMoveto)          │
  │  2. Build cell: [key_len 4B] [key] [value]           │
  │  3. If key exists (res==0): overwrite cell in place  │
  │  4. If new key: insert cell at cursor position       │
  │  5. Shift cell pointers to maintain sorted order     │
  │  6. Mark page dirty                                  │
  └──────────────────────────────────────────────────────┘

  Before insert "cat"="meow":        After:
  ┌────────────────────────┐          ┌─────────────────────────┐
  │ nCells=2               │          │ nCells=3                │
  │ [p1][p2]               │          │ [p1][p2][p3]            │
  │                        │          │                         │
  │ "bat"="..." "dog"="..."│          │ "bat" "cat" "dog"       │
  └────────────────────────┘          └─────────────────────────┘
                                       new cell inserted between
                                       "bat" and "dog" (sorted)


  CASE 2: Page full — triggers balance (split)
  ┌──────────────────────────────────────────────────────┐
  │  1. insertCell() detects overflow (page full)        │
  │  2. balance() called on the page                     │
  │  3. Allocate sibling page(s)                         │
  │  4. Redistribute cells across pages                  │
  │  5. Promote middle key to parent interior node       │
  │  6. If parent overflows: balance parent (recursive)  │
  │  7. If root splits: new root created (depth +1)      │
  └──────────────────────────────────────────────────────┘

  Before split (leaf full):
  ┌─────────────────────────────────────────┐
  │ Root (Leaf) — FULL                      │
  │ [a][b][c][d][e][f][g][h][i][j]...[z]   │
  └─────────────────────────────────────────┘

  After split:
  ┌──────────────────────────┐
  │ Root (Interior) — NEW    │
  │ [ptr→2]  "m"  [ptr→3]   │
  └──────┬───────────┬───────┘
         │           │
         ▼           ▼
  ┌──────────┐ ┌──────────┐
  │ Page 2   │ │ Page 3   │
  │ [a]..[l] │ │ [m]..[z] │
  │ (leaf)   │ │ (leaf)   │
  └──────────┘ └──────────┘


  CASE 3: Update existing key (overwrite)
  ┌──────────────────────────────────────────────────────┐
  │  1. IndexMoveto found exact match (res==0)           │
  │  2. Delete old cell                                  │
  │  3. Insert new cell with same key, new value         │
  │  4. If new value is larger and causes overflow:      │
  │     balance as in Case 2                             │
  └──────────────────────────────────────────────────────┘

  Before update "cat"="meow":        After "cat"="purrrr":
  ┌─────────────────────────┐         ┌─────────────────────────┐
  │ "bat" "cat"="meow" "dog"│         │ "bat" "cat"="purrrr" "dog"│
  └─────────────────────────┘         └─────────────────────────┘
```

---

## 12. BtreeDelete

**Used by:** `kvstore_cf_delete()`, `kvstore_cf_drop()` (metadata delete)

Deletes the cell at the current cursor position.

```
  sqlite3BtreeDelete(pCur, 0)

  CASE 1: Delete from leaf (common case)
  ┌──────────────────────────────────────────────────────┐
  │  1. Cursor points to cell on leaf page               │
  │  2. dropCell(): remove cell pointer + cell data      │
  │  3. Add freed space to page free block list          │
  │  4. Defragment page if fragmentation too high        │
  │  5. Mark page dirty                                  │
  │  6. If page below minimum fill: balance (merge)      │
  └──────────────────────────────────────────────────────┘

  Before delete "cat":                After:
  ┌──────────────────────────┐        ┌──────────────────────────┐
  │ nCells=4                 │        │ nCells=3                 │
  │ [p1][p2][p3][p4]        │        │ [p1][p2][p3]             │
  │                          │        │                          │
  │ "bat" "cat" "dog" "elk"  │        │ "bat" "dog" "elk"        │
  └──────────────────────────┘        └──────────────────────────┘
         ▲ removed                    Cell pointers re-packed.
                                      Free space reclaimed.


  CASE 2: Page underflow — triggers merge/rebalance
  ┌──────────────────────────────────────────────────────┐
  │  1. After delete, leaf page is below min fill        │
  │  2. balance() tries to redistribute with siblings    │
  │  3. If sibling also low: merge two leaves into one   │
  │  4. Remove separator key from parent                 │
  │  5. Free empty page → add to freelist                │
  │  6. If parent underflows: balance parent (recursive) │
  └──────────────────────────────────────────────────────┘

  Before merge:
  ┌──────────────────────────┐
  │ Parent (Interior)        │
  │ [ptr→2]  "d"  [ptr→3]   │
  └──────┬───────────┬───────┘
         │           │
         ▼           ▼
  ┌──────────┐ ┌──────────┐
  │ Page 2   │ │ Page 3   │
  │ [a][b]   │ │ [e]      │  ← both underfull
  └──────────┘ └──────────┘

  After merge:
  ┌──────────────────────────┐
  │ Root (becomes Leaf)      │
  │ [a][b][d][e]             │
  └──────────────────────────┘
  Pages 2, 3 freed. Depth reduced.
```

---

## 13. BtreeFirst

**Used by:** `kvstore_iterator_first()`, CF list scanning, integrity check

Positions cursor at the very first (smallest) cell in the B-tree.

```
  sqlite3BtreeFirst(pCur, &res)

  ┌──────────────────────────────────────────────────────┐
  │  1. Start at root page                               │
  │  2. Follow leftmost child pointer at each level      │
  │  3. Stop at leftmost cell of leftmost leaf           │
  │  4. res=0 if tree has entries, res=1 if empty        │
  └──────────────────────────────────────────────────────┘

  Navigation path:
  ┌──────────────────────────┐
  │ Root (Interior)          │
  │ [ptr→2] "m" [ptr→3]     │
  │  ▲ leftmost              │
  └──┼───────────────────────┘
     │
     ▼
  ┌──────────────────────────┐
  │ Page 2 (Interior)        │
  │ [ptr→4] "d" [ptr→5]     │
  │  ▲ leftmost              │
  └──┼───────────────────────┘
     │
     ▼
  ┌──────────────────────────┐
  │ Page 4 (Leaf)            │
  │ "aaa" "abc" "ace" "ant"  │
  │  ▲                        │
  │  cursor lands here (ix=0) │
  └──────────────────────────┘
```

---

## 14. BtreeNext

**Used by:** `kvstore_iterator_next()`, scanning loops

Advances cursor to the next cell in sorted order.

```
  sqlite3BtreeNext(pCur, 0)

  CASE 1: Next cell on same leaf page
  ┌──────────────────────────────────────────────────────┐
  │  1. Increment cell index: ix++                       │
  │  2. If ix < nCells: done (same page)                 │
  └──────────────────────────────────────────────────────┘

  Page (Leaf):
  "ant" "bat" "cat" "dog"
         ▲           ▲
     ix=1 (before)  ix=2 (after BtreeNext)


  CASE 2: End of leaf — move to parent then right subtree
  ┌──────────────────────────────────────────────────────┐
  │  1. ix reached end of current leaf                   │
  │  2. Pop up to parent page                            │
  │  3. Move to next cell in parent                      │
  │  4. If parent cell exists: descend to leftmost       │
  │     leaf of right subtree                            │
  │  5. If parent also exhausted: pop up again           │
  │     (repeat until found or tree exhausted → EOF)     │
  └──────────────────────────────────────────────────────┘

  ┌──────────────────────────┐
  │ Root: [ptr→2] "m" [ptr→3]│
  └──────┬──────────────┬────┘
         │              │
         ▼              ▼
  ┌────────────┐  ┌────────────┐
  │ Pg 2 (Leaf)│  │ Pg 3 (Leaf)│
  │ "a" "f" "k"│  │ "m" "r" "z"│
  └────────────┘  └────────────┘

  Cursor at Pg2, ix=2 ("k"):
    BtreeNext() →
    1. ix=3, but nCells=3 → end of page
    2. Pop to root, move to separator "m"
    3. Descend right child → Pg3
    4. Land on ix=0 → "m"
```

---

## 15. BtreePayload / BtreePayloadSize

**Used by:** Every get, iterator key/value read, CF metadata parsing

Reads cell content from the current cursor position.

```
  sz = sqlite3BtreePayloadSize(pCur)     // total cell bytes
  sqlite3BtreePayload(pCur, offset, amt, pBuf)  // read slice

  ┌──────────────────────────────────────────────────────┐
  │  1. PayloadSize: return total payload bytes of cell  │
  │  2. Payload: copy [amt] bytes starting at [offset]   │
  │     from the cell into pBuf                          │
  │  3. If cell fits on page: direct memcpy              │
  │  4. If cell overflows: follow overflow page chain    │
  └──────────────────────────────────────────────────────┘

  Cell layout on page (small cell, fits entirely):
  ┌──────────────────────────────────────────────────────┐
  │ [varint: payload_size] [cell data .................. ]│
  └──────────────────────────────────────────────────────┘

  SNKV reads it in two steps:
  ┌───────────────────────────────────────────┐
  │ Cell: [00000005] [h e l l o] [w o r l d]  │
  │       ◄──4B──►   ◄──5B key─► ◄──5B val──►│
  │                                            │
  │ PayloadSize() → 14                         │
  │ Payload(0, 4, hdr)  → read key_len = 5     │
  │ Payload(4, 5, key)  → read "hello"         │
  │ Payload(9, 5, val)  → read "world"         │
  └───────────────────────────────────────────┘


  Overflow cell (large value spanning multiple pages):
  ┌──────────┐     ┌──────────┐     ┌──────────┐
  │ Leaf Page│     │Overflow 1│     │Overflow 2│
  │ [cell    │     │          │     │          │
  │  header] │     │ ...data..│     │ ...data..│
  │ [partial │────▶│          │────▶│          │
  │  data]   │     │ next→────│─────│          │
  └──────────┘     └──────────┘     └──────────┘
  Payload() transparently follows the chain.
```

---

## 16. BtreeGetMeta / BtreeUpdateMeta

**Used by:** `kvstore_open()` (read CF roots), `kvstore_cf_create/drop()` (update CF count)

Reads or writes metadata integers stored in the database header (page 1, bytes 36-95).

```
  sqlite3BtreeGetMeta(pBt, index, &value)
  sqlite3BtreeUpdateMeta(pBt, index, value)

  ┌──────────────────────────────────────────────────────┐
  │  Page 1 header has 15 metadata slots (4 bytes each)  │
  │  SNKV uses:                                          │
  │    meta[7]  = default CF root page number            │
  │    meta[8]  = CF metadata table root page number     │
  │    meta[9]  = number of column families              │
  └──────────────────────────────────────────────────────┘

  Page 1 (bytes 36-95):
  ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐
  │  0  │  1  │  2  │  3  │  4  │  5  │  6  │  7  │  8  │  9  │
  │     │     │     │     │     │     │     │     │     │     │
  │     │     │     │     │     │     │     │  2  │  3  │  1  │
  │     │     │     │     │     │     │     │  ▲  │  ▲  │  ▲  │
  └─────┴─────┴─────┴─────┴─────┴─────┴─────┴──┼──┴──┼──┴──┼──┘
                                                │     │     │
                                          default   meta  count
                                          CF root   table  =1
                                          =page2   =page3
```

---

## 17. BtreeSetCacheSize

**Used by:** `kvstore_open()`

Configures how many pages the pager keeps in memory.

```
  sqlite3BtreeSetCacheSize(pBt, 2000)

  ┌──────────────────────────────────────────────────────┐
  │  Sets pcache capacity to 2000 pages                  │
  │  2000 × 4096 bytes = ~8 MB of page cache             │
  │                                                      │
  │  Pages evicted (LRU) when cache is full and a new    │
  │  page is needed. Dirty pages written to WAL/journal  │
  │  before eviction (pagerStress).                      │
  └──────────────────────────────────────────────────────┘

  Page cache:
  ┌─────────────────────────────────────────┐
  │ Slot 1:    Page 1   [clean]             │
  │ Slot 2:    Page 2   [dirty]             │
  │ Slot 3:    Page 5   [clean]             │
  │ ...                                     │
  │ Slot 2000: Page 847 [clean]             │
  ├─────────────────────────────────────────┤
  │ FULL → evict LRU clean page             │
  │        or stress-write dirty page first │
  └─────────────────────────────────────────┘
```

---

## 18. BtreeSetVersion

**Used by:** `kvstore_open()`

Sets the database file format version, which controls journal mode.

```
  sqlite3BtreeSetVersion(pBt, 2)   // WAL mode
  sqlite3BtreeSetVersion(pBt, 1)   // Rollback journal mode

  ┌──────────────────────────────────────────────────────┐
  │  1. Begin a write transaction                        │
  │  2. Write version number to DB header bytes 18-19    │
  │  3. On next transaction, pager detects version:      │
  │     version=2 → open WAL file automatically          │
  │     version=1 → use rollback journal                 │
  └──────────────────────────────────────────────────────┘

  DB header bytes 18-19:
  ┌────────────────────┐
  │ ...                │
  │ byte 18: read_ver  │  = 1 (journal) or 2 (WAL)
  │ byte 19: write_ver │  = 1 (journal) or 2 (WAL)
  │ ...                │
  └────────────────────┘

  version=2 triggers:
  mydata.db-wal  (WAL file created)
  mydata.db-shm  (shared memory / WAL-index created)
```

---

## 19. BtreeIntegrityCheck

**Used by:** `kvstore_integrity_check()`

Walks every page of every B-tree in the database to verify structural consistency.

```
  sqlite3BtreeIntegrityCheck(pBt, aRoot, nRoot, mxErr, &nErr, &zErr)

  ┌──────────────────────────────────────────────────────┐
  │  1. Verify page 1 header is valid                    │
  │  2. For each root page in aRoot[]:                   │
  │     a. Walk entire B-tree depth-first                │
  │     b. Verify each page header (flags, nCells)       │
  │     c. Verify cell pointers don't overlap            │
  │     d. Verify cells are in sorted order              │
  │     e. Verify child page numbers are valid           │
  │     f. Verify overflow chains are intact             │
  │     g. Verify parent-child key ordering              │
  │  3. Verify freelist pages are valid                  │
  │  4. Verify every page is accounted for               │
  │     (used by a tree OR on freelist, no orphans)      │
  │  5. Return error message if corruption found         │
  └──────────────────────────────────────────────────────┘

  Walk order (depth-first):

       Root
      / | \
     /  |  \
    A   B   C        ← interior pages
   /|  /|\  |\
  D E F G H I J      ← leaf pages

  Visit order: D, E, A, F, G, H, B, I, J, C, Root
  (verify leaves bottom-up, then interior, then root)

  Checks at each leaf:
  ┌────────────────────────────────────────┐
  │ Page N (leaf):                         │
  │ ✓ flags correct for leaf type          │
  │ ✓ nCells matches pointer count         │
  │ ✓ cells don't overlap each other       │
  │ ✓ cells are in ascending key order     │
  │ ✓ no cell extends past page boundary   │
  │ ✓ overflow pages (if any) are valid    │
  └────────────────────────────────────────┘
```

---

## 20. BtreeSetAutoVacuum

**Used by:** `kvstore_open()` (always sets incremental auto-vacuum on new databases)

Configures the auto-vacuum mode for the database. Must be called before any transaction writes data (before `BTS_PAGESIZE_FIXED` is set). For existing databases, the mode is stored in the DB header and cannot be changed.

```
  sqlite3BtreeSetAutoVacuum(pBt, BTREE_AUTOVACUUM_INCR)

  Modes:
  ┌──────────────────────────────────────────────────────┐
  │  BTREE_AUTOVACUUM_NONE (0):                          │
  │    No auto-vacuum. Deleted pages go to freelist.     │
  │    File never shrinks. Freelist reused for inserts.  │
  │                                                      │
  │  BTREE_AUTOVACUUM_FULL (1):                          │
  │    Full auto-vacuum on every commit. Slow commits.   │
  │                                                      │
  │  BTREE_AUTOVACUUM_INCR (2):  ← SNKV default         │
  │    Incremental: pages go to freelist on delete,      │
  │    but file only shrinks when BtreeIncrVacuum()      │
  │    is explicitly called. Best balance of write       │
  │    performance and space reclamation.                │
  └──────────────────────────────────────────────────────┘

  What auto-vacuum enables internally — the Pointer Map:

  Auto-vacuum needs to MOVE pages to fill gaps. But when a page
  moves, its parent's child pointer must be updated. To find the
  parent quickly, auto-vacuum databases maintain extra "pointer map"
  pages that record each page's type and parent.

  Normal database:               Auto-vacuum database:
  ┌────────┐                     ┌────────┐
  │ Page 1 │ (DB header)         │ Page 1 │ (DB header)
  │ Page 2 │ (data)              │ Page 2 │ ← POINTER MAP
  │ Page 3 │ (data)              │ Page 3 │ (data)
  │ Page 4 │ (data)              │ Page 4 │ (data)
  │ Page 5 │ (data)              │ Page 5 │ (data)
  └────────┘                     └────────┘

  Pointer map page layout (one 5-byte entry per tracked page):
  ┌─────────────────────────────────────────────┐
  │ Entry for page 3: [type=5 (BTREE)] [parent=1]  │
  │ Entry for page 4: [type=2 (FREE)]  [parent=0]  │
  │ Entry for page 5: [type=3 (OVFL1)] [parent=3]  │
  │ ...                                             │
  └─────────────────────────────────────────────┘

  Page types tracked:
    PTRMAP_ROOTPAGE  (1) — B-tree root page
    PTRMAP_FREEPAGE  (2) — unused/free page
    PTRMAP_OVERFLOW1 (3) — first overflow page of a cell
    PTRMAP_OVERFLOW2 (4) — subsequent overflow page
    PTRMAP_BTREE     (5) — non-root B-tree page (child)
```

---

## 21. BtreeIncrVacuum

**Used by:** `kvstore_incremental_vacuum()`

Performs a single step of incremental vacuum: moves one live page from the end of the file into a freelist gap, then truncates the file. Called repeatedly to reclaim all free space.

```
  sqlite3BtreeIncrVacuum(pBt)

  ┌──────────────────────────────────────────────────────┐
  │  1. Calculate final DB size:                         │
  │     nFin = nOrig - nFree - nPtrMapPages              │
  │                                                      │
  │  2. Look at the LAST page in the file (iLastPg)     │
  │                                                      │
  │  3. Consult pointer map to find page type:           │
  │     ptrmapGet(pBt, iLastPg) → eType, iPtrPage       │
  │                                                      │
  │  4a. If FREEPAGE: just remove from freelist          │
  │  4b. If live page: relocatePage() to a free slot     │
  │      - Copy content to the free page                 │
  │      - Update parent's child pointer (via ptrmap)    │
  │      - Update ptrmap for the moved page              │
  │      - Mark old location as free                     │
  │                                                      │
  │  5. Decrement page count (pBt->nPage--)              │
  │  6. Set bDoTruncate flag                             │
  │  7. On commit, pager truncates file to nPage pages   │
  │                                                      │
  │  Returns SQLITE_OK  → more pages to vacuum           │
  │  Returns SQLITE_DONE → no more free pages            │
  └──────────────────────────────────────────────────────┘


  Example: vacuum step on a 10-page database with 3 free pages

  Before (iLastPg = 10):
  ┌────┬────┬────┬────┬────┬────┬────┬────┬────┬────┐
  │ P1 │ P2 │ P3 │ P4 │ P5 │ P6 │ P7 │ P8 │ P9 │P10│
  │hdr │pmap│data│FREE│data│FREE│data│data│pmap│data│
  └────┴────┴────┴────┴────┴────┴────┴────┴────┴────┘
                   ▲         ▲                    ▲
                   free      free            last page (live)

  Step 1: Move P10 (live data) → P4 (free slot)
  ┌──────────────────────────────────────────────────────┐
  │  ptrmapGet(P10) → type=BTREE, parent=P3             │
  │  allocateBtreePage() → picks P4 (free)              │
  │  relocatePage(P10 → P4):                            │
  │    1. Copy P10 content to P4                        │
  │    2. Update P3's child ptr: P10 → P4               │
  │    3. Update ptrmap: P4 is now BTREE, parent=P3     │
  │    4. P10 becomes the new last page                 │
  │  pBt->nPage = 9                                     │
  └──────────────────────────────────────────────────────┘

  After step 1:
  ┌────┬────┬────┬────┬────┬────┬────┬────┬────┐
  │ P1 │ P2 │ P3 │ P4 │ P5 │ P6 │ P7 │ P8 │ P9│
  │hdr │pmap│data│data│data│FREE│data│data│pmap│
  └────┴────┴────┴────┴────┴────┴────┴────┴────┘
                  moved↑         ▲
                  from P10       free (1 remaining)

  Step 2: P9 is a ptrmap page (skipped), P8 is live → move to P6
  Step 3: Remaining pages compact, file truncated.

  After all steps + commit:
  ┌────┬────┬────┬────┬────┬────┬────┐
  │ P1 │ P2 │ P3 │ P4 │ P5 │ P6 │ P7│   ← file truncated
  │hdr │pmap│data│data│data│data│data│      (3 pages freed)
  └────┴────┴────┴────┴────┴────┴────┘

  kvstore_incremental_vacuum() calls BtreeIncrVacuum in a loop:
    nPage > 0: call N times (partial vacuum)
    nPage = 0: call until SQLITE_DONE (full vacuum)
```

---

## Quick Reference: kvstore.c → B-tree Mapping

| kvstore function | B-tree operations used |
|---|---|
| `kvstore_open` | Open, SetAutoVacuum, SetCacheSize, SetVersion, BeginTrans, CreateTable, GetMeta, UpdateMeta, Commit, Cursor, First, Next, Payload |
| `kvstore_close` | Close (checkpoints WAL, frees everything) |
| `kvstore_begin` | BeginTrans |
| `kvstore_commit` | Commit |
| `kvstore_rollback` | Rollback |
| `kvstore_put` | BeginTrans, Cursor, IndexMoveto, Insert, Commit |
| `kvstore_get` | BeginTrans, Cursor, IndexMoveto, PayloadSize, Payload, Commit |
| `kvstore_delete` | BeginTrans, Cursor, IndexMoveto, Delete, Commit |
| `kvstore_exists` | BeginTrans, Cursor, IndexMoveto, PayloadSize, Payload, Commit |
| `kvstore_cf_create` | BeginTrans, CreateTable, Cursor, Insert, GetMeta, UpdateMeta, Commit |
| `kvstore_cf_drop` | BeginTrans, Cursor, TableMoveto, Delete, DropTable, GetMeta, UpdateMeta, Commit |
| `kvstore_cf_list` | BeginTrans, Cursor, First, Next, PayloadSize, Payload, Commit |
| `kvstore_iterator_*` | Cursor, First, IndexMoveto, Next, PayloadSize, Payload |
| `kvstore_integrity_check` | BeginTrans, IntegrityCheck, Commit |
| `kvstore_sync` | Commit + BeginTrans (flush cycle) |
| `kvstore_incremental_vacuum` | BeginTrans, IncrVacuum (loop), Commit |
