# OS / VFS Layer Operations

All `sqlite3Os*` functions that `pager.c` and `wal.c` call. The OS layer
is the bottom of the SNKV stack — it translates abstract I/O operations
into platform-specific system calls via the VFS (Virtual File System).

---

## Architecture Context

```
  keyvaluestore.c  (API)
      │
  btree.c    (B-tree)
      │
  pager.c / wal.c  (Pager + WAL)
      │
      ▼
 ┌──────────────────────────────┐
 │  os.c + os_unix.c / os_win.c │  ◄── THIS DOCUMENT
 └──────────────────────────────┘
      │
   POSIX / Win32 syscalls
      │
    Kernel → Disk
```

---

## VFS Architecture

The VFS is a vtable (virtual method table) pattern. Two interfaces:

```
┌─────────────────────────────────────────────────────────────────┐
│ sqlite3_vfs  (VFS object — one per platform)                    │
│                                                                 │
│  Methods:                                                       │
│    xOpen()          → open a file                               │
│    xDelete()        → delete a file                             │
│    xAccess()        → check file existence/permissions           │
│    xFullPathname()  → resolve relative → absolute path          │
│    xRandomness()    → get random bytes                          │
│    xSleep()         → sleep N microseconds                      │
│    xCurrentTimeInt64() → current time                           │
│                                                                 │
│  Properties:                                                    │
│    szOsFile         → size of sqlite3_file subclass              │
│    mxPathname       → max path length (512)                     │
│    zName            → "unix" or "win32"                         │
└─────────────────────────────────────────────────────────────────┘
        │
        │  xOpen() creates ──►
        │
┌─────────────────────────────────────────────────────────────────┐
│ sqlite3_file  (file handle — one per open file)                 │
│                                                                 │
│  pMethods → sqlite3_io_methods:                                 │
│    xClose()                   xShmMap()                         │
│    xRead()                    xShmLock()                        │
│    xWrite()                   xShmBarrier()                     │
│    xTruncate()                xShmUnmap()                       │
│    xSync()                    xFetch()      (mmap)              │
│    xFileSize()                xUnfetch()    (mmap)              │
│    xLock()                                                      │
│    xUnlock()                                                    │
│    xCheckReservedLock()                                         │
│    xFileControl()                                               │
│    xSectorSize()                                                │
│    xDeviceCharacteristics()                                     │
└─────────────────────────────────────────────────────────────────┘

  Unix subclass (unixFile):
  ┌────────────────────────────────────────┐
  │ unixFile extends sqlite3_file          │
  │   h           → int fd (file desc)     │
  │   eFileLock   → current lock level     │
  │   pInode      → shared inode info      │
  │   pShm        → shared memory handle   │
  │   zPath       → file path              │
  │   pMapRegion  → mmap'd region          │
  │   mmapSize    → current mmap size      │
  └────────────────────────────────────────┘
```

---

## File Handles Managed by Pager + WAL

```
  ┌───────────────────────────────────────────────┐
  │ Pager manages 3 file handles:                  │
  │                                                │
  │   fd    → main database file  (test.db)        │
  │   jfd   → rollback journal    (test.db-journal)│
  │   sjfd  → sub-journal         (temp file)      │
  │                                                │
  │ WAL manages 2 file handles:                    │
  │                                                │
  │   pDbFd  → database file (shared with pager)   │
  │   pWalFd → WAL file          (test.db-wal)     │
  │                                                │
  │ Shared memory (WAL-index):                     │
  │                                                │
  │   test.db-shm  (via xShmMap on pDbFd)          │
  └───────────────────────────────────────────────┘

  On disk:
  ┌──────────────────────────────────────────────┐
  │  test.db          ← main database            │
  │  test.db-journal  ← rollback journal (temp)  │
  │  test.db-wal      ← WAL file                 │
  │  test.db-shm      ← WAL shared memory index  │
  └──────────────────────────────────────────────┘
```

---

## 1. sqlite3OsOpen

Opens a file and returns a file handle. Called by pager and WAL
for database, journal, and WAL files.

```
  Pager / WAL
          │
          ▼
  sqlite3OsOpen(pVfs, zPath, pFile, flags, &outFlags)
          │
          ▼  dispatches to ──►  pVfs->xOpen()
          │
          ├── [Unix: unixOpen()]
          │     │
          │     ├── Determine open() flags:
          │     │     SQLITE_OPEN_READWRITE  → O_RDWR
          │     │     SQLITE_OPEN_CREATE     → O_CREAT
          │     │     SQLITE_OPEN_EXCLUSIVE  → O_EXCL
          │     │     SQLITE_OPEN_READONLY   → O_RDONLY
          │     │
          │     ├── fd = open(zPath, openFlags, 0644)
          │     │     ┌──────────────────────────────┐
          │     │     │ Kernel                        │
          │     │     │  allocate file descriptor     │
          │     │     │  create inode entry            │
          │     │     │  return fd (integer)           │
          │     │     └──────────────────────────────┘
          │     │
          │     ├── Store fd in unixFile struct
          │     │     pFile->h = fd
          │     │
          │     ├── Set io_methods vtable:
          │     │     pFile->pMethods = &posixIoMethods
          │     │
          │     └── If journal/WAL: set DIRSYNC flag
          │           (fsync parent directory on first sync)
          │
          └── [Windows: winOpen()]
                │
                ├── hFile = CreateFileW(zPath, access, sharing, ...)
                │
                └── pFile->h = hFile

  File types opened:
  ┌──────────────────────────────────────────────┐
  │ SQLITE_OPEN_MAIN_DB       → test.db          │
  │ SQLITE_OPEN_MAIN_JOURNAL  → test.db-journal  │
  │ SQLITE_OPEN_WAL           → test.db-wal      │
  │ SQLITE_OPEN_SUBJOURNAL    → temp (sub-jrnl)  │
  │ SQLITE_OPEN_TEMP_DB       → temp database    │
  └──────────────────────────────────────────────┘
```

---

## 2. sqlite3OsClose

Closes a file handle and releases resources.

```
  Pager / WAL
          │
          ▼
  sqlite3OsClose(pFile)
          │
          ▼  pFile->pMethods->xClose()
          │
          ├── [Unix: unixClose()]
          │     │
          │     ├── Release all locks
          │     │     unixUnlock(pFile, NO_LOCK)
          │     │
          │     ├── Unmap memory (if mmap'd)
          │     │     munmap(pFile->pMapRegion, mmapSize)
          │     │
          │     ├── close(pFile->h)
          │     │     ┌──────────────────────────┐
          │     │     │ Kernel                    │
          │     │     │  release fd               │
          │     │     │  flush buffers            │
          │     │     │  decrement inode refcount │
          │     │     └──────────────────────────┘
          │     │
          │     └── Remove from inode tracking list
          │
          └── [Windows: winClose()]
                └── CloseHandle(pFile->h)
```

---

## 3. sqlite3OsRead

Reads bytes from a file at a given offset. The workhorse I/O function —
called for every page cache miss.

```
  pager.c: read page from DB  /  wal.c: read WAL frame
          │
          ▼
  sqlite3OsRead(pFile, pBuf, amt, offset)
          │
          ▼  pFile->pMethods->xRead()
          │
          ├── [Unix: unixRead()]
          │     │
          │     ├── Check mmap first (if enabled):
          │     │     if offset < mmapSize:
          │     │       memcpy(pBuf, pMapRegion + offset, amt)
          │     │       return OK  ── zero-copy from mmap!
          │     │       ┌─────────────────────────────────┐
          │     │       │ Virtual Memory (mmap'd file)     │
          │     │       │  ┌────┬────┬────┬────┬────┐     │
          │     │       │  │pg1 │pg2 │pg3 │pg4 │pg5 │     │
          │     │       │  └────┴────┴─▲──┴────┴────┘     │
          │     │       │              │                   │
          │     │       │         memcpy here              │
          │     │       └─────────────────────────────────┘
          │     │
          │     ├── Otherwise: seekAndRead()
          │     │     lseek(fd, offset, SEEK_SET)
          │     │     read(fd, pBuf, amt)
          │     │     ┌─────────────────────────────────┐
          │     │     │ Kernel                           │
          │     │     │  1. Seek to file offset          │
          │     │     │  2. Check page cache             │
          │     │     │     hit  → copy from RAM         │
          │     │     │     miss → schedule disk I/O     │
          │     │     │  3. Copy data to userspace buf   │
          │     │     └─────────────────────────────────┘
          │     │
          │     └── If short read: zero-fill remainder
          │           memset(rest, 0, amt - got)
          │           return SQLITE_IOERR_SHORT_READ
          │
          └── [Windows: winRead()]
                ├── SetFilePointer(h, offset)
                └── ReadFile(h, pBuf, amt, &got)

  Typical calls:
    Read DB page:  sqlite3OsRead(fd,  buf, 4096, (pgno-1)*4096)
    Read WAL frame: sqlite3OsRead(walFd, buf, 4096, walOffset)
    Read header:   sqlite3OsRead(fd,  buf, 100,  0)
```

---

## 4. sqlite3OsWrite

Writes bytes to a file at a given offset. Called when flushing dirty pages
to the database, writing journal entries, or appending WAL frames.

```
  pager.c: write page to DB  /  wal.c: write WAL frame
          │
          ▼
  sqlite3OsWrite(pFile, pBuf, amt, offset)
          │
          ▼  pFile->pMethods->xWrite()
          │
          ├── [Unix: unixWrite()]
          │     │
          │     ├── seekAndWrite()
          │     │     lseek(fd, offset, SEEK_SET)
          │     │     write(fd, pBuf, amt)
          │     │     ┌─────────────────────────────────┐
          │     │     │ Kernel                           │
          │     │     │  1. Seek to file offset          │
          │     │     │  2. Copy data to page cache      │
          │     │     │  3. Mark page dirty               │
          │     │     │  4. Return (data NOT on disk yet) │
          │     │     │                                   │
          │     │     │  Data in kernel page cache:       │
          │     │     │  ┌────────────────────────────┐   │
          │     │     │  │ dirty page → will flush    │   │
          │     │     │  │ when fsync() called or     │   │
          │     │     │  │ kernel decides to writeback│   │
          │     │     │  └────────────────────────────┘   │
          │     │     └─────────────────────────────────┘
          │     │
          │     ├── Handle partial writes (retry loop)
          │     │     while (written < amt):
          │     │       write(fd, buf + written, amt - written)
          │     │
          │     └── If mmap active and write overlaps:
          │           Unmap and remap after write
          │
          └── [Windows: winWrite()]
                ├── SetFilePointer(h, offset)
                └── WriteFile(h, pBuf, amt, &wrote)

  Typical calls:
    Journal entry:  sqlite3OsWrite(jfd,  origPage, 4096, jrnlOff)
    WAL frame:      sqlite3OsWrite(walFd, frame,    4096+24, walOff)
    DB page flush:  sqlite3OsWrite(fd,   newPage,  4096, (pgno-1)*4096)
```

---

## 5. sqlite3OsSync

Forces data to durable storage (fsync). The critical durability operation —
this is what makes commits survive power loss.

```
  pager.c: commit  /  wal.c: WAL frame commit
          │
          ▼
  sqlite3OsSync(pFile, flags)
          │
          ▼  pFile->pMethods->xSync()
          │
          ├── flags:
          │     SQLITE_SYNC_NORMAL  (0x02)  → fsync / fdatasync
          │     SQLITE_SYNC_FULL   (0x03)  → fullfsync (macOS F_FULLFSYNC)
          │     SQLITE_SYNC_DATAONLY (0x10) → fdatasync (skip metadata)
          │
          ├── [Unix: unixSync()]
          │     │
          │     ├── full_fsync(fd, isFullsync, isDataOnly)
          │     │     │
          │     │     ├── [macOS + FULL]: fcntl(fd, F_FULLFSYNC)
          │     │     │     Flushes disk write cache to platters
          │     │     │
          │     │     ├── [Linux + DATAONLY]: fdatasync(fd)
          │     │     │     Flushes data, skips metadata (faster)
          │     │     │
          │     │     └── [Default]: fsync(fd)
          │     │           Flushes data + metadata (inode)
          │     │
          │     │     ┌─────────────────────────────────────┐
          │     │     │ What fsync does:                     │
          │     │     │                                     │
          │     │     │  Kernel Page Cache    Disk           │
          │     │     │  ┌──────────────┐   ┌──────────┐   │
          │     │     │  │ dirty page A │──►│ sector A │   │
          │     │     │  │ dirty page B │──►│ sector B │   │
          │     │     │  │ dirty page C │──►│ sector C │   │
          │     │     │  └──────────────┘   └──────────┘   │
          │     │     │                                     │
          │     │     │  Blocks until ALL dirty pages for   │
          │     │     │  this fd are written to stable      │
          │     │     │  storage (magnetic platter / NAND)  │
          │     │     └─────────────────────────────────────┘
          │     │
          │     └── If DIRSYNC flag set (first sync):
          │           dirfd = open(parent_dir)
          │           fsync(dirfd)  ── ensure dir entry durable
          │           close(dirfd)
          │
          └── [Windows: winSync()]
                └── FlushFileBuffers(pFile->h)

  When fsync is called (SNKV default: synchronous=FULL):
  ┌─────────────────────────────────────────────────┐
  │ [Rollback mode]                                  │
  │   1. Sync journal  ← before writing DB           │
  │   2. Sync database ← after writing dirty pages   │
  │                                                  │
  │ [WAL mode]                                       │
  │   1. Sync WAL      ← after appending frames      │
  │   (database synced during checkpoint)            │
  └─────────────────────────────────────────────────┘
```

---

## 6. sqlite3OsTruncate

Truncates a file to a specified size. Used to shrink the database
after freeing pages, or to reset the WAL/journal.

```
  pager.c / wal.c
          │
          ▼
  sqlite3OsTruncate(pFile, nByte)
          │
          ▼  pFile->pMethods->xTruncate()
          │
          ├── [Unix: unixTruncate()]
          │     │
          │     ├── ftruncate(fd, nByte)
          │     │     ┌──────────────────────────────┐
          │     │     │ Before:                       │
          │     │     │  [████████████████] 40960 B   │
          │     │     │                               │
          │     │     │ ftruncate(fd, 20480)           │
          │     │     │                               │
          │     │     │ After:                        │
          │     │     │  [████████]          20480 B   │
          │     │     │           ^^^^^^^^ freed       │
          │     │     └──────────────────────────────┘
          │     │
          │     └── If mmap: adjust mapping size
          │
          └── [Windows: winTruncate()]
                ├── SetFilePointer(h, nByte)
                └── SetEndOfFile(h)

  Typical calls:
    DB shrink:    sqlite3OsTruncate(fd,    nPage * pageSize)
    WAL reset:    sqlite3OsTruncate(walFd, 0)
    Journal trim: sqlite3OsTruncate(jfd,   0)
```

---

## 7. sqlite3OsFileSize

Returns the current size of a file in bytes.

```
  pager.c / wal.c
          │
          ▼
  sqlite3OsFileSize(pFile, &size)
          │
          ▼  pFile->pMethods->xFileSize()
          │
          ├── [Unix: unixFileSize()]
          │     │
          │     ├── fstat(fd, &buf)
          │     │
          │     └── *pSize = buf.st_size
          │
          └── [Windows: winFileSize()]
                └── GetFileSizeEx(h, &size)

  Used to calculate:
    nPage = fileSize / pageSize
    ┌─────────────────────────────────────┐
    │ fileSize = 40960                     │
    │ pageSize = 4096                      │
    │ nPage    = 40960 / 4096 = 10 pages   │
    └─────────────────────────────────────┘
```

---

## 8. sqlite3OsLock

Acquires a file lock at a given level. Used in rollback journal mode
to coordinate concurrent access.

```
  pager.c: sqlite3PagerSharedLock / sqlite3PagerBegin
          │
          ▼
  sqlite3OsLock(pFile, eLock)
          │
          ▼  pFile->pMethods->xLock()
          │
          ├── [Unix: unixLock()]
          │     │
          │     ├── Lock levels and POSIX byte-range locks:
          │     │     ┌─────────────────────────────────────────────┐
          │     │     │ SQLite Lock     POSIX Implementation         │
          │     │     │─────────────────────────────────────────────│
          │     │     │ SHARED (1)      read-lock on shared range    │
          │     │     │ RESERVED (2)    write-lock on reserved byte  │
          │     │     │ PENDING (3)     write-lock on pending byte   │
          │     │     │ EXCLUSIVE (4)   write-lock on shared range   │
          │     │     └─────────────────────────────────────────────┘
          │     │
          │     ├── Lock byte offsets in database file:
          │     │     ┌──────────────────────────────────────┐
          │     │     │ Offset 0x40000000 (1GB):              │
          │     │     │   PENDING_BYTE    (1 byte)            │
          │     │     │   RESERVED_BYTE   (1 byte)            │
          │     │     │   SHARED_RANGE    (510 bytes)          │
          │     │     │                                       │
          │     │     │ These bytes are never read/written     │
          │     │     │ as data — only used for locking        │
          │     │     └──────────────────────────────────────┘
          │     │
          │     ├── fcntl(fd, F_SETLK, &lock)
          │     │     lock.l_type  = F_RDLCK or F_WRLCK
          │     │     lock.l_start = offset
          │     │     lock.l_len   = range
          │     │
          │     └── If fails: return SQLITE_BUSY
          │
          └── [Windows: winLock()]
                └── LockFileEx(h, flags, offset, len)

  Lock compatibility (rollback mode):
  ┌────────────────────────────────────────────────┐
  │             SHARED  RESERVED  PENDING  EXCL    │
  │  SHARED       ✓        ✓        ✗       ✗     │
  │  RESERVED     ✓        ✗        ✗       ✗     │
  │  PENDING      ✗        ✗        ✗       ✗     │
  │  EXCLUSIVE    ✗        ✗        ✗       ✗     │
  │                                                │
  │  ✓ = compatible (both can hold simultaneously)  │
  │  ✗ = conflicting (second requester gets BUSY)   │
  └────────────────────────────────────────────────┘
```

---

## 9. sqlite3OsUnlock

Releases a file lock, downgrading to a lower level.

```
  pager.c: commit / close
          │
          ▼
  sqlite3OsUnlock(pFile, eLock)
          │
          ▼  pFile->pMethods->xUnlock()
          │
          ├── [Unix: unixUnlock()]
          │     │
          │     ├── fcntl(fd, F_SETLK, &lock)
          │     │     lock.l_type = F_UNLCK
          │     │
          │     └── Downgrade sequence:
          │           EXCLUSIVE → SHARED:
          │             Release write-lock on shared range
          │             Keep read-lock on shared range
          │
          │           SHARED → NO_LOCK:
          │             Release all locks
          │
          └── [Windows: winUnlock()]
                └── UnlockFileEx(h, offset, len)

  Typical transitions:
    After commit:    EXCLUSIVE → SHARED   (keep reading)
    After close:     SHARED    → NO_LOCK  (release file)
```

---

## 10. sqlite3OsCheckReservedLock

Checks if another connection holds a RESERVED lock. Used by pager
to determine if it's safe to begin a write transaction.

```
  pager.c: before acquiring lock
          │
          ▼
  sqlite3OsCheckReservedLock(pFile, &isReserved)
          │
          ▼  pFile->pMethods->xCheckReservedLock()
          │
          ├── [Unix: unixCheckReservedLock()]
          │     │
          │     ├── First check in-process lock table
          │     │     (shared among threads)
          │     │
          │     └── fcntl(fd, F_GETLK, &lock)
          │           lock.l_start = RESERVED_BYTE
          │           if lock.l_type != F_UNLCK:
          │             *pResOut = 1  (someone has reserved)
          │           else:
          │             *pResOut = 0  (no one has reserved)
          │
          └── [Windows: winCheckReservedLock()]
                └── LockFile(h, RESERVED_BYTE, 1)
                    then UnlockFile() if succeeded
```

---

## 11. sqlite3OsDelete

Deletes a file from the filesystem. Used to remove journal and WAL files.

```
  pager.c: after commit (rollback mode)
          │
          ▼
  sqlite3OsDelete(pVfs, zPath, syncDir)
          │
          ▼  pVfs->xDelete()
          │
          ├── [Unix: unixDelete()]
          │     │
          │     ├── unlink(zPath)
          │     │     ┌──────────────────────────────────┐
          │     │     │ Before:                           │
          │     │     │  test.db-journal  [exists]        │
          │     │     │                                   │
          │     │     │ unlink("test.db-journal")          │
          │     │     │                                   │
          │     │     │ After:                            │
          │     │     │  test.db-journal  [deleted]        │
          │     │     │                                   │
          │     │     │ THIS IS THE ATOMIC COMMIT POINT   │
          │     │     │ (no journal = committed)           │
          │     │     └──────────────────────────────────┘
          │     │
          │     └── if syncDir:
          │           dirfd = open(parent_directory)
          │           fsync(dirfd)  ── ensure deletion durable
          │           close(dirfd)
          │
          └── [Windows: winDelete()]
                └── DeleteFileW(zPath)

  Files deleted:
    test.db-journal  ← after rollback-mode commit
    test.db-wal      ← when WAL is empty and not needed
```

---

## 12. sqlite3OsAccess

Checks if a file exists or has certain permissions.

```
  pager.c: check for hot journal / WAL file
          │
          ▼
  sqlite3OsAccess(pVfs, zPath, flags, &result)
          │
          ▼  pVfs->xAccess()
          │
          ├── flags:
          │     SQLITE_ACCESS_EXISTS    (0) → file exists?
          │     SQLITE_ACCESS_READWRITE (1) → can read+write?
          │     SQLITE_ACCESS_READ      (2) → can read?
          │
          ├── [Unix: unixAccess()]
          │     │
          │     └── access(zPath, mode)
          │           mode = F_OK / R_OK | W_OK
          │           *pResOut = (rc == 0) ? 1 : 0
          │
          └── [Windows: winAccess()]
                └── GetFileAttributesW(zPath)

  Typical checks:
  ┌──────────────────────────────────────────────────┐
  │ "test.db-journal" EXISTS?                         │
  │   yes → hot journal detected → need recovery!     │
  │   no  → clean state                               │
  │                                                   │
  │ "test.db-wal" EXISTS?                             │
  │   yes → switch to WAL mode                        │
  │   no  → use rollback journal mode                 │
  └──────────────────────────────────────────────────┘
```

---

## 13. sqlite3OsFullPathname

Converts a relative path to an absolute path.

```
  pager.c: sqlite3PagerOpen
          │
          ▼
  sqlite3OsFullPathname(pVfs, "test.db", nOut, zOut)
          │
          ▼  pVfs->xFullPathname()
          │
          ├── [Unix: unixFullPathname()]
          │     │
          │     ├── if zPath[0] == '/':
          │     │     Already absolute, copy as-is
          │     │
          │     └── else:
          │           getcwd(zBuf)
          │           zOut = zBuf + "/" + zPath
          │           ┌────────────────────────────────┐
          │           │ "test.db"                       │
          │           │   → "/home/user/data/test.db"   │
          │           └────────────────────────────────┘
          │
          └── [Windows: winFullPathname()]
                └── GetFullPathNameW(zPath, ...)
```

---

## 14. sqlite3OsFileControl

Sends control commands to the file handle. An extensible mechanism
for pager-to-VFS communication.

```
  pager.c / wal.c
          │
          ▼
  sqlite3OsFileControl(pFile, op, pArg)
          │
          ▼  pFile->pMethods->xFileControl()
          │
          ├── Common operations:
          │     ┌──────────────────────────────────────────────────┐
          │     │ SQLITE_FCNTL_LOCKSTATE     → query lock state    │
          │     │ SQLITE_FCNTL_SIZE_HINT     → hint future size    │
          │     │ SQLITE_FCNTL_CHUNK_SIZE    → set allocation size │
          │     │ SQLITE_FCNTL_SYNC          → custom sync         │
          │     │ SQLITE_FCNTL_DB_UNCHANGED  → mark unmodified     │
          │     │ SQLITE_FCNTL_PERSIST_WAL   → keep WAL on close   │
          │     │ SQLITE_FCNTL_POWERSAFE_OVERWRITE → device flag   │
          │     └──────────────────────────────────────────────────┘
          │
          ├── [Unix: unixFileControl()]
          │     switch(op):
          │       SQLITE_FCNTL_CHUNK_SIZE:
          │         pFile->szChunk = *(int*)pArg
          │         (pre-allocate disk space in chunks)
          │
          │       SQLITE_FCNTL_SIZE_HINT:
          │         if newSize > currentSize:
          │           ftruncate(fd, newSize)  ── pre-extend
          │
          └── [Windows: winFileControl()]
                Similar switch-based dispatch

  sqlite3OsFileControlHint(pFile, op, pArg)
          │
          └── Same as FileControl but ignores errors
              (used for non-critical hints)
```

---

## 15. sqlite3OsSectorSize

Returns the sector size of the underlying storage device.
Determines minimum atomic write unit.

```
  pager.c: configuration
          │
          ▼
  sqlite3OsSectorSize(pFile)
          │
          ▼  pFile->pMethods->xSectorSize()
          │
          └── [Unix: unixSectorSize()]
                │
                └── return SQLITE_DEFAULT_SECTOR_SIZE  (4096)
                    ┌───────────────────────────────────────┐
                    │ Sector size determines:                │
                    │                                       │
                    │  If sectorSize > pageSize:             │
                    │    Multiple pages journaled together   │
                    │    (prevents partial sector writes)    │
                    │                                       │
                    │  Typically:                            │
                    │    HDD:  512 bytes                     │
                    │    SSD:  4096 bytes                    │
                    │    NVMe: 4096 bytes                    │
                    └───────────────────────────────────────┘
```

---

## 16. sqlite3OsDeviceCharacteristics

Returns bitmask describing device capabilities. Affects journaling
and sync strategies.

```
  pager.c: choosing journal strategy
          │
          ▼
  sqlite3OsDeviceCharacteristics(pFile)
          │
          ▼  pFile->pMethods->xDeviceCharacteristics()
          │
          └── Returns combination of:
              ┌─────────────────────────────────────────────────┐
              │ SQLITE_IOCAP_ATOMIC        → all writes atomic  │
              │ SQLITE_IOCAP_ATOMIC4K      → 4K writes atomic   │
              │ SQLITE_IOCAP_SAFE_APPEND   → appends are atomic │
              │ SQLITE_IOCAP_SEQUENTIAL    → writes ordered      │
              │ SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN              │
              │ SQLITE_IOCAP_POWERSAFE_OVERWRITE                │
              │ SQLITE_IOCAP_BATCH_ATOMIC  → batch atomic write │
              │ SQLITE_IOCAP_SUBPAGE_READ  → sub-page read ok   │
              └─────────────────────────────────────────────────┘

              Impact on pager behavior:
              ┌─────────────────────────────────────────────────┐
              │ If ATOMIC4K + pageSize=4096:                     │
              │   → Can skip journal for single-page changes!   │
              │                                                 │
              │ If SAFE_APPEND:                                  │
              │   → Journal doesn't need sync before DB write    │
              │                                                 │
              │ If BATCH_ATOMIC:                                 │
              │   → Can use F2FS atomic write for whole commit   │
              │                                                 │
              │ If POWERSAFE_OVERWRITE:                          │
              │   → Don't need to pad writes to sector boundary │
              └─────────────────────────────────────────────────┘
```

---

## 17. sqlite3OsShmMap

Maps a region of shared memory for the WAL-index.
The WAL-index is how readers and writers coordinate in WAL mode.

```
  wal.c: access WAL-index
          │
          ▼
  sqlite3OsShmMap(pDbFd, iRegion, szRegion=32768, bExtend, &pMem)
          │
          ▼  pFile->pMethods->xShmMap()
          │
          ├── [Unix: unixShmMap()]
          │     │
          │     ├── First call: open shared memory file
          │     │     fd = open("test.db-shm", O_RDWR|O_CREAT)
          │     │
          │     ├── Extend file if needed:
          │     │     ftruncate(shmFd, nRegion * szRegion)
          │     │
          │     ├── mmap the region:
          │     │     ptr = mmap(NULL, szRegion, PROT_READ|PROT_WRITE,
          │     │                MAP_SHARED, shmFd, iRegion*szRegion)
          │     │     ┌──────────────────────────────────────┐
          │     │     │ Shared Memory File (test.db-shm)      │
          │     │     │                                       │
          │     │     │ Region 0 (32KB):                      │
          │     │     │  ┌─────────────────────────────────┐  │
          │     │     │  │ WAL-index header                │  │
          │     │     │  │ Hash table 0                    │  │
          │     │     │  │   pgno → frame# mapping        │  │
          │     │     │  └─────────────────────────────────┘  │
          │     │     │                                       │
          │     │     │ Region 1 (32KB):                      │
          │     │     │  ┌─────────────────────────────────┐  │
          │     │     │  │ Hash table 1                    │  │
          │     │     │  │   (overflow entries)            │  │
          │     │     │  └─────────────────────────────────┘  │
          │     │     └──────────────────────────────────────┘
          │     │
          │     └── *pp = mmap'd pointer
          │         (shared across all processes!)
          │
          └── [Windows: winShmMap()]
                └── CreateFileMappingW / MapViewOfFile

  How WAL-index is shared:
  ┌──────────────────────────────────────────────┐
  │ Process A (writer)        Process B (reader)  │
  │       │                         │             │
  │       ▼                         ▼             │
  │   mmap region 0 ──────────► mmap region 0     │
  │       │              (same physical pages)     │
  │       │                         │             │
  │  write hash entry         read hash entry     │
  │  (under SHM lock)        (under SHM lock)     │
  └──────────────────────────────────────────────┘
```

---

## 18. sqlite3OsShmLock

Acquires or releases shared-memory locks. Controls concurrent access
to the WAL-index. Uses byte-range locks on the shm file.

```
  wal.c: begin read/write transaction
          │
          ▼
  sqlite3OsShmLock(pFile, offset, n, flags)
          │
          ▼  pFile->pMethods->xShmLock()
          │
          ├── flags:
          │     SQLITE_SHM_LOCK    | SQLITE_SHM_SHARED     → shared lock
          │     SQLITE_SHM_LOCK    | SQLITE_SHM_EXCLUSIVE  → exclusive lock
          │     SQLITE_SHM_UNLOCK  | SQLITE_SHM_SHARED     → unlock shared
          │     SQLITE_SHM_UNLOCK  | SQLITE_SHM_EXCLUSIVE  → unlock exclusive
          │
          ├── [Unix: unixShmLock()]
          │     │
          │     ├── Track locks in per-process bitmask
          │     │
          │     └── fcntl(shmFd, F_SETLK, &lock)
          │           lock.l_start = 120 + offset
          │           lock.l_len = n
          │           lock.l_type = F_RDLCK / F_WRLCK / F_UNLCK
          │
          └── [Windows: winShmLock()]
                └── LockFileEx / UnlockFileEx on shm file

  WAL lock slots:
  ┌─────────────────────────────────────────────────────┐
  │ Slot  Name           Who holds it                    │
  │─────────────────────────────────────────────────────│
  │ [0]   WAL_WRITE_LOCK  Writer (exclusive)             │
  │ [1]   WAL_CKPT_LOCK   Checkpointer (exclusive)       │
  │ [2]   WAL_RECOVER_LOCK Recovery (exclusive)           │
  │ [3]   WAL_READ_LOCK(0) Reader snapshot 0 (shared)    │
  │ [4]   WAL_READ_LOCK(1) Reader snapshot 1 (shared)    │
  │ [5]   WAL_READ_LOCK(2) Reader snapshot 2 (shared)    │
  │ [6]   WAL_READ_LOCK(3) Reader snapshot 3 (shared)    │
  │ [7]   WAL_READ_LOCK(4) Reader snapshot 4 (shared)    │
  └─────────────────────────────────────────────────────┘

  Concurrency model:
  ┌──────────────────────────────────────────────┐
  │ Multiple readers:                             │
  │   Each takes SHARED lock on a read-mark slot  │
  │   Can read simultaneously                     │
  │                                               │
  │ Single writer:                                │
  │   Takes EXCLUSIVE lock on WAL_WRITE_LOCK       │
  │   Readers continue unblocked                   │
  │                                               │
  │ Checkpoint:                                    │
  │   Takes EXCLUSIVE lock on WAL_CKPT_LOCK        │
  │   Waits for readers on old snapshots           │
  └──────────────────────────────────────────────┘
```

---

## 19. sqlite3OsShmBarrier

Memory barrier for shared memory access. Ensures writes to shared memory
are visible to other processes.

```
  wal.c: after updating WAL-index
          │
          ▼
  sqlite3OsShmBarrier(pFile)
          │
          ▼  pFile->pMethods->xShmBarrier()
          │
          ├── [Unix: unixShmBarrier()]
          │     │
          │     ├── sqlite3MemoryBarrier()
          │     │     __sync_synchronize()  or  asm("mfence")
          │     │     ┌────────────────────────────────────┐
          │     │     │ CPU Memory Ordering                 │
          │     │     │                                    │
          │     │     │ Before barrier:                    │
          │     │     │   Store to shm may be in CPU cache │
          │     │     │   Other CPUs might not see it yet   │
          │     │     │                                    │
          │     │     │ After barrier:                     │
          │     │     │   All stores flushed to memory      │
          │     │     │   Other CPUs will see the update    │
          │     │     └────────────────────────────────────┘
          │     │
          │     └── unixEnterMutex() / unixLeaveMutex()
          │           (also acts as a compiler barrier)
          │
          └── [Windows: winShmBarrier()]
                └── MemoryBarrier()  (Win32 intrinsic)

  Used in WAL protocol:
    Writer updates WAL-index header ──► barrier ──► readers see it
```

---

## 20. sqlite3OsShmUnmap

Unmaps shared memory and optionally deletes the shm file.

```
  wal.c: sqlite3WalClose
          │
          ▼
  sqlite3OsShmUnmap(pFile, deleteFlag)
          │
          ▼  pFile->pMethods->xShmUnmap()
          │
          ├── [Unix: unixShmUnmap()]
          │     │
          │     ├── Remove connection from shm node
          │     │
          │     ├── If last connection:
          │     │     for each region:
          │     │       munmap(apRegion[i], szRegion)
          │     │
          │     │     close(shmFd)
          │     │
          │     │     if deleteFlag:
          │     │       unlink("test.db-shm")
          │     │
          │     └── Free shm node memory
          │
          └── [Windows: winShmUnmap()]
                ├── UnmapViewOfFile()
                └── CloseHandle(hMap)
```

---

## 21. sqlite3OsFetch / sqlite3OsUnfetch

Memory-mapped I/O operations. Map file pages directly into process
address space for zero-copy reads.

```
  pager.c: read page (mmap path)
          │
          ▼
  sqlite3OsFetch(pFile, offset, amt, &pp)
          │
          ▼  pFile->pMethods->xFetch()
          │
          ├── [Unix: unixFetch()]
          │     │
          │     ├── if mmap not yet set up:
          │     │     unixMapfile(pFile, -1)
          │     │       pFile->pMapRegion = mmap(NULL, fileSize,
          │     │         PROT_READ, MAP_SHARED, fd, 0)
          │     │
          │     ├── if offset + amt ≤ mmapSize:
          │     │     *pp = pMapRegion + offset
          │     │     ┌──────────────────────────────────────┐
          │     │     │ Process Address Space                 │
          │     │     │                                       │
          │     │     │  [code] [heap] [stack] [mmap region]  │
          │     │     │                         ▲              │
          │     │     │               *pp ──────┘              │
          │     │     │                                       │
          │     │     │  No data copy! Direct pointer into    │
          │     │     │  kernel page cache via page tables     │
          │     │     └──────────────────────────────────────┘
          │     │
          │     └── else: *pp = NULL (fall back to read())
          │
          └── [Windows: winFetch()]
                └── MapViewOfFile(hMap, offset, amt)


  sqlite3OsUnfetch(pFile, offset, p)
          │
          ▼  pFile->pMethods->xUnfetch()
          │
          └── [Unix: unixUnfetch()]
                │
                ├── if p == NULL:
                │     Unmap entire file
                │     munmap(pMapRegion, mmapSize)
                │
                └── else:
                      No-op (mmap pages managed by kernel)
```

---

## 22. sqlite3OsRandomness

Gets random bytes from the OS. Used for journal checksums
and PRNG initialization.

```
  global.c: sqlite3_initialize
          │
          ▼
  sqlite3OsRandomness(pVfs, nByte, zBuf)
          │
          ▼  pVfs->xRandomness()
          │
          ├── [Unix: unixRandomness()]
          │     │
          │     ├── fd = open("/dev/urandom", O_RDONLY)
          │     │     read(fd, zBuf, nByte)
          │     │     close(fd)
          │     │
          │     └── Fallback: time() + getpid()
          │
          └── [Windows: winRandomness()]
                └── SystemFunction036(zBuf, nByte)
                    (RtlGenRandom / CryptGenRandom)
```

---

## 23. sqlite3OsSleep

Pauses execution. Used by WAL for retry delays.

```
  wal.c: busy-wait during WAL recovery
          │
          ▼
  sqlite3OsSleep(pVfs, microseconds)
          │
          ▼  pVfs->xSleep()
          │
          ├── [Unix: unixSleep()]
          │     │
          │     └── usleep(microseconds)
          │           or nanosleep() if available
          │
          └── [Windows: winSleep()]
                └── Sleep(microseconds / 1000)
```

---

## 24. sqlite3OsCurrentTimeInt64

Gets the current time as milliseconds since Julian epoch.

```
  pager.c / wal.c
          │
          ▼
  sqlite3OsCurrentTimeInt64(pVfs, &timeMs)
          │
          ▼  pVfs->xCurrentTimeInt64()
          │
          ├── [Unix: unixCurrentTimeInt64()]
          │     │
          │     ├── gettimeofday(&tv, NULL)
          │     │
          │     └── *piNow = unixEpoch + tv.tv_sec*1000 + tv.tv_usec/1000
          │           (Julian day number in milliseconds)
          │
          └── [Windows: winCurrentTimeInt64()]
                └── GetSystemTimeAsFileTime(&ft)
```

---

## Complete I/O Flow: keyvaluestore_put → Disk

```
  keyvaluestore_put("key", "value")
      │
      ▼
  btree.c: sqlite3BtreeInsert()
      │
      ├── sqlite3PagerGet(pgno=3)           ─── cache miss ───►
      │     sqlite3OsRead(fd, buf, 4096, 8192)
      │       lseek(fd, 8192) + read(fd, buf, 4096)
      │
      ├── sqlite3PagerWrite(page3)
      │     [Rollback: sqlite3OsWrite(jfd, orig, 4096, jrnlOff)]
      │     [WAL: mark dirty, defer to commit]
      │
      ├── Modify cell data in page3 buffer (in-memory)
      │
      └── keyvaluestore_commit()
            │
            ├── [WAL mode]
            │     sqlite3OsWrite(walFd, frame_hdr+page3, 4120, walOff)
            │       lseek(walFd, walOff) + write(walFd, data, 4120)
            │     sqlite3OsSync(walFd, SQLITE_SYNC_NORMAL)
            │       fdatasync(walFd)  ──── DATA ON DISK ────
            │
            └── [Rollback mode]
                  sqlite3OsSync(jfd, SQLITE_SYNC_NORMAL)
                    fdatasync(jfd)    ──── journal safe ────
                  sqlite3OsWrite(fd, page3, 4096, 8192)
                    lseek(fd, 8192) + write(fd, data, 4096)
                  sqlite3OsSync(fd, SQLITE_SYNC_NORMAL)
                    fdatasync(fd)     ──── DATA ON DISK ────
                  sqlite3OsDelete(vfs, "test.db-journal")
                    unlink("test.db-journal") ── COMMITTED ──
```

---

## Quick Reference: Pager/WAL → OS Layer Mapping

| Pager/WAL Operation | OS Calls (Unix) | Syscalls |
|---------------------|-----------------|----------|
| `PagerOpen` | `OsOpen`, `OsRead` | `open()`, `read()` |
| `PagerClose` | `OsClose` | `close()` |
| `PagerGet` (miss) | `OsRead` | `lseek()+read()` or `memcpy` (mmap) |
| `PagerWrite` (rollback) | `OsWrite` (journal) | `lseek()+write()` |
| `CommitPhaseOne` (WAL) | `OsWrite`, `OsSync` | `write()`, `fdatasync()` |
| `CommitPhaseOne` (jrnl) | `OsSync`, `OsWrite`, `OsSync` | `fdatasync()`, `write()`, `fdatasync()` |
| `CommitPhaseTwo` (jrnl) | `OsDelete` | `unlink()` |
| `PagerRollback` (jrnl) | `OsRead`, `OsWrite`, `OsDelete` | `read()`, `write()`, `unlink()` |
| `SharedLock` | `OsLock`, `OsAccess` | `fcntl(F_SETLK)`, `access()` |
| `Begin` (rollback) | `OsLock` | `fcntl(F_SETLK)` → RESERVED |
| `Begin` (WAL) | `OsShmLock` | `fcntl(F_SETLK)` on shm |
| `Checkpoint` | `OsRead`, `OsWrite`, `OsSync` | `read()`, `write()`, `fdatasync()` |
| `OpenWal` | `OsAccess`, `OsOpen` | `access()`, `open()` |
| WAL-index access | `OsShmMap`, `OsShmLock`, `OsShmBarrier` | `mmap()`, `fcntl()`, `mfence` |

---

## Platform Comparison

| Operation | Linux | macOS | Windows |
|-----------|-------|-------|---------|
| File open | `open()` | `open()` | `CreateFileW()` |
| File read | `pread()` / `read()` | `pread()` / `read()` | `ReadFile()` |
| File write | `pwrite()` / `write()` | `pwrite()` / `write()` | `WriteFile()` |
| Sync | `fdatasync()` / `fsync()` | `fcntl(F_FULLFSYNC)` | `FlushFileBuffers()` |
| Truncate | `ftruncate()` | `ftruncate()` | `SetEndOfFile()` |
| File size | `fstat()` | `fstat()` | `GetFileSizeEx()` |
| Lock | `fcntl(F_SETLK)` | `fcntl(F_SETLK)` | `LockFileEx()` |
| Delete | `unlink()` | `unlink()` | `DeleteFileW()` |
| Shared mem | `mmap(MAP_SHARED)` | `mmap(MAP_SHARED)` | `CreateFileMapping()` |
| Memory barrier | `__sync_synchronize()` | `OSMemoryBarrier()` | `MemoryBarrier()` |
| Random | `/dev/urandom` | `/dev/urandom` | `RtlGenRandom()` |
| Sleep | `usleep()` | `usleep()` | `Sleep()` |
