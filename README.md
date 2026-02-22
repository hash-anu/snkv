# SNKV — a simple, crash-safe embedded key-value store

[![Build](https://github.com/hash-anu/snkv/actions/workflows/c-cpp.yml/badge.svg)](https://github.com/hash-anu/snkv/actions/workflows/c-cpp.yml)
[![Memory Leaks](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/hash-anu/snkv/badges/valgrind.json)](https://github.com/hash-anu/snkv/actions/workflows/c-cpp.yml)
[![Tests](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/hash-anu/snkv/badges/tests.json)](https://github.com/hash-anu/snkv/actions/workflows/c-cpp.yml)
[![Peak Memory](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/hash-anu/snkv/badges/memory.json)](https://github.com/hash-anu/snkv/actions/workflows/c-cpp.yml)
[![GitHub Issues](https://img.shields.io/github/issues/hash-anu/snkv?label=open%20issues&color=orange)](https://github.com/hash-anu/snkv/issues)
[![GitHub Closed Issues](https://img.shields.io/github/issues-closed/hash-anu/snkv?label=closed%20issues&color=green)](https://github.com/hash-anu/snkv/issues?q=is%3Aissue+is%3Aclosed)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](https://github.com/hash-anu/snkv/blob/master/LICENSE)

---

## What is SNKV?

**SNKV** is a lightweight, **ACID-compliant embedded key-value store** built directly on SQLite's B-Tree storage engine — without SQL.

The idea: bypass the SQL layer entirely and talk directly to SQLite's storage engine. No SQL parser. No query planner. No virtual machine. Just a clean KV API on top of a proven, battle-tested storage core.

> *SQLite-grade reliability. KV-first design. Lower overhead for read-heavy and mixed key-value workloads.*

---

## Quick Start

Single-header integration — drop it in and go:

```c
#define SNKV_IMPLEMENTATION
#include "snkv.h"

int main(void) {
    KVStore *db;
    kvstore_open("mydb.db", &db, KVSTORE_JOURNAL_WAL);

    kvstore_put(db, "key", 3, "value", 5);

    void *val; int len;
    kvstore_get(db, "key", 3, &val, &len);
    printf("%.*s\n", len, (char*)val);
    snkv_free(val);

    kvstore_close(db);
}
```

---

## Configuration

Use `kvstore_open_v2` to control how the store is opened. Zero-initialise the
config and set only what you need — unset fields resolve to safe defaults.

```c
KVStoreConfig cfg = {0};
cfg.journalMode = KVSTORE_JOURNAL_WAL;   /* WAL mode (default) */
cfg.syncLevel   = KVSTORE_SYNC_NORMAL;   /* survives process crash (default) */
cfg.cacheSize   = 4000;                  /* ~16 MB page cache (default 2000 ≈ 8 MB) */
cfg.pageSize    = 4096;                  /* DB page size, new DBs only (default 4096) */
cfg.busyTimeout = 5000;                  /* retry 5 s on SQLITE_BUSY (default 0) */
cfg.readOnly    = 0;                     /* read-write (default) */

KVStore *db;
kvstore_open_v2("mydb.db", &db, &cfg);
```

| Field | Default | Options |
|-------|---------|---------|
| `journalMode` | `KVSTORE_JOURNAL_WAL` | `KVSTORE_JOURNAL_DELETE` |
| `syncLevel` | `KVSTORE_SYNC_NORMAL` | `KVSTORE_SYNC_OFF`, `KVSTORE_SYNC_FULL` |
| `cacheSize` | 2000 pages (~8 MB) | Any positive integer |
| `pageSize` | 4096 bytes | Power of 2, 512–65536; new DBs only |
| `readOnly` | 0 | 1 to open read-only |
| `busyTimeout` | 0 (fail immediately) | Milliseconds; useful for multi-process use |

`kvstore_open` remains fully supported and uses all defaults except `journalMode`.

---

## Installation & Build

### Linux / macOS

```bash
make              # builds libsnkv.a
make snkv.h       # generates single-header version
make examples     # builds examples
make run-examples # run all examples
make test         # run all tests (CI suite)
make clean
```

### Windows (MSYS2 / MinGW64)

**1.** Install [MSYS2](https://www.msys2.org/).

**2.** Launch **"MSYS2 MinGW 64-bit"** from the Start menu (not the plain MSYS2 terminal).

**3.** Install the toolchain:

```bash
pacman -S --needed mingw-w64-x86_64-gcc make
```

**4.** Clone and build:

```bash
git clone https://github.com/hash-anu/snkv.git
cd snkv
make              # builds libsnkv.a
make snkv.h       # generates single-header
make examples     # builds .exe examples
make run-examples
make test
```

> All commands must be run from the **MSYS2 MinGW64 shell**. Running `mingw32-make` from
> a native `cmd.exe` or PowerShell window will not work — the Makefile relies on `sh` and
> standard Unix tools that are only available inside the MSYS2 environment.

---

### Python Bindings

The `python/` directory contains a CPython C extension that wraps the full SNKV C API.

**Prerequisites:** Python 3.8+, a C compiler, and `make`.

#### Build

**Linux**

```bash
sudo apt-get install -y build-essential python3-dev

cd python
python3 setup.py build_ext --inplace
```

**macOS**

```bash
# Compiler comes with Xcode Command Line Tools
xcode-select --install    # skip if already installed

cd python
python3 setup.py build_ext --inplace
```

**Windows (MSYS2 MinGW64)**

Use the **MSYS2 MinGW64 shell** (same environment as the C build).
Install the MinGW64 Python package so the extension and interpreter share the same runtime:

```bash
pacman -S --needed mingw-w64-x86_64-python mingw-w64-x86_64-python-pip \
                   mingw-w64-x86_64-python-setuptools \
                   mingw-w64-x86_64-python-pytest

cd python
python3 setup.py build_ext --inplace
```

> On all platforms, `setup.py` automatically runs `make snkv.h` in the repo root
> and copies the freshly generated amalgamated header before compiling — no
> manual header step needed.

#### Quick Start

```python
from snkv import KVStore

with KVStore("mydb.db") as db:
    db["hello"] = "world"
    print(db["hello"].decode())   # world
```

#### Run Tests

```bash
# Linux / macOS — from the repo root (no PYTHONPATH needed)
python3 -m pytest python/tests/ -v

# Or from python/
cd python && python3 -m pytest tests/ -v

# Windows (MSYS2 MinGW64 shell)
cd python && python3 -m pytest tests/ -v
```

All 247 tests should pass.

#### Run Examples

```bash
# Linux / macOS
cd python
PYTHONPATH=. python3 examples/basic.py           # CRUD, binary data, in-memory store
PYTHONPATH=. python3 examples/transactions.py    # begin/commit/rollback
PYTHONPATH=. python3 examples/column_families.py # logical namespaces
PYTHONPATH=. python3 examples/iterators.py       # ordered scan, prefix scan
PYTHONPATH=. python3 examples/config.py          # journal mode, sync, cache, WAL limit
PYTHONPATH=. python3 examples/checkpoint.py      # manual + auto WAL checkpoint
PYTHONPATH=. python3 examples/session_store.py   # real-world session store pattern

# Windows (MSYS2 MinGW64 shell) — use PYTHONPATH= instead of export
cd python
PYTHONPATH=. python3 examples/basic.py
PYTHONPATH=. python3 examples/transactions.py
# ... same pattern for all examples
```

#### API at a Glance

```python
from snkv import KVStore, JOURNAL_WAL, SYNC_NORMAL, CHECKPOINT_TRUNCATE

# Open with options (mirrors kvstore_open_v2)
with KVStore("mydb.db",
           journal_mode=JOURNAL_WAL,
           sync_level=SYNC_NORMAL,
           cache_size=2000,       # pages (~8 MB)
           busy_timeout=5000,     # ms
           wal_size_limit=100,    # auto-checkpoint every 100 frames
           ) as db:

    # CRUD
    db["key"] = "value"
    val = db["key"]               # bytes
    del db["key"]
    exists = "key" in db

    # Column families — always close cf before db
    with db.create_column_family("users") as cf:
        cf["alice"] = b"admin"

    # Transactions
    db.begin(write=True)
    db["a"] = "1"
    db.commit()                   # or db.rollback()

    # Iterators
    for key, value in db.iterator():
        print(key, value)

    for key, value in db.prefix_iterator(b"user:"):
        print(key, value)

    # Maintenance
    nlog, nckpt = db.checkpoint(CHECKPOINT_TRUNCATE)
    db.sync()
    db.vacuum(100)                # reclaim up to 100 unused pages
    db.integrity_check()          # raises CorruptError if corrupt
    stats = db.stats()            # dict: puts, gets, deletes, ...
```

#### Error Hierarchy

```
snkv.Error (base)
├── snkv.NotFoundError   (also KeyError)
├── snkv.BusyError
├── snkv.LockedError
├── snkv.ReadOnlyError
└── snkv.CorruptError
```

---

### 10 GB Crash-Safety Stress Test

A production-scale kill-9 test is included but kept separate from the CI suite.
It writes unique deterministic key-value pairs into a 10 GB WAL-mode database,
forcibly kills the writer with `SIGKILL` during active writes, and verifies on
restart that every committed transaction is present with byte-exact values, no
partial transactions are visible, and the database has zero corruption.

```bash
make test-crash-10gb          # run full 5-cycle kill-9 + verify (Linux / macOS)

# individual modes
./tests/test_crash_10gb write  tests/crash_10gb.db   # continuous writer
./tests/test_crash_10gb verify tests/crash_10gb.db   # post-crash verifier
./tests/test_crash_10gb clean  tests/crash_10gb.db   # remove DB files
```

> Requires ~11 GB free disk. `run` mode is POSIX-only; `write` and `verify` work on all platforms.

---

## How It Works

Standard database path:

```
Application → SQL Parser → Query Planner → VDBE (VM) → B-Tree → Disk
```

SNKV path:

```
Application → KV API → B-Tree → Disk
```

By removing the layers you don't need for key-value workloads, SNKV keeps the proven storage core and cuts the overhead.

| Layer         | SQLite | SNKV |
| ------------- | ------ | ---- |
| SQL Parser    | ✅      | ❌    |
| Query Planner | ✅      | ❌    |
| VDBE (VM)     | ✅      | ❌    |
| B-Tree Engine | ✅      | ✅    |
| Pager / WAL   | ✅      | ✅    |

---

## Benchmarks

> 1M records, Linux, averaged across 3 runs.
> Both SNKV and SQLite use identical settings: WAL mode, `synchronous=NORMAL`, 2000-page (8 MB) page cache, 4096-byte pages.
>
> Benchmark source: [SNKV](https://github.com/hash-anu/snkv/blob/master/tests/test_benchmark.c) · [SQLite](https://github.com/hash-anu/sqllite-benchmark-kv)

### SNKV vs SQLite (KV workloads)

SQLite benchmark uses `WITHOUT ROWID` with a BLOB primary key — the fairest possible comparison, both using a single B-tree keyed on the same field. Both run with identical settings: WAL mode, `synchronous=NORMAL`, 2000-page (8 MB) cache, 4096-byte pages. This isolates the pure cost of the SQL layer for KV operations.

> Note: Both SNKV and SQLite (`WITHOUT ROWID`) use identical peak RSS (~10.8 MB) since they share the same underlying pager and page cache infrastructure.

| Benchmark         | SQLite       | SNKV         | Notes                        |
| ----------------- | ------------ | ------------ | ---------------------------- |
| Sequential writes | 140K ops/s   | 146K ops/s   | **SNKV 1.05x faster**        |
| Random reads      | 87K ops/s    | 139K ops/s   | **SNKV 1.6x faster**         |
| Sequential scan   | 1.61M ops/s  | 3.16M ops/s  | **SNKV 2x faster**           |
| Random updates    | 17K ops/s    | 24K ops/s    | **SNKV 1.4x faster**         |
| Random deletes    | 17K ops/s    | 20K ops/s    | **SNKV 1.2x faster**         |
| Exists checks     | 87K ops/s    | 149K ops/s   | **SNKV 1.7x faster**         |
| Mixed workload    | 35K ops/s    | 50K ops/s    | **SNKV 1.4x faster**         |
| Bulk insert       | 211K ops/s   | 240K ops/s   | **SNKV 1.1x faster**         |

With identical storage configuration, SNKV wins across every benchmark. The gains come from two sources: bypassing the SQL layer (no parsing, no query planner, no VDBE) and a per-column-family cached read cursor that eliminates repeated cursor open/close overhead on the hot read path. The biggest wins are on read-heavy operations — random reads (+60%), exists checks (+70%), and sequential scan (+100%) — exactly where the cursor caching pays off most.

---

### Running your own LMDB / RocksDB comparison

If you want to benchmark SNKV against LMDB or RocksDB, the benchmark harnesses are here:

- **LMDB** — [github.com/hash-anu/lmdb-benchmark](https://github.com/hash-anu/lmdb-benchmark)
- **RocksDB** — [github.com/hash-anu/rocksdb-benchmark](https://github.com/hash-anu/rocksdb-benchmark)

---

## When to Use SNKV

**SNKV is a good fit if:**
- Your workload is read-heavy or mixed (reads + writes)
- You're running in a memory-constrained or embedded environment
- You want a clean KV API without writing SQL strings, preparing statements, and binding parameters
- You need single-header C integration with no external dependencies
- You want predictable latency — no compaction stalls, no mmap tuning

**Consider alternatives if:**
- You need maximum write/update/delete throughput → **RocksDB** (LSM-tree)
- You need maximum read/scan speed and memory isn't a constraint → **LMDB** (memory-mapped)
- You already use SQL elsewhere and want to consolidate → **SQLite directly**

---

## Features

- **ACID Transactions** — commit / rollback safety
- **WAL Mode** — concurrent readers + single writer
- **Column Families** — logical namespaces within a single database
- **Iterators** — ordered key traversal
- **Thread Safe** — built-in synchronization
- **Single-header** — drop `snkv.h` into any C/C++ project
- **Zero memory leaks** — verified with Valgrind
- **SSD-friendly** — WAL appends sequentially, reducing random writes

---

## Backup & Tooling Compatibility

Because SNKV uses SQLite's file format and pager layer, backup tools that operate at the WAL or page level work out of the box:

- ✅ **LiteFS** — distributed SQLite replication works with SNKV databases
- ✅ **SQLite Online Backup API** — operates at the page level, fully compatible
- ✅ **WAL-based backup tools** — any tool consuming WAL files works correctly
- ✅ **Rollback journal tools** — journal mode is fully supported

**Note:** Tools that rely on SQLite's schema layer — like the `sqlite3` CLI or DB Browser for SQLite — won't work. SNKV bypasses the schema layer entirely by design.

---

## Internals & Documentation

I documented the SQLite internals explored while building this:

- [B-Tree operations](https://github.com/hash-anu/snkv/blob/master/internal/BTREE_OPERATIONS.md)
- [Pager operations](https://github.com/hash-anu/snkv/blob/master/internal/PAGER_OPERATIONS.md)
- [OS layer operations](https://github.com/hash-anu/snkv/blob/master/internal/OS_LAYER_OPERATIONS.md)
- [KV layer design](https://github.com/hash-anu/snkv/blob/master/internal/KVSTORE_OPERATIONS.md)

---

## Design Principles

- **Minimalism wins** — fewer layers, less overhead
- **Proven foundations** — reuse battle-tested storage, don't reinvent it
- **Predictable performance** — no hidden query costs, no compaction stalls
- **Honest tradeoffs** — SNKV is not the fastest at everything; it's optimized for its target use case

---

## License

Apache License 2.0 © 2025 Hash Anu
