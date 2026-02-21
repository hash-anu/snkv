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

It was born from a simple observation: even conservatively tuned RocksDB (8MB block cache, 2MB write buffer, no compression) consumed ~27 MB RSS for a 1M record workload — 2.5x more than SNKV — while being slower on reads, scans, and mixed workloads. That felt like the wrong tradeoff for embedded or resource-constrained use cases where write throughput isn't the bottleneck.

The idea: bypass the SQL layer entirely and talk directly to SQLite's storage engine. No SQL parser. No query planner. No virtual machine. Just a clean KV API on top of a proven, battle-tested storage core.

> *SQLite-grade reliability. KV-first design. Lower memory footprint and faster reads than RocksDB on small workloads.*

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

```bash
make              # builds libsnkv.a
make snkv.h       # generates single-header version
make examples     # builds examples
make run-examples # run all examples
make test         # run all tests (CI suite)
make clean
```

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

> All benchmarks: 1M records, Linux. Numbers averaged across 3 runs. Peak memory from `/usr/bin/time -v` (Maximum RSS).
> SNKV vs SQLite use identical settings: WAL journal mode, `synchronous=NORMAL`, 2000-page (8 MB) page cache, 4096-byte pages.
>
> Benchmark source code: [SNKV](https://github.com/hash-anu/snkv/blob/master/tests/test_benchmark.c) · [RocksDB](https://github.com/hash-anu/rocksdb-benchmark) · [LMDB](https://github.com/hash-anu/lmdb-benchmark) · [SQLite](https://github.com/hash-anu/sqllite-benchmark-kv)

---

### SNKV vs RocksDB

RocksDB configured to match SNKV's durability level: 8MB block cache, 2MB write buffer, 4KB block size, compression disabled, bloom filters disabled, `sync=false` (matching SNKV's `synchronous=NORMAL`).

| Benchmark         | RocksDB     | SNKV        | Notes                                        |
| ----------------- | ----------- | ----------- | -------------------------------------------- |
| Sequential writes | 387K ops/s  | 147K ops/s  | RocksDB **2.6x faster** (LSM-tree)           |
| Random reads      | 50K ops/s   | 142K ops/s  | **SNKV 2.8x faster**                         |
| Sequential scan   | 937K ops/s  | 3.13M ops/s | **SNKV 3.3x faster**                         |
| Random updates    | 108K ops/s  | 23K ops/s   | RocksDB **4.6x faster** (LSM-tree)           |
| Random deletes    | 97K ops/s   | 20K ops/s   | RocksDB **4.8x faster** (LSM tombstones)     |
| Exists checks     | 37K ops/s   | 156K ops/s  | **SNKV 4.2x faster**                         |
| Mixed workload    | 40K ops/s   | 50K ops/s   | **SNKV 1.25x faster** (read-heavy 70%)       |
| Bulk insert       | 362K ops/s  | 241K ops/s  | RocksDB **1.5x faster**                      |
| **Peak RSS**      | **~27 MB**  | **10.8 MB** | **SNKV uses 2.5x less memory**               |

RocksDB's LSM-tree design dominates write-heavy operations — sequential writes, updates, deletes, and bulk insert. That's the fundamental LSM advantage: writes are append-only to a memtable, no in-place B-tree rebalancing. SNKV's B-tree wins on reads and scans: random reads are 2.8x faster and sequential scan is 3.3x faster because the data is already ordered on disk with no SST file merging overhead. Mixed workload (70% reads) goes to SNKV for the same reason. Memory usage is 2.5x lower — RocksDB's internal structures (block cache, memtables, table readers, compaction state) add up even at minimum configuration.

---

### SNKV vs LMDB

LMDB uses memory-mapped I/O, which makes it exceptionally fast — especially for reads and scans. These numbers reflect that honestly.

| Benchmark         | LMDB        | SNKV        | Notes                          |
| ----------------- | ----------- | ----------- | ------------------------------ |
| Sequential writes | 245K ops/s  | 181K ops/s  | LMDB faster                    |
| Random reads      | 779K ops/s  | 154K ops/s  | LMDB 5x faster (mmap)          |
| Sequential scan   | 37.9M ops/s | 5.95M ops/s | LMDB 6x faster (mmap)          |
| Random updates    | 119K ops/s  | 48K ops/s   | LMDB faster                    |
| Random deletes    | 120K ops/s  | 41K ops/s   | LMDB faster                    |
| Mixed workload    | 173K ops/s  | 97K ops/s   | LMDB 1.8x faster               |
| Bulk insert       | 1.5M ops/s  | 494K ops/s  | LMDB faster                    |
| **Peak RSS**      | **170 MB**  | **10.8 MB** | **SNKV uses 16x less memory**  |

LMDB wins on raw throughput across the board — that's the nature of memory-mapped storage. The tradeoff: LMDB maps the entire database into virtual address space, so memory usage scales with database size and requires upfront `mmap` size configuration. SNKV avoids that entirely — fixed, low overhead regardless of database size, and no tuning required.

---

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

## When to Use SNKV

**SNKV is a good fit if:**
- You're currently using RocksDB or LMDB and memory is a constraint
- Your workload is read-heavy or mixed (reads + writes)
- You're running in a memory-constrained or embedded environment
- You want a clean KV API without writing SQL strings, preparing statements, and binding parameters
- You need single-header C integration with no external dependencies
- You want predictable latency — no compaction stalls, no mmap tuning

**Consider alternatives if:**
- You need maximum write/update/delete throughput → **RocksDB**
- You need maximum read/scan speed and memory isn't a constraint → **LMDB**
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
