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

It was born from a simple observation: RocksDB consumed ~121 MB RSS for a 1M record workload, even when tuned for small deployments. That felt like too much overhead for embedded or resource-constrained use cases.

The idea: bypass the SQL layer entirely and talk directly to SQLite's storage engine. No SQL parser. No query planner. No virtual machine. Just a clean KV API on top of a proven, battle-tested storage core.

> *SQLite-grade reliability. KV-first design. 11x less memory than RocksDB on small workloads.*

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

## Installation & Build

```bash
make              # builds libsnkv.a
make snkv.h       # generates single-header version
make examples     # builds examples
make run-examples # run all examples
make test         # run tests
make clean
```

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

> All benchmarks: 1M records, sync-on-commit enabled for all engines, Linux. Peak memory from `/usr/bin/time -v` (Maximum RSS).

### SNKV vs RocksDB

RocksDB configured conservatively for small workloads: 2MB block cache, 2MB write buffer, compression disabled, sync-on-commit enabled.

| Benchmark         | RocksDB     | SNKV        | Notes                         |
| ----------------- | ----------- | ----------- | ----------------------------- |
| Sequential writes | 237K ops/s  | 181K ops/s  | RocksDB faster (LSM-tree)     |
| Random reads      | 95K ops/s   | 154K ops/s  | **SNKV 1.6x faster**          |
| Sequential scan   | 1.78M ops/s | 5.95M ops/s | **SNKV 3.3x faster**          |
| Random updates    | 177K ops/s  | 48K ops/s   | RocksDB faster (LSM-tree)     |
| Random deletes    | 161K ops/s  | 41K ops/s   | RocksDB faster                |
| Mixed workload    | 51K ops/s   | 97K ops/s   | **SNKV 1.9x faster**          |
| Bulk insert       | 675K ops/s  | 494K ops/s  | RocksDB faster                |
| **Peak RSS**      | **121 MB**  | **10.8 MB** | **SNKV uses 11x less memory** |

RocksDB's LSM-tree design gives it a clear edge on write-heavy workloads — that's by design and expected. The memory gap is structural: even at minimum configuration, RocksDB carries significantly more overhead. If your workload is read-heavy or memory-constrained, SNKV is worth considering.

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

Since SNKV is built on SQLite's internals, this shows what the SQL layer actually costs for pure KV operations.

| Benchmark       | SQLite      | SNKV        | Delta            |
| --------------- | ----------- | ----------- | ---------------- |
| Random reads    | ~165K ops/s | ~345K ops/s | **~2x faster**   |
| Sequential scan | ~2.2M ops/s | ~12M ops/s  | **~5x faster**   |
| Random updates  | ~115K ops/s | ~330K ops/s | **~3x faster**   |
| Random deletes  | ~60K ops/s  | ~210K ops/s | **~3.5x faster** |
| Mixed workload  | ~130K ops/s | ~500K ops/s | **~4x faster**   |
| Bulk insert     | ~240K ops/s | ~880K ops/s | **~3.5x faster** |

---

## When to Use SNKV

**SNKV is a good fit if:**
- Your workload is read-heavy or mixed (reads + writes)
- You're running in a memory-constrained or embedded environment
- You want ACID guarantees without managing a full database engine
- You need single-header C integration with no external dependencies
- You want predictable latency — no compaction stalls, no mmap tuning

**Consider alternatives if:**
- You need maximum write/update throughput → **RocksDB**
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
