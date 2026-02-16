# SNKV — Embedded Key–Value Engine Built on Proven B-Tree Technology

## Overview

**SNKV** is a lightweight, high-performance **ACID-compliant key–value database** designed for systems that need **speed, simplicity, and reliability**.

It is built directly on top of SQLite’s battle-tested **B-Tree storage engine**, while exposing a **native key–value API** — without SQL.

> ⚡ Think: *SQLite-grade reliability with a KV-first design*

By removing unnecessary layers and focusing purely on key–value operations, SNKV delivers **significantly higher throughput and lower latency** for real-world workloads.

---

## Why SNKV?

General-purpose databases optimize for flexibility.
SNKV optimizes for **focus**.

When your workload is key–value:

* No SQL parsing needed
* No query planning needed
* No virtual machine execution

SNKV removes that overhead — while keeping the **proven storage core intact**.

### What You Get

* ⚡ **Up to ~3–5× faster operations**
* 🚀 **~4× higher throughput in mixed workloads**
* 💾 **Lower memory footprint**
* 🔒 **Full ACID guarantees**
* 📦 **Single-header drop-in integration**

---

## What Makes SNKV Different?

SNKV is **not a wrapper**.

It is a **purpose-built KV engine** that directly integrates with SQLite’s internal storage layer.

| Layer         | SQLite | SNKV |
| ------------- | ------ | ---- |
| SQL Parser    | ✅      | ❌    |
| Query Planner | ✅      | ❌    |
| VDBE (VM)     | ✅      | ❌    |
| B-Tree Engine | ✅      | ✅    |
| Pager / WAL   | ✅      | ✅    |

> SNKV keeps the **engine**, removes the **overhead**, and exposes a **clean KV interface**.

---

## Design Principles

* **Minimalism wins** — fewer layers, less overhead
* **Proven foundations** — reuse battle-tested storage
* **Predictable performance** — no hidden query costs
* **Developer-first** — simple, embeddable API

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

## Quick Start (Single Header)

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

## Features

* **ACID Transactions** — commit / rollback safety
* **WAL Mode** — concurrent readers + writer
* **SSD-Friendly Writes** — WAL appends data sequentially, reducing random writes
* **Column Families** — logical namespaces
* **Iterators** — ordered traversal
* **Thread Safe** — built-in synchronization
* **Zero Memory Leaks** — verified

---

## Performance (Latest Benchmarks)

Measured on **50,000 records**, averaged across multiple runs.

### 🔥 SNKV Performance

| Operation         | Throughput        |
| ----------------- | ----------------- |
| Sequential Writes | ~260K ops/sec     |
| Random Reads      | ~340K ops/sec     |
| Sequential Scan   | **~12M ops/sec**  |
| Random Updates    | ~330K ops/sec     |
| Random Deletes    | ~210K ops/sec     |
| Exists Checks     | ~350K ops/sec     |
| Mixed Workload    | **~500K ops/sec** |
| Bulk Insert       | **~880K ops/sec** |

---

### ⚔️ SNKV vs RocksDB

```
READS / SCANS / MIXED
SNKV      ████████████████████████████
RocksDB   ███████████

WRITES / BULK INSERT
SNKV      ███████████
RocksDB   ████████████████████████████

UPDATES / DELETES
SNKV      ███████
RocksDB   █████████████████████
```

**Interpretation:**
- SNKV → Faster reads, scans, and mixed workloads
- RocksDB → Faster writes and heavy ingestion
- SNKV → More predictable latency (no compaction stalls)

---

### ⚔️ SNKV vs LMDB

    READS / SCANS
    LMDB      ███████████████████████████████████████████
    SNKV      ███████████

    WRITES
    LMDB      ███████████████████
    SNKV      ███████████

    MIXED WORKLOAD
    LMDB      ███████████████████████
    SNKV      ███████████

    MEMORY USAGE
    LMDB      ███████████████████████████████
    SNKV      ████

**Interpretation:** 
- LMDB → Extremely fast reads & scans(memory-mapped)
- LMDB → Higher memory usage (\~160MB+ observed)
- SNKV → Lower memory footprint, simpler deployment
- SNKV → No mmap tuning required


### ⚖️ Comparison (Key–Value Workloads)

| Benchmark       | SQLite | SNKV  | Improvement      |
| --------------- | ------ | ----- | ---------------- |
| Random Reads    | ~165K  | ~345K | **~2× faster**   |
| Sequential Scan | ~2.2M  | ~12M  | **~5× faster**   |
| Random Updates  | ~115K  | ~330K | **~3× faster**   |
| Random Deletes  | ~60K   | ~210K | **~3.5× faster** |
| Mixed Workload  | ~130K  | ~500K | **~4× faster**   |
| Bulk Insert     | ~240K  | ~880K | **~3.5× faster** |

> 📈 SNKV consistently delivers **significant gains in real-world KV scenarios**

---

## Why It’s Fast

SNKV interacts **directly with the storage engine**:

```
Application → KV API → B-Tree → Disk
```

No:

* SQL parsing
* Query compilation
* Virtual machine execution

---

## When to Use SNKV

Ideal for:

* Embedded systems
* High-performance services
* Configuration storage
* Metadata databases
* Session stores
* C/C++ applications
* Systems that **don’t require SQL**

---

## Philosophy

> **Use the right abstraction for the job.**

➡️ Fast, reliable, embedded key–value storage

---

## License

Apache License 2.0 © 2025 Hash Anu
