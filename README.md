# SNKV — SQLite with No query processor Key Value store

## Overview

**SNKV** is a lightweight, high‑performance **ACID compliant key–value store** built directly on top of the **SQLite B‑Tree layer**.

Instead of using SQLite through SQL queries, SNKV **bypasses the entire SQL processing stack** and directly invokes SQLite’s **production‑ready B‑Tree APIs** to perform key–value operations.

The result is a database that retains **SQLite’s proven reliability and durability**, while being **~50% faster for mix KV workloads (70% read, 20% write, 10% delete operations)** due to dramatically reduced overhead.

---

## Design Philosophy

SQLite is an excellent general‑purpose database, but for key–value workloads it carries significant overhead:

* SQL parsing and compilation
* Virtual machine execution (VDBE)
* Query optimization and schema management

SNKV removes these layers entirely and keeps only what is essential for a KV store.

## Usage

SNKV exposes a simple C API for key–value operations without any SQL involvement.

A minimal end-to-end usage example is provided in: snkv/main.c

This file demonstrates:

* Opening a database
* Creating a column family
* Put / Get / Delete operations
* Transaction operations
* Proper cleanup and shutdown

## Tests

All unit tests and benchmarks are located in the tests/ directory.

---

## Visual Architecture Comparison

### The Stack We Removed

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         The Stack We REMOVED                                │
└─────────────────────────────────────────────────────────────────────────────┘

        REMOVED from SQLite                 Why It's Not Needed for SNKV
        ═══════════════════════             ═══════════════════════════

┌──────────────────────────┐
│   SQL Interface Layer     │                No SQL → Not required
│   - sqlite3_prepare()    │
│   - sqlite3_step()       │
│   - sqlite3_bind_*()     │
└──────────────────────────┘
           │
           ▼
┌──────────────────────────┐
│     SQL Compiler         │                No SQL → No parsing or codegen
│   - Tokenizer            │
│   - Parser               │
│   - Code Generator       │
└──────────────────────────┘
           │
           ▼
┌──────────────────────────┐
│   Virtual Machine        │                No bytecode execution
│   - VDBE Executor        │
│   - Opcode Interpreter   │
│   - 200+ opcodes         │
└──────────────────────────┘
           │
           ▼
┌──────────────────────────┐
│    Backend Layer         │                No schema or query planning
│   - Query Optimizer      │
│   - Index Manager        │
│   - Schema Manager       │
└──────────────────────────┘
           │
           ▼
    ═══════════════════════════════════════════════════
```

---

## What SNKV Keeps (Unchanged)

SNKV intentionally keeps the **most battle‑tested parts of SQLite**, unchanged:

```
┌──────────────────────────┐                ┌──────────────────────────┐
│      B-Tree Engine       │   ══════════▶  │      B-Tree Engine       │
│   (SQLite proven code)   │   KEPT THIS    │   (Same proven code)     │
└──────────────────────────┘                └──────────────────────────┘
           │                                             │
           ▼                                             ▼
┌──────────────────────────┐                ┌──────────────────────────┐
│      Pager Module        │   ══════════▶  │      Pager Module        │
│   (Cache, Journaling)    │   KEPT THIS    │   (Same code)            │
└──────────────────────────┘                └──────────────────────────┘
           │                                             │
           ▼                                             ▼
┌──────────────────────────┐                ┌──────────────────────────┐
│      OS Interface        │   ══════════▶  │      OS Interface        │
│   (File I/O, Locks)      │   KEPT THIS    │   (Same code)            │
└──────────────────────────┘                └──────────────────────────┘
```

This means SNKV benefits from:

* Crash safety (rollback journal)
* Atomic commits
* Page cache and efficient I/O
* Real‑world tested

---

## Result

**Same reliability, fewer layers, significantly faster KV performance.**

* No SQL parsing or planning
* No virtual machine execution
* Direct B‑Tree access

Typical gains:

* Lower memory usage
* Predictable latency

---

## Comparison with Latest SQLite
The following table shows the **average performance across 5 runs** for both **SQLite (SQL-based KV access)** and **SNKV (direct B-Tree KV access)** using identical workloads (50,000 records).

All numbers are **operations per second (ops/sec)**.

| Benchmark                    | SQLite (avg) | SNKV (avg) | Winner          |
| ---------------------------- | ------------ | ---------- | --------------- |
| Sequential Writes            | 68,503       | 70,888     | SNKV (+3.5%)    |
| Random Reads                 | 48,206       | 36,210     | SQLite (+33%)   |
| Sequential Scan              | 1,089,049    | 2,173,141  | SNKV (\~2×)     |
| Random Updates               | 47,339       | 47,297     | Tie             |
| Random Deletes               | 31,937       | 44,046     | SNKV (+38%)     |
| Exists Checks                | 59,884       | 36,041     | SQLite (+66%)   |
| Mixed Workload (70R/20W/10D) | 50,379       | 78,860     | **SNKV (+56%)** |
| Bulk Insert (single txn)     | 104,526      | 133,566    | SNKV (+28%)     |

### Key Observations

- **SNKV dominates write-heavy and mixed workloads** due to zero SQL/VDBE overhead
- **Sequential scans are \~2× faster** in SNKV due to direct cursor traversal
- **SQLite wins point-lookups (reads / exists)** because of its highly optimized VDBE fast paths and statement caching
- Update performance is effectively identical (same B-Tree + Pager path)

### Benchmark Code

- SQLite benchmark source: [https://github.com/hash-anu/sqlite-benchmark-kv](https://github.com/hash-anu/sqlite-benchmark-kv)
- SNKV benchmark source: `snkv/tests/test_benchmark.c`

## When to Use SNKV

SNKV is ideal for:

* Embedded systems
* Low‑memory environments
* Configuration stores
* Metadata databases
* C/C++ applications needing fast KV access
* Systems that do **not** need SQL

If you need joins, ad‑hoc queries, or analytics — use SQLite.
If you need **fast, reliable key–value storage** — use SNKV.

---

## Summary

SNKV proves a simple idea:

> *If you don’t need SQL, don’t pay for it.*

By standing directly on SQLite’s B‑Tree engine, SNKV delivers a focused, fast, and reliable key–value database with minimal complexity.
