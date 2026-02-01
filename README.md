# SNKV — SQLite with No query processor Key Value store

## Overview

**SNKV** is a lightweight, high‑performance **key–value store** built directly on top of the **SQLite B‑Tree layer**.

Instead of using SQLite through SQL queries, SNKV **bypasses the entire SQL processing stack** and directly invokes SQLite’s **production‑ready B‑Tree APIs** to perform key–value operations.

The result is a database that retains **SQLite’s proven reliability and durability**, while being **44% faster for mix KV workloads (70% read, 20% write, 10% delete operations)** due to dramatically reduced overhead.

---

## Design Philosophy

SQLite is an excellent general‑purpose database, but for key–value workloads it carries significant overhead:

* SQL parsing and compilation
* Virtual machine execution (VDBE)
* Query optimization and schema management

SNKV removes these layers entirely and keeps only what is essential for a KV store.

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
