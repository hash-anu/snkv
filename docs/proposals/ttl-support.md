# Proposal: TTL (Time-To-Live) Support for SNKV

**Status:** RFC (Request for Comments)
**Author:** SNKV maintainers
**Created:** 2026-02-26
**Target version:** 0.3.0

---

## Table of Contents

- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Background](#background)
- [Design](#design)
  - [Storage Layout](#storage-layout)
  - [API](#api)
  - [Internals](#internals)
  - [Transparent Expiry on Get](#transparent-expiry-on-get)
  - [Purge](#purge)
  - [Atomicity](#atomicity)
  - [Column Family Interaction](#column-family-interaction)
- [Performance Analysis](#performance-analysis)
- [Error Handling](#error-handling)
- [Backward Compatibility](#backward-compatibility)
- [Test Plan](#test-plan)
- [Alternatives Considered](#alternatives-considered)
- [Open Questions](#open-questions)

---

## Motivation

Embedded key-value stores are commonly used for caching, session storage, rate limiting,
and other use cases where data has a natural expiry. Without TTL support, applications
must implement their own expiry logic — storing timestamps in values, running cleanup
loops, and handling race conditions around expiry. This is error-prone and results in
unbounded store growth when cleanup is missed.

Common use cases that require TTL:

- **Session store** — expire user sessions after inactivity timeout
- **Cache** — expire computed results after a freshness window
- **Rate limiting** — per-user counters that reset after a time window
- **OTP / verification tokens** — short-lived codes that must expire automatically
- **Distributed locks** — leases that expire if the holder crashes
- **Idempotency keys** — prevent duplicate requests within a time window

SNKV is positioned as an embedded, crash-safe alternative to SQLite for key-value
workloads. Adding native TTL makes it a complete solution for the above use cases
without requiring a separate caching layer.

---

## Goals

1. Per-key TTL — each key can have an independent expiry time or no TTL at all.
2. Transparent expiry on `get` — expired keys return `KVSTORE_NOTFOUND` automatically.
3. Efficient purge — `kvstore_purge_expired()` runs in O(expired_keys), not O(all_keys).
4. Atomic writes — `put_ttl` is atomic; a crash mid-write leaves no partial TTL state.
5. Zero overhead on non-TTL column families — CFs that never use TTL pay no cost.
6. Works with column families — TTL is supported on the default CF and all named CFs.
7. Works within transactions — `put_ttl` participates in explicit `kvstore_begin()` transactions.
8. Backward compatible — existing databases open and work correctly with no migration.

---

## Non-Goals

- Background expiry thread — expiry is lazy (checked on get) and batch (purge is caller-driven).
- Per-CF global TTL — no concept of a default TTL for all keys in a CF.
- Sub-second TTL precision — expiry is tracked in whole seconds (unix timestamp).
- Compaction-based cleanup — SNKV uses a B-tree, not an LSM tree; there is no compaction.
- Distributed TTL coordination — TTL is local to the database file.

---

## Background

### How RocksDB does it

RocksDB's `DBWithTTL` appends a 4-byte creation timestamp to every value, then uses a
compaction filter to drop expired keys during compaction. Expiry check happens on read
by stripping and inspecting the timestamp suffix.

This works well for LSM trees because compaction is continuous and automatic. For
SNKV's B-tree, there is no compaction — cleanup must be explicit.

### Design constraint: two access patterns with conflicting index needs

Efficient TTL requires two lookup patterns:

| Operation | Access pattern | Optimal index |
|---|---|---|
| `get(key)` — check expiry | Lookup by primary key | `key → expire_time` |
| `purge_expired()` — find all expired | Scan in expiry order | `expire_time → key` |

A single index cannot be optimal for both. This proposal uses two hidden companion
column families per data CF to satisfy both access patterns independently.

---

## Design

### Storage Layout

For each column family `<cf>` that has at least one TTL key, SNKV automatically
maintains two hidden companion CFs:

```
__snkv_ttl_k__<cf>     key index:    primary_key → 4-byte BE expire_unix_time
__snkv_ttl_e__<cf>     expiry index: [4-byte BE expire_unix_time][primary_key] → ""
```

**Key index** (`__snkv_ttl_k__<cf>`): maps primary key to its expiry timestamp.
Used by `get` to check in O(log n) whether a key is expired.

**Expiry index** (`__snkv_ttl_e__<cf>`): maps `[expire_time_BE][primary_key]` to an
empty value. Because the B-tree sorts lexicographically and the timestamp is big-endian,
all entries for the same expiry time are adjacent, and entries sort chronologically.
`purge_expired()` seeks to the beginning and scans forward, stopping at the first
non-expired entry — O(expired_keys).

**Main CF** (`<cf>`): unchanged. Values are stored as raw bytes with no embedded metadata.

**Hidden CFs** are excluded from `kvstore_cf_list()` output and cannot be opened by
name through the public API.

#### Example (key = `"session:abc"`, TTL = 3600s, current time = 1000000)

```
expire_time = 1000000 + 3600 = 1003600

main CF:
  "session:abc"  →  <session data bytes>

__snkv_ttl_k__default:
  "session:abc"  →  \x00\x0F\x4A\x90   (1003600 as 4-byte big-endian)

__snkv_ttl_e__default:
  \x00\x0F\x4A\x90 + "session:abc"  →  ""
```

---

### API

New functions added to `kvstore.h`:

```c
/*
** Insert or update a key-value pair with a TTL in the default column family.
**
** Parameters:
**   pKV        - KVStore handle
**   pKey       - Pointer to key data
**   nKey       - Length of key in bytes
**   pValue     - Pointer to value data
**   nValue     - Length of value in bytes
**   ttl        - Time-to-live in seconds (must be > 0)
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
**
** Note: If the key already exists (with or without TTL), it is overwritten.
**       The new TTL replaces any previous TTL.
*/
int kvstore_put_ttl(
  KVStore *pKV,
  const void *pKey,   int nKey,
  const void *pValue, int nValue,
  uint32_t ttl
);

/*
** Insert or update a key-value pair with a TTL in a specific column family.
*/
int kvstore_cf_put_ttl(
  KVColumnFamily *pCF,
  const void *pKey,   int nKey,
  const void *pValue, int nValue,
  uint32_t ttl
);

/*
** Get the remaining TTL for a key in the default column family.
**
** Parameters:
**   pKV          - KVStore handle
**   pKey         - Pointer to key data
**   nKey         - Length of key in bytes
**   pTtlRemaining - Output: remaining seconds, or -1 if key has no TTL,
**                   or 0 if expired (key will be deleted on next get)
**
** Returns:
**   KVSTORE_OK      on success (key exists)
**   KVSTORE_NOTFOUND if key does not exist
*/
int kvstore_ttl(
  KVStore *pKV,
  const void *pKey, int nKey,
  int64_t *pTtlRemaining
);

/*
** CF variant of kvstore_ttl.
*/
int kvstore_cf_ttl(
  KVColumnFamily *pCF,
  const void *pKey, int nKey,
  int64_t *pTtlRemaining
);

/*
** Delete all expired keys from the default column family.
**
** Scans the expiry index in chronological order and deletes all keys whose
** TTL has elapsed. Stops at the first non-expired key. This operation is
** O(expired_keys), not O(all_keys).
**
** Parameters:
**   pKV      - KVStore handle
**   pnPurged - Output: number of keys deleted (may be NULL)
**
** Returns:
**   KVSTORE_OK on success, error code otherwise
*/
int kvstore_purge_expired(KVStore *pKV, int *pnPurged);

/*
** CF variant of kvstore_purge_expired.
*/
int kvstore_cf_purge_expired(KVColumnFamily *pCF, int *pnPurged);
```

#### Behaviour of existing `kvstore_get` and `kvstore_delete`

**`kvstore_get` / `kvstore_cf_get`**: if the key has a TTL and the TTL has elapsed,
the key is lazily deleted from the main CF, key index, and expiry index, and
`KVSTORE_NOTFOUND` is returned. The deletion is atomic (single transaction).

**`kvstore_delete` / `kvstore_cf_delete`**: if the key has a TTL entry, the
corresponding entries in both hidden CFs are also deleted atomically. Callers do not
need to know whether a key has a TTL.

**`kvstore_put` / `kvstore_cf_put`**: if called on a key that previously had a TTL,
the TTL entries are removed atomically. The key becomes a permanent (non-expiring) key.

---

### Internals

#### Hidden CF lifecycle

Hidden companion CFs are created automatically on the first `put_ttl` call for a given
CF. They are never explicitly created by the caller.

A flag `hasTtl` is added to `KVColumnFamily`:

```c
struct KVColumnFamily {
  /* ... existing fields ... */
  int hasTtl;              /* 1 if any TTL key exists in this CF */
  KVColumnFamily *pTtlKeyCF;    /* __snkv_ttl_k__<name>, NULL if not yet created */
  KVColumnFamily *pTtlExpiryCF; /* __snkv_ttl_e__<name>, NULL if not yet created */
};
```

On `kvstore_cf_open` and `kvstore_open`, SNKV probes for the existence of hidden CFs
and populates `pTtlKeyCF` / `pTtlExpiryCF` if found. This allows TTL state to persist
across store close/reopen.

#### Expiry key encoding

```c
/* Encode expire_time as 4-byte big-endian, then append primary_key */
static void encodeTtlExpiryKey(
  uint32_t expire_time,
  const void *pKey, int nKey,
  unsigned char *pOut             /* caller provides nKey + 4 bytes */
){
  pOut[0] = (expire_time >> 24) & 0xFF;
  pOut[1] = (expire_time >> 16) & 0xFF;
  pOut[2] = (expire_time >>  8) & 0xFF;
  pOut[3] = (expire_time      ) & 0xFF;
  memcpy(pOut + 4, pKey, nKey);
}
```

---

### Transparent Expiry on Get

`kvstore_cf_get_internal` is modified as follows:

```
1. Seek primary key in main CF → if not found, return KVSTORE_NOTFOUND
2. If pCF->hasTtl == 0 → return value immediately (zero overhead path)
3. Look up primary key in __snkv_ttl_k__<cf> (key index)
4. If not found in key index → key has no TTL, return value
5. Decode expire_time from key index value
6. If time(NULL) < expire_time → not yet expired, return value
7. Key is expired:
   a. Begin write transaction (or join existing)
   b. Delete from main CF
   c. Delete from key index CF
   d. Delete from expiry index CF (seek by [expire_time][key])
   e. Commit
   f. Return KVSTORE_NOTFOUND
```

The `hasTtl == 0` fast path ensures **zero overhead** for CFs that never use TTL.

---

### Purge

`kvstore_cf_purge_expired` implementation:

```
1. If pCF->hasTtl == 0 → return immediately (nothing to purge)
2. Open iterator on __snkv_ttl_e__<cf> (expiry index), seek to first entry
3. now = time(NULL)
4. Begin write transaction
5. Loop:
   a. Read current expiry index key: [expire_time_BE][primary_key]
   b. Decode expire_time from first 4 bytes
   c. If expire_time > now → break (all remaining keys are in the future)
   d. Extract primary_key from remaining bytes
   e. Delete from main CF
   f. Delete from key index CF
   g. Delete current entry from expiry index CF
   h. Increment pnPurged
   i. Move iterator to next entry
6. Commit transaction
7. If pnPurged == 0, set pCF->hasTtl = 0 (all TTL keys purged)
```

The loop breaks at step (c) because the B-tree sorts expiry index entries
chronologically — once we see a non-expired entry, all subsequent entries are also
non-expired.

---

### Atomicity

Every `put_ttl` writes to three locations: main CF, key index CF, expiry index CF.
All three writes are performed inside a single btree transaction:

- If the caller has an active `kvstore_begin()` transaction, the writes join it.
- If not, an auto-transaction wraps the three writes atomically.

A crash after `put_ttl` commits leaves the database in one of two states:
- All three writes committed — consistent.
- None committed — consistent (key absent).

There is no partial state.

Similarly, lazy deletion on `get` and `kvstore_purge_expired` are transactional.

---

### Column Family Interaction

#### `kvstore_put` on a key that previously had TTL

When `kvstore_put` is called on a key that exists in the key index CF, the TTL
entries must be cleaned up atomically:

```
1. Look up key in __snkv_ttl_k__<cf>
2. If found:
   a. Decode expire_time
   b. Delete from key index CF
   c. Delete [expire_time][key] from expiry index CF
3. Write key → value to main CF
4. All in one transaction
```

This ensures `put` always results in a permanent key regardless of prior TTL state.

#### `kvstore_delete` on a TTL key

```
1. Delete from main CF (existing logic)
2. If pCF->hasTtl:
   a. Look up key in __snkv_ttl_k__<cf>
   b. If found: delete from key index CF and expiry index CF
3. All in one transaction
```

#### `kvstore_cf_drop`

Dropping a CF also drops its hidden companion CFs if they exist.

---

## Performance Analysis

| Operation | Without TTL (existing) | With TTL (this proposal) |
|---|---|---|
| `put` (no TTL) | 1 B-tree insert | 1 B-tree insert + 1 key index lookup (lazy) |
| `put_ttl` | N/A | 3 B-tree inserts, 1 transaction |
| `get` (CF no TTL) | 1 B-tree seek | 1 B-tree seek (hasTtl==0 fast path, zero overhead) |
| `get` (CF has TTL, key no TTL) | N/A | 1 B-tree seek (main) + 1 B-tree seek (key index) |
| `get` (CF has TTL, key has TTL, not expired) | N/A | 2 B-tree seeks |
| `get` (expired key) | N/A | 2 seeks + 3 deletes + 1 transaction |
| `delete` (no TTL) | 1 B-tree delete | 1 B-tree delete + 1 key index lookup |
| `delete` (TTL key) | N/A | 1 delete (main) + 2 deletes (TTL CFs), 1 transaction |
| `purge_expired` | N/A | O(expired_keys) — iterator scan stopping at first non-expired |

**Space overhead per TTL key:**
- Key index entry: `len(primary_key) + 4` bytes (key) + overhead
- Expiry index entry: `4 + len(primary_key)` bytes (key) + overhead
- Roughly 2× the primary key size in additional storage per TTL key.

---

## Error Handling

| Condition | Behaviour |
|---|---|
| `ttl == 0` | Return `KVSTORE_ERROR` — zero TTL is ambiguous (expired immediately vs no TTL). |
| `ttl` would overflow uint32 | Return `KVSTORE_ERROR`. |
| DB is read-only | `put_ttl` returns `KVSTORE_READONLY`. |
| DB is corrupted | Returns `KVSTORE_CORRUPT`, same as all other operations. |
| Lazy delete fails on `get` | Value is not returned. Error is set. Key remains until next purge. |
| Hidden CF creation fails | `put_ttl` returns the error. No partial state written. |

---

## Backward Compatibility

- Existing databases (no TTL keys) open and work identically. No migration needed.
- Hidden CFs are named with a reserved `__snkv_` prefix. This prefix is documented
  as reserved; user-created CFs must not start with `__snkv_`.
- Existing `put` / `get` / `delete` / `exists` behaviour is unchanged for keys without TTL.
- The `hasTtl == 0` fast path means zero performance regression for existing workloads.

---

## Test Plan

### Unit tests (`tests/test_ttl.c`)

| Test | Verifies |
|---|---|
| basic_put_get_expiry | put_ttl → get before expiry (found) → sleep → get after expiry (not found) |
| lazy_delete_on_get | Confirm expired key is removed from all 3 CFs after get |
| put_overwrites_ttl | put_ttl → put (no TTL) → confirm key is permanent |
| put_ttl_overwrites_put | put → put_ttl → confirm TTL is applied |
| put_ttl_change | put_ttl(60s) → put_ttl(120s) → confirm new TTL applies |
| delete_ttl_key | put_ttl → delete → confirm all 3 CF entries removed |
| purge_empty | purge_expired on CF with no TTL keys → returns 0 purged |
| purge_some_expired | Mix of expired and non-expired keys → only expired purged |
| purge_all_expired | All keys expired → all purged, hasTtl reset to 0 |
| purge_order | Confirm purge stops at first non-expired (does not scan full CF) |
| ttl_remaining | kvstore_ttl returns correct remaining seconds |
| ttl_no_ttl_key | kvstore_ttl on key without TTL returns -1 |
| ttl_expired_key | kvstore_ttl on expired key returns 0 |
| ttl_nonexistent | kvstore_ttl on missing key returns KVSTORE_NOTFOUND |
| transaction_put_ttl | put_ttl inside explicit begin/commit — atomic |
| transaction_rollback | put_ttl inside rollback → key absent, TTL CFs clean |
| cf_ttl | put_ttl on named CF → get → expiry → purge |
| cf_drop_cleans_ttl | drop CF → hidden TTL CFs are also dropped |
| error_zero_ttl | put_ttl with ttl=0 returns KVSTORE_ERROR |
| reopen_persistence | put_ttl → close → reopen → TTL still enforced |
| no_overhead_non_ttl_cf | CF with no TTL keys: get has no key index lookup |

### Integration tests

- Crash recovery: put_ttl → crash before commit → reopen → key absent
- Crash recovery: put_ttl → crash after commit → reopen → key present with TTL
- Mixed workload: concurrent puts, put_ttls, gets, purges under mutex
- Large dataset: 100k TTL keys, purge_expired, verify count and correctness

---

## Alternatives Considered

### Alternative 1: Embed timestamp in value (RocksDB style)

Store `[4-byte expire_time][original_value]` in the main CF for TTL keys.

**Rejected because:**
- Changes value format — callers who call `get` on a TTL key receive a value with a
  4-byte prefix they did not write. Requires all callers to be aware of TTL encoding.
- Purge requires a full CF scan (O(all_keys)), not O(expired_keys).
- No clean way to distinguish TTL keys from non-TTL keys without an extra flag byte.

### Alternative 2: Single TTL CF sorted by expiry only

One hidden CF: `[expire_time][key] → ""`. No key index CF.

**Rejected because:**
- `get` cannot check expiry in O(log n) — would require a seek with unknown expire_time.
- Either `get` does no expiry check (bad UX) or it does a full scan of the TTL CF (O(n)).

### Alternative 3: In-memory expiry map

Keep `key → expire_time` in a hash map in memory, persist only the expiry index CF.

**Rejected because:**
- Does not survive process restart — in-memory map is lost on close.
- Requires rebuilding map from disk on open — O(all TTL keys) scan on every open.
- Inconsistent: a crash between map update and disk write leaves inconsistent state.

### Alternative 4: Single value-embedded flag byte

Add a 1-byte flag to every value: `\x00` = no TTL, `\x01` = has TTL followed by 4-byte timestamp.

**Rejected because:**
- Adds overhead (1 byte minimum) to every key in every CF, even non-TTL.
- Purge still requires full scan.
- Breaking change for existing stored data.

---

## Open Questions

Community feedback is welcome on the following:

**Q1. Should `kvstore_get` perform transparent expiry, or should callers explicitly check TTL?**

The current design does transparent expiry (expired keys return `KVSTORE_NOTFOUND`
automatically). The alternative is: `get` always returns the value, and callers call
`kvstore_ttl()` to check remaining life. Transparent expiry is more ergonomic but adds
one extra B-tree seek per get on TTL-enabled CFs.

**Q2. Should `purge_expired` be transactional in bulk or per-key?**

Current design commits all deletes in one transaction. For very large expired sets
(millions of keys), this holds a write transaction open for a long time. An alternative
is to commit every N deletions. What is the right N, or should it be configurable?

**Q3. Should there be a `kvstore_put_ttl_abs` that takes an absolute unix timestamp?**

Some use cases (e.g., session expiry set by a remote auth server) have a fixed expiry
time rather than a relative TTL. Should we add a variant that takes an absolute
`uint32_t expire_at` instead of `uint32_t ttl_seconds`?

**Q4. Should TTL precision be seconds (uint32) or milliseconds (uint64)?**

Seconds are sufficient for session/cache use cases. Milliseconds add 4 bytes per key
and are rarely needed for TTL (rate limiting at ms granularity is unusual for an
embedded store). Current proposal uses seconds.

**Q5. Should `kvstore_purge_expired` accept a limit parameter?**

```c
kvstore_purge_expired(pKV, int max_keys, int *pnPurged);
/* 0 = no limit */
```

This allows callers to bound the write transaction size and call purge in a loop.
Useful for production systems that cannot afford a long write stall.

---

## Implementation Checklist

- [ ] Add `hasTtl`, `pTtlKeyCF`, `pTtlExpiryCF` to `KVColumnFamily` struct
- [ ] Add `kvstoreGetOrCreateTtlCFs()` internal helper
- [ ] Add `kvstoreHiddenCfName()` — builds `__snkv_ttl_k__<name>` and `__snkv_ttl_e__<name>`
- [ ] Implement `kvstore_cf_put_ttl_internal()`
- [ ] Modify `kvstore_cf_get_internal()` — add expiry check on `hasTtl` path
- [ ] Modify `kvstore_cf_put_internal()` — clean up TTL entries when overwriting a TTL key
- [ ] Modify `kvstore_cf_delete_internal()` — clean up TTL entries
- [ ] Modify `kvstore_cf_drop()` — drop hidden TTL CFs
- [ ] Modify `kvstore_cf_list()` — exclude hidden CFs
- [ ] Implement `kvstore_cf_purge_expired_internal()`
- [ ] Implement `kvstore_cf_ttl_internal()`
- [ ] Add all public API functions with default-CF wrappers
- [ ] Write `tests/test_ttl.c`
- [ ] Update `API_SPECIFICATION.md`
- [ ] Update `python/snkv/__init__.py` and `python/snkv_module.c` for Python bindings

---

*Feedback welcome via GitHub Discussions or as comments on the PR that introduced this file.*
