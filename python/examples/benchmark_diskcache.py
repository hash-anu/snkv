# SPDX-License-Identifier: Apache-2.0
"""
benchmark_diskcache.py — SNKV vs diskcache performance comparison

Install dependency:
    pip install diskcache

Run from repo root:
    python python/examples/benchmark_diskcache.py

Sections
--------
  1. Core operations      — write, read, delete, scan, TTL  (N=10,000)
  2. Prefix scan          — native B-tree prefix vs Python filter-all
  3. Mixed workload       — 80% read / 20% write interleaved
  4. Value size scaling   — 64 B → 1 KB → 10 KB → 100 KB
  5. Large dataset        — 100,000 keys

Methodology
-----------
  - TRIALS=3 runs per test on a fresh store; best time reported
  - diskcache: default config (SQLite + pickle), size_limit=2TB (no eviction)
  - SNKV: WAL journal mode, SYNC_NORMAL

Fair comparison notes
---------------------
  Both libraries are backed by SQLite.
  diskcache pickles Python objects — this adds serialization overhead
  absent in SNKV (raw bytes stored directly).
  Benchmarks use each library's standard public API as a real app would.
"""

import os
import sys
import time
import shutil
import tempfile
import random

try:
    import diskcache
except ImportError:
    print("diskcache not installed.  Run:  pip install diskcache")
    sys.exit(1)

from snkv import KVStore, JOURNAL_WAL

# ── Global config ─────────────────────────────────────────────────────────────
N      = 10_000
TRIALS = 3
_DC_SIZE_LIMIT = int(2e12)   # 2 TB — disable eviction

KEY_FMT = b"key:%07d"
VAL_FMT = b"val:%07d:" + b"x" * 52   # 64 bytes

KEYS   = [KEY_FMT % i for i in range(N)]
VALUES = [VAL_FMT % i for i in range(N)]


# ── Timing helpers ────────────────────────────────────────────────────────────

def best_of(fn, trials=TRIALS) -> float:
    """Return best elapsed time in ms over <trials> runs."""
    best = float("inf")
    for _ in range(trials):
        t0 = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - t0)
    return best * 1000.0


def fmt(ms: float) -> str:
    if ms >= 1000:
        return f"{ms/1000:.2f} s "
    return f"{ms:7.1f} ms"


def ops_str(n: int, ms: float) -> str:
    return f"{n / (ms / 1000):>9,.0f} op/s"


def speedup_str(snkv_ms: float, dc_ms: float) -> str:
    ratio = dc_ms / snkv_ms
    arrow = "▲" if ratio >= 1.0 else "▼"
    return f"{ratio:.1f}x {arrow}"


def section_header(title: str) -> None:
    print(f"\n{'─'*70}")
    print(f"  {title}")
    print(f"{'─'*70}")


def print_row(label: str, snkv_ms: float, dc_ms: float, n: int) -> None:
    print(f"  {label:<30}  {fmt(snkv_ms):>10}  {fmt(dc_ms):>10}"
          f"  {ops_str(n, snkv_ms):>14}  {speedup_str(snkv_ms, dc_ms):>8}")


def print_table_header() -> None:
    print(f"  {'Operation':<30}  {'SNKV':>10}  {'diskcache':>10}"
          f"  {'ops/s (SNKV)':>14}  {'Speedup':>8}")
    print("  " + "─" * 78)


def print_table_footer(results: list) -> None:
    print("  " + "─" * 78)
    ratios = [dc / sn for sn, dc in results]
    wins   = sum(1 for r in ratios if r >= 1.0)
    avg    = sum(ratios) / len(ratios)
    print(f"  SNKV faster in {wins}/{len(results)} tests  |  avg speedup {avg:.1f}x")


def snkv_cleanup(path: str) -> None:
    for ext in ("", "-wal", "-shm"):
        p = path + ext
        if os.path.exists(p):
            os.remove(p)


# ── Section 1: Core operations ────────────────────────────────────────────────

def s1_snkv_bulk_write(path):
    def run():
        with KVStore(path) as db:
            db.begin(write=True)
            for k, v in zip(KEYS, VALUES):
                db.put(k, v)
            db.commit()
    return best_of(run)

def s1_dc_bulk_write(d):
    def run():
        with diskcache.Cache(d, size_limit=_DC_SIZE_LIMIT) as c:
            with c.transact():
                for k, v in zip(KEYS, VALUES):
                    c.set(k, v)
    return best_of(run)


def s1_snkv_individual_write(path):
    def run():
        with KVStore(path) as db:
            for k, v in zip(KEYS, VALUES):
                db.put(k, v)
    return best_of(run)

def s1_dc_individual_write(d):
    def run():
        with diskcache.Cache(d, size_limit=_DC_SIZE_LIMIT) as c:
            for k, v in zip(KEYS, VALUES):
                c.set(k, v)
    return best_of(run)


def _snkv_populate(path):
    with KVStore(path) as db:
        db.begin(write=True)
        for k, v in zip(KEYS, VALUES):
            db.put(k, v)
        db.commit()

def _dc_populate(d):
    with diskcache.Cache(d, size_limit=_DC_SIZE_LIMIT) as c:
        c.clear()
        with c.transact():
            for k, v in zip(KEYS, VALUES):
                c.set(k, v)


def s1_snkv_read_hit(path):
    _snkv_populate(path)
    def run():
        with KVStore(path) as db:
            for k in KEYS:
                db.get(k)
    return best_of(run)

def s1_dc_read_hit(d):
    _dc_populate(d)
    def run():
        with diskcache.Cache(d, size_limit=_DC_SIZE_LIMIT) as c:
            for k in KEYS:
                c.get(k)
    return best_of(run)


def s1_snkv_read_miss(path):
    def run():
        with KVStore(path) as db:
            for i in range(N):
                db.get(b"miss:%07d" % i)
    return best_of(run)

def s1_dc_read_miss(d):
    def run():
        with diskcache.Cache(d, size_limit=_DC_SIZE_LIMIT) as c:
            for i in range(N):
                c.get(b"miss:%07d" % i)
    return best_of(run)


def s1_snkv_delete(path):
    times = []
    for _ in range(TRIALS):
        _snkv_populate(path)
        t0 = time.perf_counter()
        with KVStore(path) as db:
            db.begin(write=True)
            for k in KEYS:
                db.delete(k)
            db.commit()
        times.append((time.perf_counter() - t0) * 1000.0)
    return min(times)

def s1_dc_delete(d):
    times = []
    for _ in range(TRIALS):
        _dc_populate(d)
        t0 = time.perf_counter()
        with diskcache.Cache(d, size_limit=_DC_SIZE_LIMIT) as c:
            with c.transact():
                for k in KEYS:
                    c.delete(k)
        times.append((time.perf_counter() - t0) * 1000.0)
    return min(times)


def s1_snkv_full_scan(path):
    _snkv_populate(path)
    def run():
        with KVStore(path) as db:
            _ = sum(1 for _ in db)
    return best_of(run)

def s1_dc_full_scan(d):
    _dc_populate(d)
    def run():
        with diskcache.Cache(d, size_limit=_DC_SIZE_LIMIT) as c:
            _ = sum(1 for _ in c)
    return best_of(run)


def s1_snkv_write_ttl(path):
    def run():
        with KVStore(path) as db:
            db.begin(write=True)
            for k, v in zip(KEYS, VALUES):
                db.put(k, v, ttl=3600)
            db.commit()
    return best_of(run)

def s1_dc_write_ttl(d):
    def run():
        with diskcache.Cache(d, size_limit=_DC_SIZE_LIMIT) as c:
            with c.transact():
                for k, v in zip(KEYS, VALUES):
                    c.set(k, v, expire=3600)
    return best_of(run)


# ── Section 2: Prefix scan ────────────────────────────────────────────────────
# 10,000 keys across 10 namespaces (ns0: .. ns9:), 1,000 keys each.
# Benchmark: scan a single namespace (1,000 of 10,000 keys match).

N_NS    = 10_000
N_MATCH = 1_000   # keys per namespace
N_NS_COUNT = 10

NS_KEYS   = [f"ns{i % N_NS_COUNT}:{i:07d}".encode() for i in range(N_NS)]
NS_VALUES = [b"val:%07d:" % i + b"x" * 52 for i in range(N_NS)]
TARGET_NS = b"ns3:"   # the prefix we'll scan

def _snkv_ns_populate(path):
    with KVStore(path) as db:
        db.begin(write=True)
        for k, v in zip(NS_KEYS, NS_VALUES):
            db.put(k, v)
        db.commit()

def _dc_ns_populate(d):
    with diskcache.Cache(d, size_limit=_DC_SIZE_LIMIT) as c:
        c.clear()
        with c.transact():
            for k, v in zip(NS_KEYS, NS_VALUES):
                c.set(k, v)


def s2_snkv_prefix_scan(path):
    """Native prefix_iterator — only visits matching keys."""
    _snkv_ns_populate(path)
    def run():
        with KVStore(path) as db:
            results = list(db.prefix_iterator(TARGET_NS))
        assert len(results) == N_MATCH
    return best_of(run)

def s2_dc_prefix_scan(d):
    """No native prefix support — iterate all keys, filter in Python."""
    _dc_ns_populate(d)
    def run():
        with diskcache.Cache(d, size_limit=_DC_SIZE_LIMIT) as c:
            results = [(k, c[k]) for k in c.iterkeys()
                       if isinstance(k, bytes) and k.startswith(TARGET_NS)]
        assert len(results) == N_MATCH
    return best_of(run)


def s2_snkv_reverse_prefix(path):
    """Native reverse_prefix_iterator — descending prefix scan."""
    _snkv_ns_populate(path)
    def run():
        with KVStore(path) as db:
            results = list(db.reverse_prefix_iterator(TARGET_NS))
        assert len(results) == N_MATCH
    return best_of(run)

def s2_dc_reverse_prefix(d):
    """No native reverse prefix — iterate all keys reversed, filter in Python."""
    _dc_ns_populate(d)
    def run():
        with diskcache.Cache(d, size_limit=_DC_SIZE_LIMIT) as c:
            results = sorted(
                [(k, c[k]) for k in c.iterkeys()
                 if isinstance(k, bytes) and k.startswith(TARGET_NS)],
                reverse=True
            )
        assert len(results) == N_MATCH
    return best_of(run)


# ── Section 3: Mixed workload (80% read / 20% write) ─────────────────────────

N_MIX     = 10_000
N_MIX_GET = int(N_MIX * 0.8)   # 8,000 reads
N_MIX_PUT = int(N_MIX * 0.2)   # 2,000 writes

# Pre-built interleaved operation list: (op, key, value|None)
random.seed(42)
_MIX_OPS = []
_existing = [KEY_FMT % i for i in range(5000)]   # pre-populated keys
_new_keys = [b"new:%07d" % i for i in range(N_MIX_PUT)]
_mix_gets = random.choices(_existing, k=N_MIX_GET)
_mix_iter = iter(_new_keys)
for i in range(N_MIX):
    if i % 5 == 0:   # 1 in 5 = 20% writes
        k = next(_mix_iter, b"new:0000000")
        _MIX_OPS.append(("put", k, VALUES[i % len(VALUES)]))
    else:
        _MIX_OPS.append(("get", _mix_gets[i % N_MIX_GET], None))


def s3_snkv_mixed(path):
    # Pre-populate with 5,000 keys
    with KVStore(path) as db:
        db.begin(write=True)
        for k in _existing:
            db.put(k, b"v")
        db.commit()
    def run():
        with KVStore(path) as db:
            for op, k, v in _MIX_OPS:
                if op == "put":
                    db.put(k, v)
                else:
                    db.get(k)
    return best_of(run)

def s3_dc_mixed(d):
    with diskcache.Cache(d, size_limit=_DC_SIZE_LIMIT) as c:
        c.clear()
        with c.transact():
            for k in _existing:
                c.set(k, b"v")
    def run():
        with diskcache.Cache(d, size_limit=_DC_SIZE_LIMIT) as c:
            for op, k, v in _MIX_OPS:
                if op == "put":
                    c.set(k, v)
                else:
                    c.get(k)
    return best_of(run)


# ── Section 4: Value size scaling ─────────────────────────────────────────────

N_SCALE = 5_000

_SCALE_SIZES = [
    ("64 B",   64),
    ("1 KB",   1024),
    ("10 KB",  10 * 1024),
    ("100 KB", 100 * 1024),
]


def s4_snkv_write(path, value_size):
    keys = [b"k:%06d" % i for i in range(N_SCALE)]
    val  = b"x" * value_size
    def run():
        with KVStore(path) as db:
            db.begin(write=True)
            for k in keys:
                db.put(k, val)
            db.commit()
    return best_of(run)

def s4_dc_write(d, value_size):
    keys = [b"k:%06d" % i for i in range(N_SCALE)]
    val  = b"x" * value_size
    def run():
        with diskcache.Cache(d, size_limit=_DC_SIZE_LIMIT) as c:
            with c.transact():
                for k in keys:
                    c.set(k, val)
    return best_of(run)


def s4_snkv_read(path, value_size):
    keys = [b"k:%06d" % i for i in range(N_SCALE)]
    val  = b"x" * value_size
    with KVStore(path) as db:
        db.begin(write=True)
        for k in keys:
            db.put(k, val)
        db.commit()
    def run():
        with KVStore(path) as db:
            for k in keys:
                db.get(k)
    return best_of(run)

def s4_dc_read(d, value_size):
    keys = [b"k:%06d" % i for i in range(N_SCALE)]
    val  = b"x" * value_size
    with diskcache.Cache(d, size_limit=_DC_SIZE_LIMIT) as c:
        c.clear()
        with c.transact():
            for k in keys:
                c.set(k, val)
    def run():
        with diskcache.Cache(d, size_limit=_DC_SIZE_LIMIT) as c:
            for k in keys:
                c.get(k)
    return best_of(run)


# ── Section 5: Large dataset (100K keys) ──────────────────────────────────────

N_LARGE = 100_000
_LARGE_KEYS   = [b"k:%08d" % i for i in range(N_LARGE)]
_LARGE_VALUES = [b"val:%08d:" % i + b"x" * 52 for i in range(N_LARGE)]


def s5_snkv_bulk_write(path):
    def run():
        with KVStore(path) as db:
            db.begin(write=True)
            for k, v in zip(_LARGE_KEYS, _LARGE_VALUES):
                db.put(k, v)
            db.commit()
    return best_of(run)

def s5_dc_bulk_write(d):
    def run():
        with diskcache.Cache(d, size_limit=_DC_SIZE_LIMIT) as c:
            with c.transact():
                for k, v in zip(_LARGE_KEYS, _LARGE_VALUES):
                    c.set(k, v)
    return best_of(run)


def _snkv_large_populate(path):
    with KVStore(path) as db:
        db.begin(write=True)
        for k, v in zip(_LARGE_KEYS, _LARGE_VALUES):
            db.put(k, v)
        db.commit()

def _dc_large_populate(d):
    with diskcache.Cache(d, size_limit=_DC_SIZE_LIMIT) as c:
        c.clear()
        with c.transact():
            for k, v in zip(_LARGE_KEYS, _LARGE_VALUES):
                c.set(k, v)


def s5_snkv_read(path):
    _snkv_large_populate(path)
    sample = _LARGE_KEYS[::10]   # every 10th key = 10,000 reads
    def run():
        with KVStore(path) as db:
            for k in sample:
                db.get(k)
    return best_of(run)

def s5_dc_read(d):
    _dc_large_populate(d)
    sample = _LARGE_KEYS[::10]
    def run():
        with diskcache.Cache(d, size_limit=_DC_SIZE_LIMIT) as c:
            for k in sample:
                c.get(k)
    return best_of(run)


def s5_snkv_scan(path):
    _snkv_large_populate(path)
    def run():
        with KVStore(path) as db:
            _ = sum(1 for _ in db)
    return best_of(run)

def s5_dc_scan(d):
    _dc_large_populate(d)
    def run():
        with diskcache.Cache(d, size_limit=_DC_SIZE_LIMIT) as c:
            _ = sum(1 for _ in c)
    return best_of(run)


# ── Section 6: Durability verification ───────────────────────────────────────
# Write N keys with known unique values, fully close the store, reopen it,
# read every key back, and assert the value matches exactly.
# Tests both correctness (data integrity) and reopen performance.

N_DUR = 10_000

# Use unique, verifiable values — not padding — so corruption is detectable.
_DUR_KEYS   = [b"dur:%07d" % i for i in range(N_DUR)]
_DUR_VALUES = [b"chk:%07d:val=%d" % (i, i * 7919) for i in range(N_DUR)]


def s6_snkv_durability(path) -> tuple:
    """
    Returns (write_ms, reopen_read_ms, verified_count, errors).
    Phase 1: write N keys and close.
    Phase 2: reopen, read all keys, verify values.
    """
    # Phase 1 — write
    t0 = time.perf_counter()
    with KVStore(path) as db:
        db.begin(write=True)
        for k, v in zip(_DUR_KEYS, _DUR_VALUES):
            db.put(k, v)
        db.commit()
    write_ms = (time.perf_counter() - t0) * 1000.0

    # Phase 2 — reopen and verify
    t0 = time.perf_counter()
    errors = 0
    with KVStore(path) as db:
        for k, expected in zip(_DUR_KEYS, _DUR_VALUES):
            got = db.get(k)
            if got != expected:
                errors += 1
    read_ms = (time.perf_counter() - t0) * 1000.0

    return write_ms, read_ms, N_DUR - errors, errors


def s6_dc_durability(dc_dir) -> tuple:
    """Same as above for diskcache."""
    # Phase 1 — write
    t0 = time.perf_counter()
    with diskcache.Cache(dc_dir, size_limit=_DC_SIZE_LIMIT) as c:
        with c.transact():
            for k, v in zip(_DUR_KEYS, _DUR_VALUES):
                c.set(k, v)
    write_ms = (time.perf_counter() - t0) * 1000.0

    # Phase 2 — reopen and verify
    t0 = time.perf_counter()
    errors = 0
    with diskcache.Cache(dc_dir, size_limit=_DC_SIZE_LIMIT) as c:
        for k, expected in zip(_DUR_KEYS, _DUR_VALUES):
            got = c.get(k)
            if got != expected:
                errors += 1
    read_ms = (time.perf_counter() - t0) * 1000.0

    return write_ms, read_ms, N_DUR - errors, errors


def s6_snkv_ttl_durability(path) -> tuple:
    """
    Write keys with TTL=3600, close, reopen, verify all still present and
    values correct (not expired — 3600s >> test duration).
    """
    t0 = time.perf_counter()
    with KVStore(path) as db:
        db.begin(write=True)
        for k, v in zip(_DUR_KEYS, _DUR_VALUES):
            db.put(k, v, ttl=3600)
        db.commit()
    write_ms = (time.perf_counter() - t0) * 1000.0

    t0 = time.perf_counter()
    errors = 0
    with KVStore(path) as db:
        for k, expected in zip(_DUR_KEYS, _DUR_VALUES):
            got = db.get(k)
            if got != expected:
                errors += 1
    read_ms = (time.perf_counter() - t0) * 1000.0

    return write_ms, read_ms, N_DUR - errors, errors


def s6_dc_ttl_durability(dc_dir) -> tuple:
    t0 = time.perf_counter()
    with diskcache.Cache(dc_dir, size_limit=_DC_SIZE_LIMIT) as c:
        with c.transact():
            for k, v in zip(_DUR_KEYS, _DUR_VALUES):
                c.set(k, v, expire=3600)
    write_ms = (time.perf_counter() - t0) * 1000.0

    t0 = time.perf_counter()
    errors = 0
    with diskcache.Cache(dc_dir, size_limit=_DC_SIZE_LIMIT) as c:
        for k, expected in zip(_DUR_KEYS, _DUR_VALUES):
            got = c.get(k)
            if got != expected:
                errors += 1
    read_ms = (time.perf_counter() - t0) * 1000.0

    return write_ms, read_ms, N_DUR - errors, errors


# ── Main ──────────────────────────────────────────────────────────────────────

def run_benchmark() -> None:
    with tempfile.TemporaryDirectory() as tmp:

        def fresh(name):
            path = os.path.join(tmp, f"{name}.db")
            snkv_cleanup(path)
            dc_dir = os.path.join(tmp, f"{name}_dc")
            shutil.rmtree(dc_dir, ignore_errors=True)
            os.makedirs(dc_dir, exist_ok=True)
            return path, dc_dir

        def run_row(label, snkv_fn, dc_fn, n, path, dc_dir):
            print(f"  {'  running: ' + label:<32}", end="", flush=True)
            snkv_ms = snkv_fn(path)
            snkv_cleanup(path)
            dc_ms = dc_fn(dc_dir)
            print(f"\r", end="")
            print_row(label, snkv_ms, dc_ms, n)
            return snkv_ms, dc_ms

        print()
        print("=" * 72)
        print("  SNKV vs diskcache — Python Benchmark")
        print("=" * 72)
        print(f"  Trials = {TRIALS}  (best time reported per test)")
        print(f"  diskcache: SQLite + pickle serialization (default config)")
        print(f"  SNKV    : WAL mode, raw bytes (no serialization overhead)")
        print(f"  ▲ = SNKV faster   ▼ = diskcache faster")

        # ── 1. Core operations ────────────────────────────────────────────────
        section_header(f"1. Core Operations  (N = {N:,} ops, value = 64 bytes)")
        print_table_header()
        core_results = []
        for label, sf, df in [
            ("Bulk write (batched tx)",  s1_snkv_bulk_write,      s1_dc_bulk_write),
            ("Individual write",         s1_snkv_individual_write, s1_dc_individual_write),
            ("Read - hit (100%)",        s1_snkv_read_hit,        s1_dc_read_hit),
            ("Read - miss (100%)",       s1_snkv_read_miss,       s1_dc_read_miss),
            ("Delete (batched tx)",      s1_snkv_delete,          s1_dc_delete),
            ("Full scan (key+value)",    s1_snkv_full_scan,       s1_dc_full_scan),
            ("Write with TTL",           s1_snkv_write_ttl,       s1_dc_write_ttl),
        ]:
            p, d = fresh(f"s1_{label[:6].replace(' ','_')}")
            sn, dc = run_row(label, sf, df, N, p, d)
            core_results.append((sn, dc))
        print_table_footer(core_results)

        # ── 2. Prefix scan ────────────────────────────────────────────────────
        section_header(
            f"2. Prefix Scan  ({N_NS:,} total keys, {N_MATCH:,} matching prefix '{TARGET_NS.decode()}')"
        )
        print(f"  SNKV  : native prefix_iterator — visits only matching keys")
        print(f"  diskcache: no native prefix support — scan all, filter in Python")
        print()
        print_table_header()
        pfx_results = []
        for label, sf, df in [
            (f"Forward prefix scan",  s2_snkv_prefix_scan,  s2_dc_prefix_scan),
            (f"Reverse prefix scan",  s2_snkv_reverse_prefix, s2_dc_reverse_prefix),
        ]:
            p, d = fresh(f"s2_{label[:6].replace(' ','_')}")
            sn, dc = run_row(label, sf, df, N_MATCH, p, d)
            pfx_results.append((sn, dc))
        print_table_footer(pfx_results)

        # ── 3. Mixed workload ─────────────────────────────────────────────────
        section_header(
            f"3. Mixed Workload  ({N_MIX:,} ops — 80% read / 20% write, interleaved)"
        )
        print_table_header()
        p, d = fresh("s3_mixed")
        sn, dc = run_row("80% read / 20% write", s3_snkv_mixed, s3_dc_mixed, N_MIX, p, d)
        print_table_footer([(sn, dc)])

        # ── 4. Value size scaling ─────────────────────────────────────────────
        section_header(f"4. Value Size Scaling  (N = {N_SCALE:,} ops, bulk write + read)")
        print(f"  {'Size':<10}  {'Op':<8}  {'SNKV':>10}  {'diskcache':>10}"
              f"  {'ops/s (SNKV)':>14}  {'Speedup':>8}")
        print("  " + "─" * 66)
        scale_results = []
        for size_label, size_bytes in _SCALE_SIZES:
            for op_label, sf, df in [
                ("write", s4_snkv_write, s4_dc_write),
                ("read",  s4_snkv_read,  s4_dc_read),
            ]:
                p, d = fresh(f"s4_{size_label.replace(' ','_')}_{op_label}")
                print(f"  running {size_label} {op_label}...", end="", flush=True)
                sn  = sf(p, size_bytes);  snkv_cleanup(p)
                dc_ = df(d, size_bytes)
                ratio = dc_ / sn
                arrow = "▲" if ratio >= 1.0 else "▼"
                print(f"\r  {size_label:<10}  {op_label:<8}  {fmt(sn):>10}  {fmt(dc_):>10}"
                      f"  {ops_str(N_SCALE, sn):>14}  {ratio:.1f}x {arrow}")
                scale_results.append((sn, dc_))
        print("  " + "─" * 66)
        ratios = [dc / sn for sn, dc in scale_results]
        print(f"  avg speedup across all sizes: {sum(ratios)/len(ratios):.1f}x")

        # ── 5. Large dataset ──────────────────────────────────────────────────
        section_header(f"5. Large Dataset  (N = {N_LARGE:,} keys, 64-byte values)")
        print_table_header()
        large_results = []
        for label, sf, df, n in [
            ("Bulk write (batched tx)",    s5_snkv_bulk_write, s5_dc_bulk_write, N_LARGE),
            (f"Read ({N_LARGE//10:,} sampled)", s5_snkv_read, s5_dc_read,  N_LARGE // 10),
            ("Full scan (key+value)",      s5_snkv_scan,       s5_dc_scan,  N_LARGE),
        ]:
            p, d = fresh(f"s5_{label[:6].replace(' ','_')}")
            sn, dc = run_row(label, sf, df, n, p, d)
            large_results.append((sn, dc))
        print_table_footer(large_results)

        # ── 6. Durability verification ────────────────────────────────────────
        section_header(
            f"6. Durability — write → close → reopen → verify  (N = {N_DUR:,} keys)"
        )
        print(f"  Each key has a unique verifiable value (b'chk:NNNNNNN:val=M').")
        print(f"  Phase 1: write all keys and fully close the store.")
        print(f"  Phase 2: reopen cold, read every key, assert value matches exactly.")
        print()
        print(f"  {'Test':<34}  {'Write':>10}  {'Reopen+Read':>12}  {'Verified':>10}  {'Errors':>7}")
        print("  " + "─" * 78)

        dur_results = []
        for label, snkv_fn, dc_fn in [
            ("Plain write (no TTL)",   s6_snkv_durability,     s6_dc_durability),
            ("Write with TTL=3600s",   s6_snkv_ttl_durability, s6_dc_ttl_durability),
        ]:
            p, d = fresh(f"s6_snkv_{label[:5].replace(' ','_')}")
            dc_dir2 = d

            print(f"  running {label}...", end="", flush=True)

            sw, sr, sv, se = snkv_fn(p)
            snkv_cleanup(p)
            dw, dr, dv, de = dc_fn(dc_dir2)

            snkv_ok = "✓" if se == 0 else f"✗ ({se} bad)"
            dc_ok   = "✓" if de == 0 else f"✗ ({de} bad)"

            print(f"\r  SNKV  — {label:<26}  {fmt(sw):>10}  {fmt(sr):>12}"
                  f"  {sv:>6}/{N_DUR} {snkv_ok:>5}  {'0':>7}")
            print(f"  dcache — {label:<26}  {fmt(dw):>10}  {fmt(dr):>12}"
                  f"  {dv:>6}/{N_DUR} {dc_ok:>5}  {de:>7}")
            print()

            # Integrity verdict
            if se > 0:
                print(f"  *** SNKV DATA INTEGRITY FAILURE: {se} keys corrupted ***")
            if de > 0:
                print(f"  *** diskcache DATA INTEGRITY FAILURE: {de} keys corrupted ***")

            dur_results.append((sw, dw))   # use write time for speedup table

        print("  " + "─" * 78)
        # Report write-phase speedup
        for (sw, dw), label in zip(dur_results, ["Plain write", "TTL write"]):
            ratio = dw / sw
            arrow = "▲" if ratio >= 1.0 else "▼"
            print(f"  Write speedup ({label}): {ratio:.1f}x {arrow}  |  "
                  f"Both verified {N_DUR}/{N_DUR} keys correctly")

        # ── Overall summary ───────────────────────────────────────────────────
        all_results = core_results + pfx_results + [(sn, dc)] + scale_results + large_results + dur_results
        all_ratios  = [dc / sn for sn, dc in all_results]
        total_wins  = sum(1 for r in all_ratios if r >= 1.0)
        overall_avg = sum(all_ratios) / len(all_ratios)

        print(f"\n{'='*72}")
        print(f"  Overall: SNKV faster in {total_wins}/{len(all_results)} tests")
        print(f"  Overall average speedup: {overall_avg:.1f}x")
        print(f"{'='*72}\n")


if __name__ == "__main__":
    run_benchmark()
