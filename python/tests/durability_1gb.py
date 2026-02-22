#!/usr/bin/env python3
"""
SNKV Python API — 1 GB durability test.

Phase 1  WRITE   Generate unique deterministic key-value pairs and write to SNKV.
Phase 2  VERIFY  Read every record back and compare value byte-for-byte.

Keys and values are fully deterministic (SHA-256 seeded from the record index)
so no auxiliary storage is needed — expected values are regenerated on the fly
during verification.

Usage (from python/ directory):
    PYTHONPATH=. python3 tests/durability_1gb.py
    PYTHONPATH=. python3 tests/durability_1gb.py /tmp/mydb.db
    PYTHONPATH=. python3 tests/durability_1gb.py /tmp/mydb.db --gb 2
    PYTHONPATH=. python3 tests/durability_1gb.py /tmp/mydb.db --value-size 4096
    PYTHONPATH=. python3 tests/durability_1gb.py /tmp/mydb.db --skip-write
    PYTHONPATH=. python3 tests/durability_1gb.py /tmp/mydb.db --skip-verify

Arguments
---------
  db_path        Path for the database file  (default: /tmp/snkv_dur_test.db)
  --gb N         Target data volume in GB    (default: 1)
  --value-size B Bytes per value             (default: 1000)
  --batch-size N Records per transaction     (default: 5000)
  --skip-write   Skip write phase; verify an existing DB
  --skip-verify  Skip verify phase; write only
"""

from __future__ import annotations

import argparse
import hashlib
import os
import struct
import sys
import time
from typing import List

# ── importpath: works with PYTHONPATH=. or from repo root ──────────────────
try:
    from snkv import KVStore, JOURNAL_WAL, SYNC_NORMAL, NotFoundError
except ImportError:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from snkv import KVStore, JOURNAL_WAL, SYNC_NORMAL, NotFoundError  # type: ignore

# ── key / value generators ─────────────────────────────────────────────────

_KEY_PREFIX = b"durtest:"
_SHA_BLOCK = 32  # bytes per SHA-256 digest


def make_key(i: int) -> bytes:
    """Fixed-width key: durtest:0000000000  (18 bytes total)."""
    return _KEY_PREFIX + f"{i:010d}".encode()


def make_value(i: int, size: int) -> bytes:
    """
    Deterministic value for record i.
    Built by repeating SHA-256(big-endian uint64 i) to fill exactly `size` bytes.
    """
    seed = struct.pack(">Q", i)
    digest = hashlib.sha256(seed).digest()          # 32 bytes
    repeats = (size + _SHA_BLOCK - 1) // _SHA_BLOCK
    return (digest * repeats)[:size]


def parse_index(key: bytes) -> int:
    """Extract the integer index embedded in a durtest key."""
    return int(key[len(_KEY_PREFIX):])


# ── formatting helpers ─────────────────────────────────────────────────────

def hr_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def hr_rate(records: int, elapsed: float) -> str:
    if elapsed < 0.001:
        return "—"
    return f"{records / elapsed:,.0f} rec/s"


def print_progress(
    phase: str, done: int, total: int, data_bytes: int, t0: float
) -> None:
    elapsed = time.monotonic() - t0
    pct = done * 100.0 / total if total else 0.0
    eta = ""
    if done > 0 and elapsed > 1.0:
        secs_left = (total - done) * elapsed / done
        if secs_left >= 60:
            eta = f"  ETA {secs_left / 60:.0f}m{secs_left % 60:.0f}s"
        else:
            eta = f"  ETA {secs_left:.0f}s"
    print(
        f"\r  [{phase}] {done:>10,} / {total:,}  ({pct:5.1f}%)"
        f"  {hr_bytes(data_bytes):>10}  {hr_rate(done, elapsed)}{eta}          ",
        end="",
        flush=True,
    )


# ── write phase ────────────────────────────────────────────────────────────

def write_phase(
    db_path: str,
    n_records: int,
    value_size: int,
    target_bytes: int,
    batch_size: int,
) -> dict:
    print(f"\n{'='*64}")
    print(f"  WRITE PHASE")
    print(f"  Records    : {n_records:,}")
    print(f"  Value size : {value_size} bytes")
    print(f"  Batch size : {batch_size:,} records / transaction")
    print(f"  Target     : {hr_bytes(target_bytes)}")
    print(f"{'='*64}")

    total_bytes = 0
    t0 = time.monotonic()

    with KVStore(
        db_path,
        journal_mode=JOURNAL_WAL,
        sync_level=SYNC_NORMAL,
        cache_size=4000,        # ~16 MB page cache
        busy_timeout=5000,
        wal_size_limit=10_000,  # auto-checkpoint every 10 k WAL frames
    ) as db:
        db.begin(write=True)

        for i in range(n_records):
            key = make_key(i)
            val = make_value(i, value_size)
            db[key] = val
            total_bytes += len(key) + len(val)

            if (i + 1) % batch_size == 0:
                db.commit()
                db.begin(write=True)

            if (i + 1) % 10_000 == 0 or i == n_records - 1:
                print_progress("WRITE", i + 1, n_records, total_bytes, t0)

        db.commit()         # flush remaining batch
        print()             # newline after \r

        db.sync()           # fsync before close

    elapsed = time.monotonic() - t0
    throughput_mb = total_bytes / elapsed / 1024 / 1024

    print(
        f"\n  Wrote {n_records:,} records  {hr_bytes(total_bytes)}"
        f"  in {elapsed:.1f}s  ({throughput_mb:.1f} MB/s,"
        f" {hr_rate(n_records, elapsed)})"
    )

    return {
        "n_records": n_records,
        "value_size": value_size,
        "total_bytes": total_bytes,
        "elapsed_s": elapsed,
        "throughput_mbs": throughput_mb,
    }


# ── verify phase ───────────────────────────────────────────────────────────

_MAX_ERRORS = 20        # stop collecting after this many mismatches
_REPORT_EVERY = 10_000  # progress update interval


def verify_phase(db_path: str, n_records: int, value_size: int) -> dict:
    print(f"\n{'='*64}")
    print(f"  VERIFY PHASE  (sequential scan via iterator)")
    print(f"  Expecting  : {n_records:,} records")
    print(f"{'='*64}")

    errors: List[str] = []
    records_seen = 0
    total_bytes = 0
    t0 = time.monotonic()

    with KVStore(
        db_path,
        journal_mode=JOURNAL_WAL,
        sync_level=SYNC_NORMAL,
        cache_size=4000,
        busy_timeout=5000,
    ) as db:
        with db.iterator() as it:
            for key, actual in it:
                # ── unexpected key? ────────────────────────────────────────
                if not key.startswith(_KEY_PREFIX):
                    errors.append(f"unexpected key: {key!r}")
                    if len(errors) >= _MAX_ERRORS:
                        errors.append("... too many errors, aborting")
                        break
                    continue

                try:
                    idx = parse_index(key)
                except ValueError:
                    errors.append(f"malformed key: {key!r}")
                    continue

                # ── value check ────────────────────────────────────────────
                expected = make_value(idx, value_size)

                if len(actual) != len(expected):
                    errors.append(
                        f"record {idx}: length mismatch"
                        f" (got {len(actual)}, expected {len(expected)})"
                    )
                elif actual != expected:
                    # find first differing byte for diagnostics
                    diff_pos = next(
                        j for j, (a, b) in enumerate(zip(actual, expected)) if a != b
                    )
                    errors.append(
                        f"record {idx}: value mismatch"
                        f" at byte {diff_pos}"
                        f" (got 0x{actual[diff_pos]:02x},"
                        f" expected 0x{expected[diff_pos]:02x})"
                    )

                records_seen += 1
                total_bytes += len(key) + len(actual)

                if len(errors) >= _MAX_ERRORS:
                    errors.append("... too many errors, aborting")
                    break

                if records_seen % _REPORT_EVERY == 0 or records_seen == n_records:
                    print_progress(
                        "VERIFY", records_seen, n_records, total_bytes, t0
                    )

        print()  # newline after \r

    elapsed = time.monotonic() - t0
    throughput_mb = total_bytes / elapsed / 1024 / 1024

    # ── check record count ─────────────────────────────────────────────────
    if records_seen != n_records and len(errors) < _MAX_ERRORS:
        errors.append(
            f"record count mismatch: found {records_seen:,}, expected {n_records:,}"
        )

    print(
        f"\n  Read  {records_seen:,} records  {hr_bytes(total_bytes)}"
        f"  in {elapsed:.1f}s  ({throughput_mb:.1f} MB/s,"
        f" {hr_rate(records_seen, elapsed)})"
    )

    return {
        "n_records_expected": n_records,
        "n_records_seen": records_seen,
        "errors": errors,
        "total_bytes": total_bytes,
        "elapsed_s": elapsed,
        "throughput_mbs": throughput_mb,
    }


# ── entry point ────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="SNKV Python — 1 GB durability test (write + read-back verify).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "db_path",
        nargs="?",
        default="/tmp/snkv_dur_test.db",
        help="Path for the test database",
    )
    parser.add_argument(
        "--gb",
        type=float,
        default=1.0,
        metavar="N",
        help="Target data volume in GB",
    )
    parser.add_argument(
        "--value-size",
        type=int,
        default=1000,
        metavar="BYTES",
        help="Bytes per value",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5_000,
        metavar="N",
        help="Records per write transaction",
    )
    parser.add_argument(
        "--skip-write",
        action="store_true",
        help="Skip the write phase; verify an existing database",
    )
    parser.add_argument(
        "--skip-verify",
        action="store_true",
        help="Skip the verify phase; only write",
    )
    args = parser.parse_args()

    target_bytes = int(args.gb * 1024**3)
    key_size     = len(make_key(0))
    record_bytes = key_size + args.value_size
    n_records    = max(1, target_bytes // record_bytes)

    print()
    print("SNKV Python — Durability Test")
    print(f"  Target     : {args.gb:.1f} GB  ({hr_bytes(target_bytes)})")
    print(f"  Key        : {key_size} bytes  (e.g. {make_key(0).decode()!r})")
    print(f"  Value      : {args.value_size} bytes  (SHA-256 deterministic fill)")
    print(f"  Records    : {n_records:,}  ({record_bytes} bytes/record)")
    print(f"  Batch      : {args.batch_size:,} records / transaction")
    print(f"  DB         : {args.db_path}")

    write_result  = None
    verify_result = None

    # ── write ──────────────────────────────────────────────────────────────
    if not args.skip_write:
        write_result = write_phase(
            args.db_path, n_records, args.value_size, target_bytes, args.batch_size
        )
    else:
        print("\n  [WRITE] Skipped.")

    # ── verify ─────────────────────────────────────────────────────────────
    if not args.skip_verify:
        verify_result = verify_phase(args.db_path, n_records, args.value_size)
    else:
        print("\n  [VERIFY] Skipped.")

    # ── summary ────────────────────────────────────────────────────────────
    print(f"\n{'='*64}")
    print(f"  SUMMARY")
    print(f"{'='*64}")

    ok = True

    if write_result:
        w = write_result
        print(
            f"  Write  : {w['n_records']:>10,} records"
            f"  {hr_bytes(w['total_bytes']):>10}"
            f"  {w['elapsed_s']:6.1f}s"
            f"  {w['throughput_mbs']:.1f} MB/s"
        )

    if verify_result:
        v = verify_result
        print(
            f"  Verify : {v['n_records_seen']:>10,} records"
            f"  {hr_bytes(v['total_bytes']):>10}"
            f"  {v['elapsed_s']:6.1f}s"
            f"  {v['throughput_mbs']:.1f} MB/s"
        )

        if v["errors"]:
            ok = False
            print(f"\n  FAILED — {len(v['errors'])} error(s):")
            for err in v["errors"]:
                print(f"    ✗  {err}")
        else:
            print(
                f"\n  PASSED — all {v['n_records_seen']:,} records"
                f" verified correctly"
            )

    print(f"{'='*64}\n")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
