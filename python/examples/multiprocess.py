# SPDX-License-Identifier: Apache-2.0
"""
Multi-Process SNKV Example
Demonstrates: 5 processes concurrently reading and writing to the same database.
SQLite WAL mode handles cross-process file locking automatically.
busy_timeout tells SQLite to retry internally for up to N ms on any lock conflict,
which is simpler and more correct than retrying at the Python level.

Run:
    python examples/multiprocess.py
"""

import os
import time
import multiprocessing
from snkv import KVStore

DB_FILE = "multiprocess_example.db"
NUM_PROCESSES = 5
OPS_PER_PROCESS = 20
BUSY_TIMEOUT_MS = 5000  # SQLite retries internally for up to 5 seconds


def worker(pid: int, results: dict):
    """Each process opens its own KVStore handle and does mixed reads/writes."""
    written = 0
    read_ok = 0
    read_miss = 0
    errors = 0

    try:
        with KVStore(DB_FILE, busy_timeout=BUSY_TIMEOUT_MS) as db:
            for i in range(OPS_PER_PROCESS):
                key = f"proc{pid}:key{i}".encode()
                value = f"value-from-{pid}-iter-{i}".encode()

                # Write own key
                db.put(key, value)
                written += 1

                # Read own key back
                got = db.get(key)
                if got == value:
                    read_ok += 1
                else:
                    errors += 1

                # Try reading a key written by another process (may or may not exist yet)
                other_pid = (pid + 1) % NUM_PROCESSES
                other_key = f"proc{other_pid}:key{i}".encode()
                other_val = db.get(other_key, default=None)
                if other_val is not None:
                    read_ok += 1
                else:
                    read_miss += 1

            # Verify all own keys are readable at end
            for i in range(OPS_PER_PROCESS):
                key = f"proc{pid}:key{i}".encode()
                expected = f"value-from-{pid}-iter-{i}".encode()
                got = db.get(key)
                if got != expected:
                    errors += 1

    except Exception as e:
        results[pid] = {"error": f"{type(e).__name__}: {e!r}"}
        return

    results[pid] = {
        "written": written,
        "read_ok": read_ok,
        "read_miss": read_miss,
        "errors": errors,
    }


def main():
    # Pre-create the database so all processes open an existing file
    with KVStore(DB_FILE) as db:
        db.put(b"__init__", b"1")

    manager = multiprocessing.Manager()
    results = manager.dict()

    print(f"Spawning {NUM_PROCESSES} processes, {OPS_PER_PROCESS} ops each...")
    t0 = time.time()

    procs = []
    for pid in range(NUM_PROCESSES):
        p = multiprocessing.Process(target=worker, args=(pid, results))
        procs.append(p)
        p.start()

    for p in procs:
        p.join()

    elapsed = time.time() - t0

    # Report per-process results
    total_written = 0
    total_read_ok = 0
    total_errors = 0
    all_ok = True

    for pid in range(NUM_PROCESSES):
        r = results.get(pid, {"error": "process did not report"})
        if "error" in r:
            print(f"  proc {pid}: ERROR — {r['error']}")
            all_ok = False
        else:
            status = "OK" if r["errors"] == 0 else "FAIL"
            print(
                f"  proc {pid}: {status}  "
                f"written={r['written']}  "
                f"read_ok={r['read_ok']}  "
                f"read_miss={r['read_miss']}  "
                f"errors={r['errors']}"
            )
            total_written += r["written"]
            total_read_ok += r["read_ok"]
            total_errors += r["errors"]
            if r["errors"] > 0:
                all_ok = False

    print(f"\nTotal: written={total_written}  read_ok={total_read_ok}  errors={total_errors}")
    print(f"Time:  {elapsed:.2f}s")
    print(f"Result: {'PASS' if all_ok else 'FAIL'}")

    # Final verification: all keys from all processes must be present
    print("\nVerifying final state...")
    with KVStore(DB_FILE) as db:
        missing = 0
        for pid in range(NUM_PROCESSES):
            for i in range(OPS_PER_PROCESS):
                key = f"proc{pid}:key{i}".encode()
                expected = f"value-from-{pid}-iter-{i}".encode()
                got = db.get(key, default=None)
                if got != expected:
                    missing += 1
        total_keys = NUM_PROCESSES * OPS_PER_PROCESS
        print(f"Keys verified: {total_keys - missing}/{total_keys}")
        if missing == 0:
            print("All keys correct — multi-process read/write is consistent.")
        else:
            print(f"FAIL: {missing} keys missing or incorrect.")

    # Cleanup
    for ext in ("", "-wal", "-shm"):
        try:
            os.remove(DB_FILE + ext)
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    multiprocessing.set_start_method("spawn")
    main()
