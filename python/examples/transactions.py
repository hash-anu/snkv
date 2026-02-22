# SPDX-License-Identifier: Apache-2.0
"""
Transaction Examples
Demonstrates: Atomic batch operations, rollback, nested reads

Run:
    python examples/transactions.py
"""

import os
import snkv
from snkv import KVStore

DB_FILE = "transactions_example.db"


def atomic_batch_write():
    print("--- Atomic Batch Write ---")
    with KVStore(DB_FILE) as db:
        # Without explicit transaction: each put auto-commits individually
        db["k1"] = "v1"
        db["k2"] = "v2"

        # With an explicit write transaction: all writes commit atomically
        db.begin(write=True)
        for i in range(5):
            db[f"batch:{i}"] = f"value_{i}"
        db.commit()

        # Verify
        for i in range(5):
            assert db.get(f"batch:{i}") == f"value_{i}".encode()
        print("Batch of 5 committed atomically")


def rollback_on_error():
    print("\n--- Rollback on Error ---")
    with KVStore(DB_FILE) as db:
        db.begin(write=True)
        db["will_be_rolled_back"] = "temporary"

        # Simulate an error mid-transaction
        try:
            raise RuntimeError("simulated error")
        except RuntimeError:
            db.rollback()
            print("Transaction rolled back")

        result = db.get("will_be_rolled_back")
        print(f"Value after rollback: {result}")  # None - never committed


def context_manager_transaction():
    """Use Python try/finally as a manual transaction context."""
    print("\n--- Manual Transaction Pattern ---")
    with KVStore(DB_FILE) as db:
        db.begin(write=True)
        try:
            db["account:alice"] = "1000"
            db["account:bob"]   = "500"
            # ... do work ...
            db.commit()
            print("Transfer committed")
        except Exception as e:
            db.rollback()
            print(f"Transfer rolled back: {e}")

        print(f"alice = {db['account:alice'].decode()}")
        print(f"bob   = {db['account:bob'].decode()}")


def read_only_transaction():
    print("\n--- Read Transaction ---")
    with KVStore(DB_FILE) as db:
        db["data"] = "hello"

        # wrflag=False (default) starts a read-only transaction
        # Useful to get a consistent snapshot across multiple reads
        db.begin(write=False)
        val1 = db.get("data")
        val2 = db.get("data")
        db.commit()

        print(f"Consistent read: {val1.decode()} == {val2.decode()}: {val1 == val2}")


def large_batch_performance():
    print("\n--- Large Batch (1000 writes in one transaction) ---")
    import time
    with KVStore(DB_FILE) as db:
        t0 = time.perf_counter()
        db.begin(write=True)
        for i in range(1000):
            db[f"item:{i:05d}"] = f"value_{i}"
        db.commit()
        elapsed = time.perf_counter() - t0

        count = sum(1 for _ in db.iterator())
        print(f"1000 writes committed in {elapsed*1000:.1f} ms")
        print(f"Total keys in store: {count}")


if __name__ == "__main__":
    atomic_batch_write()
    rollback_on_error()
    context_manager_transaction()
    read_only_transaction()
    large_batch_performance()

    # Cleanup
    for ext in ("", "-wal", "-shm"):
        try:
            os.remove(DB_FILE + ext)
        except FileNotFoundError:
            pass
    print("\n[OK] transactions.py complete")
