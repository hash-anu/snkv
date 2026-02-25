# SPDX-License-Identifier: Apache-2.0
"""
Iterator Examples
Demonstrates: Full scan, prefix scan, manual control, CF iteration,
              store statistics

Run:
    python examples/iterators.py
"""

import os
from snkv import KeyValueStore

DB_FILE = "iterators_example.db"


def full_scan():
    print("--- Full Scan (for loop) ---")
    with KeyValueStore(DB_FILE) as db:
        for i in range(5):
            db[f"key:{i:03d}"] = f"value_{i}"

        # Simplest: iterate directly over the store
        for key, value in db:
            print(f"  {key.decode()} -> {value.decode()}")


def prefix_scan():
    print("\n--- Prefix Scan ---")
    with KeyValueStore(DB_FILE) as db:
        db["user:alice"]   = "30"
        db["user:bob"]     = "25"
        db["user:charlie"] = "35"
        db["post:1"]       = "Hello"
        db["post:2"]       = "World"

        print("  Keys starting with 'user:':")
        for key, value in db.prefix_iterator(b"user:"):
            print(f"    {key.decode()} -> {value.decode()}")

        print("  Keys starting with 'post:':")
        for key, value in db.prefix_iterator(b"post:"):
            print(f"    {key.decode()} -> {value.decode()}")


def iterator_context_manager():
    print("\n--- Iterator as Context Manager ---")
    with KeyValueStore(DB_FILE) as db:
        db["a"] = "1"
        db["b"] = "2"
        db["c"] = "3"

        with db.iterator() as it:
            pairs = list(it)
        print(f"  Collected {len(pairs)} pairs via context manager")


def manual_iterator_control():
    print("\n--- Manual Iterator Control ---")
    with KeyValueStore(DB_FILE) as db:
        for c in "abcde":
            db[c] = str(ord(c))

        it = db.iterator()
        it.first()
        count = 0
        while not it.eof:
            key   = it.key.decode()
            value = it.value.decode()
            print(f"  [{count}] {key} -> {value}")
            count += 1
            it.next()
        it.close()
        print(f"  Total: {count} items")


def cf_iterator():
    print("\n--- Column Family Iterator ---")
    with KeyValueStore(DB_FILE) as db:
        with db.create_column_family("fruits") as cf:
            cf["apple"]  = "red"
            cf["banana"] = "yellow"
            cf["grape"]  = "purple"

            print("  Fruits CF:")
            for key, value in cf.iterator():
                print(f"    {key.decode()} -> {value.decode()}")

            print("  CF prefix 'b':")
            for key, value in cf.prefix_iterator(b"b"):
                print(f"    {key.decode()} -> {value.decode()}")


def collect_all_keys():
    print("\n--- Collect All Keys ---")
    with KeyValueStore(DB_FILE) as db:
        for i in range(10):
            db[f"item:{i:02d}"] = str(i * i)

        keys = [k for k, _ in db.iterator()]
        print(f"  {len(keys)} keys: {[k.decode() for k in keys[:5]]} ...")


def statistics():
    print("\n--- KeyValueStore Statistics ---")
    with KeyValueStore(DB_FILE) as db:
        for i in range(20):
            db[f"stat_key:{i}"] = str(i)
        for i in range(10):
            _ = db.get(f"stat_key:{i}")
        for i in range(5):
            db.delete(f"stat_key:{i}")

        s = db.stats()
        print(f"  puts:       {s['puts']}")
        print(f"  gets:       {s['gets']}")
        print(f"  deletes:    {s['deletes']}")
        print(f"  iterations: {s['iterations']}")
        print(f"  errors:     {s['errors']}")


if __name__ == "__main__":
    full_scan()
    prefix_scan()
    iterator_context_manager()
    manual_iterator_control()
    cf_iterator()
    collect_all_keys()
    statistics()

    # Cleanup
    for ext in ("", "-wal", "-shm"):
        try:
            os.remove(DB_FILE + ext)
        except FileNotFoundError:
            pass
    print("\n[OK] iterators.py complete")
