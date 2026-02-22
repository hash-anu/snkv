# SPDX-License-Identifier: Apache-2.0
"""
Basic SNKV Examples
Demonstrates: Hello World, CRUD operations, existence checks

Run:
    python examples/basic.py
"""

import os
import snkv
from snkv import Store, NotFoundError

DB_FILE = "basic_example.db"


def hello_world():
    print("--- Hello World ---")
    with Store(DB_FILE) as db:
        db.put(b"greeting", b"Hello, SNKV!")

        value = db.get(b"greeting")
        print(f"greeting = {value.decode()}")


def crud_operations():
    print("\n--- CRUD Operations ---")
    with Store(DB_FILE) as db:
        # Create
        db["user:1"] = "Alice"
        db["user:2"] = "Bob"
        db["user:3"] = "Charlie"
        print("Inserted 3 users")

        # Read
        print(f"user:1 = {db['user:1'].decode()}")
        print(f"user:2 = {db['user:2'].decode()}")

        # Update
        db["user:1"] = "Alice Smith"
        print(f"user:1 updated = {db['user:1'].decode()}")

        # Delete
        del db["user:3"]
        print("user:3 deleted")

        # Confirm deletion via get with default
        result = db.get("user:3", default=b"(not found)")
        print(f"user:3 after delete = {result.decode()}")


def existence_checks():
    print("\n--- Existence Checks ---")
    with Store(DB_FILE) as db:
        db["present"] = "yes"

        print(f"'present' exists: {db.exists('present')}")
        print(f"'missing' exists: {db.exists('missing')}")
        print(f"'present' in db:  {'present' in db}")
        print(f"'missing' in db:  {'missing' in db}")


def error_handling():
    print("\n--- Error Handling ---")
    with Store(DB_FILE) as db:
        db["key"] = "value"

        # KeyError on missing key via [] syntax
        try:
            _ = db["does_not_exist"]
        except KeyError as e:
            print(f"KeyError caught: {e}")

        # NotFoundError is also raised (it inherits from KeyError)
        try:
            _ = db["does_not_exist"]
        except NotFoundError:
            print("NotFoundError caught (same exception)")

        # get() with default avoids exceptions
        val = db.get("does_not_exist", b"fallback")
        print(f"get with default: {val.decode()}")


def binary_data():
    print("\n--- Binary Keys and Values ---")
    with Store(DB_FILE) as db:
        # Keys and values can be arbitrary binary data
        binary_key   = bytes([0x00, 0x01, 0xFF])
        binary_value = bytes(range(256))

        db.put(binary_key, binary_value)
        retrieved = db.get(binary_key)
        print(f"Binary round-trip OK: {retrieved == binary_value}")
        print(f"Value length: {len(retrieved)} bytes")


def in_memory_store():
    print("\n--- In-Memory Store ---")
    # Pass None for path to use an in-memory database (no file created)
    with Store(None) as db:
        db["temp"] = "ephemeral data"
        print(f"In-memory value: {db['temp'].decode()}")
    print("Store closed - data is gone (no file was created)")


if __name__ == "__main__":
    hello_world()
    crud_operations()
    existence_checks()
    error_handling()
    binary_data()
    in_memory_store()

    # Cleanup
    for ext in ("", "-wal", "-shm"):
        try:
            os.remove(DB_FILE + ext)
        except FileNotFoundError:
            pass
    print("\n[OK] basic.py complete")
