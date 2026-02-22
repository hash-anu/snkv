# SPDX-License-Identifier: Apache-2.0
"""
Column Family Examples
Demonstrates: Creating namespaces, cross-CF isolation,
              listing and dropping column families

Run:
    python examples/column_families.py
"""

import os
from snkv import KVStore, NotFoundError

DB_FILE = "cf_example.db"


def create_and_use():
    print("--- Create and Use Column Families ---")
    with KVStore(DB_FILE) as db:
        # Create two independent namespaces
        users = db.create_column_family("users")
        posts = db.create_column_family("posts")

        users["alice"] = "Alice Smith"
        users["bob"]   = "Bob Jones"
        posts["post:1"] = "Hello World"
        posts["post:2"] = "SNKV is fast"

        print(f"users/alice = {users['alice'].decode()}")
        print(f"posts/post:1 = {posts['post:1'].decode()}")

        users.close()
        posts.close()


def namespace_isolation():
    print("\n--- Namespace Isolation ---")
    with KVStore(DB_FILE) as db:
        ns_a = db.create_column_family("ns_a")
        ns_b = db.create_column_family("ns_b")

        # Same key in different column families holds different values
        ns_a["shared_key"] = "value_from_ns_a"
        ns_b["shared_key"] = "value_from_ns_b"

        # Also doesn't bleed into the default CF
        db["shared_key"] = "value_from_default"

        print(f"ns_a/shared_key  = {ns_a['shared_key'].decode()}")
        print(f"ns_b/shared_key  = {ns_b['shared_key'].decode()}")
        print(f"default/shared_key = {db['shared_key'].decode()}")

        ns_a.close()
        ns_b.close()


def list_and_drop():
    print("\n--- List and Drop Column Families ---")
    with KVStore(DB_FILE) as db:
        db.create_column_family("temp_a")
        db.create_column_family("temp_b")
        db.create_column_family("keep_me")

        names = db.list_column_families()
        print(f"All CFs: {names}")

        db.drop_column_family("temp_a")
        db.drop_column_family("temp_b")

        names = db.list_column_families()
        print(f"After drop: {names}")


def open_existing():
    print("\n--- Open an Existing Column Family ---")
    with KVStore(DB_FILE) as db:
        # Create and write, then close the handle
        cf = db.create_column_family("persistent_cf")
        cf["key"] = "stored_value"
        cf.close()

        # Later, re-open by name
        cf2 = db.open_column_family("persistent_cf")
        print(f"Re-opened CF value: {cf2['key'].decode()}")
        cf2.close()

        # Opening a nonexistent CF raises an error
        try:
            db.open_column_family("does_not_exist")
        except Exception as e:
            print(f"Opening missing CF: {type(e).__name__}")


def default_column_family():
    print("\n--- Default Column Family ---")
    with KVStore(DB_FILE) as db:
        # The default CF is the same underlying namespace as db.put/get
        cf = db.default_column_family()
        cf["via_cf"]  = "written through CF handle"
        cf.close()

        # Readable directly from the store
        print(f"db.get('via_cf') = {db.get('via_cf').decode()}")


def cf_context_manager():
    print("\n--- Column Family as Context Manager ---")
    with KVStore(DB_FILE) as db:
        with db.create_column_family("ctx_cf") as cf:
            cf["item"] = "data"
            print(f"item = {cf['item'].decode()}")
        # cf.close() called automatically


if __name__ == "__main__":
    create_and_use()
    namespace_isolation()
    list_and_drop()
    open_existing()
    default_column_family()
    cf_context_manager()

    # Cleanup
    for ext in ("", "-wal", "-shm"):
        try:
            os.remove(DB_FILE + ext)
        except FileNotFoundError:
            pass
    print("\n[OK] column_families.py complete")
