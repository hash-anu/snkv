# SPDX-License-Identifier: Apache-2.0
"""
Configuration Examples
Demonstrates: kvstore_open_v2 via KVStore keyword args,
              journal modes, sync levels, cache/page size, read-only mode

Run:
    python examples/config.py
"""

import os
from snkv import (
    KVStore,
    ReadOnlyError,
    JOURNAL_WAL,
    JOURNAL_DELETE,
    SYNC_OFF,
    SYNC_NORMAL,
    SYNC_FULL,
)

DB_FILE  = "config_example.db"
DB_FILE2 = "config_example2.db"


def default_config():
    print("--- Default Config (WAL, SYNC_NORMAL, 8 MB cache) ---")
    with KVStore(DB_FILE) as db:
        db["key"] = "value"
        print(f"  value = {db['value' if False else 'key'].decode()}")


def wal_mode():
    print("\n--- WAL Mode (default, recommended) ---")
    # WAL allows concurrent readers + one writer
    # .db-wal and .db-shm sidecar files are created
    with KVStore(DB_FILE, journal_mode=JOURNAL_WAL) as db:
        db["wal_key"] = "wal_value"
        print(f"  wal_key = {db['wal_key'].decode()}")
    print("  WAL database closed (checkpoint runs on close)")


def delete_mode():
    print("\n--- DELETE Journal Mode ---")
    # Classic rollback journal; no -wal/-shm sidecar files
    with KVStore(DB_FILE2, journal_mode=JOURNAL_DELETE) as db:
        db["del_key"] = "del_value"
        print(f"  del_key = {db['del_key'].decode()}")


def sync_levels():
    print("\n--- Sync Levels ---")

    # SYNC_NORMAL (default): survives process crash, not necessarily power loss
    with KVStore(DB_FILE, sync_level=SYNC_NORMAL) as db:
        db["k"] = "normal"
        print(f"  SYNC_NORMAL: k = {db['k'].decode()}")

    # SYNC_OFF: fastest, no fsync; data may be lost on power failure
    with KVStore(DB_FILE, sync_level=SYNC_OFF) as db:
        db["k"] = "off"
        print(f"  SYNC_OFF:    k = {db['k'].decode()}")

    # SYNC_FULL: fsync on every commit; power-safe, slowest
    with KVStore(DB_FILE, sync_level=SYNC_FULL) as db:
        db["k"] = "full"
        print(f"  SYNC_FULL:   k = {db['k'].decode()}")


def custom_cache_and_page_size():
    print("\n--- Custom Cache and Page Size ---")
    # cache_size: number of 4096-byte pages to keep in memory
    # Larger cache -> better read performance at the cost of RSS
    with KVStore(DB_FILE,
               cache_size=4000,    # ~16 MB
               page_size=4096      # new databases only
               ) as db:
        for i in range(100):
            db[f"k{i:03d}"] = str(i)
        print(f"  100 keys written with 16 MB cache")
        print(f"  k050 = {db['k050'].decode()}")


def busy_timeout():
    print("\n--- Busy Timeout ---")
    # busy_timeout: ms to retry when another process holds the lock
    # Useful for multi-process shared databases
    with KVStore(DB_FILE, busy_timeout=5000) as db:
        db["shared"] = "data"
        print(f"  busy_timeout=5000ms: shared = {db['shared'].decode()}")


def read_only_mode():
    print("\n--- Read-Only Mode ---")
    # First create a DB with some data
    with KVStore(DB_FILE) as db:
        db["readonly_key"] = "readonly_value"

    # Then open it read-only
    with KVStore(DB_FILE, read_only=1) as db:
        print(f"  readonly_key = {db['readonly_key'].decode()}")

        # Writes raise ReadOnlyError
        try:
            db["new_key"] = "should_fail"
        except ReadOnlyError:
            print(f"  Write blocked: ReadOnlyError (read-only database)")


def wal_size_limit():
    print("\n--- WAL Auto-Checkpoint (wal_size_limit) ---")
    # wal_size_limit=N: trigger a PASSIVE checkpoint after every N committed
    # write transactions. Keeps the WAL file from growing unbounded.
    with KVStore(DB_FILE, wal_size_limit=50) as db:
        for i in range(200):
            db[f"auto:{i:05d}"] = str(i)
        print("  200 writes done; auto-checkpoints fired at 50-write intervals")

        nlog, nckpt = db.checkpoint()
        print(f"  Final checkpoint: nLog={nlog}, nCkpt={nckpt}")


def open_v2_full_example():
    print("\n--- open_v2: All Options at Once ---")
    with KVStore(
        DB_FILE,
        journal_mode  = JOURNAL_WAL,
        sync_level    = SYNC_NORMAL,
        cache_size    = 2000,    # ~8 MB
        page_size     = 4096,
        busy_timeout  = 1000,    # 1 s retry on lock
        wal_size_limit= 100,
    ) as db:
        db["full"] = "config"
        print(f"  full = {db['full'].decode()}")


if __name__ == "__main__":
    default_config()
    wal_mode()
    delete_mode()
    sync_levels()
    custom_cache_and_page_size()
    busy_timeout()
    read_only_mode()
    wal_size_limit()
    open_v2_full_example()

    # Cleanup
    for db_file in (DB_FILE, DB_FILE2):
        for ext in ("", "-wal", "-shm"):
            try:
                os.remove(db_file + ext)
            except FileNotFoundError:
                pass
    print("\n[OK] config.py complete")
