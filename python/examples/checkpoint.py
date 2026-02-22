# SPDX-License-Identifier: Apache-2.0
"""
WAL Checkpoint Examples
Demonstrates: walSizeLimit auto-checkpoint and manual kvstore_checkpoint()

Run:
    python examples/checkpoint.py
"""

import os
from snkv import (
    Store,
    JOURNAL_WAL,
    JOURNAL_DELETE,
    CHECKPOINT_PASSIVE,
    CHECKPOINT_FULL,
    CHECKPOINT_RESTART,
    CHECKPOINT_TRUNCATE,
)

DB_FILE = "checkpoint_example.db"


def manual_passive_checkpoint():
    print("--- Manual PASSIVE Checkpoint ---")
    # PASSIVE: copies WAL frames to the DB file without blocking readers/writers.
    # May not checkpoint all frames if readers are active.
    with Store(DB_FILE, journal_mode=JOURNAL_WAL) as db:
        for i in range(50):
            db[f"k{i:03d}"] = f"v{i}"

        nlog, nckpt = db.checkpoint(CHECKPOINT_PASSIVE)
        print(f"  PASSIVE: nLog={nlog}, nCkpt={nckpt}")
        print(f"  (nLog=total WAL frames, nCkpt=frames written to DB file)")


def manual_full_checkpoint():
    print("\n--- Manual FULL Checkpoint ---")
    # FULL: waits for all readers to finish, then copies all WAL frames.
    with Store(DB_FILE, journal_mode=JOURNAL_WAL) as db:
        for i in range(50):
            db[f"full:{i:03d}"] = f"v{i}"

        nlog, nckpt = db.checkpoint(CHECKPOINT_FULL)
        print(f"  FULL: nLog={nlog}, nCkpt={nckpt}")


def truncate_checkpoint():
    print("\n--- TRUNCATE Checkpoint ---")
    # TRUNCATE: like RESTART, then also shrinks the WAL file to zero bytes.
    # Most aggressive; best for reclaiming disk space.
    with Store(DB_FILE, journal_mode=JOURNAL_WAL) as db:
        for i in range(100):
            db[f"trunc:{i:04d}"] = f"v{i}"

        nlog, nckpt = db.checkpoint(CHECKPOINT_TRUNCATE)
        print(f"  TRUNCATE: nLog={nlog}, nCkpt={nckpt}")
        print(f"  WAL is now {'empty' if nlog == 0 else 'partially full'}")


def auto_checkpoint_via_wal_size_limit():
    print("\n--- Auto-Checkpoint via walSizeLimit ---")
    # With wal_size_limit=N, a PASSIVE checkpoint fires automatically
    # after every N committed write transactions.
    # Keeps the WAL from growing without bound.
    with Store(DB_FILE, journal_mode=JOURNAL_WAL, wal_size_limit=50) as db:
        for i in range(200):
            db[f"auto:{i:05d}"] = str(i)

        print(f"  Wrote 200 keys; auto-checkpoints fired at 50-write intervals")

        # Final manual checkpoint to see current WAL state
        nlog, nckpt = db.checkpoint(CHECKPOINT_PASSIVE)
        print(f"  Final state: nLog={nlog}")


def checkpoint_on_delete_journal():
    print("\n--- Checkpoint on DELETE Journal (no-op) ---")
    # checkpoint() on a non-WAL database is a no-op and returns (0, 0).
    with Store(DB_FILE, journal_mode=JOURNAL_DELETE) as db:
        db["key"] = "value"
        nlog, nckpt = db.checkpoint(CHECKPOINT_PASSIVE)
        print(f"  DELETE journal checkpoint: nLog={nlog}, nCkpt={nckpt} (always 0)")


def checkpoint_modes_summary():
    print("\n--- Checkpoint Modes Summary ---")
    print("  PASSIVE  - copy frames, never block;    may miss active-reader frames")
    print("  FULL     - wait for readers, copy all;  doesn't reset WAL write position")
    print("  RESTART  - like FULL + reset write pos; new writes restart from frame 0")
    print("  TRUNCATE - like RESTART + truncate file; WAL shrinks to 0 bytes on disk")

    with Store(DB_FILE, journal_mode=JOURNAL_WAL) as db:
        for i in range(30):
            db[f"mode:{i}"] = str(i)

        for mode, name in (
            (CHECKPOINT_PASSIVE,  "PASSIVE"),
            (CHECKPOINT_FULL,     "FULL"),
            (CHECKPOINT_RESTART,  "RESTART"),
            (CHECKPOINT_TRUNCATE, "TRUNCATE"),
        ):
            # Re-write some data so each checkpoint has work to do
            for j in range(10):
                db[f"mode:{j}"] = f"new_{j}"

            nlog, nckpt = db.checkpoint(mode)
            print(f"  {name:<10} nLog={nlog:3d}, nCkpt={nckpt:3d}")


if __name__ == "__main__":
    manual_passive_checkpoint()
    manual_full_checkpoint()
    truncate_checkpoint()
    auto_checkpoint_via_wal_size_limit()
    checkpoint_on_delete_journal()
    checkpoint_modes_summary()

    # Cleanup
    for ext in ("", "-wal", "-shm"):
        try:
            os.remove(DB_FILE + ext)
        except FileNotFoundError:
            pass
    print("\n[OK] checkpoint.py complete")
