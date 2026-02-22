"""
Crash recovery tests — mirrors tests/test_crash_recovery.c.

A "crash" is simulated by opening a write transaction, writing data,
and closing the store WITHOUT calling commit(). The next open must see
only the committed state.

Covers: committed data survival, uncommitted rollback, explicit rollback,
multiple crash cycles, large transaction recovery, overwrite recovery,
and delete recovery — all in both WAL and DELETE journal modes.
"""

import pytest
from snkv import KVStore, JOURNAL_WAL, JOURNAL_DELETE


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _open(path: str, mode: int) -> KVStore:
    return KVStore(path, journal_mode=mode, busy_timeout=2000)


# ---------------------------------------------------------------------------
# Committed data survives
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("mode", [JOURNAL_WAL, JOURNAL_DELETE], ids=["WAL", "DELETE"])
def test_committed_survives(tmp_path, mode):
    """500 committed keys must all be present after close + reopen."""
    path = str(tmp_path / f"committed_{mode}.db")

    with _open(path, mode) as db:
        db.begin(write=True)
        for i in range(500):
            db[f"ok{i:06d}".encode()] = f"val{i}".encode()
        db.commit()

    with _open(path, mode) as db:
        db.integrity_check()
        for i in range(500):
            assert db.get(f"ok{i:06d}".encode()) == f"val{i}".encode()


# ---------------------------------------------------------------------------
# Uncommitted data rolled back on close
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("mode", [JOURNAL_WAL, JOURNAL_DELETE], ids=["WAL", "DELETE"])
def test_uncommitted_rolled_back(tmp_path, mode):
    """Batch1 (committed) must survive; batch2 (no commit) must be absent."""
    path = str(tmp_path / f"uncommit_{mode}.db")

    # batch1 — committed
    with _open(path, mode) as db:
        db.begin(write=True)
        for i in range(200):
            db[f"batch1_{i:06d}".encode()] = f"v{i}".encode()
        db.commit()

    # batch2 — no commit (simulated crash)
    with _open(path, mode) as db:
        db.begin(write=True)
        for i in range(300):
            db[f"batch2_{i:06d}".encode()] = f"x{i}".encode()
        # intentionally NOT calling commit

    with _open(path, mode) as db:
        for i in range(200):
            assert db.get(f"batch1_{i:06d}".encode()) == f"v{i}".encode()
        for i in range(300):
            assert db.get(f"batch2_{i:06d}".encode()) is None


# ---------------------------------------------------------------------------
# Explicit rollback
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("mode", [JOURNAL_WAL, JOURNAL_DELETE], ids=["WAL", "DELETE"])
def test_explicit_rollback(tmp_path, mode):
    """'keep' keys (committed) must survive; 'discard' keys (rolled back) must vanish."""
    path = str(tmp_path / f"rollback_{mode}.db")

    with _open(path, mode) as db:
        db.begin(write=True)
        for i in range(100):
            db[f"keep{i:05d}".encode()] = f"kv{i}".encode()
        db.commit()

        db.begin(write=True)
        for i in range(200):
            db[f"discard{i:05d}".encode()] = f"dv{i}".encode()
        db.rollback()

        for i in range(100):
            assert db.get(f"keep{i:05d}".encode()) == f"kv{i}".encode()
        for i in range(200):
            assert db.get(f"discard{i:05d}".encode()) is None


# ---------------------------------------------------------------------------
# Multiple crash cycles
# ---------------------------------------------------------------------------

def test_multiple_crash_cycles_wal(tmp_path):
    """5 WAL cycles: each commits 100 keys + leaves 50 uncommitted.
    Final open must see all 500 committed keys and zero uncommitted."""
    path = str(tmp_path / "multicycle.db")

    for cycle in range(5):
        with _open(path, JOURNAL_WAL) as db:
            # verify previous cycles
            for prev in range(cycle):
                for i in range(100):
                    key = f"c{prev}_{i:04d}".encode()
                    assert db.get(key) == f"v{prev}_{i}".encode(), \
                        f"missing from cycle {prev}: {key}"

            # commit this cycle's batch
            db.begin(write=True)
            for i in range(100):
                db[f"c{cycle}_{i:04d}".encode()] = f"v{cycle}_{i}".encode()
            db.commit()

            # start uncommitted batch (simulate crash)
            db.begin(write=True)
            for i in range(50):
                db[f"ghost{cycle}_{i:04d}".encode()] = b"lost"
            # no commit

    with _open(path, JOURNAL_WAL) as db:
        for cycle in range(5):
            for i in range(100):
                assert db.get(f"c{cycle}_{i:04d}".encode()) == \
                    f"v{cycle}_{i}".encode()
            for i in range(50):
                assert db.get(f"ghost{cycle}_{i:04d}".encode()) is None


# ---------------------------------------------------------------------------
# Large transaction recovery
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("mode", [JOURNAL_WAL, JOURNAL_DELETE], ids=["WAL", "DELETE"])
def test_large_txn_recovery(tmp_path, mode):
    """5 000 'ok' keys (committed) must survive; 5 000 'lost' keys (no commit) absent."""
    path = str(tmp_path / f"large_{mode}.db")

    with _open(path, mode) as db:
        db.begin(write=True)
        for i in range(5_000):
            db[f"large_ok_{i:07d}".encode()] = f"vok{i}".encode()
        db.commit()

    with _open(path, mode) as db:
        db.begin(write=True)
        for i in range(5_000):
            db[f"large_lost_{i:07d}".encode()] = f"vx{i}".encode()
        # no commit

    with _open(path, mode) as db:
        for i in range(5_000):
            assert db.get(f"large_ok_{i:07d}".encode()) == f"vok{i}".encode()
        for i in range(5_000):
            assert db.get(f"large_lost_{i:07d}".encode()) is None


# ---------------------------------------------------------------------------
# Overwrite recovery
# ---------------------------------------------------------------------------

def test_overwrite_recovery(tmp_path):
    """v1 committed → v2 committed → v3 not committed → reopen → v2."""
    path = str(tmp_path / "overwrite.db")

    with _open(path, JOURNAL_WAL) as db:
        db.begin(write=True)
        for i in range(200):
            db[f"ov{i:05d}".encode()] = f"v1_{i}".encode()
        db.commit()

    with _open(path, JOURNAL_WAL) as db:
        db.begin(write=True)
        for i in range(200):
            db[f"ov{i:05d}".encode()] = f"v2_{i}".encode()
        db.commit()

    with _open(path, JOURNAL_WAL) as db:
        db.begin(write=True)
        for i in range(200):
            db[f"ov{i:05d}".encode()] = f"v3_{i}".encode()
        # no commit

    with _open(path, JOURNAL_WAL) as db:
        for i in range(200):
            assert db.get(f"ov{i:05d}".encode()) == f"v2_{i}".encode()


# ---------------------------------------------------------------------------
# Delete recovery
# ---------------------------------------------------------------------------

def test_delete_recovery(tmp_path):
    """Committed deletes (0-99) must stay deleted; uncommitted deletes
    (100-199) must be restored; untouched keys (200-299) unchanged."""
    path = str(tmp_path / "del_recovery.db")

    with _open(path, JOURNAL_WAL) as db:
        db.begin(write=True)
        for i in range(300):
            db[f"dr{i:05d}".encode()] = f"val{i}".encode()
        db.commit()

    with _open(path, JOURNAL_WAL) as db:
        # commit deletes of 0-99
        db.begin(write=True)
        for i in range(100):
            db.delete(f"dr{i:05d}".encode())
        db.commit()

        # uncommitted deletes of 100-199
        db.begin(write=True)
        for i in range(100, 200):
            db.delete(f"dr{i:05d}".encode())
        # no commit

    with _open(path, JOURNAL_WAL) as db:
        # 0-99: committed deletion → must be gone
        for i in range(100):
            assert db.get(f"dr{i:05d}".encode()) is None

        # 100-199: uncommitted deletion → must be restored
        for i in range(100, 200):
            assert db.get(f"dr{i:05d}".encode()) == f"val{i}".encode()

        # 200-299: untouched → must be present
        for i in range(200, 300):
            assert db.get(f"dr{i:05d}".encode()) == f"val{i}".encode()
