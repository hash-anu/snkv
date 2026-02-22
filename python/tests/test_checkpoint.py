"""
WAL checkpoint tests — mirrors tests/test_checkpoint.c.

Covers all four checkpoint modes, walSizeLimit auto-checkpoint,
checkpoint return values, and edge cases (write txn open, DELETE journal).
"""

import os
import pytest
import snkv
from snkv import (
    KVStore,
    JOURNAL_WAL,
    JOURNAL_DELETE,
    CHECKPOINT_PASSIVE,
    CHECKPOINT_FULL,
    CHECKPOINT_RESTART,
    CHECKPOINT_TRUNCATE,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _wal_path(db_path: str) -> str:
    return db_path + "-wal"


def _write_n(db: KVStore, n: int, prefix: str = "k") -> None:
    db.begin(write=True)
    for i in range(n):
        db[f"{prefix}{i:06d}".encode()] = f"v{i}".encode()
    db.commit()


# ---------------------------------------------------------------------------
# Passive checkpoint
# ---------------------------------------------------------------------------

def test_passive_checkpoint_returns_ok(tmp_path):
    """PASSIVE checkpoint after writes must return non-negative nLog/nCkpt."""
    path = str(tmp_path / "passive.db")
    with KVStore(path, journal_mode=JOURNAL_WAL) as db:
        _write_n(db, 50)
        nlog, nckpt = db.checkpoint(CHECKPOINT_PASSIVE)
        assert nlog  >= 0
        assert nckpt >= 0


def test_full_checkpoint(tmp_path):
    path = str(tmp_path / "full.db")
    with KVStore(path, journal_mode=JOURNAL_WAL) as db:
        _write_n(db, 50)
        nlog, nckpt = db.checkpoint(CHECKPOINT_FULL)
        assert nlog  >= 0
        assert nckpt >= 0


def test_restart_checkpoint(tmp_path):
    path = str(tmp_path / "restart.db")
    with KVStore(path, journal_mode=JOURNAL_WAL) as db:
        _write_n(db, 50)
        nlog, nckpt = db.checkpoint(CHECKPOINT_RESTART)
        assert nlog  >= 0
        assert nckpt >= 0


# ---------------------------------------------------------------------------
# Truncate checkpoint
# ---------------------------------------------------------------------------

def test_truncate_checkpoint_clears_wal(tmp_path):
    """TRUNCATE checkpoint must result in nLog == 0 (WAL fully cleared)."""
    path = str(tmp_path / "trunc.db")
    with KVStore(path, journal_mode=JOURNAL_WAL) as db:
        _write_n(db, 100)
        nlog, nckpt = db.checkpoint(CHECKPOINT_TRUNCATE)
        assert nlog == 0


def test_truncate_removes_wal_file(tmp_path):
    """After TRUNCATE + close, the -wal file should not exist (or be empty)."""
    path = str(tmp_path / "trunc_file.db")
    with KVStore(path, journal_mode=JOURNAL_WAL) as db:
        _write_n(db, 50)
        db.checkpoint(CHECKPOINT_TRUNCATE)

    wal = _wal_path(path)
    # WAL file is either absent or zero-sized after truncate + close
    if os.path.exists(wal):
        assert os.path.getsize(wal) == 0


# ---------------------------------------------------------------------------
# walSizeLimit auto-checkpoint
# ---------------------------------------------------------------------------

def test_wal_size_limit_auto_checkpoints(tmp_path):
    """walSizeLimit=10 causes auto-checkpoint; manual checkpoint afterward
    should see pnLog == pnCkpt (all frames already flushed)."""
    path = str(tmp_path / "auto.db")
    with KVStore(path, journal_mode=JOURNAL_WAL, wal_size_limit=10) as db:
        _write_n(db, 50)   # ~5 auto-checkpoints triggered
        nlog, nckpt = db.checkpoint(CHECKPOINT_PASSIVE)
        assert nlog == nckpt  # everything was already checkpointed


def test_wal_size_limit_zero_no_auto_checkpoint(tmp_path):
    """walSizeLimit=0 must NOT auto-checkpoint; WAL must grow while open."""
    path = str(tmp_path / "noauto.db")
    with KVStore(path, journal_mode=JOURNAL_WAL, wal_size_limit=0) as db:
        _write_n(db, 100)
        # check while the connection is still open (before close auto-checkpoint)
        wal = _wal_path(path)
        assert os.path.exists(wal), "WAL file must exist while store is open"
        assert os.path.getsize(wal) > 0, "WAL must have grown"


# ---------------------------------------------------------------------------
# Checkpoint during active write transaction
# ---------------------------------------------------------------------------

def test_checkpoint_during_write_transaction_raises(tmp_path):
    """Calling checkpoint while a write transaction is open must raise."""
    path = str(tmp_path / "busy.db")
    with KVStore(path, journal_mode=JOURNAL_WAL) as db:
        db.begin(write=True)
        db[b"k"] = b"v"
        with pytest.raises(snkv.Error):
            db.checkpoint(CHECKPOINT_PASSIVE)
        db.rollback()


# ---------------------------------------------------------------------------
# Checkpoint on DELETE-journal database
# ---------------------------------------------------------------------------

def test_checkpoint_delete_journal_is_noop(tmp_path):
    """checkpoint() on a DELETE-mode DB must return (0, 0) without error."""
    path = str(tmp_path / "del.db")
    with KVStore(path, journal_mode=JOURNAL_DELETE) as db:
        _write_n(db, 20)
        nlog, nckpt = db.checkpoint(CHECKPOINT_PASSIVE)
        assert nlog  == 0
        assert nckpt == 0


# ---------------------------------------------------------------------------
# Data integrity after checkpoint
# ---------------------------------------------------------------------------

def test_data_readable_after_checkpoint(tmp_path):
    """All data written before a checkpoint must remain readable after."""
    path = str(tmp_path / "readable.db")
    with KVStore(path, journal_mode=JOURNAL_WAL) as db:
        _write_n(db, 100)
        db.checkpoint(CHECKPOINT_TRUNCATE)

        for i in range(100):
            assert db.get(f"k{i:06d}".encode()) == f"v{i}".encode()


def test_data_readable_after_reopen_post_checkpoint(tmp_path):
    """Data must survive checkpoint + close + reopen."""
    path = str(tmp_path / "persist.db")
    with KVStore(path, journal_mode=JOURNAL_WAL) as db:
        _write_n(db, 50)
        db.checkpoint(CHECKPOINT_TRUNCATE)

    with KVStore(path, journal_mode=JOURNAL_WAL) as db:
        for i in range(50):
            assert db.get(f"k{i:06d}".encode()) == f"v{i}".encode()
