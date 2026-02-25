"""
Stress and edge-case tests — mirrors tests/test_stress.c.

Covers edge cases (empty value, binary keys, null bytes, large payloads),
high-volume write storms, large datasets, rapid open/close cycles,
mode-switch persistence, transaction cycling, mixed workloads,
CF stress, and growing value sizes.
"""

import random
import threading
import pytest
from snkv import KeyValueStore, JOURNAL_WAL, JOURNAL_DELETE, NotFoundError


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_edge_empty_value(tmp_path):
    """A key with an empty (zero-byte) value must round-trip correctly."""
    path = str(tmp_path / "edge.db")
    with KeyValueStore(path) as db:
        db[b"empty"] = b""
        assert db[b"empty"] == b""


def test_edge_binary_key_with_nulls(tmp_path):
    """A key containing embedded null bytes must be stored and retrieved."""
    path = str(tmp_path / "null_key.db")
    key = bytes([0x01, 0x00, 0x02, 0x00, 0x03])
    with KeyValueStore(path) as db:
        db[key] = b"binary_val"
        assert db[key] == b"binary_val"


def test_edge_single_byte_key(tmp_path):
    """A single-byte key (0xFF) must work."""
    path = str(tmp_path / "single.db")
    with KeyValueStore(path) as db:
        db[bytes([0xFF])] = b"ff_value"
        assert db[bytes([0xFF])] == b"ff_value"


def test_edge_large_key_and_value(tmp_path):
    """1 KB key + 1 MB value must round-trip byte-for-byte."""
    path = str(tmp_path / "bigkv.db")
    key   = bytes(range(256)) * 4                    # 1 KB
    value = bytes(range(256)) * (1024 * 1024 // 256) # 1 MB
    with KeyValueStore(path) as db:
        db[key] = value
        assert db[key] == value


def test_edge_overwrite_100_times(tmp_path):
    """100 successive overwrites of the same key — only the last survives."""
    path = str(tmp_path / "ow100.db")
    with KeyValueStore(path) as db:
        for i in range(100):
            db[b"k"] = f"version-{i}".encode()
        assert db[b"k"] == b"version-99"


def test_edge_get_nonexistent(tmp_path):
    path = str(tmp_path / "nokey.db")
    with KeyValueStore(path) as db:
        assert db.get(b"ghost") is None


def test_edge_delete_nonexistent(tmp_path):
    path = str(tmp_path / "nodel.db")
    with KeyValueStore(path) as db:
        with pytest.raises(KeyError):
            db.delete(b"ghost")


def test_edge_put_after_delete(tmp_path):
    """A key can be reinserted after deletion and must return the new value."""
    path = str(tmp_path / "readd.db")
    with KeyValueStore(path) as db:
        db[b"k"] = b"first"
        db.delete(b"k")
        db[b"k"] = b"second"
        assert db[b"k"] == b"second"


# ---------------------------------------------------------------------------
# Write storm
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("journal_mode", [JOURNAL_WAL, JOURNAL_DELETE],
                         ids=["WAL", "DELETE"])
def test_write_storm_100k(tmp_path, journal_mode):
    """100 000 keys written in batches of 1 000 — spot-check 4 random keys."""
    path = str(tmp_path / f"storm_{journal_mode}.db")
    N = 100_000
    rng = random.Random(1)

    with KeyValueStore(path, journal_mode=journal_mode) as db:
        for batch_start in range(0, N, 1_000):
            db.begin(write=True)
            for i in range(batch_start, batch_start + 1_000):
                db[f"storm{i:07d}".encode()] = f"sv{i}".encode()
            db.commit()

        for _ in range(4):
            i = rng.randint(0, N - 1)
            assert db.get(f"storm{i:07d}".encode()) == f"sv{i}".encode()

        db.integrity_check()


# ---------------------------------------------------------------------------
# Large dataset
# ---------------------------------------------------------------------------

def test_large_dataset_50k(tmp_path):
    """50 000 keys: full scan, delete first half, verify second half intact."""
    path = str(tmp_path / "large50k.db")
    N = 50_000

    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        db.begin(write=True)
        for i in range(N):
            db[f"ld{i:07d}".encode()] = f"lv{i}".encode()
        db.commit()

        count = sum(1 for _ in db.iterator())
        assert count == N

        # spot-check 10 random keys
        rng = random.Random(99)
        for _ in range(10):
            i = rng.randint(0, N - 1)
            assert db.get(f"ld{i:07d}".encode()) == f"lv{i}".encode()

        # delete first half
        db.begin(write=True)
        for i in range(0, N // 2, 1):
            db.delete(f"ld{i:07d}".encode())
        db.commit()

        for i in range(N // 2, N):
            assert db.get(f"ld{i:07d}".encode()) == f"lv{i}".encode()

        for i in range(0, 10):
            assert db.get(f"ld{i:07d}".encode()) is None

        db.integrity_check()


# ---------------------------------------------------------------------------
# Uncommitted data discarded on close
# ---------------------------------------------------------------------------

def test_crash_uncommitted_rolled_back(tmp_path):
    """Uncommitted data must be absent after close + reopen."""
    path = str(tmp_path / "crash.db")

    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        db[b"committed"] = b"safe"

    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        db.begin(write=True)
        db[b"lost"] = b"never_committed"
        # close without commit

    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        assert db[b"committed"] == b"safe"
        assert db.get(b"lost") is None


# ---------------------------------------------------------------------------
# Rapid open / close
# ---------------------------------------------------------------------------

def test_rapid_open_close_50_cycles(tmp_path):
    """50 open/close cycles must preserve a 'persist' key throughout."""
    path = str(tmp_path / "rapid.db")

    with KeyValueStore(path) as db:
        db[b"persist"] = b"anchor"

    for cycle in range(50):
        with KeyValueStore(path) as db:
            assert db[b"persist"] == b"anchor"
            if cycle % 10 == 0:
                db[f"c{cycle}".encode()] = b"v"

    with KeyValueStore(path) as db:
        db.integrity_check()


# ---------------------------------------------------------------------------
# Mode-switch persistence
# ---------------------------------------------------------------------------

def test_mode_switch_delete_wal_delete(tmp_path):
    """Data written in DELETE mode, WAL mode, then back to DELETE — all survive."""
    path = str(tmp_path / "mswitch.db")

    with KeyValueStore(path, journal_mode=JOURNAL_DELETE) as db:
        db[b"from_delete"] = b"d"

    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        assert db[b"from_delete"] == b"d"
        db[b"from_wal"] = b"w"

    with KeyValueStore(path, journal_mode=JOURNAL_DELETE) as db:
        assert db[b"from_delete"] == b"d"
        assert db[b"from_wal"]    == b"w"


# ---------------------------------------------------------------------------
# Transaction cycling
# ---------------------------------------------------------------------------

def test_transaction_cycling_1000(tmp_path):
    """1 000 individual begin/put/commit cycles — last key must be readable."""
    path = str(tmp_path / "txcycle.db")
    N = 1_000
    with KeyValueStore(path) as db:
        for i in range(N):
            db.begin(write=True)
            db[f"cyc{i:06d}".encode()] = f"cv{i}".encode()
            db.commit()

        assert db.get(f"cyc{N-1:06d}".encode()) == f"cv{N-1}".encode()
        db.integrity_check()


def test_rollback_cycling_never_persists(tmp_path):
    """1 000 begin/put/rollback cycles — the ephemeral key must never persist."""
    path = str(tmp_path / "rbcycle.db")
    with KeyValueStore(path) as db:
        db[b"anchor"] = b"stable"

        for i in range(1_000):
            db.begin(write=True)
            db[b"ephemeral"] = f"v{i}".encode()
            db.rollback()

        assert db[b"anchor"] == b"stable"
        assert db.get(b"ephemeral") is None


# ---------------------------------------------------------------------------
# Mixed workload
# ---------------------------------------------------------------------------

def test_mixed_workload_10k_ops(tmp_path):
    """10 000 random ops (40% put, 30% get, 15% delete, 15% exists)."""
    path = str(tmp_path / "mixed.db")
    rng = random.Random(7)
    key_space = [f"mk{i:05d}".encode() for i in range(500)]

    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        # seed with some initial data
        db.begin(write=True)
        for k in key_space[:100]:
            db[k] = b"init"
        db.commit()

        errors = 0
        batch_count = 0
        db.begin(write=True)

        for _ in range(10_000):
            key = rng.choice(key_space)
            op  = rng.random()

            try:
                if op < 0.40:
                    db[key] = b"v"
                elif op < 0.70:
                    db.get(key)
                elif op < 0.85:
                    if db.exists(key):
                        db.delete(key)
                else:
                    db.exists(key)
            except Exception:
                errors += 1

            batch_count += 1
            if batch_count >= 500:
                db.commit()
                db.begin(write=True)
                batch_count = 0

        db.commit()

        assert errors == 0
        db.integrity_check()


# ---------------------------------------------------------------------------
# CF stress
# ---------------------------------------------------------------------------

def test_cf_stress_10cfs(tmp_path):
    """10 CFs, 200 keys each — isolation and correct counts."""
    path = str(tmp_path / "cfstress.db")
    N_CFS = 10
    N_KEYS = 200

    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        for cf_idx in range(N_CFS):
            with db.create_column_family(f"cf{cf_idx}") as cf:
                db.begin(write=True)
                for k in range(N_KEYS):
                    cf[f"key{k:04d}".encode()] = f"cf{cf_idx}_{k}".encode()
                db.commit()

        for cf_idx in range(N_CFS):
            with db.open_column_family(f"cf{cf_idx}") as cf:
                count = sum(1 for _ in cf.iterator())
                assert count == N_KEYS, f"cf{cf_idx}: expected {N_KEYS}, got {count}"

        # CF isolation: key from CF0 must NOT be in CF1
        with db.open_column_family("cf0") as cf0:
            with db.open_column_family("cf1") as cf1:
                for k in range(5):
                    key = f"key{k:04d}".encode()
                    expected0 = f"cf0_{k}".encode()
                    assert cf0[key] == expected0
                    assert cf1.get(key) != expected0


# ---------------------------------------------------------------------------
# Growing value sizes
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("size", [1, 10, 100, 1024, 4096, 10_240,
                                   65_536, 262_144, 524_288])
def test_growing_value_sizes(tmp_path, size):
    """Values at each size must round-trip exactly."""
    path = str(tmp_path / f"grow_{size}.db")
    value = bytes((i % 256) for i in range(size))
    with KeyValueStore(path) as db:
        db[b"big"] = value
        assert db[b"big"] == value
