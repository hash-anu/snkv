"""
JSON document storage tests — mirrors tests/test_json.c.

Verifies that SNKV correctly stores, retrieves, and preserves large and
complex JSON documents (stored as raw bytes/strings). Uses Python's json
module for generation and validation.
"""

import json
import random
import time
import pytest
from snkv import KeyValueStore, JOURNAL_WAL


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_records(n: int, seed: int = 0) -> dict:
    """Generate n records as a JSON-serialisable dict."""
    rng = random.Random(seed)
    return {
        str(i): {
            "id":    i,
            "name":  f"user_{i}",
            "email": f"user{i}@example.com",
            "score": rng.uniform(0.0, 100.0),
            "active": rng.choice([True, False]),
            "tags":  [f"tag{rng.randint(0, 9)}" for _ in range(3)],
        }
        for i in range(n)
    }


def _make_nested(depth: int) -> dict:
    """Build a dict nested to the given depth."""
    node: dict = {"value": depth, "data": f"level_{depth}"}
    for d in range(depth - 1, 0, -1):
        node = {"value": d, "data": f"level_{d}", "child": node}
    return node


def _json_bytes(obj) -> bytes:
    return json.dumps(obj, separators=(",", ":")).encode()


def _from_bytes(data: bytes):
    return json.loads(data.decode())


# ---------------------------------------------------------------------------
# Basic JSON storage
# ---------------------------------------------------------------------------

def test_basic_json_storage(tmp_path):
    """Store and retrieve a 1 000-record JSON document, verify byte equality."""
    path = str(tmp_path / "json_basic.db")
    doc = _make_records(1_000)
    raw = _json_bytes(doc)

    with KeyValueStore(path) as db:
        db[b"doc"] = raw
        result = db[b"doc"]

    assert result == raw
    parsed = _from_bytes(result)
    assert parsed["0"]["id"] == 0
    assert len(parsed) == 1_000


# ---------------------------------------------------------------------------
# Multiple JSON documents
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("n_records", [10, 100, 500, 1_000, 5_000])
def test_multiple_json_documents(tmp_path, n_records):
    """Each document must round-trip with byte-exact equality."""
    path = str(tmp_path / "multi_json.db")
    docs = {
        f"doc_{n_records}_{i}": _json_bytes(_make_records(n_records, seed=i))
        for i in range(5)
    }

    with KeyValueStore(path) as db:
        for key, val in docs.items():
            db[key.encode()] = val

    with KeyValueStore(path) as db:
        for key, expected in docs.items():
            assert db[key.encode()] == expected


# ---------------------------------------------------------------------------
# Column family organisation
# ---------------------------------------------------------------------------

def test_json_column_families(tmp_path):
    """User JSON and product JSON stored in separate CFs must not mix."""
    path = str(tmp_path / "json_cf.db")

    users    = {f"user:{i}": _json_bytes({"id": i, "name": f"user_{i}"})
                for i in range(5)}
    products = {f"prod:{i}": _json_bytes({"sku": i, "price": i * 9.99})
                for i in range(5)}

    with KeyValueStore(path) as db:
        with db.create_column_family("users") as cf:
            for k, v in users.items():
                cf[k.encode()] = v

        with db.create_column_family("products") as cf:
            for k, v in products.items():
                cf[k.encode()] = v

    with KeyValueStore(path) as db:
        with db.open_column_family("users") as cf:
            for k, expected in users.items():
                result = cf[k.encode()]
                assert result == expected
                parsed = _from_bytes(result)
                assert "id" in parsed

            # products must not be accessible from users CF
            for k in products:
                assert cf.get(k.encode()) is None

        with db.open_column_family("products") as cf:
            for k, expected in products.items():
                result = cf[k.encode()]
                assert result == expected
                parsed = _from_bytes(result)
                assert "sku" in parsed

    # list CFs
    with KeyValueStore(path) as db:
        names = db.list_column_families()
        assert "users" in names
        assert "products" in names


# ---------------------------------------------------------------------------
# Nested JSON
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("depth", [5, 10, 20, 30])
def test_nested_json(tmp_path, depth):
    """Deeply nested JSON must round-trip with byte equality."""
    path = str(tmp_path / f"nested_{depth}.db")
    doc = _make_nested(depth)
    raw = _json_bytes(doc)

    with KeyValueStore(path) as db:
        db[f"nested_{depth}".encode()] = raw
        result = db[f"nested_{depth}".encode()]

    assert result == raw
    parsed = _from_bytes(result)
    assert parsed["value"] == 1


# ---------------------------------------------------------------------------
# Batch JSON operations
# ---------------------------------------------------------------------------

def test_batch_json_operations(tmp_path):
    """100 small JSON documents inserted in one transaction; verify 5 random."""
    path = str(tmp_path / "batch_json.db")
    docs = {
        f"item:{i:04d}": _json_bytes({"id": i, "val": f"value_{i}"})
        for i in range(100)
    }

    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        db.begin(write=True)
        for k, v in docs.items():
            db[k.encode()] = v
        db.commit()

        rng = random.Random(13)
        for _ in range(5):
            idx = rng.randint(0, 99)
            key = f"item:{idx:04d}".encode()
            result = db[key]
            assert result == docs[f"item:{idx:04d}"]
            parsed = _from_bytes(result)
            assert parsed["id"] == idx

        # iterate and count valid JSON values
        valid = sum(
            1 for _, v in db.iterator()
            if _from_bytes(v) is not None
        )
        assert valid == 100


# ---------------------------------------------------------------------------
# Very large JSON (~5 MB)
# ---------------------------------------------------------------------------

def test_very_large_json(tmp_path):
    """~50 000-record JSON document (~5 MB) must round-trip with byte equality."""
    path = str(tmp_path / "large_json.db")
    doc = _make_records(50_000, seed=7)
    raw = _json_bytes(doc)
    size_mb = len(raw) / 1024 / 1024

    with KeyValueStore(path, journal_mode=JOURNAL_WAL) as db:
        t0 = time.monotonic()
        db[b"large"] = raw
        db.sync()
        write_s = time.monotonic() - t0

        t1 = time.monotonic()
        result = db[b"large"]
        read_s = time.monotonic() - t1

    assert result == raw
    parsed = _from_bytes(result)
    assert len(parsed) == 50_000

    print(
        f"\n  large JSON: {size_mb:.1f} MB  "
        f"write={write_s:.2f}s  read={read_s:.2f}s"
    )


# ---------------------------------------------------------------------------
# JSON integrity after transactions
# ---------------------------------------------------------------------------

def test_json_transaction_rollback(tmp_path):
    """A rolled-back JSON write must not be visible."""
    path = str(tmp_path / "json_rb.db")
    original = _json_bytes({"version": 1})
    updated  = _json_bytes({"version": 2})

    with KeyValueStore(path) as db:
        db[b"doc"] = original

        db.begin(write=True)
        db[b"doc"] = updated
        db.rollback()

        assert db[b"doc"] == original


def test_json_persistence_across_sessions(tmp_path):
    """JSON written in session 1 must be identical in session 2."""
    path = str(tmp_path / "json_persist.db")
    doc = _make_records(500)
    raw = _json_bytes(doc)

    with KeyValueStore(path) as db:
        db[b"snapshot"] = raw

    with KeyValueStore(path) as db:
        result = db[b"snapshot"]
        assert result == raw
        assert _from_bytes(result) == doc
