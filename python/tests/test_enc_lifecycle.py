# SPDX-License-Identifier: Apache-2.0
"""
python/tests/test_enc_lifecycle.py

Full encryption lifecycle test:

  Phase 1 — ENCRYPTED   : open_encrypted → all KV operations
  Phase 2 — PLAINTEXT   : remove_encryption → same KV operations on same data
  Phase 3 — RE-ENCRYPTED: open_encrypted on plain store → same KV operations

Each phase validates:
  - put / get / delete / contains
  - forward iterator
  - reverse iterator
  - prefix iterator
  - seek iterator
  - TTL: put with ttl, get, remaining, expiry (lazy delete), purge_expired
  - column family: put / get / delete / iterator
  - put_if_absent
  - count
  - stats
"""

import time
import pytest
import snkv
from snkv import KVStore, AuthError, ColumnFamily

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tmpdb(tmp_path, name="enc_lifecycle.db"):
    return str(tmp_path / name)


def _expired_ms():
    """Return a timestamp 1 second in the past (always already expired)."""
    return int((time.time() - 1.0) * 1000)


def _future_ms(seconds=60):
    """Return a timestamp N seconds in the future."""
    return int((time.time() + seconds) * 1000)


# ---------------------------------------------------------------------------
# Core KV assertions — run identically in all three phases
# ---------------------------------------------------------------------------

def assert_put_get(db):
    db.put(b"k1", b"v1")
    db.put(b"k2", b"v2")
    db.put(b"k3", b"v3")
    assert db.get(b"k1") == b"v1"
    assert db.get(b"k2") == b"v2"
    assert db.get(b"k3") == b"v3"
    assert db.get(b"missing") is None


def assert_delete(db):
    db.put(b"del_me", b"gone")
    assert db.get(b"del_me") == b"gone"
    db.delete(b"del_me")
    assert db.get(b"del_me") is None


def assert_contains(db):
    db.put(b"present", b"1")
    assert b"present" in db
    assert b"absent_key" not in db


def assert_forward_iterator(db):
    db.put(b"iter:a", b"va")
    db.put(b"iter:b", b"vb")
    db.put(b"iter:c", b"vc")
    with db.iterator() as it:
        pairs = list(it)
    # At least our 3 keys are present (may have more from other assertions)
    keys = [k for k, _ in pairs]
    assert b"iter:a" in keys
    assert b"iter:b" in keys
    assert b"iter:c" in keys
    # Forward order: a < b < c
    a_pos = keys.index(b"iter:a")
    b_pos = keys.index(b"iter:b")
    c_pos = keys.index(b"iter:c")
    assert a_pos < b_pos < c_pos


def assert_reverse_iterator(db):
    db.put(b"rev:x", b"1")
    db.put(b"rev:y", b"2")
    db.put(b"rev:z", b"3")
    with db.iterator(reverse=True) as it:
        keys = [k for k, _ in it]
    x_pos = keys.index(b"rev:x")
    y_pos = keys.index(b"rev:y")
    z_pos = keys.index(b"rev:z")
    # Reverse: z < y < x in the list
    assert z_pos < y_pos < x_pos


def assert_prefix_iterator(db):
    db.put(b"pfx:1", b"p1")
    db.put(b"pfx:2", b"p2")
    db.put(b"pfx:3", b"p3")
    db.put(b"other:1", b"o1")
    with db.iterator(prefix=b"pfx:") as it:
        pairs = list(it)
    assert len(pairs) == 3
    assert all(k.startswith(b"pfx:") for k, _ in pairs)


def assert_seek_iterator(db):
    db.put(b"sk:a", b"1")
    db.put(b"sk:b", b"2")
    db.put(b"sk:c", b"3")
    with db.iterator() as it:
        it.seek(b"sk:b")
        assert not it.eof
        k = it.key
        v = it.value
    assert k == b"sk:b"
    assert v == b"2"


def assert_ttl(db):
    # Live key with TTL
    db.put(b"ttl:live", b"alive", ttl=60.0)
    val = db.get(b"ttl:live")
    assert val == b"alive"
    remaining = db.ttl(b"ttl:live")
    assert remaining is not None
    assert 0 < remaining <= 60.0

    # Expired key via direct C-level timestamp (epoch + 1 ms = always past)
    db._db.put_ttl(b"ttl:dead", b"ghost", 1)
    assert db.get(b"ttl:dead") is None   # lazy delete on get

    # No TTL key
    db.put(b"ttl:plain", b"forever")
    assert db.ttl(b"ttl:plain") is None


def assert_purge_expired(db):
    db._db.put_ttl(b"purge:1", b"v1", 1)
    db._db.put_ttl(b"purge:2", b"v2", 1)
    db.put(b"purge:live", b"live")
    n = db.purge_expired()
    assert n >= 2
    assert db.get(b"purge:live") == b"live"
    assert db.get(b"purge:1") is None
    assert db.get(b"purge:2") is None


def _open_or_create_cf(db, name):
    try:
        return db.create_column_family(name)
    except Exception:
        return db.open_column_family(name)


def assert_column_family(db):
    cf = _open_or_create_cf(db, "lifecycleCF")
    cf.clear()
    cf.put(b"cf:k1", b"cf:v1")
    cf.put(b"cf:k2", b"cf:v2")
    assert cf.get(b"cf:k1") == b"cf:v1"
    assert cf.get(b"cf:k2") == b"cf:v2"

    # Delete
    cf.delete(b"cf:k1")
    assert cf.get(b"cf:k1") is None

    # Iterator
    with cf.iterator() as it:
        pairs = list(it)
    assert len(pairs) == 1
    assert pairs[0] == (b"cf:k2", b"cf:v2")

    cf.close()


def assert_put_if_absent(db):
    # Ensure clean slate
    if db.get(b"pia:new") is not None:
        db.delete(b"pia:new")
    # Key does not exist → inserted
    inserted = db.put_if_absent(b"pia:new", b"newval")
    assert inserted is True
    assert db.get(b"pia:new") == b"newval"

    # Key already exists → not overwritten
    inserted = db.put_if_absent(b"pia:new", b"other")
    assert inserted is False
    assert db.get(b"pia:new") == b"newval"


def assert_count(db):
    cf = _open_or_create_cf(db, "countCF")
    cf.clear()
    assert cf.count() == 0
    cf.put(b"c1", b"v1")
    cf.put(b"c2", b"v2")
    cf.put(b"c3", b"v3")
    assert cf.count() == 3
    cf.delete(b"c1")
    assert cf.count() == 2
    cf.close()


def assert_stats(db):
    db.stats_reset()
    db.put(b"stat:k", b"stat:v")
    db.get(b"stat:k")
    s = db.stats()
    assert s["puts"] >= 1
    assert s["gets"] >= 1


# ---------------------------------------------------------------------------
# Full phase runner — called for each of the 3 phases
# ---------------------------------------------------------------------------

def run_all_assertions(db, label):
    """Run every KV assertion against `db`. `label` is used in error context."""
    assert_put_get(db)
    assert_delete(db)
    assert_contains(db)
    assert_forward_iterator(db)
    assert_reverse_iterator(db)
    assert_prefix_iterator(db)
    assert_seek_iterator(db)
    assert_ttl(db)
    assert_purge_expired(db)
    assert_column_family(db)
    assert_put_if_absent(db)
    assert_count(db)
    assert_stats(db)


# ---------------------------------------------------------------------------
# Phase 1: ENCRYPTED
# ---------------------------------------------------------------------------

def test_phase1_encrypted(tmp_path):
    path = _tmpdb(tmp_path)
    with KVStore.open_encrypted(path, b"secret") as db:
        assert db.is_encrypted() is True
        run_all_assertions(db, "phase1_encrypted")


# ---------------------------------------------------------------------------
# Phase 2: remove_encryption → PLAINTEXT
# ---------------------------------------------------------------------------

def test_phase2_plaintext(tmp_path):
    path = _tmpdb(tmp_path)

    # Setup: create encrypted store with seed data
    with KVStore.open_encrypted(path, b"secret") as db:
        db.put(b"seed:k", b"seed:v")

    # Transition: decrypt
    with KVStore.open_encrypted(path, b"secret") as db:
        db.remove_encryption()

    # Verify: now opens as plain, seed data intact
    with KVStore(path) as db:
        assert db.is_encrypted() is False
        assert db.get(b"seed:k") == b"seed:v"
        run_all_assertions(db, "phase2_plaintext")


# ---------------------------------------------------------------------------
# Phase 3: open_encrypted on plain store → RE-ENCRYPTED
# ---------------------------------------------------------------------------

def test_phase3_reencrypted(tmp_path):
    path = _tmpdb(tmp_path)

    # Setup: create encrypted, add data, remove encryption
    with KVStore.open_encrypted(path, b"first") as db:
        db.put(b"seed:k", b"seed:v")

    with KVStore.open_encrypted(path, b"first") as db:
        db.remove_encryption()

    # Transition: re-encrypt plain store with new password
    with KVStore.open_encrypted(path, b"second") as db:
        assert db.is_encrypted() is True
        # Seed data written before encryption must still be readable
        assert db.get(b"seed:k") == b"seed:v"
        run_all_assertions(db, "phase3_reencrypted")

    # Old password must fail
    with pytest.raises(AuthError):
        KVStore.open_encrypted(path, b"first")

    # New password must work
    with KVStore.open_encrypted(path, b"second") as db:
        assert db.is_encrypted() is True
        assert db.get(b"seed:k") == b"seed:v"


# ---------------------------------------------------------------------------
# Full lifecycle in one test: encrypted → plain → re-encrypted
# ---------------------------------------------------------------------------

def test_full_lifecycle(tmp_path):
    path = _tmpdb(tmp_path)
    pw1 = b"phase1pass"
    pw2 = b"phase3pass"

    # ── Phase 1: ENCRYPTED ──────────────────────────────────────────────────
    with KVStore.open_encrypted(path, pw1) as db:
        assert db.is_encrypted() is True
        db.put(b"p1:key", b"p1:val")
        db.put(b"persist", b"survives_all_phases")
        run_all_assertions(db, "lifecycle/phase1")

    # ── Transition 1→2: remove encryption ───────────────────────────────────
    with KVStore.open_encrypted(path, pw1) as db:
        db.remove_encryption()

    # ── Phase 2: PLAINTEXT ──────────────────────────────────────────────────
    with KVStore(path) as db:
        assert db.is_encrypted() is False
        # Data written in phase 1 survives decryption
        assert db.get(b"p1:key")   == b"p1:val"
        assert db.get(b"persist")  == b"survives_all_phases"
        db.put(b"p2:key", b"p2:val")
        run_all_assertions(db, "lifecycle/phase2")

    # ── Transition 2→3: re-encrypt plain store ───────────────────────────────
    with KVStore.open_encrypted(path, pw2) as db:
        assert db.is_encrypted() is True
        # Data from both prior phases survives re-encryption
        assert db.get(b"p1:key")  == b"p1:val"
        assert db.get(b"p2:key")  == b"p2:val"
        assert db.get(b"persist") == b"survives_all_phases"
        db.put(b"p3:key", b"p3:val")
        run_all_assertions(db, "lifecycle/phase3")

    # Phase 1 password must no longer work
    with pytest.raises(AuthError):
        KVStore.open_encrypted(path, pw1)

    # Phase 3 password reopens with all data intact
    with KVStore.open_encrypted(path, pw2) as db:
        assert db.get(b"p1:key")  == b"p1:val"
        assert db.get(b"p2:key")  == b"p2:val"
        assert db.get(b"p3:key")  == b"p3:val"
        assert db.get(b"persist") == b"survives_all_phases"


# ---------------------------------------------------------------------------
# Test 21 updated: open_encrypted on plain file now ENCRYPTS (not AuthError)
# ---------------------------------------------------------------------------

def test_open_enc_on_plain_file_encrypts(tmp_path):
    """
    open_encrypted on a never-encrypted plain store now encrypts the data
    instead of raising AuthError.
    """
    path = _tmpdb(tmp_path)
    db = KVStore(path)
    db.put(b"existing", b"data")
    db.close()

    # Should succeed and encrypt
    with KVStore.open_encrypted(path, b"pw") as db:
        assert db.is_encrypted() is True
        assert db.get(b"existing") == b"data"

    # Wrong password must fail
    with pytest.raises(AuthError):
        KVStore.open_encrypted(path, b"wrong")

    # Correct password works
    with KVStore.open_encrypted(path, b"pw") as db:
        assert db.get(b"existing") == b"data"
