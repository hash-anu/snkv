# SPDX-License-Identifier: Apache-2.0
"""
python/tests/test_enc.py — Encryption regression suite (Python layer)

Tests:
  1.  open_encrypted creates encrypted store; is_encrypted() is True
  2.  Re-open with correct password succeeds
  3.  Re-open with wrong password raises AuthError
  4.  Plaintext NOT in file
  5.  put/get round-trip on encrypted store (bytes keys/values)
  6.  put/get round-trip — str keys/values auto-encoded
  7.  get returns None for missing key on encrypted store
  8.  delete removes key; subsequent get returns None
  9.  __setitem__ / __getitem__ / __delitem__ on encrypted store
 10.  __contains__ on encrypted store
 11.  Iterator yields correct (key, value) pairs
 12.  Prefix iterator on encrypted store
 13.  Reverse iterator on encrypted store
 14.  TTL put/get round-trip on encrypted store
 15.  Expired key returns None (lazy delete)
 16.  purge_expired on encrypted store
 17.  reencrypt — old password fails, new password works
 18.  Data intact after reencrypt
 19.  remove_encryption — store readable without password
 20.  is_encrypted() returns False on plain store
 21.  open_encrypted on plain file raises AuthError
 22.  Context manager (__enter__/__exit__) on encrypted store
 23.  Transaction begin/commit on encrypted store
 24.  stats() includes nPuts > 0 after writes
 25.  Empty value round-trip on encrypted store
"""

import os
import sys
import tempfile
import pytest

import snkv
from snkv import KVStore, AuthError, NotFoundError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tmpdb(tmp_path, name="test.db"):
    return str(tmp_path / name)


# ---------------------------------------------------------------------------
# Test 1: create encrypted store
# ---------------------------------------------------------------------------
def test_create_encrypted(tmp_path):
    path = _tmpdb(tmp_path)
    db = KVStore.open_encrypted(path, b"password")
    assert db.is_encrypted() is True
    db.close()


# ---------------------------------------------------------------------------
# Test 2: re-open correct password
# ---------------------------------------------------------------------------
def test_reopen_correct_password(tmp_path):
    path = _tmpdb(tmp_path)
    db = KVStore.open_encrypted(path, b"secret")
    db.close()
    db = KVStore.open_encrypted(path, b"secret")
    assert db.is_encrypted() is True
    db.close()


# ---------------------------------------------------------------------------
# Test 3: wrong password raises AuthError
# ---------------------------------------------------------------------------
def test_wrong_password(tmp_path):
    path = _tmpdb(tmp_path)
    db = KVStore.open_encrypted(path, b"correct")
    db.close()
    with pytest.raises(AuthError):
        KVStore.open_encrypted(path, b"wrong")


# ---------------------------------------------------------------------------
# Test 4: plaintext NOT in raw file
# ---------------------------------------------------------------------------
def test_ciphertext_in_file(tmp_path):
    path = _tmpdb(tmp_path)
    db = KVStore.open_encrypted(path, b"pw")
    db.put(b"secret_key", b"plaintext_value")
    db.close()
    with open(path, "rb") as f:
        raw = f.read()
    assert b"plaintext_value" not in raw


# ---------------------------------------------------------------------------
# Test 5: bytes put/get round-trip
# ---------------------------------------------------------------------------
def test_bytes_roundtrip(tmp_path):
    path = _tmpdb(tmp_path)
    with KVStore.open_encrypted(path, b"pw") as db:
        db.put(b"hello", b"world")
        assert db.get(b"hello") == b"world"


# ---------------------------------------------------------------------------
# Test 6: str keys/values auto-encoded
# ---------------------------------------------------------------------------
def test_str_roundtrip(tmp_path):
    path = _tmpdb(tmp_path)
    with KVStore.open_encrypted(path, "pw") as db:
        db.put("greeting", "hello")
        assert db.get("greeting") == b"hello"


# ---------------------------------------------------------------------------
# Test 7: get returns None for missing key
# ---------------------------------------------------------------------------
def test_get_missing(tmp_path):
    path = _tmpdb(tmp_path)
    with KVStore.open_encrypted(path, b"pw") as db:
        assert db.get(b"nokey") is None


# ---------------------------------------------------------------------------
# Test 8: delete
# ---------------------------------------------------------------------------
def test_delete(tmp_path):
    path = _tmpdb(tmp_path)
    with KVStore.open_encrypted(path, b"pw") as db:
        db.put(b"k", b"v")
        db.delete(b"k")
        assert db.get(b"k") is None


# ---------------------------------------------------------------------------
# Test 9: __setitem__ / __getitem__ / __delitem__
# ---------------------------------------------------------------------------
def test_dict_interface(tmp_path):
    path = _tmpdb(tmp_path)
    with KVStore.open_encrypted(path, b"pw") as db:
        db["mykey"] = "myval"
        assert db["mykey"] == b"myval"
        del db["mykey"]
        assert db.get("mykey") is None


# ---------------------------------------------------------------------------
# Test 10: __contains__
# ---------------------------------------------------------------------------
def test_contains(tmp_path):
    path = _tmpdb(tmp_path)
    with KVStore.open_encrypted(path, b"pw") as db:
        db.put(b"present", b"1")
        assert b"present" in db
        assert b"absent" not in db


# ---------------------------------------------------------------------------
# Test 11: iterator
# ---------------------------------------------------------------------------
def test_iterator(tmp_path):
    path = _tmpdb(tmp_path)
    with KVStore.open_encrypted(path, b"pw") as db:
        db.put(b"a", b"aval")
        db.put(b"b", b"bval")
        db.put(b"c", b"cval")
        with db.iterator() as it:
            pairs = list(it)
        assert len(pairs) == 3
        assert all(len(v) == 4 for _, v in pairs)


# ---------------------------------------------------------------------------
# Test 12: prefix iterator
# ---------------------------------------------------------------------------
def test_prefix_iterator(tmp_path):
    path = _tmpdb(tmp_path)
    with KVStore.open_encrypted(path, b"pw") as db:
        db.put(b"user:1", b"alice")
        db.put(b"user:2", b"bob")
        db.put(b"other",  b"x")
        with db.iterator(prefix=b"user:") as it:
            pairs = list(it)
        assert len(pairs) == 2


# ---------------------------------------------------------------------------
# Test 13: reverse iterator
# ---------------------------------------------------------------------------
def test_reverse_iterator(tmp_path):
    path = _tmpdb(tmp_path)
    with KVStore.open_encrypted(path, b"pw") as db:
        db.put(b"a", b"1")
        db.put(b"b", b"2")
        db.put(b"c", b"3")
        with db.iterator(reverse=True) as it:
            keys = [k for k, _ in it]
        assert keys == [b"c", b"b", b"a"]


# ---------------------------------------------------------------------------
# Test 14: TTL put/get round-trip
# ---------------------------------------------------------------------------
def test_ttl_roundtrip(tmp_path):
    path = _tmpdb(tmp_path)
    with KVStore.open_encrypted(path, b"pw") as db:
        db.put(b"session", b"tok", ttl=60.0)
        val = db.get(b"session")
        assert val == b"tok"
        remaining = db.ttl(b"session")
        assert remaining is not None
        assert 0 < remaining <= 60.0


# ---------------------------------------------------------------------------
# Test 15: expired key returns None
# ---------------------------------------------------------------------------
def test_ttl_expiry(tmp_path):
    path = _tmpdb(tmp_path)
    import snkv._snkv as _snkv_mod
    with KVStore.open_encrypted(path, b"pw") as db:
        # Use put_ttl with an already-expired timestamp
        expire_ms = _snkv_mod.KVStore(_tmpdb(tmp_path, "tmp.db")).put  # just to get import
        # Direct C-level: put with ttl=-0.001 effectively past
        db._db.put_ttl(b"old", b"data", 1)  # expire_ms=1 = Jan 1 1970 + 1ms
        val = db.get(b"old")
        assert val is None


# ---------------------------------------------------------------------------
# Test 16: purge_expired
# ---------------------------------------------------------------------------
def test_purge_expired(tmp_path):
    path = _tmpdb(tmp_path)
    import snkv._snkv as _snkv
    with KVStore.open_encrypted(path, b"pw") as db:
        # Expired entries via direct C API
        db._db.put_ttl(b"ex1", b"v1", 1)
        db._db.put_ttl(b"ex2", b"v2", 1)
        db.put(b"live", b"lv")
        n = db.purge_expired()
        assert n == 2
        assert db.get(b"live") == b"lv"


# ---------------------------------------------------------------------------
# Test 17: reencrypt — old fails, new succeeds
# ---------------------------------------------------------------------------
def test_reencrypt_passwords(tmp_path):
    path = _tmpdb(tmp_path)
    db = KVStore.open_encrypted(path, b"oldpass")
    db.reencrypt(b"newpass")
    db.close()

    with pytest.raises(AuthError):
        KVStore.open_encrypted(path, b"oldpass")

    db = KVStore.open_encrypted(path, b"newpass")
    assert db.is_encrypted() is True
    db.close()


# ---------------------------------------------------------------------------
# Test 18: data intact after reencrypt
# ---------------------------------------------------------------------------
def test_reencrypt_data_intact(tmp_path):
    path = _tmpdb(tmp_path)
    db = KVStore.open_encrypted(path, b"p1")
    db.put(b"fruit", b"mango")
    db.reencrypt(b"p2")
    db.close()

    db = KVStore.open_encrypted(path, b"p2")
    assert db.get(b"fruit") == b"mango"
    db.close()


# ---------------------------------------------------------------------------
# Test 19: remove_encryption → readable as plaintext
# ---------------------------------------------------------------------------
def test_remove_encryption(tmp_path):
    path = _tmpdb(tmp_path)
    db = KVStore.open_encrypted(path, b"pw")
    db.put(b"key", b"val")
    db.remove_encryption()
    db.close()

    # Open as plain store
    db = KVStore(path)
    assert db.get(b"key") == b"val"
    db.close()


# ---------------------------------------------------------------------------
# Test 20: is_encrypted on plain store
# ---------------------------------------------------------------------------
def test_is_encrypted_plain(tmp_path):
    path = _tmpdb(tmp_path)
    db = KVStore(path)
    assert db.is_encrypted() is False
    db.close()


# ---------------------------------------------------------------------------
# Test 21: open_encrypted on plain file → encrypts store (not AuthError)
# ---------------------------------------------------------------------------
def test_open_enc_on_plain_file(tmp_path):
    path = _tmpdb(tmp_path)
    db = KVStore(path)
    db.put(b"k", b"v")
    db.close()

    # Should encrypt the store, not raise AuthError
    with KVStore.open_encrypted(path, b"pw") as db:
        assert db.is_encrypted() is True
        assert db.get(b"k") == b"v"

    # Wrong password must fail
    with pytest.raises(AuthError):
        KVStore.open_encrypted(path, b"wrong")

    # Correct password works
    with KVStore.open_encrypted(path, b"pw") as db:
        assert db.get(b"k") == b"v"


# ---------------------------------------------------------------------------
# Test 22: context manager
# ---------------------------------------------------------------------------
def test_context_manager(tmp_path):
    path = _tmpdb(tmp_path)
    with KVStore.open_encrypted(path, b"pw") as db:
        db.put(b"ctx", b"works")
        assert db.get(b"ctx") == b"works"
    # After __exit__, the store is closed — further ops should raise


# ---------------------------------------------------------------------------
# Test 23: explicit transaction
# ---------------------------------------------------------------------------
def test_transaction(tmp_path):
    path = _tmpdb(tmp_path)
    with KVStore.open_encrypted(path, b"pw") as db:
        db.begin(write=True)
        db.put(b"tx_key", b"tx_val")
        db.commit()
        assert db.get(b"tx_key") == b"tx_val"


# ---------------------------------------------------------------------------
# Test 24: stats nPuts > 0
# ---------------------------------------------------------------------------
def test_stats_puts(tmp_path):
    path = _tmpdb(tmp_path)
    with KVStore.open_encrypted(path, b"pw") as db:
        db.put(b"a", b"1")
        db.put(b"b", b"2")
        s = db.stats()
        assert s["puts"] >= 2


# ---------------------------------------------------------------------------
# Test 25: empty value round-trip
# ---------------------------------------------------------------------------
def test_empty_value(tmp_path):
    path = _tmpdb(tmp_path)
    with KVStore.open_encrypted(path, b"pw") as db:
        db.put(b"empty", b"")
        val = db.get(b"empty")
        assert val == b""


# ---------------------------------------------------------------------------
# Test 26: open encrypted store without password → garbled values
# ---------------------------------------------------------------------------
def test_plain_open_returns_garbage(tmp_path):
    """
    Opening an encrypted store via KVStore() (no password) must raise AuthError.
    """
    path = _tmpdb(tmp_path)

    with KVStore.open_encrypted(path, b"pw") as db:
        db.put(b"k", b"supersecretvalue")

    with pytest.raises(AuthError):
        KVStore(path)
