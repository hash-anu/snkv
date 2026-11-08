"""
Reverse iterator tests for the snkv Python bindings.

Verifies:
  - iterator(reverse=True) and reverse_iterator() are equivalent
  - iterator(prefix=p, reverse=True) and reverse_prefix_iterator(p) are equivalent
  - Both APIs work on KVStore (default CF) and ColumnFamily
  - Descending order, prefix filtering, empty results, TTL skip
  - Manual control (last / prev), context manager, iterator re-seek
  - Direction-mismatch: last() / prev() on a forward iterator raises Error
  - Binary keys including 0xFF prefix (BtreeLast fallback path)

Run with:
    pytest python/tests/test_iterator_reverse.py
or from the python/ directory:
    pytest tests/test_iterator_reverse.py
"""

import time
import pytest
from snkv import KVStore, JOURNAL_WAL, Error


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    path = str(tmp_path / "rev.db")
    with KVStore(path) as store:
        yield store


# ---------------------------------------------------------------------------
# 1. Basic reverse scan — descending order
# ---------------------------------------------------------------------------

def test_reverse_iterator_descending_order(db):
    """reverse_iterator() yields all keys in descending lexicographic order."""
    keys = [b"cherry", b"apple", b"date", b"banana", b"elderberry"]
    for k in keys:
        db[k] = b"v"

    result = [k for k, _ in db.reverse_iterator()]
    assert result == sorted(keys, reverse=True)


def test_iterator_reverse_true_descending_order(db):
    """iterator(reverse=True) yields all keys in descending order."""
    keys = [b"cherry", b"apple", b"date", b"banana", b"elderberry"]
    for k in keys:
        db[k] = b"v"

    result = [k for k, _ in db.iterator(reverse=True)]
    assert result == sorted(keys, reverse=True)


# ---------------------------------------------------------------------------
# 2. Equivalence: reverse_iterator() == iterator(reverse=True)
# ---------------------------------------------------------------------------

def test_reverse_iterator_equals_iterator_reverse_true(db):
    """reverse_iterator() and iterator(reverse=True) produce identical results."""
    for i in range(20):
        db[f"key:{i:03d}".encode()] = f"val:{i}".encode()

    via_flag  = list(db.iterator(reverse=True))
    via_method = list(db.reverse_iterator())
    assert via_flag == via_method


def test_reverse_prefix_equals_iterator_prefix_reverse(db):
    """reverse_prefix_iterator(p) and iterator(prefix=p, reverse=True) are identical."""
    for i in range(10):
        db[f"user:{i:03d}".encode()] = f"v{i}".encode()
    db[b"other:key"] = b"ignored"

    via_flag   = list(db.iterator(prefix=b"user:", reverse=True))
    via_method = list(db.reverse_prefix_iterator(b"user:"))
    assert via_flag == via_method
    assert len(via_flag) == 10


# ---------------------------------------------------------------------------
# 3. Reverse on empty store
# ---------------------------------------------------------------------------

def test_reverse_iterator_empty_store(db):
    assert list(db.reverse_iterator()) == []
    assert list(db.iterator(reverse=True)) == []


def test_reverse_prefix_empty_store(db):
    assert list(db.reverse_prefix_iterator(b"anything:")) == []
    assert list(db.iterator(prefix=b"anything:", reverse=True)) == []


# ---------------------------------------------------------------------------
# 4. Reverse on single key
# ---------------------------------------------------------------------------

def test_reverse_iterator_single_key(db):
    db[b"only"] = b"one"
    result = list(db.reverse_iterator())
    assert result == [(b"only", b"one")]


# ---------------------------------------------------------------------------
# 5. Forward and reverse cover the same set of keys
# ---------------------------------------------------------------------------

def test_reverse_covers_all_keys(db):
    """All keys returned by forward iterator also appear in reverse, and vice versa."""
    keys = {f"k:{i:04d}".encode() for i in range(50)}
    for k in keys:
        db[k] = b"v"

    fwd_keys = {k for k, _ in db.iterator()}
    rev_keys = {k for k, _ in db.reverse_iterator()}
    assert fwd_keys == keys
    assert rev_keys == keys


def test_reverse_is_exact_reverse_of_forward(db):
    """Reversing the forward iterator result equals the reverse iterator result."""
    for i in range(30):
        db[f"item:{i:04d}".encode()] = b"v"

    fwd = [k for k, _ in db.iterator()]
    rev = [k for k, _ in db.reverse_iterator()]
    assert rev == list(reversed(fwd))


# ---------------------------------------------------------------------------
# 6. Reverse prefix — only matching keys, descending
# ---------------------------------------------------------------------------

def test_reverse_prefix_only_matching_keys(db):
    """reverse_prefix_iterator returns only keys with matching prefix."""
    db[b"user:alice"]   = b"a"
    db[b"user:bob"]     = b"b"
    db[b"user:charlie"] = b"c"
    db[b"admin:root"]   = b"r"
    db[b"zzzother"]     = b"z"
    db[b"aaaother"]     = b"a2"

    result = [(k, v) for k, v in db.reverse_prefix_iterator(b"user:")]
    keys   = [k for k, _ in result]
    assert keys == sorted([b"user:alice", b"user:bob", b"user:charlie"], reverse=True)
    assert b"admin:root" not in keys
    assert b"zzzother" not in keys


def test_reverse_prefix_ascending_values_correct(db):
    """Values returned by reverse prefix scan match what was stored."""
    data = {f"ns:{i:03d}".encode(): f"val{i}".encode() for i in range(5)}
    for k, v in data.items():
        db[k] = v
    db[b"other:key"] = b"ignored"

    for key, val in db.reverse_prefix_iterator(b"ns:"):
        assert val == data[key]


def test_reverse_prefix_no_match_returns_empty(db):
    """reverse_prefix_iterator returns empty when no keys match prefix."""
    db[b"alpha:1"] = b"v"
    db[b"alpha:2"] = b"v"
    assert list(db.reverse_prefix_iterator(b"beta:")) == []
    assert list(db.iterator(prefix=b"beta:", reverse=True)) == []


def test_reverse_prefix_is_reverse_of_forward_prefix(db):
    """Reversing the forward prefix result equals the reverse prefix result."""
    for i in range(15):
        db[f"tag:{i:03d}".encode()] = b"v"
    db[b"other:x"] = b"ignored"

    fwd = [k for k, _ in db.prefix_iterator(b"tag:")]
    rev = [k for k, _ in db.reverse_prefix_iterator(b"tag:")]
    assert rev == list(reversed(fwd))
    assert len(rev) == 15


# ---------------------------------------------------------------------------
# 7. Column family reverse iterator
# ---------------------------------------------------------------------------

def test_cf_reverse_iterator(db):
    """CF-level reverse_iterator() yields CF keys in descending order."""
    with db.create_column_family("items") as cf:
        cf[b"aaa"] = b"1"
        cf[b"bbb"] = b"2"
        cf[b"ccc"] = b"3"

    with db.open_column_family("items") as cf:
        result = [k for k, _ in cf.reverse_iterator()]
    assert result == [b"ccc", b"bbb", b"aaa"]


def test_cf_iterator_reverse_true(db):
    """CF-level iterator(reverse=True) yields CF keys in descending order."""
    with db.create_column_family("items2") as cf:
        cf[b"aaa"] = b"1"
        cf[b"bbb"] = b"2"
        cf[b"ccc"] = b"3"

    with db.open_column_family("items2") as cf:
        result = [k for k, _ in cf.iterator(reverse=True)]
    assert result == [b"ccc", b"bbb", b"aaa"]


def test_cf_reverse_iterator_equals_iterator_reverse(db):
    """CF reverse_iterator() and CF iterator(reverse=True) produce identical results."""
    with db.create_column_family("eq_cf") as cf:
        for i in range(10):
            cf[f"k:{i:03d}".encode()] = b"v"

    with db.open_column_family("eq_cf") as cf:
        via_flag   = list(cf.iterator(reverse=True))
        via_method = list(cf.reverse_iterator())
    assert via_flag == via_method


def test_cf_reverse_prefix_iterator(db):
    """CF reverse_prefix_iterator yields only prefix-matching keys, descending."""
    with db.create_column_family("ns_cf") as cf:
        cf[b"user:alice"]   = b"a"
        cf[b"user:bob"]     = b"b"
        cf[b"user:charlie"] = b"c"
        cf[b"admin:root"]   = b"r"

    with db.open_column_family("ns_cf") as cf:
        result = [k for k, _ in cf.reverse_prefix_iterator(b"user:")]
    assert result == [b"user:charlie", b"user:bob", b"user:alice"]


def test_cf_reverse_isolated_from_default(db):
    """Reverse iterator on a CF must not include keys from the default CF."""
    with db.create_column_family("cf_iso") as cf:
        cf[b"key:1"] = b"cf"

    db[b"key:1"] = b"default"

    with db.open_column_family("cf_iso") as cf:
        result = list(cf.reverse_iterator())
    assert result == [(b"key:1", b"cf")]

    # default CF must still have its own key
    assert list(db.reverse_iterator()) == [(b"key:1", b"default")]


# ---------------------------------------------------------------------------
# 8. Manual control: last() / prev()
# ---------------------------------------------------------------------------

def test_manual_last_prev(db):
    """Manual last() + loop with prev() visits all keys in descending order."""
    for i in range(5):
        db[f"m:{i}".encode()] = b"v"

    it = db.reverse_iterator()
    it.last()
    result = []
    while not it.eof:
        result.append(it.key)
        it.prev()
    it.close()

    assert result == sorted([f"m:{i}".encode() for i in range(5)], reverse=True)


def test_last_returns_self_for_chaining(db):
    """Iterator.last() returns self so callers can chain calls."""
    db[b"k"] = b"v"
    it = db.reverse_iterator()
    assert it.last() is it
    it.close()


def test_prev_returns_none(db):
    """Iterator.prev() returns None (no chaining)."""
    db[b"k"] = b"v"
    it = db.reverse_iterator()
    it.last()
    result = it.prev()
    assert result is None
    it.close()


def test_manual_reverse_prefix_last_prev(db):
    """Manual last() is NOT needed for reverse prefix iter — already positioned."""
    for i in range(5):
        db[f"p:{i}".encode()] = b"v"
    db[b"other"] = b"v"

    it = db.reverse_prefix_iterator(b"p:")
    result = []
    while not it.eof:
        result.append(it.key)
        it.prev()
    it.close()

    assert result == sorted([f"p:{i}".encode() for i in range(5)], reverse=True)


# ---------------------------------------------------------------------------
# 9. Context manager
# ---------------------------------------------------------------------------

def test_reverse_iterator_context_manager(db):
    """Reverse iterator works as a context manager (auto-close)."""
    for i in range(3):
        db[f"cm:{i}".encode()] = b"v"

    with db.reverse_iterator() as it:
        result = list(it)

    assert len(result) == 3
    assert [k for k, _ in result] == sorted([f"cm:{i}".encode() for i in range(3)], reverse=True)


# ---------------------------------------------------------------------------
# 10. Direction mismatch: last() / prev() on forward iterator raises Error
# ---------------------------------------------------------------------------

def test_last_on_forward_iterator_raises(db):
    """last() on a forward iterator raises snkv.Error."""
    db[b"k"] = b"v"
    it = db.iterator()
    with pytest.raises(Error):
        it.last()
    it.close()


def test_prev_on_forward_iterator_raises(db):
    """prev() on a forward iterator raises snkv.Error."""
    db[b"k"] = b"v"
    it = db.iterator()
    with pytest.raises(Error):
        it.prev()
    it.close()


# ---------------------------------------------------------------------------
# 11. TTL — expired keys skipped during reverse scan
# ---------------------------------------------------------------------------

def test_reverse_iterator_skips_expired(db):
    """Expired keys are lazily deleted and not yielded by reverse_iterator()."""
    db.put(b"aaa", b"live1")
    db.put(b"bbb", b"soon", ttl=0.001)
    db.put(b"ccc", b"live2")

    time.sleep(0.05)

    result_keys = {k for k, _ in db.reverse_iterator()}
    assert b"bbb" not in result_keys
    assert b"aaa" in result_keys
    assert b"ccc" in result_keys


def test_reverse_prefix_iterator_skips_expired(db):
    """Expired keys are not yielded by reverse_prefix_iterator()."""
    db.put(b"evt:001", b"live1")
    db.put(b"evt:002", b"dead",  ttl=0.001)
    db.put(b"evt:003", b"live2")
    db.put(b"evt:004", b"dead2", ttl=0.001)
    db.put(b"evt:005", b"live3")
    db[b"other:x"] = b"ignored"

    time.sleep(0.05)

    result = list(db.reverse_prefix_iterator(b"evt:"))
    keys = [k for k, _ in result]
    assert b"evt:002" not in keys
    assert b"evt:004" not in keys
    assert len(keys) == 3
    assert keys == sorted([b"evt:001", b"evt:003", b"evt:005"], reverse=True)


# ---------------------------------------------------------------------------
# 12. Binary keys including 0xFF prefix (BtreeLast fallback)
# ---------------------------------------------------------------------------

def test_reverse_prefix_binary_0xff(db):
    """Reverse prefix with all-0xFF prefix byte uses BtreeLast fallback."""
    ff_key1 = bytes([0xFF, 0x01])
    ff_key2 = bytes([0xFF, 0x02])
    ff_prefix = bytes([0xFF])
    db[ff_key1] = b"v1"
    db[ff_key2] = b"v2"
    db[b"normal"] = b"v3"

    result_keys = [k for k, _ in db.reverse_prefix_iterator(ff_prefix)]
    assert set(result_keys) == {ff_key1, ff_key2}
    # 0xFF keys come before normal in reverse order
    assert result_keys == sorted([ff_key1, ff_key2], reverse=True)


def test_reverse_prefix_binary_keys(db):
    """Binary prefix matching works in reverse direction."""
    prefix = bytes([0x01, 0x02])
    keys = [bytes([0x01, 0x02, 0x00]),
            bytes([0x01, 0x02, 0x80]),
            bytes([0x01, 0x02, 0xFF])]
    non_matching = [bytes([0x01, 0x03, 0x00]), bytes([0x02, 0x00, 0x00])]
    for k in keys + non_matching:
        db[k] = b"v"

    result = [k for k, _ in db.reverse_prefix_iterator(prefix)]
    assert result == sorted(keys, reverse=True)


# ---------------------------------------------------------------------------
# 13. Large-scale reverse
# ---------------------------------------------------------------------------

def test_large_scale_reverse_order(tmp_path):
    """1 000 keys: reverse iterator returns them in strict descending order."""
    path = str(tmp_path / "large_rev.db")
    with KVStore(path, journal_mode=JOURNAL_WAL) as db:
        db.begin(write=True)
        for i in range(1_000):
            db[f"k:{i:08d}".encode()] = b"v"
        db.commit()

        keys = [k for k, _ in db.reverse_iterator()]
    assert len(keys) == 1_000
    assert keys == sorted(keys, reverse=True)


def test_large_scale_reverse_prefix(tmp_path):
    """Large reverse prefix scan returns the correct count and order."""
    path = str(tmp_path / "large_pfx_rev.db")
    with KVStore(path, journal_mode=JOURNAL_WAL) as db:
        db.begin(write=True)
        for i in range(1_000):
            ns = i % 5
            db[f"ns{ns}:{i:08d}".encode()] = b"v"
        db.commit()

        for ns in range(5):
            keys = [k for k, _ in db.reverse_prefix_iterator(f"ns{ns}:".encode())]
            assert len(keys) == 200
            assert keys == sorted(keys, reverse=True)


# ---------------------------------------------------------------------------
# 14. str / bytes interoperability
# ---------------------------------------------------------------------------

def test_reverse_iterator_str_keys(db):
    """str keys are encoded and yielded back as bytes in descending order."""
    db.put("zebra", "z")
    db.put("apple", "a")
    db.put("mango", "m")

    result = [k for k, _ in db.reverse_iterator()]
    assert result == [b"zebra", b"mango", b"apple"]


def test_reverse_prefix_str_prefix(db):
    """str prefix is accepted by reverse_prefix_iterator."""
    db[b"tag:a"] = b"1"
    db[b"tag:b"] = b"2"
    db[b"other"] = b"3"

    result = [k for k, _ in db.reverse_prefix_iterator("tag:")]
    assert result == [b"tag:b", b"tag:a"]
