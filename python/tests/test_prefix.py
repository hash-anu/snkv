"""
Prefix iterator tests — mirrors tests/test_prefix.c.

Covers basic prefix search, sorted order, empty results, single-char
prefixes, exact-key-as-prefix, CF prefix isolation, binary keys,
large-scale prefix, post-mutation iteration, and iterator re-seek.
"""

import pytest
from snkv import KVStore, JOURNAL_WAL


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    path = str(tmp_path / "prefix.db")
    with KVStore(path) as store:
        yield store


# ---------------------------------------------------------------------------
# Basic prefix search
# ---------------------------------------------------------------------------

def test_basic_prefix_multiple_namespaces(db):
    """Searching 'user:' returns only user keys, not other namespaces."""
    entries = {
        b"user:alice": b"1", b"user:bob": b"2",
        b"user:carol": b"3", b"user:dave": b"4",
        b"session:abc": b"s1", b"session:def": b"s2",
        b"config:host": b"localhost",
        b"admin:root": b"pw",
    }
    for k, v in entries.items():
        db[k] = v

    results = list(db.prefix_iterator(b"user:"))
    assert len(results) == 4
    keys = {k for k, _ in results}
    for k in (b"user:alice", b"user:bob", b"user:carol", b"user:dave"):
        assert k in keys
    for k in (b"session:abc", b"config:host", b"admin:root"):
        assert k not in keys


# ---------------------------------------------------------------------------
# Sorted order
# ---------------------------------------------------------------------------

def test_prefix_results_in_ascending_order(db):
    """Keys returned by prefix_iterator must be in ascending lexicographic order."""
    keys = [b"item:9", b"item:3", b"item:1", b"item:7", b"item:5",
            b"item:2", b"item:8", b"item:4", b"item:6"]
    for k in keys:
        db[k] = b"v"

    result_keys = [k for k, _ in db.prefix_iterator(b"item:")]
    assert result_keys == sorted(result_keys)
    assert len(result_keys) == len(keys)


# ---------------------------------------------------------------------------
# Empty results
# ---------------------------------------------------------------------------

def test_prefix_nonexistent_returns_empty(db):
    db[b"real:1"] = b"v"
    assert list(db.prefix_iterator(b"ghost:")) == []


def test_prefix_between_keys_returns_empty(db):
    db[b"aaa"] = b"v"
    db[b"ccc"] = b"v"
    assert list(db.prefix_iterator(b"bbb")) == []


def test_prefix_on_empty_store(db):
    assert list(db.prefix_iterator(b"anything")) == []


# ---------------------------------------------------------------------------
# Single-character prefix
# ---------------------------------------------------------------------------

def test_single_char_prefix(db):
    words = [b"apple", b"avocado", b"banana", b"blueberry",
             b"cherry", b"date"]
    for w in words:
        db[w] = b"fruit"

    a_results = {k for k, _ in db.prefix_iterator(b"a")}
    assert a_results == {b"apple", b"avocado"}

    b_results = {k for k, _ in db.prefix_iterator(b"b")}
    assert b_results == {b"banana", b"blueberry"}

    c_results = {k for k, _ in db.prefix_iterator(b"c")}
    assert c_results == {b"cherry"}


# ---------------------------------------------------------------------------
# Exact key as prefix
# ---------------------------------------------------------------------------

def test_exact_key_used_as_prefix(db):
    """prefix_iterator('app') must match 'app', 'apple', 'application', 'apply'."""
    keys = [b"app", b"apple", b"application", b"apply", b"banana"]
    for k in keys:
        db[k] = b"v"

    app_results = {k for k, _ in db.prefix_iterator(b"app")}
    assert app_results == {b"app", b"apple", b"application", b"apply"}

    apple_results = {k for k, _ in db.prefix_iterator(b"apple")}
    assert apple_results == {b"apple"}


# ---------------------------------------------------------------------------
# Column family prefix
# ---------------------------------------------------------------------------

def test_cf_prefix_isolated_from_default(db):
    """CF prefix_iterator must not include keys from the default CF."""
    with db.create_column_family("logs") as cf:
        cf[b"2024:01:01"] = b"a"
        cf[b"2024:01:15"] = b"b"
        cf[b"2024:01:31"] = b"c"
        cf[b"2024:02:01"] = b"d"

    # write same keys to default CF
    db[b"2024:01:01"] = b"default"

    with db.open_column_family("logs") as cf:
        jan = list(cf.prefix_iterator(b"2024:01:"))
        assert len(jan) == 3
        keys = {k for k, _ in jan}
        assert b"2024:02:01" not in keys


def test_cf_prefix_vs_default_cf_prefix(db):
    """prefix_iterator on the default CF must not see CF data."""
    with db.create_column_family("ns") as cf:
        cf[b"tag:x"] = b"cf_val"

    db[b"tag:y"] = b"default_val"

    results = {k for k, _ in db.prefix_iterator(b"tag:")}
    assert b"tag:y" in results
    assert b"tag:x" not in results


# ---------------------------------------------------------------------------
# Value inspection via prefix iterator
# ---------------------------------------------------------------------------

def test_prefix_values_are_correct(db):
    colors = {
        b"color:red":   b"ff0000",
        b"color:green": b"00ff00",
        b"color:blue":  b"0000ff",
        b"size:small":  b"S",
        b"size:large":  b"L",
    }
    for k, v in colors.items():
        db[k] = v

    for key, val in db.prefix_iterator(b"color:"):
        assert val == colors[key]
        assert len(val) == 6


# ---------------------------------------------------------------------------
# Binary keys
# ---------------------------------------------------------------------------

def test_binary_key_prefix(db):
    """Prefix match must work on binary keys (including embedded nulls)."""
    prefix = bytes([0x01, 0x02])
    keys = [
        bytes([0x01, 0x02, 0x03]),
        bytes([0x01, 0x02, 0xFF]),
        bytes([0x01, 0x02, 0x00]),
        bytes([0x01, 0x03, 0x00]),   # different prefix — must NOT match
        bytes([0x02, 0x00, 0x00]),   # different prefix — must NOT match
    ]
    for k in keys:
        db[k] = b"v"

    results = {k for k, _ in db.prefix_iterator(prefix)}
    assert len(results) == 3
    assert bytes([0x01, 0x02, 0x03]) in results
    assert bytes([0x01, 0x02, 0xFF]) in results
    assert bytes([0x01, 0x02, 0x00]) in results
    assert bytes([0x01, 0x03, 0x00]) not in results


# ---------------------------------------------------------------------------
# WAL mode
# ---------------------------------------------------------------------------

def test_prefix_wal_mode(tmp_path):
    """Prefix iterator must work in WAL journal mode."""
    path = str(tmp_path / "wal_pfx.db")
    with KVStore(path, journal_mode=JOURNAL_WAL) as db:
        db.begin(write=True)
        for i in range(100):
            ns = i % 3
            db[f"ns{ns}:{i:04d}".encode()] = b"v"
        db.commit()

        ns1_count = sum(1 for _ in db.prefix_iterator(b"ns1:"))
        assert ns1_count == 33 or ns1_count == 34   # 100/3 keys in ns1


# ---------------------------------------------------------------------------
# Large-scale prefix
# ---------------------------------------------------------------------------

def test_large_scale_prefix(tmp_path):
    """10 000 keys across 10 namespaces: each namespace returns 1 000 keys."""
    path = str(tmp_path / "large.db")
    with KVStore(path, journal_mode=JOURNAL_WAL) as db:
        db.begin(write=True)
        for i in range(10_000):
            ns = i % 10
            db[f"namespace{ns}:{i:08d}".encode()] = f"v{i}".encode()
        db.commit()

        for ns in range(10):
            count = sum(1 for _ in db.prefix_iterator(f"namespace{ns}:".encode()))
            assert count == 1_000, f"namespace{ns}: expected 1000, got {count}"


def test_large_scale_sub_prefix(tmp_path):
    """A narrower sub-prefix must return only the matching subset."""
    path = str(tmp_path / "sub.db")
    with KVStore(path, journal_mode=JOURNAL_WAL) as db:
        db.begin(write=True)
        for i in range(10_000):
            ns = i % 10
            db[f"namespace{ns}:{i:08d}".encode()] = b"v"
        db.commit()

        # i in [500..599] with i%10==5 → namespace5:00000505..namespace5:00000595 = 10 keys
        count = sum(1 for _ in db.prefix_iterator(b"namespace5:000005"))
        assert count == 10


# ---------------------------------------------------------------------------
# After mutations
# ---------------------------------------------------------------------------

def test_prefix_after_delete(db):
    """Deleted keys must not appear in subsequent prefix scans."""
    for k in (b"tag:a", b"tag:b", b"tag:c", b"tag:d"):
        db[k] = b"v"

    db.delete(b"tag:b")

    keys = {k for k, _ in db.prefix_iterator(b"tag:")}
    assert keys == {b"tag:a", b"tag:c", b"tag:d"}


def test_prefix_after_update(db):
    """Updated values must be visible via prefix_iterator."""
    db[b"tag:x"] = b"old"
    db[b"tag:y"] = b"val"
    db[b"tag:x"] = b"new"

    result = {k: v for k, v in db.prefix_iterator(b"tag:")}
    assert result[b"tag:x"] == b"new"
    assert result[b"tag:y"] == b"val"


# ---------------------------------------------------------------------------
# Iterator re-seek
# ---------------------------------------------------------------------------

def test_prefix_iterator_reseek_with_first(db):
    """Exhausting a prefix iterator then calling first() re-winds it."""
    for k in (b"ns:a", b"ns:b", b"ns:c"):
        db[k] = b"v"

    it = db.prefix_iterator(b"ns:")
    first_pass = list(it)   # exhausts the iterator

    it.first()
    second_pass = []
    while not it.eof:
        second_pass.append(it.item())
        it.next()

    it.close()
    assert first_pass == second_pass
    assert len(first_pass) == 3
