# SPDX-License-Identifier: Apache-2.0
"""
all_apis.py — exercises every public Python API in snkv.

Run from the repo root:
    python python/examples/all_apis.py

Each [ok] line confirms one API call worked correctly.
The script ends with "ALL CHECKS PASSED" if everything is fine.
Any failure raises AssertionError and stops immediately.
"""

import os
import time
import tempfile

from snkv import (
    KVStore,
    Error, NotFoundError, AuthError,
    JOURNAL_WAL, JOURNAL_DELETE,
    SYNC_OFF, SYNC_NORMAL, SYNC_FULL,
    CHECKPOINT_PASSIVE, CHECKPOINT_FULL, CHECKPOINT_RESTART, CHECKPOINT_TRUNCATE,
    NO_TTL,
)


def ok(label: str) -> None:
    print(f"  [ok] {label}")


def section(n: int, title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {n}. {title}")
    print(f"{'='*60}")


def subsection(parent: int, sub: int, title: str) -> None:
    print(f"\n  --- {parent}.{sub}  {title} ---")


def _validate_all_apis(db, p: str = "vt", parent: int = 0) -> None:
    """
    Exercise every public KVStore API on an already-open store.
    p       — short key prefix; all test keys use b'{p}:...' to avoid collisions.
    parent  — enclosing section number (13-16); 0 = no sub-headers printed.
    """
    def _sub(sub: int, title: str) -> None:
        if parent:
            subsection(parent, sub, title)

    # ---- put / get / delete / exists ----------------------------------------
    _sub(1, "Basic put / get / delete / exists")
    db.put(f"{p}:name".encode(), b"alice")
    assert db.get(f"{p}:name".encode()) == b"alice"
    ok("put + get")

    db.put(f"{p}:city", "london")
    assert db.get(f"{p}:city") == b"london"
    ok("str key/val auto-encode")

    assert db.get(f"{p}:nope".encode()) is None
    assert db.get(f"{p}:nope".encode(), b"fb") == b"fb"
    ok("get missing → None / default")

    assert db.exists(f"{p}:name".encode()) is True
    assert db.exists(f"{p}:nope".encode()) is False
    ok("exists present/absent")

    db.delete(f"{p}:name".encode())
    assert db.get(f"{p}:name".encode()) is None
    ok("delete → gone")

    # ---- dict interface -------------------------------------------------------
    _sub(2, "Dict-like interface")
    db[f"{p}:fruit"] = "apple"
    assert db[f"{p}:fruit"] == b"apple"
    ok("__setitem__ / __getitem__")

    assert f"{p}:fruit".encode() in db
    assert f"{p}:nope".encode() not in db
    ok("__contains__")

    del db[f"{p}:fruit"]
    assert f"{p}:fruit".encode() not in db
    ok("__delitem__")

    try:
        _ = db[f"{p}:ghost"]
        assert False
    except KeyError:
        ok("__getitem__ missing → KeyError")

    for letter in "bac":
        db[f"{p}:l{letter}"] = letter.upper()
    pfx_l = f"{p}:l".encode()
    l_pairs = [(k, v) for k, v in db if k.startswith(pfx_l)]
    assert [k for k, _ in l_pairs] == [
        f"{p}:la".encode(), f"{p}:lb".encode(), f"{p}:lc".encode()
    ]
    ok("for k, v in db: → ascending order")

    # ---- TTL ------------------------------------------------------------------
    _sub(3, "TTL — put with expiry, ttl(), purge_expired()")
    db.put(f"{p}:sess".encode(), b"tok", ttl=60)
    assert db.get(f"{p}:sess".encode()) == b"tok"
    rem = db.ttl(f"{p}:sess".encode())
    assert rem is not None and 0 < rem <= 60
    ok(f"put(ttl=60) + ttl()={rem:.1f}s")

    db.put(f"{p}:perm".encode(), b"forever")
    assert db.ttl(f"{p}:perm".encode()) is None
    ok("ttl(no-TTL key) → None")

    try:
        db.ttl(f"{p}:ghost".encode())
        assert False
    except NotFoundError:
        ok("ttl(missing) → NotFoundError")

    db[f"{p}:ttl_s", 0.05] = b"ephemeral"
    assert db.get(f"{p}:ttl_s".encode()) == b"ephemeral"
    ok("db[key, ttl] = value syntax")
    time.sleep(0.1)
    assert db.get(f"{p}:ttl_s".encode()) is None
    ok("expired key → get() None (lazy delete)")

    db.put(f"{p}:d1".encode(), b"x", ttl=0.001)
    db.put(f"{p}:d2".encode(), b"y", ttl=0.001)
    db.put(f"{p}:live".encode(), b"z", ttl=60)
    time.sleep(0.05)
    n_purge = db.purge_expired()
    assert n_purge >= 2
    assert db.get(f"{p}:live".encode()) == b"z"
    ok(f"purge_expired() → {n_purge} deleted, live key intact")

    # ---- transactions ---------------------------------------------------------
    _sub(4, "Transactions — begin/commit, begin/rollback")
    db.begin(write=True)
    for i in range(3):
        db.put(f"{p}:tx{i}".encode(), b"v")
    db.commit()
    assert all(db.get(f"{p}:tx{i}".encode()) == b"v" for i in range(3))
    ok("begin(write=True) + commit → keys visible")

    db.begin(write=True)
    db.put(f"{p}:txr".encode(), b"never")
    db.rollback()
    assert db.get(f"{p}:txr".encode()) is None
    ok("begin(write=True) + rollback → key absent")

    db.begin(write=False)
    assert db.get(f"{p}:tx0".encode()) == b"v"
    db.commit()
    ok("begin(write=False) + commit → read-only tx ok")

    # ---- iterators ------------------------------------------------------------
    _sub(5, "Forward iterators — all variants")
    for x in "edcba":
        db[f"{p}:it:{x}"] = x.upper()
    pfx_it = f"{p}:it:".encode()
    fwd = [k for k, _ in db.iterator() if k.startswith(pfx_it)]
    assert fwd == sorted(fwd)
    ok("iterator() → ascending")

    db[f"{p}:u:alice"] = "a"
    db[f"{p}:u:bob"] = "b"
    pfx_u = f"{p}:u:".encode()
    pfx_keys = [k for k, _ in db.prefix_iterator(pfx_u)]
    assert pfx_keys == [f"{p}:u:alice".encode(), f"{p}:u:bob".encode()]
    ok("prefix_iterator()")

    pfx_keys2 = [k for k, _ in db.iterator(prefix=pfx_u)]
    assert pfx_keys2 == pfx_keys
    ok("iterator(prefix=...) == prefix_iterator()")

    # seek
    db.put(f"{p}:sk:a".encode(), b"1")
    db.put(f"{p}:sk:b".encode(), b"2")
    db.put(f"{p}:sk:c".encode(), b"3")
    with db.iterator() as it:
        it.seek(f"{p}:sk:b".encode())
        assert not it.eof
        sk_k, sk_v = it.key, it.value
    assert sk_k == f"{p}:sk:b".encode() and sk_v == b"2"
    ok("iterator seek → positioned at target key")

    _sub(6, "Reverse iterators — all variants")
    rev = [k for k, _ in db.reverse_prefix_iterator(pfx_u)]
    assert rev == [f"{p}:u:bob".encode(), f"{p}:u:alice".encode()]
    ok("reverse_prefix_iterator()")

    rev2 = [k for k, _ in db.iterator(prefix=pfx_u, reverse=True)]
    assert rev2 == rev
    ok("iterator(prefix=..., reverse=True)")

    # ---- put_if_absent --------------------------------------------------------
    _sub(7, "put_if_absent")
    pia_key = f"{p}:pia".encode()
    if db.get(pia_key) is not None:
        db.delete(pia_key)
    assert db.put_if_absent(pia_key, b"first") is True
    assert db.put_if_absent(pia_key, b"second") is False
    assert db.get(pia_key) == b"first"
    ok("put_if_absent — insert once, second attempt blocked")

    # ---- count ----------------------------------------------------------------
    _sub(8, "count()")
    n_count = db.count()
    assert n_count > 0
    ok(f"count() → {n_count}")

    # ---- column families -------------------------------------------------------
    _sub(9, "Column families — create, open, list, isolation, drop + iterators + TTL")
    cf_name = f"{p}:cf"
    try:
        cf = db.create_column_family(cf_name)
    except Exception:
        cf = db.open_column_family(cf_name)
    with cf:
        cf.clear()

        cf.put(b"ck", b"cv")
        assert cf.get(b"ck") == b"cv"
        ok("CF put + get")

        assert cf.exists(b"ck") is True
        ok("CF exists")

        cf.delete(b"ck")
        assert cf.get(b"ck") is None
        ok("CF delete → gone")

        cf["cd"] = "val"
        assert cf["cd"] == b"val"
        del cf["cd"]
        assert b"cd" not in cf
        ok("CF dict interface (__setitem__, __getitem__, __delitem__, __contains__)")

        try:
            _ = cf["ghost"]
            assert False
        except KeyError:
            ok("CF __getitem__ missing → KeyError")

        for x in "cba":
            cf[f"cl:{x}"] = x.upper()
        cf_fwd = [k for k, _ in cf]
        assert cf_fwd == sorted(cf_fwd)
        ok("CF __iter__ → ascending")

        # CF iterators
        cf[f"cu:alice"] = "a"
        cf[f"cu:bob"] = "b"
        cf_pfx = [k for k, _ in cf.prefix_iterator(b"cu:")]
        assert cf_pfx == [b"cu:alice", b"cu:bob"]
        ok("CF prefix_iterator()")

        cf_pfx2 = [k for k, _ in cf.iterator(prefix=b"cu:")]
        assert cf_pfx2 == cf_pfx
        ok("CF iterator(prefix=...) == CF prefix_iterator()")

        cf_rev = [k for k, _ in cf.reverse_prefix_iterator(b"cu:")]
        assert cf_rev == [b"cu:bob", b"cu:alice"]
        ok("CF reverse_prefix_iterator()")

        cf_rev2 = [k for k, _ in cf.iterator(prefix=b"cu:", reverse=True)]
        assert cf_rev2 == cf_rev
        ok("CF iterator(prefix=..., reverse=True)")

        # CF seek
        cf.put(b"csk:a", b"1")
        cf.put(b"csk:b", b"2")
        with cf.iterator() as cit:
            cit.seek(b"csk:b")
            assert not cit.eof
            ck2, cv2 = cit.key, cit.value
        assert ck2 == b"csk:b" and cv2 == b"2"
        ok("CF iterator seek")

        # CF for k, v in cf: check
        cf_l_pairs = [(k, v) for k, v in cf if k.startswith(b"cl:")]
        assert [k for k, _ in cf_l_pairs] == [b"cl:a", b"cl:b", b"cl:c"]
        ok("for k, v in cf: → ascending")

        # CF TTL
        cf.put(b"cttl", b"tok", ttl=60)
        r = cf.ttl(b"cttl")
        assert r is not None and 0 < r <= 60
        ok(f"CF put(ttl=60) + CF ttl()={r:.1f}s")

        cf.put(b"cexp", b"x", ttl=0.001)
        time.sleep(0.05)
        assert cf.get(b"cexp") is None
        ok("CF expired key → get() None")

        n_cf_purge = cf.purge_expired()
        ok(f"CF purge_expired() → {n_cf_purge} deleted")

        # CF put_if_absent
        if cf.get(b"cpia") is not None:
            cf.delete(b"cpia")
        assert cf.put_if_absent(b"cpia", b"first") is True
        assert cf.put_if_absent(b"cpia", b"second") is False
        ok("CF put_if_absent")

        cf_count = cf.count()
        assert cf_count >= 0
        ok(f"CF count() → {cf_count}")

    names = db.list_column_families()
    assert cf_name in names
    ok("list_column_families() includes CF")

    with db.open_column_family(cf_name) as cf:
        assert cf.get(b"cpia") == b"first"
    ok("open_column_family() → data persists after reopen")

    db.drop_column_family(cf_name)
    assert cf_name not in db.list_column_families()
    ok("drop_column_family() → removed from list")

    with db.default_column_family() as dcf:
        dcf.put(f"{p}:dcf".encode(), b"dcf_val")
        assert dcf.get(f"{p}:dcf".encode()) == b"dcf_val"
    ok("default_column_family() → accesses default CF data")

    # ---- maintenance ----------------------------------------------------------
    _sub(10, "Maintenance — sync, vacuum, integrity_check, checkpoint, stats, errmsg")
    db.sync()
    ok("sync()")

    db.integrity_check()
    ok("integrity_check()")

    db.vacuum()
    ok("vacuum()")

    nlog, nckpt = db.checkpoint(CHECKPOINT_PASSIVE)
    assert isinstance(nlog, int) and isinstance(nckpt, int)
    ok(f"checkpoint(PASSIVE) nlog={nlog} nckpt={nckpt}")

    s = db.stats()
    assert isinstance(s, dict) and "puts" in s
    ok(f"stats() puts={s['puts']}")

    db.stats_reset()
    ok("stats_reset()")

    msg = db.errmsg
    assert isinstance(msg, str)
    ok(f"errmsg → {msg!r}")

    # ---- encryption state (works on plain and encrypted stores alike) ----------
    _sub(11, "Encryption state")
    enc = db.is_encrypted()
    assert isinstance(enc, bool)
    ok(f"is_encrypted() → {enc}")


def run_all() -> None:
    with tempfile.TemporaryDirectory() as tmp:

        def db_path(n: int) -> str:
            return os.path.join(tmp, f"s{n:02d}.db")

        # ------------------------------------------------------------------ #
        # 1. Basic put / get / delete / exists                                #
        # ------------------------------------------------------------------ #
        section(1, "Basic put / get / delete / exists")

        with KVStore(db_path(1)) as db:
            # bytes keys and values
            db.put(b"name", b"alice")
            assert db.get(b"name") == b"alice"
            ok("put + get bytes")

            # str keys/values are auto-encoded to UTF-8
            db.put("city", "london")
            assert db.get("city") == b"london"
            ok("put + get str keys (UTF-8 auto-encode)")

            # missing key → None (or custom default)
            assert db.get(b"missing") is None
            ok("get missing key → None")

            assert db.get(b"missing", b"fallback") == b"fallback"
            ok("get missing key with default → b'fallback'")

            # exists
            assert db.exists(b"name") is True
            assert db.exists(b"nope") is False
            ok("exists(present)=True  exists(absent)=False")

            # delete
            db.delete(b"name")
            assert db.get(b"name") is None
            ok("delete → key gone")

            # in-memory store (path=None)
            with KVStore(None) as mem:
                mem["k"] = "v"
                assert mem["k"] == b"v"
            ok("in-memory store (path=None)")

            # JOURNAL_DELETE mode
            with KVStore(db_path(0), journal_mode=JOURNAL_DELETE) as jdb:
                jdb.put(b"x", b"y")
                assert jdb.get(b"x") == b"y"
            ok("JOURNAL_DELETE mode — put/get ok")

        # ------------------------------------------------------------------ #
        # 2. Dict-like interface                                               #
        # ------------------------------------------------------------------ #
        section(2, "Dict-like interface")

        with KVStore(db_path(2)) as db:
            db["fruit"] = "apple"
            assert db["fruit"] == b"apple"
            ok("__setitem__ / __getitem__")

            assert b"fruit" in db
            assert b"nope"  not in db
            ok("__contains__ — present=True, absent=False")

            del db["fruit"]
            assert b"fruit" not in db
            ok("__delitem__ → key gone")

            # KeyError on missing key via []
            try:
                _ = db["ghost"]
                assert False, "should have raised"
            except KeyError:
                ok("db['ghost'] → KeyError")

            # for k, v in db: (uses __iter__)
            for letter in "bac":
                db[letter] = letter.upper()
            all_pairs = list(db)
            assert [k for k, _ in all_pairs] == [b"a", b"b", b"c"]
            ok("for k, v in db: → ascending order")

        # ------------------------------------------------------------------ #
        # 3. TTL                                                               #
        # ------------------------------------------------------------------ #
        section(3, "TTL — put with expiry, ttl(), purge_expired()")

        with KVStore(db_path(3)) as db:
            # put with TTL via put(ttl=...)
            db.put(b"session", b"tok123", ttl=60)
            assert db.get(b"session") == b"tok123"
            ok("put(ttl=60) → get returns value immediately")

            remaining = db.ttl(b"session")
            assert remaining is not None and 0 < remaining <= 60
            ok(f"ttl() → {remaining:.1f}s remaining (expect ~60)")

            # permanent key → ttl() returns None
            db.put(b"perm", b"forever")
            assert db.ttl(b"perm") is None
            ok("ttl(no-TTL key) → None")

            # ttl() on missing key raises NotFoundError
            try:
                db.ttl(b"ghost")
                assert False
            except NotFoundError:
                ok("ttl(missing key) → NotFoundError")

            # dict TTL syntax: db[key, ttl_seconds] = value
            db[b"quick", 0.05] = b"ephemeral"
            assert db.get(b"quick") == b"ephemeral"
            ok("db[key, ttl] = value syntax → readable immediately")

            time.sleep(0.1)
            assert db.get(b"quick") is None
            ok("after 100ms sleep — expired key → get() returns None")

            # purge_expired()
            db.put(b"die1", b"x", ttl=0.001)
            db.put(b"die2", b"y", ttl=0.001)
            db.put(b"live", b"z", ttl=60)
            time.sleep(0.05)
            n = db.purge_expired()
            assert n == 2
            assert db.get(b"live") == b"z"
            ok(f"purge_expired() → deleted {n} expired keys, live key intact")

        # ------------------------------------------------------------------ #
        # 4. Transactions                                                      #
        # ------------------------------------------------------------------ #
        section(4, "Transactions — begin/commit, begin/rollback")

        with KVStore(db_path(4)) as db:
            # write transaction + commit
            db.begin(write=True)
            for i in range(5):
                db.put(f"tx:{i}".encode(), b"v")
            db.commit()
            assert all(db.get(f"tx:{i}".encode()) == b"v" for i in range(5))
            ok("begin(write=True) + commit → 5 keys visible after commit")

            # write transaction + rollback
            db.begin(write=True)
            db.put(b"tx:rolled", b"never")
            db.rollback()
            assert db.get(b"tx:rolled") is None
            ok("begin(write=True) + rollback → key absent")

            # read transaction (explicit)
            db.begin(write=False)
            assert db.get(b"tx:0") == b"v"
            db.commit()
            ok("begin(write=False) + commit → read-only transaction ok")

        # ------------------------------------------------------------------ #
        # 5. Forward iterators                                                 #
        # ------------------------------------------------------------------ #
        section(5, "Forward iterators — all variants")

        with KVStore(db_path(5)) as db:
            for letter in "edcba":
                db[letter] = letter.upper()

            # iterator() — for loop
            fwd = [k for k, _ in db.iterator()]
            assert fwd == sorted(fwd)
            ok(f"iterator() for-loop → {[k.decode() for k in fwd]}")

            # prefix_iterator
            db["user:alice"] = "a"
            db["user:bob"]   = "b"
            db["admin:root"] = "r"

            pfx = [k for k, _ in db.prefix_iterator(b"user:")]
            assert pfx == [b"user:alice", b"user:bob"]
            ok(f"prefix_iterator('user:') → {[k.decode() for k in pfx]}")

            # iterator(prefix=...) equals prefix_iterator()
            pfx2 = [k for k, _ in db.iterator(prefix="user:")]
            assert pfx2 == pfx
            ok("iterator(prefix=...) == prefix_iterator()")

            # manual first / next / eof / key / value / item
            it = db.prefix_iterator(b"user:")
            it.first()
            items = []
            while not it.eof:
                items.append((it.key, it.value))
                it.next()
            it.close()
            assert [k for k, _ in items] == [b"user:alice", b"user:bob"]
            assert items[0][1] == b"a"
            ok("manual first/next/eof/key/value → correct")

            # item() returns (key, value) tuple
            it2 = db.prefix_iterator(b"user:")
            it2.first()
            k, v = it2.item()
            assert k == b"user:alice" and v == b"a"
            it2.close()
            ok("item() → (key, value) tuple")

            # context manager auto-closes
            with db.prefix_iterator(b"user:") as cm_it:
                cm_keys = [k for k, _ in cm_it]
            assert cm_keys == [b"user:alice", b"user:bob"]
            ok("iterator as context manager → auto-close")

            # re-seek with first() — after exhausting, rewind and re-read manually
            it3 = db.prefix_iterator(b"user:")
            pass1 = list(it3)                   # exhausts iterator
            it3.first()                          # reposition at first key
            pass2 = []
            while not it3.eof:
                pass2.append(it3.item())
                it3.next()
            it3.close()
            assert pass1 == pass2
            ok("re-seek with first() → same results on second pass")

            # last() chains (returns self)
            it4 = db.reverse_iterator()
            ret = it4.last()
            assert ret is it4
            it4.close()
            ok("last() returns self for chaining")

        # ------------------------------------------------------------------ #
        # 6. Reverse iterators                                                 #
        # ------------------------------------------------------------------ #
        section(6, "Reverse iterators — all variants + manual + direction check")

        with KVStore(db_path(6)) as db:
            for letter in "abcde":
                db[letter] = letter.upper()

            # reverse_iterator() for-loop (only a-e at this point)
            rev_ae = [k for k, _ in db.reverse_iterator()]
            assert rev_ae == sorted(rev_ae, reverse=True)
            assert rev_ae == [b"e", b"d", b"c", b"b", b"a"]
            ok(f"reverse_iterator() → {[k.decode() for k in rev_ae]}")

            # iterator(reverse=True) == reverse_iterator()
            rev2 = [k for k, _ in db.iterator(reverse=True)]
            assert rev2 == rev_ae
            ok("iterator(reverse=True) == reverse_iterator()")

            # add more keys for prefix tests
            db["user:alice"]   = "a"
            db["user:bob"]     = "b"
            db["user:charlie"] = "c"
            db["admin:root"]   = "r"

            # reverse_prefix_iterator
            rpfx = [k for k, _ in db.reverse_prefix_iterator(b"user:")]
            assert rpfx == [b"user:charlie", b"user:bob", b"user:alice"]
            ok(f"reverse_prefix_iterator → {[k.decode() for k in rpfx]}")

            # iterator(prefix=..., reverse=True) == reverse_prefix_iterator()
            rpfx2 = [k for k, _ in db.iterator(prefix="user:", reverse=True)]
            assert rpfx2 == rpfx
            ok("iterator(prefix=..., reverse=True) == reverse_prefix_iterator()")

            # manual last / prev — over all keys in DB
            rev_all = [k for k, _ in db.reverse_iterator()]
            it = db.reverse_iterator()
            it.last()
            manual = []
            while not it.eof:
                manual.append(it.key)
                it.prev()
            it.close()
            assert manual == rev_all
            ok(f"manual last/prev/eof/key → {len(manual)} keys in descending order")

            # reverse prefix iterator is already positioned — no last() needed
            it_rp = db.reverse_prefix_iterator(b"user:")
            rp_manual = []
            while not it_rp.eof:
                rp_manual.append(it_rp.key)
                it_rp.prev()
            it_rp.close()
            assert rp_manual == rpfx
            ok("reverse_prefix_iterator — already positioned, no last() needed")

            # direction mismatch: last() / prev() on forward iterator raises Error
            it_fwd = db.iterator()
            try:
                it_fwd.last()
                assert False
            except Error:
                ok("last() on forward iterator → Error")
            try:
                it_fwd.prev()
                assert False
            except Error:
                ok("prev() on forward iterator → Error")
            it_fwd.close()

        # ------------------------------------------------------------------ #
        # 7. Column families                                                   #
        # ------------------------------------------------------------------ #
        section(7, "Column families — create, open, list, isolation, drop")

        with KVStore(db_path(7)) as db:
            # create + basic ops
            with db.create_column_family("users") as cf:
                cf.put(b"alice", b"30")
                assert cf.get(b"alice") == b"30"
                ok("CF put + get")

                assert cf.exists(b"alice") is True
                assert cf.exists(b"nobody") is False
                ok("CF exists")

                cf.delete(b"alice")
                assert cf.get(b"alice") is None
                ok("CF delete → key gone")

                # dict-like interface on CF
                cf["bob"] = "25"
                assert cf["bob"] == b"25"
                ok("CF __setitem__ / __getitem__")

                assert b"bob" in cf
                assert b"ghost" not in cf
                ok("CF __contains__")

                del cf["bob"]
                assert b"bob" not in cf
                ok("CF __delitem__")

                try:
                    _ = cf["ghost"]
                    assert False
                except KeyError:
                    ok("CF __getitem__ missing → KeyError")

            # list_column_families
            db.create_column_family("logs").close()
            db.create_column_family("cache").close()
            names = db.list_column_families()
            assert "users"  in names
            assert "logs"   in names
            assert "cache"  in names
            ok(f"list_column_families() includes 'users', 'logs', 'cache'")

            # isolation: same key in CF and default CF are independent
            with db.open_column_family("users") as cf:
                cf["shared"] = "cf_value"
            db["shared"] = "default_value"

            with db.open_column_family("users") as cf:
                assert cf["shared"] == b"cf_value"
            assert db["shared"] == b"default_value"
            ok("CF isolation — CF key and default-CF key are independent")

            # default_column_family()
            with db.default_column_family() as dcf:
                assert dcf.get(b"shared") == b"default_value"
            ok("default_column_family() → accesses same data as db.get()")

            # drop
            db.drop_column_family("cache")
            names2 = db.list_column_families()
            assert "cache" not in names2
            ok("drop_column_family('cache') → removed from list")

            # open missing CF raises NotFoundError
            try:
                db.open_column_family("no_such_cf")
                assert False
            except NotFoundError:
                ok("open_column_family(missing) → NotFoundError")

        # ------------------------------------------------------------------ #
        # 8. CF iterators                                                      #
        # ------------------------------------------------------------------ #
        section(8, "Column family iterators")

        with KVStore(db_path(8)) as db:
            with db.create_column_family("items") as cf:
                for letter in "edcba":
                    cf[letter] = letter.upper()
                cf["tag:x"] = "1"
                cf["tag:y"] = "2"
                cf["other"] = "3"

                # CF __iter__ (forward, all keys)
                fwd = [k for k, _ in cf]
                assert fwd == sorted(fwd)
                ok(f"CF __iter__ → ascending — {[k.decode() for k in fwd]}")

                # CF iterator() explicit
                fwd2 = [k for k, _ in cf.iterator()]
                assert fwd2 == fwd
                ok("CF iterator() == CF __iter__")

                # CF prefix_iterator
                pfx = [k for k, _ in cf.prefix_iterator(b"tag:")]
                assert pfx == [b"tag:x", b"tag:y"]
                ok(f"CF prefix_iterator('tag:') → {[k.decode() for k in pfx]}")

                # CF iterator(prefix=...)
                pfx2 = [k for k, _ in cf.iterator(prefix=b"tag:")]
                assert pfx2 == pfx
                ok("CF iterator(prefix=...) == CF prefix_iterator()")

                # CF reverse_iterator
                rev = [k for k, _ in cf.reverse_iterator()]
                assert rev == sorted(rev, reverse=True)
                ok(f"CF reverse_iterator() → descending — {[k.decode() for k in rev]}")

                # CF iterator(reverse=True)
                rev2 = [k for k, _ in cf.iterator(reverse=True)]
                assert rev2 == rev
                ok("CF iterator(reverse=True) == CF reverse_iterator()")

                # CF reverse_prefix_iterator
                rpfx = [k for k, _ in cf.reverse_prefix_iterator(b"tag:")]
                assert rpfx == [b"tag:y", b"tag:x"]
                ok(f"CF reverse_prefix_iterator → {[k.decode() for k in rpfx]}")

                # CF iterator(prefix=..., reverse=True)
                rpfx2 = [k for k, _ in cf.iterator(prefix=b"tag:", reverse=True)]
                assert rpfx2 == rpfx
                ok("CF iterator(prefix=..., reverse=True) == CF reverse_prefix_iterator()")

        # ------------------------------------------------------------------ #
        # 9. Column family TTL                                                 #
        # ------------------------------------------------------------------ #
        section(9, "Column family TTL")

        with KVStore(db_path(9)) as db:
            with db.create_column_family("sessions") as cf:
                # put with TTL
                cf.put(b"user:1", b"tok1", ttl=60)
                assert cf.get(b"user:1") == b"tok1"
                remaining = cf.ttl(b"user:1")
                assert remaining is not None and 0 < remaining <= 60
                ok(f"CF put(ttl=60) → get ok, ttl()={remaining:.1f}s remaining")

                # no-TTL key → ttl() returns None
                cf.put(b"user:perm", b"forever")
                assert cf.ttl(b"user:perm") is None
                ok("CF ttl(no-TTL key) → None")

                # expired key → get returns None (lazy delete)
                cf.put(b"user:2", b"tok2", ttl=0.001)
                time.sleep(0.05)
                assert cf.get(b"user:2") is None
                ok("CF expired key → get() returns None")

                # TTL skip during reverse iteration
                cf.put(b"evt:001", b"live1")
                cf.put(b"evt:002", b"dead", ttl=0.001)
                cf.put(b"evt:003", b"live2")
                time.sleep(0.05)
                rev_keys = [k for k, _ in cf.reverse_prefix_iterator(b"evt:")]
                assert b"evt:002" not in rev_keys
                assert b"evt:001" in rev_keys and b"evt:003" in rev_keys
                ok("CF expired key skipped during reverse prefix iteration")

                # CF purge_expired
                cf.put(b"die1", b"x", ttl=0.001)
                cf.put(b"die2", b"y", ttl=0.001)
                cf.put(b"live", b"z", ttl=60)
                time.sleep(0.05)
                n = cf.purge_expired()
                assert n == 2
                assert cf.get(b"live") == b"z"
                ok(f"CF purge_expired() → deleted {n} expired keys")

        # ------------------------------------------------------------------ #
        # 10. Advanced config (open_v2 kwargs)                                 #
        # ------------------------------------------------------------------ #
        section(10, "Advanced config — KVStore kwargs (open_v2)")

        with KVStore(
            db_path(10),
            journal_mode=JOURNAL_WAL,
            sync_level=SYNC_NORMAL,
            cache_size=500,
            busy_timeout=1000,
        ) as db:
            db["cfg_key"] = "cfg_val"
            assert db["cfg_key"] == b"cfg_val"
            ok("sync_level=SYNC_NORMAL, cache_size=500, busy_timeout=1000 → ok")

        with KVStore(
            db_path(10) + "2",
            journal_mode=JOURNAL_WAL,
            sync_level=SYNC_OFF,
        ) as db:
            db["s"] = "v"
            assert db["s"] == b"v"
            ok("sync_level=SYNC_OFF → ok")

        with KVStore(
            db_path(10) + "3",
            journal_mode=JOURNAL_WAL,
            sync_level=SYNC_FULL,
            page_size=8192,
        ) as db:
            db["s"] = "v"
            assert db["s"] == b"v"
            ok("sync_level=SYNC_FULL, page_size=8192 → ok")

        # ------------------------------------------------------------------ #
        # 11. Maintenance                                                      #
        # ------------------------------------------------------------------ #
        section(11, "Maintenance — sync, vacuum, integrity_check, checkpoint, stats, errmsg")

        with KVStore(db_path(11)) as db:
            db.begin(write=True)
            for i in range(20):
                db[f"maint:{i:03d}"] = f"v{i}"
            db.commit()

            db.sync()
            ok("sync() — no error")

            db.vacuum()
            ok("vacuum() — no error")

            db.integrity_check()
            ok("integrity_check() — database is clean")

            nlog, nckpt = db.checkpoint(CHECKPOINT_PASSIVE)
            assert isinstance(nlog, int) and isinstance(nckpt, int)
            ok(f"checkpoint(PASSIVE) → nlog={nlog}, nckpt={nckpt}")

            nlog, nckpt = db.checkpoint(CHECKPOINT_FULL)
            ok(f"checkpoint(FULL)    → nlog={nlog}, nckpt={nckpt}")

            nlog, nckpt = db.checkpoint(CHECKPOINT_RESTART)
            ok(f"checkpoint(RESTART) → nlog={nlog}, nckpt={nckpt}")

            nlog, nckpt = db.checkpoint(CHECKPOINT_TRUNCATE)
            ok(f"checkpoint(TRUNCATE)→ nlog={nlog}, nckpt={nckpt}")

            s = db.stats()
            assert isinstance(s, dict)
            assert s["puts"] >= 20
            ok(f"stats() → puts={s['puts']}, gets={s['gets']}, deletes={s['deletes']}, errors={s['errors']}")

            msg = db.errmsg
            assert isinstance(msg, str)
            ok(f"errmsg property → {msg!r}")

        # ------------------------------------------------------------------ #
        # 12. Exceptions                                                       #
        # ------------------------------------------------------------------ #
        section(12, "Exceptions")

        with KVStore(db_path(12)) as db:
            db["k"] = "v"

            # NotFoundError IS-A KeyError
            try:
                _ = db["ghost"]
                assert False
            except NotFoundError as e:
                assert isinstance(e, KeyError)
                ok("NotFoundError IS-A KeyError")

            # delete missing key → NotFoundError
            try:
                db.delete(b"ghost")
                assert False
            except NotFoundError:
                ok("delete(missing) → NotFoundError")

            # ttl() on missing key → NotFoundError
            try:
                db.ttl(b"ghost")
                assert False
            except NotFoundError:
                ok("ttl(missing) → NotFoundError")

            # open missing CF → NotFoundError
            try:
                db.open_column_family("never_created")
                assert False
            except NotFoundError:
                ok("open_column_family(missing) → NotFoundError")

            # last() on forward iterator → Error
            it = db.iterator()
            try:
                it.last()
                assert False
            except Error:
                ok("last() on forward iterator → Error")

            # prev() on forward iterator → Error
            try:
                it.prev()
                assert False
            except Error:
                ok("prev() on forward iterator → Error")
            it.close()

            # TypeError on wrong key type
            try:
                db.put(123, b"v")   # type: ignore[arg-type]
                assert False
            except TypeError:
                ok("put(int_key) → TypeError")

        # ------------------------------------------------------------------ #
        # 13. Encrypted store — validate all APIs                          #
        # ------------------------------------------------------------------ #
        section(13, "Encrypted store — validate all APIs")

        SENTINEL = b"ENC_SENTINEL_XYZABC_12345"
        with KVStore.open_encrypted(db_path(13), b"pass13") as db:
            db.put(b"sentinel", SENTINEL)
        # Ciphertext check: known plaintext must NOT appear in raw file bytes
        with open(db_path(13), "rb") as f:
            raw = f.read()
        assert SENTINEL not in raw
        ok("encrypted file: sentinel NOT readable in raw bytes  (ciphertext confirmed)")

        with KVStore.open_encrypted(db_path(13), b"pass13") as db:
            assert db.is_encrypted() is True
            ok("is_encrypted() → True")
            try:
                KVStore.open_encrypted(db_path(13), b"wrongpass")
                assert False
            except AuthError:
                ok("wrong password → AuthError")
            _validate_all_apis(db, "s13", parent=13)

        # ------------------------------------------------------------------ #
        # 14. Encrypted → remove_encryption (plain) — validate all APIs      #
        # ------------------------------------------------------------------ #
        section(14, "Encrypted → remove_encryption (plain) — validate all APIs")

        SENTINEL = b"PLAIN_SENTINEL_UVWXYZ_67890"
        with KVStore.open_encrypted(db_path(14), b"pass14") as db:
            db.put(b"sentinel", SENTINEL)
            db.remove_encryption()
        # Plaintext check: after decryption the value must be readable in raw file
        with open(db_path(14), "rb") as f:
            raw = f.read()
        assert SENTINEL in raw
        ok("plain file after remove_encryption: sentinel readable in raw bytes  (plaintext confirmed)")

        with KVStore(db_path(14)) as db:
            assert db.is_encrypted() is False
            ok("is_encrypted() → False")
            assert db.get(b"sentinel") == SENTINEL
            ok("sentinel value readable after remove_encryption")
            _validate_all_apis(db, "s14", parent=14)

        # ------------------------------------------------------------------ #
        # 15. Encrypted → plain → re-encrypted — validate all APIs           #
        # ------------------------------------------------------------------ #
        section(15, "Encrypted → plain → re-encrypted — validate all APIs")

        SENTINEL = b"REENC_SENTINEL_MNOPQ_11223"
        # Phase 1: encrypt
        with KVStore.open_encrypted(db_path(15), b"pass15a") as db:
            db.put(b"sentinel", SENTINEL)
            db.remove_encryption()
        # Confirm plain
        with open(db_path(15), "rb") as f:
            raw = f.read()
        assert SENTINEL in raw
        ok("after remove_encryption: sentinel readable in raw bytes")

        # Phase 2: re-encrypt via open_encrypted on a plain store
        with KVStore.open_encrypted(db_path(15), b"pass15b") as db:
            assert db.is_encrypted() is True
            assert db.get(b"sentinel") == SENTINEL
            ok("open_encrypted on plain store re-encrypts, data preserved")
        # Confirm ciphertext
        with open(db_path(15), "rb") as f:
            raw = f.read()
        assert SENTINEL not in raw
        ok("after re-encrypt: sentinel NOT readable in raw bytes  (ciphertext confirmed)")

        # Phase 1 password must fail
        try:
            KVStore.open_encrypted(db_path(15), b"pass15a")
            assert False
        except AuthError:
            ok("phase-1 password rejected after re-encrypt → AuthError")

        with KVStore.open_encrypted(db_path(15), b"pass15b") as db:
            assert db.is_encrypted() is True
            ok("is_encrypted() → True")
            _validate_all_apis(db, "s15", parent=15)

        # ------------------------------------------------------------------ #
        # 16. reencrypt (password rotation) — validate all APIs              #
        # ------------------------------------------------------------------ #
        section(16, "reencrypt — password rotation — validate all APIs")

        SENTINEL = b"REKEY_SENTINEL_DEFGH_33445"
        with KVStore.open_encrypted(db_path(16), b"pass16a") as db:
            db.put(b"sentinel", SENTINEL)
            db.reencrypt(b"pass16b")
        # Confirm ciphertext (new key)
        with open(db_path(16), "rb") as f:
            raw = f.read()
        assert SENTINEL not in raw
        ok("after reencrypt: sentinel NOT readable in raw bytes  (ciphertext confirmed)")

        # Old password must fail
        try:
            KVStore.open_encrypted(db_path(16), b"pass16a")
            assert False
        except AuthError:
            ok("old password after reencrypt → AuthError")

        with KVStore.open_encrypted(db_path(16), b"pass16b") as db:
            assert db.is_encrypted() is True
            ok("new password accepted, is_encrypted() → True")
            assert db.get(b"sentinel") == SENTINEL
            ok("sentinel readable with new password")
            _validate_all_apis(db, "s16", parent=16)

    # ------------------------------------------------------------------ #
    # Done                                                                #
    # ------------------------------------------------------------------ #
    print(f"\n{'='*60}")
    print(f"  ALL CHECKS PASSED")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run_all()
