"""
CLI integration tests for snkvctl.

Covers every command and flag exposed by snkvctl:
  put [--ttl], get, del [key|--prefix], exists, list, scan,
  count, clear, set-if-absent [--ttl], ttl, purge,
  cf create/list/drop, stats [--reset], info, check,
  checkpoint [--mode], vacuum [--pages], sync, txn [--dry-run --cf],
  encrypt, decrypt, rekey
  Global flags: --cf, --password, --format, --timeout, --journal,
                --sync, --cache-size, --page-size, --read-only,
                --wal-limit, --full-mutex
  CF variations: del, exists, clear, set-if-absent --ttl, ttl, purge,
                 count, scan --seek

Run with:
    pytest python/tests/test_snkvctl.py
or from the python/ directory:
    pytest tests/test_snkvctl.py
"""

import json
import subprocess
import sys
import time

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def snkvctl(*args, stdin: str = None):
    """Run snkvctl with the given args; return CompletedProcess."""
    cmd = [sys.executable, "-m", "snkv.snkvctl", *[str(a) for a in args]]
    return subprocess.run(
        cmd,
        input=stdin,
        capture_output=True,
        text=True,
    )


def run(db: str, *args, cf: str = None, fmt: str = None,
        password: str = None, stdin: str = None, timeout: int = None,
        read_only: bool = False):
    """Convenience wrapper: build full snkvctl invocation and return (rc, stdout, stderr)."""
    global_flags = ["--db", db]
    if cf:
        global_flags += ["--cf", cf]
    if password:
        global_flags += ["--password", password]
    if fmt:
        global_flags += ["--format", fmt]
    if timeout is not None:
        global_flags += ["--timeout", str(timeout)]
    if read_only:
        global_flags += ["--read-only"]
    proc = snkvctl(*global_flags, *args, stdin=stdin)
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    return str(tmp_path / "test.db")


@pytest.fixture
def enc_db(tmp_path):
    return str(tmp_path / "enc.db")


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

class TestCRUD:
    def test_put_get(self, db):
        rc, out, _ = run(db, "put", "hello", "world")
        assert rc == 0
        rc, out, _ = run(db, "get", "hello")
        assert rc == 0
        assert out == "world"

    def test_get_missing(self, db):
        rc, out, err = run(db, "get", "nokey")
        assert rc == 2
        assert "not found" in err

    def test_del(self, db):
        run(db, "put", "k", "v")
        rc, _, _ = run(db, "del", "k")
        assert rc == 0
        rc, _, _ = run(db, "get", "k")
        assert rc == 2

    def test_del_missing(self, db):
        rc, _, err = run(db, "del", "nokey")
        assert rc == 2
        assert "not found" in err

    def test_exists_found(self, db):
        run(db, "put", "x", "1")
        rc, out, _ = run(db, "exists", "x")
        assert rc == 0
        assert "found" in out

    def test_exists_missing(self, db):
        rc, out, _ = run(db, "exists", "missing")
        assert rc == 2
        assert "not found" in out

    def test_list(self, db):
        run(db, "put", "a", "1")
        run(db, "put", "b", "2")
        rc, out, _ = run(db, "list")
        assert rc == 0
        assert "a" in out
        assert "b" in out

    def test_list_prefix(self, db):
        run(db, "put", "user:alice", "30")
        run(db, "put", "user:bob", "25")
        run(db, "put", "config:x", "y")
        rc, out, _ = run(db, "list", "--prefix", "user:")
        assert rc == 0
        assert "user:alice" in out
        assert "user:bob" in out
        assert "config:x" not in out

    def test_list_limit(self, db):
        for i in range(5):
            run(db, "put", f"k{i}", str(i))
        rc, out, _ = run(db, "list", "--limit", "2")
        assert rc == 0
        assert len(out.splitlines()) == 2

    def test_list_reverse(self, db):
        run(db, "put", "a", "1")
        run(db, "put", "b", "2")
        rc, out, _ = run(db, "list", "--reverse")
        assert rc == 0
        keys = out.splitlines()
        assert keys[0] == "b"
        assert keys[1] == "a"

    def test_scan(self, db):
        run(db, "put", "user:alice", "30")
        rc, out, _ = run(db, "scan", "--prefix", "user:")
        assert rc == 0
        assert "user:alice" in out
        assert "30" in out

    def test_scan_reverse_limit(self, db):
        run(db, "put", "user:alice", "30")
        run(db, "put", "user:bob", "25")
        rc, out, _ = run(db, "scan", "--prefix", "user:", "--reverse", "--limit", "1")
        assert rc == 0
        lines = out.splitlines()
        assert lines[0] == "user:bob"
        assert lines[1] == "25"

    def test_count_prefix(self, db):
        run(db, "put", "user:alice", "30")
        run(db, "put", "user:bob", "25")
        run(db, "put", "config:x", "y")
        rc, out, _ = run(db, "count", "--prefix", "user:")
        assert rc == 0
        assert out == "2"

    def test_count_all(self, db):
        run(db, "put", "a", "1")
        run(db, "put", "b", "2")
        rc, out, _ = run(db, "count")
        assert rc == 0
        assert int(out) >= 2

    def test_clear(self, db):
        run(db, "put", "k1", "v1")
        run(db, "put", "k2", "v2")
        rc, out, _ = run(db, "clear")
        assert rc == 0
        assert "cleared" in out
        _, out, _ = run(db, "count")
        assert out == "0"

    def test_set_if_absent_insert(self, db):
        rc, out, _ = run(db, "set-if-absent", "newkey", "hello")
        assert rc == 0
        assert "inserted" in out
        _, val, _ = run(db, "get", "newkey")
        assert val == "hello"

    def test_set_if_absent_existing(self, db):
        run(db, "put", "k", "original")
        rc, out, _ = run(db, "set-if-absent", "k", "other")
        assert rc == 0
        assert "already exists" in out
        _, val, _ = run(db, "get", "k")
        assert val == "original"


# ---------------------------------------------------------------------------
# TTL
# ---------------------------------------------------------------------------

class TestTTL:
    def test_ttl_remaining(self, db):
        run(db, "put", "sess", "tok", "--ttl", "60")
        rc, out, _ = run(db, "ttl", "sess")
        assert rc == 0
        assert "remaining" in out
        secs = float(out.split("s")[0])
        assert 55.0 < secs <= 60.0

    def test_ttl_no_expiry(self, db):
        run(db, "put", "perm", "val")
        rc, out, _ = run(db, "ttl", "perm")
        assert rc == 0
        assert "no ttl" in out.lower()

    def test_ttl_expired(self, db):
        run(db, "put", "exp", "v", "--ttl", "1")
        time.sleep(1.5)
        rc, out, _ = run(db, "ttl", "exp")
        assert rc == 0
        assert "expired" in out

    def test_ttl_missing_key(self, db):
        rc, _, err = run(db, "ttl", "nokey")
        assert rc == 2
        assert "not found" in err

    def test_purge(self, db):
        run(db, "put", "p1", "v1", "--ttl", "1")
        run(db, "put", "p2", "v2", "--ttl", "1")
        run(db, "put", "perm", "vp")
        time.sleep(1.5)
        rc, out, _ = run(db, "purge")
        assert rc == 0
        assert "2" in out
        # permanent key must survive
        _, val, _ = run(db, "get", "perm")
        assert val == "vp"

    def test_purge_nothing(self, db):
        rc, out, _ = run(db, "purge")
        assert rc == 0
        assert "0" in out


# ---------------------------------------------------------------------------
# Column families
# ---------------------------------------------------------------------------

class TestColumnFamilies:
    def test_cf_list_empty(self, db):
        rc, out, _ = run(db, "cf", "list")
        assert rc == 0
        assert "no user column families" in out.lower()

    def test_cf_create_and_list(self, db):
        rc, out, _ = run(db, "cf", "create", "users")
        assert rc == 0
        assert "users" in out
        rc, out, _ = run(db, "cf", "list")
        assert "users" in out

    def test_cf_put_get(self, db):
        run(db, "cf", "create", "users")
        run(db, "put", "alice", "30", cf="users")
        rc, out, _ = run(db, "get", "alice", cf="users")
        assert rc == 0
        assert out == "30"

    def test_cf_isolation(self, db):
        run(db, "cf", "create", "a")
        run(db, "cf", "create", "b")
        run(db, "put", "key", "in_a", cf="a")
        run(db, "put", "key", "in_b", cf="b")
        _, va, _ = run(db, "get", "key", cf="a")
        _, vb, _ = run(db, "get", "key", cf="b")
        assert va == "in_a"
        assert vb == "in_b"

    def test_cf_scan(self, db):
        run(db, "cf", "create", "fam")
        run(db, "put", "alice", "30", cf="fam")
        run(db, "put", "bob", "25", cf="fam")
        rc, out, _ = run(db, "scan", cf="fam")
        assert rc == 0
        assert "alice" in out and "30" in out
        assert "bob" in out and "25" in out

    def test_cf_drop(self, db):
        run(db, "cf", "create", "tmp")
        rc, out, _ = run(db, "cf", "drop", "tmp")
        assert rc == 0
        assert "tmp" in out
        _, out, _ = run(db, "cf", "list")
        assert "tmp" not in out


# ---------------------------------------------------------------------------
# Stats, info, admin
# ---------------------------------------------------------------------------

class TestAdmin:
    def test_stats_text(self, db):
        rc, out, _ = run(db, "stats")
        assert rc == 0
        assert "puts" in out
        assert "gets" in out
        assert "db_pages" in out

    def test_stats_json(self, db):
        rc, out, _ = run(db, "stats", fmt="json")
        assert rc == 0
        d = json.loads(out)
        assert "puts" in d
        assert "db_pages" in d

    def test_stats_reset(self, db):
        rc, _, _ = run(db, "stats", "--reset")
        assert rc == 0

    def test_info_text(self, db):
        rc, out, _ = run(db, "info")
        assert rc == 0
        assert "encrypted" in out
        assert "db_pages" in out

    def test_info_json(self, db):
        rc, out, _ = run(db, "info", fmt="json")
        assert rc == 0
        d = json.loads(out)
        assert "encrypted" in d
        assert "db_pages" in d
        assert "path" in d

    def test_info_not_encrypted(self, db):
        _, out, _ = run(db, "info")
        assert "no" in out.lower()

    def test_check(self, db):
        rc, out, _ = run(db, "check")
        assert rc == 0
        assert "passed" in out.lower()

    def test_check_json(self, db):
        rc, out, _ = run(db, "check", fmt="json")
        assert rc == 0
        d = json.loads(out)
        assert d["status"] == "ok"

    def test_checkpoint_default(self, db):
        rc, out, _ = run(db, "checkpoint")
        assert rc == 0
        assert "mode=passive" in out

    def test_checkpoint_modes(self, db):
        for mode in ("passive", "full", "restart", "truncate"):
            rc, out, _ = run(db, "checkpoint", "--mode", mode)
            assert rc == 0, f"checkpoint --mode {mode} failed"
            assert f"mode={mode}" in out

    def test_vacuum(self, db):
        rc, out, _ = run(db, "vacuum")
        assert rc == 0
        assert "vacuum complete" in out

    def test_vacuum_pages(self, db):
        rc, out, _ = run(db, "vacuum", "--pages", "5")
        assert rc == 0
        assert "pages=5" in out

    def test_sync(self, db):
        rc, out, _ = run(db, "sync")
        assert rc == 0
        assert "sync complete" in out

    def test_sync_json(self, db):
        rc, out, _ = run(db, "sync", fmt="json")
        assert rc == 0
        d = json.loads(out)
        assert d["status"] == "ok"


# ---------------------------------------------------------------------------
# Transactions (txn)
# ---------------------------------------------------------------------------

class TestTxn:
    def test_txn_basic(self, db):
        rc, out, _ = run(db, "txn", stdin="put counter 1\nput flag on\n")
        assert rc == 0
        assert "2 op(s) committed" in out
        _, v1, _ = run(db, "get", "counter")
        _, v2, _ = run(db, "get", "flag")
        assert v1 == "1"
        assert v2 == "on"

    def test_txn_del(self, db):
        run(db, "put", "gone", "bye")
        rc, out, _ = run(db, "txn", stdin="del gone\n")
        assert rc == 0
        assert "1 op(s) committed" in out
        rc, _, _ = run(db, "get", "gone")
        assert rc == 2

    def test_txn_dry_run(self, db):
        run(db, "put", "counter", "1")
        rc, out, _ = run(db, "txn", "--dry-run", stdin="put counter 999\n")
        assert rc == 0
        assert "rolled back" in out
        _, val, _ = run(db, "get", "counter")
        assert val == "1"

    def test_txn_empty_input(self, db):
        rc, out, _ = run(db, "txn", stdin="")
        assert rc == 0
        assert "nothing" in out.lower() or "0" in out

    def test_txn_bad_op(self, db):
        rc, _, err = run(db, "txn", stdin="put a 1\nbadop x\n")
        assert rc == 1
        assert "unknown op" in err

    def test_txn_cf_subcommand_flag(self, db):
        run(db, "cf", "create", "fam")
        rc, out, _ = run(db, "txn", "--cf", "fam", stdin="put x 10\nput y 20\n")
        assert rc == 0
        _, vx, _ = run(db, "get", "x", cf="fam")
        _, vy, _ = run(db, "get", "y", cf="fam")
        assert vx == "10"
        assert vy == "20"

    def test_txn_cf_global_flag(self, db):
        run(db, "cf", "create", "fam2")
        rc, out, _ = run(db, "txn", stdin="put a 1\nput b 2\n", cf="fam2")
        assert rc == 0
        _, va, _ = run(db, "get", "a", cf="fam2")
        assert va == "1"

    def test_txn_json_output(self, db):
        rc, out, _ = run(db, "txn", fmt="json", stdin="put j1 v1\n")
        assert rc == 0
        d = json.loads(out)
        assert d["committed"] == 1

    def test_txn_dry_run_json(self, db):
        rc, out, _ = run(db, "txn", "--dry-run", fmt="json", stdin="put j2 v2\n")
        assert rc == 0
        d = json.loads(out)
        assert d["committed"] == 0
        assert d["dry_run"] is True


# ---------------------------------------------------------------------------
# JSON output format
# ---------------------------------------------------------------------------

class TestJsonFormat:
    def test_get_json(self, db):
        run(db, "put", "k", "v")
        rc, out, _ = run(db, "get", "k", fmt="json")
        assert rc == 0
        assert json.loads(out) == {"value": "v"}

    def test_put_json(self, db):
        rc, out, _ = run(db, "put", "k", "v", fmt="json")
        assert rc == 0
        d = json.loads(out)
        assert d["key"] == "k"

    def test_put_ttl_json(self, db):
        rc, out, _ = run(db, "put", "k", "v", "--ttl", "30", fmt="json")
        assert rc == 0
        d = json.loads(out)
        assert d["key"] == "k"
        assert "ttl" in d

    def test_del_json(self, db):
        run(db, "put", "k", "v")
        rc, out, _ = run(db, "del", "k", fmt="json")
        assert rc == 0
        d = json.loads(out)
        assert d["deleted"] is True
        assert d["key"] == "k"

    def test_exists_json(self, db):
        run(db, "put", "k", "v")
        rc, out, _ = run(db, "exists", "k", fmt="json")
        assert rc == 0
        d = json.loads(out)
        assert d["exists"] is True

    def test_scan_json(self, db):
        run(db, "put", "user:a", "1")
        run(db, "put", "user:b", "2")
        rc, out, _ = run(db, "scan", "--prefix", "user:", fmt="json")
        assert rc == 0
        lines = [json.loads(l) for l in out.splitlines()]
        assert len(lines) == 2
        assert all("key" in l and "value" in l for l in lines)

    def test_list_json(self, db):
        run(db, "put", "a", "1")
        rc, out, _ = run(db, "list", fmt="json")
        assert rc == 0
        d = json.loads(out.splitlines()[0])
        assert "key" in d

    def test_count_json(self, db):
        run(db, "put", "a", "1")
        rc, out, _ = run(db, "count", fmt="json")
        assert rc == 0
        d = json.loads(out)
        assert "count" in d

    def test_set_if_absent_json(self, db):
        rc, out, _ = run(db, "set-if-absent", "newk", "newv", fmt="json")
        assert rc == 0
        d = json.loads(out)
        assert d["inserted"] is True

    def test_ttl_json(self, db):
        run(db, "put", "sess", "tok", "--ttl", "60")
        rc, out, _ = run(db, "ttl", "sess", fmt="json")
        assert rc == 0
        d = json.loads(out)
        assert "ttl" in d
        assert d["ttl"] > 50

    def test_purge_json(self, db):
        rc, out, _ = run(db, "purge", fmt="json")
        assert rc == 0
        d = json.loads(out)
        assert "purged" in d

    def test_cf_list_json(self, db):
        run(db, "cf", "create", "fam")
        rc, out, _ = run(db, "cf", "list", fmt="json")
        assert rc == 0
        d = json.loads(out)
        assert "column_families" in d
        assert "fam" in d["column_families"]

    def test_format_before_subcommand(self, db):
        run(db, "put", "k", "v")
        rc, out, _ = run(db, "get", "k", fmt="json")
        assert rc == 0
        assert json.loads(out) == {"value": "v"}


# ---------------------------------------------------------------------------
# Encryption
# ---------------------------------------------------------------------------

class TestEncryption:
    def test_encrypt_and_read(self, enc_db):
        run(enc_db, "put", "k1", "v1")
        rc, out, _ = run(enc_db, "encrypt", "--new-password", "secret")
        assert rc == 0
        assert "encrypted" in out.lower()
        rc, out, _ = run(enc_db, "get", "k1", password="secret")
        assert rc == 0
        assert out == "v1"

    def test_encrypted_put_get(self, enc_db):
        run(enc_db, "encrypt", "--new-password", "pw")
        run(enc_db, "put", "ek", "ev", password="pw")
        rc, out, _ = run(enc_db, "get", "ek", password="pw")
        assert rc == 0
        assert out == "ev"

    def test_wrong_password_rejected(self, enc_db):
        run(enc_db, "encrypt", "--new-password", "right")
        rc, _, err = run(enc_db, "get", "k", password="wrong")
        assert rc == 1
        assert "password" in err.lower() or "encrypted" in err.lower()

    def test_no_password_rejected(self, enc_db):
        run(enc_db, "encrypt", "--new-password", "pw")
        rc, _, err = run(enc_db, "get", "k")
        assert rc == 1

    def test_info_shows_encrypted(self, enc_db):
        run(enc_db, "encrypt", "--new-password", "pw")
        rc, out, _ = run(enc_db, "info", password="pw")
        assert rc == 0
        assert "yes" in out.lower()

    def test_rekey(self, enc_db):
        run(enc_db, "encrypt", "--new-password", "old")
        run(enc_db, "put", "k", "v", password="old")
        rc, out, _ = run(enc_db, "rekey", "--new-password", "new", password="old")
        assert rc == 0
        assert "rekeyed" in out.lower()
        rc, out, _ = run(enc_db, "get", "k", password="new")
        assert rc == 0
        assert out == "v"
        rc, _, _ = run(enc_db, "get", "k", password="old")
        assert rc == 1

    def test_decrypt(self, enc_db):
        run(enc_db, "put", "k", "v")
        run(enc_db, "encrypt", "--new-password", "pw")
        rc, out, _ = run(enc_db, "decrypt", password="pw")
        assert rc == 0
        assert "decrypted" in out.lower()
        rc, out, _ = run(enc_db, "get", "k")
        assert rc == 0
        assert out == "v"
        _, info_out, _ = run(enc_db, "info")
        assert "no" in info_out.lower()

    def test_encrypt_preserves_data(self, enc_db):
        for i in range(5):
            run(enc_db, "put", f"key{i}", f"val{i}")
        run(enc_db, "encrypt", "--new-password", "pw")
        for i in range(5):
            rc, out, _ = run(enc_db, "get", f"key{i}", password="pw")
            assert rc == 0
            assert out == f"val{i}"


# ---------------------------------------------------------------------------
# Exit codes
# ---------------------------------------------------------------------------

class TestExitCodes:
    def test_success_is_0(self, db):
        rc, _, _ = run(db, "put", "k", "v")
        assert rc == 0

    def test_not_found_is_2(self, db):
        rc, _, _ = run(db, "get", "missing")
        assert rc == 2

    def test_exists_missing_is_2(self, db):
        rc, _, _ = run(db, "exists", "missing")
        assert rc == 2

    def test_del_missing_is_2(self, db):
        rc, _, _ = run(db, "del", "missing")
        assert rc == 2

    def test_ttl_missing_is_2(self, db):
        rc, _, _ = run(db, "ttl", "missing")
        assert rc == 2

    def test_bad_args_is_1(self, db):
        proc = snkvctl("--db", db, "put")  # missing KEY VALUE
        assert proc.returncode == 1

    def test_wrong_password_is_1(self, enc_db):
        run(enc_db, "encrypt", "--new-password", "pw")
        rc, _, _ = run(enc_db, "get", "k", password="wrong")
        assert rc == 1


# ---------------------------------------------------------------------------
# Read-only mode
# ---------------------------------------------------------------------------

class TestReadOnly:
    def test_read_allowed(self, db):
        run(db, "put", "k", "v")  # create DB first
        rc, out, _ = run(db, "get", "k", read_only=True)
        assert rc == 0
        assert out == "v"

    def test_write_blocked(self, db):
        run(db, "put", "k", "v")  # create DB first
        rc, _, err = run(db, "put", "k2", "v2", read_only=True)
        assert rc == 1
        assert "read-only" in err.lower()


# ---------------------------------------------------------------------------
# Global flags
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# del --prefix
# ---------------------------------------------------------------------------

class TestDelPrefix:
    def test_del_prefix_basic(self, db):
        run(db, "put", "user:alice", "30")
        run(db, "put", "user:bob", "25")
        run(db, "put", "config:x", "y")
        rc, out, _ = run(db, "del", "--prefix", "user:")
        assert rc == 0
        assert "2" in out
        _, out, _ = run(db, "list")
        assert "user:alice" not in out
        assert "user:bob" not in out
        assert "config:x" in out

    def test_del_prefix_empty_match(self, db):
        run(db, "put", "a", "1")
        rc, out, _ = run(db, "del", "--prefix", "nobody:")
        assert rc == 0
        assert "0" in out

    def test_del_prefix_json(self, db):
        run(db, "put", "x:1", "a")
        run(db, "put", "x:2", "b")
        rc, out, _ = run(db, "del", "--prefix", "x:", fmt="json")
        assert rc == 0
        d = json.loads(out)
        assert d["deleted"] == 2
        assert d["prefix"] == "x:"

    def test_del_prefix_cf(self, db):
        run(db, "cf", "create", "fam")
        run(db, "put", "user:p1", "v1", cf="fam")
        run(db, "put", "user:p2", "v2", cf="fam")
        run(db, "put", "other", "o", cf="fam")
        rc, out, _ = run(db, "del", "--prefix", "user:", cf="fam")
        assert rc == 0
        assert "2" in out
        _, keys, _ = run(db, "list", cf="fam")
        assert "user:p1" not in keys
        assert "user:p2" not in keys
        assert "other" in keys

    def test_del_prefix_atomic(self, db):
        for i in range(10):
            run(db, "put", f"batch:{i}", str(i))
        run(db, "put", "keep", "me")
        rc, out, _ = run(db, "del", "--prefix", "batch:")
        assert rc == 0
        assert "10" in out
        _, cnt, _ = run(db, "count", "--prefix", "batch:")
        assert cnt == "0"
        _, val, _ = run(db, "get", "keep")
        assert val == "me"

    def test_del_prefix_mutually_exclusive_with_key(self, db):
        # Passing both a positional key AND --prefix should error
        proc = snkvctl("--db", db, "del", "somekey", "--prefix", "p:")
        assert proc.returncode != 0


# ---------------------------------------------------------------------------
# scan / list --seek
# ---------------------------------------------------------------------------

class TestSeek:
    def _setup(self, db):
        for k, v in [("a", "1"), ("m", "2"), ("user:alice", "30"),
                     ("user:bob", "25"), ("user:carol", "40"), ("z", "3")]:
            run(db, "put", k, v)

    def test_scan_seek_exact(self, db):
        self._setup(db)
        rc, out, _ = run(db, "scan", "--seek", "m")
        assert rc == 0
        keys = out.splitlines()[::2]   # every other line is a key
        assert keys[0] == "m"
        assert "a" not in keys

    def test_scan_seek_between_keys(self, db):
        self._setup(db)
        rc, out, _ = run(db, "scan", "--seek", "user:b")
        assert rc == 0
        keys = out.splitlines()[::2]
        assert keys[0] == "user:bob"
        assert "user:alice" not in keys

    def test_list_seek(self, db):
        self._setup(db)
        rc, out, _ = run(db, "list", "--seek", "user:b")
        assert rc == 0
        lines = out.splitlines()
        assert lines[0] == "user:bob"
        assert "user:alice" not in lines

    def test_seek_past_last_key(self, db):
        self._setup(db)
        rc, out, _ = run(db, "scan", "--seek", "zzz")
        assert rc == 0
        assert out == ""

    def test_seek_with_limit(self, db):
        self._setup(db)
        rc, out, _ = run(db, "scan", "--seek", "m", "--limit", "2")
        assert rc == 0
        keys = out.splitlines()[::2]
        assert len(keys) == 2
        assert keys[0] == "m"

    def test_seek_with_prefix(self, db):
        self._setup(db)
        rc, out, _ = run(db, "scan", "--prefix", "user:", "--seek", "user:b")
        assert rc == 0
        keys = out.splitlines()[::2]
        assert keys[0] == "user:bob"
        assert "user:alice" not in keys
        assert "z" not in keys

    def test_seek_reverse(self, db):
        self._setup(db)
        rc, out, _ = run(db, "scan", "--seek", "user:carol", "--reverse")
        assert rc == 0
        keys = out.splitlines()[::2]
        assert keys[0] == "user:carol"
        assert keys[-1] == "a"

    def test_seek_json(self, db):
        self._setup(db)
        rc, out, _ = run(db, "scan", "--seek", "user:b", "--limit", "1",
                         fmt="json")
        assert rc == 0
        d = json.loads(out)
        assert d["key"] == "user:bob"


# ---------------------------------------------------------------------------
# CF variations — operations that work on a named column family
# ---------------------------------------------------------------------------

class TestCFOperations:
    def test_cf_del_single_key(self, db):
        run(db, "cf", "create", "fam")
        run(db, "put", "k", "v", cf="fam")
        rc, _, _ = run(db, "del", "k", cf="fam")
        assert rc == 0
        rc, _, _ = run(db, "get", "k", cf="fam")
        assert rc == 2

    def test_cf_exists(self, db):
        run(db, "cf", "create", "fam")
        run(db, "put", "k", "v", cf="fam")
        rc, out, _ = run(db, "exists", "k", cf="fam")
        assert rc == 0
        assert "found" in out
        rc, out, _ = run(db, "exists", "missing", cf="fam")
        assert rc == 2

    def test_cf_clear(self, db):
        run(db, "cf", "create", "fam")
        run(db, "put", "a", "1", cf="fam")
        run(db, "put", "b", "2", cf="fam")
        rc, out, _ = run(db, "clear", cf="fam")
        assert rc == 0
        _, cnt, _ = run(db, "count", cf="fam")
        assert cnt == "0"
        # root store unaffected
        run(db, "put", "root", "val")
        _, cnt, _ = run(db, "count")
        assert int(cnt) >= 1

    def test_cf_set_if_absent(self, db):
        run(db, "cf", "create", "fam")
        rc, out, _ = run(db, "set-if-absent", "k", "v", cf="fam")
        assert rc == 0
        assert "inserted" in out
        rc, out, _ = run(db, "set-if-absent", "k", "other", cf="fam")
        assert rc == 0
        assert "already exists" in out
        _, val, _ = run(db, "get", "k", cf="fam")
        assert val == "v"

    def test_cf_set_if_absent_ttl(self, db):
        run(db, "cf", "create", "fam")
        rc, _, _ = run(db, "set-if-absent", "k", "v", "--ttl", "60", cf="fam")
        assert rc == 0
        rc, out, _ = run(db, "ttl", "k", cf="fam")
        assert rc == 0
        assert "remaining" in out

    def test_cf_ttl(self, db):
        run(db, "cf", "create", "fam")
        run(db, "put", "k", "v", "--ttl", "60", cf="fam")
        rc, out, _ = run(db, "ttl", "k", cf="fam")
        assert rc == 0
        assert "remaining" in out
        secs = float(out.split("s")[0])
        assert 55.0 < secs <= 60.0

    def test_cf_purge(self, db):
        run(db, "cf", "create", "fam")
        run(db, "put", "exp1", "v", "--ttl", "1", cf="fam")
        run(db, "put", "exp2", "v", "--ttl", "1", cf="fam")
        run(db, "put", "perm", "v", cf="fam")
        time.sleep(1.5)
        rc, out, _ = run(db, "purge", cf="fam")
        assert rc == 0
        assert "2" in out
        _, val, _ = run(db, "get", "perm", cf="fam")
        assert val == "v"

    def test_cf_count(self, db):
        run(db, "cf", "create", "fam")
        run(db, "put", "a", "1", cf="fam")
        run(db, "put", "b", "2", cf="fam")
        rc, out, _ = run(db, "count", cf="fam")
        assert rc == 0
        assert out == "2"

    def test_cf_scan_seek(self, db):
        run(db, "cf", "create", "fam")
        for k, v in [("a", "1"), ("m", "2"), ("z", "3")]:
            run(db, "put", k, v, cf="fam")
        rc, out, _ = run(db, "scan", "--seek", "m", cf="fam")
        assert rc == 0
        keys = out.splitlines()[::2]
        assert keys[0] == "m"
        assert "a" not in keys

    def test_cf_del_prefix_json(self, db):
        run(db, "cf", "create", "fam")
        run(db, "put", "x:1", "a", cf="fam")
        run(db, "put", "x:2", "b", cf="fam")
        rc, out, _ = run(db, "del", "--prefix", "x:", cf="fam", fmt="json")
        assert rc == 0
        d = json.loads(out)
        assert d["deleted"] == 2


# ---------------------------------------------------------------------------
# set-if-absent --ttl (root store)
# ---------------------------------------------------------------------------

class TestSetIfAbsentTTL:
    def test_set_if_absent_ttl_inserted(self, db):
        rc, out, _ = run(db, "set-if-absent", "k", "v", "--ttl", "60")
        assert rc == 0
        assert "inserted" in out
        _, ttl_out, _ = run(db, "ttl", "k")
        assert "remaining" in ttl_out

    def test_set_if_absent_ttl_not_applied_if_exists(self, db):
        run(db, "put", "k", "original")
        rc, out, _ = run(db, "set-if-absent", "k", "other", "--ttl", "1")
        assert rc == 0
        assert "already exists" in out
        # original had no TTL, should still have none
        _, ttl_out, _ = run(db, "ttl", "k")
        assert "no ttl" in ttl_out.lower()


# ---------------------------------------------------------------------------
# txn edge cases
# ---------------------------------------------------------------------------

class TestTxnEdgeCases:
    def test_txn_del_nonexistent_key(self, db):
        # Deleting a nonexistent key inside txn is a no-op (not an error)
        run(db, "put", "safe", "val")
        rc, out, _ = run(db, "txn", stdin="del nokey\n")
        assert rc == 0
        assert "1 op(s) committed" in out

    def test_txn_del_nonexistent_does_not_rollback_batch(self, db):
        # del of missing key must not roll back other ops in the same batch
        run(db, "put", "counter", "1")
        rc, _, _ = run(db, "txn", stdin="put counter 2\ndel nonexistent\n")
        assert rc == 0
        _, val, _ = run(db, "get", "counter")
        assert val == "2"


# ---------------------------------------------------------------------------

class TestGlobalFlags:
    def test_timeout_flag(self, db):
        rc, _, _ = run(db, "put", "k", "v", timeout=5000)
        assert rc == 0

    def test_journal_delete_mode(self, db):
        proc = snkvctl("--db", db, "--journal", "delete", "put", "k", "v")
        assert proc.returncode == 0

    def test_sync_off(self, db):
        proc = snkvctl("--db", db, "--sync", "off", "put", "k", "v")
        assert proc.returncode == 0

    def test_sync_full(self, db):
        proc = snkvctl("--db", db, "--sync", "full", "put", "k", "v")
        assert proc.returncode == 0

    def test_cache_size(self, db):
        proc = snkvctl("--db", db, "--cache-size", "500", "put", "k", "v")
        assert proc.returncode == 0

    def test_page_size(self, db):
        # page-size only takes effect on new database; use a fresh path
        proc = snkvctl("--db", db, "--page-size", "4096", "put", "k", "v")
        assert proc.returncode == 0

    def test_wal_limit(self, db):
        proc = snkvctl("--db", db, "--wal-limit", "1000", "put", "k", "v")
        assert proc.returncode == 0

    def test_full_mutex(self, db):
        proc = snkvctl("--db", db, "--full-mutex", "put", "k", "v")
        assert proc.returncode == 0

    def test_timeout_zero(self, db):
        # --timeout 0 must be passed through (not silently dropped as falsy)
        rc, _, _ = run(db, "put", "k", "v", timeout=0)
        assert rc == 0


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------

class TestValidation:
    def test_negative_ttl_rejected(self, db):
        rc, _, err = run(db, "put", "k", "v", "--ttl", "-1")
        assert rc == 1
        assert "ttl" in err.lower()

    def test_zero_ttl_rejected(self, db):
        rc, _, err = run(db, "put", "k", "v", "--ttl", "0")
        assert rc == 1
        assert "ttl" in err.lower()

    def test_negative_ttl_set_if_absent_rejected(self, db):
        rc, _, err = run(db, "set-if-absent", "k", "v", "--ttl", "-5")
        assert rc == 1
        assert "ttl" in err.lower()

    def test_negative_limit_rejected(self, db):
        rc, _, err = run(db, "list", "--limit", "-1")
        assert rc == 1
        assert "limit" in err.lower()

    def test_negative_limit_scan_rejected(self, db):
        rc, _, err = run(db, "scan", "--limit", "-1")
        assert rc == 1
        assert "limit" in err.lower()

    def test_encrypt_already_encrypted_helpful_error(self, enc_db):
        run(enc_db, "put", "k", "v")
        run(enc_db, "encrypt", "--new-password", "pw")
        # Trying to encrypt again should give a clear hint, not "snkv error"
        rc, _, err = run(enc_db, "encrypt", "--new-password", "new")
        assert rc == 1
        assert "rekey" in err.lower() or "already encrypted" in err.lower()

    def test_encrypt_json_has_ttl_preserved(self, enc_db):
        run(enc_db, "put", "k", "v")
        rc, out, _ = run(enc_db, "encrypt", "--new-password", "pw", fmt="json")
        assert rc == 0
        d = json.loads(out)
        assert d["ttl_preserved"] is False
