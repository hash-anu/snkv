#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025 SNKV Contributors
"""snkvctl — command-line interface for SNKV databases.

Installed automatically with:   pip install snkv
Usage:   snkvctl --db PATH [options] COMMAND [args]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import contextmanager
from typing import Optional

import snkv
from snkv import (
    KVStore,
    ColumnFamily,
    NotFoundError,
    CorruptError,
    AuthError,
    BusyError,
    LockedError,
    ReadOnlyError,
    Error as SnkvError,
    JOURNAL_WAL,
    JOURNAL_DELETE,
    SYNC_OFF,
    SYNC_NORMAL,
    SYNC_FULL,
    CHECKPOINT_PASSIVE,
    CHECKPOINT_FULL,
    CHECKPOINT_RESTART,
    CHECKPOINT_TRUNCATE,
)

# ── exit codes ───────────────────────────────────────────────────────────────
_OK       = 0
_ERR      = 1
_NOTFOUND = 2

# ── formatting ───────────────────────────────────────────────────────────────

def _fmt_bytes(b: bytes) -> str:
    """Return b decoded as UTF-8, or <hex:AABB…> for non-text bytes."""
    try:
        return b.decode("utf-8")
    except (UnicodeDecodeError, AttributeError):
        return "<hex:" + b.hex().upper() + ">"


def _out_value(val: bytes, fmt: str) -> None:
    """Print a single value (get command text mode: bare value)."""
    if fmt == "json":
        print(json.dumps({"value": _fmt_bytes(val)}))
    else:
        print(_fmt_bytes(val))


def _out_kv(key: bytes, val: bytes, fmt: str) -> None:
    """Print a key/value pair (scan, txn dry-run preview)."""
    k = _fmt_bytes(key)
    v = _fmt_bytes(val)
    if fmt == "json":
        print(json.dumps({"key": k, "value": v}))
    else:
        print(k)
        print(v)


def _out_key(key: bytes, fmt: str) -> None:
    """Print a key only (list command)."""
    k = _fmt_bytes(key)
    if fmt == "json":
        print(json.dumps({"key": k}))
    else:
        print(k)


def _out_record(d: dict, fmt: str) -> None:
    """Print a flat dict (stats, info, checkpoint)."""
    if fmt == "json":
        print(json.dumps(d))
    else:
        width = max((len(str(k)) for k in d), default=0)
        for k, v in d.items():
            print(f"{str(k):<{width + 2}} {v}")


# ── DB open ──────────────────────────────────────────────────────────────────

_JOURNAL_MAP = {"wal": JOURNAL_WAL, "delete": JOURNAL_DELETE}
_SYNC_MAP    = {"off": SYNC_OFF, "normal": SYNC_NORMAL, "full": SYNC_FULL}
_CKPT_MAP    = {
    "passive":  CHECKPOINT_PASSIVE,
    "full":     CHECKPOINT_FULL,
    "restart":  CHECKPOINT_RESTART,
    "truncate": CHECKPOINT_TRUNCATE,
}


def _build_kwargs(args: argparse.Namespace) -> dict:
    """Collect KVStore constructor kwargs from parsed global flags."""
    kw: dict = {}
    if args.timeout is not None:
        kw["busy_timeout"] = args.timeout
    if args.journal != "wal":
        kw["journal_mode"] = _JOURNAL_MAP[args.journal]
    if args.sync != "normal":
        kw["sync_level"] = _SYNC_MAP[args.sync]
    if args.cache_size is not None:
        kw["cache_size"] = args.cache_size
    if args.page_size is not None:
        kw["page_size"] = args.page_size
    if args.read_only:
        kw["read_only"] = 1
    if args.wal_limit is not None:
        kw["wal_size_limit"] = args.wal_limit
    if args.full_mutex:
        kw["full_mutex"] = 1
    return kw


def _open_db(args: argparse.Namespace) -> KVStore:
    """Open the store (plain or encrypted) with all config flags applied."""
    kw = _build_kwargs(args)
    if args.password:
        # open_encrypted does not yet accept config kwargs; apply timeout if any
        db = KVStore.open_encrypted(args.db, args.password)
    else:
        if kw:
            db = KVStore(args.db, **kw)
        else:
            db = KVStore(args.db)
    return db


# ── CF context manager ───────────────────────────────────────────────────────

@contextmanager
def _target(db: KVStore, args: argparse.Namespace):
    """Yield the DB itself or an open ColumnFamily if --cf was given."""
    if getattr(args, "cf", None):
        cf = db.open_column_family(args.cf)
        try:
            yield cf
        finally:
            cf.close()
    else:
        yield db


# ── CRUD commands ────────────────────────────────────────────────────────────

def cmd_put(db: KVStore, args: argparse.Namespace) -> int:
    ttl = getattr(args, "ttl", None)
    if ttl is not None and ttl <= 0:
        print("error: --ttl must be a positive number of seconds", file=sys.stderr)
        return _ERR
    with _target(db, args) as t:
        t.put(args.key, args.value, ttl=ttl)
    if args.format == "json":
        d: dict = {"key": args.key}
        if ttl is not None:
            d["ttl"] = ttl
        print(json.dumps(d))
    return _OK


def cmd_get(db: KVStore, args: argparse.Namespace) -> int:
    with _target(db, args) as t:
        val = t.get(args.key)
    if val is None:
        print(f"not found: {args.key}", file=sys.stderr)
        return _NOTFOUND
    _out_value(val, args.format)
    return _OK


def cmd_del(db: KVStore, args: argparse.Namespace) -> int:
    prefix = getattr(args, "prefix", None)
    if prefix:
        # Prefix delete: open the CF handle (if any) BEFORE begin() since
        # open_column_family cannot be called inside an active transaction.
        # We open a fresh prefix iterator per key so deletes don't invalidate
        # the cursor — O(1) memory, no intermediate list.
        prefix_bytes = prefix.encode()
        n = 0
        with _target(db, args) as t:
            db.begin(write=True)
            try:
                while True:
                    with t.iterator(prefix=prefix_bytes) as it:
                        if it.eof:
                            break
                        key = it.key   # read first matching key
                    # iterator closed before delete — cursor is not held
                    t.delete(key)
                    n += 1
                    # commit every batch to keep WAL bounded
                    if n % _COPY_BATCH == 0:
                        db.commit()
                        db.begin(write=True)
                db.commit()
            except Exception as e:
                try:
                    db.rollback()
                except Exception:
                    pass
                print(f"error: {e}", file=sys.stderr)
                return _ERR
        if args.format == "json":
            print(json.dumps({"deleted": n, "prefix": prefix}))
        else:
            print(f"{n} key(s) deleted")
        return _OK

    # Single-key delete
    with _target(db, args) as t:
        try:
            t.delete(args.key)
        except NotFoundError:
            print(f"not found: {args.key}", file=sys.stderr)
            return _NOTFOUND
    if args.format == "json":
        print(json.dumps({"deleted": True, "key": args.key}))
    return _OK


def cmd_exists(db: KVStore, args: argparse.Namespace) -> int:
    with _target(db, args) as t:
        found = t.exists(args.key)
    if args.format == "json":
        print(json.dumps({"exists": found, "key": args.key}))
    else:
        print("found" if found else "not found")
    return _OK if found else _NOTFOUND


def _iter_entries(it, seek_key: Optional[str] = None, reverse: bool = False):
    """Yield (key, value) from an open Iterator.

    When seek_key is given the iterator is positioned via seek() and then
    traversed manually.  We cannot use the Python for-loop after seek()
    because the raw C iterator's __iter__ advances past the current position,
    which would skip the seeked key.  For reverse iterators we step with
    it.prev() instead of it.next().
    """
    if seek_key:
        it.seek(seek_key.encode())
        advance = it.prev if reverse else it.next
        while not it.eof:
            yield it.key, it.value
            advance()
    else:
        yield from it


def cmd_list(db: KVStore, args: argparse.Namespace) -> int:
    if args.limit < 0:
        print("error: --limit must be >= 0 (0 means no limit)", file=sys.stderr)
        return _ERR
    prefix   = args.prefix.encode() if args.prefix else None
    seek_key = getattr(args, "seek", None)
    limit    = args.limit
    n = 0
    with _target(db, args) as t:
        with t.iterator(reverse=args.reverse, prefix=prefix) as it:
            for key, _val in _iter_entries(it, seek_key, args.reverse):
                _out_key(key, args.format)
                n += 1
                if limit and n >= limit:
                    break
    return _OK


def cmd_scan(db: KVStore, args: argparse.Namespace) -> int:
    if args.limit < 0:
        print("error: --limit must be >= 0 (0 means no limit)", file=sys.stderr)
        return _ERR
    prefix   = args.prefix.encode() if args.prefix else None
    seek_key = getattr(args, "seek", None)
    limit    = args.limit
    n = 0
    with _target(db, args) as t:
        with t.iterator(reverse=args.reverse, prefix=prefix) as it:
            for key, val in _iter_entries(it, seek_key, args.reverse):
                _out_kv(key, val, args.format)
                n += 1
                if limit and n >= limit:
                    break
    return _OK


def cmd_count(db: KVStore, args: argparse.Namespace) -> int:
    prefix = getattr(args, "prefix", None)
    if prefix:
        n = sum(1 for _ in _prefix_iter(db, args, prefix.encode()))
    else:
        with _target(db, args) as t:
            n = t.count()
    if args.format == "json":
        print(json.dumps({"count": n}))
    else:
        print(n)
    return _OK


def _prefix_iter(db: KVStore, args: argparse.Namespace, prefix: bytes):
    with _target(db, args) as t:
        with t.iterator(prefix=prefix) as it:
            yield from it


def cmd_clear(db: KVStore, args: argparse.Namespace) -> int:
    with _target(db, args) as t:
        t.clear()
    if args.format != "json":
        print("cleared")
    else:
        print(json.dumps({"status": "ok"}))
    return _OK


def cmd_set_if_absent(db: KVStore, args: argparse.Namespace) -> int:
    ttl = getattr(args, "ttl", None)
    if ttl is not None and ttl <= 0:
        print("error: --ttl must be a positive number of seconds", file=sys.stderr)
        return _ERR
    with _target(db, args) as t:
        inserted = t.put_if_absent(args.key, args.value, ttl=ttl)
    if args.format == "json":
        print(json.dumps({"inserted": inserted, "key": args.key}))
    else:
        print("inserted" if inserted else "already exists (not modified)")
    return _OK


# ── TTL commands ─────────────────────────────────────────────────────────────

def cmd_ttl(db: KVStore, args: argparse.Namespace) -> int:
    with _target(db, args) as t:
        try:
            remaining = t.ttl(args.key)
        except NotFoundError:
            print(f"not found: {args.key}", file=sys.stderr)
            return _NOTFOUND

    if remaining is None:
        label = "no ttl"
        secs  = None
    elif remaining <= 0:
        label = "expired"
        secs  = 0.0
    else:
        label = f"{remaining:.3f}s remaining"
        secs  = remaining

    if args.format == "json":
        print(json.dumps({"key": args.key, "ttl": secs, "status": label}))
    else:
        print(label)
    return _OK


def cmd_purge(db: KVStore, args: argparse.Namespace) -> int:
    with _target(db, args) as t:
        n = t.purge_expired()
    if args.format == "json":
        print(json.dumps({"purged": n}))
    else:
        print(f"{n} expired key(s) purged")
    return _OK


# ── Column family commands ────────────────────────────────────────────────────

def cmd_cf(db: KVStore, args: argparse.Namespace) -> int:
    sub = args.cf_cmd
    if sub == "list":
        names = db.list_column_families()
        if args.format == "json":
            print(json.dumps({"column_families": names}))
        else:
            if names:
                for name in names:
                    print(name)
            else:
                print("(no user column families)")
    elif sub == "create":
        if not args.name:
            print("error: cf create requires NAME", file=sys.stderr)
            return _ERR
        cf = db.create_column_family(args.name)
        cf.close()
        if args.format == "json":
            print(json.dumps({"created": args.name}))
        else:
            print(f"created: {args.name}")
    elif sub == "drop":
        if not args.name:
            print("error: cf drop requires NAME", file=sys.stderr)
            return _ERR
        db.drop_column_family(args.name)
        if args.format == "json":
            print(json.dumps({"dropped": args.name}))
        else:
            print(f"dropped: {args.name}")
    return _OK


# ── Maintenance commands ──────────────────────────────────────────────────────

def cmd_stats(db: KVStore, args: argparse.Namespace) -> int:
    s = db.stats()
    if args.reset:
        db.stats_reset()
    _out_record(s, args.format)
    return _OK


def cmd_checkpoint(db: KVStore, args: argparse.Namespace) -> int:
    mode = _CKPT_MAP.get(args.mode, CHECKPOINT_PASSIVE)
    nlog, nckpt = db.checkpoint(mode)
    if args.format == "json":
        print(json.dumps({"nlog": nlog, "nckpt": nckpt, "mode": args.mode}))
    else:
        print(f"mode={args.mode}  nlog={nlog}  nckpt={nckpt}")
    return _OK


def cmd_vacuum(db: KVStore, args: argparse.Namespace) -> int:
    db.vacuum(args.pages)
    if args.format == "json":
        print(json.dumps({"status": "ok", "pages": args.pages}))
    else:
        print(f"vacuum complete (pages={args.pages})")
    return _OK


def cmd_check(db: KVStore, args: argparse.Namespace) -> int:
    try:
        db.integrity_check()
    except CorruptError as e:
        if args.format == "json":
            print(json.dumps({"status": "corrupt", "detail": str(e)}))
        else:
            print(f"CORRUPT: {e}", file=sys.stderr)
        return _ERR
    if args.format == "json":
        print(json.dumps({"status": "ok"}))
    else:
        print("integrity check passed")
    return _OK


def cmd_info(db: KVStore, args: argparse.Namespace) -> int:
    cfs   = db.list_column_families()
    enc   = db.is_encrypted()
    stats = db.stats()
    size_bytes = 0
    if args.db and os.path.isfile(args.db):
        size_bytes = os.path.getsize(args.db)
    d = {
        "path":       args.db,
        "encrypted":  "yes" if enc else "no",
        "cf_count":   len(cfs),
        "cfs":        ", ".join(cfs) if cfs else "(none)",
        "db_pages":   stats.get("db_pages", 0),
        "size_bytes": size_bytes,
    }
    _out_record(d, args.format)
    return _OK


def cmd_sync(db: KVStore, args: argparse.Namespace) -> int:
    db.sync()
    if args.format == "json":
        print(json.dumps({"status": "ok"}))
    else:
        print("sync complete")
    return _OK


# ── Transaction (batch) command ───────────────────────────────────────────────

def cmd_txn(db: KVStore, args: argparse.Namespace) -> int:
    """Read put/del ops from stdin; execute all atomically in one transaction.

    Lines are parsed and executed as they are read — O(1) memory regardless
    of input size.  Any parse or write error rolls back the entire transaction.
    """
    dry_run    = getattr(args, "dry_run", False)
    cf_handle: Optional[ColumnFamily] = None
    n          = 0

    db.begin(write=True)
    try:
        if getattr(args, "cf", None):
            cf_handle = db.open_column_family(args.cf)
            target = cf_handle
        else:
            target = db

        for lineno, raw in enumerate(sys.stdin, 1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(None, 2)
            verb  = parts[0].lower()
            if verb == "put":
                if len(parts) < 3:
                    raise ValueError(
                        f"line {lineno}: 'put' requires KEY and VALUE"
                    )
                target.put(parts[1], parts[2])
            elif verb in ("del", "delete", "rm"):
                if len(parts) < 2:
                    raise ValueError(
                        f"line {lineno}: '{verb}' requires KEY"
                    )
                try:
                    target.delete(parts[1])
                except NotFoundError:
                    pass  # deleting a non-existent key is a no-op in batch mode
            else:
                raise ValueError(
                    f"line {lineno}: unknown op {verb!r} "
                    f"(expected 'put KEY VALUE' or 'del KEY')"
                )
            n += 1

        if cf_handle is not None:
            cf_handle.close()
            cf_handle = None

        if n == 0:
            db.rollback()
            if args.format == "json":
                print(json.dumps({"committed": 0, "ops": 0}))
            else:
                print("nothing to do (empty input)")
            return _OK

        if dry_run:
            db.rollback()
            if args.format == "json":
                print(json.dumps({"committed": 0, "dry_run": True, "ops": n}))
            else:
                print(f"dry-run: {n} op(s) validated, rolled back")
        else:
            db.commit()
            if args.format == "json":
                print(json.dumps({"committed": n}))
            else:
                print(f"{n} op(s) committed")

    except Exception as e:
        if cf_handle is not None:
            try:
                cf_handle.close()
            except Exception:
                pass
        try:
            db.rollback()
        except Exception:
            pass
        print(f"error: {e}", file=sys.stderr)
        return _ERR

    return _OK


# ── Crypto commands (manage their own DB lifecycle) ───────────────────────────

def _cleanup_temp(path: str) -> None:
    for ext in ("", "-wal", "-shm"):
        try:
            os.remove(path + ext)
        except FileNotFoundError:
            pass


_COPY_BATCH = 10_000  # commit every N rows to bound WAL size


def _copy_entries(db: KVStore, target, iterator) -> None:
    """Copy key/value pairs from iterator into target in batched transactions."""
    db.begin(write=True)
    try:
        n = 0
        for key, val in iterator:
            target.put(key, val)
            n += 1
            if n % _COPY_BATCH == 0:
                db.commit()
                db.begin(write=True)
        db.commit()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        raise


def _cmd_encrypt(args: argparse.Namespace) -> int:
    """Migrate a plaintext store to an encrypted store (in-place)."""
    if not args.new_password:
        print("error: --new-password is required for encrypt", file=sys.stderr)
        return _ERR

    # Open source as plaintext
    kw = _build_kwargs(args)
    kw.pop("read_only", None)  # must be writable for migration
    src: Optional[KVStore] = None
    enc: Optional[KVStore] = None
    tmp_path = args.db + ".snkvctl_enc_tmp"

    try:
        try:
            src = KVStore(args.db, **kw) if kw else KVStore(args.db)
        except SnkvError as e:
            print(f"error: cannot open store as plaintext: {e}", file=sys.stderr)
            print("hint: if the store is already encrypted, use 'rekey' to change the password",
                  file=sys.stderr)
            return _ERR

        if src.is_encrypted():
            src.close()
            print("error: store is already encrypted; use 'rekey' to change the password",
                  file=sys.stderr)
            return _ERR

        _cleanup_temp(tmp_path)
        enc = KVStore.open_encrypted(tmp_path, args.new_password)

        _copy_entries(enc, enc, src.iterator())

        # Copy user column families
        for cf_name in src.list_column_families():
            src_cf = src.open_column_family(cf_name)
            dst_cf = enc.create_column_family(cf_name)
            try:
                _copy_entries(enc, dst_cf, src_cf.iterator())
            finally:
                src_cf.close()
                dst_cf.close()

        enc.close(); enc = None
        src.close(); src = None

        # Atomic replace
        os.replace(tmp_path, args.db)
        # Remove stale WAL/SHM from the original plaintext store
        for ext in ("-wal", "-shm"):
            try: os.remove(args.db + ext)
            except FileNotFoundError: pass
        # Remove temp WAL/SHM (should be empty after clean close)
        for ext in ("-wal", "-shm"):
            try: os.remove(tmp_path + ext)
            except FileNotFoundError: pass

        if args.format == "json":
            print(json.dumps({"status": "ok", "encrypted": True, "ttl_preserved": False}))
        else:
            print("encrypted successfully")
            print("note: per-key TTL metadata is not preserved during migration")

    except Exception as e:
        if enc is not None:
            try: enc.close()
            except Exception: pass
        if src is not None:
            try: src.close()
            except Exception: pass
        _cleanup_temp(tmp_path)
        print(f"error: {e}", file=sys.stderr)
        return _ERR

    return _OK


def _cmd_decrypt(args: argparse.Namespace) -> int:
    """Remove encryption from a store (encrypted → plaintext)."""
    if not args.password:
        print("error: --password is required for decrypt", file=sys.stderr)
        return _ERR
    try:
        db = KVStore.open_encrypted(args.db, args.password)
        try:
            db.remove_encryption()
        finally:
            db.close()
    except AuthError:
        print("error: wrong password", file=sys.stderr)
        return _ERR
    if args.format == "json":
        print(json.dumps({"status": "ok", "encrypted": False}))
    else:
        print("decrypted successfully")
    return _OK


def _cmd_rekey(args: argparse.Namespace) -> int:
    """Change the encryption password of an encrypted store."""
    if not args.password:
        print("error: --password is required for rekey", file=sys.stderr)
        return _ERR
    if not args.new_password:
        print("error: --new-password is required for rekey", file=sys.stderr)
        return _ERR
    try:
        db = KVStore.open_encrypted(args.db, args.password)
        try:
            db.reencrypt(args.new_password)
        finally:
            db.close()
    except AuthError:
        print("error: wrong password", file=sys.stderr)
        return _ERR
    if args.format == "json":
        print(json.dumps({"status": "ok"}))
    else:
        print("rekeyed successfully")
    return _OK


# ── Argument parser ───────────────────────────────────────────────────────────

class _Parser(argparse.ArgumentParser):
    """ArgumentParser that exits with code 1 (error) on bad arguments.

    argparse's default is to exit 2 for usage errors, which would collide with
    our exit code 2 meaning "key not found". We remap to 1 so the three-code
    contract (0=ok, 1=error, 2=not-found) is unambiguous in shell scripts.
    """
    def error(self, message: str) -> None:
        self.print_usage(sys.stderr)
        self.exit(1, f"{self.prog}: error: {message}\n")


def build_parser() -> argparse.ArgumentParser:
    # Shared parent so --format can appear after the subcommand name.
    # SUPPRESS default means: don't overwrite the global default when omitted.
    _fmt = _Parser(add_help=False)
    _fmt.add_argument("--format", choices=["text", "json"],
                      default=argparse.SUPPRESS,
                      help="output format: text (default) or json")

    p = _Parser(
        prog="snkvctl",
        description="Command-line interface for SNKV embedded key-value stores.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  snkvctl --db mydb.db put hello world
  snkvctl --db mydb.db get hello
  snkvctl --db mydb.db scan --prefix user: --limit 20 --format json
  snkvctl --db mydb.db --cf sessions put tok123 active --ttl 300
  snkvctl --db mydb.db stats --format json
  printf 'put a 1\\ndel b\\n' | snkvctl --db mydb.db txn
  snkvctl --db mydb.db encrypt --new-password s3cr3t
""",
    )

    # ── global flags ──────────────────────────────────────────────────────────
    p.add_argument("--db",       required=True, metavar="PATH",
                   help="database file path")
    p.add_argument("--cf",       metavar="NAME", default=None,
                   help="operate on this column family (default: default CF)")
    p.add_argument("--password", metavar="PASS", default=None,
                   help="password for encrypted stores")
    p.add_argument("--timeout",  metavar="MS",   type=int, default=3000,
                   help="busy-retry timeout in ms (default: 3000)")
    p.add_argument("--format",   choices=["text", "json"], default="text",
                   help="output format (default: text); also accepted after COMMAND")

    # ── KVStoreConfig flags ───────────────────────────────────────────────────
    p.add_argument("--journal",    choices=["wal", "delete"], default="wal",
                   help="journal mode: wal (default) or delete")
    p.add_argument("--sync",       choices=["off", "normal", "full"], default="normal",
                   help="fsync level: off / normal (default) / full")
    p.add_argument("--cache-size", metavar="N", type=int, dest="cache_size", default=None,
                   help="page cache size in pages (~4 KB each, default 2000 ~= 8 MB)")
    p.add_argument("--page-size",  metavar="N", type=int, dest="page_size", default=None,
                   help="database page size in bytes; new databases only (default 4096)")
    p.add_argument("--read-only",  action="store_true", dest="read_only",
                   help="open in read-only mode")
    p.add_argument("--wal-limit",  metavar="N", type=int, dest="wal_limit", default=None,
                   help="auto-checkpoint every N committed transactions (0 = off)")
    p.add_argument("--full-mutex", action="store_true", dest="full_mutex",
                   help="serialize all ops with a recursive mutex (shared-handle threading)")

    sub = p.add_subparsers(dest="cmd", required=True, metavar="COMMAND")

    # put
    sp = sub.add_parser("put", parents=[_fmt], help="insert or update a key")
    sp.add_argument("key")
    sp.add_argument("value")
    sp.add_argument("--ttl", metavar="SECS", type=float, default=None,
                    help="seconds until the key expires")

    # get
    sp = sub.add_parser("get", parents=[_fmt], help="fetch a key's value")
    sp.add_argument("key")

    # del
    sp = sub.add_parser("del", parents=[_fmt],
                        help="delete a key, or all keys matching --prefix")
    grp = sp.add_mutually_exclusive_group(required=True)
    grp.add_argument("key", nargs="?", default=None,
                     help="single key to delete")
    grp.add_argument("--prefix", metavar="P", default=None,
                     help="delete all keys that start with this prefix")

    # exists
    sp = sub.add_parser("exists", parents=[_fmt],
                        help="check whether a key exists (exit 0/2)")
    sp.add_argument("key")

    # list
    sp = sub.add_parser("list", parents=[_fmt], help="print keys (no values)")
    sp.add_argument("--prefix",  metavar="P", default=None)
    sp.add_argument("--seek",    metavar="KEY", default=None,
                    help="start iteration at first key >= KEY")
    sp.add_argument("--reverse", action="store_true")
    sp.add_argument("--limit",   metavar="N", type=int, default=0)

    # scan
    sp = sub.add_parser("scan", parents=[_fmt], help="print key+value pairs")
    sp.add_argument("--prefix",  metavar="P", default=None)
    sp.add_argument("--seek",    metavar="KEY", default=None,
                    help="start iteration at first key >= KEY")
    sp.add_argument("--reverse", action="store_true")
    sp.add_argument("--limit",   metavar="N", type=int, default=0)

    # count
    sp = sub.add_parser("count", parents=[_fmt], help="count entries")
    sp.add_argument("--prefix", metavar="P", default=None)

    # clear
    sub.add_parser("clear", parents=[_fmt],
                   help="delete all keys in the store or column family")

    # set-if-absent
    sp = sub.add_parser("set-if-absent", parents=[_fmt],
                        help="insert a key only if it does not already exist")
    sp.add_argument("key")
    sp.add_argument("value")
    sp.add_argument("--ttl", metavar="SECS", type=float, default=None)

    # ttl
    sp = sub.add_parser("ttl", parents=[_fmt], help="show remaining TTL for a key")
    sp.add_argument("key")

    # purge
    sub.add_parser("purge", parents=[_fmt],
                   help="delete all expired keys; print count removed")

    # txn
    sp = sub.add_parser(
        "txn",
        parents=[_fmt],
        help="execute a batch of put/del ops atomically (reads from stdin)",
        description=(
            "Read 'put KEY VALUE' and 'del KEY' lines from stdin.\n"
            "All ops are executed inside a single begin→commit transaction.\n"
            "Any error rolls back the entire batch.\n\n"
            "example:\n"
            "  printf 'put a 1\\ndel b\\n' | snkvctl --db x.db txn"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sp.add_argument("--dry-run", action="store_true", dest="dry_run",
                    help="validate ops but roll back instead of committing")
    sp.add_argument("--cf", metavar="NAME", default=argparse.SUPPRESS,
                    help="column family to apply ops to (overrides global --cf)")

    # sync
    sub.add_parser("sync", parents=[_fmt],
                   help="flush all pending writes to disk (fsync)")

    # cf
    sp = sub.add_parser("cf", parents=[_fmt], help="column family management")
    sp.add_argument("cf_cmd", choices=["list", "create", "drop"],
                    metavar="{list,create,drop}")
    sp.add_argument("name", nargs="?", metavar="NAME",
                    help="column family name (required for create/drop)")

    # stats
    sp = sub.add_parser("stats", parents=[_fmt], help="show operation statistics")
    sp.add_argument("--reset", action="store_true",
                    help="reset cumulative counters after printing")

    # checkpoint
    sp = sub.add_parser("checkpoint", parents=[_fmt], help="run a WAL checkpoint")
    sp.add_argument("--mode",
                    choices=["passive", "full", "restart", "truncate"],
                    default="passive")

    # vacuum
    sp = sub.add_parser("vacuum", parents=[_fmt], help="reclaim unused pages")
    sp.add_argument("--pages", type=int, default=0, metavar="N",
                    help="max pages to reclaim (0 = all, default)")

    # check
    sub.add_parser("check", parents=[_fmt],
                   help="run an integrity check; exit 1 on corruption")

    # info
    sub.add_parser("info", parents=[_fmt],
                   help="print path, encryption status, CF list, page count, size")

    # encrypt
    sp = sub.add_parser("encrypt", parents=[_fmt],
                        help="migrate a plaintext store to encrypted (in-place copy)")
    sp.add_argument("--new-password", dest="new_password", required=True,
                    metavar="PASS")

    # decrypt
    sub.add_parser("decrypt", parents=[_fmt],
                   help="remove encryption from a store (requires --password)")

    # rekey
    sp = sub.add_parser("rekey", parents=[_fmt],
                        help="change the encryption password")
    sp.add_argument("--new-password", dest="new_password", required=True,
                    metavar="PASS")

    return p


# ── dispatch ──────────────────────────────────────────────────────────────────

_DISPATCH = {
    "put":           cmd_put,
    "get":           cmd_get,
    "del":           cmd_del,
    "exists":        cmd_exists,
    "list":          cmd_list,
    "scan":          cmd_scan,
    "count":         cmd_count,
    "clear":         cmd_clear,
    "set-if-absent": cmd_set_if_absent,
    "ttl":           cmd_ttl,
    "purge":         cmd_purge,
    "cf":            cmd_cf,
    "stats":         cmd_stats,
    "checkpoint":    cmd_checkpoint,
    "vacuum":        cmd_vacuum,
    "check":         cmd_check,
    "info":          cmd_info,
    "sync":          cmd_sync,
    "txn":           cmd_txn,
}

# Commands that manage their own DB lifecycle
_CRYPTO_CMDS = {"encrypt", "decrypt", "rekey"}


def main() -> None:
    parser = build_parser()
    args   = parser.parse_args()

    # Crypto commands open and close the DB themselves
    if args.cmd in _CRYPTO_CMDS:
        if args.cmd == "encrypt":
            rc = _cmd_encrypt(args)
        elif args.cmd == "decrypt":
            rc = _cmd_decrypt(args)
        else:
            rc = _cmd_rekey(args)
        sys.exit(rc)

    # All other commands share a single open/close lifecycle
    try:
        db = _open_db(args)
    except AuthError:
        print("error: wrong password or store is not encrypted", file=sys.stderr)
        sys.exit(_ERR)
    except SnkvError as e:
        print(f"error opening {args.db!r}: {e}", file=sys.stderr)
        sys.exit(_ERR)
    except OSError as e:
        print(f"error opening {args.db!r}: {e}", file=sys.stderr)
        sys.exit(_ERR)

    rc = _ERR
    try:
        handler = _DISPATCH.get(args.cmd)
        if handler is None:
            print(f"error: unknown command {args.cmd!r}", file=sys.stderr)
            rc = _ERR
        else:
            rc = handler(db, args)
    except NotFoundError as e:
        print(f"not found: {e}", file=sys.stderr)
        rc = _NOTFOUND
    except ReadOnlyError:
        print("error: store is read-only", file=sys.stderr)
        rc = _ERR
    except BusyError:
        print("error: database is busy; increase --timeout and retry", file=sys.stderr)
        rc = _ERR
    except LockedError:
        print("error: database is locked", file=sys.stderr)
        rc = _ERR
    except AuthError:
        print("error: wrong password or store is not encrypted", file=sys.stderr)
        rc = _ERR
    except CorruptError as e:
        print(f"error: database corruption detected: {e}", file=sys.stderr)
        rc = _ERR
    except SnkvError as e:
        print(f"error: {e}", file=sys.stderr)
        rc = _ERR
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        rc = _ERR
    finally:
        try:
            db.close()
        except Exception:
            pass

    sys.exit(rc)


if __name__ == "__main__":
    main()
