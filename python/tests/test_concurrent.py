"""
Concurrent access tests — mirrors tests/test_concurrent.c.

Each writer opens its own WAL-mode connection (matching real multi-process
usage). Readers also use independent connections. WAL mode allows concurrent
reads alongside a single active writer, so readers always make progress.
"""

import threading
import pytest
from snkv import KVStore, JOURNAL_WAL


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NUM_WRITERS     = 5
KEYS_PER_WRITER = 500      # keys per writer
READER_ROUNDS   = 2        # scans per reader
NUM_READERS     = 5
BATCH           = 50       # records per transaction


def _expected_value(writer_id: int, key_idx: int) -> bytes:
    return f"writer{writer_id}_key{key_idx:06d}".encode()


# ---------------------------------------------------------------------------
# Thread workers (one connection per thread)
# ---------------------------------------------------------------------------

def _writer(db_path: str, writer_id: int, errors: list, lock: threading.Lock) -> None:
    """Write KEYS_PER_WRITER keys in auto-committed batches."""
    try:
        with KVStore(db_path, journal_mode=JOURNAL_WAL, busy_timeout=15_000) as db:
            for batch_start in range(0, KEYS_PER_WRITER, BATCH):
                db.begin(write=True)
                for i in range(batch_start, min(batch_start + BATCH, KEYS_PER_WRITER)):
                    db[f"w{writer_id:02d}_{i:06d}".encode()] = (
                        _expected_value(writer_id, i)
                    )
                db.commit()
    except Exception as exc:
        with lock:
            errors.append(f"writer {writer_id}: {exc}")


def _reader(db_path: str, reader_id: int, errors: list, lock: threading.Lock) -> None:
    """Scan all possible keys READER_ROUNDS times; flag any value corruption."""
    try:
        with KVStore(db_path, journal_mode=JOURNAL_WAL, busy_timeout=15_000) as db:
            for _ in range(READER_ROUNDS):
                for wid in range(NUM_WRITERS):
                    for kid in range(KEYS_PER_WRITER):
                        key = f"w{wid:02d}_{kid:06d}".encode()
                        val = db.get(key)
                        if val is not None and val != _expected_value(wid, kid):
                            with lock:
                                errors.append(
                                    f"reader {reader_id}: corrupt key={key!r}"
                                )
                            return
    except Exception as exc:
        with lock:
            errors.append(f"reader {reader_id}: {exc}")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_concurrent_write_read(tmp_path):
    """Writers + concurrent readers: no corruption, all keys land."""
    path = str(tmp_path / "conc.db")
    # pre-create the database so readers can open it immediately
    with KVStore(path, journal_mode=JOURNAL_WAL):
        pass

    errors: list = []
    lock = threading.Lock()

    threads = (
        [threading.Thread(target=_writer, args=(path, wid, errors, lock))
         for wid in range(NUM_WRITERS)]
        + [threading.Thread(target=_reader, args=(path, rid, errors, lock))
           for rid in range(NUM_READERS)]
    )
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, "Thread errors:\n" + "\n".join(errors)

    with KVStore(path, journal_mode=JOURNAL_WAL) as db:
        for wid in range(NUM_WRITERS):
            for kid in range(KEYS_PER_WRITER):
                key = f"w{wid:02d}_{kid:06d}".encode()
                assert db[key] == _expected_value(wid, kid)
        db.integrity_check()


def test_concurrent_writes_only(tmp_path):
    """Multiple writer connections: all writes land without data loss."""
    path = str(tmp_path / "writers_only.db")
    with KVStore(path, journal_mode=JOURNAL_WAL):
        pass

    errors: list = []
    lock = threading.Lock()

    threads = [
        threading.Thread(target=_writer, args=(path, wid, errors, lock))
        for wid in range(NUM_WRITERS)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, "\n".join(errors)

    with KVStore(path, journal_mode=JOURNAL_WAL) as db:
        total = sum(
            1 for wid in range(NUM_WRITERS)
            for kid in range(KEYS_PER_WRITER)
            if db.exists(f"w{wid:02d}_{kid:06d}".encode())
        )
        assert total == NUM_WRITERS * KEYS_PER_WRITER


def test_concurrent_integrity_after_writes(tmp_path):
    """integrity_check must pass after concurrent writes."""
    path = str(tmp_path / "integrity.db")
    with KVStore(path, journal_mode=JOURNAL_WAL):
        pass

    errors: list = []
    lock = threading.Lock()

    threads = [
        threading.Thread(target=_writer, args=(path, wid, errors, lock))
        for wid in range(4)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, "\n".join(errors)

    with KVStore(path, journal_mode=JOURNAL_WAL) as db:
        db.integrity_check()
