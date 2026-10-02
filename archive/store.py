"""Offline archive store with recoverable batch destruction.

Layout on disk:
    <root>/archive.sqlite3   metadata (records, destroy batches, receipts)
    <root>/blobs/<sha256>    content-addressed blob files

File bytes are never stored in SQLite; all reads and checksum
verification stream from the blob files.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Callable, Iterable, Union

from .errors import (
    BlobCorrupt,
    BlobMissing,
    DuplicateRecord,
    IdempotencyConflict,
    RecordNotFound,
    RetentionNotMet,
)
from .faults import crash_point

Clock = Callable[[], datetime]

_CHUNK = 1024 * 1024

_SCHEMA = """
CREATE TABLE IF NOT EXISTS records (
    record_id    TEXT PRIMARY KEY,
    sha256       TEXT NOT NULL,
    retain_until TEXT NOT NULL,
    state        TEXT NOT NULL CHECK (state IN ('alive', 'destroyed')),
    batch_id     INTEGER REFERENCES batches(id)
);
CREATE TABLE IF NOT EXISTS batches (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key TEXT NOT NULL UNIQUE,
    record_ids      TEXT NOT NULL,
    state           TEXT NOT NULL CHECK (state IN ('accepted', 'completed')),
    receipt         TEXT
);
"""


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class Receipt:
    """Stable, permanent proof of a completed destroy batch."""

    batch_id: int
    idempotency_key: str
    records: tuple  # of (record_id, sha256), sorted by record_id
    reclaimed_blobs: tuple  # sha256 of blobs physically removed
    retained_blobs: tuple  # sha256 of blobs kept due to outside sharing
    completed_at: str


class ArchiveStore:
    """Single-process, serial-access archive store."""

    def __init__(self, root: Union[str, Path], clock: Clock = _utcnow) -> None:
        self._root = Path(root)
        self._clock = clock
        self._blob_dir = self._root / "blobs"
        self._blob_dir.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(self._root / "archive.sqlite3"))
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA foreign_keys=ON")
        self._db.executescript(_SCHEMA)
        self._db.commit()
        # Recovery must finish before any other operation is accepted.
        self.recover()

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> "ArchiveStore":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def ingest(
        self, record_id: str, stream: BinaryIO, retain_until: datetime
    ) -> str:
        """Store a new record; returns the blob sha256."""
        row = self._db.execute(
            "SELECT state FROM records WHERE record_id = ?", (record_id,)
        ).fetchone()
        if row is not None:
            raise DuplicateRecord(record_id)

        sha, tmp_path = self._spool_blob(stream)
        final_path = self._blob_dir / sha
        if not final_path.exists():
            os.replace(tmp_path, final_path)
        else:
            os.unlink(tmp_path)

        self._db.execute(
            "INSERT INTO records (record_id, sha256, retain_until, state)"
            " VALUES (?, ?, ?, 'alive')",
            (record_id, sha, self._fmt_time(retain_until)),
        )
        self._db.commit()
        return sha

    def _spool_blob(self, stream: BinaryIO) -> tuple:
        """Stream bytes to a temp file while hashing; returns (sha, path)."""
        hasher = hashlib.sha256()
        fd, tmp_name = tempfile.mkstemp(dir=self._blob_dir, prefix=".tmp-")
        try:
            with os.fdopen(fd, "wb") as out:
                while True:
                    chunk = stream.read(_CHUNK)
                    if not chunk:
                        break
                    hasher.update(chunk)
                    out.write(chunk)
        except BaseException:
            os.unlink(tmp_name)
            raise
        return hasher.hexdigest(), Path(tmp_name)

    @staticmethod
    def _fmt_time(value: datetime) -> str:
        if value.tzinfo is None:
            raise ValueError("retain_until must be timezone-aware (UTC)")
        return value.astimezone(timezone.utc).isoformat()

    def destroy(
        self, record_ids: Iterable[str], idempotency_key: str
    ) -> Receipt:
        """Accept and execute a destroy batch; returns its receipt."""
        targets = sorted(set(record_ids))
        if not targets:
            raise ValueError("record_ids must be non-empty")

        row = self._db.execute(
            "SELECT id, record_ids, state, receipt FROM batches"
            " WHERE idempotency_key = ?",
            (idempotency_key,),
        ).fetchone()
        if row is not None:
            # The key is permanently bound to its (unordered) record set.
            if set(json.loads(row[1])) != set(targets):
                raise IdempotencyConflict(idempotency_key)
            if row[2] == "completed":
                return self._receipt_from_json(row[3])
            # Accepted but interrupted: finish it, converging on the
            # same single receipt.
            return self._execute_batch(row[0])

        self._validate_targets(targets)

        # Acceptance: the destroy decision becomes irrevocable here.
        crash_point("destroy.before_accept_commit")
        cur = self._db.execute(
            "INSERT INTO batches (idempotency_key, record_ids, state)"
            " VALUES (?, ?, 'accepted')",
            (idempotency_key, json.dumps(targets)),
        )
        batch_id = cur.lastrowid
        self._db.execute(
            "UPDATE records SET state = 'destroyed', batch_id = ?"
            " WHERE record_id IN (%s)" % ",".join("?" * len(targets)),
            [batch_id, *targets],
        )
        self._db.commit()
        crash_point("destroy.after_accept_commit")

        return self._execute_batch(batch_id)

    def _validate_targets(self, targets: list) -> None:
        """Reject unless every target is live, expired and intact."""
        now = self._fmt_time(self._clock())
        rows = self._db.execute(
            "SELECT record_id, sha256, retain_until, state FROM records"
            " WHERE record_id IN (%s)" % ",".join("?" * len(targets)),
            targets,
        ).fetchall()
        found = {r[0]: r for r in rows}
        for record_id in targets:
            if record_id not in found:
                raise RecordNotFound(record_id)
        checked = set()
        for record_id in targets:
            _, sha, retain_until, state = found[record_id]
            if state != "alive":
                raise RecordNotFound(record_id)
            if retain_until > now:
                raise RetentionNotMet(record_id)
            if sha in checked:
                continue
            checked.add(sha)
            self._verify_blob(sha)

    def _verify_blob(self, sha: str) -> None:
        path = self._blob_dir / sha
        if not path.exists():
            raise BlobMissing(sha)
        hasher = hashlib.sha256()
        with open(path, "rb") as handle:
            while True:
                chunk = handle.read(_CHUNK)
                if not chunk:
                    break
                hasher.update(chunk)
        if hasher.hexdigest() != sha:
            raise BlobCorrupt(sha)

    def _execute_batch(self, batch_id: int) -> Receipt:
        """Reclaim blobs and finalize the receipt.  Idempotent: safe to
        re-run after a crash at any point."""
        rows = self._db.execute(
            "SELECT record_id, sha256 FROM records WHERE batch_id = ?"
            " ORDER BY record_id",
            (batch_id,),
        ).fetchall()
        shas = sorted({r[1] for r in rows})

        reclaimed, retained = [], []
        for sha in shas:
            live_refs = self._db.execute(
                "SELECT COUNT(*) FROM records"
                " WHERE sha256 = ? AND state = 'alive'",
                (sha,),
            ).fetchone()[0]
            if live_refs > 0:
                retained.append(sha)
                continue
            crash_point("destroy.before_blob_delete")
            path = self._blob_dir / sha
            if path.exists():
                # Missing file is only meaningful because the accepted
                # batch row in SQLite proves this blob was already
                # handed to the destroyer.
                os.unlink(path)
            crash_point("destroy.after_blob_delete")
            reclaimed.append(sha)

        receipt = Receipt(
            batch_id=batch_id,
            idempotency_key=self._db.execute(
                "SELECT idempotency_key FROM batches WHERE id = ?",
                (batch_id,),
            ).fetchone()[0],
            records=tuple((r[0], r[1]) for r in rows),
            reclaimed_blobs=tuple(reclaimed),
            retained_blobs=tuple(retained),
            completed_at=self._fmt_time(self._clock()),
        )
        crash_point("destroy.before_complete_commit")
        self._db.execute(
            "UPDATE batches SET state = 'completed', receipt = ?"
            " WHERE id = ?",
            (self._receipt_to_json(receipt), batch_id),
        )
        self._db.commit()
        crash_point("destroy.after_complete_commit")
        return receipt

    @staticmethod
    def _receipt_to_json(receipt: Receipt) -> str:
        return json.dumps(
            {
                "batch_id": receipt.batch_id,
                "idempotency_key": receipt.idempotency_key,
                "records": [list(r) for r in receipt.records],
                "reclaimed_blobs": list(receipt.reclaimed_blobs),
                "retained_blobs": list(receipt.retained_blobs),
                "completed_at": receipt.completed_at,
            },
            sort_keys=True,
        )

    @staticmethod
    def _receipt_from_json(payload: str) -> Receipt:
        data = json.loads(payload)
        return Receipt(
            batch_id=data["batch_id"],
            idempotency_key=data["idempotency_key"],
            records=tuple(tuple(r) for r in data["records"]),
            reclaimed_blobs=tuple(data["reclaimed_blobs"]),
            retained_blobs=tuple(data["retained_blobs"]),
            completed_at=data["completed_at"],
        )

    def read(self, record_id: str) -> Union[bytes, Receipt]:
        """Return original bytes for live records, Receipt for destroyed."""
        row = self._db.execute(
            "SELECT sha256, state, batch_id FROM records WHERE record_id = ?",
            (record_id,),
        ).fetchone()
        if row is None:
            raise RecordNotFound(record_id)
        sha, state, batch_id = row
        if state == "destroyed":
            receipt_row = self._db.execute(
                "SELECT receipt FROM batches WHERE id = ?", (batch_id,)
            ).fetchone()
            return self._receipt_from_json(receipt_row[0])
        path = self._blob_dir / sha
        if not path.exists():
            # Unexpected loss: never confused with a destroy receipt.
            raise BlobMissing(sha)
        chunks = []
        with open(path, "rb") as handle:
            while True:
                chunk = handle.read(_CHUNK)
                if not chunk:
                    break
                chunks.append(chunk)
        return b"".join(chunks)

    def recover(self) -> None:
        """Finish any interrupted destroy batches."""
        rows = self._db.execute(
            "SELECT id FROM batches WHERE state = 'accepted' ORDER BY id"
        ).fetchall()
        for (batch_id,) in rows:
            self._execute_batch(batch_id)
