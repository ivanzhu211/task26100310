"""Resumable batch destruction for an offline archive.

Storage model:
  - SQLite holds only metadata (records, destroy batches, blob deletion
    intents). Blob bytes live as content-addressed files on disk.
  - A destroy batch, once accepted, is irrevocable: recovery must finish it,
    never roll it back.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
import time
from dataclasses import dataclass
from typing import Callable, Optional, Sequence


class ArchiveError(Exception):
    """Base class for archive errors."""


class RecordNotFound(ArchiveError):
    """The record_id is unknown to the archive."""


class RecordDestroyed(ArchiveError):
    """The record has been permanently destroyed."""


class DestroyRejected(ArchiveError):
    """The destroy set failed validation; nothing was changed."""


class IdempotencyConflict(ArchiveError):
    """The idempotency key is bound to a different record set."""


@dataclass(frozen=True)
class Receipt:
    """Stable destruction receipt for one accepted batch."""

    idempotency_key: str
    records: tuple  # tuple of (record_id, sha256) sorted by record_id
    reclaimed_blobs: tuple  # sha256 of blobs physically removed
    retained_blobs: tuple  # sha256 of blobs kept due to outside sharing

    def to_json(self) -> str:
        return json.dumps(
            {
                "idempotency_key": self.idempotency_key,
                "records": [list(r) for r in self.records],
                "reclaimed_blobs": list(self.reclaimed_blobs),
                "retained_blobs": list(self.retained_blobs),
            },
            sort_keys=True,
        )

    @staticmethod
    def from_json(payload: str) -> "Receipt":
        data = json.loads(payload)
        return Receipt(
            idempotency_key=data["idempotency_key"],
            records=tuple(tuple(r) for r in data["records"]),
            reclaimed_blobs=tuple(data["reclaimed_blobs"]),
            retained_blobs=tuple(data["retained_blobs"]),
        )


class Archive:
    def __init__(
        self,
        db_path: str,
        blob_dir: str,
        clock: Callable[[], float] = time.time,
        fault_hook: Optional[Callable[[str], None]] = None,
    ) -> None:
        self._db_path = db_path
        self._blob_dir = blob_dir
        self._clock = clock
        self._fault_hook = fault_hook or (lambda point: None)
        os.makedirs(blob_dir, exist_ok=True)
        self._db = sqlite3.connect(db_path)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA foreign_keys=ON")
        self._init_schema()
        # Recovery must complete before any other operation is accepted.
        self.recover()

    def _init_schema(self) -> None:
        self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS records (
                record_id    TEXT PRIMARY KEY,
                sha256       TEXT NOT NULL,
                retain_until REAL NOT NULL,
                batch_id     INTEGER REFERENCES batches(id)
            );
            CREATE TABLE IF NOT EXISTS batches (
                id              INTEGER PRIMARY KEY,
                idempotency_key TEXT NOT NULL UNIQUE,
                record_ids      TEXT NOT NULL,  -- canonical sorted JSON array
                receipt         TEXT NOT NULL,  -- stable receipt JSON, fixed at accept
                done            INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS blob_deletions (
                batch_id INTEGER NOT NULL REFERENCES batches(id),
                sha256   TEXT NOT NULL,
                deleted  INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (batch_id, sha256)
            );
            """
        )
        self._db.commit()

    def _hook(self, point: str) -> None:
        self._fault_hook(point)

    def _blob_path(self, sha256: str) -> str:
        return os.path.join(self._blob_dir, sha256)

    def close(self) -> None:
        self._db.close()

    def ingest(self, record_id: str, stream, retain_until: float) -> str:
        """Store a new record; returns the SHA-256 hex digest of its content."""
        row = self._db.execute(
            "SELECT 1 FROM records WHERE record_id = ?", (record_id,)
        ).fetchone()
        if row is not None:
            raise ArchiveError(f"record_id already used: {record_id!r}")

        hasher = hashlib.sha256()
        fd, tmp_path = tempfile.mkstemp(dir=self._blob_dir, prefix=".ingest-")
        try:
            with os.fdopen(fd, "wb") as tmp:
                while True:
                    chunk = stream.read(1 << 20)
                    if not chunk:
                        break
                    hasher.update(chunk)
                    tmp.write(chunk)
            digest = hasher.hexdigest()
            final_path = self._blob_path(digest)
            if os.path.exists(final_path):
                os.unlink(tmp_path)  # identical content already stored
            else:
                os.rename(tmp_path, final_path)
        except BaseException:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

        self._db.execute(
            "INSERT INTO records (record_id, sha256, retain_until, batch_id)"
            " VALUES (?, ?, ?, NULL)",
            (record_id, digest, float(retain_until)),
        )
        self._db.commit()
        return digest

    def destroy(self, record_ids: Sequence[str], idempotency_key: str) -> Receipt:
        canonical = sorted(set(record_ids))
        if not canonical:
            raise DestroyRejected("empty destroy set")

        existing = self._db.execute(
            "SELECT id, record_ids, receipt, done FROM batches"
            " WHERE idempotency_key = ?",
            (idempotency_key,),
        ).fetchone()
        if existing is not None:
            batch_id, stored_ids, receipt_json, done = existing
            if json.loads(stored_ids) != canonical:
                raise IdempotencyConflict(
                    f"idempotency key {idempotency_key!r} is bound to a"
                    " different record set"
                )
            if not done:
                self._execute_batch(batch_id)
            return Receipt.from_json(receipt_json)

        now = self._clock()
        targets = []
        for record_id in canonical:
            row = self._db.execute(
                "SELECT sha256, retain_until, batch_id FROM records"
                " WHERE record_id = ?",
                (record_id,),
            ).fetchone()
            if row is None:
                raise DestroyRejected(f"unknown record: {record_id!r}")
            sha256, retain_until, batch_id = row
            if batch_id is not None:
                raise DestroyRejected(f"record already destroyed: {record_id!r}")
            if retain_until > now:
                raise DestroyRejected(f"record not yet expired: {record_id!r}")
            targets.append((record_id, sha256))

        for record_id, sha256 in targets:
            self._verify_blob(record_id, sha256)

        batch_shas = sorted({sha for _, sha in targets})
        reclaimed, retained = [], []
        for sha256 in batch_shas:
            placeholders = ",".join("?" for _ in canonical)
            outside = self._db.execute(
                f"SELECT COUNT(*) FROM records WHERE sha256 = ?"
                f" AND batch_id IS NULL AND record_id NOT IN ({placeholders})",
                (sha256, *canonical),
            ).fetchone()[0]
            (reclaimed if outside == 0 else retained).append(sha256)

        receipt = Receipt(
            idempotency_key=idempotency_key,
            records=tuple(targets),
            reclaimed_blobs=tuple(reclaimed),
            retained_blobs=tuple(retained),
        )

        # Accept: the destroy decision becomes irrevocable in one commit.
        self._hook("destroy.before_accept_commit")
        cursor = self._db.execute(
            "INSERT INTO batches (idempotency_key, record_ids, receipt, done)"
            " VALUES (?, ?, ?, 0)",
            (idempotency_key, json.dumps(canonical), receipt.to_json()),
        )
        batch_id = cursor.lastrowid
        self._db.executemany(
            "UPDATE records SET batch_id = ? WHERE record_id = ?",
            [(batch_id, rid) for rid in canonical],
        )
        self._db.executemany(
            "INSERT INTO blob_deletions (batch_id, sha256, deleted)"
            " VALUES (?, ?, 0)",
            [(batch_id, sha) for sha in reclaimed],
        )
        self._db.commit()
        self._hook("destroy.after_accept_commit")

        self._execute_batch(batch_id)
        return receipt

    def _verify_blob(self, record_id: str, sha256: str) -> None:
        path = self._blob_path(sha256)
        if not os.path.exists(path):
            raise DestroyRejected(f"blob missing for record {record_id!r}")
        hasher = hashlib.sha256()
        with open(path, "rb") as fh:
            while True:
                chunk = fh.read(1 << 20)
                if not chunk:
                    break
                hasher.update(chunk)
        if hasher.hexdigest() != sha256:
            raise DestroyRejected(f"checksum mismatch for record {record_id!r}")

    def read(self, record_id: str):
        """Returns raw bytes for a live record, or a Receipt for a destroyed one."""
        row = self._db.execute(
            "SELECT sha256, batch_id FROM records WHERE record_id = ?",
            (record_id,),
        ).fetchone()
        if row is None:
            raise RecordNotFound(record_id)
        sha256, batch_id = row
        if batch_id is not None:
            receipt_json = self._db.execute(
                "SELECT receipt FROM batches WHERE id = ?", (batch_id,)
            ).fetchone()[0]
            return Receipt.from_json(receipt_json)
        hasher = hashlib.sha256()
        chunks = []
        try:
            with open(self._blob_path(sha256), "rb") as fh:
                while True:
                    chunk = fh.read(1 << 20)
                    if not chunk:
                        break
                    hasher.update(chunk)
                    chunks.append(chunk)
        except OSError as exc:
            raise ArchiveError(
                f"blob unexpectedly missing for live record {record_id!r}"
            ) from exc
        if hasher.hexdigest() != sha256:
            raise ArchiveError(f"blob corrupted for live record {record_id!r}")
        return b"".join(chunks)

    def recover(self) -> None:
        """Finish any interrupted destroy batches."""
        pending = self._db.execute(
            "SELECT id FROM batches WHERE done = 0 ORDER BY id"
        ).fetchall()
        for (batch_id,) in pending:
            self._execute_batch(batch_id)

    def _execute_batch(self, batch_id: int) -> None:
        """Physically reclaim blobs and finalize the batch. Idempotent."""
        pending = self._db.execute(
            "SELECT sha256 FROM blob_deletions WHERE batch_id = ? AND deleted = 0"
            " ORDER BY sha256",
            (batch_id,),
        ).fetchall()
        for (sha256,) in pending:
            path = self._blob_path(sha256)
            self._hook("execute.before_delete_file")
            # A missing file here means a previous attempt already deleted it:
            # the persistent blob_deletions row proves it belongs to an
            # accepted batch, so absence is completion, not data loss.
            if os.path.exists(path):
                os.unlink(path)
            self._hook("execute.after_delete_file")
            self._hook("execute.before_delete_commit")
            self._db.execute(
                "UPDATE blob_deletions SET deleted = 1"
                " WHERE batch_id = ? AND sha256 = ?",
                (batch_id, sha256),
            )
            self._db.commit()
            self._hook("execute.after_delete_commit")
        self._hook("execute.before_done_commit")
        self._db.execute("UPDATE batches SET done = 1 WHERE id = ?", (batch_id,))
        self._db.commit()
        self._hook("execute.after_done_commit")
