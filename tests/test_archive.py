"""Functional tests for ArchiveStore (no crash injection)."""

import io
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from archive import (
    ArchiveStore,
    BlobMissing,
    DuplicateRecord,
    IdempotencyConflict,
    Receipt,
    RecordNotFound,
    RetentionNotMet,
)

PAST = datetime(2020, 1, 1, tzinfo=timezone.utc)
FUTURE = datetime(2099, 1, 1, tzinfo=timezone.utc)


class ArchiveTestCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, True)
        self.store = ArchiveStore(self.dir)
        self.addCleanup(self.store.close)

    def ingest(self, record_id, data, retain_until=PAST):
        return self.store.ingest(record_id, io.BytesIO(data), retain_until)

    def blob_path(self, sha):
        return os.path.join(self.dir, "blobs", sha)


class SharedBlobTest(ArchiveTestCase):
    def test_shared_blob_survives_until_last_reference_destroyed(self):
        data = b"shared-content" * 1000
        sha = self.ingest("r1", data, PAST)
        self.ingest("r2", data, PAST)
        self.ingest("r3", data, FUTURE)

        receipt = self.store.destroy(["r1", "r2"], "batch-1")
        self.assertEqual(receipt.reclaimed_blobs, ())
        self.assertEqual(receipt.retained_blobs, (sha,))
        # Outside record, not yet expired, still reads byte-for-byte.
        self.assertEqual(self.store.read("r3"), data)
        self.assertTrue(os.path.exists(self.blob_path(sha)))

        # Force r3 to expire via an injectable clock: reopen with a clock
        # far in the future.
        self.store.close()
        later = datetime(2100, 1, 1, tzinfo=timezone.utc)
        self.store = ArchiveStore(self.dir, clock=lambda: later)
        receipt2 = self.store.destroy(["r3"], "batch-2")
        self.assertEqual(receipt2.reclaimed_blobs, (sha,))
        self.assertEqual(receipt2.retained_blobs, ())
        self.assertFalse(os.path.exists(self.blob_path(sha)))

    def test_expired_but_unrequested_record_keeps_blob(self):
        data = b"keep-me"
        sha = self.ingest("victim", data, PAST)
        self.ingest("bystander", data, PAST)  # expired too, but not requested
        receipt = self.store.destroy(["victim"], "k")
        self.assertEqual(receipt.retained_blobs, (sha,))
        self.assertEqual(self.store.read("bystander"), data)


class DestroyValidationTest(ArchiveTestCase):
    def test_corrupted_target_rejects_whole_set(self):
        good = b"good-data"
        sha_good = self.ingest("good", good, PAST)
        sha_bad = self.ingest("bad", b"bad-data", PAST)
        with open(self.blob_path(sha_bad), "wb") as handle:
            handle.write(b"tampered")

        with self.assertRaises(Exception) as ctx:
            self.store.destroy(["good", "bad"], "k-corrupt")
        self.assertIn("corrupt", type(ctx.exception).__name__.lower())

        # Nothing was destroyed; no receipt; key not consumed.
        self.assertEqual(self.store.read("good"), good)
        self.assertTrue(os.path.exists(self.blob_path(sha_good)))
        self.assertTrue(os.path.exists(self.blob_path(sha_bad)))
        self.assertRaises(
            RecordNotFound, self.store.destroy, ["good", "bad", "ghost"], "k2"
        )
        # The rejected key is free for a corrected retry.
        with open(self.blob_path(sha_bad), "wb") as handle:
            handle.write(b"bad-data")
        receipt = self.store.destroy(["good", "bad"], "k-corrupt")
        self.assertIsInstance(receipt, Receipt)

    def test_missing_blob_rejects_whole_set(self):
        self.ingest("x", b"x-data", PAST)
        sha_y = self.ingest("y", b"y-data", PAST)
        os.unlink(self.blob_path(sha_y))
        with self.assertRaises(BlobMissing):
            self.store.destroy(["x", "y"], "k-missing")
        self.assertEqual(self.store.read("x"), b"x-data")

    def test_unexpired_target_rejects_whole_set(self):
        self.ingest("old", b"old", PAST)
        self.ingest("young", b"young", FUTURE)
        with self.assertRaises(RetentionNotMet):
            self.store.destroy(["old", "young"], "k-young")
        self.assertEqual(self.store.read("old"), b"old")
        self.assertEqual(self.store.read("young"), b"young")


class IdempotencyTest(ArchiveTestCase):
    def test_same_set_converges_to_same_receipt(self):
        self.ingest("a", b"a", PAST)
        self.ingest("b", b"b", PAST)
        first = self.store.destroy(["a", "b"], "key")
        again = self.store.destroy(["b", "a"], "key")  # unordered retry
        self.assertEqual(first, again)

    def test_different_set_same_key_rejected(self):
        self.ingest("a", b"a", PAST)
        self.ingest("b", b"b", PAST)
        self.store.destroy(["a"], "key")
        with self.assertRaises(IdempotencyConflict):
            self.store.destroy(["a", "b"], "key")
        with self.assertRaises(IdempotencyConflict):
            self.store.destroy(["b"], "key")

    def test_destroyed_record_id_cannot_be_reingested(self):
        self.ingest("a", b"a", PAST)
        self.store.destroy(["a"], "key")
        with self.assertRaises(DuplicateRecord):
            self.ingest("a", b"other", PAST)


class ReadTest(ArchiveTestCase):
    def test_destroyed_read_returns_receipt_not_confused_with_loss(self):
        sha = self.ingest("gone", b"data", PAST)
        receipt = self.store.destroy(["gone"], "key")
        result = self.store.read("gone")
        self.assertIsInstance(result, Receipt)
        self.assertEqual(result, receipt)
        self.assertEqual(result.records, (("gone", sha),))

    def test_missing_blob_of_live_record_is_an_error(self):
        sha = self.ingest("alive", b"data", PAST)
        os.unlink(self.blob_path(sha))
        with self.assertRaises(BlobMissing):
            self.store.read("alive")

    def test_unknown_record(self):
        with self.assertRaises(RecordNotFound):
            self.store.read("nope")


if __name__ == "__main__":
    unittest.main()
