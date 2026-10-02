import hashlib
import io
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from archive_destroy import (  # noqa: E402
    Archive,
    ArchiveError,
    DestroyRejected,
    IdempotencyConflict,
    Receipt,
    RecordNotFound,
)

CHILD = os.path.join(os.path.dirname(os.path.abspath(__file__)), "crash_child.py")


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ArchiveCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.now = [1000.0]
        self.archive = Archive(
            os.path.join(self.tmp.name, "db.sqlite"),
            os.path.join(self.tmp.name, "blobs"),
            clock=lambda: self.now[0],
        )
        self.addCleanup(self.archive.close)

    def blob_path(self, digest):
        return os.path.join(self.tmp.name, "blobs", digest)

    def ingest(self, record_id, data, retain_until):
        return self.archive.ingest(record_id, io.BytesIO(data), retain_until)


class SharedBlobTest(ArchiveCase):
    def test_shared_blob_survives_until_last_reference_destroyed(self):
        shared = b"shared-payload" * 500
        digest = self.ingest("a1", shared, 500.0)
        self.ingest("a2", shared, 500.0)
        self.ingest("keep", shared, 10_000.0)  # same blob, longer retention
        self.assertTrue(os.path.exists(self.blob_path(digest)))

        receipt = self.archive.destroy(["a1", "a2"], "batch-1")
        self.assertEqual(receipt.reclaimed_blobs, ())
        self.assertEqual(receipt.retained_blobs, (digest,))
        # Shared blob retained: outside record still reads back byte-exact.
        self.assertEqual(self.archive.read("keep"), shared)
        self.assertTrue(os.path.exists(self.blob_path(digest)))
        # Destroyed records resolve to the batch receipt.
        self.assertEqual(self.archive.read("a1"), receipt)
        self.assertEqual(self.archive.read("a2"), receipt)

        self.now[0] = 20_000.0
        receipt2 = self.archive.destroy(["keep"], "batch-2")
        self.assertEqual(receipt2.reclaimed_blobs, (digest,))
        self.assertEqual(receipt2.retained_blobs, ())
        # Last live reference gone: physical file removed.
        self.assertFalse(os.path.exists(self.blob_path(digest)))
        self.assertEqual(self.archive.read("keep"), receipt2)

    def test_expired_but_unsubmitted_record_keeps_blob_alive(self):
        shared = b"retained-by-expired-record"
        digest = self.ingest("victim", shared, 500.0)
        # Also expired, but never submitted for destruction.
        self.ingest("bystander", shared, 500.0)
        self.now[0] = 600.0
        receipt = self.archive.destroy(["victim"], "k")
        self.assertEqual(receipt.retained_blobs, (digest,))
        self.assertTrue(os.path.exists(self.blob_path(digest)))
        self.assertEqual(self.archive.read("bystander"), shared)


class RejectionTest(ArchiveCase):
    def test_corrupted_member_rejects_whole_set(self):
        self.ingest("ok1", b"ok-one", 500.0)
        self.ingest("ok2", b"ok-two", 500.0)
        bad_digest = self.ingest("bad", b"original", 500.0)
        self.now[0] = 600.0
        with open(self.blob_path(bad_digest), "wb") as fh:
            fh.write(b"tampered")

        with self.assertRaises(DestroyRejected):
            self.archive.destroy(["ok1", "ok2", "bad"], "k-corrupt")

        # Whole set untouched; no receipt produced; key not consumed.
        self.assertEqual(self.archive.read("ok1"), b"ok-one")
        self.assertEqual(self.archive.read("ok2"), b"ok-two")
        receipt = self.archive.destroy(["ok1", "ok2"], "k-corrupt")
        self.assertEqual({r[0] for r in receipt.records}, {"ok1", "ok2"})

    def test_missing_member_blob_rejects_whole_set(self):
        self.ingest("ok", b"fine", 500.0)
        gone_digest = self.ingest("gone", b"vanished", 500.0)
        self.now[0] = 600.0
        os.unlink(self.blob_path(gone_digest))

        with self.assertRaises(DestroyRejected):
            self.archive.destroy(["ok", "gone"], "k-missing")
        self.assertEqual(self.archive.read("ok"), b"fine")
        # Rejected request did not bind the idempotency key.
        receipt = self.archive.destroy(["ok"], "k-missing")
        self.assertEqual([r[0] for r in receipt.records], ["ok"])

    def test_unexpired_member_rejects_whole_set(self):
        self.ingest("expired", b"old", 500.0)
        self.ingest("fresh", b"new", 5_000.0)
        self.now[0] = 600.0
        with self.assertRaises(DestroyRejected):
            self.archive.destroy(["expired", "fresh"], "k-time")
        self.assertEqual(self.archive.read("expired"), b"old")
        self.assertEqual(self.archive.read("fresh"), b"new")


class IdempotencyTest(ArchiveCase):
    def test_same_set_converges_different_set_rejected(self):
        self.ingest("x", b"1", 100.0)
        self.ingest("y", b"2", 100.0)
        self.ingest("z", b"3", 100.0)
        first = self.archive.destroy(["x", "y"], "key")
        # Unordered retry with the same set converges to the same receipt.
        again = self.archive.destroy(["y", "x"], "key")
        self.assertEqual(first, again)
        with self.assertRaises(IdempotencyConflict):
            self.archive.destroy(["x", "z"], "key")
        with self.assertRaises(IdempotencyConflict):
            self.archive.destroy(["x"], "key")

    def test_destroyed_record_id_cannot_be_reused(self):
        self.ingest("rid", b"v1", 100.0)
        self.archive.destroy(["rid"], "key")
        with self.assertRaises(ArchiveError):
            self.ingest("rid", b"v2", 100.0)

    def test_read_distinguishes_destroyed_from_lost(self):
        live_digest = self.ingest("live", b"live-data", 100.0)
        self.ingest("doomed", b"doomed-data", 100.0)
        receipt = self.archive.destroy(["doomed"], "key")
        # Destroyed record returns its receipt.
        self.assertEqual(self.archive.read("doomed"), receipt)
        # A live record whose blob vanished unexpectedly is an error,
        # never a receipt.
        os.unlink(self.blob_path(live_digest))
        with self.assertRaises(ArchiveError):
            self.archive.read("live")
        with self.assertRaises(RecordNotFound):
            self.archive.read("never-existed")


class CrashConvergenceTest(unittest.TestCase):
    def run_child(self, workdir, mode, crash_at):
        env = dict(os.environ, CRASH_AT=str(crash_at))
        return subprocess.run(
            [sys.executable, CHILD, workdir, mode],
            env=env,
            capture_output=True,
        ).returncode

    def test_repeated_crashes_converge_to_single_receipt(self):
        from tests.crash_child import BLOB_A, BLOB_B

        digest_a, digest_b = sha(BLOB_A), sha(BLOB_B)
        canonical = None
        crashed_points = set()
        crash_at = 1
        while True:
            with tempfile.TemporaryDirectory() as workdir:
                with open(os.path.join(workdir, "clock.txt"), "w") as fh:
                    fh.write("1000.0")

                rc = self.run_child(workdir, "first", crash_at)
                if rc == 0:
                    break  # crash_at beyond the last fault point
                self.assertEqual(rc, 3)
                with open(os.path.join(workdir, "hooks.log")) as fh:
                    points = fh.read().splitlines()
                self.assertEqual(len(points), crash_at)
                crashed_points.add(points[-1])

                # Crash again inside recovery/retry on reopen. If the first
                # crash happened after the final commit, no fault points
                # remain and the retry simply completes.
                rc2 = self.run_child(workdir, "first", 1)
                self.assertIn(rc2, (0, 3))
                if rc2 == 3:
                    # Clean run finishes the batch; retries converge.
                    self.assertEqual(self.run_child(workdir, "first", 0), 0)

                with open(os.path.join(workdir, "receipt1.json")) as fh:
                    receipt1 = fh.read()
                if canonical is None:
                    canonical = receipt1
                self.assertEqual(receipt1, canonical)

                archive = Archive(
                    os.path.join(workdir, "db.sqlite"),
                    os.path.join(workdir, "blobs"),
                    clock=lambda: 1000.0,
                )
                try:
                    # Protected shared content still reads byte-exact.
                    self.assertEqual(archive.read("r3"), BLOB_A)
                    # Destroyed records resolve to the same receipt.
                    self.assertEqual(
                        archive.read("r1"), Receipt.from_json(canonical)
                    )
                    # Sole-reference blob reclaimed; shared blob retained.
                    blobs = os.path.join(workdir, "blobs")
                    self.assertFalse(os.path.exists(os.path.join(blobs, digest_b)))
                    self.assertTrue(os.path.exists(os.path.join(blobs, digest_a)))
                finally:
                    archive.close()

                # Later, the last reference is destroyed and the shared
                # blob physically disappears.
                with open(os.path.join(workdir, "clock.txt"), "w") as fh:
                    fh.write("2000000000000.0")
                self.assertEqual(self.run_child(workdir, "finish", 0), 0)
                blobs = os.path.join(workdir, "blobs")
                self.assertFalse(os.path.exists(os.path.join(blobs, digest_a)))

            crash_at += 1

        self.assertIsNotNone(canonical)
        # Every commit/file-state fault point was exercised, including the
        # window where the file is deleted but the batch is not yet final.
        self.assertGreaterEqual(crash_at, 8)
        for point in (
            "destroy.before_accept_commit",
            "destroy.after_accept_commit",
            "execute.before_delete_file",
            "execute.after_delete_file",
            "execute.before_delete_commit",
            "execute.after_delete_commit",
            "execute.before_done_commit",
            "execute.after_done_commit",
        ):
            self.assertIn(point, crashed_points)


if __name__ == "__main__":
    unittest.main()
