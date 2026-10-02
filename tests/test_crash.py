"""Crash-recovery tests: real subprocess kills at every fault point.

The worker process is hard-killed (os._exit) around every database commit
and every file-state change in Destroy/Recover.  The parent then reopens
the archive and retries, asserting convergence on a single stable
receipt, survival of shared content, and eventual physical reclamation.
"""

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORKER = os.path.join(REPO_ROOT, "tests", "crash_worker.py")

sys.path.insert(0, REPO_ROOT)
from tests.crash_worker import SHARED_DATA, SOLO_DATA  # noqa: E402

FAULT_POINTS = [
    "destroy.before_accept_commit",
    "destroy.after_accept_commit",
    "destroy.before_blob_delete",
    "destroy.after_blob_delete",
    "destroy.before_complete_commit",
    "destroy.after_complete_commit",
]

SHARED_SHA = hashlib.sha256(SHARED_DATA).hexdigest()
SOLO_SHA = hashlib.sha256(SOLO_DATA).hexdigest()


def run_worker(*args, crash_point=None):
    env = dict(os.environ)
    if crash_point:
        env["ARCHIVE_CRASH_POINTS"] = crash_point
    return subprocess.run(
        [sys.executable, WORKER, *args],
        capture_output=True,
        text=True,
        env=env,
        cwd=REPO_ROOT,
    )


def receipt_of(proc):
    for line in proc.stdout.splitlines():
        if line.startswith("RECEIPT:"):
            return json.loads(line[len("RECEIPT:"):])
    return None


class CrashRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="archive-crash-")
        self.addCleanup(shutil.rmtree, self.dir, True)
        proc = run_worker("setup", self.dir)
        self.assertEqual(proc.returncode, 0, proc.stderr)

    def blob_exists(self, sha):
        return os.path.exists(os.path.join(self.dir, "blobs", sha))

    def destroy_until_done(self, key="K", ids="v1,v2,solo"):
        """Retry destroy (each attempt reopens and recovers first)."""
        for _ in range(10):
            proc = run_worker("destroy", self.dir, key, ids)
            if proc.returncode == 0:
                return receipt_of(proc)
        self.fail("destroy did not converge after repeated retries")

    def test_crash_at_every_fault_point_converges(self):
        for point in FAULT_POINTS:
            with self.subTest(point=point):
                crashed = run_worker(
                    "destroy", self.dir, "K", "v1,v2,solo", crash_point=point
                )
                self.assertEqual(crashed.returncode, 1)

                receipt = self.destroy_until_done()
                self.assertIsNotNone(receipt)

                # Retries converge on the same single receipt.
                again = self.destroy_until_done()
                self.assertEqual(receipt, again)
                read_back = run_worker("read", self.dir, "v1")
                self.assertEqual(receipt_of(read_back), receipt)

                # Receipt distinguishes reclaimed vs shared-retained blobs.
                self.assertEqual(receipt["reclaimed_blobs"], [SOLO_SHA])
                self.assertEqual(receipt["retained_blobs"], [SHARED_SHA])

                # Shared content still readable byte-for-byte; solo blob
                # physically gone.
                keeper = run_worker("read", self.dir, "keeper")
                self.assertTrue(keeper.stdout.startswith("BYTES:"))
                self.assertEqual(
                    bytes.fromhex(keeper.stdout.strip()[6:]), SHARED_DATA
                )
                self.assertTrue(self.blob_exists(SHARED_SHA))
                self.assertFalse(self.blob_exists(SOLO_SHA))

                # Reset for the next fault point.
                shutil.rmtree(self.dir)
                os.makedirs(self.dir)
                self.assertEqual(run_worker("setup", self.dir).returncode, 0)

    def test_recover_itself_repeatedly_killed(self):
        # Accept the batch, then die before any blob is touched.
        proc = run_worker(
            "destroy",
            self.dir,
            "K",
            "v1,v2,solo",
            crash_point="destroy.after_accept_commit",
        )
        self.assertEqual(proc.returncode, 1)

        # Recover (triggered by reopening) is killed right after deleting
        # the first blob: the file is gone but no receipt exists yet.
        proc = run_worker(
            "open", self.dir, crash_point="destroy.after_blob_delete"
        )
        self.assertEqual(proc.returncode, 1)

        # Kill recover again at the completion commit boundary.
        proc = run_worker(
            "open", self.dir, crash_point="destroy.before_complete_commit"
        )
        self.assertEqual(proc.returncode, 1)

        # A clean reopen finishes recovery; the receipt is stable across
        # every subsequent retry.
        receipt = self.destroy_until_done()
        self.assertEqual(receipt, self.destroy_until_done())
        self.assertEqual(receipt["reclaimed_blobs"], [SOLO_SHA])
        self.assertFalse(self.blob_exists(SOLO_SHA))
        keeper = run_worker("read", self.dir, "keeper")
        self.assertEqual(bytes.fromhex(keeper.stdout.strip()[6:]), SHARED_DATA)

    def test_file_deleted_before_receipt_window(self):
        # Crash precisely in the window: blob file already unlinked,
        # success receipt not yet committed.
        proc = run_worker(
            "destroy",
            self.dir,
            "K",
            "v1,v2,solo",
            crash_point="destroy.after_blob_delete",
        )
        self.assertEqual(proc.returncode, 1)
        # At least one target blob is already gone from disk, yet no
        # completed receipt may exist.
        self.assertFalse(self.blob_exists(SOLO_SHA))

        receipt = self.destroy_until_done()
        self.assertEqual(receipt["reclaimed_blobs"], [SOLO_SHA])
        # Retried reads of destroyed records return that same receipt,
        # never a "missing file" error.
        for record in ("v1", "v2", "solo"):
            proc = run_worker("read", self.dir, record)
            self.assertEqual(receipt_of(proc), receipt)

    def test_conflict_after_crash_before_return(self):
        # First call commits the completed receipt but dies before
        # returning it; the key is still permanently bound.
        proc = run_worker(
            "destroy",
            self.dir,
            "K",
            "v1,v2,solo",
            crash_point="destroy.after_complete_commit",
        )
        self.assertEqual(proc.returncode, 1)

        conflict = run_worker("destroy", self.dir, "K", "v1,v2")
        self.assertEqual(conflict.returncode, 42)

        # The correct retry converges on the original receipt.
        receipt = self.destroy_until_done()
        self.assertEqual(receipt["idempotency_key"], "K")
        self.assertEqual(
            sorted(r[0] for r in receipt["records"]), ["solo", "v1", "v2"]
        )


if __name__ == "__main__":
    unittest.main()
