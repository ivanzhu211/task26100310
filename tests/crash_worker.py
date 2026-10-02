"""Subprocess worker for crash-injection tests.

Invoked as: python tests/crash_worker.py <mode> <dir> [args...]
Crash points are armed through the ARCHIVE_CRASH_POINTS env var; when one
fires the worker dies via os._exit(1) (no exception simulation).

Exit codes: 0 ok, 1 crashed by fault point, 42 IdempotencyConflict,
2 any other error.
"""

import dataclasses
import io
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from archive import ArchiveStore, IdempotencyConflict, Receipt  # noqa: E402

FIXED_CLOCK = lambda: datetime(2030, 1, 1, tzinfo=timezone.utc)  # noqa: E731
PAST = datetime(2020, 1, 1, tzinfo=timezone.utc)
FUTURE = datetime(2099, 1, 1, tzinfo=timezone.utc)

SHARED_DATA = b"shared-payload" * 4096
SOLO_DATA = b"solo-payload" * 4096


def open_store(root):
    return ArchiveStore(root, clock=FIXED_CLOCK)


def receipt_json(receipt: Receipt) -> str:
    return json.dumps(dataclasses.asdict(receipt), sort_keys=True)


def main(argv):
    mode, root = argv[1], argv[2]

    if mode == "setup":
        store = open_store(root)
        # v1/v2/keeper share one blob; keeper stays unexpired under the
        # fixed 2030 clock.  solo owns a unique, reclaimable blob.
        store.ingest("v1", io.BytesIO(SHARED_DATA), PAST)
        store.ingest("v2", io.BytesIO(SHARED_DATA), PAST)
        store.ingest("keeper", io.BytesIO(SHARED_DATA), FUTURE)
        store.ingest("solo", io.BytesIO(SOLO_DATA), PAST)
        store.close()
        return 0

    if mode == "destroy":
        key = argv[3]
        ids = argv[4].split(",")
        store = open_store(root)
        try:
            receipt = store.destroy(ids, key)
        except IdempotencyConflict:
            return 42
        print("RECEIPT:" + receipt_json(receipt))
        store.close()
        return 0

    if mode == "read":
        store = open_store(root)
        result = store.read(argv[3])
        if isinstance(result, Receipt):
            print("RECEIPT:" + receipt_json(result))
        else:
            print("BYTES:" + result.hex())
        store.close()
        return 0

    if mode == "open":
        store = open_store(root)  # recovery runs on open
        store.close()
        return 0

    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
