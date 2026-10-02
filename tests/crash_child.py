"""Child-process worker for crash-injection tests.

Usage: python tests/crash_child.py <workdir> <first|finish>

Env:
  CRASH_AT=N  -- os._exit(3) when the Nth fault-injection point fires.

The child appends every fault point it reaches to <workdir>/hooks.log so the
parent can verify exactly which crash windows were exercised.
"""

import io
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from archive_destroy import Archive  # noqa: E402

BLOB_A = b"shared-content-" + b"a" * 4096
BLOB_B = b"sole-content-" + b"b" * 4096


def main() -> None:
    workdir = sys.argv[1]
    mode = sys.argv[2]
    crash_at = int(os.environ.get("CRASH_AT", "0"))
    state = {"n": 0}
    log_path = os.path.join(workdir, "hooks.log")

    def hook(point: str) -> None:
        state["n"] += 1
        with open(log_path, "a") as log:
            log.write(point + "\n")
        if crash_at and state["n"] == crash_at:
            os._exit(3)

    def clock() -> float:
        with open(os.path.join(workdir, "clock.txt")) as fh:
            return float(fh.read().strip())

    archive = Archive(
        os.path.join(workdir, "db.sqlite"),
        os.path.join(workdir, "blobs"),
        clock=clock,
        fault_hook=hook,
    )

    if mode == "first":
        marker = os.path.join(workdir, "ingested")
        if not os.path.exists(marker):
            archive.ingest("r1", io.BytesIO(BLOB_A), 100.0)
            archive.ingest("r2", io.BytesIO(BLOB_A), 100.0)
            archive.ingest("r3", io.BytesIO(BLOB_A), 1e12)
            archive.ingest("r4", io.BytesIO(BLOB_B), 100.0)
            open(marker, "w").close()
        receipt = archive.destroy(["r1", "r2", "r4"], "k1")
        with open(os.path.join(workdir, "receipt1.json"), "w") as fh:
            fh.write(receipt.to_json())
    else:
        receipt = archive.destroy(["r3"], "k2")
        with open(os.path.join(workdir, "receipt2.json"), "w") as fh:
            fh.write(receipt.to_json())
    archive.close()


if __name__ == "__main__":
    main()
