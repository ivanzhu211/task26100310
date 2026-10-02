"""Crash fault-injection hooks for Destroy/Recover durability testing.

Fault points are enabled through the ARCHIVE_CRASH_POINTS environment
variable: a comma-separated list of point names.  When a named point is
reached, the process exits immediately via os._exit(1), simulating a
forced kill (no exception unwinding, no cleanup handlers).
"""

import os

ENV_VAR = "ARCHIVE_CRASH_POINTS"


def crash_point(name: str) -> None:
    """Hard-exit the process if ``name`` is listed in ARCHIVE_CRASH_POINTS."""
    points = os.environ.get(ENV_VAR)
    if points and name in points.split(","):
        os._exit(1)
