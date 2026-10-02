"""Offline archive library with recoverable batch destruction."""

from .errors import (
    ArchiveError,
    BlobCorrupt,
    BlobMissing,
    DuplicateRecord,
    IdempotencyConflict,
    RecordNotFound,
    RetentionNotMet,
)
from .store import ArchiveStore, Receipt

__all__ = [
    "ArchiveError",
    "ArchiveStore",
    "BlobCorrupt",
    "BlobMissing",
    "DuplicateRecord",
    "IdempotencyConflict",
    "Receipt",
    "RecordNotFound",
    "RetentionNotMet",
]
