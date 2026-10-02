"""Exception types for the archive store."""


class ArchiveError(Exception):
    """Base class for all archive errors."""


class RecordNotFound(ArchiveError):
    """The record_id is unknown (never ingested)."""


class DuplicateRecord(ArchiveError):
    """The record_id is already in use (alive or destroyed)."""


class RetentionNotMet(ArchiveError):
    """A destroy target has not reached its retain_until deadline."""


class BlobCorrupt(ArchiveError):
    """A blob's recomputed checksum does not match the recorded one."""


class BlobMissing(ArchiveError):
    """A blob file is unexpectedly absent from disk."""


class IdempotencyConflict(ArchiveError):
    """The idempotency key is bound to a different record set."""
