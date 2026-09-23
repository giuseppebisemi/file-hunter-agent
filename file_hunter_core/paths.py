"""Path utilities for filesystem string handling.

On Linux, filenames are raw bytes. Python's os.scandir() decodes them
using the surrogateescape error handler — bytes that aren't valid UTF-8
become surrogate code points (U+DC80..U+DCFF). These look like normal
strings but cannot be encoded to UTF-8, which breaks JSON serialisation,
HTTP response encoding, and SQLite TEXT storage.

safe_str() replaces surrogates with U+FFFD (replacement character) so
the string is safe for any output boundary. Use it on every path or
filename that leaves the process. Keep the raw path for actual file
operations (open, stat, hash).
"""

import logging
import os
from datetime import datetime, timezone


def safe_str(s: str) -> str:
    """Return a UTF-8 safe copy of a filesystem string."""
    try:
        s.encode("utf-8")
        return s
    except UnicodeEncodeError:
        return s.encode("utf-8", errors="surrogateescape").decode(
            "utf-8", errors="replace"
        )


def safe_path(path: str) -> bytes:
    """Encode a filesystem path to bytes for safe SQLite BLOB storage.

    The raw bytes survive the round-trip through SQLite and can be
    decoded back with os.fsdecode() for file operations.
    """
    return os.fsencode(path)


logger = logging.getLogger("file_hunter_agent")

_DATETIME_MIN_UTC = datetime.min.replace(tzinfo=timezone.utc)
TIMESTAMP_INVALID = _DATETIME_MIN_UTC.isoformat(timespec="seconds")


def safe_timestamp(ts: float, path: str = "") -> str:
    """Convert a filesystem timestamp to an ISO 8601 string.

    Returns TIMESTAMP_INVALID (0001-01-01T00:00:00+00:00) for timestamps
    that fall outside Python's supported year range (1-9999).
    Compare the result against TIMESTAMP_INVALID to detect clamped values.
    """
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(
            timespec="seconds"
        )
    except (ValueError, OverflowError, OSError):
        logger.warning("Invalid timestamp %.0f on %s — using datetime.min", ts, path)
        return TIMESTAMP_INVALID


def norm_inode(ino: int) -> int:
    """Wrap an inode number into the signed 64-bit range SQLite accepts.

    Some filesystems (exFAT via fskit on macOS, several FUSE drivers)
    hand out unsigned 64-bit inode numbers. Anything at or above 2**63
    raises OverflowError on insert, which kills a scan for one file.
    Wrapping is lossless within a filesystem: values stay unique and
    keep their order, which is all the column is used for (file
    identity and inode-ordered reads).
    """
    ino &= (1 << 64) - 1
    return ino - (1 << 64) if ino >= (1 << 63) else ino
