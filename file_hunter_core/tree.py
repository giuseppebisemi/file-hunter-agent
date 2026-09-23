"""Two-phase tree walk — metadata first, then hashes in inode order.

Synchronous BFS generator streaming TSV over a single HTTP response.

Phase 1 (metadata): streams D and F records as directories are discovered.
  Fast — stat only, no file reads. Server can ingest immediately.

Phase 2 (hashing): streams H records for all files sorted by inode.
  Reads first+last 64KB per file. Inode order minimises disk seeks.
  File list spilled to a temp SQLite DB during phase 1 so memory stays
  O(1) regardless of location size.

TSV format (tabs in filenames converted to spaces):
    D\trel_dir\n
    F\trel_path\tsize\tmtime\tctime\tinode\n
    W\trel_path\tmessage\n
    P\thashing\ttotal_files\n
    H\trel_path\thash_partial\n
    E\ttotal_dirs\ttotal_files\n
"""

import logging
import os
import sqlite3
import stat
import tempfile
import time
from collections import deque
from file_hunter_core.hasher import hash_file_partial_sync
from file_hunter_core.paths import safe_str, safe_path, safe_timestamp, TIMESTAMP_INVALID

logger = logging.getLogger("file_hunter_agent")


def walk_tree(root: str, prefix: str | None = None, fmt: str = "tsv",
              metadata_only: bool = False):
    """BFS generator: metadata phase then hash phase.

    Args:
        root: absolute path to location root
        prefix: optional subdirectory prefix (relative to root) to scope the walk
        fmt: "tsv" (default). Only supported format.
        metadata_only: if True, skip the hash phase (for rescan diff).
    """
    if fmt != "tsv":
        raise ValueError(f"Unsupported format: {fmt!r} (only 'tsv' is supported)")

    scope = prefix or root
    logger.info("Tree walk starting: %s", scope)
    t0 = time.monotonic()
    last_log = t0
    start = os.path.join(root, prefix) if prefix else root
    queue = deque([start])
    total_dirs = 0
    total_files = 0

    # Spill file list to disk so memory stays O(1) for the hash phase
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".db", prefix="tree_")
    os.close(tmp_fd)
    spill_db = sqlite3.connect(tmp_path, check_same_thread=False)
    spill_db.execute("PRAGMA journal_mode=OFF")
    spill_db.execute("PRAGMA synchronous=OFF")
    spill_db.execute(
        "CREATE TABLE files (inode INTEGER, full_path BLOB, rel_path TEXT, size INTEGER)"
    )
    spill_batch: list[tuple] = []
    hashable_count = 0

    try:
        yield from _walk_and_hash(
            root, start, queue, spill_db, spill_batch, scope, t0, last_log,
            total_dirs, total_files, hashable_count, metadata_only, tmp_path,
        )
    finally:
        spill_db.close()
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def _walk_and_hash(root, start, queue, spill_db, spill_batch, scope, t0, last_log,
                   total_dirs, total_files, hashable_count, metadata_only, tmp_path):
    """Inner generator — separated so walk_tree's finally block always cleans up."""

    # --- Phase 1: metadata ---
    while queue:
        dirpath = queue.popleft()

        rel_dir = safe_str(os.path.relpath(dirpath, root).replace(os.sep, "/"))
        if rel_dir == ".":
            rel_dir = ""

        now = time.monotonic()
        if now - last_log >= 5.0:
            logger.info(
                "Tree walk metadata: %s — %d dirs, %d files so far (%.1fs)",
                scope,
                total_dirs,
                total_files,
                now - t0,
            )
            last_log = now

        logger.debug("Entering: %s", dirpath)

        try:
            scandir_it = os.scandir(dirpath)
        except (PermissionError, OSError) as e:
            logger.debug("Skipped (permission/OS error): %s — %s", dirpath, e)
            continue

        subdirs = []
        dir_file_count = 0
        line_buf = [f"D\t{rel_dir.replace(chr(9), ' ')}\n"]

        for entry in scandir_it:
            try:
                if entry.is_symlink():
                    continue
                if entry.is_dir(follow_symlinks=False):
                    subdirs.append(entry.path)
                    continue
                st = entry.stat(follow_symlinks=False)
            except OSError:
                continue

            if not stat.S_ISREG(st.st_mode):
                continue

            name = safe_str(entry.name)
            rel_path = f"{rel_dir}/{name}" if rel_dir else name
            mtime = safe_timestamp(st.st_mtime, rel_path)
            ctime = safe_timestamp(
                st.st_birthtime if hasattr(st, "st_birthtime") else st.st_ctime,
                rel_path,
            )

            ino = st.st_ino
            if ino >= 2**63:
                ino -= 2**64

            safe_rel = rel_path.replace(chr(9), " ")
            line_buf.append(f"F\t{safe_rel}\t{st.st_size}\t{mtime}\t{ctime}\t{ino}\n")
            if mtime == TIMESTAMP_INVALID or ctime == TIMESTAMP_INVALID:
                line_buf.append(f"W\t{safe_rel}\tInvalid file timestamp\n")
            dir_file_count += 1

            if st.st_size > 0:
                spill_batch.append((ino, safe_path(entry.path), rel_path, st.st_size))
                hashable_count += 1
                if len(spill_batch) >= 5000:
                    spill_db.executemany("INSERT INTO files VALUES (?,?,?,?)", spill_batch)
                    spill_db.commit()
                    spill_batch.clear()

            if len(line_buf) >= 1000:
                yield "".join(line_buf)
                line_buf.clear()

        total_dirs += 1
        total_files += dir_file_count

        if line_buf:
            yield "".join(line_buf)

        for sd in sorted(subdirs):
            queue.append(sd)

    walk_elapsed = time.monotonic() - t0
    logger.info(
        "Tree walk metadata complete: %s — %d dirs, %d files in %.1fs",
        scope,
        total_dirs,
        total_files,
        walk_elapsed,
    )

    if metadata_only:
        logger.info("Tree walk: metadata_only — skipping hash phase")
        yield f"E\t{total_dirs}\t{total_files}\n"
        return

    # --- Phase 2: hash partials in inode order ---
    # Flush remaining spill batch and index for sorted read
    if spill_batch:
        spill_db.executemany("INSERT INTO files VALUES (?,?,?,?)", spill_batch)
        spill_db.commit()
        spill_batch.clear()
    spill_db.execute("CREATE INDEX idx_inode ON files(inode)")
    spill_db.commit()

    logger.info("Tree hash phase: %d files to hash (inode-sorted)", hashable_count)

    yield f"P\thashing\t{hashable_count}\n"

    hash_t0 = time.monotonic()
    last_flush = hash_t0
    last_log = hash_t0
    hashed = 0
    buf: list[str] = []

    cursor = spill_db.execute("SELECT inode, full_path, rel_path, size FROM files ORDER BY inode")
    for ino, full_path_bytes, rel_path, size in cursor:
        try:
            hp = hash_file_partial_sync(os.fsdecode(full_path_bytes))
        except (OSError, PermissionError) as e:
            logger.debug("Hash skipped: %s — %s", rel_path, e)
            continue

        safe_rel = rel_path.replace(chr(9), " ")
        buf.append(f"H\t{safe_rel}\t{hp}\n")
        hashed += 1

        now = time.monotonic()

        # Flush on time interval — gives accurate counts to server
        if now - last_flush >= 5.0:
            yield "".join(buf)
            buf.clear()
            last_flush = now

        if now - last_log >= 5.0:
            rate = hashed / (now - hash_t0) if (now - hash_t0) > 0 else 0
            logger.info(
                "Tree hash progress: %s — %d / %d hashed (%.0f/sec, %.1fs)",
                scope,
                hashed,
                hashable_count,
                rate,
                now - hash_t0,
            )
            last_log = now

    if buf:
        yield "".join(buf)

    hash_elapsed = time.monotonic() - hash_t0
    total_elapsed = time.monotonic() - t0
    logger.info(
        "Tree walk complete: %s — %d dirs, %d files, %d hashed in %.1fs (walk %.1fs, hash %.1fs)",
        scope,
        total_dirs,
        total_files,
        hashed,
        total_elapsed,
        walk_elapsed,
        hash_elapsed,
    )
    yield f"E\t{total_dirs}\t{total_files}\n"
