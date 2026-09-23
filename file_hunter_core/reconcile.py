"""Directory reconciliation — compare expected catalog contents against disk reality.

Synchronous. The agent endpoint wraps this in asyncio.to_thread().
"""

import logging
import os
import stat

from file_hunter_core.classify import classify_file
from file_hunter_core.hasher import hash_file_partial_sync
from file_hunter_core.paths import safe_str, safe_timestamp

logger = logging.getLogger("file_hunter_agent")

RECONCILE_PAGE_SIZE = 2000


def reconcile_directory(
    dirpath: str, root_path: str, expected: list[dict], cursor=None
) -> dict:
    """Compare expected catalog entries against what's on disk.

    Args:
        dirpath: absolute path to the directory to iterate
        root_path: location root for rel_path computation
        expected: list of dicts from the server's catalog, each with:
            rel_path, file_size, modified_date, hash_fast
        cursor: pagination offset. None = old server (no pagination),
            0 = first page (new server), N = continuation from offset N.

    Returns dict with:
        unchanged: list of rel_paths confirmed on disk matching expected
        changed: list of full file metadata dicts (size/mtime/hash differs)
        gone: list of rel_paths in expected but not on disk
        new: list of full file metadata dicts (on disk but not in expected)
        subdirs: list of subdirectory names found on disk
        cursor: next offset (int) or None if last page (only when paginating)
        total_new: total new file count (only when paginating)
        total_changed: total changed file count (only when paginating)
    """
    rel_dir = safe_str(os.path.relpath(dirpath, root_path))
    if rel_dir == ".":
        rel_dir = ""

    logger.info("reconcile: %s", rel_dir or "/")

    # Build lookup of expected files by rel_path
    expected_by_rel = {e["rel_path"]: e for e in expected}

    # --- Phase 1: listing + comparison (fast — no hashing) ---
    disk_files = {}  # rel_path -> (full_path, name, st)
    subdirs = []

    try:
        entries = os.listdir(dirpath)
    except (PermissionError, OSError):
        # Can't read directory — everything expected is gone
        return {
            "unchanged": [],
            "changed": [],
            "gone": [e["rel_path"] for e in expected],
            "new": [],
            "subdirs": [],
        }

    for raw_name in entries:
        full_path = os.path.join(dirpath, raw_name)
        name = safe_str(raw_name)

        try:
            is_link = os.path.islink(full_path)
        except OSError:
            continue

        if is_link:
            continue

        try:
            is_dir = os.path.isdir(full_path)
        except OSError:
            continue

        if is_dir:
            subdirs.append(name)
            continue

        # Regular file
        try:
            st = os.stat(full_path)
        except OSError:
            continue

        if not stat.S_ISREG(st.st_mode):
            continue

        # Skip .moved and .sources stubs
        if name.endswith(".moved") or name.endswith(".sources"):
            continue

        rel_path = os.path.join(rel_dir, name) if rel_dir else name
        disk_files[rel_path] = (full_path, name, st)

    # Compare expected against disk
    unchanged = []
    changed_raw = []  # (rel_path, full_path, name, st)
    gone = []

    for rel_path, exp in expected_by_rel.items():
        if rel_path not in disk_files:
            gone.append(rel_path)
            continue

        full_path, name, st = disk_files[rel_path]
        mtime = safe_timestamp(st.st_mtime, rel_path)

        if st.st_size == exp.get("file_size") and mtime == exp.get("modified_date"):
            unchanged.append(rel_path)
        else:
            changed_raw.append((rel_path, full_path, name, st))

    # Find new files (on disk but not expected)
    new_raw = []  # (rel_path, full_path, name, st)
    for rel_path, (full_path, name, st) in disk_files.items():
        if rel_path not in expected_by_rel:
            new_raw.append((rel_path, full_path, name, st))

    # --- Phase 2: pagination gate ---
    paginate = cursor is not None  # absent = old server

    if not paginate:
        # Old behavior: hash everything, return everything
        changed = [
            _build_file_info(full_path, name, rel_path, rel_dir, st)
            for rel_path, full_path, name, st in changed_raw
        ]
        new_files = [
            _build_file_info(full_path, name, rel_path, rel_dir, st)
            for rel_path, full_path, name, st in new_raw
        ]

        hashed = sum(1 for f in new_files + changed if f.get("hash_partial"))
        logger.info(
            "  %d unchanged, %d new, %d changed, %d gone, %d hashed",
            len(unchanged),
            len(new_files),
            len(changed),
            len(gone),
            hashed,
        )

        return {
            "unchanged": unchanged,
            "changed": changed,
            "gone": gone,
            "new": new_files,
            "subdirs": subdirs,
        }

    # Paginated path: combine new+changed, sort for deterministic cursor
    # Tag each item so we can split back into new vs changed after slicing
    combined = sorted(
        [("changed", rp, fp, n, s) for rp, fp, n, s in changed_raw]
        + [("new", rp, fp, n, s) for rp, fp, n, s in new_raw],
        key=lambda t: t[1],  # sort by rel_path
    )

    start = cursor if cursor else 0
    page = combined[start : start + RECONCILE_PAGE_SIZE]
    next_cursor = (
        (start + RECONCILE_PAGE_SIZE)
        if (start + RECONCILE_PAGE_SIZE) < len(combined)
        else None
    )

    # Hash ONLY this page
    page_new = []
    page_changed = []
    for kind, rel_path, full_path, name, st in page:
        info = _build_file_info(full_path, name, rel_path, rel_dir, st)
        if kind == "new":
            page_new.append(info)
        else:
            page_changed.append(info)

    is_first_page = start == 0
    hashed = sum(1 for f in page_new + page_changed if f.get("hash_partial"))
    logger.info(
        "  page %d-%d of %d: %d new, %d changed, %d hashed%s",
        start,
        start + len(page),
        len(combined),
        len(page_new),
        len(page_changed),
        hashed,
        "" if is_first_page else " (continuation)",
    )
    if is_first_page:
        logger.info(
            "  totals: %d unchanged, %d new, %d changed, %d gone",
            len(unchanged),
            len(new_raw),
            len(changed_raw),
            len(gone),
        )

    return {
        "unchanged": unchanged if is_first_page else [],
        "gone": gone if is_first_page else [],
        "subdirs": subdirs if is_first_page else [],
        "new": page_new,
        "changed": page_changed,
        "cursor": next_cursor,
        "total_new": len(new_raw),
        "total_changed": len(changed_raw),
    }


def _build_file_info(
    full_path: str, name: str, rel_path: str, rel_dir: str, st: os.stat_result
) -> dict:
    """Build file metadata dict with hash_partial computed."""
    hidden = name.startswith(".") or (
        rel_dir and any(part.startswith(".") for part in rel_dir.split(os.sep))
    )

    type_high, type_low = classify_file(name)

    # Compute hash_partial for non-hidden, non-zero files
    hash_partial = None
    if st.st_size > 0:
        try:
            hash_partial = hash_file_partial_sync(full_path)
        except OSError:
            pass

    return {
        "filename": name,
        "full_path": safe_str(full_path),
        "rel_path": rel_path,
        "rel_dir": rel_dir,
        "file_size": st.st_size,
        "created_date": safe_timestamp(
            st.st_birthtime if hasattr(st, "st_birthtime") else st.st_ctime,
            rel_path,
        ),
        "modified_date": safe_timestamp(st.st_mtime, rel_path),
        "file_type_high": type_high,
        "file_type_low": type_low,
        "hidden": 1 if hidden else 0,
        "hash_partial": hash_partial,
    }
