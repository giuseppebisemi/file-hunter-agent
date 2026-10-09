"""Scan orchestrator — BFS walk + hash, result batching via WebSocket.

Uses file_hunter_core for synchronous filesystem operations, wrapped in
asyncio.to_thread() to keep the event loop responsive.

Incremental mode: on rescan, compares filesystem state against a local
cache to identify new/changed/deleted files. Only new and changed files
are hashed and sent to the server. Deleted file paths are reported so
the server can mark them stale. The cache is updated incrementally as
batches are processed, so cancelled scans can resume.
"""

import asyncio
import logging
import os
import time
from collections import deque

from file_hunter_core.walker import scan_directory
from file_hunter_core.hasher import FileNotLocalError, hash_file_partial_sync

_SCAN_BATCH = 200  # entries consumed per thread dispatch from scan_directory

logger = logging.getLogger("file_hunter_agent")

# Scan state
_scan_task: asyncio.Task | None = None
_cancel_flag = False
_scanning = False
_current_path = ""

# Message send function — set by client.py after WS connects
_send_fn = None

BATCH_SIZE = 50
PROGRESS_INTERVAL = 0.5  # seconds


def set_send_fn(fn):
    """Register the WebSocket send function for streaming results."""
    global _send_fn
    _send_fn = fn


def is_scanning() -> bool:
    return _scanning


def get_current_path() -> str:
    return _current_path


def cancel_scan():
    global _cancel_flag
    _cancel_flag = True


async def start_scan(path: str, root_path: str | None = None) -> bool:
    """Start a scan. Returns False if already scanning.

    path: directory to walk (scan target — may be a subfolder)
    root_path: location root for rel_path computation (defaults to path)
    """
    global _scan_task, _cancel_flag, _scanning, _current_path

    if _scanning:
        return False

    _cancel_flag = False
    _scanning = True
    _current_path = path
    _scan_task = asyncio.create_task(_run_scan(path, root_path or path))
    return True


async def _send(msg: dict):
    """Send a message via the WebSocket client if connected."""
    if _send_fn:
        logger.info("Scanner sending: %s", msg.get("type", "?"))
        await _send_fn(msg)
    else:
        logger.warning(
            "Scanner: no send function registered, message dropped: %s",
            msg.get("type", "?"),
        )


async def _run_scan(path: str, root_path: str):
    global _scanning, _current_path

    files_found = 0
    files_hashed = 0
    files_unchanged = 0
    last_progress = 0.0
    file_batch = []

    from file_hunter_agent.services.cache import ScanCache

    scan_cache = ScanCache(root_path)

    try:
        # --- Check cache for incremental mode ---
        await asyncio.to_thread(scan_cache.open)
        incremental = await asyncio.to_thread(scan_cache.has_entries)
        if incremental:
            logger.info("Incremental scan for %s", root_path)
            await asyncio.to_thread(scan_cache.begin_seen_tracking)
        else:
            logger.info("Full scan (no cache): %s", root_path)

        seen_batch: list[str] = []

        # --- Walk and hash inline ---
        logger.info("Scan starting for: %s (root: %s)", path, root_path)
        queue = deque([(path, False)])  # (dirpath, is_hidden)

        while queue:
            if _cancel_flag:
                await _send(
                    {
                        "type": "scan_cancelled",
                        "path": path,
                        "filesFound": files_found,
                        "filesHashed": files_hashed,
                    }
                )
                return

            dirpath, dir_hidden = queue.popleft()
            scan_iter = scan_directory(dirpath, root_path, dir_hidden)

            def _next_batch(it=scan_iter):
                batch = []
                for _ in range(_SCAN_BATCH):
                    item = next(it, None)
                    if item is None:
                        break
                    batch.append(item)
                return batch

            while True:
                batch = await asyncio.to_thread(_next_batch)
                if not batch:
                    break
                for item_type, data in batch:
                    if item_type == "dir":
                        sub_hidden = dir_hidden or os.path.basename(data).startswith(".")
                        queue.append((data, sub_hidden))
                        continue
                    fi = data
                    name = fi["filename"]
                    if name.endswith(".moved") or name.endswith(".sources"):
                        continue

                    files_found += 1
                    rel_path = fi["rel_path"]

                    if incremental:
                        seen_batch.append(rel_path)
                        if len(seen_batch) >= 500:
                            await asyncio.to_thread(scan_cache.mark_seen_batch, seen_batch)
                            seen_batch.clear()

                        cached = await asyncio.to_thread(scan_cache.lookup, rel_path)
                        if (
                            cached
                            and cached[0] == fi["file_size"]
                            and cached[1] == fi["modified_date"]
                        ):
                            files_unchanged += 1
                            continue

                    # Hash inline instead of accumulating
                    fp = fi["full_path"]
                    if fi.get("hidden") or fi.get("file_size", 0) == 0:
                        fi["hash_partial"] = None
                        fi["hash_fast"] = None
                        fi["hash_strong"] = None
                    else:
                        try:
                            partial = await asyncio.to_thread(hash_file_partial_sync, fp)
                            fi["hash_partial"] = partial
                            fi["hash_fast"] = None
                            fi["hash_strong"] = None
                        except OSError as e:
                            log = (
                                logger.debug
                                if isinstance(e, FileNotLocalError)
                                else logger.warning
                            )
                            log("Hash failed: %s: %s", fp, e)
                            fi["hash_partial"] = None
                            fi["hash_fast"] = None
                            fi["hash_strong"] = None

                    files_hashed += 1
                    file_batch.append(fi)

                    if len(file_batch) >= BATCH_SIZE:
                        await _send({"type": "scan_files", "path": path, "files": file_batch})
                        await asyncio.to_thread(scan_cache.update_batch, file_batch)
                        file_batch = []

            # Throttled progress broadcast
            now = time.monotonic()
            if now - last_progress >= PROGRESS_INTERVAL:
                last_progress = now
                await _send(
                    {
                        "type": "scan_progress",
                        "path": path,
                        "phase": "scanning",
                        "filesFound": files_found,
                        "filesHashed": files_hashed,
                        "filesUnchanged": files_unchanged,
                    }
                )

        # Flush remaining batch
        if file_batch:
            await _send({"type": "scan_files", "path": path, "files": file_batch})
            await asyncio.to_thread(scan_cache.update_batch, file_batch)

        # --- Identify deleted files ---
        deleted_paths = []
        if incremental:
            if seen_batch:
                await asyncio.to_thread(scan_cache.mark_seen_batch, seen_batch)
                seen_batch.clear()
            deleted_paths = await asyncio.to_thread(scan_cache.get_deleted)
            if deleted_paths:
                logger.info(
                    "Incremental: %d deleted files detected", len(deleted_paths)
                )
                await asyncio.to_thread(scan_cache.remove_deleted, deleted_paths)

        await _send(
            {
                "type": "scan_completed",
                "path": path,
                "filesFound": files_found,
                "filesHashed": files_hashed,
                "filesUnchanged": files_unchanged,
                "incremental": incremental,
                "deleted": deleted_paths,
            }
        )

    except Exception as e:
        logger.error("Scan failed with exception: %s", e, exc_info=True)
        await _send(
            {
                "type": "scan_error",
                "path": path,
                "error": str(e),
            }
        )

    finally:
        logger.info(
            "Scan finished for: %s (found=%d, hashed=%d, unchanged=%d)",
            path,
            files_found,
            files_hashed,
            files_unchanged,
        )
        await asyncio.to_thread(scan_cache.close)
        _scanning = False
        _current_path = ""
