"""Shallow directory listing — files and folders at a single path, non-recursive."""

import asyncio
import os

from starlette.requests import Request

from file_hunter_agent.config import is_path_allowed
from file_hunter_agent.response import json_ok, json_error
from file_hunter_core.paths import safe_str

_FORBIDDEN = "Path is not within a configured location."


def _list_path(path):
    """List a single directory. Returns (folders, files). Runs in thread."""
    folders = []
    files = []
    with os.scandir(path) as it:
        for entry in it:
            try:
                st = entry.stat()
                name = safe_str(entry.name)
                if entry.is_dir(follow_symlinks=False):
                    folders.append({
                        "name": name,
                        "mtime": st.st_mtime,
                    })
                elif entry.is_file(follow_symlinks=False):
                    files.append({
                        "name": name,
                        "size": st.st_size,
                        "mtime": st.st_mtime,
                        "ctime": st.st_birthtime if hasattr(st, "st_birthtime") else st.st_ctime,
                        "inode": st.st_ino,
                    })
            except (PermissionError, OSError):
                continue
    return folders, files


async def list_dir(request: Request):
    """POST /list-dir — shallow listing of a single directory."""
    body = await request.json()
    path = body.get("path", "")
    if not path:
        return json_error("path is required.")
    if not is_path_allowed(path):
        return json_error(_FORBIDDEN, status=403)

    exists = await asyncio.to_thread(os.path.isdir, path)
    if not exists:
        return json_error("Directory not found.", status=404)

    folders, files = await asyncio.to_thread(_list_path, path)
    return json_ok({
        "path": path,
        "folders": folders,
        "files": files,
    })
