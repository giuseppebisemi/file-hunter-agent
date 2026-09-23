"""Directory walker — synchronous scan of a single directory.

Generator yielding subdirectory paths and file metadata one at a time.
No database, no async. The caller handles DB upsert separately.
"""

import os
import stat

from file_hunter_core.classify import classify_file
from file_hunter_core.paths import safe_str, safe_timestamp


def scan_directory(dirpath: str, root_path: str, parent_hidden: bool = False):
    """Scan a single directory. Yields ("dir", path) or ("file", info_dict).

    Subdirectories yield as ("dir", full_path).
    Files yield as ("file", metadata_dict).

    Dotfiles/dotfolders are included with hidden=1. Files inside a hidden
    parent directory inherit hidden status.
    """
    try:
        scandir_it = os.scandir(dirpath)
    except (PermissionError, OSError):
        return

    rel_dir = safe_str(os.path.relpath(dirpath, root_path).replace(os.sep, "/"))
    if rel_dir == ".":
        rel_dir = ""

    for entry in scandir_it:
        try:
            if entry.is_symlink():
                continue
            if entry.is_dir(follow_symlinks=False):
                yield ("dir", entry.path)
                continue
            st = entry.stat(follow_symlinks=False)
        except OSError:
            continue

        if not stat.S_ISREG(st.st_mode):
            continue

        name = safe_str(entry.name)
        hidden = parent_hidden or name.startswith(".")
        rel_path = f"{rel_dir}/{name}" if rel_dir else name
        type_high, type_low = classify_file(name)

        yield (
            "file",
            {
                "filename": name,
                "full_path": safe_str(entry.path),
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
            },
        )
