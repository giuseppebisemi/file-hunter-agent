"""Camera raw to JPEG conversion via dcraw + ImageMagick (or PPM fallback).

Detects dcraw at startup. Converts raw files (NEF, CR2, ARW, etc.) to
full-resolution JPEGs. Output is written to a temp dir first, then moved
alongside the source — same pattern as transcode.
"""

import asyncio
import logging
import os
import shutil
import tempfile

logger = logging.getLogger("file_hunter_agent")

# State
_task: asyncio.Task | None = None
_send_fn = None
_current_path: str | None = None

# Detected at startup
_available = False
_has_convert = False  # ImageMagick convert/magick

# Camera raw extensions that dcraw supports
RAW_EXTENSIONS = {
    "nef", "cr2", "cr3", "arw", "orf", "raf", "dng", "rw2",
    "pef", "srw", "nrw", "raw", "mrw", "dcr", "kdc", "erf",
    "3fr", "mef", "mos", "iiq",
}


def set_send_fn(fn):
    """Register the WebSocket send function for progress updates."""
    global _send_fn
    _send_fn = fn


def detect():
    """Check for dcraw and ImageMagick at startup."""
    global _available, _has_convert
    _available = shutil.which("dcraw") is not None
    # ImageMagick: try 'magick' (v7) then 'convert' (v6)
    _has_convert = (
        shutil.which("magick") is not None
        or shutil.which("convert") is not None
    )
    if _available:
        logger.info(
            "Raw convert: dcraw available%s",
            ", ImageMagick available" if _has_convert else ", no ImageMagick (PPM fallback)",
        )


def is_available() -> bool:
    return _available


def is_converting() -> bool:
    return _task is not None and not _task.done()


def is_raw_extension(ext: str) -> bool:
    """Check if a file extension is a supported camera raw format."""
    return ext.lower().lstrip(".") in RAW_EXTENSIONS


async def _send(msg: dict):
    if _send_fn:
        await _send_fn(msg)


def _output_path(path: str) -> str:
    """Build the output JPEG path, avoiding collisions."""
    name, _ = os.path.splitext(path)
    candidate = f"{name}.jpg"
    if not os.path.exists(candidate):
        return candidate
    n = 2
    while True:
        candidate = f"{name}-{n}.jpg"
        if not os.path.exists(candidate):
            return candidate
        n += 1


async def start_convert(path: str) -> bool:
    """Start a raw conversion. Returns False if already running."""
    global _task, _current_path

    if is_converting():
        return False

    _current_path = path
    _task = asyncio.create_task(_run_convert(path))
    return True


async def _run_convert(path: str):
    """Run dcraw to convert a raw file to JPEG."""
    global _current_path

    tmp_dir = None
    try:
        final_path = _output_path(path)
        filename = os.path.basename(final_path)

        # Check disk space — raw files are large, JPEGs much smaller,
        # but be safe and require at least the source file size free
        source_size = os.path.getsize(path)
        stat_fs = os.statvfs(os.path.dirname(path))
        free_bytes = stat_fs.f_bavail * stat_fs.f_frsize
        if free_bytes < source_size:
            await _send({
                "type": "rawconvert_error",
                "path": path,
                "error": "Insufficient disk space for conversion.",
            })
            return

        tmp_dir = tempfile.mkdtemp(prefix="fh-rawconvert-")

        await _send({
            "type": "rawconvert_progress",
            "path": path,
            "output": final_path,
            "status": "converting",
        })

        # dcraw -c -w outputs PPM to stdout with white balance applied
        # Pipe through ImageMagick to get JPEG, or save as PPM and convert
        tmp_output = os.path.join(tmp_dir, filename)

        if _has_convert:
            # dcraw -> pipe -> ImageMagick -> JPEG
            convert_cmd = "magick" if shutil.which("magick") else "convert"
            cmd = f'dcraw -c -w "{path}" | {convert_cmd} ppm:- -quality 92 "{tmp_output}"'
            proc = await asyncio.create_subprocess_shell(
                cmd,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()
        else:
            # No ImageMagick — dcraw can write TIFF with -T, but not JPEG.
            # Write PPM then we'd need something to convert it.
            # Fall back to dcraw -T for TIFF (browsers can't display but
            # it's full resolution). This path shouldn't normally be hit
            # since ImageMagick is widely available.
            tmp_tiff = os.path.join(tmp_dir, os.path.splitext(filename)[0] + ".tiff")
            tmp_output = tmp_tiff
            final_path = os.path.splitext(final_path)[0] + ".tiff"
            filename = os.path.basename(final_path)
            proc = await asyncio.create_subprocess_exec(
                "dcraw", "-T", "-w", "-o", "1", path,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
                cwd=tmp_dir,
            )
            _, stderr = await proc.communicate()
            # dcraw -T writes output next to the source, not in cwd
            # It creates <name>.tiff next to the input file
            dcraw_output = os.path.splitext(path)[0] + ".tiff"
            if os.path.exists(dcraw_output):
                await asyncio.to_thread(shutil.move, dcraw_output, tmp_output)

        if proc.returncode != 0:
            error_msg = stderr.decode("utf-8", errors="replace")[-500:] if stderr else "dcraw failed"
            logger.error("Raw convert failed (rc=%d): %s", proc.returncode, error_msg)
            await _send({
                "type": "rawconvert_error",
                "path": path,
                "error": f"dcraw exited with code {proc.returncode}",
            })
            return

        if not os.path.exists(tmp_output):
            await _send({
                "type": "rawconvert_error",
                "path": path,
                "error": "Conversion produced no output file.",
            })
            return

        # Move to final location
        await asyncio.to_thread(shutil.move, tmp_output, final_path)

        st = os.stat(final_path)
        await _send({
            "type": "rawconvert_complete",
            "path": path,
            "output": final_path,
            "filename": filename,
            "size": st.st_size,
            "mtime": st.st_mtime,
            "ctime": st.st_ctime,
            "inode": st.st_ino,
        })
        logger.info("Raw convert complete: %s -> %s", path, final_path)

    except Exception as exc:
        logger.exception("Raw convert failed: %s", exc)
        await _send({
            "type": "rawconvert_error",
            "path": path,
            "error": str(exc),
        })
    finally:
        _current_path = None
        if tmp_dir and os.path.isdir(tmp_dir):
            shutil.rmtree(tmp_dir, ignore_errors=True)
