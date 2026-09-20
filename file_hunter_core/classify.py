"""File type classification and size formatting."""

# Extension to type mapping
_EXT_MAP = {
    # Images
    "jpg": ("image", "jpg"),
    "jpeg": ("image", "jpg"),
    "png": ("image", "png"),
    "gif": ("image", "gif"),
    "bmp": ("image", "bmp"),
    "heic": ("image", "heic"),
    "heif": ("image", "heif"),
    "webp": ("image", "webp"),
    "svg": ("image", "svg"),
    "tiff": ("image", "tiff"),
    "tif": ("image", "tiff"),
    "nef": ("image", "nef"),
    "cr2": ("image", "cr2"),
    "cr3": ("image", "cr3"),
    "arw": ("image", "arw"),
    "orf": ("image", "orf"),
    "raf": ("image", "raf"),
    "dng": ("image", "dng"),
    "rw2": ("image", "rw2"),
    "pef": ("image", "pef"),
    "srw": ("image", "srw"),
    "nrw": ("image", "nrw"),
    "raw": ("image", "raw"),
    # Video
    "mp4": ("video", "mp4"),
    "avi": ("video", "avi"),
    "mov": ("video", "mov"),
    "mkv": ("video", "mkv"),
    "wmv": ("video", "wmv"),
    "flv": ("video", "flv"),
    "webm": ("video", "webm"),
    "mpg": ("video", "mpeg"),
    "mpeg": ("video", "mpeg"),
    "mts": ("video", "mts"),
    "m2ts": ("video", "m2ts"),
    "m4v": ("video", "m4v"),
    "3gp": ("video", "3gp"),
    "vob": ("video", "vob"),
    "ts": ("video", "ts"),
    # Audio
    "mp3": ("audio", "mp3"),
    "flac": ("audio", "flac"),
    "wav": ("audio", "wav"),
    "aac": ("audio", "aac"),
    "ogg": ("audio", "ogg"),
    "wma": ("audio", "wma"),
    "m4a": ("audio", "m4a"),
    "aif": ("audio", "aiff"),
    "aiff": ("audio", "aiff"),
    "mid": ("audio", "midi"),
    "midi": ("audio", "midi"),
    "alac": ("audio", "alac"),
    "caf": ("audio", "caf"),
    "ra": ("audio", "ra"),
    "ram": ("audio", "ra"),
    "ac3": ("audio", "ac3"),
    "dts": ("audio", "dts"),
    "au": ("audio", "au"),
    "snd": ("audio", "snd"),
    # Documents
    "pdf": ("document", "pdf"),
    "doc": ("document", "doc"),
    "docx": ("document", "docx"),
    "xls": ("document", "xls"),
    "xlsx": ("document", "xlsx"),
    "ppt": ("document", "ppt"),
    "pptx": ("document", "pptx"),
    "odt": ("document", "odt"),
    "ods": ("document", "ods"),
    # Text
    "txt": ("text", "txt"),
    "md": ("text", "md"),
    "csv": ("text", "csv"),
    "json": ("text", "json"),
    "xml": ("text", "xml"),
    "html": ("text", "html"),
    "css": ("text", "css"),
    "js": ("text", "js"),
    "py": ("text", "py"),
    "log": ("text", "log"),
    # Compressed
    "zip": ("compressed", "zip"),
    "gz": ("compressed", "gz"),
    "bz2": ("compressed", "bz2"),
    "xz": ("compressed", "xz"),
    "tar": ("compressed", "tar"),
    "7z": ("compressed", "7z"),
    "rar": ("compressed", "rar"),
    "tgz": ("compressed", "tgz"),
    "zst": ("compressed", "zst"),
    "lz4": ("compressed", "lz4"),
    "cab": ("compressed", "cab"),
    "iso": ("compressed", "iso"),
    "dmg": ("compressed", "dmg"),
    # Font
    "ttf": ("font", "ttf"),
    "otf": ("font", "otf"),
    "woff": ("font", "woff"),
    "woff2": ("font", "woff2"),
    "eot": ("font", "eot"),
}


def classify_file(filename: str) -> tuple[str, str]:
    """Return (type_high, type_low) for a filename based on its extension."""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    result = _EXT_MAP.get(ext)
    if result:
        return result
    if filename.startswith("."):
        return ("text", "dotfile")
    return ("other", ext or "unknown")


def format_size(size_bytes: int) -> str:
    """Format a byte count as a human-readable string."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    if size_bytes < 1048576:
        return f"{size_bytes / 1024:.1f} KB"
    if size_bytes < 1073741824:
        return f"{size_bytes / 1048576:.1f} MB"
    if size_bytes < 1099511627776:
        return f"{size_bytes / 1073741824:.1f} GB"
    if size_bytes < 1125899906842624:
        return f"{size_bytes / 1099511627776:.1f} TB"
    return f"{size_bytes / 1125899906842624:.1f} PB"
