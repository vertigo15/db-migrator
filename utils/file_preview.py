"""Bounded file helpers for Streamlit pages.

Large migration artifacts must never be read into memory merely because a page
reran. These helpers make the size limits explicit and are intentionally free
of Streamlit dependencies so they are easy to regression-test.
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


SQL_PREVIEW_BYTES = 50_000
INLINE_DOWNLOAD_BYTES = 10 * 1024 * 1024


def read_text_preview(
    path: str,
    max_bytes: int = SQL_PREVIEW_BYTES,
) -> Tuple[str, bool]:
    """Read at most ``max_bytes`` and report whether content was truncated."""
    if max_bytes <= 0:
        raise ValueError("max_bytes must be positive")

    file_size = os.path.getsize(path)
    with open(path, "rb") as handle:
        raw = handle.read(max_bytes)
    text = raw.decode("utf-8", errors="replace")
    return text, file_size > len(raw)


def read_inline_download(
    path: str,
    max_bytes: int = INLINE_DOWNLOAD_BYTES,
) -> Optional[bytes]:
    """Return small file contents; large files deliberately remain server-side."""
    if os.path.getsize(path) > max_bytes:
        return None
    return Path(path).read_bytes()


def human_file_size(size: int) -> str:
    """Format a byte size without reading the file."""
    value = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{size} B"


def migration_file_metadata(
    file_paths: Iterable[str],
    database_by_prefix: Dict[str, str],
) -> List[dict]:
    """Describe only explicitly supplied migration paths."""
    metadata = []
    for path in sorted(
        {
            os.path.abspath(str(path))
            for path in file_paths
            if path and str(path).endswith(".sql") and os.path.isfile(path)
        }
    ):
        filename = os.path.basename(path)
        target_db = next(
            (
                database
                for prefix, database in database_by_prefix.items()
                if filename.startswith(prefix)
            ),
            "user_db",
        )
        metadata.append(
            {
                "path": path,
                "filename": filename,
                "target_db": target_db,
                "size": os.path.getsize(path),
                "modified": datetime.fromtimestamp(os.path.getmtime(path)),
            }
        )
    return metadata
