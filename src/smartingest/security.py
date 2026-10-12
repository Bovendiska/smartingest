"""Security helpers: upload checks, access tokens, rate limiting.

Kept free of web-framework imports so every rule here is unit-testable.
"""

from __future__ import annotations

import hashlib
import hmac
import io
import secrets
import threading
import time
import zipfile
from collections import deque
from dataclasses import dataclass
from pathlib import PurePath


class UploadRejected(ValueError):
    """Raised with a message that is safe to show to the uploader."""


@dataclass(frozen=True)
class UploadLimits:
    max_bytes: int
    max_rows: int
    max_columns: int
    max_cell_chars: int = 2_000
    max_xlsx_uncompressed_bytes: int = 50 * 1024 * 1024
    max_xlsx_compression_ratio: int = 100


ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".json"}
XLSX_MAGIC = b"PK\x03\x04"


def safe_extension(filename: str | None) -> str:
    """Only the extension of the uploaded name is used; the rest is never trusted."""
    ext = PurePath(filename or "").suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise UploadRejected(f"File type not allowed. Use one of: {', '.join(sorted(ALLOWED_EXTENSIONS))}")
    return ext


def check_file_content(content: bytes, ext: str, limits: UploadLimits) -> None:
    """Check size and that the bytes really are what the extension claims."""
    if not content:
        raise UploadRejected("File is empty")
    if len(content) > limits.max_bytes:
        raise UploadRejected(f"File is larger than {limits.max_bytes // (1024 * 1024)} MB")

    if ext == ".xlsx":
        if not content.startswith(XLSX_MAGIC):
            raise UploadRejected("File is not a valid .xlsx workbook")
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                infos = archive.infolist()
        except zipfile.BadZipFile as exc:
            raise UploadRejected("File is not a valid .xlsx workbook") from exc
        if not any(i.filename == "[Content_Types].xml" for i in infos):
            raise UploadRejected("File is not a valid .xlsx workbook")
        # Zip-bomb guard: a tiny file must not expand into gigabytes when opened.
        total = sum(i.file_size for i in infos)
        packed = max(sum(i.compress_size for i in infos), 1)
        if total > limits.max_xlsx_uncompressed_bytes or total / packed > limits.max_xlsx_compression_ratio:
            raise UploadRejected("Workbook expands to an unsafe size")
        return

    # CSV / JSON must be UTF-8 text without binary content.
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise UploadRejected("File must be UTF-8 text") from exc
    if "\x00" in text:
        raise UploadRejected("File contains binary data")
    if ext == ".json" and text.lstrip()[:1] not in ("[", "{"):
        raise UploadRejected("JSON must be an array of records or an object")


def check_table_shape(n_rows: int, n_columns: int, longest_cell: int, limits: UploadLimits) -> None:
    if n_rows == 0:
        raise UploadRejected("File has no rows")
    if n_rows > limits.max_rows:
        raise UploadRejected(f"Too many rows: {n_rows} (limit {limits.max_rows})")
    if n_columns > limits.max_columns:
        raise UploadRejected(f"Too many columns: {n_columns} (limit {limits.max_columns})")
    if longest_cell > limits.max_cell_chars:
        raise UploadRejected(f"A cell is longer than {limits.max_cell_chars} characters")


# --- Tokens -------------------------------------------------------------------------


def new_token() -> str:
    return secrets.token_urlsafe(32)


def hash_token(token: str) -> str:
    """Only the hash is stored, so a database leak does not leak usable tokens."""
    return hashlib.sha256(token.encode()).hexdigest()


def same_secret(given: str | None, expected: str | None) -> bool:
    if not given or not expected:
        return False
    return hmac.compare_digest(given.encode(), expected.encode())


# --- Rate limiting ----------------------------------------------------------------------


class RateLimiter:
    """Sliding-window limiter kept in memory (fine for a single API process)."""

    def __init__(self, max_events: int, window_seconds: float) -> None:
        self.max_events = max_events
        self.window = window_seconds
        self._events: dict[str, deque[float]] = {}
        self._lock = threading.Lock()

    def allow(self, key: str, now: float | None = None) -> bool:
        now = time.monotonic() if now is None else now
        with self._lock:
            events = self._events.setdefault(key, deque())
            while events and now - events[0] >= self.window:
                events.popleft()
            if len(events) >= self.max_events:
                return False
            events.append(now)
            if len(self._events) > 50_000:  # bound memory under a flood of distinct keys
                self._events = {k: v for k, v in self._events.items() if v and now - v[-1] < self.window}
            return True
