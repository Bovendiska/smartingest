from __future__ import annotations

import io
import sys
import zipfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from smartingest.config import Settings  # noqa: E402
from smartingest.security import (  # noqa: E402
    RateLimiter,
    UploadLimits,
    UploadRejected,
    check_file_content,
    check_table_shape,
    hash_token,
    new_token,
    safe_extension,
    same_secret,
)
from smartingest.storage_names import silver_table_for  # noqa: E402

LIMITS = UploadLimits(max_bytes=1024 * 1024, max_rows=100, max_columns=10)


def xlsx_bytes(payload: bytes = b"<x/>") -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", "<Types/>")
        z.writestr("xl/workbook.xml", payload)
    return buffer.getvalue()


def test_only_known_extensions_are_allowed() -> None:
    assert safe_extension("Data Pelanggan.XLSX") == ".xlsx"
    assert safe_extension("../../etc/passwd.csv") == ".csv"
    for bad in ("evil.exe", "report.xls", "noext", None, "a.csv.php"):
        with pytest.raises(UploadRejected):
            safe_extension(bad)


def test_content_must_match_extension() -> None:
    check_file_content(b"a,b\n1,2\n", ".csv", LIMITS)
    check_file_content(b'[{"a": 1}]', ".json", LIMITS)
    check_file_content(xlsx_bytes(), ".xlsx", LIMITS)
    cases = [
        (b"MZ\x90\x00 binary", ".xlsx"),        # an .exe renamed to .xlsx
        (b"a,b\n\x00\x01", ".csv"),             # binary pretending to be csv
        (b"\xff\xfe\x00", ".csv"),              # not UTF-8
        (b"hello", ".json"),                    # not a JSON document
        (b"", ".csv"),                          # empty
        (b"x" * (LIMITS.max_bytes + 1), ".csv"),  # too big
    ]
    for content, ext in cases:
        with pytest.raises(UploadRejected):
            check_file_content(content, ext, LIMITS)


def test_zip_bomb_is_rejected() -> None:
    bomb = xlsx_bytes(b"0" * (60 * 1024 * 1024))  # ~60 MB of zeros compresses to ~60 KB
    assert len(bomb) < LIMITS.max_bytes
    with pytest.raises(UploadRejected, match="unsafe size"):
        check_file_content(bomb, ".xlsx", LIMITS)


def test_table_shape_limits() -> None:
    check_table_shape(100, 10, 50, LIMITS)
    for rows, cols, cell in ((0, 1, 1), (101, 1, 1), (1, 11, 1), (1, 1, 2_001)):
        with pytest.raises(UploadRejected):
            check_table_shape(rows, cols, cell, LIMITS)


def test_tokens_and_secrets() -> None:
    a, b = new_token(), new_token()
    assert a != b and len(a) >= 40
    assert hash_token(a) == hash_token(a) != hash_token(b)
    assert same_secret("x" * 40, "x" * 40)
    assert not same_secret("x" * 40, "y" * 40)
    assert not same_secret(None, "x") and not same_secret("x", None) and not same_secret("", "")


def test_rate_limiter_window() -> None:
    limiter = RateLimiter(max_events=3, window_seconds=60)
    assert [limiter.allow("ip", now=t) for t in (0, 1, 2, 3)] == [True, True, True, False]
    assert limiter.allow("other-ip", now=3)
    assert limiter.allow("ip", now=61)


def test_demo_visitors_get_isolated_tables() -> None:
    import uuid

    owner = uuid.UUID("12345678-1234-5678-1234-567812345678")
    assert silver_table_for(None, "Customers") == "customers"
    assert silver_table_for(owner, "Customers") == "demo_123456781234_customers"
    assert silver_table_for(owner, "x" * 100).startswith("demo_") and len(silver_table_for(owner, "x" * 100)) <= 63


def test_unsafe_configuration_is_refused() -> None:
    good = dict(
        database_url="postgresql://smartingest_app:s3cret-strong-pass@postgres/smartingest",
        admin_api_key="k" * 64,
    )
    Settings(**good).check()
    for override in (
        {"admin_api_key": "short"},
        {"database_url": "postgresql://smartingest_app:change-me@postgres/smartingest"},
    ):
        with pytest.raises(RuntimeError):
            Settings(**{**good, **override}).check()


def _settings(**overrides: object) -> Settings:
    base = dict(
        database_url="postgresql://smartingest_app:s3cret-strong-pass@postgres/smartingest",
        admin_api_key="k" * 64,
    )
    return Settings(**{**base, **overrides})


def test_local_bronze_store_round_trip(tmp_path: Path) -> None:
    import uuid

    from smartingest.bronze import BronzeStore

    store = BronzeStore(_settings(bronze_local_path=str(tmp_path)))
    key = store.put("customers", uuid.uuid4(), ".csv", b"a,b\n1,2\n")
    assert (tmp_path / key).read_bytes() == b"a,b\n1,2\n"
    assert key.startswith("customers/") and key.endswith(".csv")
    store.delete([key])
    assert not (tmp_path / key).exists()
    with pytest.raises(ValueError):
        store._local_path("../../etc/passwd")


def test_s3_backend_needs_credentials() -> None:
    with pytest.raises(RuntimeError):
        _settings(bronze_backend="s3").check()
    with pytest.raises(RuntimeError):
        _settings(bronze_backend="ftp").check()
