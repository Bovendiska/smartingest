"""File parsing and column-name sanitising."""

from __future__ import annotations

import io
import json
import math
import re
import unicodedata
from pathlib import Path

import pandas as pd

SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".json"}  # legacy .xls needs an extra parser; not accepted
MAX_COLUMN_NAME_LENGTH = 63  # PostgreSQL identifier limit


class UnsupportedFileError(ValueError):
    pass


def slugify_column(name: str) -> str:
    """Turn any header into a safe SQL identifier: [a-z0-9_], not starting with a digit."""
    text = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode()
    text = re.sub(r"[^a-zA-Z0-9]+", "_", text).strip("_").lower()
    if not text:
        text = "column"
    if text[0].isdigit():
        text = f"c_{text}"
    if text.startswith("_"):
        text = f"c{text}"
    return text[:MAX_COLUMN_NAME_LENGTH]


def unique_column_names(names: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    result: list[str] = []
    for raw in names:
        slug = slugify_column(raw)
        count = seen.get(slug, 0)
        seen[slug] = count + 1
        result.append(slug if count == 0 else f"{slug}_{count + 1}"[:MAX_COLUMN_NAME_LENGTH])
    return result


def read_table(content: bytes, filename: str) -> pd.DataFrame:
    """Parse CSV / Excel / JSON into a DataFrame where every cell is ``str`` or ``None``.

    Everything is read as text on purpose: type inference is our job, not pandas'.
    Leading zeros in phone numbers and IDs survive this way.
    """
    ext = Path(filename).suffix.lower()
    if ext not in SUPPORTED_EXTENSIONS:
        raise UnsupportedFileError(f"Unsupported file type '{ext}'. Use one of {sorted(SUPPORTED_EXTENSIONS)}")

    if ext == ".csv":
        df = pd.read_csv(io.BytesIO(content), dtype=str, keep_default_na=False, sep=None, engine="python")
    elif ext == ".xlsx":
        df = pd.read_excel(io.BytesIO(content), dtype=str, keep_default_na=False)
    else:
        payload = json.loads(content.decode("utf-8-sig"))
        if isinstance(payload, dict):
            # Accept {"data": [...]} or a single record.
            payload = next((v for v in payload.values() if isinstance(v, list)), [payload])
        df = pd.json_normalize(payload, sep="_")

    # Normalise every cell to str | None (pandas may hand back NaN, numbers or StringDtype).
    original_columns = [str(c) for c in df.columns]
    records = [[_to_cell(v) for v in row] for row in df.itertuples(index=False, name=None)]
    result = pd.DataFrame(records, columns=unique_column_names(original_columns), dtype=object)
    result.attrs["original_columns"] = original_columns
    return result


def _to_cell(value: object) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)) or value is pd.NA:
        return None
    text = str(value)
    return text if text != "" else None
