"""Value detectors.

Two layers on purpose:
- ``looks_like_*`` (loose): used for type inference, tolerant of dirty values
  so that ``reindra@`` still counts as evidence for an email column.
- ``is_canonical_*`` (strict): used for validation; a value must already be in
  the canonical storage form to pass.
"""

from __future__ import annotations

import re
from datetime import date

from smartingest.models import SemanticType

# --- Canonical (strict) patterns ------------------------------------------------

EMAIL_RE = re.compile(r"^[a-z0-9._%+-]+@[a-z0-9-]+(\.[a-z0-9-]+)*\.[a-z]{2,}$")
PHONE_ID_RE = re.compile(r"^\+628\d{7,11}$")
ISO_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
INTEGER_RE = re.compile(r"^-?(0|[1-9]\d*)$")
DECIMAL_RE = re.compile(r"^-?(0|[1-9]\d*)(\.\d+)?$")
BOOLEAN_CANONICAL = {"true", "false"}

# --- Loose patterns -------------------------------------------------------------

LOOSE_DATE_RE = re.compile(r"^\s*\d{1,4}[-/.]\d{1,2}[-/.]\d{1,4}\s*$")
LOOSE_NUMBER_RE = re.compile(r"^\s*(rp\.?|idr)?\s*-?[\d.,\s]+\s*$", re.IGNORECASE)
BOOLEAN_WORDS = {
    "true", "false", "yes", "no", "y", "n", "ya", "tidak", "t", "f", "1", "0",
}

NAME_HINTS: dict[SemanticType, tuple[str, ...]] = {
    SemanticType.EMAIL: ("email", "e_mail", "mail", "surel"),
    SemanticType.PHONE_ID: ("phone", "hp", "telp", "telepon", "whatsapp", "wa", "mobile", "no_hp"),
    SemanticType.DATE: ("date", "tgl", "tanggal", "lahir", "dob", "created", "updated"),
    SemanticType.CURRENCY_IDR: ("total", "harga", "price", "amount", "belanja", "gaji", "biaya", "nominal"),
    SemanticType.BOOLEAN: ("is_", "member", "aktif", "active", "flag", "status_member"),
}


def is_blank(value: object) -> bool:
    if value is None or (isinstance(value, float) and value != value):  # None or NaN
        return True
    return str(value).strip() == ""


def digits_only(value: str) -> str:
    return re.sub(r"\D", "", value)


def valid_calendar_date(year: int, month: int, day: int) -> bool:
    try:
        date(year, month, day)
    except ValueError:
        return False
    return 1900 <= year <= 2100


def looks_like(value: str, semantic_type: SemanticType) -> bool:
    v = value.strip()
    if not v:
        return False
    if semantic_type is SemanticType.EMAIL:
        return "@" in v or bool(re.search(r"\.(com|co\.id|id|net|org)$", v, re.IGNORECASE))
    if semantic_type is SemanticType.PHONE_ID:
        d = digits_only(v)
        return 5 <= len(d) <= 15 and bool(re.match(r"^\s*(\+?62|0|8)", v)) and not LOOSE_DATE_RE.match(v)
    if semantic_type is SemanticType.DATE:
        return bool(LOOSE_DATE_RE.match(v))
    if semantic_type is SemanticType.BOOLEAN:
        return v.lower() in BOOLEAN_WORDS
    if semantic_type is SemanticType.CURRENCY_IDR:
        return bool(re.match(r"^\s*(rp\.?|idr)", v, re.IGNORECASE)) or (
            bool(LOOSE_NUMBER_RE.match(v)) and not v.startswith("0")
        )
    if semantic_type is SemanticType.INTEGER:
        return bool(INTEGER_RE.match(v))
    if semantic_type is SemanticType.DECIMAL:
        return bool(re.match(r"^-?\d+([.,]\d+)?$", v))
    return True  # TEXT matches anything


def is_canonical(value: str, semantic_type: SemanticType) -> bool:
    if semantic_type is SemanticType.EMAIL:
        return bool(EMAIL_RE.match(value))
    if semantic_type is SemanticType.PHONE_ID:
        return bool(PHONE_ID_RE.match(value))
    if semantic_type is SemanticType.DATE:
        m = ISO_DATE_RE.match(value)
        return bool(m) and valid_calendar_date(*(int(g) for g in m.groups()))
    if semantic_type in (SemanticType.INTEGER, SemanticType.CURRENCY_IDR):
        return bool(INTEGER_RE.match(value))
    if semantic_type is SemanticType.DECIMAL:
        return bool(DECIMAL_RE.match(value))
    if semantic_type is SemanticType.BOOLEAN:
        return value in BOOLEAN_CANONICAL
    # TEXT: canonical when trimmed and without repeated inner whitespace
    return value == re.sub(r"\s+", " ", value).strip()


def name_hint(column_name: str) -> SemanticType | None:
    """Guess a semantic type from a slugified column name."""
    padded = f"_{column_name}_"
    for semantic_type, keywords in NAME_HINTS.items():
        for kw in keywords:
            if kw.endswith("_") and column_name.startswith(kw):
                return semantic_type
            if f"_{kw}_" in padded:
                return semantic_type
    return None
