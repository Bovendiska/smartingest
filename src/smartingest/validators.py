"""Rule-based validation and fix suggestions.

Every fixer receives a raw value and returns ``None`` when the value is already
canonical, or a ``Finding`` describing the problem and (maybe) a suggestion.

Confidence policy:
- high   -> lossless normalisation (trim, case, format). Safe to auto-apply.
- medium -> very likely right, but a human should confirm.
- low    -> an educated guess (e.g. filling a missing email domain).
- none   -> we know it is wrong but cannot propose a value.
"""

from __future__ import annotations

import difflib
import re
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

from smartingest.detectors import (
    EMAIL_RE,
    ISO_DATE_RE,
    digits_only,
    is_canonical,
    valid_calendar_date,
)
from smartingest.models import Confidence, SemanticType

COMMON_EMAIL_DOMAINS = (
    "gmail.com", "yahoo.com", "yahoo.co.id", "outlook.com", "hotmail.com", "icloud.com", "ymail.com",
)
KNOWN_DOMAIN_TYPOS = {
    "gmial.com": "gmail.com", "gmai.com": "gmail.com", "gmail.co": "gmail.com", "gamil.com": "gmail.com",
    "gmail.con": "gmail.com", "yaho.com": "yahoo.com", "yahooo.com": "yahoo.com", "outlok.com": "outlook.com",
    "outllok.com": "outlook.com", "hotmial.com": "hotmail.com", "hotmai.com": "hotmail.com",
}
BOOLEAN_MAP = {
    "true": "true", "t": "true", "yes": "true", "y": "true", "ya": "true", "1": "true",
    "false": "false", "f": "false", "no": "false", "n": "false", "tidak": "false", "0": "false",
}


@dataclass(frozen=True)
class Finding:
    rule: str
    suggestion: str | None
    confidence: Confidence
    reason: str


@dataclass
class ColumnContext:
    """Column-level facts that help fix individual values."""

    top_email_domain: str | None = None
    extra: dict[str, str] = field(default_factory=dict)

    @classmethod
    def build(cls, semantic_type: SemanticType, values: list[str]) -> ColumnContext:
        if semantic_type is not SemanticType.EMAIL:
            return cls()
        domains = Counter(
            v.strip().lower().split("@", 1)[1] for v in values if EMAIL_RE.match(v.strip().lower())
        )
        top = domains.most_common(1)[0][0] if domains else None
        return cls(top_email_domain=top)


# --- Fixers ---------------------------------------------------------------------


def fix_email(value: str, ctx: ColumnContext) -> Finding | None:
    if EMAIL_RE.match(value):
        domain = value.split("@", 1)[1]
        # Syntactically valid is not enough: "gmial.com" is valid syntax but almost surely a typo.
        if domain not in KNOWN_DOMAIN_TYPOS and (
            domain in COMMON_EMAIL_DOMAINS
            or not difflib.get_close_matches(domain, COMMON_EMAIL_DOMAINS, n=1, cutoff=0.85)
        ):
            return None
    v = value.strip().lower()

    if "@" in v:
        local, _, domain = v.partition("@")
        if not domain:
            if ctx.top_email_domain:
                return Finding(
                    "email_missing_domain", f"{local}@{ctx.top_email_domain}", Confidence.LOW,
                    f"Domain kosong.'{ctx.top_email_domain}' adalah domain terbanyak di kolom ini",
                )
            return Finding("email_missing_domain", None, Confidence.NONE, "Domain email kosong")
        if domain in KNOWN_DOMAIN_TYPOS:
            return Finding(
                "email_domain_typo", f"{local}@{KNOWN_DOMAIN_TYPOS[domain]}", Confidence.HIGH,
                f"Typo domain yang dikenal: {domain}",
            )
        close = difflib.get_close_matches(domain, COMMON_EMAIL_DOMAINS, n=1, cutoff=0.85)
        if close and close[0] != domain and EMAIL_RE.match(f"{local}@{close[0]}"):
            return Finding(
                "email_domain_typo", f"{local}@{close[0]}", Confidence.MEDIUM,
                f"Domain '{domain}' mirip '{close[0]}'",
            )
        if EMAIL_RE.match(v):
            return Finding("email_format", v, Confidence.HIGH, "Spasi atau huruf besar dirapikan")
        return Finding("email_invalid", None, Confidence.NONE, "Format email tidak valid")

    # No '@': maybe it was replaced by a dot, e.g. "reindra.gmail.com".
    for domain in COMMON_EMAIL_DOMAINS:
        if v.endswith("." + domain) and len(v) > len(domain) + 1:
            candidate = f"{v[: -len(domain) - 1]}@{domain}"
            if EMAIL_RE.match(candidate):
                return Finding("email_missing_at", candidate, Confidence.MEDIUM, "Tanda '@' hilang")
    return Finding("email_invalid", None, Confidence.NONE, "Tidak ada tanda '@'")


def fix_phone(value: str, ctx: ColumnContext) -> Finding | None:
    if is_canonical(value, SemanticType.PHONE_ID):
        return None
    d = digits_only(value)
    if d.startswith("62"):
        candidate, confidence = f"+{d}", Confidence.HIGH
    elif d.startswith("08"):
        candidate, confidence = f"+62{d[1:]}", Confidence.HIGH
    elif d.startswith("8"):
        candidate, confidence = f"+62{d}", Confidence.MEDIUM
    else:
        return Finding("phone_invalid", None, Confidence.NONE, "Bukan pola nomor HP Indonesia")
    if is_canonical(candidate, SemanticType.PHONE_ID):
        reason = "Format disamakan ke +62" if confidence is Confidence.HIGH else "Angka 0 di depan hilang"
        return Finding("phone_format", candidate, confidence, reason)
    return Finding("phone_invalid", None, Confidence.NONE, f"Jumlah digit tidak wajar ({len(d)})")


def fix_date(value: str, ctx: ColumnContext) -> Finding | None:
    if is_canonical(value, SemanticType.DATE):
        return None
    v = value.strip()
    if ISO_DATE_RE.match(v):
        return Finding("date_impossible", None, Confidence.NONE, "Tanggal tidak ada di kalender")

    parts = re.split(r"[-/.]", v)
    if len(parts) != 3 or not all(p.isdigit() for p in parts):
        return Finding("date_invalid", None, Confidence.NONE, "Format tanggal tidak dikenali")
    a, b, c = (int(p) for p in parts)

    if len(parts[0]) == 4:  # yyyy/mm/dd
        y, m, d = a, b, c
        ambiguous = False
    elif len(parts[2]) == 4:  # dd/mm/yyyy (Indonesian default) or mm/dd/yyyy
        y = c
        if a > 12 and b <= 12:
            d, m, ambiguous = a, b, False
        elif b > 12 and a <= 12:
            m, d, ambiguous = a, b, False
        else:
            d, m, ambiguous = a, b, a != b
    else:
        return Finding("date_invalid", None, Confidence.NONE, "Tahun harus 4 digit")

    if not valid_calendar_date(y, m, d):
        return Finding("date_impossible", None, Confidence.NONE, "Tanggal tidak ada di kalender")
    iso = f"{y:04d}-{m:02d}-{d:02d}"
    if ambiguous:
        return Finding("date_format", iso, Confidence.MEDIUM, "Diasumsikan format dd/mm/yyyy")
    return Finding("date_format", iso, Confidence.HIGH, "Format disamakan ke YYYY-MM-DD")


def _parse_idr_number(value: str) -> Decimal | None:
    v = re.sub(r"(?i)^(rp\.?|idr)", "", value.strip()).strip().replace(" ", "")
    if not v:
        return None
    # Indonesian style: '.' thousands, ',' decimals. Handle '1,500,000' too.
    if re.fullmatch(r"-?\d{1,3}(\.\d{3})+(,\d+)?", v) or re.fullmatch(r"-?\d+,\d{1,2}", v):
        v = v.replace(".", "").replace(",", ".")
    elif re.fullmatch(r"-?\d{1,3}(,\d{3})+(\.\d+)?", v):
        v = v.replace(",", "")
    try:
        return Decimal(v)
    except InvalidOperation:
        return None


def fix_currency(value: str, ctx: ColumnContext) -> Finding | None:
    if is_canonical(value, SemanticType.CURRENCY_IDR):
        return None
    number = _parse_idr_number(value)
    if number is None:
        return Finding("number_invalid", None, Confidence.NONE, "Bukan angka")
    if number != number.to_integral_value():
        return Finding("currency_format", str(number.quantize(Decimal(1))), Confidence.MEDIUM, "Pecahan rupiah dibulatkan")
    return Finding("currency_format", str(int(number)), Confidence.HIGH, "Simbol Rp dan pemisah ribuan dihapus")


def fix_integer(value: str, ctx: ColumnContext) -> Finding | None:
    if is_canonical(value, SemanticType.INTEGER):
        return None
    number = _parse_idr_number(value)
    if number is None or number != number.to_integral_value():
        return Finding("number_invalid", None, Confidence.NONE, "Bukan bilangan bulat")
    return Finding("number_format", str(int(number)), Confidence.HIGH, "Pemisah ribuan dihapus")


def fix_decimal(value: str, ctx: ColumnContext) -> Finding | None:
    if is_canonical(value, SemanticType.DECIMAL):
        return None
    number = _parse_idr_number(value)
    if number is None:
        return Finding("number_invalid", None, Confidence.NONE, "Bukan angka")
    return Finding("number_format", format(number.normalize(), "f"), Confidence.HIGH, "Format angka dirapikan")


def fix_boolean(value: str, ctx: ColumnContext) -> Finding | None:
    if is_canonical(value, SemanticType.BOOLEAN):
        return None
    mapped = BOOLEAN_MAP.get(value.strip().lower())
    if mapped is None:
        return Finding("boolean_invalid", None, Confidence.NONE, "Bukan nilai ya/tidak")
    return Finding("boolean_format", mapped, Confidence.HIGH, "Disamakan ke true/false")


def fix_text(value: str, ctx: ColumnContext) -> Finding | None:
    if is_canonical(value, SemanticType.TEXT):
        return None
    return Finding("text_whitespace", re.sub(r"\s+", " ", value).strip(), Confidence.HIGH, "Spasi berlebih dihapus")


FIXERS = {
    SemanticType.EMAIL: fix_email,
    SemanticType.PHONE_ID: fix_phone,
    SemanticType.DATE: fix_date,
    SemanticType.CURRENCY_IDR: fix_currency,
    SemanticType.INTEGER: fix_integer,
    SemanticType.DECIMAL: fix_decimal,
    SemanticType.BOOLEAN: fix_boolean,
    SemanticType.TEXT: fix_text,
}


def check_value(value: str, semantic_type: SemanticType, ctx: ColumnContext) -> Finding | None:
    return FIXERS[semantic_type](value, ctx)
