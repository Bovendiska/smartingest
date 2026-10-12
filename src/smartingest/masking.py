"""PII masking before anything leaves the server.

The LLM only needs the *shape* of a value to reason about it:
``Reindra@gmial.com`` -> ``Aaaaaaa@gmial.com``. Public email domains are kept
because they carry no personal information and are needed for typo fixes.
"""

from __future__ import annotations

import re

from smartingest.models import SemanticType
from smartingest.validators import COMMON_EMAIL_DOMAINS, KNOWN_DOMAIN_TYPOS

PUBLIC_DOMAINS = set(COMMON_EMAIL_DOMAINS) | set(KNOWN_DOMAIN_TYPOS)


def mask_shape(text: str) -> str:
    text = re.sub(r"[a-z]", "a", text)
    text = re.sub(r"[A-Z]", "A", text)
    return re.sub(r"\d", "9", text)


def mask_value(value: str | None) -> str | None:
    if value is None:
        return None
    if "@" in value:
        local, _, domain = value.partition("@")
        domain_clean = domain.strip().lower()
        masked_domain = domain if domain_clean in PUBLIC_DOMAINS or not domain_clean else mask_shape(domain)
        return f"{mask_shape(local)}@{masked_domain}"
    return mask_shape(value)


# Amounts, flags and counts are not personal data; masking them would also hide
# exactly what the LLM needs (e.g. "seratus ribu" -> 100000).
UNMASKED_TYPES = {SemanticType.CURRENCY_IDR, SemanticType.INTEGER, SemanticType.DECIMAL, SemanticType.BOOLEAN}


def mask_for_type(value: str | None, semantic_type: SemanticType) -> str | None:
    if semantic_type in UNMASKED_TYPES:
        return value
    return mask_value(value)
