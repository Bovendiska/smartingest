"""Column profiling and semantic type inference."""

from __future__ import annotations

from collections import Counter

import pandas as pd

from smartingest.detectors import is_blank, looks_like, name_hint
from smartingest.models import ColumnProfile, SemanticType

# Order matters for ties: more specific types first.
CANDIDATE_TYPES: tuple[SemanticType, ...] = (
    SemanticType.EMAIL,
    SemanticType.PHONE_ID,
    SemanticType.DATE,
    SemanticType.BOOLEAN,
    SemanticType.INTEGER,
    SemanticType.DECIMAL,
    SemanticType.CURRENCY_IDR,
)

MIN_MATCH_RATIO = 0.6        # evidence needed from values alone
MIN_HINTED_MATCH_RATIO = 0.4  # lower bar when the column name agrees
REQUIRED_MAX_NULL_RATIO = 0.1


def infer_type(column_name: str, values: list[str]) -> tuple[SemanticType, float, float, str]:
    """Return (type, confidence, match_ratio, source) for one column."""
    if not values:
        return SemanticType.TEXT, 0.0, 0.0, "rule"

    ratios = {t: sum(looks_like(v, t) for v in values) / len(values) for t in CANDIDATE_TYPES}
    hint = name_hint(column_name)

    if hint is not None and ratios.get(hint, 0) >= MIN_HINTED_MATCH_RATIO:
        ratio = ratios[hint]
        return hint, round(min(1.0, ratio + 0.15), 3), round(ratio, 3), "name_hint"

    best = max(CANDIDATE_TYPES, key=lambda t: ratios[t])  # max keeps first on ties
    if ratios[best] >= MIN_MATCH_RATIO:
        return best, round(ratios[best], 3), round(ratios[best], 3), "rule"

    return SemanticType.TEXT, round(1 - ratios[best], 3), 1.0, "rule"


def profile_dataframe(df: pd.DataFrame) -> list[ColumnProfile]:
    original_names = df.attrs.get("original_columns", list(df.columns))
    profiles: list[ColumnProfile] = []
    n_rows = max(len(df), 1)

    for original, column in zip(original_names, df.columns, strict=True):
        series = df[column]
        non_blank = [str(v) for v in series if not is_blank(v)]
        null_ratio = 1 - len(non_blank) / n_rows
        semantic_type, confidence, match_ratio, source = infer_type(column, non_blank)
        counts = Counter(v.strip().lower() for v in non_blank)
        profiles.append(
            ColumnProfile(
                original_name=original,
                column_name=column,
                semantic_type=semantic_type,
                type_confidence=confidence,
                type_source=source,
                null_ratio=round(null_ratio, 4),
                match_ratio=match_ratio,
                required=null_ratio <= REQUIRED_MAX_NULL_RATIO,
                distinct_count=len(counts),
                top_values=dict(counts.most_common(5)),
            )
        )
    return profiles
