"""Shared data models for the SmartIngest pipeline."""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class SemanticType(str, Enum):
    EMAIL = "email"
    PHONE_ID = "phone_id"
    DATE = "date"
    CURRENCY_IDR = "currency_idr"
    INTEGER = "integer"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    TEXT = "text"


class Confidence(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    NONE = "none"


class IssueStatus(str, Enum):
    OPEN = "open"
    AUTO_FIXED = "auto_fixed"
    ACCEPTED = "accepted"
    EDITED = "edited"
    VALUE_DROPPED = "value_dropped"
    ROW_DROPPED = "row_dropped"


ROW_LEVEL_COLUMN = "_row"


class ColumnProfile(BaseModel):
    original_name: str
    column_name: str
    semantic_type: SemanticType
    type_confidence: float = Field(ge=0, le=1)
    type_source: str = "rule"  # "rule" | "name_hint" | "llm"
    null_ratio: float
    match_ratio: float
    required: bool
    distinct_count: int
    top_values: dict[str, int] = Field(default_factory=dict)


class Issue(BaseModel):
    row_index: int
    column_name: str
    rule: str
    original_value: str | None
    suggestion: str | None = None
    reason: str = ""
    confidence: Confidence = Confidence.NONE
    source: str = "rule"  # "rule" | "llm"
    status: IssueStatus = IssueStatus.OPEN
    final_value: str | None = None  # set when a reviewer edits the value by hand


class PipelineReport(BaseModel):
    rows_total: int
    rows_clean: int
    rows_auto_fixed: int
    rows_quarantined: int
    issues_total: int
    issues_auto_fixed: int
    issues_open: int
    issues_by_rule: dict[str, int]
    quality_score: float
