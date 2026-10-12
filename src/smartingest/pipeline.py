"""End-to-end validation pipeline (pure Python, no I/O).

parse -> profile -> (LLM column check) -> validate -> auto-fix high-confidence
-> (LLM suggestions for the rest) -> duplicate check -> split clean / quarantine.
"""

from __future__ import annotations

import hashlib
import logging
from collections import Counter
from dataclasses import dataclass, field

import pandas as pd

from smartingest.detectors import is_blank
from smartingest.llm import Advisor, NullAdvisor
from smartingest.models import (
    ROW_LEVEL_COLUMN,
    ColumnProfile,
    Confidence,
    Issue,
    IssueStatus,
    PipelineReport,
)
from smartingest.profiler import profile_dataframe
from smartingest.validators import ColumnContext, check_value

logger = logging.getLogger(__name__)

LLM_RECLASSIFY_BELOW = 0.7     # ask the LLM only about columns we are unsure of
LLM_ACCEPT_TYPE_ABOVE = 0.8    # and only take its answer when it is confident
LLM_FIX_CONFIDENCES = {Confidence.NONE, Confidence.LOW}


@dataclass
class PipelineResult:
    profiles: list[ColumnProfile]
    issues: list[Issue]
    clean: pd.DataFrame          # rows with no open issues, auto-fixes applied
    quarantine: pd.DataFrame     # rows waiting for review, auto-fixes applied
    report: PipelineReport
    fixed: pd.DataFrame = field(repr=False, default_factory=pd.DataFrame)  # all rows, auto-fixes applied


def _row_fingerprint(values: list[str | None]) -> str:
    joined = "\x1f".join("" if is_blank(v) else str(v).strip().lower() for v in values)
    return hashlib.sha256(joined.encode()).hexdigest()


def _reclassify_with_llm(df: pd.DataFrame, profiles: list[ColumnProfile], advisor: Advisor) -> list[ColumnProfile]:
    updated: list[ColumnProfile] = []
    for profile in profiles:
        if profile.type_confidence >= LLM_RECLASSIFY_BELOW:
            updated.append(profile)
            continue
        samples = [str(v) for v in df[profile.column_name] if not is_blank(v)][:8]
        answer = advisor.classify_column(profile, samples)
        if answer and answer.confidence >= LLM_ACCEPT_TYPE_ABOVE and answer.semantic_type != profile.semantic_type:
            logger.info("LLM reclassified %s: %s -> %s", profile.column_name, profile.semantic_type, answer.semantic_type)
            profile = profile.model_copy(
                update={"semantic_type": answer.semantic_type, "type_confidence": answer.confidence, "type_source": "llm"}
            )
        updated.append(profile)
    return updated


def run_pipeline(df: pd.DataFrame, advisor: Advisor | None = None, auto_apply_high: bool = True) -> PipelineResult:
    advisor = advisor or NullAdvisor()
    profiles = _reclassify_with_llm(df, profile_dataframe(df), advisor)
    fixed = df.copy()
    issues: list[Issue] = []

    # 1. Value-level rules
    for profile in profiles:
        column = profile.column_name
        non_blank = [str(v) for v in df[column] if not is_blank(v)]
        ctx = ColumnContext.build(profile.semantic_type, non_blank)
        column_issues: list[Issue] = []

        for row_index, value in df[column].items():
            if is_blank(value):
                if profile.required:
                    column_issues.append(
                        Issue(row_index=int(row_index), column_name=column, rule="missing_required",
                              original_value=None, reason="Kolom wajib diisi kosong")
                    )
                continue
            finding = check_value(str(value), profile.semantic_type, ctx)
            if finding is None:
                continue
            issue = Issue(
                row_index=int(row_index), column_name=column, rule=finding.rule, original_value=str(value),
                suggestion=finding.suggestion, reason=finding.reason, confidence=finding.confidence,
            )
            if auto_apply_high and finding.confidence is Confidence.HIGH and finding.suggestion is not None:
                issue.status = IssueStatus.AUTO_FIXED
                fixed.at[row_index, column] = finding.suggestion
            column_issues.append(issue)

        # 2. LLM suggestions for what rules could not fix (never auto-applied)
        needs_llm = [
            i for i in column_issues
            if i.status is IssueStatus.OPEN and i.original_value is not None and i.confidence in LLM_FIX_CONFIDENCES
        ]
        if needs_llm:
            by_row = {i.row_index: i for i in needs_llm}
            for fix in advisor.suggest_fixes(profile, [(i.row_index, i.original_value, i.rule) for i in needs_llm]):
                target = by_row.get(fix.row_index)
                if target is not None:
                    target.suggestion, target.confidence = fix.suggestion, fix.confidence
                    target.reason, target.source = fix.reason, "llm"
        issues.extend(column_issues)

    # 3. Duplicate rows (checked after normalisation so " A@B.COM" == "a@b.com")
    seen: dict[str, int] = {}
    columns = [p.column_name for p in profiles]
    for row_index, *values in fixed[columns].itertuples(index=True, name=None):
        fingerprint = _row_fingerprint(values)
        if fingerprint in seen:
            issues.append(
                Issue(row_index=int(row_index), column_name=ROW_LEVEL_COLUMN, rule="duplicate_row",
                      original_value=None, suggestion="drop_row", confidence=Confidence.MEDIUM,
                      reason=f"Sama persis dengan baris {seen[fingerprint]}")
            )
        else:
            seen[fingerprint] = int(row_index)

    open_rows = {i.row_index for i in issues if i.status is IssueStatus.OPEN}
    auto_rows = {i.row_index for i in issues if i.status is IssueStatus.AUTO_FIXED} - open_rows
    quarantine_mask = fixed.index.isin(sorted(open_rows))

    n_rows = len(df)
    issues_auto = sum(i.status is IssueStatus.AUTO_FIXED for i in issues)
    report = PipelineReport(
        rows_total=n_rows,
        rows_clean=n_rows - len(open_rows) - len(auto_rows),
        rows_auto_fixed=len(auto_rows),
        rows_quarantined=len(open_rows),
        issues_total=len(issues),
        issues_auto_fixed=issues_auto,
        issues_open=len(issues) - issues_auto,
        issues_by_rule=dict(Counter(i.rule for i in issues).most_common()),
        quality_score=round(100 * (1 - len(open_rows) / n_rows), 2) if n_rows else 100.0,
    )
    return PipelineResult(
        profiles=profiles,
        issues=issues,
        clean=fixed[~quarantine_mask],
        quarantine=fixed[quarantine_mask],
        report=report,
        fixed=fixed,
    )


def resolve_row(values: dict[str, str | None], row_issues: list[Issue]) -> dict[str, str | None] | None:
    """Apply human decisions to one quarantined row.

    Returns the final row, ``None`` if the row was dropped, and raises if any
    issue is still open (the row must stay in quarantine).
    """
    if any(i.status is IssueStatus.OPEN for i in row_issues):
        raise ValueError("row still has open issues")
    if any(i.status is IssueStatus.ROW_DROPPED for i in row_issues):
        return None
    result = dict(values)
    for issue in row_issues:
        if issue.column_name == ROW_LEVEL_COLUMN:
            continue
        if issue.status in (IssueStatus.ACCEPTED, IssueStatus.AUTO_FIXED):
            result[issue.column_name] = issue.suggestion
        elif issue.status is IssueStatus.EDITED:
            result[issue.column_name] = issue.final_value
        elif issue.status is IssueStatus.VALUE_DROPPED:
            result[issue.column_name] = None
    return result
