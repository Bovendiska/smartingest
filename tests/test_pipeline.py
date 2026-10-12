from __future__ import annotations

import io
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from smartingest.llm import FixItem, NullAdvisor  # noqa: E402
from smartingest.masking import mask_for_type, mask_value  # noqa: E402
from smartingest.models import Confidence, IssueStatus, SemanticType  # noqa: E402
from smartingest.parsing import read_table, slugify_column, unique_column_names  # noqa: E402
from smartingest.pipeline import resolve_row, run_pipeline  # noqa: E402
from smartingest.validators import ColumnContext, check_value  # noqa: E402

CSV = b"""Nama,Mail,No HP,Tgl Lahir,Total Belanja,Member
Budi Santoso,budi@gmail.com,+6281234567890,1990-01-31,1500000,true
Siti Aminah,siti@gmail.com,081298765432,31/01/1991,Rp 2.500.000,Ya
Reindra,reindra@,0812-1111-2222,1992-02-30,seratus ribu,tidak
Dewi,dewi@gmial.com,+6281300001111,1993-03-03,750000,false
Agus,agus@outlook.com,+6281300002222,1994-04-04,800000,true
"""


def frame() -> pd.DataFrame:
    return read_table(CSV, "customers.csv")


# --- parsing ---------------------------------------------------------------------


def test_slugify_makes_safe_identifiers() -> None:
    assert slugify_column("No HP") == "no_hp"
    assert slugify_column('Mail"; DROP TABLE x;--') == "mail_drop_table_x"
    assert slugify_column("2024 Sales") == "c_2024_sales"
    assert slugify_column("Tgl. Lahir (DD/MM)") == "tgl_lahir_dd_mm"
    assert unique_column_names(["Mail", "mail", "MAIL"]) == ["mail", "mail_2", "mail_3"]


def test_read_table_keeps_text_and_leading_zeros() -> None:
    df = frame()
    assert list(df.columns) == ["nama", "mail", "no_hp", "tgl_lahir", "total_belanja", "member"]
    assert df.at[1, "no_hp"] == "081298765432"
    assert df.attrs["original_columns"][2] == "No HP"


def test_read_json_and_excel_match_csv() -> None:
    df = frame()
    as_json = json.dumps(df.rename(columns=dict(zip(df.columns, df.attrs["original_columns"], strict = True))).to_dict("records"))
    assert read_table(as_json.encode(), "x.json").equals(df)
    buffer = io.BytesIO()
    df.to_excel(buffer, index=False)
    assert read_table(buffer.getvalue(), "x.xlsx").values.tolist() == df.values.tolist()


# --- validators --------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "semantic_type", "suggestion", "confidence"),
    [
        ("dewi@gmial.com", SemanticType.EMAIL, "dewi@gmail.com", Confidence.HIGH),
        (" Budi@GMAIL.com ", SemanticType.EMAIL, "budi@gmail.com", Confidence.HIGH),
        ("reindra@", SemanticType.EMAIL, "reindra@gmail.com", Confidence.LOW),
        ("reindra.gmail.com", SemanticType.EMAIL, "reindra@gmail.com", Confidence.MEDIUM),
        ("0812-1111-2222", SemanticType.PHONE_ID, "+6281211112222", Confidence.HIGH),
        ("81211112222", SemanticType.PHONE_ID, "+6281211112222", Confidence.MEDIUM),
        ("31/01/1991", SemanticType.DATE, "1991-01-31", Confidence.HIGH),
        ("05/04/1991", SemanticType.DATE, "1991-04-05", Confidence.MEDIUM),
        ("Rp 2.500.000", SemanticType.CURRENCY_IDR, "2500000", Confidence.HIGH),
        ("Ya", SemanticType.BOOLEAN, "true", Confidence.HIGH),
        ("  Budi   Santoso ", SemanticType.TEXT, "Budi Santoso", Confidence.HIGH),
    ],
)
def test_fix_suggestions(value: str, semantic_type: SemanticType, suggestion: str, confidence: Confidence) -> None:
    finding = check_value(value, semantic_type, ColumnContext(top_email_domain="gmail.com"))
    assert finding is not None
    assert (finding.suggestion, finding.confidence) == (suggestion, confidence)


@pytest.mark.parametrize(
    ("value", "semantic_type"),
    [
        ("1992-02-30", SemanticType.DATE),
        ("seratus ribu", SemanticType.CURRENCY_IDR),
        ("081234", SemanticType.PHONE_ID),
        ("not an email", SemanticType.EMAIL),
    ],
)
def test_unfixable_values_have_no_suggestion(value: str, semantic_type: SemanticType) -> None:
    finding = check_value(value, semantic_type, ColumnContext(top_email_domain="gmail.com"))
    assert finding is not None and finding.suggestion is None and finding.confidence is Confidence.NONE


def test_canonical_values_pass() -> None:
    ctx = ColumnContext()
    assert check_value("budi@gmail.com", SemanticType.EMAIL, ctx) is None
    assert check_value("someone@company.co.id", SemanticType.EMAIL, ctx) is None
    assert check_value("+6281234567890", SemanticType.PHONE_ID, ctx) is None
    assert check_value("2024-02-29", SemanticType.DATE, ctx) is None


# --- masking -----------------------------------------------------------------------


def test_masking_hides_personal_data_but_keeps_shape() -> None:
    assert mask_value("Reindra@gmial.com") == "Aaaaaaa@gmial.com"
    assert mask_value("budi@acme-corp.co.id") == "aaaa@aaaa-aaaa.aa.aa"
    assert mask_value("0812-1111-2222") == "9999-9999-9999"
    assert mask_for_type("seratus ribu", SemanticType.CURRENCY_IDR) == "seratus ribu"


# --- pipeline ----------------------------------------------------------------------


def test_pipeline_infers_types() -> None:
    types = {p.column_name: p.semantic_type for p in run_pipeline(frame()).profiles}
    assert types == {
        "nama": SemanticType.TEXT, "mail": SemanticType.EMAIL, "no_hp": SemanticType.PHONE_ID,
        "tgl_lahir": SemanticType.DATE, "total_belanja": SemanticType.CURRENCY_IDR, "member": SemanticType.BOOLEAN,
    }


def test_pipeline_auto_fixes_high_and_quarantines_the_rest() -> None:
    result = run_pipeline(frame())
    assert result.fixed.at[1, "total_belanja"] == "2500000"
    assert result.fixed.at[3, "mail"] == "dewi@gmail.com"
    assert result.fixed.at[2, "mail"] == "reindra@"  # low confidence: not applied
    assert list(result.quarantine.index) == [2]
    assert result.report.rows_quarantined == 1
    assert result.report.rows_total == 5


def test_duplicate_rows_detected_after_normalisation() -> None:
    dup = CSV + b"agus ,AGUS@outlook.com,081300002222,04/04/1994,Rp800000,y\n"
    result = run_pipeline(read_table(dup, "d.csv"))
    rules = {(i.row_index, i.rule) for i in result.issues}
    assert (5, "duplicate_row") in rules


def test_llm_suggestions_fill_gaps_but_are_never_auto_applied() -> None:
    class FakeAdvisor(NullAdvisor):
        def suggest_fixes(self, profile, items):  # type: ignore[override]
            return [FixItem(row_index=r, suggestion="100000", confidence=Confidence.MEDIUM, reason="kata ke angka")
                    for r, v, _ in items if v == "seratus ribu"]

    result = run_pipeline(frame(), advisor=FakeAdvisor())
    issue = next(i for i in result.issues if i.column_name == "total_belanja" and i.row_index == 2)
    assert (issue.suggestion, issue.source, issue.status) == ("100000", "llm", IssueStatus.OPEN)
    assert result.fixed.at[2, "total_belanja"] == "seratus ribu"


def test_resolve_row_applies_decisions() -> None:
    result = run_pipeline(frame())
    row_issues = [i for i in result.issues if i.row_index == 2]
    with pytest.raises(ValueError):
        resolve_row(result.fixed.loc[2].to_dict(), row_issues)
    for issue in row_issues:
        if issue.status is IssueStatus.OPEN:
            if issue.suggestion is not None:
                issue.status = IssueStatus.ACCEPTED
            else:
                issue.status = IssueStatus.EDITED
                issue.final_value = {"tgl_lahir": "1992-02-29", "total_belanja": "100000"}[issue.column_name]
    final = resolve_row(result.fixed.loc[2].to_dict(), row_issues)
    assert final is not None
    assert final["mail"] == "reindra@gmail.com"
    assert final["tgl_lahir"] == "1992-02-29"
    assert final["total_belanja"] == "100000"
    assert final["member"] == "false"
