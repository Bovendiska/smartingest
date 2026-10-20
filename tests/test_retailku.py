"""RetailKu simulator and landing-zone logic (the plain-Python parts of the Airflow DAGs)."""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "dags")]

from retailku.landing import ARCHIVE, REJECTED, ingest_file, pending_files, summarize  # noqa: E402
from retailku.simulator import (  # noqa: E402
    BRANCHES, CUSTOMER_COLUMNS, TRANSACTION_COLUMNS, customers_rows, drop_day, to_csv, transactions_rows,
)
from smartingest.models import SemanticType  # noqa: E402
from smartingest.parsing import read_table  # noqa: E402
from smartingest.pipeline import run_pipeline  # noqa: E402

DAY = date(2026, 10, 15)


def test_same_day_gives_byte_identical_files(tmp_path: Path) -> None:
    first = {p.relative_to(tmp_path): p.read_bytes() for p in drop_day(tmp_path / "a", DAY)}
    second = {p.relative_to(tmp_path): p.read_bytes() for p in drop_day(tmp_path / "b", DAY)}
    assert len(first) == 20  # 10 branches x 2 files
    assert [v for _, v in sorted(first.items())] == [v for _, v in sorted(second.items())]


def test_different_days_and_branches_differ() -> None:
    a, b = BRANCHES[0], BRANCHES[1]
    assert to_csv(transactions_rows(a, DAY), TRANSACTION_COLUMNS) != to_csv(transactions_rows(b, DAY), TRANSACTION_COLUMNS)
    assert to_csv(customers_rows(a, DAY), CUSTOMER_COLUMNS) != to_csv(customers_rows(a, date(2026, 10, 16)), CUSTOMER_COLUMNS)


def test_inferred_types_are_stable_across_days() -> None:
    """Every daily file must infer the same column types, or SmartIngest would reject it as schema drift."""
    expected = {
        "customers": {"mail": SemanticType.EMAIL, "no_hp": SemanticType.PHONE_ID, "tgl_daftar": SemanticType.DATE,
                      "member": SemanticType.BOOLEAN, "id_pelanggan": SemanticType.TEXT},
        "transactions": {"tanggal_transaksi": SemanticType.DATE, "qty": SemanticType.INTEGER,
                         "harga_satuan": SemanticType.CURRENCY_IDR, "total": SemanticType.CURRENCY_IDR},
    }
    for offset in range(0, 60, 7):
        day = date.fromordinal(DAY.toordinal() - offset)
        for branch in BRANCHES:
            for name, rows, cols in (("customers", customers_rows(branch, day), CUSTOMER_COLUMNS),
                                     ("transactions", transactions_rows(branch, day), TRANSACTION_COLUMNS)):
                types = {p.column_name: p.semantic_type for p in run_pipeline(read_table(to_csv(rows, cols), "f.csv")).profiles}
                assert {c: types[c] for c in expected[name]} == expected[name], (name, branch.code, day)


def test_files_contain_errors_to_catch() -> None:
    rows = [r for b in BRANCHES for r in transactions_rows(b, DAY)]
    result = run_pipeline(read_table(to_csv(rows, TRANSACTION_COLUMNS), "t.csv"))
    rules = {i.rule for i in result.issues}
    assert {"currency_format", "duplicate_row"} <= rules
    assert 0 < result.report.rows_quarantined < result.report.rows_total * 0.2


def test_pending_files_order_and_exclusions(tmp_path: Path) -> None:
    drop_day(tmp_path, DAY, BRANCHES[:2])
    drop_day(tmp_path, date(2026, 10, 14), BRANCHES[:1])
    (tmp_path / DAY.isoformat() / "JKT01" / ".customers.csv.part").write_text("half")
    (tmp_path / DAY.isoformat() / "JKT01" / "notes.csv").write_text("x")
    names = [p.relative_to(tmp_path).as_posix() for p in pending_files(tmp_path)]
    assert names[:2] == ["2026-10-14/JKT01/customers.csv", "2026-10-14/JKT01/transactions.csv"]
    assert len(names) == 6 and not any("part" in n or "notes" in n for n in names)


class FakeResponse:
    def __init__(self, status: int, body: dict) -> None:
        self.status_code, self._body = status, body

    def json(self) -> dict:
        return self._body

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeClient:
    def __init__(self, response: FakeResponse) -> None:
        self.response, self.calls = response, []

    def upload(self, source: str, path: Path) -> FakeResponse:
        self.calls.append(source)
        return self.response


@pytest.mark.parametrize(
    ("status", "body", "outcome", "area"),
    [
        (201, {"batch_id": "b1", "status": "needs_review", "report": {"rows_total": 10, "rows_quarantined": 2}},
         "needs_review", ARCHIVE),
        (200, {"batch_id": "b1", "already_ingested": True, "status": "loaded"}, "already_ingested", ARCHIVE),
        (409, {"detail": {"message": "Schema drift detected"}}, "rejected", REJECTED),
    ],
)
def test_ingest_moves_file_by_outcome(tmp_path: Path, status: int, body: dict, outcome: str, area: str) -> None:
    path = drop_day(tmp_path, DAY, BRANCHES[:1])[0]
    client = FakeClient(FakeResponse(status, body))
    result = ingest_file(client, tmp_path, path)  # type: ignore[arg-type]
    assert client.calls == ["retailku_customers"]
    assert result.outcome == outcome and not path.exists()
    moved = tmp_path / area / path.relative_to(tmp_path)
    assert moved.exists()
    if area == REJECTED:
        assert json.loads(moved.with_suffix(".reason.json").read_text())["http_status"] == 409


def test_ingest_server_error_keeps_file_for_retry(tmp_path: Path) -> None:
    path = drop_day(tmp_path, DAY, BRANCHES[:1])[0]
    with pytest.raises(RuntimeError):
        ingest_file(FakeClient(FakeResponse(503, {})), tmp_path, path)  # type: ignore[arg-type]
    assert path.exists()


def test_summarize() -> None:
    results = [
        {"path": "a", "outcome": "loaded", "rows_total": 5, "rows_quarantined": 0},
        {"path": "b", "outcome": "rejected", "rows_total": None, "rows_quarantined": None},
    ]
    assert summarize(results) == {"files": 2, "by_outcome": {"loaded": 1, "rejected": 1}, "rows_total": 5,
                                  "rows_quarantined": 0, "rejected": ["b"]}
