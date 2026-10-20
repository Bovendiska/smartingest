"""The landing zone: where branch files wait until SmartIngest has taken them.

    landing/<date>/<branch>/<file>.csv            waiting
    landing/_archive/<date>/<branch>/<file>.csv   accepted by SmartIngest (201 new batch, or 200 already ingested)
    landing/_rejected/<date>/<branch>/<file>.csv  refused (409 schema drift, 422 unreadable), with a .reason.json

A file only moves after SmartIngest has answered, so a crash in between is harmless: the next run uploads
the same bytes again, SmartIngest recognises the batch_id and answers 200 without loading anything twice.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

ARCHIVE = "_archive"
REJECTED = "_rejected"
# File name in the landing zone -> SmartIngest source name (= silver table name)
SOURCES = {"customers.csv": "retailku_customers", "transactions.csv": "retailku_transactions"}
# Customers first, so a day's new members are in silver before that day's sales.
_ORDER = {name: i for i, name in enumerate(SOURCES)}


@dataclass
class IngestResult:
    path: str
    source: str
    outcome: str            # loaded | needs_review | already_ingested | rejected
    http_status: int
    batch_id: str | None = None
    rows_total: int | None = None
    rows_quarantined: int | None = None
    detail: str | None = None

    def as_dict(self) -> dict:
        return asdict(self)


def pending_files(landing: Path) -> list[Path]:
    """Files still waiting, oldest business date first. Partial files (.part) are never picked up."""
    files = [
        p for p in landing.glob("*/*/*.csv")
        if p.parts[-3] not in (ARCHIVE, REJECTED) and p.name in SOURCES
    ]
    return sorted(files, key=lambda p: (p.parts[-3], p.parts[-2], _ORDER[p.name]))


def _move(path: Path, landing: Path, area: str) -> Path:
    target = landing / area / path.relative_to(landing)
    target.parent.mkdir(parents=True, exist_ok=True)
    path.replace(target)
    return target


class SmartIngestClient:
    """Minimal client for the SmartIngest upload endpoint, authenticated with the admin key."""

    def __init__(self, base_url: str, api_key: str, timeout: float = 120, max_429_retries: int = 5) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_429_retries = max_429_retries
        self.session = requests.Session()
        self.session.headers["X-API-Key"] = api_key

    def ready(self) -> None:
        self.session.get(f"{self.base_url}/health/ready", timeout=10).raise_for_status()

    def upload(self, source: str, path: Path) -> requests.Response:
        for attempt in range(self.max_429_retries + 1):
            with path.open("rb") as fh:
                response = self.session.post(
                    f"{self.base_url}/sources/{source}/uploads",
                    files={"file": (path.name, fh, "text/csv")},
                    timeout=self.timeout,
                )
            if response.status_code != 429 or attempt == self.max_429_retries:
                return response
            wait = 2 ** attempt * 5  # rate limited: back off 5, 10, 20, 40, 80 seconds
            logger.warning("Rate limited on %s, retrying in %ss", path, wait)
            time.sleep(wait)
        raise AssertionError("unreachable")


def ingest_file(client: SmartIngestClient, landing: Path, path: Path) -> IngestResult:
    """Upload one file and move it to the archive or the rejected area.

    Raises on anything unexpected (5xx, network, auth), so Airflow retries the task and the file stays put.
    """
    source = SOURCES[path.name]
    rel = str(path.relative_to(landing))
    response = client.upload(source, path)

    if response.status_code in (200, 201):
        body = response.json()
        _move(path, landing, ARCHIVE)
        if body.get("already_ingested"):
            return IngestResult(rel, source, "already_ingested", 200, body["batch_id"])
        report = body["report"]
        return IngestResult(
            rel, source, body["status"], response.status_code, body["batch_id"],
            report["rows_total"], report["rows_quarantined"],
        )

    if response.status_code in (409, 422):
        # The file itself is the problem; retrying will not help. Park it with the reason for a human.
        detail = response.json().get("detail")
        target = _move(path, landing, REJECTED)
        target.with_suffix(".reason.json").write_text(
            json.dumps({"http_status": response.status_code, "detail": detail}, indent=2, ensure_ascii=False)
        )
        return IngestResult(rel, source, "rejected", response.status_code, detail=json.dumps(detail, ensure_ascii=False))

    response.raise_for_status()  # 401, 404, 429 after retries, 5xx: fail loudly
    raise RuntimeError(f"Unexpected status {response.status_code} for {rel}")


def summarize(results: list[dict]) -> dict:
    counts: dict[str, int] = {}
    for r in results:
        counts[r["outcome"]] = counts.get(r["outcome"], 0) + 1
    return {
        "files": len(results),
        "by_outcome": counts,
        "rows_total": sum(r["rows_total"] or 0 for r in results),
        "rows_quarantined": sum(r["rows_quarantined"] or 0 for r in results),
        "rejected": [r["path"] for r in results if r["outcome"] == "rejected"],
    }
