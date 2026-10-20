"""
### smartingest_daily (the consumer): bronze -> silver -> gold

Runs whenever the `retailku_landing` asset is updated.

1. **check_api**: SmartIngest must answer `/health/ready` (API and database up).
2. **find_files**: list the files waiting in the landing zone.
3. **ingest** (one mapped task per file): upload through the SmartIngest API with the admin key.
   SmartIngest keeps the raw file (bronze), validates it, loads clean rows into silver and quarantines the rest.
   Accepted files move to `_archive/`; files refused for schema drift (409) or as unreadable (422)
   move to `_rejected/` with the reason. Re-running is safe: the same file always maps to the same batch_id.
4. **report**: totals in the log, also when there was nothing new.
5. **dbt_run** then **dbt_test**: rebuild the gold layer and check it. Gold is rebuilt even without new
   files, because rows a human approved in quarantine since the last run are now in silver.

The API key never appears in this file or in the logs: it comes from the `smartingest_api` connection,
which Airflow masks.
"""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Asset, Connection, dag, task

LANDING = Path(os.environ.get("RETAILKU_LANDING", "/opt/airflow/landing"))
LANDING_ASSET = Asset("retailku_landing")
DBT = (
    "/opt/dbt-venv/bin/dbt {cmd} --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt "
    "--target-path /tmp/dbt/target --log-path /tmp/dbt/logs"
)


def _client():
    from retailku.landing import SmartIngestClient

    conn = Connection.get("smartingest_api")
    return SmartIngestClient(f"http://{conn.host}:{conn.port or 8000}", conn.password)


@dag(
    schedule=[LANDING_ASSET],
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Jakarta"),
    catchup=False,
    max_active_runs=1,  # two runs must never move the same files at once
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    doc_md=__doc__,
    tags=["retailku", "smartingest", "dbt"],
)
def smartingest_daily():
    @task
    def check_api() -> None:
        _client().ready()

    @task
    def find_files() -> list[str]:
        from retailku.landing import pending_files

        files = [str(p) for p in pending_files(LANDING)]
        print(f"{len(files)} file(s) waiting in {LANDING}")
        return files

    @task(max_active_tis_per_dagrun=2)  # stay well under the API's per-minute rate limit
    def ingest(path: str) -> dict:
        from retailku.landing import ingest_file

        result = ingest_file(_client(), LANDING, Path(path))
        print(result)
        return result.as_dict()

    @task(trigger_rule="none_failed")  # also runs when there were no files (ingest skipped)
    def report(results) -> dict:
        from retailku.landing import summarize

        summary = summarize(list(results or []))
        print(summary)
        if summary["rejected"]:
            print(f"WARNING: {len(summary['rejected'])} file(s) rejected, see {LANDING / '_rejected'}")
        return summary

    @task.bash
    def dbt_run() -> str:
        return DBT.format(cmd="run")

    @task.bash
    def dbt_test() -> str:
        return DBT.format(cmd="test")

    files = find_files()
    check_api() >> files
    summary = report(ingest.expand(path=files))
    summary >> dbt_run() >> dbt_test()


smartingest_daily()
