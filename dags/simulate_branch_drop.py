"""
### simulate_branch_drop (the producer)

Stands in for RetailKu's 10 branches. Every night at 01:00 WIB each branch "exports" yesterday's
`customers.csv` and `transactions.csv` into the landing zone, with realistic mistakes in them.

In a real company this DAG would not exist: the files would arrive by SFTP, email or a POS export.
It is here so the pipeline has fresh, messy data every day.

**Trigger manually** with `business_date` (YYYY-MM-DD) to drop a specific day, for example to backfill.
Dropping the same day twice writes byte-identical files, so SmartIngest ingests them only once.

When the files are written it updates the `retailku_landing` asset, which starts `smartingest_daily`.
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Asset, Param, dag, task

TZ = "Asia/Jakarta"
LANDING = Path(os.environ.get("RETAILKU_LANDING", "/opt/airflow/landing"))
LANDING_ASSET = Asset("retailku_landing")


@dag(
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__,
    tags=["retailku", "producer"],
    params={
        "business_date": Param(
            None, type=["null", "string"], format="date",
            description="Day to simulate (YYYY-MM-DD). Empty = yesterday in WIB.",
        ),
    },
)
def simulate_branch_drop():
    @task(outlets=[LANDING_ASSET])
    def drop_files(**context) -> dict:
        from retailku.simulator import drop_day

        chosen = context["params"].get("business_date")
        if chosen:
            day = date.fromisoformat(chosen)
        else:
            run_after = context["dag_run"].run_after  # logical_date can be empty on manual runs in Airflow 3
            day = pendulum.instance(run_after).in_timezone(TZ).date() - timedelta(days=1)

        written = drop_day(LANDING, day)
        print(f"Dropped {len(written)} files for {day} into {LANDING}")
        return {"business_date": day.isoformat(), "files": len(written)}

    drop_files()


simulate_branch_drop()
