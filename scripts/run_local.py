"""Run the pipeline on a local file without Docker or a database.

Usage:
    python scripts/run_local.py examples/customers_dirty.xlsx --out examples/report
    python scripts/run_local.py data.csv --llm      # needs ANTHROPIC_API_KEY

Writes clean.csv, quarantine.csv, issues.csv and report.json into --out.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from smartingest.llm import NullAdvisor, build_advisor  # noqa: E402
from smartingest.parsing import read_table  # noqa: E402
from smartingest.pipeline import run_pipeline  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("file", type=Path)
    parser.add_argument("--out", type=Path, default=Path("output"))
    parser.add_argument("--llm", action="store_true")
    args = parser.parse_args()

    df = read_table(args.file.read_bytes(), args.file.name)
    result = run_pipeline(df, advisor=build_advisor() if args.llm else NullAdvisor())

    args.out.mkdir(parents=True, exist_ok=True)
    result.clean.to_csv(args.out / "clean.csv", index_label="row_index")
    result.quarantine.to_csv(args.out / "quarantine.csv", index_label="row_index")
    pd.DataFrame([i.model_dump(mode="json") for i in result.issues]).to_csv(args.out / "issues.csv", index=False)
    (args.out / "report.json").write_text(
        json.dumps(
            {"report": result.report.model_dump(), "columns": [p.model_dump(mode="json") for p in result.profiles]},
            indent=2,
            ensure_ascii=False,
        )
    )

    r = result.report
    print(f"{r.rows_total} rows: {r.rows_clean} clean, {r.rows_auto_fixed} auto-fixed, {r.rows_quarantined} quarantined")
    print(f"quality score {r.quality_score} | {r.issues_total} issues ({r.issues_auto_fixed} auto-fixed)")
    for p in result.profiles:
        print(f"  {p.original_name:<16} -> {p.semantic_type.value:<13} ({p.type_confidence:.2f}, {p.type_source})")
    print(f"Output written to {args.out}/")


if __name__ == "__main__":
    main()
