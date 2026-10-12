"""Evaluate the pipeline against the generator's ground truth.

Usage:
    python scripts/evaluate.py --data examples/customers_dirty.csv --truth examples/ground_truth.csv
    python scripts/evaluate.py ... --llm        # rule + Claude (needs ANTHROPIC_API_KEY)
    python scripts/evaluate.py ... --json out.json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from smartingest.llm import LLMCallRecord, NullAdvisor, build_advisor  # noqa: E402
from smartingest.models import ROW_LEVEL_COLUMN, IssueStatus  # noqa: E402
from smartingest.parsing import read_table, slugify_column  # noqa: E402
from smartingest.pipeline import run_pipeline  # noqa: E402


def evaluate(data_path: Path, truth_path: Path, use_llm: bool) -> dict:
    df = read_table(data_path.read_bytes(), data_path.name)
    truth = pd.read_csv(truth_path, dtype=str, keep_default_na=False)
    truth["column"] = truth["column"].map(lambda c: c if c == ROW_LEVEL_COLUMN else slugify_column(c))
    truth["row_index"] = truth["row_index"].astype(int)

    calls: list[LLMCallRecord] = []
    advisor = build_advisor(on_call=calls.append) if use_llm else NullAdvisor()
    result = run_pipeline(df, advisor=advisor)

    issues = {(i.row_index, i.column_name): i for i in result.issues}
    truth_map = {(r.row_index, r.column): r for r in truth.itertuples()}

    detected = [k for k in truth_map if k in issues]
    true_positive_issues = [k for k in issues if k in truth_map]
    with_suggestion = [k for k in detected if issues[k].suggestion is not None]
    correct_fix = [k for k in with_suggestion if issues[k].suggestion == truth_map[k].clean_value]
    auto_fixed = [k for k, i in issues.items() if i.status is IssueStatus.AUTO_FIXED]
    wrong_auto = [k for k in auto_fixed if k not in truth_map or issues[k].suggestion != truth_map[k].clean_value]

    per_type: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for key, row in truth_map.items():
        stats = per_type[row.error_type]
        stats["injected"] += 1
        if key in issues:
            stats["detected"] += 1
            if issues[key].suggestion is not None:
                stats["suggested"] += 1
                stats["correct"] += issues[key].suggestion == row.clean_value

    def ratio(a: int, b: int) -> float:
        return round(a / b, 4) if b else 0.0

    return {
        "mode": "rule + claude" if use_llm and not isinstance(advisor, NullAdvisor) else "rule-only",
        "rows": result.report.rows_total,
        "injected_errors": len(truth_map),
        "reported_issues": len(issues),
        "detection_recall": ratio(len(detected), len(truth_map)),
        "detection_precision": ratio(len(true_positive_issues), len(issues)),
        "suggestion_coverage": ratio(len(with_suggestion), len(detected)),
        "fix_accuracy": ratio(len(correct_fix), len(with_suggestion)),
        "auto_fixed": len(auto_fixed),
        "auto_fix_error_rate": ratio(len(wrong_auto), len(auto_fixed)),
        "rows_quarantined": result.report.rows_quarantined,
        "quality_score": result.report.quality_score,
        "llm_calls": len(calls),
        "llm_input_tokens": sum(c.input_tokens for c in calls),
        "llm_output_tokens": sum(c.output_tokens for c in calls),
        "per_error_type": {k: dict(v) for k, v in sorted(per_type.items())},
    }


def print_report(m: dict) -> None:
    print(f"\nMode: {m['mode']}  |  rows={m['rows']}  injected={m['injected_errors']}  reported={m['reported_issues']}")
    print(f"  detection recall     {m['detection_recall']:.1%}")
    print(f"  detection precision  {m['detection_precision']:.1%}")
    print(f"  suggestion coverage  {m['suggestion_coverage']:.1%}")
    print(f"  fix accuracy         {m['fix_accuracy']:.1%}")
    print(f"  auto-fix error rate  {m['auto_fix_error_rate']:.2%}  ({m['auto_fixed']} auto-fixed)")
    print(f"  rows quarantined     {m['rows_quarantined']}  (quality score {m['quality_score']})")
    if m["llm_calls"]:
        print(f"  LLM calls            {m['llm_calls']}  ({m['llm_input_tokens']} in / {m['llm_output_tokens']} out tokens)")
    print("\n  error_type               injected detected suggested correct")
    for name, s in m["per_error_type"].items():
        print(f"  {name:<24} {s.get('injected', 0):>8} {s.get('detected', 0):>8} {s.get('suggested', 0):>9} {s.get('correct', 0):>7}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data", type=Path, default=Path("examples/customers_dirty.csv"))
    parser.add_argument("--truth", type=Path, default=Path("examples/ground_truth.csv"))
    parser.add_argument("--llm", action="store_true")
    parser.add_argument("--json", type=Path)
    args = parser.parse_args()
    metrics = evaluate(args.data, args.truth, args.llm)
    print_report(metrics)
    if args.json:
        args.json.write_text(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
