"""Generate a realistic dirty customer dataset with ground-truth labels.

Usage:
    python scripts/generate_dirty_data.py --rows 2000 --error-rate 0.08 --out examples/

Outputs:
    customers_clean.csv   canonical values (what the pipeline should produce)
    customers_dirty.csv   the same rows with injected errors (+ duplicates)
    customers_dirty.xlsx  same as above, Excel
    customers_dirty.json  same as above, JSON records
    ground_truth.csv      one line per injected error: row, column, error_type, clean_value, dirty_value

No third-party faker is needed: names and cities come from small built-in lists,
which keeps the generator deterministic and dependency-free.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

FIRST_NAMES = [
    "Budi", "Siti", "Agus", "Dewi", "Reindra", "Rina", "Andi", "Putri", "Fajar", "Nur", "Dimas", "Ayu", "Bayu",
    "Intan", "Rizki", "Wulan", "Hendra", "Maya", "Yoga", "Sari", "Teguh", "Lestari", "Arif", "Fitri", "Bona",
]
LAST_NAMES = [
    "Santoso", "Wijaya", "Saputra", "Lestari", "Pratama", "Kusuma", "Hidayat", "Nugroho", "Wibowo", "Siregar",
    "Hutapea", "Simanjuntak", "Setiawan", "Rahmawati", "Putra", "Halim", "Gunawan", "Susanto",
]
CITIES = ["Jakarta", "Bandung", "Surabaya", "Medan", "Yogyakarta", "Semarang", "Makassar", "Denpasar", "Malang", "Bogor"]
DOMAINS = ["gmail.com"] * 6 + ["yahoo.com"] * 2 + ["outlook.com", "hotmail.com"]
TYPO_FOR = {"gmail.com": ["gmial.com", "gmai.com", "gamil.com"], "yahoo.com": ["yaho.com"],
            "outlook.com": ["outlok.com"], "hotmail.com": ["hotmial.com"]}
NUMBER_WORDS = {100000: "seratus ribu", 250000: "dua ratus lima puluh ribu", 500000: "lima ratus ribu",
                1000000: "satu juta"}

COLUMNS = ["Nama", "Mail", "No HP", "Tgl Lahir", "Kota", "Total Belanja", "Member"]


def clean_row(rng: random.Random, i: int) -> dict[str, str]:
    first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
    local = f"{first}.{last}{i}".lower()
    birth = date(1965, 1, 1) + timedelta(days=rng.randint(0, 365 * 40))
    total = rng.choice(list(NUMBER_WORDS)) if rng.random() < 0.03 else rng.randrange(50_000, 20_000_000, 1_000)
    return {
        "Nama": f"{first} {last}",
        "Mail": f"{local}@{rng.choice(DOMAINS)}",
        "No HP": f"+628{rng.randint(10, 99)}{rng.randint(1_000_000, 99_999_999)}",
        "Tgl Lahir": birth.isoformat(),
        "Kota": rng.choice(CITIES),
        "Total Belanja": str(total),
        "Member": rng.choice(["true", "false"]),
    }


def corrupt(rng: random.Random, column: str, value: str) -> tuple[str, str] | None:
    """Return (error_type, dirty_value), or None if no applicable error."""
    if column == "Mail":
        local, domain = value.split("@")
        options = [
            ("email_missing_domain", f"{local}@"),
            ("email_case_whitespace", f"  {local.title()}@{domain.upper()} "),
            ("email_missing_at", f"{local}.{domain}"),
            ("missing_required", ""),
        ]
        if domain in TYPO_FOR:
            options.append(("email_domain_typo", f"{local}@{rng.choice(TYPO_FOR[domain])}"))
        return rng.choice(options)
    if column == "No HP":
        local = "0" + value[3:]
        options = [
            ("phone_local_format", local),
            ("phone_dashes", f"{local[:4]}-{local[4:8]}-{local[8:]}"),
            ("phone_missing_zero", local[1:]),
            ("phone_too_short", local[:6]),
        ]
        return rng.choice(options)
    if column == "Tgl Lahir":
        y, m, d = value.split("-")
        options = [("date_dmy", f"{d}/{m}/{y}"), ("date_dmy_dash", f"{d}-{m}-{y}")]
        if int(d) <= 28:
            options.append(("date_impossible", f"{y}-02-30"))
        return rng.choice(options)
    if column == "Total Belanja":
        n = int(value)
        if n in NUMBER_WORDS:
            return "number_in_words", NUMBER_WORDS[n]
        return rng.choice([("currency_rp_dots", f"Rp {n:,}".replace(",", ".")), ("currency_rp_plain", f"Rp{n}")])
    if column == "Nama":
        first, last = value.split(" ", 1)
        return "text_whitespace", f"  {first}   {last} "
    if column == "Member":
        return "boolean_variant", rng.choice(["Ya", "YES", "y"] if value == "true" else ["Tidak", "NO", "n"])
    return None


def generate(rows: int, error_rate: float, duplicate_rate: float, seed: int) -> tuple[list[dict], list[dict], list[dict]]:
    rng = random.Random(seed)
    clean = [clean_row(rng, i) for i in range(rows)]
    dirty = [dict(r) for r in clean]
    truth: list[dict] = []

    for row_index, row in enumerate(dirty):
        for column in COLUMNS:
            # Amounts that have a word form are always written in words (a classic manual-entry error).
            words = column == "Total Belanja" and int(clean[row_index][column]) in NUMBER_WORDS
            if not words and rng.random() >= error_rate:
                continue
            result = corrupt(rng, column, clean[row_index][column])
            if result is None:
                continue
            error_type, dirty_value = result
            row[column] = dirty_value
            truth.append({"row_index": row_index, "column": column, "error_type": error_type,
                          "clean_value": clean[row_index][column], "dirty_value": dirty_value})

    # Duplicates: exact copies of earlier dirty rows inserted at random positions.
    n_dup = int(rows * duplicate_rate)
    for _ in range(n_dup):
        src = rng.randrange(len(dirty))
        pos = rng.randrange(src + 1, len(dirty) + 1)
        dirty.insert(pos, dict(dirty[src]))
        clean.insert(pos, dict(clean[src]))
        for t in truth:
            if t["row_index"] >= pos:
                t["row_index"] += 1
        truth.append({"row_index": pos, "column": "_row", "error_type": "duplicate_row",
                      "clean_value": "drop_row", "dirty_value": ""})
        for t in [t for t in truth if t["row_index"] == (src if src < pos else src + 1) and t["column"] != "_row"]:
            truth.append({**t, "row_index": pos})
    truth.sort(key=lambda t: (t["row_index"], t["column"]))
    return clean, dirty, truth


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rows", type=int, default=500)
    parser.add_argument("--error-rate", type=float, default=0.08, help="chance per cell of an injected error")
    parser.add_argument("--duplicate-rate", type=float, default=0.01)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", type=Path, default=Path("examples"))
    args = parser.parse_args()

    clean, dirty, truth = generate(args.rows, args.error_rate, args.duplicate_rate, args.seed)
    args.out.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(clean, columns=COLUMNS).to_csv(args.out / "customers_clean.csv", index=False)
    dirty_df = pd.DataFrame(dirty, columns=COLUMNS)
    dirty_df.to_csv(args.out / "customers_dirty.csv", index=False)
    dirty_df.to_excel(args.out / "customers_dirty.xlsx", index=False)
    (args.out / "customers_dirty.json").write_text(json.dumps(dirty, ensure_ascii=False, indent=1))
    with (args.out / "ground_truth.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["row_index", "column", "error_type", "clean_value", "dirty_value"])
        writer.writeheader()
        writer.writerows(truth)
    print(f"{len(dirty)} rows, {len(truth)} injected errors -> {args.out}/")


if __name__ == "__main__":
    main()
