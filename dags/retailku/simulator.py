"""Simulated daily file drops from RetailKu's 10 branches.

Each branch sends two CSV files per business day, the way a store's point-of-sale export would:

    landing/<yyyy-mm-dd>/<branch>/customers.csv      new members registered that day (+ a few updates)
    landing/<yyyy-mm-dd>/<branch>/transactions.csv   that day's sales

Like real exports, the files are messy: dates typed as dd/mm/yyyy, "Rp 15.000" amounts, emails without
a domain, rows sent twice, a transaction re-sent the next day. Every value is a pure function of
(branch, business date), so re-generating a day gives byte-identical files. That is what lets the
pipeline prove idempotency: same file -> same batch_id -> no duplicate data.

No third-party packages: this runs inside Airflow and in CI.

Usage (without Airflow):
    python dags/retailku/simulator.py --start 2026-09-01 --days 30 --out landing
"""

from __future__ import annotations

import csv
import hashlib
import io
import random
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

EPOCH = date(2026, 1, 1)          # customer ids are only generated from this day on
LOOKBACK_DAYS = 21                # transactions pick members registered in the last 3 weeks
WALK_IN = "UMUM"                  # customer id used for non-member sales


@dataclass(frozen=True)
class Branch:
    code: str
    city: str
    size: float  # relative traffic: 1.0 = average store


BRANCHES: tuple[Branch, ...] = (
    Branch("JKT01", "Jakarta", 1.6),
    Branch("JKT02", "Jakarta", 1.3),
    Branch("BDG01", "Bandung", 1.1),
    Branch("SBY01", "Surabaya", 1.2),
    Branch("MDN01", "Medan", 0.9),
    Branch("YOG01", "Yogyakarta", 0.8),
    Branch("SMG01", "Semarang", 0.8),
    Branch("MKS01", "Makassar", 0.7),
    Branch("DPS01", "Denpasar", 0.9),
    Branch("MLG01", "Malang", 0.6),
)
BRANCH_BY_CODE = {b.code: b for b in BRANCHES}

# (product, category, unit price in rupiah)
PRODUCTS: tuple[tuple[str, str, int], ...] = (
    ("Beras Premium 5kg", "Sembako", 78_000),
    ("Minyak Goreng 2L", "Sembako", 38_500),
    ("Gula Pasir 1kg", "Sembako", 17_500),
    ("Telur Ayam 1kg", "Sembako", 29_000),
    ("Kopi Bubuk 200g", "Minuman", 24_000),
    ("Teh Celup 25s", "Minuman", 9_500),
    ("Susu UHT 1L", "Minuman", 19_900),
    ("Air Mineral 600ml", "Minuman", 4_000),
    ("Mi Instan Goreng", "Makanan", 3_500),
    ("Roti Tawar", "Makanan", 16_000),
    ("Biskuit Kaleng", "Makanan", 65_000),
    ("Sabun Mandi", "Perawatan", 5_500),
    ("Sampo 340ml", "Perawatan", 32_000),
    ("Pasta Gigi 190g", "Perawatan", 14_500),
    ("Deterjen 1.8kg", "Rumah Tangga", 42_000),
    ("Tisu Gulung 10s", "Rumah Tangga", 36_000),
)
PAYMENT_METHODS = ("Tunai", "Tunai", "QRIS", "QRIS", "QRIS", "Debit", "Kartu Kredit")

FIRST_NAMES = (
    "Budi", "Siti", "Agus", "Dewi", "Reindra", "Rina", "Andi", "Putri", "Fajar", "Nur", "Dimas", "Ayu", "Bayu",
    "Intan", "Rizki", "Wulan", "Hendra", "Maya", "Yoga", "Sari", "Teguh", "Lestari", "Arif", "Fitri", "Bona",
)
LAST_NAMES = (
    "Santoso", "Wijaya", "Saputra", "Lestari", "Pratama", "Kusuma", "Hidayat", "Nugroho", "Wibowo", "Siregar",
    "Hutapea", "Simanjuntak", "Setiawan", "Rahmawati", "Putra", "Halim", "Gunawan", "Susanto",
)
DOMAINS = ("gmail.com",) * 6 + ("yahoo.com",) * 2 + ("outlook.com", "hotmail.com")
DOMAIN_TYPOS = {"gmail.com": "gmial.com", "yahoo.com": "yaho.com", "outlook.com": "outlok.com", "hotmail.com": "hotmial.com"}
NUMBER_WORDS = {100_000: "seratus ribu", 150_000: "seratus lima puluh ribu", 200_000: "dua ratus ribu"}

CUSTOMER_COLUMNS = ("ID Pelanggan", "Kode Cabang", "Nama", "Mail", "No HP", "Tgl Daftar", "Kota", "Member")
TRANSACTION_COLUMNS = (
    "ID Transaksi", "Kode Cabang", "ID Pelanggan", "Tanggal Transaksi", "Produk", "Kategori", "Qty",
    "Harga Satuan", "Total", "Metode Bayar",
)

CUSTOMER_ERROR_RATE = 0.06     # per cell
TRANSACTION_ERROR_RATE = 0.03  # per cell


def _rng(*parts: object) -> random.Random:
    """A random generator seeded from the parts, stable across Python versions and machines."""
    seed = hashlib.sha256("|".join(str(p) for p in parts).encode()).hexdigest()
    return random.Random(int(seed[:16], 16))


def _traffic(branch: Branch, day: date) -> float:
    weekend = 1.35 if day.weekday() >= 5 else 1.0
    payday = 1.25 if day.day >= 25 or day.day <= 2 else 1.0  # Indonesian payday at month end
    return branch.size * weekend * payday


def new_customer_count(branch: Branch, day: date) -> int:
    return max(3, round(_rng("ncust", branch.code, day).gauss(8, 2) * branch.size))


def customer_id(branch: Branch, day: date, n: int) -> str:
    return f"C-{branch.code}-{day:%y%m%d}-{n:02d}"


def _clean_customer(branch: Branch, day: date, n: int) -> dict[str, str]:
    rng = _rng("cust", branch.code, day, n)
    first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
    return {
        "ID Pelanggan": customer_id(branch, day, n),
        "Kode Cabang": branch.code,
        "Nama": f"{first} {last}",
        "Mail": f"{first}.{last}.{branch.code}{day:%y%m%d}{n:02d}".lower() + "@" + rng.choice(DOMAINS),
        "No HP": f"+628{rng.randint(11, 99)}{rng.randint(1_000_000, 99_999_999)}",
        "Tgl Daftar": day.isoformat(),
        "Kota": branch.city if rng.random() < 0.9 else rng.choice(BRANCHES).city,
        "Member": "true" if rng.random() < 0.7 else "false",
    }


def _corrupt_customer(rng: random.Random, column: str, value: str) -> str:
    if column == "Mail":
        local, domain = value.split("@")
        return rng.choice([
            f"{local}@",                                   # domain missing: needs a human
            f" {local.title()}@{domain.upper()}",           # case/whitespace: auto-fixed
            f"{local}.{domain}",                           # '@' missing: medium confidence
            f"{local}@{DOMAIN_TYPOS[domain]}",             # known typo: auto-fixed
            "",                                            # missing required value
        ])
    if column == "No HP":
        local = "0" + value[3:]
        return rng.choice([local, f"{local[:4]}-{local[4:8]}-{local[8:]}", value[3:], local[:6]])
    if column == "Tgl Daftar":
        d = date.fromisoformat(value)
        return rng.choice([f"{d:%d/%m/%Y}", f"{d:%d-%m-%Y}", f"{d.year}-02-30"])
    if column == "Member":
        return rng.choice(["Ya", "Tidak", "Y", "N"])
    if column == "Nama":
        return f"  {value.upper()} "
    return value


def customers_rows(branch: Branch, day: date) -> list[dict[str, str]]:
    rng = _rng("cust-file", branch.code, day)
    rows = [_clean_customer(branch, day, n) for n in range(1, new_customer_count(branch, day) + 1)]

    # About one file in five also re-sends an older member whose phone number changed.
    if rng.random() < 0.2 and day > EPOCH:
        back = day - timedelta(days=rng.randint(1, min(30, (day - EPOCH).days)))
        old = _clean_customer(branch, back, rng.randint(1, new_customer_count(branch, back)))
        old["No HP"] = f"+628{rng.randint(11, 99)}{rng.randint(1_000_000, 99_999_999)}"
        rows.append(old)

    for row in rows:
        for column in ("Nama", "Mail", "No HP", "Tgl Daftar", "Member"):
            if rng.random() < CUSTOMER_ERROR_RATE:
                row[column] = _corrupt_customer(rng, column, row[column])
    if rng.random() < 0.15:  # the export ran twice and a row is duplicated
        rows.append(dict(rng.choice(rows)))
    return rows


def _clean_transactions(branch: Branch, day: date) -> list[dict[str, str]]:
    rng = _rng("trx", branch.code, day)
    count = max(5, round(rng.gauss(40, 8) * _traffic(branch, day)))
    rows = []
    for n in range(1, count + 1):
        product, category, price = rng.choice(PRODUCTS)
        qty = rng.choices((1, 2, 3, 4, 5, 6), weights=(30, 30, 18, 10, 7, 5))[0]
        if rng.random() < 0.35:
            customer = WALK_IN
        else:
            back = day - timedelta(days=rng.randint(0, min(LOOKBACK_DAYS, (day - EPOCH).days)))
            customer = customer_id(branch, back, rng.randint(1, new_customer_count(branch, back)))
        rows.append({
            "ID Transaksi": f"T-{branch.code}-{day:%y%m%d}-{n:04d}",
            "Kode Cabang": branch.code,
            "ID Pelanggan": customer,
            "Tanggal Transaksi": day.isoformat(),
            "Produk": product,
            "Kategori": category,
            "Qty": str(qty),
            "Harga Satuan": str(price),
            "Total": str(qty * price),
            "Metode Bayar": rng.choice(PAYMENT_METHODS),
        })
    return rows


def _rupiah(value: str) -> str:
    return "Rp " + f"{int(value):,}".replace(",", ".")


def _corrupt_transaction(rng: random.Random, column: str, value: str) -> str:
    if column in ("Harga Satuan", "Total"):
        if column == "Total" and int(value) in NUMBER_WORDS:
            return NUMBER_WORDS[int(value)]               # amount written in words
        return rng.choice([_rupiah(value), f"{int(value):,}".replace(",", "."), f"Rp{value}"])
    if column == "Tanggal Transaksi":
        d = date.fromisoformat(value)
        return rng.choice([f"{d:%d/%m/%Y}", f"{d:%d/%m/%Y}", f"{d:%Y/%m/%d}", f"{d.year}-{d.month:02d}-32"])
    if column == "Qty":
        return f"{value} pcs" if rng.random() < 0.7 else ""
    if column == "Produk":
        return f" {value}  "
    return value


def transactions_rows(branch: Branch, day: date) -> list[dict[str, str]]:
    rng = _rng("trx-file", branch.code, day)
    rows = _clean_transactions(branch, day)
    for row in rows:
        for column in ("Produk", "Qty", "Harga Satuan", "Total", "Tanggal Transaksi"):
            if rng.random() < TRANSACTION_ERROR_RATE:
                row[column] = _corrupt_transaction(rng, column, row[column])
        if rng.random() < 0.004:  # a cashier typed the total by hand
            row["Total"] = str(int(row["Total"]) + rng.choice((-1000, 500, 10_000))) if row["Total"].isdigit() else row["Total"]
    # Late arrivals: a few of yesterday's sales are exported again today (same transaction id).
    if day > EPOCH and rng.random() < 0.3:
        yesterday = _clean_transactions(branch, day - timedelta(days=1))
        rows.extend(rng.sample(yesterday, k=min(2, len(yesterday))))
    if rng.random() < 0.2:
        rows.append(dict(rng.choice(rows)))
    return rows


def to_csv(rows: list[dict[str, str]], columns: tuple[str, ...]) -> bytes:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")


def drop_day(landing: Path, day: date, branches: tuple[Branch, ...] = BRANCHES) -> list[Path]:
    """Write one business day of files for every branch. Returns the paths written.

    Files are written to a temporary name and renamed, so a consumer never picks up half a file.
    """
    if day < EPOCH:
        raise ValueError(f"business date must be on or after {EPOCH}")
    written = []
    for branch in branches:
        folder = landing / day.isoformat() / branch.code
        folder.mkdir(parents=True, exist_ok=True)
        for name, content in (
            ("customers.csv", to_csv(customers_rows(branch, day), CUSTOMER_COLUMNS)),
            ("transactions.csv", to_csv(transactions_rows(branch, day), TRANSACTION_COLUMNS)),
        ):
            target = folder / name
            tmp = folder / f".{name}.part"
            tmp.write_bytes(content)
            tmp.replace(target)
            written.append(target)
    return written


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument("--out", type=Path, default=Path("landing"))
    args = parser.parse_args()
    total = sum(len(drop_day(args.out, args.start + timedelta(days=i))) for i in range(args.days))
    print(f"wrote {total} files to {args.out}")
