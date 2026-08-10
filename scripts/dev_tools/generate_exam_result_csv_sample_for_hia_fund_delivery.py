#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_CSV = (
    PROJECT_ROOT
    / "docs"
    / "spec"
    / "exam_result_csv_import"
    / "samples"
    / "murakami_iin"
    / "murakami_iin_paper_sample_001.csv"
)
DEFAULT_OUTPUT = (
    PROJECT_ROOT
    / "docs"
    / "spec"
    / "hia_fund_ledger_xml"
    / "samples"
    / "csv_import"
    / "murakami_iin_event2_sample_001.csv"
)


SAMPLE_REPLACEMENTS = [
    {
        "CSV_Header": "1",
        "NAME_FULL": "サンプル 太郎",
        "NAME_KANA": "サンプル タロウ",
        "POSTALCODE": "060-0001",
        "ADDRESS": "北海道札幌市中央区北一条西１－１－１",
        "HEALTH_EXAMINATION_DATE": "2026/5/15",
        "BIRTHDAY": "1978/4/12",
        "GENDER": "男性",
        "INSURER_NUMBER": "6139463",
        "INSURANCE_CARD_SYMBOL": "100",
        "INSURANCE_CARD_NUMBER": "700001",
    },
    {
        "CSV_Header": "2",
        "NAME_FULL": "サンプル 花子",
        "NAME_KANA": "サンプル ハナコ",
        "POSTALCODE": "060-0002",
        "ADDRESS": "北海道札幌市中央区北二条西２－２－２",
        "HEALTH_EXAMINATION_DATE": "2026/5/20",
        "BIRTHDAY": "1984/9/21",
        "GENDER": "女性",
        "INSURER_NUMBER": "6139463",
        "INSURANCE_CARD_SYMBOL": "100",
        "INSURANCE_CARD_NUMBER": "700002",
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a CSV import sample that matches the HIA fund delivery subscriber fixtures.",
    )
    parser.add_argument("--source", type=Path, default=SOURCE_CSV)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    with args.source.open("r", encoding="utf-8-sig", newline="") as fp:
        rows = list(csv.reader(fp))

    if len(rows) < 4:
        raise ValueError(f"source CSV has too few rows: {args.source}")

    header = rows[0]
    context = rows[1]
    source_data_rows = rows[2:4]
    index = {name: i for i, name in enumerate(header)}

    output_rows = [header, context]
    for source_row, replacements in zip(source_data_rows, SAMPLE_REPLACEMENTS, strict=True):
        row = list(source_row)
        for field, value in replacements.items():
            row[index[field]] = value
        output_rows.append(row)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.writer(fp, lineterminator="\n")
        writer.writerows(output_rows)

    print(f"generated={args.output}")
    print(f"rows={len(output_rows)} cols={len(header)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
