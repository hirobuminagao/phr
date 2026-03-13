#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
hia_import_dashboard_csv.py

HIA ダッシュボードCSV 取り込みスクリプト（v1）

責務:
- data/hia_export/input_dashboard_csv/<insurer_number>/ からCSVを取得
- CSVを1行ずつ読み込み
- match用の正規化値を生成
- snapshot_identity_key を生成
- row_sha256 を生成
- hia_dashboard_status と比較
- 新規 / 変更 / 変更なし を判定
- 変更があれば
    - history テーブルへ記帳
    - status テーブル更新
- etl_runs / etl_errors にログ

※ まだDB書き込みロジックは骨格のみ
"""

from __future__ import annotations

import csv
import hashlib
from pathlib import Path
from datetime import datetime


from scripts.work_folder.lib.normalize.common import (
    normalize_insurance_number_match,
    normalize_insurance_symbol_match,
)

# ------------------------------------------------------------
# 設定
# ------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[3]
INPUT_BASE = BASE_DIR / "data" / "hia_export" / "input_dashboard_csv"


# ------------------------------------------------------------
# 正規化関数
# ------------------------------------------------------------

def normalize_relation_match(value: str) -> str:
    """続柄の match 用正規化（trim ベース）"""
    if not value:
        return ""
    return value.strip()


# ------------------------------------------------------------
# SHA生成
# ------------------------------------------------------------

def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ------------------------------------------------------------
# snapshot key
# ------------------------------------------------------------

def build_snapshot_key(insurer_number: str, symbol_match: str, number_match: str, relation_match: str) -> str:
    """
    人識別キー
    """

    key = f"{insurer_number}|{symbol_match}|{number_match}|{relation_match}"

    return sha256_text(key)


# ------------------------------------------------------------
# row sha
# ------------------------------------------------------------

def build_row_sha(row: dict) -> str:

    raw = "|".join(str(v) for v in row.values())

    return sha256_text(raw)


# ------------------------------------------------------------
# CSV読み込み
# ------------------------------------------------------------

def process_csv(csv_path: Path, insurer_number: str):

    print(f"CSV処理開始: {csv_path}")

    with open(csv_path, "r", encoding="utf-8-sig") as f:

        reader = csv.DictReader(f)

        for i, row in enumerate(reader, start=1):

            symbol_match = normalize_insurance_symbol_match(
                row.get("被保険者記号", "")
            ) or ""
            number_match = normalize_insurance_number_match(
                row.get("被保険者番号", "")
            ) or ""
            relation_match = normalize_relation_match(row.get("続柄", ""))

            snapshot_key = build_snapshot_key(
                insurer_number,
                symbol_match,
                number_match,
                relation_match,
            )

            row_sha = build_row_sha(row)

            # ------------------------------------------------
            # TODO
            # DB照合
            # ------------------------------------------------

            print(
                i,
                symbol_match,
                number_match,
                relation_match,
                snapshot_key[:8],
                row_sha[:8],
            )


# ------------------------------------------------------------
# main
# ------------------------------------------------------------


def main():

    for insurer_dir in INPUT_BASE.iterdir():

        if not insurer_dir.is_dir():
            continue

        insurer_number = insurer_dir.name

        for csv_path in insurer_dir.glob("*.csv"):

            process_csv(csv_path, insurer_number)


if __name__ == "__main__":

    start = datetime.now()

    print("=== HIA dashboard CSV import start ===")

    main()

    print("finished", datetime.now() - start)