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
from typing import Optional


from scripts.work_folder.lib.normalize.common import (
    normalize_insurance_number_match,
    normalize_insurance_symbol_match,
)
from scripts.work_folder.lib.etl.metrics import RunMetrics
from scripts.work_folder.lib.errors import NormalizeError

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


def normalize_name_match(value: str) -> str:
    """氏名の match 用正規化（前後空白除去 + 連続空白整理）"""
    if not value:
        return ""
    value = " ".join(value.replace("　", " ").split())
    return value.strip()



def parse_date_ymd(value: str) -> Optional[str]:
    """YYYY-MM-DD / YYYY/MM/DD / 空値を date 文字列へ寄せる。"""
    if not value:
        return None
    s = value.strip().replace("/", "-")
    if s == "":
        return None
    return s



def parse_int_or_none(value: str) -> Optional[int]:
    if value is None:
        return None
    s = str(value).strip()
    if s == "":
        return None
    return int(s)



def split_reminder_datetimes(value: str) -> list[str]:
    """受診勧奨送信日時を `|` 区切りで分解する。"""
    if not value:
        return []
    return [part.strip() for part in value.split("|") if part.strip()]


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

def build_row_sha(normalized_row: dict) -> str:
    """比較対象となる正規化済み値から row_sha256 を作る。"""

    ordered_values = [
        normalized_row.get("status", ""),
        normalized_row.get("name_match", ""),
        normalized_row.get("insurance_symbol_match", ""),
        normalized_row.get("insurance_number_match", ""),
        normalized_row.get("relationship_match", ""),
        normalized_row.get("company_name", ""),
        normalized_row.get("department_name", ""),
        normalized_row.get("medical_institution", ""),
        normalized_row.get("course_name", ""),
        normalized_row.get("reservation_date") or "",
        normalized_row.get("exam_date") or "",
        normalized_row.get("employee_number", ""),
        normalized_row.get("email", ""),
        str(normalized_row.get("reminder_send_count") or ""),
        normalized_row.get("exclusion_reason", ""),
    ]

    raw = "|".join(str(v) for v in ordered_values)
    return sha256_text(raw)


def normalize_dashboard_row(row: dict, insurer_number: str) -> dict:
    """CSV 1行を正規化して comparison / insert 用 dict を返す。"""

    symbol_match = normalize_insurance_symbol_match(
        row.get("被保険者記号", "")
    ) or ""
    number_match = normalize_insurance_number_match(
        row.get("被保険者番号", "")
    ) or ""
    relation_match = normalize_relation_match(row.get("続柄", ""))
    name_match = normalize_name_match(row.get("氏名", ""))

    normalized = {
        "insurer_number": insurer_number,
        "status": (row.get("ステータス", "") or "").strip(),
        "name": (row.get("氏名", "") or "").strip(),
        "name_match": name_match,
        "insurance_symbol": (row.get("被保険者記号", "") or "").strip(),
        "insurance_number": (row.get("被保険者番号", "") or "").strip(),
        "branch_number": (row.get("枝番", "") or "").strip() or None,
        "insured_type": (row.get("被保険者分類", "") or "").strip(),
        "relationship": (row.get("続柄", "") or "").strip(),
        "insurance_symbol_match": symbol_match,
        "insurance_number_match": number_match,
        "relationship_match": relation_match,
        "company_name": (row.get("企業名", "") or "").strip(),
        "department_name": (row.get("部署名", "") or "").strip(),
        "medical_institution": (row.get("医療機関", "") or "").strip(),
        "course_name": (row.get("対象コース名", "") or "").strip(),
        "reservation_date": parse_date_ymd(row.get("予約日", "") or ""),
        "exam_date": parse_date_ymd(row.get("受診日", "") or ""),
        "employee_number": (row.get("社員番号", "") or "").strip(),
        "email": (row.get("メールアドレス", "") or "").strip(),
        "reminder_send_count": parse_int_or_none(row.get("受診勧奨送信回数", "")),
        "reminder_send_datetimes": split_reminder_datetimes(
            row.get("受診勧奨送信日時", "") or ""
        ),
        "exclusion_reason": (row.get("除外理由", "") or "").strip(),
    }

    normalized["snapshot_identity_key"] = build_snapshot_key(
        insurer_number,
        symbol_match,
        number_match,
        relation_match,
    )
    normalized["row_sha256"] = build_row_sha(normalized)

    return normalized


# ------------------------------------------------------------
# CSV読み込み
# ------------------------------------------------------------

def process_csv(csv_path: Path, insurer_number: str, metrics: RunMetrics):

    print(f"CSV処理開始: {csv_path}")

    metrics.files += 1

    with open(csv_path, "r", encoding="utf-8-sig") as f:

        reader = csv.DictReader(f)

        for i, row in enumerate(reader, start=1):

            metrics.rows_seen += 1

            try:
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

            except NormalizeError as e:
                metrics.errors += 1
                print(f"normalize error row={i}: {e}")
                continue

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
    metrics = RunMetrics()

    for insurer_dir in INPUT_BASE.iterdir():

        if not insurer_dir.is_dir():
            continue

        insurer_number = insurer_dir.name

        for csv_path in insurer_dir.glob("*.csv"):

            process_csv(csv_path, insurer_number, metrics)

    print("metrics:", metrics)


if __name__ == "__main__":

    start = datetime.now()

    print("=== HIA dashboard CSV import start ===")

    main()

    print("finished", datetime.now() - start)