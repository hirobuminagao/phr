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

import sys
from pathlib import Path

# ------------------------------------------------------------
# ファイル直実行でも project root を import path に追加
# ------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import csv
import hashlib
import json
import re
import unicodedata

from datetime import datetime
from typing import Optional
from typing import Any


from scripts.work_folder.lib.normalize.common import (
    normalize_insurance_number_match,
    normalize_insurance_symbol_match,
    normalize_name_kanji_match,
)
from scripts.work_folder.lib.etl.metrics import RunMetrics
from scripts.work_folder.lib.errors import NormalizeError
from scripts.work_folder.lib.db.config import load_mysql_params
from scripts.work_folder.lib.db.mysql import connect_ctx, dict_cursor
from scripts.work_folder.lib.etl.errors import log_normalize_error
from scripts.work_folder.lib.etl.runs import finish_run, start_run

# ------------------------------------------------------------
# 設定
# ------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[3]
INPUT_BASE = BASE_DIR / "data" / "hia_export" / "input_dashboard_csv"

COMPARE_COLUMNS = [
    "status",
    "name",
    "name_match",
    "subscriber_person_id_custom",
    "subscriber_name_kana_full",
    "subscriber_name_kana_full_match",
    "subscriber_gender_code",
    "subscriber_birth",
    "insurance_symbol",
    "insurance_number",
    "branch_number",
    "insured_type",
    "relationship",
    "insurance_symbol_match",
    "insurance_number_match",
    "relationship_match",
    "company_name",
    "department_name",
    "medical_institution",
    "course_name",
    "reservation_date",
    "exam_date",
    "employee_number",
    "email",
    "reminder_send_count",
    "exclusion_reason",
    "row_sha256",
]
def fetch_subscriber_enrichment(
    cur: Any,
    insurer_number: str,
    insurance_symbol_match: str,
    insurance_number_match: str,
    name_full_match: str,
) -> dict:
    """dev_phr.subscribers から dashboard 補完用の人物情報を取得する。"""
    sql = """
        SELECT
            person_id_custom,
            name_kana_full,
            name_kana_full_match,
            gender_code,
            birth
        FROM dev_phr.subscribers
        WHERE insurer_number = %s
          AND insurance_symbol_match = %s
          AND insurance_number_match = %s
          AND name_full_match = %s
        LIMIT 1
    """
    cur.execute(
        sql,
        (
            insurer_number,
            insurance_symbol_match,
            insurance_number_match,
            name_full_match,
        ),
    )
    row = cur.fetchone()
    if not row:
        return {
            "subscriber_person_id_custom": None,
            "subscriber_name_kana_full": None,
            "subscriber_name_kana_full_match": None,
            "subscriber_gender_code": None,
            "subscriber_birth": None,
        }

    return {
        "subscriber_person_id_custom": row.get("person_id_custom"),
        "subscriber_name_kana_full": row.get("name_kana_full"),
        "subscriber_name_kana_full_match": row.get("name_kana_full_match"),
        "subscriber_gender_code": row.get("gender_code"),
        "subscriber_birth": row.get("birth"),
    }



def normalize_relation_match(value: str) -> str:
    """続柄の match 用正規化（trim ベース）"""
    if not value:
        return ""
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


def format_file_mtime(dt: datetime) -> str:
    """etl_runs.notes 用のファイル更新日時文字列"""
    return dt.strftime("%Y-%m-%d %H:%M:%S")



def build_run_notes(csv_path: Path) -> str:
    """CSV 単位 run の notes 文字列を作る。"""
    stat = csv_path.stat()
    file_mtime = format_file_mtime(datetime.fromtimestamp(stat.st_mtime))
    return f"filename={csv_path.name} file_mtime={file_mtime}"


# ------------------------------------------------------------
# SHA生成
# ------------------------------------------------------------

def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ------------------------------------------------------------
# snapshot key
# ------------------------------------------------------------

# NOTE:
# 現状のダッシュボードCSVでは
# insurer_number + symbol_match + number_match + relationship_match + name_match
# を人物識別キーとして扱う。
# 氏名違いを同一人物として束ねないため、name_match も identity に含める。
def build_snapshot_key(
    insurer_number: str,
    symbol_match: str,
    number_match: str,
    relation_match: str,
    name_match: str,
) -> str:
    """
    人識別キー
    """

    key = f"{insurer_number}|{symbol_match}|{number_match}|{relation_match}|{name_match}"

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


def normalize_dashboard_row(row: dict, insurer_number: str, cur: Any) -> dict:
    """CSV 1行を正規化して comparison / insert 用 dict を返す。"""

    symbol_match = normalize_insurance_symbol_match(
        row.get("被保険者記号", "")
    ) or ""
    number_match = normalize_insurance_number_match(
        row.get("被保険者番号", "")
    ) or ""
    relation_match = normalize_relation_match(row.get("続柄", ""))
    name_match = normalize_name_kanji_match(row.get("氏名", ""), cur=cur) or ""

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
        name_match,
    )
    normalized["row_sha256"] = build_row_sha(normalized)

    return normalized


def build_raw_row_json(row: dict) -> str:
    """元CSV 1行を JSON 文字列で保持する。"""
    return json.dumps(row, ensure_ascii=False, sort_keys=True)



def build_status_record(normalized: dict, run_id: int, raw_row_json: str) -> dict:
    """hia_dashboard_status insert / update 用 dict を作る。"""
    return {
        "snapshot_identity_key": normalized["snapshot_identity_key"],
        "insurer_number": normalized["insurer_number"],
        "insurance_symbol": normalized["insurance_symbol"],
        "insurance_number": normalized["insurance_number"],
        "relationship": normalized["relationship"],
        "branch_number": normalized["branch_number"],
        "insurance_symbol_match": normalized["insurance_symbol_match"],
        "insurance_number_match": normalized["insurance_number_match"],
        "relationship_match": normalized["relationship_match"],
        "name": normalized["name"],
        "name_match": normalized["name_match"],
        "subscriber_person_id_custom": normalized["subscriber_person_id_custom"],
        "subscriber_name_kana_full": normalized["subscriber_name_kana_full"],
        "subscriber_name_kana_full_match": normalized["subscriber_name_kana_full_match"],
        "subscriber_gender_code": normalized["subscriber_gender_code"],
        "subscriber_birth": normalized["subscriber_birth"],
        "status": normalized["status"],
        "reservation_date": normalized["reservation_date"],
        "exam_date": normalized["exam_date"],
        "company_name": normalized["company_name"],
        "department_name": normalized["department_name"],
        "medical_institution": normalized["medical_institution"],
        "course_name": normalized["course_name"],
        "employee_number": normalized["employee_number"],
        "email": normalized["email"],
        "reminder_send_count": normalized["reminder_send_count"],
        "exclusion_reason": normalized["exclusion_reason"],
        "row_sha256": normalized["row_sha256"],
        "first_seen_run_id": run_id,
        "last_seen_run_id": run_id,
        "raw_row_json": raw_row_json,
    }



def diff_status_columns(existing: dict, normalized: dict) -> list[dict]:
    """existing と normalized を比較して column 単位の差分一覧を返す。"""
    diffs: list[dict] = []

    for col in COMPARE_COLUMNS:
        old_val = existing.get(col)
        new_val = normalized.get(col)

        if old_val != new_val:
            diffs.append(
                {
                    "column_name": col,
                    "old_value": None if old_val is None else str(old_val),
                    "new_value": None if new_val is None else str(new_val),
                }
            )

    return diffs



def build_reminder_event_records(
    hia_dashboard_person_id: int,
    run_id: int,
    normalized: dict,
) -> list[dict]:
    """hia_dashboard_reminder_events insert 用 dict 配列を作る。"""
    rows: list[dict] = []

    for sent_at in normalized.get("reminder_send_datetimes", []):
        rows.append(
            {
                "hia_dashboard_person_id": hia_dashboard_person_id,
                "run_id": run_id,
                "sent_at": sent_at,
            }
        )

    return rows


def fetch_existing_status(cur: Any, snapshot_identity_key: str) -> Optional[dict]:
    """snapshot_identity_key で現在状態を1件取得する。"""
    sql = """
        SELECT
            hia_dashboard_person_id,
            snapshot_identity_key,
            insurer_number,
            insurance_symbol,
            insurance_number,
            relationship,
            branch_number,
            insurance_symbol_match,
            insurance_number_match,
            relationship_match,
            name,
            name_match,
            subscriber_person_id_custom,
            subscriber_name_kana_full,
            subscriber_name_kana_full_match,
            subscriber_gender_code,
            subscriber_birth,
            status,
            reservation_date,
            exam_date,
            company_name,
            department_name,
            medical_institution,
            course_name,
            employee_number,
            email,
            reminder_send_count,
            exclusion_reason,
            row_sha256,
            first_seen_run_id,
            last_seen_run_id,
            created_at,
            updated_at
        FROM work_other.hia_dashboard_status
        WHERE snapshot_identity_key = %s
    """
    cur.execute(sql, (snapshot_identity_key,))
    return cur.fetchone()



def insert_status(cur: Any, status_record: dict) -> int:
    """hia_dashboard_status に新規登録し、採番IDを返す。"""
    sql = """
        INSERT INTO work_other.hia_dashboard_status (
            snapshot_identity_key,
            insurer_number,
            insurance_symbol,
            insurance_number,
            relationship,
            branch_number,
            insurance_symbol_match,
            insurance_number_match,
            relationship_match,
            name,
            name_match,
            subscriber_person_id_custom,
            subscriber_name_kana_full,
            subscriber_name_kana_full_match,
            subscriber_gender_code,
            subscriber_birth,
            status,
            reservation_date,
            exam_date,
            company_name,
            department_name,
            medical_institution,
            course_name,
            employee_number,
            email,
            reminder_send_count,
            exclusion_reason,
            row_sha256,
            first_seen_run_id,
            last_seen_run_id
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s
        )
    """
    cur.execute(
        sql,
        (
            status_record["snapshot_identity_key"],
            status_record["insurer_number"],
            status_record["insurance_symbol"],
            status_record["insurance_number"],
            status_record["relationship"],
            status_record["branch_number"],
            status_record["insurance_symbol_match"],
            status_record["insurance_number_match"],
            status_record["relationship_match"],
            status_record["name"],
            status_record["name_match"],
            status_record["subscriber_person_id_custom"],
            status_record["subscriber_name_kana_full"],
            status_record["subscriber_name_kana_full_match"],
            status_record["subscriber_gender_code"],
            status_record["subscriber_birth"],
            status_record["status"],
            status_record["reservation_date"],
            status_record["exam_date"],
            status_record["company_name"],
            status_record["department_name"],
            status_record["medical_institution"],
            status_record["course_name"],
            status_record["employee_number"],
            status_record["email"],
            status_record["reminder_send_count"],
            status_record["exclusion_reason"],
            status_record["row_sha256"],
            status_record["first_seen_run_id"],
            status_record["last_seen_run_id"],
        ),
    )
    return int(cur.lastrowid)



def touch_last_seen_run(cur: Any, hia_dashboard_person_id: int, run_id: int) -> None:
    """変更なしでも last_seen_run_id / updated_at は更新する。"""
    sql = """
        UPDATE work_other.hia_dashboard_status
        SET last_seen_run_id = %s,
            updated_at = CURRENT_TIMESTAMP(3)
        WHERE hia_dashboard_person_id = %s
    """
    cur.execute(sql, (run_id, hia_dashboard_person_id))



def update_status(cur: Any, hia_dashboard_person_id: int, status_record: dict, run_id: int) -> None:
    """hia_dashboard_status の最新状態を更新する。"""
    sql = """
        UPDATE work_other.hia_dashboard_status
        SET insurer_number = %s,
            insurance_symbol = %s,
            insurance_number = %s,
            relationship = %s,
            branch_number = %s,
            insurance_symbol_match = %s,
            insurance_number_match = %s,
            relationship_match = %s,
            name = %s,
            name_match = %s,
            subscriber_person_id_custom = %s,
            subscriber_name_kana_full = %s,
            subscriber_name_kana_full_match = %s,
            subscriber_gender_code = %s,
            subscriber_birth = %s,
            status = %s,
            reservation_date = %s,
            exam_date = %s,
            company_name = %s,
            department_name = %s,
            medical_institution = %s,
            course_name = %s,
            employee_number = %s,
            email = %s,
            reminder_send_count = %s,
            exclusion_reason = %s,
            row_sha256 = %s,
            last_seen_run_id = %s,
            updated_at = CURRENT_TIMESTAMP(3)
        WHERE hia_dashboard_person_id = %s
    """
    cur.execute(
        sql,
        (
            status_record["insurer_number"],
            status_record["insurance_symbol"],
            status_record["insurance_number"],
            status_record["relationship"],
            status_record["branch_number"],
            status_record["insurance_symbol_match"],
            status_record["insurance_number_match"],
            status_record["relationship_match"],
            status_record["name"],
            status_record["name_match"],
            status_record["subscriber_person_id_custom"],
            status_record["subscriber_name_kana_full"],
            status_record["subscriber_name_kana_full_match"],
            status_record["subscriber_gender_code"],
            status_record["subscriber_birth"],
            status_record["status"],
            status_record["reservation_date"],
            status_record["exam_date"],
            status_record["company_name"],
            status_record["department_name"],
            status_record["medical_institution"],
            status_record["course_name"],
            status_record["employee_number"],
            status_record["email"],
            status_record["reminder_send_count"],
            status_record["exclusion_reason"],
            status_record["row_sha256"],
            run_id,
            hia_dashboard_person_id,
        ),
    )



def insert_history_rows(cur: Any, hia_dashboard_person_id: int, run_id: int, diffs: list[dict]) -> None:
    """差分を hia_dashboard_status_history へ列単位で記録する。"""
    if not diffs:
        return

    sql = """
        INSERT INTO work_other.hia_dashboard_status_history (
            hia_dashboard_person_id,
            run_id,
            column_name,
            old_value,
            new_value
        ) VALUES (%s, %s, %s, %s, %s)
    """
    params = [
        (
            hia_dashboard_person_id,
            run_id,
            diff["column_name"],
            diff["old_value"],
            diff["new_value"],
        )
        for diff in diffs
    ]
    cur.executemany(sql, params)



def insert_reminder_events(cur: Any, rows: list[dict]) -> None:
    """reminder events を重複無視で登録する。"""
    if not rows:
        return

    sql = """
        INSERT IGNORE INTO work_other.hia_dashboard_reminder_events (
            hia_dashboard_person_id,
            run_id,
            sent_at
        ) VALUES (%s, %s, %s)
    """
    params = [
        (
            row["hia_dashboard_person_id"],
            row["run_id"],
            row["sent_at"],
        )
        for row in rows
    ]
    cur.executemany(sql, params)

# ------------------------------------------------------------
# CSV読み込み
# ------------------------------------------------------------

def process_csv(
    csv_path: Path,
    insurer_number: str,
    cur: Any,
    run_id: int,
) -> RunMetrics:

    print(f"CSV処理開始: {csv_path}")

    notes = build_run_notes(csv_path)
    print("run notes:", notes)

    metrics = RunMetrics()
    metrics.files += 1

    with open(csv_path, "r", encoding="utf-8-sig") as f:

        reader = csv.DictReader(f)

        for i, row in enumerate(reader, start=1):

            metrics.rows_seen += 1

            try:
                normalized = normalize_dashboard_row(row, insurer_number, cur)
                raw_row_json = build_raw_row_json(row)
                subscriber_enrichment = fetch_subscriber_enrichment(
                    cur,
                    insurer_number=insurer_number,
                    insurance_symbol_match=normalized["insurance_symbol_match"],
                    insurance_number_match=normalized["insurance_number_match"],
                    name_full_match=normalized["name_match"],
                )
                normalized.update(subscriber_enrichment)

                status_record = build_status_record(
                    normalized,
                    run_id=run_id,
                    raw_row_json=raw_row_json,
                )

            except NormalizeError as e:
                metrics.errors += 1
                print(f"normalize error row={i}: {e}")
                log_normalize_error(
                    cur,
                    run_id=run_id,
                    phase="import",
                    source="hia_fund_dashboard_csv",
                    insurer_number=insurer_number,
                    src_file=str(csv_path),
                    row_no=i,
                    line_no=i,
                    err=e,
                )
                continue

            # normalized には insert / update 判定に必要な値がすべて入っている
            # status_record は hia_dashboard_status へそのまま流せる形
            # diff_status_columns(existing, normalized) で履歴行を作れる
            # build_reminder_event_records(...) で reminder_events 行を作れる
            existing = fetch_existing_status(
                cur,
                normalized["snapshot_identity_key"],
            )

            if existing is None:
                hia_dashboard_person_id = insert_status(cur, status_record)
                metrics.rows_inserted += 1

            else:
                hia_dashboard_person_id = int(existing["hia_dashboard_person_id"])

                diffs = diff_status_columns(existing, normalized)

                if not diffs:
                    touch_last_seen_run(cur, hia_dashboard_person_id, run_id)
                    metrics.rows_unchanged += 1

                else:
                    insert_history_rows(
                        cur,
                        hia_dashboard_person_id,
                        run_id,
                        diffs,
                    )
                    update_status(
                        cur,
                        hia_dashboard_person_id,
                        status_record,
                        run_id,
                    )
                    metrics.rows_updated += 1

            reminder_rows = build_reminder_event_records(
                hia_dashboard_person_id,
                run_id,
                normalized,
            )
            insert_reminder_events(cur, reminder_rows)

            print(
                i,
                normalized["insurance_symbol_match"],
                normalized["insurance_number_match"],
                normalized["relationship_match"],
                normalized["snapshot_identity_key"][:8],
                normalized["row_sha256"][:8],
            )

    return metrics


# ------------------------------------------------------------
# main
# ------------------------------------------------------------


def main():

    params = load_mysql_params()

    with connect_ctx(params) as conn:
        cur = dict_cursor(conn)

        for insurer_dir in INPUT_BASE.iterdir():

            if not insurer_dir.is_dir():
                continue

            insurer_number = insurer_dir.name

            for csv_path in insurer_dir.glob("*.csv"):

                run_id = start_run(
                    cur,
                    phase="import",
                    source="hia_fund_dashboard_csv",
                    db_schema="work_other",
                    db_path="",
                    insurer_number=insurer_number,
                    input_base=str(insurer_dir),
                    input_file=str(csv_path),
                    dry_run=False,
                    limit_rows=None,
                )
                conn.commit()

                try:
                    metrics = process_csv(
                        csv_path,
                        insurer_number,
                        cur=cur,
                        run_id=run_id,
                    )
                    finish_run(
                        cur,
                        run_id,
                        metrics,
                        status_override="success",
                    )
                    conn.commit()
                    print("metrics:", metrics)

                except Exception as e:
                    metrics = RunMetrics()
                    metrics.errors += 1
                    finish_run(
                        cur,
                        run_id,
                        metrics,
                        status_override="failed",
                        extra_notes=f" exception={e}",
                    )
                    conn.commit()
                    raise


if __name__ == "__main__":

    start = datetime.now()

    print("=== HIA dashboard CSV import start ===")

    main()

    print("finished", datetime.now() - start)