#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.etl import RunMetrics
from scripts.lib.etl import finish_run as etl_finish_run
from scripts.lib.etl import log_error as etl_log_error
from scripts.lib.etl import start_run as etl_start_run
from scripts.lib.identity.base_norm import base_normalize
from scripts.lib.identity.field.insurance_number import normalize_insurance_number
from scripts.lib.identity.field.insurance_symbol import normalize_insurance_symbol
from scripts.lib.identity.field.name_kana import normalize_name_kana_full
from scripts.lib.identity.field.name_kanji import normalize_name_kanji_full


DEFAULT_INPUT_BASE = REPO_ROOT / "data" / "hia_export" / "input_dashboard_csv"
DEFAULT_WORK_DB = "work_other"
DEFAULT_DEV_DB = "dev_phr"
ETL_PHASE = "import"
ETL_SOURCE = "hia_fund_dashboard_csv"

COMPARE_COLUMNS = (
    "status",
    "name",
    "name_match",
    "dashboard_name_kana",
    "dashboard_name_kana_match",
    "subscriber_person_id_custom",
    "subscribers_id",
    "hia_subscriber_id",
    "subscriber_name_kana_full",
    "subscriber_name_kana_full_match",
    "subscriber_gender_code",
    "subscriber_birth",
    "identity_hash",
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
)


@dataclass(frozen=True)
class Config:
    input_base: Path
    input_dir: Path | None
    work_db: str
    dev_db: str
    dry_run: bool
    limit: int
    deactivate_missing: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import HIA dashboard CSV files into work_other.hia_dashboard_status.")
    parser.add_argument("--base", default=str(DEFAULT_INPUT_BASE))
    parser.add_argument("--input", help="Specific insurer-number folder to import.")
    parser.add_argument("--work-db", default=DEFAULT_WORK_DB)
    parser.add_argument("--dev-db", default=DEFAULT_DEV_DB)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument(
        "--partial-import",
        action="store_true",
        help="Treat input as filtered/partial CSV and keep rows not seen in the import active.",
    )
    parser.add_argument("--db-prefix", default="PHR_DB_")
    return parser.parse_args()


def qname(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", name):
        raise ValueError(f"invalid SQL identifier: {name!r}")
    return f"`{name}`"


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def text_or_none(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def parse_date_ymd(value: Any) -> str | None:
    text = text_or_none(value)
    if text is None:
        return None
    normalized = text.replace("/", "-")
    if re.fullmatch(r"\d{8}", normalized):
        return datetime.strptime(normalized, "%Y%m%d").strftime("%Y-%m-%d")
    if re.fullmatch(r"\d{4}-\d{1,2}-\d{1,2}", normalized):
        return datetime.strptime(normalized, "%Y-%m-%d").strftime("%Y-%m-%d")
    return normalized


def parse_int_or_none(value: Any) -> int | None:
    text = text_or_none(value)
    if text is None:
        return None
    return int(text)


def parse_reminder_datetime(value: str) -> str | None:
    text = text.strip()
    if not text:
        return None
    normalized = text.replace("/", "-")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    return normalized


def split_reminder_datetimes(value: Any) -> list[str]:
    text = text_or_none(value)
    if text is None:
        return []
    return [parsed for part in text.split("|") if (parsed := parse_reminder_datetime(part))]


def normalize_relation_match(value: Any) -> str:
    return str(value or "").strip()


def build_snapshot_key(row: dict[str, Any]) -> str:
    hia_subscriber_id = text_or_none(row.get("hia_subscriber_id"))
    if hia_subscriber_id:
        return sha256_text(f"{row['insurer_number']}|HIA_SUBSCRIBER_ID|{hia_subscriber_id}")
    return build_legacy_snapshot_key(row)


def build_legacy_snapshot_key(row: dict[str, Any]) -> str:
    return sha256_text(
        "|".join(
            str(row.get(key) or "")
            for key in (
                "insurer_number",
                "insurance_symbol_match",
                "insurance_number_match",
                "relationship_match",
                "name_match",
            )
        )
    )


def build_row_sha(row: dict[str, Any]) -> str:
    return sha256_text("|".join(str(row.get(col) or "") for col in COMPARE_COLUMNS))


def fetch_subscriber_by_hia_id(cur: Any, *, dev_db: str, hia_subscriber_id: str) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT
          id AS subscribers_id,
          person_id_custom,
          hia_subscriber_id,
          name_kana_full,
          name_kana_full_match,
          gender_code,
          birth,
          identity_hash
        FROM {qname(dev_db)}.subscribers
        WHERE hia_subscriber_id = %s
        LIMIT 1
        """,
        (hia_subscriber_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def fetch_subscriber_by_legacy_key(
    cur: Any,
    *,
    dev_db: str,
    insurer_number: str,
    insurance_symbol_match: str | None,
    insurance_number_match: str | None,
    name_full_match: str | None,
) -> dict[str, Any] | None:
    if not insurance_symbol_match or not insurance_number_match or not name_full_match:
        return None
    cur.execute(
        f"""
        SELECT
          id AS subscribers_id,
          person_id_custom,
          hia_subscriber_id,
          name_kana_full,
          name_kana_full_match,
          gender_code,
          birth,
          identity_hash
        FROM {qname(dev_db)}.subscribers
        WHERE insurer_number = %s
          AND insurance_symbol_match = %s
          AND insurance_number_match = %s
          AND name_full_match = %s
        LIMIT 1
        """,
        (insurer_number, insurance_symbol_match, insurance_number_match, name_full_match),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def empty_subscriber_enrichment(hia_subscriber_id: str | None) -> dict[str, Any]:
    return {
        "subscriber_person_id_custom": None,
        "subscribers_id": None,
        "hia_subscriber_id": hia_subscriber_id,
        "subscriber_name_kana_full": None,
        "subscriber_name_kana_full_match": None,
        "subscriber_gender_code": None,
        "subscriber_birth": None,
        "identity_hash": None,
    }


def subscriber_enrichment(row: dict[str, Any], subscriber: dict[str, Any] | None) -> dict[str, Any]:
    if not subscriber:
        return empty_subscriber_enrichment(text_or_none(row.get("hia_subscriber_id")))
    return {
        "subscriber_person_id_custom": subscriber.get("person_id_custom"),
        "subscribers_id": subscriber.get("subscribers_id"),
        "hia_subscriber_id": subscriber.get("hia_subscriber_id") or row.get("hia_subscriber_id"),
        "subscriber_name_kana_full": subscriber.get("name_kana_full"),
        "subscriber_name_kana_full_match": subscriber.get("name_kana_full_match"),
        "subscriber_gender_code": subscriber.get("gender_code"),
        "subscriber_birth": subscriber.get("birth"),
        "identity_hash": subscriber.get("identity_hash"),
    }


def normalize_dashboard_row(raw: dict[str, Any], *, insurer_number: str, cur: Any, dev_db: str) -> dict[str, Any]:
    symbol = normalize_insurance_symbol(text_or_none(raw.get("被保険者記号")))
    number = normalize_insurance_number(text_or_none(raw.get("被保険者番号")))
    kanji = normalize_name_kanji_full(text_or_none(raw.get("氏名")), cur=cur)
    kana = normalize_name_kana_full(text_or_none(raw.get("氏名カナ")))
    normalized: dict[str, Any] = {
        "insurer_number": insurer_number,
        "status": text_or_none(raw.get("ステータス")),
        "name": text_or_none(raw.get("氏名")),
        "name_match": kanji.get("match"),
        "dashboard_name_kana": text_or_none(raw.get("氏名カナ")),
        "dashboard_name_kana_match": kana.get("match"),
        "insurance_symbol": text_or_none(raw.get("被保険者記号")),
        "insurance_number": text_or_none(raw.get("被保険者番号")),
        "branch_number": text_or_none(raw.get("枝番")),
        "insured_type": text_or_none(raw.get("被保険者分類")),
        "relationship": text_or_none(raw.get("続柄")),
        "insurance_symbol_match": symbol.get("match"),
        "insurance_number_match": number.get("match"),
        "relationship_match": normalize_relation_match(raw.get("続柄")),
        "company_name": text_or_none(raw.get("企業名")),
        "department_name": text_or_none(raw.get("部署名")),
        "medical_institution": text_or_none(raw.get("医療機関")),
        "course_name": text_or_none(raw.get("対象コース名")),
        "reservation_date": parse_date_ymd(raw.get("予約日")),
        "exam_date": parse_date_ymd(raw.get("受診日")),
        "employee_number": text_or_none(raw.get("社員番号")),
        "email": text_or_none(raw.get("メールアドレス")),
        "reminder_send_count": parse_int_or_none(raw.get("受診勧奨送信回数")),
        "reminder_send_datetimes": split_reminder_datetimes(raw.get("受診勧奨送信日時")),
        "exclusion_reason": text_or_none(raw.get("除外理由")),
        "hia_subscriber_id": text_or_none(raw.get("加入者ID")),
    }
    subscriber = None
    if normalized["hia_subscriber_id"]:
        subscriber = fetch_subscriber_by_hia_id(cur, dev_db=dev_db, hia_subscriber_id=normalized["hia_subscriber_id"])
    if subscriber is None:
        subscriber = fetch_subscriber_by_legacy_key(
            cur,
            dev_db=dev_db,
            insurer_number=insurer_number,
            insurance_symbol_match=normalized["insurance_symbol_match"],
            insurance_number_match=normalized["insurance_number_match"],
            name_full_match=normalized["name_match"],
        )
    normalized.update(subscriber_enrichment(normalized, subscriber))
    normalized["snapshot_identity_key"] = build_snapshot_key(normalized)
    normalized["legacy_snapshot_identity_key"] = build_legacy_snapshot_key(normalized)
    normalized["row_sha256"] = build_row_sha(normalized)
    return normalized


def fetch_existing_status(cur: Any, *, work_db: str, snapshot_identity_key: str) -> dict[str, Any] | None:
    columns = [
        "hia_dashboard_person_id",
        "snapshot_identity_key",
        "insurer_number",
        "insurance_symbol",
        "insurance_number",
        "insured_type",
        "relationship",
        "branch_number",
        "insurance_symbol_match",
        "insurance_number_match",
        "relationship_match",
        "name",
        "name_match",
        "dashboard_name_kana",
        "dashboard_name_kana_match",
        "subscriber_person_id_custom",
        "subscribers_id",
        "hia_subscriber_id",
        "subscriber_name_kana_full",
        "subscriber_name_kana_full_match",
        "subscriber_gender_code",
        "subscriber_birth",
        "identity_hash",
        "status",
        "reservation_date",
        "exam_date",
        "company_name",
        "department_name",
        "medical_institution",
        "course_name",
        "employee_number",
        "email",
        "reminder_send_count",
        "exclusion_reason",
        "row_sha256",
        "is_active",
        "inactive_run_id",
        "inactive_at",
        "inactive_reason",
    ]
    cur.execute(
        f"""
        SELECT {', '.join(f'`{column}`' for column in columns)}
        FROM {qname(work_db)}.hia_dashboard_status
        WHERE snapshot_identity_key = %s
        """,
        (snapshot_identity_key,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def normalize_diff_value(column_name: str, value: Any) -> Any:
    if value is None:
        return None
    if column_name in {"reservation_date", "exam_date", "subscriber_birth"}:
        return str(value)
    return value


def diff_status_columns(existing: dict[str, Any], normalized: dict[str, Any]) -> list[dict[str, Any]]:
    diffs: list[dict[str, Any]] = []
    for column in COMPARE_COLUMNS:
        old_value = normalize_diff_value(column, existing.get(column))
        new_value = normalize_diff_value(column, normalized.get(column))
        if old_value != new_value:
            diffs.append(
                {
                    "column_name": column,
                    "old_value": None if old_value is None else str(old_value),
                    "new_value": None if new_value is None else str(new_value),
                }
            )
    return diffs


def insert_status(cur: Any, *, work_db: str, normalized: dict[str, Any], run_id: int) -> int:
    columns = [
        "snapshot_identity_key",
        "insurer_number",
        "insurance_symbol",
        "insurance_number",
        "insured_type",
        "relationship",
        "branch_number",
        "insurance_symbol_match",
        "insurance_number_match",
        "relationship_match",
        "name",
        "name_match",
        "dashboard_name_kana",
        "dashboard_name_kana_match",
        "subscriber_person_id_custom",
        "subscribers_id",
        "hia_subscriber_id",
        "subscriber_name_kana_full",
        "subscriber_name_kana_full_match",
        "subscriber_gender_code",
        "subscriber_birth",
        "identity_hash",
        "status",
        "reservation_date",
        "exam_date",
        "company_name",
        "department_name",
        "medical_institution",
        "course_name",
        "employee_number",
        "email",
        "reminder_send_count",
        "exclusion_reason",
        "row_sha256",
        "is_active",
        "inactive_run_id",
        "inactive_at",
        "inactive_reason",
        "first_seen_run_id",
        "last_seen_run_id",
    ]
    values = [normalized.get(column) for column in columns[:-6]] + [1, None, None, None, run_id, run_id]
    cur.execute(
        f"""
        INSERT INTO {qname(work_db)}.hia_dashboard_status (
          {', '.join(f'`{column}`' for column in columns)}
        ) VALUES ({', '.join(['%s'] * len(columns))})
        """,
        tuple(values),
    )
    return int(cur.lastrowid)


def update_status(cur: Any, *, work_db: str, hia_dashboard_person_id: int, normalized: dict[str, Any], run_id: int) -> None:
    columns = [
        "snapshot_identity_key",
        "insurer_number",
        "insurance_symbol",
        "insurance_number",
        "insured_type",
        "relationship",
        "branch_number",
        "insurance_symbol_match",
        "insurance_number_match",
        "relationship_match",
        "name",
        "name_match",
        "dashboard_name_kana",
        "dashboard_name_kana_match",
        "subscriber_person_id_custom",
        "subscribers_id",
        "hia_subscriber_id",
        "subscriber_name_kana_full",
        "subscriber_name_kana_full_match",
        "subscriber_gender_code",
        "subscriber_birth",
        "identity_hash",
        "status",
        "reservation_date",
        "exam_date",
        "company_name",
        "department_name",
        "medical_institution",
        "course_name",
        "employee_number",
        "email",
        "reminder_send_count",
        "exclusion_reason",
        "row_sha256",
        "is_active",
        "inactive_run_id",
        "inactive_at",
        "inactive_reason",
    ]
    values = [normalized.get(column) for column in columns[:-4]] + [1, None, None, None, run_id, hia_dashboard_person_id]
    cur.execute(
        f"""
        UPDATE {qname(work_db)}.hia_dashboard_status
        SET {', '.join(f'`{column}` = %s' for column in columns)},
            last_seen_run_id = %s,
            updated_at = CURRENT_TIMESTAMP(3)
        WHERE hia_dashboard_person_id = %s
        """,
        tuple(values),
    )


def touch_status(cur: Any, *, work_db: str, hia_dashboard_person_id: int, run_id: int) -> None:
    cur.execute(
        f"""
        UPDATE {qname(work_db)}.hia_dashboard_status
        SET last_seen_run_id = %s,
            updated_at = CURRENT_TIMESTAMP(3)
        WHERE hia_dashboard_person_id = %s
        """,
        (run_id, hia_dashboard_person_id),
    )


def insert_history_rows(cur: Any, *, work_db: str, hia_dashboard_person_id: int, run_id: int, diffs: list[dict[str, Any]]) -> None:
    if not diffs:
        return
    cur.executemany(
        f"""
        INSERT INTO {qname(work_db)}.hia_dashboard_status_history (
          hia_dashboard_person_id, run_id, column_name, old_value, new_value
        ) VALUES (%s, %s, %s, %s, %s)
        """,
        [
            (
                hia_dashboard_person_id,
                run_id,
                diff["column_name"],
                diff["old_value"],
                diff["new_value"],
            )
            for diff in diffs
        ],
    )


def insert_reminder_events(cur: Any, *, work_db: str, hia_dashboard_person_id: int, run_id: int, normalized: dict[str, Any]) -> None:
    rows = normalized.get("reminder_send_datetimes") or []
    if not rows:
        return
    cur.executemany(
        f"""
        INSERT IGNORE INTO {qname(work_db)}.hia_dashboard_reminder_events (
          hia_dashboard_person_id, run_id, sent_at
        ) VALUES (%s, %s, %s)
        """,
        [(hia_dashboard_person_id, run_id, sent_at) for sent_at in rows],
    )


def deactivate_missing_rows(cur: Any, *, work_db: str, insurer_number: str, run_id: int) -> int:
    cur.execute(
        f"""
        SELECT hia_dashboard_person_id
        FROM {qname(work_db)}.hia_dashboard_status
        WHERE insurer_number = %s
          AND is_active = 1
          AND last_seen_run_id <> %s
        """,
        (insurer_number, run_id),
    )
    target_ids = [int(row["hia_dashboard_person_id"]) for row in cur.fetchall()]
    if not target_ids:
        return 0

    cur.executemany(
        f"""
        INSERT INTO {qname(work_db)}.hia_dashboard_status_history (
          hia_dashboard_person_id, run_id, column_name, old_value, new_value
        ) VALUES (%s, %s, 'is_active', '1', '0')
        """,
        [(row_id, run_id) for row_id in target_ids],
    )

    placeholders = ", ".join(["%s"] * len(target_ids))
    cur.execute(
        f"""
        UPDATE {qname(work_db)}.hia_dashboard_status
        SET is_active = 0,
            inactive_run_id = %s,
            inactive_at = CURRENT_TIMESTAMP(3),
            inactive_reason = 'NOT_IN_LATEST_FULL_IMPORT',
            updated_at = CURRENT_TIMESTAMP(3)
        WHERE hia_dashboard_person_id IN ({placeholders})
        """,
        (run_id, *target_ids),
    )
    return len(target_ids)


def process_csv(cur: Any, *, config: Config, csv_path: Path, insurer_number: str, run_id: int) -> RunMetrics:
    metrics = RunMetrics(files=1)
    with csv_path.open("r", encoding="utf-8-sig", newline="") as fp:
        reader = csv.DictReader(fp)
        for row_no, raw in enumerate(reader, start=1):
            if config.limit and metrics.rows_seen >= config.limit:
                break
            metrics.rows_seen += 1
            try:
                normalized = normalize_dashboard_row(raw, insurer_number=insurer_number, cur=cur, dev_db=config.dev_db)
                existing = fetch_existing_status(
                    cur,
                    work_db=config.work_db,
                    snapshot_identity_key=str(normalized["snapshot_identity_key"]),
                )
                if existing is None and normalized.get("legacy_snapshot_identity_key") != normalized.get("snapshot_identity_key"):
                    existing = fetch_existing_status(
                        cur,
                        work_db=config.work_db,
                        snapshot_identity_key=str(normalized["legacy_snapshot_identity_key"]),
                    )
                if existing is None:
                    hia_dashboard_person_id = 0
                    if not config.dry_run:
                        hia_dashboard_person_id = insert_status(
                            cur,
                            work_db=config.work_db,
                            normalized=normalized,
                            run_id=run_id,
                        )
                    metrics.rows_inserted += 1
                else:
                    hia_dashboard_person_id = int(existing["hia_dashboard_person_id"])
                    if existing.get("row_sha256") == normalized.get("row_sha256"):
                        if not config.dry_run:
                            if existing.get("snapshot_identity_key") != normalized.get("snapshot_identity_key"):
                                update_status(
                                    cur,
                                    work_db=config.work_db,
                                    hia_dashboard_person_id=hia_dashboard_person_id,
                                    normalized=normalized,
                                    run_id=run_id,
                                )
                            else:
                                touch_status(cur, work_db=config.work_db, hia_dashboard_person_id=hia_dashboard_person_id, run_id=run_id)
                        metrics.rows_unchanged += 1
                    else:
                        diffs = diff_status_columns(existing, normalized)
                        if not config.dry_run:
                            insert_history_rows(
                                cur,
                                work_db=config.work_db,
                                hia_dashboard_person_id=hia_dashboard_person_id,
                                run_id=run_id,
                                diffs=diffs,
                            )
                            update_status(
                                cur,
                                work_db=config.work_db,
                                hia_dashboard_person_id=hia_dashboard_person_id,
                                normalized=normalized,
                                run_id=run_id,
                            )
                        metrics.rows_updated += 1
                if not config.dry_run and hia_dashboard_person_id:
                    insert_reminder_events(
                        cur,
                        work_db=config.work_db,
                        hia_dashboard_person_id=hia_dashboard_person_id,
                        run_id=run_id,
                        normalized=normalized,
                    )
            except Exception as exc:
                metrics.errors += 1
                metrics.rows_skipped += 1
                if not config.dry_run:
                    etl_log_error(
                        cur,
                        run_id,
                        phase=ETL_PHASE,
                        source=ETL_SOURCE,
                        insurer_number=insurer_number,
                        src_file=str(csv_path),
                        row_no=row_no,
                        line_no=row_no,
                        field="ROW",
                        field_value=json.dumps(raw, ensure_ascii=False, sort_keys=True),
                        error_code=type(exc).__name__,
                        message=str(exc),
                    )
    return metrics


def iter_target_dirs(config: Config) -> list[Path]:
    if config.input_dir is not None:
        return [config.input_dir]
    if not config.input_base.exists():
        return []
    return sorted(path for path in config.input_base.iterdir() if path.is_dir())


def insurer_number_from_dir(path: Path) -> str:
    text = path.name.strip()
    if not re.fullmatch(r"\d{8}", text):
        raise ValueError(f"input folder must be 8-digit insurer number: {path}")
    return text


def run(config: Config, *, db_prefix: str) -> int:
    params = load_mysql_base_params(db_prefix)
    with connect_ctx(params, database=config.work_db) as conn:
        cur = dict_cursor(conn)
        for target_dir in iter_target_dirs(config):
            insurer_number = insurer_number_from_dir(target_dir)
            for csv_path in sorted(target_dir.glob("*.csv")):
                run_id = etl_start_run(
                    cur,
                    phase=ETL_PHASE,
                    source=ETL_SOURCE,
                    db_schema=config.work_db,
                    db_path="",
                    input_base=str(target_dir),
                    input_file=str(csv_path),
                    insurer_number=insurer_number,
                    dry_run=config.dry_run,
                    limit_rows=config.limit or None,
                )
                conn.commit()
                try:
                    metrics = process_csv(cur, config=config, csv_path=csv_path, insurer_number=insurer_number, run_id=run_id)
                    if config.deactivate_missing:
                        if not config.dry_run:
                            metrics.rows_updated += deactivate_missing_rows(
                                cur,
                                work_db=config.work_db,
                                insurer_number=insurer_number,
                                run_id=run_id,
                            )
                    etl_finish_run(cur, run_id, metrics, status_override="success" if metrics.errors == 0 else None)
                    conn.commit()
                    print(
                        "hia_dashboard_import "
                        f"file={csv_path.name} rows={metrics.rows_seen} inserted={metrics.rows_inserted} "
                        f"updated={metrics.rows_updated} unchanged={metrics.rows_unchanged} errors={metrics.errors} "
                        f"dry_run={config.dry_run}"
                    )
                except Exception as exc:
                    conn.rollback()
                    metrics = RunMetrics(files=1, errors=1)
                    etl_finish_run(cur, run_id, metrics, status_override="failed", extra_notes=f"exception={type(exc).__name__}: {exc}")
                    conn.commit()
                    raise
    return 0


def main() -> int:
    args = parse_args()
    input_base = Path(args.base)
    config = Config(
        input_base=input_base,
        input_dir=None if not args.input else Path(args.input),
        work_db=args.work_db,
        dev_db=args.dev_db,
        dry_run=bool(args.dry_run),
        limit=int(args.limit or 0),
        deactivate_missing=not bool(args.partial_import),
    )
    return run(config, db_prefix=args.db_prefix)


if __name__ == "__main__":
    raise SystemExit(main())
