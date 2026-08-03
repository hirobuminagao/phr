#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Import health exam result CSV files from file_receipts."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping, cast


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.csv.csv_loader import CsvLoadResult, row_sha256
from scripts.lib.csv.exam_result_format_matcher import load_csv_matching_registered_header
from scripts.lib.csv.exam_result_mapping_extractor import ExtractedCsvRuleValue, extract_row_values
from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.lookup.csv_exam_result_mapping import CsvMappingRule, get_csv_format_version_by_id, load_csv_mapping_rules
from scripts.lib.db.lookup.event import get_event_age_rule, get_event_insurer_number
from scripts.lib.db.lookup.exam_item_master import get_exam_item
from scripts.lib.db.lookup.subscriber_identity import resolve_subscriber_identity
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import PHR_MASTER
from scripts.lib.etl import RunMetrics
from scripts.lib.etl import finish_run as etl_finish_run
from scripts.lib.etl import log_error as etl_log_error
from scripts.lib.etl import start_run as etl_start_run
from scripts.lib.examination.value_normalizer import normalize_exam_item_value
from scripts.lib.examination.report_classification import (
    classify_report_codes_by_age,
    resolve_age_reference_date,
)
from scripts.lib.identity.field.gender_code import normalize_gender_code
from scripts.lib.identity.generator import generate_identity_bundle, generate_person_id_custom
from scripts.from_medical.script_lib.basic_info_completion import resolve_basic_info_completion


HEALTH_EXAM_RESULT_DB = "health_exam_result"
DEV_PHR_DB = "dev_phr"
ETL_PHASE = "IMPORT_CSV_EXAM_RESULTS"
ETL_SOURCE = "FROM_MEDICAL"
FILE_STATUS_DISCOVERED = "DISCOVERED"
FILE_STATUS_READY = "READY"
FILE_STATUS_WAITING_CONFIRM = "WAITING_CONFIRM"
FILE_STATUS_IMPORTED = "IMPORTED"
LEDGER_TYPE_CSV = "CSV"
SUBSCRIBER_MATCH_MATCHED = "MATCHED"
SUBSCRIBER_MATCH_CANDIDATE = "CANDIDATE"
SUBSCRIBER_MATCH_NOT_FOUND = "NOT_FOUND"
SUBSCRIBER_MATCH_IDENTITY_ERROR = "IDENTITY_ERROR"
SUBSCRIBER_MATCH_MULTIPLE = "MULTIPLE_MATCH"
SUBSCRIBER_METHOD_IDENTITY_HASH = "identity_hash"
SUBSCRIBER_METHOD_PERSON_ID_CUSTOM = "person_id_custom"
CDA_SECTION_CODE_SYSTEM = "1.2.392.200119.6.1010"
CDA_SECTION_NAMES = {
    "01010": "特定健診・問診結果セクション",
    "01020": "広域連合保健事業セクション",
    "01030": "労働安全衛生法健診結果セクション",
    "01040": "学校保健安全法健診結果セクション",
    "01060": "がん検診セクション",
    "01090": "肝炎検診セクション",
    "01990": "任意追加項目セクション",
}


@dataclass(frozen=True)
class ImportConfig:
    event_id: int | None
    health_db: str
    dev_db: str
    master_db: str
    dry_run: bool
    limit: int
    include_imported: bool


@dataclass
class ImportSummary:
    files: int = 0
    files_imported: int = 0
    files_waiting_confirm: int = 0
    rows_seen: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_skipped: int = 0
    exam_item_values_inserted: int = 0
    errors: int = 0

    def to_metrics(self) -> RunMetrics:
        return RunMetrics(
            files=self.files,
            rows_seen=self.rows_seen,
            rows_inserted=self.rows_inserted,
            rows_updated=self.rows_updated,
            rows_skipped=self.rows_skipped,
            errors=self.errors,
        )

    def to_message(self) -> str:
        return (
            f"csv_exam_import files={self.files} imported={self.files_imported} "
            f"waiting_confirm={self.files_waiting_confirm} rows_seen={self.rows_seen} "
            f"rows_inserted={self.rows_inserted} rows_updated={self.rows_updated} "
            f"rows_skipped={self.rows_skipped} values={self.exam_item_values_inserted} "
            f"errors={self.errors}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import CSV exam result files from file_receipts.")
    parser.add_argument("--event-id", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--include-imported", action="store_true")
    parser.add_argument("--db-prefix", default="PHR_DB_")
    parser.add_argument("--health-db", default=HEALTH_EXAM_RESULT_DB)
    parser.add_argument("--dev-db", default=DEV_PHR_DB)
    parser.add_argument("--master-db", default=PHR_MASTER)
    return parser.parse_args()


def qname(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def compact_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_date_value(value: Any) -> date | None:
    text = compact_text(value)
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d", "%Y.%m.%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fp:
        while True:
            chunk = fp.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def normalize_gender_match(value: Any) -> str | None:
    result = normalize_gender_code(compact_text(value))
    if not result.get("ok"):
        return None
    return cast(str | None, result.get("match"))


def section_name_for_code(section_code: Any) -> str | None:
    code = compact_text(section_code)
    if code is None:
        return None
    return CDA_SECTION_NAMES.get(code)


def build_csv_identity(ledger_fields: Mapping[str, Any]) -> dict[str, Any]:
    gender_code = normalize_gender_match(ledger_fields.get("gender_raw"))
    identity_raw = {
        "birthdate": ledger_fields.get("birthdate"),
        "insurer_number_raw": ledger_fields.get("insurer_number"),
        "insurance_symbol_raw": ledger_fields.get("insurance_symbol_raw"),
        "insurance_number_raw": ledger_fields.get("insurance_number_raw"),
        "name_kana_full_raw": ledger_fields.get("name_kana_raw"),
        "gender_code": gender_code,
    }
    bundle = generate_identity_bundle(**identity_raw)
    if bundle.get("ok"):
        field_results = cast(Mapping[str, Any], bundle.get("field_results") or {})
        name_result = cast(Mapping[str, Any], field_results.get("name_kana_full") or {})
        return {
            "ok": True,
            "person_id_custom": bundle.get("person_id_custom"),
            "identity_hash": bundle.get("identity_hash"),
            "name_kana_match": name_result.get("match"),
            "gender_code": gender_code,
            "reason": None,
        }

    pid_result = generate_person_id_custom(
        birthdate=ledger_fields.get("birthdate"),
        insurer_number_raw=ledger_fields.get("insurer_number"),
        insurance_symbol_raw=ledger_fields.get("insurance_symbol_raw"),
        insurance_number_raw=ledger_fields.get("insurance_number_raw"),
    )
    if pid_result.get("ok"):
        return {
            "ok": False,
            "person_id_custom": pid_result.get("value"),
            "identity_hash": None,
            "name_kana_match": None,
            "gender_code": gender_code,
            "reason": bundle.get("reason"),
        }

    return {
        "ok": False,
        "person_id_custom": None,
        "identity_hash": None,
        "name_kana_match": None,
        "gender_code": gender_code,
        "reason": pid_result.get("reason") or bundle.get("reason"),
    }


def resolve_csv_subscriber(cur: Any, *, config: ImportConfig, identity: Mapping[str, Any]) -> dict[str, Any]:
    if identity.get("identity_hash") is None and identity.get("person_id_custom") is None:
        return {
            "subscriber_id": None,
            "hia_subscriber_id": None,
            "subscriber_match_status": SUBSCRIBER_MATCH_IDENTITY_ERROR,
            "subscriber_match_method": None,
            "subscriber_match_reason": compact_text(identity.get("reason")),
        }

    result = resolve_subscriber_identity(
        cur,
        identity_hash=cast(str | None, identity.get("identity_hash")),
        person_id_custom=cast(str | None, identity.get("person_id_custom")),
        dev_db=config.dev_db,
    )
    if result.status == "matched":
        row = result.rows[0]
        status = (
            SUBSCRIBER_MATCH_CANDIDATE
            if result.matched_by == SUBSCRIBER_METHOD_PERSON_ID_CUSTOM
            else SUBSCRIBER_MATCH_MATCHED
        )
        return {
            "subscriber_id": row.get("subscriber_id"),
            "hia_subscriber_id": row.get("hia_subscriber_id"),
            "subscriber_match_status": status,
            "subscriber_match_method": result.matched_by,
            "subscriber_match_reason": None if status == SUBSCRIBER_MATCH_MATCHED else "person_id_custom candidate",
        }
    if result.status == "multiple_match":
        return {
            "subscriber_id": None,
            "hia_subscriber_id": None,
            "subscriber_match_status": SUBSCRIBER_MATCH_MULTIPLE,
            "subscriber_match_method": result.matched_by,
            "subscriber_match_reason": f"candidates={result.candidate_count}",
        }
    return {
        "subscriber_id": None,
        "hia_subscriber_id": None,
        "subscriber_match_status": SUBSCRIBER_MATCH_NOT_FOUND,
        "subscriber_match_method": result.matched_by,
        "subscriber_match_reason": compact_text(identity.get("reason")) or "subscriber not found",
    }


def start_import_run(cur: Any, *, config: ImportConfig) -> int:
    return etl_start_run(
        cur,
        phase=ETL_PHASE,
        source=ETL_SOURCE,
        db_schema=config.health_db,
        db_path=config.health_db,
        input_base=f"event_id={config.event_id}" if config.event_id is not None else None,
        input_file=None,
        insurer_number=None,
        dry_run=config.dry_run,
        limit_rows=config.limit or None,
    )


def record_error(
    cur: Any,
    *,
    run_id: int,
    summary: ImportSummary,
    src_file: str | None,
    row_no: int | None,
    field: str,
    error_code: str,
    message: str,
    field_value: str | None = None,
) -> None:
    summary.errors += 1
    etl_log_error(
        cur,
        run_id,
        phase=ETL_PHASE,
        source=ETL_SOURCE,
        insurer_number=None,
        src_file=src_file,
        row_no=row_no,
        line_no=row_no,
        field=field,
        field_value=field_value,
        error_code=error_code,
        message=message,
    )


def fetch_csv_file_receipts(cur: Any, *, config: ImportConfig) -> list[dict[str, Any]]:
    params: list[Any] = []
    statuses = [FILE_STATUS_READY]
    if config.include_imported:
        statuses.append(FILE_STATUS_IMPORTED)
    status_conditions = [f"status IN ({', '.join(['%s'] * len(statuses))})"]
    params.extend(statuses)
    status_conditions.append("(status = %s AND import_resume_approved = 1)")
    params.append(FILE_STATUS_WAITING_CONFIRM)
    where = ["file_type = 'CSV'", f"({' OR '.join(status_conditions)})"]
    if config.event_id is not None:
        where.append("event_id = %s")
        params.append(config.event_id)
    limit_sql = ""
    if config.limit:
        limit_sql = "LIMIT %s"
        params.append(config.limit)
    cur.execute(
        f"""
        SELECT *
        FROM {qname(config.health_db)}.file_receipts
        WHERE {" AND ".join(where)}
        ORDER BY id
        {limit_sql}
        """,
        tuple(params),
    )
    return [dict(row) for row in cur.fetchall()]


def fetch_active_format_candidates(
    cur: Any,
    *,
    config: ImportConfig,
    exam_facility_id: int,
) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT *
        FROM {qname(config.master_db)}.csv_format_versions
        WHERE exam_facility_id = %s
          AND is_active = 1
        ORDER BY valid_from DESC, csv_format_version_id DESC
        """,
        (exam_facility_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def resolve_format(
    cur: Any,
    *,
    config: ImportConfig,
    file_receipt: Mapping[str, Any],
) -> tuple[dict[str, Any] | None, CsvLoadResult | None, str | None]:
    exam_facility_id = file_receipt.get("exam_facility_id")
    if exam_facility_id is None:
        return None, None, None

    source_path = str(file_receipt["source_path"])
    matched_format_id = file_receipt.get("matched_csv_format_version_id")
    if matched_format_id is not None:
        fmt = get_csv_format_version_by_id(
            cur,
            int(matched_format_id),
            master_db=config.master_db,
        )
        if fmt is not None and int(fmt.get("exam_facility_id") or 0) == int(exam_facility_id):
            csv_result, actual_header_sha256 = load_csv_matching_registered_header(source_path, fmt)
            if csv_result is not None:
                return fmt, csv_result, actual_header_sha256
            return None, None, actual_header_sha256

    actual_header_sha256: str | None = None
    for candidate in fetch_active_format_candidates(
        cur,
        config=config,
        exam_facility_id=int(exam_facility_id),
    ):
        csv_result, actual_header_sha256 = load_csv_matching_registered_header(source_path, candidate)
        if csv_result is not None:
            return candidate, csv_result, actual_header_sha256
    return None, None, actual_header_sha256


def update_file_receipt_header(
    cur: Any,
    *,
    config: ImportConfig,
    file_receipt_id: int,
    actual_header_sha256: str | None,
    actual_character_encoding: str | None,
    csv_format_version_id: int | None,
    status: str | None = None,
    summary_message: str | None = None,
) -> None:
    cur.execute(
        f"""
        UPDATE {qname(config.health_db)}.file_receipts
        SET actual_header_sha256 = %s,
            actual_character_encoding = %s,
            matched_csv_format_version_id = %s,
            status = COALESCE(%s, status),
            summary_message = COALESCE(%s, summary_message),
            content_checked_at = CURRENT_TIMESTAMP(3)
        WHERE id = %s
        """,
        (
            actual_header_sha256,
            actual_character_encoding,
            csv_format_version_id,
            status,
            summary_message,
            file_receipt_id,
        ),
    )


def get_existing_row_ledger(
    cur: Any,
    *,
    config: ImportConfig,
    file_receipt_id: int,
    src_row_no: int,
) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT *
        FROM {qname(config.health_db)}.csv_row_ledger
        WHERE file_receipt_id = %s
          AND src_row_no = %s
        LIMIT 1
        """,
        (file_receipt_id, src_row_no),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def upsert_row_ledger(
    cur: Any,
    *,
    config: ImportConfig,
    run_id: int,
    file_receipt: Mapping[str, Any],
    fmt: Mapping[str, Any],
    src_row_no: int,
    row_hash: str,
    raw_row_json: str,
    ledger_fields: Mapping[str, Any],
    row_status: str,
    row_reason: str | None,
    exam_item_count: int,
    exam_item_error_count: int,
    basic_info: Mapping[str, Any],
) -> tuple[int, str]:
    file_receipt_id = int(file_receipt["id"])
    existing = get_existing_row_ledger(
        cur,
        config=config,
        file_receipt_id=file_receipt_id,
        src_row_no=src_row_no,
    )
    params = {
        "etl_run_id": run_id,
        "event_id": int(file_receipt["event_id"]),
        "row_sha256": row_hash,
        "raw_row_json": raw_row_json,
        "actual_header_sha256": fmt.get("header_sha256"),
        "mapping_version": fmt.get("mapping_version"),
        "insurer_number": ledger_fields.get("insurer_number"),
        "exam_facility_id": file_receipt.get("exam_facility_id"),
        "facility_code": ledger_fields.get("facility_code") or file_receipt.get("facility_code"),
        "facility_name": ledger_fields.get("facility_name") or file_receipt.get("facility_name"),
        "exam_date": parse_date_value(ledger_fields.get("exam_date")),
        "name_full_raw": ledger_fields.get("name_full_raw"),
        "name_kana_raw": ledger_fields.get("name_kana_raw"),
        "insurance_symbol_raw": ledger_fields.get("insurance_symbol_raw"),
        "insurance_number_raw": ledger_fields.get("insurance_number_raw"),
        "insurance_branch_number_raw": ledger_fields.get("insurance_branch_number_raw"),
        "birthdate": parse_date_value(ledger_fields.get("birthdate")),
        "gender_raw": ledger_fields.get("gender_raw"),
        "health_exam_report_category": ledger_fields.get("health_exam_report_category"),
        "program_code": ledger_fields.get("program_code"),
        "postal_code": ledger_fields.get("postal_code"),
        "address": ledger_fields.get("address"),
        "basic_info_status": basic_info.get("basic_info_status"),
        "basic_info_reason": basic_info.get("basic_info_reason"),
        "address_source": basic_info.get("address_source"),
        "address_completion_status": basic_info.get("address_completion_status"),
        "address_completion_reason": basic_info.get("address_completion_reason"),
        "address_completed_value": basic_info.get("address_completed_value"),
        "postal_code_completed_value": basic_info.get("postal_code_completed_value"),
        "person_id_custom": ledger_fields.get("person_id_custom"),
        "subscriber_match_status": "NOT_EXECUTED",
        "exam_item_status": "ERROR" if exam_item_error_count else "READY",
        "exam_item_count": exam_item_count,
        "exam_item_error_count": exam_item_error_count,
        "exam_item_reason": row_reason,
        "row_status": row_status,
        "row_reason": row_reason,
    }
    if existing is None:
        cur.execute(
            f"""
            INSERT INTO {qname(config.health_db)}.csv_row_ledger (
                file_receipt_id, etl_run_id, event_id, src_row_no, src_line_no,
                row_sha256, raw_row_json, actual_header_sha256, mapping_version,
                insurer_number, exam_facility_id, facility_code, facility_name,
                exam_date, name_full_raw, name_kana_raw,
                insurance_symbol_raw, insurance_number_raw, insurance_branch_number_raw,
                birthdate, gender_raw, health_exam_report_category, program_code,
                postal_code, address,
                basic_info_status, basic_info_reason, address_source,
                address_completion_status, address_completion_reason,
                address_completed_value, postal_code_completed_value,
                person_id_custom,
                subscriber_match_status, exam_item_status, exam_item_count,
                exam_item_error_count, exam_item_reason, row_status, row_reason
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s
            )
            """,
            (
                file_receipt_id,
                params["etl_run_id"],
                params["event_id"],
                src_row_no,
                src_row_no,
                params["row_sha256"],
                params["raw_row_json"],
                params["actual_header_sha256"],
                params["mapping_version"],
                params["insurer_number"],
                params["exam_facility_id"],
                params["facility_code"],
                params["facility_name"],
                params["exam_date"],
                params["name_full_raw"],
                params["name_kana_raw"],
                params["insurance_symbol_raw"],
                params["insurance_number_raw"],
                params["insurance_branch_number_raw"],
                params["birthdate"],
                params["gender_raw"],
                params["health_exam_report_category"],
                params["program_code"],
                params["postal_code"],
                params["address"],
                params["basic_info_status"],
                params["basic_info_reason"],
                params["address_source"],
                params["address_completion_status"],
                params["address_completion_reason"],
                params["address_completed_value"],
                params["postal_code_completed_value"],
                params["person_id_custom"],
                params["subscriber_match_status"],
                params["exam_item_status"],
                params["exam_item_count"],
                params["exam_item_error_count"],
                params["exam_item_reason"],
                params["row_status"],
                params["row_reason"],
            ),
        )
        return int(cur.lastrowid), "inserted"

    ledger_id = int(existing["csv_row_ledger_id"])
    cur.execute(
        f"""
        UPDATE {qname(config.health_db)}.csv_row_ledger
        SET etl_run_id = %s,
            row_sha256 = %s,
            raw_row_json = %s,
            actual_header_sha256 = %s,
            mapping_version = %s,
            insurer_number = %s,
            exam_facility_id = %s,
            facility_code = %s,
            facility_name = %s,
            exam_date = %s,
            name_full_raw = %s,
            name_kana_raw = %s,
            insurance_symbol_raw = %s,
            insurance_number_raw = %s,
            insurance_branch_number_raw = %s,
            birthdate = %s,
            gender_raw = %s,
            health_exam_report_category = %s,
            program_code = %s,
            postal_code = %s,
            address = %s,
            basic_info_status = %s,
            basic_info_reason = %s,
            address_source = %s,
            address_completion_status = %s,
            address_completion_reason = %s,
            address_completed_value = %s,
            postal_code_completed_value = %s,
            person_id_custom = %s,
            subscriber_match_status = %s,
            exam_item_status = %s,
            exam_item_count = %s,
            exam_item_error_count = %s,
            exam_item_reason = %s,
            row_status = %s,
            row_reason = %s
        WHERE csv_row_ledger_id = %s
        """,
        (
            params["etl_run_id"],
            params["row_sha256"],
            params["raw_row_json"],
            params["actual_header_sha256"],
            params["mapping_version"],
            params["insurer_number"],
            params["exam_facility_id"],
            params["facility_code"],
            params["facility_name"],
            params["exam_date"],
            params["name_full_raw"],
            params["name_kana_raw"],
            params["insurance_symbol_raw"],
            params["insurance_number_raw"],
            params["insurance_branch_number_raw"],
            params["birthdate"],
            params["gender_raw"],
            params["health_exam_report_category"],
            params["program_code"],
            params["postal_code"],
            params["address"],
            params["basic_info_status"],
            params["basic_info_reason"],
            params["address_source"],
            params["address_completion_status"],
            params["address_completion_reason"],
            params["address_completed_value"],
            params["postal_code_completed_value"],
            params["person_id_custom"],
            params["subscriber_match_status"],
            params["exam_item_status"],
            params["exam_item_count"],
            params["exam_item_error_count"],
            params["exam_item_reason"],
            params["row_status"],
            params["row_reason"],
            ledger_id,
        ),
    )
    return ledger_id, "updated"


def delete_csv_exam_item_values(cur: Any, *, config: ImportConfig, ledger_id: int) -> None:
    cur.execute(
        f"""
        DELETE FROM {qname(config.health_db)}.exam_item_values
        WHERE ledger_type = %s
          AND ledger_id = %s
        """,
        (LEDGER_TYPE_CSV, ledger_id),
    )


def update_row_ledger_subscriber(
    cur: Any,
    *,
    config: ImportConfig,
    ledger_id: int,
    identity: Mapping[str, Any],
    subscriber: Mapping[str, Any],
) -> None:
    cur.execute(
        f"""
        UPDATE {qname(config.health_db)}.csv_row_ledger
        SET subscriber_id = %s,
            hia_subscriber_id = %s,
            identity_hash = %s,
            person_id_custom = %s,
            name_kana_match = %s,
            gender_code = %s,
            subscriber_match_status = %s,
            subscriber_match_method = %s,
            subscriber_match_reason = %s
        WHERE csv_row_ledger_id = %s
        """,
        (
            subscriber.get("subscriber_id"),
            subscriber.get("hia_subscriber_id"),
            identity.get("identity_hash"),
            identity.get("person_id_custom"),
            identity.get("name_kana_match"),
            identity.get("gender_code"),
            subscriber.get("subscriber_match_status"),
            subscriber.get("subscriber_match_method"),
            subscriber.get("subscriber_match_reason"),
            ledger_id,
        ),
    )


def insert_exam_item_value(
    cur: Any,
    *,
    config: ImportConfig,
    run_id: int,
    file_receipt: Mapping[str, Any],
    ledger_id: int,
    rule: CsvMappingRule,
    raw_value: str | None,
    subscriber_id: int | None = None,
    hia_subscriber_id: str | None = None,
) -> tuple[bool, str | None]:
    if raw_value is None or raw_value == "":
        return False, None

    item = get_exam_item(cur, rule.target_namecode, dev_db=config.dev_db)
    normalized = normalize_exam_item_value(
        cur,
        namecode=cast(str, rule.target_namecode),
        raw_value=raw_value,
        raw_unit=rule.raw_unit,
        exam_item=item,
        dev_db=config.dev_db,
        master_db=config.master_db,
    )
    columns = normalized.as_exam_item_value_columns()
    cur.execute(
        f"""
        INSERT INTO {qname(config.health_db)}.exam_item_values (
            event_id, ledger_type, ledger_id,
            subscriber_id, hia_subscriber_id,
            namecode, section_code, section_code_system, section_name,
            occurrence_no,
            raw_value, raw_value_type, raw_unit,
            normalized_value, normalized_unit,
            nullflavor, code_system, code_value, code_display,
            namecode_display_name, identity_item_code, jun_no,
            normalize_status, normalize_reason,
            validation_status, validation_reason,
            extracted_run_id, extracted_at, normalized_at
        )
        VALUES (
            %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            1,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3)
        )
        """,
        (
            int(file_receipt["event_id"]),
            LEDGER_TYPE_CSV,
            ledger_id,
            subscriber_id,
            hia_subscriber_id,
            rule.target_namecode,
            item.get("section_code") if item else None,
            CDA_SECTION_CODE_SYSTEM if item and item.get("section_code") else None,
            section_name_for_code(item.get("section_code")) if item else None,
            columns["raw_value"],
            columns["raw_value_type"],
            columns["raw_unit"],
            columns["normalized_value"],
            columns["normalized_unit"],
            columns["nullflavor"],
            columns["code_system"],
            columns["code_value"],
            columns["code_display"],
            item.get("item_name") if item else None,
            item.get("identity_item_code") if item else None,
            item.get("jun_no") if item else None,
            columns["normalize_status"],
            columns["normalize_reason"],
            columns["validation_status"],
            columns["validation_reason"],
            run_id,
        ),
    )
    if columns["validation_status"] == "INVALID":
        return True, cast(str | None, columns["validation_reason"])
    return True, None


def values_to_ledger_fields(extracted: list[ExtractedCsvRuleValue]) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for result in extracted:
        rule = result.rule
        if rule.target_kind != "LEDGER_FIELD" or not rule.target_field:
            continue
        fields[rule.target_field] = result.values_by_role.get("VALUE")
    return fields


def resolve_insurer_number(
    csv_value: Any,
    file_receipt_value: Any,
    event_value: Any,
) -> str | None:
    return (
        compact_text(csv_value)
        or compact_text(file_receipt_value)
        or compact_text(event_value)
    )


def fill_missing_report_codes(
    ledger_fields: dict[str, Any],
    *,
    event_age_rule: Mapping[str, Any] | None,
) -> None:
    if compact_text(ledger_fields.get("health_exam_report_category")) is not None and compact_text(
        ledger_fields.get("program_code")
    ) is not None:
        return

    birthdate = parse_date_value(ledger_fields.get("birthdate"))
    exam_date = parse_date_value(ledger_fields.get("exam_date"))
    if birthdate is None or event_age_rule is None:
        return

    reference_date = resolve_age_reference_date(
        age_rule_type=compact_text(event_age_rule.get("age_rule_type")),
        age_reference_date=parse_date_value(event_age_rule.get("age_reference_date")),
        exam_date=exam_date,
    )
    if reference_date is None:
        return

    report_category, program_code = classify_report_codes_by_age(
        birthdate=birthdate,
        reference_date=reference_date,
    )
    if compact_text(ledger_fields.get("health_exam_report_category")) is None:
        ledger_fields["health_exam_report_category"] = report_category
    if compact_text(ledger_fields.get("program_code")) is None:
        ledger_fields["program_code"] = program_code


def process_file_receipt(
    cur: Any,
    *,
    config: ImportConfig,
    run_id: int,
    summary: ImportSummary,
    file_receipt: Mapping[str, Any],
) -> None:
    file_receipt_id = int(file_receipt["id"])
    event_insurer_number = get_event_insurer_number(
        cur,
        event_id=int(file_receipt["event_id"]),
        dev_db=config.dev_db,
    )
    event_age_rule = get_event_age_rule(
        cur,
        event_id=int(file_receipt["event_id"]),
        dev_db=config.dev_db,
    )
    src_file = compact_text(file_receipt.get("source_path"))
    source_path = Path(str(file_receipt["source_path"]))
    current_file_sha256 = sha256_file(source_path)
    if current_file_sha256 != file_receipt.get("file_sha256"):
        summary.rows_skipped += 1
        record_error(
            cur,
            run_id=run_id,
            summary=summary,
            src_file=src_file,
            row_no=None,
            field="FILE",
            error_code="FILE_SHA256_MISMATCH",
            message=f"source file hash changed after scan: file_receipt_id={file_receipt_id}",
            field_value=current_file_sha256,
        )
        return

    fmt, csv_result, actual_header_sha256 = resolve_format(cur, config=config, file_receipt=file_receipt)
    if fmt is None or csv_result is None:
        update_file_receipt_header(
            cur,
            config=config,
            file_receipt_id=file_receipt_id,
            actual_header_sha256=actual_header_sha256,
            actual_character_encoding=None,
            csv_format_version_id=None,
            status=FILE_STATUS_WAITING_CONFIRM,
            summary_message="CSV header did not match registered format.",
        )
        summary.files_waiting_confirm += 1
        record_error(
            cur,
            run_id=run_id,
            summary=summary,
            src_file=src_file,
            row_no=None,
            field="CSV_HEADER",
            error_code="CSV_FORMAT_NOT_FOUND",
            message=f"csv format not found: file_receipt_id={file_receipt_id}",
            field_value=actual_header_sha256,
        )
        return

    update_file_receipt_header(
        cur,
        config=config,
        file_receipt_id=file_receipt_id,
        actual_header_sha256=csv_result.header_set.header_sha256,
        actual_character_encoding=csv_result.encoding,
        csv_format_version_id=int(fmt["csv_format_version_id"]),
        status=FILE_STATUS_READY,
        summary_message=f"CSV format matched: {fmt.get('mapping_version')}",
    )
    rules = load_csv_mapping_rules(
        cur,
        csv_format_version_id=int(fmt["csv_format_version_id"]),
        csv_result=csv_result,
        master_db=config.master_db,
    )

    file_error_count = 0
    for row_index, row in enumerate(csv_result.rows, start=csv_result.data_start_row_no):
        summary.rows_seen += 1
        if all((cell or "").strip() == "" for cell in row):
            summary.rows_skipped += 1
            continue

        extracted = extract_row_values(row, rules)
        ledger_fields = values_to_ledger_fields(extracted)
        ledger_fields["insurer_number"] = resolve_insurer_number(
            ledger_fields.get("insurer_number"),
            file_receipt.get("insurer_number"),
            event_insurer_number,
        )
        fill_missing_report_codes(ledger_fields, event_age_rule=event_age_rule)
        row_errors: list[str] = []
        for result in extracted:
            if result.errors and result.rule.is_required:
                row_errors.extend(result.errors)

        identity = build_csv_identity(ledger_fields)
        subscriber = resolve_csv_subscriber(cur, config=config, identity=identity)
        basic_info = resolve_basic_info_completion(cur, row=ledger_fields, master_db=config.master_db)
        item_results = [result for result in extracted if result.rule.target_kind == "EXAM_ITEM_VALUE"]
        raw_row_json = json.dumps(row, ensure_ascii=False, separators=(",", ":"))
        ledger_id, action = upsert_row_ledger(
            cur,
            config=config,
            run_id=run_id,
            file_receipt=file_receipt,
            fmt=fmt,
            src_row_no=row_index,
            row_hash=row_sha256(row),
            raw_row_json=raw_row_json,
            ledger_fields=ledger_fields,
            row_status="ERROR" if row_errors else "READY",
            row_reason="; ".join(row_errors) if row_errors else None,
            exam_item_count=0,
            exam_item_error_count=len(row_errors),
            basic_info=basic_info.as_db_params(),
        )
        update_row_ledger_subscriber(
            cur,
            config=config,
            ledger_id=ledger_id,
            identity=identity,
            subscriber=subscriber,
        )
        if action == "inserted":
            summary.rows_inserted += 1
        else:
            summary.rows_updated += 1

        delete_csv_exam_item_values(cur, config=config, ledger_id=ledger_id)
        inserted_values = 0
        item_error_count = len(row_errors)
        item_reasons = list(row_errors)
        for result in item_results:
            raw_value = result.values_by_role.get("VALUE")
            inserted, reason = insert_exam_item_value(
                cur,
                config=config,
                run_id=run_id,
                file_receipt=file_receipt,
                ledger_id=ledger_id,
                rule=result.rule,
                raw_value=raw_value,
                subscriber_id=cast(int | None, subscriber.get("subscriber_id")),
                hia_subscriber_id=cast(str | None, subscriber.get("hia_subscriber_id")),
            )
            if inserted:
                inserted_values += 1
                summary.exam_item_values_inserted += 1
            if reason:
                item_error_count += 1
                item_reasons.append(f"{result.rule.rule_key}:{reason}")

        cur.execute(
            f"""
            UPDATE {qname(config.health_db)}.csv_row_ledger
            SET exam_item_count = %s,
                exam_item_error_count = %s,
                exam_item_status = %s,
                exam_item_reason = %s,
                row_status = %s,
                row_reason = %s
            WHERE csv_row_ledger_id = %s
            """,
            (
                inserted_values,
                item_error_count,
                "ERROR" if item_error_count else "READY",
                "; ".join(item_reasons) if item_reasons else None,
                "ERROR" if item_error_count else "READY",
                "; ".join(item_reasons) if item_reasons else None,
                ledger_id,
            ),
        )
        if item_error_count:
            file_error_count += item_error_count

    cur.execute(
        f"""
        UPDATE {qname(config.health_db)}.file_receipts
        SET status = %s,
            summary_message = %s,
            processable_count = %s,
            processed_at = CURRENT_TIMESTAMP(3)
        WHERE id = %s
        """,
        (
            FILE_STATUS_IMPORTED,
            f"CSV imported: mapping_version={fmt.get('mapping_version')} errors={file_error_count}",
            len(csv_result.rows),
            file_receipt_id,
        ),
    )
    summary.files_imported += 1


def run_import(conn: Any, config: ImportConfig) -> ImportSummary:
    summary = ImportSummary()
    cur = dict_cursor(conn)
    run_id = start_import_run(cur, config=config)
    conn.commit()
    try:
        receipts = fetch_csv_file_receipts(cur, config=config)
        summary.files = len(receipts)
        for receipt in receipts:
            process_file_receipt(cur, config=config, run_id=run_id, summary=summary, file_receipt=receipt)
            if not config.dry_run:
                conn.commit()
            else:
                conn.rollback()

        etl_finish_run(
            cur,
            run_id,
            summary.to_metrics(),
            status_override="partial" if summary.errors else "success",
            extra_notes=summary.to_message(),
        )
        conn.commit()
        return summary
    except Exception as exc:
        conn.rollback()
        error_cur = dict_cursor(conn)
        try:
            record_error(
                error_cur,
                run_id=run_id,
                summary=summary,
                src_file=None,
                row_no=None,
                field="UNEXPECTED",
                error_code="UNEXPECTED_CSV_IMPORT_ERROR",
                message=f"unexpected csv import error: {type(exc).__name__}: {exc}",
            )
            etl_finish_run(
                error_cur,
                run_id,
                summary.to_metrics(),
                status_override="failed",
                extra_notes=summary.to_message(),
            )
            conn.commit()
        finally:
            error_cur.close()
        raise
    finally:
        cur.close()


def main() -> int:
    args = parse_args()
    config = ImportConfig(
        event_id=args.event_id,
        health_db=args.health_db,
        dev_db=args.dev_db,
        master_db=args.master_db,
        dry_run=bool(args.dry_run),
        limit=int(args.limit or 0),
        include_imported=bool(args.include_imported),
    )
    params = load_mysql_base_params(args.db_prefix)
    with connect_ctx(params, database=config.health_db, autocommit=False) as conn:
        summary = run_import(conn, config)
    print(summary.to_message())
    return 1 if summary.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
