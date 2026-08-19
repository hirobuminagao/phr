#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Shared implementation for Article 44 exam result checks."""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, cast

import yaml


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.etl import RunMetrics
from scripts.lib.etl import finish_run as etl_finish_run
from scripts.lib.etl import log_error as etl_log_error
from scripts.lib.etl import start_run as etl_start_run
from scripts.from_medical.script_lib.article44_checker import ARTICLE44_CHECKERS
from scripts.from_medical.script_lib.article44_checker import check_article44
from scripts.from_medical.script_lib.article44_models import Article44Result, CheckResult, ExpectedValueType, RequiredNamecode
from scripts.from_medical.script_lib.article44_required_namecodes import ARTICLE44_GROUP_CODE
from scripts.from_medical.script_lib.article44_required_namecodes import fetch_article44_required_namecodes
from scripts.from_medical.script_lib.article44_value_loader import load_article44_value_map
from scripts.from_medical.script_lib.export_case_readiness import refresh_export_case_readiness
from scripts.from_medical.script_lib.specific_health_checker import SPECIFIC_REQUIRED_NAMECODES
from scripts.from_medical.script_lib.specific_health_checker import aggregate_specific_result
from scripts.from_medical.script_lib.specific_health_required_namecodes import fetch_specific_health_required_namecodes
from scripts.lib.db.lookup.event import get_event_year
from scripts.lib.examination.lookup import qname
from scripts.lib.examination.models import RESULT_NG, RESULT_OK
from scripts.lib.examination.models import STATUS_ALTERNATIVE, STATUS_CALCULATED, STATUS_INVALID, STATUS_MISSING, STATUS_OK
from scripts.lib.examination.report_classification import fiscal_year_end_date


HEALTH_EXAM_RESULT_DB = "health_exam_result"
DEV_PHR_DB = "dev_phr"
DEFAULT_CONFIG_PATH = REPO_ROOT / "scripts" / "from_medical" / "config" / "import_xml.yml"
ETL_PHASE = "CHECK_EXAM_RESULTS"
ETL_SOURCE = "FROM_MEDICAL"

CHECK_STATUS_PENDING = "PENDING"
CHECK_STATUS_OK = "OK"
CHECK_STATUS_WARNING = "WARNING"
CHECK_STATUS_NG = "NG"
LEDGER_TYPE_XML = "XML"
LEDGER_TYPE_CSV = "CSV"
LEDGER_TYPE_EXAM = "EXAM"
LEDGER_TYPE_EXPORT_CASE = "EXPORT_CASE"
LEDGER_TYPE_ALL = "ALL"
LEDGER_TYPES = {LEDGER_TYPE_XML, LEDGER_TYPE_CSV, LEDGER_TYPE_EXAM, LEDGER_TYPE_EXPORT_CASE, LEDGER_TYPE_ALL}

ARTICLE44_OK_STATUSES = {STATUS_OK, STATUS_CALCULATED, STATUS_ALTERNATIVE}
ARTICLE44_PROBLEM_STATUSES = {STATUS_MISSING, STATUS_INVALID}
ARTICLE44_ALLOWED_STATUSES = ARTICLE44_OK_STATUSES | ARTICLE44_PROBLEM_STATUSES
PLACEHOLDER_REVIEW_OPEN_STATUSES = {
    "NONE",
    "NEEDS_CONFIRMATION",
    "WAITING_RESUBMISSION",
}
ARTICLE44_DETAIL_NAMES: dict[str, str] = {
    "4401001001": "既往歴",
    "4402001001": "自覚症状",
    "4402001002": "他覚症状",
    "4403001001": "身長",
    "4403002001": "体重",
    "4403003001": "腹囲",
    "4403004001": "視力",
    "4403005001": "聴力",
    "4404001001": "胸部X線",
    "4405001001": "収縮期血圧",
    "4405001002": "拡張期血圧",
    "4406001001": "血色素量",
    "4406001002": "赤血球数",
    "4407001001": "AST",
    "4407001002": "ALT",
    "4407001003": "γ-GT",
    "4408001001": "LDLコレステロール",
    "4408001002": "HDLコレステロール",
    "4408001003": "中性脂肪",
    "4409001001": "血糖",
    "4410001001": "尿糖",
    "4410001002": "尿蛋白",
    "4411001001": "心電図",
}


@dataclass(frozen=True)
class CheckConfig:
    event_id: int
    health_db: str
    dev_db: str
    dry_run: bool
    limit: int
    verbose: bool
    ledger_type: str


@dataclass
class CheckSummary:
    event_id: int
    dry_run: bool
    ledgers_seen: int = 0
    rows_deleted: int = 0
    rows_inserted: int = 0
    ledgers_updated: int = 0
    ok: int = 0
    warning: int = 0
    ng: int = 0
    errors: int = 0

    def to_metrics(self) -> RunMetrics:
        return RunMetrics(
            files=0,
            rows_seen=self.ledgers_seen,
            rows_inserted=self.rows_inserted,
            rows_updated=self.ledgers_updated,
            rows_skipped=0,
            errors=self.errors,
        )

    def to_message(self) -> str:
        return (
            "check_exam_results "
            f"event_id={self.event_id} ledgers={self.ledgers_seen} "
            f"inserted={self.rows_inserted} updated={self.ledgers_updated} "
            f"ok={self.ok} warning={self.warning} ng={self.ng} errors={self.errors}"
        )

    def print(self) -> None:
        print(self.to_message())
        print(f"  dry_run={self.dry_run}")
        print(f"  deleted={self.rows_deleted}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate exam_check_results from imported exam item values.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Check config YAML path.")
    parser.add_argument("--event-id", type=int, default=None, help="Override config event_id.")
    parser.add_argument("--dry-run", action="store_true", help="Read and report without DB writes.")
    parser.add_argument("--limit", type=int, default=None, help="Override maximum xml_ledger rows to process. 0 means unlimited.")
    parser.add_argument(
        "--ledger-type",
        choices=(LEDGER_TYPE_ALL, LEDGER_TYPE_XML, LEDGER_TYPE_CSV, LEDGER_TYPE_EXAM, LEDGER_TYPE_EXPORT_CASE),
        default=None,
        help="Ledger source to check. EXPORT_CASE uses exam_export_cases. EXAM uses imported exam_ledgers. Default is ALL.",
    )
    parser.add_argument("--db-prefix", default="PHR_DB_", help="Environment prefix for DB connection.")
    parser.add_argument("--health-db", default=None, help="Override health_exam_result schema name.")
    parser.add_argument("--dev-db", default=None, help="Override dev_phr schema name.")
    parser.add_argument("--verbose", action="store_true", help="Print per-ledger details with --dry-run.")
    return parser.parse_args()


def load_check_config(path: str | Path) -> CheckConfig:
    with Path(path).open("r", encoding="utf-8") as fp:
        raw_data = yaml.safe_load(fp) or {}
    data = cast(Mapping[str, Any], raw_data)
    raw_event_id = data.get("event_id")
    event_id = int(raw_event_id) if raw_event_id not in (None, "") else 0
    return CheckConfig(
        event_id=event_id,
        health_db=str(data.get("health_db") or HEALTH_EXAM_RESULT_DB),
        dev_db=str(data.get("dev_db") or DEV_PHR_DB),
        dry_run=bool(data.get("dry_run", False)),
        limit=int(data.get("limit", 0) or 0),
        verbose=False,
        ledger_type=str(data.get("ledger_type") or LEDGER_TYPE_ALL).upper(),
    )


def resolve_config(args: argparse.Namespace) -> CheckConfig:
    config = load_check_config(args.config)
    resolved = CheckConfig(
        event_id=args.event_id if args.event_id is not None else config.event_id,
        health_db=args.health_db if args.health_db is not None else config.health_db,
        dev_db=args.dev_db if args.dev_db is not None else config.dev_db,
        dry_run=True if args.dry_run else config.dry_run,
        limit=args.limit if args.limit is not None else config.limit,
        verbose=bool(args.verbose),
        ledger_type=(args.ledger_type or config.ledger_type).upper(),
    )
    validate_config(resolved)
    return resolved


def validate_config(config: CheckConfig) -> None:
    if config.event_id <= 0:
        raise ValueError("event_id must be positive")
    if config.limit < 0:
        raise ValueError("limit must be >= 0")
    if config.ledger_type not in LEDGER_TYPES:
        raise ValueError(f"ledger_type must be one of {sorted(LEDGER_TYPES)}")


def start_check_run(cur: Any, config: CheckConfig) -> int:
    return etl_start_run(
        cur,
        phase=ETL_PHASE,
        source=ETL_SOURCE,
        db_schema=config.health_db,
        db_path=config.health_db,
        input_base=f"event_id={config.event_id}",
        input_file=None,
        insurer_number=None,
        dry_run=config.dry_run,
        limit_rows=config.limit or None,
    )


def finish_check_run(cur: Any, run_id: int, summary: CheckSummary) -> None:
    etl_finish_run(cur, run_id, summary.to_metrics(), extra_notes=summary.to_message())


def record_script_error(
    cur: Any | None,
    *,
    run_id: int | None,
    summary: CheckSummary,
    error_code: str,
    message: str,
    field_value: str | None = None,
) -> None:
    summary.errors += 1
    if cur is None or run_id is None:
        return
    etl_log_error(
        cur,
        run_id,
        phase=ETL_PHASE,
        source=ETL_SOURCE,
        insurer_number=None,
        src_file=None,
        row_no=None,
        line_no=None,
        field="SCRIPT",
        field_value=field_value,
        error_code=error_code,
        message=message,
    )


def aggregate_check_status(legal_result: str) -> str:
    if legal_result == RESULT_NG:
        return CHECK_STATUS_NG
    return CHECK_STATUS_OK


def validate_article44_result(article44_result: Article44Result) -> None:
    expected_detail_nos = tuple(ARTICLE44_CHECKERS)
    detail_name_nos = tuple(ARTICLE44_DETAIL_NAMES)
    if detail_name_nos != expected_detail_nos:
        raise ValueError(
            "ARTICLE44_DETAIL_NAMES keys mismatch: "
            f"expected_count={len(expected_detail_nos)} actual_count={len(detail_name_nos)} "
            f"expected={expected_detail_nos!r} actual={detail_name_nos!r}"
        )
    actual_detail_nos = tuple(article44_result)
    if actual_detail_nos != expected_detail_nos:
        raise ValueError(
            "Article44Result detail numbers mismatch: "
            f"expected={expected_detail_nos!r} actual={actual_detail_nos!r}"
        )
    for detail_no, result in article44_result.items():
        if not isinstance(result, CheckResult):
            raise ValueError(f"Article44Result value must be CheckResult: detail_no={detail_no}")
        if result.status not in ARTICLE44_ALLOWED_STATUSES:
            raise ValueError(f"Article44Result has invalid status: detail_no={detail_no} status={result.status!r}")


def article44_result_columns(
    article44_result: Article44Result,
) -> dict[str, object]:
    validate_article44_result(article44_result)
    columns: dict[str, object] = {}
    for detail_no, result in article44_result.items():
        columns[f"a44_{detail_no}_status"] = result.status
        columns[f"a44_{detail_no}_reason"] = result.reason
    return columns


def aggregate_article44_legal_result(article44_result: Article44Result) -> tuple[str, str | None]:
    validate_article44_result(article44_result)
    reasons: list[str] = []
    for detail_no, result in article44_result.items():
        if result.status in ARTICLE44_OK_STATUSES:
            continue
        if result.status in ARTICLE44_PROBLEM_STATUSES:
            detail_name = ARTICLE44_DETAIL_NAMES[detail_no]
            reason = result.reason or result.status
            reasons.append(f"{detail_no}:{detail_name}:{reason}")
            continue
        raise ValueError(f"Article44Result has invalid status: detail_no={detail_no} status={result.status!r}")
    if reasons:
        return RESULT_NG, " | ".join(reasons)
    return RESULT_OK, None


def article44_problem_results(article44_result: Article44Result) -> list[tuple[str, CheckResult]]:
    validate_article44_result(article44_result)
    return [
        (detail_no, result)
        for detail_no, result in article44_result.items()
        if result.status in ARTICLE44_PROBLEM_STATUSES or (result.status == STATUS_ALTERNATIVE and result.reason)
    ]


@dataclass(frozen=True)
class MissingPlaceholderItem:
    namecode: str
    item_name: str
    raw_value_type: str
    validation_reason: str
    check_scope: str


ARTICLE44_MISSING_PLACEHOLDER_REASONS = {
    "MISSING",
    "NOT_FOUND",
    "NULL",
    "EMPTY",
    "CODE_VALUE_MISSING",
    "TEXT_VALUE_MISSING",
}


def expected_value_type_text(required: RequiredNamecode) -> str:
    return str(required.expected_value_type.value)


def fetch_article44_required_namecodes_by_detail(
    cursor: Any,
    *,
    dev_db: str,
) -> dict[str, tuple[RequiredNamecode, ...]]:
    """Fetch Article 44 required namecodes grouped by legal detail number."""

    cursor.execute(
        f"""
        SELECT
          namecode,
          value_type,
          notes
        FROM {qname(dev_db)}.exam_item_group_members
        WHERE group_code = %s
        ORDER BY priority, namecode
        """,
        (ARTICLE44_GROUP_CODE,),
    )
    grouped: dict[str, list[RequiredNamecode]] = {}
    for row in cursor.fetchall():
        notes = str(row.get("notes") or "")
        match = re.search(r"Article44\s+(?P<detail_no>44\d{8})\s*:", notes)
        if not match:
            continue
        detail_no = match.group("detail_no")
        if detail_no not in ARTICLE44_DETAIL_NAMES:
            continue
        namecode = str(row.get("namecode") or "").strip()
        value_type = str(row.get("value_type") or "").strip().upper()
        if not namecode or value_type not in {"PQ", "CD", "CO", "ST"}:
            continue
        expected_value_type = ExpectedValueType.CD if value_type == "CO" else ExpectedValueType(value_type)
        grouped.setdefault(detail_no, []).append(
            RequiredNamecode(
                namecode=namecode,
                expected_value_type=expected_value_type,
            )
        )
    return {detail_no: tuple(items) for detail_no, items in grouped.items()}


def parse_article44_missing_placeholder_items(
    article44_result: Article44Result,
    required_namecodes_by_detail: Mapping[str, tuple[RequiredNamecode, ...]],
) -> list[MissingPlaceholderItem]:
    """Build review placeholder targets from Article 44 MISSING results."""

    validate_article44_result(article44_result)
    results: list[MissingPlaceholderItem] = []
    seen: set[str] = set()
    for detail_no, check_result in article44_result.items():
        if check_result.status != STATUS_MISSING:
            continue
        reason = (check_result.reason or check_result.status).strip()
        if reason not in ARTICLE44_MISSING_PLACEHOLDER_REASONS:
            continue
        detail_name = ARTICLE44_DETAIL_NAMES[detail_no]
        for required in required_namecodes_by_detail.get(detail_no, ()):
            if required.namecode in seen:
                continue
            seen.add(required.namecode)
            results.append(
                MissingPlaceholderItem(
                    namecode=required.namecode,
                    item_name=detail_name,
                    raw_value_type=expected_value_type_text(required),
                    validation_reason=f"ARTICLE44:{detail_no}:{reason}",
                    check_scope="ARTICLE44",
                )
            )
    return results


def parse_specific_missing_placeholder_items(
    specific_summary: str | None,
    required_namecodes: tuple[RequiredNamecode, ...],
) -> list[MissingPlaceholderItem]:
    """Build review placeholder targets from specific-health NG reason text."""

    if not specific_summary:
        return []
    required_by_namecode = {required.namecode: required for required in required_namecodes}
    results: list[MissingPlaceholderItem] = []
    seen: set[str] = set()
    for part in specific_summary.split("|"):
        text = part.strip()
        match = re.match(r"^(?P<namecode>[A-Za-z0-9]{17}):(?P<item_name>[^:]+):(?P<reason>.+)$", text)
        if not match:
            continue
        namecode = match.group("namecode")
        required = required_by_namecode.get(namecode)
        if required is None or namecode in seen:
            continue
        reason = match.group("reason").strip()
        if not reason:
            continue
        if reason not in {"NOT_FOUND", "NULL", "EMPTY", "CODE_VALUE_MISSING", "TEXT_VALUE_MISSING"}:
            continue
        seen.add(namecode)
        results.append(
            MissingPlaceholderItem(
                namecode=namecode,
                item_name=match.group("item_name").strip(),
                raw_value_type=expected_value_type_text(required),
                validation_reason=f"SPECIFIC_HEALTH:{reason}",
                check_scope="SPECIFIC_HEALTH",
            )
        )
    return results


def sync_export_case_missing_placeholders(
    cur: Any,
    *,
    health_db: str,
    ledger: Mapping[str, Any],
    article44_result: Article44Result,
    article44_required_namecodes_by_detail: Mapping[str, tuple[RequiredNamecode, ...]],
    specific_summary: str | None,
    specific_required_namecodes: tuple[RequiredNamecode, ...],
) -> int:
    """Upsert case-level MISSING_PLACEHOLDER rows for reviewable missing items."""

    if ledger.get("ledger_type") != LEDGER_TYPE_EXPORT_CASE:
        return 0
    case_id = int(ledger["id"])
    items = [
        *parse_article44_missing_placeholder_items(article44_result, article44_required_namecodes_by_detail),
        *parse_specific_missing_placeholder_items(specific_summary, specific_required_namecodes),
    ]
    active_keys = {(item.namecode, 1) for item in items}
    changed = 0

    for item in items:
        cur.execute(
            f"""
            SELECT id, review_status
            FROM {qname(health_db)}.exam_item_values
            WHERE ledger_type = %s
              AND ledger_id = %s
              AND value_source_role = 'MISSING_PLACEHOLDER'
              AND namecode = %s
              AND occurrence_no = 1
            LIMIT 1
            """,
            (LEDGER_TYPE_EXPORT_CASE, case_id, item.namecode),
        )
        existing = cur.fetchone()
        if existing:
            current_status = str(existing.get("review_status") or "NONE")
            next_status = "NEEDS_CONFIRMATION" if current_status == "RESOLVED_BY_SOURCE_VALUE" else current_status
            cur.execute(
                f"""
                UPDATE {qname(health_db)}.exam_item_values
                SET namecode_display_name = %s,
                    raw_value_type = %s,
                    normalize_status = 'SKIPPED',
                    normalize_reason = %s,
                    validation_status = 'INVALID',
                    validation_reason = %s,
                    review_status = %s,
                    updated_at = CURRENT_TIMESTAMP(3)
                WHERE id = %s
                """,
                (
                    item.item_name,
                    item.raw_value_type,
                    f"{item.check_scope}_MISSING_PLACEHOLDER",
                    item.validation_reason,
                    next_status,
                    existing["id"],
                ),
            )
            changed += int(cur.rowcount or 0)
            continue

        cur.execute(
            f"""
            INSERT INTO {qname(health_db)}.exam_item_values (
              event_id, ledger_type, ledger_id, subscriber_id, hia_subscriber_id,
              namecode, occurrence_no, raw_value_type, namecode_display_name,
              normalize_status, normalize_reason, validation_status, validation_reason,
              value_source_role, review_status, extracted_at, normalized_at
            )
            VALUES (
              %s, %s, %s, %s, %s,
              %s, 1, %s, %s,
              'SKIPPED', %s, 'INVALID', %s,
              'MISSING_PLACEHOLDER', 'NEEDS_CONFIRMATION', CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3)
            )
            """,
            (
                ledger.get("event_id"),
                LEDGER_TYPE_EXPORT_CASE,
                case_id,
                ledger.get("subscriber_id"),
                ledger.get("hia_subscriber_id"),
                item.namecode,
                item.raw_value_type,
                item.item_name,
                f"{item.check_scope}_MISSING_PLACEHOLDER",
                item.validation_reason,
            ),
        )
        changed += int(cur.rowcount or 0)

    cur.execute(
        f"""
        SELECT id, namecode, occurrence_no, review_status
        FROM {qname(health_db)}.exam_item_values
        WHERE ledger_type = %s
          AND ledger_id = %s
          AND value_source_role = 'MISSING_PLACEHOLDER'
          AND normalize_reason IN (
            'ARTICLE44_MISSING_PLACEHOLDER',
            'SPECIFIC_HEALTH_MISSING_PLACEHOLDER'
          )
        """,
        (LEDGER_TYPE_EXPORT_CASE, case_id),
    )
    for row in cur.fetchall():
        key = (str(row.get("namecode") or ""), int(row.get("occurrence_no") or 1))
        if key in active_keys:
            continue
        if str(row.get("review_status") or "") not in PLACEHOLDER_REVIEW_OPEN_STATUSES:
            continue
        cur.execute(
            f"""
            UPDATE {qname(health_db)}.exam_item_values
            SET review_status = 'RESOLVED_BY_SOURCE_VALUE',
                validation_status = 'INVALID',
                validation_reason = 'RESOLVED_BY_SOURCE_VALUE',
                reviewed_at = CURRENT_TIMESTAMP(3),
                updated_at = CURRENT_TIMESTAMP(3)
            WHERE id = %s
            """,
            (row["id"],),
        )
        changed += int(cur.rowcount or 0)

    return changed


def print_dry_run_detail(
    *,
    ledger: dict[str, Any],
    legal_result: str,
    specific_result: str | None,
    check_status: str,
    check_reason: str | None,
    article44_result: Article44Result,
) -> None:
    print("dry_run_detail:")
    print(f"  ledger_type={ledger['ledger_type']}")
    print(f"  ledger_id={ledger['id']}")
    print(f"  subscriber_id={ledger.get('subscriber_id')}")
    print(f"  hia_subscriber_id={ledger.get('hia_subscriber_id')}")
    print(f"  legal_check_result={legal_result}")
    print(f"  specific_check_result={specific_result}")
    print(f"  check_status={check_status}")
    print(f"  check_status_basis=legal_only:{legal_result}")
    print(f"  check_reason={check_reason}")

    problem_results = article44_problem_results(article44_result)
    if not problem_results:
        print("  article44_problem_items=none")
        return

    print("  article44_problem_items:")
    for detail_no, result in problem_results:
        print(f"    - {detail_no}: status={result.status} reason={result.reason}")


def insert_check_result(
    cur: Any,
    *,
    health_db: str,
    ledger: dict[str, Any],
    article44_result: Article44Result,
    legal_result: str,
    specific_result: str | None,
    legal_summary: str | None,
    specific_summary: str | None,
) -> None:
    row: dict[str, Any] = {
        "ledger_type": ledger["ledger_type"],
        "exam_ledger_id": ledger.get("exam_ledger_id"),
        "exam_export_case_id": ledger["id"] if ledger["ledger_type"] == LEDGER_TYPE_EXPORT_CASE else None,
        "xml_ledger_id": ledger["id"] if ledger["ledger_type"] == LEDGER_TYPE_XML else None,
        "csv_row_ledger_id": ledger["id"] if ledger["ledger_type"] == LEDGER_TYPE_CSV else None,
        "event_id": ledger["event_id"],
        "subscriber_id": ledger.get("subscriber_id"),
        "hia_subscriber_id": ledger.get("hia_subscriber_id"),
        "legal_check_result": legal_result,
        "specific_check_result": specific_result,
        "legal_reason_summary": legal_summary,
        "specific_reason_summary": specific_summary,
    }
    row.update(article44_result_columns(article44_result))
    columns = list(row.keys())
    placeholders = ", ".join(["%s"] * len(columns))
    column_sql = ", ".join(f"`{column}`" for column in columns)
    cur.execute(
        f"""
        INSERT INTO {qname(health_db)}.exam_check_results ({column_sql})
        VALUES ({placeholders})
        """,
        tuple(row[column] for column in columns),
    )


def delete_existing_results(
    cur: Any,
    *,
    health_db: str,
    ledger_refs: list[tuple[str, int]],
) -> int:
    if not ledger_refs:
        return 0
    deleted = 0
    exam_ids = [ledger_id for ledger_type, ledger_id in ledger_refs if ledger_type == LEDGER_TYPE_EXAM]
    case_ids = [ledger_id for ledger_type, ledger_id in ledger_refs if ledger_type == LEDGER_TYPE_EXPORT_CASE]
    xml_ids = [ledger_id for ledger_type, ledger_id in ledger_refs if ledger_type == LEDGER_TYPE_XML]
    csv_ids = [ledger_id for ledger_type, ledger_id in ledger_refs if ledger_type == LEDGER_TYPE_CSV]
    if exam_ids:
        placeholders = ", ".join(["%s"] * len(exam_ids))
        cur.execute(
            f"""
            DELETE FROM {qname(health_db)}.exam_check_results
            WHERE ledger_type = %s
              AND exam_ledger_id IN ({placeholders})
            """,
            (LEDGER_TYPE_EXAM, *exam_ids),
        )
        deleted += int(cur.rowcount or 0)
    if case_ids:
        placeholders = ", ".join(["%s"] * len(case_ids))
        cur.execute(
            f"""
            DELETE FROM {qname(health_db)}.exam_check_results
            WHERE ledger_type = %s
              AND exam_export_case_id IN ({placeholders})
            """,
            (LEDGER_TYPE_EXPORT_CASE, *case_ids),
        )
        deleted += int(cur.rowcount or 0)
    if xml_ids:
        placeholders = ", ".join(["%s"] * len(xml_ids))
        cur.execute(
            f"""
            DELETE FROM {qname(health_db)}.exam_check_results
            WHERE ledger_type = %s
              AND xml_ledger_id IN ({placeholders})
            """,
            (LEDGER_TYPE_XML, *xml_ids),
        )
        deleted += int(cur.rowcount or 0)
    if csv_ids:
        placeholders = ", ".join(["%s"] * len(csv_ids))
        cur.execute(
            f"""
            DELETE FROM {qname(health_db)}.exam_check_results
            WHERE ledger_type = %s
              AND csv_row_ledger_id IN ({placeholders})
            """,
            (LEDGER_TYPE_CSV, *csv_ids),
        )
        deleted += int(cur.rowcount or 0)
    return deleted


def update_xml_ledger_check(
    cur: Any,
    *,
    health_db: str,
    ledger_id: int,
    check_status: str,
    check_reason: str | None,
) -> None:
    cur.execute(
        f"""
        UPDATE {qname(health_db)}.xml_ledger
        SET check_status = %s,
            check_reason = %s
        WHERE id = %s
        """,
        (check_status, check_reason, ledger_id),
    )


def update_csv_row_ledger_check(
    cur: Any,
    *,
    health_db: str,
    ledger_id: int,
    check_status: str,
    check_reason: str | None,
) -> None:
    cur.execute(
        f"""
        UPDATE {qname(health_db)}.csv_row_ledger
        SET check_status = %s,
            check_reason = %s
        WHERE csv_row_ledger_id = %s
        """,
        (check_status, check_reason, ledger_id),
    )


def update_exam_ledger_check(
    cur: Any,
    *,
    health_db: str,
    exam_ledger_id: int,
    check_status: str,
    check_reason: str | None,
) -> None:
    cur.execute(
        f"""
        UPDATE {qname(health_db)}.exam_ledgers
        SET check_status = %s,
            check_reason = %s
        WHERE exam_ledger_id = %s
        """,
        (check_status, check_reason, exam_ledger_id),
    )


def update_exam_export_case_check(
    cur: Any,
    *,
    health_db: str,
    exam_export_case_id: int,
    check_status: str,
    check_reason: str | None,
) -> None:
    cur.execute(
        f"""
        UPDATE {qname(health_db)}.exam_export_cases
        SET check_status = %s,
            check_reason = %s,
            updated_at = CURRENT_TIMESTAMP(3)
        WHERE exam_export_case_id = %s
        """,
        (check_status, check_reason, exam_export_case_id),
    )


def update_exam_ledgers_by_source_check(
    cur: Any,
    *,
    health_db: str,
    ledger_type: str,
    ledger_id: int,
    check_status: str,
    check_reason: str | None,
) -> None:
    if ledger_type == LEDGER_TYPE_XML:
        source_column = "source_xml_ledger_id"
    elif ledger_type == LEDGER_TYPE_CSV:
        source_column = "source_csv_row_ledger_id"
    else:
        return
    cur.execute(
        f"""
        UPDATE {qname(health_db)}.exam_ledgers
        SET check_status = %s,
            check_reason = %s
        WHERE source_type = %s
          AND {qname(source_column)} = %s
        """,
        (check_status, check_reason, ledger_type, ledger_id),
    )


def fetch_target_xml_ledgers(
    cur: Any,
    *,
    health_db: str,
    event_id: int,
    limit: int = 0,
) -> list[dict[str, Any]]:
    params: list[Any] = [event_id]
    limit_sql = ""
    if limit:
        limit_sql = "LIMIT %s"
        params.append(limit)
    cur.execute(
        f"""
        SELECT
          'XML' AS ledger_type,
          id,
          event_id,
          subscriber_id,
          hia_subscriber_id,
          exam_date,
          birthdate
        FROM {qname(health_db)}.xml_ledger
        WHERE event_id = %s
          AND xml_status = 'READY'
        ORDER BY id
        {limit_sql}
        """,
        tuple(params),
    )
    return list(cur.fetchall())


def fetch_target_csv_ledgers(
    cur: Any,
    *,
    health_db: str,
    event_id: int,
    limit: int = 0,
) -> list[dict[str, Any]]:
    params: list[Any] = [event_id]
    limit_sql = ""
    if limit:
        limit_sql = "LIMIT %s"
        params.append(limit)
    cur.execute(
        f"""
        SELECT
          'CSV' AS ledger_type,
          csv_row_ledger_id AS id,
          event_id,
          subscriber_id,
          hia_subscriber_id,
          exam_date,
          birthdate
        FROM {qname(health_db)}.csv_row_ledger
        WHERE event_id = %s
          AND row_status IN ('READY', 'ERROR')
        ORDER BY csv_row_ledger_id
        {limit_sql}
        """,
        tuple(params),
    )
    return list(cur.fetchall())


def fetch_target_exam_ledgers(
    cur: Any,
    *,
    health_db: str,
    event_id: int,
    limit: int = 0,
) -> list[dict[str, Any]]:
    params: list[Any] = [event_id]
    limit_sql = ""
    if limit:
        limit_sql = "LIMIT %s"
        params.append(limit)
    cur.execute(
        f"""
        SELECT
          'EXAM' AS ledger_type,
          exam_ledger_id AS id,
          exam_ledger_id,
          source_type,
          source_xml_ledger_id,
          source_csv_row_ledger_id,
          event_id,
          subscriber_id,
          hia_subscriber_id,
          exam_date,
          birthdate
        FROM {qname(health_db)}.exam_ledgers
        WHERE event_id = %s
          AND (
            (source_type = 'XML' AND xml_status = 'READY')
            OR (source_type = 'CSV' AND row_status IN ('READY', 'ERROR'))
            OR source_type = 'PAPER'
          )
        ORDER BY exam_ledger_id
        {limit_sql}
        """,
        tuple(params),
    )
    return list(cur.fetchall())


def fetch_target_case_ledgers(
    cur: Any,
    *,
    health_db: str,
    event_id: int,
    limit: int = 0,
) -> list[dict[str, Any]]:
    params: list[Any] = [event_id]
    limit_sql = ""
    if limit:
        limit_sql = "LIMIT %s"
        params.append(limit)
    cur.execute(
        f"""
        SELECT
          'EXPORT_CASE' AS ledger_type,
          exam_export_case_id AS id,
          exam_export_case_id,
          event_id,
          subscriber_id,
          hia_subscriber_id,
          exam_date,
          birthdate
        FROM {qname(health_db)}.exam_export_cases
        WHERE event_id = %s
          AND value_build_status = 'READY'
          AND subscriber_match_status = 'MATCHED'
        ORDER BY exam_export_case_id
        {limit_sql}
        """,
        tuple(params),
    )
    return list(cur.fetchall())


def fetch_target_check_ledgers(cur: Any, *, config: CheckConfig) -> list[dict[str, Any]]:
    if config.ledger_type == LEDGER_TYPE_EXPORT_CASE:
        return fetch_target_case_ledgers(
            cur,
            health_db=config.health_db,
            event_id=config.event_id,
            limit=config.limit,
        )
    if config.ledger_type == LEDGER_TYPE_EXAM:
        return fetch_target_exam_ledgers(
            cur,
            health_db=config.health_db,
            event_id=config.event_id,
            limit=config.limit,
        )
    if config.ledger_type == LEDGER_TYPE_XML:
        return fetch_target_xml_ledgers(
            cur,
            health_db=config.health_db,
            event_id=config.event_id,
            limit=config.limit,
        )
    if config.ledger_type == LEDGER_TYPE_CSV:
        return fetch_target_csv_ledgers(
            cur,
            health_db=config.health_db,
            event_id=config.event_id,
            limit=config.limit,
        )
    exam_ledgers = fetch_target_exam_ledgers(
        cur,
        health_db=config.health_db,
        event_id=config.event_id,
        limit=0,
    )
    case_ledgers = fetch_target_case_ledgers(
        cur,
        health_db=config.health_db,
        event_id=config.event_id,
        limit=0,
    )
    ledgers = [*exam_ledgers, *case_ledgers]
    return ledgers[: config.limit] if config.limit else ledgers


def resolve_value_lookup_ref(ledger: Mapping[str, Any]) -> tuple[str, int]:
    ledger_type = str(ledger["ledger_type"])
    if ledger_type != LEDGER_TYPE_EXAM:
        return ledger_type, int(ledger["id"])

    source_type = str(ledger.get("source_type") or "")
    if source_type == LEDGER_TYPE_XML and ledger.get("source_xml_ledger_id") is not None:
        return LEDGER_TYPE_XML, int(ledger["source_xml_ledger_id"])
    if source_type == LEDGER_TYPE_CSV and ledger.get("source_csv_row_ledger_id") is not None:
        return LEDGER_TYPE_CSV, int(ledger["source_csv_row_ledger_id"])
    return LEDGER_TYPE_EXAM, int(ledger["exam_ledger_id"])


def process_ledgers(
    health_conn: Any,
    health_cur: Any,
    dev_cur: Any,
    config: CheckConfig,
    summary: CheckSummary,
) -> None:
    required_namecodes = fetch_article44_required_namecodes(dev_cur, dev_db=config.dev_db)
    article44_required_namecodes_by_detail = fetch_article44_required_namecodes_by_detail(
        dev_cur,
        dev_db=config.dev_db,
    )
    specific_required_namecodes = fetch_specific_health_required_namecodes(
        dev_cur,
        dev_db=config.dev_db,
        fallback=SPECIFIC_REQUIRED_NAMECODES,
    )
    event_year = get_event_year(dev_cur, event_id=config.event_id, dev_db=config.dev_db)
    fiscal_year_end = fiscal_year_end_date(event_year) if event_year is not None else None

    ledgers = fetch_target_check_ledgers(health_cur, config=config)
    ledger_refs = [(str(ledger["ledger_type"]), int(ledger["id"])) for ledger in ledgers]
    summary.ledgers_seen = len(ledgers)

    if not config.dry_run:
        summary.rows_deleted = delete_existing_results(health_cur, health_db=config.health_db, ledger_refs=ledger_refs)

    for ledger in ledgers:
        ledger_id = int(ledger["id"])
        value_ledger_type, value_ledger_id = resolve_value_lookup_ref(ledger)
        value_map = load_article44_value_map(
            health_cur,
            ledger_type=value_ledger_type,
            ledger_id=value_ledger_id,
            required_namecodes=required_namecodes,
            result_db=config.health_db,
            dev_db=config.dev_db,
        )
        article44_result = check_article44(value_map)
        validate_article44_result(article44_result)
        legal_result, legal_summary = aggregate_article44_legal_result(article44_result)
        specific_value_map = load_article44_value_map(
            health_cur,
            ledger_type=value_ledger_type,
            ledger_id=value_ledger_id,
            required_namecodes=specific_required_namecodes,
            result_db=config.health_db,
            dev_db=config.dev_db,
        )
        specific_result, specific_summary = aggregate_specific_result(
            value_map=specific_value_map,
            required_namecodes=specific_required_namecodes,
            birthdate=ledger.get("birthdate"),
            age_reference_date=fiscal_year_end,
            legal_result=legal_result,
        )
        check_status = aggregate_check_status(legal_result)
        check_reason = legal_summary
        if check_status == CHECK_STATUS_OK:
            summary.ok += 1
        elif check_status == CHECK_STATUS_WARNING:
            summary.warning += 1
        else:
            summary.ng += 1

        if config.dry_run and config.verbose:
            print_dry_run_detail(
                ledger=ledger,
                legal_result=legal_result,
                specific_result=specific_result,
                check_status=check_status,
                check_reason=check_reason,
                article44_result=article44_result,
            )

        if config.dry_run:
            continue
        insert_check_result(
            health_cur,
            health_db=config.health_db,
            ledger=ledger,
            article44_result=article44_result,
            legal_result=legal_result,
            specific_result=specific_result,
            legal_summary=legal_summary,
            specific_summary=specific_summary,
        )
        summary.rows_inserted += 1
        if ledger["ledger_type"] == LEDGER_TYPE_EXPORT_CASE:
            sync_export_case_missing_placeholders(
                health_cur,
                health_db=config.health_db,
                ledger=ledger,
                article44_result=article44_result,
                article44_required_namecodes_by_detail=article44_required_namecodes_by_detail,
                specific_summary=specific_summary,
                specific_required_namecodes=specific_required_namecodes,
            )
        if ledger["ledger_type"] == LEDGER_TYPE_EXAM:
            update_exam_ledger_check(
                health_cur,
                health_db=config.health_db,
                exam_ledger_id=ledger_id,
                check_status=check_status,
                check_reason=check_reason,
            )
        elif ledger["ledger_type"] == LEDGER_TYPE_EXPORT_CASE:
            update_exam_export_case_check(
                health_cur,
                health_db=config.health_db,
                exam_export_case_id=ledger_id,
                check_status=check_status,
                check_reason=check_reason,
            )
        elif ledger["ledger_type"] == LEDGER_TYPE_XML:
            update_xml_ledger_check(
                health_cur,
                health_db=config.health_db,
                ledger_id=ledger_id,
                check_status=check_status,
                check_reason=check_reason,
            )
            update_exam_ledgers_by_source_check(
                health_cur,
                health_db=config.health_db,
                ledger_type=LEDGER_TYPE_XML,
                ledger_id=ledger_id,
                check_status=check_status,
                check_reason=check_reason,
            )
        else:
            update_csv_row_ledger_check(
                health_cur,
                health_db=config.health_db,
                ledger_id=ledger_id,
                check_status=check_status,
                check_reason=check_reason,
            )
            update_exam_ledgers_by_source_check(
                health_cur,
                health_db=config.health_db,
                ledger_type=LEDGER_TYPE_CSV,
                ledger_id=ledger_id,
                check_status=check_status,
                check_reason=check_reason,
            )
        summary.ledgers_updated += 1

    if not config.dry_run and config.ledger_type == LEDGER_TYPE_EXPORT_CASE:
        refresh_export_case_readiness(health_cur, health_db=config.health_db, event_id=config.event_id)

    if not config.dry_run:
        health_conn.commit()


def run(config: CheckConfig, *, db_prefix: str = "PHR_DB_") -> CheckSummary:
    summary = CheckSummary(event_id=config.event_id, dry_run=config.dry_run)
    params = load_mysql_base_params(db_prefix)
    run_id: int | None = None

    with connect_ctx(params, database=config.health_db, autocommit=False) as health_conn:
        with connect_ctx(params, database=config.dev_db, autocommit=False) as dev_conn:
            with dict_cursor(health_conn) as health_cur:
                with dict_cursor(dev_conn) as dev_cur:
                    if not config.dry_run:
                        run_id = start_check_run(health_cur, config)
                        health_conn.commit()
                    try:
                        process_ledgers(health_conn, health_cur, dev_cur, config, summary)
                    except Exception as exc:
                        if not config.dry_run:
                            health_conn.rollback()
                            record_script_error(
                                health_cur,
                                run_id=run_id,
                                summary=summary,
                                error_code="CHECK_EXAM_RESULTS_FAILED",
                                message=f"check_exam_results failed: {type(exc).__name__}: {exc}",
                                field_value=f"event_id={config.event_id}",
                            )
                            if run_id is not None:
                                etl_finish_run(
                                    health_cur,
                                    run_id,
                                    summary.to_metrics(),
                                    status_override="failed",
                                    extra_notes=summary.to_message(),
                                )
                            health_conn.commit()
                        raise
                    if not config.dry_run and run_id is not None:
                        finish_check_run(health_cur, run_id, summary)
                        health_conn.commit()
    return summary


def main() -> int:
    args = parse_args()
    try:
        config = resolve_config(args)
        summary = run(config, db_prefix=args.db_prefix)
        summary.print()
        return 0 if summary.errors == 0 else 1
    except ValueError as exc:
        print(f"CONFIG_INVALID: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"CHECK_EXAM_RESULTS_FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
