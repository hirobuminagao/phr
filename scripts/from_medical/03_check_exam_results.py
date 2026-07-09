#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Phase7 exam check result entry point."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
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
from scripts.lib.examination.lookup import COMMON_GROUP, LEGAL_GROUP, SPECIFIC_GROUP
from scripts.lib.examination.lookup import fetch_exam_values
from scripts.lib.examination.lookup import fetch_group_namecodes
from scripts.lib.examination.lookup import fetch_identity_members
from scripts.lib.examination.lookup import fetch_method_rules
from scripts.lib.examination.lookup import fetch_target_ledgers
from scripts.lib.examination.lookup import qname
from scripts.lib.examination.models import RESULT_NG, RESULT_OK, RESULT_WARNING
from scripts.lib.examination.models import STATUS_INVALID, STATUS_MISSING, ItemResult
from scripts.lib.examination.rules import build_value_index, evaluate_identity


HEALTH_EXAM_RESULT_DB = "health_exam_result"
DEV_PHR_DB = "dev_phr"
ETL_PHASE = "CHECK_EXAM_RESULTS"
ETL_SOURCE = "FROM_MEDICAL"

CHECK_STATUS_PENDING = "PENDING"
CHECK_STATUS_OK = "OK"
CHECK_STATUS_WARNING = "WARNING"
CHECK_STATUS_NG = "NG"

GROUP_CODES = (COMMON_GROUP, LEGAL_GROUP, SPECIFIC_GROUP)


@dataclass(frozen=True)
class CheckConfig:
    event_id: int
    health_db: str
    dev_db: str
    dry_run: bool
    limit: int


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
    parser.add_argument("--event-id", type=int, required=True, help="Target event_id.")
    parser.add_argument("--dry-run", action="store_true", help="Read and report without DB writes.")
    parser.add_argument("--limit", type=int, default=0, help="Maximum xml_ledger rows to process. 0 means unlimited.")
    parser.add_argument("--db-prefix", default="PHR_DB_", help="Environment prefix for DB connection.")
    parser.add_argument("--health-db", default=HEALTH_EXAM_RESULT_DB, help="Override health_exam_result schema name.")
    parser.add_argument("--dev-db", default=DEV_PHR_DB, help="Override dev_phr schema name.")
    return parser.parse_args()


def config_from_args(args: argparse.Namespace) -> CheckConfig:
    if args.event_id <= 0:
        raise ValueError("event_id must be positive")
    if args.limit < 0:
        raise ValueError("limit must be >= 0")
    return CheckConfig(
        event_id=args.event_id,
        health_db=args.health_db,
        dev_db=args.dev_db,
        dry_run=bool(args.dry_run),
        limit=int(args.limit or 0),
    )


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


def build_item_results(
    *,
    common_identities: dict[str, dict[str, Any]],
    common_rules: dict[str, list[Any]],
    common_namecodes: dict[str, set[str]],
    values: list[Any],
) -> dict[str, ItemResult]:
    index = build_value_index(values)
    results: dict[str, ItemResult] = {}
    ordered_identity_codes = sorted(
        common_identities,
        key=lambda code: (
            common_identities[code].get("sort_no") is None,
            int(common_identities[code].get("sort_no") or 0),
            code,
        ),
    )

    visiting: set[str] = set()

    def ensure_result(identity_code: str) -> ItemResult:
        if identity_code in results:
            return results[identity_code]
        if identity_code in visiting:
            result = ItemResult(identity_code, STATUS_INVALID, "RULE_DEPENDENCY_CYCLE")
            results[identity_code] = result
            return result
        visiting.add(identity_code)
        for rule in common_rules.get(identity_code, []):
            for source_identity in rule.source_identity_codes:
                if source_identity in common_identities and source_identity not in results:
                    ensure_result(source_identity)
        result = evaluate_identity(
            identity_code,
            rules=common_rules.get(identity_code, []),
            namecodes=common_namecodes.get(identity_code, set()),
            index=index,
            result_by_identity=results,
        )
        results[identity_code] = result
        visiting.discard(identity_code)
        return result

    for identity_code in ordered_identity_codes:
        ensure_result(identity_code)
    return results


def summarize_group(
    group_members: dict[str, dict[str, Any]],
    item_results: dict[str, ItemResult],
    *,
    missing_result: str,
) -> tuple[str, str | None]:
    reasons: list[str] = []
    final_result = RESULT_OK
    for identity_code, member in sorted_group_members(group_members):
        required = int(member.get("required_flag") or 0) == 1
        result = item_results.get(identity_code)
        if result is None:
            if required:
                final_result = worse_result(final_result, missing_result)
                reasons.append(f"{identity_code}:MISSING")
            continue
        if result.reason:
            reasons.append(f"{identity_code}:{result.reason}")
        if required and not result.is_ok_like:
            final_result = worse_result(final_result, missing_result)
    return final_result, " | ".join(reasons) if reasons else None


def sorted_group_members(group_members: dict[str, dict[str, Any]]) -> list[tuple[str, dict[str, Any]]]:
    return sorted(
        group_members.items(),
        key=lambda item: (
            item[1].get("sort_no") is None,
            int(item[1].get("sort_no") or 0),
            item[0],
        ),
    )


def worse_result(current: str, candidate: str) -> str:
    rank = {RESULT_OK: 0, RESULT_WARNING: 1, RESULT_NG: 2}
    return candidate if rank[candidate] > rank[current] else current


def aggregate_check_status(legal_result: str, specific_result: str) -> str:
    if legal_result == RESULT_NG:
        return CHECK_STATUS_NG
    if specific_result == RESULT_NG:
        return CHECK_STATUS_NG
    if legal_result == RESULT_WARNING or specific_result == RESULT_WARNING:
        return CHECK_STATUS_WARNING
    return CHECK_STATUS_OK


def aggregate_check_reason(legal_summary: str | None, specific_summary: str | None) -> str | None:
    parts: list[str] = []
    if legal_summary:
        parts.append(f"LEGAL:{legal_summary}")
    if specific_summary:
        parts.append(f"SPECIFIC:{specific_summary}")
    return " | ".join(parts) if parts else None


def result_columns(item_results: dict[str, ItemResult]) -> dict[str, Any]:
    columns: dict[str, Any] = {}
    for identity_code, result in item_results.items():
        suffix = identity_code.lower()
        columns[f"status_{suffix}"] = result.status
        columns[f"reason_{suffix}"] = result.reason
    return columns


def insert_check_result(
    cur: Any,
    *,
    health_db: str,
    ledger: dict[str, Any],
    item_results: dict[str, ItemResult],
    legal_result: str,
    specific_result: str,
    legal_summary: str | None,
    specific_summary: str | None,
) -> None:
    row: dict[str, Any] = {
        "xml_ledger_id": ledger["id"],
        "event_id": ledger["event_id"],
        "subscriber_id": ledger.get("subscriber_id"),
        "hia_subscriber_id": ledger.get("hia_subscriber_id"),
        "legal_check_result": legal_result,
        "specific_check_result": specific_result,
        "legal_reason_summary": legal_summary,
        "specific_reason_summary": specific_summary,
    }
    row.update(result_columns(item_results))
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


def delete_existing_results(cur: Any, *, health_db: str, ledger_ids: list[int]) -> int:
    if not ledger_ids:
        return 0
    placeholders = ", ".join(["%s"] * len(ledger_ids))
    cur.execute(
        f"""
        DELETE FROM {qname(health_db)}.exam_check_results
        WHERE xml_ledger_id IN ({placeholders})
        """,
        tuple(ledger_ids),
    )
    return int(cur.rowcount or 0)


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


def process_ledgers(
    health_conn: Any,
    health_cur: Any,
    dev_cur: Any,
    config: CheckConfig,
    summary: CheckSummary,
) -> None:
    identities = fetch_identity_members(dev_cur, dev_db=config.dev_db, group_codes=GROUP_CODES)
    method_rules = fetch_method_rules(dev_cur, dev_db=config.dev_db, group_codes=GROUP_CODES)
    group_namecodes = fetch_group_namecodes(dev_cur, dev_db=config.dev_db, group_codes=GROUP_CODES)
    common_identities = identities.get(COMMON_GROUP, {})
    legal_identities = identities.get(LEGAL_GROUP, {})
    specific_identities = identities.get(SPECIFIC_GROUP, {})

    ledgers = fetch_target_ledgers(health_cur, health_db=config.health_db, event_id=config.event_id, limit=config.limit)
    ledger_ids = [int(ledger["id"]) for ledger in ledgers]
    summary.ledgers_seen = len(ledgers)
    values_by_ledger = fetch_exam_values(
        health_cur,
        health_db=config.health_db,
        dev_db=config.dev_db,
        ledger_ids=ledger_ids,
    )

    if not config.dry_run:
        summary.rows_deleted = delete_existing_results(health_cur, health_db=config.health_db, ledger_ids=ledger_ids)

    for ledger in ledgers:
        ledger_id = int(ledger["id"])
        item_results = build_item_results(
            common_identities=common_identities,
            common_rules=method_rules.get(COMMON_GROUP, {}),
            common_namecodes=group_namecodes.get(COMMON_GROUP, {}),
            values=values_by_ledger.get(ledger_id, []),
        )
        legal_result, legal_summary = summarize_group(legal_identities, item_results, missing_result=RESULT_NG)
        specific_result, specific_summary = summarize_group(specific_identities, item_results, missing_result=RESULT_WARNING)
        check_status = aggregate_check_status(legal_result, specific_result)
        check_reason = aggregate_check_reason(legal_summary, specific_summary)
        if check_status == CHECK_STATUS_OK:
            summary.ok += 1
        elif check_status == CHECK_STATUS_WARNING:
            summary.warning += 1
        else:
            summary.ng += 1

        if config.dry_run:
            continue
        insert_check_result(
            health_cur,
            health_db=config.health_db,
            ledger=ledger,
            item_results=item_results,
            legal_result=legal_result,
            specific_result=specific_result,
            legal_summary=legal_summary,
            specific_summary=specific_summary,
        )
        summary.rows_inserted += 1
        update_xml_ledger_check(
            health_cur,
            health_db=config.health_db,
            ledger_id=ledger_id,
            check_status=check_status,
            check_reason=check_reason,
        )
        summary.ledgers_updated += 1

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
        config = config_from_args(args)
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
