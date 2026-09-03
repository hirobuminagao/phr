#!/usr/bin/env python3
"""Diagnose or merge one duplicate exam export case into another."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.examination.lookup import qname
from scripts.from_medical.script_lib.case_insurer_resolution import (
    load_event_insurer_context,
    resolve_case_insurer_number,
)


AUTOMATIC_REVIEW_STATUSES = {"NEEDS_CONFIRMATION", "RESOLVED_BY_SOURCE_VALUE", "NONE"}


def review_is_automatic(row: dict[str, Any]) -> bool:
    return (
        row.get("source_status") in AUTOMATIC_REVIEW_STATUSES
        and not row.get("source_note")
        and row.get("source_reviewed_by") is None
        and int(row.get("source_human_audit_count") or 0) == 0
        and row.get("target_status") in AUTOMATIC_REVIEW_STATUSES
        and not row.get("target_note")
        and row.get("target_reviewed_by") is None
        and int(row.get("target_human_audit_count") or 0) == 0
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Diagnose or merge duplicate exam export cases.")
    parser.add_argument("--target-case-id", type=int, required=True)
    parser.add_argument("--source-case-id", type=int, required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--app-user-id", type=int)
    parser.add_argument("--apply", action="store_true", help="Apply changes. Omit for dry-run.")
    parser.add_argument("--db-prefix", default="PHR_DB_")
    parser.add_argument("--health-db", default="health_exam_result")
    parser.add_argument("--dev-db", default="dev_phr")
    return parser.parse_args()


def _load_case(cur: Any, *, health_db: str, case_id: int, lock: bool) -> dict[str, Any]:
    cur.execute(
        f"SELECT * FROM {qname(health_db)}.exam_export_cases "
        "WHERE exam_export_case_id = %s" + (" FOR UPDATE" if lock else ""),
        (case_id,),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError(f"CASE_NOT_FOUND: case_id={case_id}")
    return dict(row)


def _count(cur: Any, sql: str, params: tuple[Any, ...]) -> int:
    cur.execute(sql, params)
    row = cur.fetchone()
    return int(row["count_value"] if row else 0)


def merge_cases(
    conn: Any,
    *,
    health_db: str,
    dev_db: str,
    target_case_id: int,
    source_case_id: int,
    reason: str,
    app_user_id: int | None,
    apply: bool,
) -> dict[str, Any]:
    if target_case_id == source_case_id:
        raise ValueError("CASE_MERGE_SELF_NOT_ALLOWED")
    if not reason.strip():
        raise ValueError("CASE_MERGE_REASON_REQUIRED")
    with dict_cursor(conn) as cur:
        target = _load_case(cur, health_db=health_db, case_id=target_case_id, lock=apply)
        source = _load_case(cur, health_db=health_db, case_id=source_case_id, lock=apply)
        identity_fields = ("event_id", "subscriber_id", "exam_date", "exam_facility_id")
        mismatches = [field for field in identity_fields if target.get(field) != source.get(field)]
        if mismatches:
            raise ValueError(f"CASE_IDENTITY_MISMATCH: fields={','.join(mismatches)}")
        if target.get("case_lifecycle_status") != "ACTIVE":
            raise ValueError(f"TARGET_CASE_NOT_ACTIVE: status={target.get('case_lifecycle_status')}")
        if source.get("case_lifecycle_status") != "ACTIVE":
            raise ValueError(f"SOURCE_CASE_NOT_ACTIVE: status={source.get('case_lifecycle_status')}")

        cur.execute(
            f"""
            SELECT ledger.insurer_number, ledger.insurer_number_export_value
            FROM {qname(health_db)}.exam_export_case_sources AS case_source
            INNER JOIN {qname(health_db)}.exam_ledgers AS ledger
              ON ledger.exam_ledger_id = case_source.source_exam_ledger_id
            WHERE case_source.exam_export_case_id IN (%s, %s)
              AND case_source.source_status = 'ACTIVE'
            """,
            (target_case_id, source_case_id),
        )
        ledger_rows = [dict(row) for row in cur.fetchall()]
        cur.execute(
            f"""
            SELECT normalized_value AS insurer_number_export_value
            FROM {qname(health_db)}.exam_case_basic_info_corrections
            WHERE exam_export_case_id IN (%s, %s)
              AND field_code = 'insurer_number'
              AND correction_status = 'ACTIVE'
            """,
            (target_case_id, source_case_id),
        )
        ledger_rows.extend(dict(row) for row in cur.fetchall())
        cur.execute(
            f"SELECT insurer_number FROM {qname(dev_db)}.subscribers WHERE id = %s LIMIT 1",
            (target["subscriber_id"],),
        )
        subscriber = cur.fetchone() or {}
        insurer_context = load_event_insurer_context(
            cur,
            event_id=int(target["event_id"]),
            exam_date=target["exam_date"],
            dev_db=dev_db,
        )
        resolved_insurer_number = resolve_case_insurer_number(
            context=insurer_context,
            subscriber_insurer_number=subscriber.get("insurer_number"),
            ledgers=ledger_rows,
        )

        cur.execute(
            f"""
            SELECT
              source_item.exam_case_check_review_item_id AS source_review_id,
              target_item.exam_case_check_review_item_id AS target_review_id,
              source_item.check_scope,
              source_item.check_item_code,
              source_item.review_status AS source_status,
              source_item.review_note AS source_note,
              source_item.reviewed_by_app_user_id AS source_reviewed_by,
              target_item.review_status AS target_status,
              target_item.review_note AS target_note,
              target_item.reviewed_by_app_user_id AS target_reviewed_by,
              (
                SELECT COUNT(*)
                FROM {qname(health_db)}.exam_case_check_review_item_audit_logs source_audit
                WHERE source_audit.exam_case_check_review_item_id = source_item.exam_case_check_review_item_id
                  AND (source_audit.changed_by_app_user_id IS NOT NULL OR source_audit.source <> 'CHECK_EXAM_RESULTS')
              ) AS source_human_audit_count,
              (
                SELECT COUNT(*)
                FROM {qname(health_db)}.exam_case_check_review_item_audit_logs target_audit
                WHERE target_audit.exam_case_check_review_item_id = target_item.exam_case_check_review_item_id
                  AND (target_audit.changed_by_app_user_id IS NOT NULL OR target_audit.source <> 'CHECK_EXAM_RESULTS')
              ) AS target_human_audit_count
            FROM {qname(health_db)}.exam_case_check_review_items source_item
            INNER JOIN {qname(health_db)}.exam_case_check_review_items target_item
              ON target_item.exam_export_case_id = %s
             AND target_item.check_scope = source_item.check_scope
             AND target_item.check_item_code = source_item.check_item_code
            WHERE source_item.exam_export_case_id = %s
            """,
            (target_case_id, source_case_id),
        )
        review_overlaps = [dict(row) for row in cur.fetchall()]
        manual_review_conflicts = [row for row in review_overlaps if not review_is_automatic(row)]
        correction_conflicts = _count(
            cur,
            f"""
            SELECT COUNT(*) AS count_value
            FROM {qname(health_db)}.exam_case_basic_info_corrections source_correction
            INNER JOIN {qname(health_db)}.exam_case_basic_info_corrections target_correction
              ON target_correction.exam_export_case_id = %s
             AND target_correction.field_code = source_correction.field_code
            WHERE source_correction.exam_export_case_id = %s
            """,
            (target_case_id, source_case_id),
        )
        if manual_review_conflicts or correction_conflicts:
            raise ValueError(
                "CASE_MERGE_MANUAL_STATE_CONFLICT: "
                f"review_items={len(manual_review_conflicts)} corrections={correction_conflicts}"
            )

        tables = {
            "sources": "exam_export_case_sources",
            "values": "exam_export_case_values",
            "reviews": "exam_case_check_review_items",
            "corrections": "exam_case_basic_info_corrections",
            "manual_drafts": "manual_exam_entry_drafts",
            "export_list_entries": "ops_xml_export_list_cases",
        }
        counts = {
            name: _count(
                cur,
                f"SELECT COUNT(*) AS count_value FROM {qname(health_db)}.{table} WHERE exam_export_case_id = %s",
                (source_case_id,),
            )
            for name, table in tables.items()
        }
        counts["export_members"] = _count(
            cur,
            f"""
            SELECT COUNT(*) AS count_value
            FROM {qname(health_db)}.xml_export_members
            WHERE ledger_type = 'CASE' AND ledger_id = %s
            """,
            (source_case_id,),
        )
        result = {
            "mode": "apply" if apply else "dry-run",
            "target_case_id": target_case_id,
            "source_case_id": source_case_id,
            "identity": {field: str(target.get(field)) for field in identity_fields},
            "target_insurer_number": target.get("insurer_number_export_value") or target.get("insurer_number"),
            "source_insurer_number": source.get("insurer_number_export_value") or source.get("insurer_number"),
            "resolved_insurer_number": resolved_insurer_number,
            "allowed_insurer_numbers": sorted(insurer_context.allowed_insurer_numbers),
            "source_counts": counts,
            "automatic_review_overlaps": [
                {
                    "check_scope": row["check_scope"],
                    "check_item_code": row["check_item_code"],
                    "target_status": row["target_status"],
                    "source_status": row["source_status"],
                }
                for row in review_overlaps
            ],
        }
        if not apply:
            return result

        for overlap in review_overlaps:
            cur.execute(
                f"""
                UPDATE {qname(health_db)}.exam_case_check_review_item_audit_logs
                SET exam_case_check_review_item_id = %s,
                    exam_export_case_id = %s
                WHERE exam_case_check_review_item_id = %s
                """,
                (overlap["target_review_id"], target_case_id, overlap["source_review_id"]),
            )
            cur.execute(
                f"DELETE FROM {qname(health_db)}.exam_case_check_review_items "
                "WHERE exam_case_check_review_item_id = %s",
                (overlap["source_review_id"],),
            )

        for table in ("exam_export_case_sources", "exam_case_check_review_items", "exam_case_basic_info_corrections", "manual_exam_entry_drafts"):
            cur.execute(
                f"UPDATE {qname(health_db)}.{table} SET exam_export_case_id = %s WHERE exam_export_case_id = %s",
                (target_case_id, source_case_id),
            )
        cur.execute(
            f"""
            UPDATE {qname(health_db)}.exam_export_cases
            SET case_lifecycle_status = 'MERGED',
                merged_into_case_id = %s,
                merged_at = CURRENT_TIMESTAMP(3),
                merged_by_app_user_id = %s,
                merge_operation_reason = %s,
                case_status = 'MERGED',
                export_readiness_status = 'BLOCKED',
                export_readiness_reason = %s
            WHERE exam_export_case_id = %s
            """,
            (target_case_id, app_user_id, reason, f"MERGED_INTO_CASE:{target_case_id}", source_case_id),
        )
        cur.execute(
            f"""
            UPDATE {qname(health_db)}.exam_export_cases
            SET insurer_number = %s,
                insurer_number_export_value = %s,
                updated_at = CURRENT_TIMESTAMP(3)
            WHERE exam_export_case_id = %s
            """,
            (resolved_insurer_number, resolved_insurer_number, target_case_id),
        )
        conn.commit()
        return result


def main() -> int:
    args = parse_args()
    params = load_mysql_base_params(args.db_prefix)
    with connect_ctx(params, database=args.health_db) as conn:
        try:
            result = merge_cases(
                conn,
                health_db=args.health_db,
                dev_db=args.dev_db,
                target_case_id=args.target_case_id,
                source_case_id=args.source_case_id,
                reason=args.reason.strip(),
                app_user_id=args.app_user_id,
                apply=bool(args.apply),
            )
        except Exception:
            conn.rollback()
            raise
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
