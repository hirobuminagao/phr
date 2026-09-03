#!/usr/bin/env python3
"""Diagnose or close empty cases left by exam-facility re-resolution."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.examination.lookup import qname


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Close orphaned cases superseded by facility re-resolution.")
    parser.add_argument("--event-id", type=int)
    parser.add_argument("--old-case-id", type=int, action="append", default=[])
    parser.add_argument("--apply", action="store_true", help="Apply changes. Omit for dry-run.")
    parser.add_argument("--all-eligible", action="store_true")
    parser.add_argument("--reason", default="健診機関再解決により空になった旧caseを差し替え済みに整理")
    parser.add_argument("--app-user-id", type=int)
    parser.add_argument("--db-prefix", default="PHR_DB_")
    parser.add_argument("--health-db", default="health_exam_result")
    return parser.parse_args()


def classify_pair(old: dict[str, Any], successor: dict[str, Any], dependencies: dict[str, int]) -> list[str]:
    reasons: list[str] = []
    if any(old.get(field) != successor.get(field) for field in ("event_id", "subscriber_id", "exam_date", "facility_code")):
        reasons.append("IDENTITY_MISMATCH")
    if old.get("exam_facility_id") == successor.get("exam_facility_id"):
        reasons.append("FACILITY_ID_NOT_CHANGED")
    if old.get("case_lifecycle_status") != "ACTIVE" or successor.get("case_lifecycle_status") != "ACTIVE":
        reasons.append("CASE_NOT_ACTIVE")
    if int(old.get("active_source_count") or 0):
        reasons.append("OLD_HAS_ACTIVE_SOURCE")
    if int(old.get("value_count") or 0):
        reasons.append("OLD_HAS_VALUES")
    if not int(successor.get("active_source_count") or 0):
        reasons.append("SUCCESSOR_HAS_NO_ACTIVE_SOURCE")
    if not int(successor.get("value_count") or 0):
        reasons.append("SUCCESSOR_HAS_NO_VALUES")
    if old.get("created_at") and successor.get("created_at") and old["created_at"] >= successor["created_at"]:
        reasons.append("OLD_NOT_EARLIER")
    if old.get("xml_export_status") == "EXPORTED" or dependencies.get("export_members", 0):
        reasons.append("OLD_HAS_EXPORT_HISTORY")
    if dependencies.get("human_reviews", 0):
        reasons.append("OLD_HAS_HUMAN_REVIEW")
    if dependencies.get("corrections", 0):
        reasons.append("OLD_HAS_CORRECTION")
    if dependencies.get("manual_drafts", 0):
        reasons.append("OLD_HAS_MANUAL_DRAFT")
    if dependencies.get("active_exported_list_entries", 0):
        reasons.append("OLD_HAS_ACTIVE_EXPORTED_LIST_ENTRY")
    return reasons


def load_active_cases(cur: Any, *, health_db: str, event_id: int | None) -> list[dict[str, Any]]:
    event_filter = "AND eec.event_id = %s" if event_id is not None else ""
    params: tuple[Any, ...] = (event_id,) if event_id is not None else ()
    cur.execute(
        f"""
        SELECT eec.exam_export_case_id, eec.event_id, eec.subscriber_id, eec.exam_date,
               eec.exam_facility_id, eec.facility_code, eec.facility_name,
               eec.case_lifecycle_status, eec.case_status, eec.xml_export_status, eec.created_at,
               COALESCE(src.active_source_count, 0) AS active_source_count,
               COALESCE(val.value_count, 0) AS value_count
        FROM {qname(health_db)}.exam_export_cases eec
        LEFT JOIN (
          SELECT exam_export_case_id, COUNT(*) AS active_source_count
          FROM {qname(health_db)}.exam_export_case_sources
          WHERE source_status = 'ACTIVE'
          GROUP BY exam_export_case_id
        ) src ON src.exam_export_case_id = eec.exam_export_case_id
        LEFT JOIN (
          SELECT exam_export_case_id, COUNT(*) AS value_count
          FROM {qname(health_db)}.exam_export_case_values
          GROUP BY exam_export_case_id
        ) val ON val.exam_export_case_id = eec.exam_export_case_id
        WHERE eec.case_lifecycle_status = 'ACTIVE'
          AND eec.subscriber_id IS NOT NULL
          AND COALESCE(eec.facility_code, '') <> ''
          {event_filter}
        ORDER BY eec.event_id, eec.subscriber_id, eec.exam_date, eec.facility_code,
                 eec.created_at, eec.exam_export_case_id
        """,
        params,
    )
    return [dict(row) for row in cur.fetchall()]


def load_dependencies(cur: Any, *, health_db: str, case_id: int) -> dict[str, int]:
    cur.execute(
        f"""
        SELECT
          (SELECT COUNT(*) FROM {qname(health_db)}.exam_case_check_review_items review
           WHERE review.exam_export_case_id = %s AND (
             review.review_status NOT IN ('NEEDS_CONFIRMATION', 'RESOLVED_BY_SOURCE_VALUE', 'NONE')
             OR COALESCE(review.review_note, '') <> ''
             OR review.reviewed_by_app_user_id IS NOT NULL
             OR EXISTS (
               SELECT 1 FROM {qname(health_db)}.exam_case_check_review_item_audit_logs audit
               WHERE audit.exam_case_check_review_item_id = review.exam_case_check_review_item_id
                 AND (audit.changed_by_app_user_id IS NOT NULL OR audit.source <> 'CHECK_EXAM_RESULTS')
             )
           )) AS human_reviews,
          (SELECT COUNT(*) FROM {qname(health_db)}.exam_case_basic_info_corrections WHERE exam_export_case_id = %s) AS corrections,
          (SELECT COUNT(*) FROM {qname(health_db)}.manual_exam_entry_drafts WHERE exam_export_case_id = %s) AS manual_drafts,
          (SELECT COUNT(*) FROM {qname(health_db)}.xml_export_members WHERE ledger_type = 'CASE' AND ledger_id = %s) AS export_members,
          (SELECT COUNT(*) FROM {qname(health_db)}.ops_xml_export_list_cases
           WHERE exam_export_case_id = %s AND removed_at IS NULL) AS active_export_list_entries,
          (SELECT COUNT(*) FROM {qname(health_db)}.ops_xml_export_list_cases
           WHERE exam_export_case_id = %s AND removed_at IS NULL AND list_case_status = 'EXPORTED') AS active_exported_list_entries
        """,
        (case_id, case_id, case_id, case_id, case_id, case_id),
    )
    return {key: int(value or 0) for key, value in dict(cur.fetchone() or {}).items()}


def diagnose(cur: Any, *, health_db: str, event_id: int | None) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for case in load_active_cases(cur, health_db=health_db, event_id=event_id):
        groups[(case["event_id"], case["subscriber_id"], case["exam_date"], case["facility_code"])].append(case)

    results: list[dict[str, Any]] = []
    for group in groups.values():
        if len(group) < 2:
            continue
        old_cases = [row for row in group if not row["active_source_count"] and not row["value_count"]]
        successors = [row for row in group if row["active_source_count"] and row["value_count"]]
        for old in old_cases:
            dependencies = load_dependencies(cur, health_db=health_db, case_id=int(old["exam_export_case_id"]))
            successor = successors[0] if len(successors) == 1 else None
            reasons = classify_pair(old, successor, dependencies) if successor else ["SUCCESSOR_NOT_UNIQUE"]
            results.append({
                "old_case_id": int(old["exam_export_case_id"]),
                "successor_case_id": int(successor["exam_export_case_id"]) if successor else None,
                "event_id": int(old["event_id"]),
                "subscriber_id": int(old["subscriber_id"]),
                "exam_date": str(old["exam_date"]),
                "facility_code": old["facility_code"],
                "old_exam_facility_id": int(old["exam_facility_id"]),
                "successor_exam_facility_id": int(successor["exam_facility_id"]) if successor else None,
                "eligible": not reasons,
                "blocked_reasons": reasons,
                "dependencies": dependencies,
            })
    return results


def load_locked_pair(cur: Any, *, health_db: str, old_case_id: int, successor_case_id: int) -> tuple[dict[str, Any], dict[str, Any]]:
    cur.execute(
        f"""
        SELECT eec.exam_export_case_id, eec.event_id, eec.subscriber_id, eec.exam_date,
               eec.exam_facility_id, eec.facility_code, eec.case_lifecycle_status,
               eec.xml_export_status, eec.created_at,
               (SELECT COUNT(*) FROM {qname(health_db)}.exam_export_case_sources src
                WHERE src.exam_export_case_id = eec.exam_export_case_id AND src.source_status = 'ACTIVE') AS active_source_count,
               (SELECT COUNT(*) FROM {qname(health_db)}.exam_export_case_values val
                WHERE val.exam_export_case_id = eec.exam_export_case_id) AS value_count
        FROM {qname(health_db)}.exam_export_cases eec
        WHERE eec.exam_export_case_id IN (%s, %s)
        ORDER BY eec.exam_export_case_id
        FOR UPDATE
        """,
        (old_case_id, successor_case_id),
    )
    by_id = {int(item["exam_export_case_id"]): dict(item) for item in cur.fetchall()}
    if old_case_id not in by_id or successor_case_id not in by_id:
        raise ValueError(f"CASE_NOT_FOUND: old_case_id={old_case_id} successor_case_id={successor_case_id}")
    return by_id[old_case_id], by_id[successor_case_id]


def apply_one(cur: Any, *, health_db: str, row: dict[str, Any], reason: str, app_user_id: int | None) -> None:
    old_case_id = int(row["old_case_id"])
    successor_case_id = int(row["successor_case_id"])
    old, successor = load_locked_pair(
        cur, health_db=health_db, old_case_id=old_case_id, successor_case_id=successor_case_id
    )
    dependencies = load_dependencies(cur, health_db=health_db, case_id=old_case_id)
    blocked_reasons = classify_pair(old, successor, dependencies)
    cur.execute(
        f"""
        SELECT eec.exam_export_case_id
        FROM {qname(health_db)}.exam_export_cases eec
        WHERE eec.event_id = %s
          AND eec.subscriber_id = %s
          AND eec.exam_date = %s
          AND eec.facility_code = %s
          AND eec.case_lifecycle_status = 'ACTIVE'
          AND EXISTS (
            SELECT 1 FROM {qname(health_db)}.exam_export_case_sources src
            WHERE src.exam_export_case_id = eec.exam_export_case_id AND src.source_status = 'ACTIVE'
          )
          AND EXISTS (
            SELECT 1 FROM {qname(health_db)}.exam_export_case_values val
            WHERE val.exam_export_case_id = eec.exam_export_case_id
          )
        FOR UPDATE
        """,
        (old["event_id"], old["subscriber_id"], old["exam_date"], old["facility_code"]),
    )
    successor_ids = [int(item["exam_export_case_id"]) for item in cur.fetchall()]
    if successor_ids != [successor_case_id]:
        blocked_reasons.append("SUCCESSOR_NOT_UNIQUE")
    if blocked_reasons:
        raise ValueError(f"CASE_ELIGIBILITY_CHANGED: old_case_id={old_case_id} reasons={blocked_reasons}")
    cur.execute(
        f"""
        UPDATE {qname(health_db)}.ops_xml_export_list_cases
        SET list_case_status = 'REMOVED', removed_by = %s, removed_at = CURRENT_TIMESTAMP(3), remove_reason = %s
        WHERE exam_export_case_id = %s AND removed_at IS NULL AND list_case_status <> 'EXPORTED'
        """,
        (str(app_user_id) if app_user_id is not None else "SUPPORT_SCRIPT", reason, old_case_id),
    )
    cur.execute(
        f"""
        UPDATE {qname(health_db)}.exam_export_cases
        SET case_lifecycle_status = 'SUPERSEDED', successor_case_id = %s,
            lifecycle_closed_at = CURRENT_TIMESTAMP(3), lifecycle_closed_by_app_user_id = %s,
            lifecycle_close_reason = %s, case_status = 'SUPERSEDED',
            export_readiness_status = 'BLOCKED', export_readiness_reason = %s
        WHERE exam_export_case_id = %s AND case_lifecycle_status = 'ACTIVE'
        """,
        (successor_case_id, app_user_id, reason, f"SUPERSEDED_BY_CASE:{successor_case_id}", old_case_id),
    )
    if cur.rowcount != 1:
        raise ValueError(f"CASE_STATE_CHANGED: old_case_id={old_case_id}")


def main() -> int:
    args = parse_args()
    if args.all_eligible and args.old_case_id:
        raise SystemExit("--all-eligible and --old-case-id cannot be used together")
    if args.apply and not args.all_eligible and not args.old_case_id:
        raise SystemExit("--apply requires --old-case-id or --all-eligible")
    params = load_mysql_base_params(args.db_prefix)
    with connect_ctx(params, database=args.health_db) as conn:
        with dict_cursor(conn) as cur:
            rows = diagnose(cur, health_db=args.health_db, event_id=args.event_id)
            selected_ids = set(args.old_case_id)
            selected = [row for row in rows if row["eligible"] and (args.all_eligible or row["old_case_id"] in selected_ids)]
            missing_ids = sorted(selected_ids - {row["old_case_id"] for row in selected})
            if args.apply and missing_ids:
                raise ValueError(f"OLD_CASE_NOT_ELIGIBLE_OR_NOT_FOUND: case_ids={missing_ids}")
            if args.apply:
                try:
                    for row in selected:
                        apply_one(cur, health_db=args.health_db, row=row, reason=args.reason.strip(), app_user_id=args.app_user_id)
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
            output = {
                "mode": "apply" if args.apply else "dry-run",
                "candidate_count": len(rows),
                "eligible_count": sum(1 for row in rows if row["eligible"]),
                "blocked_count": sum(1 for row in rows if not row["eligible"]),
                "applied_count": len(selected) if args.apply else 0,
                "candidates": rows,
            }
    print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
