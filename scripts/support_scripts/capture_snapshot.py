#!/usr/bin/env python3
"""Capture query results as immutable support incident snapshot targets."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor


SUPPORT_DB = "phr_system_support"
QUERY_DIR = Path(__file__).resolve().parent / "queries"
REQUIRED_COLUMNS = {"target_type", "target_id"}
SNAPSHOT_COLUMNS = {
    "target_type",
    "target_schema",
    "target_table",
    "target_id",
    "event_id",
    "exam_export_case_id",
    "source_exam_ledger_id",
    "reprocess_required",
    "reexport_required",
}


def json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


def load_select_sql(path: Path) -> str:
    sql = path.read_text(encoding="utf-8").strip()
    statement = sql[:-1].rstrip() if sql.endswith(";") else sql
    if ";" in statement:
        raise ValueError("query file must contain exactly one SELECT statement")
    if not re.match(r"^(SELECT|WITH)\b", statement, flags=re.IGNORECASE):
        raise ValueError("query file must start with SELECT or WITH")
    return statement


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="事象IDに対応するSQLで修正前後スナップショットを取得します。")
    parser.add_argument("--incident-id", type=int, required=True)
    parser.add_argument("--phase", choices=("BEFORE", "AFTER", "VERIFY"), default="BEFORE")
    parser.add_argument("--query-file", type=Path, help="省略時は queries/{incident_id:03d}.sql")
    parser.add_argument("--batch-id", default=None, help="省略時はUUIDを生成")
    parser.add_argument("--captured-by", default=os.getenv("USER") or "support_script")
    parser.add_argument("--database", default=SUPPORT_DB)
    parser.add_argument("--db-prefix", default="PHR_DB_")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--create-export-list",
        action="store_true",
        help="AFTERで再出力対象を事象専用の出力リストへ登録する",
    )
    parser.add_argument("--export-list-name", default=None, help="再出力リスト名。省略時は事象名から生成")
    return parser.parse_args()


def default_query_path(incident_id: int, phase: str) -> Path:
    suffix = "_after" if phase == "AFTER" else ""
    return QUERY_DIR / f"{incident_id:03d}{suffix}.sql"


def create_or_update_reexport_lists(
    cur: Any,
    *,
    incident: dict[str, Any],
    rows: list[dict[str, Any]],
    operator: str,
    list_name: str | None,
) -> dict[int, int]:
    targets_by_event: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        if not bool(row.get("reexport_required")):
            continue
        event_id = int(row["event_id"])
        targets_by_event.setdefault(event_id, []).append(row)

    result: dict[int, int] = {}
    for event_id, targets in targets_by_event.items():
        selector_marker = f"support_incident_id={incident['incident_id']}\nsupport_phase=AFTER"
        cur.execute(
            """
            SELECT xml_export_list_id
            FROM health_exam_result.ops_xml_export_lists
            WHERE event_id = %s
              AND selector_summary LIKE %s
              AND list_status IN ('DRAFT', 'READY', 'PARTIAL', 'ERROR')
            ORDER BY xml_export_list_id DESC
            LIMIT 1
            """,
            (event_id, f"%{selector_marker}%"),
        )
        existing = cur.fetchone()
        if existing:
            xml_export_list_id = int(existing["xml_export_list_id"])
        else:
            generated_name = list_name or f"【再出力】{incident['incident_key']} event {event_id}"
            cur.execute(
                """
                INSERT INTO health_exam_result.ops_xml_export_lists (
                  event_id, list_name, list_status, selector_summary,
                  include_exported, created_by, confirmed_by, confirmed_at, list_note
                ) VALUES (%s, %s, 'READY', %s, 1, %s, %s, CURRENT_TIMESTAMP(3), %s)
                """,
                (
                    event_id,
                    generated_name,
                    selector_marker,
                    operator,
                    operator,
                    f"phr_system_support incident_id={incident['incident_id']} のAFTER比較から自動作成",
                ),
            )
            xml_export_list_id = int(cur.lastrowid)

        for row in targets:
            cur.execute(
                """
                INSERT INTO health_exam_result.ops_xml_export_list_cases (
                  xml_export_list_id, exam_export_case_id, list_case_status,
                  export_readiness_status_snapshot, export_readiness_reason_snapshot,
                  added_by, list_case_note
                ) VALUES (%s, %s, 'READY', %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                  list_case_status = 'READY',
                  export_readiness_status_snapshot = VALUES(export_readiness_status_snapshot),
                  export_readiness_reason_snapshot = VALUES(export_readiness_reason_snapshot),
                  added_by = VALUES(added_by),
                  added_at = CURRENT_TIMESTAMP(3),
                  removed_by = NULL,
                  removed_at = NULL,
                  remove_reason = NULL,
                  list_case_note = VALUES(list_case_note),
                  updated_at = CURRENT_TIMESTAMP(3)
                """,
                (
                    xml_export_list_id,
                    row["exam_export_case_id"],
                    row.get("current_export_readiness_status"),
                    row.get("current_export_readiness_reason"),
                    operator,
                    f"support incident {incident['incident_id']} author修復後の再出力対象",
                ),
            )
        result[event_id] = xml_export_list_id
    return result


def main() -> int:
    args = parse_args()
    if args.create_export_list and args.phase != "AFTER":
        raise ValueError("--create-export-list can only be used with --phase AFTER")
    query_path = args.query_file or default_query_path(args.incident_id, args.phase)
    query_sql = load_select_sql(query_path)
    batch_id = args.batch_id or str(uuid.uuid4())
    params = load_mysql_base_params(args.db_prefix)

    with connect_ctx(params, database=args.database, autocommit=False) as conn:
        cur = dict_cursor(conn)
        cur.execute(
            "SELECT incident_id, incident_key, event_id, status FROM support_incidents WHERE incident_id = %s",
            (args.incident_id,),
        )
        incident = cur.fetchone()
        if not incident:
            raise RuntimeError(f"incident_id={args.incident_id} was not found")

        cur.execute(
            query_sql,
            {"incident_id": args.incident_id, "event_id": incident.get("event_id")},
        )
        rows = [dict(row) for row in cur.fetchall()]
        missing = REQUIRED_COLUMNS - set(rows[0]) if rows else set()
        if missing:
            raise ValueError(f"query result is missing required columns: {', '.join(sorted(missing))}")

        print(
            f"incident_id={args.incident_id} key={incident['incident_key']} "
            f"phase={args.phase} batch_id={batch_id} targets={len(rows)} dry_run={args.dry_run}"
        )
        for row in rows[:20]:
            print(
                f"  {row['target_type']}:{row['target_id']} "
                f"status={row.get('comparison_status') or '-'} "
                f"reprocess={int(bool(row.get('reprocess_required')))} "
                f"reexport={int(bool(row.get('reexport_required')))}"
            )
        if len(rows) > 20:
            print(f"  ... and {len(rows) - 20} more")
        if args.dry_run:
            conn.rollback()
            return 0

        insert_sql = """
            INSERT INTO support_snapshot_targets (
              incident_id, capture_batch_id, snapshot_phase,
              target_type, target_schema, target_table, target_id,
              event_id, exam_export_case_id, source_exam_ledger_id,
              reprocess_required, reexport_required, snapshot_data, captured_by
            ) VALUES (
              %(incident_id)s, %(capture_batch_id)s, %(snapshot_phase)s,
              %(target_type)s, %(target_schema)s, %(target_table)s, %(target_id)s,
              %(event_id)s, %(exam_export_case_id)s, %(source_exam_ledger_id)s,
              %(reprocess_required)s, %(reexport_required)s, %(snapshot_data)s, %(captured_by)s
            )
        """
        for row in rows:
            cur.execute(
                insert_sql,
                {
                    "incident_id": args.incident_id,
                    "capture_batch_id": batch_id,
                    "snapshot_phase": args.phase,
                    "target_type": str(row["target_type"]),
                    "target_schema": row.get("target_schema"),
                    "target_table": row.get("target_table"),
                    "target_id": str(row["target_id"]),
                    "event_id": row.get("event_id"),
                    "exam_export_case_id": row.get("exam_export_case_id"),
                    "source_exam_ledger_id": row.get("source_exam_ledger_id"),
                    "reprocess_required": int(bool(row.get("reprocess_required"))),
                    "reexport_required": int(bool(row.get("reexport_required"))),
                    "snapshot_data": json.dumps(row, ensure_ascii=False, default=json_default),
                    "captured_by": args.captured_by,
                },
            )
        export_list_ids: dict[int, int] = {}
        if args.create_export_list:
            export_list_ids = create_or_update_reexport_lists(
                cur,
                incident=dict(incident),
                rows=rows,
                operator=args.captured_by,
                list_name=args.export_list_name,
            )
            for event_id, xml_export_list_id in export_list_ids.items():
                cur.execute(
                    """
                    UPDATE support_snapshot_targets
                    SET action_status = 'PROCESSED',
                        snapshot_data = JSON_SET(snapshot_data, '$.xml_export_list_id', %s),
                        processed_at = CURRENT_TIMESTAMP(3)
                    WHERE incident_id = %s
                      AND capture_batch_id = %s
                      AND snapshot_phase = 'AFTER'
                      AND event_id = %s
                      AND reexport_required = 1
                    """,
                    (xml_export_list_id, args.incident_id, batch_id, event_id),
                )
        cur.execute(
            "UPDATE support_incidents SET status = 'SNAPSHOTTED' WHERE incident_id = %s AND status = 'OPEN'",
            (args.incident_id,),
        )
        conn.commit()
        for event_id, xml_export_list_id in export_list_ids.items():
            print(f"export_list event_id={event_id} xml_export_list_id={xml_export_list_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
