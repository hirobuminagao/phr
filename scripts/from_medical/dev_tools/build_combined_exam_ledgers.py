#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build COMBINED exam ledgers from matched XML/CSV source ledgers."""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.etl import RunMetrics
from scripts.lib.etl import finish_run as etl_finish_run
from scripts.lib.etl import start_run as etl_start_run


HEALTH_DB = "health_exam_result"
ETL_PHASE = "BUILD_COMBINED_EXAM_LEDGERS"
ETL_SOURCE = "FROM_MEDICAL"
LEDGER_TYPE_EXAM = "EXAM"


@dataclass(frozen=True)
class BuildConfig:
    event_id: int
    health_db: str
    dry_run: bool
    include_exported: bool
    limit_groups: int


@dataclass
class BuildSummary:
    event_id: int
    dry_run: bool
    source_ledgers: int = 0
    candidate_groups: int = 0
    combined_inserted: int = 0
    combined_updated: int = 0
    sources_upserted: int = 0
    values_inserted: int = 0
    values_deleted: int = 0
    groups_skipped: int = 0
    precedence_rules_applied: int = 0

    def to_message(self) -> str:
        return (
            f"build_combined_exam_ledgers event_id={self.event_id} sources={self.source_ledgers} "
            f"groups={self.candidate_groups} combined_inserted={self.combined_inserted} "
            f"combined_updated={self.combined_updated} sources_upserted={self.sources_upserted} "
            f"values_deleted={self.values_deleted} values_inserted={self.values_inserted} "
            f"precedence_rules_applied={self.precedence_rules_applied} "
            f"groups_skipped={self.groups_skipped} dry_run={self.dry_run}"
        )


LEDGER_COPY_COLUMNS = [
    "event_id",
    "subscriber_id",
    "hia_subscriber_id",
    "identity_hash",
    "person_id_custom",
    "subscriber_match_status",
    "subscriber_match_method",
    "subscriber_match_reason",
    "insurer_number",
    "exam_facility_id",
    "facility_code",
    "facility_name",
    "exam_date",
    "name_full_raw",
    "name_kana_raw",
    "name_kana_match",
    "name_kana_export_value",
    "name_kana_export_source",
    "name_kana_export_reason",
    "insurance_symbol_raw",
    "insurance_symbol_match",
    "insurance_symbol_export_value",
    "insurance_symbol_export_source",
    "insurance_symbol_export_reason",
    "insurance_number_raw",
    "insurance_number_match",
    "insurance_number_export_value",
    "insurance_number_export_source",
    "insurance_number_export_reason",
    "insurance_branch_number_raw",
    "insurance_branch_number_match",
    "birthdate",
    "gender_code",
    "gender_raw",
    "report_category_code",
    "program_type_code",
    "health_exam_report_category",
    "program_code",
    "postal_code",
    "address",
    "basic_info_status",
    "basic_info_reason",
    "insurer_number_source",
    "insurer_number_completion_status",
    "insurer_number_completion_reason",
    "insurer_number_export_value",
    "address_source",
    "address_completion_status",
    "address_completion_reason",
    "address_completed_value",
    "postal_code_completed_value",
    "exam_facility_postal_code",
    "exam_facility_address",
    "exam_facility_phone_number",
    "manual_export_approved",
    "manual_export_reason",
    "manual_export_approved_at",
    "manual_export_approved_by",
]

ITEM_COPY_COLUMNS = [
    "event_id",
    "subscriber_id",
    "hia_subscriber_id",
    "namecode",
    "section_code",
    "section_code_system",
    "section_name",
    "occurrence_no",
    "raw_value",
    "raw_value_type",
    "raw_unit",
    "source_reference_lower",
    "source_reference_upper",
    "normalized_value",
    "normalized_unit",
    "nullflavor",
    "code_system",
    "code_value",
    "code_display",
    "interpretation_code",
    "interpretation_code_system",
    "interpretation_name",
    "namecode_display_name",
    "negation_ind",
    "identity_item_code",
    "jun_no",
    "normalize_status",
    "normalize_reason",
    "validation_status",
    "validation_reason",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build COMBINED exam_ledgers from XML/CSV source ledgers.")
    parser.add_argument("--event-id", type=int, default=2)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--include-exported", action="store_true")
    parser.add_argument("--limit-groups", type=int, default=0)
    parser.add_argument("--db-prefix", default="PHR_DB_")
    parser.add_argument("--health-db", default=HEALTH_DB)
    return parser.parse_args()


def qname(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", name):
        raise ValueError(f"invalid SQL identifier: {name!r}")
    return f"`{name}`"


def source_ref(row: dict[str, Any]) -> tuple[str, int]:
    if row["source_type"] == "XML":
        return "XML", int(row["source_xml_ledger_id"])
    if row["source_type"] == "CSV":
        return "CSV", int(row["source_csv_row_ledger_id"])
    raise ValueError(f"unsupported source_type: {row['source_type']}")


def group_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("event_id"),
        row.get("subscriber_id"),
        row.get("exam_date"),
        row.get("exam_facility_id"),
        row.get("insurer_number"),
    )


def build_group_hash(rows: Iterable[dict[str, Any]]) -> str:
    parts = [f"{row['source_type']}:{row['exam_ledger_id']}" for row in rows]
    return hashlib.sha256("|".join(sorted(parts)).encode("utf-8")).hexdigest()


def fetch_source_ledgers(cur: Any, config: BuildConfig) -> list[dict[str, Any]]:
    filters = [
        "event_id = %s",
        "source_type IN ('XML', 'CSV')",
        "subscriber_id IS NOT NULL",
        "subscriber_match_status = 'MATCHED'",
        "exam_date IS NOT NULL",
        "exam_facility_id IS NOT NULL",
    ]
    params: list[Any] = [config.event_id]
    if not config.include_exported:
        filters.append("xml_export_status <> 'EXPORTED'")
    cur.execute(
        f"""
        SELECT *
        FROM {qname(config.health_db)}.exam_ledgers
        WHERE {' AND '.join(filters)}
        ORDER BY subscriber_id, exam_date, exam_facility_id, source_type, exam_ledger_id
        """,
        tuple(params),
    )
    return [dict(row) for row in cur.fetchall()]


def select_candidate_groups(rows: list[dict[str, Any]], limit: int) -> list[list[dict[str, Any]]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[group_key(row)].append(row)
    candidates = [
        group
        for group in grouped.values()
        if len(group) >= 2
        and {row["source_type"] for row in group} >= {"XML", "CSV"}
    ]
    candidates.sort(key=lambda group: (group[0]["subscriber_id"], group[0]["exam_date"], group[0]["exam_facility_id"]))
    return candidates[:limit] if limit else candidates


def choose_primary(group: list[dict[str, Any]]) -> dict[str, Any]:
    xml_rows = [row for row in group if row["source_type"] == "XML"]
    if xml_rows:
        return sorted(xml_rows, key=lambda row: (row.get("check_status") != "OK", row["exam_ledger_id"]))[0]
    return sorted(group, key=lambda row: row["exam_ledger_id"])[0]


def find_existing_combined(cur: Any, config: BuildConfig, primary: dict[str, Any]) -> int | None:
    cur.execute(
        f"""
        SELECT exam_ledger_id
        FROM {qname(config.health_db)}.exam_ledgers
        WHERE event_id = %s
          AND source_type = 'COMBINED'
          AND subscriber_id = %s
          AND exam_date = %s
          AND exam_facility_id = %s
          AND COALESCE(insurer_number, '') = COALESCE(%s, '')
        ORDER BY exam_ledger_id
        LIMIT 1
        """,
        (
            primary["event_id"],
            primary["subscriber_id"],
            primary["exam_date"],
            primary["exam_facility_id"],
            primary.get("insurer_number"),
        ),
    )
    row = cur.fetchone()
    return int(row["exam_ledger_id"]) if row else None


def upsert_combined_ledger(cur: Any, config: BuildConfig, group: list[dict[str, Any]], run_id: int) -> tuple[int, str]:
    primary = choose_primary(group)
    existing_id = find_existing_combined(cur, config, primary)
    group_hash = build_group_hash(group)
    source_summary = ",".join(f"{row['source_type']}:{row['exam_ledger_id']}" for row in group)
    values = {column: primary.get(column) for column in LEDGER_COPY_COLUMNS}
    values.update(
        {
            "source_type": "COMBINED",
            "source_etl_run_id": run_id,
            "row_sha256": group_hash,
            "exam_item_status": "READY",
            "exam_item_count": None,
            "exam_item_error_count": None,
            "exam_item_reason": None,
            "check_status": "PENDING",
            "check_reason": None,
            "xml_export_status": "PENDING",
            "merge_status": "MERGED",
            "merge_reason": f"XML優先で不足項目を補完: sources={source_summary}",
        }
    )
    columns = [
        *LEDGER_COPY_COLUMNS,
        "source_type",
        "source_etl_run_id",
        "row_sha256",
        "exam_item_status",
        "exam_item_count",
        "exam_item_error_count",
        "exam_item_reason",
        "check_status",
        "check_reason",
        "xml_export_status",
        "merge_status",
        "merge_reason",
    ]
    if existing_id is None:
        cur.execute(
            f"""
            INSERT INTO {qname(config.health_db)}.exam_ledgers (
              {', '.join(f'`{column}`' for column in columns)}
            ) VALUES ({', '.join(['%s'] * len(columns))})
            """,
            tuple(values.get(column) for column in columns),
        )
        return int(cur.lastrowid), "inserted"

    cur.execute(
        f"""
        UPDATE {qname(config.health_db)}.exam_ledgers
        SET {', '.join(f'`{column}` = %s' for column in columns if column != 'source_type')},
            updated_at = CURRENT_TIMESTAMP(3)
        WHERE exam_ledger_id = %s
        """,
        tuple(values.get(column) for column in columns if column != "source_type") + (existing_id,),
    )
    return existing_id, "updated"


def upsert_sources(cur: Any, config: BuildConfig, combined_id: int, group: list[dict[str, Any]], primary_id: int) -> int:
    count = 0
    for row in group:
        source_type, source_ledger_id = source_ref(row)
        source_role = "PRIMARY" if int(row["exam_ledger_id"]) == primary_id else "SUPPLEMENT"
        priority = 10 if source_role == "PRIMARY" else 100
        cur.execute(
            f"""
            INSERT INTO {qname(config.health_db)}.exam_ledger_sources (
              exam_ledger_id, source_type, source_ledger_id, file_receipt_id,
              source_priority, source_role, source_status, source_reason
            ) VALUES (%s, %s, %s, %s, %s, %s, 'ACTIVE', %s)
            ON DUPLICATE KEY UPDATE
              exam_ledger_id = VALUES(exam_ledger_id),
              file_receipt_id = VALUES(file_receipt_id),
              source_priority = VALUES(source_priority),
              source_role = VALUES(source_role),
              source_status = VALUES(source_status),
              source_reason = VALUES(source_reason)
            """,
            (
                combined_id,
                source_type,
                source_ledger_id,
                row.get("file_receipt_id"),
                priority,
                source_role,
                "XML優先結合" if source_role == "PRIMARY" else "不足項目補完候補",
            ),
        )
        count += int(cur.rowcount > 0)
    return count


def fetch_valid_values(cur: Any, config: BuildConfig, ledger_type: str, ledger_id: int) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT *
        FROM {qname(config.health_db)}.exam_item_values
        WHERE ledger_type = %s
          AND ledger_id = %s
          AND validation_status = 'VALID'
          AND namecode IS NOT NULL
        ORDER BY COALESCE(jun_no, 999999), id
        """,
        (ledger_type, ledger_id),
    )
    return [dict(row) for row in cur.fetchall()]


def fetch_precedence_rules(cur: Any, config: BuildConfig, group: list[dict[str, Any]]) -> list[dict[str, Any]]:
    facility_ids = sorted({int(row["exam_facility_id"]) for row in group if row.get("exam_facility_id") is not None})
    if not facility_ids:
        return []
    placeholders = ", ".join(["%s"] * len(facility_ids))
    cur.execute(
        f"""
        SELECT *
        FROM {qname(config.health_db)}.exam_item_value_precedence_rules
        WHERE is_active = 1
          AND (event_id IS NULL OR event_id = %s)
          AND (exam_facility_id IS NULL OR exam_facility_id IN ({placeholders}))
        ORDER BY
          CASE WHEN event_id IS NULL THEN 1 ELSE 0 END,
          CASE WHEN exam_facility_id IS NULL THEN 1 ELSE 0 END,
          priority,
          precedence_rule_id
        """,
        (config.event_id, *facility_ids),
    )
    return [dict(row) for row in cur.fetchall()]


def value_text(value: dict[str, Any]) -> str:
    for column in ("raw_value", "normalized_value", "code_display", "code_value"):
        cell = value.get(column)
        if cell is not None and str(cell).strip() != "":
            return str(cell).strip()
    return ""


def condition_matches(value: dict[str, Any] | None, condition_type: str | None, pattern: str | None) -> bool:
    condition = (condition_type or "ALWAYS").upper()
    text = value_text(value) if value is not None else ""
    target = pattern or ""
    if condition == "ALWAYS":
        return True
    if condition == "NOT_EMPTY":
        return text != ""
    if condition == "EQUALS":
        return text == target
    if condition == "CONTAINS":
        return target in text
    if condition == "REGEXP":
        return bool(re.search(target, text))
    return False


def matching_rule(
    rules: list[dict[str, Any]],
    *,
    config: BuildConfig,
    group: list[dict[str, Any]],
    key: tuple[Any, Any],
    xml_value: dict[str, Any] | None,
    csv_value: dict[str, Any] | None,
) -> dict[str, Any] | None:
    facility_ids = {int(row["exam_facility_id"]) for row in group if row.get("exam_facility_id") is not None}
    namecode, occurrence_no = key
    for rule in rules:
        if rule.get("event_id") is not None and int(rule["event_id"]) != config.event_id:
            continue
        if rule.get("exam_facility_id") is not None and int(rule["exam_facility_id"]) not in facility_ids:
            continue
        if rule.get("namecode") != namecode:
            continue
        if rule.get("occurrence_no") is not None and int(rule["occurrence_no"]) != int(occurrence_no or 1):
            continue
        if not condition_matches(xml_value, rule.get("xml_value_condition_type"), rule.get("xml_value_condition_pattern")):
            continue
        if not condition_matches(csv_value, rule.get("csv_value_condition_type"), rule.get("csv_value_condition_pattern")):
            continue
        return rule
    return None


def source_priority(source_type: str) -> int:
    if source_type == "XML":
        return 0
    if source_type == "CSV":
        return 1
    return 9


def combine_text_values(xml_value: dict[str, Any] | None, csv_value: dict[str, Any] | None, separator: str) -> str:
    parts: list[str] = []
    for value in (xml_value, csv_value):
        text = value_text(value)
        if text and text not in parts:
            parts.append(text)
    return separator.join(parts)


def make_joined_value(
    xml_value: dict[str, Any] | None,
    csv_value: dict[str, Any] | None,
    *,
    separator: str,
) -> tuple[dict[str, Any], str, int | None, str]:
    base = dict(xml_value or csv_value or {})
    source_ledger_id = base.get("ledger_id")
    base["raw_value"] = combine_text_values(xml_value, csv_value, separator)
    base["normalized_value"] = base["raw_value"]
    base["raw_value_type"] = "ST"
    base["normalize_status"] = "OK"
    base["normalize_reason"] = "MERGED_XML_CSV"
    base["validation_status"] = "VALID"
    base["validation_reason"] = None
    return base, "MERGED", int(source_ledger_id) if source_ledger_id is not None else None, "MERGED"


def choose_value_for_key(
    candidates: list[tuple[dict[str, Any], str, int, str]],
    *,
    config: BuildConfig,
    group: list[dict[str, Any]],
    key: tuple[Any, Any],
    rules: list[dict[str, Any]],
) -> tuple[dict[str, Any], str, int | None, str, bool]:
    xml_candidates = [candidate for candidate in candidates if candidate[1] == "XML"]
    csv_candidates = [candidate for candidate in candidates if candidate[1] == "CSV"]
    xml_candidate = xml_candidates[0] if xml_candidates else None
    csv_candidate = csv_candidates[0] if csv_candidates else None
    xml_value = xml_candidate[0] if xml_candidate else None
    csv_value = csv_candidate[0] if csv_candidate else None
    rule = matching_rule(rules, config=config, group=group, key=key, xml_value=xml_value, csv_value=csv_value)
    if rule:
        action = str(rule["action"]).upper()
        if action in {"CSV_FIRST", "CSV_IF_XML_MATCHES_PATTERN"} and csv_candidate:
            return (*csv_candidate, True)
        if action == "JOIN_XML_CSV" and (xml_candidate or csv_candidate):
            return (*make_joined_value(xml_value, csv_value, separator=rule.get("join_separator") or "\n"), True)
        if action == "MANUAL_REVIEW" and xml_candidate:
            return (*xml_candidate, True)
        if action == "XML_FIRST" and xml_candidate:
            return (*xml_candidate, True)

    return (*sorted(candidates, key=lambda candidate: (candidate[3] != "PRIMARY", source_priority(candidate[1]), candidate[0]["id"]))[0], False)


def rebuild_combined_values(cur: Any, config: BuildConfig, combined_id: int, group: list[dict[str, Any]], primary_id: int, run_id: int) -> tuple[int, int, int]:
    cur.execute(
        f"""
        DELETE FROM {qname(config.health_db)}.exam_item_values
        WHERE ledger_type = %s
          AND ledger_id = %s
        """,
        (LEDGER_TYPE_EXAM, combined_id),
    )
    deleted = int(cur.rowcount)

    rules = fetch_precedence_rules(cur, config, group)
    ordered = sorted(group, key=lambda row: (0 if int(row["exam_ledger_id"]) == primary_id else 1, row["source_type"] != "XML", row["exam_ledger_id"]))
    candidates_by_key: dict[tuple[Any, Any], list[tuple[dict[str, Any], str, int, str]]] = defaultdict(list)
    for row in ordered:
        source_type, source_ledger_id = source_ref(row)
        role = "PRIMARY" if int(row["exam_ledger_id"]) == primary_id else "SUPPLEMENT"
        for value in fetch_valid_values(cur, config, source_type, source_ledger_id):
            key = (value.get("namecode"), value.get("occurrence_no") or 1)
            candidates_by_key[key].append((value, source_type, source_ledger_id, role))

    if not candidates_by_key:
        return deleted, 0, 0

    rules_applied = 0
    chosen: dict[tuple[Any, Any], tuple[dict[str, Any], str, int | None, str]] = {}
    for key, candidates in candidates_by_key.items():
        value, source_type, source_ledger_id, role, applied = choose_value_for_key(
            candidates,
            config=config,
            group=group,
            key=key,
            rules=rules,
        )
        chosen[key] = (value, source_type, source_ledger_id, role)
        rules_applied += int(applied)

    insert_columns = [
        "ledger_type",
        "ledger_id",
        *ITEM_COPY_COLUMNS,
        "source_ledger_type",
        "source_ledger_id",
        "source_exam_item_value_id",
        "value_source_role",
        "extracted_run_id",
    ]
    rows = []
    for value, source_type, source_ledger_id, role in chosen.values():
        rows.append(
            (
                LEDGER_TYPE_EXAM,
                combined_id,
                *(value.get(column) for column in ITEM_COPY_COLUMNS),
                source_type,
                source_ledger_id,
                value.get("id"),
                role,
                run_id,
            )
        )
    cur.executemany(
        f"""
        INSERT INTO {qname(config.health_db)}.exam_item_values (
          {', '.join(f'`{column}`' for column in insert_columns)}
        ) VALUES ({', '.join(['%s'] * len(insert_columns))})
        """,
        rows,
    )
    return deleted, int(cur.rowcount), rules_applied


def build_combined_ledgers(conn: Any, config: BuildConfig) -> BuildSummary:
    cur = dict_cursor(conn)
    try:
        source_rows = fetch_source_ledgers(cur, config)
        groups = select_candidate_groups(source_rows, config.limit_groups)
        summary = BuildSummary(
            event_id=config.event_id,
            dry_run=config.dry_run,
            source_ledgers=len(source_rows),
            candidate_groups=len(groups),
        )
        if config.dry_run:
            return summary

        run_id = etl_start_run(
            cur,
            phase=ETL_PHASE,
            source=ETL_SOURCE,
            db_schema=config.health_db,
            db_path=config.health_db,
            input_base=f"event_id={config.event_id}",
            input_file=None,
            insurer_number=None,
            dry_run=False,
            limit_rows=config.limit_groups or None,
        )
        conn.commit()
        try:
            for group in groups:
                primary = choose_primary(group)
                combined_id, action = upsert_combined_ledger(cur, config, group, run_id)
                if action == "inserted":
                    summary.combined_inserted += 1
                else:
                    summary.combined_updated += 1
                summary.sources_upserted += upsert_sources(cur, config, combined_id, group, int(primary["exam_ledger_id"]))
                deleted, inserted, rules_applied = rebuild_combined_values(cur, config, combined_id, group, int(primary["exam_ledger_id"]), run_id)
                summary.values_deleted += deleted
                summary.values_inserted += inserted
                summary.precedence_rules_applied += rules_applied
            etl_finish_run(
                cur,
                run_id,
                RunMetrics(
                    rows_seen=summary.source_ledgers,
                    rows_inserted=summary.combined_inserted + summary.values_inserted,
                    rows_updated=summary.combined_updated + summary.sources_upserted,
                    rows_skipped=summary.groups_skipped,
                ),
                status_override="success",
                extra_notes=summary.to_message(),
            )
            conn.commit()
            return summary
        except Exception as exc:
            conn.rollback()
            etl_finish_run(
                cur,
                run_id,
                RunMetrics(rows_seen=summary.source_ledgers, errors=1),
                status_override="failed",
                extra_notes=f"{summary.to_message()} error={type(exc).__name__}: {exc}",
            )
            conn.commit()
            raise
    finally:
        cur.close()


def main() -> int:
    args = parse_args()
    config = BuildConfig(
        event_id=args.event_id,
        health_db=args.health_db,
        dry_run=bool(args.dry_run),
        include_exported=bool(args.include_exported),
        limit_groups=int(args.limit_groups or 0),
    )
    params = load_mysql_base_params(args.db_prefix)
    with connect_ctx(params, database=config.health_db, autocommit=False) as conn:
        summary = build_combined_ledgers(conn, config)
    print(summary.to_message())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
