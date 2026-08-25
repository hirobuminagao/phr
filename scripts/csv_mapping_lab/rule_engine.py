#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Apply reusable CSV mapping rules to analyzed CSV columns."""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from scripts.csv_mapping_lab.analyze_csv import normalize_header
from scripts.lib.db.mysql import dict_cursor


def qname(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


@dataclass(frozen=True)
class RuleHit:
    rule_id: int
    score: Decimal
    target_kind: str
    target_namecode: str | None
    target_ledger_field: str | None
    mapping_strategy: str
    reason: str


def decode_sensitive_category(column: dict[str, Any]) -> str | None:
    value_profile = column.get("value_profile_json")
    if isinstance(value_profile, str):
        try:
            value_profile = json.loads(value_profile)
        except json.JSONDecodeError:
            value_profile = None
    if isinstance(value_profile, dict):
        value = value_profile.get("sensitive_category")
        return str(value) if value else None
    return None


def load_rules(cur: Any, *, lab_db: str, facility_code: str | None = None, event_id: int | None = None) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT *
        FROM {qname(lab_db)}.`csv_mapping_rules`
        WHERE `active` = 1
          AND (
            `scope` = 'global'
            OR (`scope` = 'facility' AND `facility_code` = %s)
            OR (`scope` = 'event' AND `event_id` = %s)
          )
        ORDER BY
          CASE `scope` WHEN 'event' THEN 3 WHEN 'facility' THEN 2 ELSE 1 END DESC,
          `confidence` DESC,
          `rule_id` DESC
        """,
        (facility_code, event_id),
    )
    return [dict(row) for row in cur.fetchall()]


def load_analysis_columns(cur: Any, *, lab_db: str, analysis_file_id: int) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          `analysis_column_id`, `analysis_file_id`, `column_no`, `header_name`, `normalized_header_name`,
          `inferred_value_type`, `inferred_format`, `sensitive_hint`, `value_profile_json`, `decision_status`
        FROM {qname(lab_db)}.`analysis_columns`
        WHERE `analysis_file_id` = %s
        ORDER BY `column_no`
        """,
        (analysis_file_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def rule_matches(rule: dict[str, Any], column: dict[str, Any]) -> tuple[bool, str]:
    if rule.get("value_type") and rule.get("value_type") != column.get("inferred_value_type"):
        return False, ""
    if rule.get("sensitive_category"):
        category = decode_sensitive_category(column)
        if category != rule.get("sensitive_category"):
            return False, ""

    header_name = str(column.get("header_name") or "")
    normalized_header = str(column.get("normalized_header_name") or normalize_header(header_name) or "")
    condition_type = str(rule.get("condition_type") or "normalized_header_exact")
    header_pattern = str(rule.get("header_pattern") or "")
    normalized_pattern = str(rule.get("normalized_header_pattern") or normalize_header(header_pattern) or "")

    if condition_type == "header_exact" and header_name == header_pattern:
        return True, f"ヘッダー完全一致: {header_pattern}"
    if condition_type == "normalized_header_exact" and normalized_header and normalized_header == normalized_pattern:
        return True, f"正規化ヘッダー一致: {normalized_pattern}"
    if condition_type == "header_contains" and normalized_pattern and normalized_pattern in normalized_header:
        return True, f"ヘッダー部分一致: {normalized_pattern}"
    if condition_type == "sensitive_category" and rule.get("sensitive_category"):
        return True, f"個人系カテゴリ一致: {rule.get('sensitive_category')}"
    return False, ""


def score_for_rule(rule: dict[str, Any]) -> Decimal:
    confidence = Decimal(str(rule.get("confidence") or "0.9000"))
    scope_bonus = {"event": Decimal("0.0300"), "facility": Decimal("0.0200"), "global": Decimal("0.0000")}
    condition_bonus = {
        "header_exact": Decimal("0.0200"),
        "normalized_header_exact": Decimal("0.0150"),
        "header_contains": Decimal("-0.0500"),
        "sensitive_category": Decimal("-0.0800"),
    }
    score = confidence + scope_bonus.get(str(rule.get("scope")), Decimal("0.0000"))
    score += condition_bonus.get(str(rule.get("condition_type")), Decimal("0.0000"))
    return max(Decimal("0.0000"), min(Decimal("1.0000"), score)).quantize(Decimal("0.0001"))


def target_key(hit: RuleHit) -> tuple[str, str | None, str | None]:
    return hit.target_kind, hit.target_namecode, hit.target_ledger_field


def pick_candidate(hits: list[RuleHit]) -> RuleHit | None:
    if not hits:
        return None
    hits = sorted(hits, key=lambda hit: (hit.score, hit.rule_id), reverse=True)
    best = hits[0]
    conflicting = [hit for hit in hits[1:] if target_key(hit) != target_key(best)]
    if conflicting and conflicting[0].score >= best.score - Decimal("0.0500"):
        return RuleHit(
            rule_id=best.rule_id,
            score=best.score,
            target_kind="REVIEW",
            target_namecode=None,
            target_ledger_field=None,
            mapping_strategy="NEEDS_CONFIRMATION",
            reason=f"複数ルールが近いスコアでヒット: rule {best.rule_id}, rule {conflicting[0].rule_id}",
        )
    return best


def apply_rules_to_analysis(
    conn: Any,
    *,
    lab_db: str,
    analysis_file_id: int,
    facility_code: str | None = None,
    event_id: int | None = None,
    overwrite_human_decision: bool = False,
) -> dict[str, int]:
    cur = dict_cursor(conn)
    rules = load_rules(cur, lab_db=lab_db, facility_code=facility_code, event_id=event_id)
    columns = load_analysis_columns(cur, lab_db=lab_db, analysis_file_id=analysis_file_id)
    applied = 0
    hit_count = 0

    for column in columns:
        if not overwrite_human_decision and column.get("decision_status") != "UNREVIEWED":
            continue
        hits: list[RuleHit] = []
        for rule in rules:
            matched, reason = rule_matches(rule, column)
            if not matched:
                continue
            hit = RuleHit(
                rule_id=int(rule["rule_id"]),
                score=score_for_rule(rule),
                target_kind=str(rule["target_kind"]),
                target_namecode=rule.get("target_namecode"),
                target_ledger_field=rule.get("target_ledger_field"),
                mapping_strategy=str(rule.get("mapping_strategy") or "DIRECT"),
                reason=reason,
            )
            hits.append(hit)
            cur.execute(
                f"""
                INSERT INTO {qname(lab_db)}.`csv_mapping_rule_hits`
                  (`analysis_column_id`, `rule_id`, `score`, `reason`)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                  `score` = VALUES(`score`),
                  `reason` = VALUES(`reason`)
                """,
                (column["analysis_column_id"], hit.rule_id, hit.score, hit.reason),
            )
        hit_count += len(hits)
        candidate = pick_candidate(hits)
        if not candidate:
            continue
        cur.execute(
            f"""
            UPDATE {qname(lab_db)}.`analysis_columns`
            SET
              `candidate_target_kind` = %s,
              `candidate_namecode` = %s,
              `candidate_ledger_field` = %s,
              `candidate_confidence` = %s,
              `analysis_note` = %s
            WHERE `analysis_column_id` = %s
            """,
            (
                candidate.target_kind,
                candidate.target_namecode,
                candidate.target_ledger_field,
                candidate.score,
                f"rule {candidate.rule_id}: {candidate.reason}",
                column["analysis_column_id"],
            ),
        )
        applied += 1

    cur.close()
    return {"rules": len(rules), "hits": hit_count, "applied_columns": applied}
