"""Lookup helpers for Phase7 examination checks."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from .models import ExamValue, MethodRule


COMMON_GROUP = "v2_2026_CHECK_72_ITEMS"
LEGAL_GROUP = "v2_2026_LSIO_Legal_Item"
SPECIFIC_GROUP = "v2_2026_Specific_Health_Item"


def qname(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def fetch_target_ledgers(cur: Any, *, health_db: str, event_id: int, limit: int = 0) -> list[dict[str, Any]]:
    params: list[Any] = [event_id]
    limit_sql = ""
    if limit:
        limit_sql = "LIMIT %s"
        params.append(limit)
    cur.execute(
        f"""
        SELECT id, event_id, subscriber_id, hia_subscriber_id
        FROM {qname(health_db)}.xml_ledger
        WHERE event_id = %s
          AND xml_status = 'READY'
        ORDER BY id
        {limit_sql}
        """,
        tuple(params),
    )
    return list(cur.fetchall())


def fetch_exam_values(cur: Any, *, health_db: str, dev_db: str, ledger_ids: list[int]) -> dict[int, list[ExamValue]]:
    if not ledger_ids:
        return {}
    placeholders = ", ".join(["%s"] * len(ledger_ids))
    cur.execute(
        f"""
        SELECT
          eiv.id,
          eiv.ledger_id,
          eiv.namecode,
          eiv.raw_value,
          eiv.normalized_value,
          eiv.nullflavor,
          eiv.negation_ind,
          COALESCE(eiv.identity_item_code, em.identity_item_code) AS identity_item_code,
          em.xml_method_code,
          eiv.validation_status
        FROM {qname(health_db)}.exam_item_values eiv
        LEFT JOIN {qname(dev_db)}.exam_item_master em
          ON em.namecode = eiv.namecode
        WHERE eiv.ledger_type = 'XML'
          AND eiv.ledger_id IN ({placeholders})
        ORDER BY eiv.ledger_id, eiv.id
        """,
        tuple(ledger_ids),
    )
    values_by_ledger: dict[int, list[ExamValue]] = defaultdict(list)
    for row in cur.fetchall():
        values_by_ledger[int(row["ledger_id"])].append(ExamValue.from_row(dict(row)))
    return values_by_ledger


def fetch_identity_members(cur: Any, *, dev_db: str, group_codes: tuple[str, ...]) -> dict[str, dict[str, dict[str, Any]]]:
    placeholders = ", ".join(["%s"] * len(group_codes))
    cur.execute(
        f"""
        SELECT group_code, identity_item_code, required_flag, presence_value_mode, sort_no
        FROM {qname(dev_db)}.exam_item_group_identity_members
        WHERE group_code IN ({placeholders})
        ORDER BY group_code, sort_no, identity_item_code
        """,
        group_codes,
    )
    result: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in cur.fetchall():
        result[str(row["group_code"])][str(row["identity_item_code"])] = dict(row)
    return result


def fetch_method_rules(cur: Any, *, dev_db: str, group_codes: tuple[str, ...]) -> dict[str, dict[str, list[MethodRule]]]:
    placeholders = ", ".join(["%s"] * len(group_codes))
    cur.execute(
        f"""
        SELECT
          DISTINCT
          mm.group_code,
          mm.xml_method_code,
          em.identity_item_code,
          mm.priority,
          mm.presence_value_mode,
          mm.required_flag,
          mm.rule_code,
          mm.rule_source_identity_codes,
          mm.rule_source_method_codes,
          mm.rule_source_namecodes
        FROM {qname(dev_db)}.exam_item_group_method_members mm
        INNER JOIN {qname(dev_db)}.exam_item_master em
          ON em.xml_method_code = mm.xml_method_code
        INNER JOIN {qname(dev_db)}.exam_item_group_identity_members im
          ON im.group_code = mm.group_code
         AND im.identity_item_code = em.identity_item_code
        WHERE mm.group_code IN ({placeholders})
          AND mm.is_active = 1
          AND em.identity_item_code IS NOT NULL
        ORDER BY mm.group_code, mm.priority, mm.xml_method_code, em.identity_item_code
        """,
        group_codes,
    )
    result: dict[str, dict[str, list[MethodRule]]] = defaultdict(lambda: defaultdict(list))
    for row in cur.fetchall():
        rule = MethodRule.from_row(dict(row))
        result[rule.group_code][rule.identity_code].append(rule)
    return result


def fetch_group_namecodes(cur: Any, *, dev_db: str, group_codes: tuple[str, ...]) -> dict[str, dict[str, set[str]]]:
    placeholders = ", ".join(["%s"] * len(group_codes))
    cur.execute(
        f"""
        SELECT gm.group_code, gm.namecode, em.identity_item_code
        FROM {qname(dev_db)}.exam_item_group_members gm
        INNER JOIN {qname(dev_db)}.exam_item_master em
          ON em.namecode = gm.namecode
        INNER JOIN {qname(dev_db)}.exam_item_group_identity_members im
          ON im.group_code = gm.group_code
         AND im.identity_item_code = em.identity_item_code
        WHERE gm.group_code IN ({placeholders})
          AND em.identity_item_code IS NOT NULL
        ORDER BY gm.group_code, gm.priority, gm.namecode
        """,
        group_codes,
    )
    result: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for row in cur.fetchall():
        namecode = str(row["namecode"])
        result[str(row["group_code"])][str(row["identity_item_code"])].add(namecode)
    return result
