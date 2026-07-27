"""Lookup helpers for examination item master rows.

This module provides small, reusable lookup functions for code that needs
metadata for health examination items by `namecode`.

The caller owns the DB connection and transaction. Pass an existing cursor so
this module does not create connections by itself.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from scripts.lib.db.schemas import DEV_PHR


EXAM_ITEM_MASTER_COLUMNS = """
    namecode,
    item_name,
    xml_value_type AS data_type,
    COALESCE(ucum_unit, display_unit) AS unit,
    nullflavor_allowed AS nullable,
    display_unit,
    ucum_unit,
    result_code_oid,
    data_type_label,
    identity_item_code,
    jun_no
"""


class ExamItemMasterLookupError(RuntimeError):
    """Raised when exam item master lookup input is invalid."""


def _normalize_namecode(namecode: str | None) -> str | None:
    """Normalize a namecode for lookup.

    The lookup layer intentionally keeps this light. XML/parser-specific
    normalization should happen before calling this helper.
    """

    if namecode is None:
        return None
    normalized = str(namecode).strip()
    return normalized or None


def _dedupe_namecodes(namecodes: Iterable[str | None]) -> list[str]:
    """Return non-empty unique namecodes while preserving input order."""

    seen: set[str] = set()
    deduped: list[str] = []
    for raw_namecode in namecodes:
        namecode = _normalize_namecode(raw_namecode)
        if namecode is None or namecode in seen:
            continue
        seen.add(namecode)
        deduped.append(namecode)
    return deduped


def get_exam_item(
    cur: Any,
    namecode: str | None,
    *,
    dev_db: str = DEV_PHR,
) -> dict[str, Any] | None:
    """Return one exam item master row by namecode.

    Returns `None` when the namecode is empty or not found.
    """

    normalized_namecode = _normalize_namecode(namecode)
    if normalized_namecode is None:
        return None

    cur.execute(
        f"""
        SELECT
            {EXAM_ITEM_MASTER_COLUMNS}
        FROM `{dev_db}`.`exam_item_master`
        WHERE `namecode` = %s
        LIMIT 1
        """,
        (normalized_namecode,),
    )
    row = cur.fetchone()
    return dict(row) if row is not None else None


def get_exam_items(
    cur: Any,
    namecodes: Iterable[str | None],
    *,
    dev_db: str = DEV_PHR,
) -> dict[str, dict[str, Any]]:
    """Return exam item master rows keyed by namecode.

    Missing namecodes are simply absent from the returned dict.
    """

    normalized_namecodes = _dedupe_namecodes(namecodes)
    if not normalized_namecodes:
        return {}

    placeholders = ", ".join(["%s"] * len(normalized_namecodes))
    cur.execute(
        f"""
        SELECT
            {EXAM_ITEM_MASTER_COLUMNS}
        FROM `{dev_db}`.`exam_item_master`
        WHERE `namecode` IN ({placeholders})
        """,
        tuple(normalized_namecodes),
    )

    rows = cur.fetchall()
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        item = dict(row)
        row_namecode = _normalize_namecode(item.get("namecode"))
        if row_namecode is None:
            continue
        result[row_namecode] = item
    return result


def find_missing_namecodes(
    requested_namecodes: Iterable[str | None],
    master_by_namecode: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    """Return requested namecodes that were not found in master lookup results."""

    normalized_requested = _dedupe_namecodes(requested_namecodes)
    return [
        namecode
        for namecode in normalized_requested
        if namecode not in master_by_namecode
    ]
