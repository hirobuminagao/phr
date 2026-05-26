# -*- coding: utf-8 -*-
"""
============================================================
Module : apply_action_subscriber_audit.py
Path   : scripts/hia/script_lib/apply_action_subscriber_audit.py
Project: PHR

Purpose:
    Write subscriber audit rows for HIA apply.

Responsibility:
    - provide field-level subscriber_audit insert helper
    - keep audit granularity as 1 changed field = 1 audit row
    - skip audit when old_value == new_value

Non-goals:
    - subscribers root apply
    - subscriber_addresses apply
    - subscriber_contact_points apply
    - staging processed mark
    - prepare / compare decision
    - event payload audit

Notes:
    subscriber_audit must stay field-searchable.

    Required granularity:

        1 changed field = 1 audit row

    Do not collapse changes into JSON payload events.
============================================================
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


# ============================================================
# data model
# ============================================================


@dataclass(frozen=True)
class SubscriberAuditRow:
    """subscriber_audit へINSERTする1 field分の監査行。"""

    subscriber_id: int
    field: str
    old_value: Any
    new_value: Any
    source: str
    note: str
    change_run_id: int


# ============================================================
# helpers
# ============================================================


def _as_db_text(value: Any) -> str | None:
    """subscriber_audit.old_value/new_value 用のDB文字列へ変換する。"""

    if value is None:
        return None
    return str(value)


def _same_for_audit(old_value: Any, new_value: Any) -> bool:
    """audit上で同値とみなすか判定する。"""

    return _as_db_text(old_value) == _as_db_text(new_value)


# ============================================================
# row builders
# ============================================================


def build_subscriber_audit_row(
    *,
    subscriber_id: int,
    field: str,
    old_value: Any,
    new_value: Any,
    source: str,
    note: str,
    change_run_id: int,
) -> SubscriberAuditRow | None:
    """
    1 field分の audit row を作る。

    old_value と new_value が同じ場合は None を返す。
    """

    if _same_for_audit(old_value, new_value):
        return None

    return SubscriberAuditRow(
        subscriber_id=subscriber_id,
        field=field,
        old_value=old_value,
        new_value=new_value,
        source=source,
        note=note,
        change_run_id=change_run_id,
    )


def build_subscriber_audit_rows_from_fields(
    *,
    subscriber_id: int,
    fields: Iterable[str],
    old_values: dict[str, Any],
    new_values: dict[str, Any],
    source: str,
    note: str,
    change_run_id: int,
) -> list[SubscriberAuditRow]:
    """field一覧と old/new dict から audit rows を作る。"""

    rows: list[SubscriberAuditRow] = []

    for field in fields:
        audit_row = build_subscriber_audit_row(
            subscriber_id=subscriber_id,
            field=field,
            old_value=old_values.get(field),
            new_value=new_values.get(field),
            source=source,
            note=note,
            change_run_id=change_run_id,
        )
        if audit_row is not None:
            rows.append(audit_row)

    return rows


# ============================================================
# insert
# ============================================================


def insert_subscriber_audit_row(
    cur,
    *,
    audit_row: SubscriberAuditRow,
) -> None:
    """subscriber_audit に1 field分の audit row を INSERT する。"""

    cur.execute(
        """
        INSERT INTO subscriber_audit (
            subscriber_id,
            field,
            old_value,
            new_value,
            changed_at,
            source,
            note,
            change_run_id
        )
        VALUES (
            %(subscriber_id)s,
            %(field)s,
            %(old_value)s,
            %(new_value)s,
            NOW(),
            %(source)s,
            %(note)s,
            %(change_run_id)s
        )
        """,
        {
            "subscriber_id": audit_row.subscriber_id,
            "field": audit_row.field,
            "old_value": _as_db_text(audit_row.old_value),
            "new_value": _as_db_text(audit_row.new_value),
            "source": audit_row.source,
            "note": audit_row.note,
            "change_run_id": audit_row.change_run_id,
        },
    )


def insert_subscriber_audit_rows(
    cur,
    *,
    audit_rows: Iterable[SubscriberAuditRow],
) -> int:
    """subscriber_audit に複数field分の audit row を INSERT する。"""

    inserted = 0

    for audit_row in audit_rows:
        insert_subscriber_audit_row(cur, audit_row=audit_row)
        inserted += 1

    return inserted