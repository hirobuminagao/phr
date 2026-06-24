# -*- coding: utf-8 -*-
"""
============================================================
Module : apply_action_subscriber_contact_point.py
Path   : scripts/hia/script_lib/apply_action_subscriber_contact_point.py
Project: PHR

Purpose:
    Apply one staging subscriber row to subscriber_contact_points.

Responsibility:
    - apply phone / email contact points for one subscriber
    - handle noop / switch_current / insert / clear_current
    - keep subscriber_contact_points as history table
    - write field-level subscriber_audit rows for contact point changes

Non-goals:
    - subscribers root apply
    - subscriber_addresses apply
    - staging processed mark
    - prepare / compare decision
    - JSON/event payload audit

Notes:
    Contact point apply is based on compare result already written to staging.

    HIA null/blank value means:
        current value does not exist in the HIA source of truth

    Therefore blank phone/email clears current rows for that contact_type.

    Audit policy:
        - 1 changed field = 1 subscriber_audit row
        - field names are contact_point.phone / contact_point.email
============================================================
"""

from __future__ import annotations

from typing import Any

from scripts.lib.db.lookup.subscriber_contact_points import get_contact_point_by_id
from scripts.hia.script_lib.apply_action_subscriber_audit import (
    build_subscriber_audit_row,
    insert_subscriber_audit_row,
)


# ============================================================
# helpers
# ============================================================


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)



def _to_none_if_blank(value: Any) -> Any:
    if value in (None, ""):
        return None
    return value


# Helper to normalize IDs to int or None
def _to_int_or_none(value: Any) -> int | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    return int(text)


def _field_name_for_contact_type(contact_type: str) -> str:
    return f"contact_point.{contact_type}"


# ============================================================
# current row lookup
# ============================================================


def load_current_contact_point(
    cur,
    *,
    subscriber_id: int,
    contact_type: str,
) -> dict[str, Any]:
    """audit比較用に current contact point row を取得する。"""

    cur.execute(
        """
        SELECT
            contact_point_id,
            subscriber_id,
            contact_type,
            contact_value,
            is_current,
            valid_from,
            valid_to,
            source
        FROM subscriber_contact_points
        WHERE subscriber_id = %(subscriber_id)s
          AND contact_type = %(contact_type)s
          AND is_current = 1
        ORDER BY contact_point_id DESC
        LIMIT 1
        """,
        {
            "subscriber_id": subscriber_id,
            "contact_type": contact_type,
        },
    )

    row = cur.fetchone()
    if not row:
        return {}

    return dict(row)


# ============================================================
# audit
# ============================================================


def build_contact_point_audit_row(
    *,
    subscriber_id: int,
    contact_type: str,
    old_value: Any,
    new_value: Any,
    apply_run_id: int,
    note: str,
) -> Any:
    """contact point field単位の subscriber_audit row を生成する。"""

    return build_subscriber_audit_row(
        subscriber_id=subscriber_id,
        field=_field_name_for_contact_type(contact_type),
        old_value=old_value,
        new_value=new_value,
        source="hia_apply",
        note=note,
        change_run_id=apply_run_id,
    )


# ============================================================
# current control
# ============================================================


def clear_current_contact_points(
    cur,
    *,
    subscriber_id: int,
    contact_type: str,
    apply_run_id: int,
) -> None:
    """対象 subscriber/contact_type の current contact point を全て history 化する。"""

    cur.execute(
        """
        UPDATE subscriber_contact_points
        SET
            is_current = 0,
            valid_to = NOW(),
            updated_at = NOW()
        WHERE subscriber_id = %(subscriber_id)s
          AND contact_type = %(contact_type)s
          AND is_current = 1
        """,
        {
            "subscriber_id": subscriber_id,
            "contact_type": contact_type,
        },
    )


def switch_current_contact_point(
    cur,
    *,
    subscriber_id: int,
    contact_type: str,
    target_contact_point_id: int,
    apply_run_id: int,
) -> None:
    """
    既存 history contact point を current に戻す。

    Preconditions:
        - same contact_type + contact_value exists
        - compare phase determined switch_current
    """

    current_row = load_current_contact_point(
        cur,
        subscriber_id=subscriber_id,
        contact_type=contact_type,
    )

    target_row = get_contact_point_by_id(cur.connection, target_contact_point_id)
    if not target_row:
        raise RuntimeError(
            "subscriber_contact_points switch_current target not found: "
            f"contact_point_id={target_contact_point_id}"
        )

    if int(target_row.get("subscriber_id") or 0) != int(subscriber_id):
        raise RuntimeError(
            "subscriber_contact_points switch_current target subscriber mismatch: "
            f"contact_point_id={target_contact_point_id}, subscriber_id={subscriber_id}"
        )

    if _as_text(target_row.get("contact_type")) != contact_type:
        raise RuntimeError(
            "subscriber_contact_points switch_current target contact_type mismatch: "
            f"contact_point_id={target_contact_point_id}, contact_type={contact_type}"
        )

    audit_row = build_contact_point_audit_row(
        subscriber_id=subscriber_id,
        contact_type=contact_type,
        old_value=current_row.get("contact_value"),
        new_value=target_row.get("contact_value"),
        apply_run_id=apply_run_id,
        note="subscriber contact_point switch_current",
    )

    clear_current_contact_points(
        cur,
        subscriber_id=subscriber_id,
        contact_type=contact_type,
        apply_run_id=apply_run_id,
    )

    cur.execute(
        """
        UPDATE subscriber_contact_points
        SET
            is_current = 1,
            valid_from = NOW(),
            valid_to = NULL,
            updated_at = NOW()
        WHERE contact_point_id = %(target_contact_point_id)s
        ORDER BY contact_point_id DESC
        LIMIT 1
        """,
        {
            "target_contact_point_id": target_contact_point_id,
        },
    )

    if cur.rowcount == 0:
        raise RuntimeError(
            "subscriber_contact_points switch_current affected 0 rows: "
            f"contact_point_id={target_contact_point_id}"
        )

    if audit_row is not None:
        insert_subscriber_audit_row(cur, audit_row=audit_row)


# ============================================================
# insert
# ============================================================


def insert_contact_point(
    cur,
    *,
    subscriber_id: int,
    contact_type: str,
    contact_value: str,
    apply_run_id: int,
    source: str = "hia_apply",
) -> None:
    """subscriber_contact_points に新しい current contact point row を INSERT する。"""

    current_row = load_current_contact_point(
        cur,
        subscriber_id=subscriber_id,
        contact_type=contact_type,
    )

    audit_row = build_contact_point_audit_row(
        subscriber_id=subscriber_id,
        contact_type=contact_type,
        old_value=current_row.get("contact_value"),
        new_value=contact_value,
        apply_run_id=apply_run_id,
        note="subscriber contact_point insert",
    )

    clear_current_contact_points(
        cur,
        subscriber_id=subscriber_id,
        contact_type=contact_type,
        apply_run_id=apply_run_id,
    )

    cur.execute(
        """
        INSERT INTO subscriber_contact_points (
            subscriber_id,
            contact_type,
            contact_value,
            is_current,
            valid_from,
            valid_to,
            source,
            created_at,
            updated_at
        )
        VALUES (
            %(subscriber_id)s,
            %(contact_type)s,
            %(contact_value)s,
            1,
            NOW(),
            NULL,
            %(source)s,
            NOW(),
            NOW()
        )
        """,
        {
            "subscriber_id": subscriber_id,
            "contact_type": contact_type,
            "contact_value": contact_value,
            "source": source,
        },
    )

    if audit_row is not None:
        insert_subscriber_audit_row(cur, audit_row=audit_row)


# ============================================================
# single contact apply
# ============================================================


def apply_single_contact_point(
    cur,
    *,
    subscriber_id: int,
    contact_type: str,
    contact_value: str | None,
    diff_status: str,
    target_contact_point_id: int | None,
    apply_run_id: int,
) -> None:
    """
    1 contact_type の contact point apply を実行する。

    Preconditions:
        - compare phase has already determined diff_status
        - compare phase has already determined target_contact_point_id
          for switch_current

    Apply behavior:
        - noop
        - clear_current
        - insert
        - switch_current
    """

    value = _as_text(contact_value)

    if diff_status in {"", "noop"}:
        return

    if diff_status == "clear_current":
        current_row = load_current_contact_point(
            cur,
            subscriber_id=subscriber_id,
            contact_type=contact_type,
        )

        audit_row = build_contact_point_audit_row(
            subscriber_id=subscriber_id,
            contact_type=contact_type,
            old_value=current_row.get("contact_value"),
            new_value=None,
            apply_run_id=apply_run_id,
            note="subscriber contact_point cleared by HIA blank",
        )

        clear_current_contact_points(
            cur,
            subscriber_id=subscriber_id,
            contact_type=contact_type,
            apply_run_id=apply_run_id,
        )

        if audit_row is not None:
            insert_subscriber_audit_row(cur, audit_row=audit_row)

        return

    if diff_status == "insert":
        if not value:
            raise RuntimeError(
                f"insert contact point requires value: {contact_type}"
            )
        insert_contact_point(
            cur,
            subscriber_id=subscriber_id,
            contact_type=contact_type,
            contact_value=value,
            apply_run_id=apply_run_id,
        )
        return

    if diff_status == "switch_current":
        if target_contact_point_id is None:
            raise RuntimeError(
                f"switch_current requires target_contact_point_id: {contact_type}"
            )

        switch_current_contact_point(
            cur,
            subscriber_id=subscriber_id,
            contact_type=contact_type,
            target_contact_point_id=target_contact_point_id,
            apply_run_id=apply_run_id,
        )
        return

    raise RuntimeError(
        f"unsupported contact point diff_status={diff_status} for {contact_type}"
    )


# ============================================================
# public entry
# ============================================================


def apply_subscriber_contact_points(
    cur,
    *,
    row: dict[str, Any],
    subscriber_id: int | None,
    apply_run_id: int,
) -> None:
    """
    1 staging row に対する subscriber_contact_points apply を行う。
    """

    if subscriber_id is None:
        return

    contact_point_diff_status = _as_text(row.get("contact_point_diff_status"))

    if contact_point_diff_status in {"", "noop"}:
        return

    if contact_point_diff_status == "review":
        return

    phone_diff_status = _as_text(row.get("phone_diff_status"))
    phone_target_contact_point_id = _to_int_or_none(
        row.get("phone_target_contact_point_id")
    )

    email_diff_status = _as_text(row.get("email_diff_status"))
    email_target_contact_point_id = _to_int_or_none(
        row.get("email_target_contact_point_id")
    )

    apply_single_contact_point(
        cur,
        subscriber_id=int(subscriber_id),
        contact_type="phone",
        contact_value=_to_none_if_blank(row.get("phone")),
        diff_status=phone_diff_status,
        target_contact_point_id=phone_target_contact_point_id,
        apply_run_id=apply_run_id,
    )

    apply_single_contact_point(
        cur,
        subscriber_id=int(subscriber_id),
        contact_type="email",
        contact_value=_to_none_if_blank(row.get("email")),
        diff_status=email_diff_status,
        target_contact_point_id=email_target_contact_point_id,
        apply_run_id=apply_run_id,
    )