

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

Non-goals:
    - subscribers root apply
    - subscriber_addresses apply
    - subscriber_audit insert
    - staging processed mark
    - prepare / compare decision

Notes:
    Contact point apply is based on compare result already written to staging.

    HIA null/blank value means:
        current value does not exist in the HIA source of truth

    Therefore blank phone/email clears current rows for that contact_type.
============================================================
"""

from __future__ import annotations

from typing import Any


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


# ============================================================
# current control
# ============================================================


def clear_current_contact_points(
    cur,
    *,
    subscriber_id: int,
    contact_type: str,
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
    contact_value: str,
) -> None:
    """
    既存 history contact point を current に戻す。

    Preconditions:
        - same contact_type + contact_value exists
        - compare phase determined switch_current
    """

    clear_current_contact_points(
        cur,
        subscriber_id=subscriber_id,
        contact_type=contact_type,
    )

    cur.execute(
        """
        UPDATE subscriber_contact_points
        SET
            is_current = 1,
            valid_from = NOW(),
            valid_to = NULL,
            updated_at = NOW()
        WHERE subscriber_id = %(subscriber_id)s
          AND contact_type = %(contact_type)s
          AND contact_value = %(contact_value)s
        ORDER BY contact_point_id DESC
        LIMIT 1
        """,
        {
            "subscriber_id": subscriber_id,
            "contact_type": contact_type,
            "contact_value": contact_value,
        },
    )


# ============================================================
# insert
# ============================================================


def insert_contact_point(
    cur,
    *,
    subscriber_id: int,
    contact_type: str,
    contact_value: str,
    source: str = "hia_apply",
) -> None:
    """subscriber_contact_points に新しい current contact point row を INSERT する。"""

    clear_current_contact_points(
        cur,
        subscriber_id=subscriber_id,
        contact_type=contact_type,
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


# ============================================================
# single contact apply
# ============================================================


def apply_single_contact_point(
    cur,
    *,
    subscriber_id: int,
    contact_type: str,
    contact_value: str | None,
) -> None:
    """
    1 contact_type の contact point を staging 値に合わせる。

    Rules:
        - blank -> clear current
        - same current exists -> noop
        - same history exists -> switch current
        - not exists -> insert
    """

    value = _as_text(contact_value)

    if not value:
        clear_current_contact_points(
            cur,
            subscriber_id=subscriber_id,
            contact_type=contact_type,
        )
        return

    cur.execute(
        """
        SELECT
            contact_point_id,
            is_current
        FROM subscriber_contact_points
        WHERE subscriber_id = %(subscriber_id)s
          AND contact_type = %(contact_type)s
          AND contact_value = %(contact_value)s
        ORDER BY is_current DESC, contact_point_id DESC
        """,
        {
            "subscriber_id": subscriber_id,
            "contact_type": contact_type,
            "contact_value": value,
        },
    )

    rows = list(cur.fetchall())

    if not rows:
        insert_contact_point(
            cur,
            subscriber_id=subscriber_id,
            contact_type=contact_type,
            contact_value=value,
        )
        return

    current_rows = [row for row in rows if int(row.get("is_current") or 0) == 1]

    if current_rows:
        return

    switch_current_contact_point(
        cur,
        subscriber_id=subscriber_id,
        contact_type=contact_type,
        contact_value=value,
    )


# ============================================================
# public entry
# ============================================================


def apply_subscriber_contact_points(
    cur,
    *,
    row: dict[str, Any],
    subscriber_id: int | None,
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

    apply_single_contact_point(
        cur,
        subscriber_id=int(subscriber_id),
        contact_type="phone",
        contact_value=_to_none_if_blank(row.get("phone")),
    )

    apply_single_contact_point(
        cur,
        subscriber_id=int(subscriber_id),
        contact_type="email",
        contact_value=_to_none_if_blank(row.get("email")),
    )