

# -*- coding: utf-8 -*-
"""
============================================================
Module : apply_action_subscriber_address.py
Path   : scripts/hia/script_lib/apply_action_subscriber_address.py
Project: PHR

Purpose:
    Apply one staging subscriber row to subscriber_addresses.

Responsibility:
    - apply address child resource for one subscriber
    - handle address noop / switch_current / insert
    - keep subscriber_addresses as history table

Non-goals:
    - subscribers root apply
    - subscriber_contact_points apply
    - subscriber_audit insert
    - staging processed mark
    - prepare / compare decision

Notes:
    Address apply is based on compare result already written to staging.

    address_diff_status:
        - noop
        - switch_current
        - insert
        - review

    subscriber_addresses is history/current managed:
        - current row is_current = 1
        - history rows is_current = 0
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


def clear_current_subscriber_addresses(
    cur,
    *,
    subscriber_id: int,
) -> None:
    """対象 subscriber の current address を全て history 化する。"""

    cur.execute(
        """
        UPDATE subscriber_addresses
        SET
            is_current = 0,
            valid_to = NOW(),
            updated_at = NOW()
        WHERE subscriber_id = %(subscriber_id)s
          AND is_current = 1
        """,
        {"subscriber_id": subscriber_id},
    )


def switch_current_subscriber_address(
    cur,
    *,
    subscriber_id: int,
    address_hash: str,
) -> None:
    """
    既存 history address を current に戻す。

    Preconditions:
        - same address_hash exists
        - compare phase determined switch_current
    """

    clear_current_subscriber_addresses(cur, subscriber_id=subscriber_id)

    cur.execute(
        """
        UPDATE subscriber_addresses
        SET
            is_current = 1,
            valid_from = NOW(),
            valid_to = NULL,
            updated_at = NOW()
        WHERE subscriber_id = %(subscriber_id)s
          AND address_hash = %(address_hash)s
        ORDER BY address_id DESC
        LIMIT 1
        """,
        {
            "subscriber_id": subscriber_id,
            "address_hash": address_hash,
        },
    )


# ============================================================
# insert
# ============================================================


def insert_subscriber_address(
    cur,
    *,
    subscriber_id: int,
    row: dict[str, Any],
) -> None:
    """subscriber_addresses に新しい current address row を INSERT する。"""

    clear_current_subscriber_addresses(cur, subscriber_id=subscriber_id)

    cur.execute(
        """
        INSERT INTO subscriber_addresses (
            subscriber_id,
            postal_code,
            address_line,
            building,
            address_hash,
            is_current,
            valid_from,
            valid_to,
            created_at,
            updated_at
        )
        VALUES (
            %(subscriber_id)s,
            %(postal_code)s,
            %(address_line)s,
            %(building)s,
            %(address_hash)s,
            1,
            NOW(),
            NULL,
            NOW(),
            NOW()
        )
        """,
        {
            "subscriber_id": subscriber_id,
            "postal_code": _to_none_if_blank(row.get("postal_code")),
            "address_line": _to_none_if_blank(row.get("address_line")),
            "building": _to_none_if_blank(row.get("building")),
            "address_hash": row.get("address_hash"),
        },
    )


# ============================================================
# public entry
# ============================================================


def apply_subscriber_address(
    cur,
    *,
    row: dict[str, Any],
    subscriber_id: int | None,
) -> None:
    """
    1 staging row に対する subscriber_addresses apply を行う。
    """

    if subscriber_id is None:
        return

    address_diff_status = _as_text(row.get("address_diff_status"))

    if address_diff_status in {"", "noop"}:
        return

    if address_diff_status == "review":
        return

    address_hash = _as_text(row.get("address_hash"))
    if not address_hash:
        return

    if address_diff_status == "switch_current":
        switch_current_subscriber_address(
            cur,
            subscriber_id=int(subscriber_id),
            address_hash=address_hash,
        )
        return

    if address_diff_status == "insert":
        insert_subscriber_address(
            cur,
            subscriber_id=int(subscriber_id),
            row=row,
        )
        return

    # Unknown status is intentionally ignored here.
    # compare phase should route ambiguous rows to review.
    return