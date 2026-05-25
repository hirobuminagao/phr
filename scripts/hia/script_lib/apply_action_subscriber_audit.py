

# -*- coding: utf-8 -*-
"""
============================================================
Module : apply_action_subscriber_audit.py
Path   : scripts/hia/script_lib/apply_action_subscriber_audit.py
Project: PHR

Purpose:
    Write subscriber apply audit for one staging subscriber row.

Responsibility:
    - write lightweight audit event for one subscriber apply
    - record staging row / apply_run_id / target subscribers.id
    - record apply_action and diff/status summary

Non-goals:
    - subscribers root apply
    - subscriber_addresses apply
    - subscriber_contact_points apply
    - staging processed mark
    - prepare / compare decision

Notes:
    This module records the fact that apply orchestration handled one row.

    Detailed before/after value audit can be expanded later.
    For now, this keeps a compact trail that connects:

        staging row
        apply run
        subscribers.id
        apply_action
        diff/status summary
============================================================
"""

from __future__ import annotations

import json
from typing import Any


# ============================================================
# helpers
# ============================================================


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _json_dumps(payload: dict[str, Any]) -> str:
    """JSON文字列を安定した形で作る。"""

    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )


# ============================================================
# payload builder
# ============================================================


def build_apply_audit_payload(
    *,
    row: dict[str, Any],
    subscriber_id: int | None,
    apply_run_id: int,
) -> dict[str, Any]:
    """subscriber_audit に保存する lightweight payload を作る。"""

    return {
        "apply_run_id": apply_run_id,
        "staging_subscriber_hub_id": row.get("staging_subscriber_hub_id"),
        "import_run_id": row.get("import_run_id"),
        "subscriber_id": subscriber_id,
        "apply_action": row.get("apply_action"),
        "apply_diff_columns": row.get("apply_diff_columns"),
        "identity_match_status": row.get("identity_match_status"),
        "address_diff_status": row.get("address_diff_status"),
        "contact_point_diff_status": row.get("contact_point_diff_status"),
        "hia_subscriber_id": row.get("hia_subscriber_id"),
        "current_subscriber_id": row.get("current_subscriber_id"),
    }


# ============================================================
# insert
# ============================================================


def insert_subscriber_apply_audit(
    cur,
    *,
    row: dict[str, Any],
    subscriber_id: int | None,
    apply_run_id: int,
) -> None:
    """
    subscriber_audit に apply event を1件 INSERT する。

    Expected subscriber_audit columns:
        - subscriber_id
        - event_type
        - event_source
        - event_payload
        - run_id
        - created_at

    If the actual table differs, adjust only this INSERT layer.
    """

    payload = build_apply_audit_payload(
        row=row,
        subscriber_id=subscriber_id,
        apply_run_id=apply_run_id,
    )

    cur.execute(
        """
        INSERT INTO subscriber_audit (
            subscriber_id,
            event_type,
            event_source,
            event_payload,
            run_id,
            created_at
        )
        VALUES (
            %(subscriber_id)s,
            %(event_type)s,
            %(event_source)s,
            %(event_payload)s,
            %(run_id)s,
            NOW()
        )
        """,
        {
            "subscriber_id": subscriber_id,
            "event_type": "hia_subscriber_apply",
            "event_source": "hia_apply_orchestration",
            "event_payload": _json_dumps(payload),
            "run_id": apply_run_id,
        },
    )


# ============================================================
# public entry
# ============================================================


def apply_subscriber_audit(
    cur,
    *,
    row: dict[str, Any],
    subscriber_id: int | None,
    apply_run_id: int,
) -> None:
    """1 staging row に対する subscriber apply audit を保存する。"""

    action = _as_text(row.get("apply_action"))

    if action in {"", "review"}:
        return

    insert_subscriber_apply_audit(
        cur,
        row=row,
        subscriber_id=subscriber_id,
        apply_run_id=apply_run_id,
    )