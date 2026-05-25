

# -*- coding: utf-8 -*-
"""
============================================================
Module : apply_action_staging_mark.py
Path   : scripts/hia/script_lib/apply_action_staging_mark.py
Project: PHR

Purpose:
    Mark one staging subscriber row as processed or errored.

Responsibility:
    - mark staging_subscribers_hub processed
    - mark staging_subscribers_hub apply error
    - store apply run linkage
    - store lightweight apply error summary

Non-goals:
    - subscribers root apply
    - subscriber_addresses apply
    - subscriber_contact_points apply
    - subscriber_audit insert
    - prepare / compare decision

Notes:
    This module is intentionally small.

    apply orchestration handles one staging row at a time:

        apply success
            -> processed mark

        apply failure
            -> error mark

    Review rows are intentionally not processed here.
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


# ============================================================
# processed mark
# ============================================================


def mark_staging_processed(
    cur,
    *,
    staging_id: int,
    apply_run_id: int,
) -> None:
    """
    staging_subscribers_hub row を processed としてマークする。

    Effects:
        - processed_run_id set
        - processed_at set
        - apply_error_* cleared
    """

    cur.execute(
        """
        UPDATE staging_subscribers_hub
        SET
            processed_run_id = %(apply_run_id)s,
            processed_at = NOW(),
            apply_error_code = NULL,
            apply_error_message = NULL,
            updated_at = NOW()
        WHERE staging_subscriber_hub_id = %(staging_id)s
        """,
        {
            "staging_id": staging_id,
            "apply_run_id": apply_run_id,
        },
    )


# ============================================================
# error mark
# ============================================================


def mark_staging_apply_error(
    cur,
    *,
    staging_id: int,
    apply_run_id: int,
    error_code: str,
    error_message: str,
) -> None:
    """
    staging_subscribers_hub row に apply error を記録する。

    Notes:
        - processed_at は更新しない
        - retry 可能な状態を維持する
    """

    cur.execute(
        """
        UPDATE staging_subscribers_hub
        SET
            apply_error_code = %(error_code)s,
            apply_error_message = %(error_message)s,
            apply_error_at = NOW(),
            apply_error_run_id = %(apply_run_id)s,
            updated_at = NOW()
        WHERE staging_subscriber_hub_id = %(staging_id)s
        """,
        {
            "staging_id": staging_id,
            "apply_run_id": apply_run_id,
            "error_code": _as_text(error_code)[:190],
            "error_message": _as_text(error_message)[:4000],
        },
    )


# ============================================================
# reset
# ============================================================


def reset_staging_apply_error(
    cur,
    *,
    staging_id: int,
) -> None:
    """staging row の apply error 情報をクリアする。"""

    cur.execute(
        """
        UPDATE staging_subscribers_hub
        SET
            apply_error_code = NULL,
            apply_error_message = NULL,
            apply_error_at = NULL,
            apply_error_run_id = NULL,
            updated_at = NOW()
        WHERE staging_subscriber_hub_id = %(staging_id)s
        """,
        {
            "staging_id": staging_id,
        },
    )