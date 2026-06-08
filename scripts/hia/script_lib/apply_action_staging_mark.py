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
    - store apply run linkage on staging_subscribers_hub
    - store apply errors in etl_errors

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
            -> etl_errors insert
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
    """

    cur.execute(
        """
        UPDATE staging_subscribers_hub
        SET
            processed_run_id = %(apply_run_id)s,
            processed_at = NOW()
        WHERE id = %(staging_id)s
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
    apply error を etl_errors に記録する。

    Notes:
        - staging_subscribers_hub.processed_at は更新しない
        - retry 可能な状態を維持する
    """

    cur.execute(
        """
        INSERT INTO etl_errors (
            run_id,
            phase,
            source,
            insurer_number,
            src_file,
            src_row_no,
            src_line_no,
            staging_rowid,
            person_id_custom,
            error_code,
            message
        )
        SELECT
            %(apply_run_id)s,
            'apply',
            'staging_subscribers_hub',
            insurer_number,
            src_file,
            src_row_no,
            src_line_no,
            id,
            person_id_custom,
            %(error_code)s,
            %(error_message)s
        FROM staging_subscribers_hub
        WHERE id = %(staging_id)s
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
    """staging row に紐づく apply error 情報を etl_errors から削除する。"""

    cur.execute(
        """
        DELETE FROM etl_errors
        WHERE phase = 'apply'
          AND source = 'staging_subscribers_hub'
          AND staging_rowid = %(staging_id)s
        """,
        {
            "staging_id": staging_id,
        },
    )