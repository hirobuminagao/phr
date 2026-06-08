# -*- coding: utf-8 -*-
"""
============================================================
Module : hub_subscriber_apply.py
Path   : scripts/hia/script_lib/hub_subscriber_apply.py
Project: PHR

Purpose:
    Hub subscribers apply orchestration helper.

Responsibility:
    - load compared staging rows for one import_run_id
    - process one staging row at a time
    - dispatch apply_action
    - call subscriber / address / contact point / audit / mark modules in order

Non-goals:
    - prepare / compare decision
    - subscribers root SQL implementation
    - subscriber_addresses SQL implementation
    - subscriber_contact_points SQL implementation
    - subscriber_audit SQL implementation
    - DB connection lifecycle
    - etl run start / finish

Notes:
    This module is intentionally thin.

    It orchestrates one subscriber row:

        staging row
          ↓
        apply_action dispatch
          ↓
        subscriber root apply
          ↓
        address apply
          ↓
        contact point apply
          ↓
        processed mark

    Field-level audit is written inside each apply_action_* module.
    Implementation details live in apply_action_* modules.
============================================================
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from scripts.hia.script_lib.apply_action_subscriber import apply_subscriber_root
from scripts.hia.script_lib.apply_action_subscriber_address import apply_subscriber_address
from scripts.hia.script_lib.apply_action_subscriber_contact_point import (
    apply_subscriber_contact_points,
)
from scripts.hia.script_lib.apply_action_staging_mark import (
    mark_staging_apply_error,
    mark_staging_processed,
)


# ============================================================
# metrics / result
# ============================================================


@dataclass
class ApplyMetrics:
    """Hub subscriber apply phase metrics."""

    rows_seen: int = 0
    rows_applied: int = 0
    rows_noop: int = 0
    rows_dry_run: int = 0
    rows_review_skipped: int = 0
    rows_error: int = 0
    subscriber_inserts: int = 0
    subscriber_updates: int = 0
    processed_marked: int = 0


# ============================================================
# load staging rows
# ============================================================


APPLY_ROW_COLUMNS = """
    id AS staging_subscriber_hub_id,
    import_run_id,

    apply_action,
    apply_diff_columns,
    identity_match_status,
    address_diff_status,
    contact_point_diff_status,

    current_subscriber_id,

    hia_subscriber_id,
    person_id_custom,
    identity_hash,
    compare_identity_norm_hash,
    compare_other_hash,

    name_kana_full,
    name_kana_full_match,
    name_kanji_full,
    name_kanji_full_match,
    name_kanji_family,
    name_kanji_middle,
    name_kanji_given,
    name_kana_family,
    name_kana_middle,
    name_kana_given,

    gender_code,
    birth,
    insured_attribute_name,
    relationship_name,

    insurer_number,
    insurance_symbol,
    insurance_symbol_digits,
    insurance_number,
    insurance_branchnumber,

    qualification_acquired_date,
    qualification_lost_date,

    postal_code,
    address_line,
    building,
    address_hash,

    phone,
    email,

    employer_code,
    department_code,
    distribution_code,
    employee_code,
    connect_id
"""


def load_staging_rows_for_apply(
    cur,
    *,
    import_run_id: int,
    limit: int = 0,
) -> list[dict[str, Any]]:
    """
    apply対象の staging 行を取得する。

    Conditions:
        - import_run_id が一致
        - processed_run_id IS NULL
        - apply_action IS NOT NULL
    """

    limit_sql = ""
    params: dict[str, Any] = {"import_run_id": import_run_id}

    if limit and limit > 0:
        limit_sql = "\nLIMIT %(limit)s"
        params["limit"] = limit

    cur.execute(
        f"""
        SELECT
            {APPLY_ROW_COLUMNS}
        FROM staging_subscribers_hub
        WHERE import_run_id = %(import_run_id)s
          AND processed_run_id IS NULL
          AND apply_action IS NOT NULL
        ORDER BY id ASC
        {limit_sql}
        """,
        params,
    )

    return list(cur.fetchall())


# ============================================================
# helpers
# ============================================================


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


# ============================================================
# one-row orchestration
# ============================================================


def apply_one_subscriber_row(
    cur,
    *,
    row: dict[str, Any],
    apply_run_id: int,
    dry_run: bool = False,
) -> str:
    """
    1 staging row を subscriber root -> address -> contact point -> mark の順で処理する。

    Returns:
        applied / noop / review / dry_run / error
    """

    staging_id = row["staging_subscriber_hub_id"]
    action = _as_text(row.get("apply_action"))

    if action == "review":
        return "review"

    if action == "noop":
        if not dry_run:
            mark_staging_processed(
                cur,
                staging_id=staging_id,
                apply_run_id=apply_run_id,
            )
        return "noop"

    if action not in {"insert", "update"}:
        return "review"

    try:
        if dry_run:
            return "dry_run"

        subscriber_id = apply_subscriber_root(
            cur,
            row=row,
            apply_run_id=apply_run_id,
        )

        apply_subscriber_address(
            cur,
            row=row,
            subscriber_id=subscriber_id,
            apply_run_id=apply_run_id,
        )

        apply_subscriber_contact_points(
            cur,
            row=row,
            subscriber_id=subscriber_id,
            apply_run_id=apply_run_id,
        )

        mark_staging_processed(
            cur,
            staging_id=staging_id,
            apply_run_id=apply_run_id,
        )

        return "applied"

    except Exception as exc:
        if not dry_run:
            mark_staging_apply_error(
                cur,
                staging_id=staging_id,
                apply_run_id=apply_run_id,
                error_code=type(exc).__name__,
                error_message=str(exc),
            )
        return "error"


# ============================================================
# public entry
# ============================================================


def apply_hia_subscriber_rows(
    cur,
    *,
    import_run_id: int,
    apply_run_id: int,
    limit: int = 0,
    dry_run: bool = False,
) -> ApplyMetrics:
    """
    compare済み staging 行を1人ずつ順繰り処理する。

    Notes:
        - review は skip して processed mark しない
        - noop は processed mark する
        - dry_run は DB更新・processed mark・audit を一切行わない
        - insert/update は subscriber -> address -> contact_points -> mark の順で処理する
        - audit は各 apply_action_* module 内で field単位に保存する
    """

    metrics = ApplyMetrics()

    rows = load_staging_rows_for_apply(
        cur,
        import_run_id=import_run_id,
        limit=limit,
    )

    for row in rows:
        metrics.rows_seen += 1

        action = _as_text(row.get("apply_action"))
        result = apply_one_subscriber_row(
            cur,
            row=row,
            apply_run_id=apply_run_id,
            dry_run=dry_run,
        )

        if result == "applied":
            metrics.rows_applied += 1
            metrics.processed_marked += 1
            if action == "insert":
                metrics.subscriber_inserts += 1
            elif action == "update":
                metrics.subscriber_updates += 1
        elif result == "dry_run":
            metrics.rows_dry_run += 1
        elif result == "noop":
            metrics.rows_noop += 1
            metrics.processed_marked += 1
        elif result == "review":
            metrics.rows_review_skipped += 1
        elif result == "error":
            metrics.rows_error += 1

    return metrics
