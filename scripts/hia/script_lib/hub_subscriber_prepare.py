# -*- coding: utf-8 -*-
"""
============================================================
Module : hub_subscriber_prepare.py
Path   : scripts/hia/script_lib/hub_subscriber_prepare.py
Project: PHR

Purpose:
    Hub subscribers staging prepare helper。

Responsibility:
    - load unprocessed staging rows for one import_run_id
    - inspect current snapshot status
    - compare import-side hash and current-side hash
    - write lightweight compare status to staging_subscribers_hub
    - decide preliminary apply_action

Non-goals:
    - subscribers / addresses / contact points update
    - subscriber_audit insert
    - DB connection lifecycle
    - etl run start / finish
    - detailed address/contact point apply

Notes:
    This module is the first step of apply orchestration.

    It treats staging_subscribers_hub as a compare workspace:

        import values
        current snapshot values
        compare status
        apply_action

    Contact point detailed comparison is intentionally left as
    pending_compare in this prepare step. The later compare/apply step
    can refine contact_point_diff_status before actual apply.
============================================================
"""

from __future__ import annotations

import json

from dataclasses import dataclass
from typing import Any, Optional


# ============================================================
# metrics / result
# ============================================================


@dataclass
class PrepareMetrics:
    """Hub subscriber prepare phase metrics."""

    rows_seen: int = 0
    rows_prepared: int = 0
    insert_candidates: int = 0
    update_candidates: int = 0
    noop_candidates: int = 0
    review_candidates: int = 0


@dataclass(frozen=True)
class PrepareDecision:
    """staging_subscribers_hub に保存する prepare 判定結果。"""

    apply_action: str
    apply_diff_columns: str
    identity_match_status: str
    address_diff_status: str
    contact_point_diff_status: str
    phone_diff_status: str
    phone_target_contact_point_id: int | None
    email_diff_status: str
    email_target_contact_point_id: int | None


# ============================================================
# load staging rows
# ============================================================


PREPARE_ROW_COLUMNS = """
    id AS staging_subscriber_hub_id,
    import_run_id,

    hia_subscriber_id,
    identity_hash,
    compare_identity_norm_hash,
    compare_other_hash,
    address_hash,
    phone,
    email,

    current_subscriber_id,
    current_hia_subscriber_id,
    current_identity_hash,
    current_compare_identity_norm_hash,
    current_compare_other_hash,
    current_address_id,
    current_address_hash,
    current_phone_contact_point_id,
    current_email_contact_point_id,
    current_lookup_status
"""


def load_staging_rows_for_prepare(
    cur,
    *,
    import_run_id: int,
    limit: int = 0,
) -> list[dict[str, Any]]:
    """
    apply orchestration prepare 対象の staging 行を取得する。

    Conditions:
        - import_run_id が一致
        - processed_run_id IS NULL

    Notes:
        - apply_action の有無では絞らない
        - 再prepareを許容するため、未処理行を対象に再評価する
    """

    limit_sql = ""
    params: dict[str, Any] = {"import_run_id": import_run_id}

    if limit and limit > 0:
        limit_sql = "\nLIMIT %(limit)s"
        params["limit"] = limit

    cur.execute(
        f"""
        SELECT
            {PREPARE_ROW_COLUMNS}
        FROM staging_subscribers_hub
        WHERE import_run_id = %(import_run_id)s
          AND processed_run_id IS NULL
        ORDER BY id ASC
        {limit_sql}
        """,
        params,
    )

    return list(cur.fetchall())


# ============================================================
# decision helpers
# ============================================================


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _same(left: Any, right: Any) -> bool:
    return _as_text(left) == _as_text(right)


def _join_diff_columns(columns: list[str]) -> str:
    return json.dumps(sorted(set(columns)), ensure_ascii=False)


def decide_prepare_action(row: dict[str, Any]) -> PrepareDecision:
    """
    current snapshot と import-side hash を使って一次 apply_action を決める。

    Policy:
        - current_lookup_status が multiple_match / review 系なら review
        - current がない場合は insert
        - HIA加入者IDが current と異なる場合は review
        - hash差分がある場合は update
        - contact point はこの段階では pending_compare
    """

    current_lookup_status = _as_text(row.get("current_lookup_status"))
    current_subscriber_id = row.get("current_subscriber_id")

    if current_lookup_status in {"multiple_match", "review", "projection_error"}:
        return PrepareDecision(
            apply_action="review",
            apply_diff_columns="current_lookup_status",
            identity_match_status=current_lookup_status,
            address_diff_status="not_checked",
            contact_point_diff_status="not_checked",
            phone_diff_status="not_checked",
            phone_target_contact_point_id=None,
            email_diff_status="not_checked",
            email_target_contact_point_id=None,
        )

    if current_lookup_status == "not_found" or current_subscriber_id is None:
        return PrepareDecision(
            apply_action="insert",
            apply_diff_columns="subscriber,address,contact_point",
            identity_match_status="not_found",
            address_diff_status="insert",
            contact_point_diff_status="insert",
            phone_diff_status="insert",
            phone_target_contact_point_id=None,
            email_diff_status="insert",
            email_target_contact_point_id=None,
        )

    hia_subscriber_id = _as_text(row.get("hia_subscriber_id"))
    current_hia_subscriber_id = _as_text(row.get("current_hia_subscriber_id"))

    if hia_subscriber_id and current_hia_subscriber_id and hia_subscriber_id != current_hia_subscriber_id:
        return PrepareDecision(
            apply_action="review",
            apply_diff_columns="hia_subscriber_id",
            identity_match_status="hia_subscriber_id_mismatch",
            address_diff_status="not_checked",
            contact_point_diff_status="not_checked",
            phone_diff_status="not_checked",
            phone_target_contact_point_id=None,
            email_diff_status="not_checked",
            email_target_contact_point_id=None,
        )

    diff_columns: list[str] = []

    if _same(row.get("identity_hash"), row.get("current_identity_hash")):
        identity_match_status = "identity_hash_matched"
    else:
        identity_match_status = "identity_hash_mismatch"
        diff_columns.append("identity_hash")

    if not _same(
        row.get("compare_identity_norm_hash"),
        row.get("current_compare_identity_norm_hash"),
    ):
        diff_columns.append("compare_identity_norm_hash")

    if not _same(
        row.get("compare_other_hash"),
        row.get("current_compare_other_hash"),
    ):
        diff_columns.append("compare_other_hash")

    current_address_id = row.get("current_address_id")
    if current_address_id is None:
        address_diff_status = "insert"
        diff_columns.append("address")
    elif _same(row.get("address_hash"), row.get("current_address_hash")):
        address_diff_status = "noop"
    else:
        address_diff_status = "changed"
        diff_columns.append("address")

    phone_diff_status = "pending_compare"
    phone_target_contact_point_id = row.get("current_phone_contact_point_id")
    email_diff_status = "pending_compare"
    email_target_contact_point_id = row.get("current_email_contact_point_id")

    contact_point_diff_status = "pending_compare"
    if _as_text(row.get("phone")) or _as_text(row.get("email")):
        diff_columns.append("contact_point")

    if diff_columns:
        apply_action = "update"
    else:
        apply_action = "noop"

    return PrepareDecision(
        apply_action=apply_action,
        apply_diff_columns=_join_diff_columns(diff_columns),
        identity_match_status=identity_match_status,
        address_diff_status=address_diff_status,
        contact_point_diff_status=contact_point_diff_status,
        phone_diff_status=phone_diff_status,
        phone_target_contact_point_id=phone_target_contact_point_id,
        email_diff_status=email_diff_status,
        email_target_contact_point_id=email_target_contact_point_id,
    )


# ============================================================
# update staging
# ============================================================


def update_staging_prepare_result(
    cur,
    *,
    staging_id: int,
    decision: PrepareDecision,
) -> None:
    """prepare 判定結果を staging_subscribers_hub に保存する。"""

    cur.execute(
        """
        UPDATE staging_subscribers_hub
        SET
            apply_action = %(apply_action)s,
            apply_diff_columns = %(apply_diff_columns)s,
            identity_match_status = %(identity_match_status)s,
            address_diff_status = %(address_diff_status)s,
            contact_point_diff_status = %(contact_point_diff_status)s,
            phone_diff_status = %(phone_diff_status)s,
            phone_target_contact_point_id = %(phone_target_contact_point_id)s,
            email_diff_status = %(email_diff_status)s,
            email_target_contact_point_id = %(email_target_contact_point_id)s,
            apply_checked_at = NOW()
        WHERE id = %(staging_id)s
        """,
        {
            "staging_id": staging_id,
            "apply_action": decision.apply_action,
            "apply_diff_columns": decision.apply_diff_columns,
            "identity_match_status": decision.identity_match_status,
            "address_diff_status": decision.address_diff_status,
            "contact_point_diff_status": decision.contact_point_diff_status,
            "phone_diff_status": decision.phone_diff_status,
            "phone_target_contact_point_id": decision.phone_target_contact_point_id,
            "email_diff_status": decision.email_diff_status,
            "email_target_contact_point_id": decision.email_target_contact_point_id,
        },
    )


# ============================================================
# public entry
# ============================================================


def prepare_hia_subscriber_apply_actions(
    cur,
    *,
    import_run_id: int,
    limit: int = 0,
    dry_run: bool = False,
) -> PrepareMetrics:
    """
    HIA subscriber apply orchestration の prepare を実行する。

    Returns:
        PrepareMetrics
    """

    metrics = PrepareMetrics()

    rows = load_staging_rows_for_prepare(
        cur,
        import_run_id=import_run_id,
        limit=limit,
    )

    for row in rows:
        metrics.rows_seen += 1

        staging_id = row["staging_subscriber_hub_id"]
        decision = decide_prepare_action(row)

        if not dry_run:
            update_staging_prepare_result(
                cur,
                staging_id=staging_id,
                decision=decision,
            )

        metrics.rows_prepared += 1

        if decision.apply_action == "insert":
            metrics.insert_candidates += 1
        elif decision.apply_action == "update":
            metrics.update_candidates += 1
        elif decision.apply_action == "noop":
            metrics.noop_candidates += 1
        elif decision.apply_action == "review":
            metrics.review_candidates += 1

    return metrics