

# -*- coding: utf-8 -*-
"""
============================================================
Module : hub_subscriber_compare.py
Path   : scripts/hia/script_lib/hub_subscriber_compare.py
Project: PHR

Purpose:
    Hub subscribers staging detailed compare helper。

Responsibility:
    - load prepared staging rows for one import_run_id
    - refine address_diff_status using subscriber_addresses history
    - refine contact_point_diff_status using subscriber_contact_points history
    - refresh apply_action / apply_diff_columns after detailed compare
    - write compare result to staging_subscribers_hub

Non-goals:
    - subscribers / addresses / contact points update
    - subscriber_audit insert
    - DB connection lifecycle
    - etl run start / finish

Notes:
    This module is the second step of apply orchestration.

    prepare step performs lightweight classification.
    compare step performs detailed table checks needed before apply.
============================================================
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from scripts.lib.db.lookup.subscriber_contact_points import get_contact_point_by_id


# ============================================================
# metrics / result
# ============================================================


@dataclass
class CompareMetrics:
    """Hub subscriber compare phase metrics."""

    rows_seen: int = 0
    rows_compared: int = 0
    insert_candidates: int = 0
    update_candidates: int = 0
    noop_candidates: int = 0
    review_candidates: int = 0


@dataclass(frozen=True)
class CompareDecision:
    """staging_subscribers_hub に保存する detailed compare 判定結果。"""

    apply_action: str
    apply_diff_columns: str
    address_diff_status: str
    contact_point_diff_status: str


@dataclass(frozen=True)
class AddressCompareResult:
    """住所詳細compare結果。"""

    status: str
    reason: str = ""


@dataclass(frozen=True)
class ContactPointCompareResult:
    """contact point詳細compare結果。"""

    status: str
    reason: str = ""


# ============================================================
# load staging rows
# ============================================================


COMPARE_ROW_COLUMNS = """
    id AS staging_subscriber_hub_id,
    import_run_id,

    current_subscriber_id,
    current_lookup_status,

    compare_identity_norm_hash,
    current_compare_identity_norm_hash,
    compare_other_hash,
    current_compare_other_hash,

    address_hash,
    current_address_hash,
    address_diff_status,

    phone,
    email,
    current_phone_contact_point_id,
    current_email_contact_point_id,
    contact_point_diff_status,

    apply_action,
    apply_diff_columns,
    identity_match_status
"""


def load_staging_rows_for_compare(
    cur,
    *,
    import_run_id: int,
    limit: int = 0,
) -> list[dict[str, Any]]:
    """
    detailed compare 対象の staging 行を取得する。

    Conditions:
        - import_run_id が一致
        - processed_run_id IS NULL
        - apply_action は insert / update / noop / review を問わず再評価可能
    """

    limit_sql = ""
    params: dict[str, Any] = {"import_run_id": import_run_id}

    if limit and limit > 0:
        limit_sql = "\nLIMIT %(limit)s"
        params["limit"] = limit

    cur.execute(
        f"""
        SELECT
            {COMPARE_ROW_COLUMNS}
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
# helpers
# ============================================================


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _to_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return int(text)


def _same(left: Any, right: Any) -> bool:
    return _as_text(left) == _as_text(right)


def _split_diff_columns(value: Any) -> set[str]:
    text = _as_text(value)
    if not text:
        return set()
    return {part.strip() for part in text.split(",") if part.strip()}


def _join_diff_columns(columns: set[str]) -> str:
    return ",".join(sorted(columns))


# ============================================================
# address compare
# ============================================================


def compare_address(
    cur,
    *,
    subscriber_id: int | None,
    address_hash: str | None,
) -> AddressCompareResult:
    """
    subscriber_addresses に対して address_hash の存在と current 状態を確認する。

    status:
        - noop
        - switch_current
        - insert
        - review
    """

    if subscriber_id is None:
        return AddressCompareResult(status="insert", reason="no_current_subscriber")

    if not _as_text(address_hash):
        return AddressCompareResult(status="noop", reason="no_import_address_hash")

    cur.execute(
        """
        SELECT
            address_id,
            is_current
        FROM subscriber_addresses
        WHERE subscriber_id = %(subscriber_id)s
          AND address_hash = %(address_hash)s
        ORDER BY is_current DESC, address_id DESC
        """,
        {
            "subscriber_id": subscriber_id,
            "address_hash": address_hash,
        },
    )

    rows = list(cur.fetchall())

    if not rows:
        return AddressCompareResult(status="insert", reason="address_hash_not_found")

    current_rows = [row for row in rows if int(row.get("is_current") or 0) == 1]

    if len(current_rows) == 1:
        return AddressCompareResult(status="noop", reason="same_current_address")

    if len(current_rows) > 1:
        return AddressCompareResult(status="review", reason="multiple_current_same_address_hash")

    return AddressCompareResult(status="switch_current", reason="same_address_in_history")


# ============================================================
# contact point compare
# ============================================================


def compare_single_contact_point(
    cur,
    *,
    subscriber_id: int | None,
    contact_type: str,
    contact_value: str | None,
    current_contact_point_id: int | None,
) -> ContactPointCompareResult:
    """
    subscriber_contact_points に対して単一 contact_type の比較を行う。

    current snapshot の contact point id を起点に current 値を取得し、
    import value と比較する。

    null / blank import value は current解除候補として扱う。
    """

    if subscriber_id is None:
        return ContactPointCompareResult(status="insert", reason="no_current_subscriber")

    value = _as_text(contact_value)
    conn = cur.connection
    current_row = get_contact_point_by_id(conn, current_contact_point_id)

    if current_row is not None:
        current_subscriber_id = _to_int_or_none(current_row.get("subscriber_id"))
        current_contact_type = _as_text(current_row.get("contact_type"))
        current_is_current = int(current_row.get("is_current") or 0)

        if current_subscriber_id != int(subscriber_id):
            return ContactPointCompareResult(status="review", reason="current_contact_point_subscriber_mismatch")

        if current_contact_type != contact_type:
            return ContactPointCompareResult(status="review", reason="current_contact_point_type_mismatch")

        if current_is_current != 1:
            return ContactPointCompareResult(status="review", reason="current_contact_point_not_current")

    elif current_contact_point_id is not None:
        return ContactPointCompareResult(status="review", reason="current_contact_point_id_not_found")

    current_value = _as_text(current_row.get("contact_value")) if current_row else ""

    if not value:
        if not current_value:
            return ContactPointCompareResult(status="noop", reason="no_import_value_no_current")
        return ContactPointCompareResult(status="clear_current", reason="import_value_null")

    if current_value == value:
        return ContactPointCompareResult(status="noop", reason="same_current_contact_value")

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
        return ContactPointCompareResult(status="insert", reason="contact_value_not_found")

    current_rows = [row for row in rows if int(row.get("is_current") or 0) == 1]

    if current_rows:
        return ContactPointCompareResult(status="review", reason="same_contact_value_current_changed_after_snapshot")

    return ContactPointCompareResult(status="switch_current", reason="same_contact_value_in_history")


def compare_contact_points(
    cur,
    *,
    subscriber_id: int | None,
    phone: str | None,
    email: str | None,
    current_phone_contact_point_id: int | None,
    current_email_contact_point_id: int | None,
) -> ContactPointCompareResult:
    """
    phone / email の detailed compare を行い、集約statusを返す。
    """

    phone_result = compare_single_contact_point(
        cur,
        subscriber_id=subscriber_id,
        contact_type="phone",
        contact_value=phone,
        current_contact_point_id=current_phone_contact_point_id,
    )
    email_result = compare_single_contact_point(
        cur,
        subscriber_id=subscriber_id,
        contact_type="email",
        contact_value=email,
        current_contact_point_id=current_email_contact_point_id,
    )

    statuses = {phone_result.status, email_result.status}

    if "review" in statuses:
        return ContactPointCompareResult(
            status="review",
            reason=f"phone={phone_result.reason};email={email_result.reason}",
        )

    if statuses == {"noop"}:
        return ContactPointCompareResult(
            status="noop",
            reason=f"phone={phone_result.reason};email={email_result.reason}",
        )

    return ContactPointCompareResult(
        status="changed",
        reason=f"phone={phone_result.status};email={email_result.status}",
    )


# ============================================================
# decision
# ============================================================


def decide_compare_action(
    row: dict[str, Any],
    *,
    address_result: AddressCompareResult,
    contact_result: ContactPointCompareResult,
) -> CompareDecision:
    """detailed compare 結果から最終 apply_action を再決定する。"""

    original_action = _as_text(row.get("apply_action"))

    if original_action == "review":
        return CompareDecision(
            apply_action="review",
            apply_diff_columns=_as_text(row.get("apply_diff_columns")),
            address_diff_status=row.get("address_diff_status") or "not_checked",
            contact_point_diff_status=row.get("contact_point_diff_status") or "not_checked",
        )

    if original_action == "insert":
        return CompareDecision(
            apply_action="insert",
            apply_diff_columns="subscriber,address,contact_point",
            address_diff_status="insert",
            contact_point_diff_status="insert",
        )

    diff_columns = _split_diff_columns(row.get("apply_diff_columns"))

    if address_result.status == "review" or contact_result.status == "review":
        diff_columns.add("review")
        return CompareDecision(
            apply_action="review",
            apply_diff_columns=_join_diff_columns(diff_columns),
            address_diff_status=address_result.status,
            contact_point_diff_status=contact_result.status,
        )

    if address_result.status in {"insert", "switch_current"}:
        diff_columns.add("address")
    elif address_result.status == "noop":
        diff_columns.discard("address")

    if contact_result.status == "changed":
        diff_columns.add("contact_point")
    elif contact_result.status == "noop":
        diff_columns.discard("contact_point")

    if not _same(
        row.get("compare_identity_norm_hash"),
        row.get("current_compare_identity_norm_hash"),
    ):
        diff_columns.add("compare_identity_norm_hash")

    if not _same(row.get("compare_other_hash"), row.get("current_compare_other_hash")):
        diff_columns.add("compare_other_hash")

    apply_action = "update" if diff_columns else "noop"

    return CompareDecision(
        apply_action=apply_action,
        apply_diff_columns=_join_diff_columns(diff_columns),
        address_diff_status=address_result.status,
        contact_point_diff_status=contact_result.status,
    )


# ============================================================
# update staging
# ============================================================


def update_staging_compare_result(
    cur,
    *,
    staging_id: int,
    decision: CompareDecision,
) -> None:
    """detailed compare 結果を staging_subscribers_hub に保存する。"""

    cur.execute(
        """
        UPDATE staging_subscribers_hub
        SET
            apply_action = %(apply_action)s,
            apply_diff_columns = %(apply_diff_columns)s,
            address_diff_status = %(address_diff_status)s,
            contact_point_diff_status = %(contact_point_diff_status)s,
            apply_checked_at = NOW()
        WHERE id = %(staging_id)s
        """,
        {
            "staging_id": staging_id,
            "apply_action": decision.apply_action,
            "apply_diff_columns": decision.apply_diff_columns,
            "address_diff_status": decision.address_diff_status,
            "contact_point_diff_status": decision.contact_point_diff_status,
        },
    )


# ============================================================
# public entry
# ============================================================


def compare_hia_subscriber_apply_actions(
    cur,
    *,
    import_run_id: int,
    limit: int = 0,
    dry_run: bool = False,
) -> CompareMetrics:
    """HIA subscriber apply orchestration の detailed compare を実行する。"""

    metrics = CompareMetrics()

    rows = load_staging_rows_for_compare(
        cur,
        import_run_id=import_run_id,
        limit=limit,
    )

    for row in rows:
        metrics.rows_seen += 1

        staging_id = row["staging_subscriber_hub_id"]
        current_subscriber_id = row.get("current_subscriber_id")

        address_result = compare_address(
            cur,
            subscriber_id=current_subscriber_id,
            address_hash=row.get("address_hash"),
        )

        contact_result = compare_contact_points(
            cur,
            subscriber_id=current_subscriber_id,
            phone=row.get("phone"),
            email=row.get("email"),
            current_phone_contact_point_id=_to_int_or_none(row.get("current_phone_contact_point_id")),
            current_email_contact_point_id=_to_int_or_none(row.get("current_email_contact_point_id")),
        )

        decision = decide_compare_action(
            row,
            address_result=address_result,
            contact_result=contact_result,
        )

        if not dry_run:
            update_staging_compare_result(
                cur,
                staging_id=staging_id,
                decision=decision,
            )

        metrics.rows_compared += 1

        if decision.apply_action == "insert":
            metrics.insert_candidates += 1
        elif decision.apply_action == "update":
            metrics.update_candidates += 1
        elif decision.apply_action == "noop":
            metrics.noop_candidates += 1
        elif decision.apply_action == "review":
            metrics.review_candidates += 1

    return metrics