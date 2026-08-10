from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


VALID_TARGET_STATUSES = {"SUBMITTED", "SUBMISSION_ERROR", "PENDING"}


@dataclass(frozen=True)
class FundDeliverySubmissionConfig:
    delivery_list_id: int
    delivery_member_ids: tuple[int, ...] = ()
    all_members: bool = False
    target_status: str = "SUBMITTED"
    submitted_at: datetime | None = None
    submitted_by: str | None = None
    submission_note: str | None = None
    dry_run: bool = True


@dataclass
class FundDeliverySubmissionSummary:
    delivery_list_id: int
    members_seen: int = 0
    members_updated: int = 0
    runs_updated: int = 0
    person_status_updated: int = 0
    list_status: str | None = None
    errors: int = 0


def validate_config(config: FundDeliverySubmissionConfig) -> None:
    if config.target_status not in VALID_TARGET_STATUSES:
        raise ValueError(f"invalid target_status: {config.target_status}")
    if config.all_members and config.delivery_member_ids:
        raise ValueError("use either --all or --delivery-member-id, not both")
    if not config.all_members and not config.delivery_member_ids:
        raise ValueError("--all or --delivery-member-id is required")


def _submitted_at(config: FundDeliverySubmissionConfig) -> datetime:
    return config.submitted_at or datetime.now()


def _member_id_predicate(config: FundDeliverySubmissionConfig) -> tuple[str, list[Any]]:
    if config.all_members:
        return "", []
    placeholders = ", ".join(["%s"] * len(config.delivery_member_ids))
    return f" AND m.delivery_member_id IN ({placeholders})", list(config.delivery_member_ids)


def load_target_members(cur: Any, config: FundDeliverySubmissionConfig) -> list[dict[str, Any]]:
    member_sql, member_params = _member_id_predicate(config)
    cur.execute(
        f"""
        SELECT
            m.delivery_member_id,
            m.delivery_run_id,
            m.person_year_id,
            m.member_status,
            r.delivery_list_id
          FROM fund_delivery_members m
          JOIN fund_delivery_runs r
            ON r.delivery_run_id = m.delivery_run_id
         WHERE r.delivery_list_id = %s
           {member_sql}
         ORDER BY m.delivery_run_id, m.delivery_member_id
        """,
        [config.delivery_list_id, *member_params],
    )
    rows = list(cur.fetchall() or [])
    if not rows:
        raise ValueError(f"no fund_delivery_members found for delivery_list_id={config.delivery_list_id}")
    if not config.all_members and len(rows) != len(set(config.delivery_member_ids)):
        found = {int(row["delivery_member_id"]) for row in rows}
        missing = sorted(set(config.delivery_member_ids) - found)
        raise ValueError(f"delivery_member_id not found in list {config.delivery_list_id}: {missing}")
    return rows


def update_members(cur: Any, config: FundDeliverySubmissionConfig, rows: list[dict[str, Any]]) -> int:
    submitted_at = _submitted_at(config)
    ids = [row["delivery_member_id"] for row in rows]
    placeholders = ", ".join(["%s"] * len(ids))
    submitted_fields = ""
    params: list[Any] = [config.target_status, config.submission_note]
    if config.target_status == "SUBMITTED":
        submitted_fields = ", submitted_at = %s, submitted_by = %s"
        params.extend([submitted_at, config.submitted_by])
    cur.execute(
        f"""
        UPDATE fund_delivery_members
           SET member_status = %s,
               submission_note = %s
               {submitted_fields},
               updated_at = CURRENT_TIMESTAMP(3)
         WHERE delivery_member_id IN ({placeholders})
        """,
        [*params, *ids],
    )
    return int(cur.rowcount)


def _compute_aggregate_statuses(cur: Any, delivery_list_id: int) -> dict[str, Any]:
    cur.execute(
        """
        SELECT
            COUNT(*) AS total_count,
            SUM(CASE WHEN m.member_status = 'SUBMITTED' THEN 1 ELSE 0 END) AS submitted_count,
            SUM(CASE WHEN m.member_status = 'SUBMISSION_ERROR' THEN 1 ELSE 0 END) AS error_count,
            SUM(CASE WHEN m.member_status = 'PENDING' THEN 1 ELSE 0 END) AS pending_count,
            SUM(CASE WHEN m.member_status = 'CREATED' THEN 1 ELSE 0 END) AS created_count
          FROM fund_delivery_members m
          JOIN fund_delivery_runs r
            ON r.delivery_run_id = m.delivery_run_id
         WHERE r.delivery_list_id = %s
        """,
        (delivery_list_id,),
    )
    row = cur.fetchone() or {}
    total = int(row.get("total_count") or 0)
    submitted = int(row.get("submitted_count") or 0)
    errors = int(row.get("error_count") or 0)
    pending = int(row.get("pending_count") or 0)
    created = int(row.get("created_count") or 0)

    if total == 0:
        list_status = "DRAFT"
    elif errors and submitted:
        list_status = "PARTIAL_ERROR"
    elif errors:
        list_status = "SUBMISSION_ERROR"
    elif submitted == total:
        list_status = "SUBMITTED"
    elif submitted:
        list_status = "PARTIAL_SUBMITTED"
    elif pending:
        list_status = "PENDING"
    elif created:
        list_status = "CREATED"
    else:
        list_status = "DRAFT"

    return {
        "total": total,
        "submitted": submitted,
        "errors": errors,
        "pending": pending,
        "created": created,
        "list_status": list_status,
    }


def update_runs(cur: Any, config: FundDeliverySubmissionConfig) -> int:
    cur.execute(
        """
        UPDATE fund_delivery_runs r
        JOIN (
            SELECT
                m.delivery_run_id,
                COUNT(*) AS total_count,
                SUM(CASE WHEN m.member_status = 'SUBMITTED' THEN 1 ELSE 0 END) AS submitted_count,
                SUM(CASE WHEN m.member_status = 'SUBMISSION_ERROR' THEN 1 ELSE 0 END) AS error_count
              FROM fund_delivery_members m
             GROUP BY m.delivery_run_id
        ) s
          ON s.delivery_run_id = r.delivery_run_id
           SET r.delivery_status =
               CASE
                 WHEN s.error_count > 0 AND s.submitted_count > 0 THEN 'PARTIAL_ERROR'
                 WHEN s.error_count > 0 THEN 'SUBMISSION_ERROR'
                 WHEN s.submitted_count = s.total_count THEN 'SUBMITTED'
                 WHEN s.submitted_count > 0 THEN 'PARTIAL_SUBMITTED'
                 ELSE 'CREATED'
               END,
               r.updated_at = CURRENT_TIMESTAMP(3)
         WHERE r.delivery_list_id = %s
        """,
        (config.delivery_list_id,),
    )
    return int(cur.rowcount)


def update_person_status(
    cur: Any,
    config: FundDeliverySubmissionConfig,
    rows: list[dict[str, Any]],
) -> int:
    if config.target_status != "SUBMITTED":
        return 0
    submitted_at = _submitted_at(config)
    count = 0
    for row in rows:
        cur.execute(
            """
            UPDATE fund_delivery_person_status
               SET delivery_tracking_status = 'DELIVERED',
                   last_delivery_run_id = %s,
                   last_delivery_member_id = %s,
                   last_delivered_at = %s,
                   last_delivered_by = %s,
                   tracking_reason = %s,
                   updated_at = CURRENT_TIMESTAMP(3)
             WHERE person_year_id = %s
            """,
            (
                row["delivery_run_id"],
                row["delivery_member_id"],
                submitted_at,
                config.submitted_by,
                config.submission_note or "fund delivery submitted",
                row["person_year_id"],
            ),
        )
        count += int(cur.rowcount)
    return count


def update_list(cur: Any, config: FundDeliverySubmissionConfig) -> str:
    aggregate = _compute_aggregate_statuses(cur, config.delivery_list_id)
    list_status = str(aggregate["list_status"])
    submitted_at = _submitted_at(config)
    submitted_fields = ""
    params: list[Any] = [list_status, config.submission_note]
    if list_status == "SUBMITTED":
        submitted_fields = ", submitted_at = %s, submitted_by = %s"
        params.extend([submitted_at, config.submitted_by])
    cur.execute(
        f"""
        UPDATE fund_delivery_lists
           SET list_status = %s,
               submission_note = %s
               {submitted_fields},
               updated_at = CURRENT_TIMESTAMP(3)
         WHERE delivery_list_id = %s
        """,
        [*params, config.delivery_list_id],
    )
    return list_status


def mark_fund_delivery_submitted(
    cur: Any,
    config: FundDeliverySubmissionConfig,
) -> FundDeliverySubmissionSummary:
    validate_config(config)
    rows = load_target_members(cur, config)
    summary = FundDeliverySubmissionSummary(delivery_list_id=config.delivery_list_id)
    summary.members_seen = len(rows)
    summary.members_updated = update_members(cur, config, rows)
    summary.person_status_updated = update_person_status(cur, config, rows)
    summary.runs_updated = update_runs(cur, config)
    summary.list_status = update_list(cur, config)
    return summary
