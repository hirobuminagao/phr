from __future__ import annotations

from dataclasses import dataclass
from typing import Any


VALID_OUTPUT_MODES = {"EXAM_MONTH", "ALL"}
VALID_DELIVERY_POLICIES = {
    "NOT_DELIVERED_ONLY",
    "REDELIVERY_ONLY",
    "NOT_DELIVERED_AND_REDELIVERY",
    "ALL",
}
VALID_SAME_EXAM_DATE_POLICIES = {"LATEST_DOWNLOAD", "EARLIEST_DOWNLOAD", "MANUAL_REVIEW"}
VALID_GROUPING_MODES = {"ALL", "BY_FACILITY"}


@dataclass(frozen=True)
class FundDeliveryListConfig:
    event_id: int | None
    insurer_number: str
    list_name: str
    output_mode: str = "EXAM_MONTH"
    exam_month: str | None = None
    delivery_policy: str = "NOT_DELIVERED_ONLY"
    same_exam_date_policy: str = "LATEST_DOWNLOAD"
    grouping_mode: str = "ALL"
    sender_code: str = "1322100106"
    sender_name: str | None = None
    created_by: str | None = None
    dry_run: bool = True


@dataclass
class FundDeliveryListSummary:
    valid_xmls_seen: int = 0
    candidate_groups_seen: int = 0
    candidates_upserted: int = 0
    selected_candidates: int = 0
    not_selected_candidates: int = 0
    review_required_candidates: int = 0
    person_status_upserted: int = 0
    list_id: int | None = None
    list_created: int = 0
    list_members_inserted: int = 0
    list_members_updated: int = 0
    list_members_seen: int = 0
    skipped_by_delivery_policy: int = 0


def validate_config(config: FundDeliveryListConfig) -> None:
    if config.output_mode not in VALID_OUTPUT_MODES:
        raise ValueError(f"invalid output_mode: {config.output_mode}")
    if config.delivery_policy not in VALID_DELIVERY_POLICIES:
        raise ValueError(f"invalid delivery_policy: {config.delivery_policy}")
    if config.same_exam_date_policy not in VALID_SAME_EXAM_DATE_POLICIES:
        raise ValueError(f"invalid same_exam_date_policy: {config.same_exam_date_policy}")
    if config.grouping_mode not in VALID_GROUPING_MODES:
        raise ValueError(f"invalid grouping_mode: {config.grouping_mode}")
    if config.output_mode == "EXAM_MONTH" and not config.exam_month:
        raise ValueError("exam_month is required when output_mode=EXAM_MONTH")
    if config.exam_month and (len(config.exam_month) != 6 or not config.exam_month.isdigit()):
        raise ValueError(f"invalid exam_month: {config.exam_month}")
    if not config.insurer_number:
        raise ValueError("insurer_number is required")
    if not config.list_name:
        raise ValueError("list_name is required")
    if not config.sender_code:
        raise ValueError("sender_code is required")


def _event_clause(alias: str, config: FundDeliveryListConfig) -> tuple[str, list[Any]]:
    if config.event_id is None:
        return "", []
    return f" AND {alias}.event_id = %s", [config.event_id]


def _exam_month_clause(alias: str, config: FundDeliveryListConfig) -> tuple[str, list[Any]]:
    if config.output_mode != "EXAM_MONTH":
        return "", []
    return f" AND {alias}.exam_month = %s", [config.exam_month]


def count_valid_xmls(cur: Any, config: FundDeliveryListConfig) -> int:
    event_sql, event_params = _event_clause("x", config)
    month_sql, month_params = _exam_month_clause("x", config)
    cur.execute(
        f"""
        SELECT COUNT(*) AS cnt
          FROM hia_download_xmls x
          JOIN hia_download_zips z
            ON z.download_zip_id = x.download_zip_id
         WHERE x.parse_status = 'PARSED'
           AND x.is_active_in_zip = 1
           AND x.insurer_number = %s
           {event_sql}
           {month_sql}
        """,
        [config.insurer_number, *event_params, *month_params],
    )
    row = cur.fetchone()
    return int(row["cnt"] if row else 0)


def load_candidate_groups(cur: Any, config: FundDeliveryListConfig) -> list[list[dict[str, Any]]]:
    event_sql, event_params = _event_clause("x", config)
    month_sql, month_params = _exam_month_clause("x", config)
    cur.execute(
        f"""
        SELECT
            py.person_year_id,
            pxe.person_xml_event_id,
            x.hia_download_xml_id,
            x.download_zip_id,
            x.exam_date,
            x.exam_month,
            z.dl_date,
            z.send_seq
          FROM hia_download_xmls x
          JOIN hia_download_zips z
            ON z.download_zip_id = x.download_zip_id
          JOIN hia_person_xml_events pxe
            ON pxe.hia_download_xml_id = x.hia_download_xml_id
           AND pxe.event_status = 'ACTIVE'
          JOIN hia_person_years py
            ON py.person_year_id = pxe.person_year_id
         WHERE x.parse_status = 'PARSED'
           AND x.is_active_in_zip = 1
           AND x.insurer_number = %s
           {event_sql}
           {month_sql}
         ORDER BY
            py.person_year_id,
            x.exam_date,
            z.dl_date,
            z.send_seq,
            x.hia_download_xml_id
        """,
        [config.insurer_number, *event_params, *month_params],
    )
    groups: list[list[dict[str, Any]]] = []
    current_key: tuple[Any, Any] | None = None
    current_group: list[dict[str, Any]] = []
    for row in cur.fetchall():
        key = (row["person_year_id"], row["exam_date"])
        if current_key is not None and key != current_key:
            groups.append(current_group)
            current_group = []
        current_key = key
        current_group.append(row)
    if current_group:
        groups.append(current_group)
    return groups


def choose_group_candidate(group: list[dict[str, Any]], config: FundDeliveryListConfig) -> dict[str, Any] | None:
    if config.same_exam_date_policy == "MANUAL_REVIEW" and len(group) > 1:
        return None
    reverse = config.same_exam_date_policy == "LATEST_DOWNLOAD"
    return sorted(
        group,
        key=lambda row: (
            row["dl_date"],
            int(row["send_seq"] or 0),
            int(row["hia_download_xml_id"]),
        ),
        reverse=reverse,
    )[0]


def upsert_candidates(cur: Any, config: FundDeliveryListConfig) -> FundDeliveryListSummary:
    summary = FundDeliveryListSummary()
    summary.valid_xmls_seen = count_valid_xmls(cur, config)
    groups = load_candidate_groups(cur, config)
    summary.candidate_groups_seen = len(groups)

    for group in groups:
        chosen = choose_group_candidate(group, config)
        for row in group:
            is_chosen = chosen is not None and row["hia_download_xml_id"] == chosen["hia_download_xml_id"]
            if chosen is None:
                status = "REVIEW_REQUIRED"
                selection_reason = None
                not_selected_reason = "same exam date has multiple XMLs and policy=MANUAL_REVIEW"
                summary.review_required_candidates += 1
            elif is_chosen:
                status = "SELECTED"
                selection_reason = (
                    f"{config.same_exam_date_policy}: selected by dl_date/send_seq within "
                    f"person_year_id={row['person_year_id']} exam_date={row['exam_date']}"
                )
                not_selected_reason = None
                summary.selected_candidates += 1
            else:
                status = "NOT_SELECTED"
                selection_reason = None
                not_selected_reason = (
                    f"{config.same_exam_date_policy}: another XML selected within "
                    f"person_year_id={row['person_year_id']} exam_date={row['exam_date']}"
                )
                summary.not_selected_candidates += 1

            if not config.dry_run:
                cur.execute(
                    """
                    INSERT INTO fund_delivery_xml_candidates (
                        event_id, person_year_id, hia_download_xml_id, person_xml_event_id,
                        exam_date, exam_month, dl_date, send_seq, candidate_status,
                        selection_policy, selection_reason, not_selected_reason
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s
                    )
                    ON DUPLICATE KEY UPDATE
                        delivery_candidate_id = LAST_INSERT_ID(delivery_candidate_id),
                        event_id = VALUES(event_id),
                        person_year_id = VALUES(person_year_id),
                        person_xml_event_id = VALUES(person_xml_event_id),
                        exam_date = VALUES(exam_date),
                        exam_month = VALUES(exam_month),
                        dl_date = VALUES(dl_date),
                        send_seq = VALUES(send_seq),
                        candidate_status = VALUES(candidate_status),
                        selection_policy = VALUES(selection_policy),
                        selection_reason = VALUES(selection_reason),
                        not_selected_reason = VALUES(not_selected_reason),
                        updated_at = CURRENT_TIMESTAMP(3)
                    """,
                    (
                        config.event_id,
                        row["person_year_id"],
                        row["hia_download_xml_id"],
                        row["person_xml_event_id"],
                        row["exam_date"],
                        row["exam_month"],
                        row["dl_date"],
                        row["send_seq"],
                        status,
                        config.same_exam_date_policy,
                        selection_reason,
                        not_selected_reason,
                    ),
                )
            summary.candidates_upserted += 1

    return summary


def upsert_person_status(cur: Any, config: FundDeliveryListConfig) -> int:
    event_sql, event_params = _event_clause("c", config)
    month_sql, month_params = _exam_month_clause("c", config)
    cur.execute(
        f"""
        INSERT INTO fund_delivery_person_status (
            event_id, person_year_id, current_hia_download_xml_id,
            current_delivery_candidate_id, delivery_tracking_status, tracking_reason
        )
        SELECT
            c.event_id,
            c.person_year_id,
            c.hia_download_xml_id,
            c.delivery_candidate_id,
            'NOT_DELIVERED',
            'candidate selected'
          FROM fund_delivery_xml_candidates c
         WHERE c.candidate_status = 'SELECTED'
           {event_sql}
           {month_sql}
        ON DUPLICATE KEY UPDATE
            event_id = VALUES(event_id),
            current_hia_download_xml_id = VALUES(current_hia_download_xml_id),
            current_delivery_candidate_id = VALUES(current_delivery_candidate_id),
            tracking_reason = VALUES(tracking_reason),
            updated_at = CURRENT_TIMESTAMP(3)
        """,
        [*event_params, *month_params],
    )
    return int(cur.rowcount)


def create_delivery_list(cur: Any, config: FundDeliveryListConfig) -> int:
    list_status = "READY" if not config.dry_run else "DRAFT"
    search_note = (
        f"output_mode={config.output_mode}; exam_month={config.exam_month}; "
        f"delivery_policy={config.delivery_policy}; same_exam_date_policy={config.same_exam_date_policy}; "
        f"grouping_mode={config.grouping_mode}; sender_code={config.sender_code}"
    )
    cur.execute(
        """
        INSERT INTO fund_delivery_lists (
            event_id, insurer_number, list_name, list_status, output_mode, exam_month,
            grouping_mode, sender_code, sender_name, delivery_policy, same_exam_date_policy,
            include_delivery_status, search_condition_note, created_by
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s
        )
        """,
        (
            config.event_id,
            config.insurer_number,
            config.list_name,
            list_status,
            config.output_mode,
            config.exam_month,
            config.grouping_mode,
            config.sender_code,
            config.sender_name,
            config.delivery_policy,
            config.same_exam_date_policy,
            _include_delivery_status_text(config.delivery_policy),
            search_note,
            config.created_by,
        ),
    )
    return int(cur.lastrowid)


def _include_delivery_status_text(delivery_policy: str) -> str:
    if delivery_policy == "NOT_DELIVERED_ONLY":
        return "NOT_DELIVERED"
    if delivery_policy == "REDELIVERY_ONLY":
        return "REDELIVERY_NEEDED"
    if delivery_policy == "NOT_DELIVERED_AND_REDELIVERY":
        return "NOT_DELIVERED,REDELIVERY_NEEDED"
    return "ALL"


def _delivery_policy_predicate(config: FundDeliveryListConfig) -> str:
    if config.delivery_policy == "NOT_DELIVERED_ONLY":
        return "ps.delivery_tracking_status = 'NOT_DELIVERED'"
    if config.delivery_policy == "REDELIVERY_ONLY":
        return "ps.delivery_tracking_status = 'REDELIVERY_NEEDED'"
    if config.delivery_policy == "NOT_DELIVERED_AND_REDELIVERY":
        return "ps.delivery_tracking_status IN ('NOT_DELIVERED', 'REDELIVERY_NEEDED')"
    return "1 = 1"


def count_selected_candidates_for_policy(cur: Any, config: FundDeliveryListConfig) -> tuple[int, int]:
    event_sql, event_params = _event_clause("c", config)
    month_sql, month_params = _exam_month_clause("c", config)
    predicate = _delivery_policy_predicate(config)
    cur.execute(
        f"""
        SELECT
            COUNT(*) AS selected_count,
            SUM(CASE WHEN {predicate} THEN 1 ELSE 0 END) AS included_count
         FROM fund_delivery_xml_candidates c
          JOIN fund_delivery_person_status ps
            ON ps.person_year_id = c.person_year_id
         WHERE c.candidate_status = 'SELECTED'
           {event_sql}
           {month_sql}
        """,
        [*event_params, *month_params],
    )
    row = cur.fetchone()
    selected_count = int(row["selected_count"] or 0) if row else 0
    included_count = int(row["included_count"] or 0) if row else 0
    return selected_count, included_count


def insert_list_members(cur: Any, config: FundDeliveryListConfig, *, delivery_list_id: int) -> tuple[int, int, int]:
    event_sql, event_params = _event_clause("c", config)
    month_sql, month_params = _exam_month_clause("c", config)
    predicate = _delivery_policy_predicate(config)
    cur.execute(
        f"""
        INSERT INTO fund_delivery_list_members (
            delivery_list_id, person_year_id, delivery_candidate_id, hia_download_xml_id,
            list_member_status, list_member_reason, added_by
        )
        SELECT
            %s,
            c.person_year_id,
            c.delivery_candidate_id,
            c.hia_download_xml_id,
            'INCLUDED',
            CONCAT('delivery_policy=', %s, '; candidate_status=SELECTED'),
            %s
         FROM fund_delivery_xml_candidates c
          JOIN fund_delivery_person_status ps
            ON ps.person_year_id = c.person_year_id
         WHERE c.candidate_status = 'SELECTED'
           AND {predicate}
           {event_sql}
           {month_sql}
        ON DUPLICATE KEY UPDATE
            delivery_candidate_id = VALUES(delivery_candidate_id),
            hia_download_xml_id = VALUES(hia_download_xml_id),
            list_member_status = VALUES(list_member_status),
            list_member_reason = VALUES(list_member_reason),
            added_by = VALUES(added_by),
            updated_at = CURRENT_TIMESTAMP(3)
        """,
        [delivery_list_id, config.delivery_policy, config.created_by, *event_params, *month_params],
    )
    affected = int(cur.rowcount)
    cur.execute(
        """
        SELECT COUNT(*) AS cnt
          FROM fund_delivery_list_members
         WHERE delivery_list_id = %s
        """,
        (delivery_list_id,),
    )
    row = cur.fetchone()
    members_seen = int(row["cnt"] or 0) if row else 0
    return affected, members_seen, 0


def build_fund_delivery_list(cur: Any, config: FundDeliveryListConfig) -> FundDeliveryListSummary:
    validate_config(config)
    summary = upsert_candidates(cur, config)

    if config.dry_run:
        selected_count = summary.selected_candidates
        summary.skipped_by_delivery_policy = 0
        summary.list_members_seen = selected_count
        return summary

    summary.person_status_upserted = upsert_person_status(cur, config)
    selected_count, included_count = count_selected_candidates_for_policy(cur, config)
    summary.skipped_by_delivery_policy = max(selected_count - included_count, 0)
    summary.list_id = create_delivery_list(cur, config)
    summary.list_created = 1
    affected, members_seen, updated = insert_list_members(cur, config, delivery_list_id=summary.list_id)
    summary.list_members_inserted = affected
    summary.list_members_updated = updated
    summary.list_members_seen = members_seen
    return summary
