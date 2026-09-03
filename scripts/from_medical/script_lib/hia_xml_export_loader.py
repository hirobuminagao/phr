from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from scripts.lib.examination.lookup import qname
from scripts.lib.examination.mhlw_v08_xml import ExamItem


@dataclass(frozen=True)
class ExportSelectors:
    event_id: int
    xml_export_list_id: int | None = None
    facility_ids: tuple[int, ...] = ()
    facility_codes: tuple[str, ...] = ()
    file_receipt_ids: tuple[int, ...] = ()
    ledger_ids: tuple[int, ...] = ()
    subscriber_ids: tuple[int, ...] = ()
    hia_subscriber_ids: tuple[str, ...] = ()
    person_id_customs: tuple[str, ...] = ()
    exam_month: str | None = None
    include_exported: bool = False
    limit: int = 0


@dataclass(frozen=True)
class CandidateDecision:
    allowed: bool
    reason: str | None = None


def check_reason_is_missing_only(reason: Any) -> bool:
    text = str(reason or "").strip()
    if not text:
        return False
    statuses = re.findall(r":([A-Z_]+)(?:\s*\||$)", text)
    return bool(statuses) and set(statuses) == {"MISSING"}


def decide_candidate(row: Mapping[str, Any]) -> CandidateDecision:
    if row.get("export_readiness_status") not in ("EXPORT_READY", "APPROVED_WITH_REASON", "EXPORTED"):
        return CandidateDecision(False, row.get("export_readiness_reason") or "NOT_EXPORT_READY")
    required = (
        ("health_exam_report_category", "REPORT_CATEGORY_MISSING"),
        ("program_code", "PROGRAM_CODE_MISSING"),
        ("exam_date", "EXAM_DATE_MISSING"),
        ("exam_facility_id", "EXAM_FACILITY_MISSING"),
        ("subscriber_id", "SUBSCRIBER_ID_MISSING"),
    )
    for field, reason in required:
        if row.get(field) in (None, ""):
            return CandidateDecision(False, reason)
    if row.get("basic_info_status") == "NG":
        return CandidateDecision(False, row.get("basic_info_reason") or "BASIC_INFO_NG")
    if row.get("address") in (None, "") and row.get("address_completed_value") in (None, ""):
        return CandidateDecision(False, "ADDRESS_MISSING")
    if row.get("subscriber_match_status") != "MATCHED":
        return CandidateDecision(False, "SUBSCRIBER_NOT_MATCHED")
    if row.get("check_status") == "OK":
        return CandidateDecision(True)
    if row.get("check_status") != "NG" or not check_reason_is_missing_only(row.get("check_reason")):
        return CandidateDecision(False, "CHECK_NOT_EXPORTABLE")
    if not bool(row.get("manual_export_approved")):
        return CandidateDecision(False, "MANUAL_APPROVAL_REQUIRED")
    if not row.get("manual_export_reason") or not row.get("manual_export_approved_at") or not row.get("manual_export_approved_by"):
        return CandidateDecision(False, "MANUAL_APPROVAL_INCOMPLETE")
    return CandidateDecision(True)


def candidate_duplicate_key(row: Mapping[str, Any]) -> tuple[str, str, str, str] | None:
    decision = decide_candidate(row)
    if not decision.allowed:
        return None
    required = (
        row.get("subscriber_id"),
        row.get("exam_date"),
        row.get("exam_facility_id"),
        row.get("insurer_number"),
    )
    if any(value in (None, "") for value in required):
        return None
    insurer_number = re.sub(r"\D", "", str(row.get("insurer_number") or ""))
    if insurer_number:
        insurer_number = insurer_number.zfill(8)
    return (
        str(row.get("subscriber_id")),
        str(row.get("exam_date")),
        str(row.get("exam_facility_id")),
        insurer_number,
    )


def detect_unresolved_duplicates(rows: Iterable[Mapping[str, Any]]) -> set[int]:
    grouped: dict[tuple[str, str, str, str], list[int]] = {}
    for row in rows:
        key = candidate_duplicate_key(row)
        row_id = row.get("csv_row_ledger_id") or row.get("exam_ledger_id") or row.get("exam_export_case_id")
        if key is None or row_id in (None, ""):
            continue
        grouped.setdefault(key, []).append(int(row_id))
    return {row_id for row_ids in grouped.values() if len(row_ids) > 1 for row_id in row_ids}


def _in_clause(values: Iterable[Any], params: list[Any]) -> str:
    data = tuple(values)
    params.extend(data)
    return ", ".join(["%s"] * len(data))


def fetch_candidates(
    cur: Any,
    *,
    selectors: ExportSelectors,
    health_db: str,
    dev_db: str = "dev_phr",
    master_db: str,
) -> list[dict[str, Any]]:
    params: list[Any] = [selectors.event_id]
    filters = ["eec.event_id = %s"]
    if selectors.xml_export_list_id is not None:
        list_case_statuses = "'SELECTED', 'READY', 'EXPORT_ERROR'"
        filters.append(
            "EXISTS ("
            f"SELECT 1 FROM {qname(health_db)}.ops_xml_export_list_cases xelc "
            "WHERE xelc.exam_export_case_id = eec.exam_export_case_id "
            "AND xelc.xml_export_list_id = %s "
            f"AND xelc.list_case_status IN ({list_case_statuses}) "
            "AND xelc.removed_at IS NULL"
            ")"
        )
        params.append(selectors.xml_export_list_id)
    if selectors.facility_ids:
        filters.append(f"eec.exam_facility_id IN ({_in_clause(selectors.facility_ids, params)})")
    if selectors.facility_codes:
        filters.append(f"ef.exam_facility_code IN ({_in_clause(selectors.facility_codes, params)})")
    if selectors.file_receipt_ids:
        filters.append(
            "EXISTS ("
            f"SELECT 1 FROM {qname(health_db)}.exam_export_case_sources src_filter "
            "WHERE src_filter.exam_export_case_id = eec.exam_export_case_id "
            f"AND src_filter.file_receipt_id IN ({_in_clause(selectors.file_receipt_ids, params)})"
            ")"
        )
    if selectors.ledger_ids:
        filters.append(f"eec.exam_export_case_id IN ({_in_clause(selectors.ledger_ids, params)})")
    if selectors.subscriber_ids:
        filters.append(f"eec.subscriber_id IN ({_in_clause(selectors.subscriber_ids, params)})")
    if selectors.hia_subscriber_ids:
        filters.append(f"eec.hia_subscriber_id IN ({_in_clause(selectors.hia_subscriber_ids, params)})")
    if selectors.person_id_customs:
        filters.append(f"eec.person_id_custom IN ({_in_clause(selectors.person_id_customs, params)})")
    if selectors.exam_month:
        filters.append("DATE_FORMAT(COALESCE(eec.exam_date_export_value, eec.exam_date), '%Y-%m') = %s")
        params.append(selectors.exam_month)
    if not selectors.include_exported:
        filters.append(
            f"NOT EXISTS (SELECT 1 FROM {qname(health_db)}.xml_export_members xem "
            "WHERE xem.ledger_type = 'CASE' AND xem.ledger_id = eec.exam_export_case_id)"
        )
        filters.append("eec.xml_export_status <> 'EXPORTED'")
    if selectors.include_exported:
        filters.append("eec.export_readiness_status IN ('EXPORT_READY', 'APPROVED_WITH_REASON', 'EXPORTED')")
    else:
        filters.append("eec.export_readiness_status IN ('EXPORT_READY', 'APPROVED_WITH_REASON')")
    filters.append("eec.case_lifecycle_status = 'ACTIVE'")
    limit_sql = ""
    if selectors.limit:
        limit_sql = "LIMIT %s"
        params.append(selectors.limit)

    cur.execute(
        f"""
        SELECT
          eec.*,
          src.file_receipt_id,
          src.relative_path,
          src.file_name AS source_file_name,
          ef.exam_facility_code AS master_facility_code,
          ef.exam_facility_name AS master_facility_name,
          COALESCE(eec.exam_facility_postal_code, ef.postal_code) AS master_facility_postal_code,
          COALESCE(eec.exam_facility_address, ef.address) AS master_facility_address,
          COALESCE(eec.exam_facility_phone_number, ef.phone_number) AS master_facility_phone_number,
          sa.postal_code AS subscriber_postal_code,
          sa.address_line AS subscriber_address_line
        FROM {qname(health_db)}.exam_export_cases eec
        LEFT JOIN (
          SELECT
            ecs.exam_export_case_id,
            MIN(ecs.file_receipt_id) AS file_receipt_id,
            SUBSTRING_INDEX(
              GROUP_CONCAT(fr.relative_path ORDER BY ecs.source_priority, ecs.exam_export_case_source_id SEPARATOR '\\n'),
              '\\n',
              1
            ) AS relative_path,
            SUBSTRING_INDEX(
              GROUP_CONCAT(fr.file_name ORDER BY ecs.source_priority, ecs.exam_export_case_source_id SEPARATOR '\\n'),
              '\\n',
              1
            ) AS file_name
          FROM {qname(health_db)}.exam_export_case_sources ecs
          LEFT JOIN {qname(health_db)}.file_receipts fr ON fr.id = ecs.file_receipt_id
          WHERE ecs.source_status = 'ACTIVE'
          GROUP BY ecs.exam_export_case_id
        ) src ON src.exam_export_case_id = eec.exam_export_case_id
        INNER JOIN {qname(master_db)}.exam_facilities ef ON ef.exam_facility_id = eec.exam_facility_id
        LEFT JOIN {qname(dev_db)}.subscriber_addresses sa
          ON sa.subscriber_id = eec.subscriber_id
         AND sa.is_current = 1
        WHERE {' AND '.join(filters)}
        ORDER BY eec.exam_facility_id, eec.insurer_number, eec.exam_date, eec.exam_export_case_id
        {limit_sql}
        """,
        tuple(params),
    )
    rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        if row.get("postal_code") in (None, "") and row.get("postal_code_completed_value") in (None, ""):
            row["postal_code_completed_value"] = row.get("subscriber_postal_code")
        if row.get("address") in (None, "") and row.get("address_completed_value") in (None, ""):
            row["address_completed_value"] = row.get("subscriber_address_line")
            if row.get("address_completed_value") not in (None, ""):
                row["address_source"] = row.get("address_source") or "SUBSCRIBER"
                row["address_completion_status"] = row.get("address_completion_status") or "SUBSCRIBER"
                row["address_completion_reason"] = row.get("address_completion_reason") or "subscriber address fallback"
    return rows


def facility_folder_name(relative_path: Any) -> str:
    text = str(relative_path or "").replace("\\", "/").strip("/")
    if not text:
        raise ValueError("FACILITY_FOLDER_NOT_FOUND")
    return text.split("/", 1)[0]


def fetch_valid_items(cur: Any, *, ledger_id: int, health_db: str, dev_db: str, master_db: str) -> list[ExamItem]:
    cur.execute(
        f"""
        SELECT
          ecv.namecode,
          COALESCE(NULLIF(em.cda_section_code_default, ''), '01990') AS section_code,
          COALESCE(em.xml_value_type, 'ST') AS value_type,
          CASE
            WHEN COALESCE(em.xml_value_type, 'ST') IN ('CD', 'CO') THEN NULL
            ELSE ecv.normalized_value
          END AS normalized_value,
          CASE
            WHEN COALESCE(em.xml_value_type, 'ST') = 'PQ'
              THEN COALESCE(NULLIF(em.ucum_unit, ''), NULLIF(ecv.normalized_unit, ''), NULLIF(em.display_unit, ''))
            ELSE ecv.normalized_unit
          END AS normalized_unit,
          ecv.nullflavor,
          NULLIF(em.result_code_oid, '') AS code_system,
          ecv.code_value,
          ecv.code_display,
          ecv.interpretation_code,
          NULL AS interpretation_code_system,
          ecv.interpretation_name,
          em.item_name AS display_name,
          em.xml_method_code AS method_code,
          NULLIF(ecv.source_reference_lower, '') AS source_reference_lower,
          NULLIF(ecv.source_reference_upper, '') AS source_reference_upper,
          em.annex2_series_group_identifier AS series_group_identifier,
          em.annex2_series_group_relation_code AS series_group_relation_code,
          em.annex2_author_item_code AS author_item_code,
          EXISTS (
            SELECT 1
            FROM {qname(dev_db)}.exam_item_master author_parent
            WHERE CONVERT(author_parent.annex2_author_item_code USING utf8mb4) COLLATE utf8mb4_unicode_ci
                = CONVERT(ecv.namecode USING utf8mb4) COLLATE utf8mb4_unicode_ci
          ) AS is_author_item,
          ecv.negation_ind,
          ecv.occurrence_no,
          em.jun_no AS jun_no
        FROM {qname(health_db)}.exam_export_case_values ecv
        INNER JOIN {qname(health_db)}.exam_export_cases eec
          ON eec.exam_export_case_id = ecv.exam_export_case_id
        LEFT JOIN {qname(dev_db)}.exam_item_master em
          ON CONVERT(em.namecode USING utf8mb4) COLLATE utf8mb4_unicode_ci
           = CONVERT(ecv.namecode USING utf8mb4) COLLATE utf8mb4_unicode_ci
        LEFT JOIN {qname(master_db)}.exam_item_output_policies AS fpolicy
          ON fpolicy.exam_facility_id = eec.exam_facility_id
         AND CONVERT(fpolicy.namecode USING utf8mb4) COLLATE utf8mb4_unicode_ci
           = CONVERT(ecv.namecode USING utf8mb4) COLLATE utf8mb4_unicode_ci
         AND fpolicy.is_active = 1
        LEFT JOIN {qname(master_db)}.exam_item_output_policies AS gpolicy
          ON gpolicy.exam_facility_id = 0
         AND CONVERT(gpolicy.namecode USING utf8mb4) COLLATE utf8mb4_unicode_ci
           = CONVERT(ecv.namecode USING utf8mb4) COLLATE utf8mb4_unicode_ci
         AND gpolicy.is_active = 1
        WHERE ecv.exam_export_case_id = %s
          AND ecv.namecode IS NOT NULL
          AND COALESCE(fpolicy.output_policy, gpolicy.output_policy, 'INCLUDE') = 'INCLUDE'
        ORDER BY COALESCE(em.jun_no, 999999), ecv.exam_export_case_value_id
        """,
        (ledger_id,),
    )
    return [
        ExamItem(
            namecode=str(row["namecode"]),
            section_code=str(row["section_code"]),
            value_type=str(row["value_type"]),
            normalized_value=None if row["normalized_value"] is None else str(row["normalized_value"]),
            normalized_unit=None if row["normalized_unit"] is None else str(row["normalized_unit"]),
            nullflavor=None if row["nullflavor"] is None else str(row["nullflavor"]),
            code_system=None if row["code_system"] is None else str(row["code_system"]),
            code_value=None if row["code_value"] is None else str(row["code_value"]),
            code_display=None if row["code_display"] is None else str(row["code_display"]),
            display_name=None if row["display_name"] is None else str(row["display_name"]),
            method_code=None if row["method_code"] is None else str(row["method_code"]),
            interpretation_code=None if row["interpretation_code"] is None else str(row["interpretation_code"]),
            interpretation_code_system=None
            if row["interpretation_code_system"] is None
            else str(row["interpretation_code_system"]),
            interpretation_name=None if row["interpretation_name"] is None else str(row["interpretation_name"]),
            source_reference_lower=None
            if row["source_reference_lower"] is None
            else str(row["source_reference_lower"]),
            source_reference_upper=None
            if row["source_reference_upper"] is None
            else str(row["source_reference_upper"]),
            series_group_identifier=None
            if row["series_group_identifier"] is None
            else str(row["series_group_identifier"]),
            series_group_relation_code=None
            if row["series_group_relation_code"] is None
            else str(row["series_group_relation_code"]),
            author_item_code=None
            if row.get("author_item_code") is None
            else str(row["author_item_code"]),
            is_author_item=bool(row.get("is_author_item")),
            negation_ind=None if row["negation_ind"] is None else bool(row["negation_ind"]),
            occurrence_no=int(row["occurrence_no"] or 1),
            jun_no=None if row["jun_no"] is None else int(row["jun_no"]),
        )
        for row in cur.fetchall()
    ]
