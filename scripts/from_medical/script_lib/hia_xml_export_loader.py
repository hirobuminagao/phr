from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from scripts.lib.examination.lookup import qname
from scripts.lib.examination.mhlw_v08_xml import ExamItem


@dataclass(frozen=True)
class ExportSelectors:
    event_id: int
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
    if row.get("postal_code") in (None, "") and row.get("postal_code_completed_value") in (None, ""):
        return CandidateDecision(False, "POSTAL_CODE_MISSING")
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


def _in_clause(values: Iterable[Any], params: list[Any]) -> str:
    data = tuple(values)
    params.extend(data)
    return ", ".join(["%s"] * len(data))


def fetch_candidates(
    cur: Any,
    *,
    selectors: ExportSelectors,
    health_db: str,
    master_db: str,
) -> list[dict[str, Any]]:
    params: list[Any] = [selectors.event_id]
    filters = ["eec.event_id = %s"]
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
        filters.append("DATE_FORMAT(eec.exam_date, '%Y-%m') = %s")
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
          COALESCE(eec.exam_facility_phone_number, ef.phone_number) AS master_facility_phone_number
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
        WHERE {' AND '.join(filters)}
        ORDER BY eec.exam_facility_id, eec.insurer_number, eec.exam_date, eec.exam_export_case_id
        {limit_sql}
        """,
        tuple(params),
    )
    return [dict(row) for row in cur.fetchall()]


def facility_folder_name(relative_path: Any) -> str:
    text = str(relative_path or "").replace("\\", "/").strip("/")
    if not text:
        raise ValueError("FACILITY_FOLDER_NOT_FOUND")
    return text.split("/", 1)[0]


def fetch_valid_items(cur: Any, *, ledger_id: int, health_db: str, dev_db: str) -> list[ExamItem]:
    cur.execute(
        f"""
        SELECT
          ecv.namecode,
          COALESCE(NULLIF(em.cda_section_code_default, ''), '01990') AS section_code,
          COALESCE(em.xml_value_type, 'ST') AS value_type,
          ecv.normalized_value,
          COALESCE(NULLIF(ecv.normalized_unit, ''), NULLIF(em.ucum_unit, ''), NULLIF(em.display_unit, '')) AS normalized_unit,
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
          ecv.negation_ind,
          ecv.occurrence_no,
          em.jun_no AS jun_no
        FROM {qname(health_db)}.exam_export_case_values ecv
        LEFT JOIN {qname(dev_db)}.exam_item_master em ON em.namecode = ecv.namecode
        WHERE ecv.exam_export_case_id = %s
          AND ecv.namecode IS NOT NULL
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
            negation_ind=None if row["negation_ind"] is None else bool(row["negation_ind"]),
            occurrence_no=int(row["occurrence_no"] or 1),
            jun_no=None if row["jun_no"] is None else int(row["jun_no"]),
        )
        for row in cur.fetchall()
    ]
