from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from scripts.lib.examination.lookup import qname
from scripts.lib.examination.mhlw_v08_xml import ExamItem
from scripts.lib.identity.base_norm import base_normalize
from scripts.lib.identity.primitive.digits import extract_digits, zero_pad


@dataclass(frozen=True)
class ExportSelectors:
    event_id: int
    facility_ids: tuple[int, ...] = ()
    file_receipt_ids: tuple[int, ...] = ()
    ledger_ids: tuple[int, ...] = ()
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
    filters = ["crl.event_id = %s"]
    if selectors.facility_ids:
        filters.append(f"crl.exam_facility_id IN ({_in_clause(selectors.facility_ids, params)})")
    if selectors.file_receipt_ids:
        filters.append(f"crl.file_receipt_id IN ({_in_clause(selectors.file_receipt_ids, params)})")
    if selectors.ledger_ids:
        filters.append(f"crl.csv_row_ledger_id IN ({_in_clause(selectors.ledger_ids, params)})")
    if selectors.exam_month:
        filters.append("DATE_FORMAT(crl.exam_date, '%Y-%m') = %s")
        params.append(selectors.exam_month)
    if not selectors.include_exported:
        filters.append(
            f"NOT EXISTS (SELECT 1 FROM {qname(health_db)}.xml_export_members xem "
            "WHERE xem.ledger_type = 'CSV' AND xem.ledger_id = crl.csv_row_ledger_id)"
        )
    limit_sql = ""
    if selectors.limit:
        limit_sql = "LIMIT %s"
        params.append(selectors.limit)

    cur.execute(
        f"""
        SELECT
          crl.*,
          fr.relative_path,
          fr.file_name AS source_file_name,
          ef.exam_facility_code AS master_facility_code,
          ef.exam_facility_name AS master_facility_name,
          ef.postal_code AS master_facility_postal_code,
          ef.address AS master_facility_address,
          ef.phone_number AS master_facility_phone_number
        FROM {qname(health_db)}.csv_row_ledger crl
        INNER JOIN {qname(health_db)}.file_receipts fr ON fr.id = crl.file_receipt_id
        INNER JOIN {qname(master_db)}.exam_facilities ef ON ef.exam_facility_id = crl.exam_facility_id
        WHERE {' AND '.join(filters)}
        ORDER BY crl.exam_facility_id, crl.insurer_number, crl.exam_date, crl.csv_row_ledger_id
        {limit_sql}
        """,
        tuple(params),
    )
    return [dict(row) for row in cur.fetchall()]


def detect_unresolved_duplicates(rows: Iterable[Mapping[str, Any]]) -> set[int]:
    rows = list(rows)

    def canonical_insurer(value: Any) -> str:
        digits = extract_digits(base_normalize(None if value is None else str(value)))
        return zero_pad(digits, 8) or ""

    rows_and_keys = [
        (row, (
            row.get("event_id"),
            row.get("exam_facility_id"),
            canonical_insurer(row.get("insurer_number")),
            row.get("subscriber_id"),
            row.get("exam_date"),
        ))
        for row in rows
        if row.get("event_id") is not None
        and row.get("exam_facility_id") is not None
        and canonical_insurer(row.get("insurer_number"))
        and row.get("subscriber_id") is not None
        and row.get("exam_date") is not None
    ]
    keys = [key for _, key in rows_and_keys]
    counts = Counter(keys)
    return {
        int(row["csv_row_ledger_id"])
        for row, key in rows_and_keys
        if counts[key] > 1
    }


def facility_folder_name(relative_path: Any) -> str:
    text = str(relative_path or "").replace("\\", "/").strip("/")
    if not text:
        raise ValueError("FACILITY_FOLDER_NOT_FOUND")
    return text.split("/", 1)[0]


def fetch_valid_items(cur: Any, *, ledger_id: int, health_db: str, dev_db: str) -> list[ExamItem]:
    cur.execute(
        f"""
        SELECT
          eiv.namecode,
          COALESCE(NULLIF(eiv.section_code, ''), NULLIF(em.cda_section_code_default, ''), '01990') AS section_code,
          COALESCE(NULLIF(eiv.raw_value_type, ''), em.xml_value_type, 'ST') AS value_type,
          eiv.normalized_value,
          COALESCE(NULLIF(eiv.normalized_unit, ''), NULLIF(em.ucum_unit, ''), NULLIF(em.display_unit, '')) AS normalized_unit,
          eiv.nullflavor,
          COALESCE(NULLIF(eiv.code_system, ''), NULLIF(em.result_code_oid, '')) AS code_system,
          eiv.code_value,
          eiv.code_display,
          eiv.interpretation_code,
          eiv.interpretation_code_system,
          eiv.interpretation_name,
          COALESCE(NULLIF(eiv.namecode_display_name, ''), em.item_name) AS display_name,
          em.xml_method_code AS method_code,
          NULLIF(eiv.source_reference_lower, '') AS source_reference_lower,
          NULLIF(eiv.source_reference_upper, '') AS source_reference_upper,
          em.annex2_series_group_identifier AS series_group_identifier,
          em.annex2_series_group_relation_code AS series_group_relation_code,
          eiv.negation_ind,
          eiv.occurrence_no,
          COALESCE(eiv.jun_no, em.jun_no) AS jun_no
        FROM {qname(health_db)}.exam_item_values eiv
        LEFT JOIN {qname(dev_db)}.exam_item_master em ON em.namecode = eiv.namecode
        WHERE eiv.ledger_type = 'CSV'
          AND eiv.ledger_id = %s
          AND eiv.validation_status = 'VALID'
          AND eiv.namecode IS NOT NULL
        ORDER BY COALESCE(eiv.jun_no, em.jun_no), eiv.id
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
