"""Shared CSV exam result format matching helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from scripts.lib.csv.csv_loader import CsvLoadResult, load_csv_result
from scripts.lib.db.lookup.csv_exam_result_mapping import find_csv_format_versions_by_header
from scripts.lib.db.schemas import PHR_MASTER


@dataclass(frozen=True)
class CsvFormatMatchResult:
    result: str
    actual_header_sha256: str | None
    csv_format_version_id: int | None
    message: str
    mapping_version: str | None = None


def compact_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def qname(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def load_csv_for_format(path: str, fmt: dict[str, Any]) -> CsvLoadResult:
    header_count = max(int(fmt.get("data_start_row_no") or 2) - 1, 0)
    return load_csv_result(
        path,
        header_count=header_count,
        delimiter=str(fmt.get("delimiter") or ","),
        encoding=compact_text(fmt.get("character_encoding")),
        quote_char=str(fmt.get("quote_char") or '"'),
        active_header_row_no=(
            int(fmt["active_header_row_no"]) if fmt.get("active_header_row_no") is not None else None
        ),
        data_start_row_no=int(fmt.get("data_start_row_no") or header_count + 1),
    )


def fetch_active_format_candidates(
    cur: Any,
    *,
    exam_facility_id: int,
    master_db: str = PHR_MASTER,
) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT *
        FROM {qname(master_db)}.csv_format_versions
        WHERE exam_facility_id = %s
          AND is_active = 1
        ORDER BY is_default_for_facility DESC, valid_from DESC, csv_format_version_id DESC
        """,
        (exam_facility_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def match_csv_format_for_file(
    cur: Any,
    *,
    source_path: str,
    exam_facility_id: int | None,
    master_db: str = PHR_MASTER,
) -> CsvFormatMatchResult:
    if exam_facility_id is None:
        return CsvFormatMatchResult(
            result="ERROR",
            actual_header_sha256=None,
            csv_format_version_id=None,
            message="CSV format match failed: exam_facility_id is not set.",
        )

    candidates = fetch_active_format_candidates(cur, exam_facility_id=exam_facility_id, master_db=master_db)
    if not candidates:
        return CsvFormatMatchResult(
            result="NOT_FOUND",
            actual_header_sha256=None,
            csv_format_version_id=None,
            message="CSV format not found: no active format for exam facility.",
        )

    actual_header_sha256: str | None = None
    for candidate in candidates:
        csv_result = load_csv_for_format(source_path, candidate)
        actual_header_sha256 = csv_result.header_set.header_sha256
        matches = find_csv_format_versions_by_header(
            cur,
            exam_facility_id=exam_facility_id,
            header_sha256=actual_header_sha256,
            master_db=master_db,
        )
        if not matches:
            continue
        if len(matches) == 1:
            fmt = matches[0]
            return CsvFormatMatchResult(
                result="MATCHED",
                actual_header_sha256=actual_header_sha256,
                csv_format_version_id=int(fmt["csv_format_version_id"]),
                message=f"CSV format matched: {fmt.get('mapping_version')}",
                mapping_version=compact_text(fmt.get("mapping_version")),
            )
        default_matches = [fmt for fmt in matches if int(fmt.get("is_default_for_facility") or 0) == 1]
        if len(default_matches) == 1:
            fmt = default_matches[0]
            return CsvFormatMatchResult(
                result="MATCHED",
                actual_header_sha256=actual_header_sha256,
                csv_format_version_id=int(fmt["csv_format_version_id"]),
                message=f"CSV format matched by facility default: {fmt.get('mapping_version')}",
                mapping_version=compact_text(fmt.get("mapping_version")),
            )
        versions = ", ".join(str(fmt.get("mapping_version")) for fmt in matches)
        return CsvFormatMatchResult(
            result="MULTIPLE",
            actual_header_sha256=actual_header_sha256,
            csv_format_version_id=None,
            message=f"CSV format ambiguous: {versions}",
        )

    return CsvFormatMatchResult(
        result="NOT_FOUND",
        actual_header_sha256=actual_header_sha256,
        csv_format_version_id=None,
        message="CSV format not found: header did not match registered format.",
    )

