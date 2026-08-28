"""Shared CSV exam result format matching helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from scripts.lib.csv.csv_loader import CsvLoadResult, load_csv_result
from scripts.lib.db.lookup.csv_exam_result_mapping import find_csv_format_versions_by_header
from scripts.lib.db.lookup.csv_exam_result_mapping import get_csv_format_version_by_id
from scripts.lib.db.schemas import PHR_MASTER


@dataclass(frozen=True)
class CsvFormatMatchResult:
    result: str
    actual_header_sha256: str | None
    csv_format_version_id: int | None
    message: str
    mapping_version: str | None = None
    actual_character_encoding: str | None = None


def compact_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def qname(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def load_csv_for_format(
    path: str,
    fmt: dict[str, Any],
    *,
    encoding: str | None = None,
) -> CsvLoadResult:
    header_count = max(int(fmt.get("data_start_row_no") or 2) - 1, 0)
    return load_csv_result(
        path,
        header_count=header_count,
        delimiter=str(fmt.get("delimiter") or ","),
        encoding=encoding or compact_text(fmt.get("character_encoding")),
        quote_char=str(fmt.get("quote_char") or '"'),
        active_header_row_no=(
            int(fmt["active_header_row_no"]) if fmt.get("active_header_row_no") is not None else None
        ),
        data_start_row_no=int(fmt.get("data_start_row_no") or header_count + 1),
    )


def encoding_candidates(fmt: dict[str, Any]) -> list[str]:
    registered = compact_text(fmt.get("character_encoding")) or "CP932"
    policy = compact_text(fmt.get("encoding_fallback_policy")) or "STRICT"
    candidates = [registered]
    if policy == "ALLOW_COMMON_ENCODINGS":
        candidates.extend(["utf-8-sig", "utf-8", "cp932"])

    result: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = candidate.lower().replace("_", "-")
        if key not in seen:
            seen.add(key)
            result.append(candidate)
    return result


def load_csv_matching_registered_header(
    path: str,
    fmt: dict[str, Any],
) -> tuple[CsvLoadResult | None, str | None]:
    last_header_sha256: str | None = None
    for encoding in encoding_candidates(fmt):
        try:
            csv_result = load_csv_for_format(path, fmt, encoding=encoding)
        except UnicodeError:
            continue
        last_header_sha256 = csv_result.header_set.header_sha256
        if last_header_sha256 == fmt.get("header_sha256"):
            return csv_result, last_header_sha256
    return None, last_header_sha256


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
    preferred_csv_format_version_id: int | None = None,
    master_db: str = PHR_MASTER,
) -> CsvFormatMatchResult:
    if exam_facility_id is None:
        return CsvFormatMatchResult(
            result="ERROR",
            actual_header_sha256=None,
            csv_format_version_id=None,
            message="CSV format match failed: exam_facility_id is not set.",
        )

    if preferred_csv_format_version_id is not None:
        preferred = get_csv_format_version_by_id(
            cur,
            int(preferred_csv_format_version_id),
            master_db=master_db,
        )
        if preferred is not None and int(preferred.get("exam_facility_id") or 0) == int(exam_facility_id):
            csv_result, actual_header_sha256 = load_csv_matching_registered_header(source_path, preferred)
            if csv_result is not None:
                return CsvFormatMatchResult(
                    result="MATCHED",
                    actual_header_sha256=actual_header_sha256,
                    csv_format_version_id=int(preferred["csv_format_version_id"]),
                    message=f"CSV format matched by alias setting: {preferred.get('mapping_version')}",
                    mapping_version=compact_text(preferred.get("mapping_version")),
                    actual_character_encoding=csv_result.encoding,
                )
        elif preferred is not None:
            return CsvFormatMatchResult(
                result="ERROR",
                actual_header_sha256=None,
                csv_format_version_id=None,
                message="CSV format match failed: alias template facility does not match receipt facility.",
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
        csv_result, candidate_header_sha256 = load_csv_matching_registered_header(source_path, candidate)
        if candidate_header_sha256 is not None:
            actual_header_sha256 = candidate_header_sha256
        if csv_result is None:
            continue
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
                actual_character_encoding=csv_result.encoding,
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
                actual_character_encoding=csv_result.encoding,
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
