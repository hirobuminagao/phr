"""Lookup helpers for phr_master postal code addresses."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Any

from scripts.lib.db.schemas import PHR_MASTER


POSTAL_ADDRESS_COLUMNS = """
    postal_code_address_id,
    jis_code,
    old_postal_code,
    postal_code,
    postal_code_formatted,
    prefecture_kana,
    city_kana,
    town_area_kana,
    prefecture,
    city,
    town_area_raw,
    town_area_normalized,
    address_for_xml,
    is_multi_postal_town,
    has_koaza_numbering,
    has_chome,
    is_multi_town_postal,
    update_flag,
    change_reason_code,
    normalization_note,
    data_source_name,
    data_source_file_name,
    data_source_file_sha256,
    source_file_updated_at
"""


@dataclass(frozen=True)
class PostalAddressLookupResult:
    ok: bool
    postal_code: str | None
    postal_code_formatted: str | None
    candidate_count: int
    selected_address_for_xml: str | None
    selected_candidate: dict[str, Any] | None
    candidates: list[dict[str, Any]]
    reason: str


def qname(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", name):
        raise ValueError(f"invalid SQL identifier: {name!r}")
    return f"`{name}`"


def normalize_postal_code_for_lookup(value: Any) -> str | None:
    """Return 7 digit postal code, accepting hyphenated values."""

    if value is None:
        return None
    text = unicodedata.normalize("NFKC", str(value))
    digits = re.sub(r"\D", "", text)
    if len(digits) != 7:
        return None
    return digits


def format_postal_code(postal_code: str | None) -> str | None:
    if not postal_code:
        return None
    return f"{postal_code[:3]}-{postal_code[3:]}"


def _to_candidate(row: Any) -> dict[str, Any]:
    candidate = dict(row)
    candidate["postal_code_address_id"] = int(candidate["postal_code_address_id"])
    for key in (
        "is_multi_postal_town",
        "has_koaza_numbering",
        "has_chome",
        "is_multi_town_postal",
    ):
        candidate[key] = int(candidate.get(key) or 0)
    return candidate


def _failure(postal_code: str | None, reason: str) -> PostalAddressLookupResult:
    return PostalAddressLookupResult(
        ok=False,
        postal_code=postal_code,
        postal_code_formatted=format_postal_code(postal_code),
        candidate_count=0,
        selected_address_for_xml=None,
        selected_candidate=None,
        candidates=[],
        reason=reason,
    )


def _select_candidate(candidates: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, str, bool]:
    if not candidates:
        return None, "NOT_FOUND", False
    if len(candidates) == 1:
        return candidates[0], "SINGLE_CANDIDATE", True

    no_town_candidates = [
        row
        for row in candidates
        if str(row.get("town_area_normalized") or "") == ""
    ]
    if len(no_town_candidates) == 1:
        return no_town_candidates[0], "MULTIPLE_CANDIDATES_SELECTED_CITY_LEVEL", True

    city_level_addresses = {
        f"{row.get('prefecture') or ''}{row.get('city') or ''}"
        for row in candidates
        if row.get("prefecture") and row.get("city")
    }
    if len(city_level_addresses) == 1:
        selected = dict(candidates[0])
        selected["address_for_xml"] = next(iter(city_level_addresses))
        selected["town_area_normalized"] = ""
        selected["normalization_note"] = "複数候補のため市区町村までを代表補完住所として返却。"
        return selected, "MULTIPLE_CANDIDATES_CITY_LEVEL_FALLBACK", True

    return None, "MULTIPLE_CANDIDATES_REVIEW_REQUIRED", False


def lookup_postal_code_address(
    cur: Any,
    postal_code: Any,
    *,
    master_db: str = PHR_MASTER,
    limit: int = 200,
) -> PostalAddressLookupResult:
    """Return postal address candidates and a selected address when safe enough.

    The lookup layer does not decide whether fallback addresses may be used for
    a given business process. Callers should inspect `reason` and
    `candidate_count` before adopting `selected_address_for_xml`.
    """

    normalized = normalize_postal_code_for_lookup(postal_code)
    if normalized is None:
        return _failure(None, "INVALID_POSTAL_CODE")

    cur.execute(
        f"""
        SELECT
            {POSTAL_ADDRESS_COLUMNS}
        FROM {qname(master_db)}.`postal_code_addresses`
        WHERE `postal_code` = %s
          AND `is_active` = 1
        ORDER BY
            CASE WHEN `town_area_normalized` = '' THEN 0 ELSE 1 END,
            `postal_code_address_id`
        LIMIT %s
        """,
        (normalized, int(limit)),
    )
    candidates = [_to_candidate(row) for row in (cur.fetchall() or [])]
    selected, reason, ok = _select_candidate(candidates)
    if selected is None:
        return PostalAddressLookupResult(
            ok=False,
            postal_code=normalized,
            postal_code_formatted=format_postal_code(normalized),
            candidate_count=len(candidates),
            selected_address_for_xml=None,
            selected_candidate=None,
            candidates=candidates,
            reason=reason,
        )

    return PostalAddressLookupResult(
        ok=ok,
        postal_code=normalized,
        postal_code_formatted=format_postal_code(normalized),
        candidate_count=len(candidates),
        selected_address_for_xml=str(selected.get("address_for_xml") or "") or None,
        selected_candidate=selected,
        candidates=candidates,
        reason=reason,
    )


def lookup_postal_code_address_for_xml(
    cur: Any,
    postal_code: Any,
    *,
    master_db: str = PHR_MASTER,
) -> tuple[str | None, PostalAddressLookupResult]:
    """Convenience wrapper returning `(address_for_xml, lookup_result)`."""

    result = lookup_postal_code_address(cur, postal_code, master_db=master_db)
    return result.selected_address_for_xml, result
