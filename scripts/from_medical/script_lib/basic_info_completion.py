from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from scripts.lib.db.lookup.postal_code_address import lookup_postal_code_address
from scripts.lib.identity.field.address import normalize_address_export, normalize_postal_code_export


BASIC_INFO_STATUS_OK = "OK"
BASIC_INFO_STATUS_WARNING = "WARNING"
BASIC_INFO_STATUS_NG = "NG"

ADDRESS_SOURCE_SOURCE = "SOURCE"
ADDRESS_SOURCE_POSTAL_LOOKUP = "POSTAL_LOOKUP"
ADDRESS_SOURCE_NONE = "NONE"

ADDRESS_COMPLETION_NOT_NEEDED = "NOT_NEEDED"
ADDRESS_COMPLETION_AVAILABLE = "AVAILABLE"
ADDRESS_COMPLETION_NEED_REVIEW = "NEED_REVIEW"
ADDRESS_COMPLETION_NOT_FOUND = "NOT_FOUND"
ADDRESS_COMPLETION_INVALID = "INVALID"
ADDRESS_COMPLETION_MISSING = "MISSING"


@dataclass(frozen=True)
class BasicInfoCompletion:
    basic_info_status: str
    basic_info_reason: str | None
    address_source: str
    address_completion_status: str
    address_completion_reason: str | None
    address_completed_value: str | None
    postal_code_completed_value: str | None

    def as_db_params(self) -> dict[str, Any]:
        return {
            "basic_info_status": self.basic_info_status,
            "basic_info_reason": self.basic_info_reason,
            "address_source": self.address_source,
            "address_completion_status": self.address_completion_status,
            "address_completion_reason": self.address_completion_reason,
            "address_completed_value": self.address_completed_value,
            "postal_code_completed_value": self.postal_code_completed_value,
        }


def resolve_basic_info_completion(
    cur: Any,
    *,
    row: Mapping[str, Any],
    master_db: str,
) -> BasicInfoCompletion:
    """Resolve prepared XML-export basic-info completion candidates without changing source values."""
    source_address = normalize_address_export(row.get("address"))
    source_postal_code = normalize_postal_code_export(row.get("postal_code"))
    if source_address:
        return BasicInfoCompletion(
            basic_info_status=BASIC_INFO_STATUS_OK,
            basic_info_reason=None,
            address_source=ADDRESS_SOURCE_SOURCE,
            address_completion_status=ADDRESS_COMPLETION_NOT_NEEDED,
            address_completion_reason=None,
            address_completed_value=None,
            postal_code_completed_value=source_postal_code,
        )

    if not row.get("postal_code"):
        return BasicInfoCompletion(
            basic_info_status=BASIC_INFO_STATUS_NG,
            basic_info_reason="ADDRESS_AND_POSTAL_CODE_MISSING",
            address_source=ADDRESS_SOURCE_NONE,
            address_completion_status=ADDRESS_COMPLETION_MISSING,
            address_completion_reason="ADDRESS_AND_POSTAL_CODE_MISSING",
            address_completed_value=None,
            postal_code_completed_value=None,
        )

    result = lookup_postal_code_address(cur, row.get("postal_code"), master_db=master_db)
    if result.ok and result.selected_address_for_xml:
        return BasicInfoCompletion(
            basic_info_status=BASIC_INFO_STATUS_OK,
            basic_info_reason="ADDRESS_COMPLETED_FROM_POSTAL_CODE",
            address_source=ADDRESS_SOURCE_POSTAL_LOOKUP,
            address_completion_status=ADDRESS_COMPLETION_AVAILABLE,
            address_completion_reason=result.reason,
            address_completed_value=normalize_address_export(result.selected_address_for_xml),
            postal_code_completed_value=result.postal_code_formatted,
        )

    if result.reason == "INVALID_POSTAL_CODE":
        status = ADDRESS_COMPLETION_INVALID
    elif result.reason == "NOT_FOUND":
        status = ADDRESS_COMPLETION_NOT_FOUND
    else:
        status = ADDRESS_COMPLETION_NEED_REVIEW

    return BasicInfoCompletion(
        basic_info_status=BASIC_INFO_STATUS_NG,
        basic_info_reason=f"ADDRESS_COMPLETION_{status}",
        address_source=ADDRESS_SOURCE_NONE,
        address_completion_status=status,
        address_completion_reason=result.reason,
        address_completed_value=None,
        postal_code_completed_value=result.postal_code_formatted,
    )
