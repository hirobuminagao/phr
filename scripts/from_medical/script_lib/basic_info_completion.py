from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from scripts.lib.db.lookup.postal_code_address import lookup_postal_code_address
from scripts.lib.identity.field.address import normalize_address_export, normalize_postal_code_export
from scripts.lib.identity.field.insurer_number import normalize_insurer_number
from scripts.lib.identity.primitive.digits import zero_pad


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

INSURER_NUMBER_SOURCE_SOURCE = "SOURCE"
INSURER_NUMBER_SOURCE_EVENT = "EVENT"
INSURER_NUMBER_SOURCE_NONE = "NONE"

INSURER_NUMBER_STATUS_NOT_NEEDED = "NOT_NEEDED"
INSURER_NUMBER_STATUS_FILLED_FROM_EVENT = "FILLED_FROM_EVENT"
INSURER_NUMBER_STATUS_CONFLICT = "CONFLICT"
INSURER_NUMBER_STATUS_INVALID = "INVALID"
INSURER_NUMBER_STATUS_MISSING = "MISSING"


@dataclass(frozen=True)
class BasicInfoCompletion:
    basic_info_status: str
    basic_info_reason: str | None
    insurer_number_source: str
    insurer_number_completion_status: str
    insurer_number_completion_reason: str | None
    insurer_number_export_value: str | None
    address_source: str
    address_completion_status: str
    address_completion_reason: str | None
    address_completed_value: str | None
    postal_code_completed_value: str | None

    def as_db_params(self) -> dict[str, Any]:
        return {
            "basic_info_status": self.basic_info_status,
            "basic_info_reason": self.basic_info_reason,
            "insurer_number_source": self.insurer_number_source,
            "insurer_number_completion_status": self.insurer_number_completion_status,
            "insurer_number_completion_reason": self.insurer_number_completion_reason,
            "insurer_number_export_value": self.insurer_number_export_value,
            "address_source": self.address_source,
            "address_completion_status": self.address_completion_status,
            "address_completion_reason": self.address_completion_reason,
            "address_completed_value": self.address_completed_value,
            "postal_code_completed_value": self.postal_code_completed_value,
        }


@dataclass(frozen=True)
class InsurerNumberCompletion:
    status: str
    reason: str | None
    source: str
    export_value: str | None

    @property
    def ok(self) -> bool:
        return self.status in {INSURER_NUMBER_STATUS_NOT_NEEDED, INSURER_NUMBER_STATUS_FILLED_FROM_EVENT}


def _normalize_insurer_number_export(value: Any) -> tuple[str | None, str | None]:
    result = normalize_insurer_number(value)
    if not result.get("ok") or result.get("field_norm") in (None, ""):
        return None, str(result.get("reason") or "INVALID_INSURER_NUMBER")
    normalized = zero_pad(str(result["field_norm"]), 8)
    if normalized is None or len(normalized) != 8:
        return None, "INVALID_INSURER_NUMBER_LENGTH"
    return normalized, None


def _is_all_zero_insurer_number(value: Any) -> bool:
    result = normalize_insurer_number(value)
    return bool(result.get("ok") and result.get("field_norm") == "0")


def resolve_insurer_number_completion(
    *,
    source_value: Any,
    event_value: Any,
) -> InsurerNumberCompletion:
    source_is_all_zero = _is_all_zero_insurer_number(source_value)
    source_number, source_reason = _normalize_insurer_number_export(source_value)
    event_number, event_reason = _normalize_insurer_number_export(event_value)

    if event_number is None:
        return InsurerNumberCompletion(
            status=INSURER_NUMBER_STATUS_MISSING,
            reason=f"EVENT_INSURER_NUMBER_{event_reason or 'MISSING'}",
            source=INSURER_NUMBER_SOURCE_NONE,
            export_value=None,
        )

    if source_value in (None, "") or source_is_all_zero:
        return InsurerNumberCompletion(
            status=INSURER_NUMBER_STATUS_FILLED_FROM_EVENT,
            reason="SOURCE_INSURER_NUMBER_ALL_ZERO" if source_is_all_zero else None,
            source=INSURER_NUMBER_SOURCE_EVENT,
            export_value=event_number,
        )

    if source_number is None:
        return InsurerNumberCompletion(
            status=INSURER_NUMBER_STATUS_INVALID,
            reason=source_reason,
            source=INSURER_NUMBER_SOURCE_SOURCE,
            export_value=None,
        )

    if source_number != event_number:
        return InsurerNumberCompletion(
            status=INSURER_NUMBER_STATUS_CONFLICT,
            reason=f"INSURER_NUMBER_CONFLICT: source={source_number} event={event_number}",
            source=INSURER_NUMBER_SOURCE_SOURCE,
            export_value=source_number,
        )

    return InsurerNumberCompletion(
        status=INSURER_NUMBER_STATUS_NOT_NEEDED,
        reason=None,
        source=INSURER_NUMBER_SOURCE_SOURCE,
        export_value=source_number,
    )


def resolve_basic_info_completion(
    cur: Any,
    *,
    row: Mapping[str, Any],
    event_insurer_number: Any = None,
    master_db: str,
) -> BasicInfoCompletion:
    """Resolve prepared XML-export basic-info completion candidates without changing source values."""
    insurer = resolve_insurer_number_completion(
        source_value=row.get("insurer_number"),
        event_value=event_insurer_number,
    )
    source_address = normalize_address_export(row.get("address"))
    source_postal_code = normalize_postal_code_export(row.get("postal_code"))
    if source_address:
        return BasicInfoCompletion(
            basic_info_status=BASIC_INFO_STATUS_OK if insurer.ok else BASIC_INFO_STATUS_NG,
            basic_info_reason=None if insurer.ok else insurer.reason,
            insurer_number_source=insurer.source,
            insurer_number_completion_status=insurer.status,
            insurer_number_completion_reason=insurer.reason,
            insurer_number_export_value=insurer.export_value,
            address_source=ADDRESS_SOURCE_SOURCE,
            address_completion_status=ADDRESS_COMPLETION_NOT_NEEDED,
            address_completion_reason=None,
            address_completed_value=None,
            postal_code_completed_value=source_postal_code,
        )

    if not row.get("postal_code"):
        return BasicInfoCompletion(
            basic_info_status=BASIC_INFO_STATUS_NG,
            basic_info_reason=insurer.reason or "ADDRESS_AND_POSTAL_CODE_MISSING",
            insurer_number_source=insurer.source,
            insurer_number_completion_status=insurer.status,
            insurer_number_completion_reason=insurer.reason,
            insurer_number_export_value=insurer.export_value,
            address_source=ADDRESS_SOURCE_NONE,
            address_completion_status=ADDRESS_COMPLETION_MISSING,
            address_completion_reason="ADDRESS_AND_POSTAL_CODE_MISSING",
            address_completed_value=None,
            postal_code_completed_value=None,
        )

    result = lookup_postal_code_address(cur, row.get("postal_code"), master_db=master_db)
    if result.ok and result.selected_address_for_xml:
        return BasicInfoCompletion(
            basic_info_status=BASIC_INFO_STATUS_OK if insurer.ok else BASIC_INFO_STATUS_NG,
            basic_info_reason="ADDRESS_COMPLETED_FROM_POSTAL_CODE" if insurer.ok else insurer.reason,
            insurer_number_source=insurer.source,
            insurer_number_completion_status=insurer.status,
            insurer_number_completion_reason=insurer.reason,
            insurer_number_export_value=insurer.export_value,
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
        basic_info_reason=insurer.reason or f"ADDRESS_COMPLETION_{status}",
        insurer_number_source=insurer.source,
        insurer_number_completion_status=insurer.status,
        insurer_number_completion_reason=insurer.reason,
        insurer_number_export_value=insurer.export_value,
        address_source=ADDRESS_SOURCE_NONE,
        address_completion_status=status,
        address_completion_reason=result.reason,
        address_completed_value=None,
        postal_code_completed_value=result.postal_code_formatted,
    )
