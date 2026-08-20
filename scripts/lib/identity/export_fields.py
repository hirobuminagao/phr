from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from scripts.lib.identity.field.address import normalize_address_export, normalize_postal_code_export
from scripts.lib.identity.field.date_field import normalize_date_to_ymd_and_compact
from scripts.lib.identity.field.gender_code import normalize_gender_code
from scripts.lib.identity.field.insurance_number import normalize_insurance_number
from scripts.lib.identity.field.insurance_symbol import normalize_insurance_symbol
from scripts.lib.identity.field.insurer_number import normalize_insurer_number
from scripts.lib.identity.field.name_kana import normalize_name_kana_full
from scripts.lib.identity.field.ticket_identifier import normalize_ticket_identifier
from scripts.lib.identity.primitive.digits import zero_pad


@dataclass(frozen=True)
class XmlExportFields:
    insurer_number: str
    insurance_symbol: str
    insurance_number: str
    name_kana: str
    gender_code: str
    birthdate: str
    exam_date: str
    postal_code: str | None
    address: str | None
    exam_ticket_number: str | None = None
    exam_ticket_number_root_oid: str | None = None
    exam_ticket_kind_code: str | None = None
    exam_ticket_kind_code_system: str | None = None
    exam_ticket_expires_on: str | None = None


class ExportFieldError(ValueError):
    pass


def _required(result: Mapping[str, Any], key: str, field_name: str) -> str:
    value = result.get(key)
    if not result.get("ok") or value in (None, ""):
        reason = result.get("reason") or "missing"
        raise ExportFieldError(f"{field_name}: {reason}")
    return str(value)


def _optional(result: Mapping[str, Any], key: str, field_name: str) -> str | None:
    value = result.get(key)
    if value in (None, ""):
        if result.get("ok") or result.get("missing"):
            return None
        reason = result.get("reason") or "invalid"
        raise ExportFieldError(f"{field_name}: {reason}")
    if not result.get("ok"):
        reason = result.get("reason") or "invalid"
        raise ExportFieldError(f"{field_name}: {reason}")
    return str(value)


def build_xml_export_fields(
    row: Mapping[str, Any],
    *,
    insurer_number_override: Any = None,
    postal_code_override: Any = None,
    address_override: Any = None,
) -> XmlExportFields:
    insurer = normalize_insurer_number(
        row.get("insurer_number") if insurer_number_override is None else insurer_number_override
    )
    symbol = normalize_insurance_symbol(row.get("insurance_symbol_export_value") or row.get("insurance_symbol_raw"))
    number = normalize_insurance_number(row.get("insurance_number_export_value") or row.get("insurance_number_raw"))
    kana = normalize_name_kana_full(row.get("name_kana_export_value") or row.get("name_kana_raw"))
    gender = normalize_gender_code(row.get("gender_code") or row.get("gender_raw"))
    birth = normalize_date_to_ymd_and_compact(row.get("birthdate"), purpose="birthdate")
    exam = normalize_date_to_ymd_and_compact(
        row.get("exam_date_export_value") or row.get("exam_date"),
        purpose="exam_date",
    )

    insurer_number = zero_pad(_required(insurer, "field_norm", "insurer_number"), 8)
    assert insurer_number is not None
    if len(insurer_number) != 8:
        raise ExportFieldError("insurer_number: invalid_length")

    exam_ticket = None
    if row.get("exam_ticket_number_export_value") not in (None, ""):
        exam_ticket = normalize_ticket_identifier(
            row.get("exam_ticket_number_export_value"),
            ticket_kind="exam_ticket",
            issuer_insurer_number=insurer_number,
        )
        if not exam_ticket.get("ok"):
            raise ExportFieldError(f"exam_ticket_number: {exam_ticket.get('reason') or 'invalid'}")
    exam_ticket_expires_on = normalize_date_to_ymd_and_compact(
        row.get("exam_ticket_expires_on_export_value"),
        purpose="exam_ticket_expires_on",
    )

    return XmlExportFields(
        insurer_number=insurer_number,
        insurance_symbol=_required(symbol, "export", "insurance_symbol"),
        insurance_number=_required(number, "field_norm", "insurance_number"),
        name_kana=_required(kana, "field_norm", "name_kana"),
        gender_code=_required(gender, "field_norm", "gender_code"),
        birthdate=_required(birth, "match", "birthdate"),
        exam_date=_required(exam, "match", "exam_date"),
        postal_code=normalize_postal_code_export(
            row.get("postal_code") if postal_code_override is None else postal_code_override
        ),
        address=normalize_address_export(
            row.get("address") if address_override is None else address_override
        ),
        exam_ticket_number=str(exam_ticket["field_norm"]) if exam_ticket else None,
        exam_ticket_number_root_oid=str(exam_ticket["root_oid"]) if exam_ticket else None,
        exam_ticket_kind_code=str(exam_ticket["ticket_kind_code"]) if exam_ticket else None,
        exam_ticket_kind_code_system=str(exam_ticket["ticket_kind_code_system"]) if exam_ticket else None,
        exam_ticket_expires_on=_optional(exam_ticket_expires_on, "match", "exam_ticket_expires_on"),
    )
