from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from scripts.lib.identity.base_norm import base_normalize
from scripts.lib.identity.primitive.digits import extract_digits
from scripts.lib.identity.field.insurer_number import normalize_insurer_number
from scripts.lib.identity.primitive.digits import zero_pad


OID_TICKET_KIND = "1.2.392.200119.6.208"
OID_EXAM_TICKET_NUMBER_PREFIX = "1.2.392.200119.6.209."
OID_GUIDANCE_TICKET_NUMBER_PREFIX = "1.2.392.200119.6.210."
TICKET_IDENTIFIER_LENGTH = 11
TICKET_IDENTIFIER_SEPARATORS = frozenset(" -‐‑‒–—―ーｰ")


@dataclass(frozen=True)
class TicketKindRule:
    field_name: str
    ticket_kind: str
    ticket_kind_code: str
    display_name: str
    number_oid_prefix: str


TICKET_KIND_RULES: dict[str, TicketKindRule] = {
    "exam_ticket": TicketKindRule(
        field_name="exam_ticket_number",
        ticket_kind="exam_ticket",
        ticket_kind_code="1",
        display_name="受診券",
        number_oid_prefix=OID_EXAM_TICKET_NUMBER_PREFIX,
    ),
    "guidance_ticket": TicketKindRule(
        field_name="guidance_ticket_number",
        ticket_kind="guidance_ticket",
        ticket_kind_code="2",
        display_name="利用券",
        number_oid_prefix=OID_GUIDANCE_TICKET_NUMBER_PREFIX,
    ),
}


def _error(
    *,
    rule: TicketKindRule | None,
    raw: Any,
    base_norm: str | None = None,
    field_norm: str | None = None,
    issuer_insurer_number: str | None = None,
    issuer_insurer_number_oid_suffix: str | None = None,
    reason: str,
) -> dict[str, Any]:
    return {
        "field_name": rule.field_name if rule else "ticket_identifier",
        "ticket_kind": rule.ticket_kind if rule else None,
        "ticket_kind_code": rule.ticket_kind_code if rule else None,
        "ticket_kind_code_system": OID_TICKET_KIND if rule else None,
        "ticket_kind_display_name": rule.display_name if rule else None,
        "raw": raw,
        "base_norm": base_norm,
        "field_norm": field_norm,
        "export": field_norm,
        "match": field_norm,
        "issuer_insurer_number": issuer_insurer_number,
        "issuer_insurer_number_oid_suffix": issuer_insurer_number_oid_suffix,
        "root_oid": None,
        "ok": False,
        "missing": reason.startswith("missing_"),
        "reason": reason,
    }


def normalize_ticket_identifier(
    raw: Any,
    *,
    ticket_kind: str,
    issuer_insurer_number: Any = None,
) -> dict[str, Any]:
    """受診券/利用券の整理番号をXML出力用に正規化する。

    厚生労働省の特定健診・特定保健指導XML仕様では、受診券整理番号
    および利用券整理番号は数字11桁固定で扱う。保険証番号とは違い、
    先頭0は意味を持つため保持する。
    """

    rule = TICKET_KIND_RULES.get(ticket_kind)
    if rule is None:
        return _error(rule=None, raw=raw, reason="unknown_ticket_kind")

    base = base_normalize(None if raw is None else str(raw))
    if base is None:
        return _error(rule=rule, raw=raw, base_norm=base, reason="missing_raw_or_base_norm")

    compact = "".join(ch for ch in base if ch not in TICKET_IDENTIFIER_SEPARATORS)
    digits_only = extract_digits(compact)
    if digits_only is None:
        return _error(rule=rule, raw=raw, base_norm=base, reason="missing_digits")
    if digits_only != compact:
        return _error(
            rule=rule,
            raw=raw,
            base_norm=base,
            field_norm=digits_only,
            reason="non_digit_characters_removed",
        )
    if len(digits_only) != TICKET_IDENTIFIER_LENGTH:
        return _error(
            rule=rule,
            raw=raw,
            base_norm=base,
            field_norm=digits_only,
            reason="invalid_length",
        )

    issuer_norm = None
    issuer_oid_suffix = None
    root_oid = None
    if issuer_insurer_number not in (None, ""):
        issuer_result = normalize_insurer_number(str(issuer_insurer_number))
        issuer_norm = issuer_result.get("field_norm")
        if not issuer_result.get("ok") or issuer_norm in (None, ""):
            return _error(
                rule=rule,
                raw=raw,
                base_norm=base,
                field_norm=digits_only,
                reason="invalid_issuer_insurer_number",
            )
        issuer_norm = zero_pad(str(issuer_norm), 8)
        if issuer_norm is None or len(issuer_norm) != 8:
            return _error(
                rule=rule,
                raw=raw,
                base_norm=base,
                field_norm=digits_only,
                issuer_insurer_number=issuer_norm,
                reason="invalid_issuer_insurer_number_length",
            )
        issuer_oid_suffix = f"1{issuer_norm}"
        root_oid = f"{rule.number_oid_prefix}{issuer_oid_suffix}"

    return {
        "field_name": rule.field_name,
        "ticket_kind": rule.ticket_kind,
        "ticket_kind_code": rule.ticket_kind_code,
        "ticket_kind_code_system": OID_TICKET_KIND,
        "ticket_kind_display_name": rule.display_name,
        "raw": raw,
        "base_norm": base,
        "field_norm": digits_only,
        "export": digits_only,
        "match": digits_only,
        "issuer_insurer_number": issuer_norm,
        "issuer_insurer_number_oid_suffix": issuer_oid_suffix,
        "root_oid": root_oid,
        "ok": True,
        "missing": False,
        "reason": None,
    }
