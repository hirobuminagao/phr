"""Small data models for health examination checks."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any


STATUS_OK = "OK"
STATUS_CALCULATED = "CALCULATED"
STATUS_ALTERNATIVE = "ALTERNATIVE"
STATUS_MISSING = "MISSING"
STATUS_INVALID = "INVALID"

RESULT_OK = "OK"
RESULT_WARNING = "WARNING"
RESULT_NG = "NG"

REASON_NOT_IMPLEMENTED = "NOT_IMPLEMENTED"
REASON_MISSING = "MISSING"
REASON_INVALID_VALUE = "INVALID_VALUE"
REASON_CALCULATION_SOURCE_MISSING = "CALCULATION_SOURCE_MISSING"


@dataclass(frozen=True)
class ExamValue:
    """One raw value candidate from exam_item_values."""

    id: int
    namecode: str | None
    method_code: str | None
    identity_code: str | None
    raw_value: str | None
    nullflavor: str | None
    negation_ind: int | None
    normalized_value: str | None = None
    validation_status: str | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "ExamValue":
        namecode = row.get("namecode")
        method_code = row.get("xml_method_code")
        identity_code = row.get("identity_item_code")
        return cls(
            id=int(row["id"]),
            namecode=str(namecode) if namecode else None,
            method_code=str(method_code) if method_code else None,
            identity_code=str(identity_code) if identity_code else None,
            raw_value=str(row["raw_value"]) if row.get("raw_value") is not None else None,
            nullflavor=str(row["nullflavor"]) if row.get("nullflavor") else None,
            negation_ind=int(row["negation_ind"]) if row.get("negation_ind") is not None else None,
            normalized_value=str(row["normalized_value"]) if row.get("normalized_value") is not None else None,
            validation_status=str(row["validation_status"]) if row.get("validation_status") else None,
        )

    @property
    def has_record(self) -> bool:
        return True

    @property
    def has_valid_value(self) -> bool:
        if self.negation_ind == 1:
            return False
        if self.nullflavor:
            return False
        value = self.normalized_value if self.normalized_value is not None else self.raw_value
        return value is not None and str(value).strip() != ""

    def decimal_value(self) -> Decimal | None:
        value = self.normalized_value if self.normalized_value is not None else self.raw_value
        if value is None:
            return None
        try:
            return Decimal(str(value).strip())
        except Exception:
            return None


@dataclass(frozen=True)
class MethodRule:
    """One method-level rule from exam_item_group_method_members."""

    group_code: str
    method_code: str
    identity_code: str
    priority: int
    presence_value_mode: str | None
    required_flag: int | None
    rule_code: str | None
    source_identity_codes: tuple[str, ...]
    source_method_codes: tuple[str, ...]
    source_namecodes: tuple[str, ...]

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "MethodRule":
        method_code = str(row["xml_method_code"])
        return cls(
            group_code=str(row["group_code"]),
            method_code=method_code,
            identity_code=str(row["identity_item_code"]),
            priority=int(row.get("priority") or 100),
            presence_value_mode=row.get("presence_value_mode"),
            required_flag=int(row["required_flag"]) if row.get("required_flag") is not None else None,
            rule_code=row.get("rule_code"),
            source_identity_codes=parse_csv(row.get("rule_source_identity_codes")),
            source_method_codes=parse_csv(row.get("rule_source_method_codes")),
            source_namecodes=parse_csv(row.get("rule_source_namecodes")),
        )


@dataclass(frozen=True)
class ItemResult:
    """The status/reason pair persisted to exam_check_results."""

    identity_code: str
    status: str
    reason: str | None = None

    @property
    def is_ok_like(self) -> bool:
        return self.status in {STATUS_OK, STATUS_CALCULATED, STATUS_ALTERNATIVE}


def parse_csv(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    seen: set[str] = set()
    result: list[str] = []
    for part in str(value).split(","):
        item = part.strip()
        if not item or item in seen:
            continue
        seen.add(item)
        result.append(item)
    return tuple(result)
