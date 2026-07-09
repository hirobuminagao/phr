"""Alternative rule helpers for Phase7 examination checks."""

from __future__ import annotations

from .models import ExamValue


def has_alternative_value(
    *,
    values_by_identity: dict[str, list[ExamValue]],
    values_by_method: dict[str, list[ExamValue]],
    values_by_namecode: dict[str, list[ExamValue]],
    source_identity_codes: tuple[str, ...],
    source_method_codes: tuple[str, ...],
    source_namecodes: tuple[str, ...],
) -> bool:
    for identity_code in source_identity_codes:
        if any(value.has_valid_value for value in values_by_identity.get(identity_code, ())):
            return True
    for method_code in source_method_codes:
        if any(value.has_valid_value for value in values_by_method.get(method_code, ())):
            return True
    for namecode in source_namecodes:
        if any(value.has_valid_value for value in values_by_namecode.get(namecode, ())):
            return True
    return False
