"""Calculation functions for Phase7 examination checks."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation


class CalculationError(RuntimeError):
    """Raised when a calculation cannot be completed."""


def calculate(rule_code: str, sources: dict[str, Decimal]) -> Decimal:
    if rule_code == "BMI":
        return bmi(sources["9N001"], sources["9N006"])
    if rule_code == "OBESITY_INDEX":
        return obesity_index(sources["9N001"], sources["9N006"])
    if rule_code == "NON_HDL_CHOLESTEROL":
        return non_hdl_cholesterol(sources["3F050"], sources["3F070"])
    raise NotImplementedError(rule_code)


def bmi(height_cm: Decimal, weight_kg: Decimal) -> Decimal:
    try:
        height_m = height_cm / Decimal("100")
        if height_m <= 0:
            raise CalculationError("height must be positive")
        return weight_kg / (height_m * height_m)
    except (InvalidOperation, ZeroDivisionError) as exc:
        raise CalculationError(str(exc)) from exc


def obesity_index(height_cm: Decimal, weight_kg: Decimal) -> Decimal:
    try:
        height_m = height_cm / Decimal("100")
        standard_weight = height_m * height_m * Decimal("22")
        if standard_weight <= 0:
            raise CalculationError("standard weight must be positive")
        return ((weight_kg / standard_weight) - Decimal("1")) * Decimal("100")
    except (InvalidOperation, ZeroDivisionError) as exc:
        raise CalculationError(str(exc)) from exc


def non_hdl_cholesterol(total_cholesterol: Decimal, hdl_cholesterol: Decimal) -> Decimal:
    return total_cholesterol - hdl_cholesterol
