"""Common numeric comparisons for normalized examination values."""

from decimal import Decimal
from typing import Protocol


class ComparableNumericValue(Protocol):
    """Minimal contract required by numeric comparison checks."""

    is_valid: bool
    numeric_value: Decimal | None


def _numeric_value(value: ComparableNumericValue) -> Decimal | None:
    if not value.is_valid:
        return None
    return value.numeric_value


def is_equal(
    value: ComparableNumericValue,
    target: Decimal,
) -> bool:
    """Return True when the value equals the target."""

    numeric_value = _numeric_value(value)
    if numeric_value is None:
        return False
    return numeric_value == target


def is_greater_than(
    value: ComparableNumericValue,
    target: Decimal,
) -> bool:
    """Return True when the value is greater than the target."""

    numeric_value = _numeric_value(value)
    if numeric_value is None:
        return False
    return numeric_value > target


def is_greater_than_or_equal(
    value: ComparableNumericValue,
    target: Decimal,
) -> bool:
    """Return True when the value is greater than or equal to the target."""

    numeric_value = _numeric_value(value)
    if numeric_value is None:
        return False
    return numeric_value >= target


def is_less_than(
    value: ComparableNumericValue,
    target: Decimal,
) -> bool:
    """Return True when the value is less than the target."""

    numeric_value = _numeric_value(value)
    if numeric_value is None:
        return False
    return numeric_value < target


def is_less_than_or_equal(
    value: ComparableNumericValue,
    target: Decimal,
) -> bool:
    """Return True when the value is less than or equal to the target."""

    numeric_value = _numeric_value(value)
    if numeric_value is None:
        return False
    return numeric_value <= target


def is_between(
    value: ComparableNumericValue,
    minimum: Decimal,
    maximum: Decimal,
) -> bool:
    """Return True when the value is within the inclusive range."""

    if minimum > maximum:
        raise ValueError("minimum must be less than or equal to maximum")

    numeric_value = _numeric_value(value)
    if numeric_value is None:
        return False
    return minimum <= numeric_value <= maximum
