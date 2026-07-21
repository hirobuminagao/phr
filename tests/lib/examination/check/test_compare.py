from decimal import Decimal

import pytest

from scripts.lib.examination.check.compare import (
    is_between,
    is_equal,
    is_greater_than,
    is_greater_than_or_equal,
    is_less_than,
    is_less_than_or_equal,
)


class DummyNumericValue:
    def __init__(
        self,
        numeric_value: Decimal | None,
        *,
        is_valid: bool = True,
    ) -> None:
        self.numeric_value = numeric_value
        self.is_valid = is_valid


class IntConversionBlocked:
    def __int__(self) -> int:
        raise AssertionError("int conversion must not be used")

    def __float__(self) -> float:
        raise AssertionError("float conversion must not be used")

    def __eq__(self, other: object) -> bool:
        return other == Decimal("1")

    def __gt__(self, other: object) -> bool:
        return other == Decimal("0")

    def __ge__(self, other: object) -> bool:
        return other in {Decimal("0"), Decimal("1")}

    def __lt__(self, other: object) -> bool:
        return other == Decimal("2")

    def __le__(self, other: object) -> bool:
        return other in {Decimal("1"), Decimal("2")}


PUBLIC_FUNCTIONS = (
    is_equal,
    is_greater_than,
    is_greater_than_or_equal,
    is_less_than,
    is_less_than_or_equal,
)


@pytest.mark.parametrize("compare_func", PUBLIC_FUNCTIONS)
def test_numeric_comparisons_return_false_for_invalid_value(compare_func) -> None:
    assert compare_func(DummyNumericValue(Decimal("1"), is_valid=False), Decimal("1")) is False


@pytest.mark.parametrize("compare_func", PUBLIC_FUNCTIONS)
def test_numeric_comparisons_return_false_for_none_numeric_value(compare_func) -> None:
    assert compare_func(DummyNumericValue(None), Decimal("1")) is False


def test_is_between_returns_false_for_invalid_value() -> None:
    assert is_between(DummyNumericValue(Decimal("1"), is_valid=False), Decimal("0"), Decimal("2")) is False


def test_is_between_returns_false_for_none_numeric_value() -> None:
    assert is_between(DummyNumericValue(None), Decimal("0"), Decimal("2")) is False


@pytest.mark.parametrize(
    ("numeric_value", "target", "expected"),
    [
        (Decimal("1"), Decimal("1"), True),
        (Decimal("0.9"), Decimal("1"), False),
        (Decimal("1.1"), Decimal("1"), False),
    ],
)
def test_is_equal(numeric_value: Decimal, target: Decimal, expected: bool) -> None:
    assert is_equal(DummyNumericValue(numeric_value), target) is expected


@pytest.mark.parametrize(
    ("numeric_value", "target", "expected"),
    [
        (Decimal("1.1"), Decimal("1"), True),
        (Decimal("1"), Decimal("1"), False),
        (Decimal("0.9"), Decimal("1"), False),
    ],
)
def test_is_greater_than(numeric_value: Decimal, target: Decimal, expected: bool) -> None:
    assert is_greater_than(DummyNumericValue(numeric_value), target) is expected


@pytest.mark.parametrize(
    ("numeric_value", "target", "expected"),
    [
        (Decimal("1.1"), Decimal("1"), True),
        (Decimal("1"), Decimal("1"), True),
        (Decimal("0.9"), Decimal("1"), False),
    ],
)
def test_is_greater_than_or_equal(numeric_value: Decimal, target: Decimal, expected: bool) -> None:
    assert is_greater_than_or_equal(DummyNumericValue(numeric_value), target) is expected


@pytest.mark.parametrize(
    ("numeric_value", "target", "expected"),
    [
        (Decimal("0.9"), Decimal("1"), True),
        (Decimal("1"), Decimal("1"), False),
        (Decimal("1.1"), Decimal("1"), False),
    ],
)
def test_is_less_than(numeric_value: Decimal, target: Decimal, expected: bool) -> None:
    assert is_less_than(DummyNumericValue(numeric_value), target) is expected


@pytest.mark.parametrize(
    ("numeric_value", "target", "expected"),
    [
        (Decimal("0.9"), Decimal("1"), True),
        (Decimal("1"), Decimal("1"), True),
        (Decimal("1.1"), Decimal("1"), False),
    ],
)
def test_is_less_than_or_equal(numeric_value: Decimal, target: Decimal, expected: bool) -> None:
    assert is_less_than_or_equal(DummyNumericValue(numeric_value), target) is expected


@pytest.mark.parametrize(
    ("numeric_value", "minimum", "maximum", "expected"),
    [
        (Decimal("1"), Decimal("1"), Decimal("3"), True),
        (Decimal("3"), Decimal("1"), Decimal("3"), True),
        (Decimal("2"), Decimal("1"), Decimal("3"), True),
        (Decimal("0.9"), Decimal("1"), Decimal("3"), False),
        (Decimal("3.1"), Decimal("1"), Decimal("3"), False),
        (Decimal("2"), Decimal("2"), Decimal("2"), True),
    ],
)
def test_is_between_closed_range(
    numeric_value: Decimal,
    minimum: Decimal,
    maximum: Decimal,
    expected: bool,
) -> None:
    assert is_between(DummyNumericValue(numeric_value), minimum, maximum) is expected


def test_is_between_raises_value_error_before_checking_invalid_value() -> None:
    with pytest.raises(ValueError):
        is_between(DummyNumericValue(None, is_valid=False), Decimal("3"), Decimal("1"))


def test_numeric_comparisons_return_bool() -> None:
    assert type(is_equal(DummyNumericValue(Decimal("1")), Decimal("1"))) is bool
    assert type(is_between(DummyNumericValue(Decimal("1")), Decimal("0"), Decimal("2"))) is bool


def test_numeric_comparisons_do_not_use_int_or_float_conversion() -> None:
    value = DummyNumericValue(IntConversionBlocked())  # type: ignore[arg-type]

    assert is_equal(value, Decimal("1")) is True
    assert is_greater_than(value, Decimal("0")) is True
    assert is_greater_than_or_equal(value, Decimal("1")) is True
    assert is_less_than(value, Decimal("2")) is True
    assert is_less_than_or_equal(value, Decimal("1")) is True
    assert is_between(value, Decimal("1"), Decimal("1")) is True
