from decimal import Decimal

from scripts.from_medical.script_lib.article44_models import PQValue, ValueState
from scripts.lib.examination.check.any import has_any_valid


class DummyValue:
    def __init__(self, is_valid: bool) -> None:
        self.is_valid = is_valid


class OnlyIsValidValue:
    def __init__(self, is_valid: bool) -> None:
        self.is_valid = is_valid


def test_has_any_valid_returns_false_for_empty_iterable() -> None:
    assert has_any_valid([]) is False


def test_has_any_valid_returns_false_for_one_false_value() -> None:
    assert has_any_valid([DummyValue(False)]) is False


def test_has_any_valid_returns_true_for_one_true_value() -> None:
    assert has_any_valid([DummyValue(True)]) is True


def test_has_any_valid_returns_false_when_all_values_are_false() -> None:
    assert has_any_valid([DummyValue(False), DummyValue(False)]) is False


def test_has_any_valid_returns_true_when_one_middle_value_is_true() -> None:
    assert has_any_valid([DummyValue(False), DummyValue(True), DummyValue(False)]) is True


def test_has_any_valid_returns_true_when_all_values_are_true() -> None:
    assert has_any_valid([DummyValue(True), DummyValue(True)]) is True


def test_has_any_valid_accepts_tuple() -> None:
    assert has_any_valid((DummyValue(False), DummyValue(True))) is True


def test_has_any_valid_accepts_generator() -> None:
    values = (DummyValue(is_valid) for is_valid in (False, True))

    assert has_any_valid(values) is True


def test_has_any_valid_accepts_article44_value_type() -> None:
    value = PQValue(
        value_state=ValueState.PRESENT,
        raw_value="1",
        numeric_value=Decimal("1"),
        unit=None,
        is_valid=True,
        invalid_reason=None,
        duplicate_count=None,
    )

    assert has_any_valid([value]) is True


def test_has_any_valid_depends_only_on_is_valid_attribute() -> None:
    assert has_any_valid([OnlyIsValidValue(True)]) is True
