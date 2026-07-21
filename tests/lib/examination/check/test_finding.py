import pytest

from scripts.lib.examination.check.finding import contains_any_keyword, has_text


class DummyTextValue:
    def __init__(
        self,
        text: str | None,
        *,
        is_valid: bool = True,
        raw_text: str | None = None,
    ) -> None:
        self.text = text
        self.is_valid = is_valid
        self.raw_text = raw_text


def test_has_text_returns_true_for_valid_non_empty_text() -> None:
    assert has_text(DummyTextValue("所見あり")) is True


def test_has_text_returns_false_for_invalid_text_value() -> None:
    assert has_text(DummyTextValue("所見あり", is_valid=False)) is False


def test_has_text_returns_false_for_none_text() -> None:
    assert has_text(DummyTextValue(None)) is False


def test_has_text_returns_false_for_empty_text() -> None:
    assert has_text(DummyTextValue("")) is False


def test_has_text_treats_space_as_text_without_normalization() -> None:
    assert has_text(DummyTextValue(" ")) is True


def test_has_text_does_not_use_raw_text() -> None:
    assert has_text(DummyTextValue("", raw_text="raw has text")) is False


def test_contains_any_keyword_returns_true_for_one_match() -> None:
    assert contains_any_keyword(DummyTextValue("胸部X線 所見あり"), ("所見",)) is True


def test_contains_any_keyword_returns_true_when_one_of_multiple_keywords_matches() -> None:
    assert contains_any_keyword(DummyTextValue("胸部X線 所見あり"), ("異常なし", "所見")) is True


def test_contains_any_keyword_returns_false_when_no_keywords_match() -> None:
    assert contains_any_keyword(DummyTextValue("胸部X線 所見あり"), ("異常なし",)) is False


def test_contains_any_keyword_returns_false_for_empty_keywords() -> None:
    assert contains_any_keyword(DummyTextValue("胸部X線 所見あり"), ()) is False


def test_contains_any_keyword_returns_false_for_invalid_text_value() -> None:
    assert contains_any_keyword(DummyTextValue("胸部X線 所見あり", is_valid=False), ("所見",)) is False


def test_contains_any_keyword_returns_false_for_none_text() -> None:
    assert contains_any_keyword(DummyTextValue(None), ("所見",)) is False


def test_contains_any_keyword_returns_false_for_empty_text() -> None:
    assert contains_any_keyword(DummyTextValue(""), ("",)) is False


def test_contains_any_keyword_uses_substring_matching() -> None:
    assert contains_any_keyword(DummyTextValue("胸部X線 所見あり"), ("所見",)) is True


def test_contains_any_keyword_treats_exact_match_as_substring_match() -> None:
    assert contains_any_keyword(DummyTextValue("所見"), ("所見",)) is True


def test_contains_any_keyword_is_case_sensitive() -> None:
    assert contains_any_keyword(DummyTextValue("Finding"), ("finding",)) is False


def test_contains_any_keyword_does_not_apply_nfkc_normalization() -> None:
    assert contains_any_keyword(DummyTextValue("ＡＢＣ"), ("ABC",)) is False


def test_contains_any_keyword_does_not_use_raw_text() -> None:
    assert contains_any_keyword(DummyTextValue("", raw_text="所見あり"), ("所見",)) is False


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("所見あり", True),
        ("", False),
    ],
)
def test_contains_any_keyword_with_empty_keyword_uses_current_substring_contract(
    text: str,
    expected: bool,
) -> None:
    assert contains_any_keyword(DummyTextValue(text), ("",)) is expected
