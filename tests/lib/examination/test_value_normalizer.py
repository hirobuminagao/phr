from scripts.lib.examination.value_normalizer import normalize_exam_item_value


def test_normalize_numeric_less_than_symbol_preserves_raw_and_uses_threshold() -> None:
    result = normalize_exam_item_value(
        None,
        namecode="9E160162100000001",
        raw_value="<0.1",
        exam_item={"data_type": "PQ", "unit": None},
    )

    assert result.raw_value == "<0.1"
    assert result.raw_value_type == "PQ"
    assert result.normalized_value == "0.1"
    assert result.normalize_status == "OK"
    assert result.normalize_reason == "RAW_VALUE_NUMERIC_COMPARATOR_NORMALIZED"
    assert result.validation_status == "VALID"


def test_normalize_numeric_less_than_japanese_preserves_raw_and_uses_threshold() -> None:
    result = normalize_exam_item_value(
        None,
        namecode="9E160162100000001",
        raw_value="0.1未満",
        exam_item={"data_type": "PQ", "unit": None},
    )

    assert result.raw_value == "0.1未満"
    assert result.normalized_value == "0.1"
    assert result.normalize_status == "OK"
    assert result.normalize_reason == "RAW_VALUE_NUMERIC_COMPARATOR_NORMALIZED"
    assert result.validation_status == "VALID"


def test_normalize_numeric_unknown_text_stays_invalid_value_type() -> None:
    result = normalize_exam_item_value(
        None,
        namecode="9E160162100000001",
        raw_value="不明",
        exam_item={"data_type": "PQ", "unit": None},
    )

    assert result.raw_value == "不明"
    assert result.normalized_value is None
    assert result.normalize_status == "ERROR"
    assert result.normalize_reason == "INVALID_VALUE_TYPE"
    assert result.validation_status == "INVALID"
