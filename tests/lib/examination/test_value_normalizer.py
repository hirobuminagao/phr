from scripts.lib.examination.value_normalizer import mhlw_text_byte_length, normalize_exam_item_value


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


def test_normalize_numeric_or_less_japanese_preserves_raw_and_uses_threshold() -> None:
    result = normalize_exam_item_value(
        None,
        namecode="1A030000000190301",
        raw_value="1.005以下",
        exam_item={"data_type": "PQ", "unit": "1"},
    )

    assert result.raw_value == "1.005以下"
    assert result.normalized_value == "1.005"
    assert result.normalized_unit == "1"
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


def test_normalize_halfwidth_kana_cancel_is_no_result() -> None:
    result = normalize_exam_item_value(
        None,
        namecode="1A030000000190301",
        raw_value="ｷﾔﾝｾﾙ",
        exam_item={"data_type": "PQ", "unit": "1"},
    )

    assert result.raw_value == "ｷﾔﾝｾﾙ"
    assert result.raw_value_type == "PQ"
    assert result.normalized_value is None
    assert result.normalize_status == "SKIPPED"
    assert result.normalize_reason == "RAW_VALUE_NO_RESULT"
    assert result.validation_status == "WARNING"


def test_mhlw_text_byte_length_counts_ascii_as_one_and_non_ascii_as_two() -> None:
    assert mhlw_text_byte_length("ABCあいう") == 9


def test_normalize_st_at_mhlw_text_limit_is_valid() -> None:
    result = normalize_exam_item_value(
        None,
        namecode="9N511000000000049",
        raw_value="あ" * 128,
        exam_item={"data_type": "ST", "unit": None},
    )

    assert result.normalized_value == "あ" * 128
    assert result.normalize_status == "OK"
    assert result.validation_status == "VALID"


def test_normalize_st_over_mhlw_text_limit_is_invalid() -> None:
    result = normalize_exam_item_value(
        None,
        namecode="9N511000000000049",
        raw_value="あ" * 129,
        exam_item={"data_type": "ST", "unit": None},
    )

    assert result.raw_value == "あ" * 129
    assert result.normalized_value is None
    assert result.normalize_status == "ERROR"
    assert result.normalize_reason == "ST_MAX_BYTE_LENGTH_EXCEEDED"
    assert result.validation_status == "INVALID"
    assert result.validation_reason == "ST_MAX_BYTE_LENGTH_EXCEEDED"
