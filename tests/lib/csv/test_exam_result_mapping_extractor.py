from __future__ import annotations

from scripts.lib.csv.exam_result_mapping_extractor import extract_rule_value
from scripts.lib.db.lookup.csv_exam_result_mapping import CsvMappingCondition, CsvMappingRule


def condition(
    condition_id: int,
    *,
    condition_type: str,
    column_no: int,
    source_role: str = "VALUE",
    operator: str | None = None,
    expected_value: str | None = None,
) -> CsvMappingCondition:
    return CsvMappingCondition(
        condition_id=condition_id,
        rule_id=1,
        condition_group_no=1,
        condition_type=condition_type,
        locator_type="COLUMN_NO",
        header_context=None,
        header_name=None,
        header_occurrence=None,
        column_no=column_no,
        operator=operator,
        expected_value=expected_value,
        source_role=source_role,
        priority=condition_id,
        resolved_column_no=column_no,
    )


def rule(
    *conditions: CsvMappingCondition,
    value_source_type: str = "SOURCE",
    fixed_value: str | None = None,
    value_join_separator: str | None = None,
    value_exclude_values: tuple[str, ...] = (),
) -> CsvMappingRule:
    return CsvMappingRule(
        rule_id=1,
        csv_format_version_id=1,
        rule_key="finding.rule",
        target_kind="EXAM_ITEM_VALUE",
        target_resolution_type="SINGLE_NAMECODE",
        selection_mode="DIRECT",
        selection_group_code=None,
        target_namecode="9N066000000000011",
        target_identity_item_code=None,
        target_field=None,
        method_structure_type=None,
        value_source_type=value_source_type,
        fixed_value=fixed_value,
        value_join_separator=value_join_separator,
        value_exclude_values=value_exclude_values,
        raw_value_type=None,
        raw_unit=None,
        is_required=False,
        priority=1000,
        conditions=conditions,
    )


def test_fixed_value_is_emitted_when_row_condition_matches() -> None:
    mapping = rule(
        condition(
            1,
            condition_type="CELL_VALUE",
            column_no=1,
            source_role="QUALIFIER",
            operator="NOT_EQUALS",
            expected_value="異常所見なし",
        ),
        value_source_type="FIXED",
        fixed_value="1",
    )

    result = extract_rule_value(["心雑音 要受診"], mapping)

    assert result.values_by_role["VALUE"] == "1"
    assert result.errors == ()


def test_multiple_value_columns_are_joined_while_blank_values_are_ignored() -> None:
    mapping = rule(
        condition(1, condition_type="HEADER_MATCH", column_no=1),
        condition(2, condition_type="HEADER_MATCH", column_no=2),
        value_join_separator=" / ",
    )

    result = extract_rule_value(["心雑音 要受診", "高血圧 要観察"], mapping)
    blank_result = extract_rule_value(["心雑音 要受診", ""], mapping)

    assert result.values_by_role["VALUE"] == "心雑音 要受診 / 高血圧 要観察"
    assert blank_result.values_by_role["VALUE"] == "心雑音 要受診"


def test_multiple_value_columns_ignore_configured_noise_values() -> None:
    mapping = rule(
        condition(1, condition_type="HEADER_MATCH", column_no=1),
        condition(2, condition_type="HEADER_MATCH", column_no=2),
        condition(3, condition_type="HEADER_MATCH", column_no=3),
        value_join_separator=" / ",
        value_exclude_values=("異常なし/",),
    )

    result = extract_rule_value(["脂肪肝/", "異常なし/", "腎嚢胞/"], mapping)
    only_noise_result = extract_rule_value(["異常なし/", "", "異常なし/"], mapping)

    assert result.values_by_role["VALUE"] == "脂肪肝/ / 腎嚢胞/"
    assert only_noise_result.values_by_role["VALUE"] is None


def test_multiple_value_columns_require_explicit_join_separator() -> None:
    mapping = rule(
        condition(1, condition_type="HEADER_MATCH", column_no=1),
        condition(2, condition_type="HEADER_MATCH", column_no=2),
    )

    result = extract_rule_value(["所見1", "所見2"], mapping)

    assert result.values_by_role == {}
    assert result.errors == ("MULTIPLE_VALUE_SOURCES_REQUIRE_JOIN_SEPARATOR",)


def test_screen_created_source_column_is_extracted_as_value() -> None:
    source = condition(1, condition_type="SOURCE_COLUMN", column_no=1)
    mapping = rule(source)

    result = extract_rule_value(["画面設定の値"], mapping)

    assert result.matched_condition_group_no == 1
    assert result.values_by_role == {"VALUE": "画面設定の値"}
    assert result.errors == ()
