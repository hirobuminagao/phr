from __future__ import annotations

from pathlib import Path

from scripts.lib.csv.mapping_data_check import (
    compare_csv_headers,
    read_csv_stream_header,
    run_csv_mapping_data_check,
)
from scripts.lib.db.lookup.csv_exam_result_mapping import CsvMappingCondition, CsvMappingRule
from scripts.lib.examination.value_normalizer import NormalizedExamValue


def mapping_rule(*, namecode: str = "9D100163100000011") -> CsvMappingRule:
    condition = CsvMappingCondition(
        condition_id=1,
        rule_id=1,
        condition_group_no=1,
        condition_type="HEADER_MATCH",
        locator_type="HEADER_NAME",
        header_context=None,
        header_name="聴力",
        header_occurrence=1,
        column_no=None,
        operator=None,
        expected_value=None,
        source_role="VALUE",
        priority=1,
        resolved_column_no=1,
    )
    return CsvMappingRule(
        rule_id=1,
        csv_format_version_id=10,
        rule_key="hearing.right.1000",
        target_kind="EXAM_ITEM_VALUE",
        target_resolution_type="SINGLE_NAMECODE",
        selection_mode="DIRECT",
        selection_group_code=None,
        target_namecode=namecode,
        target_identity_item_code=None,
        target_field=None,
        method_structure_type=None,
        value_source_type="SOURCE",
        fixed_value=None,
        value_join_separator=None,
        value_exclude_values=(),
        raw_value_type="CD",
        raw_unit=None,
        is_required=False,
        priority=100,
        conditions=(condition,),
    )


def normalized_code(raw_value: str) -> NormalizedExamValue:
    code, display = {"1": ("2", "異常所見なし"), "2": ("1", "異常所見あり")}[raw_value]
    return NormalizedExamValue(
        raw_value=raw_value,
        raw_value_type="CD",
        raw_unit=None,
        normalized_value=None,
        normalized_unit=None,
        nullflavor=None,
        code_system="1.2.3",
        code_value=code,
        code_display=display,
        normalize_status="OK",
        normalize_reason="RAW_VALUE_EXACT_MATCH",
        validation_status="VALID",
        validation_reason=None,
    )


def write_csv(path: Path, rows: list[str]) -> None:
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def test_streaming_check_uses_processed_row_count_as_code_rate_denominator(tmp_path: Path) -> None:
    path = tmp_path / "hearing.csv"
    write_csv(path, ["聴力", "1", "1", "2", "1"])
    header = read_csv_stream_header(path, data_start_row_no=2)

    result = run_csv_mapping_data_check(
        path,
        stream_header=header,
        rules=[mapping_rule()],
        normalize=lambda _rule, raw: normalized_code(raw),
        max_rows=3,
    )

    assert result["total_data_rows"] == 4
    assert result["processed_rows"] == 3
    assert result["omitted_rows"] == 1
    target = result["targets"][0]
    assert target["value_count"] == 3
    assert target["success_count"] == 3
    assert target["code_rows"][0] == {
        "code_value": "2",
        "code_display": "異常所見なし",
        "count": 2,
        "rate": 66.67,
        "raw_values": [{"raw_value": "1", "count": 2, "rate": 66.67}],
    }
    assert target["code_rows"][1]["code_value"] == "1"
    assert target["code_rows"][1]["rate"] == 33.33


def test_streaming_check_counts_blank_rows_without_normalizing(tmp_path: Path) -> None:
    path = tmp_path / "blank.csv"
    write_csv(path, ["聴力", "", "1"])
    header = read_csv_stream_header(path, data_start_row_no=2)
    calls: list[str] = []

    result = run_csv_mapping_data_check(
        path,
        stream_header=header,
        rules=[mapping_rule()],
        normalize=lambda _rule, raw: calls.append(raw) or normalized_code(raw),
    )

    assert result["empty_rows"] == 1
    assert result["processed_rows"] == 1
    assert calls == ["1"]


def test_header_compare_distinguishes_order_and_missing(tmp_path: Path) -> None:
    path = tmp_path / "headers.csv"
    write_csv(path, ["氏名,聴力,追加", "山田,1,x"])
    header = read_csv_stream_header(path, data_start_row_no=2)
    snapshot = {
        "normalized_columns": [
            {"column_no": 1, "context": None, "header_name": "聴力", "occurrence": 1},
            {"column_no": 2, "context": None, "header_name": "氏名", "occurrence": 1},
            {"column_no": 3, "context": None, "header_name": "保険者番号", "occurrence": 1},
        ]
    }

    result = compare_csv_headers(header, snapshot)

    assert result["is_exact"] is False
    assert result["counts"] == {"ORDER_ONLY": 2, "MISSING": 1, "ADDED": 1}
