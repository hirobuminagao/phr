from apps.health_exam_admin.main import build_csv_mapping_template_edit_rules


def _conditional_rule(
    rule_id: int,
    *,
    operator: str,
    expected_value: str | None,
    fixed_value: str,
    priority: int,
    include_not_empty: bool = False,
) -> dict:
    conditions = []
    if include_not_empty:
        conditions.append(
            {
                "operator": "NOT_EMPTY",
                "expected_value": None,
                "header_name": "胸部X線判定",
                "column_no": 20,
                "priority": 100,
            }
        )
    conditions.append(
        {
            "operator": operator,
            "expected_value": expected_value,
            "header_name": "胸部X線判定",
            "column_no": 20,
            "priority": 110,
        }
    )
    return {
        "csv_exam_result_mapping_rule_id": rule_id,
        "selection_group_code": "screen_cond_test",
        "method_structure_type": "CONDITIONAL_FIXED",
        "value_source_type": "FIXED",
        "fixed_value": fixed_value,
        "priority": priority,
        "edit_capability": "CONDITIONAL_FIXED",
        "is_active": 1,
        "screen_conditions": conditions,
        "screen_edit_payload": {
            "targetKind": "EXAM_ITEM_VALUE",
            "targetCode": "9N000000000000001",
        },
    }


def test_conditional_rules_are_reconstructed_as_one_editable_group() -> None:
    result = build_csv_mapping_template_edit_rules(
        [
            _conditional_rule(11, operator="EQUALS", expected_value="A", fixed_value="2", priority=100),
            _conditional_rule(
                12,
                operator="NOT_EQUALS",
                expected_value="A",
                fixed_value="1",
                priority=110,
                include_not_empty=True,
            ),
        ]
    )

    assert len(result) == 1
    payload = result[0]["screen_edit_payload"]
    assert payload["ruleIds"] == [11, 12]
    assert payload["groupCode"] == "screen_cond_test"
    assert payload["headers"] == [
        {"columnNo": "20", "headerName": "胸部X線判定", "headerContext": ""}
    ]
    assert payload["conditional"] == {
        "blankPolicy": "SKIP",
        "blankOutputValue": "",
        "branches": [
            {"operator": "EQUALS", "expectedValue": "A", "outputValue": "2"},
            {"operator": "NOT_EQUALS", "expectedValue": "A", "outputValue": "1"},
        ],
    }
