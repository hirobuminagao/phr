from apps.health_exam_admin.main import build_hearing_judgement_summary


def test_build_hearing_judgement_summary_converts_oroku_codes_and_counts_ledgers() -> None:
    rows = [
        {"exam_ledger_id": 1, "header_name": "オージオ（右）1000Ｈｚ", "raw_value": "1"},
        {"exam_ledger_id": 1, "header_name": "オージオ（左）1000Ｈｚ", "raw_value": "1"},
        {"exam_ledger_id": 2, "header_name": "オージオ（右）1000Ｈｚ", "raw_value": "1"},
        {"exam_ledger_id": 2, "header_name": "オージオ（左）4000Ｈｚ", "raw_value": "2"},
        {"exam_ledger_id": 3, "header_name": "オージオ（右）4000Ｈｚ", "raw_value": "9"},
    ]

    summary = build_hearing_judgement_summary(rows)

    assert summary["ledger_total"] == 3
    assert summary["normal_ledger_count"] == 1
    assert summary["abnormal_ledger_count"] == 1
    assert summary["unknown_ledger_count"] == 1
    assert summary["abnormal_ledger_rate"] == 50.0
    right_1000 = summary["item_rows"][0]
    assert right_1000["normal_count"] == 2
    assert right_1000["abnormal_count"] == 0


def test_build_hearing_judgement_summary_ignores_blank_values() -> None:
    summary = build_hearing_judgement_summary(
        [{"exam_ledger_id": 1, "header_name": "オージオ（右）1000Ｈｚ", "raw_value": ""}]
    )

    assert summary["available"] is False
    assert summary["ledger_total"] == 0
    assert summary["abnormal_ledger_rate"] is None
