from apps.health_exam_admin.main import build_person_id_custom_row, parse_person_id_custom_rows


def test_build_person_id_custom_without_database_lookup() -> None:
    row = build_person_id_custom_row(
        row_no=1,
        raw_line="06139463\t123\t456\t1990-01-02",
        parsed={
            "insurer_number": "06139463",
            "insurance_symbol": "123",
            "insurance_number": "456",
            "birthdate": "1990-01-02",
        },
    )

    assert row["ok"] is True
    assert row["person_id_custom"]
    assert row["reason"] == ""


def test_bulk_rows_can_use_fixed_insurer_number() -> None:
    rows = parse_person_id_custom_rows(
        raw_text="123\t456\t19900102\n789\t012\t19851231",
        delimiter="tab",
        custom_delimiter="",
        has_header=False,
        column_map=["insurance_symbol", "insurance_number", "birthdate"],
        fixed_insurer_number="06139463",
    )

    assert len(rows) == 2
    assert all(row["ok"] for row in rows)
    assert all(row["parsed"]["insurer_number"] == "06139463" for row in rows)
    assert rows[0]["person_id_custom"] != rows[1]["person_id_custom"]


def test_invalid_bulk_row_returns_row_level_error() -> None:
    rows = parse_person_id_custom_rows(
        raw_text="06139463\t123\t456\t",
        delimiter="tab",
        custom_delimiter="",
        has_header=False,
        column_map=["insurer_number", "insurance_symbol", "insurance_number", "birthdate"],
        fixed_insurer_number="",
    )

    assert rows[0]["ok"] is False
    assert rows[0]["person_id_custom"] is None
    assert "birthdate" in rows[0]["reason"]


def test_blank_line_is_preserved_as_error_row() -> None:
    rows = parse_person_id_custom_rows(
        raw_text="06139463\t123\t456\t19900102\n\n06139463\t789\t012\t19851231",
        delimiter="tab",
        custom_delimiter="",
        has_header=False,
        column_map=["insurer_number", "insurance_symbol", "insurance_number", "birthdate"],
        fixed_insurer_number="",
    )

    assert len(rows) == 3
    assert rows[0]["ok"] is True
    assert rows[1]["row_no"] == 2
    assert rows[1]["raw_line"] == ""
    assert rows[1]["ok"] is False
    assert rows[1]["person_id_custom"] is None
    assert rows[1]["reason"]
    assert rows[2]["row_no"] == 3
    assert rows[2]["ok"] is True
