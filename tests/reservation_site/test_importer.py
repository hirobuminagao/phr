from __future__ import annotations

import csv
import io

import pytest

from scripts.reservation_site.importer import BASE_HEADERS, OPTION_HEADERS, OPTION_SLOT_COUNT, parse_csv


def make_csv(
    *,
    duplicate_id: bool = False,
    header_option_slots: int = OPTION_SLOT_COUNT,
    row_option_slots: int | None = None,
) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(list(BASE_HEADERS) + list(OPTION_HEADERS) * header_option_slots)
    base = [
        "予約医療機関", "1827", "300272", "28816", "基本コース", "2026/9/5",
        "10:15", "", "3", "", "1", "34100", "", "", "",
        "06139463", "健康保険組合", "2026/9/7 20:13", "2026/9/7 20:15",
        "185291", "466501", "サンプル 花子", "サンプルハナコ", "09000000000",
        "1", "19750203", "0030022", "札幌市", "100", "177040", "",
        "227", "HIAコース", "34100", "9130",
    ]
    row_option_slots = header_option_slots if row_option_slots is None else row_option_slots
    options = (["OP-A", "追加検査", "1000"] if row_option_slots else []) + [""] * max(0, (row_option_slots - 1) * 3)
    writer.writerow(base + options)
    if duplicate_id:
        writer.writerow(base + options)
    return output.getvalue().encode("utf-8-sig")


def test_parse_csv_keeps_all_eight_duplicate_header_option_slots() -> None:
    result = parse_csv(make_csv(), event_id=2)

    assert result["errors"] == []
    assert result["row_count"] == 1
    assert result["rows"][0]["record"]["reservation_id"] == 300272
    assert result["rows"][0]["record"]["insurer_number_match"] == "6139463"
    assert len(result["rows"][0]["options"]) == 8
    assert result["rows"][0]["options"][0]["option_hia_code"] == "OP-A"
    assert result["rows"][0]["options"][1]["option_hia_code"] is None


def test_parse_csv_accepts_rows_with_only_present_option_columns() -> None:
    result = parse_csv(make_csv(header_option_slots=8, row_option_slots=1), event_id=2)

    assert result["errors"] == []
    assert result["header_option_slot_count"] == 8
    assert len(result["rows"][0]["options"]) == 8
    assert result["rows"][0]["options"][0]["option_hia_code"] == "OP-A"
    assert result["rows"][0]["options"][1]["option_hia_code"] is None


def test_parse_csv_accepts_variable_option_headers() -> None:
    result = parse_csv(make_csv(header_option_slots=2, row_option_slots=1), event_id=2)

    assert result["errors"] == []
    assert result["header_option_slot_count"] == 2
    assert len(result["rows"][0]["options"]) == 8


def test_parse_csv_rejects_partial_option_group() -> None:
    raw = make_csv(header_option_slots=2, row_option_slots=1)
    text = raw.decode("utf-8-sig")
    lines = text.splitlines()
    lines[1] += ",extra"

    result = parse_csv(("\r\n".join(lines) + "\r\n").encode("utf-8-sig"), event_id=2)

    assert result["rows"] == []
    assert "3列1組" in result["errors"][0]["message"]


def test_parse_csv_rejects_duplicate_reservation_id() -> None:
    result = parse_csv(make_csv(duplicate_id=True), event_id=2)

    assert result["row_count"] == 2
    assert len(result["rows"]) == 1
    assert "同じ予約ID" in result["errors"][0]["message"]


def test_parse_csv_rejects_header_difference() -> None:
    raw = make_csv().replace(b"hospital_id", b"facility_id", 1)

    with pytest.raises(ValueError, match="CSVヘッダー"):
        parse_csv(raw, event_id=2)
