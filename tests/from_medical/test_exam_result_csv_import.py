from __future__ import annotations

import importlib


csv_import = importlib.import_module("scripts.from_medical.02_02_exam_result_csv_import")


def test_resolve_insurer_number_uses_csv_then_receipt_then_event() -> None:
    assert csv_import.resolve_insurer_number("11111111", "22222222", "33333333") == "11111111"
    assert csv_import.resolve_insurer_number(None, "22222222", "33333333") == "22222222"
    assert csv_import.resolve_insurer_number("", None, "33333333") == "33333333"
    assert csv_import.resolve_insurer_number(None, None, None) is None


def test_fill_missing_report_codes_uses_event_reference_age() -> None:
    fields = {"birthdate": "1986/11/30"}

    csv_import.fill_missing_report_codes(
        fields,
        event_age_rule={
            "age_rule_type": "FIXED_DATE",
            "age_reference_date": "2026-11-30",
        },
    )

    assert fields["health_exam_report_category"] == "10"
    assert fields["program_code"] == "010"


def test_fill_missing_report_codes_uses_other_codes_outside_age_range() -> None:
    fields = {"birthdate": "1986/12/01"}

    csv_import.fill_missing_report_codes(
        fields,
        event_age_rule={
            "age_rule_type": "FIXED_DATE",
            "age_reference_date": "2026-11-30",
        },
    )

    assert fields["health_exam_report_category"] == "40"
    assert fields["program_code"] == "990"

    fields = {"birthdate": "1951/11/30"}
    csv_import.fill_missing_report_codes(
        fields,
        event_age_rule={
            "age_rule_type": "FIXED_DATE",
            "age_reference_date": "2026-11-30",
        },
    )

    assert fields["health_exam_report_category"] == "40"
    assert fields["program_code"] == "990"


def test_fill_missing_report_codes_includes_age_74_boundary() -> None:
    fields = {"birthdate": "1951/12/01"}

    csv_import.fill_missing_report_codes(
        fields,
        event_age_rule={
            "age_rule_type": "FIXED_DATE",
            "age_reference_date": "2026-11-30",
        },
    )

    assert fields["health_exam_report_category"] == "10"
    assert fields["program_code"] == "010"


def test_fill_missing_report_codes_preserves_received_values() -> None:
    fields = {
        "birthdate": "1986/11/30",
        "health_exam_report_category": "40",
        "program_code": "990",
    }

    csv_import.fill_missing_report_codes(
        fields,
        event_age_rule={
            "age_rule_type": "FIXED_DATE",
            "age_reference_date": "2026-11-30",
        },
    )

    assert fields["health_exam_report_category"] == "40"
    assert fields["program_code"] == "990"


class FakeCursor:
    def __init__(self) -> None:
        self.sql = ""
        self.params: tuple[object, ...] = ()
        self.lastrowid = 1
        self.fetchone_result: dict[str, object] | None = None

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.sql = sql
        self.params = params

    def fetchall(self) -> list[dict[str, object]]:
        return []

    def fetchone(self) -> dict[str, object] | None:
        return self.fetchone_result


def test_fetch_csv_file_receipts_excludes_discovered() -> None:
    cur = FakeCursor()
    config = csv_import.ImportConfig(
        event_id=2,
        health_db="health_exam_result",
        dev_db="dev_phr",
        master_db="phr_master",
        dry_run=False,
        limit=0,
        include_imported=False,
    )

    csv_import.fetch_csv_file_receipts(cur, config=config)

    assert "DISCOVERED" not in cur.params
    assert cur.params == ("READY", "WAITING_CONFIRM", 2)
    assert "import_resume_approved = 1" in cur.sql


def test_fetch_csv_file_receipts_can_include_imported() -> None:
    cur = FakeCursor()
    config = csv_import.ImportConfig(
        event_id=None,
        health_db="health_exam_result",
        dev_db="dev_phr",
        master_db="phr_master",
        dry_run=False,
        limit=10,
        include_imported=True,
    )

    csv_import.fetch_csv_file_receipts(cur, config=config)

    assert cur.params == ("READY", "IMPORTED", "WAITING_CONFIRM", 10)
    assert "status IN (%s, %s)" in cur.sql
    assert "LIMIT %s" in cur.sql


def test_upsert_row_ledger_inserts_mapped_report_category() -> None:
    cur = FakeCursor()
    config = csv_import.ImportConfig(
        event_id=2,
        health_db="health_exam_result",
        dev_db="dev_phr",
        master_db="phr_master",
        dry_run=False,
        limit=0,
        include_imported=False,
    )

    ledger_id, action = csv_import.upsert_row_ledger(
        cur,
        config=config,
        run_id=10,
        file_receipt={"id": 20, "event_id": 2},
        fmt={"header_sha256": "a" * 64, "mapping_version": "TEST_V1"},
        src_row_no=2,
        row_hash="b" * 64,
        raw_row_json="[]",
        ledger_fields={"health_exam_report_category": "10"},
        row_status="READY",
        row_reason=None,
        exam_item_count=0,
        exam_item_error_count=0,
    )

    assert (ledger_id, action) == (1, "inserted")
    assert "health_exam_report_category" in cur.sql
    assert "10" in cur.params
    assert cur.sql.count("%s") == len(cur.params)


def test_upsert_row_ledger_uses_file_receipt_facility_snapshot_when_csv_is_unmapped() -> None:
    cur = FakeCursor()
    config = csv_import.ImportConfig(
        event_id=2,
        health_db="health_exam_result",
        dev_db="dev_phr",
        master_db="phr_master",
        dry_run=False,
        limit=0,
        include_imported=False,
    )

    csv_import.upsert_row_ledger(
        cur,
        config=config,
        run_id=10,
        file_receipt={
            "id": 20,
            "event_id": 2,
            "exam_facility_id": 30,
            "facility_code": "4710114044",
            "facility_name": "医療法人　禄寿会　小禄病院",
        },
        fmt={"header_sha256": "a" * 64, "mapping_version": "OROKU_TEST"},
        src_row_no=2,
        row_hash="b" * 64,
        raw_row_json="[]",
        ledger_fields={},
        row_status="READY",
        row_reason=None,
        exam_item_count=0,
        exam_item_error_count=0,
    )

    assert "4710114044" in cur.params
    assert "医療法人　禄寿会　小禄病院" in cur.params


def test_upsert_row_ledger_updates_mapped_report_category() -> None:
    cur = FakeCursor()
    cur.fetchone_result = {"csv_row_ledger_id": 7}
    config = csv_import.ImportConfig(
        event_id=2,
        health_db="health_exam_result",
        dev_db="dev_phr",
        master_db="phr_master",
        dry_run=False,
        limit=0,
        include_imported=False,
    )

    ledger_id, action = csv_import.upsert_row_ledger(
        cur,
        config=config,
        run_id=10,
        file_receipt={"id": 20, "event_id": 2},
        fmt={"header_sha256": "a" * 64, "mapping_version": "TEST_V1"},
        src_row_no=2,
        row_hash="b" * 64,
        raw_row_json="[]",
        ledger_fields={"health_exam_report_category": "20"},
        row_status="READY",
        row_reason=None,
        exam_item_count=0,
        exam_item_error_count=0,
    )

    assert (ledger_id, action) == (7, "updated")
    assert "health_exam_report_category = %s" in cur.sql
    assert "20" in cur.params
    assert cur.sql.count("%s") == len(cur.params)
