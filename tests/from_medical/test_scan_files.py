from __future__ import annotations

import importlib


scan_files = importlib.import_module("scripts.from_medical.01_scan_files")


class FakeCursor:
    lastrowid = 123

    def __init__(self) -> None:
        self.sql = ""
        self.params: tuple[object, ...] = ()

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.sql = sql
        self.params = params


class ExistingReceiptCursor(FakeCursor):
    rowcount = 1

    def __init__(self, row: dict[str, object] | None) -> None:
        super().__init__()
        self.row = row

    def fetchone(self) -> dict[str, object] | None:
        return self.row


def test_supersede_only_unprocessed_receipts_for_same_path() -> None:
    cur = FakeCursor()

    scan_files.supersede_unprocessed_path_receipts(
        cur,
        event_id=2,
        relative_path="facility/result.csv",
        current_receipt_id=99,
    )

    assert "status = 'SUPERSEDED'" in cur.sql
    assert "status IN ('DISCOVERED', 'READY', 'WAITING_CONFIRM')" in cur.sql
    assert cur.params == (99, 2, "facility/result.csv", 99)


def test_find_existing_receipt_loads_alias_state() -> None:
    cur = ExistingReceiptCursor({"id": 20, "medical_folder_alias_id": None})

    row = scan_files.find_existing_receipt(
        cur,
        event_id=2,
        relative_path="facility/result.csv",
        file_sha256="abc",
    )

    assert row == {"id": 20, "medical_folder_alias_id": None}
    assert "medical_folder_alias_id" in cur.sql


def test_backfill_receipt_alias_only_updates_null_alias() -> None:
    cur = ExistingReceiptCursor(None)

    updated = scan_files.backfill_receipt_alias_if_missing(
        cur,
        receipt_id=20,
        medical_folder_alias_id=12,
    )

    assert updated
    assert "medical_folder_alias_id IS NULL" in cur.sql
    assert cur.params == (12, 20)


def test_absent_alias_folder_is_not_a_scan_error(tmp_path) -> None:
    summary = scan_files.ScanSummary(event_id=2)

    keep_scanning = scan_files.scan_alias_files(
        object(),
        run_id=None,
        event_id=2,
        insurer_number="06139463",
        root=tmp_path,
        alias={"src_folder_raw": "1310438796_未受領施設"},
        summary=summary,
        dry_run=True,
        limit=0,
        chunk_size=1024,
        master_db="phr_master",
    )

    assert keep_scanning
    assert summary.edit_folders_missing == 0
    assert summary.errors == 0


def test_insert_file_receipt_persists_alias_id(tmp_path) -> None:
    cur = FakeCursor()
    path = tmp_path / "result.csv"

    receipt_id = scan_files.insert_file_receipt(
        cur,
        event_id=2,
        source_path=str(path),
        relative_path="facility/result.csv",
        path=path,
        file_type="CSV",
        file_sha256="abc",
        file_size=10,
        run_id=9,
        insurer_number="06139463",
        exam_facility_id=70,
        medical_folder_alias_id=12,
        facility_code="0110119674",
        facility_name="facility",
        actual_header_sha256=None,
        actual_character_encoding=None,
        matched_csv_format_version_id=None,
        status="READY",
        summary_message=None,
    )

    assert receipt_id == 123
    assert "medical_folder_alias_id" in cur.sql
    assert cur.params[13] == 12
