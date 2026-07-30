from __future__ import annotations

import importlib


scan_files = importlib.import_module("scripts.from_medical.01_scan_files")


class FakeCursor:
    def __init__(self) -> None:
        self.sql = ""
        self.params: tuple[object, ...] = ()

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.sql = sql
        self.params = params


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
