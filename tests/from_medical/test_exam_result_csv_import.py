from __future__ import annotations

import importlib


csv_import = importlib.import_module("scripts.from_medical.02_02_exam_result_csv_import")


class FakeCursor:
    def __init__(self) -> None:
        self.sql = ""
        self.params: tuple[object, ...] = ()

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.sql = sql
        self.params = params

    def fetchall(self) -> list[dict[str, object]]:
        return []


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
