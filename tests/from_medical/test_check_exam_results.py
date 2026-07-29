from __future__ import annotations

import importlib


check_results = importlib.import_module("scripts.from_medical.03_check_exam_results")


class FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.rowcount = 0

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.calls.append((sql, params))
        self.rowcount = 1


def test_delete_existing_results_deletes_xml_and_csv_separately() -> None:
    cur = FakeCursor()

    deleted = check_results.delete_existing_results(
        cur,
        health_db="health_exam_result",
        ledger_refs=[("XML", 10), ("CSV", 20), ("CSV", 21)],
    )

    assert deleted == 2
    assert len(cur.calls) == 2
    assert "xml_ledger_id IN" in cur.calls[0][0]
    assert cur.calls[0][1] == ("XML", 10)
    assert "csv_row_ledger_id IN" in cur.calls[1][0]
    assert cur.calls[1][1] == ("CSV", 20, 21)


def test_insert_check_result_uses_csv_row_ledger_id_for_csv() -> None:
    cur = FakeCursor()
    article44_result = {
        detail_no: check_results.CheckResult(check_results.STATUS_OK, None)
        for detail_no in check_results.ARTICLE44_CHECKERS
    }

    check_results.insert_check_result(
        cur,
        health_db="health_exam_result",
        ledger={
            "ledger_type": "CSV",
            "id": 20,
            "event_id": 2,
            "subscriber_id": 100,
            "hia_subscriber_id": "HIA-1",
        },
        article44_result=article44_result,
        legal_result=check_results.RESULT_OK,
        specific_result=None,
        legal_summary=None,
        specific_summary=None,
    )

    sql, params = cur.calls[0]

    assert "`ledger_type`" in sql
    assert "`xml_ledger_id`" in sql
    assert "`csv_row_ledger_id`" in sql
    assert params[0:4] == ("CSV", None, 20, 2)


def test_update_csv_row_ledger_check_updates_csv_table() -> None:
    cur = FakeCursor()

    check_results.update_csv_row_ledger_check(
        cur,
        health_db="health_exam_result",
        ledger_id=20,
        check_status="NG",
        check_reason="missing",
    )

    sql, params = cur.calls[0]

    assert "csv_row_ledger" in sql
    assert "xml_ledger" not in sql
    assert params == ("NG", "missing", 20)
