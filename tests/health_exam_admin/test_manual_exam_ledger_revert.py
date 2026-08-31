from typing import Any

from apps.health_exam_admin.main import (
    reconcile_reverted_export_list_cases_after_recheck,
    revert_manual_exam_ledger_to_draft,
)


class FakeCursor:
    def __init__(self) -> None:
        self.executions: list[tuple[str, tuple[Any, ...]]] = []
        self.rowcount = 0
        self._result: Any = None

    def execute(self, sql: str, params: tuple[Any, ...]) -> None:
        normalized = " ".join(sql.split())
        self.executions.append((normalized, params))
        self.rowcount = 0
        if "FROM" in normalized and "exam_ledgers" in normalized and "AS el" in normalized:
            self._result = {
                "exam_ledger_id": 101,
                "event_id": 2,
                "source_type": "MANUAL",
                "row_status": "IMPORTED",
                "xml_export_status": "PENDING",
                "manual_exam_entry_draft_id": 501,
                "draft_status": "APPLIED",
                "item_value_count": 8,
                "case_source_count": 1,
                "adopted_value_count": 4,
                "list_case_count": 2,
            }
        elif normalized.startswith("SELECT DISTINCT oelc.xml_export_list_case_id"):
            self._result = [
                {
                    "xml_export_list_case_id": 1001,
                    "xml_export_list_id": 71,
                    "list_case_status": "READY",
                    "list_name": "確認用",
                },
                {
                    "xml_export_list_case_id": 1002,
                    "xml_export_list_id": 72,
                    "list_case_status": "EXPORTED",
                    "list_name": "提出用",
                },
            ]
        else:
            self._result = None
            if "ops_xml_export_list_cases" in normalized and normalized.startswith("UPDATE"):
                self.rowcount = 2

    def fetchone(self) -> dict[str, Any] | None:
        return self._result if isinstance(self._result, dict) else None

    def fetchall(self) -> list[dict[str, Any]]:
        return self._result if isinstance(self._result, list) else []


def test_revert_removes_active_export_list_cases_and_keeps_recovery_ids() -> None:
    cur = FakeCursor()

    result = revert_manual_exam_ledger_to_draft(
        cur,
        exam_ledger_id=101,
        user={"app_user_id": 9, "employee_no": "E009", "display_name": "担当者"},
    )

    assert result["removed_export_list_case_count"] == 2
    assert result["removed_export_list_ids"] == [71, 72]
    removal_sql, removal_params = next(
        execution
        for execution in cur.executions
        if "ops_xml_export_list_cases" in execution[0] and execution[0].startswith("UPDATE")
    )
    assert "list_case_status = 'REMOVED'" in removal_sql
    assert removal_params == ("E009", "SOURCE_LEDGER_REVERTED: ledger_id=101", 101)


class RecheckCursor:
    def __init__(self) -> None:
        self.executions: list[tuple[str, tuple[Any, ...]]] = []
        self.rowcount = 0
        self._rows = [
            {
                "xml_export_list_case_id": 1001,
                "xml_export_list_id": 71,
                "exam_export_case_id": 201,
                "remove_reason": "SOURCE_LEDGER_REVERTED: ledger_id=101",
                "removed_at": "2026-08-31 10:00:00",
                "export_readiness_status": "EXPORT_READY",
                "export_readiness_reason": "",
            },
            {
                "xml_export_list_case_id": 1002,
                "xml_export_list_id": 72,
                "exam_export_case_id": 202,
                "remove_reason": "SOURCE_LEDGER_REVERTED: ledger_id=102",
                "removed_at": "2026-08-31 10:00:00",
                "export_readiness_status": "BLOCKED",
                "export_readiness_reason": "MISSING",
            },
        ]

    def execute(self, sql: str, params: tuple[Any, ...]) -> None:
        normalized = " ".join(sql.split())
        self.executions.append((normalized, params))
        self.rowcount = 0 if normalized.startswith("SELECT") else 1

    def fetchall(self) -> list[dict[str, Any]]:
        return self._rows


def test_person_recheck_restores_only_ready_reverted_list_cases() -> None:
    cur = RecheckCursor()

    result = reconcile_reverted_export_list_cases_after_recheck(
        cur,
        exam_export_case_ids=[201, 202],
        operator="E009",
    )

    assert result == {
        "considered": 2,
        "restored": 1,
        "kept_removed": 1,
        "list_ids": [71, 72],
    }
    executed_sql = "\n".join(sql for sql, _params in cur.executions)
    assert "list_case_status = 'READY'" in executed_sql
    assert "AUTO_RESTORED_AFTER_CASE_RECHECK" in executed_sql
    assert "CASE_RECHECK_NOT_READY" not in executed_sql
    blocked_params = cur.executions[-1][1]
    assert blocked_params[2] == "CASE_RECHECK_NOT_READY: BLOCKED / MISSING"
