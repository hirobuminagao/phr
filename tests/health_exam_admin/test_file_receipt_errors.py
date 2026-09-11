from pathlib import Path

from apps.health_exam_admin.main import load_file_receipt_error_detail


class Cursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.receipt = {
            "id": 123,
            "source_path": r"C:\receive\outer.zip",
            "status": "ERROR",
        }
        self.errors = [{"error_id": 1, "phase": "SCAN_FILES"}]

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.calls.append((sql, params))

    def fetchone(self) -> dict[str, object]:
        return self.receipt

    def fetchall(self) -> list[dict[str, object]]:
        return self.errors


def test_file_receipt_error_detail_matches_file_and_nested_member_paths() -> None:
    cur = Cursor()

    receipt, errors = load_file_receipt_error_detail(cur, file_receipt_id=123)

    assert receipt == cur.receipt
    assert errors == cur.errors
    sql, params = cur.calls[1]
    assert "src_file = %s" in sql
    assert "src_file LIKE CONCAT(%s, '!%%')" in sql
    assert params == (
        r"C:\receive\outer.zip",
        r"C:\receive\outer.zip",
        r"C:\receive\outer.zip",
    )


def test_file_receipt_error_status_links_to_detail_page() -> None:
    template = Path("apps/health_exam_admin/templates/file_receipts.html").read_text(encoding="utf-8")

    assert 'href="/file-receipts/{{ row.id }}/errors"' in template
    assert "row.status == 'ERROR'" in template
