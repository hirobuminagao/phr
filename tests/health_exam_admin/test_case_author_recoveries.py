from pathlib import Path

from apps.health_exam_admin.main import load_case_author_recoveries


class AuthorRecoveryCursor:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.sql = ""
        self.params: tuple[object, ...] = ()

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.sql = sql
        self.params = params

    def fetchall(self) -> list[dict[str, object]]:
        return self.rows


def test_case_author_recoveries_are_grouped_by_case() -> None:
    cur = AuthorRecoveryCursor(
        [
            {"exam_export_case_id": 10, "namecode": "9N516000000000049"},
            {"exam_export_case_id": 20, "namecode": "9N521000000000049"},
            {"exam_export_case_id": 10, "namecode": "9N526000000000049"},
        ]
    )

    result = load_case_author_recoveries(cur, case_ids=[10, 20, 10])

    assert [row["namecode"] for row in result[10]] == ["9N516000000000049", "9N526000000000049"]
    assert result[20][0]["namecode"] == "9N521000000000049"
    assert "eiv.normalize_reason = 'XML_AUTHOR_ELEMENT'" in cur.sql
    assert "parent_item.annex2_author_item_code" in cur.sql
    assert cur.params == (10, 20)


def test_case_author_recoveries_skip_query_for_empty_page() -> None:
    cur = AuthorRecoveryCursor([])

    assert load_case_author_recoveries(cur, case_ids=[]) == {}
    assert cur.sql == ""


def test_case_pages_show_author_recovery_details() -> None:
    list_template = Path("apps/health_exam_admin/templates/exam_export_cases.html").read_text(encoding="utf-8")
    detail_template = Path("apps/health_exam_admin/templates/exam_export_case_detail.html").read_text(encoding="utf-8")

    assert "data-help-toggle=\"author-recovery-" in list_template
    assert "recovery.parent_items" in list_template
    assert "recovery.source_file_name" in list_template
    assert "XML authorから補完" in detail_template
    assert "value.adopted_normalize_reason == 'XML_AUTHOR_ELEMENT'" in detail_template
