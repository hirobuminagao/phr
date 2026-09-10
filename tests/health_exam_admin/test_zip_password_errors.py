from apps.health_exam_admin.main import load_zip_password_error_rows


class Cursor:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.sql = ""
        self.params: tuple[object, ...] = ()

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.sql = sql
        self.params = params

    def fetchall(self) -> list[dict[str, object]]:
        return self.rows


def test_zip_password_errors_include_scan_and_import_with_labels() -> None:
    cur = Cursor(
        [
            {"phase": "SCAN_FILES", "error_code": "ZIP_PASSWORD_NOT_FOUND"},
            {"phase": "IMPORT_XML", "error_code": "ZIP_DECRYPT_FAILED"},
        ]
    )

    rows = load_zip_password_error_rows(cur)

    assert "ZIP_PASSWORD_NOT_FOUND" in cur.sql
    assert "ZIP_DECRYPT_FAILED" in cur.sql
    assert rows[0]["phase_label"] == "scan"
    assert rows[0]["error_label"] == "パスワード未登録"
    assert rows[1]["phase_label"] == "XML import"
    assert rows[1]["error_label"] == "復号失敗"


def test_zip_password_error_filters_are_bound_parameters() -> None:
    cur = Cursor([])

    load_zip_password_error_rows(cur, query="札幌", phase="IMPORT_XML", limit=25)

    assert cur.params == ("IMPORT_XML", *("%札幌%",) * 7, 25)
