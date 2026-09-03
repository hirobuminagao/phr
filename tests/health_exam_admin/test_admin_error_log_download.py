from apps.health_exam_admin.main import format_app_error_log_text


def test_format_app_error_log_text_matches_visible_log_content() -> None:
    text = format_app_error_log_text(
        {
            "log_id": "ERR-20260903095149-ac4346",
            "created_at": "2026-09-03 09:51:49",
            "method": "GET",
            "path": "/subscriber-match-review",
            "query_string": "event_id=2",
            "status_code": 500,
            "exception_type": "DatabaseError",
            "exception_message": "collation mismatch",
            "employee_no": "1107858",
            "client_ip": "127.0.0.1",
            "traceback_text": "Traceback text",
        }
    )

    assert "ログID: ERR-20260903095149-ac4346" in text
    assert "リクエスト: GET /subscriber-match-review?event_id=2" in text
    assert "例外: DatabaseError: collation mismatch" in text
    assert text.endswith("Traceback text")
