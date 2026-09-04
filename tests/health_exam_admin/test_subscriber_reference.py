from apps.health_exam_admin.main import (
    load_subscriber_reference_history,
    load_subscriber_reference_search_rows,
    mask_subscriber_text,
    subscriber_reference_pii_level,
)


class SubscriberHistoryCursor:
    def __init__(self) -> None:
        self.sql = ""

    def execute(self, sql, params=()) -> None:
        self.sql = sql

    def fetchall(self):
        return []


class SubscriberSearchCursor:
    def __init__(self) -> None:
        self.execute_count = 0
        self.search_sql = ""
        self.search_params = ()

    def execute(self, sql, params=()) -> None:
        self.execute_count += 1
        if self.execute_count == 3:
            self.search_sql = sql
            self.search_params = params

    def fetchone(self):
        return {"cnt": 1}

    def fetchall(self):
        if self.execute_count == 1:
            return [{"column_name": "employee_code"}, {"column_name": "qualification_lost_date"}]
        return [
            {
                "subscriber_id": 10,
                "hia_subscriber_id": "HIA-10",
                "name_kana_full": "テスト タロウ",
                "birth": "1990-01-02",
                "insurer_number": "06139463",
                "insurance_symbol": "ABC",
                "insurance_number": "123456",
                "employee_code": "E10",
                "qualification_lost_date": None,
                "updated_at": "2026-09-04 10:00:00",
                "latest_case_id": 501,
                "latest_exam_date": "2026-08-20",
                "latest_exam_facility_name": "札幌健診センター",
                "event_count": 1,
                "case_count": 1,
                "dashboard_status": "受診済み",
                "dashboard_reservation_date": "2026-08-01",
                "dashboard_exam_date": "2026-08-20",
                "dashboard_medical_institution": "札幌健診センター",
                "dashboard_course_name": "定期健診",
                "dashboard_updated_at": "2026-09-03 09:00:00",
            }
        ]


def test_mask_subscriber_text_keeps_only_requested_suffix() -> None:
    assert mask_subscriber_text("12345678", keep_end=4) == "****5678"
    assert mask_subscriber_text("ABCD", keep_end=2) == "**CD"
    assert mask_subscriber_text(None) == "未登録"


def test_subscriber_reference_pii_level_defaults_to_hidden() -> None:
    assert subscriber_reference_pii_level({"permissions": []}) == "HIDDEN"


def test_subscriber_reference_pii_level_uses_most_restrictive_permission() -> None:
    assert subscriber_reference_pii_level(
        {
            "permissions": [
                "subscriber_reference.pii.full",
                "subscriber_reference.pii.masked",
                "subscriber_reference.pii.hidden",
            ]
        }
    ) == "HIDDEN"


def test_system_manager_can_use_full_display() -> None:
    assert subscriber_reference_pii_level({"permissions": ["users.manage"]}) == "FULL"


def test_admin_all_permissions_still_resolves_to_full() -> None:
    assert subscriber_reference_pii_level(
        {
            "permissions": [
                "users.manage",
                "subscriber_reference.pii.full",
                "subscriber_reference.pii.masked",
                "subscriber_reference.pii.hidden",
            ]
        }
    ) == "FULL"


def test_subscriber_search_includes_latest_active_dashboard_status() -> None:
    cur = SubscriberSearchCursor()

    rows = load_subscriber_reference_search_rows(
        cur,
        filters={"hia_subscriber_id": "HIA-10"},
        pii_level="MASKED",
    )

    assert len(rows) == 1
    assert rows[0]["dashboard_status"] == "受診済み"
    assert rows[0]["dashboard_medical_institution"] == "札幌健診センター"
    assert rows[0]["latest_case_id"] == 501
    assert rows[0]["latest_exam_facility_name"] == "札幌健診センター"
    assert "ROW_NUMBER() OVER" in cur.search_sql
    assert "PARTITION BY hds.subscribers_id" in cur.search_sql
    assert "WHERE hds.is_active = 1" in cur.search_sql
    assert "LEFT JOIN" in cur.search_sql
    assert "ORDER BY candidate.exam_date DESC, candidate.exam_export_case_id DESC" in cur.search_sql
    assert cur.search_params == ("%HIA-10%",)


def test_subscriber_history_values_are_selected_only_for_full_display(monkeypatch) -> None:
    monkeypatch.setattr("apps.health_exam_admin.main.manual_exam_entry_table_exists", lambda *args: True)
    masked_cursor = SubscriberHistoryCursor()
    full_cursor = SubscriberHistoryCursor()

    load_subscriber_reference_history(masked_cursor, subscriber_id=10, include_values=False)
    load_subscriber_reference_history(full_cursor, subscriber_id=10, include_values=True)

    assert "old_value" not in masked_cursor.sql
    assert "new_value" not in masked_cursor.sql
    assert "old_value, new_value" in full_cursor.sql
