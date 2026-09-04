from inspect import getsource

from apps.health_exam_admin.main import (
    _subscriber_export_update_values,
    insurer_number_matches_event,
    load_subscriber_match_candidate_rows,
    load_subscriber_match_resolution_counts,
    load_subscriber_match_resolved_rows,
    load_subscriber_match_workload_counts,
    subscriber_match_issue_where_parts,
)


def test_insurer_number_match_ignores_leading_zeroes() -> None:
    assert insurer_number_matches_event("06139463", "6139463") is True
    assert insurer_number_matches_event("6139463", "06139463") is True


def test_insurer_number_match_detects_difference_and_missing_source() -> None:
    assert insurer_number_matches_event("06139463", "06139464") is False
    assert insurer_number_matches_event(None, "06139463") is False
    assert insurer_number_matches_event("06139463", None) is None


def test_subscriber_apply_sets_insurer_export_without_replacing_raw() -> None:
    updates, applied_fields = _subscriber_export_update_values({"insurer_number": "6139463"})

    assert updates["insurer_number_export_value"] == "06139463"
    assert updates["insurer_number_source"] == "SUBSCRIBER"
    assert updates["insurer_number_completion_status"] == "FILLED_FROM_SUBSCRIBER"
    assert "insurer_number" not in updates
    assert applied_fields == ["insurer_number"]


class Cursor:
    def __init__(self) -> None:
        self.sql = ""
        self.params: tuple[object, ...] = ()

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.sql = sql
        self.params = params

    def fetchone(self) -> dict[str, object]:
        return {
            "resolved_person_count": 12,
            "resolved_ledger_count": 15,
            "current_confirmation_ledger_count": 156,
            "listed_ledger_count": 171,
        }

    def fetchall(self) -> list[dict[str, object]]:
        return []


def test_load_subscriber_match_resolution_counts_uses_manual_unique_people() -> None:
    cursor = Cursor()

    result = load_subscriber_match_resolution_counts(cursor, event_id=2)

    assert result == {"resolved_person_count": 12, "resolved_ledger_count": 15}
    assert "COUNT(DISTINCT new_subscriber_id)" in cursor.sql
    assert "COUNT(DISTINCT exam_ledger_id)" in cursor.sql
    assert "new_subscriber_match_method = 'manual'" in cursor.sql
    assert cursor.params == (2,)


def test_load_subscriber_match_workload_counts_adds_resolved_and_current_ledgers() -> None:
    cursor = Cursor()

    result = load_subscriber_match_workload_counts(cursor, event_id=2)

    assert result == {
        "resolved_ledger_count": 15,
        "current_confirmation_ledger_count": 156,
        "listed_ledger_count": 171,
    }
    assert "resolved.resolved_ledger_count + current_issues.current_confirmation_ledger_count" in cursor.sql
    assert "new_subscriber_match_method = 'manual'" in cursor.sql
    assert "subscriber_match_method, '') IN ('identity_hash', 'manual')" in cursor.sql
    assert "MANUAL_CONFIRMED" in cursor.sql
    assert cursor.params == (2, 2)


def test_default_subscriber_match_issues_exclude_confirmed_manual_ledgers() -> None:
    where_parts, params = subscriber_match_issue_where_parts(
        {
            "event_id": "2",
            "status_filter": "",
            "q": "",
            "facility_q": "",
            "facility_codes": "",
            "exam_month": "",
        }
    )
    sql = " ".join(where_parts)

    assert "MANUAL_CONFIRMED" in sql
    assert "el.subscriber_id IS NOT NULL" in sql
    assert "('identity_hash', 'manual')" in sql
    assert params == ["2"]


def test_resolved_rows_require_current_manual_match_and_filter_query() -> None:
    cursor = Cursor()

    rows = load_subscriber_match_resolved_rows(cursor, event_id=2, query="山田", limit=100)

    assert rows == []
    assert "el.subscriber_match_status = 'MATCHED'" in cursor.sql
    assert "el.subscriber_match_method = 'manual'" in cursor.sql
    assert "latest.new_subscriber_match_status = 'MATCHED'" in cursor.sql
    assert "el.name_full_raw LIKE %s" in cursor.sql
    assert cursor.params[0] == 2
    assert cursor.params[-1] == 100


def test_candidate_insurer_scope_uses_normalized_number_comparison() -> None:
    source = getsource(load_subscriber_match_candidate_rows)

    assert "TRIM(LEADING '0' FROM REGEXP_REPLACE(COALESCE(s.insurer_number, ''), '[^0-9]', ''))" in source
    assert "TRIM(LEADING '0' FROM REGEXP_REPLACE(COALESCE(e.insurer_number, ''), '[^0-9]', ''))" in source
    assert source.index("if not where_parts and not filter_parts:") < source.index("include_other_insurers =")


def test_candidate_rows_restore_search_values_from_received_raw_fields() -> None:
    source = getsource(load_subscriber_match_candidate_rows)

    assert 'normalize_name_kana_full(str(ledger.get("name_kana_raw")))' in source
    assert 'normalize_insurance_number(str(ledger.get("insurance_number_raw")))' in source


def test_candidate_rows_include_latest_active_dashboard_status() -> None:
    source = getsource(load_subscriber_match_candidate_rows)

    assert "hds.status AS hia_dashboard_status" in source
    assert "hds.medical_institution AS hia_dashboard_medical_institution" in source
    assert "hds_latest.subscribers_id = s.id" in source
    assert "ORDER BY hds_latest.updated_at DESC" in source
