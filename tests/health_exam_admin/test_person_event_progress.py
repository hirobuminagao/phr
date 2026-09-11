from inspect import getsource
from pathlib import Path

from apps.health_exam_admin.main import (
    build_person_event_progress_pagination,
    load_person_event_dashboard_status_options,
    load_person_event_exam_progress,
    load_person_event_facility_subscriber_ids,
    load_person_event_month_options,
    load_person_event_month_subscriber_ids,
    load_person_event_progress_rows,
    prepare_person_event_reservation_status_filter,
    person_event_progress,
)
from scripts.health_exam_event.sync_person_event_hia_dashboard_status import (
    create_temp_dashboard_status,
    insert_status_items,
)


def test_person_event_progress_uses_person_event_without_health_result_tables() -> None:
    source = getsource(load_person_event_progress_rows)

    assert ".person_event pe" in source
    assert ".person_event_status_items psi" in source
    assert "HIA_DASHBOARD_STATUS" in source
    assert "load_subscriber_reservation_candidates" in source
    assert "load_person_event_exam_progress" in source
    assert "prepare_person_event_reservation_status_filter" in source
    assert "HIA_DASHBOARD_STATUS" in source
    assert "per_page: int = 30" in source


def test_person_event_progress_route_uses_subscriber_reference_permission() -> None:
    source = getsource(person_event_progress)

    assert "can_view_subscriber_reference" in source
    assert "PERSONAL_INFO_VIEW_PERSON_EVENT_PROGRESS" in source


def test_person_event_progress_template_has_base_progress_columns() -> None:
    template = Path("apps/health_exam_admin/templates/person_event_progress.html").read_text(encoding="utf-8")

    assert "イベント対象者" in template
    assert "予約システム" in template
    assert "HIAダッシュボード" in template
    assert "加入者詳細" in template
    assert "結果受領" in template
    assert "case・出力" in template
    assert "予約システムの状態" in template
    assert "HIAダッシュボードの状態" in template
    assert 'name="exam_month"' in template
    assert "data-month-picker" in template
    assert "状態を更新" in template
    assert "data-checkbox-choice-card" in template
    assert template.count("{{ progress_pagination(pagination") == 2


def test_exam_progress_is_loaded_only_for_current_page_subscribers() -> None:
    source = getsource(load_person_event_exam_progress)

    assert "subscriber_id IN" in source
    assert "exam_ledgers" in source
    assert "exam_export_cases" in source
    assert "case_lifecycle_status='ACTIVE'" in source
    assert "ops_xml_export_list_cases" in source


def test_person_event_progress_pagination_preserves_multi_value_filters() -> None:
    pagination = build_person_event_progress_pagination(
        event_id=2,
        query="札幌 太郎",
        insurance_symbol="ABC",
        insurance_number="001234",
        relationship="本人",
        reservation_statuses=["1", "3"],
        dashboard_statuses=["受診済み", "結果待ち"],
        exam_months=["2026-05", "2026-06"],
        case_presence="EXISTS",
        exam_facility_id=7386,
        exam_facility_display="札幌健診センター",
        total_count=7316,
        row_count=30,
        page=4,
        page_count=244,
        per_page=30,
    )

    assert pagination["start"] == 91
    assert pagination["end"] == 120
    assert pagination["has_previous"] is True
    assert pagination["has_next"] is True
    assert "reservation_status=1&reservation_status=3" in pagination["next_url"]
    assert "exam_facility_id=7386" in pagination["next_url"]
    assert "exam_month=2026-05%2C+2026-06" in pagination["next_url"]
    assert "case_presence=EXISTS" in pagination["next_url"]
    assert "insurance_symbol=ABC" in pagination["next_url"]
    assert "insurance_number=001234" in pagination["next_url"]
    assert "%E6%9C%AC%E4%BA%BA" in pagination["next_url"]
    assert "%E6%9C%AD%E5%B9%8C+%E5%A4%AA%E9%83%8E" in pagination["next_url"]
    assert pagination["pages"][0]["page"] == 1
    assert pagination["pages"][-1]["page"] == 244


def test_person_event_progress_can_filter_by_facility_across_stages() -> None:
    source = getsource(load_person_event_progress_rows)
    facility_source = getsource(load_person_event_facility_subscriber_ids)
    template = Path("apps/health_exam_admin/templates/person_event_progress.html").read_text(encoding="utf-8")

    assert "load_person_event_facility_subscriber_ids" in source
    assert "pe.subscriber_id IN" in source
    assert "exam_ledgers" in facility_source
    assert "exam_export_cases" in facility_source
    assert "reservation_site_records" in facility_source
    assert 'name="exam_facility_id"' in template
    assert "data-alias-facility-picker-modal" in template


def test_person_event_progress_month_filter_covers_reservation_ledger_and_case() -> None:
    option_source = getsource(load_person_event_month_options)
    filter_source = getsource(load_person_event_month_subscriber_ids)
    loader_source = getsource(load_person_event_progress_rows)

    for source in (option_source, filter_source):
        assert "reservation_site_records" in source
        assert "exam_ledgers" in source
        assert "exam_export_cases" in source
        assert "CONCAT(YEAR(" in source
        assert "%%Y-%%m" not in source
    assert "load_person_event_month_subscriber_ids" in loader_source


def test_person_event_progress_can_filter_active_case_presence() -> None:
    source = getsource(load_person_event_progress_rows)
    template = Path("apps/health_exam_admin/templates/person_event_progress.html").read_text(encoding="utf-8")

    assert "case_filter.case_lifecycle_status='ACTIVE'" in source
    assert 'name="case_presence"' in template
    assert "caseあり" in template
    assert "caseなし" in template


def test_person_event_progress_can_filter_subscriber_insurance_and_relationship() -> None:
    source = getsource(load_person_event_progress_rows)
    template = Path("apps/health_exam_admin/templates/person_event_progress.html").read_text(encoding="utf-8")

    assert "normalize_insurance_symbol" in source
    assert "normalize_insurance_number" in source
    assert "s.relationship_name LIKE" in source
    assert "s.relationship_code" not in source
    assert 'name="insurance_symbol"' in template
    assert 'name="insurance_number"' in template
    assert 'name="relationship"' in template


def test_reservation_status_filter_is_prepared_once_for_count_and_rows() -> None:
    source = getsource(prepare_person_event_reservation_status_filter)
    loader_source = getsource(load_person_event_progress_rows)

    assert "CREATE TEMPORARY TABLE tmp_person_event_reservation_status_filter" in source
    assert "reservation_status_raw IN" in source
    assert "ADD PRIMARY KEY (subscriber_id)" in source
    assert loader_source.count("prepare_person_event_reservation_status_filter") == 1
    assert "SELECT subscriber_id FROM tmp_person_event_reservation_status_filter" in loader_source


def test_dashboard_sync_selects_one_latest_row_per_person_event() -> None:
    source = getsource(create_temp_dashboard_status)

    assert "ROW_NUMBER() OVER" in source
    assert "PARTITION BY p.person_event_id" in source
    assert "ORDER BY d.is_active DESC, d.updated_at DESC, d.hia_dashboard_person_id DESC" in source
    assert "CAST(d.updated_at AS DATETIME(6)) AS source_updated_at" in source
    assert "WHERE ranked.dashboard_row_number = 1" in source


def test_dashboard_filter_options_fall_back_to_active_source_rows() -> None:
    source = getsource(load_person_event_dashboard_status_options)

    assert "person_event_status_items" in source
    assert "hia_dashboard_status d" in source
    assert "d.is_active=1" in source
    assert source.count("COLLATE utf8mb4_unicode_ci AS value_code") == 2


def test_dashboard_code_status_values_are_written_to_value_code_column() -> None:
    source = getsource(insert_status_items)

    assert "NULL, NULL, NULL, NULLIF(t.{column}, '')" in source
