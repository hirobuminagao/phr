from inspect import getsource
from pathlib import Path

from apps.health_exam_admin.main import (
    build_person_event_progress_pagination,
    load_person_event_dashboard_status_options,
    load_person_event_exam_progress,
    load_person_event_progress_rows,
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
    assert "reservation_status_raw IN" in source
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
    assert "予約システムの状態（複数選択）" in template
    assert "HIAダッシュボードの状態（複数選択）" in template
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
        reservation_statuses=["1", "3"],
        dashboard_statuses=["受診済み", "結果待ち"],
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
    assert "%E6%9C%AD%E5%B9%8C+%E5%A4%AA%E9%83%8E" in pagination["next_url"]
    assert pagination["pages"][0]["page"] == 1
    assert pagination["pages"][-1]["page"] == 244


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
