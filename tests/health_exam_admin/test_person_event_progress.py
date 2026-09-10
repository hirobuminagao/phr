from inspect import getsource
from pathlib import Path

from apps.health_exam_admin.main import (
    load_person_event_dashboard_status_options,
    load_person_event_progress_rows,
    person_event_progress,
)
from scripts.health_exam_event.sync_person_event_hia_dashboard_status import create_temp_dashboard_status


def test_person_event_progress_uses_person_event_without_health_result_tables() -> None:
    source = getsource(load_person_event_progress_rows)

    assert ".person_event pe" in source
    assert ".person_event_status_items psi" in source
    assert "HIA_DASHBOARD_STATUS" in source
    assert "load_subscriber_reservation_candidates" in source
    assert "exam_ledgers" not in source
    assert "exam_export_cases" not in source
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
    assert "健診結果" not in template
    assert "予約システムの状態（複数選択）" in template
    assert "HIAダッシュボードの状態（複数選択）" in template
    assert "状態を更新" in template
    assert "data-checkbox-choice-card" in template


def test_dashboard_sync_selects_one_latest_row_per_person_event() -> None:
    source = getsource(create_temp_dashboard_status)

    assert "ROW_NUMBER() OVER" in source
    assert "PARTITION BY p.person_event_id" in source
    assert "ORDER BY d.is_active DESC, d.updated_at DESC, d.hia_dashboard_person_id DESC" in source
    assert "WHERE ranked.dashboard_row_number = 1" in source


def test_dashboard_filter_options_fall_back_to_active_source_rows() -> None:
    source = getsource(load_person_event_dashboard_status_options)

    assert "person_event_status_items" in source
    assert "hia_dashboard_status d" in source
    assert "d.is_active=1" in source
