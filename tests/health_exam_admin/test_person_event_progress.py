from inspect import getsource
from pathlib import Path

from apps.health_exam_admin.main import load_person_event_progress_rows, person_event_progress


def test_person_event_progress_uses_person_event_without_health_result_tables() -> None:
    source = getsource(load_person_event_progress_rows)

    assert ".person_event pe" in source
    assert ".person_event_status_items psi" in source
    assert "HIA_DASHBOARD_STATUS" in source
    assert "load_subscriber_reservation_candidates" in source
    assert "exam_ledgers" not in source
    assert "exam_export_cases" not in source


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
