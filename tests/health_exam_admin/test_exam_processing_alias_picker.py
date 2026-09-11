from pathlib import Path

from inspect import getsource

from apps.health_exam_admin.main import run_exam_processing, run_exam_processing_step, selected_exam_processing_alias


def test_selected_exam_processing_alias_returns_only_matching_alias() -> None:
    aliases = [
        {"alias_id": 10, "facility_name": "施設A"},
        {"alias_id": 20, "facility_name": "施設B"},
    ]

    assert selected_exam_processing_alias(aliases, 20) == aliases[1]
    assert selected_exam_processing_alias(aliases, 30) is None
    assert selected_exam_processing_alias(aliases, None) is None


def test_exam_processing_page_uses_single_alias_picker() -> None:
    template = Path("apps/health_exam_admin/templates/exam_processing.html").read_text(encoding="utf-8")

    assert 'name="alias_scope" value="ALL"' in template
    assert 'name="alias_scope" value="SELECTED"' in template
    assert 'type="hidden" name="medical_folder_alias_id"' in template
    assert 'data-modal-open="exam-processing-alias-modal"' in template
    assert "data-exam-processing-alias-select" in template
    assert "multiple" not in template


def test_exam_processing_alias_picker_has_client_side_scope_control() -> None:
    script = Path("apps/health_exam_admin/static/app.js").read_text(encoding="utf-8")

    assert "data-exam-processing-alias-picker" in script
    assert "openButton.disabled = !selectedOnly" in script
    assert 'input.value === "SELECTED"' in script


def test_selected_alias_pipeline_allows_subscriber_only_import_errors() -> None:
    runner_source = getsource(run_exam_processing_step)
    route_source = getsource(run_exam_processing)

    assert 'cmd.append("--continue-on-subscriber-errors")' in runner_source
    assert "continue_on_subscriber_errors=medical_folder_alias_id is not None" in route_source
