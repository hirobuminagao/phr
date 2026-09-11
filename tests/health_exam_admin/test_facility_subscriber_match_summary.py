from inspect import getsource
from pathlib import Path

from apps.health_exam_admin.main import load_facility_summary_rows


def test_facility_summary_counts_manual_subscriber_resolutions_separately() -> None:
    source = getsource(load_facility_summary_rows)

    assert "AS subscriber_match_issue_count" in source
    assert "AS subscriber_match_resolved_count" in source
    assert "el.subscriber_match_status = 'MATCHED'" in source
    assert "el.subscriber_match_method = 'manual'" in source
    assert '"subscriber_match_resolved_count"' in source


def test_facility_summary_labels_subscriber_columns_clearly() -> None:
    template = Path("apps/health_exam_admin/templates/facility_summary.html").read_text(encoding="utf-8")

    assert "加入者NG" in template
    assert "解決済み" in template
    assert "row.subscriber_match_issue_count" in template
    assert "row.subscriber_match_resolved_count" in template
    assert '<th colspan="6"' in template
