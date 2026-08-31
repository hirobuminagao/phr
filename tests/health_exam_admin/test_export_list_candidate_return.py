from urllib.parse import parse_qs, urlsplit

from apps.health_exam_admin.main import export_list_candidate_return_url


def test_export_list_candidate_return_url_preserves_candidate_filters() -> None:
    url = export_list_candidate_return_url(
        xml_export_list_id=12,
        return_query=(
            "show_candidates=1&case_q=abc&facility_codes=001%0A002"
            "&exam_item_namecode=A&exam_item_namecode=B&exam_item_match_mode=all"
        ),
        message="追加しました。",
    )

    parsed = urlsplit(url)
    query = parse_qs(parsed.query)
    assert parsed.path == "/export-lists/12"
    assert query["show_candidates"] == ["1"]
    assert query["case_q"] == ["abc"]
    assert query["facility_codes"] == ["001\n002"]
    assert query["exam_item_namecode"] == ["A", "B"]
    assert query["exam_item_match_mode"] == ["all"]
    assert query["message"] == ["追加しました。"]


def test_export_list_candidate_return_url_drops_unrelated_query_values() -> None:
    url = export_list_candidate_return_url(
        xml_export_list_id=34,
        return_query="next=https%3A%2F%2Fexample.com&error=old&case_q=123",
        error="追加できません。",
    )

    query = parse_qs(urlsplit(url).query)
    assert "next" not in query
    assert query["show_candidates"] == ["1"]
    assert query["case_q"] == ["123"]
    assert query["error"] == ["追加できません。"]
