from apps.health_exam_admin.main import (
    create_external_feedback_item_detail,
    external_feedback_detail_type_from_form,
)


class Cursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.lastrowid = 91

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.calls.append((sql, params))


def test_detail_type_falls_back_from_legacy_category() -> None:
    assert external_feedback_detail_type_from_form({"issue_category": "BASIC_INFO"}) == "BASIC_INFO"
    assert external_feedback_detail_type_from_form({"issue_category": "EXAM_ITEM"}) == "EXAM_ITEM"
    assert external_feedback_detail_type_from_form({"issue_category": "UPLOAD"}) == "OTHER"


def test_create_exam_item_detail_keeps_namecode_and_section() -> None:
    cur = Cursor()

    detail_id = create_external_feedback_item_detail(
        cur,
        item_id=12,
        form={
            "detail_type": "EXAM_ITEM",
            "namecode": "9N516000000000049",
            "section_code": "01010",
            "external_message": "医師名がありません",
        },
        user={"employee_number": "1107858", "display_name": "担当者"},
    )

    assert detail_id == 91
    assert len(cur.calls) == 2
    insert_sql, params = cur.calls[0]
    assert "ops_external_feedback_item_details" in insert_sql
    assert params[0:5] == (12, "EXAM_ITEM", "", "9N516000000000049", "01010")
    assert params[-1] == "1107858 担当者"
    assert "DETAIL_CREATE" in cur.calls[1][0]


def test_exam_item_detail_requires_namecode() -> None:
    cur = Cursor()

    try:
        create_external_feedback_item_detail(
            cur,
            item_id=12,
            form={"detail_type": "EXAM_ITEM", "external_message": "値が不正です"},
            user={},
        )
    except ValueError as exc:
        assert "namecode" in str(exc)
    else:
        raise AssertionError("namecodeなしの健診項目が登録されました")
