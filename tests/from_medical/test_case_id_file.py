from __future__ import annotations

import json

import pytest

from scripts.from_medical.script_lib.case_id_file import load_case_id_file


def test_load_case_id_file_returns_sorted_case_ids(tmp_path) -> None:
    path = tmp_path / "cases.json"
    path.write_text(json.dumps({"event_id": 2, "case_ids": [20, 10]}), encoding="utf-8")

    assert load_case_id_file(str(path), event_id=2) == (10, 20)


def test_load_case_id_file_rejects_empty_scope(tmp_path) -> None:
    path = tmp_path / "cases.json"
    path.write_text(json.dumps({"event_id": 2, "case_ids": []}), encoding="utf-8")

    with pytest.raises(ValueError, match="no target cases"):
        load_case_id_file(str(path), event_id=2)
