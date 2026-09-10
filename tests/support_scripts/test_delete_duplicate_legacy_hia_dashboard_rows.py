from scripts.support_scripts.delete_duplicate_legacy_hia_dashboard_rows import (
    delete_rows,
    find_duplicate_legacy_rows,
)


class Cursor:
    def __init__(self) -> None:
        self.calls = []
        self.rowcount = 2

    def execute(self, sql, params=()) -> None:
        self.calls.append((sql, params))

    def fetchall(self):
        return [{"legacy_id": 10, "current_id": 20}]


def test_find_only_pairs_with_a_current_hia_id_key() -> None:
    cur = Cursor()
    rows = find_duplicate_legacy_rows(cur, work_db="work_other", insurer_number="06139463")

    assert rows == [{"legacy_id": 10, "current_id": 20}]
    sql, params = cur.calls[0]
    assert "|HIA_SUBSCRIBER_ID|" in sql
    assert "legacy.snapshot_identity_key <> SHA2" in sql
    assert "current_row.snapshot_identity_key = SHA2" in sql
    assert params == ("06139463",)


def test_delete_rows_is_limited_to_selected_legacy_ids() -> None:
    cur = Cursor()

    assert delete_rows(cur, work_db="work_other", row_ids=[10, 11]) == 2
    assert "hia_dashboard_person_id IN (%s, %s)" in cur.calls[0][0]
    assert cur.calls[0][1] == (10, 11)
