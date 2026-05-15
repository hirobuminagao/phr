from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, cast

from scripts.lib.db.mysql import dict_cursor
from scripts.lib.db.insert.subscriber_audit import (
    insert_subscriber_audit_rows_and_touch_last_change_run,
)
from scripts.lib.db.schemas import DEV_PHR


@dataclass
class ApplySubscribersFundNamePartsResult:
    rows_seen_count: int
    rows_updated_count: int
    rows_skipped_count: int
    row_error_count: int


_COLUMN_MAP = {
    # kana parts norm -> subscribers parts
    "name_kana_family_norm": "name_kana_family",
    "name_kana_middle_norm": "name_kana_middle",
    "name_kana_given_norm": "name_kana_given",

    # kanji parts norm -> subscribers parts
    "name_kanji_family_norm": "name_kanji_family",
    "name_kanji_middle_norm": "name_kanji_middle",
    "name_kanji_given_norm": "name_kanji_given",

    # kana parts match -> subscribers parts match
    "name_kana_family_match": "name_kana_family_match",
    "name_kana_middle_match": "name_kana_middle_match",
    "name_kana_given_match": "name_kana_given_match",

    # kanji parts match -> subscribers parts match
    "name_kanji_family_match": "name_kanji_family_match",
    "name_kanji_middle_match": "name_kanji_middle_match",
    "name_kanji_given_match": "name_kanji_given_match",
}



def apply_name_parts_from_staging_subscribers_fund(
    conn: Any,
    run_id: int,
    *,
    audit_source: str,
    change_run_id: int,
) -> ApplySubscribersFundNamePartsResult:
    """staging_subscribers_fund から subscribers へ name parts を空欄補完する。

    方針:
    - 対象は import_run_id = run_id かつ parts_apply_subscriber_id IS NOT NULL かつ parts_apply_status = 'IDENTITY_MATCHED' の staging 行のみ
    - subscribers 側が空欄の列に限って更新する
    - staging 側の norm / match 値をそのまま利用する
    - 新規 subscriber 作成や既存非空欄値の上書きは行わない
    """
    rows_seen_count = 0
    rows_updated_count = 0
    rows_skipped_count = 0
    row_error_count = 0

    staging_rows = fetch_apply_target_rows(conn, run_id)

    for row in staging_rows:
        rows_seen_count += 1

        subscriber_id = row.get("parts_apply_subscriber_id")
        if subscriber_id is None:
            rows_skipped_count += 1
            continue

        subscriber_row = fetch_subscriber_name_parts(conn, int(subscriber_id))
        if not subscriber_row:
            row_error_count += 1
            continue

        update_values = build_name_parts_update_values(row, subscriber_row)
        if not update_values:
            rows_skipped_count += 1
            continue

        audit_rows = build_subscriber_name_parts_audit_rows(
            subscriber_id=int(subscriber_id),
            subscriber_row=subscriber_row,
            update_values=update_values,
            source=audit_source,
            change_run_id=change_run_id,
        )

        cur = dict_cursor(conn)
        try:
            updated = update_subscriber_name_parts_if_empty(cur, int(subscriber_id), update_values)
            if updated:
                insert_subscriber_audit_rows_and_touch_last_change_run(cur, audit_rows)
                rows_updated_count += 1
            else:
                rows_skipped_count += 1
        finally:
            cur.close()

    return ApplySubscribersFundNamePartsResult(
        rows_seen_count=rows_seen_count,
        rows_updated_count=rows_updated_count,
        rows_skipped_count=rows_skipped_count,
        row_error_count=row_error_count,
    )



def fetch_apply_target_rows(conn: Any, run_id: int) -> list[dict[str, Any]]:
    """apply 対象となる staging 行を取得する。"""
    cols = ", ".join(
        [
            "id",
            "parts_apply_subscriber_id",
            "parts_apply_status",
            "import_run_id",
            *list(_COLUMN_MAP.keys()),
        ]
    )
    sql = f"""
        SELECT {cols}
        FROM {DEV_PHR}.staging_subscribers_fund
        WHERE import_run_id = %s
          AND parts_apply_subscriber_id IS NOT NULL
          AND parts_apply_status = 'IDENTITY_MATCHED'
        ORDER BY id
    """

    cur = dict_cursor(conn)
    try:
        cur.execute(sql, (run_id,))
        rows = cast(list[Mapping[str, Any]], cur.fetchall() or [])
    finally:
        cur.close()

    return [dict(row) for row in rows]



def fetch_subscriber_name_parts(conn: Any, subscriber_id: int) -> dict[str, Any] | None:
    """subscribers 側の name parts を取得する。"""
    cols = ", ".join(["id", *list(_COLUMN_MAP.values())])
    sql = f"""
        SELECT {cols}
        FROM {DEV_PHR}.subscribers
        WHERE id = %s
        LIMIT 1
    """

    cur = dict_cursor(conn)
    try:
        cur.execute(sql, (subscriber_id,))
        row = cast(Mapping[str, Any] | None, cur.fetchone())
    finally:
        cur.close()

    return dict(row) if row else None




def build_name_parts_update_values(
    staging_row: Mapping[str, Any],
    subscriber_row: Mapping[str, Any],
) -> dict[str, str]:
    """staging / subscriber を比較し、空欄補完対象だけを返す。"""
    updates: dict[str, str] = {}

    for staging_column, subscriber_column in _COLUMN_MAP.items():
        subscriber_value = subscriber_row.get(subscriber_column)
        staging_value = staging_row.get(staging_column)

        if not is_effectively_blank_value(subscriber_value):
            continue
        if is_effectively_blank_value(staging_value):
            continue

        updates[subscriber_column] = str(staging_value)

    return updates


# --- audit support ---

def build_subscriber_name_parts_audit_rows(
    *,
    subscriber_id: int,
    subscriber_row: Mapping[str, Any],
    update_values: Mapping[str, str],
    source: str,
    change_run_id: int,
) -> list[dict[str, Any]]:
    """subscriber_audit 用の行 dict を構築する。"""
    rows: list[dict[str, Any]] = []

    for column, new_value in update_values.items():
        rows.append(
            {
                "subscriber_id": subscriber_id,
                "field": column,
                "old_value": subscriber_row.get(column),
                "new_value": new_value,
                "source": source,
                "note": None,
                "change_run_id": change_run_id,
            }
        )

    return rows



def update_subscriber_name_parts_if_empty(
    cur: Any,
    subscriber_id: int,
    update_values: Mapping[str, str],
) -> bool:
    """subscribers の空欄列だけを更新する。"""
    if not update_values:
        return False

    set_clauses = []
    params: list[Any] = []

    for column, value in update_values.items():
        set_clauses.append(f"`{column}` = %s")
        params.append(value)

    params.append(subscriber_id)

    sql = f"""
        UPDATE {DEV_PHR}.subscribers
        SET {', '.join(set_clauses)}
        WHERE id = %s
    """

    cur.execute(sql, tuple(params))
    rowcount = cur.rowcount

    return rowcount > 0



def is_effectively_blank_value(value: Any) -> bool:
    """None / 空文字 / 空白のみを空欄として扱う。"""
    if value is None:
        return True
    return str(value).strip() == ""
