from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, cast

from scripts.lib.db.mysql import dict_cursor
from scripts.lib.db.schemas import DEV_PHR


@dataclass
class ApplySubscribersFundNamePartsResult:
    rows_seen_count: int
    rows_updated_count: int
    rows_skipped_count: int
    row_error_count: int


_TARGET_COLUMNS = (
    "name_kana_family_norm",
    "name_kana_middle_norm",
    "name_kana_given_norm",
    "name_kanji_family_norm",
    "name_kanji_middle_norm",
    "name_kanji_given_norm",
)



def apply_name_parts_from_staging_subscribers_fund(
    conn: Any,
    run_id: int,
) -> ApplySubscribersFundNamePartsResult:
    """staging_subscribers_fund から subscribers へ name parts を空欄補完する。

    方針:
    - 対象は import_run_id = run_id かつ matched_subscriber_id IS NOT NULL の staging 行のみ
    - subscribers 側が空欄の列に限って更新する
    - staging 側の norm 値をそのまま利用する
    - 新規 subscriber 作成や既存非空欄値の上書きは行わない
    """
    rows_seen_count = 0
    rows_updated_count = 0
    rows_skipped_count = 0
    row_error_count = 0

    staging_rows = fetch_apply_target_rows(conn, run_id)

    for row in staging_rows:
        rows_seen_count += 1

        subscriber_id = row.get("matched_subscriber_id")
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

        updated = update_subscriber_name_parts_if_empty(conn, int(subscriber_id), update_values)
        if updated:
            rows_updated_count += 1
        else:
            rows_skipped_count += 1

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
            "matched_subscriber_id",
            "import_run_id",
            *list(_TARGET_COLUMNS),
        ]
    )
    sql = f"""
        SELECT {cols}
        FROM {DEV_PHR}.staging_subscribers_fund
        WHERE import_run_id = %s
          AND matched_subscriber_id IS NOT NULL
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
    """subscribers 側の name parts norm を取得する。"""
    cols = ", ".join(["id", *list(_TARGET_COLUMNS)])
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

    for column in _TARGET_COLUMNS:
        subscriber_value = subscriber_row.get(column)
        staging_value = staging_row.get(column)

        if not is_effectively_blank_value(subscriber_value):
            continue
        if is_effectively_blank_value(staging_value):
            continue

        updates[column] = str(staging_value)

    return updates



def update_subscriber_name_parts_if_empty(
    conn: Any,
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

    cur = dict_cursor(conn)
    try:
        cur.execute(sql, tuple(params))
        rowcount = cur.rowcount
    finally:
        cur.close()

    return rowcount > 0



def is_effectively_blank_value(value: Any) -> bool:
    """None / 空文字 / 空白のみを空欄として扱う。"""
    if value is None:
        return True
    return str(value).strip() == ""
