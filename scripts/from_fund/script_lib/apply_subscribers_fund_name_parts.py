from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, cast

from scripts.lib.db.mysql import dict_cursor
from scripts.lib.db.insert.subscriber_audit import (
    insert_subscriber_audit_rows_and_touch_last_change_run,
)
from scripts.lib.db.schemas import DEV_PHR


STATUS_IDENTITY_MATCHED = "IDENTITY_MATCHED"
STATUS_PARTS_APPLIED = "PARTS_APPLIED"
STATUS_PARTS_FAILED = "PARTS_FAILED"

REASON_NO_STAGING_PARTS = "NO_STAGING_PARTS"
REASON_INVALID_STAGING_PARTS = "INVALID_STAGING_PARTS"
REASON_PARTS_GROUP_ALREADY_FILLED = "PARTS_GROUP_ALREADY_FILLED"
REASON_NOTHING_TO_UPDATE = "NOTHING_TO_UPDATE"
REASON_SUBSCRIBER_NOT_FOUND = "SUBSCRIBER_NOT_FOUND"

_REASON_ORDER = [
    REASON_NO_STAGING_PARTS,
    REASON_INVALID_STAGING_PARTS,
    REASON_PARTS_GROUP_ALREADY_FILLED,
    REASON_NOTHING_TO_UPDATE,
    REASON_SUBSCRIBER_NOT_FOUND,
]


@dataclass
class ApplySubscribersFundNamePartsResult:
    rows_seen_count: int
    rows_updated_count: int
    rows_skipped_count: int
    row_error_count: int


@dataclass(frozen=True)
class NamePartsGroup:
    name: str
    source_columns: tuple[str, str, str]
    source_match_columns: tuple[str, str, str]
    subscriber_columns: tuple[str, str, str]
    subscriber_match_columns: tuple[str, str, str]


_NAME_PARTS_GROUPS = (
    NamePartsGroup(
        name="kanji",
        source_columns=(
            "name_kanji_family_norm",
            "name_kanji_middle_norm",
            "name_kanji_given_norm",
        ),
        source_match_columns=(
            "name_kanji_family_match",
            "name_kanji_middle_match",
            "name_kanji_given_match",
        ),
        subscriber_columns=(
            "name_kanji_family",
            "name_kanji_middle",
            "name_kanji_given",
        ),
        subscriber_match_columns=(
            "name_kanji_family_match",
            "name_kanji_middle_match",
            "name_kanji_given_match",
        ),
    ),
    NamePartsGroup(
        name="kana",
        source_columns=(
            "name_kana_family_norm",
            "name_kana_middle_norm",
            "name_kana_given_norm",
        ),
        source_match_columns=(
            "name_kana_family_match",
            "name_kana_middle_match",
            "name_kana_given_match",
        ),
        subscriber_columns=(
            "name_kana_family",
            "name_kana_middle",
            "name_kana_given",
        ),
        subscriber_match_columns=(
            "name_kana_family_match",
            "name_kana_middle_match",
            "name_kana_given_match",
        ),
    ),
)

_STAGING_NAME_PARTS_COLUMNS = tuple(
    column
    for group in _NAME_PARTS_GROUPS
    for column in (*group.source_columns, *group.source_match_columns)
)

_SUBSCRIBER_NAME_PARTS_COLUMNS = tuple(
    column
    for group in _NAME_PARTS_GROUPS
    for column in (*group.subscriber_columns, *group.subscriber_match_columns)
)



def apply_name_parts_from_staging_subscribers_fund(
    conn: Any,
    run_id: int | None,
    *,
    audit_source: str = "from_fund_name_parts_apply",
    change_run_id: int | None = None,
    dry_run: bool = False,
) -> ApplySubscribersFundNamePartsResult:
    """staging_subscribers_fund から subscribers へ name parts を空欄補完する。

    方針:
    - 対象は import_run_id = run_id かつ parts_apply_subscriber_id IS NOT NULL かつ parts_apply_status = 'IDENTITY_MATCHED' の staging 行のみ
    - 漢字 parts / カナ parts をグループ単位で判定する
    - staging 側は1グループ内で2項目以上の parts がある場合のみ有効な補完元とする
    - subscriber 側は該当グループの parts / parts_match がすべて空欄の場合のみ補完する
    - identity 変更時に既存 parts を空に戻す責務は HIA → staging_subscribers_hub → apply 側にある
    - 本処理は parts が空である前提で補完のみを行う
    - match のみ残存していないか等の整合性確認は本処理の責務外とする
    - parts 本体列と parts_match 列をペアで更新する
    - 新規 subscriber 作成や既存非空欄値の上書きは行わない
    - 更新した行は PARTS_APPLIED、更新ゼロの行は PARTS_FAILED とする
    """
    rows_seen_count = 0
    rows_updated_count = 0
    rows_skipped_count = 0
    row_error_count = 0
    default_change_run_id = change_run_id

    staging_rows = fetch_apply_target_rows(conn, run_id)

    for row in staging_rows:
        rows_seen_count += 1
        effective_change_run_id = (
            default_change_run_id
            if default_change_run_id is not None
            else int(row.get("import_run_id") or 0)
        )

        subscriber_id = row.get("parts_apply_subscriber_id")
        if subscriber_id is None:
            rows_skipped_count += 1
            continue

        subscriber_row = fetch_subscriber_name_parts(conn, int(subscriber_id))
        if not subscriber_row:
            row_error_count += 1
            if not dry_run:
                update_staging_parts_apply_status(
                    conn,
                    staging_id=int(row["id"]),
                    status=STATUS_PARTS_FAILED,
                    reason=REASON_SUBSCRIBER_NOT_FOUND,
                )
            continue

        update_values, failure_reasons = build_name_parts_update_values(
            row,
            subscriber_row,
        )

        if not update_values:
            rows_skipped_count += 1
            reason = build_parts_apply_reason(failure_reasons)
            if not dry_run:
                update_staging_parts_apply_status(
                    conn,
                    staging_id=int(row["id"]),
                    status=STATUS_PARTS_FAILED,
                    reason=reason,
                )
            continue

        audit_rows = build_subscriber_name_parts_audit_rows(
            subscriber_id=int(subscriber_id),
            subscriber_row=subscriber_row,
            update_values=update_values,
            source=audit_source,
            change_run_id=effective_change_run_id,
        )

        cur = dict_cursor(conn)
        try:
            if dry_run:
                rows_updated_count += 1
                continue

            updated = update_subscriber_name_parts_if_empty(cur, int(subscriber_id), update_values)
            if updated:
                insert_subscriber_audit_rows_and_touch_last_change_run(cur, audit_rows)
                update_staging_parts_apply_status(
                    conn,
                    staging_id=int(row["id"]),
                    status=STATUS_PARTS_APPLIED,
                    reason=None,
                )
                rows_updated_count += 1
            else:
                rows_skipped_count += 1
                reason = build_parts_apply_reason([REASON_NOTHING_TO_UPDATE])
                update_staging_parts_apply_status(
                    conn,
                    staging_id=int(row["id"]),
                    status=STATUS_PARTS_FAILED,
                    reason=reason,
                )
        finally:
            cur.close()

    return ApplySubscribersFundNamePartsResult(
        rows_seen_count=rows_seen_count,
        rows_updated_count=rows_updated_count,
        rows_skipped_count=rows_skipped_count,
        row_error_count=row_error_count,
    )



def fetch_apply_target_rows(conn: Any, run_id: int | None) -> list[dict[str, Any]]:
    """apply 対象となる staging 行を取得する。"""
    cols = ", ".join(
        [
            "id",
            "parts_apply_subscriber_id",
            "parts_apply_status",
            "parts_apply_reason",
            "import_run_id",
            *_STAGING_NAME_PARTS_COLUMNS,
        ]
    )
    where_clauses = [
        "parts_apply_subscriber_id IS NOT NULL",
        "parts_apply_status = %s",
    ]
    params: list[Any] = [STATUS_IDENTITY_MATCHED]
    if run_id is not None:
        where_clauses.insert(0, "import_run_id = %s")
        params.insert(0, run_id)

    sql = f"""
        SELECT {cols}
        FROM {DEV_PHR}.staging_subscribers_fund
        WHERE {' AND '.join(where_clauses)}
        ORDER BY import_run_id, id
    """

    cur = dict_cursor(conn)
    try:
        cur.execute(sql, tuple(params))
        rows = cast(list[Mapping[str, Any]], cur.fetchall() or [])
    finally:
        cur.close()

    return [dict(row) for row in rows]



def fetch_subscriber_name_parts(conn: Any, subscriber_id: int) -> dict[str, Any] | None:
    """subscribers 側の name parts を取得する。"""
    cols = ", ".join(["id", *_SUBSCRIBER_NAME_PARTS_COLUMNS])
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
) -> tuple[dict[str, str], list[str]]:
    """staging / subscriber を比較し、グループ単位の補完対象と失敗理由を返す。"""
    updates: dict[str, str] = {}
    reasons: list[str] = []
    any_staging_value = False

    for group in _NAME_PARTS_GROUPS:
        group_updates, group_reason, has_staging_value = build_group_update_values(
            group=group,
            staging_row=staging_row,
            subscriber_row=subscriber_row,
        )
        any_staging_value = any_staging_value or has_staging_value
        if group_updates:
            updates.update(group_updates)
        elif group_reason:
            reasons.append(group_reason)

    if not any_staging_value:
        reasons.append(REASON_NO_STAGING_PARTS)

    if not updates and not reasons:
        reasons.append(REASON_NOTHING_TO_UPDATE)

    return updates, reasons



def build_group_update_values(
    *,
    group: NamePartsGroup,
    staging_row: Mapping[str, Any],
    subscriber_row: Mapping[str, Any],
) -> tuple[dict[str, str], str | None, bool]:
    """1グループ分の補完対象を判定する。"""
    source_values = [staging_row.get(column) for column in group.source_columns]
    source_value_count = sum(not is_effectively_blank_value(value) for value in source_values)
    has_staging_value = source_value_count > 0

    if source_value_count == 0:
        return {}, None, False

    if source_value_count < 2:
        return {}, REASON_INVALID_STAGING_PARTS, True

    subscriber_values = [subscriber_row.get(column) for column in group.subscriber_columns]
    subscriber_match_values = [subscriber_row.get(column) for column in group.subscriber_match_columns]
    subscriber_group_values = [*subscriber_values, *subscriber_match_values]

    # identity 変更時の既存 parts クリアは上流処理の責務。
    # 本処理では「空なら補完、空でなければ補完しない」のみを行う。
    # 中途半端なデータ整合性チェックは行わない。
    if any(not is_effectively_blank_value(value) for value in subscriber_group_values):
        return {}, REASON_PARTS_GROUP_ALREADY_FILLED, True

    updates: dict[str, str] = {}
    for source_column, subscriber_column in zip(group.source_columns, group.subscriber_columns, strict=True):
        staging_value = staging_row.get(source_column)
        if not is_effectively_blank_value(staging_value):
            updates[subscriber_column] = str(staging_value)

    for source_column, subscriber_column in zip(group.source_match_columns, group.subscriber_match_columns, strict=True):
        staging_value = staging_row.get(source_column)
        if not is_effectively_blank_value(staging_value):
            updates[subscriber_column] = str(staging_value)

    if not updates:
        return {}, REASON_NOTHING_TO_UPDATE, True

    return updates, None, True


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
    where_clauses = ["id = %s"]
    params: list[Any] = []

    for column, value in update_values.items():
        set_clauses.append(f"`{column}` = %s")
        params.append(value)
        where_clauses.append(f"(`{column}` IS NULL OR TRIM(`{column}`) = '')")

    params.append(subscriber_id)

    sql = f"""
        UPDATE {DEV_PHR}.subscribers
        SET {', '.join(set_clauses)}
        WHERE {' AND '.join(where_clauses)}
    """

    cur.execute(sql, tuple(params))
    rowcount = cur.rowcount

    return rowcount > 0



def update_staging_parts_apply_status(
    conn: Any,
    *,
    staging_id: int,
    status: str,
    reason: str | None,
) -> None:
    """staging_subscribers_fund の parts_apply_status / reason を更新する。"""
    cur = dict_cursor(conn)
    try:
        cur.execute(
            f"""
            UPDATE {DEV_PHR}.staging_subscribers_fund
            SET
              parts_apply_status = %s,
              parts_apply_reason = %s,
              parts_apply_checked_at = NOW()
            WHERE id = %s
            """,
            (status, reason, staging_id),
        )
    finally:
        cur.close()



def build_parts_apply_reason(reasons: list[str]) -> str:
    """固定順で重複除去し、reason を `|` 区切りで返す。"""
    unique_reasons = set(reasons)
    ordered = [reason for reason in _REASON_ORDER if reason in unique_reasons]
    if not ordered:
        ordered = [REASON_NOTHING_TO_UPDATE]
    return "|".join(ordered)



def is_effectively_blank_value(value: Any) -> bool:
    """None / 空文字 / 空白のみを空欄として扱う。"""
    if value is None:
        return True
    return str(value).strip() == ""
