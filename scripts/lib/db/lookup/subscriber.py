

from __future__ import annotations

from typing import Any, Mapping, cast

from scripts.lib.db.mysql import dict_cursor
from scripts.lib.db.schemas import DEV_PHR


class SubscriberLookupError(Exception):
    """subscriber lookup 系の基底例外。"""


class SubscriberAmbiguousError(SubscriberLookupError):
    """1件想定の lookup で複数件ヒットした場合。"""


# NOTE:
# subscribers 側の現行スキーマでは、漢字 full match は `name_full_match` という列名。
# staging_subscribers_fund 側の `name_kanji_full_match` とは命名が異なるため、
# lookup 結果では subscribers の現行列名をそのまま返す。
# 将来的に subscribers 側へ漢字 parts match 列を追加する場合は、
# 本モジュールの返却列も拡張する。
_SUBSCRIBER_LOOKUP_COLUMNS = (
    "id",
    "identity_hash",
    "person_id_custom",
    "insurer_number",
    "name_kana_full_match",
    "name_full_match",
    "insurance_symbol_match",
    "insurance_number_match",
)


def _build_select_sql() -> str:
    cols = ", ".join(_SUBSCRIBER_LOOKUP_COLUMNS)
    return f"""
        SELECT {cols}
        FROM {DEV_PHR}.subscribers
        WHERE identity_hash = %s
        ORDER BY id
    """


def list_subscribers_by_identity_hash(conn: Any, identity_hash: str | None) -> list[dict[str, Any]]:
    """identity_hash に一致する subscribers 行をすべて返す。"""
    if identity_hash is None or identity_hash == "":
        return []

    cursor = dict_cursor(conn)
    try:
        cursor.execute(_build_select_sql(), (identity_hash,))
        rows = cast(list[Mapping[str, Any]], cursor.fetchall() or [])
    finally:
        cursor.close()

    return [dict(row) for row in rows]


def get_subscriber_map_by_identity_hash(conn: Any, identity_hash: str | None) -> dict[int, dict[str, Any]]:
    """identity_hash 一致行を id キーの dict で返す。複数件ヒットを許容する。"""
    rows = list_subscribers_by_identity_hash(conn, identity_hash)
    result: dict[int, dict[str, Any]] = {}
    for row in rows:
        subscriber_id = row.get("id")
        if subscriber_id is None:
            continue
        result[int(subscriber_id)] = row
    return result


def get_single_subscriber_id_by_identity_hash(conn: Any, identity_hash: str | None) -> int | None:
    """identity_hash 一致が 0 件なら None、1 件なら id、複数件なら例外。"""
    subscriber_map = get_subscriber_map_by_identity_hash(conn, identity_hash)
    if not subscriber_map:
        return None
    if len(subscriber_map) > 1:
        ids = sorted(subscriber_map.keys())
        raise SubscriberAmbiguousError(
            f"multiple subscribers found for identity_hash={identity_hash}: ids={ids}"
        )
    return next(iter(subscriber_map.keys()))