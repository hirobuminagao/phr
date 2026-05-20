

# -*- coding: utf-8 -*-
"""
============================================================
Module : subscriber_projection.py
Path   : scripts/lib/db/lookup/subscriber_projection.py
Project: PHR

Purpose:
    subscribers.id リストを受け取り、用途別に必要な列だけを SELECT して返す。

Responsibility:
    - subscribers.id list を入力にする
    - 用途別 projection column set を定義する
    - 指定 id の subscribers row を軽量 dict として返す

Non-goals:
    - subscriber search
    - identity resolve
    - ambiguity handling
    - apply_action decision
    - subscribers update
============================================================
"""

from __future__ import annotations

from typing import Any, Iterable


# ============================================================
# column sets
# ============================================================


HIA_CURRENT_SNAPSHOT_COLUMNS = """
    id AS subscriber_id,
    hia_subscriber_id,
    identity_hash,
    person_id_custom,
    name_kana_full_match
"""


HIA_CURRENT_ADDRESS_COLUMNS = """
    subscriber_id,
    address_id AS current_address_id
"""


HIA_CURRENT_CONTACT_COLUMNS = """
    subscriber_id,
    contact_id AS current_contact_id
"""


# ============================================================
# helpers
# ============================================================


def _normalize_subscriber_ids(
    subscriber_ids: Iterable[int],
) -> list[int]:
    """subscriber_ids を重複排除し、空値を除外して list[int] にする。"""
    normalized: list[int] = []
    seen: set[int] = set()

    for raw_id in subscriber_ids:
        if raw_id is None:
            continue

        subscriber_id = int(raw_id)
        if subscriber_id in seen:
            continue

        seen.add(subscriber_id)
        normalized.append(subscriber_id)

    return normalized



def _build_in_placeholders(count: int) -> str:
    """IN句用 placeholder を生成する。"""
    return ", ".join(["%s"] * count)


# ============================================================
# projection
# ============================================================


def load_subscriber_rows_for_hia_current_snapshot(
    cur,
    *,
    subscriber_ids: Iterable[int],
) -> list[dict[str, Any]]:
    """
    HIA current snapshot 用の subscribers 軽量行を取得する。

    Input:
        subscribers.id list

    Output:
        current_snapshot 更新に必要な lightweight rows

    Notes:
        - 検索は行わない
        - 渡された subscribers.id のみを対象にする
        - address / contact は別 projection / hydrate で扱う
    """
    ids = _normalize_subscriber_ids(subscriber_ids)
    if not ids:
        return []

    placeholders = _build_in_placeholders(len(ids))

    cur.execute(
        f"""
        SELECT
            {HIA_CURRENT_SNAPSHOT_COLUMNS}
        FROM subscribers
        WHERE id IN ({placeholders})
        ORDER BY id
        """,
        ids,
    )

    return list(cur.fetchall())


# ============================================================
# current address/contact projection for HIA current snapshot
# ============================================================


def load_current_address_rows_for_hia_current_snapshot(
    cur,
    *,
    subscriber_ids: Iterable[int],
) -> list[dict[str, Any]]:
    """
    HIA current snapshot 用の current address 行を取得する。

    Notes:
        - subscriber_addresses.is_current = 1 を current として扱う
        - subscribers.id list のみを対象にする
        - lookup / resolve は行わない
    """
    ids = _normalize_subscriber_ids(subscriber_ids)
    if not ids:
        return []

    placeholders = _build_in_placeholders(len(ids))

    cur.execute(
        f"""
        SELECT
            {HIA_CURRENT_ADDRESS_COLUMNS}
        FROM subscriber_addresses
        WHERE subscriber_id IN ({placeholders})
          AND is_current = 1
        ORDER BY address_id DESC
        """,
        ids,
    )

    return list(cur.fetchall())



def load_current_contact_rows_for_hia_current_snapshot(
    cur,
    *,
    subscriber_ids: Iterable[int],
) -> list[dict[str, Any]]:
    """
    HIA current snapshot 用の current contact 行を取得する。

    Notes:
        - subscriber_contacts.is_current = 1 を current として扱う
        - subscribers.id list のみを対象にする
        - lookup / resolve は行わない
    """
    ids = _normalize_subscriber_ids(subscriber_ids)
    if not ids:
        return []

    placeholders = _build_in_placeholders(len(ids))

    cur.execute(
        f"""
        SELECT
            {HIA_CURRENT_CONTACT_COLUMNS}
        FROM subscriber_contacts
        WHERE subscriber_id IN ({placeholders})
          AND is_current = 1
        ORDER BY contact_id DESC
        """,
        ids,
    )

    return list(cur.fetchall())