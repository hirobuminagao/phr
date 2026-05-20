

# -*- coding: utf-8 -*-
"""
============================================================
Module : subscriber_identity.py
Path   : scripts/lib/db/lookup/subscriber_identity.py
Project: PHR

Purpose:
    subscriber identity lookup / resolver layer。

Responsibility:
    - subscribers を identity 系キーで検索する
    - lightweight identity handle を返す
    - candidate / multiple match / not found を整理する

Non-goals:
    - subscribers 更新
    - address / contact hydrate
    - apply_action decision
    - fuzzy business rule

Design:
    - 既存 scripts.lib.db.lookup.subscriber.py の挙動は崩さない
    - 本モジュールは追加 layer として利用する
    - lookup は重い業務データを返さず、後続 hydrate / compare に使いやすい軽量列のみ返す
============================================================
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Optional


# ============================================================
# types
# ============================================================

LookupStatus = Literal[
    "matched",
    "not_found",
    "multiple_match",
    "invalid_input",
]

MatchedBy = Literal[
    "hia_subscriber_id",
    "identity_hash",
    "person_id_custom",
]


IDENTITY_HANDLE_COLUMNS = """
    id AS subscriber_id,
    hia_subscriber_id,
    identity_hash,
    person_id_custom,
    name_kana_full_match
"""


@dataclass(frozen=True)
class SubscriberIdentityLookupResult:
    """subscriber identity lookup の返却形式。"""

    status: LookupStatus
    matched_by: Optional[MatchedBy]
    rows: list[dict[str, Any]]

    @property
    def is_single_match(self) -> bool:
        return self.status == "matched" and len(self.rows) == 1

    @property
    def subscriber_id(self) -> Optional[int]:
        if not self.is_single_match:
            return None
        return self.rows[0].get("subscriber_id")


# ============================================================
# low-level exact lookup
# ============================================================


def list_identity_handles_by_hia_subscriber_id(
    cur,
    *,
    hia_subscriber_id: str | None,
) -> list[dict[str, Any]]:
    """HIA加入者ID完全一致で軽量 identity handle を返す。"""
    if not hia_subscriber_id:
        return []

    cur.execute(
        f"""
        SELECT
            {IDENTITY_HANDLE_COLUMNS}
        FROM subscribers
        WHERE hia_subscriber_id = %s
        ORDER BY id
        """,
        (hia_subscriber_id,),
    )
    return list(cur.fetchall())



def list_identity_handles_by_identity_hash(
    cur,
    *,
    identity_hash: str | None,
) -> list[dict[str, Any]]:
    """identity_hash完全一致で軽量 identity handle を返す。"""
    if not identity_hash:
        return []

    cur.execute(
        f"""
        SELECT
            {IDENTITY_HANDLE_COLUMNS}
        FROM subscribers
        WHERE identity_hash = %s
        ORDER BY id
        """,
        (identity_hash,),
    )
    return list(cur.fetchall())



def list_identity_handles_by_person_id_custom(
    cur,
    *,
    person_id_custom: str | None,
) -> list[dict[str, Any]]:
    """person_id_custom完全一致で軽量 identity handle を返す。"""
    if not person_id_custom:
        return []

    cur.execute(
        f"""
        SELECT
            {IDENTITY_HANDLE_COLUMNS}
        FROM subscribers
        WHERE person_id_custom = %s
        ORDER BY id
        """,
        (person_id_custom,),
    )
    return list(cur.fetchall())


# ============================================================
# result helper
# ============================================================


def _to_lookup_result(
    *,
    matched_by: MatchedBy,
    rows: list[dict[str, Any]],
) -> SubscriberIdentityLookupResult:
    if len(rows) == 0:
        return SubscriberIdentityLookupResult(
            status="not_found",
            matched_by=matched_by,
            rows=[],
        )

    if len(rows) == 1:
        return SubscriberIdentityLookupResult(
            status="matched",
            matched_by=matched_by,
            rows=rows,
        )

    return SubscriberIdentityLookupResult(
        status="multiple_match",
        matched_by=matched_by,
        rows=rows,
    )


# ============================================================
# resolver
# ============================================================


def resolve_subscriber_identity(
    cur,
    *,
    hia_subscriber_id: str | None = None,
    identity_hash: str | None = None,
    person_id_custom: str | None = None,
) -> SubscriberIdentityLookupResult:
    """
    subscriber identity を段階的に解決する。

    lookup priority:
        1. hia_subscriber_id
        2. identity_hash
        3. person_id_custom

    返却は lightweight identity handle のみ。
    address / contact / business attributes は hydrate layer で取得する。
    """
    if hia_subscriber_id:
        rows = list_identity_handles_by_hia_subscriber_id(
            cur,
            hia_subscriber_id=hia_subscriber_id,
        )
        result = _to_lookup_result(
            matched_by="hia_subscriber_id",
            rows=rows,
        )
        if result.status != "not_found":
            return result

    if identity_hash:
        rows = list_identity_handles_by_identity_hash(
            cur,
            identity_hash=identity_hash,
        )
        result = _to_lookup_result(
            matched_by="identity_hash",
            rows=rows,
        )
        if result.status != "not_found":
            return result

    if person_id_custom:
        rows = list_identity_handles_by_person_id_custom(
            cur,
            person_id_custom=person_id_custom,
        )
        result = _to_lookup_result(
            matched_by="person_id_custom",
            rows=rows,
        )
        if result.status != "not_found":
            return result

    return SubscriberIdentityLookupResult(
        status="not_found",
        matched_by=None,
        rows=[],
    )