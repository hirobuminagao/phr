# ============================================================
# value-to-text helper
# ============================================================

def _value_to_text(value: Any) -> str | None:
    """compare hash 用に任意値を文字列へ変換する。"""

    if value is None:
        return None

    return str(value)
# -*- coding: utf-8 -*-
"""
============================================================
Module : compare_hash.py
Path   : scripts/lib/hash/compare_hash.py
Project: PHR

Purpose:
    canonical compare hash を共通手順で生成する。

Responsibility:
    - values list を受け取る
    - 各値へ base_norm を適用する
    - delimiter join を行う
    - sha256 hash を返す

Non-goals:
    - field-specific normalize
    - match generation
    - identity hash generation
    - DB access
============================================================
"""

from __future__ import annotations

import hashlib
from typing import Any, Iterable

from scripts.lib.identity.base_norm import base_normalize


# ============================================================
# constants
# ============================================================

MAX_COMPARE_HASH_VALUES = 16
COMPARE_HASH_DELIMITER = "|"


# ============================================================
# helpers
# ============================================================


def _normalize_values(values: Iterable[Any]) -> list[str]:
    """
    compare hash 用 values を canonical text list に変換する。

    Notes:
        - values 順序は維持する
        - 各値は base_norm を適用する
        - None は空文字へ変換する
    """
    normalized: list[str] = []

    for value in values:
        text = _value_to_text(value)
        normalized.append(base_normalize(text) or "")

    return normalized


# ============================================================
# public api
# ============================================================


def build_compare_hash(values: Iterable[Any]) -> str:
    """
    canonical compare hash を生成する。

    Flow:
        1. values を list 化
        2. values count を検証
        3. base_norm を適用
        4. delimiter join
        5. sha256 hex digest を返却

    Notes:
        - compare hash の標準用途は norm 値
        - match 値を hash 化したい場合は呼び出し側で生成する
        - field-specific normalize は行わない
    """
    values_list = list(values)

    if len(values_list) > MAX_COMPARE_HASH_VALUES:
        raise ValueError(
            "compare hash values count exceeds limit: "
            f"{len(values_list)} > {MAX_COMPARE_HASH_VALUES}"
        )

    normalized_values = _normalize_values(values_list)

    joined = COMPARE_HASH_DELIMITER.join(normalized_values)

    return hashlib.sha256(
        joined.encode("utf-8")
    ).hexdigest()
# -*- coding: utf-8 -*-
"""
============================================================
Module : compare_hash.py
Path   : scripts/lib/hash/compare_hash.py
Project: PHR

Purpose:
    canonical compare hash を共通手順で生成する。

Responsibility:
    - values list を受け取る
    - 各値へ base_normalize を適用する
    - delimiter join を行う
    - sha256 hash を返す

Non-goals:
    - field-specific normalize
    - match generation
    - identity hash generation
    - DB access
============================================================
"""

from __future__ import annotations

import hashlib
from typing import Any, Iterable

from scripts.lib.identity.base_norm import base_normalize


# ============================================================
# constants
# ============================================================

MAX_COMPARE_HASH_VALUES = 16
COMPARE_HASH_DELIMITER = "|"


# ============================================================
# helpers
# ============================================================


def _value_to_text(value: Any) -> str | None:
    """compare hash 用に任意値を文字列へ変換する。"""

    if value is None:
        return None

    return str(value)


def _normalize_values(values: Iterable[Any]) -> list[str]:
    """
    compare hash 用 values を canonical text list に変換する。

    Notes:
        - values 順序は維持する
        - 各値は文字列化してから base_normalize を適用する
        - None は空文字へ変換する
    """
    normalized: list[str] = []

    for value in values:
        text = _value_to_text(value)
        normalized.append(base_normalize(text) or "")

    return normalized


# ============================================================
# public api
# ============================================================


def build_compare_hash(values: Iterable[Any]) -> str:
    """
    canonical compare hash を生成する。

    Flow:
        1. values を list 化
        2. values count を検証
        3. base_normalize を適用
        4. delimiter join
        5. sha256 hex digest を返却

    Notes:
        - compare hash の標準用途は norm 値
        - match 値を hash 化したい場合は呼び出し側で生成する
        - field-specific normalize は行わない
    """
    values_list = list(values)

    if len(values_list) > MAX_COMPARE_HASH_VALUES:
        raise ValueError(
            "compare hash values count exceeds limit: "
            f"{len(values_list)} > {MAX_COMPARE_HASH_VALUES}"
        )

    normalized_values = _normalize_values(values_list)
    joined = COMPARE_HASH_DELIMITER.join(normalized_values)

    return hashlib.sha256(joined.encode("utf-8")).hexdigest()