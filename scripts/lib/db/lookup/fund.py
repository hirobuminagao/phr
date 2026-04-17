#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fund.py

fund 関連の参照系 lookup ライブラリ。

責務:
- insurer_number から fund_id を解決する

方針:
- DB接続そのものは scripts.lib.db.mysql 側へ委譲する
- 本モジュールは SELECT のみを担当する
- 1関数 = 1責務を基本とする
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, cast

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import DEV_PHR


class FundLookupError(RuntimeError):
    """fund lookup 共通例外。"""


class FundNotFoundError(FundLookupError):
    """insurer_number に対応する fund_id が見つからない。"""


class FundAmbiguousError(FundLookupError):
    """insurer_number に対応する fund_id が複数見つかった。"""


def row_get_int(row: Mapping[str, Any], key: str) -> int:
    value = row.get(key)
    if value is None:
        raise ValueError(f"missing column: {key}")
    return int(value)


def _build_insurer_number_candidates(insurer_number: str) -> list[str]:
    """
    insurer_number の候補値を作る。

    目的:
    - フォルダ名側の 0 埋め有無に揺れがあっても解決できるようにする

    例:
    - "06139463" -> ["06139463", "6139463"]
    - "6139463"  -> ["6139463", "06139463"]
    """
    if insurer_number is None:
        raise ValueError("insurer_number is required")

    raw = str(insurer_number).strip()
    if raw == "":
        raise ValueError("insurer_number is empty")

    digits = "".join(ch for ch in raw if ch.isdigit())
    if digits == "":
        raise ValueError(f"insurer_number must contain digits: {insurer_number!r}")

    stripped = digits.lstrip("0") or "0"
    padded8 = stripped.zfill(8)

    candidates: list[str] = []
    for value in (raw, digits, stripped, padded8):
        if value not in candidates:
            candidates.append(value)
    return candidates


def _fetch_fund_ids_by_candidates(candidates: Iterable[str]) -> list[int]:
    """候補 insurer_number 群から fund_id 一覧を取得する。"""
    candidate_list = list(candidates)
    if not candidate_list:
        return []

    placeholders = ", ".join(["%s"] * len(candidate_list))
    sql = f"""
        SELECT DISTINCT fund_id
        FROM {DEV_PHR}.fund_insurer_numbers
        WHERE insurer_number IN ({placeholders})
        ORDER BY fund_id
    """

    params = load_mysql_base_params()
    with connect_ctx(params, database=DEV_PHR) as conn:
        cursor = dict_cursor(conn)
        try:
            cursor.execute(sql, tuple(candidate_list))
            rows = cast(list[Mapping[str, Any]], cursor.fetchall())
        finally:
            cursor.close()

    fund_ids: list[int] = []
    for row in rows:
        try:
            fund_ids.append(row_get_int(row, "fund_id"))
        except ValueError:
            continue
    return fund_ids


def get_fund_id_from_insurer_number(insurer_number: str) -> int:
    """
    insurer_number から fund_id を解決する。

    Args:
        insurer_number: フォルダ名等から取得した保険者番号

    Returns:
        fund_id

    Raises:
        ValueError: insurer_number が空 / 不正
        FundNotFoundError: 対応する fund_id が見つからない
        FundAmbiguousError: fund_id が複数見つかる
    """
    candidates = _build_insurer_number_candidates(insurer_number)
    fund_ids = _fetch_fund_ids_by_candidates(candidates)

    if not fund_ids:
        raise FundNotFoundError(
            f"fund_id not found for insurer_number={insurer_number!r} candidates={candidates!r}"
        )

    unique_ids = sorted(set(fund_ids))
    if len(unique_ids) > 1:
        raise FundAmbiguousError(
            f"multiple fund_id found for insurer_number={insurer_number!r}: {unique_ids!r}"
        )

    return unique_ids[0]