# -*- coding: utf-8 -*-
"""
kanji_dict.py

漢字正規化辞書のロード用ユーティリティ。

責務:
- dev_phr.identity_kanji_normalization から辞書を読む
- original_char -> normalized_char の dict を返す

備考:
- 実際の置換処理そのものは common.py 側の
  apply_kanji_normalization_dict(...) に寄せる
- このモジュールは DB から辞書を取得する責務に絞る
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

# プロセス内キャッシュ
# - 同一プロセス中では 1 回ロードした辞書を再利用する
# - 辞書更新を即時反映したいケースでは use_cache=False を渡す
_KANJI_NORMALIZATION_MAP_CACHE: Optional[Dict[str, str]] = None


def load_kanji_normalization_map(cur: Any, *, use_cache: bool = True) -> Dict[str, str]:
    """dev_phr.identity_kanji_normalization から漢字正規化辞書を読む。

    Parameters
    ----------
    cur:
        mysql.connector の cursor(dictionary=True) を想定。
    use_cache:
        True の場合、プロセス内キャッシュがあれば再利用する。

    Returns
    -------
    dict[str, str]
        {original_char: normalized_char} の辞書。

    Notes
    -----
    - 空文字や NULL は読み飛ばす
    - original_char が重複していた場合は後勝ち
    """
    global _KANJI_NORMALIZATION_MAP_CACHE

    if use_cache and _KANJI_NORMALIZATION_MAP_CACHE is not None:
        return dict(_KANJI_NORMALIZATION_MAP_CACHE)

    sql = """
        SELECT
            original_char,
            normalized_char
        FROM dev_phr.identity_kanji_normalization
        ORDER BY normalization_id
    """
    cur.execute(sql)
    rows = cur.fetchall() or []

    result: Dict[str, str] = {}
    for row in rows:
        original = row.get("original_char") if isinstance(row, Mapping) else None
        normalized = row.get("normalized_char") if isinstance(row, Mapping) else None

        if not original or not normalized:
            continue

        result[str(original)] = str(normalized)

    if use_cache:
        _KANJI_NORMALIZATION_MAP_CACHE = dict(result)

    return result