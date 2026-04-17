from __future__ import annotations

from scripts.lib.identity.base_norm import base_normalize
from scripts.lib.identity.primitive.convert import (
    hiragana_to_katakana,
    normalize_small_kana,
    to_fullwidth_ascii,
)
from scripts.lib.identity.primitive.remove import remove_spaces, remove_kana_symbols
from scripts.lib.identity.primitive.split import split_by_delimiter


def normalize_name_kana_full(raw: str | None) -> dict:
    """name_kana_full の field_norm / match を生成する。

    v1.1.0 仕様:

    - base_norm を起点にする
    - ひらがな → カタカナ
    - 小書き → 大文字
    - スペース除去
    - 中黒・長音・ハイフン等を除去（照合用）
    - 全角カナへ寄せる
    - field_norm は格納・表示用の norm 値
    - match は照合用の値
    - field_norm と match は同一とは限らない
    """

    base = base_normalize(raw)

    if base is None:
        return {
            "field_name": "name_kana_full",
            "raw": raw,
            "base_norm": None,
            "field_norm": None,
            "match": None,
            "ok": False,
            "missing": True,
            "reason": "missing_raw_or_base_norm",
        }

    v = hiragana_to_katakana(base)
    v = remove_spaces(v)
    v = remove_kana_symbols(v)
    v = to_fullwidth_ascii(v)

    if v is None or v == "":
        return {
            "field_name": "name_kana_full",
            "raw": raw,
            "base_norm": base,
            "field_norm": None,
            "match": None,
            "ok": False,
            "missing": True,
            "reason": "empty_after_normalize",
        }

    return {
        "field_name": "name_kana_full",
        "raw": raw,
        "base_norm": base,
        "field_norm": v,
        "match": v,
        "ok": True,
        "missing": False,
        "reason": None,
    }


def normalize_name_kana_full_to_parts(raw: str | None) -> dict:
    """name_kana_full を family / middle / given へ分解する。

    方針:

    - base_norm を起点にする
    - ひらがな → カタカナ
    - 小書き → 大文字
    - ASCII は全角へ寄せる
    - split のため、スペース除去は行わない
    - 半角スペースを全角スペースへ統一する
    - delimiter 分割は primitive.split_by_delimiter へ委譲する
    - 分割後は以下で解釈する
      - 1要素: family のみ
      - 2要素: family / given
      - 3要素以上: family / middle(2番目〜末尾手前を全角スペース結合) / given
    - full は norm 側の値として扱う
    - family / middle / given も match ではなく norm 側の parts として扱う
    - match parts が必要な場合は、norm parts を入力にして別関数で生成する
    - match_full から parts は復元しない
    - full は分解後の parts を全角スペースで再結合した値とする
    """

    base = base_normalize(raw)

    if base is None:
        return {
            "field_name": "name_kana_parts",
            "raw": raw,
            "base_norm": None,
            "full": None,
            "family": None,
            "middle": None,
            "given": None,
            "ok": False,
            "missing": True,
            "reason": "missing_raw_or_base_norm",
        }

    v = hiragana_to_katakana(base)
    v = to_fullwidth_ascii(v)

    if v is None or v == "":
        return {
            "field_name": "name_kana_parts",
            "raw": raw,
            "base_norm": base,
            "full": None,
            "family": None,
            "middle": None,
            "given": None,
            "ok": False,
            "missing": True,
            "reason": "empty_after_normalize",
        }

    v = v.replace(" ", "　")

    parts = split_by_delimiter(v, delimiter="　", keep_empty=False)
    if parts is None or len(parts) == 0:
        return {
            "field_name": "name_kana_parts",
            "raw": raw,
            "base_norm": base,
            "full": None,
            "family": None,
            "middle": None,
            "given": None,
            "ok": False,
            "missing": True,
            "reason": "empty_after_split",
        }

    family: str | None
    middle: str | None
    given: str | None

    if len(parts) == 1:
        family = parts[0]
        middle = None
        given = None
    elif len(parts) == 2:
        family = parts[0]
        middle = None
        given = parts[1]
    else:
        family = parts[0]
        middle = "　".join(parts[1:-1])
        given = parts[-1]

    full = "　".join(parts)

    return {
        "field_name": "name_kana_parts",
        "raw": raw,
        "base_norm": base,
        "full": full,
        "family": family,
        "middle": middle,
        "given": given,
        "ok": True,
        "missing": False,
        "reason": None,
    }


def _kana_norm_part_to_match(value: str | None) -> str | None:
    """norm 側の kana part を match 用へ変換する。"""
    if value is None:
        return None

    v = normalize_small_kana(value)
    v = remove_kana_symbols(v)
    v = remove_spaces(v)
    v = to_fullwidth_ascii(v)

    if v is None or v == "":
        return None
    return v



def norm_parts_to_match_parts(parts: dict) -> dict:
    """norm parts から match parts を生成する。

    方針:
    - split 用の構造情報は norm parts 側で保持する
    - match_full から parts は復元できないため、必ず norm parts を入力にする
    - family / middle / given を個別に match 用へ変換する
    - match_full が必要な場合は、match parts を結合して別途生成する
    """
    family = _kana_norm_part_to_match(parts.get("family"))
    middle = _kana_norm_part_to_match(parts.get("middle"))
    given = _kana_norm_part_to_match(parts.get("given"))

    match_full_parts = [p for p in (family, middle, given) if p not in (None, "")]
    match_full = "".join(match_full_parts) if match_full_parts else None

    return {
        "field_name": "name_kana_match_parts",
        "raw": parts.get("raw"),
        "base_norm": parts.get("base_norm"),
        "full": parts.get("full"),
        "family": family,
        "middle": middle,
        "given": given,
        "match_full": match_full,
        "ok": parts.get("ok", False),
        "missing": parts.get("missing", True),
        "reason": parts.get("reason"),
    }