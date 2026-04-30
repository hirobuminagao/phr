from __future__ import annotations

from typing import Any, Mapping

from scripts.lib.identity.base_norm import base_normalize
from scripts.lib.identity.kanji_dict import load_kanji_normalization_map
from scripts.lib.identity.primitive.convert import (
    hiragana_to_katakana,
    normalize_small_kana,
    to_fullwidth_ascii,
)
from scripts.lib.identity.primitive.remove import remove_kana_symbols, remove_spaces
from scripts.lib.identity.primitive.split import split_by_delimiter



_HYPHEN_SYMBOLS = ("-", "－", "ー", "―", "ｰ", "‐", "‑", "‒", "–", "—", "⁃")

# 中黒 (middle dot) symbols to remove for match normalization
_MIDDLE_DOT_SYMBOLS = ("・", "･", "•", "・")


def _normalize_name_kanji_norm_full(raw: str | None) -> str | None:
    """漢字氏名の norm 用 full を作る。意味を変える変換は行わない。"""
    base = base_normalize(raw)
    if base is None:
        return None

    normalized = "　".join(part for part in base.replace(" ", "　").split("　") if part != "")
    return normalized or None


def _apply_kanji_normalization_dict(value: str | None, cur: Any, *, use_cache: bool = True) -> str | None:
    """照合用に漢字正規化辞書を適用する。"""
    if value is None:
        return None

    mapping = load_kanji_normalization_map(cur, use_cache=use_cache)
    if not mapping:
        return value

    return "".join(mapping.get(ch, ch) for ch in value)


def _remove_hyphen_symbols(value: str | None) -> str | None:
    if value is None:
        return None

    v = value
    for symbol in _HYPHEN_SYMBOLS:
        v = v.replace(symbol, "")
    return v


# Remove middle dot symbols for match normalization
def _remove_middle_dot_symbols(value: str | None) -> str | None:
    if value is None:
        return None

    v = value
    for symbol in _MIDDLE_DOT_SYMBOLS:
        v = v.replace(symbol, "")
    return v


def _kanji_norm_part_to_match(value: str | None, cur: Any, *, use_cache: bool = True) -> str | None:
    """norm 側の漢字 part を match 用へ変換する。"""
    if value is None:
        return None

    v = _apply_kanji_normalization_dict(value, cur, use_cache=use_cache)
    v = hiragana_to_katakana(v)
    v = normalize_small_kana(v)
    v = remove_kana_symbols(v)
    v = _remove_hyphen_symbols(v)
    v = _remove_middle_dot_symbols(v)
    v = remove_spaces(v)
    v = to_fullwidth_ascii(v)

    if v is None or v == "":
        return None
    return v


def normalize_name_kanji_full(raw: str | None, cur: Any | None = None, *, use_cache: bool = True) -> dict:
    """name_kanji_full の norm / match を生成する。

    方針:
    - norm は DB 格納・表示用の最小整形に留める
    - 漢字の意味を変える辞書変換は norm では行わない
    - match は照合専用とし、辞書変換やかな寄せは match 側でのみ行う
    - cur が渡されない場合、match は未生成(None)とする
    """
    field_norm = _normalize_name_kanji_norm_full(raw)

    if field_norm is None:
        base = base_normalize(raw)
        return {
            "field_name": "name_kanji_full",
            "raw": raw,
            "base_norm": base,
            "field_norm": None,
            "match": None,
            "ok": False,
            "missing": True,
            "reason": "missing_raw_or_base_norm" if base is None else "empty_after_normalize",
        }

    match = None
    if cur is not None:
        match = _kanji_norm_part_to_match(field_norm, cur, use_cache=use_cache)

    return {
        "field_name": "name_kanji_full",
        "raw": raw,
        "base_norm": base_normalize(raw),
        "field_norm": field_norm,
        "match": match,
        "ok": True,
        "missing": False,
        "reason": None,
    }


def normalize_name_kanji_full_to_parts(raw: str | None) -> dict:
    """name_kanji_full を family / middle / given へ分解する。

    方針:
    - まず normalize_name_kanji_full で norm 側の full を作る
    - delimiter 分割は primitive.split_by_delimiter へ委譲する
    - family / middle / given は norm 側の parts として扱う
    - match はここでは生成しない
    """
    full_result = normalize_name_kanji_full(raw)
    if not full_result["ok"]:
        return {
            "field_name": "name_kanji_parts",
            "raw": raw,
            "base_norm": full_result["base_norm"],
            "full": None,
            "family": None,
            "middle": None,
            "given": None,
            "ok": False,
            "missing": full_result["missing"],
            "reason": full_result["reason"],
        }

    full = full_result["field_norm"]
    parts = split_by_delimiter(full, delimiter="　", keep_empty=False)

    if not parts:
        return {
            "field_name": "name_kanji_parts",
            "raw": raw,
            "base_norm": full_result["base_norm"],
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

    return {
        "field_name": "name_kanji_parts",
        "raw": raw,
        "base_norm": full_result["base_norm"],
        "full": full,
        "family": family,
        "middle": middle,
        "given": given,
        "ok": True,
        "missing": False,
        "reason": None,
    }


def norm_parts_to_match_parts(parts: Mapping[str, Any], cur: Any, *, use_cache: bool = True) -> dict:
    """norm parts から match parts を生成する。

    方針:
    - split 用の構造情報は norm parts 側で保持する
    - 漢字辞書変換・かな寄せは match 側のみで行う
    - match_full から parts は復元できないため、必ず norm parts を入力にする
    - match_full が必要な場合は、match parts を結合して別途生成する
    """
    family = _kanji_norm_part_to_match(parts.get("family"), cur, use_cache=use_cache)
    middle = _kanji_norm_part_to_match(parts.get("middle"), cur, use_cache=use_cache)
    given = _kanji_norm_part_to_match(parts.get("given"), cur, use_cache=use_cache)

    match_full_parts = [p for p in (family, middle, given) if p not in (None, "")]
    match_full = "".join(match_full_parts) if match_full_parts else None

    return {
        "field_name": "name_kanji_match_parts",
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