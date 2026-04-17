

from __future__ import annotations

from scripts.lib.identity.base_norm import base_normalize
from scripts.lib.identity.primitive.split import split_by_delimiter


def normalize_name_kanji_full(raw: str | None) -> dict:
    """name_kanji_full の norm を生成する。

    方針:
    - base_norm を起点にする
    - 漢字氏名の norm はまず match を考えず、field_norm のみを安定生成する
    - 前後trim後、半角/全角スペースは全角スペースへ統一する
    - 連続スペースは 1 個に圧縮する
    - full は全角スペース区切りで再結合した値を返す
    """

    base = base_normalize(raw)

    if base is None:
        return {
            "field_name": "name_kanji_full",
            "raw": raw,
            "base_norm": None,
            "field_norm": None,
            "ok": False,
            "missing": True,
            "reason": "missing_raw_or_base_norm",
        }

    normalized = "　".join(part for part in base.replace(" ", "　").split("　") if part != "")

    if normalized == "":
        return {
            "field_name": "name_kanji_full",
            "raw": raw,
            "base_norm": base,
            "field_norm": None,
            "ok": False,
            "missing": True,
            "reason": "empty_after_normalize",
        }

    return {
        "field_name": "name_kanji_full",
        "raw": raw,
        "base_norm": base,
        "field_norm": normalized,
        "ok": True,
        "missing": False,
        "reason": None,
    }


def normalize_name_kanji_full_to_parts(raw: str | None) -> dict:
    """name_kanji_full を family / middle / given へ分解する。

    方針:
    - まず normalize_name_kanji_full で full norm を作る
    - delimiter 分割は primitive.split_by_delimiter へ委譲する
    - 分割後は以下で解釈する
      - 1要素: family のみ
      - 2要素: family / given
      - 3要素以上: family / middle(2番目〜末尾手前を全角スペース結合) / given
    - match はまだここでは作らない
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