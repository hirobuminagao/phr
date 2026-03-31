

from __future__ import annotations

import hashlib



def _build_identity_hash_input(
    *,
    person_id_custom: str,
    name_kana_full_match: str,
    gender_code_match: str,
) -> str:
    """identity_hash 用の連結入力文字列を生成する。

    v1.1.0 固定方針:
    - 連結順は固定
    - 区切り文字は `|`
    - builder は再正規化しない
    """
    return f"{person_id_custom}|{name_kana_full_match}|{gender_code_match}"



def _sha256_hexdigest(text: str) -> str:
    """UTF-8 で SHA-256 を計算し lowercase hex digest を返す。"""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()



def build_identity_hash(
    *,
    person_id_custom: str | None,
    name_kana_full_match: str | None,
    gender_code_match: str | None,
) -> dict:
    """identity_hash を生成する。

    v1.1.0 方針:

    - builder は再正規化しない
    - field / builder 側で生成済みの canonical 値だけを受け取る
    - 必須材料が1つでも欠ければ生成しない
    - 連結順は以下で固定する
      1. `person_id_custom`
      2. `name_kana_full_match`
      3. `gender_code_match`
    - 連結形式は `{person_id_custom}|{name_kana_full_match}|{gender_code_match}`
    - SHA-256 / UTF-8 / lowercase hex digest を採用する
    """
    inputs = {
        "person_id_custom": person_id_custom,
        "name_kana_full_match": name_kana_full_match,
        "gender_code_match": gender_code_match,
    }

    missing_fields = [name for name, value in inputs.items() if value in (None, "")]
    if missing_fields:
        return {
            "name": "identity_hash",
            "value": None,
            "ok": False,
            "missing_fields": missing_fields,
            "upstream_missing_fields": missing_fields[:],
            "reason": "missing_required_fields",
        }

    joined_input = _build_identity_hash_input(
        person_id_custom=person_id_custom,
        name_kana_full_match=name_kana_full_match,
        gender_code_match=gender_code_match,
    )
    value = _sha256_hexdigest(joined_input)

    if value == "":
        return {
            "name": "identity_hash",
            "value": None,
            "ok": False,
            "missing_fields": [],
            "upstream_missing_fields": [],
            "reason": "hash_returned_empty",
        }

    return {
        "name": "identity_hash",
        "value": value,
        "ok": True,
        "missing_fields": [],
        "upstream_missing_fields": [],
        "reason": None,
    }