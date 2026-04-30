from scripts.lib.identity.base_norm import base_normalize
from scripts.lib.identity.primitive.convert import to_fullwidth_ascii


from __future__ import annotations


def resolve_relationship_name(
    relationship_name_norm: str | None,
    relationship_code_norm: str | None,
) -> str | None:
    """登録用・比較用の続柄名称を決定する。

    方針:
    - 続柄名称がある場合はその値を優先する
    - 続柄名称がなく、続柄コードが 0 / 00 の場合は「本人」とする
    - 続柄名称がなく、続柄コードが 0 / 00 以外ならコード値を返す
    - 両方空なら None を返す
    """
    name = _normalize_optional_text(relationship_name_norm)
    if name is not None:
        return name

    code = _normalize_optional_text(relationship_code_norm)
    if code is None:
        return None

    if code in {"0", "00"}:
        return "本人"

    return code




def _normalize_optional_text(value: str | None) -> str | None:
    """None / 空文字 / 空白のみを None として扱い、全角寄せする。"""
    if value is None:
        return None

    normalized = base_normalize(value)
    if normalized is None:
        return None

    text = str(normalized).strip()
    if text == "":
        return None

    # 続柄は比較用として全角寄せ
    return to_fullwidth_ascii(text)


# 続柄コードの match 用正規化
def normalize_relationship_code_match(
    relationship_code_norm: str | None,
) -> str | None:
    """続柄コードの match 用正規化。

    方針:
    - None / 空白は None
    - "0" は "00" に補正（CSV→Excel問題対策）
    - それ以外はそのまま返す
    """
    code = _normalize_optional_text(relationship_code_norm)
    if code is None:
        return None

    if code == "0":
        return "00"

    return code