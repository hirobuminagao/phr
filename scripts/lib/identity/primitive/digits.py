

from __future__ import annotations


def extract_digits(text: str | None) -> str | None:
    """文字列から数字だけを抽出する。

    - `None` はそのまま返す
    - 数字が1文字も残らない場合は `None` を返す
    """
    if text is None:
        return None

    value = "".join(ch for ch in text if ch.isdigit())
    return value if value != "" else None


def strip_leading_zeros_keep_zero(text: str | None) -> str | None:
    """先頭0を除去し、全て0の場合は `0` を返す。

    - `None` はそのまま返す
    - `"000"` のような入力は `"0"` に正規化する
    """
    if text is None:
        return None

    stripped = text.lstrip("0")
    return stripped if stripped != "" else "0"


def zero_pad(text: str | None, length: int) -> str | None:
    """指定桁数になるよう左側を0で埋める。

    - `None` はそのまま返す
    - 最小実装として `str.zfill()` を用いる
    """
    if text is None:
        return None
    return text.zfill(length)


def has_exact_length(text: str | None, length: int) -> bool:
    """指定桁数と一致するかを返す。

    - `None` は `False`
    - 長さが `length` と一致する場合のみ `True`
    - 数字以外を含むかどうかはここでは判定しない
    """
    if text is None:
        return False
    return len(text) == length


def has_max_length(text: str | None, max_length: int) -> bool:
    """指定した最大桁数以下かを返す。

    - `None` は `False`
    - 長さが `max_length` 以下なら `True`
    - 数字以外を含むかどうかはここでは判定しない

    insurer_number のように、正規化後の値が上限桁数を超えていないかを
    確認したい場面で使う。
    """
    if text is None:
        return False
    return len(text) <= max_length