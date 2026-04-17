from __future__ import annotations


def split_by_delimiter(
    value: str | None,
    delimiter: str = "　",
    *,
    keep_empty: bool = False,
) -> list[str] | None:
    """delimiter で単純分割した parts を返す。

    primitive の責務に合わせ、ここでは氏名としての解釈は行わない。
    つまり、以下は扱わない。

    - family / middle / given への割当
    - 文字種の正規化
    - スペース正規化
    - trim

    これらは field 側で前処理した上で、本関数を利用する前提とする。

    Args:
        value: 分割対象の文字列
        delimiter: 区切り文字。既定は全角スペース
        keep_empty: 空要素を保持するかどうか

    Returns:
        list[str] | None:
            - value が None の場合は None
            - それ以外は delimiter 分割後の list を返す

    Raises:
        ValueError: delimiter が空文字の場合
    """
    if value is None:
        return None

    if delimiter == "":
        raise ValueError("delimiter must not be empty")

    parts = str(value).split(delimiter)

    if keep_empty:
        return parts

    return [part for part in parts if part != ""]
