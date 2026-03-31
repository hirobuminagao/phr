from __future__ import annotations


def remove_control_chars(text: str | None) -> str | None:
    """制御文字を除去する。

    v1.1.0 の最小実装として、以下を除去対象とする。

    - Unicode category が `Cc` の文字

    ただし、`None` はそのまま返す。
    """
    if text is None:
        return None

    return "".join(ch for ch in text if ch.isprintable() or ch in ("\t", "\n", "\r"))


def remove_spaces(text: str | None) -> str | None:
    """空白をすべて除去する。

    v1.1.0 の最小実装として、以下を除去対象とする。

    - 半角スペース
    - 全角スペース
    - NBSP (`\u00A0`)

    ただし、`None` はそのまま返す。
    """
    if text is None:
        return None

    return (
        text.replace(" ", "")
        .replace("\u3000", "")
        .replace("\u00A0", "")
    )


def remove_kana_symbols(text: str | None) -> str | None:
    """氏名カナ照合に不要な記号を除去する。

    v1.1.0 の最小実装として、以下を除去対象とする。

    - 中黒: `・`
    - 長音・ハイフン類: `ー`, `ｰ`, `-`, `－`, `―`, `‐`, `−`

    ただし、`None` はそのまま返す。
    """
    if text is None:
        return None

    remove_chars = "・ーｰ-－―‐−"
    return text.translate(str.maketrans("", "", remove_chars))


def remove_symbol_noise(text: str | None) -> str | None:
    """記号系ノイズを除去する。

    v1.1.0 の最小実装として、以下を除去対象とする。

    - 括弧: `(`, `)`, `（`, `）`
    - スラッシュ: `/`, `／`
    - シャープ: `#`, `＃`
    - アスタリスク: `*`, `＊`
    - プラス: `+`, `＋`
    - 記号: `※`

    ただし、`None` はそのまま返す。
    """
    if text is None:
        return None

    remove_chars = "()（）/／#＃*＊+＋※"
    return text.translate(str.maketrans("", "", remove_chars))
