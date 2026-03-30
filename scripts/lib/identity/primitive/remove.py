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
