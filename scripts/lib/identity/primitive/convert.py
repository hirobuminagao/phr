from __future__ import annotations

# 小書きカナ → 大文字カナ
_SMALL_TO_LARGE_KANA_MAP = str.maketrans(
    {
        "ァ": "ア",
        "ィ": "イ",
        "ゥ": "ウ",
        "ェ": "エ",
        "ォ": "オ",
        "ッ": "ツ",
        "ャ": "ヤ",
        "ュ": "ユ",
        "ョ": "ヨ",
        "ヮ": "ワ",
        "ヵ": "カ",
        "ヶ": "ケ",
        "ぁ": "あ",
        "ぃ": "い",
        "ぅ": "う",
        "ぇ": "え",
        "ぉ": "お",
        "っ": "つ",
        "ゃ": "や",
        "ゅ": "ゆ",
        "ょ": "よ",
        "ゎ": "わ",
        "ゕ": "か",
        "ゖ": "け",
    }
)


def hiragana_to_katakana(text: str | None) -> str | None:
    """ひらがなをカタカナへ変換する。

    - `None` はそのまま返す
    - ひらがな以外の文字はそのまま保持する
    - 半角カナ吸収はここでは行わない（NFKC 側に委ねる）
    """
    if text is None:
        return None

    out_chars: list[str] = []
    for ch in text:
        code = ord(ch)
        if 0x3041 <= code <= 0x3096:
            out_chars.append(chr(code + 0x60))
        else:
            out_chars.append(ch)
    return "".join(out_chars)


def to_lower(text: str | None) -> str | None:
    """文字列を小文字へ寄せる。

    - `None` はそのまま返す
    - Python の `str.lower()` を用いる
    - 意味解釈は行わず、単純な文字形変換だけを担当する
    """
    if text is None:
        return None
    return text.lower()


def to_upper(text: str | None) -> str | None:
    """文字列を大文字へ寄せる。

    - `None` はそのまま返す
    - Python の `str.upper()` を用いる
    - 意味解釈は行わず、単純な文字形変換だけを担当する
    """
    if text is None:
        return None
    return text.upper()


def normalize_small_kana(text: str | None) -> str | None:
    """小書きカナを大文字へ正規化する。

    - `None` はそのまま返す
    - カタカナ/ひらがなの小書き文字のみを対象とする
    """
    if text is None:
        return None
    return text.translate(_SMALL_TO_LARGE_KANA_MAP)


def to_halfwidth_ascii(text: str | None) -> str | None:
    """英数字と一部 ASCII 記号を半角へ寄せる。

    v1.1.0 の最小実装として、全角 ASCII 相当文字（！〜～）を半角へ寄せる。
    それ以外の文字はそのまま保持する。
    """
    if text is None:
        return None

    out_chars: list[str] = []
    for ch in text:
        code = ord(ch)
        if 0xFF01 <= code <= 0xFF5E:
            out_chars.append(chr(code - 0xFEE0))
        else:
            out_chars.append(ch)
    return "".join(out_chars)


def to_fullwidth_ascii(text: str | None) -> str | None:
    """英数字と一部 ASCII 記号を全角へ寄せる。

    v1.1.0 の最小実装として、半角 ASCII（!〜~）を全角へ寄せる。
    半角スペースは全角スペースへ変換する。
    それ以外の文字はそのまま保持する。
    """
    if text is None:
        return None

    out_chars: list[str] = []
    for ch in text:
        code = ord(ch)
        if ch == " ":
            out_chars.append("　")
        elif 0x21 <= code <= 0x7E:
            out_chars.append(chr(code + 0xFEE0))
        else:
            out_chars.append(ch)
    return "".join(out_chars)
