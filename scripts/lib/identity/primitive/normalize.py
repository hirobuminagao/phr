from __future__ import annotations

import unicodedata


def to_nfkc(text: str | None) -> str | None:
    """NFKC 正規化を行う。

    - `None` はそのまま返す
    - 文字列は `unicodedata.normalize("NFKC", ...)` で正規化する
    """
    if text is None:
        return None
    return unicodedata.normalize("NFKC", text)


def normalize_spaces(text: str | None) -> str | None:
    """空白系の基本正規化を行う。

    v1.1.0 の最小実装として、以下のみを対象とする。

    - 全角空白 `\u3000` を半角空白へ寄せる
    - NBSP (`\u00A0`) を半角空白へ寄せる

    それ以外の trim や空文字判定はここでは行わない。
    """
    if text is None:
        return None

    return text.replace("\u3000", " ").replace("\u00A0", " ")


def trim(text: str | None) -> str | None:
    """前後空白を除去する。

    - `None` はそのまま返す
    - ここでは `str.strip()` のみを行う
    """
    if text is None:
        return None
    return text.strip()


def empty_to_none(text: str | None) -> str | None:
    """空値を `None` に統一する。

    v1.1.0 の最小実装として、以下を空とみなす。

    - `None`
    - 空文字 `""`

    trim や空白正規化は呼び出し側で先に行う前提とする。
    """
    if text is None:
        return None
    if text == "":
        return None
    return text


# ------------------------------------------------------------
# 記号正規化
# ------------------------------------------------------------

import re

_HYPHEN_LIKE_RE = re.compile(r"[ーｰ－―‐−]")


def unify_hyphen(text: str | None) -> str | None:
    """ハイフン類・長音符類を半角ハイフン `-` に統一する。

    対象:
    - 長音符: ー
    - 半角長音: ｰ
    - 全角ハイフン: －
    - ダッシュ系: ―, ‐, −

    例:
    - "01－0023" → "01-0023"
    - "Aー001" → "A-001"

    None はそのまま返す。
    """
    if text is None:
        return None
    return _HYPHEN_LIKE_RE.sub("-", text)
