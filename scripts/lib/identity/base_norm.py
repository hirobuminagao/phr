from __future__ import annotations

from lib.identity.primitive.normalize import empty_to_none, normalize_spaces, to_nfkc, trim
from lib.identity.primitive.remove import remove_control_chars


def base_normalize(raw: str | None) -> str | None:
    """全項目共通の下ごしらえ正規化を行う。

    v1.1.0 の spec に合わせて、以下の順で処理する。

    1. `None` 判定
    2. NFKC 正規化
    3. 制御文字除去
    4. 空白類の基本正規化
    5. trim
    6. 空値を `None` に統一

    ここでは項目依存の意味変換は行わない。
    """
    if raw is None:
        return None

    value = to_nfkc(raw)
    value = remove_control_chars(value)
    value = normalize_spaces(value)
    value = trim(value)
    value = empty_to_none(value)
    return value