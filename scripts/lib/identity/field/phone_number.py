from __future__ import annotations

from typing import Any

from scripts.lib.identity.base_norm import base_normalize
from scripts.lib.identity.primitive.digits import extract_digits


def normalize_phone_number_export(raw: Any) -> str | None:
    """電話番号をCDA telecom用のtel:数字形式へ正規化する。"""
    normalized = base_normalize(None if raw is None else str(raw))
    digits = extract_digits(normalized)
    return f"tel:{digits}" if digits else None
