#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any
from scripts.lib.identity.base_norm import base_normalize
from scripts.lib.identity.primitive.convert import to_fullwidth_ascii


# ------------------------------------------------------------
# Postal code (match)
# ------------------------------------------------------------

def build_postal_code_match(postal_code_norm: Any) -> str | None:
    """
    郵便番号の照合用値を生成

    ルール:
    - 数字のみ抽出
    - 7桁に0埋め
    - 空 or 数字なし → None
    """
    if postal_code_norm is None:
        return None

    base = base_normalize(postal_code_norm)
    if base is None:
        return None

    digits = "".join(ch for ch in str(base) if ch.isdigit())

    if not digits:
        return None

    return digits.zfill(7)


# ------------------------------------------------------------
# Address (match)
# ------------------------------------------------------------

def build_address_match(
    address_line_norm: Any,
    building_norm: Any,
) -> str | None:
    """
    住所の照合用値を生成

    ルール:
    - address_line_norm がベース
    - building_norm があれば「全角スペース」で結合
    - 空白トリム
    - buildingが空なら address_line のみ
    """
    if address_line_norm is None:
        return None

    address_base = base_normalize(address_line_norm)
    if address_base is None:
        return None

    address = str(address_base).strip()
    if not address:
        return None

    # building はXML側で落としているため、matchも address_line のみを使用
    # subscribers側に合わせて英数字・記号を全角寄せ
    return to_fullwidth_ascii(address)