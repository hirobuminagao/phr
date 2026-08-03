#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from typing import Any
from scripts.lib.identity.base_norm import base_normalize
from scripts.lib.identity.primitive.convert import to_fullwidth_ascii


ADDRESS_EXPORT_MAX_CP932_BYTES = 80


@dataclass(frozen=True)
class AddressExportNormalizationResult:
    value: str | None
    ok: bool
    truncated: bool
    cp932_bytes: int
    reason: str | None


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

    digits = "".join(ch for ch in unicodedata.normalize("NFKC", str(base)) if ch.isdigit())

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


def normalize_postal_code_export(postal_code: Any) -> str | None:
    """郵便番号をXML出力用のXXX-XXXX形式へ正規化する。"""
    digits = build_postal_code_match(postal_code)
    if digits is None or len(digits) != 7:
        return None
    return f"{digits[:3]}-{digits[3:]}"


def truncate_cp932_text(value: str, max_bytes: int = ADDRESS_EXPORT_MAX_CP932_BYTES) -> tuple[str, bool, int]:
    """Return text trimmed to a CP932 byte limit without splitting characters."""
    out: list[str] = []
    used = 0
    truncated = False
    for ch in value:
        try:
            b = ch.encode("cp932")
        except UnicodeEncodeError:
            truncated = True
            continue
        if used + len(b) > max_bytes:
            truncated = True
            break
        out.append(ch)
        used += len(b)
    return "".join(out), truncated, used


def normalize_address_export_result(address: Any) -> AddressExportNormalizationResult:
    """住所をXML出力用に正規化し、80バイト切り詰め有無も返す。"""
    value = build_address_match(address, None)
    if value is None:
        return AddressExportNormalizationResult(
            value=None,
            ok=False,
            truncated=False,
            cp932_bytes=0,
            reason="missing_raw_or_base_norm",
        )
    value = "".join(ch for ch in value if ch not in " \t\r\n　")
    value, truncated, byte_len = truncate_cp932_text(value, ADDRESS_EXPORT_MAX_CP932_BYTES)
    if not value:
        return AddressExportNormalizationResult(
            value=None,
            ok=False,
            truncated=truncated,
            cp932_bytes=0,
            reason="empty_after_normalize",
        )
    return AddressExportNormalizationResult(
        value=value,
        ok=True,
        truncated=truncated,
        cp932_bytes=byte_len,
        reason="TRUNCATED_TO_80_CP932_BYTES" if truncated else None,
    )


def normalize_address_export(address: Any) -> str | None:
    """住所を空白なし・全角寄せ・CP932換算80バイト以内にする。"""
    return normalize_address_export_result(address).value
