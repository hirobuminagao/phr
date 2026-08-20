from __future__ import annotations

from datetime import date

from scripts.lib.identity.field.date_field import normalize_date_to_ymd_and_compact


def normalize_birthdate(raw: str | date | None) -> dict:
    """birthdate の field_norm / match を生成する互換入口。"""
    return normalize_date_to_ymd_and_compact(raw, purpose="birthdate")
