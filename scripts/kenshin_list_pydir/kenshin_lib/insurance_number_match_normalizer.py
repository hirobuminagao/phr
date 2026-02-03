# -*- coding: utf-8 -*-
from __future__ import annotations
import unicodedata
import re

def normalize_insurance_number_for_match(value: str | None) -> str:
    if not value:
        return ""

    s = unicodedata.normalize("NFKC", str(value))
    # 数字以外を除去
    s = re.sub(r"[^0-9]", "", s)
    return s
