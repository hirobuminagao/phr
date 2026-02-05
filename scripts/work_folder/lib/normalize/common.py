# -*- coding: utf-8 -*-
r"""
共通の正規化ユーティリティ（型・日付・数字抽出など）。

Path: work_folder/phr/lib/normalize/common.py
"""

from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Optional, Tuple

from phr.lib.errors import NormalizeError

# 全角数字 → 半角数字（軽量）
_FW_DIGITS = str.maketrans("０１２３４５６７８９", "0123456789")

# 全角 <-> 半角（記号含む）用
_FW2HW = str.maketrans(
    "０１２３４５６７８９"
    "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ"
    "ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ"
    "－　・，．／＼＿（）［］｛｝：；＠！？”’＋＊＝＜＞｜＾～",
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz"
    "- ･,./\\_()[]{}:;@!\"' +*=<>|^~",
)

_DASHES = {"ー", "―", "—", "ｰ", "－"}
_MIDDOTS = {"・", "･"}


# ------------------------------------------------------------
# 基本：数字系
# ------------------------------------------------------------

def to_half_digits(s: str) -> str:
    """全角数字→半角数字（NFKCでもOKだが、ここは軽く translate を優先）"""
    if s is None:
        return ""
    return (str(s) or "").translate(_FW_DIGITS)


def digits_only(s: str) -> str:
    """文字列中の数字だけ抽出"""
    if not s:
        return ""
    return "".join(ch for ch in to_half_digits(s) if ch.isdigit())


def split_digit_chunks(s: str) -> list[str]:
    """任意区切り値を → [数字ブロック, ...]"""
    src = to_half_digits(s or "")
    return [p for p in re.split(r"[^\d]+", src) if p]


# ------------------------------------------------------------
# 保険証番号（必須）
# ------------------------------------------------------------

def normalize_insurance_number_required(
    raw: str,
    *,
    field: str = "insurance_number",
    src: Optional[str] = None,
    line_no: Optional[int] = None,
) -> str:
    s = to_half_digits(raw or "")
    d = "".join(ch for ch in s if ch.isdigit())
    if d == "":
        where = f" file={src}" if src else ""
        if line_no is not None:
            where += f" line={line_no}"
        raise NormalizeError(
            field=field,
            code="required_digits",
            raw_value=raw or "",
            message=f"{field} が空または数字無しです。{where}",
        )
    return d


# ------------------------------------------------------------
# 枝番（任意）
# ------------------------------------------------------------

def normalize_branchnumber_optional(raw: str) -> Optional[str]:
    s = to_half_digits(raw or "")
    d = "".join(ch for ch in s if ch.isdigit())
    return d or None


# ------------------------------------------------------------
# 生年月日（YYYYMMDD）
# ------------------------------------------------------------

def normalize_birth_yyyymmdd(
    raw: str,
    *,
    src: Optional[str] = None,
    line_no: Optional[int] = None,
) -> str:
    original = raw or ""
    t = to_half_digits(original).strip()

    if len(t) == 8 and t.isdigit():
        return t

    parts = [p for p in re.split(r"[^\d]+", t) if p]
    if len(parts) == 3:
        if len(parts[0]) == 4:
            y, m, d = parts[0], parts[1], parts[2]
        else:
            y, m, d = parts[2], parts[0], parts[1]
        try:
            y_i = int(y)
            m_i = int(m)
            d_i = int(d)
            if not (1 <= m_i <= 12 and 1 <= d_i <= 31):
                raise ValueError
            return f"{y_i:04d}{m_i:02d}{d_i:02d}"
        except Exception:
            pass

    where = f" file={src}" if src else ""
    if line_no is not None:
        where += f" line={line_no}"

    raise NormalizeError(
        field="birth",
        code="invalid_date",
        raw_value=original,
        message=f"生年月日を YYYYMMDD に解釈できませんでした。{where}",
    )


# ------------------------------------------------------------
# YYYYMMDD → ISO 日付文字列 (YYYY-MM-DD)
# ------------------------------------------------------------

def yyyymmdd_to_iso_date(
    raw: Optional[str],
    *,
    field: str = "date",
    src: Optional[str] = None,
    line_no: Optional[int] = None,
) -> Optional[str]:
    s = (raw or "").strip() if raw is not None else ""
    if not s:
        return None

    s = to_half_digits(s)
    if len(s) != 8 or not s.isdigit():
        where = f" file={src}" if src else ""
        if line_no is not None:
            where += f" line={line_no}"
        raise NormalizeError(
            field=field,
            code="invalid_yyyymmdd",
            raw_value=raw or "",
            message=f"{field} に YYYYMMDD 形式でない値が渡されました: {s} {where}",
        )

    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"


# ------------------------------------------------------------
# 汎用日付正規化 → ISO (YYYY-MM-DD)
# ------------------------------------------------------------

def normalize_date_iso(
    raw: Optional[str],
    *,
    field: str = "date",
    src: Optional[str] = None,
    line_no: Optional[int] = None,
) -> Optional[str]:
    original = raw or ""
    t = to_half_digits(original).strip()

    if not t:
        return None

    if len(t) == 10 and t[4] == "-" and t[7] == "-":
        parts = t.split("-")
        if len(parts) == 3 and all(p.isdigit() for p in parts):
            y, m, d = parts
            try:
                y_i = int(y)
                m_i = int(m)
                d_i = int(d)
                if not (1 <= m_i <= 12 and 1 <= d_i <= 31):
                    raise ValueError
                return f"{y_i:04d}-{m_i:02d}-{d_i:02d}"
            except Exception:
                pass

    if len(t) == 8 and t.isdigit():
        return yyyymmdd_to_iso_date(t, field=field, src=src, line_no=line_no)

    parts = [p for p in re.split(r"[^\d]+", t) if p]
    if len(parts) == 3:
        if len(parts[0]) == 4:
            y, m, d = parts[0], parts[1], parts[2]
        else:
            y, m, d = parts[2], parts[0], parts[1]
        try:
            y_i = int(y)
            m_i = int(m)
            d_i = int(d)
            if not (1 <= m_i <= 12 and 1 <= d_i <= 31):
                raise ValueError
            return f"{y_i:04d}-{m_i:02d}-{d_i:02d}"
        except Exception:
            pass

    where = f" file={src}" if src else ""
    if line_no is not None:
        where += f" line={line_no}"

    raise NormalizeError(
        field=field,
        code="invalid_date",
        raw_value=original,
        message=f"{field} を ISO 日付 (YYYY-MM-DD) に解釈できませんでした。{where}",
    )


# ------------------------------------------------------------
# 性別コード
# ------------------------------------------------------------

def normalize_gender_code(raw: str) -> str:
    t = (raw or "").strip().lower()
    if t in {"1", "男", "male", "m"}:
        return "1"
    if t in {"2", "女", "female", "f"}:
        return "2"
    return "9"


# ------------------------------------------------------------
# 記号（半角主体 + 数字抽出）
# ------------------------------------------------------------

def normalize_insurance_symbol(raw: str) -> Tuple[str, Optional[int]]:
    s = (raw or "").translate(_FW2HW)
    s = s.replace("\u3000", " ")
    s = re.sub(r"\s+", "", s)

    buf = []
    for ch in s:
        if ch in _DASHES:
            buf.append("-")
        elif ch in _MIDDOTS:
            buf.append("･")
        else:
            buf.append(ch)
    s_norm = "".join(buf)

    digits = re.findall(r"\d+", s_norm)
    digits_val = int("".join(digits)) if digits else None
    return s_norm, digits_val


# ------------------------------------------------------------
# フォルダ名 → 保険者番号（8桁 int）
# ------------------------------------------------------------

def normalize_insurer_folder_name_to_int(folder: Path) -> int:
    name = folder.name
    d = "".join(ch for ch in name if ch.isdigit())
    if len(d) != 8:
        raise NormalizeError(
            field="insurer_folder",
            code="invalid_folder_name",
            raw_value=name,
            message=f"フォルダ名から 8 桁の保険者番号を取得できません: {name}",
        )
    iv = int(d)
    if not (0 <= iv <= 99999999):
        raise NormalizeError(
            field="insurer_folder",
            code="out_of_range",
            raw_value=name,
            message=f"保険者番号が範囲外です: {iv}",
        )
    return iv
