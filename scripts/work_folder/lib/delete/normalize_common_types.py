# -*- coding: utf-8 -*-
r"""
加入者系で共通して使う「型ごとの正規化」関数群。
Path: work_folder/phr/lib/normalize_common_types.py
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

from phr.lib.errors import NormalizeError

# 全角数字 → 半角数字
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


def _to_half_digits(s: str) -> str:
    return (s or "").translate(_FW_DIGITS)


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
    """
    - 全角→半角
    - 数字以外は捨てる
    - 空になったら NormalizeError
    """
    s = _to_half_digits(raw or "")
    digits = "".join(ch for ch in s if ch.isdigit())
    if digits == "":
        where = f" file={src}" if src else ""
        if line_no is not None:
            where += f" line={line_no}"
        raise NormalizeError(
            field=field,
            code="required_digits",
            raw_value=raw or "",
            message=f"{field} が空または数字無しです。{where}",
        )
    return digits


# ------------------------------------------------------------
# 枝番（任意）
# ------------------------------------------------------------

def normalize_branchnumber_optional(raw: str) -> Optional[str]:
    """
    - 全角→半角
    - 数字以外は捨てる
    - 空なら None
    """
    s = _to_half_digits(raw or "")
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits or None


# ------------------------------------------------------------
# 生年月日（YYYYMMDD）
# ------------------------------------------------------------

def normalize_birth_yyyymmdd(
    raw: str,
    *,
    src: Optional[str] = None,
    line_no: Optional[int] = None,
) -> str:
    """
    受け取り例:
        - '19750307'
        - '1975-03-07'
        - '1975/3/7'
    などを YYYYMMDD に揃える。解釈不能なら NormalizeError。
    """
    original = raw or ""
    t = _to_half_digits(original).strip()

    # すでに YYYYMMDD ならそのまま
    if len(t) == 8 and t.isdigit():
        return t

    # 区切り記号で分解して Y/M/D を推測
    parts = [p for p in re.split(r"[^\d]+", t) if p]
    if len(parts) == 3:
        if len(parts[0]) == 4:
            y, m, d = parts[0], parts[1], parts[2]
        else:
            # '03/07/1975' みたいなパターン
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
# 性別コード
# ------------------------------------------------------------

def normalize_gender_code(raw: str) -> str:
    """
    - 男性: '1','男','male','m' → '1'
    - 女性: '2','女','female','f' → '2'
    - それ以外 → '9'
    """
    t = (raw or "").strip().lower()
    if t in {"1", "男", "male", "m"}:
        return "1"
    if t in {"2", "女", "female", "f"}:
        return "2"
    return "9"


# ------------------------------------------------------------
# 記号（半角主体 + 数字抽出）
# ------------------------------------------------------------

def normalize_insurance_symbol(raw: str) -> tuple[str, Optional[int]]:
    """
    - 全角→半角
    - スペース類削除
    - 長音記号を '-'、中点を '･' に揃える
    - 中の数字を全部くっつけて int にしたものを返す（無ければ None）
    """
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
    """
    フォルダ名から 8 桁の数字を抜き出して int で返す。
    - 例: '06110779' → 6110779 ではなく 6110779? ではなく… という話は置いておき、
      ひとまず「8桁数字として妥当か」をチェックする。
    """
    name = folder.name
    digits = "".join(ch for ch in name if ch.isdigit())
    if len(digits) != 8:
        raise NormalizeError(
            field="insurer_folder",
            code="invalid_folder_name",
            raw_value=name,
            message=f"フォルダ名から 8 桁の保険者番号を取得できません: {name}",
        )
    iv = int(digits)
    if not (0 <= iv <= 99999999):
        raise NormalizeError(
            field="insurer_folder",
            code="out_of_range",
            raw_value=name,
            message=f"保険者番号が範囲外です: {iv}",
        )
    return iv


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
    """
    - '' / None → None
    - 'YYYYMMDD' → 'YYYY-MM-DD'
    - それ以外 → NormalizeError
    """
    s = (raw or "").strip() if raw is not None else ""
    if not s:
        return None

    s = _to_half_digits(s)

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
# 汎用日付正規化 → ISO 日付文字列 (YYYY-MM-DD)
# ------------------------------------------------------------

def normalize_date_iso(
    raw: Optional[str],
    *,
    field: str = "date",
    src: Optional[str] = None,
    line_no: Optional[int] = None,
) -> Optional[str]:
    """
    汎用日付正規化。

    想定入力:
        - '2025-12-10'
        - '2025/12/10'
        - '2025.12.10'
        - '20251210'
        - '2025 12 10'
        - 全角数字混じり など

    挙動:
        - None / 空文字 → None
        - 解釈可能なもの → 'YYYY-MM-DD' 文字列
        - 解釈不能 → NormalizeError
    """
    original = raw or ""
    t = _to_half_digits(original).strip()

    if not t:
        return None

    # すでに 'YYYY-MM-DD' 形式っぽい場合（ざっくりチェック）
    # ※厳密チェックはこのあと共通ロジックでやる
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
                # ここで落ちたら下の汎用パーサに回す
                pass

    # 8桁数字の場合（YYYYMMDD想定）
    if len(t) == 8 and t.isdigit():
        return yyyymmdd_to_iso_date(
            t,
            field=field,
            src=src,
            line_no=line_no,
        )

    # 区切り記号で分割して Y/M/D を推測
    parts = [p for p in re.split(r"[^\d]+", t) if p]
    if len(parts) == 3:
        if len(parts[0]) == 4:
            # 先頭4桁が年とみなす
            y, m, d = parts[0], parts[1], parts[2]
        else:
            # 年が最後に来る '03/07/1975' など
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
