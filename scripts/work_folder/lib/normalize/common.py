# -*- coding: utf-8 -*-
r"""
normalize/common.py — 共通の正規化ユーティリティ（数字抽出・日付・記号正規化など）

Path   : scripts/work_folder/lib/normalize/common.py
Project: PHR / work_folder/phr

Purpose:
    - CSV/外部入力の「ゆれ」を吸収し、後段の照合・格納処理が扱える形へ整形する。
    - 例外（NormalizeError）で「この行は採用できない」理由をコード化して返す。

Design (v1.0 as-is):
    - なるべく軽量: translate / regex を中心に、過剰な依存は持たない
    - 日付は厳密な暦チェックまではしない（m=1..12, d=1..31 程度の粗チェック）
    - 返り値は基本的に文字列（digits-only / YYYYMMDD / YYYY-MM-DD）

V1.0 Freeze (Scope / Contract):
    - Inputs:
        - raw 文字列（全角/半角混在、区切り記号、空白、記号ゆれを許容）
    - Outputs:
        - digits-only 文字列、YYYYMMDD、ISO日付(YYYY-MM-DD)、記号正規化済み文字列 など
    - Error policy:
        - 必須フィールドの欠落や形式不正は NormalizeError（field/code/raw_value/message）で通知
        - ここでは "どのテーブルに入れるか" は関知しない（利用側が行スキップ/中断を決める）
    - Non-goals:
        - 住所や氏名などドメイン固有の正規化（subscriber.py 側の責務）
        - 日付の存在検証（2/30 などの厳密判定）

Notes:
    - normalize_insurance_symbol は「表記ゆれを減らした文字列」と「含まれる数字の連結 int?」を返す
      → v1.0 の照合キーは別途定義（本関数は値の標準化のみ）
"""

from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Mapping, Optional, Tuple

from scripts.work_folder.lib.errors import NormalizeError
from scripts.work_folder.lib.normalize.kanji_dict import load_kanji_normalization_map

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

# 氏名 match 用の除去記号
_NAME_MATCH_REMOVE_RE = re.compile(
    r"[・･\-ー―−ｰ‐‑‒–—〜~～_＿()（）\[\]［］{}｛｝/／\\、,，.．。'\"`´]"
)

# 氏名カナ match 用の除去記号
_KANA_MATCH_REMOVE_RE = re.compile(
    r"[・･\-ー―−ｰ‐‑‒–—〜~～_＿()（）\[\]［］{}｛｝/／\\、,，.．。'\"`´]"
)

# ひらがな → カタカナ
_HIRAGANA_TO_KATAKANA = str.maketrans(
    {chr(code): chr(code + 0x60) for code in range(ord("ぁ"), ord("ゖ") + 1)}
)

# 小書きカナ → 通常カナ
_SMALL_KANA_TO_LARGE = str.maketrans(
    {
        "ァ": "ア",
        "ィ": "イ",
        "ゥ": "ウ",
        "ェ": "エ",
        "ォ": "オ",
        "ャ": "ヤ",
        "ュ": "ユ",
        "ョ": "ヨ",
        "ッ": "ツ",
        "ヮ": "ワ",
        "ヵ": "カ",
        "ヶ": "ケ",
        "ゎ": "わ",
        "ぁ": "あ",
        "ぃ": "い",
        "ぅ": "う",
        "ぇ": "え",
        "ぉ": "お",
        "ゃ": "や",
        "ゅ": "ゆ",
        "ょ": "よ",
        "っ": "つ",
    }
)



# ------------------------------------------------------------
# 基本：数字系
# ------------------------------------------------------------

# --- 氏名・カナ match 共通正規化 ---
def _normalize_nfkc_strip(raw: Optional[str]) -> str:
    """NFKC + trim の基本正規化。"""
    return unicodedata.normalize("NFKC", raw or "").strip()


def _remove_all_spaces(value: str) -> str:
    """半角/全角空白をすべて除去する。"""
    return value.replace(" ", "").replace("　", "")


def _hiragana_to_katakana(value: str) -> str:
    """ひらがなをカタカナへ寄せる。"""
    return value.translate(_HIRAGANA_TO_KATAKANA)


def _normalize_small_kana(value: str) -> str:
    """小書きカナを通常カナへ寄せる。"""
    return value.translate(_SMALL_KANA_TO_LARGE)


def apply_kanji_normalization_dict(
    value: str,
    *,
    kanji_map: Optional[Mapping[str, str]] = None,
) -> str:
    """漢字正規化辞書を適用する。

    辞書が未指定ならそのまま返す。
    辞書は 1文字置換を前提とする。
    """
    if not value or not kanji_map:
        return value

    return "".join(kanji_map.get(ch, ch) for ch in value)


def normalize_name_kanji_match(
    raw: Optional[str],
    *,
    cur=None,
    kanji_map: Optional[Mapping[str, str]] = None,
) -> Optional[str]:
    """氏名（漢字）の match 用共通正規化。

    手順:
    - NFKC
    - trim
    - 半角/全角空白除去
    - 中黒・ハイフン系・括弧・区切り記号除去
    - 漢字正規化辞書適用

    方針:
    - 漢字 match の生成では辞書適用を前提とする
    - `kanji_map` が未指定の場合は `cur` から辞書をロードする
    - `cur` も `kanji_map` も無い場合は辞書未適用のまま返す
    """
    value = _normalize_nfkc_strip(raw)
    if not value:
        return None

    value = _remove_all_spaces(value)
    value = _NAME_MATCH_REMOVE_RE.sub("", value)

    if kanji_map is None and cur is not None:
        kanji_map = load_kanji_normalization_map(cur)

    value = apply_kanji_normalization_dict(value, kanji_map=kanji_map)

    return value or None


def normalize_name_kana_match(raw: Optional[str]) -> Optional[str]:
    """氏名（カナ）の match 用共通正規化。

    手順:
    - NFKC
    - trim
    - 半角/全角空白除去
    - ひらがな→カタカナ
    - 小書きカナ正規化
    - 中黒・ハイフン系・括弧・区切り記号除去
    """
    value = _normalize_nfkc_strip(raw)
    if not value:
        return None

    value = _remove_all_spaces(value)
    value = _hiragana_to_katakana(value)
    value = _normalize_small_kana(value)
    value = _KANA_MATCH_REMOVE_RE.sub("", value)

    return value or None

# v1.0 固定メモ:
# - to_half_digits / digits_only は "入力のゆれ吸収" の最下層。
# - ここでの digits_only は "数字以外を捨てる" だけで、桁数や必須判定は上位関数で行う。

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


def trim_leading_zeros(num_text: str) -> str:
    """数字文字列の先頭0を削除（全て0なら '0' を返す）"""
    trimmed = (num_text or "").lstrip("0")
    return trimmed if trimmed else "0"



def split_digit_chunks(s: str) -> list[str]:
    """任意区切り値を → [数字ブロック, ...]（例: '12-34' → ['12','34']）"""
    src = to_half_digits(s or "")
    return [p for p in re.split(r"[^\d]+", src) if p]



# ------------------------------------------------------------
# 保険証番号（必須）
# ------------------------------------------------------------

# v1.0: 保険証番号は必須（数字が1つも無い場合は行エラー）
# - 返り値は digits-only（桁数固定はしない。桁の妥当性は下流の突合仕様で扱う）

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


def normalize_insurance_number_match(raw: str) -> Optional[str]:
    """
    保険証番号の match 用共通正規化。

    ルール:
    - NFKC 正規化
    - 数字以外除去
    - 半角数字へ統一
    - 先頭0削除

    数字が1つも無い場合は None を返す。
    """
    s = unicodedata.normalize("NFKC", raw or "")
    d = "".join(ch for ch in s if ch.isdigit())
    if d == "":
        return None
    return trim_leading_zeros(d)



# ------------------------------------------------------------
# 枝番（任意）
# ------------------------------------------------------------

# v1.0: 枝番は任意（空なら None）

def normalize_branchnumber_optional(raw: str) -> Optional[str]:
    s = to_half_digits(raw or "")
    d = "".join(ch for ch in s if ch.isdigit())
    return d or None



# ------------------------------------------------------------
# 生年月日（YYYYMMDD）
# ------------------------------------------------------------

# v1.0: 生年月日は "YYYYMMDD" に寄せる（区切り記号は許容）
# - 厳密な暦チェックはしない（m=1..12, d=1..31 程度）

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

# v1.0: YYYYMMDD → ISO 変換（空は None）
# - 形式が崩れている場合は NormalizeError

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

# v1.0: 汎用日付の正規化 → ISO（YYYY-MM-DD）
# - 空は None
# - 形式の推定に失敗したら NormalizeError

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

# v1.0: 性別コードは 1=男, 2=女, 9=不明/その他 に寄せる（入力ゆれは吸収）

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

# v1.0: 記号の正規化（表記ゆれを減らす）
# - 文字列としての "記号" を保持しつつ、含まれる数字を連結した int? も返す
# - ここでは digits-only 運用への強制はしない（必要なら呼び出し側で digits_only を適用）

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



def _normalize_symbol_match_delimiters(value: str) -> str:
    """記号 match 用に空白・ダッシュ類を除去する。"""
    return re.sub(r"[ 　\-‐‑‒–—―ー－ｰ]+", "", value)



def _trim_leading_zeros_in_digit_chunks(value: str) -> str:
    """文字列中の数字連続部分ごとに先頭0を削除する。"""

    def repl(m: re.Match[str]) -> str:
        return trim_leading_zeros(m.group(0))

    return re.sub(r"\d+", repl, value)



def normalize_insurance_symbol_match(raw: str) -> Optional[str]:
    """
    保険証記号の match 用共通正規化。

    HIA export ZIP v1 で確定した正規化手順を共通ルールとして使用する。

    ルール:
    - NFKC 正規化
    - 空白 / ダッシュ類を除去
    - 数字連続部分ごとに先頭0削除
    - 英字・数字は半角のまま（全角化しない）
    - 非数字部分も保持したまま canonical value とする

    空になった場合は None を返す。
    """
    s = unicodedata.normalize("NFKC", raw or "").strip()
    if not s:
        return None

    s = _normalize_symbol_match_delimiters(s)
    if not s:
        return None

    s = _trim_leading_zeros_in_digit_chunks(s)

    return s or None



# ------------------------------------------------------------
# フォルダ名 → 保険者番号（8桁 int）
# ------------------------------------------------------------

# v1.0: 入力フォルダ名（8桁）→ 保険者番号 int
# - 8桁でない場合は NormalizeError

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
