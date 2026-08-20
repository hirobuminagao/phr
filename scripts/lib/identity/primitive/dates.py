

from __future__ import annotations

from datetime import date, timedelta


def detect_date_format(text: str | None) -> str | None:
    """入力文字列のフォーマットを判定する。

    戻り値:
        "yyyymmdd"      : 西暦8桁（例: 19900101）
        "era_code_7"    : 元号コード7桁（例: 5yymmdd）
        None            : 判定不可
    """
    if text is None:
        return None

    if text.isdigit():
        if len(text) == 8:
            return "yyyymmdd"
        if len(text) == 7:
            return "era_code_7"

    return None


def parse_yyyymmdd(text: str | None) -> tuple[int, int, int] | None:
    """YYYYMMDD を (year, month, day) に分解する。"""
    if text is None or len(text) != 8 or not text.isdigit():
        return None

    y = int(text[0:4])
    m = int(text[4:6])
    d = int(text[6:8])

    try:
        date(y, m, d)
    except ValueError:
        return None

    return y, m, d


def parse_excel_serial_date(text: str | None) -> tuple[int, int, int] | None:
    """Excel シリアル日付を (year, month, day) に変換する。

    Excel の 1900 年うるう年バグを実務上の換算に合わせるため、
    基準日は 1899-12-30 とする。
    """
    if text is None or not text.isdigit():
        return None

    serial = int(text)
    if serial <= 0:
        return None

    parsed = date(1899, 12, 30) + timedelta(days=serial)
    return parsed.year, parsed.month, parsed.day


# 元号コード: 明治=1, 大正=2, 昭和=3, 平成=4, 令和=5
_ERA_BASE_YEAR = {
    1: 1867,  # 明治 (1868-)
    2: 1911,  # 大正 (1912-)
    3: 1925,  # 昭和 (1926-)
    4: 1988,  # 平成 (1989-)
    5: 2018,  # 令和 (2019-)
}


def parse_era_code_7(text: str | None) -> tuple[int, int, int] | None:
    """元号コード7桁 (gyymmdd) を西暦 (year, month, day) に変換する。"""
    if text is None or len(text) != 7 or not text.isdigit():
        return None

    era = int(text[0])
    yy = int(text[1:3])
    mm = int(text[3:5])
    dd = int(text[5:7])

    base = _ERA_BASE_YEAR.get(era)
    if base is None:
        return None

    year = base + yy

    try:
        date(year, mm, dd)
    except ValueError:
        return None

    return year, mm, dd


def to_yyyymmdd(year: int, month: int, day: int) -> str:
    """(year, month, day) → YYYYMMDD"""
    return f"{year:04d}{month:02d}{day:02d}"


def to_yyyy_mm_dd(year: int, month: int, day: int) -> str:
    """(year, month, day) → YYYY-MM-DD"""
    return f"{year:04d}-{month:02d}-{day:02d}"
