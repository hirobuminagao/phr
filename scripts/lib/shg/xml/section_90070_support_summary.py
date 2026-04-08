from __future__ import annotations

from typing import Any
import xml.etree.ElementTree as ET

from scripts.lib.shg.xml.common import get_int_value


def extract_support_summary(root: ET.Element) -> dict[str, Any]:
    """90070 支援実施内容集計セクションを抽出する。

    戻り値:
    {
        "counts": {...},
        "durations_min": {...},
        "_total_points": int,
        "_grand_total": int,
    }
    """
    section_code = "90070"

    counts = {
        "個別支援(対面)": get_int_value(root, section_code, "1042010010"),
        "個別支援(遠隔)": get_int_value(root, section_code, "1042010020"),
        "グループ支援(対面)": get_int_value(root, section_code, "1042010030"),
        "グループ支援(遠隔)": get_int_value(root, section_code, "1042010040"),
        "電話": get_int_value(root, section_code, "1042010050"),
        "電子メール等": get_int_value(root, section_code, "1042010060"),
    }

    durations_min = {
        "個別支援(対面)": get_int_value(root, section_code, "1042020010"),
        "個別支援(遠隔)": get_int_value(root, section_code, "1042020020"),
        "グループ支援(対面)": get_int_value(root, section_code, "1042020030"),
        "グループ支援(遠隔)": get_int_value(root, section_code, "1042020040"),
        "電話": get_int_value(root, section_code, "1042020050"),
    }

    total_points = get_int_value(root, section_code, "1042010070")
    grand_total = get_int_value(root, section_code, "1042010080")

    return {
        "counts": counts,
        "durations_min": durations_min,
        "_total_points": total_points,
        "_grand_total": grand_total,
    }