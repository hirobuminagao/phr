# -*- coding: utf-8 -*-
"""
section_90060_final.py

CDA Section 90060（最終評価 / アウトカム）抽出

責務:
- 90060 セクションから達成状況を抽出
- 合計ポイント（アウトカム）を算出
- 腹囲・体重改善の表示文言を返す
- 最終腹囲 / 最終体重を抽出する
- （互換用）初回面談方式（最終XML側）も取得できるようにする

主関数:
- extract_final_outcomes(root) -> tuple[dict[str, bool], int, str]
- extract_final_measurements(root) -> tuple[Optional[float], Optional[float]]

互換関数:
- extract_final_outcome(root) -> dict
"""

from typing import Optional, Dict
import xml.etree.ElementTree as ET

from scripts.lib.shg.xml.common import (
    find_observation_in_section,
    find_section_by_code,
    get_int_value,
    get_observation_value_code,
    get_observation_value_raw,
    get_pq_float_or_none,
    get_value_code,
    get_value_display_name,
)


# ------------------------------------------------------------
# 90060 抽出
# ------------------------------------------------------------
def extract_final_outcome(root: ET.Element) -> Dict[str, Optional[object]]:
    """
    90060（最終評価）からアウトカム抽出
    """

    section = find_section_by_code(root, "90060")
    if section is None:
        return {
            "outcome_points": 0,
            "init_mode_code": None,
            "init_mode_text": None,
        }

    # --------------------------------------------------------
    # アウトカム合計（1042001060）
    # --------------------------------------------------------
    total_points = 0
    obs_total = find_observation_in_section(section, "1042001060")
    if obs_total is not None:
        v = None if obs_total is None else int(get_observation_value_raw(root, "90060", "1042001060") or 0)
        if v is not None:
            total_points = v

    # --------------------------------------------------------
    # 初回面談方式（最終XML側）
    # --------------------------------------------------------
    # ※ 24010: 主対応内容
    init_mode_code = None
    init_mode_text = None

    obs_mode = find_observation_in_section(section, "1.2.392.200119.6.24010")
    if obs_mode is not None:
        init_mode_code = get_value_code(obs_mode)
        init_mode_text = get_value_display_name(obs_mode)

    return {
        "outcome_points": total_points,
        "init_mode_code": init_mode_code,
        "init_mode_text": init_mode_text,
    }


def extract_final_outcomes(root: ET.Element) -> tuple[Dict[str, bool], int, str]:
    """90060 最終評価セクションから達成状況・アウトカム合計・腹囲体重改善文言を抽出する。"""
    belly_code = ""
    section = find_section_by_code(root, "90060")
    if section is not None:
        obs = find_observation_in_section(section, "1042001044")
        if obs is not None:
            belly_code = get_value_code(obs) or ""

    belly_ok = belly_code in {"1", "2"}
    belly_text_map = {"1": "1cm/1kg", "2": "2cm/2kg"}
    belly_text = belly_text_map.get(belly_code, "未達成")

    def ok1(code: str) -> bool:
        sec = find_section_by_code(root, "90060")
        if sec is None:
            return False
        obs = find_observation_in_section(sec, code)
        if obs is None:
            return False
        return (get_value_code(obs) or "") == "1"

    outs: Dict[str, bool] = {}
    outs["腹囲・体重の改善"] = belly_ok
    outs["生活習慣の改善(食習慣)"] = ok1("1042001042")
    outs["生活習慣の改善(運動習慣)"] = ok1("1042001041")
    outs["生活習慣の改善(喫煙習慣)"] = ok1("1042001043")
    outs["生活習慣の改善(休養習慣)"] = ok1("1042001045")
    outs["生活習慣の改善(その他の生活習慣)"] = ok1("1042001046")

    total_pts_raw = get_observation_value_raw(root, "90060", "1042001060")
    try:
        total_pts = int(total_pts_raw or 0)
    except Exception:
        total_pts = 0

    return outs, total_pts, belly_text


def extract_final_measurements(root: ET.Element) -> tuple[Optional[float], Optional[float]]:
    """90060 最終評価セクションから最終腹囲・最終体重を抽出する。"""
    waist_cm = get_pq_float_or_none(root, "90060", "1042001031")
    weight_kg = get_pq_float_or_none(root, "90060", "1042001032")
    return waist_cm, weight_kg