from __future__ import annotations

from typing import Optional
import xml.etree.ElementTree as ET

from scripts.lib.shg.xml.common import NS, get_observation_value_code


def _robust_bool_from_value_code(code: str) -> bool:
    """計画系コードを bool 的に扱う。

    90030 では腹囲体重が 0/1/2 を取りうるため、
    `1` だけでなく `2` も「計画あり」とみなす。
    """
    return (code or "").strip() in {"1", "2", "true", "True"}


def extract_initial_date(root: ET.Element) -> Optional[str]:
    """90030 初回面談情報セクションから初回面接実施日を取得する。"""
    for sec in root.findall(".//cda:section", NS):
        sec_code = sec.find("cda:code", NS)
        if sec_code is None:
            continue
        if (sec_code.get("code") or "").strip() != "90030":
            continue

        for act in sec.findall(".//cda:entry/cda:act", NS):
            code_el = act.find("cda:code", NS)
            if code_el is None:
                continue
            if (code_el.get("codeSystem") or "").strip() != "1.2.392.200119.6.24010":
                continue

            eff_el = act.find("cda:effectiveTime", NS)
            if eff_el is None:
                return None

            val = (eff_el.get("value") or "").strip()
            return val if val else None

    return None


def extract_initial_goal_levels(root: ET.Element) -> dict[str, Optional[int]]:
    """90030 初回面談情報セクションから計画値の raw level を抽出する。

    返り値は XML に記載された code 値をそのまま int 化したもの。
    例:
    - 腹囲・体重の改善: 0 / 1 / 2
    - 食 / 運動 / 喫煙 / 休養 / その他: 0 / 1
    """

    def level(code: str) -> Optional[int]:
        raw = get_observation_value_code(root, "90030", code)
        raw = (raw or "").strip()
        if not raw:
            return None
        try:
            return int(raw)
        except Exception:
            return None

    levels: dict[str, Optional[int]] = {}
    levels["腹囲・体重の改善"] = level("1021001053")
    levels["生活習慣の改善(食習慣)"] = level("1021001054")
    levels["生活習慣の改善(運動習慣)"] = level("1021001055")
    levels["生活習慣の改善(喫煙習慣)"] = level("1021001056")
    levels["生活習慣の改善(休養習慣)"] = level("1021001057")
    levels["生活習慣の改善(その他)"] = level("1021001058")
    return levels


def extract_initial_goals(root: ET.Element) -> dict[str, bool]:
    """90030 初回面談情報セクションから目標項目を抽出する。"""

    def flag(code: str) -> bool:
        return _robust_bool_from_value_code(
            get_observation_value_code(root, "90030", code)
        )

    goals: dict[str, bool] = {}
    goals["腹囲・体重の改善"] = flag("1021001053")
    goals["生活習慣の改善(食習慣)"] = flag("1021001054")
    goals["生活習慣の改善(運動習慣)"] = flag("1021001055")
    goals["生活習慣の改善(喫煙習慣)"] = flag("1021001056")
    goals["生活習慣の改善(休養習慣)"] = flag("1021001057")
    goals["生活習慣の改善(その他)"] = flag("1021001058")
    return goals