

from __future__ import annotations

from typing import Optional
import xml.etree.ElementTree as ET

from scripts.lib.shg.xml.common import NS


# ----------------------------------------
# 90010 保健指導情報（guidance）
# ----------------------------------------

GUIDANCE_CODE = "1020000001"

GUIDANCE_TYPE_MAP = {
    "1": "積極的支援",
    "2": "動機付け支援",
    "3": "動機付け支援相当",
}


def _get_guidance_type(root: ET.Element) -> Optional[str]:
    """
    保健指導区分を取得する。

    仕様:
    - section code=90010 配下を探索
    - observation/code[@code='1020000001'] を対象
    - value/@code を取得
    """

    sections = root.findall(".//cda:section", NS)

    for sec in sections:
        sec_code = sec.find("cda:code", NS)
        if sec_code is None:
            continue

        if (sec_code.get("code") or "").strip() != "90010":
            continue

        # 90010内のobservation探索
        for obs in sec.findall(".//cda:observation", NS):
            code_el = obs.find("cda:code", NS)
            value_el = obs.find("cda:value", NS)

            if code_el is None or value_el is None:
                continue

            if (code_el.get("code") or "").strip() == GUIDANCE_CODE:
                val = (value_el.get("code") or "").strip()
                return val if val else None

    return None


def extract_90010_guidance(root: ET.Element) -> dict[str, Optional[str]]:
    """
    90010 保健指導情報を抽出する

    Returns:
        {
            "guidance_type_code": str | None,
            "guidance_type_name": str | None,
        }
    """

    code = _get_guidance_type(root)

    return {
        "guidance_type_code": code,
        "guidance_type_name": GUIDANCE_TYPE_MAP.get(code) if code else None,
    }