from __future__ import annotations

import xml.etree.ElementTree as ET

from scripts.lib.shg.xml.common import get_observation_value_code


def _robust_bool_from_value_code(code: str) -> bool:
    return (code or "").strip() in {"1", "true", "True"}


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