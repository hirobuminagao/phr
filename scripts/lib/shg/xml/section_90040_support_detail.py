from __future__ import annotations

from typing import Any, Dict, List
import xml.etree.ElementTree as ET

from scripts.lib.shg.xml.common import (
    NS,
    find_section_by_code,
    find_observation_in_section,
    get_value_raw,
)


def _get_minutes_from_obs(obs: ET.Element | None) -> int:
    """
    1032300013（時間）から minutes を取得
    observation/effectiveTime/width/@value
    """
    if obs is None:
        return 0

    width = obs.find(".//cda:effectiveTime/cda:width", NS)
    if width is None:
        return 0

    raw = (width.get("value") or "").strip()
    if raw == "":
        return 0

    try:
        return int(raw)
    except Exception:
        try:
            return int(float(raw))
        except Exception:
            return 0


def _get_points_from_obs(obs: ET.Element | None) -> int:
    """
    1032300014（ポイント）から値を取得
    """
    raw = get_value_raw(obs)
    if raw == "":
        return 0

    try:
        return int(raw)
    except Exception:
        try:
            return int(float(raw))
        except Exception:
            return 0


def extract_process_events(root: ET.Element) -> Dict[str, Any]:
    """
    90040（支援明細）からイベント単位のログを抽出

    Returns:
        {
            "events": [
                {
                    "mode_code": str,
                    "date": str,
                    "minutes": int,
                    "points": int,
                },
                ...
            ],
            "_total_points": int,
            "_total_minutes": int,
        }
    """

    section = find_section_by_code(root, "90040")

    if section is None:
        return {
            "events": [],
            "_total_points": 0,
            "_total_minutes": 0,
        }

    events: List[Dict[str, Any]] = []

    total_points = 0
    total_minutes = 0

    # entry/act を走査
    for act in section.findall(".//cda:entry/cda:act", NS):
        # mode_code
        code_el = act.find("cda:code", NS)
        mode_code = (code_el.get("code") or "").strip() if code_el is not None else ""

        # date
        eff = act.find("cda:effectiveTime", NS)
        date_val = (eff.get("value") or "").strip() if eff is not None else ""

        # points（1032300014）
        obs_points = find_observation_in_section(act, "1032300014")
        points = _get_points_from_obs(obs_points)

        # minutes（1032300013）
        obs_minutes = find_observation_in_section(act, "1032300013")
        minutes = _get_minutes_from_obs(obs_minutes)

        total_points += points
        total_minutes += minutes

        events.append(
            {
                "mode_code": mode_code,
                "date": date_val,
                "minutes": minutes,
                "points": points,
            }
        )

    return {
        "events": events,
        "_total_points": total_points,
        "_total_minutes": total_minutes,
    }
