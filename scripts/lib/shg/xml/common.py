

from __future__ import annotations

import xml.etree.ElementTree as ET


NS = {
    "cda": "urn:hl7-org:v3",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
}


def text_or(elem: ET.Element | None, default: str = "") -> str:
    if elem is None:
        return default
    return (elem.text or "").strip()


def find_section_by_code(root: ET.Element, section_code: str) -> ET.Element | None:
    for sec in root.findall(".//cda:section", NS):
        code_el = sec.find("cda:code", NS)
        if code_el is not None and (code_el.get("code") or "").strip() == section_code:
            return sec
    return None


def find_observation_in_section(
    section: ET.Element | None,
    observation_code: str,
) -> ET.Element | None:
    if section is None:
        return None
    for obs in section.findall(".//cda:observation", NS):
        code_el = obs.find("cda:code", NS)
        if code_el is not None and (code_el.get("code") or "").strip() == observation_code:
            return obs
    return None


def get_value_element(obs: ET.Element | None) -> ET.Element | None:
    if obs is None:
        return None
    return obs.find("cda:value", NS)


def get_value_code(obs: ET.Element | None) -> str:
    value_el = get_value_element(obs)
    if value_el is None:
        return ""
    return (value_el.get("code") or "").strip()


def get_value_display_name(obs: ET.Element | None) -> str:
    value_el = get_value_element(obs)
    if value_el is None:
        return ""
    return (value_el.get("displayName") or "").strip()


def get_value_raw(obs: ET.Element | None) -> str:
    value_el = get_value_element(obs)
    if value_el is None:
        return ""
    return (value_el.get("value") or "").strip()


def get_observation_value_code(
    root: ET.Element,
    section_code: str,
    observation_code: str,
) -> str:
    section = find_section_by_code(root, section_code)
    obs = find_observation_in_section(section, observation_code)
    return get_value_code(obs)


def get_observation_value_raw(
    root: ET.Element,
    section_code: str,
    observation_code: str,
) -> str:
    section = find_section_by_code(root, section_code)
    obs = find_observation_in_section(section, observation_code)
    return get_value_raw(obs)


def get_int_value(root: ET.Element, section_code: str, observation_code: str) -> int:
    raw = get_observation_value_raw(root, section_code, observation_code)
    if raw == "":
        return 0
    try:
        return int(raw)
    except Exception:
        try:
            return int(float(raw))
        except Exception:
            return 0


def get_pq_float_or_none(
    root: ET.Element,
    section_code: str,
    observation_code: str,
) -> float | None:
    raw = get_observation_value_raw(root, section_code, observation_code)
    if raw == "":
        return None
    try:
        return float(raw)
    except Exception:
        return None