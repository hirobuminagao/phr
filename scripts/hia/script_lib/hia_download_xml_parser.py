from __future__ import annotations

from pathlib import Path
import xml.etree.ElementTree as ET

NS = {"hl7": "urn:hl7-org:v3"}


def _find_attr(root: ET.Element, xpath: str, attr_name: str) -> str | None:
    elem = root.find(xpath, NS)
    if elem is None:
        return None
    value = elem.get(attr_name)
    if value is None:
        return None
    value = value.strip()
    return value or None


def _find_text(root: ET.Element, xpath: str) -> str | None:
    elem = root.find(xpath, NS)
    if elem is None or elem.text is None:
        return None
    value = elem.text.strip()
    return value or None


def _find_id_extension_by_root(root: ET.Element, xpath_base: str, root_oid: str) -> str | None:
    return _find_attr(root, f"{xpath_base}/hl7:id[@root='{root_oid}']", "extension")


def _build_name(root: ET.Element) -> str | None:
    name_elem = root.find(".//hl7:patientRole/hl7:patient/hl7:name", NS)
    if name_elem is None:
        return None

    parts: list[str] = []
    for child in list(name_elem):
        if child.text:
            value = child.text.strip()
            if value:
                parts.append(value)

    if not parts and name_elem.text:
        value = name_elem.text.strip()
        if value:
            parts.append(value)

    return "".join(parts) if parts else None


def _parse_hl7_date(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip()
    if len(value) >= 8 and value[:8].isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
    return None


def parse_hia_download_xml(xml_path: str | Path) -> dict[str, str | None]:
    xml_path = Path(xml_path)
    tree = ET.parse(xml_path)
    root = tree.getroot()

    exam_date_raw = (
        _find_attr(root, ".//hl7:documentationOf//hl7:effectiveTime/hl7:low", "value")
        or _find_attr(root, ".//hl7:documentationOf//hl7:serviceEvent/hl7:effectiveTime/hl7:low", "value")
        or _find_attr(root, ".//hl7:documentationOf//hl7:serviceEvent/hl7:effectiveTime", "value")
    )
    exam_date = _parse_hl7_date(exam_date_raw)

    patient_base = ".//hl7:recordTarget/hl7:patientRole/hl7:patient"
    name = _build_name(root)
    birthdate = _parse_hl7_date(_find_attr(root, f"{patient_base}/hl7:birthTime", "value"))
    gender_code = _find_attr(root, f"{patient_base}/hl7:administrativeGenderCode", "code")

    patient_role_base = ".//hl7:recordTarget/hl7:patientRole"
    insurer_number = _find_id_extension_by_root(root, patient_role_base, "1.2.392.200119.6.101")
    insurance_symbol = _find_id_extension_by_root(root, patient_role_base, "1.2.392.200119.6.204")
    insurance_number = _find_id_extension_by_root(root, patient_role_base, "1.2.392.200119.6.205")

    facility_code = (
        _find_id_extension_by_root(
            root,
            ".//hl7:author/hl7:assignedAuthor/hl7:representedOrganization",
            "1.2.392.200119.6.102",
        )
        or _find_attr(root, ".//hl7:custodian//hl7:representedCustodianOrganization/hl7:id", "extension")
    )
    facility_name = (
        _find_text(root, ".//hl7:author/hl7:assignedAuthor/hl7:representedOrganization/hl7:name")
        or _find_text(root, ".//hl7:custodian//hl7:representedCustodianOrganization/hl7:name")
    )

    report_category_code = _find_attr(
        root,
        "hl7:code[@codeSystem='1.2.392.200119.6.1001']",
        "code",
    )
    program_type_code = _find_attr(
        root,
        ".//hl7:documentationOf/hl7:serviceEvent/hl7:code[@codeSystem='1.2.392.200119.6.1002']",
        "code",
    )

    return {
        "xml_path": str(xml_path),
        "exam_date": exam_date,
        "name_kana_raw": name,
        "birthdate": birthdate,
        "gender_code": gender_code,
        "insurer_number": insurer_number,
        "insurance_symbol_raw": insurance_symbol,
        "insurance_number_raw": insurance_number,
        "facility_code": facility_code,
        "facility_name": facility_name,
        "report_category_code": report_category_code,
        "program_type_code": program_type_code,
    }
