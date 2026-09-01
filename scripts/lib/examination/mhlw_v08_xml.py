from __future__ import annotations

import hashlib
import shutil
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Iterable

from lxml import etree


NS_HL7 = "urn:hl7-org:v3"
NS_XSI = "http://www.w3.org/2001/XMLSchema-instance"
NS_IX08 = "https://www.mhlw.go.jp/stf/seisakunitsuite/bunya/0000161103.html"

OID_REPORT_CATEGORY = "1.2.392.200119.6.1001"
OID_PROGRAM_TYPE = "1.2.392.200119.6.1002"
OID_ITEM_CODE = "1.2.392.200119.6.1005"
OID_SECTION_CODE = "1.2.392.200119.6.1010"
OID_INSURER = "1.2.392.200119.6.101"
OID_FACILITY = "1.2.392.200119.6.102"
OID_SYMBOL = "1.2.392.200119.6.204"
OID_NUMBER = "1.2.392.200119.6.205"
OID_GENDER = "1.2.392.200119.6.1104"
OID_TICKET_KIND = "1.2.392.200119.6.208"

ET.register_namespace("", NS_HL7)
ET.register_namespace("xsi", NS_XSI)


@dataclass(frozen=True)
class Facility:
    code: str
    name: str
    postal_code: str | None = None
    address: str | None = None
    phone: str | None = None


@dataclass(frozen=True)
class Person:
    insurer_number: str
    insurance_symbol: str
    insurance_number: str
    name_kana: str
    gender_code: str
    birthdate: str
    exam_date: str
    report_category_code: str
    program_type_code: str
    postal_code: str | None = None
    address: str | None = None
    exam_ticket_number: str | None = None
    exam_ticket_number_root_oid: str | None = None
    exam_ticket_kind_code: str | None = None
    exam_ticket_kind_code_system: str | None = None
    exam_ticket_expires_on: str | None = None


@dataclass(frozen=True)
class ExamItem:
    namecode: str
    section_code: str
    value_type: str
    normalized_value: str | None
    normalized_unit: str | None = None
    nullflavor: str | None = None
    code_system: str | None = None
    code_value: str | None = None
    code_display: str | None = None
    display_name: str | None = None
    method_code: str | None = None
    interpretation_code: str | None = None
    interpretation_code_system: str | None = None
    interpretation_name: str | None = None
    source_reference_lower: str | None = None
    source_reference_upper: str | None = None
    series_group_identifier: str | None = None
    series_group_relation_code: str | None = None
    author_item_code: str | None = None
    is_author_item: bool = False
    negation_ind: bool | None = None
    occurrence_no: int = 1
    jun_no: int | None = None


class XmlValidationError(ValueError):
    pass


def _h(tag: str) -> str:
    return f"{{{NS_HL7}}}{tag}"


def _ix(tag: str) -> str:
    return f"{{{NS_IX08}}}{tag}"


def _add_address(parent: ET.Element, postal_code: str | None, address: str | None) -> None:
    if not postal_code and not address:
        return
    node = ET.SubElement(parent, _h("addr"))
    if postal_code:
        postal = ET.SubElement(node, _h("postalCode"))
        postal.text = postal_code
        if address:
            postal.tail = address
    elif address:
        node.text = address


def _add_organization(parent: ET.Element, facility: Facility) -> None:
    organization = ET.SubElement(parent, _h("representedOrganization"))
    ET.SubElement(organization, _h("id"), {"extension": facility.code, "root": OID_FACILITY})
    ET.SubElement(organization, _h("name")).text = facility.name
    if facility.phone:
        ET.SubElement(organization, _h("telecom"), {"value": facility.phone})
    _add_address(organization, facility.postal_code, facility.address)


def _add_exam_ticket_participant(parent: ET.Element, person: Person) -> None:
    if not person.exam_ticket_number:
        return
    if not person.exam_ticket_number_root_oid:
        raise ValueError("exam_ticket_number_root_oid is missing")

    participant = ET.SubElement(parent, _h("participant"), {"typeCode": "HLD"})
    ET.SubElement(
        participant,
        _h("functionCode"),
        {
            "code": person.exam_ticket_kind_code or "1",
            "codeSystem": person.exam_ticket_kind_code_system or OID_TICKET_KIND,
        },
    )
    time_node = ET.SubElement(participant, _h("time"))
    if person.exam_ticket_expires_on:
        ET.SubElement(time_node, _h("high"), {"value": person.exam_ticket_expires_on})
    associated = ET.SubElement(participant, _h("associatedEntity"), {"classCode": "IDENT"})
    ET.SubElement(
        associated,
        _h("id"),
        {
            "extension": person.exam_ticket_number,
            "root": person.exam_ticket_number_root_oid,
        },
    )
    scoping = ET.SubElement(associated, _h("scopingOrganization"))
    ET.SubElement(scoping, _h("id"), {"extension": person.insurer_number, "root": OID_INSURER})


def _add_observation_author(observation: ET.Element, author_item: ExamItem) -> None:
    author_name = (author_item.normalized_value or "").strip()
    if not author_name:
        return
    author = ET.SubElement(observation, _h("author"))
    ET.SubElement(author, _h("time"), {"nullFlavor": "NI"})
    assigned_author = ET.SubElement(author, _h("assignedAuthor"))
    ET.SubElement(assigned_author, _h("id"), {"nullFlavor": "NI"})
    assigned_person = ET.SubElement(assigned_author, _h("assignedPerson"))
    ET.SubElement(assigned_person, _h("name")).text = author_name


def _add_exam_item_observation(
    parent: ET.Element,
    item: ExamItem,
    author_item: ExamItem | None = None,
) -> ET.Element:
    if not item.display_name:
        raise ValueError(f"{item.namecode}: displayName is missing")

    observation_attrs = {"classCode": "OBS", "moodCode": "EVN"}
    if item.negation_ind is not None:
        observation_attrs["negationInd"] = "true" if item.negation_ind else "false"
    observation = ET.SubElement(parent, _h("observation"), observation_attrs)

    code_attrs = {"code": item.namecode, "codeSystem": OID_ITEM_CODE}
    if item.display_name:
        code_attrs["displayName"] = item.display_name
    ET.SubElement(observation, _h("code"), code_attrs)

    value_type = (item.value_type or "ST").upper()
    if value_type in {"CD", "CO"}:
        has_value = bool(item.nullflavor or item.code_value is not None)
    else:
        has_value = bool(item.nullflavor or item.normalized_value is not None)
    if has_value or not item.negation_ind:
        value_attrs = {f"{{{NS_XSI}}}type": value_type}
        value = ET.SubElement(observation, _h("value"), value_attrs)
        if item.nullflavor:
            value.set("nullFlavor", item.nullflavor)
        elif value_type == "PQ":
            if item.normalized_value is None:
                raise ValueError(f"{item.namecode}: PQ value is missing")
            value.set("value", item.normalized_value)
            if item.normalized_unit:
                value.set("unit", item.normalized_unit)
        elif value_type in {"CD", "CO"}:
            if not item.code_value or not item.code_system:
                raise ValueError(f"{item.namecode}: coded value is incomplete")
            value.set("code", item.code_value)
            value.set("codeSystem", item.code_system)
            if item.code_display:
                value.set("displayName", item.code_display)
        else:
            if item.normalized_value is None:
                raise ValueError(f"{item.namecode}: ST value is missing")
            value.text = item.normalized_value

    if item.interpretation_code:
        attrs = {"code": item.interpretation_code}
        if item.interpretation_code_system:
            attrs["codeSystem"] = item.interpretation_code_system
        if item.interpretation_name:
            attrs["displayName"] = item.interpretation_name
        ET.SubElement(observation, _h("interpretationCode"), attrs)
    if item.method_code:
        ET.SubElement(observation, _h("methodCode"), {"code": item.method_code})
    if author_item is not None:
        _add_observation_author(observation, author_item)
    if item.source_reference_lower is not None or item.source_reference_upper is not None:
        if value_type != "PQ":
            raise ValueError(f"{item.namecode}: reference range is only supported for PQ")
        reference_range = ET.SubElement(observation, _h("referenceRange"))
        observation_range = ET.SubElement(reference_range, _h("observationRange"), {"classCode": "OBS", "moodCode": "EVN.CRT"})
        interval = ET.SubElement(observation_range, _h("value"), {f"{{{NS_XSI}}}type": "IVL_PQ"})
        unit_attrs = {"unit": item.normalized_unit} if item.normalized_unit else {}
        if item.source_reference_lower is not None:
            ET.SubElement(interval, _h("low"), {"value": item.source_reference_lower, **unit_attrs})
        if item.source_reference_upper is not None:
            ET.SubElement(interval, _h("high"), {"value": item.source_reference_upper, **unit_attrs})
    return observation


def build_clinical_document(person: Person, facility: Facility, items: Iterable[ExamItem], file_date: str) -> ET.ElementTree:
    ET.register_namespace("", NS_HL7)
    ET.register_namespace("xsi", NS_XSI)
    root = ET.Element(
        _h("ClinicalDocument"),
        {
            f"{{{NS_XSI}}}schemaLocation": f"{NS_HL7} ../XSD/hc08_V08.xsd",
        },
    )
    ET.SubElement(root, _h("typeId"), {"extension": "POCD_HD000040", "root": "2.16.840.1.113883.1.3"})
    ET.SubElement(root, _h("id"), {"nullFlavor": "NI"})
    ET.SubElement(root, _h("code"), {"code": person.report_category_code, "codeSystem": OID_REPORT_CATEGORY})
    ET.SubElement(root, _h("effectiveTime"), {"value": file_date})
    ET.SubElement(root, _h("confidentialityCode"), {"code": "N", "codeSystem": "2.16.840.1.113883.5.25"})

    patient_role = ET.SubElement(ET.SubElement(root, _h("recordTarget")), _h("patientRole"))
    ET.SubElement(patient_role, _h("id"), {"extension": person.insurer_number, "root": OID_INSURER})
    ET.SubElement(patient_role, _h("id"), {"extension": person.insurance_symbol, "root": OID_SYMBOL})
    ET.SubElement(patient_role, _h("id"), {"extension": person.insurance_number, "root": OID_NUMBER})
    _add_address(patient_role, person.postal_code, person.address)
    patient = ET.SubElement(patient_role, _h("patient"))
    ET.SubElement(patient, _h("name")).text = person.name_kana
    ET.SubElement(patient, _h("administrativeGenderCode"), {"code": person.gender_code, "codeSystem": OID_GENDER})
    ET.SubElement(patient, _h("birthTime"), {"value": person.birthdate})

    author = ET.SubElement(root, _h("author"))
    ET.SubElement(author, _h("time"), {"value": file_date})
    assigned_author = ET.SubElement(author, _h("assignedAuthor"))
    ET.SubElement(assigned_author, _h("id"), {"nullFlavor": "NI"})
    _add_organization(assigned_author, facility)

    custodian_org = ET.SubElement(ET.SubElement(ET.SubElement(root, _h("custodian")), _h("assignedCustodian")), _h("representedCustodianOrganization"))
    ET.SubElement(custodian_org, _h("id"), {"nullFlavor": "NI"})

    _add_exam_ticket_participant(root, person)

    service_event = ET.SubElement(ET.SubElement(root, _h("documentationOf")), _h("serviceEvent"))
    ET.SubElement(service_event, _h("code"), {"code": person.program_type_code, "codeSystem": OID_PROGRAM_TYPE})
    ET.SubElement(service_event, _h("effectiveTime"), {"value": person.exam_date})
    assigned_entity = ET.SubElement(ET.SubElement(service_event, _h("performer"), {"typeCode": "PRF"}), _h("assignedEntity"))
    ET.SubElement(assigned_entity, _h("id"), {"nullFlavor": "NI"})
    _add_organization(assigned_entity, facility)

    structured = ET.SubElement(ET.SubElement(root, _h("component")), _h("structuredBody"))
    all_items = list(items)
    author_item_codes = {item.author_item_code for item in all_items if item.author_item_code}
    author_item_codes.update(item.namecode for item in all_items if item.is_author_item)
    author_items: dict[str, list[ExamItem]] = {}
    for item in all_items:
        if item.namecode in author_item_codes:
            author_items.setdefault(item.namecode, []).append(item)

    def matching_author(item: ExamItem) -> ExamItem | None:
        if not item.author_item_code:
            return None
        candidates = author_items.get(item.author_item_code, [])
        return next(
            (candidate for candidate in candidates if candidate.occurrence_no == item.occurrence_no),
            candidates[0] if candidates else None,
        )

    grouped: dict[str, list[ExamItem]] = {}
    for item in all_items:
        if item.namecode in author_item_codes:
            continue
        grouped.setdefault(item.section_code or "01990", []).append(item)

    for section_code in sorted(grouped):
        section = ET.SubElement(ET.SubElement(structured, _h("component")), _h("section"))
        section_name = "検査・問診結果セクション" if section_code == "01010" else "任意追加項目セクション" if section_code == "01990" else section_code
        ET.SubElement(section, _h("code"), {"code": section_code, "codeSystem": OID_SECTION_CODE, "displayName": section_name})
        ET.SubElement(section, _h("title")).text = section_name
        ET.SubElement(section, _h("text"))
        section_items = sorted(grouped[section_code], key=lambda i: (i.jun_no is None, i.jun_no or 0, i.namecode, i.occurrence_no))
        emitted_groups: set[str] = set()
        for item in section_items:
            group_identifier = item.series_group_identifier
            if not group_identifier:
                _add_exam_item_observation(
                    ET.SubElement(section, _h("entry")),
                    item,
                    matching_author(item),
                )
                continue
            if group_identifier in emitted_groups:
                continue
            emitted_groups.add(group_identifier)
            parent = ET.SubElement(
                ET.SubElement(section, _h("entry")),
                _h("observation"),
                {"classCode": "OBS", "moodCode": "EVN"},
            )
            ET.SubElement(parent, _h("code"), {"nullFlavor": "NA"})
            grouped_items = [
                grouped_item
                for grouped_item in section_items
                if grouped_item.series_group_identifier == group_identifier
            ]
            grouped_items.sort(
                key=lambda grouped_item: (
                    (grouped_item.series_group_relation_code or "").upper() != "COMP",
                    grouped_item.jun_no is None,
                    grouped_item.jun_no or 0,
                    grouped_item.namecode,
                    grouped_item.occurrence_no,
                )
            )
            for grouped_item in grouped_items:
                relation_code = (grouped_item.series_group_relation_code or "").upper()
                if relation_code not in {"COMP", "RSON"}:
                    raise ValueError(
                        f"{grouped_item.namecode}: invalid series group relation code "
                        f"{grouped_item.series_group_relation_code!r}"
                    )
                relationship = ET.SubElement(parent, _h("entryRelationship"), {"typeCode": relation_code})
                _add_exam_item_observation(
                    relationship,
                    grouped_item,
                    matching_author(grouped_item),
                )
    return ET.ElementTree(root)


def build_ix08(sender_code: str, receiver_code: str, file_date: str, total_count: int) -> ET.ElementTree:
    ET.register_namespace("", NS_IX08)
    ET.register_namespace("xsi", NS_XSI)
    root = ET.Element(_ix("index"), {f"{{{NS_XSI}}}schemaLocation": f"{NS_IX08} ./XSD/ix08_V08.xsd"})
    ET.SubElement(root, _ix("interactionType"), {"code": "6"})
    ET.SubElement(root, _ix("creationTime"), {"value": file_date})
    sender = ET.SubElement(root, _ix("sender"))
    ET.SubElement(sender, _ix("id"), {"root": OID_FACILITY, "extension": sender_code})
    receiver = ET.SubElement(root, _ix("receiver"))
    ET.SubElement(receiver, _ix("id"), {"root": OID_INSURER, "extension": receiver_code})
    ET.SubElement(root, _ix("serviceEventType"), {"code": "1"})
    ET.SubElement(root, _ix("totalRecordCount"), {"value": str(total_count)})
    return ET.ElementTree(root)


def xml_bytes(tree: ET.ElementTree) -> bytes:
    ET.indent(tree, space="  ")
    return ET.tostring(tree.getroot(), encoding="utf-8", xml_declaration=True)


@lru_cache(maxsize=8)
def _load_schema(xsd_path: str) -> etree.XMLSchema:
    return etree.XMLSchema(etree.parse(xsd_path))


def validate_xml(content: bytes, xsd_path: Path) -> None:
    schema = _load_schema(str(xsd_path.resolve()))
    document = etree.fromstring(content)
    if not schema.validate(document):
        details = "; ".join(str(error) for error in schema.error_log)
        raise XmlValidationError(details)


def copy_xsd_bundle(bundle_dir: Path, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    for source in bundle_dir.iterdir():
        if source.name == "bundle.yml":
            continue
        target = destination / source.name
        if source.is_dir():
            shutil.copytree(source, target, dirs_exist_ok=True)
        elif source.suffix.lower() == ".xsd":
            shutil.copy2(source, target)


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def root_dir_name(facility_code: str, insurer_number: str, file_date: str, split_no: int) -> str:
    return f"{facility_code}_{insurer_number}_{file_date}{split_no}_1"


def person_xml_file_name(facility_code: str, file_date: str, split_no: int, sequence: int) -> str:
    return f"h{facility_code}{file_date}{split_no}1{sequence:06d}.xml"
