from __future__ import annotations

from pathlib import Path
from xml.etree import ElementTree

from scripts.lib.examination.mhlw_v08_xml import (
    ExamItem,
    Facility,
    Person,
    build_clinical_document,
    build_ix08,
    person_xml_file_name,
    root_dir_name,
    validate_xml,
    xml_bytes,
)


XSD_ROOT = Path(__file__).resolve().parents[3] / "scripts" / "from_medical" / "source" / "XSD" / "mhlw_v4_20230331_v08"


def test_generated_clinical_document_and_index_validate_against_v08() -> None:
    facility = Facility("0123456789", "テスト健診機関", "123-4567", "東京都千代田区", "tel:0312345678")
    person = Person("06139463", "ＡＢ－０１", "123", "ヤマダ　タロウ", "1", "19800102", "20260603", "10", "010")
    items = [
        ExamItem("9N511000000000049", "01010", "ST", "異常なし", display_name="医師の診断"),
        ExamItem("9A755000000000001", "01010", "PQ", "170.0", normalized_unit="cm", display_name="身長"),
        ExamItem(
            "9N701000000000011",
            "01010",
            "CD",
            None,
            code_value="2",
            code_system="1.2.392.200119.6.2201",
            display_name="既往歴",
        ),
    ]

    clinical = xml_bytes(build_clinical_document(person, facility, items, "20260730"))
    index = xml_bytes(build_ix08(facility.code, person.insurer_number, "20260730", 1))
    assert b'xsi:type="v3:' not in clinical
    assert b'xmlns:v3=' not in clinical
    assert b'xsi:type="ST"' in clinical
    assert b'xsi:type="PQ"' in clinical
    assert b'xsi:type="CD"' in clinical
    validate_xml(clinical, XSD_ROOT / "hc08_V08.xsd")
    validate_xml(index, XSD_ROOT / "ix08_V08.xsd")


def test_official_file_names() -> None:
    assert root_dir_name("0123456789", "06139463", "20260730", 0) == "0123456789_06139463_202607300_1"
    assert person_xml_file_name("0123456789", "20260730", 0, 1) == "h01234567892026073001000001.xml"


def test_coded_values_do_not_emit_normalized_value_as_value_body() -> None:
    facility = Facility("0123456789", "テスト健診機関")
    person = Person("06139463", "1", "2", "ヤマダ　タロウ", "1", "19800102", "20260603", "10", "010")
    items = [
        ExamItem(
            "9N206160700000011",
            "01990",
            "CD",
            "所見なし",
            code_system="1.2.392.200119.6.2102",
            code_value="2",
            code_display="所見なし",
            display_name="胸部Ｘ線検査(一般:直接撮影)(所見の有無)",
        ),
        ExamItem(
            "9N701000000000011",
            "01010",
            "CO",
            "特記事項なし",
            code_system="1.2.392.200119.6.2201",
            code_value="2",
            code_display="特記事項なし",
            display_name="既往歴",
        ),
    ]

    content = xml_bytes(build_clinical_document(person, facility, items, "20260730"))
    assert b'xsi:type="CD" code="2"' in content
    assert b'xsi:type="CO" code="2"' in content
    assert b'value="\xe6\x89\x80\xe8\xa6\x8b\xe3\x81\xaa\xe3\x81\x97"' not in content
    assert b">" + "所見なし".encode() + b"</value>" not in content
    validate_xml(content, XSD_ROOT / "hc08_V08.xsd")


def test_annex2_series_group_reference_range_and_interpretation() -> None:
    facility = Facility("0123456789", "テスト健診機関")
    person = Person("06139463", "1", "2", "ヤマダ　タロウ", "1", "19800102", "20260603", "10", "010")
    group_id = "2A020161001930149"
    items = [
        ExamItem(
            "2A040000001930102",
            "01010",
            "PQ",
            "34.6",
            normalized_unit="%",
            interpretation_code="L",
            source_reference_lower="35.5",
            source_reference_upper="48.9",
            series_group_identifier=group_id,
            series_group_relation_code="COMP",
            display_name="ヘマトクリット値",
        ),
        ExamItem(
            group_id,
            "01010",
            "ST",
            "医師の診察に基づき実施",
            series_group_identifier=group_id,
            series_group_relation_code="RSON",
            display_name="貧血検査実施理由",
        ),
        ExamItem("9A755000000000001", "01010", "PQ", None, negation_ind=True, display_name="身長"),
    ]

    content = xml_bytes(build_clinical_document(person, facility, items, "20260730"))
    assert b'xsi:type="IVL_PQ"' in content
    validate_xml(content, XSD_ROOT / "hc08_V08.xsd")

    root = ElementTree.fromstring(content)
    ns = {"h": "urn:hl7-org:v3"}
    parent = next(
        observation
        for observation in root.findall(".//h:entry/h:observation", ns)
        if (code := observation.find("h:code", ns)) is not None and code.get("nullFlavor") == "NA"
    )
    assert parent is not None
    assert [node.get("typeCode") for node in parent.findall("h:entryRelationship", ns)] == ["COMP", "RSON"]
    assert parent.find(".//h:low", ns).get("value") == "35.5"
    assert parent.find(".//h:high", ns).get("value") == "48.9"
    assert parent.find(".//h:interpretationCode", ns).get("code") == "L"

    negated = next(
        observation
        for observation in root.findall(".//h:observation", ns)
        if (code := observation.find("h:code", ns)) is not None and code.get("code") == "9A755000000000001"
    )
    assert negated is not None
    assert negated.get("negationInd") == "true"
    assert negated.find("h:value", ns) is None
