from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree


MODULE_PATH = Path(__file__).resolve().parents[2] / "scripts" / "from_medical" / "02_import_xml.py"
SPEC = importlib.util.spec_from_file_location("import_xml_02", MODULE_PATH)
import_xml = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = import_xml
SPEC.loader.exec_module(import_xml)


def parse_xml(body: str) -> ElementTree.Element:
    return ElementTree.fromstring(
        f"""
        <ClinicalDocument xmlns="urn:hl7-org:v3">
          <component>
            <structuredBody>
              {body}
            </structuredBody>
          </component>
        </ClinicalDocument>
        """
    )


def section_xml(
    *,
    section_code: str | None = "01030",
    section_code_system: str | None = "1.2.392.200119.6.1010",
    display_name: str | None = "労働安全衛生法健診結果セクション",
    title: str | None = None,
    entries: str,
) -> str:
    attrs = []
    if section_code is not None:
        attrs.append(f'code="{section_code}"')
    if section_code_system is not None:
        attrs.append(f'codeSystem="{section_code_system}"')
    if display_name is not None:
        attrs.append(f'displayName="{display_name}"')
    title_xml = f"<title>{title}</title>" if title is not None else ""
    return f"""
    <component>
      <section>
        <code {' '.join(attrs)}/>
        {title_xml}
        {entries}
      </section>
    </component>
    """


def observation_xml(
    namecode: str,
    value: str = "1",
    interpretation_code: str | None = None,
    interpretation_code_system: str | None = None,
    interpretation_name: str | None = None,
) -> str:
    interpretation_attrs = []
    if interpretation_code is not None:
        interpretation_attrs.append(f'code="{interpretation_code}"')
    if interpretation_code_system is not None:
        interpretation_attrs.append(f'codeSystem="{interpretation_code_system}"')
    if interpretation_name is not None:
        interpretation_attrs.append(f'displayName="{interpretation_name}"')
    interpretation_xml = (
        f"<interpretationCode {' '.join(interpretation_attrs)}/>"
        if interpretation_attrs
        else ""
    )
    return f"""
    <entry>
      <observation>
        <code code="{namecode}" displayName="項目"/>
        <value xsi:type="ST" value="{value}" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"/>
        {interpretation_xml}
      </observation>
    </entry>
    """


class FakeCursor:
    def __init__(self) -> None:
        self.lastrowid = 1
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.execute_calls.append((sql, params))

    def executemany(self, sql: str, params: list[tuple[object, ...]]) -> None:
        self.executemany_calls.append((sql, params))


def test_extract_basic_info_reads_report_and_program_codes() -> None:
    root = ElementTree.fromstring(
        """
        <ClinicalDocument xmlns="urn:hl7-org:v3">
          <code code="10"/>
          <documentationOf>
            <serviceEvent>
              <code code="010"/>
            </serviceEvent>
          </documentationOf>
        </ClinicalDocument>
        """
    )

    basic = import_xml.extract_basic_info(root)

    assert basic["report_category_code"] == "10"
    assert basic["program_type_code"] == "010"


def test_update_xml_ledger_report_codes_only_backfills_non_null_source_values() -> None:
    cur = FakeCursor()
    config = SimpleNamespace(health_db="health_exam_result")

    import_xml.update_xml_ledger_report_codes(
        cur,
        config,
        ledger_id=123,
        report_category_code="10",
        program_type_code="010",
    )

    sql, params = cur.execute_calls[0]
    assert "report_category_code = COALESCE" in " ".join(sql.split())
    assert "program_type_code = COALESCE" in " ".join(sql.split())
    assert params == ("10", "010", 123)


def test_insert_xml_ledger_includes_report_and_program_codes() -> None:
    cur = FakeCursor()
    config = SimpleNamespace(event_id=2, health_db="health_exam_result")

    ledger_id, inserted = import_xml.insert_xml_ledger(
        cur,
        config,
        xml_sha256="a" * 64,
        xml_file_name="h0001.xml",
        xml_status="READY",
        xml_reason=None,
        basic={
            "report_category_code": "10",
            "program_type_code": "010",
        },
        identity_bundle=None,
        subscriber={"subscriber_match_status": "NOT_EXECUTED"},
    )

    sql, params = cur.execute_calls[0]
    assert (ledger_id, inserted) == (1, True)
    assert "report_category_code, program_type_code" in " ".join(sql.split())
    assert "10" in params
    assert "010" in params
    assert sql.count("%s") == len(params)


def test_extract_exam_items_adds_section_info_to_row() -> None:
    root = parse_xml(
        section_xml(entries=observation_xml("9N056160400000049", "既往歴あり"))
    )

    rows = import_xml.extract_exam_items(root).rows

    assert len(rows) == 1
    assert rows[0]["section_code"] == "01030"
    assert rows[0]["section_code_system"] == "1.2.392.200119.6.1010"
    assert rows[0]["section_name"] == "労働安全衛生法健診結果セクション"
    assert rows[0]["namecode"] == "9N056160400000049"
    assert rows[0]["raw_value"] == "既往歴あり"


def test_extract_exam_items_adds_interpretation_code_to_row() -> None:
    root = parse_xml(
        section_xml(
            entries=observation_xml(
                "9N056160400000049",
                "既往歴あり",
                interpretation_code="H",
                interpretation_code_system="2.16.840.1.113883.5.83",
                interpretation_name="High",
            )
        )
    )

    rows = import_xml.extract_exam_items(root).rows

    assert len(rows) == 1
    assert rows[0]["interpretation_code"] == "H"
    assert rows[0]["interpretation_code_system"] == "2.16.840.1.113883.5.83"
    assert rows[0]["interpretation_name"] == "High"


def test_extract_exam_items_uses_title_when_display_name_is_missing() -> None:
    root = parse_xml(
        section_xml(
            display_name=None,
            title="<content>タイトル由来セクション</content>",
            entries=observation_xml("9N056160400000049"),
        )
    )

    rows = import_xml.extract_exam_items(root).rows

    assert rows[0]["section_name"] == "タイトル由来セクション"


def test_extract_exam_items_keeps_none_when_section_name_is_missing() -> None:
    root = parse_xml(
        section_xml(display_name=None, title=None, entries=observation_xml("9N056160400000049"))
    )

    rows = import_xml.extract_exam_items(root).rows

    assert rows[0]["section_name"] is None


def test_extract_exam_items_keeps_same_namecode_rows_from_different_sections() -> None:
    namecode = "9N056160400000049"
    root = parse_xml(
        section_xml(section_code="01010", display_name="特定健診・問診結果セクション", entries=observation_xml(namecode))
        + section_xml(section_code="01030", display_name="労働安全衛生法健診結果セクション", entries=observation_xml(namecode))
    )

    rows = import_xml.extract_exam_items(root).rows

    assert len(rows) == 2
    assert [row["namecode"] for row in rows] == [namecode, namecode]
    assert [row["section_code"] for row in rows] == ["01010", "01030"]


def test_extract_exam_items_applies_same_section_info_to_multiple_observations() -> None:
    root = parse_xml(
        section_xml(
            section_code="01030",
            entries=observation_xml("9N056160400000049") + observation_xml("9N056000000000011"),
        )
    )

    rows = import_xml.extract_exam_items(root).rows

    assert len(rows) == 2
    assert {row["section_code"] for row in rows} == {"01030"}
    assert {row["section_code_system"] for row in rows} == {"1.2.392.200119.6.1010"}


def test_extract_exam_items_keeps_observations_outside_sections_with_null_section_info() -> None:
    root = parse_xml(observation_xml("9N056160400000049"))

    rows = import_xml.extract_exam_items(root).rows

    assert len(rows) == 1
    assert rows[0]["section_code"] is None
    assert rows[0]["section_code_system"] is None
    assert rows[0]["section_name"] is None
    assert rows[0]["namecode"] == "9N056160400000049"


def test_insert_exam_item_values_includes_section_columns_and_params() -> None:
    cur = FakeCursor()
    config = SimpleNamespace(event_id=123, health_db="health_exam_result")

    inserted = import_xml.insert_exam_item_values(
        cur,
        config,
        ledger_id=456,
        subscriber_id=789,
        hia_subscriber_id="hia-1",
        run_id=10,
        rows=[
            {
                "section_code": "01030",
                "section_code_system": "1.2.392.200119.6.1010",
                "section_name": "労働安全衛生法健診結果セクション",
                "namecode": "9N056160400000049",
                "occurrence_no": 1,
                "raw_value": "既往歴あり",
                "raw_value_type": "ST",
                "raw_unit": None,
                "nullflavor": None,
                "code_system": None,
                "code_value": None,
                "code_display": None,
                "interpretation_code": "H",
                "interpretation_code_system": "2.16.840.1.113883.5.83",
                "interpretation_name": "High",
                "namecode_display_name": "既往歴",
                "negation_ind": None,
                "identity_item_code": None,
                "jun_no": None,
            }
        ],
    )

    assert inserted == 1
    sql, params = cur.executemany_calls[0]
    assert "section_code, section_code_system, section_name" in " ".join(sql.split())
    assert "interpretation_code, interpretation_code_system, interpretation_name" in " ".join(
        sql.split()
    )
    assert params[0][4:8] == (
        "9N056160400000049",
        "01030",
        "1.2.392.200119.6.1010",
        "労働安全衛生法健診結果セクション",
    )
    assert params[0][16:19] == (
        "H",
        "2.16.840.1.113883.5.83",
        "High",
    )
