import xml.etree.ElementTree as ET

from scripts.lib.shg.xml.section_90010_guidance_info import extract_90010_guidance


def test_extracts_health_checkup_date_from_90010_item() -> None:
    root = ET.fromstring(
        """
        <ClinicalDocument xmlns="urn:hl7-org:v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
          <component><structuredBody><component><section>
            <code code="90010" />
            <entry><act><entryRelationship><observation>
              <code code="1020000001" />
              <value xsi:type="CD" code="1" />
            </observation></entryRelationship><entryRelationship><observation>
              <code code="1020000004" />
              <value xsi:type="ST">20240922</value>
            </observation></entryRelationship></act></entry>
          </section></component></structuredBody></component>
        </ClinicalDocument>
        """
    )

    result = extract_90010_guidance(root)

    assert result["guidance_type_code"] == "1"
    assert result["health_checkup_date"] == "20240922"
