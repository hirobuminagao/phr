from __future__ import annotations

import zipfile
from pathlib import Path

from scripts.hia.script_lib.fund_delivery_zip_exporter import _zip_dir


def test_zip_dir_includes_payload_root_folder(tmp_path: Path) -> None:
    payload_root = tmp_path / "1322100106_06139463_202608100_1"
    (payload_root / "DATA").mkdir(parents=True)
    (payload_root / "XSD").mkdir()
    (payload_root / "DATA" / "h13221001062026081001000001.xml").write_text("<root />", encoding="utf-8")
    (payload_root / "XSD" / "schema.xsd").write_text("<schema />", encoding="utf-8")
    (payload_root / "ix08_V08.xml").write_text("<ix />", encoding="utf-8")
    (payload_root / "su08_V08.xml").write_text("<su />", encoding="utf-8")

    output_zip = tmp_path / "1322100106_06139463_202608100_1.zip"

    _zip_dir(payload_root, output_zip)

    with zipfile.ZipFile(output_zip) as zf:
        names = set(zf.namelist())

    assert "1322100106_06139463_202608100_1/DATA/h13221001062026081001000001.xml" in names
    assert "1322100106_06139463_202608100_1/XSD/schema.xsd" in names
    assert "1322100106_06139463_202608100_1/ix08_V08.xml" in names
    assert "1322100106_06139463_202608100_1/su08_V08.xml" in names
