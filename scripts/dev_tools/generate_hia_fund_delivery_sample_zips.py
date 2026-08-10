#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import zipfile
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SAMPLE_XML_DIR = PROJECT_ROOT / "docs" / "spec" / "hia_fund_ledger_xml" / "samples" / "hia_download_xml"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "hia_export" / "input_zip" / "06139463"


ZIP_FIXTURES = [
    {
        "zip_name": "0110717770_06139463_202606010_1.zip",
        "xmls": [
            ("hia_sample_001_normal.xml", "h001.xml"),
            ("hia_sample_002_duplicate_old.xml", "h002.xml"),
        ],
    },
    {
        "zip_name": "0110717770_06139463_202606150_1.zip",
        "xmls": [
            ("hia_sample_002_duplicate_new.xml", "h002.xml"),
            ("hia_sample_004_symbol_branch.xml", "h004.xml"),
        ],
    },
    {
        "zip_name": "0110717770_06139463_202606200_1.zip",
        "xmls": [
            ("hia_sample_003_missing_insurance_number.xml", "h003.xml"),
        ],
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate non-sensitive HIA downloaded ZIP samples for fund delivery workflow checks.",
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--clean", action="store_true", help="Remove existing generated sample ZIPs first.")
    return parser.parse_args()


def write_zip(output_dir: Path, fixture: dict[str, object]) -> Path:
    zip_path = output_dir / str(fixture["zip_name"])
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("ix08_V08.xml", "<?xml version=\"1.0\" encoding=\"UTF-8\"?><index />\n")
        zf.writestr("su08_V08.xml", "<?xml version=\"1.0\" encoding=\"UTF-8\"?><summary />\n")
        for source_name, inner_name in fixture["xmls"]:  # type: ignore[index]
            source_path = SAMPLE_XML_DIR / source_name
            zf.write(source_path, f"DATA/{inner_name}")
    return zip_path


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir
    if args.clean and output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    generated: list[Path] = []
    for fixture in ZIP_FIXTURES:
        generated.append(write_zip(output_dir, fixture))

    print(f"generated={len(generated)} output_dir={output_dir}")
    for path in generated:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
