#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import re
import sys
import zipfile

from lxml import etree

if __name__ == "__main__" and __package__ is None:
    project_root = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(project_root))

from scripts.lib.examination.value_normalizer import (  # noqa: E402
    MHLW_TEXT_MAX_BYTES,
    mhlw_text_byte_length,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_XSD_DIR = PROJECT_ROOT / "scripts" / "from_medical" / "source" / "XSD" / "mhlw_v4_20230331_v08"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "data" / "hia_xml_zip_checks"
NS_HL7 = "urn:hl7-org:v3"
NS_XSI = "http://www.w3.org/2001/XMLSchema-instance"
XML_PARSER = etree.XMLParser(remove_blank_text=False, recover=False)

CODE_SYSTEM_BY_NAMECODE = {
    "1B040Z121015Z0111": "1.2.392.200119.6.2100",
    "1B040Z122015Z0111": "1.2.392.200119.6.2100",
}


@dataclass(frozen=True)
class Finding:
    zip_path: str
    xml_inner_path: str
    check_type: str
    severity: str
    namecode: str | None
    item_display_name: str | None
    message: str
    namecode_source: str | None = None
    value_preview: str | None = None
    mhlw_byte_length: int | None = None
    max_byte_length: int | None = None
    can_fix: bool = False
    fix_note: str | None = None
    fixed: bool = False


@dataclass(frozen=True)
class XsdValidationError:
    message: str
    line: int | None = None
    column: int | None = None


@dataclass
class Summary:
    zip_files_seen: int = 0
    xml_files_seen: int = 0
    findings: int = 0
    errors: int = 0
    warnings: int = 0
    fixed: int = 0
    fixed_zip_path: str | None = None
    report_csv_path: str | None = None


def _resolve_path(path: str | Path) -> Path:
    value = Path(path).expanduser()
    if value.is_absolute():
        return value
    return PROJECT_ROOT / value


def _schema_for_xml_name(xsd_dir: Path, inner_path: str) -> Path | None:
    name = Path(inner_path).name.lower()
    if name.startswith("h") and name.endswith(".xml"):
        return xsd_dir / "hc08_V08.xsd"
    if name.startswith("ix08") and name.endswith(".xml"):
        return xsd_dir / "ix08_V08.xsd"
    if name.startswith("su08") and name.endswith(".xml"):
        return xsd_dir / "su08_V08.xsd"
    return None


def _load_schema(path: Path) -> etree.XMLSchema:
    return etree.XMLSchema(etree.parse(str(path.resolve())))


def _validate_xml(content: bytes, xsd_path: Path) -> list[XsdValidationError]:
    schema = _load_schema(xsd_path)
    document = etree.fromstring(content, parser=XML_PARSER)
    if schema.validate(document):
        return []
    return [
        XsdValidationError(
            message=str(error),
            line=error.line or None,
            column=error.column or None,
        )
        for error in schema.error_log
    ]


def _namecode_for_value(value: etree._Element) -> tuple[str | None, str | None]:
    observation = value.getparent()
    while observation is not None and etree.QName(observation).localname != "observation":
        observation = observation.getparent()
    if observation is None:
        return None, None
    code = observation.find(f"{{{NS_HL7}}}code")
    if code is None:
        return None, None
    return code.get("code"), code.get("displayName")


def _namecode_for_element(element: etree._Element | None) -> tuple[str | None, str | None]:
    current = element
    while current is not None:
        if etree.QName(current).localname == "observation":
            code = current.find(f"{{{NS_HL7}}}code")
            if code is not None:
                return code.get("code"), code.get("displayName")
        current = current.getparent()
    return None, None


def _element_for_error_line(document: etree._Element, line: int | None) -> tuple[etree._Element | None, str | None]:
    if line is None:
        return None, None
    exact_candidates = [
        element
        for element in document.iter()
        if element.sourceline is not None and element.sourceline == line
    ]
    if exact_candidates:
        return exact_candidates[-1], "ERROR_LINE_ELEMENT"
    candidates = [
        element
        for element in document.iter()
        if element.sourceline is not None and element.sourceline <= line
    ]
    if not candidates:
        return None, None
    return max(candidates, key=lambda element: int(element.sourceline or 0)), "NEAREST_PREVIOUS_ELEMENT"


def _namecode_for_xsd_error(document: etree._Element, error: XsdValidationError) -> tuple[str | None, str | None, str | None]:
    namecode = _extract_namecode_from_xsd_error(error.message)
    if namecode:
        display_name = _display_name_for_namecode(document, namecode)
        return namecode, display_name, "MESSAGE_CODE"
    element, source = _element_for_error_line(document, error.line)
    namecode, display_name = _namecode_for_element(element)
    if not namecode:
        return None, None, None
    return namecode, display_name, source


def _display_name_for_namecode(document: etree._Element, namecode: str) -> str | None:
    xpath = f".//*[local-name()='observation']/*[local-name()='code'][@code={namecode!r}]"
    code = document.xpath(xpath)
    if not code:
        return None
    return code[0].get("displayName")


def _text_preview(value: str | None, limit: int = 80) -> str | None:
    if value is None:
        return None
    text = value.replace("\r", "\\r").replace("\n", "\\n")
    return text if len(text) <= limit else text[:limit] + "..."


def _check_and_fix_xml(
    *,
    zip_path: Path,
    inner_path: str,
    content: bytes,
    xsd_dir: Path,
    fix: bool,
) -> tuple[bytes, list[Finding]]:
    findings: list[Finding] = []
    updated_content = content
    try:
        document = etree.fromstring(content, parser=XML_PARSER)
    except etree.XMLSyntaxError as exc:
        findings.append(
            Finding(
                zip_path=str(zip_path),
                xml_inner_path=inner_path,
                check_type="XML_PARSE",
                severity="ERROR",
                namecode=None,
                item_display_name=None,
                message=str(exc),
            )
        )
        return updated_content, findings

    changed = False
    for value in document.xpath(".//*[local-name()='value']"):
        value_type = value.get(f"{{{NS_XSI}}}type")
        if value_type in {"CD", "CO"} and (value.get("codeSystem") or "") == "":
            namecode, display_name = _namecode_for_value(value)
            replacement = CODE_SYSTEM_BY_NAMECODE.get(namecode or "")
            if replacement:
                if fix:
                    value.set("codeSystem", replacement)
                    changed = True
                findings.append(
                    Finding(
                        zip_path=str(zip_path),
                        xml_inner_path=inner_path,
                        check_type="CODE_SYSTEM_EMPTY",
                        severity="FIXED" if fix else "WARNING",
                        namecode=namecode,
                        item_display_name=display_name,
                        namecode_source="VALUE_PARENT_OBSERVATION",
                        message=f"codeSystem is empty; can fill from known namecode: {replacement}",
                        value_preview=value.get("code"),
                        can_fix=True,
                        fix_note=f"codeSystem={replacement}",
                        fixed=fix,
                    )
                )
            else:
                findings.append(
                    Finding(
                        zip_path=str(zip_path),
                        xml_inner_path=inner_path,
                        check_type="CODE_SYSTEM_EMPTY",
                        severity="ERROR",
                        namecode=namecode,
                        item_display_name=display_name,
                        namecode_source="VALUE_PARENT_OBSERVATION" if namecode else None,
                        message="codeSystem is empty and no safe fix rule is registered",
                        value_preview=value.get("code"),
                    )
                )
        if value_type in {"ST", "TX"}:
            text = value.text or ""
            byte_length = mhlw_text_byte_length(text)
            if byte_length > MHLW_TEXT_MAX_BYTES:
                namecode, display_name = _namecode_for_value(value)
                findings.append(
                    Finding(
                        zip_path=str(zip_path),
                        xml_inner_path=inner_path,
                        check_type="ST_MAX_BYTE_LENGTH_EXCEEDED",
                        severity="ERROR",
                        namecode=namecode,
                        item_display_name=display_name,
                        namecode_source="VALUE_PARENT_OBSERVATION" if namecode else None,
                        message="ST/TX text exceeds MHLW byte length limit",
                        value_preview=_text_preview(text),
                        mhlw_byte_length=byte_length,
                        max_byte_length=MHLW_TEXT_MAX_BYTES,
                    )
                )

    if changed:
        updated_content = etree.tostring(
            document,
            encoding="utf-8",
            xml_declaration=content.lstrip().startswith(b"<?xml"),
        )

    schema_path = _schema_for_xml_name(xsd_dir, inner_path)
    if schema_path is not None:
        try:
            xsd_errors = _validate_xml(updated_content, schema_path)
        except Exception as exc:
            xsd_errors = [XsdValidationError(message=str(exc))]
        for error in xsd_errors:
            namecode, display_name, namecode_source = _namecode_for_xsd_error(document, error)
            findings.append(
                Finding(
                    zip_path=str(zip_path),
                    xml_inner_path=inner_path,
                    check_type="XSD",
                    severity="ERROR",
                    namecode=namecode,
                    item_display_name=display_name,
                    namecode_source=namecode_source,
                    message=error.message,
                )
            )

    return updated_content, findings


def _extract_namecode_from_xsd_error(message: str) -> str | None:
    match = re.search(r"'([0-9A-Za-z]{17})'", message)
    return match.group(1) if match else None


def _iter_target_zips(paths: list[Path]) -> list[Path]:
    result: list[Path] = []
    for path in paths:
        if path.is_dir():
            result.extend(sorted(path.rglob("*.zip")))
        elif path.suffix.lower() == ".zip":
            result.append(path)
        else:
            raise ValueError(f"not a ZIP file or directory: {path}")
    return result


def _fixed_zip_path(source: Path, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir / f"{source.stem}_fixed{source.suffix}"


def check_zip(zip_path: Path, *, xsd_dir: Path, fix: bool, fixed_output_dir: Path) -> tuple[Summary, list[Finding]]:
    summary = Summary(zip_files_seen=1)
    findings: list[Finding] = []
    fixed_zip_path = _fixed_zip_path(zip_path, fixed_output_dir) if fix else None
    with zipfile.ZipFile(zip_path, "r") as zin:
        zout = zipfile.ZipFile(fixed_zip_path, "w", compression=zipfile.ZIP_DEFLATED) if fixed_zip_path else None
        try:
            for info in zin.infolist():
                content = zin.read(info.filename)
                updated = content
                if info.filename.lower().endswith(".xml"):
                    summary.xml_files_seen += 1
                    updated, item_findings = _check_and_fix_xml(
                        zip_path=zip_path,
                        inner_path=info.filename,
                        content=content,
                        xsd_dir=xsd_dir,
                        fix=fix,
                    )
                    findings.extend(item_findings)
                if zout is not None:
                    zout.writestr(info, updated)
        finally:
            if zout is not None:
                zout.close()
    if fixed_zip_path:
        summary.fixed_zip_path = str(fixed_zip_path)
    return summary, findings


def write_report(findings: list[Finding], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = output_dir / f"hia_xml_zip_check_{stamp}.csv"
    columns = [
        "zip_path",
        "xml_inner_path",
        "severity",
        "check_type",
        "namecode",
        "item_display_name",
        "namecode_source",
        "message",
        "value_preview",
        "mhlw_byte_length",
        "max_byte_length",
        "can_fix",
        "fix_note",
        "fixed",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=columns)
        writer.writeheader()
        for finding in findings:
            writer.writerow({column: getattr(finding, column) for column in columns})
    return path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check HIA/MHLW XML ZIP files with XSD, ST length, and safe XML fixes.",
    )
    parser.add_argument("paths", nargs="+", help="ZIP file(s) or directory path(s). Directories are scanned recursively.")
    parser.add_argument("--xsd-dir", default=str(DEFAULT_XSD_DIR))
    parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR))
    parser.add_argument("--fixed-output-dir", default=str(DEFAULT_REPORT_DIR / "fixed"))
    parser.add_argument("--fix", action="store_true", help="Write *_fixed.zip with safe XML fixes.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    xsd_dir = _resolve_path(args.xsd_dir)
    report_dir = _resolve_path(args.report_dir)
    fixed_output_dir = _resolve_path(args.fixed_output_dir)
    zip_paths = _iter_target_zips([_resolve_path(path) for path in args.paths])

    all_findings: list[Finding] = []
    summaries: list[Summary] = []
    for zip_path in zip_paths:
        summary, findings = check_zip(zip_path, xsd_dir=xsd_dir, fix=args.fix, fixed_output_dir=fixed_output_dir)
        summaries.append(summary)
        all_findings.extend(findings)

    report_csv = write_report(all_findings, report_dir)
    total = Summary()
    total.zip_files_seen = sum(item.zip_files_seen for item in summaries)
    total.xml_files_seen = sum(item.xml_files_seen for item in summaries)
    total.findings = len(all_findings)
    total.errors = sum(1 for item in all_findings if item.severity == "ERROR")
    total.warnings = sum(1 for item in all_findings if item.severity == "WARNING")
    total.fixed = sum(1 for item in all_findings if item.fixed)
    total.report_csv_path = str(report_csv)

    print(
        "hia_xml_zip_check "
        f"zips={total.zip_files_seen} xmls={total.xml_files_seen} "
        f"findings={total.findings} errors={total.errors} warnings={total.warnings} "
        f"fixed={total.fixed} report={total.report_csv_path}"
    )
    for summary in summaries:
        if summary.fixed_zip_path:
            print(f"  fixed_zip={summary.fixed_zip_path}")
    return 1 if total.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
