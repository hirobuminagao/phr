#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase4 import_xml entry point.

Read ZIP/XML receipts discovered by Phase3, ledger XML contents, resolve
subscriber identity, and register raw exam item values.
"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import re
import sys
import zipfile
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence, cast
from xml.etree import ElementTree

import yaml
from mysql.connector import errorcode
from mysql.connector.errors import IntegrityError


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.lookup.event import get_event_insurer_number
from scripts.lib.db.lookup.exam_item_master import get_exam_items
from scripts.lib.db.lookup.subscriber_identity import resolve_subscriber_identity
from scripts.lib.db.lookup.subscriber_export_projection import (
    load_subscriber_basic_export_projection_by_id,
    resolve_basic_identity_export_values,
)
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import PHR_MASTER
from scripts.lib.etl import RunMetrics
from scripts.lib.etl import finish_run as etl_finish_run
from scripts.lib.etl import log_error as etl_log_error
from scripts.lib.etl import start_run as etl_start_run
from scripts.lib.examination.value_normalizer import CODE_DATA_TYPES, normalize_exam_item_value
from scripts.lib.identity.generator import generate_identity_bundle
from scripts.from_medical.script_lib.basic_info_completion import resolve_basic_info_completion


HEALTH_EXAM_RESULT_DB = "health_exam_result"
DEV_PHR_DB = "dev_phr"
WORK_OTHER_DB = "work_other"
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config" / "import_xml.yml"

ETL_PHASE = "IMPORT_XML"
ETL_SOURCE = "FROM_MEDICAL"

FILE_STATUS_DISCOVERED = "DISCOVERED"
FILE_STATUS_IMPORTING = "IMPORTING"
FILE_STATUS_IMPORTED = "IMPORTED"
FILE_STATUS_WARNING = "WARNING"
FILE_STATUS_ERROR = "ERROR"
FILE_STATUS_WAITING_PASSWORD = "WAITING_PASSWORD"

FILE_TYPE_ZIP = "ZIP"
FILE_TYPE_XML = "XML"
XML_STATUS_READY = "READY"
XML_STATUS_PARSE_ERROR = "PARSE_ERROR"
LEDGER_TYPE_EXAM = "EXAM"

SUBSCRIBER_MATCH_MATCHED = "MATCHED"
SUBSCRIBER_MATCH_CANDIDATE = "CANDIDATE"
SUBSCRIBER_MATCH_NOT_FOUND = "NOT_FOUND"
SUBSCRIBER_MATCH_IDENTITY_ERROR = "IDENTITY_ERROR"
SUBSCRIBER_MATCH_NOT_EXECUTED = "NOT_EXECUTED"
SUBSCRIBER_METHOD_PERSON_ID_CUSTOM = "PERSON_ID_CUSTOM"
SUBSCRIBER_REASON_PERSON_ID_CUSTOM_CANDIDATE = "PERSON_ID_CUSTOM_CANDIDATE"

ERROR_FIELD_CONFIG = "CONFIG"
ERROR_FIELD_FILE = "FILE"
ERROR_FIELD_ZIP = "ZIP"
ERROR_FIELD_XML = "XML"
ERROR_FIELD_IDENTITY = "IDENTITY"
ERROR_FIELD_SUBSCRIBER = "SUBSCRIBER"
ERROR_FIELD_DB = "DB"

XSI_TYPE_ATTR = "{http://www.w3.org/2001/XMLSchema-instance}type"
NAMECODE_RE = re.compile(r"^[0-9A-Za-z]{17}$")
PATIENT_ID_ROOT_INSURER_NUMBER = "1.2.392.200119.6.101"
PATIENT_ID_ROOT_INSURANCE_SYMBOL = "1.2.392.200119.6.204"
PATIENT_ID_ROOT_INSURANCE_NUMBER = "1.2.392.200119.6.205"
FACILITY_CODE_ROOT = "1.2.392.200119.6.102"
FINDING_PRESENCE_PAIRS: dict[str, tuple[str, str, str]] = {
    "9N056160400000049": ("9N056000000000011", "1.2.392.200119.6.2001", "既往歴"),
    "9N061160800000049": ("9N061000000000011", "1.2.392.200119.6.2001", "自覚症状"),
    "9N066160800000049": ("9N066000000000011", "1.2.392.200119.6.2001", "他覚症状"),
    "9A110160800000049": ("9A110160700000011", "1.2.392.200119.6.2002", "心電図（所見の有無）"),
    "9N206160800000049": ("9N206160700000011", "1.2.392.200119.6.2002", "胸部X線（所見の有無）"),
    "9N251160800000049": ("9N251160700000011", "1.2.392.200119.6.2002", "眼底検査（所見の有無）"),
    "9N256160800000049": ("9N256160700000011", "1.2.392.200119.6.2002", "胃部X線（所見の有無）"),
    "9N266160800000049": ("9N266160700000011", "1.2.392.200119.6.2002", "胃内視鏡（所見の有無）"),
    "9N271160800000049": ("9N271160700000011", "1.2.392.200119.6.2002", "腹部超音波（所見の有無）"),
    "9N276160800000049": ("9N276160700000011", "1.2.392.200119.6.2002", "乳房視触診（所見の有無）"),
    "9N281160800000049": ("9N281160700000011", "1.2.392.200119.6.2002", "マンモグラフィ（所見の有無）"),
    "9F140160800000049": ("9F140160700000011", "1.2.392.200119.6.2002", "子宮頸部細胞診（所見の有無）"),
    "9N291160800000049": ("9N291160700000011", "1.2.392.200119.6.2002", "婦人科診察（所見の有無）"),
}


@dataclass(frozen=True)
class InputConfig:
    file_status: str
    file_statuses: tuple[str, ...]
    file_types: tuple[str, ...]
    etl_run_id: int | None


@dataclass(frozen=True)
class ZipConfig:
    target_xml_pattern: str
    exclude_prefixes: tuple[str, ...]
    exclude_keywords: tuple[str, ...]
    keep_work: bool


@dataclass(frozen=True)
class ImportConfig:
    event_id: int
    health_db: str
    dev_db: str
    master_db: str
    work_db: str
    dry_run: bool
    limit: int
    chunk_size_mb: int
    input: InputConfig
    zip: ZipConfig


@dataclass
class ImportSummary:
    event_id: int
    dry_run: bool
    files: int = 0
    xml_seen: int = 0
    xml_ledgers_inserted: int = 0
    xml_ledgers_existing: int = 0
    xml_links_inserted: int = 0
    xml_links_duplicate: int = 0
    file_receipts_updated: int = 0
    exam_item_values_inserted: int = 0
    xml_parse_errors: int = 0
    identity_errors: int = 0
    subscriber_not_found: int = 0
    zip_no_target_xml: int = 0
    xml_excluded: int = 0
    errors: int = 0
    file_status_counts: dict[str, int] = field(default_factory=dict)

    def bump_file_status(self, status: str) -> None:
        self.file_status_counts[status] = self.file_status_counts.get(status, 0) + 1

    def to_metrics(self) -> RunMetrics:
        return RunMetrics(
            files=self.files,
            rows_seen=self.xml_seen,
            rows_inserted=self.xml_ledgers_inserted,
            rows_updated=self.xml_links_inserted + self.file_receipts_updated,
            rows_skipped=self.xml_ledgers_existing,
            errors=self.errors,
        )

    def to_message(self) -> str:
        return (
            "import_xml "
            f"event_id={self.event_id} files={self.files} xml_seen={self.xml_seen} "
            f"xml_inserted={self.xml_ledgers_inserted} xml_existing={self.xml_ledgers_existing} "
            f"links={self.xml_links_inserted} exam_items={self.exam_item_values_inserted} "
            f"errors={self.errors}"
        )

    def print(self) -> None:
        print(self.to_message())
        print(f"  dry_run={self.dry_run}")
        print(
            "  files: "
            f"target={self.files} updated={self.file_receipts_updated} "
            f"statuses={dict(sorted(self.file_status_counts.items()))}"
        )
        print(
            "  xml: "
            f"seen={self.xml_seen} ledgers_inserted={self.xml_ledgers_inserted} "
            f"ledgers_existing={self.xml_ledgers_existing} parse_errors={self.xml_parse_errors} "
            f"excluded_xml={self.xml_excluded}"
        )
        print(
            "  links/items: "
            f"links_inserted={self.xml_links_inserted} links_duplicate={self.xml_links_duplicate} "
            f"exam_item_values={self.exam_item_values_inserted}"
        )
        print(
            "  errors: "
            f"total={self.errors} identity={self.identity_errors} "
            f"subscriber_not_found={self.subscriber_not_found} zip_no_target={self.zip_no_target_xml}"
        )


@dataclass(frozen=True)
class XmlCandidate:
    file_receipt: Mapping[str, Any]
    inner_path: str | None
    data: bytes

    @property
    def display_path(self) -> str:
        source_path = str(self.file_receipt.get("source_path") or "")
        if self.inner_path:
            return f"{source_path}!{self.inner_path}"
        return source_path

    @property
    def xml_file_name(self) -> str | None:
        raw_path = self.inner_path or str(self.file_receipt.get("source_path") or "")
        if not raw_path:
            return None
        parts = [part for part in re.split(r"[\\/]+", raw_path) if part]
        return parts[-1] if parts else None


@dataclass
class XmlProcessResult:
    ok: bool
    new_ledger: bool = False
    existing_ledger: bool = False
    link_inserted: bool = False
    link_duplicate: bool = False
    exam_item_count: int = 0
    parse_error: bool = False
    error_count: int = 0


class ImportDbError(RuntimeError):
    def __init__(self, error_code: str, message: str, field_value: str | None = None) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.message = message
        self.field_value = field_value


class ZipPasswordNotFoundError(RuntimeError):
    pass


class ZipDecryptError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import discovered ZIP/XML files into XML ledger tables.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Import config YAML path.")
    parser.add_argument("--event-id", type=int, default=None, help="Override event_id.")
    parser.add_argument("--etl-run-id", type=int, default=None, help="Limit input file_receipts to a specific scan run.")
    parser.add_argument("--dry-run", action="store_true", help="Read and report without DB writes. DB reads are still required.")
    parser.add_argument("--limit", type=int, default=None, help="Override maximum file_receipts to process. 0 means unlimited.")
    parser.add_argument(
        "--include-imported",
        action="store_true",
        help="Also process file_receipts already marked IMPORTED or WARNING.",
    )
    parser.add_argument("--db-prefix", default="PHR_DB_", help="Environment prefix for DB connection.")
    parser.add_argument("--health-db", default=None, help="Override health_exam_result schema name.")
    parser.add_argument("--dev-db", default=None, help="Override dev_phr schema name.")
    parser.add_argument("--master-db", default=None, help="Override phr_master schema name.")
    parser.add_argument("--work-db", default=None, help="Override work_other schema name.")
    parser.add_argument("--keep-work", action="store_true", help="Override config zip.keep_work to true.")
    return parser.parse_args()


def load_import_config(path: str | Path) -> ImportConfig:
    with Path(path).open("r", encoding="utf-8") as fp:
        raw_data = yaml.safe_load(fp) or {}

    data = cast(Mapping[str, Any], raw_data)
    raw_input = cast(Mapping[str, Any], data.get("input") or {})
    raw_zip = cast(Mapping[str, Any], data.get("zip") or {})
    raw_processing = cast(Mapping[str, Any], data.get("processing") or {})

    file_types = tuple(str(v).upper() for v in raw_input.get("file_types") or [FILE_TYPE_ZIP, FILE_TYPE_XML])
    raw_file_statuses = raw_input.get("file_statuses")
    if raw_file_statuses:
        file_statuses = tuple(str(v).upper() for v in raw_file_statuses)
    else:
        file_status = str(raw_input.get("file_status") or FILE_STATUS_DISCOVERED).upper()
        file_statuses = (file_status,)
        if file_status == FILE_STATUS_DISCOVERED:
            file_statuses = (FILE_STATUS_DISCOVERED, FILE_STATUS_WAITING_PASSWORD)
    raw_etl_run_id = raw_input.get("etl_run_id")
    etl_run_id = int(raw_etl_run_id) if raw_etl_run_id not in (None, "") else None

    return ImportConfig(
        event_id=int(data.get("event_id", 2)),
        health_db=str(data.get("health_db") or HEALTH_EXAM_RESULT_DB),
        dev_db=str(data.get("dev_db") or DEV_PHR_DB),
        master_db=str(data.get("master_db") or PHR_MASTER),
        work_db=str(data.get("work_db") or WORK_OTHER_DB),
        dry_run=bool(data.get("dry_run", False)),
        limit=int(data.get("limit", 0) or 0),
        chunk_size_mb=int(raw_processing.get("chunk_size_mb", 8) or 8),
        input=InputConfig(
            file_status=str(raw_input.get("file_status") or FILE_STATUS_DISCOVERED),
            file_statuses=file_statuses,
            file_types=file_types,
            etl_run_id=etl_run_id,
        ),
        zip=ZipConfig(
            target_xml_pattern=str(raw_zip.get("target_xml_pattern") or "h*.xml"),
            exclude_prefixes=tuple(str(v).lower() for v in raw_zip.get("exclude_prefixes") or ("ix08", "su08")),
            exclude_keywords=tuple(str(v).lower() for v in raw_zip.get("exclude_keywords") or ("schema", "xsd")),
            keep_work=bool(raw_zip.get("keep_work", False)),
        ),
    )


def resolve_config(args: argparse.Namespace) -> ImportConfig:
    config = load_import_config(args.config)
    file_statuses = config.input.file_statuses
    if args.include_imported:
        file_statuses = tuple(dict.fromkeys((*file_statuses, FILE_STATUS_IMPORTED, FILE_STATUS_WARNING)))
    return ImportConfig(
        event_id=args.event_id if args.event_id is not None else config.event_id,
        health_db=args.health_db if args.health_db is not None else config.health_db,
        dev_db=args.dev_db if args.dev_db is not None else config.dev_db,
        master_db=args.master_db if args.master_db is not None else config.master_db,
        work_db=args.work_db if args.work_db is not None else config.work_db,
        dry_run=True if args.dry_run else config.dry_run,
        limit=args.limit if args.limit is not None else config.limit,
        chunk_size_mb=config.chunk_size_mb,
        input=InputConfig(
            file_status=config.input.file_status,
            file_statuses=file_statuses,
            file_types=config.input.file_types,
            etl_run_id=args.etl_run_id if args.etl_run_id is not None else config.input.etl_run_id,
        ),
        zip=ZipConfig(
            target_xml_pattern=config.zip.target_xml_pattern,
            exclude_prefixes=config.zip.exclude_prefixes,
            exclude_keywords=config.zip.exclude_keywords,
            keep_work=True if args.keep_work else config.zip.keep_work,
        ),
    )


def validate_config(config: ImportConfig) -> None:
    if not config.event_id:
        raise ValueError("event_id is required")
    if config.limit < 0:
        raise ValueError("limit must be >= 0")
    if config.chunk_size_mb <= 0:
        raise ValueError("processing.chunk_size_mb must be > 0")
    file_types = set(config.input.file_types)
    if not file_types:
        raise ValueError("input.file_types is required")
    unsupported = file_types - {FILE_TYPE_ZIP, FILE_TYPE_XML}
    if unsupported:
        raise ValueError(f"unsupported input.file_types: {', '.join(sorted(unsupported))}")


def qname(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def compact_text(value: Any) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    return text or None


def attr_value(elem: ElementTree.Element, *names: str) -> str | None:
    wanted = {name.lower() for name in names}
    for key, value in elem.attrib.items():
        if local_name(key).lower() in wanted:
            text = compact_text(value)
            if text:
                return text
    return None


def elem_text(elem: ElementTree.Element | None) -> str | None:
    if elem is None:
        return None
    return compact_text("".join(elem.itertext()))


def iter_named(root: ElementTree.Element, *names: str) -> Iterable[ElementTree.Element]:
    wanted = {name.lower() for name in names}
    for elem in root.iter():
        if local_name(elem.tag).lower() in wanted:
            yield elem


def first_exact_attr_or_text(root: ElementTree.Element, names: Sequence[str], attrs: Sequence[str]) -> str | None:
    wanted = {name.lower() for name in names}
    for elem in root.iter():
        if local_name(elem.tag).lower() not in wanted:
            continue
        for attr in attrs:
            value = attr_value(elem, attr)
            if value:
                return value
        value = elem_text(elem)
        if value:
            return value
    return None


def first_child_attr(parent: ElementTree.Element, child_name: str, *attrs: str) -> str | None:
    child_name_l = child_name.lower()
    for child in list(parent):
        if local_name(child.tag).lower() == child_name_l:
            for attr in attrs:
                value = attr_value(child, attr)
                if value:
                    return value
            return elem_text(child)
    return None


def child_by_local(parent: ElementTree.Element | None, child_name: str) -> ElementTree.Element | None:
    if parent is None:
        return None
    child_name_l = child_name.lower()
    for child in list(parent):
        if local_name(child.tag).lower() == child_name_l:
            return child
    return None


def children_by_local(parent: ElementTree.Element | None, child_name: str) -> list[ElementTree.Element]:
    if parent is None:
        return []
    child_name_l = child_name.lower()
    return [child for child in list(parent) if local_name(child.tag).lower() == child_name_l]


def path_by_local(root: ElementTree.Element, *path: str) -> ElementTree.Element | None:
    elem: ElementTree.Element | None = root
    parts = list(path)
    if parts and local_name(root.tag).lower() == parts[0].lower():
        parts = parts[1:]
    for part in parts:
        elem = child_by_local(elem, part)
        if elem is None:
            return None
    return elem


def attr_at_path(root: ElementTree.Element, path: Sequence[str], attr_name: str) -> str | None:
    elem = path_by_local(root, *path)
    return attr_value(elem, attr_name) if elem is not None else None


def text_at_path(root: ElementTree.Element, path: Sequence[str]) -> str | None:
    elem = path_by_local(root, *path)
    return elem_text(elem) if elem is not None else None


def attr_at_first_path(root: ElementTree.Element, paths: Sequence[Sequence[str]], attr_name: str) -> str | None:
    for path in paths:
        value = attr_at_path(root, path, attr_name)
        if value:
            return value
    return None


def patient_role(root: ElementTree.Element) -> ElementTree.Element | None:
    return path_by_local(root, "ClinicalDocument", "recordTarget", "patientRole")


def patient_role_id_extension(root: ElementTree.Element, id_root: str) -> str | None:
    role = patient_role(root)
    for elem in children_by_local(role, "id"):
        if attr_value(elem, "root") == id_root:
            return attr_value(elem, "extension")
    return None


def service_event(root: ElementTree.Element) -> ElementTree.Element | None:
    return path_by_local(root, "ClinicalDocument", "documentationOf", "serviceEvent")


def extract_document_id(root: ElementTree.Element) -> str | None:
    doc_id = path_by_local(root, "ClinicalDocument", "id")
    if doc_id is None:
        return None
    root_value = attr_value(doc_id, "root")
    extension = attr_value(doc_id, "extension")
    if root_value and extension:
        return f"{root_value}|{extension}"
    return root_value or extension


def parse_mysql_date(value: str | None) -> str | None:
    if not value:
        return None
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) < 8:
        return None
    y, m, d = int(digits[:4]), int(digits[4:6]), int(digits[6:8])
    try:
        return date(y, m, d).isoformat()
    except ValueError:
        return None


def extract_basic_info(root: ElementTree.Element) -> dict[str, Any]:
    info: dict[str, Any] = {
        "document_id": None,
        "insurer_number_raw": None,
        "insurance_symbol_raw": None,
        "insurance_number_raw": None,
        "name_kana_full_raw": None,
        "gender_code": None,
        "birthdate": None,
        "report_category_code": None,
        "program_type_code": None,
        "facility_code": None,
        "facility_name": None,
        "exam_date": None,
    }

    info["document_id"] = extract_document_id(root)
    info["birthdate"] = attr_at_path(
        root,
        ("ClinicalDocument", "recordTarget", "patientRole", "patient", "birthTime"),
        "value",
    )
    info["gender_code"] = attr_at_path(
        root,
        ("ClinicalDocument", "recordTarget", "patientRole", "patient", "administrativeGenderCode"),
        "code",
    )
    info["report_category_code"] = attr_at_path(
        root,
        ("ClinicalDocument", "code"),
        "code",
    )
    info["program_type_code"] = attr_at_path(
        root,
        ("ClinicalDocument", "documentationOf", "serviceEvent", "code"),
        "code",
    )
    exam_raw = attr_at_first_path(
        root,
        (
            ("ClinicalDocument", "documentationOf", "serviceEvent", "effectiveTime"),
            ("ClinicalDocument", "documentationOf", "serviceEvent", "effectiveTime", "low"),
        ),
        "value",
    )
    info["exam_date"] = parse_mysql_date(exam_raw)

    for elem in iter_named(root, "nameKana", "name_kana", "kanaName", "kana_name"):
        value = elem_text(elem)
        if value:
            info["name_kana_full_raw"] = value
            break
    if not info["name_kana_full_raw"]:
        info["name_kana_full_raw"] = text_at_path(
            root,
            ("ClinicalDocument", "recordTarget", "patientRole", "patient", "name"),
        )

    event = service_event(root)
    performer_org = path_by_local(
        root,
        "ClinicalDocument",
        "documentationOf",
        "serviceEvent",
        "performer",
        "assignedEntity",
        "representedOrganization",
    )
    if performer_org is not None:
        for org_id in children_by_local(performer_org, "id"):
            if attr_value(org_id, "root") == FACILITY_CODE_ROOT:
                info["facility_code"] = attr_value(org_id, "extension")
                break
        info["facility_name"] = first_child_attr(performer_org, "name")
    if not info["facility_code"] and event is not None:
        info["facility_code"] = first_child_attr(event, "id", "extension")

    info["insurer_number_raw"] = patient_role_id_extension(root, PATIENT_ID_ROOT_INSURER_NUMBER)
    info["insurance_symbol_raw"] = patient_role_id_extension(root, PATIENT_ID_ROOT_INSURANCE_SYMBOL)
    info["insurance_number_raw"] = patient_role_id_extension(root, PATIENT_ID_ROOT_INSURANCE_NUMBER)

    return info


def identity_error_message(bundle: Mapping[str, Any]) -> str:
    field_results = cast(Mapping[str, Any], bundle.get("field_results") or {})
    parts: list[str] = []
    for field_name, result in field_results.items():
        if isinstance(result, Mapping) and not result.get("ok"):
            reason = compact_text(result.get("reason")) or "UNKNOWN"
            parts.append(f"{field_name}=NG({reason})")
    if not parts:
        reason = compact_text(bundle.get("reason")) or "UNKNOWN"
        parts.append(f"identity=NG({reason})")
    return "identity generation failed: " + ", ".join(parts)


def find_child(elem: ElementTree.Element, child_name: str) -> ElementTree.Element | None:
    child_name_l = child_name.lower()
    for child in list(elem):
        if local_name(child.tag).lower() == child_name_l:
            return child
    return None


def is_valid_namecode(value: str | None) -> bool:
    return bool(value and NAMECODE_RE.match(value))


def find_code_element(elem: ElementTree.Element) -> ElementTree.Element | None:
    if local_name(elem.tag).lower() == "code":
        return elem
    return find_child(elem, "code")


def find_namecode(elem: ElementTree.Element) -> str | None:
    code_elem = find_code_element(elem)
    if code_elem is None:
        return None
    value = attr_value(code_elem, "code", "value")
    if is_valid_namecode(value):
        return value
    return None


def value_type(value_elem: ElementTree.Element) -> str | None:
    return (
        attr_value(value_elem, "type")
        or compact_text(value_elem.attrib.get(XSI_TYPE_ATTR))
        or attr_value(value_elem, "valueType")
    )


def extract_value_raw(value_elem: ElementTree.Element | None) -> str | None:
    if value_elem is None:
        return None
    return (
        attr_value(value_elem, "value")
        or attr_value(value_elem, "code")
        or elem_text(value_elem)
    )


def observation_text(elem: ElementTree.Element) -> ElementTree.Element | None:
    return find_child(elem, "text")


def has_entry_relationship(elem: ElementTree.Element) -> bool:
    return find_child(elem, "entryRelationship") is not None


def is_nullflavor_only_code(elem: ElementTree.Element | None) -> bool:
    if elem is None:
        return False
    if attr_value(elem, "code", "value"):
        return False
    return attr_value(elem, "nullFlavor") is not None


def is_grouping_observation(
    *,
    code_elem: ElementTree.Element | None,
    value_elem: ElementTree.Element | None,
    text_elem: ElementTree.Element | None,
    elem: ElementTree.Element,
) -> bool:
    code_missing_or_nullflavor = code_elem is None or is_nullflavor_only_code(code_elem)
    return (
        code_missing_or_nullflavor
        and value_elem is None
        and text_elem is None
        and has_entry_relationship(elem)
    )


@dataclass(frozen=True)
class SectionInfo:
    section_code: str | None
    section_code_system: str | None
    section_name: str | None


@dataclass(frozen=True)
class UnsupportedNamecode:
    code: str | None
    code_system: str | None


@dataclass(frozen=True)
class ExamExtraction:
    rows: list[dict[str, Any]]
    unsupported_namecodes: tuple[UnsupportedNamecode, ...]


def section_info(section: ElementTree.Element | None) -> SectionInfo:
    if section is None:
        return SectionInfo(None, None, None)
    code_elem = find_child(section, "code")
    section_name = attr_value(code_elem, "displayName") if code_elem is not None else None
    if section_name is None:
        section_name = elem_text(find_child(section, "title"))
    return SectionInfo(
        section_code=attr_value(code_elem, "code") if code_elem is not None else None,
        section_code_system=attr_value(code_elem, "codeSystem") if code_elem is not None else None,
        section_name=section_name,
    )


def iter_section_observations(section: ElementTree.Element) -> Iterable[ElementTree.Element]:
    for child in list(section):
        child_name = local_name(child.tag).lower()
        if child_name == "section":
            continue
        if child_name == "observation":
            yield child
        yield from iter_section_observations(child)


def collect_observation_candidates(
    root: ElementTree.Element,
) -> list[tuple[ElementTree.Element, SectionInfo]]:
    candidates: list[tuple[ElementTree.Element, SectionInfo]] = []
    seen_element_ids: set[int] = set()
    for section in (elem for elem in root.iter() if local_name(elem.tag).lower() == "section"):
        info = section_info(section)
        for elem in iter_section_observations(section):
            if id(elem) in seen_element_ids:
                continue
            seen_element_ids.add(id(elem))
            candidates.append((elem, info))

    null_section = SectionInfo(None, None, None)
    for elem in root.iter():
        if local_name(elem.tag).lower() != "observation" or id(elem) in seen_element_ids:
            continue
        seen_element_ids.add(id(elem))
        candidates.append((elem, null_section))
    return candidates


def extract_exam_items(root: ElementTree.Element) -> ExamExtraction:
    candidates = collect_observation_candidates(root)
    if not candidates:
        null_section = SectionInfo(None, None, None)
        candidates = [(elem, null_section) for elem in root.iter() if find_namecode(elem)]

    occurrence_by_namecode: dict[str, int] = {}
    unsupported_occurrence = 0
    rows: list[dict[str, Any]] = []
    unsupported_namecodes: list[UnsupportedNamecode] = []
    seen_element_ids: set[int] = set()

    for elem, info in candidates:
        if id(elem) in seen_element_ids:
            continue
        seen_element_ids.add(id(elem))
        namecode = find_namecode(elem)
        code_elem = find_code_element(elem)
        raw_code = attr_value(code_elem, "code", "value") if code_elem is not None else attr_value(elem, "code")
        raw_code_system = attr_value(code_elem, "codeSystem") if code_elem is not None else attr_value(elem, "codeSystem")
        raw_code_display = attr_value(code_elem, "displayName") if code_elem is not None else attr_value(elem, "displayName")
        value_elem = find_child(elem, "value")
        text_elem = observation_text(elem)
        if is_grouping_observation(code_elem=code_elem, value_elem=value_elem, text_elem=text_elem, elem=elem):
            continue
        raw_value = extract_value_raw(value_elem) or elem_text(text_elem)
        raw_value_type = value_type(value_elem) if value_elem is not None else "ST" if raw_value else None
        raw_unit = attr_value(value_elem, "unit") if value_elem is not None else None
        nullflavor = attr_value(value_elem, "nullFlavor") if value_elem is not None else attr_value(elem, "nullFlavor")
        value_code_system = attr_value(value_elem, "codeSystem") if value_elem is not None else None
        value_code = attr_value(value_elem, "code") if value_elem is not None else None
        value_display = attr_value(value_elem, "displayName") if value_elem is not None else None
        interpretation_elem = find_child(elem, "interpretationCode")
        interpretation_code = (
            attr_value(interpretation_elem, "code") if interpretation_elem is not None else None
        )
        interpretation_code_system = (
            attr_value(interpretation_elem, "codeSystem")
            if interpretation_elem is not None
            else None
        )
        interpretation_name = (
            attr_value(interpretation_elem, "displayName")
            if interpretation_elem is not None
            else None
        )
        negation_ind = parse_bool_int(attr_value(elem, "negationInd"))
        if not namecode:
            unsupported_occurrence += 1
            unsupported_namecodes.append(UnsupportedNamecode(code=raw_code, code_system=raw_code_system))
            rows.append(
                {
                    "section_code": info.section_code,
                    "section_code_system": info.section_code_system,
                    "section_name": info.section_name,
                    "namecode": None,
                    "occurrence_no": unsupported_occurrence,
                    "raw_value": raw_value,
                    "raw_value_type": raw_value_type,
                    "raw_unit": raw_unit,
                    "nullflavor": nullflavor,
                    "code_system": value_code_system,
                    "code_value": value_code,
                    "code_display": value_display,
                    "interpretation_code": interpretation_code,
                    "interpretation_code_system": interpretation_code_system,
                    "interpretation_name": interpretation_name,
                    "namecode_display_name": raw_code_display,
                    "negation_ind": negation_ind,
                    "identity_item_code": attr_value(elem, "identityItemCode", "identity_item_code"),
                    "jun_no": parse_int(attr_value(elem, "junNo", "jun_no")),
                }
            )
            continue

        occurrence_by_namecode[namecode] = occurrence_by_namecode.get(namecode, 0) + 1
        rows.append(
            {
                "section_code": info.section_code,
                "section_code_system": info.section_code_system,
                "section_name": info.section_name,
                "namecode": namecode,
                "occurrence_no": occurrence_by_namecode[namecode],
                "raw_value": raw_value,
                "raw_value_type": raw_value_type,
                "raw_unit": raw_unit,
                "nullflavor": nullflavor,
                "code_system": value_code_system,
                "code_value": value_code,
                "code_display": value_display,
                "interpretation_code": interpretation_code,
                "interpretation_code_system": interpretation_code_system,
                "interpretation_name": interpretation_name,
                "namecode_display_name": raw_code_display,
                "negation_ind": negation_ind,
                "identity_item_code": attr_value(elem, "identityItemCode", "identity_item_code"),
                "jun_no": parse_int(attr_value(elem, "junNo", "jun_no")),
            }
        )

    rows = supplement_missing_finding_presence_rows(rows)
    return ExamExtraction(rows=rows, unsupported_namecodes=tuple(unsupported_namecodes))


def supplement_missing_finding_presence_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    existing = {str(row.get("namecode")) for row in rows if row.get("namecode")}
    occurrence_by_namecode: dict[str, int] = {}
    for row in rows:
        namecode = row.get("namecode")
        if namecode:
            occurrence_by_namecode[str(namecode)] = max(
                occurrence_by_namecode.get(str(namecode), 0),
                int(row.get("occurrence_no") or 1),
            )

    supplemented = list(rows)
    for row in rows:
        finding_namecode = row.get("namecode")
        if not finding_namecode or str(finding_namecode) not in FINDING_PRESENCE_PAIRS:
            continue
        if not compact_text(row.get("raw_value")):
            continue
        presence_namecode, code_system, display_name = FINDING_PRESENCE_PAIRS[str(finding_namecode)]
        if presence_namecode in existing:
            continue
        occurrence_by_namecode[presence_namecode] = occurrence_by_namecode.get(presence_namecode, 0) + 1
        supplemented.append(
            {
                "section_code": row.get("section_code"),
                "section_code_system": row.get("section_code_system"),
                "section_name": row.get("section_name"),
                "namecode": presence_namecode,
                "occurrence_no": occurrence_by_namecode[presence_namecode],
                "raw_value": "所見あり",
                "raw_value_type": "CD",
                "raw_unit": None,
                "nullflavor": None,
                "code_system": code_system,
                "code_value": "1",
                "code_display": "所見あり",
                "interpretation_code": None,
                "interpretation_code_system": None,
                "interpretation_name": None,
                "namecode_display_name": display_name,
                "negation_ind": 0,
                "identity_item_code": presence_namecode[:5],
                "jun_no": None,
            }
        )
        existing.add(presence_namecode)
    return supplemented


def normalize_xml_exam_item_rows(
    cur: Any,
    config: ImportConfig,
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    item_by_namecode = get_exam_items(
        cur,
        [cast(str | None, row.get("namecode")) for row in rows],
        dev_db=config.dev_db,
    )
    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        normalized_row = dict(row)
        namecode = compact_text(row.get("namecode"))
        raw_value = row.get("raw_value")
        raw_unit = cast(str | None, row.get("raw_unit"))
        item = item_by_namecode.get(namecode or "")
        data_type = compact_text(item.get("data_type")) if item else compact_text(row.get("raw_value_type"))
        result_code_oid = compact_text(item.get("result_code_oid")) if item else None
        code_system = compact_text(row.get("code_system"))
        code_value = compact_text(row.get("code_value"))
        code_display = compact_text(row.get("code_display"))

        if namecode and item is None:
            normalized_row.update(
                {
                    "normalize_status": "ERROR",
                    "normalize_reason": "EXAM_ITEM_MASTER_NOT_FOUND",
                    "validation_status": "INVALID",
                    "validation_reason": "EXAM_ITEM_MASTER_NOT_FOUND",
                }
            )
            normalized_rows.append(normalized_row)
            continue

        if namecode and data_type in CODE_DATA_TYPES and code_value:
            if result_code_oid and code_system and code_system != result_code_oid:
                normalized_row.update(
                    {
                        "normalize_status": "ERROR",
                        "normalize_reason": "RESULT_CODE_OID_MISMATCH",
                        "validation_status": "INVALID",
                        "validation_reason": "RESULT_CODE_OID_MISMATCH",
                    }
                )
            elif result_code_oid and not code_system:
                normalized_row.update(
                    {
                        "normalize_status": "ERROR",
                        "normalize_reason": "RESULT_CODE_OID_MISSING_IN_XML",
                        "validation_status": "INVALID",
                        "validation_reason": "RESULT_CODE_OID_MISSING_IN_XML",
                    }
                )
            else:
                normalized_row.update(
                    {
                        "raw_value_type": data_type,
                        "code_system": code_system or result_code_oid,
                        "code_value": code_value,
                        "code_display": code_display,
                        "normalize_status": "OK",
                        "normalize_reason": "XML_CODE_MATCH",
                        "validation_status": "VALID",
                        "validation_reason": None,
                    }
                )
            normalized_rows.append(normalized_row)
            continue

        value_for_normalize = raw_value if compact_text(raw_value) else code_display
        if namecode:
            normalized = normalize_exam_item_value(
                cur,
                namecode=namecode,
                raw_value=value_for_normalize,
                raw_unit=raw_unit,
                exam_item=item,
                dev_db=config.dev_db,
                master_db=config.master_db,
            )
            normalized_row.update(normalized.as_exam_item_value_columns())
        else:
            normalized_row.update(
                {
                    "normalize_status": "ERROR",
                    "normalize_reason": "NAMECODE_NOT_FOUND",
                    "validation_status": "INVALID",
                    "validation_reason": "NAMECODE_NOT_FOUND",
                }
            )
        normalized_rows.append(normalized_row)
    return normalized_rows


def parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def parse_bool_int(value: str | None) -> int | None:
    if value is None:
        return None
    value_l = value.strip().lower()
    if value_l in {"true", "1"}:
        return 1
    if value_l in {"false", "0"}:
        return 0
    return None


def is_target_inner_xml(name: str, zip_config: ZipConfig) -> bool:
    leaf = Path(name).name
    leaf_l = leaf.lower()
    if not leaf_l.endswith(".xml"):
        return False
    if not fnmatch.fnmatch(leaf_l, zip_config.target_xml_pattern.lower()):
        return False
    if any(leaf_l.startswith(prefix) for prefix in zip_config.exclude_prefixes):
        return False
    if any(keyword in leaf_l for keyword in zip_config.exclude_keywords):
        return False
    return True


def fetch_file_receipts(cur: Any, config: ImportConfig) -> list[dict[str, Any]]:
    status_placeholders = ", ".join(["%s"] * len(config.input.file_statuses))
    type_placeholders = ", ".join(["%s"] * len(config.input.file_types))
    params: list[Any] = [config.event_id, *config.input.file_statuses, *config.input.file_types]
    etl_filter = ""
    if config.input.etl_run_id is not None:
        etl_filter = "AND etl_run_id = %s"
        params.append(config.input.etl_run_id)
    limit_sql = ""
    if config.limit:
        limit_sql = "LIMIT %s"
        params.append(config.limit)

    cur.execute(
        f"""
        SELECT *
        FROM {qname(config.health_db)}.file_receipts
        WHERE event_id = %s
          AND status IN ({status_placeholders})
          AND file_type IN ({type_placeholders})
          {etl_filter}
        ORDER BY id
        {limit_sql}
        """,
        tuple(params),
    )
    return list(cur.fetchall())


def start_import_run(cur: Any, config: ImportConfig) -> int:
    return etl_start_run(
        cur,
        phase=ETL_PHASE,
        source=ETL_SOURCE,
        db_schema=config.health_db,
        db_path=config.health_db,
        input_base=f"event_id={config.event_id}",
        input_file=None,
        insurer_number=None,
        dry_run=config.dry_run,
        limit_rows=config.limit or None,
    )


def finish_import_run(cur: Any, run_id: int, summary: ImportSummary) -> None:
    etl_finish_run(cur, run_id, summary.to_metrics(), extra_notes=summary.to_message())


def record_import_error(
    cur: Any | None,
    *,
    run_id: int | None,
    summary: ImportSummary,
    field: str,
    error_code: str,
    message: str,
    src_file: str | None = None,
    field_value: str | None = None,
    person_id_custom: str | None = None,
) -> None:
    summary.errors += 1
    if field == ERROR_FIELD_IDENTITY:
        summary.identity_errors += 1
    elif error_code == "SUBSCRIBER_NOT_FOUND":
        summary.subscriber_not_found += 1
    elif error_code == "ZIP_NO_TARGET_XML":
        summary.zip_no_target_xml += 1

    if cur is None or run_id is None:
        return
    etl_log_error(
        cur,
        run_id,
        phase=ETL_PHASE,
        source=ETL_SOURCE,
        insurer_number=None,
        src_file=(src_file or "")[:190] or None,
        row_no=None,
        line_no=None,
        field=field,
        field_value=field_value,
        error_code=error_code,
        message=message,
        person_id_custom=person_id_custom,
    )


def update_file_importing(cur: Any, config: ImportConfig, file_receipt_id: int) -> None:
    cur.execute(
        f"""
        UPDATE {qname(config.health_db)}.file_receipts
        SET status = %s
        WHERE id = %s
        """,
        (FILE_STATUS_IMPORTING, file_receipt_id),
    )


def update_file_status(
    cur: Any,
    config: ImportConfig,
    *,
    file_receipt_id: int,
    status: str,
    processable_count: int,
    message: str,
) -> None:
    cur.execute(
        f"""
        UPDATE {qname(config.health_db)}.file_receipts
        SET
            status = %s,
            processable_count = %s,
            content_checked_at = CURRENT_TIMESTAMP(3),
            processed_at = CASE WHEN %s IN ('IMPORTED', 'WARNING', 'ERROR') THEN CURRENT_TIMESTAMP(3) ELSE processed_at END,
            summary_message = %s
        WHERE id = %s
        """,
        (status, processable_count, status, message, file_receipt_id),
    )


def get_xml_ledger(cur: Any, config: ImportConfig, xml_sha256: str) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT *
        FROM {qname(config.health_db)}.exam_ledgers
        WHERE xml_sha256 = %s
          AND source_type = 'XML'
        """,
        (xml_sha256,),
    )
    return cur.fetchone()


def xml_basic_to_completion_row(basic: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "insurer_number": basic.get("insurer_number_raw"),
        "insurance_symbol_raw": basic.get("insurance_symbol_raw"),
        "insurance_number_raw": basic.get("insurance_number_raw"),
        "name_kana_raw": basic.get("name_kana_full_raw"),
        "postal_code": basic.get("postal_code"),
        "address": basic.get("address"),
    }


def insert_xml_ledger(
    cur: Any,
    config: ImportConfig,
    *,
    xml_sha256: str,
    xml_file_name: str | None,
    xml_status: str,
    xml_reason: str | None,
    file_receipt_id: int,
    run_id: int | None,
    basic: Mapping[str, Any],
    identity_bundle: Mapping[str, Any] | None,
    subscriber: Mapping[str, Any],
    basic_info: Mapping[str, Any] | None = None,
    basic_identity_export: Mapping[str, Any] | None = None,
) -> tuple[int, bool]:
    person_id_custom = None
    identity_hash = None
    if identity_bundle and identity_bundle.get("ok"):
        person_id_custom = identity_bundle.get("person_id_custom")
        identity_hash = identity_bundle.get("identity_hash")
    basic_info = basic_info or {}
    basic_identity_export = basic_identity_export or {}

    try:
        cur.execute(
            f"""
            INSERT INTO {qname(config.health_db)}.exam_ledgers (
                event_id, source_type, file_receipt_id, source_etl_run_id,
                subscriber_id, hia_subscriber_id,
                xml_sha256, xml_file_name, document_id,
                insurer_number, facility_code, facility_name, exam_date,
                name_kana_raw,
                name_kana_export_value, name_kana_export_source, name_kana_export_reason,
                insurance_symbol_raw, insurance_number_raw,
                insurance_symbol_export_value, insurance_symbol_export_source, insurance_symbol_export_reason,
                insurance_number_export_value, insurance_number_export_source, insurance_number_export_reason,
                birthdate, gender_code,
                report_category_code, program_type_code,
                postal_code, address,
                basic_info_status, basic_info_reason,
                insurer_number_source, insurer_number_completion_status,
                insurer_number_completion_reason, insurer_number_export_value,
                address_source,
                address_completion_status, address_completion_reason,
                address_completed_value, postal_code_completed_value,
                identity_hash, person_id_custom,
                subscriber_match_status, subscriber_match_method, subscriber_match_reason,
                xml_status, xml_reason,
                source_created_at, source_updated_at
            )
            VALUES (
                %s, 'XML', %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s, %s,
                %s,
                %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3)
            )
            """,
            (
                config.event_id,
                file_receipt_id,
                run_id,
                subscriber.get("subscriber_id"),
                subscriber.get("hia_subscriber_id"),
                xml_sha256,
                xml_file_name,
                basic.get("document_id"),
                basic.get("insurer_number_raw"),
                basic.get("facility_code"),
                basic.get("facility_name"),
                basic.get("exam_date"),
                basic.get("name_kana_full_raw"),
                basic_identity_export.get("name_kana_export_value"),
                basic_identity_export.get("name_kana_export_source"),
                basic_identity_export.get("name_kana_export_reason"),
                basic.get("insurance_symbol_raw"),
                basic.get("insurance_number_raw"),
                basic_identity_export.get("insurance_symbol_export_value"),
                basic_identity_export.get("insurance_symbol_export_source"),
                basic_identity_export.get("insurance_symbol_export_reason"),
                basic_identity_export.get("insurance_number_export_value"),
                basic_identity_export.get("insurance_number_export_source"),
                basic_identity_export.get("insurance_number_export_reason"),
                parse_mysql_date(cast(str | None, basic.get("birthdate"))),
                basic.get("gender_code"),
                basic.get("report_category_code"),
                basic.get("program_type_code"),
                basic.get("postal_code"),
                basic.get("address"),
                basic_info.get("basic_info_status"),
                basic_info.get("basic_info_reason"),
                basic_info.get("insurer_number_source"),
                basic_info.get("insurer_number_completion_status"),
                basic_info.get("insurer_number_completion_reason"),
                basic_info.get("insurer_number_export_value"),
                basic_info.get("address_source"),
                basic_info.get("address_completion_status"),
                basic_info.get("address_completion_reason"),
                basic_info.get("address_completed_value"),
                basic_info.get("postal_code_completed_value"),
                identity_hash,
                person_id_custom,
                subscriber.get("subscriber_match_status"),
                subscriber.get("subscriber_match_method"),
                subscriber.get("subscriber_match_reason"),
                xml_status,
                xml_reason,
            ),
        )
    except IntegrityError as exc:
        if exc.errno == errorcode.ER_DUP_ENTRY:
            existing = get_xml_ledger(cur, config, xml_sha256)
            if existing:
                update_xml_ledger_report_codes(
                    cur,
                    config,
                    ledger_id=int(existing["exam_ledger_id"]),
                    file_receipt_id=file_receipt_id,
                    run_id=run_id,
                    report_category_code=cast(str | None, basic.get("report_category_code")),
                    program_type_code=cast(str | None, basic.get("program_type_code")),
                    basic=basic,
                    basic_info=basic_info,
                    basic_identity_export=basic_identity_export,
                    identity_bundle=identity_bundle,
                    subscriber=subscriber,
                )
                return int(existing["exam_ledger_id"]), False
        raise
    ledger_id = cur.lastrowid
    if ledger_id is None:
        raise RuntimeError("failed to get inserted exam_ledgers.exam_ledger_id")
    return int(ledger_id), True


def update_xml_ledger_report_codes(
    cur: Any,
    config: ImportConfig,
    *,
    ledger_id: int,
    file_receipt_id: int,
    run_id: int | None,
    report_category_code: str | None,
    program_type_code: str | None,
    basic: Mapping[str, Any],
    basic_info: Mapping[str, Any],
    basic_identity_export: Mapping[str, Any],
    identity_bundle: Mapping[str, Any] | None,
    subscriber: Mapping[str, Any],
) -> None:
    person_id_custom = None
    identity_hash = None
    if identity_bundle and identity_bundle.get("ok"):
        person_id_custom = identity_bundle.get("person_id_custom")
        identity_hash = identity_bundle.get("identity_hash")
    cur.execute(
        f"""
        UPDATE {qname(config.health_db)}.exam_ledgers
        SET file_receipt_id = COALESCE(%s, file_receipt_id),
            source_etl_run_id = COALESCE(%s, source_etl_run_id),
            report_category_code = COALESCE(%s, report_category_code),
            program_type_code = COALESCE(%s, program_type_code),
            insurer_number = COALESCE(%s, insurer_number),
            facility_code = COALESCE(%s, facility_code),
            facility_name = COALESCE(%s, facility_name),
            exam_date = COALESCE(%s, exam_date),
            name_kana_raw = COALESCE(%s, name_kana_raw),
            name_kana_export_value = %s,
            name_kana_export_source = %s,
            name_kana_export_reason = %s,
            insurance_symbol_raw = COALESCE(%s, insurance_symbol_raw),
            insurance_symbol_export_value = %s,
            insurance_symbol_export_source = %s,
            insurance_symbol_export_reason = %s,
            insurance_number_raw = COALESCE(%s, insurance_number_raw),
            insurance_number_export_value = %s,
            insurance_number_export_source = %s,
            insurance_number_export_reason = %s,
            birthdate = COALESCE(%s, birthdate),
            gender_code = COALESCE(%s, gender_code),
            postal_code = COALESCE(%s, postal_code),
            address = COALESCE(%s, address),
            basic_info_status = %s,
            basic_info_reason = %s,
            insurer_number_source = %s,
            insurer_number_completion_status = %s,
            insurer_number_completion_reason = %s,
            insurer_number_export_value = %s,
            address_source = %s,
            address_completion_status = %s,
            address_completion_reason = %s,
            address_completed_value = %s,
            postal_code_completed_value = %s,
            identity_hash = COALESCE(%s, identity_hash),
            person_id_custom = COALESCE(%s, person_id_custom),
            subscriber_id = COALESCE(%s, subscriber_id),
            hia_subscriber_id = COALESCE(%s, hia_subscriber_id),
            subscriber_match_status = COALESCE(%s, subscriber_match_status),
            subscriber_match_method = COALESCE(%s, subscriber_match_method),
            subscriber_match_reason = COALESCE(%s, subscriber_match_reason)
        WHERE exam_ledger_id = %s
        """,
        (
            file_receipt_id,
            run_id,
            report_category_code,
            program_type_code,
            basic.get("insurer_number_raw"),
            basic.get("facility_code"),
            basic.get("facility_name"),
            basic.get("exam_date"),
            basic.get("name_kana_full_raw"),
            basic_identity_export.get("name_kana_export_value"),
            basic_identity_export.get("name_kana_export_source"),
            basic_identity_export.get("name_kana_export_reason"),
            basic.get("insurance_symbol_raw"),
            basic_identity_export.get("insurance_symbol_export_value"),
            basic_identity_export.get("insurance_symbol_export_source"),
            basic_identity_export.get("insurance_symbol_export_reason"),
            basic.get("insurance_number_raw"),
            basic_identity_export.get("insurance_number_export_value"),
            basic_identity_export.get("insurance_number_export_source"),
            basic_identity_export.get("insurance_number_export_reason"),
            parse_mysql_date(cast(str | None, basic.get("birthdate"))),
            basic.get("gender_code"),
            basic.get("postal_code"),
            basic.get("address"),
            basic_info.get("basic_info_status"),
            basic_info.get("basic_info_reason"),
            basic_info.get("insurer_number_source"),
            basic_info.get("insurer_number_completion_status"),
            basic_info.get("insurer_number_completion_reason"),
            basic_info.get("insurer_number_export_value"),
            basic_info.get("address_source"),
            basic_info.get("address_completion_status"),
            basic_info.get("address_completion_reason"),
            basic_info.get("address_completed_value"),
            basic_info.get("postal_code_completed_value"),
            identity_hash,
            person_id_custom,
            subscriber.get("subscriber_id"),
            subscriber.get("hia_subscriber_id"),
            subscriber.get("subscriber_match_status"),
            subscriber.get("subscriber_match_method"),
            subscriber.get("subscriber_match_reason"),
            ledger_id,
        ),
    )


def insert_xml_file_link(
    cur: Any,
    config: ImportConfig,
    *,
    file_receipt_id: int,
    exam_ledger_id: int,
    xml_inner_path: str | None,
) -> str:
    _ = (cur, config, file_receipt_id, exam_ledger_id, xml_inner_path)
    return "skipped"


def insert_exam_item_values(
    cur: Any,
    config: ImportConfig,
    *,
    ledger_id: int,
    subscriber_id: int | None,
    hia_subscriber_id: str | None,
    run_id: int | None,
    rows: Sequence[Mapping[str, Any]],
) -> int:
    if not rows:
        return 0

    params = [
        (
            config.event_id,
            LEDGER_TYPE_EXAM,
            ledger_id,
            subscriber_id,
            hia_subscriber_id,
            row.get("namecode"),
            row.get("section_code"),
            row.get("section_code_system"),
            row.get("section_name"),
            row.get("occurrence_no"),
            row.get("raw_value"),
            row.get("raw_value_type"),
            row.get("raw_unit"),
            row.get("normalized_value"),
            row.get("normalized_unit"),
            row.get("nullflavor"),
            row.get("code_system"),
            row.get("code_value"),
            row.get("code_display"),
            row.get("interpretation_code"),
            row.get("interpretation_code_system"),
            row.get("interpretation_name"),
            row.get("namecode_display_name"),
            row.get("negation_ind"),
            row.get("identity_item_code"),
            row.get("jun_no"),
            row.get("normalize_status"),
            row.get("normalize_reason"),
            row.get("validation_status"),
            row.get("validation_reason"),
            run_id,
        )
        for row in rows
    ]
    cur.executemany(
        f"""
        INSERT INTO {qname(config.health_db)}.exam_item_values (
            event_id, ledger_type, ledger_id,
            subscriber_id, hia_subscriber_id,
            namecode, section_code, section_code_system, section_name,
            occurrence_no,
            raw_value, raw_value_type, raw_unit,
            normalized_value, normalized_unit,
            nullflavor, code_system, code_value, code_display,
            interpretation_code, interpretation_code_system, interpretation_name,
            namecode_display_name, negation_ind,
            identity_item_code, jun_no,
            normalize_status, normalize_reason,
            validation_status, validation_reason,
            extracted_run_id, extracted_at, normalized_at
        )
        VALUES (
            %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            %s,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3)
        )
        """,
        params,
    )
    return len(rows)


def subscriber_status_from_identity(
    dev_cur: Any,
    identity_bundle: Mapping[str, Any],
) -> dict[str, Any]:
    if not identity_bundle.get("ok"):
        return {
            "subscriber_id": None,
            "hia_subscriber_id": None,
            "subscriber_match_status": SUBSCRIBER_MATCH_IDENTITY_ERROR,
            "subscriber_match_method": None,
            "subscriber_match_reason": compact_text(identity_bundle.get("reason")),
        }

    result = resolve_subscriber_identity(
        dev_cur,
        identity_hash=cast(str | None, identity_bundle.get("identity_hash")),
        person_id_custom=cast(str | None, identity_bundle.get("person_id_custom")),
    )
    if result.status == "matched":
        row = result.rows[0]
        if result.matched_by == "person_id_custom":
            return {
                "subscriber_id": row.get("subscriber_id"),
                "hia_subscriber_id": row.get("hia_subscriber_id"),
                "subscriber_match_status": SUBSCRIBER_MATCH_CANDIDATE,
                "subscriber_match_method": SUBSCRIBER_METHOD_PERSON_ID_CUSTOM,
                "subscriber_match_reason": SUBSCRIBER_REASON_PERSON_ID_CUSTOM_CANDIDATE,
            }
        return {
            "subscriber_id": row.get("subscriber_id"),
            "hia_subscriber_id": row.get("hia_subscriber_id"),
            "subscriber_match_status": SUBSCRIBER_MATCH_MATCHED,
            "subscriber_match_method": result.matched_by,
            "subscriber_match_reason": None,
        }
    if result.status == "not_found":
        return {
            "subscriber_id": None,
            "hia_subscriber_id": None,
            "subscriber_match_status": SUBSCRIBER_MATCH_NOT_FOUND,
            "subscriber_match_method": None,
            "subscriber_match_reason": "subscriber not found",
        }
    return {
        "subscriber_id": None,
        "hia_subscriber_id": None,
        "subscriber_match_status": SUBSCRIBER_MATCH_NOT_FOUND,
        "subscriber_match_method": result.matched_by,
        "subscriber_match_reason": f"{result.status}: candidates={result.candidate_count}",
    }


def facility_folder_name(file_receipt: Mapping[str, Any]) -> str | None:
    relative_path = compact_text(file_receipt.get("relative_path"))
    if not relative_path:
        return None
    parts = [part for part in re.split(r"[\\/]+", relative_path) if part]
    return parts[0] if parts else None


def resolve_zip_password(cur: Any, config: ImportConfig, file_receipt: Mapping[str, Any]) -> bytes | None:
    facility_codes = [
        compact_text(file_receipt.get("facility_code")),
        compact_text(file_receipt.get("submitter_facility_code")),
    ]
    facility_codes = [code for code in facility_codes if code]
    facility_placeholders = ", ".join(["%s"] * len(facility_codes)) or "NULL"
    params: list[Any] = [
        compact_text(file_receipt.get("file_sha256")),
        compact_text(file_receipt.get("file_name")) or Path(str(file_receipt.get("source_path") or "")).name,
        *facility_codes,
        facility_folder_name(file_receipt),
    ]
    cur.execute(
        f"""
        SELECT password_text
        FROM {qname(config.work_db)}.medi_zip_passwords
        WHERE is_active = 1
          AND (
            (scope_type = 'ZIP_SHA256' AND zip_sha256 = %s)
            OR (scope_type = 'ZIP_NAME' AND zip_name = %s)
            OR (
              scope_type = 'FACILITY'
              AND (
                facility_code IN ({facility_placeholders})
                OR facility_folder_name = %s
              )
            )
          )
        ORDER BY
          CASE scope_type
            WHEN 'ZIP_SHA256' THEN 1
            WHEN 'ZIP_NAME' THEN 2
            WHEN 'FACILITY' THEN 3
            ELSE 9
          END,
          priority,
          zip_password_id
        LIMIT 1
        """,
        tuple(params),
    )
    row = cur.fetchone()
    if not row:
        return None
    password = compact_text(row.get("password_text") if isinstance(row, Mapping) else row[0])
    return password.encode("utf-8") if password is not None else None


def is_encrypted_zip_info(info: zipfile.ZipInfo) -> bool:
    return bool(info.flag_bits & 0x1)


def read_zip_member(zf: zipfile.ZipFile, info: zipfile.ZipInfo, password: bytes | None) -> bytes:
    try:
        return zf.read(info, pwd=password)
    except RuntimeError as exc:
        message = str(exc).lower()
        if "password" in message or "encrypted" in message or "decrypt" in message:
            raise ZipDecryptError(type(exc).__name__) from exc
        raise


def read_candidates_from_file(
    cur: Any,
    file_receipt: Mapping[str, Any],
    config: ImportConfig,
    summary: ImportSummary,
) -> tuple[list[XmlCandidate], int]:
    source_path = Path(str(file_receipt.get("source_path") or ""))
    file_type = str(file_receipt.get("file_type") or "").upper()
    if file_type == FILE_TYPE_XML:
        data = source_path.read_bytes()
        return [XmlCandidate(file_receipt=file_receipt, inner_path=None, data=data)], 0

    excluded = 0
    candidates: list[XmlCandidate] = []
    with zipfile.ZipFile(source_path) as zf:
        target_infos: list[zipfile.ZipInfo] = []
        for info in zf.infolist():
            if info.is_dir():
                continue
            if is_target_inner_xml(info.filename, config.zip):
                target_infos.append(info)
            elif info.filename.lower().endswith(".xml"):
                excluded += 1
        password: bytes | None = None
        if any(is_encrypted_zip_info(info) for info in target_infos):
            password = resolve_zip_password(cur, config, file_receipt)
            if password is None:
                raise ZipPasswordNotFoundError
        for info in target_infos:
            candidates.append(
                XmlCandidate(
                    file_receipt=file_receipt,
                    inner_path=info.filename,
                    data=read_zip_member(zf, info, password if is_encrypted_zip_info(info) else None),
                )
            )
    summary.xml_excluded += excluded
    return candidates, excluded


def process_xml_candidate(
    health_cur: Any,
    dev_cur: Any,
    config: ImportConfig,
    summary: ImportSummary,
    *,
    run_id: int | None,
    candidate: XmlCandidate,
) -> XmlProcessResult:
    file_receipt = candidate.file_receipt
    file_receipt_id = int(cast(Any, file_receipt.get("id")))
    xml_sha256 = sha256_bytes(candidate.data)
    existing = get_xml_ledger(health_cur, config, xml_sha256)

    try:
        root = ElementTree.fromstring(candidate.data)
    except ElementTree.ParseError as exc:
        message = (
            f"xml parse failed: path={file_receipt.get('source_path')}, "
            f"inner_path={candidate.inner_path}, reason={compact_text(exc) or type(exc).__name__}"
        )
        if not config.dry_run:
            try:
                ledger_id, ledger_inserted = insert_xml_ledger(
                    health_cur,
                    config,
                    xml_sha256=xml_sha256,
                    xml_file_name=candidate.xml_file_name,
                    xml_status=XML_STATUS_PARSE_ERROR,
                    xml_reason=message,
                    file_receipt_id=file_receipt_id,
                    run_id=run_id,
                    basic={},
                    identity_bundle=None,
                    subscriber={"subscriber_match_status": SUBSCRIBER_MATCH_NOT_EXECUTED},
                )
            except Exception as exc:
                raise ImportDbError(
                    "DB_EXAM_LEDGER_SAVE_FAILED",
                    f"exam_ledger save failed: path={candidate.display_path}, reason={compact_text(exc) or type(exc).__name__}",
                    candidate.inner_path,
                ) from exc
            try:
                link_result = insert_xml_file_link(
                    health_cur,
                    config,
                    file_receipt_id=file_receipt_id,
                    exam_ledger_id=ledger_id,
                    xml_inner_path=candidate.inner_path,
                )
            except Exception as exc:
                raise ImportDbError(
                    "DB_XML_FILE_LINK_SAVE_FAILED",
                    f"xml_file_links save failed: path={candidate.display_path}, reason={compact_text(exc) or type(exc).__name__}",
                    candidate.inner_path,
                ) from exc
        else:
            link_result = "inserted"
            ledger_inserted = True
        record_import_error(
            health_cur if not config.dry_run else None,
            run_id=run_id,
            summary=summary,
            field=ERROR_FIELD_XML,
            error_code="XML_PARSE_FAILED",
            message=message,
            src_file=candidate.display_path,
            field_value=candidate.inner_path or str(file_receipt.get("source_path") or ""),
        )
        return XmlProcessResult(
            ok=False,
            new_ledger=ledger_inserted,
            existing_ledger=not ledger_inserted,
            link_inserted=link_result == "inserted",
            link_duplicate=link_result == "duplicate",
            parse_error=True,
            error_count=1,
        )

    try:
        basic = extract_basic_info(root)
        exam_extraction = extract_exam_items(root)
    except Exception as exc:
        message = (
            f"xml raw extract failed: path={file_receipt.get('source_path')}, "
            f"inner_path={candidate.inner_path}, field=XML, reason={compact_text(exc) or type(exc).__name__}"
        )
        if not config.dry_run:
            try:
                ledger_id, ledger_inserted = insert_xml_ledger(
                    health_cur,
                    config,
                    xml_sha256=xml_sha256,
                    xml_file_name=candidate.xml_file_name,
                    xml_status=XML_STATUS_READY,
                    xml_reason="XML_RAW_EXTRACT_FAILED",
                    file_receipt_id=file_receipt_id,
                    run_id=run_id,
                    basic={},
                    identity_bundle=None,
                    subscriber={"subscriber_match_status": SUBSCRIBER_MATCH_NOT_EXECUTED},
                )
            except Exception as db_exc:
                raise ImportDbError(
                    "DB_EXAM_LEDGER_SAVE_FAILED",
                    f"exam_ledger save failed after raw extract error: path={candidate.display_path}, reason={compact_text(db_exc) or type(db_exc).__name__}",
                    candidate.inner_path,
                ) from db_exc
            try:
                link_result = insert_xml_file_link(
                    health_cur,
                    config,
                    file_receipt_id=file_receipt_id,
                    exam_ledger_id=ledger_id,
                    xml_inner_path=candidate.inner_path,
                )
            except Exception as db_exc:
                raise ImportDbError(
                    "DB_XML_FILE_LINK_SAVE_FAILED",
                    f"xml_file_links save failed after raw extract error: path={candidate.display_path}, reason={compact_text(db_exc) or type(db_exc).__name__}",
                    candidate.inner_path,
                ) from db_exc
        else:
            ledger_inserted = True
            link_result = "inserted"
        record_import_error(
            health_cur if not config.dry_run else None,
            run_id=run_id,
            summary=summary,
            field=ERROR_FIELD_XML,
            error_code="XML_RAW_EXTRACT_FAILED",
            message=message,
            src_file=candidate.display_path,
            field_value="XML",
        )
        return XmlProcessResult(
            ok=False,
            new_ledger=ledger_inserted,
            existing_ledger=not ledger_inserted,
            link_inserted=link_result == "inserted",
            link_duplicate=link_result == "duplicate",
            error_count=1,
        )

    exam_items = normalize_xml_exam_item_rows(health_cur, config, exam_extraction.rows)
    local_error_count = 0
    if exam_extraction.unsupported_namecodes:
        samples = [
            f"code={item.code}, codeSystem={item.code_system}"
            for item in exam_extraction.unsupported_namecodes[:5]
        ]
        sample_message = "; ".join(samples)
        message = (
            f"xml unsupported namecode: path={file_receipt.get('source_path')}, "
            f"inner_path={candidate.inner_path}, {sample_message}"
        )
        record_import_error(
            health_cur if not config.dry_run else None,
            run_id=run_id,
            summary=summary,
            field=ERROR_FIELD_XML,
            error_code="XML_UNSUPPORTED_NAMECODE",
            message=message,
            src_file=candidate.display_path,
            field_value=sample_message,
        )
        local_error_count += 1

    event_insurer_number = get_event_insurer_number(
        dev_cur,
        event_id=config.event_id,
        dev_db=config.dev_db,
    )
    completion_row = xml_basic_to_completion_row(basic)
    basic_info = resolve_basic_info_completion(
        health_cur,
        row=completion_row,
        event_insurer_number=event_insurer_number,
        master_db=config.master_db,
    )
    basic_info_params = basic_info.as_db_params()
    basic_for_ledger = dict(basic)
    if basic_info_params.get("insurer_number_export_value"):
        basic_for_ledger["insurer_number_raw"] = basic_info_params["insurer_number_export_value"]

    identity_raw = {
        "birthdate": basic_for_ledger.get("birthdate"),
        "insurer_number_raw": basic_for_ledger.get("insurer_number_raw"),
        "insurance_symbol_raw": basic_for_ledger.get("insurance_symbol_raw"),
        "insurance_number_raw": basic_for_ledger.get("insurance_number_raw"),
        "name_kana_full_raw": basic_for_ledger.get("name_kana_full_raw"),
        "gender_code": basic_for_ledger.get("gender_code"),
    }
    identity_bundle = cast(Mapping[str, Any], generate_identity_bundle(**identity_raw))
    if not identity_bundle.get("ok"):
        message = identity_error_message(identity_bundle)
        record_import_error(
            health_cur if not config.dry_run else None,
            run_id=run_id,
            summary=summary,
            field=ERROR_FIELD_IDENTITY,
            error_code="IDENTITY_GENERATION_FAILED",
            message=message,
            src_file=candidate.display_path,
            field_value="IDENTITY",
            person_id_custom=cast(str | None, identity_bundle.get("person_id_custom")),
        )
        local_error_count += 1

    try:
        subscriber = subscriber_status_from_identity(dev_cur, identity_bundle)
    except Exception as exc:
        message = f"subscriber lookup failed: path={candidate.display_path}, reason={compact_text(exc) or type(exc).__name__}"
        record_import_error(
            health_cur if not config.dry_run else None,
            run_id=run_id,
            summary=summary,
            field=ERROR_FIELD_SUBSCRIBER,
            error_code="SUBSCRIBER_LOOKUP_FAILED",
            message=message,
            src_file=candidate.display_path,
            field_value=cast(str | None, identity_bundle.get("identity_hash")),
            person_id_custom=cast(str | None, identity_bundle.get("person_id_custom")),
        )
        subscriber = {
            "subscriber_id": None,
            "hia_subscriber_id": None,
            "subscriber_match_status": SUBSCRIBER_MATCH_NOT_FOUND,
            "subscriber_match_method": None,
            "subscriber_match_reason": compact_text(exc) or type(exc).__name__,
        }
        local_error_count += 1

    if subscriber.get("subscriber_match_status") == SUBSCRIBER_MATCH_NOT_FOUND:
        message = f"subscriber not found: path={candidate.display_path}, method={subscriber.get('subscriber_match_method')}"
        record_import_error(
            health_cur if not config.dry_run else None,
            run_id=run_id,
            summary=summary,
            field=ERROR_FIELD_SUBSCRIBER,
            error_code="SUBSCRIBER_NOT_FOUND",
            message=message,
            src_file=candidate.display_path,
            field_value=cast(str | None, identity_bundle.get("identity_hash")),
            person_id_custom=cast(str | None, identity_bundle.get("person_id_custom")),
        )
        local_error_count += 1

    projection = None
    if subscriber.get("subscriber_match_status") == SUBSCRIBER_MATCH_MATCHED:
        projection = load_subscriber_basic_export_projection_by_id(
            dev_cur,
            subscriber_id=cast(int | None, subscriber.get("subscriber_id")),
            dev_db=config.dev_db,
        )
    basic_identity_export = resolve_basic_identity_export_values(
        xml_basic_to_completion_row(basic_for_ledger),
        subscriber=projection,
    ).as_db_params()

    if config.dry_run:
        return XmlProcessResult(
            ok=True,
            new_ledger=existing is None,
            existing_ledger=existing is not None,
            link_inserted=True,
            exam_item_count=len(exam_items),
            error_count=local_error_count,
        )

    try:
        if existing:
            ledger_id = int(existing["exam_ledger_id"])
            update_xml_ledger_report_codes(
                health_cur,
                config,
                ledger_id=ledger_id,
                file_receipt_id=file_receipt_id,
                run_id=run_id,
                report_category_code=cast(str | None, basic_for_ledger.get("report_category_code")),
                program_type_code=cast(str | None, basic_for_ledger.get("program_type_code")),
                basic=basic_for_ledger,
                basic_info=basic_info_params,
                basic_identity_export=basic_identity_export,
                identity_bundle=identity_bundle,
                subscriber=subscriber,
            )
            ledger_inserted = False
        else:
            ledger_id, ledger_inserted = insert_xml_ledger(
                health_cur,
                config,
                xml_sha256=xml_sha256,
                xml_file_name=candidate.xml_file_name,
                xml_status=XML_STATUS_READY,
                xml_reason=None,
                file_receipt_id=file_receipt_id,
                run_id=run_id,
                basic=basic_for_ledger,
                identity_bundle=identity_bundle,
                subscriber=subscriber,
                basic_info=basic_info_params,
                basic_identity_export=basic_identity_export,
            )
    except Exception as exc:
        raise ImportDbError(
            "DB_EXAM_LEDGER_SAVE_FAILED",
            f"exam_ledger save failed: path={candidate.display_path}, reason={compact_text(exc) or type(exc).__name__}",
            candidate.inner_path,
        ) from exc
    try:
        link_result = insert_xml_file_link(
            health_cur,
            config,
            file_receipt_id=file_receipt_id,
            exam_ledger_id=ledger_id,
            xml_inner_path=candidate.inner_path,
        )
    except Exception as exc:
        raise ImportDbError(
            "DB_XML_FILE_LINK_SAVE_FAILED",
            f"xml_file_links save failed: path={candidate.display_path}, reason={compact_text(exc) or type(exc).__name__}",
            candidate.inner_path,
        ) from exc
    try:
        health_cur.execute(
            f"""
            DELETE FROM {qname(config.health_db)}.exam_item_values
            WHERE ledger_type = %s
              AND ledger_id = %s
            """,
            (LEDGER_TYPE_EXAM, ledger_id),
        )
        exam_item_count = insert_exam_item_values(
            health_cur,
            config,
            ledger_id=ledger_id,
            subscriber_id=cast(int | None, subscriber.get("subscriber_id")),
            hia_subscriber_id=cast(str | None, subscriber.get("hia_subscriber_id")),
            run_id=run_id,
            rows=exam_items,
        )
    except Exception as exc:
        raise ImportDbError(
            "DB_EXAM_ITEM_VALUES_SAVE_FAILED",
            f"exam_item_values save failed: path={candidate.display_path}, reason={compact_text(exc) or type(exc).__name__}",
            candidate.inner_path,
        ) from exc
    return XmlProcessResult(
        ok=True,
        new_ledger=ledger_inserted,
        existing_ledger=not ledger_inserted,
        link_inserted=link_result == "inserted",
        link_duplicate=link_result == "duplicate",
        exam_item_count=exam_item_count,
        error_count=local_error_count,
    )


def final_file_status(target_count: int, ok_count: int, error_count: int) -> str:
    if target_count == 0:
        return FILE_STATUS_ERROR
    if ok_count == 0:
        return FILE_STATUS_ERROR
    if error_count:
        return FILE_STATUS_WARNING
    return FILE_STATUS_IMPORTED


def process_file_receipt(
    conn: Any,
    health_cur: Any,
    dev_cur: Any,
    config: ImportConfig,
    summary: ImportSummary,
    *,
    run_id: int | None,
    file_receipt: Mapping[str, Any],
) -> None:
    file_receipt_id = int(cast(Any, file_receipt.get("id")))
    source_path = Path(str(file_receipt.get("source_path") or ""))
    file_error_count_before = summary.errors
    candidate_read_failed = False
    waiting_password = False

    if not config.dry_run:
        try:
            update_file_importing(health_cur, config, file_receipt_id)
            conn.commit()
        except Exception as exc:
            conn.rollback()
            message = f"file_receipts status update failed: path={source_path}, reason={compact_text(exc) or type(exc).__name__}"
            record_import_error(
                health_cur,
                run_id=run_id,
                summary=summary,
                field=ERROR_FIELD_DB,
                error_code="DB_FILE_RECEIPT_STATUS_UPDATE_FAILED",
                message=message,
                src_file=str(source_path),
                field_value=str(file_receipt_id),
            )
            conn.commit()
            summary.bump_file_status(FILE_STATUS_ERROR)
            return

    try:
        candidates, _skipped = read_candidates_from_file(health_cur, file_receipt, config, summary)
    except FileNotFoundError:
        candidate_read_failed = True
        message = f"file not found: path={source_path}"
        record_import_error(
            health_cur if not config.dry_run else None,
            run_id=run_id,
            summary=summary,
            field=ERROR_FIELD_FILE,
            error_code="FILE_NOT_FOUND",
            message=message,
            src_file=str(source_path),
            field_value=str(source_path),
        )
        candidates = []
    except zipfile.BadZipFile as exc:
        candidate_read_failed = True
        message = f"zip open failed: path={source_path}, reason={compact_text(exc) or type(exc).__name__}"
        record_import_error(
            health_cur if not config.dry_run else None,
            run_id=run_id,
            summary=summary,
            field=ERROR_FIELD_ZIP,
            error_code="ZIP_OPEN_FAILED",
            message=message,
            src_file=str(source_path),
            field_value=str(source_path),
        )
        candidates = []
    except ZipPasswordNotFoundError:
        candidate_read_failed = True
        waiting_password = True
        message = f"zip password not found: path={source_path}"
        record_import_error(
            health_cur if not config.dry_run else None,
            run_id=run_id,
            summary=summary,
            field=ERROR_FIELD_ZIP,
            error_code="ZIP_PASSWORD_NOT_FOUND",
            message=message,
            src_file=str(source_path),
            field_value=str(source_path),
        )
        candidates = []
    except ZipDecryptError as exc:
        candidate_read_failed = True
        message = f"zip decrypt failed: path={source_path}, reason={compact_text(exc) or type(exc).__name__}"
        record_import_error(
            health_cur if not config.dry_run else None,
            run_id=run_id,
            summary=summary,
            field=ERROR_FIELD_ZIP,
            error_code="ZIP_DECRYPT_FAILED",
            message=message,
            src_file=str(source_path),
            field_value=str(source_path),
        )
        candidates = []
    except OSError as exc:
        candidate_read_failed = True
        message = f"file read failed: path={source_path}, reason={compact_text(exc) or type(exc).__name__}"
        record_import_error(
            health_cur if not config.dry_run else None,
            run_id=run_id,
            summary=summary,
            field=ERROR_FIELD_FILE,
            error_code="FILE_READ_FAILED",
            message=message,
            src_file=str(source_path),
            field_value=str(source_path),
        )
        candidates = []

    target_count = len(candidates)
    summary.xml_seen += target_count
    ok_count = 0
    xml_error_count = 0

    if (
        target_count == 0
        and not candidate_read_failed
        and str(file_receipt.get("file_type") or "").upper() == FILE_TYPE_ZIP
    ):
        message = (
            f"zip has no target xml: path={source_path}, "
            "pattern=h*.xml, excludes=ix08,su08,schema,xsd"
        )
        record_import_error(
            health_cur if not config.dry_run else None,
            run_id=run_id,
            summary=summary,
            field=ERROR_FIELD_ZIP,
            error_code="ZIP_NO_TARGET_XML",
            message=message,
            src_file=str(source_path),
            field_value=str(source_path),
        )

    for candidate in candidates:
        try:
            result = process_xml_candidate(
                health_cur,
                dev_cur,
                config,
                summary,
                run_id=run_id,
                candidate=candidate,
            )
            if result.ok:
                ok_count += 1
            else:
                xml_error_count += 1
            if result.new_ledger:
                summary.xml_ledgers_inserted += 1
            if result.existing_ledger:
                summary.xml_ledgers_existing += 1
            if result.link_inserted:
                summary.xml_links_inserted += 1
            if result.link_duplicate:
                summary.xml_links_duplicate += 1
            if result.exam_item_count:
                summary.exam_item_values_inserted += result.exam_item_count
            if result.parse_error:
                summary.xml_parse_errors += 1
            if not config.dry_run:
                conn.commit()
        except ImportDbError as exc:
            conn.rollback()
            xml_error_count += 1
            record_import_error(
                health_cur if not config.dry_run else None,
                run_id=run_id,
                summary=summary,
                field=ERROR_FIELD_DB,
                error_code=exc.error_code,
                message=exc.message,
                src_file=candidate.display_path,
                field_value=exc.field_value,
            )
            if not config.dry_run:
                conn.commit()
        except Exception as exc:
            conn.rollback()
            xml_error_count += 1
            message = f"xml read failed: path={candidate.display_path}, reason={compact_text(exc) or type(exc).__name__}"
            record_import_error(
                health_cur if not config.dry_run else None,
                run_id=run_id,
                summary=summary,
                field=ERROR_FIELD_XML,
                error_code="XML_READ_FAILED",
                message=message,
                src_file=candidate.display_path,
                field_value=candidate.inner_path,
            )
            if not config.dry_run:
                conn.commit()

    file_error_count = summary.errors - file_error_count_before
    if waiting_password:
        status = FILE_STATUS_WAITING_PASSWORD
    else:
        status = final_file_status(target_count, ok_count, file_error_count + xml_error_count)
    summary.bump_file_status(status)
    message = (
        f"import_xml target_xml={target_count} ok={ok_count} "
        f"errors={file_error_count + xml_error_count}"
    )
    if not config.dry_run:
        try:
            update_file_status(
                health_cur,
                config,
                file_receipt_id=file_receipt_id,
                status=status,
                processable_count=target_count,
                message=message,
            )
            summary.file_receipts_updated += 1
            conn.commit()
        except Exception as exc:
            conn.rollback()
            record_import_error(
                health_cur,
                run_id=run_id,
                summary=summary,
                field=ERROR_FIELD_DB,
                error_code="DB_FILE_RECEIPT_STATUS_UPDATE_FAILED",
                message=f"file_receipts status update failed: path={source_path}, reason={compact_text(exc) or type(exc).__name__}",
                src_file=str(source_path),
                field_value=str(file_receipt_id),
            )
            conn.commit()


def run(config: ImportConfig, *, db_prefix: str) -> ImportSummary:
    validate_config(config)
    summary = ImportSummary(event_id=config.event_id, dry_run=config.dry_run)
    params = load_mysql_base_params(db_prefix)
    run_id: int | None = None

    with connect_ctx(params, database=config.health_db, autocommit=False) as health_conn:
        with connect_ctx(params, database=config.dev_db, autocommit=False) as dev_conn:
            with dict_cursor(health_conn) as health_cur:
                with dict_cursor(dev_conn) as dev_cur:
                    if not config.dry_run:
                        run_id = start_import_run(health_cur, config)
                        health_conn.commit()

                    file_receipts = fetch_file_receipts(health_cur, config)
                    summary.files = len(file_receipts)
                    for file_receipt in file_receipts:
                        process_file_receipt(
                            health_conn,
                            health_cur,
                            dev_cur,
                            config,
                            summary,
                            run_id=run_id,
                            file_receipt=file_receipt,
                        )

                    if not config.dry_run and run_id is not None:
                        finish_import_run(health_cur, run_id, summary)
                        health_conn.commit()

    return summary


def main() -> int:
    args = parse_args()
    try:
        config = resolve_config(args)
        summary = run(config, db_prefix=args.db_prefix)
        summary.print()
        return 0 if summary.errors == 0 else 1
    except ValueError as exc:
        print(f"CONFIG_INVALID: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"IMPORT_XML_FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
