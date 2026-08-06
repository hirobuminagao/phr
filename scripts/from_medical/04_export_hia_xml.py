#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import re
import shutil
import sys
import tempfile
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping, cast

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.from_medical.script_lib.hia_xml_export_loader import (
    ExportSelectors,
    decide_candidate,
    facility_folder_name,
    fetch_candidates,
    fetch_valid_items,
)
from scripts.from_medical.script_lib.export_case_readiness import (
    mark_export_case_export_error,
    mark_export_case_exported,
    refresh_export_case_readiness,
)
from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.etl import RunMetrics
from scripts.lib.etl import finish_run as etl_finish_run
from scripts.lib.etl import log_error as etl_log_error
from scripts.lib.etl import start_run as etl_start_run
from scripts.lib.examination.lookup import qname
from scripts.lib.examination.mhlw_v08_xml import (
    Facility,
    Person,
    build_clinical_document,
    build_ix08,
    copy_xsd_bundle,
    person_xml_file_name,
    root_dir_name,
    sha256_bytes,
    validate_xml,
    xml_bytes,
)
from scripts.lib.identity.export_fields import build_xml_export_fields
from scripts.lib.identity.base_norm import base_normalize
from scripts.lib.identity.field.address import normalize_address_export, normalize_postal_code_export
from scripts.lib.identity.field.phone_number import normalize_phone_number_export
from scripts.lib.identity.primitive.digits import extract_digits, zero_pad


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config" / "export_hia_xml.yml"
DEFAULT_XSD_ROOT = Path(__file__).resolve().parent / "source" / "XSD"
ETL_PHASE = "EXPORT_HIA_XML"
ETL_SOURCE = "FROM_MEDICAL"
UPLOAD_DIR_NAME = "03_健診結果（アップロードデータ）"
HISTORY_DIR_NAME = "xml作成_出力履歴"


@dataclass(frozen=True)
class ExportConfig:
    selectors: ExportSelectors
    health_db: str
    dev_db: str
    master_db: str
    xsd_bundle_id: str
    all_facilities: bool
    split_no: int | None
    file_date: date
    dry_run: bool


@dataclass
class ExportSummary:
    candidates_seen: int = 0
    candidates_ready: int = 0
    members_exported: int = 0
    groups_exported: int = 0
    skipped: int = 0
    errors: int = 0

    def metrics(self) -> RunMetrics:
        return RunMetrics(
            files=self.groups_exported,
            rows_seen=self.candidates_seen,
            rows_inserted=self.members_exported,
            rows_skipped=self.skipped,
            errors=self.errors,
        )

    def message(self) -> str:
        return (
            f"export_hia_xml candidates={self.candidates_seen} ready={self.candidates_ready} "
            f"members={self.members_exported} zips={self.groups_exported} "
            f"skipped={self.skipped} errors={self.errors}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export checked CSV exam results as MHLW V08 XML ZIP files.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--event-id", type=int)
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument("--facility-id", type=int, action="append", default=[])
    scope.add_argument("--facility-code", action="append", default=[])
    scope.add_argument("--all-facilities", action="store_true")
    parser.add_argument("--file-receipt-id", type=int, action="append", default=[])
    parser.add_argument("--case-id", "--ledger-id", dest="ledger_id", type=int, action="append", default=[])
    parser.add_argument("--subscriber-id", type=int, action="append", default=[])
    parser.add_argument("--hia-subscriber-id", action="append", default=[])
    parser.add_argument("--person-id-custom", action="append", default=[])
    parser.add_argument("--exam-month", help="YYYY-MM")
    parser.add_argument("--include-exported", action="store_true")
    parser.add_argument("--split-no", type=int, choices=range(10))
    parser.add_argument("--file-date", help="YYYYMMDD; default is today")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--db-prefix", default="PHR_DB_")
    parser.add_argument("--health-db", help="Override health_exam_result schema name")
    parser.add_argument("--dev-db", help="Override dev_phr schema name")
    parser.add_argument("--master-db", help="Override phr_master schema name")
    return parser.parse_args()


def _parse_file_date(value: Any) -> date:
    if not value:
        return date.today()
    return datetime.strptime(str(value), "%Y%m%d").date()


def _int_tuple(value: Any) -> tuple[int, ...]:
    if value in (None, ""):
        return ()
    if isinstance(value, int):
        return (value,)
    if isinstance(value, str):
        value = [part.strip() for part in value.split(",") if part.strip()]
    if not isinstance(value, list | tuple):
        raise ValueError(f"Expected int list, got {type(value).__name__}")
    return tuple(int(item) for item in value)


def _str_tuple(value: Any) -> tuple[str, ...]:
    if value in (None, ""):
        return ()
    if isinstance(value, int):
        return (str(value),)
    if isinstance(value, str):
        value = [part.strip() for part in value.split(",") if part.strip()]
    if not isinstance(value, list | tuple):
        raise ValueError(f"Expected string list, got {type(value).__name__}")
    return tuple(str(item).strip() for item in value if str(item).strip())


def _optional_split_no(value: Any) -> int | None:
    if value in (None, ""):
        return None
    split_no = int(value)
    if split_no < 0 or split_no > 9:
        raise ValueError("split_no must be 0-9")
    return split_no


def load_config(args: argparse.Namespace) -> ExportConfig:
    with Path(args.config).open("r", encoding="utf-8") as fp:
        data = cast(Mapping[str, Any], yaml.safe_load(fp) or {})
    event_id = args.event_id if args.event_id is not None else int(data.get("event_id") or 0)
    all_facilities = bool(args.all_facilities or data.get("all_facilities", False))
    facility_ids = tuple(args.facility_id or ()) or _int_tuple(data.get("facility_ids"))
    facility_codes = tuple(args.facility_code or ()) or _str_tuple(data.get("facility_codes"))
    file_receipt_ids = tuple(args.file_receipt_id or ()) or _int_tuple(data.get("file_receipt_ids"))
    ledger_ids = tuple(args.ledger_id or ()) or _int_tuple(data.get("ledger_ids"))
    subscriber_ids = tuple(args.subscriber_id or ()) or _int_tuple(data.get("subscriber_ids"))
    hia_subscriber_ids = tuple(args.hia_subscriber_id or ()) or _str_tuple(data.get("hia_subscriber_ids"))
    person_id_customs = tuple(args.person_id_custom or ()) or _str_tuple(data.get("person_id_customs"))
    exam_month = args.exam_month if args.exam_month is not None else data.get("exam_month")
    if event_id <= 0:
        raise ValueError("event_id is required")
    if (
        not all_facilities
        and not facility_ids
        and not facility_codes
        and not file_receipt_ids
        and not ledger_ids
        and not subscriber_ids
        and not hia_subscriber_ids
        and not person_id_customs
    ):
        raise ValueError(
            "Specify --facility-id, --facility-code, --all-facilities, file_receipt_ids, "
            "ledger_ids, subscriber_ids, hia_subscriber_ids, or person_id_customs explicitly"
        )
    if exam_month and not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", str(exam_month)):
        raise ValueError("exam_month must be YYYY-MM")
    selectors = ExportSelectors(
        event_id=event_id,
        facility_ids=facility_ids,
        facility_codes=facility_codes,
        file_receipt_ids=file_receipt_ids,
        ledger_ids=ledger_ids,
        subscriber_ids=subscriber_ids,
        hia_subscriber_ids=hia_subscriber_ids,
        person_id_customs=person_id_customs,
        exam_month=None if not exam_month else str(exam_month),
        include_exported=bool(args.include_exported or data.get("include_exported", False)),
        limit=args.limit if args.limit is not None else int(data.get("limit") or 0),
    )
    return ExportConfig(
        selectors=selectors,
        health_db=str(args.health_db or data.get("health_db") or "health_exam_result"),
        dev_db=str(args.dev_db or data.get("dev_db") or "dev_phr"),
        master_db=str(args.master_db or data.get("master_db") or "phr_master"),
        xsd_bundle_id=str(data.get("xsd_bundle_id") or "mhlw_v4_20230331_v08"),
        all_facilities=all_facilities,
        split_no=args.split_no if args.split_no is not None else _optional_split_no(data.get("split_no")),
        file_date=_parse_file_date(args.file_date if args.file_date is not None else data.get("file_date")),
        dry_run=bool(args.dry_run or data.get("dry_run", False)),
    )


def fetch_result_root(cur: Any, config: ExportConfig) -> Path:
    cur.execute(f"SELECT result_root_path FROM {qname(config.dev_db)}.event WHERE event_id = %s", (config.selectors.event_id,))
    row = cur.fetchone()
    if not row or not row.get("result_root_path"):
        raise ValueError(f"event.result_root_path is missing: event_id={config.selectors.event_id}")
    return Path(str(row["result_root_path"]))


def _digits(value: Any, width: int, field: str) -> str:
    text = extract_digits(base_normalize(None if value is None else str(value)))
    if not text or len(text) > width:
        raise ValueError(f"{field}: invalid value {value!r}")
    padded = zero_pad(text, width)
    assert padded is not None
    return padded


def exam_month_yyyymm(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y%m")
    if isinstance(value, date):
        return value.strftime("%Y%m")
    text = str(value or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return datetime.strptime(text, "%Y-%m-%d").strftime("%Y%m")
    if re.fullmatch(r"\d{8}", text):
        return datetime.strptime(text, "%Y%m%d").strftime("%Y%m")
    raise ValueError(f"exam_date: invalid value {value!r}")


def choose_split_no(facility_output_root: Path, facility_code: str, insurer_number: str, file_date: str, requested: int | None) -> int:
    pattern = re.compile(rf"^{re.escape(facility_code)}_{re.escape(insurer_number)}_{file_date}([0-9])_1\.zip$")
    used = {
        int(match.group(1))
        for path in facility_output_root.rglob("*.zip") if (match := pattern.match(path.name))
    } if facility_output_root.exists() else set()
    if requested is not None:
        if requested in used:
            raise ValueError(f"split_no {requested} is already used for {facility_code}/{insurer_number}/{file_date}")
        return requested
    for candidate in range(10):
        if candidate not in used:
            return candidate
    raise ValueError(f"split_no exhausted for {facility_code}/{insurer_number}/{file_date}")


def resolve_export_address(row: Mapping[str, Any]) -> str | None:
    """Return a prepared source/completed address for XML export."""
    current = normalize_address_export(row.get("address"))
    if current:
        return current
    return normalize_address_export(row.get("address_completed_value"))


def resolve_export_postal_code(row: Mapping[str, Any]) -> str | None:
    """Return a prepared source/completed postal code for XML export."""
    current = normalize_postal_code_export(row.get("postal_code"))
    if current:
        return current
    return normalize_postal_code_export(row.get("postal_code_completed_value"))


def resolve_export_insurer_number(row: Mapping[str, Any]) -> str | None:
    """Return a prepared source/event insurer number for XML export."""
    return cast(str | None, row.get("insurer_number_export_value") or row.get("insurer_number"))


def make_zip(source_root: Path, zip_path: Path) -> None:
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(source_root.rglob("*")):
            if path.is_file():
                archive.write(path, path.relative_to(source_root.parent))


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fp:
        for chunk in iter(lambda: fp.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def start_run(cur: Any, config: ExportConfig, result_root: Path) -> int:
    return etl_start_run(
        cur,
        phase=ETL_PHASE,
        source=ETL_SOURCE,
        db_schema=config.health_db,
        db_path=None,
        input_base=str(result_root),
        input_file=None,
        insurer_number=None,
        dry_run=False,
        limit_rows=config.selectors.limit or None,
    )


def log_failure(cur: Any, *, run_id: int, summary: ExportSummary, row: Mapping[str, Any] | None, error_code: str, message: str) -> None:
    summary.errors += 1
    etl_log_error(
        cur,
        run_id,
        phase=ETL_PHASE,
        source=ETL_SOURCE,
        insurer_number=None if row is None else str(row.get("insurer_number") or "") or None,
        src_file=None if row is None else str(row.get("source_file_name") or "") or None,
        row_no=None if row is None else int(row.get("src_row_no") or 0) or None,
        line_no=None if row is None else int(row.get("src_line_no") or 0) or None,
        staging_rowid=None if row is None else int(row["exam_export_case_id"]),
        person_id_custom=None if row is None else str(row.get("person_id_custom") or "") or None,
        field="xml_export",
        field_value=None,
        error_code=error_code,
        message=message,
    )


def insert_history(cur: Any, *, config: ExportConfig, run_id: int, group: list[dict[str, Any]], folder_name: str, split_no: int, root_name: str, zip_path: Path, member_info: list[tuple[dict[str, Any], str, str]]) -> None:
    first = group[0]
    cur.execute(
        f"""
        INSERT INTO {qname(config.health_db)}.xml_export_zips (
          etl_run_id, event_id, exam_facility_id, facility_code, facility_name,
          facility_folder_name, insurer_number, file_date, split_no,
          implementation_code, root_dir_name, zip_file_name, zip_path,
          zip_sha256, member_count, xsd_bundle_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, '1', %s, %s, %s, %s, %s, %s)
        """,
        (
            run_id, config.selectors.event_id, first["exam_facility_id"], first["master_facility_code"],
            first["master_facility_name"], folder_name, _digits(first["insurer_number"], 8, "insurer_number"),
            config.file_date, split_no, root_name, zip_path.name, str(zip_path), file_sha256(zip_path),
            len(member_info), config.xsd_bundle_id,
        ),
    )
    zip_id = int(cur.lastrowid)
    for row, filename, digest in member_info:
        cur.execute(
            f"""
            INSERT INTO {qname(config.health_db)}.xml_export_members (
              xml_export_zip_id, etl_run_id, event_id, ledger_type, ledger_id,
              source_file_receipt_id, subscriber_id, hia_subscriber_id,
              person_xml_file_name, person_xml_sha256, report_category_code,
              program_type_code, manual_export_approved, manual_export_reason,
              manual_export_approved_at, manual_export_approved_by
            ) VALUES (%s, %s, %s, 'CASE', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                zip_id, run_id, config.selectors.event_id, row["exam_export_case_id"], row.get("file_receipt_id"),
                row.get("subscriber_id"), row.get("hia_subscriber_id"), filename, digest,
                row["health_exam_report_category"], row["program_code"], bool(row.get("manual_export_approved")),
                row.get("manual_export_reason"), row.get("manual_export_approved_at"), row.get("manual_export_approved_by"),
            ),
        )
        mark_export_case_exported(
            cur,
            health_db=config.health_db,
            exam_export_case_id=int(row["exam_export_case_id"]),
            output_zip_path=str(zip_path),
            output_zip_file_name=zip_path.name,
            output_xml_file_name=filename,
            etl_run_id=run_id,
        )
    refresh_export_case_readiness(cur, health_db=config.health_db, event_id=config.selectors.event_id)


def mark_group_export_error(cur: Any, *, health_db: str, event_id: int, run_id: int, group: list[dict[str, Any]], reason: str) -> None:
    for row in group:
        mark_export_case_export_error(
            cur,
            health_db=health_db,
            exam_export_case_id=int(row["exam_export_case_id"]),
            reason=reason,
            etl_run_id=run_id,
        )
    refresh_export_case_readiness(cur, health_db=health_db, event_id=event_id)


def build_group(
    cur: Any,
    *,
    config: ExportConfig,
    result_root: Path,
    timestamp: str,
    group: list[dict[str, Any]],
    run_id: int | None,
) -> tuple[Path | None, tuple[str, str, str, str, str, int]]:
    first = group[0]
    facility_code = _digits(first["master_facility_code"], 10, "facility_code")
    insurer_number = _digits(first["insurer_number"], 8, "insurer_number")
    folder_names = {facility_folder_name(row.get("relative_path")) for row in group}
    if len(folder_names) != 1:
        raise ValueError(f"FACILITY_FOLDER_CONFLICT: {sorted(folder_names)}")
    folder_name = next(iter(folder_names))
    exam_months = {exam_month_yyyymm(row.get("exam_date")) for row in group}
    if len(exam_months) != 1:
        raise ValueError(f"EXAM_MONTH_CONFLICT: {sorted(exam_months)}")
    exam_month = next(iter(exam_months))
    for row in group:
        ledger_code = _digits(row.get("facility_code"), 10, "ledger.facility_code")
        if ledger_code != facility_code:
            raise ValueError(f"FACILITY_CODE_CONFLICT: ledger={ledger_code} master={facility_code}")

    facility = Facility(
        code=facility_code,
        name=str(first["master_facility_name"]),
        postal_code=normalize_postal_code_export(first.get("master_facility_postal_code")),
        address=normalize_address_export(first.get("master_facility_address")),
        phone=normalize_phone_number_export(first.get("master_facility_phone_number")),
    )
    output_root = result_root / folder_name / UPLOAD_DIR_NAME
    month_output_root = output_root / timestamp / exam_month
    file_date = config.file_date.strftime("%Y%m%d")
    split_no = choose_split_no(output_root, facility_code, insurer_number, file_date, config.split_no)
    root_name = root_dir_name(facility_code, insurer_number, file_date, split_no)
    final_dir = month_output_root
    final_zip = final_dir / f"{root_name}.zip"
    bundle_dir = DEFAULT_XSD_ROOT / config.xsd_bundle_id
    if not bundle_dir.is_dir():
        raise ValueError(f"XSD_BUNDLE_NOT_FOUND: {bundle_dir}")

    member_info: list[tuple[dict[str, Any], str, str]] = []
    with tempfile.TemporaryDirectory(prefix="phr_hia_xml_") as temp_name:
        temp = Path(temp_name)
        package_root = temp / root_name
        data_dir = package_root / "DATA"
        data_dir.mkdir(parents=True)
        copy_xsd_bundle(bundle_dir, package_root / "XSD")
        for sequence, row in enumerate(group, start=1):
            fields = build_xml_export_fields(
                row,
                insurer_number_override=resolve_export_insurer_number(row),
                postal_code_override=resolve_export_postal_code(row),
                address_override=resolve_export_address(row),
            )
            if fields.insurer_number != insurer_number:
                raise ValueError(f"INSURER_CONFLICT: ledger={fields.insurer_number} group={insurer_number}")
            person = Person(
                insurer_number=fields.insurer_number,
                insurance_symbol=fields.insurance_symbol,
                insurance_number=fields.insurance_number,
                name_kana=fields.name_kana,
                gender_code=fields.gender_code,
                birthdate=fields.birthdate,
                exam_date=fields.exam_date,
                report_category_code=str(row["health_exam_report_category"]),
                program_type_code=str(row["program_code"]),
                postal_code=fields.postal_code,
                address=fields.address,
            )
            items = fetch_valid_items(cur, ledger_id=int(row["exam_export_case_id"]), health_db=config.health_db, dev_db=config.dev_db)
            if not items:
                raise ValueError(f"NO_VALID_EXAM_ITEMS: case_id={row['exam_export_case_id']}")
            content = xml_bytes(build_clinical_document(person, facility, items, file_date))
            validate_xml(content, bundle_dir / "hc08_V08.xsd")
            filename = person_xml_file_name(facility_code, file_date, split_no, sequence)
            (data_dir / filename).write_bytes(content)
            member_info.append((row, filename, sha256_bytes(content)))

        ix_content = xml_bytes(build_ix08(facility_code, insurer_number, file_date, len(group)))
        validate_xml(ix_content, bundle_dir / "ix08_V08.xsd")
        (package_root / "ix08_V08.xml").write_bytes(ix_content)
        temp_zip = temp / final_zip.name
        make_zip(package_root, temp_zip)
        if config.dry_run:
            return None, (facility_code, facility.name, folder_name, exam_month, final_zip.name, len(group))

        final_dir.mkdir(parents=True, exist_ok=True)
        if final_zip.exists():
            raise ValueError(f"OUTPUT_ALREADY_EXISTS: {final_zip}")
        shutil.move(str(temp_zip), final_zip)

    assert run_id is not None
    try:
        insert_history(
            cur,
            config=config,
            run_id=run_id,
            group=group,
            folder_name=folder_name,
            split_no=split_no,
            root_name=root_name,
            zip_path=final_zip,
            member_info=member_info,
        )
    except Exception:
        final_zip.unlink(missing_ok=True)
        raise
    return final_zip, (facility_code, facility.name, folder_name, exam_month, final_zip.name, len(group))


def write_operator_log(result_root: Path, timestamp: str, rows: list[tuple[str, str, str, str, str, int]]) -> Path:
    log_dir = result_root / HISTORY_DIR_NAME / timestamp
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / "健診結果XML出力履歴.csv"
    temporary_path = path.with_suffix(".csv.tmp")
    with temporary_path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.writer(fp)
        writer.writerow(["健診機関コード", "健診機関名", "健診機関フォルダ名", "出力フォルダ", "健診実施月", "ZIP名", "人数"])
        for facility_code, facility_name, folder_name, exam_month, zip_name, count in rows:
            writer.writerow([facility_code, facility_name, folder_name, timestamp, exam_month, zip_name, count])
    temporary_path.replace(path)
    return path


def run(config: ExportConfig, *, db_prefix: str) -> ExportSummary:
    summary = ExportSummary()
    params = load_mysql_base_params(db_prefix)
    with connect_ctx(params, database=config.health_db, autocommit=False) as conn:
        with dict_cursor(conn) as cur:
            result_root = fetch_result_root(cur, config)
            run_id: int | None = None
            if not config.dry_run:
                run_id = start_run(cur, config, result_root)
                conn.commit()
            try:
                candidates = fetch_candidates(
                    cur,
                    selectors=config.selectors,
                    health_db=config.health_db,
                    master_db=config.master_db,
                )
                summary.candidates_seen = len(candidates)
                ready: list[dict[str, Any]] = []
                for row in candidates:
                    decision = decide_candidate(row)
                    if decision.reason:
                        summary.skipped += 1
                        print(f"[SKIP] case_id={row['exam_export_case_id']} reason={decision.reason}")
                        continue
                    ready.append(row)
                summary.candidates_ready = len(ready)
                groups: dict[tuple[int, str, str], list[dict[str, Any]]] = defaultdict(list)
                for row in ready:
                    insurer = _digits(row.get("insurer_number"), 8, "insurer_number")
                    groups[(int(row["exam_facility_id"]), insurer, exam_month_yyyymm(row.get("exam_date")))].append(row)

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                operator_rows: list[tuple[str, str, str, str, str, int]] = []
                for _, group in groups.items():
                    try:
                        zip_path, operator_row = build_group(
                            cur,
                            config=config,
                            result_root=result_root,
                            timestamp=timestamp,
                            group=group,
                            run_id=run_id,
                        )
                        if not config.dry_run:
                            conn.commit()
                        operator_rows.append(operator_row)
                        summary.groups_exported += 1
                        summary.members_exported += len(group)
                        print(f"[OK] count={len(group)} zip={zip_path or '(dry-run)'}")
                    except Exception as exc:
                        conn.rollback()
                        if run_id is not None:
                            mark_group_export_error(
                                cur,
                                health_db=config.health_db,
                                event_id=config.selectors.event_id,
                                run_id=run_id,
                                group=group,
                                reason=f"{type(exc).__name__}: {exc}",
                            )
                            log_failure(
                                cur,
                                run_id=run_id,
                                summary=summary,
                                row=group[0],
                                error_code="XML_EXPORT_GROUP_FAILED",
                                message=f"{type(exc).__name__}: {exc}",
                            )
                            conn.commit()
                        else:
                            summary.errors += 1
                        print(
                            f"[ERROR] facility={group[0].get('master_facility_name')} "
                            f"insurer={group[0].get('insurer_number')}: {exc}",
                            file=sys.stderr,
                        )

                if operator_rows and not config.dry_run:
                    print(f"[LOG] {write_operator_log(result_root, timestamp, operator_rows)}")
                if run_id is not None:
                    etl_finish_run(
                        cur,
                        run_id,
                        summary.metrics(),
                        status_override="partial" if summary.errors and summary.members_exported else "failed" if summary.errors else "success",
                        extra_notes=summary.message(),
                    )
                    conn.commit()
            except Exception as exc:
                conn.rollback()
                if run_id is not None:
                    log_failure(
                        cur,
                        run_id=run_id,
                        summary=summary,
                        row=None,
                        error_code="XML_EXPORT_RUN_FAILED",
                        message=f"{type(exc).__name__}: {exc}",
                    )
                    etl_finish_run(
                        cur,
                        run_id,
                        summary.metrics(),
                        status_override="partial" if summary.members_exported else "failed",
                        extra_notes=summary.message(),
                    )
                    conn.commit()
                raise
    return summary


def main() -> int:
    args = parse_args()
    try:
        config = load_config(args)
        summary = run(config, db_prefix=args.db_prefix)
        print(summary.message())
        return 0 if summary.errors == 0 else 1
    except Exception as exc:
        print(f"EXPORT_HIA_XML_FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
