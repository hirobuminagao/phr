from __future__ import annotations

import hashlib
import re
import shutil
import zipfile
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from scripts.hia.script_lib.hia_download_xml_parser import parse_hia_download_xml
from scripts.lib.identity.field.birthdate import normalize_birthdate
from scripts.lib.identity.field.insurance_number import normalize_insurance_number
from scripts.lib.identity.field.insurance_symbol import normalize_insurance_symbol
from scripts.lib.identity.field.name_kana import normalize_name_kana_full
from scripts.lib.identity.generator import generate_identity_bundle


XML_FILENAME_PATTERN = re.compile(r"^h[^\\/]*\.xml$", re.IGNORECASE)


@dataclass(frozen=True)
class HiaDownloadImportConfig:
    project_root: Path
    input_zip_dir: Path
    archive_zip_dir: Path
    work_dir: Path
    event_id: int | None
    exam_year_start_month: int = 4
    exam_year_start_day: int = 1
    archive_mode: str = "copy"
    dry_run: bool = False


@dataclass
class HiaDownloadImportSummary:
    files_seen: int = 0
    files_imported: int = 0
    files_skipped: int = 0
    xml_seen: int = 0
    xml_inserted: int = 0
    xml_updated: int = 0
    person_years_upserted: int = 0
    person_xml_events_upserted: int = 0
    errors: int = 0


def calc_file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fp:
        for chunk in iter(lambda: fp.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def resolve_exam_year(exam_date: str, *, start_month: int, start_day: int) -> int:
    d = date.fromisoformat(exam_date)
    boundary = date(d.year, start_month, start_day)
    return d.year if d >= boundary else d.year - 1


def parse_zip_context(zip_path: Path) -> dict[str, Any]:
    parts = zip_path.stem.split("_")
    if len(parts) != 4:
        raise ValueError(f"invalid ZIP filename format: {zip_path.name}")

    facility_code, insurer_number, dl_raw, send_seq_raw = parts
    if len(dl_raw) < 8 or not dl_raw[:8].isdigit():
        raise ValueError(f"invalid dl_date block in ZIP filename: {zip_path.name}")
    if not send_seq_raw.isdigit():
        raise ValueError(f"invalid send_seq in ZIP filename: {zip_path.name}")

    return {
        "facility_code": facility_code,
        "insurer_number": insurer_number,
        "dl_date": f"{dl_raw[:4]}-{dl_raw[4:6]}-{dl_raw[6:8]}",
        "send_seq": int(send_seq_raw),
        "folder_name": zip_path.parent.name,
        "zip_name": zip_path.name,
    }


def find_zip_files(input_zip_dir: Path) -> list[Path]:
    if not input_zip_dir.exists():
        return []
    return sorted(
        zip_path
        for zip_path in input_zip_dir.glob("*/*.zip")
        if zip_path.parent.name.isdigit()
    )


def _reset_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def extract_zip(zip_path: Path, extract_dir: Path) -> None:
    _reset_dir(extract_dir)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)


def collect_data_xml_files(extract_dir: Path) -> list[Path]:
    return sorted(
        p
        for p in extract_dir.rglob("h*.xml")
        if "DATA" in p.parts and XML_FILENAME_PATTERN.match(p.name)
    )


def _normalize_identity(row: dict[str, Any], config: HiaDownloadImportConfig) -> dict[str, Any]:
    symbol = normalize_insurance_symbol(row.get("insurance_symbol_raw"))
    number = normalize_insurance_number(row.get("insurance_number_raw"))
    birthdate = normalize_birthdate(row.get("birthdate"))
    name_kana = normalize_name_kana_full(row.get("name_kana_raw"))

    row["insurance_symbol_match"] = symbol.get("match")
    row["insurance_number_match"] = number.get("match")
    row["birthdate"] = birthdate.get("field_norm")
    row["name_kana_norm"] = name_kana.get("field_norm")

    identity = generate_identity_bundle(
        birthdate=row.get("birthdate"),
        insurer_number_raw=row.get("insurer_number"),
        insurance_symbol_raw=row.get("insurance_symbol_raw"),
        insurance_number_raw=row.get("insurance_number_raw"),
        name_kana_full_raw=row.get("name_kana_raw"),
        gender_code=row.get("gender_code"),
    )
    row["person_id_custom"] = identity.get("person_id_custom")
    row["identity_hash"] = identity.get("identity_hash")

    if row.get("exam_date"):
        row["exam_year"] = resolve_exam_year(
            row["exam_date"],
            start_month=config.exam_year_start_month,
            start_day=config.exam_year_start_day,
        )
        row["exam_month"] = str(row["exam_date"]).replace("-", "")[:6]
    else:
        row["exam_year"] = None
        row["exam_month"] = None

    row["_identity_reason"] = identity.get("reason")
    return row


def validate_download_xml_row(row: dict[str, Any]) -> list[str]:
    required = {
        "insurer_number": "INSURER_NUMBER_MISSING",
        "insurance_symbol_raw": "INSURANCE_SYMBOL_MISSING",
        "insurance_number_raw": "INSURANCE_NUMBER_MISSING",
        "birthdate": "BIRTHDATE_MISSING",
        "name_kana_raw": "NAME_KANA_MISSING",
        "name_kana_norm": "NAME_KANA_NORMALIZE_FAILED",
        "gender_code": "GENDER_CODE_MISSING",
        "exam_date": "EXAM_DATE_MISSING",
        "exam_year": "EXAM_YEAR_MISSING",
        "person_id_custom": "PERSON_ID_CUSTOM_BUILD_FAILED",
        "identity_hash": "IDENTITY_HASH_BUILD_FAILED",
    }
    return [code for key, code in required.items() if row.get(key) in (None, "")]


def get_existing_download_zip(cur: Any, *, insurer_number: str, zip_name: str) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT download_zip_id, zip_sha256, import_status
          FROM hia_download_zips
         WHERE insurer_number = %s
           AND zip_name = %s
         LIMIT 1
        """,
        (insurer_number, zip_name),
    )
    return cur.fetchone()


def upsert_download_zip(
    cur: Any,
    *,
    run_id: int,
    config: HiaDownloadImportConfig,
    zip_ctx: dict[str, Any],
    zip_sha256: str,
    source_zip_path: Path,
) -> int:
    cur.execute(
        """
        INSERT INTO hia_download_zips (
            etl_run_id, event_id, insurer_number, facility_code, folder_name,
            zip_name, dl_date, send_seq, zip_sha256, source_zip_path, import_status
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, 'PROCESSING'
        )
        ON DUPLICATE KEY UPDATE
            download_zip_id = LAST_INSERT_ID(download_zip_id),
            etl_run_id = VALUES(etl_run_id),
            event_id = VALUES(event_id),
            facility_code = VALUES(facility_code),
            folder_name = VALUES(folder_name),
            dl_date = VALUES(dl_date),
            send_seq = VALUES(send_seq),
            zip_sha256 = VALUES(zip_sha256),
            source_zip_path = VALUES(source_zip_path),
            import_status = 'PROCESSING',
            import_reason = NULL,
            updated_at = CURRENT_TIMESTAMP(3)
        """,
        (
            run_id,
            config.event_id,
            zip_ctx["insurer_number"],
            zip_ctx["facility_code"],
            zip_ctx["folder_name"],
            zip_ctx["zip_name"],
            zip_ctx["dl_date"],
            zip_ctx["send_seq"],
            zip_sha256,
            str(source_zip_path),
        ),
    )
    return int(cur.lastrowid)


def update_download_zip_result(
    cur: Any,
    *,
    download_zip_id: int,
    status: str,
    reason: str | None,
    total: int,
    success: int,
    error: int,
    archive_zip_path: str | None = None,
) -> None:
    cur.execute(
        """
        UPDATE hia_download_zips
           SET import_status = %s,
               import_reason = %s,
               xml_count_total = %s,
               xml_count_success = %s,
               xml_count_error = %s,
               archive_zip_path = COALESCE(%s, archive_zip_path),
               updated_at = CURRENT_TIMESTAMP(3)
         WHERE download_zip_id = %s
        """,
        (status, reason, total, success, error, archive_zip_path, download_zip_id),
    )


def upsert_download_xml(
    cur: Any,
    *,
    run_id: int,
    config: HiaDownloadImportConfig,
    download_zip_id: int,
    zip_ctx: dict[str, Any],
    row: dict[str, Any],
    xml_path: Path,
    extract_dir: Path,
    parse_status: str,
    parse_reason: str | None,
) -> tuple[int, str]:
    inner_path = xml_path.relative_to(extract_dir).as_posix()
    xml_sha256 = calc_file_sha256(xml_path)
    cur.execute(
        """
        INSERT INTO hia_download_xmls (
            download_zip_id, etl_run_id, event_id, xml_filename, xml_inner_path,
            xml_sha256, exam_date, exam_year, exam_month, facility_code,
            facility_name, report_category_code, program_type_code, insurer_number,
            insurance_symbol_raw, insurance_number_raw, insurance_symbol_match,
            insurance_number_match, birthdate, name_kana_raw, name_kana_norm,
            gender_code, person_id_custom, identity_hash, parse_status, parse_reason,
            is_active_in_zip
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            1
        )
        ON DUPLICATE KEY UPDATE
            hia_download_xml_id = LAST_INSERT_ID(hia_download_xml_id),
            etl_run_id = VALUES(etl_run_id),
            event_id = VALUES(event_id),
            xml_sha256 = VALUES(xml_sha256),
            exam_date = VALUES(exam_date),
            exam_year = VALUES(exam_year),
            exam_month = VALUES(exam_month),
            facility_code = VALUES(facility_code),
            facility_name = VALUES(facility_name),
            report_category_code = VALUES(report_category_code),
            program_type_code = VALUES(program_type_code),
            insurer_number = VALUES(insurer_number),
            insurance_symbol_raw = VALUES(insurance_symbol_raw),
            insurance_number_raw = VALUES(insurance_number_raw),
            insurance_symbol_match = VALUES(insurance_symbol_match),
            insurance_number_match = VALUES(insurance_number_match),
            birthdate = VALUES(birthdate),
            name_kana_raw = VALUES(name_kana_raw),
            name_kana_norm = VALUES(name_kana_norm),
            gender_code = VALUES(gender_code),
            person_id_custom = VALUES(person_id_custom),
            identity_hash = VALUES(identity_hash),
            parse_status = VALUES(parse_status),
            parse_reason = VALUES(parse_reason),
            is_active_in_zip = 1,
            updated_at = CURRENT_TIMESTAMP(3)
        """,
        (
            download_zip_id,
            run_id,
            config.event_id,
            xml_path.name,
            inner_path,
            xml_sha256,
            row.get("exam_date"),
            row.get("exam_year"),
            row.get("exam_month"),
            row.get("facility_code") or zip_ctx.get("facility_code"),
            row.get("facility_name"),
            row.get("report_category_code"),
            row.get("program_type_code"),
            row.get("insurer_number") or zip_ctx.get("insurer_number"),
            row.get("insurance_symbol_raw"),
            row.get("insurance_number_raw"),
            row.get("insurance_symbol_match"),
            row.get("insurance_number_match"),
            row.get("birthdate"),
            row.get("name_kana_raw"),
            row.get("name_kana_norm"),
            row.get("gender_code"),
            row.get("person_id_custom"),
            row.get("identity_hash"),
            parse_status,
            parse_reason,
        ),
    )
    action = "updated" if cur.rowcount == 2 else "inserted"
    return int(cur.lastrowid), action


def upsert_person_year(cur: Any, row: dict[str, Any], *, download_zip_id: int, hia_download_xml_id: int, event_id: int | None) -> int:
    cur.execute(
        """
        INSERT INTO hia_person_years (
            event_id, person_id_custom, identity_hash, name_kana_raw, name_kana_norm,
            gender_code, exam_year, insurer_number, insurance_symbol_raw,
            insurance_number_raw, insurance_symbol_match, insurance_number_match,
            birthdate, report_category_code, program_type_code, dl_count,
            first_seen_dl_date, first_seen_download_zip_id, first_seen_hia_download_xml_id,
            last_seen_dl_date, last_seen_download_zip_id, last_seen_hia_download_xml_id
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, 1,
            %s, %s, %s,
            %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            person_year_id = LAST_INSERT_ID(person_year_id),
            identity_hash = VALUES(identity_hash),
            report_category_code = VALUES(report_category_code),
            program_type_code = VALUES(program_type_code),
            dl_count = dl_count + 1,
            last_seen_dl_date = VALUES(last_seen_dl_date),
            last_seen_download_zip_id = VALUES(last_seen_download_zip_id),
            last_seen_hia_download_xml_id = VALUES(last_seen_hia_download_xml_id),
            updated_at = CURRENT_TIMESTAMP(3)
        """,
        (
            event_id,
            row["person_id_custom"],
            row.get("identity_hash"),
            row.get("name_kana_raw"),
            row["name_kana_norm"],
            row["gender_code"],
            row["exam_year"],
            row["insurer_number"],
            row.get("insurance_symbol_raw"),
            row.get("insurance_number_raw"),
            row["insurance_symbol_match"],
            row["insurance_number_match"],
            row["birthdate"],
            row.get("report_category_code"),
            row.get("program_type_code"),
            row.get("exam_date"),
            download_zip_id,
            hia_download_xml_id,
            row.get("exam_date"),
            download_zip_id,
            hia_download_xml_id,
        ),
    )
    return int(cur.lastrowid)


def upsert_person_xml_event(
    cur: Any,
    *,
    person_year_id: int,
    hia_download_xml_id: int,
    download_zip_id: int,
) -> int:
    cur.execute(
        """
        INSERT INTO hia_person_xml_events (
            person_year_id, hia_download_xml_id, download_zip_id,
            event_type, event_status, is_current
        ) VALUES (
            %s, %s, %s,
            'LINKED', 'ACTIVE', 1
        )
        ON DUPLICATE KEY UPDATE
            person_xml_event_id = LAST_INSERT_ID(person_xml_event_id),
            download_zip_id = VALUES(download_zip_id),
            event_status = 'ACTIVE',
            is_current = 1,
            updated_at = CURRENT_TIMESTAMP(3)
        """,
        (person_year_id, hia_download_xml_id, download_zip_id),
    )
    return int(cur.lastrowid)


def archive_zip(zip_path: Path, config: HiaDownloadImportConfig, *, run_token: str, insurer_number: str) -> Path | None:
    if config.dry_run:
        return None
    if config.archive_mode == "none":
        return None

    target_dir = config.archive_zip_dir / run_token / insurer_number
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / zip_path.name
    if config.archive_mode == "move":
        zip_path.rename(target_path)
    elif config.archive_mode == "copy":
        shutil.copy2(zip_path, target_path)
    else:
        raise ValueError(f"unsupported archive_mode: {config.archive_mode}")
    return target_path


def import_hia_download_zips(cur: Any, *, config: HiaDownloadImportConfig, run_id: int) -> HiaDownloadImportSummary:
    summary = HiaDownloadImportSummary()
    run_token = datetime.now().strftime("%Y%m%d%H%M%S")

    for zip_path in find_zip_files(config.input_zip_dir):
        summary.files_seen += 1
        zip_ctx = parse_zip_context(zip_path)
        zip_sha256 = calc_file_sha256(zip_path)
        existing = get_existing_download_zip(
            cur,
            insurer_number=zip_ctx["insurer_number"],
            zip_name=zip_ctx["zip_name"],
        )
        if existing and existing.get("zip_sha256") == zip_sha256 and existing.get("import_status") == "IMPORTED":
            summary.files_skipped += 1
            continue

        extract_dir = config.work_dir / run_token / zip_ctx["insurer_number"] / zip_path.stem
        extract_zip(zip_path, extract_dir)
        xml_files = collect_data_xml_files(extract_dir)
        summary.xml_seen += len(xml_files)

        download_zip_id = upsert_download_zip(
            cur,
            run_id=run_id,
            config=config,
            zip_ctx=zip_ctx,
            zip_sha256=zip_sha256,
            source_zip_path=zip_path,
        )

        xml_errors = 0
        xml_success = 0
        for xml_path in xml_files:
            parse_status = "PARSED"
            parse_reason = None
            try:
                row = _normalize_identity(parse_hia_download_xml(xml_path), config)
                errors = validate_download_xml_row(row)
                if errors:
                    parse_status = "ERROR"
                    parse_reason = ",".join(errors)
                    xml_errors += 1
                else:
                    xml_success += 1
            except Exception as exc:
                row = {}
                parse_status = "ERROR"
                parse_reason = f"{type(exc).__name__}: {exc}"
                xml_errors += 1

            hia_download_xml_id, action = upsert_download_xml(
                cur,
                run_id=run_id,
                config=config,
                download_zip_id=download_zip_id,
                zip_ctx=zip_ctx,
                row=row,
                xml_path=xml_path,
                extract_dir=extract_dir,
                parse_status=parse_status,
                parse_reason=parse_reason,
            )
            if action == "inserted":
                summary.xml_inserted += 1
            else:
                summary.xml_updated += 1

            if parse_status == "ERROR":
                continue

            person_year_id = upsert_person_year(
                cur,
                row,
                download_zip_id=download_zip_id,
                hia_download_xml_id=hia_download_xml_id,
                event_id=config.event_id,
            )
            summary.person_years_upserted += 1
            upsert_person_xml_event(
                cur,
                person_year_id=person_year_id,
                hia_download_xml_id=hia_download_xml_id,
                download_zip_id=download_zip_id,
            )
            summary.person_xml_events_upserted += 1

        status = "IMPORTED" if xml_errors == 0 else "ERROR"
        reason = None if xml_errors == 0 else f"xml_errors={xml_errors}"
        archive_path = archive_zip(zip_path, config, run_token=run_token, insurer_number=zip_ctx["insurer_number"])
        update_download_zip_result(
            cur,
            download_zip_id=download_zip_id,
            status=status,
            reason=reason,
            total=len(xml_files),
            success=xml_success,
            error=xml_errors,
            archive_zip_path=str(archive_path) if archive_path else None,
        )
        if status == "IMPORTED":
            summary.files_imported += 1
        else:
            summary.errors += 1

    return summary
