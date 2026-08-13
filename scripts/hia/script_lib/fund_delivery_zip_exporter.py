from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
import csv
import hashlib
import re
import shutil
import tempfile
from typing import Any
import xml.etree.ElementTree as ET
import zipfile


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "fund_delivery" / "output"
DEFAULT_XSD_DIR = PROJECT_ROOT / "scripts" / "from_medical" / "source" / "XSD" / "mhlw_v4_20230331_v08"
NS_MHLW = "https://www.mhlw.go.jp/stf/seisakunitsuite/bunya/0000161103.html"
NS_XSI = "http://www.w3.org/2001/XMLSchema-instance"
OID_FACILITY = "1.2.392.200119.6.102"
OID_INSURER = "1.2.392.200119.6.101"


@dataclass(frozen=True)
class FundDeliveryZipExportConfig:
    delivery_list_id: int
    output_base_dir: Path = DEFAULT_OUTPUT_DIR
    xsd_dir: Path = DEFAULT_XSD_DIR
    delivery_date: str | None = None
    output_seq: int = 0
    service_event_type_code: str = "1"
    created_by: str | None = None
    dry_run: bool = True


@dataclass
class FundDeliveryZipExportSummary:
    delivery_list_id: int
    delivery_run_id: int | None = None
    members_seen: int = 0
    members_written: int = 0
    output_zip_name: str | None = None
    output_zip_path: str | None = None
    output_zip_sha256: str | None = None
    summary_csv_path: str | None = None
    members_csv_path: str | None = None
    person_raw_csv_path: str | None = None
    source_zip_count: int = 0
    report_category_10_count: int = 0
    errors: int = 0


def _today_yyyymmdd() -> str:
    return date.today().strftime("%Y%m%d")


def _h(tag: str) -> str:
    return f"{{{NS_MHLW}}}{tag}"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fp:
        for chunk in iter(lambda: fp.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _pick_existing_zip_path(row: dict[str, Any]) -> Path:
    for key in ("archive_zip_path", "source_zip_path"):
        raw = row.get(key)
        if raw:
            path = Path(str(raw))
            if path.exists():
                return path
    raise FileNotFoundError(
        "source ZIP not found: "
        f"download_zip_id={row.get('download_zip_id')} "
        f"source={row.get('source_zip_path')} archive={row.get('archive_zip_path')}"
    )


def load_delivery_list(cur: Any, delivery_list_id: int) -> dict[str, Any]:
    cur.execute(
        """
        SELECT *
          FROM fund_delivery_lists
         WHERE delivery_list_id = %s
        """,
        (delivery_list_id,),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError(f"fund_delivery_list not found: delivery_list_id={delivery_list_id}")
    if row["grouping_mode"] != "ALL":
        raise ValueError(
            f"grouping_mode={row['grouping_mode']} is not supported by 03 export yet. "
            "Use grouping_mode=ALL for the initial fund delivery flow."
        )
    return row


def load_delivery_members(cur: Any, delivery_list_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
            lm.delivery_list_member_id,
            lm.person_year_id,
            lm.delivery_candidate_id,
            lm.hia_download_xml_id,
            c.person_xml_event_id,
            py.identity_hash,
            py.birthdate,
            py.exam_year,
            x.download_zip_id,
            x.xml_filename AS source_xml_filename,
            x.xml_inner_path,
            x.xml_sha256 AS source_xml_sha256,
            x.exam_date,
            x.exam_month,
            x.facility_code,
            x.facility_name,
            x.report_category_code,
            x.program_type_code,
            x.parse_status,
            z.zip_name AS source_zip_name,
            z.source_zip_path,
            z.archive_zip_path,
            z.dl_date,
            z.send_seq
          FROM fund_delivery_list_members lm
          JOIN hia_person_years py
            ON py.person_year_id = lm.person_year_id
          JOIN hia_download_xmls x
            ON x.hia_download_xml_id = lm.hia_download_xml_id
          JOIN hia_download_zips z
            ON z.download_zip_id = x.download_zip_id
          LEFT JOIN fund_delivery_xml_candidates c
            ON c.delivery_candidate_id = lm.delivery_candidate_id
         WHERE lm.delivery_list_id = %s
           AND lm.list_member_status = 'INCLUDED'
         ORDER BY
            x.exam_month,
            x.facility_code,
            lm.person_year_id,
            x.hia_download_xml_id
        """,
        (delivery_list_id,),
    )
    rows = list(cur.fetchall() or [])
    for row in rows:
        if row["parse_status"] != "PARSED":
            raise ValueError(
                f"delivery list contains non-PARSED XML: "
                f"hia_download_xml_id={row['hia_download_xml_id']} parse_status={row['parse_status']}"
            )
    return rows


def _output_dir(config: FundDeliveryZipExportConfig, list_row: dict[str, Any], run_stamp: str) -> Path:
    base = config.output_base_dir / run_stamp
    if list_row["output_mode"] == "EXAM_MONTH" and list_row.get("exam_month"):
        return base / str(list_row["exam_month"])
    return base


def _output_zip_name(config: FundDeliveryZipExportConfig, list_row: dict[str, Any]) -> str:
    delivery_date = config.delivery_date or _today_yyyymmdd()
    sender_code = str(list_row["sender_code"])
    insurer_number = str(list_row["insurer_number"])
    return f"{sender_code}_{insurer_number}_{delivery_date}{config.output_seq}_{config.service_event_type_code}.zip"


def _output_data_xml_name(config: FundDeliveryZipExportConfig, list_row: dict[str, Any], serial_no: int) -> str:
    delivery_date = config.delivery_date or _today_yyyymmdd()
    sender_code = str(list_row["sender_code"])
    return (
        f"h{sender_code}{delivery_date}{config.output_seq}"
        f"{config.service_event_type_code}{serial_no:06d}.xml"
    )


def _read_zip_member(row: dict[str, Any]) -> bytes:
    zip_path = _pick_existing_zip_path(row)
    with zipfile.ZipFile(zip_path, "r") as zf:
        return zf.read(str(row["xml_inner_path"]))


def _find_support_text(rows: list[dict[str, Any]], filename_prefix: str) -> str | None:
    for row in rows:
        zip_path = _pick_existing_zip_path(row)
        with zipfile.ZipFile(zip_path, "r") as zf:
            for name in zf.namelist():
                if Path(name).name.lower().startswith(filename_prefix.lower()):
                    return zf.read(name).decode("utf-8")
    return None


def _replace_or_insert_value(xml_text: str, tag: str, value: int) -> str | None:
    pattern = rf'(<(?:[A-Za-z0-9_]+:)?{tag}\b[^>]*\bvalue=")[^"]*(")'
    replaced = re.sub(pattern, rf"\g<1>{value}\2", xml_text, count=1)
    if replaced != xml_text:
        return replaced
    return None


def _build_ix08_text(sender_code: str, receiver_code: str, file_date: str, total_count: int) -> str:
    ET.register_namespace("", NS_MHLW)
    ET.register_namespace("xsi", NS_XSI)
    root = ET.Element(_h("index"), {f"{{{NS_XSI}}}schemaLocation": f"{NS_MHLW} ./XSD/ix08_V08.xsd"})
    ET.SubElement(root, _h("interactionType"), {"code": "6"})
    ET.SubElement(root, _h("creationTime"), {"value": file_date})
    sender = ET.SubElement(root, _h("sender"))
    ET.SubElement(sender, _h("id"), {"root": OID_FACILITY, "extension": sender_code})
    receiver = ET.SubElement(root, _h("receiver"))
    ET.SubElement(receiver, _h("id"), {"root": OID_INSURER, "extension": receiver_code})
    ET.SubElement(root, _h("serviceEventType"), {"code": "1"})
    ET.SubElement(root, _h("totalRecordCount"), {"value": str(total_count)})
    ET.indent(ET.ElementTree(root), space="  ")
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


def _build_su08_text(total_subject_count: int) -> str:
    ET.register_namespace("", NS_MHLW)
    ET.register_namespace("xsi", NS_XSI)
    root = ET.Element(_h("summary"), {f"{{{NS_XSI}}}schemaLocation": f"{NS_MHLW} ./XSD/su08_V08.xsd"})
    ET.SubElement(root, _h("serviceEventType"), {"code": "1"})
    ET.SubElement(root, _h("totalSubjectCount"), {"value": str(total_subject_count)})
    ET.SubElement(root, _h("totalCostAmount"), {"value": "0"})
    ET.SubElement(root, _h("totalPaymentAmount"), {"value": "0"})
    ET.SubElement(root, _h("totalClaimAmount"), {"value": "0"})
    ET.indent(ET.ElementTree(root), space="  ")
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


def build_ix08_text(rows: list[dict[str, Any]], list_row: dict[str, Any], config: FundDeliveryZipExportConfig) -> str:
    total_count = len(rows)
    source_text = _find_support_text(rows, "ix08")
    if source_text:
        replaced = _replace_or_insert_value(source_text, "totalRecordCount", total_count)
        if replaced:
            return replaced
    return _build_ix08_text(
        sender_code=str(list_row["sender_code"]),
        receiver_code=str(list_row["insurer_number"]),
        file_date=config.delivery_date or _today_yyyymmdd(),
        total_count=total_count,
    )


def build_su08_text(rows: list[dict[str, Any]]) -> str:
    total_subject_count = sum(1 for row in rows if str(row.get("report_category_code") or "") == "10")
    source_text = _find_support_text(rows, "su08")
    if source_text:
        replaced = _replace_or_insert_value(source_text, "totalSubjectCount", total_subject_count)
        if replaced:
            return replaced
    return _build_su08_text(total_subject_count)


def _copy_xsd_dir(source: Path, selected_root: Path) -> None:
    if not source.exists():
        return
    destination = selected_root / "XSD"
    shutil.copytree(source, destination, dirs_exist_ok=True)


def _write_payload_dir(
    selected_root: Path,
    rows: list[dict[str, Any]],
    list_row: dict[str, Any],
    config: FundDeliveryZipExportConfig,
) -> list[dict[str, Any]]:
    data_dir = selected_root / "DATA"
    data_dir.mkdir(parents=True, exist_ok=True)
    written_rows: list[dict[str, Any]] = []
    for index, row in enumerate(rows, start=1):
        output_filename = _output_data_xml_name(config, list_row, index)
        content = _read_zip_member(row)
        output_path = data_dir / output_filename
        output_path.write_bytes(content)
        written = dict(row)
        written["output_xml_filename"] = output_filename
        written["output_xml_sha256"] = hashlib.sha256(content).hexdigest()
        written_rows.append(written)

    (selected_root / "ix08_V08.xml").write_text(build_ix08_text(rows, list_row, config), encoding="utf-8")
    (selected_root / "su08_V08.xml").write_text(build_su08_text(rows), encoding="utf-8")
    _copy_xsd_dir(config.xsd_dir, selected_root)
    return written_rows


def _zip_dir(source_dir: Path, output_zip_path: Path) -> None:
    archive_root = source_dir.name
    with zipfile.ZipFile(output_zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(source_dir.rglob("*")):
            if path.is_file():
                archive_path = Path(archive_root) / path.relative_to(source_dir)
                zf.write(path, archive_path.as_posix())


def create_delivery_run(
    cur: Any,
    *,
    list_row: dict[str, Any],
    config: FundDeliveryZipExportConfig,
    etl_run_id: int,
    output_zip_name: str,
    output_zip_path: Path,
    member_count: int,
    source_zip_count: int,
) -> int:
    cur.execute(
        """
        INSERT INTO fund_delivery_runs (
            etl_run_id, delivery_list_id, event_id, insurer_number,
            output_mode, exam_month, grouping_mode, sender_code, sender_name,
            delivery_policy, same_exam_date_policy, include_delivery_status,
            output_zip_name, output_zip_path, delivery_status,
            delivery_xml_count, delivery_person_count, source_download_zip_id,
            created_by, note
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, 'CREATED',
            %s, %s, NULL,
            %s, %s
        )
        """,
        (
            etl_run_id,
            list_row["delivery_list_id"],
            list_row["event_id"],
            list_row["insurer_number"],
            list_row["output_mode"],
            list_row["exam_month"],
            list_row["grouping_mode"],
            list_row["sender_code"],
            list_row["sender_name"],
            list_row["delivery_policy"],
            list_row["same_exam_date_policy"],
            list_row["include_delivery_status"],
            output_zip_name,
            str(output_zip_path),
            member_count,
            member_count,
            config.created_by,
            f"source_zip_count={source_zip_count}; output_seq={config.output_seq}; "
            f"service_event_type_code={config.service_event_type_code}",
        ),
    )
    return int(cur.lastrowid)


def insert_delivery_members(cur: Any, *, delivery_run_id: int, rows: list[dict[str, Any]]) -> int:
    count = 0
    for row in rows:
        cur.execute(
            """
            INSERT INTO fund_delivery_members (
                delivery_run_id, person_year_id, hia_download_xml_id, delivery_candidate_id,
                person_xml_event_id, xml_filename, xml_sha256, facility_code, facility_name,
                exam_date, exam_month, report_category_code, program_type_code,
                member_status, member_reason
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                'CREATED', %s
            )
            """,
            (
                delivery_run_id,
                row["person_year_id"],
                row["hia_download_xml_id"],
                row["delivery_candidate_id"],
                row["person_xml_event_id"],
                row["output_xml_filename"],
                row["output_xml_sha256"],
                row["facility_code"],
                row["facility_name"],
                row["exam_date"],
                row["exam_month"],
                row["report_category_code"],
                row["program_type_code"],
                f"source_xml={row['source_xml_filename']}; source_zip={row['source_zip_name']}",
            ),
        )
        count += 1
    return count


def finalize_delivery_run(cur: Any, *, delivery_run_id: int, output_zip_sha256: str) -> None:
    cur.execute(
        """
        UPDATE fund_delivery_runs
           SET output_zip_sha256 = %s,
               updated_at = CURRENT_TIMESTAMP(3)
         WHERE delivery_run_id = %s
        """,
        (output_zip_sha256, delivery_run_id),
    )


def _csv_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _fiscal_year_end_age(birthdate: Any, exam_year: Any) -> str:
    born = _parse_date(birthdate)
    if born is None or exam_year in (None, ""):
        return ""
    try:
        year = int(exam_year)
    except (TypeError, ValueError):
        return ""
    as_of = date(year + 1, 3, 31)
    age = as_of.year - born.year - ((as_of.month, as_of.day) < (born.month, born.day))
    return str(age)


def _write_output_csvs(
    output_dir: Path,
    *,
    delivery_run_id: int,
    list_row: dict[str, Any],
    output_zip_name: str,
    rows: list[dict[str, Any]],
) -> tuple[Path, Path, Path]:
    summary_path = output_dir / f"{Path(output_zip_name).stem}_summary.csv"
    members_path = output_dir / f"{Path(output_zip_name).stem}_members.csv"
    person_raw_path = output_dir / f"{Path(output_zip_name).stem}_person_raw.csv"

    grouped: dict[tuple[str, str, str], int] = {}
    for row in rows:
        key = (
            _csv_text(row.get("exam_month")),
            _csv_text(row.get("facility_code")),
            _csv_text(row.get("facility_name")),
        )
        grouped[key] = grouped.get(key, 0) + 1

    with summary_path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.writer(fp)
        writer.writerow([
            "delivery_run_id",
            "delivery_list_id",
            "output_zip_name",
            "insurer_number",
            "exam_month",
            "facility_code",
            "facility_name",
            "person_count",
        ])
        for (exam_month, facility_code, facility_name), count in sorted(grouped.items()):
            writer.writerow([
                delivery_run_id,
                list_row["delivery_list_id"],
                output_zip_name,
                list_row["insurer_number"],
                exam_month,
                facility_code,
                facility_name,
                count,
            ])

    with members_path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.writer(fp)
        writer.writerow([
            "delivery_run_id",
            "delivery_list_id",
            "output_zip_name",
            "output_xml_filename",
            "person_year_id",
            "hia_download_xml_id",
            "person_xml_event_id",
            "exam_date",
            "exam_month",
            "facility_code",
            "facility_name",
            "report_category_code",
            "program_type_code",
            "source_zip_name",
            "source_xml_filename",
            "source_xml_sha256",
            "output_xml_sha256",
        ])
        for row in rows:
            writer.writerow([
                delivery_run_id,
                list_row["delivery_list_id"],
                output_zip_name,
                row["output_xml_filename"],
                row["person_year_id"],
                row["hia_download_xml_id"],
                row["person_xml_event_id"],
                row["exam_date"],
                row["exam_month"],
                row["facility_code"],
                row["facility_name"],
                row["report_category_code"],
                row["program_type_code"],
                row["source_zip_name"],
                row["source_xml_filename"],
                row["source_xml_sha256"],
                row["output_xml_sha256"],
            ])

    with person_raw_path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.writer(fp)
        writer.writerow([
            "delivery_run_id",
            "delivery_list_id",
            "output_zip_name",
            "output_xml_filename",
            "person_year_id",
            "identity_hash",
            "facility_code",
            "facility_name",
            "exam_month",
            "exam_date",
            "birthdate",
            "fiscal_year_end_age",
            "report_category_code",
            "program_type_code",
            "source_zip_name",
            "source_xml_filename",
            "source_xml_sha256",
            "output_xml_sha256",
        ])
        for row in rows:
            writer.writerow([
                delivery_run_id,
                list_row["delivery_list_id"],
                output_zip_name,
                row["output_xml_filename"],
                row["person_year_id"],
                row["identity_hash"],
                row["facility_code"],
                row["facility_name"],
                row["exam_month"],
                row["exam_date"],
                row["birthdate"],
                _fiscal_year_end_age(row.get("birthdate"), row.get("exam_year")),
                row["report_category_code"],
                row["program_type_code"],
                row["source_zip_name"],
                row["source_xml_filename"],
                row["source_xml_sha256"],
                row["output_xml_sha256"],
            ])

    return summary_path, members_path, person_raw_path


def export_fund_delivery_zip(
    cur: Any,
    *,
    config: FundDeliveryZipExportConfig,
    etl_run_id: int,
) -> FundDeliveryZipExportSummary:
    list_row = load_delivery_list(cur, config.delivery_list_id)
    rows = load_delivery_members(cur, config.delivery_list_id)
    summary = FundDeliveryZipExportSummary(delivery_list_id=config.delivery_list_id)
    summary.members_seen = len(rows)
    summary.source_zip_count = len({row["download_zip_id"] for row in rows})
    summary.report_category_10_count = sum(1 for row in rows if str(row.get("report_category_code") or "") == "10")

    if not rows:
        raise ValueError(f"delivery list has no INCLUDED members: delivery_list_id={config.delivery_list_id}")

    run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = _output_dir(config, list_row, run_stamp)
    output_zip_name = _output_zip_name(config, list_row)
    output_zip_path = output_dir / output_zip_name
    summary.output_zip_name = output_zip_name
    summary.output_zip_path = str(output_zip_path)

    if config.dry_run:
        summary.members_written = len(rows)
        return summary

    output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="fund_delivery_", dir=str(output_dir)) as tmp:
        payload_root = Path(tmp) / output_zip_path.stem
        payload_root.mkdir(parents=True, exist_ok=True)
        written_rows = _write_payload_dir(payload_root, rows, list_row, config)
        if output_zip_path.exists():
            output_zip_path.unlink()
        _zip_dir(payload_root, output_zip_path)

    output_zip_sha256 = _sha256_file(output_zip_path)
    delivery_run_id = create_delivery_run(
        cur,
        list_row=list_row,
        config=config,
        etl_run_id=etl_run_id,
        output_zip_name=output_zip_name,
        output_zip_path=output_zip_path,
        member_count=len(written_rows),
        source_zip_count=summary.source_zip_count,
    )
    inserted = insert_delivery_members(cur, delivery_run_id=delivery_run_id, rows=written_rows)
    finalize_delivery_run(cur, delivery_run_id=delivery_run_id, output_zip_sha256=output_zip_sha256)
    summary_csv_path, members_csv_path, person_raw_csv_path = _write_output_csvs(
        output_dir,
        delivery_run_id=delivery_run_id,
        list_row=list_row,
        output_zip_name=output_zip_name,
        rows=written_rows,
    )

    summary.delivery_run_id = delivery_run_id
    summary.members_written = inserted
    summary.output_zip_sha256 = output_zip_sha256
    summary.summary_csv_path = str(summary_csv_path)
    summary.members_csv_path = str(members_csv_path)
    summary.person_raw_csv_path = str(person_raw_csv_path)
    return summary
