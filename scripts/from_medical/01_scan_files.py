#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase3 scan_files entry point.

Find ZIP/XML files under event.result_root_path and register new physical
receipts into health_exam_result.file_receipts.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, cast

import yaml
from mysql.connector import errorcode
from mysql.connector.errors import IntegrityError


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import PHR_MASTER
from scripts.lib.csv.exam_result_format_matcher import match_csv_format_for_file
from scripts.lib.etl import RunMetrics
from scripts.lib.etl import finish_run as etl_finish_run
from scripts.lib.etl import log_error as etl_log_error
from scripts.lib.etl import start_run as etl_start_run


HEALTH_EXAM_RESULT_DB = "health_exam_result"
DEV_PHR_DB = "dev_phr"
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config" / "scan_files.yml"

EDIT_FOLDER_NAME = "02_健診結果（編集）"
ETL_PHASE = "SCAN_FILES"
ETL_SOURCE = "FROM_MEDICAL"
ETL_STATUS_SUCCESS = "success"
ETL_STATUS_PARTIAL = "partial"
ETL_STATUS_FAILED = "failed"

FILE_ROLE = "FROM_MEDICAL"
STORAGE_FOLDER_TYPE = "MEDICAL_RESULT_ROOT"
FILE_STATUS_DISCOVERED = "DISCOVERED"
FILE_STATUS_READY = "READY"
FILE_STATUS_WAITING_CONFIRM = "WAITING_CONFIRM"
TARGET_EXTS = {"zip": "ZIP", "xml": "XML", "csv": "CSV"}
SKIP_PREFIXES = (".", "~$")
SKIP_SUFFIXES = (".tmp", ".part", ".crdownload")


@dataclass(frozen=True)
class ScanConfig:
    event_id: int
    health_db: str
    dev_db: str
    master_db: str
    limit: int
    chunk_size_mb: int
    dry_run: bool


@dataclass
class ScanSummary:
    event_id: int
    result_root_path: str | None = None
    aliases_total: int = 0
    aliases_active: int = 0
    aliases_inactive: int = 0
    aliases_manual: int = 0
    unknown_folders: int = 0
    edit_folders_missing: int = 0
    files_seen: int = 0
    files_target: int = 0
    files_inserted: int = 0
    files_duplicate: int = 0
    files_skipped: int = 0
    csv_format_matched: int = 0
    csv_format_not_found: int = 0
    csv_format_multiple: int = 0
    errors: int = 0
    fatal_error: bool = False
    ext_counts: dict[str, int] = field(default_factory=dict)
    type_counts: dict[str, int] = field(default_factory=dict)

    def bump_ext(self, ext: str) -> None:
        self.ext_counts[ext] = self.ext_counts.get(ext, 0) + 1

    def bump_type(self, file_type: str) -> None:
        self.type_counts[file_type] = self.type_counts.get(file_type, 0) + 1

    def to_message(self) -> str:
        parts = [
            f"event_id={self.event_id}",
            f"aliases={self.aliases_total}",
            f"active={self.aliases_active}",
            f"inserted={self.files_inserted}",
            f"duplicate={self.files_duplicate}",
            f"skipped={self.files_skipped}",
            f"csv_format_matched={self.csv_format_matched}",
            f"errors={self.errors}",
        ]
        if self.unknown_folders:
            parts.append(f"unknown_folders={self.unknown_folders}")
        if self.edit_folders_missing:
            parts.append(f"missing_edit_folders={self.edit_folders_missing}")
        if self.csv_format_not_found:
            parts.append(f"csv_format_not_found={self.csv_format_not_found}")
        if self.csv_format_multiple:
            parts.append(f"csv_format_multiple={self.csv_format_multiple}")
        return "scan_files " + " ".join(parts)

    def print(self) -> None:
        print(self.to_message())
        print(f"  result_root_path={self.result_root_path}")
        print(
            "  aliases: "
            f"total={self.aliases_total} active={self.aliases_active} "
            f"inactive={self.aliases_inactive} manual={self.aliases_manual}"
        )
        print(
            "  files: "
            f"seen={self.files_seen} target={self.files_target} "
            f"inserted={self.files_inserted} duplicate={self.files_duplicate} "
            f"skipped={self.files_skipped}"
        )
        print(f"  folders: unknown={self.unknown_folders} missing_edit={self.edit_folders_missing}")
        print(
            "  csv_format: "
            f"matched={self.csv_format_matched} not_found={self.csv_format_not_found} "
            f"multiple={self.csv_format_multiple}"
        )
        print(f"  errors={self.errors}")
        if self.ext_counts:
            print("  ext_counts=" + ", ".join(f"{k}:{v}" for k, v in sorted(self.ext_counts.items())))
        if self.type_counts:
            print("  file_type_counts=" + ", ".join(f"{k}:{v}" for k, v in sorted(self.type_counts.items())))

    def to_metrics(self) -> RunMetrics:
        return RunMetrics(
            files=self.files_target,
            rows_seen=self.files_seen,
            rows_inserted=self.files_inserted,
            rows_skipped=self.files_skipped + self.files_duplicate,
            errors=self.errors,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan medical result files into file_receipts.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Scan config YAML path.")
    parser.add_argument("--event-id", type=int, default=None, help="Override dev_phr.event.event_id.")
    parser.add_argument("--dry-run", action="store_true", help="Scan and report without DB writes.")
    parser.add_argument("--limit", type=int, default=None, help="Override maximum target files to process. 0 means unlimited.")
    parser.add_argument("--db-prefix", default="PHR_DB_", help="Environment prefix for DB connection.")
    parser.add_argument("--health-db", default=None, help="Override health_exam_result schema name.")
    parser.add_argument("--dev-db", default=None, help="Override dev_phr schema name.")
    parser.add_argument("--master-db", default=None, help="Override phr_master schema name.")
    parser.add_argument("--chunk-size-mb", type=int, default=None, help="Override SHA256 read chunk size in MiB.")
    return parser.parse_args()


def load_scan_config(path: str | Path) -> ScanConfig:
    with Path(path).open("r", encoding="utf-8") as fp:
        raw_data = yaml.safe_load(fp) or {}

    data = cast(Mapping[str, Any], raw_data)
    return ScanConfig(
        event_id=int(data.get("event_id", 2)),
        health_db=str(data.get("health_db") or HEALTH_EXAM_RESULT_DB),
        dev_db=str(data.get("dev_db") or DEV_PHR_DB),
        master_db=str(data.get("master_db") or PHR_MASTER),
        limit=int(data.get("limit", 0) or 0),
        chunk_size_mb=int(data.get("chunk_size_mb", 8) or 8),
        dry_run=bool(data.get("dry_run", False)),
    )


def resolve_config(args: argparse.Namespace) -> ScanConfig:
    config = load_scan_config(args.config)
    return ScanConfig(
        event_id=args.event_id if args.event_id is not None else config.event_id,
        health_db=args.health_db if args.health_db is not None else config.health_db,
        dev_db=args.dev_db if args.dev_db is not None else config.dev_db,
        master_db=args.master_db if args.master_db is not None else config.master_db,
        limit=args.limit if args.limit is not None else config.limit,
        chunk_size_mb=args.chunk_size_mb if args.chunk_size_mb is not None else config.chunk_size_mb,
        dry_run=True if args.dry_run else config.dry_run,
    )


def start_scan_run(cur: Any, *, config: ScanConfig) -> int:
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


def finish_scan_run(cur: Any, *, run_id: int, summary: ScanSummary, status: str) -> None:
    etl_finish_run(
        cur,
        run_id,
        summary.to_metrics(),
        status_override=status,
        extra_notes=summary.to_message(),
    )


def record_scan_error(
    cur: Any,
    *,
    run_id: int,
    field: str,
    error_code: str,
    message: str,
    field_value: str | None = None,
) -> None:
    src_file = None if field_value is None else field_value[:190]
    etl_log_error(
        cur,
        run_id,
        phase=ETL_PHASE,
        source=ETL_SOURCE,
        insurer_number=None,
        src_file=src_file,
        row_no=None,
        line_no=None,
        field=field,
        field_value=field_value,
        error_code=error_code,
        message=message,
    )


def get_result_root_path(cur: Any, *, dev_db: str, event_id: int) -> str | None:
    cur.execute(
        f"""
        SELECT result_root_path
        FROM `{dev_db}`.`event`
        WHERE event_id = %s
        """,
        (event_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    value = row.get("result_root_path")
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def fetch_aliases(cur: Any, *, master_db: str, event_id: int) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
            mfa.alias_id,
            mfa.src_folder_raw,
            mfa.dst_folder_norm,
            mfa.manual_judgement,
            mfa.is_active,
            mfa.note,
            ef.exam_facility_id,
            ef.exam_facility_code,
            ef.exam_facility_name
        FROM `{master_db}`.`medical_folder_aliases` mfa
        LEFT JOIN `{master_db}`.`exam_facilities` ef
          ON ef.exam_facility_id = mfa.exam_facility_id
        WHERE mfa.event_id = %s
        ORDER BY mfa.src_folder_raw
        """,
        (event_id,),
    )
    return list(cur.fetchall())


def sha256_file(path: Path, *, chunk_size: int) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fp:
        while True:
            chunk = fp.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def file_ext(path: Path) -> str:
    return path.suffix.lower().lstrip(".")


def is_target_standalone_xml(path: Path) -> bool:
    name = path.name.lower()
    if not name.endswith(".xml"):
        return False
    if name.startswith(("ix08", "su08")):
        return False
    if "schema" in name or "xsd" in name:
        return False
    return name.startswith("h")


def is_hidden_or_temp(path: Path) -> bool:
    name = path.name
    lower_name = name.lower()
    return name.startswith(SKIP_PREFIXES) or lower_name.endswith(SKIP_SUFFIXES)


def iter_files(edit_dir: Path) -> Iterable[Path]:
    for path in edit_dir.rglob("*"):
        if path.is_file():
            yield path


def relative_to_root(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def receipt_exists(cur: Any, *, event_id: int, relative_path: str, file_sha256: str) -> bool:
    cur.execute(
        """
        SELECT id
        FROM file_receipts
        WHERE event_id = %s
          AND relative_path = %s
          AND file_sha256 = %s
        LIMIT 1
        """,
        (event_id, relative_path, file_sha256),
    )
    return cur.fetchone() is not None


def insert_file_receipt(
    cur: Any,
    *,
    event_id: int,
    source_path: str,
    relative_path: str,
    path: Path,
    file_type: str,
    file_sha256: str,
    file_size: int,
    run_id: int,
    exam_facility_id: int | None,
    facility_code: str | None,
    facility_name: str | None,
    actual_header_sha256: str | None,
    matched_csv_format_version_id: int | None,
    status: str,
    summary_message: str | None,
) -> int:
    cur.execute(
        """
        INSERT INTO file_receipts (
            event_id,
            file_role,
            file_type,
            file_name,
            file_ext,
            source_path,
            relative_path,
            file_sha256,
            file_size,
            facility_code,
            facility_name,
            exam_facility_id,
            actual_header_sha256,
            matched_csv_format_version_id,
            processable_count,
            storage_folder_type,
            status,
            summary_message,
            etl_run_id,
            first_seen_at,
            last_seen_at,
            received_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, %s, %s, %s, %s,
            CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3)
        )
        """,
        (
            event_id,
            FILE_ROLE,
            file_type,
            path.name,
            file_ext(path),
            source_path,
            relative_path,
            file_sha256,
            file_size,
            facility_code,
            facility_name,
            exam_facility_id,
            actual_header_sha256,
            matched_csv_format_version_id,
            STORAGE_FOLDER_TYPE,
            status,
            summary_message,
            run_id,
        ),
    )
    return int(cur.lastrowid)


def is_duplicate_key_error(exc: Exception) -> bool:
    return isinstance(exc, IntegrityError) and getattr(exc, "errno", None) == errorcode.ER_DUP_ENTRY


def scan_unknown_folders(
    cur: Any,
    *,
    run_id: int | None,
    root: Path,
    alias_names: set[str],
    summary: ScanSummary,
    dry_run: bool,
) -> None:
    try:
        children = [p for p in root.iterdir() if p.is_dir()]
    except OSError as exc:
        summary.errors += 1
        if not dry_run and run_id is not None:
            record_scan_error(
                cur,
                run_id=run_id,
                field="SCAN_PRECONDITION",
                error_code="RESULT_ROOT_PATH_READ_FAILED",
                message=f"result_root_path cannot be listed: {root}: {exc}",
                field_value=str(root),
            )
        return

    for child in children:
        if child.name in alias_names:
            continue
        summary.unknown_folders += 1
        summary.errors += 1
        if not dry_run and run_id is not None:
            record_scan_error(
                cur,
                run_id=run_id,
                field="FOLDER_SCAN",
                error_code="UNKNOWN_MEDICAL_FOLDER",
                message=f"unknown medical folder: {child}",
                field_value=str(child),
            )


def scan_alias_files(
    cur: Any,
    *,
    run_id: int | None,
    event_id: int,
    root: Path,
    alias: dict[str, Any],
    summary: ScanSummary,
    dry_run: bool,
    limit: int,
    chunk_size: int,
    master_db: str,
) -> bool:
    if limit and summary.files_target >= limit:
        return False

    src_folder_raw = str(alias["src_folder_raw"])
    edit_dir = root / src_folder_raw / EDIT_FOLDER_NAME
    if not edit_dir.exists() or not edit_dir.is_dir():
        summary.edit_folders_missing += 1
        summary.errors += 1
        if not dry_run and run_id is not None:
            record_scan_error(
                cur,
                run_id=run_id,
                field="FOLDER_SCAN",
                error_code="EDIT_FOLDER_NOT_FOUND",
                message=f"edit folder not found: {edit_dir}",
                field_value=str(edit_dir),
            )
        return True

    for path in iter_files(edit_dir):
        summary.files_seen += 1
        ext = file_ext(path)
        summary.bump_ext(ext or "(none)")

        if is_hidden_or_temp(path) or ext not in TARGET_EXTS:
            summary.files_skipped += 1
            continue

        file_type = TARGET_EXTS[ext]
        if file_type == "XML" and not is_target_standalone_xml(path):
            summary.files_skipped += 1
            continue

        summary.files_target += 1
        summary.bump_type(file_type)

        try:
            stat = path.stat()
            file_hash = sha256_file(path, chunk_size=chunk_size)
            rel_path = relative_to_root(path, root)
        except OSError as exc:
            summary.errors += 1
            if not dry_run and run_id is not None:
                record_scan_error(
                    cur,
                    run_id=run_id,
                    field="FILE_SCAN",
                    error_code="FILE_READ_FAILED",
                    message=f"file read failed: {path}: {exc}",
                    field_value=str(path),
                )
            continue

        if receipt_exists(cur, event_id=event_id, relative_path=rel_path, file_sha256=file_hash):
            summary.files_duplicate += 1
            continue

        if dry_run:
            summary.files_inserted += 1
            if limit and summary.files_target >= limit:
                return False
            continue

        if run_id is None:
            raise RuntimeError("run_id is required when registering file_receipts")

        exam_facility_id = int(alias["exam_facility_id"]) if alias.get("exam_facility_id") is not None else None
        receipt_status = FILE_STATUS_DISCOVERED
        summary_message: str | None = None
        actual_header_sha256: str | None = None
        matched_csv_format_version_id: int | None = None

        if file_type == "CSV":
            match_result = match_csv_format_for_file(
                cur,
                source_path=str(path),
                exam_facility_id=exam_facility_id,
                master_db=master_db,
            )
            actual_header_sha256 = match_result.actual_header_sha256
            matched_csv_format_version_id = match_result.csv_format_version_id
            summary_message = match_result.message
            if match_result.result == "MATCHED":
                receipt_status = FILE_STATUS_READY
                summary.csv_format_matched += 1
            elif match_result.result == "MULTIPLE":
                receipt_status = FILE_STATUS_WAITING_CONFIRM
                summary.csv_format_multiple += 1
                summary.errors += 1
                record_scan_error(
                    cur,
                    run_id=run_id,
                    field="CSV_FORMAT_MATCH",
                    error_code="CSV_FORMAT_MULTIPLE",
                    message=match_result.message,
                    field_value=str(path),
                )
            else:
                receipt_status = FILE_STATUS_WAITING_CONFIRM
                summary.csv_format_not_found += 1
                summary.errors += 1
                record_scan_error(
                    cur,
                    run_id=run_id,
                    field="CSV_FORMAT_MATCH",
                    error_code=f"CSV_FORMAT_{match_result.result}",
                    message=match_result.message,
                    field_value=str(path),
                )

        try:
            insert_file_receipt(
                cur,
                event_id=event_id,
                source_path=str(path),
                relative_path=rel_path,
                path=path,
                file_type=file_type,
                file_sha256=file_hash,
                file_size=int(stat.st_size),
                run_id=run_id,
                exam_facility_id=exam_facility_id,
                facility_code=alias.get("exam_facility_code"),
                facility_name=alias.get("exam_facility_name"),
                actual_header_sha256=actual_header_sha256,
                matched_csv_format_version_id=matched_csv_format_version_id,
                status=receipt_status,
                summary_message=summary_message,
            )
            summary.files_inserted += 1
        except Exception as exc:
            if is_duplicate_key_error(exc):
                summary.files_duplicate += 1
                continue
            summary.errors += 1
            record_scan_error(
                cur,
                run_id=run_id,
                field="DB_WRITE",
                error_code="FILE_RECEIPT_INSERT_FAILED",
                message=f"file_receipt insert failed: {path}: {exc}",
                field_value=str(path),
            )

        if limit and summary.files_target >= limit:
            return False

    return True


def run_scan(conn: Any, config: ScanConfig) -> ScanSummary:
    summary = ScanSummary(event_id=config.event_id)
    chunk_size = max(config.chunk_size_mb, 1) * 1024 * 1024
    run_id: int | None = None

    cur = dict_cursor(conn)
    try:
        if not config.dry_run:
            run_id = start_scan_run(cur, config=config)
            conn.commit()

        try:
            root_text = get_result_root_path(cur, dev_db=config.dev_db, event_id=config.event_id)
            summary.result_root_path = root_text
            if not root_text:
                summary.errors += 1
                summary.fatal_error = True
                if not config.dry_run and run_id is not None:
                    record_scan_error(
                        cur,
                        run_id=run_id,
                        field="SCAN_PRECONDITION",
                        error_code="RESULT_ROOT_PATH_MISSING",
                        message=f"result_root_path is not set: event_id={config.event_id}",
                        field_value=f"event_id={config.event_id}",
                    )
                    finish_scan_run(cur, run_id=run_id, summary=summary, status=ETL_STATUS_FAILED)
                    conn.commit()
                return summary

            root = Path(root_text).expanduser()
            if not root.exists() or not root.is_dir():
                summary.errors += 1
                summary.fatal_error = True
                if not config.dry_run and run_id is not None:
                    record_scan_error(
                        cur,
                        run_id=run_id,
                        field="SCAN_PRECONDITION",
                        error_code="RESULT_ROOT_PATH_NOT_FOUND",
                        message=f"result_root_path is not a directory: {root}",
                        field_value=str(root),
                    )
                    finish_scan_run(cur, run_id=run_id, summary=summary, status=ETL_STATUS_FAILED)
                    conn.commit()
                return summary

            aliases = fetch_aliases(cur, master_db=config.master_db, event_id=config.event_id)
            summary.aliases_total = len(aliases)
            alias_names = {str(row["src_folder_raw"]) for row in aliases}
            scan_unknown_folders(
                cur,
                run_id=run_id,
                root=root,
                alias_names=alias_names,
                summary=summary,
                dry_run=config.dry_run,
            )

            keep_scanning = True
            for alias in aliases:
                is_active = int(alias.get("is_active") or 0)
                manual_judgement = int(alias.get("manual_judgement") or 0)
                src_folder_raw = str(alias["src_folder_raw"])

                if not is_active:
                    summary.aliases_inactive += 1
                    summary.errors += 1
                    if not config.dry_run and run_id is not None:
                        record_scan_error(
                            cur,
                            run_id=run_id,
                            field="FOLDER_ALIAS",
                            error_code="ALIAS_INACTIVE",
                            message=f"inactive alias skipped: {src_folder_raw}",
                            field_value=src_folder_raw,
                        )
                    continue

                if manual_judgement:
                    summary.aliases_manual += 1
                    summary.errors += 1
                    if not config.dry_run and run_id is not None:
                        record_scan_error(
                            cur,
                            run_id=run_id,
                            field="FOLDER_ALIAS",
                            error_code="ALIAS_MANUAL_JUDGEMENT",
                            message=f"manual_judgement alias skipped: {src_folder_raw}",
                            field_value=src_folder_raw,
                    )
                    continue

                if alias.get("exam_facility_id") is None:
                    summary.errors += 1
                    if not config.dry_run and run_id is not None:
                        record_scan_error(
                            cur,
                            run_id=run_id,
                            field="FOLDER_ALIAS",
                            error_code="EXAM_FACILITY_UNRESOLVED",
                            message=f"exam facility unresolved for alias: {src_folder_raw}",
                            field_value=src_folder_raw,
                        )

                summary.aliases_active += 1
                keep_scanning = scan_alias_files(
                    cur,
                    run_id=run_id,
                    event_id=config.event_id,
                    root=root,
                    alias=alias,
                    summary=summary,
                    dry_run=config.dry_run,
                    limit=config.limit,
                    chunk_size=chunk_size,
                    master_db=config.master_db,
                )
                if not keep_scanning:
                    break

            if not config.dry_run and run_id is not None:
                status = ETL_STATUS_PARTIAL if summary.errors else ETL_STATUS_SUCCESS
                finish_scan_run(cur, run_id=run_id, summary=summary, status=status)
                conn.commit()

            return summary
        except Exception:
            conn.rollback()
            if not config.dry_run and run_id is not None:
                error_cur = dict_cursor(conn)
                try:
                    record_scan_error(
                        error_cur,
                        run_id=run_id,
                        field="UNEXPECTED",
                        error_code="UNEXPECTED_SCAN_ERROR",
                        message="unexpected scan error",
                    )
                    finish_scan_run(
                        error_cur,
                        run_id=run_id,
                        summary=summary,
                        status=ETL_STATUS_FAILED,
                    )
                    conn.commit()
                finally:
                    error_cur.close()
            raise
    finally:
        cur.close()


def main() -> int:
    args = parse_args()
    config = resolve_config(args)
    params = load_mysql_base_params(args.db_prefix)

    with connect_ctx(params, database=config.health_db, autocommit=False) as conn:
        summary = run_scan(conn, config)
        if config.dry_run:
            conn.rollback()
        summary.print()
        return 1 if summary.fatal_error else 0


if __name__ == "__main__":
    raise SystemExit(main())
