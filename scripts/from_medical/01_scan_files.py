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
from typing import Any, Iterable

from mysql.connector import errorcode
from mysql.connector.errors import IntegrityError


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor


HEALTH_EXAM_RESULT_DB = "health_exam_result"
DEV_PHR_DB = "dev_phr"

EDIT_FOLDER_NAME = "02_健診結果（編集）"
RUN_TYPE = "SCAN_FILES"
STATUS_RUNNING = "RUNNING"
STATUS_SUCCESS = "SUCCESS"
STATUS_WARNING = "WARNING"
STATUS_ERROR = "ERROR"
ERROR_STATUS_OPEN = "OPEN"

FILE_ROLE = "FROM_MEDICAL"
STORAGE_FOLDER_TYPE = "MEDICAL_RESULT_ROOT"
FILE_STATUS_DISCOVERED = "DISCOVERED"
TARGET_EXTS = {"zip": "ZIP", "xml": "XML"}
SKIP_PREFIXES = (".", "~$")
SKIP_SUFFIXES = (".tmp", ".part", ".crdownload")


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
            f"errors={self.errors}",
        ]
        if self.unknown_folders:
            parts.append(f"unknown_folders={self.unknown_folders}")
        if self.edit_folders_missing:
            parts.append(f"missing_edit_folders={self.edit_folders_missing}")
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
        print(f"  errors={self.errors}")
        if self.ext_counts:
            print("  ext_counts=" + ", ".join(f"{k}:{v}" for k, v in sorted(self.ext_counts.items())))
        if self.type_counts:
            print("  file_type_counts=" + ", ".join(f"{k}:{v}" for k, v in sorted(self.type_counts.items())))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan medical result files into file_receipts.")
    parser.add_argument("--event-id", type=int, required=True, help="dev_phr.event.event_id")
    parser.add_argument("--dry-run", action="store_true", help="Scan and report without DB writes.")
    parser.add_argument("--limit", type=int, default=0, help="Maximum target files to process. 0 means unlimited.")
    parser.add_argument("--db-prefix", default="PHR_DB_", help="Environment prefix for DB connection.")
    parser.add_argument("--health-db", default=HEALTH_EXAM_RESULT_DB, help="health_exam_result schema name.")
    parser.add_argument("--dev-db", default=DEV_PHR_DB, help="dev_phr schema name.")
    parser.add_argument("--chunk-size-mb", type=int, default=8, help="SHA256 read chunk size in MiB.")
    return parser.parse_args()


def start_run(cur: Any, *, event_id: int) -> int:
    cur.execute(
        """
        INSERT INTO etl_runs (run_type, event_id, status, summary_message)
        VALUES (%s, %s, %s, %s)
        """,
        (RUN_TYPE, event_id, STATUS_RUNNING, "scan_files started"),
    )
    return int(cur.lastrowid)


def finish_run(cur: Any, *, run_id: int, status: str, summary_message: str) -> None:
    cur.execute(
        """
        UPDATE etl_runs
        SET status = %s,
            finished_at = CURRENT_TIMESTAMP(3),
            summary_message = %s
        WHERE id = %s
        """,
        (status, summary_message, run_id),
    )


def log_error(
    cur: Any,
    *,
    run_id: int,
    error_type: str,
    error_code: str,
    message: str,
    file_receipt_id: int | None = None,
) -> None:
    cur.execute(
        """
        INSERT INTO etl_errors (
            run_id,
            file_receipt_id,
            error_type,
            error_code,
            error_message,
            status
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (run_id, file_receipt_id, error_type, error_code, message, ERROR_STATUS_OPEN),
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


def fetch_aliases(cur: Any, *, event_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT alias_id, src_folder_raw, dst_folder_norm, manual_judgement, is_active, note
        FROM medical_folder_aliases
        WHERE event_id = %s
        ORDER BY src_folder_raw
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
            processable_count,
            storage_folder_type,
            status,
            etl_run_id,
            first_seen_at,
            last_seen_at,
            received_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, %s, %s, %s,
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
            STORAGE_FOLDER_TYPE,
            FILE_STATUS_DISCOVERED,
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
            log_error(
                cur,
                run_id=run_id,
                error_type="SCAN_PRECONDITION",
                error_code="RESULT_ROOT_PATH_READ_FAILED",
                message=f"result_root_path cannot be listed: {root}: {exc}",
            )
        return

    for child in children:
        if child.name in alias_names:
            continue
        summary.unknown_folders += 1
        summary.errors += 1
        if not dry_run and run_id is not None:
            log_error(
                cur,
                run_id=run_id,
                error_type="FOLDER_SCAN",
                error_code="UNKNOWN_MEDICAL_FOLDER",
                message=f"unknown medical folder: {child}",
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
) -> bool:
    if limit and summary.files_target >= limit:
        return False

    src_folder_raw = str(alias["src_folder_raw"])
    edit_dir = root / src_folder_raw / EDIT_FOLDER_NAME
    if not edit_dir.exists() or not edit_dir.is_dir():
        summary.edit_folders_missing += 1
        summary.errors += 1
        if not dry_run and run_id is not None:
            log_error(
                cur,
                run_id=run_id,
                error_type="FOLDER_SCAN",
                error_code="EDIT_FOLDER_NOT_FOUND",
                message=f"edit folder not found: {edit_dir}",
            )
        return True

    for path in iter_files(edit_dir):
        summary.files_seen += 1
        ext = file_ext(path)
        summary.bump_ext(ext or "(none)")

        if is_hidden_or_temp(path) or ext not in TARGET_EXTS:
            summary.files_skipped += 1
            continue

        summary.files_target += 1
        file_type = TARGET_EXTS[ext]
        summary.bump_type(file_type)

        try:
            stat = path.stat()
            file_hash = sha256_file(path, chunk_size=chunk_size)
            rel_path = relative_to_root(path, root)
        except OSError as exc:
            summary.errors += 1
            if not dry_run and run_id is not None:
                log_error(
                    cur,
                    run_id=run_id,
                    error_type="FILE_SCAN",
                    error_code="FILE_READ_FAILED",
                    message=f"file read failed: {path}: {exc}",
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
                run_id=int(run_id),
            )
            summary.files_inserted += 1
        except Exception as exc:
            if is_duplicate_key_error(exc):
                summary.files_duplicate += 1
                continue
            summary.errors += 1
            log_error(
                cur,
                run_id=int(run_id),
                error_type="DB_WRITE",
                error_code="FILE_RECEIPT_INSERT_FAILED",
                message=f"file_receipt insert failed: {path}: {exc}",
            )

        if limit and summary.files_target >= limit:
            return False

    return True


def run_scan(conn: Any, args: argparse.Namespace) -> ScanSummary:
    summary = ScanSummary(event_id=args.event_id)
    chunk_size = max(args.chunk_size_mb, 1) * 1024 * 1024
    run_id: int | None = None

    cur = dict_cursor(conn)
    try:
        if not args.dry_run:
            run_id = start_run(cur, event_id=args.event_id)
            conn.commit()

        try:
            root_text = get_result_root_path(cur, dev_db=args.dev_db, event_id=args.event_id)
            summary.result_root_path = root_text
            if not root_text:
                summary.errors += 1
                summary.fatal_error = True
                if not args.dry_run and run_id is not None:
                    log_error(
                        cur,
                        run_id=run_id,
                        error_type="SCAN_PRECONDITION",
                        error_code="RESULT_ROOT_PATH_MISSING",
                        message=f"result_root_path is not set: event_id={args.event_id}",
                    )
                    finish_run(cur, run_id=run_id, status=STATUS_ERROR, summary_message=summary.to_message())
                    conn.commit()
                return summary

            root = Path(root_text).expanduser()
            if not root.exists() or not root.is_dir():
                summary.errors += 1
                summary.fatal_error = True
                if not args.dry_run and run_id is not None:
                    log_error(
                        cur,
                        run_id=run_id,
                        error_type="SCAN_PRECONDITION",
                        error_code="RESULT_ROOT_PATH_NOT_FOUND",
                        message=f"result_root_path is not a directory: {root}",
                    )
                    finish_run(cur, run_id=run_id, status=STATUS_ERROR, summary_message=summary.to_message())
                    conn.commit()
                return summary

            aliases = fetch_aliases(cur, event_id=args.event_id)
            summary.aliases_total = len(aliases)
            alias_names = {str(row["src_folder_raw"]) for row in aliases}
            scan_unknown_folders(
                cur,
                run_id=run_id,
                root=root,
                alias_names=alias_names,
                summary=summary,
                dry_run=args.dry_run,
            )

            keep_scanning = True
            for alias in aliases:
                is_active = int(alias.get("is_active") or 0)
                manual_judgement = int(alias.get("manual_judgement") or 0)
                src_folder_raw = str(alias["src_folder_raw"])

                if not is_active:
                    summary.aliases_inactive += 1
                    summary.errors += 1
                    if not args.dry_run and run_id is not None:
                        log_error(
                            cur,
                            run_id=run_id,
                            error_type="FOLDER_ALIAS",
                            error_code="ALIAS_INACTIVE",
                            message=f"inactive alias skipped: {src_folder_raw}",
                        )
                    continue

                if manual_judgement:
                    summary.aliases_manual += 1
                    summary.errors += 1
                    if not args.dry_run and run_id is not None:
                        log_error(
                            cur,
                            run_id=run_id,
                            error_type="FOLDER_ALIAS",
                            error_code="ALIAS_MANUAL_JUDGEMENT",
                            message=f"manual_judgement alias skipped: {src_folder_raw}",
                        )
                    continue

                summary.aliases_active += 1
                keep_scanning = scan_alias_files(
                    cur,
                    run_id=run_id,
                    event_id=args.event_id,
                    root=root,
                    alias=alias,
                    summary=summary,
                    dry_run=args.dry_run,
                    limit=args.limit,
                    chunk_size=chunk_size,
                )
                if not keep_scanning:
                    break

            if not args.dry_run and run_id is not None:
                status = STATUS_WARNING if summary.errors else STATUS_SUCCESS
                finish_run(cur, run_id=run_id, status=status, summary_message=summary.to_message())
                conn.commit()

            return summary
        except Exception:
            conn.rollback()
            if not args.dry_run and run_id is not None:
                error_cur = dict_cursor(conn)
                try:
                    log_error(
                        error_cur,
                        run_id=run_id,
                        error_type="UNEXPECTED",
                        error_code="UNEXPECTED_SCAN_ERROR",
                        message="unexpected scan error",
                    )
                    finish_run(
                        error_cur,
                        run_id=run_id,
                        status=STATUS_ERROR,
                        summary_message=summary.to_message(),
                    )
                    conn.commit()
                finally:
                    error_cur.close()
            raise
    finally:
        cur.close()


def main() -> int:
    args = parse_args()
    params = load_mysql_base_params(args.db_prefix)

    with connect_ctx(params, database=args.health_db, autocommit=False) as conn:
        summary = run_scan(conn, args)
        if args.dry_run:
            conn.rollback()
        summary.print()
        return 1 if summary.fatal_error else 0


if __name__ == "__main__":
    raise SystemExit(main())
