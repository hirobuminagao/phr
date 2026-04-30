#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, cast

import yaml

# ------------------------------------------------------------
# sys.path bootstrap
# ------------------------------------------------------------
# 直接実行時でも `scripts.*` import を解決できるようにする
if __package__ in (None, ""):
    THIS_FILE = Path(__file__).resolve()
    REPO_ROOT = THIS_FILE.parents[2]
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

from scripts.from_fund.script_lib.diff_classifier import (
    DIFF_STATUS_ADD,
    DIFF_STATUS_NO_CHANGE,
    DIFF_STATUS_UPDATE,
    DIFF_STATUS_UNKNOWN,
    classify_staging_row,
)
from scripts.from_fund.script_lib.hia_subscribers_exporter import (
    build_hia_export_base_dir,
    write_hia_subscriber_export_files,
)
from scripts.from_fund.script_lib.major_candidate_finder import find_major_candidate
from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import DEV_PHR

from datetime import datetime

DIFF_OUTPUT_DIR = Path("data/from_fund/diff_output")
DEFAULT_EXPORT_SPLIT_SIZE = 1000


@dataclass(frozen=True)
class DiffConfig:
    insurer_number: str
    fund_id: int | None
    import_run_ids: list[int]
    diff_mode: bool
    export_mode: bool
    export_split_size: int


@dataclass(frozen=True)
class DiffSummary:
    insurer_number: str
    import_run_ids: list[int]
    diff_mode: bool
    export_mode: bool
    export_split_size: int
    staging_total: int
    no_change: int
    add: int
    update: int
    unknown: int
    major_candidate: int
    missing_from_new: int
    missing_from_new_path: str | None
    major_candidate_path: str | None
    add_export_paths: list[str]
    update_export_paths: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="staging_subscribers_fund と subscribers の差分ステータス更新、およびHIA登録用CSV出力を行う",
    )
    default_config_path = Path(__file__).parent / "config" / "diff_status.yml"

    parser.add_argument(
        "--config",
        default=str(default_config_path),
        help="差分判定設定YAMLファイルパス（未指定時は scripts/from_fund/config/diff_status.yml を使用）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="DB更新とCSV出力を行わず、判定件数のみ確認する",
    )
    return parser.parse_args()


def load_config(path: str | Path) -> DiffConfig:
    with Path(path).open("r", encoding="utf-8") as f:
        raw_data = yaml.safe_load(f) or {}

    data = cast(Mapping[str, Any], raw_data)

    insurer_number = str(data["insurer_number"])
    fund_id_raw = data.get("fund_id")
    fund_id = int(fund_id_raw) if fund_id_raw is not None else None
    import_run_ids = [int(v) for v in data["import_run_ids"]]

    if not import_run_ids:
        raise ValueError("import_run_ids is empty")

    export_split_size = int(data.get("export_split_size") or DEFAULT_EXPORT_SPLIT_SIZE)
    if export_split_size <= 0:
        raise ValueError("export_split_size must be greater than 0")

    return DiffConfig(
        insurer_number=insurer_number,
        fund_id=fund_id,
        import_run_ids=sorted(import_run_ids),
        diff_mode=bool(data.get("diff_mode", True)),
        export_mode=bool(data.get("export_mode", True)),
        export_split_size=export_split_size,
    )


def _placeholders(values: list[Any]) -> str:
    if not values:
        raise ValueError("values must not be empty")
    return ",".join(["%s"] * len(values))


def fetch_target_staging_rows(
    conn: Any,
    *,
    insurer_number: str,
    import_run_ids: list[int],
) -> list[dict[str, Any]]:
    cursor = dict_cursor(conn)
    try:
        cursor.execute(
            f"""
            SELECT
              *
            FROM {DEV_PHR}.staging_subscribers_fund
            WHERE insurer_number_norm = %s
              AND import_run_id IN ({_placeholders(import_run_ids)})
            ORDER BY import_run_id, id
            """,
            (insurer_number, *import_run_ids),
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()
    return [dict(cast(Mapping[str, Any], row)) for row in rows]


def fetch_current_subscribers_by_ids(
    conn: Any,
    subscriber_ids: list[int],
) -> dict[int, dict[str, Any]]:
    if not subscriber_ids:
        return {}

    cursor = dict_cursor(conn)
    try:
        cursor.execute(
            f"""
            SELECT
              s.*,
              a.postal_code AS postal_code,
              a.address_line AS address_line,
              c.phone AS phone,
              c.email AS email
            FROM {DEV_PHR}.subscribers s
            LEFT JOIN {DEV_PHR}.subscriber_addresses a
              ON a.subscriber_id = s.id
             AND a.is_current = 1
            LEFT JOIN {DEV_PHR}.subscriber_contacts c
              ON c.subscriber_id = s.id
             AND c.is_current = 1
            WHERE s.id IN ({_placeholders(subscriber_ids)})
            """,
            tuple(subscriber_ids),
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()
    normalized_rows = [dict(cast(Mapping[str, Any], row)) for row in rows]
    return {int(row["id"]): row for row in normalized_rows}


def fetch_current_subscribers_for_candidate_search(
    conn: Any,
    *,
    insurer_number: str,
) -> list[dict[str, Any]]:
    cursor = dict_cursor(conn)
    try:
        cursor.execute(
            f"""
            SELECT *
            FROM {DEV_PHR}.subscribers
            WHERE insurer_number = %s
            """,
            (insurer_number,),
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()

    return [dict(cast(Mapping[str, Any], row)) for row in rows]


def fetch_missing_from_new_rows(
    conn: Any,
    *,
    insurer_number: str,
    import_run_ids: list[int],
) -> list[dict[str, Any]]:
    cursor = dict_cursor(conn)
    try:
        cursor.execute(
            f"""
            SELECT
              s.*
            FROM {DEV_PHR}.subscribers s
            WHERE s.insurer_number = %s
              AND NOT EXISTS (
                SELECT 1
                FROM {DEV_PHR}.staging_subscribers_fund stg
                WHERE BINARY stg.insurer_number_norm = BINARY s.insurer_number
                  AND stg.import_run_id IN ({_placeholders(import_run_ids)})
                  AND BINARY stg.identity_hash = BINARY s.identity_hash
              )
            ORDER BY s.insurance_symbol_match, s.insurance_number_match, s.name_kana_full_match
            """,
            (insurer_number, *import_run_ids),
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()
    return [dict(cast(Mapping[str, Any], row)) for row in rows]


def update_diff_status(
    conn: Any,
    *,
    staging_id: int,
    diff_status: str,
    diff_reason: str,
) -> None:
    cursor = dict_cursor(conn)
    try:
        cursor.execute(
            f"""
            UPDATE {DEV_PHR}.staging_subscribers_fund
            SET
              diff_status = %s,
              diff_status_method = 'script',
              diff_status_reason = %s
            WHERE id = %s
            """,
            (diff_status, diff_reason, staging_id),
        )
    finally:
        cursor.close()


def build_missing_output_path(
    *,
    insurer_number: str,
    import_run_ids: list[int],
    now: datetime | None = None,
) -> Path:
    now = now or datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    run_part = "-".join(str(v) for v in sorted(import_run_ids))
    filename = f"{timestamp}_{insurer_number}_missing_from_{run_part}.csv"
    return DIFF_OUTPUT_DIR / filename


def build_major_candidate_output_path(
    *,
    insurer_number: str,
    import_run_ids: list[int],
    now: datetime | None = None,
) -> Path:
    now = now or datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    run_part = "-".join(str(v) for v in sorted(import_run_ids))
    filename = f"{timestamp}_{insurer_number}_major_candidate_{run_part}.csv"
    return DIFF_OUTPUT_DIR / filename


def write_major_candidate_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return

    fieldnames = [
        "staging_id",
        "import_run_id",
        "src_row_no",
        "major_candidate_pattern",
        "major_candidate_reason",
        "candidate_subscriber_id",
        "staging_insurance_symbol_match",
        "candidate_insurance_symbol_match",
        "staging_insurance_number_match",
        "candidate_insurance_number_match",
        "staging_name_kana_full_match",
        "candidate_name_kana_full_match",
        "staging_name_kana_given_match",
        "candidate_name_kana_given_match",
        "staging_name_kanji_given_match",
        "candidate_name_kanji_given_match",
        "staging_birth",
        "candidate_birth",
        "staging_gender_code",
        "candidate_gender_code",
    ]

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_missing_from_new_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    import csv
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def build_major_candidate_log_row(
    *,
    staging_row: dict[str, Any],
    candidate_result: Any,
    subscribers_by_id: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    candidate: dict[str, Any] = {}
    if candidate_result.candidate_subscriber_id is not None:
        candidate = subscribers_by_id.get(int(candidate_result.candidate_subscriber_id), {})

    return {
        "staging_id": staging_row.get("id"),
        "import_run_id": staging_row.get("import_run_id"),
        "src_row_no": staging_row.get("src_row_no"),
        "major_candidate_pattern": candidate_result.pattern,
        "major_candidate_reason": candidate_result.reason,
        "candidate_subscriber_id": candidate_result.candidate_subscriber_id,
        "staging_insurance_symbol_match": staging_row.get("insurance_symbol_match"),
        "candidate_insurance_symbol_match": candidate.get("insurance_symbol_match"),
        "staging_insurance_number_match": staging_row.get("insurance_number_match"),
        "candidate_insurance_number_match": candidate.get("insurance_number_match"),
        "staging_name_kana_full_match": staging_row.get("name_kana_full_match"),
        "candidate_name_kana_full_match": candidate.get("name_kana_full_match"),
        "staging_name_kana_given_match": staging_row.get("name_kana_given_match"),
        "candidate_name_kana_given_match": candidate.get("name_kana_given_match"),
        "staging_name_kanji_given_match": staging_row.get("name_kanji_given_match"),
        "candidate_name_kanji_given_match": candidate.get("name_kanji_given_match"),
        "staging_birth": staging_row.get("birth_norm"),
        "candidate_birth": candidate.get("birth"),
        "staging_gender_code": staging_row.get("gender_code_norm"),
        "candidate_gender_code": candidate.get("gender_code"),
    }


def run(config: DiffConfig, *, dry_run: bool = False) -> DiffSummary:
    params = load_mysql_base_params()

    no_change = 0
    add = 0
    update = 0
    unknown = 0
    major_candidate = 0
    add_export_rows: list[dict[str, Any]] = []
    update_export_rows: list[dict[str, Any]] = []
    major_candidate_rows: list[dict[str, Any]] = []

    with connect_ctx(params, database=DEV_PHR, autocommit=False) as conn:
        staging_rows = fetch_target_staging_rows(
            conn,
            insurer_number=config.insurer_number,
            import_run_ids=config.import_run_ids,
        )
        matched_ids = sorted(
            {
                int(row["matched_subscriber_id"])
                for row in staging_rows
                if row.get("matched_subscriber_id") is not None
            }
        )
        subscribers_by_id = fetch_current_subscribers_by_ids(conn, matched_ids)

        candidate_subscribers = fetch_current_subscribers_for_candidate_search(
            conn,
            insurer_number=config.insurer_number,
        )
        candidate_subscribers_by_id = {
            int(row["id"]): row for row in candidate_subscribers if row.get("id") is not None
        }

        for row in staging_rows:
            if row.get("matched_subscriber_id") is not None:
                result = classify_staging_row(row, subscribers_by_id)
                status = result.diff_status
                reason = result.diff_reason
            else:
                candidate_result = find_major_candidate(row, candidate_subscribers)
                status = candidate_result.status
                reason = candidate_result.reason

                if status == "major_candidate":
                    major_candidate += 1
                    major_candidate_rows.append(
                        build_major_candidate_log_row(
                            staging_row=row,
                            candidate_result=candidate_result,
                            subscribers_by_id=candidate_subscribers_by_id,
                        )
                    )
                else:
                    add += 1
                    add_export_rows.append(row)

                if config.diff_mode and not dry_run:
                    update_diff_status(
                        conn,
                        staging_id=int(row["id"]),
                        diff_status=status,
                        diff_reason=reason,
                    )
                continue

            if status == DIFF_STATUS_NO_CHANGE:
                no_change += 1
            elif status == DIFF_STATUS_ADD:
                add += 1
                add_export_rows.append(row)
            elif status == DIFF_STATUS_UPDATE:
                update += 1
                update_export_rows.append(row)
            else:
                unknown += 1

            if config.diff_mode and not dry_run:
                update_diff_status(
                    conn,
                    staging_id=int(row["id"]),
                    diff_status=status,
                    diff_reason=reason,
                )

        missing_rows: list[dict[str, Any]] = []
        missing_path: Path | None = None
        if config.diff_mode:
            missing_rows = fetch_missing_from_new_rows(
                conn,
                insurer_number=config.insurer_number,
                import_run_ids=config.import_run_ids,
            )
            if missing_rows:
                missing_path = build_missing_output_path(
                    insurer_number=config.insurer_number,
                    import_run_ids=config.import_run_ids,
                )
                if not dry_run:
                    write_missing_from_new_csv(missing_path, missing_rows)

        major_candidate_path: Path | None = None
        if config.diff_mode and major_candidate_rows:
            major_candidate_path = build_major_candidate_output_path(
                insurer_number=config.insurer_number,
                import_run_ids=config.import_run_ids,
            )
            if not dry_run:
                write_major_candidate_csv(major_candidate_path, major_candidate_rows)

        add_export_paths: list[Path] = []
        update_export_paths: list[Path] = []
        if config.export_mode and (add_export_rows or update_export_rows):
            hia_export_base_dir = build_hia_export_base_dir(
                insurer_number=config.insurer_number,
                import_run_ids=config.import_run_ids,
            )
            if not dry_run:
                add_export_paths = write_hia_subscriber_export_files(
                    base_dir=hia_export_base_dir,
                    status_label="add",
                    insurer_number=config.insurer_number,
                    rows=add_export_rows,
                    split_size=config.export_split_size,
                )
                update_export_paths = write_hia_subscriber_export_files(
                    base_dir=hia_export_base_dir,
                    status_label="update",
                    insurer_number=config.insurer_number,
                    rows=update_export_rows,
                    split_size=config.export_split_size,
                )

        if dry_run:
            conn.rollback()
        else:
            conn.commit()

    summary = DiffSummary(
        insurer_number=config.insurer_number,
        import_run_ids=config.import_run_ids,
        diff_mode=config.diff_mode,
        export_mode=config.export_mode,
        export_split_size=config.export_split_size,
        staging_total=len(staging_rows),
        no_change=no_change,
        add=add,
        update=update,
        unknown=unknown,
        major_candidate=major_candidate,
        missing_from_new=len(missing_rows),
        missing_from_new_path=str(missing_path) if missing_path else None,
        major_candidate_path=str(major_candidate_path) if major_candidate_path else None,
        add_export_paths=[str(path) for path in add_export_paths],
        update_export_paths=[str(path) for path in update_export_paths],
    )
    print(summary)
    return summary


def main() -> None:
    args = parse_args()
    print(f"[INFO] using config: {args.config}")
    config = load_config(args.config)
    run(config, dry_run=args.dry_run)


if __name__ == "__main__":
    main()