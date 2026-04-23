

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

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


from scripts.lib.db.mysql import connect_ctx, dict_cursor, load_mysql_params
from scripts.lib.db.schemas import WORK_OTHER


CONFIG_PATH = (
    Path(__file__).resolve().parent
    / "config"
    / "snapshot_hia_dashboard_year_end_status.yml"
)

SOURCE_TABLE = "hia_dashboard_status"
SNAPSHOT_TABLE = "hia_dashboard_year_end_status"


# ------------------------------------------------------------
# config
# ------------------------------------------------------------


@dataclass(frozen=True)
class SnapshotConfig:
    fiscal_year: int
    target_mode: str
    insurer_numbers: list[str]
    on_conflict: str
    notes: str


class ConfigError(ValueError):
    pass


class SnapshotError(RuntimeError):
    pass


# ------------------------------------------------------------
# helpers
# ------------------------------------------------------------


def normalize_insurer_number(value: Any) -> str:
    text = str(value).strip()
    if not text:
        raise ConfigError("insurer_numbers に空文字は指定できません。")
    return text



def load_config(config_path: Path = CONFIG_PATH) -> SnapshotConfig:
    if not config_path.exists():
        raise ConfigError(f"設定ファイルが見つかりません: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    if not isinstance(raw, dict):
        raise ConfigError("設定ファイルのルートは map / object である必要があります。")

    fiscal_year = raw.get("fiscal_year")
    if not isinstance(fiscal_year, int):
        raise ConfigError("fiscal_year は int で指定してください。")

    target_mode = str(raw.get("target_mode", "")).strip().lower()
    if target_mode not in {"all", "selected"}:
        raise ConfigError("target_mode は 'all' または 'selected' を指定してください。")

    insurer_numbers_raw = raw.get("insurer_numbers") or []
    if not isinstance(insurer_numbers_raw, list):
        raise ConfigError("insurer_numbers は list で指定してください。")

    insurer_numbers = [normalize_insurer_number(v) for v in insurer_numbers_raw]

    if target_mode == "selected" and not insurer_numbers:
        raise ConfigError(
            "target_mode=selected の場合、insurer_numbers を1件以上指定してください。"
        )

    on_conflict = str(raw.get("on_conflict", "")).strip().lower()
    if on_conflict not in {"error", "overwrite"}:
        raise ConfigError("on_conflict は 'error' または 'overwrite' を指定してください。")

    notes = str(raw.get("notes", "")).strip()

    return SnapshotConfig(
        fiscal_year=fiscal_year,
        target_mode=target_mode,
        insurer_numbers=insurer_numbers,
        on_conflict=on_conflict,
        notes=notes,
    )



def build_in_clause_params(values: list[str]) -> tuple[str, list[str]]:
    if not values:
        raise SnapshotError("IN句の対象が空です。")
    placeholders = ", ".join(["%s"] * len(values))
    return placeholders, list(values)



def resolve_target_insurer_numbers(cur: Any, config: SnapshotConfig) -> list[str]:
    if config.target_mode == "selected":
        return list(dict.fromkeys(config.insurer_numbers))

    sql = f"""
        SELECT DISTINCT insurer_number
        FROM {SOURCE_TABLE}
        WHERE insurer_number IS NOT NULL
          AND insurer_number <> ''
        ORDER BY insurer_number
    """
    cur.execute(sql)
    rows = cur.fetchall()
    insurer_numbers = [str(row["insurer_number"]).strip() for row in rows]
    insurer_numbers = [x for x in insurer_numbers if x]

    if not insurer_numbers:
        raise SnapshotError(
            f"{SOURCE_TABLE} に snapshot 対象となる insurer_number が存在しません。"
        )

    return insurer_numbers



def count_existing_snapshot_rows(
    cur: Any,
    fiscal_year: int,
    insurer_numbers: list[str],
) -> int:
    placeholders, params = build_in_clause_params(insurer_numbers)
    sql = f"""
        SELECT COUNT(*) AS cnt
        FROM {SNAPSHOT_TABLE}
        WHERE fiscal_year = %s
          AND insurer_number IN ({placeholders})
    """
    cur.execute(sql, [fiscal_year, *params])
    row = cur.fetchone()
    return int(row["cnt"])



def delete_existing_snapshot_rows(
    cur: Any,
    fiscal_year: int,
    insurer_numbers: list[str],
) -> int:
    placeholders, params = build_in_clause_params(insurer_numbers)
    sql = f"""
        DELETE FROM {SNAPSHOT_TABLE}
        WHERE fiscal_year = %s
          AND insurer_number IN ({placeholders})
    """
    cur.execute(sql, [fiscal_year, *params])
    return int(cur.rowcount)



def insert_snapshot_rows(
    cur: Any,
    fiscal_year: int,
    insurer_numbers: list[str],
) -> int:
    placeholders, params = build_in_clause_params(insurer_numbers)
    sql = f"""
        INSERT INTO {SNAPSHOT_TABLE} (
            identity_hash,
            fiscal_year,
            insurer_number,
            person_id_custom,
            subscribers_id,
            hia_subscriber_id,
            status,
            reservation_date,
            exam_date,
            medical_institution_code,
            medical_institution_name,
            snapshot_at
        )
        SELECT
            s.identity_hash,
            %s AS fiscal_year,
            s.insurer_number,
            s.person_id_custom,
            s.subscribers_id,
            s.hia_subscriber_id,
            s.status,
            s.reservation_date,
            s.exam_date,
            s.medical_institution_code,
            s.medical_institution_name,
            NOW() AS snapshot_at
        FROM {SOURCE_TABLE} AS s
        WHERE s.insurer_number IN ({placeholders})
    """
    cur.execute(sql, [fiscal_year, *params])
    return int(cur.rowcount)


# ------------------------------------------------------------
# main
# ------------------------------------------------------------


def main() -> None:
    config = load_config(CONFIG_PATH)

    print("=== HIA dashboard year-end snapshot start ===")
    print(f"config_path    : {CONFIG_PATH}")
    print(f"db_schema      : {WORK_OTHER}")
    print(f"fiscal_year    : {config.fiscal_year}")
    print(f"target_mode    : {config.target_mode}")
    print(f"on_conflict    : {config.on_conflict}")
    if config.notes:
        print(f"notes          : {config.notes}")

    params = load_mysql_params()

    with connect_ctx(params, database=WORK_OTHER, autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:

            insurer_numbers = resolve_target_insurer_numbers(cur, config)
            print(f"target_insurers: {len(insurer_numbers)}件")
            print(f"insurer_numbers: {', '.join(insurer_numbers)}")

            existing_count = count_existing_snapshot_rows(
                cur,
                fiscal_year=config.fiscal_year,
                insurer_numbers=insurer_numbers,
            )
            print(f"existing_rows  : {existing_count}")

            if existing_count > 0 and config.on_conflict == "error":
                raise SnapshotError(
                    "既存 snapshot データが存在するため処理を停止します。 "
                    f"fiscal_year={config.fiscal_year}, insurers={insurer_numbers}"
                )

            deleted_count = 0
            if existing_count > 0 and config.on_conflict == "overwrite":
                deleted_count = delete_existing_snapshot_rows(
                    cur,
                    fiscal_year=config.fiscal_year,
                    insurer_numbers=insurer_numbers,
                )
                print(f"deleted_rows   : {deleted_count}")

            inserted_count = insert_snapshot_rows(
                cur,
                fiscal_year=config.fiscal_year,
                insurer_numbers=insurer_numbers,
            )
            print(f"inserted_rows  : {inserted_count}")

            conn.commit()
            print("commit         : done")
        except Exception:
            conn.rollback()
            print("rollback       : done")
            raise

    print("=== HIA dashboard year-end snapshot finished ===")


if __name__ == "__main__":
    start = datetime.now()
    try:
        main()
    finally:
        print("elapsed        :", datetime.now() - start)