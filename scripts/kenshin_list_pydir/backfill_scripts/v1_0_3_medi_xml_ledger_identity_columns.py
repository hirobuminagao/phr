#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
v1_0_3_medi_xml_ledger_identity_columns.py

目的:
- work_other.medi_xml_ledger の identity 系列を backfill する
- name_kana_match を work_folder 正本ロジック準拠で再生成する
- insurance_symbol_match / insurance_number_match を再生成する
- raw 値から person_id_custom を生成する
- person_id_custom + name_kana_match + gender_code から identity_hash を生成する

前提:
- medi_xml_ledger は XML単位の台帳であり、人単位集約は行わない
- custom_id_gen は raw 値を入力として使う
- common.py は kenshin_list_pydir 側へコピー済みのものを使う
- DB接続は MEDI_IMPORT_DB_* 環境変数を使う
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional, cast

import mysql.connector
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.cursor import MySQLCursorDict

# ------------------------------------------------------------
# import path
# backfill_scripts/ 直下実行でも kenshin_list_pydir 配下を import できるようにする
# ------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from lib.errors import NormalizeError
from lib.normalize.common import (  # noqa: E402
    build_identity_hash,
    normalize_insurance_number_match,
    normalize_insurance_symbol_match,
    normalize_name_kana_match,
)
from lib.custom_id_gen import generate_id as generate_person_id_custom  # noqa: E402


DEFAULT_DB_HOST = os.getenv("MEDI_IMPORT_DB_HOST", "127.0.0.1")
DEFAULT_DB_PORT = int(os.getenv("MEDI_IMPORT_DB_PORT", "3306"))
DEFAULT_DB_NAME = os.getenv("MEDI_IMPORT_DB_NAME", "work_other")
DEFAULT_DB_USER = os.getenv("MEDI_IMPORT_DB_USER", "root")
DEFAULT_DB_PASSWORD = os.getenv("MEDI_IMPORT_DB_PASSWORD", "")

DEFAULT_BATCH_SIZE = 500
TARGET_TABLE = "medi_xml_ledger"


@dataclass
class RowUpdate:
    xml_ledger_id: int
    name_kana_match: Optional[str]
    insurance_symbol_match: Optional[str]
    insurance_number_match: Optional[str]
    person_id_custom: Optional[str]
    identity_hash: Optional[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill medi_xml_ledger identity columns for v1.0.3"
    )
    parser.add_argument("--host", default=DEFAULT_DB_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_DB_PORT)
    parser.add_argument("--db", default=DEFAULT_DB_NAME)
    parser.add_argument("--user", default=DEFAULT_DB_USER)
    parser.add_argument("--password", default=DEFAULT_DB_PASSWORD)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="処理件数上限。0 なら全件。",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="更新SQLは実行せず件数だけ確認する。",
    )
    parser.add_argument(
        "--where",
        default="",
        help="追加WHERE句（先頭の WHERE は不要）。例: xml_ledger_id >= 1000",
    )
    return parser.parse_args()


def connect_db(args: argparse.Namespace) -> MySQLConnectionAbstract:
    return mysql.connector.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.db,
        charset="utf8mb4",
        use_unicode=True,
        autocommit=False,
    )


def to_birth_yyyymmdd(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        digits = "".join(ch for ch in text if ch.isdigit())
        if len(digits) == 8:
            return digits
        return None
    return None


def to_str_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def safe_normalize_name_kana_match(raw: Any) -> Optional[str]:
    text = to_str_or_none(raw)
    if not text:
        return None
    try:
        return normalize_name_kana_match(text)
    except NormalizeError:
        return None


def safe_normalize_insurance_symbol_match(raw: Any) -> Optional[str]:
    text = to_str_or_none(raw)
    if not text:
        return None
    try:
        return normalize_insurance_symbol_match(text)
    except NormalizeError:
        return None


def safe_normalize_insurance_number_match(raw: Any) -> Optional[str]:
    text = to_str_or_none(raw)
    if not text:
        return None
    try:
        return normalize_insurance_number_match(text)
    except NormalizeError:
        return None


def safe_generate_person_id_custom(
    insurer_number: Any,
    insurance_symbol: Any,
    insurance_number: Any,
    birth_date: Any,
) -> Optional[str]:
    raw_insurer_number = to_str_or_none(insurer_number)
    raw_insurance_symbol = to_str_or_none(insurance_symbol)
    raw_insurance_number = to_str_or_none(insurance_number)
    raw_birth_yyyymmdd = to_birth_yyyymmdd(birth_date)

    if not (
        raw_insurer_number
        and raw_insurance_symbol
        and raw_insurance_number
        and raw_birth_yyyymmdd
    ):
        return None

    try:
        person_id_custom, _meta = generate_person_id_custom(
            insurer_number=raw_insurer_number,
            symbol=raw_insurance_symbol,
            insurance_number=raw_insurance_number,
            birth_yyyymmdd=raw_birth_yyyymmdd,
        )
        return person_id_custom
    except Exception:
        return None


def safe_build_identity_hash(
    person_id_custom: Optional[str],
    name_kana_match: Optional[str],
    gender_code: Any,
) -> Optional[str]:
    gender_code_text = to_str_or_none(gender_code)
    try:
        return build_identity_hash(
            person_id_custom=person_id_custom,
            name_kana_full_match=name_kana_match,
            gender_code=gender_code_text,
        )
    except NormalizeError:
        return None


def fetch_rows(
    cur: MySQLCursorDict,
    batch_size: int,
    last_id: int,
    extra_where: str,
    limit_remaining: int,
) -> list[dict[str, Any]]:
    where_parts = ["xml_ledger_id > %(last_id)s"]
    if extra_where:
        where_parts.append(f"({extra_where})")

    sql = f"""
    SELECT
        xml_ledger_id,
        insurer_number,
        insurance_symbol,
        insurance_number,
        birth_date,
        gender_code,
        name_kana_full,
        name_kana_match,
        insurance_symbol_match,
        insurance_number_match,
        person_id_custom,
        identity_hash
    FROM {TARGET_TABLE}
    WHERE {' AND '.join(where_parts)}
    ORDER BY xml_ledger_id
    LIMIT %(limit_rows)s
    """
    params: dict[str, Any] = {
        "last_id": last_id,
        "limit_rows": min(batch_size, limit_remaining) if limit_remaining > 0 else batch_size,
    }
    cur.execute(sql, params)
    rows = cur.fetchall()
    return [dict(row) for row in rows if row is not None]


def build_update(row: dict[str, Any]) -> Optional[RowUpdate]:
    name_kana_match = safe_normalize_name_kana_match(row.get("name_kana_full"))
    insurance_symbol_match = safe_normalize_insurance_symbol_match(row.get("insurance_symbol"))
    insurance_number_match = safe_normalize_insurance_number_match(row.get("insurance_number"))
    person_id_custom = safe_generate_person_id_custom(
        insurer_number=row.get("insurer_number"),
        insurance_symbol=row.get("insurance_symbol"),
        insurance_number=row.get("insurance_number"),
        birth_date=row.get("birth_date"),
    )
    identity_hash = safe_build_identity_hash(
        person_id_custom=person_id_custom,
        name_kana_match=name_kana_match,
        gender_code=row.get("gender_code"),
    )

    no_change = (
        row.get("name_kana_match") == name_kana_match
        and row.get("insurance_symbol_match") == insurance_symbol_match
        and row.get("insurance_number_match") == insurance_number_match
        and row.get("person_id_custom") == person_id_custom
        and row.get("identity_hash") == identity_hash
    )
    if no_change:
        return None

    return RowUpdate(
        xml_ledger_id=int(row["xml_ledger_id"]),
        name_kana_match=name_kana_match,
        insurance_symbol_match=insurance_symbol_match,
        insurance_number_match=insurance_number_match,
        person_id_custom=person_id_custom,
        identity_hash=identity_hash,
    )


def apply_updates(cur: MySQLCursorDict, updates: list[RowUpdate]) -> None:
    if not updates:
        return

    sql = f"""
    UPDATE {TARGET_TABLE}
       SET name_kana_match = %(name_kana_match)s,
           insurance_symbol_match = %(insurance_symbol_match)s,
           insurance_number_match = %(insurance_number_match)s,
           person_id_custom = %(person_id_custom)s,
           identity_hash = %(identity_hash)s
     WHERE xml_ledger_id = %(xml_ledger_id)s
    """
    params = [
        {
            "xml_ledger_id": row.xml_ledger_id,
            "name_kana_match": row.name_kana_match,
            "insurance_symbol_match": row.insurance_symbol_match,
            "insurance_number_match": row.insurance_number_match,
            "person_id_custom": row.person_id_custom,
            "identity_hash": row.identity_hash,
        }
        for row in updates
    ]
    cur.executemany(sql, params)


def main() -> int:
    args = parse_args()
    conn = connect_db(args)
    cur = cast(MySQLCursorDict, conn.cursor(dictionary=True))

    processed = 0
    changed = 0
    last_id = 0
    limit_remaining = args.limit

    try:
        while True:
            rows = fetch_rows(
                cur=cur,
                batch_size=args.batch_size,
                last_id=last_id,
                extra_where=args.where,
                limit_remaining=limit_remaining,
            )
            if not rows:
                break

            batch_updates: list[RowUpdate] = []
            for row in rows:
                last_id = int(row["xml_ledger_id"])
                processed += 1
                update_row = build_update(row)
                if update_row is not None:
                    batch_updates.append(update_row)

            changed += len(batch_updates)

            if not args.dry_run:
                apply_updates(cur, batch_updates)
                conn.commit()

            print(
                f"processed={processed} changed={changed} last_id={last_id} dry_run={args.dry_run}"
            )

            if limit_remaining > 0:
                limit_remaining -= len(rows)
                if limit_remaining <= 0:
                    break

        print("done")
        print(f"processed={processed}")
        print(f"changed={changed}")
        return 0

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())