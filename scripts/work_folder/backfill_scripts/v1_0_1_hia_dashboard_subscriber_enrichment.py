#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
v1_0_1_hia_dashboard_subscriber_enrichment.py

Backfill script for v1.0.1

Purpose:
Populate subscriber enrichment columns in work_other.hia_dashboard_status
from dev_phr.subscribers.

Columns populated:
    - subscriber_person_id_custom
    - subscriber_name_kana_full
    - subscriber_gender_code
    - subscriber_birth

Join keys:
    insurer_number
    insurance_symbol_match
    insurance_number_match
    name_match (= subscribers.name_full_match)

Important:
This is a **one‑time migration script**.
It should be executed manually and not used in regular operations.
"""

import argparse
import os
import sys
from typing import Dict, Any, Optional, Mapping, cast

import mysql.connector
from mysql.connector import Error

from pathlib import Path


def load_local_env() -> None:
    """Load scripts/work_folder/.env if present (KEY=VALUE format)."""
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key and key not in os.environ:
            os.environ[key] = value

load_local_env()

# ------------------------------------------------------------
# DB connection
# ------------------------------------------------------------

def get_conn(args: argparse.Namespace):
    return mysql.connector.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
    )


def require_row_dict(row: Any) -> Dict[str, Any]:
    """mysql.connector の取得行を dict として扱える形に正規化する。"""
    if isinstance(row, dict):
        return cast(Dict[str, Any], row)
    raise TypeError(f"Expected dict row, got {type(row)!r}")

# ------------------------------------------------------------
# fetch subscribers
# ------------------------------------------------------------

def fetch_subscriber(cur, row: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    sql = """
        SELECT
            person_id_custom,
            name_kana_full,
            gender_code,
            birth
        FROM dev_phr.subscribers
        WHERE insurer_number = %s
          AND insurance_symbol_match = %s
          AND insurance_number_match = %s
          AND name_full_match = %s
        LIMIT 1
    """

    cur.execute(
        sql,
        (
            row["insurer_number"],
            row["insurance_symbol_match"],
            row["insurance_number_match"],
            row["name_match"],
        ),
    )

    return cur.fetchone()


# ------------------------------------------------------------
# update dashboard
# ------------------------------------------------------------

def update_dashboard(cur, person_id: int, sub):
    sql = """
        UPDATE work_other.hia_dashboard_status
        SET
            subscriber_person_id_custom = %s,
            subscriber_name_kana_full = %s,
            subscriber_gender_code = %s,
            subscriber_birth = %s
        WHERE hia_dashboard_person_id = %s
    """

    cur.execute(
        sql,
        (
            sub["person_id_custom"],
            sub["name_kana_full"],
            sub["gender_code"],
            sub["birth"],
            person_id,
        ),
    )


# ------------------------------------------------------------
# main backfill logic
# ------------------------------------------------------------

def run_backfill(args: argparse.Namespace) -> int:

    try:
        conn = get_conn(args)
    except Error as e:
        print("[ERROR] MySQL connection failed")
        print(
            f"        host={args.host} port={args.port} "
            f"user={args.user} database={args.database}"
        )
        print(f"        detail: {e}")
        return 1

    cur = conn.cursor(dictionary=True)

    select_sql = """
        SELECT
            hia_dashboard_person_id,
            insurer_number,
            insurance_symbol_match,
            insurance_number_match,
            name_match
        FROM work_other.hia_dashboard_status
    """

    cur.execute(select_sql)

    rows = cur.fetchall()

    total = len(rows)
    matched = 0
    unmatched = 0

    print(f"rows: {total}")

    for r in rows:

        row_dict = require_row_dict(r)
        sub = fetch_subscriber(cur, row_dict)

        if not sub:
            unmatched += 1
            continue

        matched += 1

        if not args.dry_run:
            update_dashboard(cur, int(row_dict["hia_dashboard_person_id"]), sub)

    if not args.dry_run:
        conn.commit()

    print("------------------------------")
    print(f"matched   : {matched}")
    print(f"unmatched : {unmatched}")

    cur.close()
    conn.close()

    return 0


# ------------------------------------------------------------
# entry point
# ------------------------------------------------------------

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--host", default=os.getenv("PHR_MYSQL_HOST"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PHR_MYSQL_PORT", "3306")))
    parser.add_argument("--user", default=os.getenv("PHR_MYSQL_USER"))
    parser.add_argument("--password", default=os.getenv("PHR_MYSQL_PASSWORD"))
    parser.add_argument("--database", default=os.getenv("PHR_MYSQL_DATABASE", "work_other"))

    args = parser.parse_args()

    raise SystemExit(run_backfill(args))