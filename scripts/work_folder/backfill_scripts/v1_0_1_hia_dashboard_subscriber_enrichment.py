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
import sys
from typing import Dict, Any, Optional, Mapping

import mysql.connector


# ------------------------------------------------------------
# DB connection
# ------------------------------------------------------------

def get_conn():
    return mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="",
        database="work_other",
    )


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

def run_backfill(dry_run: bool = False):

    conn = get_conn()
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

        sub = fetch_subscriber(cur, r)

        if not sub:
            unmatched += 1
            continue

        matched += 1

        if not dry_run:
            update_dashboard(cur, int(r["hia_dashboard_person_id"]), sub)

    if not dry_run:
        conn.commit()

    print("------------------------------")
    print(f"matched   : {matched}")
    print(f"unmatched : {unmatched}")

    cur.close()
    conn.close()


# ------------------------------------------------------------
# entry point
# ------------------------------------------------------------

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    run_backfill(dry_run=args.dry_run)