
# -*- coding: utf-8 -*-
r"""
============================================================
Script : apply_subscribers_from_staging_hub.py
Path   : scripts/work_folder/scripts/apply_subscribers_from_staging_hub.py
Project: PHR / work_folder/phr

Purpose (PHR v1.0.1):
    - `staging_subscribers_hub` の未処理行を `subscribers` に反映する（apply 相当）。
    - 既存 subscriber は `person_id_custom + name_kana_full_match + gender_code` で照合する。
    - v1.0.1 追加の identity match columns を生成して `subscribers` に保存する。
    - Hub DLフォーマット先頭列の加入者IDを `hia_subscriber_id` として `subscribers` に保存する。

Design:
    - ETL ログは lib.etl（etl_runs / etl_errors）に一元化する
    - 取込対象は `processed_run_id IS NULL` の staging 行のみ
    - 1行ごとに insert / update / noop を判定する
    - dry-run の場合は subscribers 更新を rollback する（run / error は残る）
    - match columns は apply 時に生成する

V1.0.1 Scope:
    - READS:
        - `staging_subscribers_hub`
        - `subscribers`
    - WRITES:
        - `subscribers`
        - `subscriber_addresses`
        - `subscriber_contacts`
        - `subscriber_audit`
        - `staging_subscribers_hub.processed_run_id / processed_at`
        - `etl_runs` / `etl_errors`
============================================================
"""

from __future__ import annotations
import sys

import argparse
import logging
from pathlib import Path
from typing import Any, Dict, Optional

# ------------------------------------------------------------
# ファイル直実行でも repo root を import path に追加
# ------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.work_folder.lib.db.config import load_mysql_params
from scripts.work_folder.lib.db.mysql import connect_ctx, dict_cursor, MySQLParams
from scripts.work_folder.lib.etl import (
    RunMetrics,
    start_run,
    finish_run,
    log_error,
)
from scripts.work_folder.lib.normalize.common import (
    normalize_insurance_number_match,
    normalize_insurance_symbol_match,
    normalize_name_kanji_match,
    normalize_name_kana_match,
    normalize_insurance_symbol_export,
    build_identity_hash,
)


JOB_NAME = "apply_subscribers_from_staging_hub"
PROGRESS_LOG_EVERY = 100
PROGRESS_LOG_FIRST = 10
# ============================================================
# main
# ============================================================


def should_log_progress(index: int, total: int) -> bool:
    """進捗ログを出すタイミングを判定する。"""
    if index <= 0:
        return False
    if index <= PROGRESS_LOG_FIRST:
        return True
    if index == total:
        return True
    return (index % PROGRESS_LOG_EVERY) == 0




# ============================================================
# subscriber row helpers
# ============================================================

def build_subscriber_vals(cur, srow: dict[str, Any]) -> Dict[str, Any]:
    """staging 1行から subscribers 反映用の値 dict を作る。"""

    insurance_symbol = srow.get("insurance_symbol") or ""
    insurance_number = srow.get("insurance_number") or ""
    name_kanji_full = srow.get("name_kanji_full") or ""
    name_kana_full = srow.get("name_kana_full") or ""

    name_kana_full_match = normalize_name_kana_match(name_kana_full) or ""
    name_full_match = normalize_name_kanji_match(name_kanji_full, cur=cur) or ""
    person_id_custom = srow.get("person_id_custom")
    gender_code = srow.get("gender_code")
    identity_hash = build_identity_hash(
        person_id_custom=person_id_custom,
        name_kana_full_match=name_kana_full_match,
        gender_code=gender_code,
    )

    return {
        "person_id_custom": person_id_custom,
        "hia_subscriber_id": srow.get("hia_subscriber_id"),
        "name_kana_full": name_kana_full,
        "name_kanji_full": name_kanji_full,
        "name_kanji_family": srow.get("name_kanji_family"),
        "name_kanji_middle": srow.get("name_kanji_middle"),
        "name_kanji_given": srow.get("name_kanji_given"),
        "name_kana_family": srow.get("name_kana_family"),
        "name_kana_middle": srow.get("name_kana_middle"),
        "name_kana_given": srow.get("name_kana_given"),
        "name_kana_full_match": name_kana_full_match,
        "name_full_match": name_full_match,
        "gender_code": gender_code,
        "birth": srow.get("birth"),
        "insured_attribute_name": srow.get("insured_attribute_name"),
        "relationship_name": srow.get("relationship_name"),
        "insurer_number": srow.get("insurer_number"),
        "insurance_symbol": insurance_symbol,
        "insurance_symbol_export": normalize_insurance_symbol_export(insurance_symbol),
        "insurance_symbol_digits": srow.get("insurance_symbol_digits"),
        "insurance_symbol_match": normalize_insurance_symbol_match(insurance_symbol),
        "insurance_number": insurance_number,
        "insurance_number_match": normalize_insurance_number_match(insurance_number),
        "insurance_branchnumber": srow.get("insurance_branchnumber"),
        "qualification_acquired_date": srow.get("qualification_acquired_date"),
        "qualification_lost_date": srow.get("qualification_lost_date"),
        # Removed: "postal_code", "address_line", "building", "phone", "email"
        "employer_code": srow.get("employer_code"),
        "department_code": srow.get("department_code"),
        "distribution_code": srow.get("distribution_code"),
        "employee_code": srow.get("employee_code"),
        "connect_id": srow.get("connect_id"),
        "identity_hash": identity_hash,
    }




def fetch_existing_subscriber_by_hia_id(cur, hia_subscriber_id: Any) -> Optional[dict[str, Any]]:
    """HIA加入者IDで既存subscriberを取得する。"""
    if hia_subscriber_id is None or str(hia_subscriber_id).strip() == "":
        return None

    sql = """
        SELECT
            id,
            person_id_custom,
            hia_subscriber_id,
            name_kana_full,
            name_kanji_full,
            name_kanji_family,
            name_kanji_middle,
            name_kanji_given,
            name_kana_family,
            name_kana_middle,
            name_kana_given,
            name_kana_full_match,
            name_full_match,
            gender_code,
            birth,
            insured_attribute_name,
            relationship_name,
            insurer_number,
            insurance_symbol,
            insurance_symbol_export,
            insurance_symbol_digits,
            insurance_symbol_match,
            insurance_number,
            insurance_number_match,
            insurance_branchnumber,
            qualification_acquired_date,
            qualification_lost_date,
            -- removed: postal_code,
            -- removed: address_line,
            -- removed: building,
            -- removed: phone,
            -- removed: email,
            employer_code,
            department_code,
            distribution_code,
            employee_code,
            connect_id,
            identity_hash
        FROM subscribers
        WHERE hia_subscriber_id = %s
        LIMIT 1
    """
    cur.execute(sql, (str(hia_subscriber_id).strip(),))
    return cur.fetchone()


def fetch_existing_subscriber(cur, person_id_custom: str, name_kana_full_match: str, gender_code) -> Optional[dict[str, Any]]:
    sql = """
        SELECT
            id,
            person_id_custom,
            hia_subscriber_id,
            name_kana_full,
            name_kanji_full,
            name_kanji_family,
            name_kanji_middle,
            name_kanji_given,
            name_kana_family,
            name_kana_middle,
            name_kana_given,
            name_kana_full_match,
            name_full_match,
            gender_code,
            birth,
            insured_attribute_name,
            relationship_name,
            insurer_number,
            insurance_symbol,
            insurance_symbol_export,
            insurance_symbol_digits,
            insurance_symbol_match,
            insurance_number,
            insurance_number_match,
            insurance_branchnumber,
            qualification_acquired_date,
            qualification_lost_date,
            -- removed: postal_code,
            -- removed: address_line,
            -- removed: building,
            -- removed: phone,
            -- removed: email,
            employer_code,
            department_code,
            distribution_code,
            employee_code,
            connect_id
            , identity_hash
        FROM subscribers
        WHERE person_id_custom = %s
          AND name_kana_full_match = %s
          AND gender_code <=> %s
        LIMIT 1
    """
    cur.execute(sql, (person_id_custom, name_kana_full_match, gender_code))
    return cur.fetchone()



COMPARE_COLUMNS = [
    "name_kana_full",
    "name_kanji_full",
    "name_kanji_family",
    "name_kanji_middle",
    "name_kanji_given",
    "name_kana_family",
    "name_kana_middle",
    "name_kana_given",
    "name_kana_full_match",
    "name_full_match",
    "hia_subscriber_id",
    "gender_code",
    "birth",
    "insured_attribute_name",
    "relationship_name",
    "insurer_number",
    "insurance_symbol",
    "insurance_symbol_export",
    "insurance_symbol_digits",
    "insurance_symbol_match",
    "insurance_number",
    "insurance_number_match",
    "insurance_branchnumber",
    "qualification_acquired_date",
    "qualification_lost_date",
    # Removed: "postal_code", "address_line", "building", "phone", "email"
    "employer_code",
    "department_code",
    "distribution_code",
    "employee_code",
    "connect_id",
    "identity_hash",
]



AUDIT_COLUMNS = [
    "insurer_number",
    "insurance_symbol",
    "insurance_symbol_export",
    "insurance_symbol_digits",
    "insurance_number",
    "insurance_branchnumber",
    "birth",
    "gender_code",
    "name_kana_full",
    "name_kanji_full",
    "relationship_name",
    "qualification_acquired_date",
    "qualification_lost_date",
    "employer_code",
    "department_code",
    "distribution_code",
    "employee_code",
    "connect_id",
    "hia_subscriber_id",
    "identity_hash",
]

PARTS_COLUMNS = [
    "name_kanji_family",
    "name_kanji_middle",
    "name_kanji_given",
    "name_kana_family",
    "name_kana_middle",
    "name_kana_given",
]


def audit_value(value: Any) -> Optional[str]:
    """subscriber_audit 保存用に値を文字列化する。NULL は NULL のまま保持。"""
    if value is None:
        return None
    return str(value)


def insert_subscriber_audit_rows(cur, rows: list[tuple[Any, ...]]) -> None:
    """subscriber_audit へ複数行を一括 INSERT する。"""
    if not rows:
        return

    cur.executemany(
        """
        INSERT INTO subscriber_audit (
            subscriber_id,
            field,
            old_value,
            new_value,
            source,
            change_run_id
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """,
        rows,
    )


def build_insert_audit_rows(subscriber_id: int, run_id: int) -> list[tuple[Any, ...]]:
    return [
        (
            subscriber_id,
            "__insert__",
            None,
            "inserted",
            JOB_NAME,
            run_id,
        )
    ]



def build_update_audit_rows(
    subscriber_id: int,
    existing: dict[str, Any],
    vals: Dict[str, Any],
    run_id: int,
) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for col in AUDIT_COLUMNS:
        old_val = existing.get(col)
        new_val = vals.get(col)
        if old_val != new_val:
            rows.append(
                (
                    subscriber_id,
                    col,
                    audit_value(old_val),
                    audit_value(new_val),
                    JOB_NAME,
                    run_id,
                )
            )
    return rows


def is_blank_like(value: Any) -> bool:
    """NULL または空文字を未設定として扱う。"""
    if value is None:
        return True
    return str(value).strip() == ""


def build_effective_subscriber_update_vals(
    existing: dict[str, Any],
    vals: Dict[str, Any],
) -> Dict[str, Any]:
    """subscribers 更新用の実効値を作る。

    v1.1.0 方針:
    - parts 列は、staging 側が NULL/空文字なら既存値を保持する
    - parts 列は、staging 側が値ありならその値で更新候補とする
    - parts 以外は従来通り staging 側の値をそのまま採用する
    """
    effective = dict(vals)

    for col in PARTS_COLUMNS:
        incoming = vals.get(col)
        if is_blank_like(incoming):
            effective[col] = existing.get(col)

    return effective



def subscriber_differs(existing: dict[str, Any], vals: Dict[str, Any]) -> bool:
    effective = build_effective_subscriber_update_vals(existing, vals)
    for col in COMPARE_COLUMNS:
        if existing.get(col) != effective.get(col):
            return True
    return False



def insert_subscriber(cur, vals: Dict[str, Any], run_id: int) -> int:
    sql = """
        INSERT INTO subscribers (
            person_id_custom,
            hia_subscriber_id,
            name_kana_full,
            name_kanji_full,
            name_kanji_family,
            name_kanji_middle,
            name_kanji_given,
            name_kana_family,
            name_kana_middle,
            name_kana_given,
            name_kana_full_match,
            name_full_match,
            gender_code,
            birth,
            insured_attribute_name,
            relationship_name,
            insurer_number,
            insurance_symbol,
            insurance_symbol_export,
            insurance_symbol_digits,
            insurance_symbol_match,
            insurance_number,
            insurance_number_match,
            insurance_branchnumber,
            qualification_acquired_date,
            qualification_lost_date,
            employer_code,
            department_code,
            distribution_code,
            employee_code,
            connect_id,
            identity_hash,
            last_change_run_id,
            created_at,
            updated_at
        ) VALUES (
            %(person_id_custom)s,
            %(hia_subscriber_id)s,
            %(name_kana_full)s,
            %(name_kanji_full)s,
            %(name_kanji_family)s,
            %(name_kanji_middle)s,
            %(name_kanji_given)s,
            %(name_kana_family)s,
            %(name_kana_middle)s,
            %(name_kana_given)s,
            %(name_kana_full_match)s,
            %(name_full_match)s,
            %(gender_code)s,
            %(birth)s,
            %(insured_attribute_name)s,
            %(relationship_name)s,
            %(insurer_number)s,
            %(insurance_symbol)s,
            %(insurance_symbol_export)s,
            %(insurance_symbol_digits)s,
            %(insurance_symbol_match)s,
            %(insurance_number)s,
            %(insurance_number_match)s,
            %(insurance_branchnumber)s,
            %(qualification_acquired_date)s,
            %(qualification_lost_date)s,
            %(employer_code)s,
            %(department_code)s,
            %(distribution_code)s,
            %(employee_code)s,
            %(connect_id)s,
            %(identity_hash)s,
            %(last_change_run_id)s,
            NOW(3),
            NOW(3)
        )
    """
    params = dict(vals)
    params["last_change_run_id"] = run_id
    cur.execute(sql, params)
    return int(cur.lastrowid)




def update_subscriber(cur, subscriber_id: int, existing: dict[str, Any], vals: Dict[str, Any], run_id: int) -> None:
    effective_vals = build_effective_subscriber_update_vals(existing, vals)
    sql = """
        UPDATE subscribers
        SET
            name_kana_full = %(name_kana_full)s,
            name_kanji_full = %(name_kanji_full)s,
            name_kanji_family = %(name_kanji_family)s,
            name_kanji_middle = %(name_kanji_middle)s,
            name_kanji_given = %(name_kanji_given)s,
            name_kana_family = %(name_kana_family)s,
            name_kana_middle = %(name_kana_middle)s,
            name_kana_given = %(name_kana_given)s,
            name_kana_full_match = %(name_kana_full_match)s,
            name_full_match = %(name_full_match)s,
            hia_subscriber_id = %(hia_subscriber_id)s,
            gender_code = %(gender_code)s,
            birth = %(birth)s,
            insured_attribute_name = %(insured_attribute_name)s,
            relationship_name = %(relationship_name)s,
            insurer_number = %(insurer_number)s,
            insurance_symbol = %(insurance_symbol)s,
            insurance_symbol_export = %(insurance_symbol_export)s,
            insurance_symbol_digits = %(insurance_symbol_digits)s,
            insurance_symbol_match = %(insurance_symbol_match)s,
            insurance_number = %(insurance_number)s,
            insurance_number_match = %(insurance_number_match)s,
            insurance_branchnumber = %(insurance_branchnumber)s,
            qualification_acquired_date = %(qualification_acquired_date)s,
            qualification_lost_date = %(qualification_lost_date)s,
            employer_code = %(employer_code)s,
            department_code = %(department_code)s,
            distribution_code = %(distribution_code)s,
            employee_code = %(employee_code)s,
            connect_id = %(connect_id)s,
            identity_hash = %(identity_hash)s,
            last_change_run_id = %(last_change_run_id)s,
            updated_at = NOW(3)
        WHERE id = %(id)s
    """
    params = dict(effective_vals)
    params["last_change_run_id"] = run_id
    params["id"] = subscriber_id
    cur.execute(sql, params)


# Address and Contact apply helpers
def address_apply(cur, subscriber_id: int, postal_code: Any, address_line: Any, building: Any) -> None:
    """subscriber_addresses の current 行を staging 値で同期する。"""
    postal_code = postal_code or None
    address_line = address_line or None
    building = building or None

    cur.execute(
        """
        SELECT
            address_id,
            postal_code,
            address_line,
            building
        FROM subscriber_addresses
        WHERE subscriber_id = %s
          AND is_current = 1
        ORDER BY address_id DESC
        LIMIT 1
        """,
        (subscriber_id,),
    )
    existing = cur.fetchone()

    if existing is None:
        if postal_code is None and address_line is None and building is None:
            return
        cur.execute(
            """
            INSERT INTO subscriber_addresses (
                subscriber_id,
                postal_code,
                address_line,
                building,
                is_current,
                source,
                created_at,
                updated_at
            ) VALUES (
                %s, %s, %s, %s,
                1, 'hub_apply', NOW(3), NOW(3)
            )
            """,
            (subscriber_id, postal_code, address_line, building),
        )
        return

    if (
        existing.get("postal_code") == postal_code
        and existing.get("address_line") == address_line
        and existing.get("building") == building
    ):
        return

    cur.execute(
        """
        UPDATE subscriber_addresses
        SET is_current = 0,
            valid_to = NOW(3),
            updated_at = NOW(3)
        WHERE address_id = %s
        """,
        (existing["address_id"],),
    )

    if postal_code is None and address_line is None and building is None:
        return

    cur.execute(
        """
        INSERT INTO subscriber_addresses (
            subscriber_id,
            postal_code,
            address_line,
            building,
            valid_from,
            is_current,
            source,
            created_at,
            updated_at
        ) VALUES (
            %s, %s, %s, %s,
            NOW(3), 1, 'hub_apply', NOW(3), NOW(3)
        )
        """,
        (subscriber_id, postal_code, address_line, building),
    )


def contact_apply(cur, subscriber_id: int, phone: Any, email: Any) -> None:
    """subscriber_contacts の current 行を staging 値で同期する。"""
    phone = phone or None
    email = email or None

    cur.execute(
        """
        SELECT
            contact_id,
            phone,
            email
        FROM subscriber_contacts
        WHERE subscriber_id = %s
          AND is_current = 1
        ORDER BY contact_id DESC
        LIMIT 1
        """,
        (subscriber_id,),
    )
    existing = cur.fetchone()

    if existing is None:
        if phone is None and email is None:
            return
        cur.execute(
            """
            INSERT INTO subscriber_contacts (
                subscriber_id,
                phone,
                email,
                is_current,
                source,
                created_at,
                updated_at
            ) VALUES (
                %s, %s, %s,
                1, 'hub_apply', NOW(3), NOW(3)
            )
            """,
            (subscriber_id, phone, email),
        )
        return

    if existing.get("phone") == phone and existing.get("email") == email:
        return

    cur.execute(
        """
        UPDATE subscriber_contacts
        SET is_current = 0,
            valid_to = NOW(3),
            updated_at = NOW(3)
        WHERE contact_id = %s
        """,
        (existing["contact_id"],),
    )

    if phone is None and email is None:
        return

    cur.execute(
        """
        INSERT INTO subscriber_contacts (
            subscriber_id,
            phone,
            email,
            valid_from,
            is_current,
            source,
            created_at,
            updated_at
        ) VALUES (
            %s, %s, %s,
            NOW(3), 1, 'hub_apply', NOW(3), NOW(3)
        )
        """,
        (subscriber_id, phone, email),
    )



def mark_staging_processed(cur, stg_id: int, run_id: int) -> None:
    sql = """
        UPDATE staging_subscribers_hub
        SET processed_run_id = %s,
            processed_at = NOW(3)
        WHERE id = %s
    """
    cur.execute(sql, (run_id, stg_id))



def fetch_pending_staging_rows(cur, limit: int) -> list[dict[str, Any]]:
    sql = """
        SELECT
            id,
            person_id_custom,
            hia_subscriber_id,
            name_kana_full,
            name_kanji_full,
            name_kanji_family,
            name_kanji_middle,
            name_kanji_given,
            name_kana_family,
            name_kana_middle,
            name_kana_given,
            gender_code,
            birth,
            insured_attribute_name,
            relationship_name,
            insurer_number,
            insurance_symbol,
            insurance_symbol_digits,
            insurance_number,
            insurance_branchnumber,
            qualification_acquired_date,
            qualification_lost_date,
            postal_code,
            address_line,
            building,
            phone,
            email,
            employer_code,
            department_code,
            distribution_code,
            employee_code,
            connect_id,
            src_file,
            src_row_no,
            src_line_no,
            import_run_id,
            processed_run_id
        FROM staging_subscribers_hub
        WHERE processed_run_id IS NULL
        ORDER BY id ASC
    """
    if limit > 0:
        sql += " LIMIT %s"
        cur.execute(sql, (limit,))
    else:
        cur.execute(sql)
    return list(cur.fetchall())


# ============================================================
# apply core
# ============================================================

def apply_once(cur, srow: dict[str, Any], run_id: int) -> str:
    """return: insert | update | noop"""
    vals = build_subscriber_vals(cur, srow)

    # 1. HIA加入者IDがあれば最優先で既存subscriberを照合する。
    existing = fetch_existing_subscriber_by_hia_id(
        cur,
        vals.get("hia_subscriber_id"),
    )

    # 2. HIA加入者IDで見つからなければ、従来のidentity照合へfallbackする。
    if existing is None:
        existing = fetch_existing_subscriber(
            cur,
            vals["person_id_custom"],
            vals["name_kana_full_match"],
            vals["gender_code"],
        )

    if existing is None:
        subscriber_id = insert_subscriber(cur, vals, run_id)
        audit_rows = build_insert_audit_rows(subscriber_id, run_id)
        insert_subscriber_audit_rows(cur, audit_rows)
        address_apply(
            cur,
            subscriber_id,
            srow.get("postal_code"),
            srow.get("address_line"),
            srow.get("building"),
        )
        contact_apply(
            cur,
            subscriber_id,
            srow.get("phone"),
            srow.get("email"),
        )
        return "insert"

    subscriber_id = int(existing["id"])

    if subscriber_differs(existing, vals):
        audit_rows = build_update_audit_rows(subscriber_id, existing, vals, run_id)
        update_subscriber(cur, subscriber_id, existing, vals, run_id)
        insert_subscriber_audit_rows(cur, audit_rows)
        address_apply(
            cur,
            subscriber_id,
            srow.get("postal_code"),
            srow.get("address_line"),
            srow.get("building"),
        )
        contact_apply(
            cur,
            subscriber_id,
            srow.get("phone"),
            srow.get("email"),
        )
        return "update"

    address_apply(
        cur,
        subscriber_id,
        srow.get("postal_code"),
        srow.get("address_line"),
        srow.get("building"),
    )
    contact_apply(
        cur,
        subscriber_id,
        srow.get("phone"),
        srow.get("email"),
    )
    return "noop"


# ============================================================
# main
# ============================================================

def main() -> int:
    ap = argparse.ArgumentParser(description="Apply staging_subscribers_hub rows into subscribers")
    ap.add_argument("--schema", default=None, help="接続先 DB スキーマ名")
    ap.add_argument("--limit", type=int, default=0, help="処理する staging 行数の上限 (0 = 無制限)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    params_raw: MySQLParams = load_mysql_params()
    schema_name = "dev_phr"
    params = MySQLParams(
        host=params_raw.host,
        port=params_raw.port,
        user=params_raw.user,
        password=params_raw.password,
        database=schema_name,
    )

    db_path_str = f"{params.host}:{params.port}/{schema_name}"

    print(f"[INFO] DB_SCHEMA = {schema_name} (forced)")
    print(f"[INFO] DRY_RUN   = {args.dry_run}")
    print(f"[INFO] LIMIT     = {args.limit}")

    try:
        with connect_ctx(params) as conn:
            cur = dict_cursor(conn)
            metrics = RunMetrics()

            run_id = start_run(
                cur,
                phase="apply",
                source=JOB_NAME,
                db_schema=schema_name,
                db_path=db_path_str,
                input_base="staging_subscribers_hub",
                input_file=None,
                insurer_number=None,
                dry_run=args.dry_run,
                limit_rows=args.limit,
            )
            conn.commit()
            print(f"[INFO] run_id = {run_id}")

            try:
                rows = fetch_pending_staging_rows(cur, args.limit)
                total_rows = len(rows)
                print(f"[INFO] staging rows to apply = {total_rows}")

                for idx, srow in enumerate(rows, start=1):
                    metrics.rows_seen += 1
                    try:
                        op = apply_once(cur, srow, run_id)

                        if op == "insert":
                            metrics.rows_inserted += 1
                        elif op == "update":
                            metrics.rows_updated += 1
                        else:
                            metrics.rows_unchanged += 1

                        if not args.dry_run:
                            mark_staging_processed(cur, int(srow["id"]), run_id)

                        if should_log_progress(idx, total_rows):
                            print(
                                "[PROGRESS] "
                                f"{idx}/{total_rows} "
                                f"inserted={metrics.rows_inserted} "
                                f"updated={metrics.rows_updated} "
                                f"unchanged={metrics.rows_unchanged} "
                                f"errors={metrics.errors}",
                                flush=True,
                            )

                    except Exception as e:
                        metrics.errors += 1
                        log_error(
                            cur,
                            run_id,
                            phase="apply",
                            source=JOB_NAME,
                            insurer_number=(srow.get("insurer_number") or None),
                            src_file=(srow.get("src_file") or None),
                            row_no=srow.get("src_row_no"),
                            line_no=srow.get("src_line_no"),
                            field=None,
                            field_value=None,
                            error_code=type(e).__name__,
                            message=str(e),
                        )
                        if should_log_progress(idx, total_rows):
                            print(
                                "[PROGRESS] "
                                f"{idx}/{total_rows} "
                                f"inserted={metrics.rows_inserted} "
                                f"updated={metrics.rows_updated} "
                                f"unchanged={metrics.rows_unchanged} "
                                f"errors={metrics.errors}",
                                flush=True,
                            )
                        continue

                finish_run(
                    cur,
                    run_id,
                    metrics,
                    extra_notes="apply from staging_subscribers_hub",
                )
                if args.dry_run:
                    conn.rollback()
                    print(
                        f"[DRY-RUN] inserted={metrics.rows_inserted} "
                        f"updated={metrics.rows_updated} unchanged={metrics.rows_unchanged} "
                        f"errors={metrics.errors} run_id={run_id}"
                    )
                else:
                    conn.commit()
                    print(
                        f"[OK] inserted={metrics.rows_inserted} "
                        f"updated={metrics.rows_updated} unchanged={metrics.rows_unchanged} "
                        f"errors={metrics.errors} run_id={run_id}"
                    )

            except Exception as e:
                conn.rollback()
                metrics.errors += 1
                finish_run(
                    cur,
                    run_id,
                    metrics,
                    status_override="failed",
                    extra_notes=f"error={e}",
                )
                conn.commit()
                print(f"[ERR] apply 中に例外発生: {e}")
                return 7

    except Exception as e:
        print(f"[FATAL] DB 接続または実行時エラー: {e}")
        return 7

    return 0


if __name__ == "__main__":
    raise SystemExit(main())