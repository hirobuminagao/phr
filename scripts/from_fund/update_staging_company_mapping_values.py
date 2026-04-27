

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, cast

# ------------------------------------------------------------
# sys.path bootstrap
# ------------------------------------------------------------
# 直接実行時でも `scripts.*` import を解決できるようにする
if __package__ in (None, ""):
    THIS_FILE = Path(__file__).resolve()
    REPO_ROOT = THIS_FILE.parents[2]
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

from scripts.from_fund.script_lib.company_resolver import resolve_company_mapping
from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.lookup.hia_company import fetch_hia_company_master_rows_by_insurer_number
from scripts.lib.db.lookup.subscriber import get_subscriber_company_codes_by_id
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import DEV_PHR


@dataclass(frozen=True)
class UpdateSummary:
    total: int
    mapped: int
    not_found: int
    multiple_match: int
    config_error: int
    not_matched: int
    updated: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="staging_subscribers_fund の会社・部署mapping値を補完する",
    )
    parser.add_argument(
        "--import-run-id",
        required=True,
        type=int,
        help="対象となる staging_subscribers_fund.import_run_id",
    )
    parser.add_argument(
        "--insurer-number",
        required=True,
        help="対象保険者番号",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="UPDATEせず件数と判定だけ確認する",
    )
    return parser.parse_args()


def fetch_company_mapping_rules(
    conn: Any,
    insurer_number: str,
) -> list[dict[str, Any]]:
    """保険者番号に紐づく有効な会社mappingルールを取得する。"""
    cursor = dict_cursor(conn)
    try:
        cursor.execute(
            f"""
            SELECT
              fund_company_mapping_id,
              insurer_number,
              match_style,
              mapping_type,
              source_target_columns,
              source_match_rule,
              source_match_key,
              company_lookup_columns,
              company_lookup_rule,
              fixed_employer_code,
              fixed_department_code,
              priority,
              is_active,
              notes
            FROM {DEV_PHR}.fund_company_mapping
            WHERE insurer_number = %s
              AND is_active = 1
            ORDER BY priority, fund_company_mapping_id
            """,
            (insurer_number,),
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()

    return [dict(cast(Mapping[str, Any], row)) for row in rows]


def fetch_target_staging_rows(
    conn: Any,
    *,
    import_run_id: int,
    insurer_number: str,
) -> list[dict[str, Any]]:
    """対象import_run_idのstaging行を取得する。"""
    cursor = dict_cursor(conn)
    try:
        cursor.execute(
            f"""
            SELECT
              id,
              import_run_id,
              insurer_number_norm,
              insurance_symbol_norm,
              insurance_number_norm,
              received_company_code_norm,
              received_company_name_norm,
              received_department_code_norm,
              received_distribution_code_norm,
              received_employee_code_norm,
              relationship_code_norm,
              relationship_name_norm,
              matched_subscriber_id,
              mapped_employer_code,
              mapped_department_code,
              subscribers_employer_code,
              subscribers_department_code
            FROM {DEV_PHR}.staging_subscribers_fund
            WHERE import_run_id = %s
              AND insurer_number_norm = %s
            ORDER BY id
            """,
            (import_run_id, insurer_number),
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()

    return [dict(cast(Mapping[str, Any], row)) for row in rows]


def update_staging_company_values(
    conn: Any,
    *,
    staging_id: int,
    mapped_employer_code: int | None,
    mapped_department_code: int | None,
    subscribers_employer_code: int | None,
    subscribers_department_code: int | None,
) -> None:
    cursor = dict_cursor(conn)
    try:
        cursor.execute(
            f"""
            UPDATE {DEV_PHR}.staging_subscribers_fund
            SET
              mapped_employer_code = %s,
              mapped_department_code = %s,
              subscribers_employer_code = %s,
              subscribers_department_code = %s
            WHERE id = %s
            """,
            (
                mapped_employer_code,
                mapped_department_code,
                subscribers_employer_code,
                subscribers_department_code,
                staging_id,
            ),
        )
    finally:
        cursor.close()


def _subscriber_company_codes(
    conn: Any,
    matched_subscriber_id: Any,
) -> tuple[int | None, int | None]:
    subscriber_id = int(matched_subscriber_id) if matched_subscriber_id is not None else None
    row = get_subscriber_company_codes_by_id(conn, subscriber_id)
    if row is None:
        return None, None
    return row.get("employer_code"), row.get("department_code")


def run(
    *,
    import_run_id: int,
    insurer_number: str,
    dry_run: bool = False,
) -> UpdateSummary:
    params = load_mysql_base_params()

    total = 0
    mapped = 0
    not_found = 0
    multiple_match = 0
    config_error = 0
    not_matched = 0
    updated = 0

    with connect_ctx(params, database=DEV_PHR, autocommit=False) as conn:
        mappings = fetch_company_mapping_rules(conn, insurer_number)
        hia_company_rows = fetch_hia_company_master_rows_by_insurer_number(
            conn,
            insurer_number,
        )
        staging_rows = fetch_target_staging_rows(
            conn,
            import_run_id=import_run_id,
            insurer_number=insurer_number,
        )

        print(f"start update staging company mapping values")
        print(f"import_run_id: {import_run_id}")
        print(f"insurer_number: {insurer_number}")
        print(f"staging rows: {len(staging_rows)}")
        print(f"mapping rules: {len(mappings)}")
        print(f"hia company rows: {len(hia_company_rows)}")
        print(f"dry_run: {dry_run}")

        for row in staging_rows:
            total += 1
            subscribers_employer_code, subscribers_department_code = _subscriber_company_codes(
                conn,
                row.get("matched_subscriber_id"),
            )

            result = resolve_company_mapping(
                staging_row=row,
                mappings=mappings,
                hia_company_rows=hia_company_rows,
            )

            if result.status == "mapped":
                mapped += 1
            elif result.status == "multiple_match":
                multiple_match += 1
            elif result.status == "config_error":
                config_error += 1
            elif result.status == "not_matched":
                not_matched += 1
            else:
                not_found += 1

            if not dry_run:
                update_staging_company_values(
                    conn,
                    staging_id=int(row["id"]),
                    mapped_employer_code=result.mapped_employer_code,
                    mapped_department_code=result.mapped_department_code,
                    subscribers_employer_code=subscribers_employer_code,
                    subscribers_department_code=subscribers_department_code,
                )
                updated += 1

            if total % 1000 == 0:
                print(
                    "progress "
                    f"total={total}, mapped={mapped}, not_found={not_found}, "
                    f"multiple_match={multiple_match}, config_error={config_error}, "
                    f"not_matched={not_matched}"
                )

        if dry_run:
            conn.rollback()
        else:
            conn.commit()

    summary = UpdateSummary(
        total=total,
        mapped=mapped,
        not_found=not_found,
        multiple_match=multiple_match,
        config_error=config_error,
        not_matched=not_matched,
        updated=updated,
    )
    print(summary)
    return summary


def main() -> None:
    args = parse_args()
    run(
        import_run_id=args.import_run_id,
        insurer_number=args.insurer_number,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()