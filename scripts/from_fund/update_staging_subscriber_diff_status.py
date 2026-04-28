#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence, cast

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

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import DEV_PHR

DIFF_OUTPUT_DIR = Path("data/from_fund/diff_output")
HIA_SUBSCRIBERS_EXPORT_DIR = Path("data/from_fund/export_staging_to_hia_subscribers")

DIFF_STATUS_NO_CHANGE = "no_change"
DIFF_STATUS_ADD = "add"
DIFF_STATUS_UPDATE = "update"
DIFF_STATUS_UNKNOWN = "unknown"

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
    missing_from_new: int
    missing_from_new_path: str | None
    add_export_paths: list[str]
    update_export_paths: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="staging_subscribers_fund と subscribers の差分ステータス更新、およびHIA登録用CSV出力を行う",
    )
    parser.add_argument(
        "--config",
        required=True,
        help="差分判定設定YAMLファイルパス",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="DB更新とCSV出力を行わず、判定件数のみ確認する",
    )
    return parser.parse_args()


def load_config(path: str | Path) -> DiffConfig:
    with Path(path).open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

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


def _placeholders(values: Sequence[Any]) -> str:
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
              id,
              import_run_id,
              src_row_no,
              insurer_number_norm,
              insurance_symbol_norm,
              insurance_number_norm,
              insurance_symbol_match,
              insurance_number_match,
              insurance_branchnumber_norm,
              person_id_custom,
              identity_hash,
              matched_subscriber_id,
              name_kana_full_norm,
              name_kana_full_match,
              name_kanji_full_norm,
              name_kanji_full_match,
              birth_norm,
              gender_code_norm,
              relationship_name_norm,
              relationship_name_match,
              qualification_acquired_date_norm,
              qualification_lost_date_norm,
              postal_code_norm,
              address_line_norm,
              building_norm,
              phone_norm,
              email_norm,
              received_company_code_norm,
              received_department_code_norm,
              received_distribution_code_norm,
              received_employee_code_norm,
              connect_id_norm,
              mapped_employer_code,
              mapped_department_code,
              subscribers_employer_code,
              subscribers_department_code,
              diff_status,
              diff_status_method,
              diff_status_reason
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
              id,
              insurer_number,
              insurance_symbol,
              insurance_number,
              insurance_branchnumber,
              insurance_symbol_match,
              insurance_number_match,
              person_id_custom,
              identity_hash,
              name_kana_full,
              name_kana_full_match,
              name_kanji_full,
              name_kanji_full_match,
              birth,
              gender_code,
              relationship_name,
              qualification_acquired_date,
              qualification_lost_date,
              postal_code,
              address_line,
              building,
              phone,
              email,
              employer_code,
              department_code
            FROM {DEV_PHR}.subscribers
            WHERE id IN ({_placeholders(subscriber_ids)})
            """,
            tuple(subscriber_ids),
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()

    return {int(row["id"]): dict(cast(Mapping[str, Any], row)) for row in rows}
def fetch_missing_from_new_rows(
    conn: Any,
    *,
    insurer_number: str,
    import_run_ids: list[int],
) -> list[dict[str, Any]]:
    """subscribersに存在し、対象staging import_runに存在しない人を取得する。"""
    cursor = dict_cursor(conn)
    try:
        cursor.execute(
            f"""
            SELECT
              s.id AS subscriber_id,
              s.insurer_number,
              s.insurance_symbol,
              s.insurance_number,
              s.insurance_branchnumber,
              s.insurance_symbol_match,
              s.insurance_number_match,
              s.person_id_custom,
              s.identity_hash,
              s.name_kana_full,
              s.name_kana_full_match,
              s.name_kanji_full,
              s.name_kanji_full_match,
              s.birth,
              s.gender_code,
              s.relationship_name,
              s.qualification_acquired_date,
              s.qualification_lost_date,
              s.employer_code,
              s.department_code
            FROM {DEV_PHR}.subscribers s
            WHERE s.insurer_number = %s
              AND NOT EXISTS (
                SELECT 1
                FROM {DEV_PHR}.staging_subscribers_fund stg
                WHERE stg.insurer_number_norm = s.insurer_number
                  AND stg.import_run_id IN ({_placeholders(import_run_ids)})
                  AND stg.identity_hash = s.identity_hash
              )
            ORDER BY s.insurance_symbol_match, s.insurance_number_match, s.name_kana_full_match
            """,
            (insurer_number, *import_run_ids),
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()

    return [dict(cast(Mapping[str, Any], row)) for row in rows]
def _norm_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _same(a: Any, b: Any) -> bool:
    return _norm_text(a) == _norm_text(b)


def _date_same(a: Any, b: Any) -> bool:
    # MySQL DATE / str / None の比較揺れを避けるため文字列化して比較する。
    return _norm_text(a) == _norm_text(b)


def classify_staging_row(
    staging_row: dict[str, Any],
    subscribers_by_id: dict[int, dict[str, Any]],
) -> tuple[str, str]:
    """staging行を no_change / add / update / unknown に分類する。

    現時点では、identity_hash が既存 subscribers に一致しない行は add に寄せる。
    major変更候補の探索（identity構成要素による候補検索）は将来対応とする。
    """
    matched_subscriber_id = staging_row.get("matched_subscriber_id")
    if matched_subscriber_id is None:
        # 現時点では identity_hash が既存 subscribers に一致しない行は add に寄せる。
        # 将来的には、person_id_custom / 記号番号 / 氏名カナ / 性別などから
        # 既存候補を探索し、major変更候補として分離する余地を残す。
        return DIFF_STATUS_ADD, "identity_hash not matched; treated as add"

    subscriber = subscribers_by_id.get(int(matched_subscriber_id))
    if subscriber is None:
        # matched_subscriber_id が指す subscribers が存在しない場合も、現時点では自動更新せず add に寄せる。
        # データ不整合の可能性があるため、reasonにはIDを残す。
        return DIFF_STATUS_ADD, f"matched_subscriber_id not found; treated as add: {matched_subscriber_id}"

    differences: list[str] = []

    checks = [
        ("insurance_symbol_match", staging_row.get("insurance_symbol_match"), subscriber.get("insurance_symbol_match")),
        ("insurance_number_match", staging_row.get("insurance_number_match"), subscriber.get("insurance_number_match")),
        ("name_kana_full_match", staging_row.get("name_kana_full_match"), subscriber.get("name_kana_full_match")),
        ("name_kanji_full_match", staging_row.get("name_kanji_full_match"), subscriber.get("name_kanji_full_match")),
        ("gender_code", staging_row.get("gender_code_norm"), subscriber.get("gender_code")),
        ("relationship_name", staging_row.get("relationship_name_norm"), subscriber.get("relationship_name")),
        ("postal_code", staging_row.get("postal_code_norm"), subscriber.get("postal_code")),
        ("address_line", staging_row.get("address_line_norm"), subscriber.get("address_line")),
        ("building", staging_row.get("building_norm"), subscriber.get("building")),
        ("phone", staging_row.get("phone_norm"), subscriber.get("phone")),
        ("email", staging_row.get("email_norm"), subscriber.get("email")),
        ("employer_code", staging_row.get("mapped_employer_code"), subscriber.get("employer_code")),
        ("department_code", staging_row.get("mapped_department_code"), subscriber.get("department_code")),
    ]

    for label, left, right in checks:
        if not _same(left, right):
            differences.append(label)

    date_checks = [
        ("birth", staging_row.get("birth_norm"), subscriber.get("birth")),
        ("qualification_acquired_date", staging_row.get("qualification_acquired_date_norm"), subscriber.get("qualification_acquired_date")),
        ("qualification_lost_date", staging_row.get("qualification_lost_date_norm"), subscriber.get("qualification_lost_date")),
    ]
    for label, left, right in date_checks:
        if not _date_same(left, right):
            differences.append(label)

    if differences:
        return DIFF_STATUS_UPDATE, "changed: " + ",".join(differences)

    return DIFF_STATUS_NO_CHANGE, "no differences"
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


def write_missing_from_new_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "subscriber_id",
        "insurer_number",
        "insurance_symbol",
        "insurance_number",
        "insurance_branchnumber",
        "insurance_symbol_match",
        "insurance_number_match",
        "person_id_custom",
        "identity_hash",
        "name_kana_full",
        "name_kana_full_match",
        "name_kanji_full",
        "name_kanji_full_match",
        "birth",
        "gender_code",
        "relationship_name",
        "qualification_acquired_date",
        "qualification_lost_date",
        "employer_code",
        "department_code",
    ]

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def build_hia_export_base_dir(
    *,
    insurer_number: str,
    import_run_ids: list[int],
    now: datetime | None = None,
) -> Path:
    now = now or datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    run_part = "-".join(str(v) for v in sorted(import_run_ids))
    dirname = f"{timestamp}_{insurer_number}_sort{run_part}"
    return HIA_SUBSCRIBERS_EXPORT_DIR / dirname


def _csv_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _csv_date(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (date, datetime)):
        return value.strftime("%Y-%m-%d")
    return str(value)


def _gender_name(value: Any) -> str:
    text = _norm_text(value)
    if text == "1":
        return "男"
    if text == "2":
        return "女"
    return ""


def _row_no_value(row: dict[str, Any]) -> int:
    value = row.get("src_row_no") or row.get("id")
    return int(value)


def _row_no_range(rows: list[dict[str, Any]]) -> str:
    row_numbers = [_row_no_value(row) for row in rows]
    return f"{min(row_numbers)}-{max(row_numbers)}"


def _chunks(rows: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [rows[i : i + size] for i in range(0, len(rows), size)]


def build_hia_subscriber_export_row(staging_row: dict[str, Any]) -> dict[str, str]:
    """staging行からHIA加入者情報登録用CSV 1行を生成する。"""
    return {
        "被保険者証記号": _csv_value(staging_row.get("insurance_symbol_norm")),
        "被保険者証番号": _csv_value(staging_row.get("insurance_number_norm")),
        "被保険者証枝番": _csv_value(staging_row.get("insurance_branchnumber_norm")),
        "被保険者属性名": "",
        "続柄名称": _csv_value(staging_row.get("relationship_name_norm")),
        "対象者氏名（漢字）": _csv_value(staging_row.get("name_kanji_full_norm")),
        "対象者氏名（カナ）": _csv_value(staging_row.get("name_kana_full_norm")),
        "性別": _gender_name(staging_row.get("gender_code_norm")),
        "生年月日": _csv_date(staging_row.get("birth_norm")),
        "資格取得日（家族認定日）": _csv_date(staging_row.get("qualification_acquired_date_norm")),
        "資格喪失日（家族削除日）": _csv_date(staging_row.get("qualification_lost_date_norm")),
        "郵便番号": _csv_value(staging_row.get("postal_code_norm")),
        "住所": _csv_value(staging_row.get("address_line_norm")),
        "住所（建物名）": _csv_value(staging_row.get("building_norm")),
        "電話番号": _csv_value(staging_row.get("phone_norm")),
        "メールアドレス": _csv_value(staging_row.get("email_norm")),
        "事業所（企業）コード": _csv_value(staging_row.get("mapped_employer_code")),
        "所属コード": _csv_value(staging_row.get("mapped_department_code")),
        "配付先コード": _csv_value(staging_row.get("received_distribution_code_norm")),
        "社員コード": _csv_value(staging_row.get("received_employee_code_norm")),
        "connectID": _csv_value(staging_row.get("connect_id_norm")),
        "個人ID": "",
    }


def write_hia_subscriber_export_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "被保険者証記号",
        "被保険者証番号",
        "被保険者証枝番",
        "被保険者属性名",
        "続柄名称",
        "対象者氏名（漢字）",
        "対象者氏名（カナ）",
        "性別",
        "生年月日",
        "資格取得日（家族認定日）",
        "資格喪失日（家族削除日）",
        "郵便番号",
        "住所",
        "住所（建物名）",
        "電話番号",
        "メールアドレス",
        "事業所（企業）コード",
        "所属コード",
        "配付先コード",
        "社員コード",
        "connectID",
        "個人ID",
    ]

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(build_hia_subscriber_export_row(row))


def write_hia_subscriber_export_files(
    *,
    base_dir: Path,
    status_label: str,
    insurer_number: str,
    rows: list[dict[str, Any]],
    split_size: int,
) -> list[Path]:
    if not rows:
        return []

    paths: list[Path] = []
    status_dir = base_dir / status_label
    sorted_rows = sorted(rows, key=lambda row: (_row_no_value(row), int(row["id"])))
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    for chunk in _chunks(sorted_rows, split_size):
        row_range = _row_no_range(chunk)
        filename = f"{status_label}_{insurer_number}_{timestamp}_{row_range}.csv"
        path = status_dir / filename
        write_hia_subscriber_export_csv(path, chunk)
        paths.append(path)

    return paths


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
              id,
              insurer_number,
              insurance_symbol,
              insurance_number,
              insurance_branchnumber,
              insurance_symbol_match,
              insurance_number_match,
              person_id_custom,
              identity_hash,
              name_kana_full,
              name_kana_full_match,
              name_kanji_full,
              name_kanji_full_match,
              birth,
              gender_code,
              relationship_name,
              qualification_acquired_date,
              qualification_lost_date,
              postal_code,
              address_line,
              building,
              phone,
              email,
              employer_code,
              department_code
            FROM {DEV_PHR}.subscribers
            WHERE id IN ({_placeholders(subscriber_ids)})
            """,
            tuple(subscriber_ids),
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()

    return {int(row["id"]): dict(cast(Mapping[str, Any], row)) for row in rows}


def fetch_missing_from_new_rows(
    conn: Any,
    *,
    insurer_number: str,
    import_run_ids: list[int],
) -> list[dict[str, Any]]:
    """subscribersに存在し、対象staging import_runに存在しない人を取得する。"""
    cursor = dict_cursor(conn)
    try:
        cursor.execute(
            f"""
            SELECT
              s.id AS subscriber_id,
              s.insurer_number,
              s.insurance_symbol,
              s.insurance_number,
              s.insurance_branchnumber,
              s.insurance_symbol_match,
              s.insurance_number_match,
              s.person_id_custom,
              s.identity_hash,
              s.name_kana_full,
              s.name_kana_full_match,
              s.name_kanji_full,
              s.name_kanji_full_match,
              s.birth,
              s.gender_code,
              s.relationship_name,
              s.qualification_acquired_date,
              s.qualification_lost_date,
              s.employer_code,
              s.department_code
            FROM {DEV_PHR}.subscribers s
            WHERE s.insurer_number = %s
              AND NOT EXISTS (
                SELECT 1
                FROM {DEV_PHR}.staging_subscribers_fund stg
                WHERE stg.insurer_number_norm = s.insurer_number
                  AND stg.import_run_id IN ({_placeholders(import_run_ids)})
                  AND stg.identity_hash = s.identity_hash
              )
            ORDER BY s.insurance_symbol_match, s.insurance_number_match, s.name_kana_full_match
            """,
            (insurer_number, *import_run_ids),
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()

    return [dict(cast(Mapping[str, Any], row)) for row in rows]


def _norm_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _same(a: Any, b: Any) -> bool:
    return _norm_text(a) == _norm_text(b)


def _date_same(a: Any, b: Any) -> bool:
    # MySQL DATE / str / None の比較揺れを避けるため文字列化して比較する。
    return _norm_text(a) == _norm_text(b)


def classify_staging_row(
    staging_row: dict[str, Any],
    subscribers_by_id: dict[int, dict[str, Any]],
) -> tuple[str, str]:
    """staging行を no_change / add / update / unknown に分類する。"""
    matched_subscriber_id = staging_row.get("matched_subscriber_id")
    if matched_subscriber_id is None:
        return DIFF_STATUS_ADD, "matched_subscriber_id is NULL"

    subscriber = subscribers_by_id.get(int(matched_subscriber_id))
    if subscriber is None:
        return DIFF_STATUS_UNKNOWN, f"matched_subscriber_id not found: {matched_subscriber_id}"

    differences: list[str] = []

    checks = [
        ("insurance_symbol_match", staging_row.get("insurance_symbol_match"), subscriber.get("insurance_symbol_match")),
        ("insurance_number_match", staging_row.get("insurance_number_match"), subscriber.get("insurance_number_match")),
        ("name_kana_full_match", staging_row.get("name_kana_full_match"), subscriber.get("name_kana_full_match")),
        ("name_kanji_full_match", staging_row.get("name_kanji_full_match"), subscriber.get("name_kanji_full_match")),
        ("gender_code", staging_row.get("gender_code_norm"), subscriber.get("gender_code")),
        ("relationship_name", staging_row.get("relationship_name_norm"), subscriber.get("relationship_name")),
        ("postal_code", staging_row.get("postal_code_norm"), subscriber.get("postal_code")),
        ("address_line", staging_row.get("address_line_norm"), subscriber.get("address_line")),
        ("building", staging_row.get("building_norm"), subscriber.get("building")),
        ("phone", staging_row.get("phone_norm"), subscriber.get("phone")),
        ("email", staging_row.get("email_norm"), subscriber.get("email")),
        ("employer_code", staging_row.get("mapped_employer_code"), subscriber.get("employer_code")),
        ("department_code", staging_row.get("mapped_department_code"), subscriber.get("department_code")),
    ]

    for label, left, right in checks:
        if not _same(left, right):
            differences.append(label)

    date_checks = [
        ("birth", staging_row.get("birth_norm"), subscriber.get("birth")),
        ("qualification_acquired_date", staging_row.get("qualification_acquired_date_norm"), subscriber.get("qualification_acquired_date")),
        ("qualification_lost_date", staging_row.get("qualification_lost_date_norm"), subscriber.get("qualification_lost_date")),
    ]
    for label, left, right in date_checks:
        if not _date_same(left, right):
            differences.append(label)

    if differences:
        return DIFF_STATUS_UPDATE, "changed: " + ",".join(differences)

    return DIFF_STATUS_NO_CHANGE, "no differences"


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


def write_missing_from_new_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "subscriber_id",
        "insurer_number",
        "insurance_symbol",
        "insurance_number",
        "insurance_branchnumber",
        "insurance_symbol_match",
        "insurance_number_match",
        "person_id_custom",
        "identity_hash",
        "name_kana_full",
        "name_kana_full_match",
        "name_kanji_full",
        "name_kanji_full_match",
        "birth",
        "gender_code",
        "relationship_name",
        "qualification_acquired_date",
        "qualification_lost_date",
        "employer_code",
        "department_code",
    ]

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def run(config: DiffConfig, *, dry_run: bool = False) -> DiffSummary:
    params = load_mysql_base_params()

    no_change = 0
    add = 0
    update = 0
    unknown = 0
    add_export_rows: list[dict[str, Any]] = []
    update_export_rows: list[dict[str, Any]] = []

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

        for row in staging_rows:
            status, reason = classify_staging_row(row, subscribers_by_id)
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
        missing_from_new=len(missing_rows),
        missing_from_new_path=str(missing_path) if missing_path else None,
        add_export_paths=[str(path) for path in add_export_paths],
        update_export_paths=[str(path) for path in update_export_paths],
    )
    print(summary)
    return summary


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    run(config, dry_run=args.dry_run)


if __name__ == "__main__":
    main()