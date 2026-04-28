#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
from datetime import date, datetime
from pathlib import Path
from typing import Any

HIA_SUBSCRIBERS_EXPORT_DIR = Path("data/from_fund/export_staging_to_hia_subscribers")


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


def _norm_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _gender_name(value: Any) -> str:
    text = _norm_text(value)
    if text == "1":
        return "男"
    if text == "2":
        return "女"
    return ""


def _to_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    return int(text)


def _row_no_value(row: dict[str, Any]) -> int:
    value = row.get("src_row_no") or row.get("id")
    coerced = _to_int_or_none(value)
    if coerced is None:
        raise ValueError(f"row number is empty: id={row.get('id')}")
    return coerced


def _row_id_value(row: dict[str, Any]) -> int:
    coerced = _to_int_or_none(row.get("id"))
    if coerced is None:
        raise ValueError("row id is empty")
    return coerced


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
    sorted_rows = sorted(rows, key=lambda row: (_row_no_value(row), _row_id_value(row)))
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    for chunk in _chunks(sorted_rows, split_size):
        row_range = _row_no_range(chunk)
        filename = f"{status_label}_{insurer_number}_{timestamp}_{row_range}.csv"
        path = status_dir / filename
        write_hia_subscriber_export_csv(path, chunk)
        paths.append(path)

    return paths
