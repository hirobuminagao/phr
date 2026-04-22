#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
import_staging_subscribers_fund.py

健保受領CSVを staging_subscribers_fund へ取り込む実装。

今回版の目的:
- input/<insurer_number>/ 配下のCSVを取得する
- csv_loader でCSVを読む
- insurer_number -> fund_id を lookup で解決する
- templates / template_mappings を取得する
- mapping に従って *_norm / 補助列を生成する
- insurer_number_norm をスクリプト側で注入する
- relationship_name_match を補完する
- person_id_custom / identity_hash を生成する
- matched_subscriber_id を解決する
- staging_subscribers_fund へ INSERT する
- CSV単位で etl_run / etl_errors を記録する
- import_run_id / loaded_at を staging 行へ付与する
- success / partial かつ rows_inserted > 0 の場合、apply スクリプトを起動する

今回版ではまだ行わないもの:
- archive 移動
- skip 理由の明細記録
- apply 側エラーの import run への集約
"""

from __future__ import annotations

import argparse
from datetime import datetime
import sys
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, cast

# VS Code の Run で直接実行した場合でも `scripts.*` を import できるようにする
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.lib.csv.csv_loader import load_csv
from scripts.lib.db.config import MySQLBaseParams, load_mysql_base_params
from scripts.lib.db.lookup.fund import get_fund_id_from_insurer_number
from scripts.lib.db.lookup.subscriber import get_single_subscriber_id_by_identity_hash
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import DEV_PHR
from scripts.lib.identity.field.date_field import normalize_date_to_ymd_and_compact
from scripts.lib.identity.field.name_kana import (
    normalize_name_kana_full,
    normalize_name_kana_full_to_parts,
    norm_parts_to_match_parts as kana_norm_parts_to_match_parts,
)
from scripts.lib.identity.field.name_kanji import (
    normalize_name_kanji_full,
    normalize_name_kanji_full_to_parts,
    norm_parts_to_match_parts as kanji_norm_parts_to_match_parts,
)
from scripts.lib.identity.base_norm import base_normalize
from scripts.lib.identity.field.insurance_number import normalize_insurance_number
from scripts.lib.identity.field.insurance_symbol import normalize_insurance_symbol
from scripts.lib.identity.generator import (
    generate_identity_hash,
    generate_person_id_custom,
)

from scripts.lib.transform.relationship import (
    normalize_relationship_code_match,
    resolve_relationship_name,
)

from scripts.lib.etl.metrics import RunMetrics
from scripts.lib.etl.runs import finish_run, start_run
from scripts.lib.etl.errors import log_error


DEFAULT_INPUT_BASE_DIR = Path("data/from_fund/import_subscribers_staging/input")
ETL_PHASE = "import"
ETL_SOURCE = "staging_subscribers_fund"
SUPPORTED_RULES = {
    "as_is",
    "symbol_norm",
    "symbol_digits",
    "symbol_match",
    "number_match",
    "insurance_number_norm",
    "digits_required",
    "digits_or_null",
    "birth_norm",
    "gender_code_norm",
    "date_or_null",
    "kana_full_no_space",
    "name_kana_full_match",
    "split_family_kana_match",
    "split_middle_kana_match",
    "split_given_kana_match",
    "name_kanji_full_norm",
    "name_kanji_full_match",
    "split_family_match",
    "split_middle_match",
    "split_given_match",
    "split_family",
    "split_middle",
    "split_given",
    "split_family_kana",
    "split_middle_kana",
    "split_given_kana",
}


@dataclass
class TemplateRow:
    fund_id: int
    version: int
    template_type: str
    target_table: str


@dataclass
class MappingRow:
    csv_header: str
    target_column: str
    rule: str
    required: int


@dataclass
class RowErrorRecord:
    src_file: str
    src_row_no: int
    csv_header: str
    target_column: str
    rule: str
    raw_value: Any
    reason: str


def decide_row_error_code(message: str) -> str:
    """行エラー文言から error_code を決定する。"""
    if message.startswith("required missing:"):
        return "required_missing"
    if message.startswith("rule apply failed:"):
        return "rule_apply_failed"
    return "row_error"


@dataclass
class ProcessFileResult:
    rows_seen_count: int
    inserted_row_count: int
    skipped_empty_row_count: int
    row_error_count: int
    row_errors: list[RowErrorRecord]


def validate_mapping_headers(loader: Any, mappings: list[MappingRow]) -> None:
    header_map = loader.get_header_dict()
    missing = sorted({m.csv_header for m in mappings if m.csv_header not in header_map})
    if missing:
        headers = list(header_map.keys())
        raise ValueError(
            "template_mappings.csv_header が CSV ヘッダーに存在しません: "
            f"missing={missing} available={headers}"
        )



def is_effectively_empty_row(source_row: Mapping[str, Any]) -> bool:
    """Excelエクスポート由来の全列空行をスキップ対象とする。"""
    key_fields = [
        source_row.get("記号"),
        source_row.get("番号"),
        source_row.get("氏名（カナ）"),
        source_row.get("氏名（漢字）"),
    ]
    return all((v is None or str(v).strip() == "") for v in key_fields)


# None / 空文字 / 空白のみ（全角空白含む）を空値として扱う
def is_effectively_blank_value(value: Any) -> bool:
    """None / 空文字 / 空白のみ（全角空白含む）を空値として扱う。"""
    if value is None:
        return True
    text = str(value).replace("　", " ").strip()
    return text == ""


def row_get_str(row: Mapping[str, Any], key: str) -> str:
    value = row.get(key)
    if value is None:
        raise ValueError(f"missing column: {key}")
    return str(value)



def row_get_int(row: Mapping[str, Any], key: str) -> int:
    value = row.get(key)
    if value is None:
        raise ValueError(f"missing column: {key}")
    return int(value)



def build_db_path(params: MySQLBaseParams, schema_name: str) -> str:
    """ETL run 記録用の db_path を host:port/schema 形式で返す。"""
    host = str(params.host).strip() if params.host is not None else ""
    port = params.port
    if host == "":
        host = "unknown-host"
    port_text = str(port).strip() if port is not None else "unknown-port"
    return f"{host}:{port_text}/{schema_name}"


def run_apply_staging_subscribers_fund(import_run_id: int) -> None:
    """staging_subscribers_fund apply スクリプトを run_id 指定で起動する。"""
    subprocess.run(
        [
            sys.executable,
            "-m",
            "scripts.from_fund.apply_staging_subscribers_fund_to_subscribers",
            "--run-id",
            str(import_run_id),
        ],
        check=True,
    )


def normalize_digits_or_none(value: str) -> str | None:
    digits = "".join(ch for ch in value if ch.isdigit())
    return digits or None


def normalize_gender_code(value: str) -> int | None:
    text = str(value).strip()
    if text in {"1", "男", "男性"}:
        return 1
    if text in {"2", "女", "女性"}:
        return 2
    if text in {"9", "0", ""}:
        return None
    return None


def enrich_relationship_fields(row: dict[str, Any]) -> dict[str, Any]:
    """relationship_name_norm / relationship_code_norm から relationship_name_match を補完する。"""
    if row.get("relationship_name_match") not in (None, ""):
        return row

    relationship_name_norm = row.get("relationship_name_norm")
    relationship_code_norm = row.get("relationship_code_norm")
    relationship_code_match = normalize_relationship_code_match(relationship_code_norm)

    relationship_name_match = resolve_relationship_name(
        relationship_name_norm=relationship_name_norm,
        relationship_code_norm=relationship_code_match,
    )
    if relationship_name_match not in (None, ""):
        row["relationship_name_match"] = relationship_name_match

    return row


def _to_int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def enrich_identity_fields(
    row: dict[str, Any],
    insurer_number: str,
    source_row: Mapping[str, Any],
) -> dict[str, Any]:
    """row と source_row から person_id_custom / identity_hash を補完する。

    方針:
    - generator には raw を基本入力として渡す
    - gender は既に row 側で norm 済みならそれを利用する
    - 必須材料が不足する場合は無理に生成しない
    - matched_subscriber_id の解決はこの段階では行わない
    """
    birth_raw = source_row.get("生年月日")
    symbol_raw = source_row.get("記号")
    number_raw = source_row.get("番号")
    name_kana_raw = source_row.get("氏名（カナ）")
    gender_code = _to_int_or_none(row.get("gender_code_norm"))

    if row.get("person_id_custom") in (None, ""):
        pid_res = generate_person_id_custom(
            birthdate=birth_raw,
            insurer_number_raw=insurer_number,
            insurance_symbol_raw=symbol_raw,
            insurance_number_raw=number_raw,
        )
        if pid_res.get("ok") and pid_res.get("value") not in (None, ""):
            row["person_id_custom"] = pid_res["value"]

    if row.get("identity_hash") in (None, ""):
        person_id_custom = row.get("person_id_custom")
        if person_id_custom not in (None, "") and name_kana_raw not in (None, "") and gender_code is not None:
            identity_res = generate_identity_hash(
                person_id_custom=person_id_custom,
                name_kana_full_raw=name_kana_raw,
                gender_code=gender_code,
            )
            if identity_res.get("ok") and identity_res.get("value") not in (None, ""):
                row["identity_hash"] = identity_res["value"]

    return row


def apply_rule(rule: str, value: str | None, *, kanji_cur: Any | None = None) -> Any:
    if value is None:
        return None
    v = str(value).strip()

    if rule == "as_is":
        return v or None

    if rule == "symbol_norm":
        result = normalize_insurance_symbol(v)
        if not result["ok"]:
            return None
        return result["field_norm"]

    if rule == "symbol_match":
        result = normalize_insurance_symbol(v)
        if not result["ok"]:
            return None
        return result["match"]

    if rule == "symbol_digits":
        result = normalize_insurance_symbol(v)
        if not result["ok"]:
            return None
        digits = result.get("person_id_custom")
        return int(digits) if digits else None

    if rule == "digits_required":
        digits = normalize_digits_or_none(v)
        if not digits:
            raise ValueError("digits_required: digits not found")
        return digits

    if rule == "insurance_number_norm":
        result = normalize_insurance_number(v)
        if not result["ok"]:
            raise ValueError(f"insurance_number_norm: {result['reason']}")
        return result["field_norm"]

    if rule == "number_match":
        result = normalize_insurance_number(v)
        if not result["ok"]:
            return None
        return result["match"]

    if rule == "digits_or_null":
        return normalize_digits_or_none(v)

    if rule == "birth_norm":
        result = normalize_date_to_ymd_and_compact(v, purpose="birthdate")
        if not result["ok"]:
            raise ValueError(f"birth_norm: {result['reason']}")
        return result["field_norm"]

    if rule == "gender_code_norm":
        return normalize_gender_code(v)

    if rule == "date_or_null":
        result = normalize_date_to_ymd_and_compact(v, purpose="date_field")
        if not result["ok"]:
            return None
        return result["field_norm"]

    if rule == "kana_full_no_space":
        result = normalize_name_kana_full(v)
        if not result["ok"]:
            return None
        return result["field_norm"]

    if rule == "name_kana_full_match":
        result = normalize_name_kana_full(v)
        if not result["ok"]:
            return None
        return result["match"]

    if rule == "split_family_kana_match":
        parts = normalize_name_kana_full_to_parts(v)
        if not parts["ok"]:
            return None
        match_parts = kana_norm_parts_to_match_parts(parts)
        return match_parts["family"]

    if rule == "split_middle_kana_match":
        parts = normalize_name_kana_full_to_parts(v)
        if not parts["ok"]:
            return None
        match_parts = kana_norm_parts_to_match_parts(parts)
        return match_parts["middle"]

    if rule == "split_given_kana_match":
        parts = normalize_name_kana_full_to_parts(v)
        if not parts["ok"]:
            return None
        match_parts = kana_norm_parts_to_match_parts(parts)
        return match_parts["given"]

    if rule == "name_kanji_full_norm":
        result = normalize_name_kanji_full(v)
        if not result["ok"]:
            return None
        return result["field_norm"]

    if rule == "name_kanji_full_match":
        if kanji_cur is None:
            raise ValueError("name_kanji_full_match requires kanji_cur")
        result = normalize_name_kanji_full(v, cur=kanji_cur)
        if not result["ok"]:
            return None
        return result["match"]

    if rule == "split_family_match":
        if kanji_cur is None:
            raise ValueError("split_family_match requires kanji_cur")
        parts = normalize_name_kanji_full_to_parts(v)
        if not parts["ok"]:
            return None
        match_parts = kanji_norm_parts_to_match_parts(parts, kanji_cur)
        return match_parts["family"]

    if rule == "split_middle_match":
        if kanji_cur is None:
            raise ValueError("split_middle_match requires kanji_cur")
        parts = normalize_name_kanji_full_to_parts(v)
        if not parts["ok"]:
            return None
        match_parts = kanji_norm_parts_to_match_parts(parts, kanji_cur)
        return match_parts["middle"]

    if rule == "split_given_match":
        if kanji_cur is None:
            raise ValueError("split_given_match requires kanji_cur")
        parts = normalize_name_kanji_full_to_parts(v)
        if not parts["ok"]:
            return None
        match_parts = kanji_norm_parts_to_match_parts(parts, kanji_cur)
        return match_parts["given"]

    if rule == "split_family":
        result = normalize_name_kanji_full_to_parts(v)
        if not result["ok"]:
            return None
        return result["family"]

    if rule == "split_middle":
        result = normalize_name_kanji_full_to_parts(v)
        if not result["ok"]:
            return None
        return result["middle"]

    if rule == "split_given":
        result = normalize_name_kanji_full_to_parts(v)
        if not result["ok"]:
            return None
        return result["given"]

    if rule == "split_family_kana":
        result = normalize_name_kana_full_to_parts(v)
        if not result["ok"]:
            return None
        return result["family"]

    if rule == "split_middle_kana":
        result = normalize_name_kana_full_to_parts(v)
        if not result["ok"]:
            return None
        return result["middle"]

    if rule == "split_given_kana":
        result = normalize_name_kana_full_to_parts(v)
        if not result["ok"]:
            return None
        return result["given"]

    raise ValueError(f"unsupported rule: {rule}")


def fetch_latest_template(conn: Any, fund_id: int) -> TemplateRow:
    cursor = dict_cursor(conn)
    try:
        cursor.execute(
            f"""
            SELECT fund_id, version, template_type, target_table
            FROM {DEV_PHR}.templates
            WHERE fund_id = %s
            ORDER BY version DESC
            LIMIT 1
            """,
            (fund_id,),
        )
        row = cast(Mapping[str, Any] | None, cursor.fetchone())
    finally:
        cursor.close()

    if not row:
        raise ValueError(f"template not found: fund_id={fund_id}")

    return TemplateRow(
        fund_id=row_get_int(row, "fund_id"),
        version=row_get_int(row, "version"),
        template_type=row_get_str(row, "template_type"),
        target_table=row_get_str(row, "target_table"),
    )


def fetch_template_mappings(conn: Any, fund_id: int, version: int) -> list[MappingRow]:
    cursor = dict_cursor(conn)
    try:
        cursor.execute(
            f"""
            SELECT csv_header, target_column, rule, required
            FROM {DEV_PHR}.template_mappings
            WHERE fund_id = %s AND version = %s
            ORDER BY col_order, target_column
            """,
            (fund_id, version),
        )
        rows = cast(list[Mapping[str, Any]], cursor.fetchall())
    finally:
        cursor.close()

    return [
        MappingRow(
            csv_header=row_get_str(row, "csv_header"),
            target_column=row_get_str(row, "target_column"),
            rule=row_get_str(row, "rule"),
            required=row_get_int(row, "required"),
        )
        for row in rows
    ]


def build_row(
    conn: Any,
    fund_id: int,
    version: int,
    insurer_number: str,
    src_file: str,
    src_row_no: int,
    source_row: dict[str, Any],
    mappings: list[MappingRow],
    *,
    import_run_id: int | None = None,
    loaded_at: datetime | None = None,
    kanji_cur: Any | None = None,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "fund_id": fund_id,
        "version": version,
        "import_run_id": import_run_id,
        "loaded_at": loaded_at,
        "insurer_number_norm": insurer_number.zfill(8),
        "src_file": src_file,
        "src_row_no": src_row_no,
        "src_line_no": src_row_no + 1,
    }

    for m in mappings:
        raw_value = source_row.get(m.csv_header)
        try:
            value = apply_rule(m.rule, raw_value, kanji_cur=kanji_cur)
        except Exception as e:
            raise ValueError(
                "rule apply failed: "
                f"header={m.csv_header} target={m.target_column} rule={m.rule} "
                f"raw_value={raw_value!r} src_file={src_file} src_row_no={src_row_no}"
            ) from e

        if m.required == 1 and (value is None or value == ""):
            raise ValueError(
                "required missing: "
                f"header={m.csv_header} target={m.target_column} rule={m.rule} "
                f"raw_value={raw_value!r} src_file={src_file} src_row_no={src_row_no}"
            )

        existing_value = row.get(m.target_column)

        # 同一 target_column に対して複数 mapping がある場合の現行最適解:
        # - 後勝ちを基本とする
        # - ただし、後続値が空値(None / "" / 空白のみ)なら既存の有効値を潰さない
        if existing_value is not None and not is_effectively_blank_value(existing_value):
            if is_effectively_blank_value(value):
                continue

        row[m.target_column] = value

    row = enrich_relationship_fields(row)
    row = enrich_identity_fields(
        row,
        insurer_number=insurer_number,
        source_row=source_row,
    )
    row = enrich_matched_subscriber_id(conn, row)

    return row

def enrich_matched_subscriber_id(conn: Any, row: dict[str, Any]) -> dict[str, Any]:
    """identity_hash から matched_subscriber_id を補完する。

    方針:
    - identity_hash が無い場合は何もしない
    - 1件一致なら matched_subscriber_id を設定する
    - 0件なら未設定のままにする
    - 複数件ヒット時の扱いは lookup 側に委譲する
    """
    if row.get("matched_subscriber_id") not in (None, ""):
        return row

    identity_hash = row.get("identity_hash")
    if identity_hash in (None, ""):
        return row

    subscriber_id = get_single_subscriber_id_by_identity_hash(conn, identity_hash)
    if subscriber_id is not None:
        row["matched_subscriber_id"] = subscriber_id

    return row


def to_row_error_record(
    error: Exception,
    mappings: list[MappingRow],
    source_row: Mapping[str, Any],
    src_file: str,
    src_row_no: int,
) -> RowErrorRecord:
    """行エラーをログ用の構造へ変換する。"""
    message = str(error)

    for m in mappings:
        raw_value = source_row.get(m.csv_header)
        marker = f"header={m.csv_header} target={m.target_column} rule={m.rule}"
        if marker in message:
            return RowErrorRecord(
                src_file=src_file,
                src_row_no=src_row_no,
                csv_header=m.csv_header,
                target_column=m.target_column,
                rule=m.rule,
                raw_value=raw_value,
                reason=message,
            )

    return RowErrorRecord(
        src_file=src_file,
        src_row_no=src_row_no,
        csv_header="",
        target_column="",
        rule="",
        raw_value=None,
        reason=message,
    )


def insert_row(conn: Any, row: dict[str, Any]) -> None:
    cols = sorted(row.keys())
    placeholders = ", ".join(["%s"] * len(cols))
    col_sql = ", ".join(f"`{c}`" for c in cols)
    sql = f"""
        INSERT INTO {DEV_PHR}.staging_subscribers_fund
        ({col_sql})
        VALUES ({placeholders})
    """
    values = [row[c] for c in cols]

    cursor = dict_cursor(conn)
    try:
        cursor.execute(sql, tuple(values))
    finally:
        cursor.close()


def process_file(
    conn: Any,
    etl_conn: Any,
    insurer_number: str,
    path: Path,
    *,
    import_run_id: int | None = None,
    loaded_at: datetime | None = None,
) -> ProcessFileResult:
    fund_id = get_fund_id_from_insurer_number(insurer_number)
    template = fetch_latest_template(conn, fund_id)
    mappings = fetch_template_mappings(conn, fund_id, template.version)

    unsupported = sorted({m.rule for m in mappings if m.rule not in SUPPORTED_RULES})
    if unsupported:
        raise ValueError(f"unsupported rules found: {unsupported}")

    loader = load_csv(path=str(path), header_count=1)
    validate_mapping_headers(loader, mappings)

    inserted_row_count = 0
    skipped_empty_row_count = 0
    row_error_count = 0
    rows_seen_count = 0
    row_errors: list[RowErrorRecord] = []
    kanji_cur = dict_cursor(conn)
    try:
        for i, row_src in enumerate(loader.iter_dict_rows(), start=1):
            rows_seen_count += 1
            if is_effectively_empty_row(row_src):
                skipped_empty_row_count += 1
                continue

            try:
                row = build_row(
                    conn,
                    fund_id,
                    template.version,
                    insurer_number,
                    path.name,
                    i,
                    row_src,
                    mappings,
                    import_run_id=import_run_id,
                    loaded_at=loaded_at,
                    kanji_cur=kanji_cur,
                )
                insert_row(conn, row)
                inserted_row_count += 1
            except ValueError as e:
                row_error_count += 1
                row_error = to_row_error_record(e, mappings, row_src, path.name, i)
                row_errors.append(row_error)

                if import_run_id is not None:
                    error_cur = dict_cursor(etl_conn)
                    try:
                        log_error(
                            error_cur,
                            import_run_id,
                            phase=ETL_PHASE,
                            source=ETL_SOURCE,
                            insurer_number=insurer_number,
                            src_file=row_error.src_file,
                            row_no=row_error.src_row_no,
                            line_no=row_error.src_row_no + 1,
                            field=row_error.target_column or row_error.csv_header,
                            field_value=(
                                None if row_error.raw_value is None else str(row_error.raw_value)
                            ),
                            error_code=decide_row_error_code(row_error.reason),
                            message=row_error.reason,
                            person_id_custom=None,
                        )
                    finally:
                        error_cur.close()
                continue
    finally:
        kanji_cur.close()

    return ProcessFileResult(
        rows_seen_count=rows_seen_count,
        inserted_row_count=inserted_row_count,
        skipped_empty_row_count=skipped_empty_row_count,
        row_error_count=row_error_count,
        row_errors=row_errors,
    )


def list_files(base: Path) -> list[tuple[str, Path]]:
    results: list[tuple[str, Path]] = []
    if not base.exists():
        return results

    for d in sorted(base.iterdir()):
        if not d.is_dir():
            continue
        for f in sorted(d.glob("*.csv")):
            results.append((d.name, f))
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-base-dir", default=str(DEFAULT_INPUT_BASE_DIR))
    args = parser.parse_args()

    base = Path(args.input_base_dir)
    files = list_files(base)

    if not files:
        print(f"[INFO] no csv files found under: {base}")
        return

    params = load_mysql_base_params()
    db_path = build_db_path(params, DEV_PHR)

    with connect_ctx(params, database=DEV_PHR, autocommit=False) as conn, connect_ctx(
        params, database=DEV_PHR, autocommit=True
    ) as etl_conn:
        total_inserted = 0
        total_skipped_empty = 0
        total_row_errors = 0
        completed_with_errors = False

        for insurer_number, path in files:
            metrics = RunMetrics()
            loaded_at = datetime.now()

            run_cur = dict_cursor(etl_conn)
            try:
                run_id = start_run(
                    run_cur,
                    phase=ETL_PHASE,
                    source=ETL_SOURCE,
                    db_schema=DEV_PHR,
                    db_path=db_path,
                    input_base=str(base),
                    input_file=path.name,
                    insurer_number=insurer_number,
                    dry_run=False,
                    limit_rows=None,
                )
            finally:
                run_cur.close()

            print(f"processing: {insurer_number} {path}")

            try:
                result = process_file(
                    conn,
                    etl_conn,
                    insurer_number,
                    path,
                    import_run_id=run_id,
                    loaded_at=loaded_at,
                )

                metrics.files = 1
                metrics.rows_seen = result.rows_seen_count
                metrics.rows_inserted = result.inserted_row_count
                metrics.rows_skipped = result.skipped_empty_row_count
                metrics.errors = result.row_error_count

                conn.commit()

                run_cur = dict_cursor(etl_conn)
                try:
                    finish_run(run_cur, run_id, metrics)
                finally:
                    run_cur.close()

                total_inserted += result.inserted_row_count
                total_skipped_empty += result.skipped_empty_row_count
                total_row_errors += result.row_error_count

                print(f"inserted rows: {result.inserted_row_count}")
                print(f"skipped empty rows: {result.skipped_empty_row_count}")
                print(f"row errors: {result.row_error_count}")

                if result.row_errors:
                    completed_with_errors = True
                    print("[WARN] row errors detected:")
                    for row_error in result.row_errors[:20]:
                        print(
                            "  - "
                            f"src_file={row_error.src_file} "
                            f"src_row_no={row_error.src_row_no} "
                            f"csv_header={row_error.csv_header!r} "
                            f"target_column={row_error.target_column!r} "
                            f"rule={row_error.rule!r} "
                            f"raw_value={row_error.raw_value!r} "
                            f"reason={row_error.reason}"
                        )
                    if len(result.row_errors) > 20:
                        print(f"  ... omitted {len(result.row_errors) - 20} more row errors")

                if result.row_error_count > 0:
                    run_status = "partial"
                    print("run status: partial")
                else:
                    run_status = "success"
                    print("run status: success")

                if run_status in {"success", "partial"} and result.inserted_row_count > 0:
                    print(f"apply start: run_id={run_id}")
                    try:
                        run_apply_staging_subscribers_fund(run_id)
                        print(f"apply done: run_id={run_id}")
                    except Exception as e:
                        print(f"[WARN] apply failed: run_id={run_id} error={e}")

            except Exception as e:
                conn.rollback()

                run_cur = dict_cursor(etl_conn)
                try:
                    finish_run(
                        run_cur,
                        run_id,
                        metrics,
                        status_override="failed",
                        extra_notes=str(e),
                    )
                finally:
                    run_cur.close()

                print("run status: failed")
                raise

        print(f"total inserted rows: {total_inserted}")
        print(f"total skipped empty rows: {total_skipped_empty}")
        print(f"total row errors: {total_row_errors}")

        if completed_with_errors:
            print("overall status: completed_with_errors")
        else:
            print("overall status: success")


if __name__ == "__main__":
    main()