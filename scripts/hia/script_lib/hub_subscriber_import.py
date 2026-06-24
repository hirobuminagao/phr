

# -*- coding: utf-8 -*-

"""
============================================================
Module : hub_subscriber_import.py
Path   : scripts/hia/script_lib/hub_subscriber_import.py
Project: PHR

Purpose:
    Hub subscribers CSV import helper。

Responsibility:
    - Hub CSV row import orchestration
    - header mapping
    - canonical field generation
    - identity generation
    - import-side compare hash generation
    - staging_subscribers_hub INSERT
    - per-folder metrics aggregation

Non-goals:
    - argparse
    - DB connection lifecycle
    - etl run start / finish
    - ProgressLogger lifecycle
    - current snapshot lookup / hydrate
    - apply_action decision
    - detailed compare
============================================================
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict

from scripts.lib.etl import (
    RunMetrics,
    ProgressLogger,
    log_error,
)

from scripts.lib.csv.csv_loader import load_csv
from scripts.lib.io.directory_discovery import list_files_by_suffix

from scripts.lib.identity.generator import generate_identity_bundle
from scripts.lib.hash.compare_hash import build_compare_hash
from scripts.lib.identity.field.insurance_number import normalize_insurance_number
from scripts.lib.identity.field.insurance_symbol import normalize_insurance_symbol
from scripts.lib.identity.field.birthdate import normalize_birthdate
from scripts.lib.identity.field.gender_code import normalize_gender_code
from scripts.lib.identity.field.date_field import normalize_date_to_ymd_and_compact

from scripts.lib.identity.field.name_kana import (
    normalize_name_kana_full,
    normalize_name_kana_full_to_parts,
)

from scripts.lib.identity.field.name_kanji import (
    normalize_name_kanji_full,
    normalize_name_kanji_full_to_parts,
)


# ============================================================
# header mapping
# ============================================================

MAP: Dict[str, str] = {
    "加入者ID": "hia_subscriber_id",
    "被保険者証記号": "insurance_symbol",
    "被保険者証番号": "insurance_number",
    "被保険者証枝番": "insurance_branchnumber",
    "対象者氏名（カナ）": "name_kana_full",
    "対象者氏名（漢字）": "name_kanji_full",
    "性別": "gender_code",
    "生年月日": "birth",
    "資格取得日（家族認定日）": "qualification_acquired_date",
    "資格喪失日（家族削除日）": "qualification_lost_date",
    "郵便番号": "postal_code",
    "住所": "address_line",
    "住所（建物名）": "building",
    "電話番号": "phone",
    "メールアドレス": "email",
    "事業所（企業）コード": "employer_code",
    "所属コード": "department_code",
    "配付先コード": "distribution_code",
    "社員コード": "employee_code",
    "connectID": "connect_id",
    "個人ID": "external_person_id",
    "続柄名称": "relationship_name",
    "被保険者属性名": "insured_attribute_name",
}


# ============================================================
# metrics
# ============================================================

@dataclass
class FolderMetrics:
    """1 保険者フォルダ分の表示用集計（RunMetricsとは別、表示目的のみ）"""

    files: int = 0
    rows_seen: int = 0
    rows_inserted: int = 0
    rows_skipped: int = 0
    errors: int = 0

# ============================================================
# field normalize helpers
# ============================================================


def _require_field_ok(
    result: dict,
    *,
    field: str,
    src: str,
    line_no: int,
) -> dict:
    """field normalize result の ok を検査し、NGなら ValueError に変換する。"""

    if result.get("ok"):
        return result

    raise ValueError(
        f"正規化失敗: {field} "
        f"reason={result.get('reason')} "
        f"raw={result.get('raw')} "
        f"file={src} line={line_no}"
    )


def _normalize_optional_branch_number(raw: str) -> str:
    """保険証枝番 optional canonical。空欄は空欄のまま扱う。"""

    if raw is None or str(raw).strip() == "":
        return ""

    res = normalize_insurance_number(raw)
    if not res.get("ok"):
        return ""

    return str(res.get("field_norm") or "")

# ============================================================
# import
# ============================================================


def process_csv_dir(
    cur,
    run_id: int,
    insurer_number: int,
    folder: Path,
    *,
    metrics_all: RunMetrics,
    plog: ProgressLogger,
    limit: int = 0,
    dry_run: bool = False,
) -> FolderMetrics:

    m = FolderMetrics()
    csv_files = list_files_by_suffix(folder, ".csv")

    if not csv_files:
        print(f"[WARN] CSV が見つかりません: {folder}")
        return m

    for csv_path in csv_files:
        m.files += 1
        metrics_all.files += 1

        loader = load_csv(
            path=str(csv_path),
            header_count=1,
        )

        line_no = 1
        csv_row_no = 0

        for row in loader.iter_dict_rows():
            line_no += 1
            csv_row_no += 1

            m.rows_seen += 1
            metrics_all.rows_seen += 1

            try:
                # csv_loader は BOM / encoding / header handling を担当する
                # --- 1) ヘッダ名 → 内部キーへマッピング（未知キーはそのまま残す） ---
                src = {MAP.get(k, k): (row.get(k, "") or "") for k in row.keys()}

                # --- 2) 必須キーの正規化（insurance_number / birth / gender / symbol） ---
                # field normalize は dict を返す。
                # DB保存・compare hash 用には field_norm を使う。
                try:
                    number_res = _require_field_ok(
                        normalize_insurance_number(
                            src.get("insurance_number", "")
                        ),
                        field="insurance_number",
                        src=csv_path.name,
                        line_no=line_no,
                    )

                    symbol_res = _require_field_ok(
                        normalize_insurance_symbol(
                            src.get("insurance_symbol", "")
                        ),
                        field="insurance_symbol",
                        src=csv_path.name,
                        line_no=line_no,
                    )

                    birth_res = _require_field_ok(
                        normalize_birthdate(
                            src.get("birth", "")
                            or row.get("生年月日", "")
                        ),
                        field="birth",
                        src=csv_path.name,
                        line_no=line_no,
                    )

                    gender_res = _require_field_ok(
                        normalize_gender_code(
                            src.get("gender_code", "")
                            or row.get("性別", "")
                        ),
                        field="gender_code",
                        src=csv_path.name,
                        line_no=line_no,
                    )

                    insurance_number_text = str(
                        number_res.get("field_norm") or ""
                    )

                    branchnum_text = _normalize_optional_branch_number(
                        src.get("insurance_branchnumber", "")
                    )

                    birth = str(
                        birth_res.get("field_norm") or ""
                    )

                    gender_code = str(
                        gender_res.get("field_norm") or ""
                    )

                    insurance_symbol_norm = str(
                        symbol_res.get("field_norm") or ""
                    )

                    sym_digits = str(
                        symbol_res.get("digits")
                        or symbol_res.get("person_id_custom")
                        or ""
                    )
                except Exception as e:
                    m.rows_skipped += 1
                    m.errors += 1
                    metrics_all.rows_skipped += 1
                    metrics_all.errors += 1
                    log_error(
                        cur,
                        run_id,
                        phase="import",
                        source="import_subscribers_to_staging_hub",
                        insurer_number=f"{insurer_number:08d}",
                        src_file=csv_path.name,
                        row_no=csv_row_no,
                        line_no=line_no,
                        field=None,
                        field_value=None,
                        error_code=type(e).__name__,
                        message=str(e),
                    )
                    plog.tick()
                    continue

                # --- 3) 氏名 canonical field生成（カナ必須） ---
                kanji_full_raw = (
                    src.get("name_kanji_full", "")
                    or row.get("対象者氏名（漢字）", "")
                ).strip()
                kana_full_raw = (
                    src.get("name_kana_full", "")
                    or row.get("対象者氏名（カナ）", "")
                ).strip()

                if not kana_full_raw:
                    raise ValueError(
                        f"必須フィールド欠損: name_kana_full "
                        f"file={csv_path.name} line={line_no}"
                    )

                try:
                    kana_full_res = normalize_name_kana_full(kana_full_raw)
                    kana_parts_res = normalize_name_kana_full_to_parts(kana_full_raw)

                    kanji_full_res = normalize_name_kanji_full(kanji_full_raw)
                    kanji_parts_res = normalize_name_kanji_full_to_parts(kanji_full_raw)

                except Exception as e:
                    m.rows_skipped += 1
                    m.errors += 1
                    metrics_all.rows_skipped += 1
                    metrics_all.errors += 1
                    log_error(
                        cur,
                        run_id,
                        phase="import",
                        source="import_subscribers_to_staging_hub",
                        insurer_number=f"{insurer_number:08d}",
                        src_file=csv_path.name,
                        row_no=csv_row_no,
                        line_no=line_no,
                        field=None,
                        field_value=None,
                        error_code=type(e).__name__,
                        message=str(e),
                    )
                    plog.tick()
                    continue

                # compare hash 方針:
                #   - import行から生成できる hash はここで作る
                #   - DB current snapshot 由来の hash は current_snapshot 側で持つ
                #   - apply_action 判定や detailed compare はここでは行わない
                #   - compare hash は match値ではなく、staging格納用のnorm値を材料にする
                # --- 4) identity生成（generator責務） ---
                try:
                    identity_bundle = generate_identity_bundle(
                        birthdate=src.get("birth", "") or row.get("生年月日", ""),
                        insurer_number_raw=f"{insurer_number:08d}",
                        insurance_symbol_raw=src.get("insurance_symbol", ""),
                        insurance_number_raw=src.get("insurance_number", ""),
                        gender_code=gender_code,
                        name_kana_full_raw=kana_full_raw,
                    )

                    person_id_custom = identity_bundle["person_id_custom"]
                    identity_hash = identity_bundle["identity_hash"]

                except Exception as e:
                    m.rows_skipped += 1
                    m.errors += 1
                    metrics_all.rows_skipped += 1
                    metrics_all.errors += 1
                    log_error(
                        cur,
                        run_id,
                        phase="import",
                        source="import_subscribers_to_staging_hub",
                        insurer_number=f"{insurer_number:08d}",
                        src_file=csv_path.name,
                        row_no=csv_row_no,
                        line_no=line_no,
                        field=None,
                        field_value=None,
                        error_code=type(e).__name__,
                        message=str(e),
                    )
                    plog.tick()
                    continue

                qualification_acquired_res = normalize_date_to_ymd_and_compact(
                    src.get("qualification_acquired_date", ""),
                    purpose="qualification_date",
                )
                qualification_lost_res = normalize_date_to_ymd_and_compact(
                    src.get("qualification_lost_date", ""),
                    purpose="qualification_date",
                )
                qualification_acquired_date_hash = str(
                    qualification_acquired_res.get("field_norm") or ""
                )
                qualification_lost_date_hash = str(
                    qualification_lost_res.get("field_norm") or ""
                )

                qualification_acquired_date_insert = (
                    qualification_acquired_res.get("field_norm")
                    if qualification_acquired_res.get("ok")
                    else None
                )
                qualification_lost_date_insert = (
                    qualification_lost_res.get("field_norm")
                    if qualification_lost_res.get("ok")
                    else None
                )

                # --- 5) import-side compare hash生成 ---
                compare_identity_norm_hash = build_compare_hash(
                    [
                        insurance_symbol_norm,
                        insurance_number_text,
                        kana_full_res["field_norm"],
                        kanji_full_res["field_norm"],
                        birth,
                        gender_code,
                    ]
                )

                compare_other_hash = build_compare_hash(
                    [
                        src.get("insured_attribute_name", ""),
                        src.get("relationship_name", ""),
                        qualification_acquired_date_hash,
                        qualification_lost_date_hash,
                        src.get("employer_code", ""),
                        src.get("department_code", ""),
                        src.get("distribution_code", ""),
                        src.get("employee_code", ""),
                        src.get("connect_id", ""),
                    ]
                )

                postal_code = src.get("postal_code", "")
                address_line = src.get("address_line", "")
                building = src.get("building", "")

                if (
                    not str(postal_code).strip()
                    and not str(address_line).strip()
                    and not str(building).strip()
                ):
                    address_hash = None
                else:
                    address_hash = build_compare_hash(
                        [
                            postal_code,
                            address_line,
                            building,
                        ]
                    )

                vals = {
                    "person_id_custom": person_id_custom,
                    "identity_hash": identity_hash,
                    "compare_identity_norm_hash": compare_identity_norm_hash,
                    "compare_other_hash": compare_other_hash,
                    "hia_subscriber_id": src.get("hia_subscriber_id", ""),
                    "name_kana_full": kana_full_res["field_norm"],
                    "name_kana_full_match": kana_full_res["match"],
                    "name_kanji_full": kanji_full_res["field_norm"],
                    "name_kanji_full_match": kanji_full_res["match"],
                    "name_kanji_family": kanji_parts_res.get("family", ""),
                    "name_kanji_middle": kanji_parts_res.get("middle", ""),
                    "name_kanji_given": kanji_parts_res.get("given", ""),
                    "name_kana_family": kana_parts_res.get("family", ""),
                    "name_kana_middle": kana_parts_res.get("middle", ""),
                    "name_kana_given": kana_parts_res.get("given", ""),
                    "gender_code": gender_code,
                    "birth": birth,
                    "insured_attribute_name": src.get("insured_attribute_name", ""),
                    "relationship_name": src.get("relationship_name", ""),
                    "insurer_number": f"{insurer_number:08d}",
                    "insurance_symbol": insurance_symbol_norm,
                    "insurance_symbol_digits": sym_digits,
                    "insurance_number": insurance_number_text,
                    "insurance_branchnumber": branchnum_text,
                    "qualification_acquired_date": qualification_acquired_date_insert,
                    "qualification_lost_date": qualification_lost_date_insert,
                    "postal_code": postal_code,
                    "address_line": address_line,
                    "building": building,
                    "address_hash": address_hash,
                    "phone": src.get("phone", ""),
                    "email": src.get("email", ""),
                    "employer_code": src.get("employer_code", ""),
                    "department_code": src.get("department_code", ""),
                    "distribution_code": src.get("distribution_code", ""),
                    "employee_code": src.get("employee_code", ""),
                    "connect_id": src.get("connect_id", ""),
                    "src_file": csv_path.name,
                    "src_row_no": csv_row_no,
                    "src_line_no": line_no,
                    "import_run_id": run_id,
                }

                if not dry_run:
                    cur.execute(
                        """
                        INSERT INTO staging_subscribers_hub (
                            person_id_custom,
                            identity_hash,
                            compare_identity_norm_hash,
                            compare_other_hash,
                            hia_subscriber_id,
                            name_kana_full_match,
                            name_kanji_full_match,
                            name_kana_full, name_kanji_full,
                            name_kanji_family, name_kanji_middle, name_kanji_given,
                            name_kana_family, name_kana_middle, name_kana_given,
                            gender_code, birth,
                            insured_attribute_name, relationship_name,
                            insurer_number, insurance_symbol, insurance_symbol_digits,
                            insurance_number, insurance_branchnumber,
                            qualification_acquired_date, qualification_lost_date,
                            postal_code, address_line, building,
                            address_hash,
                            phone, email,
                            employer_code, department_code, distribution_code,
                            employee_code, connect_id,
                            created_at, loaded_at, processed_at,
                            src_file, src_row_no, src_line_no, import_run_id
                        )
                        VALUES (
                            %(person_id_custom)s,
                            %(identity_hash)s,
                            %(compare_identity_norm_hash)s,
                            %(compare_other_hash)s,
                            %(hia_subscriber_id)s,
                            %(name_kana_full_match)s,
                            %(name_kanji_full_match)s,
                            %(name_kana_full)s, %(name_kanji_full)s,
                            %(name_kanji_family)s, %(name_kanji_middle)s, %(name_kanji_given)s,
                            %(name_kana_family)s, %(name_kana_middle)s, %(name_kana_given)s,
                            %(gender_code)s, %(birth)s,
                            %(insured_attribute_name)s, %(relationship_name)s,
                            %(insurer_number)s, %(insurance_symbol)s, %(insurance_symbol_digits)s,
                            %(insurance_number)s, %(insurance_branchnumber)s,
                            %(qualification_acquired_date)s, %(qualification_lost_date)s,
                            %(postal_code)s, %(address_line)s, %(building)s,
                            %(address_hash)s,
                            %(phone)s, %(email)s,
                            %(employer_code)s, %(department_code)s, %(distribution_code)s,
                            %(employee_code)s, %(connect_id)s,
                            NOW(), NOW(), NULL,
                            %(src_file)s, %(src_row_no)s, %(src_line_no)s, %(import_run_id)s
                        )
                        """,
                        vals,
                    )

                m.rows_inserted += 1
                metrics_all.rows_inserted += 1

            except ValueError as e:
                m.rows_skipped += 1
                m.errors += 1
                metrics_all.rows_skipped += 1
                metrics_all.errors += 1
                log_error(
                    cur,
                    run_id,
                    phase="import",
                    source="import_subscribers_to_staging_hub",
                    insurer_number=f"{insurer_number:08d}",
                    src_file=csv_path.name,
                    row_no=csv_row_no,
                    line_no=line_no,
                    field=None,
                    field_value=None,
                    error_code=type(e).__name__,
                    message=str(e),
                )
            except Exception as e:
                m.rows_skipped += 1
                m.errors += 1
                metrics_all.rows_skipped += 1
                metrics_all.errors += 1
                log_error(
                    cur,
                    run_id,
                    phase="import",
                    source="import_subscribers_to_staging_hub",
                    insurer_number=f"{insurer_number:08d}",
                    src_file=csv_path.name,
                    row_no=csv_row_no,
                    line_no=line_no,
                    field=None,
                    field_value=None,
                    error_code=type(e).__name__,
                    message=str(e),
                )

            plog.tick()

            if limit and (
                metrics_all.rows_inserted + metrics_all.rows_skipped
            ) >= limit:
                break

        if limit and (
            metrics_all.rows_inserted + metrics_all.rows_skipped
        ) >= limit:
            break

    return m

