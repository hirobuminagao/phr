#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
hia_import_zip.py

HIA → fund ledger import pipeline

責務:
- input_zip から HIA ダウンロードZIPを取得
- ZIP展開
- XMLの人物識別情報を読み取り
- person_year / xml_event をDBに記帳
- エラーを error.txt に出力
- 成功ZIPを archive_zip に移動

注意:
- ZIP単位 all-or-nothing
- XMLを全件検証してからDB記帳
"""

import zipfile
import shutil
from pathlib import Path
from datetime import datetime, date


import re
import unicodedata
import sys
from pathlib import Path

# ------------------------------------------------------------
# VSCode Run ボタン (file実行) 対応
# ファイル直実行でも project root を import path に追加する
# ------------------------------------------------------------
if __name__ == "__main__" and __package__ is None:
    project_root = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(project_root))

# VSCode / Pylance では phr ルートを workspace root として解決させるため、
# import は scripts.work_folder... の絶対パッケージ形式に統一する。
# 実行時も phr ルートで `python -m scripts.work_folder.scripts.hia_import_zip` を前提にする。
from scripts.work_folder.scripts.hia_parse_xml import parse_hia_xml_identity
from scripts.work_folder.lib.custom_id_gen import generate_id
from scripts.work_folder.lib.db.config import load_mysql_params
from scripts.work_folder.lib.db.mysql import connect_ctx, dict_cursor
from scripts.work_folder.lib.normalize.common import (
    normalize_insurance_number_match,
    normalize_insurance_symbol_match,
    normalize_symbol_for_custom_id,
    normalize_birth_yyyymmdd,
    normalize_name_kana_match,
    build_identity_hash,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
WORK_FOLDER_DIR = PROJECT_ROOT / "scripts" / "work_folder"
DATA_DIR = PROJECT_ROOT / "data"


# ============================================================
# 初期設定
# ============================================================

# data はコード置き場 scripts と分離し、phr/data 配下に置く。
# HIA の入出力実データは scripts/work_folder ではなく data/hia_export を使う。
HIA_EXPORT_DIR = DATA_DIR / "hia_export"
MAT_DIR = WORK_FOLDER_DIR / "mat"

INPUT_ZIP_DIR = HIA_EXPORT_DIR / "input_zip"
ARCHIVE_ZIP_DIR = HIA_EXPORT_DIR / "archive_zip"
WORK_DIR = HIA_EXPORT_DIR / "work"
OUTPUT_DIR = HIA_EXPORT_DIR / "output_to_fund"

EXAM_YEAR_START_MONTH = 4
EXAM_YEAR_START_DAY = 1

# ------------------------------------------------------------
# import_zip の責務は以下に限定する。
# - ZIP単位の探索
# - ZIP名からの文脈取得
# - ZIP展開
# - XML identity の読取
# - 必須項目チェック
# - ZIP単位 all-or-nothing 制御
# - DB への記帳（hia_import_zips / hia_person_years / hia_xml_events）
# - 成功ZIPの archive 移動
# - error.txt 出力
#
# delivery 用の XML 抽出・対象月の絞り込み・同一人物の過去 XML 整理は
# 別スクリプト hia_build_delivery_zip.py の責務とする。
# ------------------------------------------------------------


# ============================================================
# run_id
# ============================================================

def build_run_id():
    """YYYYMMDDHHMMSS"""
    return datetime.now().strftime("%Y%m%d%H%M%S")


def resolve_exam_year(exam_date_str: str) -> int:
    """
    exam_date (YYYY-MM-DD) から業務年度を返す。
    年度開始日は EXAM_YEAR_START_MONTH / EXAM_YEAR_START_DAY を使用する。

    例:
        2026-03-31 -> 2025
        2026-04-01 -> 2026
    """
    d = date.fromisoformat(exam_date_str)
    boundary = date(d.year, EXAM_YEAR_START_MONTH, EXAM_YEAR_START_DAY)
    return d.year if d >= boundary else d.year - 1



# ============================================================
# person_id_custom helper
# ============================================================

def build_person_id_custom(row: dict[str, object]) -> str | None:
    """
    custom_id_gen.py を用いて person_id_custom を生成する。

    入力順は custom_id_gen.py の DEFAULT_COMPOSE_ORDER に従う:
      [birth_yyyymmdd][insurance_number][insurer_number][symbol]

    ここで使う値は DB照合用 match 値ではなく、custom_id 用 normalize 後の値。
    """
    birth_val = row.get("birth_yyyymmdd")
    insurance_number_val = row.get("insurance_number_for_custom_id")
    insurer_number_val = row.get("insurer_number")
    symbol_val = row.get("symbol_for_custom_id")

    if not isinstance(birth_val, str) or not birth_val:
        return None
    if not isinstance(insurance_number_val, str) or not insurance_number_val:
        return None
    if not isinstance(insurer_number_val, str) or not insurer_number_val:
        return None
    if not isinstance(symbol_val, str) or not symbol_val:
        return None

    birth_yyyymmdd: str = birth_val
    insurance_number_for_custom_id: str = insurance_number_val
    insurer_number: str = insurer_number_val
    symbol_for_custom_id: str = symbol_val

    person_id_custom, _meta = generate_id(
        insurer_number=insurer_number,
        symbol=symbol_for_custom_id,
        insurance_number=insurance_number_for_custom_id,
        birth_yyyymmdd=birth_yyyymmdd,
        mat_dir=MAT_DIR,
    )
    return person_id_custom


# ============================================================
# ディレクトリ準備
# ============================================================

def ensure_run_dirs(run_id: str, insurer_number: str):
    """import_zip 用の最小作業ディレクトリだけを準備する。"""
    run_work = WORK_DIR / run_id / insurer_number
    run_work.mkdir(parents=True, exist_ok=True)

    return {
        "run_work": run_work,
    }


# ============================================================
# ZIP探索
# ============================================================

def find_insurer_dirs():

    return [
        d for d in INPUT_ZIP_DIR.iterdir()
        if d.is_dir()
    ]


def find_zip_files(insurer_dir: Path):

    return sorted(insurer_dir.glob("*.zip"))


def parse_zip_context(zip_path: Path):
    """
    ZIP 名から最低限の文脈を取得する。

    期待形式:
        {facility_code}_{insurer_number}_{yyyymmddX}_{send_seq}.zip

    現時点では dl_date は3ブロック目の先頭8桁を採用する。
    例:
        0110118098_06139463_202511250_1.zip
          -> facility_code=0110118098
             insurer_number=06139463
             dl_date=2025-11-25
             send_seq=1
    """
    stem = zip_path.stem
    parts = stem.split("_")

    if len(parts) != 4:
        raise ValueError(f"Invalid ZIP filename format: {zip_path.name}")

    facility_code = parts[0]
    insurer_number = parts[1]
    dl_raw = parts[2]
    send_seq_raw = parts[3]

    if len(dl_raw) < 8 or not dl_raw[:8].isdigit():
        raise ValueError(f"Invalid dl_date block in ZIP filename: {zip_path.name}")

    if not send_seq_raw.isdigit():
        raise ValueError(f"Invalid send_seq in ZIP filename: {zip_path.name}")

    dl_date = f"{dl_raw[:4]}-{dl_raw[4:6]}-{dl_raw[6:8]}"
    send_seq = int(send_seq_raw)

    return {
        "facility_code": facility_code,
        "insurer_number": insurer_number,
        "dl_date": dl_date,
        "send_seq": send_seq,
        "folder_name": zip_path.parent.name,
        "zip_name": zip_path.name,
    }


def build_extract_dir(run_work: Path, zip_path: Path):
    """
    ZIP ごとの一時展開先を返す。
    shared extract dir を使うと前回展開物が混ざるため、ZIP ごとに分離する。
    """
    extract_dir = run_work / "zip_extract" / zip_path.stem
    if extract_dir.exists():
        shutil.rmtree(extract_dir)
    extract_dir.mkdir(parents=True, exist_ok=True)
    return extract_dir


# ============================================================
# ZIP展開
# ============================================================

def extract_zip(zip_path: Path, extract_dir: Path):

    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(extract_dir)


# ============================================================
# XML収集
# ============================================================

def collect_xml_files(extract_dir: Path):
    """
    HIA対象XMLは DATA 配下の実データXMLのみを収集する。
    ix08_V08.xml / su08_V08.xml などの補助XMLは対象外。
    二重フォルダ構成でも拾えるよう、DATA をパスに含む h*.xml に絞る。
    """
    return sorted(
        p for p in extract_dir.rglob("h*.xml")
        if "DATA" in p.parts
    )


def calc_file_sha256(path: Path) -> str:
    """ファイルの SHA256 を返す。"""
    import hashlib

    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


# ============================================================
# ZIP Import Deduplication Helpers
# ============================================================

def find_existing_zip_import(cur, zip_name: str) -> dict | None:
    """
    zip_name で既存 import を取得
    """
    sql = """
    SELECT
        z.zip_id,
        z.import_status,
        COALESCE(COUNT(e.zip_error_id),0) AS error_count
    FROM hia_import_zips z
    LEFT JOIN hia_import_zip_errors e
      ON e.zip_id = z.zip_id
    WHERE z.zip_name = %s
    GROUP BY z.zip_id, z.import_status
    ORDER BY z.zip_id DESC
    LIMIT 1
    """
    cur.execute(sql, (zip_name,))
    return cur.fetchone()


def is_successfully_imported(row: dict | None) -> bool:
    if not row:
        return False

    return (
        row.get("import_status") == "IMPORTED"
        and int(row.get("error_count") or 0) == 0
    )


def delete_existing_zip_run(cur, zip_id: int):
    """
    エラーZIP再処理用
    """
    cur.execute("DELETE FROM hia_xml_events WHERE zip_id=%s", (zip_id,))
    cur.execute("DELETE FROM hia_import_zip_errors WHERE zip_id=%s", (zip_id,))
    cur.execute("DELETE FROM hia_import_zips WHERE zip_id=%s", (zip_id,))


# ============================================================
# DB helpers
# ============================================================


def insert_import_zip(cur, zip_ctx: dict, zip_sha256: str) -> int:
    """
    hia_import_zips に PROCESSING で仮登録する。
    """
    sql = """
    INSERT INTO hia_import_zips (
        insurer_number,
        folder_name,
        zip_name,
        dl_date,
        send_seq,
        zip_sha256,
        xml_count_total,
        xml_count_success,
        xml_count_error,
        import_status
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """
    cur.execute(
        sql,
        (
            zip_ctx["insurer_number"],
            zip_ctx["folder_name"],
            zip_ctx["zip_name"],
            zip_ctx["dl_date"],
            zip_ctx["send_seq"],
            zip_sha256,
            0,
            0,
            0,
            "PROCESSING",
        ),
    )
    return int(cur.lastrowid)


def update_import_zip_status(
    cur,
    zip_id: int,
    import_status: str,
    xml_count_total: int,
    xml_count_success: int,
    xml_count_error: int,
):
    """
    hia_import_zips の処理結果を更新する。
    """
    sql = """
    UPDATE hia_import_zips
       SET import_status = %s,
           xml_count_total = %s,
           xml_count_success = %s,
           xml_count_error = %s,
           updated_at = CURRENT_TIMESTAMP
     WHERE zip_id = %s
    """
    cur.execute(
        sql,
        (
            import_status,
            xml_count_total,
            xml_count_success,
            xml_count_error,
            zip_id,
        ),
    )



def insert_zip_error(
    cur,
    zip_id: int,
    xml_filename: str | None,
    error_code: str,
    error_message: str,
    error_detail: str | None = None,
):
    """
    hia_import_zip_errors に 1件記帳する。
    """
    sql = """
    INSERT INTO hia_import_zip_errors (
        zip_id,
        xml_filename,
        error_code,
        error_message,
        error_detail
    ) VALUES (
        %s, %s, %s, %s, %s
    )
    """
    cur.execute(
        sql,
        (
            zip_id,
            xml_filename,
            error_code,
            error_message,
            error_detail,
        ),
    )


def upsert_person_year(cur, row: dict, zip_ctx: dict) -> int:
    """
    hia_person_years を upsert し、person_year_id を返す。

    UNIQUE KEY:
      (person_id_custom, name_kana_norm, gender_code, exam_year)
    を利用し、既存時は LAST_INSERT_ID(person_year_id) を使って id を返す。
    """
    sql = """
    INSERT INTO hia_person_years (
        person_id_custom,
        name_kana_norm,
        gender_code,
        exam_year,
        insurer_number,
        insurance_symbol,
        insurance_number,
        insurance_symbol_match,
        insurance_number_match,
        report_category,
        health_program_code,
        birthdate,
        name_kana_raw,
        identity_hash,
        dl_count,
        first_seen_dl_date,
        first_seen_zip_name,
        first_seen_xml_filename,
        last_seen_dl_date,
        last_seen_zip_name,
        last_seen_xml_filename
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON DUPLICATE KEY UPDATE
        person_year_id = LAST_INSERT_ID(person_year_id),
        last_seen_dl_date = VALUES(last_seen_dl_date),
        last_seen_zip_name = VALUES(last_seen_zip_name),
        last_seen_xml_filename = VALUES(last_seen_xml_filename),
        identity_hash = VALUES(identity_hash),
        dl_count = dl_count + 1,
        updated_at = CURRENT_TIMESTAMP
    """
    cur.execute(
        sql,
        (
            row["person_id_custom"],
            row["name_kana_norm"],
            row["gender_code"],
            row["exam_year"],
            row["insurer_number"],
            row["insurance_symbol"],
            row["insurance_number"],
            row["insurance_symbol_match"],
            row["insurance_number_match"],
            row.get("report_category"),
            row.get("health_program_code"),
            row["birthdate"],
            row["name_kana"],
            row.get("identity_hash"),
            1,
            zip_ctx["dl_date"],
            zip_ctx["zip_name"],
            Path(row["xml_path"]).name,
            zip_ctx["dl_date"],
            zip_ctx["zip_name"],
            Path(row["xml_path"]).name,
        ),
    )
    return int(cur.lastrowid)


def insert_xml_event(cur, person_year_id: int, zip_id: int, row: dict, zip_ctx: dict):
    """
    hia_xml_events に 1件 insert する。
    """
    sql = """
    INSERT INTO hia_xml_events (
        person_year_id,
        zip_id,
        xml_filename,
        xml_sha256,
        exam_date,
        facility_code,
        facility_name,
        dl_date
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s
    )
    """
    cur.execute(
        sql,
        (
            person_year_id,
            zip_id,
            Path(row["xml_path"]).name,
            calc_file_sha256(Path(row["xml_path"])),
            row["exam_date"],
            row.get("facility_code"),
            row.get("facility_name"),
            zip_ctx["dl_date"],
        ),
    )


# ============================================================
# 必須チェック
# ============================================================

def validate_required_fields(row):

    errors = []

    # --------------------------------------------------
    # 人照合に必要な必須項目
    # --------------------------------------------------
    if not row.get("insurer_number"):
        errors.append("INSURER_NUMBER_MISSING")

    if not row.get("insurance_symbol"):
        errors.append("INSURANCE_SYMBOL_MISSING")

    if not row.get("insurance_number"):
        errors.append("INSURANCE_NUMBER_MISSING")

    if not row.get("birthdate"):
        errors.append("BIRTHDATE_MISSING")

    if not row.get("name_kana"):
        errors.append("NAME_KANA_MISSING")

    if not row.get("gender_code"):
        errors.append("GENDER_CODE_EMPTY")

    # --------------------------------------------------
    # 年度判定に必要な必須項目
    # --------------------------------------------------
    if not row.get("exam_date"):
        errors.append("EXAM_DATE_MISSING")

    return errors


def append_zip_error(error_lines: list[str], header: str, detail_lines: list[str] | None = None):
    """error.txt 出力用のヘルパー。"""
    error_lines.append(header)
    if detail_lines:
        error_lines.extend(detail_lines)


# ============================================================
# ZIPアーカイブ
# ============================================================

def move_zip_to_archive(zip_path: Path, run_id: str, insurer_number: str) -> Path:
    """成功した ZIP を archive へ移動し、移動先パスを返す。"""

    # archive structure: archive_zip/{run_id}/{insurer_number}/
    target_dir = ARCHIVE_ZIP_DIR / run_id / insurer_number
    target_dir.mkdir(parents=True, exist_ok=True)

    target_path = target_dir / zip_path.name
    zip_path.rename(target_path)
    return target_path


def update_import_zip_archive_info(cur, zip_id: int, archived_zip_path: str):
    """hia_import_zips に archive 後の物理パスを記録する。"""
    sql = """
    UPDATE hia_import_zips
       SET archived_zip_path = %s,
           archived_at = CURRENT_TIMESTAMP,
           updated_at = CURRENT_TIMESTAMP
     WHERE zip_id = %s
    """
    cur.execute(sql, (archived_zip_path, zip_id))


# ============================================================
# main
# ============================================================

def main():

    run_id = build_run_id()

    error_lines = []
    mysql_params = load_mysql_params()

    insurer_dirs = find_insurer_dirs()

    for insurer_dir in insurer_dirs:

        insurer_number = insurer_dir.name

        zip_files = find_zip_files(insurer_dir)

        for zip_path in zip_files:

            dirs = ensure_run_dirs(run_id, insurer_number)
            zip_ctx = parse_zip_context(zip_path)

            if zip_ctx["insurer_number"] != insurer_number:
                error_lines.append(
                    f"ZIP ERROR: {zip_path.name} insurer mismatch "
                    f"(dir={insurer_number}, zip={zip_ctx['insurer_number']})"
                )
                continue

            extract_dir = build_extract_dir(dirs["run_work"], zip_path)
            zip_sha256 = calc_file_sha256(zip_path)

            print(f"Processing ZIP: {zip_path}")

            extract_zip(zip_path, extract_dir)

            xml_files = collect_xml_files(extract_dir)

            xml_count_total = len(xml_files)
            xml_count_success = 0
            xml_count_error = 0

            zip_errors = []
            xml_rows = []

            for xml_path in xml_files:

                row = parse_hia_xml_identity(xml_path)

                # --------------------------------------------------
                # normalize for DB match
                # --------------------------------------------------

                row["insurance_symbol_match"] = normalize_insurance_symbol_match(
                    row.get("insurance_symbol")
                )

                row["insurance_number_match"] = normalize_insurance_number_match(
                    row.get("insurance_number")
                )

                # --------------------------------------------------
                # normalize for person_id_custom
                # --------------------------------------------------

                row["symbol_for_custom_id"] = normalize_symbol_for_custom_id(
                    row.get("insurance_symbol")
                )

                row["insurance_number_for_custom_id"] = normalize_insurance_number_match(
                    row.get("insurance_number")
                )

                row["birth_yyyymmdd"] = normalize_birth_yyyymmdd(
                    row.get("birthdate")
                )

                row["name_kana_norm"] = normalize_name_kana_match(
                    row.get("name_kana")
                )

                # --------------------------------------------------
                # exam year
                # --------------------------------------------------

                if row.get("exam_date"):
                    row["exam_year"] = resolve_exam_year(row["exam_date"])
                else:
                    row["exam_year"] = None

                # --------------------------------------------------
                # person_id_custom
                # --------------------------------------------------

                row["person_id_custom"] = build_person_id_custom(row)
                row["identity_hash"] = build_identity_hash(
                    person_id_custom=row.get("person_id_custom"),
                    name_kana_full_match=row.get("name_kana_norm"),
                    gender_code=row.get("gender_code"),
                )

                errors = validate_required_fields(row)

                if not row.get("person_id_custom"):
                    errors.append("PERSON_ID_CUSTOM_BUILD_FAILED")

                if errors:
                    zip_errors.append((xml_path, errors))
                    xml_count_error += 1
                else:
                    xml_rows.append(row)
                    xml_count_success += 1

            # --------------------------------------------------
            # ZIP単位 all-or-nothing
            # --------------------------------------------------

            with connect_ctx(mysql_params, autocommit=False) as conn:
                cur = dict_cursor(conn)

                existing = find_existing_zip_import(cur, zip_ctx["zip_name"])

                if is_successfully_imported(existing):
                    print(f"Skip already imported ZIP: {zip_ctx['zip_name']}")
                    conn.rollback()
                    continue

                if existing:
                    print(f"Reprocessing errored ZIP: {zip_ctx['zip_name']}")
                    delete_existing_zip_run(cur, int(existing["zip_id"]))

                zip_id = insert_import_zip(cur, zip_ctx, zip_sha256)

                if zip_errors:

                    detail_lines = []
                    for xml_path, errors in zip_errors:
                        detail_lines.append(
                            f"  XML ERROR: {Path(xml_path).name} -> {', '.join(errors)}"
                        )
                        for error_code in errors:
                            insert_zip_error(
                                cur,
                                zip_id=zip_id,
                                xml_filename=Path(xml_path).name,
                                error_code=error_code,
                                error_message=error_code,
                                error_detail=str(xml_path),
                            )

                    update_import_zip_status(
                        cur,
                        zip_id=zip_id,
                        import_status="ERROR",
                        xml_count_total=xml_count_total,
                        xml_count_success=xml_count_success,
                        xml_count_error=xml_count_error,
                    )
                    conn.commit()

                    append_zip_error(
                        error_lines,
                        (
                            f"ZIP ERROR: {zip_ctx['zip_name']} "
                            f"(insurer={zip_ctx['insurer_number']}, dl_date={zip_ctx['dl_date']}, "
                            f"send_seq={zip_ctx['send_seq']}, xml_errors={len(zip_errors)})"
                        ),
                        detail_lines,
                    )

                    continue

                for row in xml_rows:
                    person_year_id = upsert_person_year(cur, row, zip_ctx)
                    insert_xml_event(cur, person_year_id, zip_id, row, zip_ctx)

                update_import_zip_status(
                    cur,
                    zip_id=zip_id,
                    import_status="IMPORTED",
                    xml_count_total=xml_count_total,
                    xml_count_success=xml_count_success,
                    xml_count_error=xml_count_error,
                )
                conn.commit()

            archived_zip_path = move_zip_to_archive(zip_path, run_id, insurer_number)

            with connect_ctx(mysql_params, autocommit=False) as conn:
                cur = dict_cursor(conn)
                update_import_zip_archive_info(
                    cur,
                    zip_id=zip_id,
                    archived_zip_path=str(archived_zip_path),
                )
                conn.commit()

    # --------------------------------------------------
    # error.txt
    # --------------------------------------------------

    run_output_dir = OUTPUT_DIR / run_id
    run_output_dir.mkdir(parents=True, exist_ok=True)

    error_file = run_output_dir / "error.txt"

    if error_lines:

        with open(error_file, "w", encoding="utf-8") as f:
            for line in error_lines:
                f.write(line + "\n")


if __name__ == "__main__":
    main()