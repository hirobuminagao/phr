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

from hia_parse_xml import parse_hia_xml_identity


# ============================================================
# 初期設定
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[2]
HIA_EXPORT_DIR = BASE_DIR / "scripts" / "work_folder" / "hia_export"

INPUT_ZIP_DIR = HIA_EXPORT_DIR / "input_zip"
ARCHIVE_ZIP_DIR = HIA_EXPORT_DIR / "archive_zip"
WORK_DIR = HIA_EXPORT_DIR / "work"
OUTPUT_DIR = HIA_EXPORT_DIR / "output_to_fund"

EXAM_YEAR_START_MONTH = 4
EXAM_YEAR_START_DAY = 1

# ------------------------------------------------------------
# normalize / person_id_custom の実装は後続で接続する。
# 現時点では import_zip の責務を以下に限定する:
# - ZIP単位の探索
# - ZIP名からの文脈取得
# - ZIP展開
# - XML identity の読取
# - 必須項目チェック
# - ZIP単位 all-or-nothing 制御
# - 成功ZIPの archive 移動
# - error.txt 出力
#
# 次フェーズで追加予定:
# - insurance_symbol_match / insurance_number_match 生成
# - person_id_custom 生成
# - exam_year 算出
# - hia_import_zips / hia_person_years / hia_xml_events へのDB記帳
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
# ディレクトリ準備
# ============================================================

def ensure_run_dirs(run_id: str, insurer_number: str):

    run_work = WORK_DIR / run_id / insurer_number

    xml_selected = run_work / "xml_selected"
    run_output = OUTPUT_DIR / run_id / insurer_number

    xml_selected.mkdir(parents=True, exist_ok=True)
    run_output.mkdir(parents=True, exist_ok=True)

    return {
        "run_work": run_work,
        "xml_selected": xml_selected,
        "run_output": run_output,
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

    return list(extract_dir.rglob("*.xml"))


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

def move_zip_to_archive(zip_path: Path, run_id: str, insurer_number: str):

    # archive structure: archive_zip/{run_id}/{insurer_number}/
    target_dir = ARCHIVE_ZIP_DIR / run_id / insurer_number
    target_dir.mkdir(parents=True, exist_ok=True)

    target_path = target_dir / zip_path.name

    zip_path.rename(target_path)


# ============================================================
# main
# ============================================================

def main():

    run_id = build_run_id()

    error_lines = []

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

            print(f"Processing ZIP: {zip_path}")

            extract_zip(zip_path, extract_dir)

            xml_files = collect_xml_files(extract_dir)

            zip_errors = []

            xml_rows = []

            for xml_path in xml_files:

                row = parse_hia_xml_identity(xml_path)

                # TODO(next):
                # - insurance_symbol_match 生成
                # - insurance_number_match 生成
                # - person_id_custom 生成
                # - exam_year 算出 (resolve_exam_year)

                errors = validate_required_fields(row)

                if errors:
                    zip_errors.append((xml_path, errors))
                else:
                    xml_rows.append(row)

            # --------------------------------------------------
            # ZIP単位 all-or-nothing
            # --------------------------------------------------

            if zip_errors:

                detail_lines = []
                for xml_path, errors in zip_errors:
                    detail_lines.append(
                        f"  XML ERROR: {Path(xml_path).name} -> {', '.join(errors)}"
                    )

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

            # --------------------------------------------------
            # TODO: DB記帳
            # --------------------------------------------------

            # TODO(next):
            # 1. insurance_symbol_match / insurance_number_match を生成
            # 2. person_id_custom を生成
            # 3. exam_year を算出
            # 4. hia_import_zips を記帳
            # 5. hia_person_years を upsert
            # 6. hia_xml_events を insert
            # 7. エラー時は hia_import_zip_errors を記帳

            move_zip_to_archive(zip_path, run_id, insurer_number)

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