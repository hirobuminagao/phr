#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
hia_build_delivery_zip.py

HIA → fund delivery rebuild pipeline

責務:
- fund_delivery/input に置かれた元ZIPを読み込む
- DB台帳から納品対象XMLを抽出する
- 過去登場済み person_year を除外する
- hia_delivery_exclusion_rules を適用する
- ix08 / su08 などの集計XMLを除外・再構成する
- XSD をコピーする
- 元ZIPと同名の納品ZIPを fund_delivery/output に再構成する

注意:
- HIA取込台帳(hia_import_zips / hia_xml_events / hia_person_years)を前提にする
- 再構成対象の実データXMLは DATA/h*.xml のみ
- fund_delivery/work ディレクトリはスクリプト側で自動作成する
"""

import re
import shutil
import sys
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

# ------------------------------------------------------------
# VSCode Run ボタン (file実行) 対応
# ------------------------------------------------------------
if __name__ == "__main__" and __package__ is None:
    project_root = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(project_root))

from scripts.work_folder.lib.db.config import load_mysql_params
from scripts.work_folder.lib.db.mysql import connect_ctx, dict_cursor


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = PROJECT_ROOT / "data"

FUND_DELIVERY_DIR = DATA_DIR / "fund_delivery"
FUND_INPUT_DIR = FUND_DELIVERY_DIR / "input"
FUND_WORK_DIR = FUND_DELIVERY_DIR / "work"
FUND_OUTPUT_DIR = FUND_DELIVERY_DIR / "output"

NS = {"hl7": "urn:hl7-org:v3"}
XML_FILENAME_PATTERN = re.compile(r"^h[^\\/]*\.xml$", re.IGNORECASE)


# ============================================================
# run_id / path helpers
# ============================================================


def build_run_id() -> str:
    return datetime.now().strftime("%Y%m%d%H%M%S")



def ensure_delivery_dirs(run_id: str, insurer_number: str, zip_stem: str) -> dict[str, Path]:
    """
    fund_delivery/work, output 配下の実行用ディレクトリを自動作成する。
    """
    run_work = FUND_WORK_DIR / run_id / insurer_number / zip_stem
    extract_dir = run_work / "extract"
    selected_root = run_work / "selected"
    selected_data_dir = selected_root / "DATA"
    output_dir = FUND_OUTPUT_DIR / insurer_number

    extract_dir.mkdir(parents=True, exist_ok=True)
    selected_root.mkdir(parents=True, exist_ok=True)
    selected_data_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    return {
        "run_work": run_work,
        "extract_dir": extract_dir,
        "selected_root": selected_root,
        "selected_data_dir": selected_data_dir,
        "output_dir": output_dir,
    }


# ============================================================
# ZIP探索 / 文脈
# ============================================================


def find_input_insurer_dirs() -> list[Path]:
    if not FUND_INPUT_DIR.exists():
        return []
    return sorted([d for d in FUND_INPUT_DIR.iterdir() if d.is_dir()])



def find_input_zip_files(insurer_dir: Path) -> list[Path]:
    return sorted(insurer_dir.glob("*.zip"))



def parse_zip_context(zip_path: Path) -> dict[str, object]:
    """
    ZIP 名から最低限の文脈を取得する。

    期待形式:
        {facility_code}_{insurer_number}_{yyyymmddX}_{send_seq}.zip
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
        "zip_name": zip_path.name,
        "zip_stem": zip_path.stem,
    }


# ============================================================
# ZIP展開 / XML収集
# ============================================================


def reset_dir(path: Path):
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)



def extract_zip(zip_path: Path, extract_dir: Path):
    reset_dir(extract_dir)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)



def collect_data_xml_files(extract_dir: Path) -> dict[str, Path]:
    """
    元ZIP内の DATA/h*.xml を filename -> Path で返す。
    二重フォルダ構成でも拾えるよう DATA をパスに含む h*.xml のみ対象にする。
    """
    xml_map: dict[str, Path] = {}
    for p in sorted(extract_dir.rglob("h*.xml")):
        if "DATA" not in p.parts:
            continue
        xml_map[p.name] = p
    return xml_map



def find_first_named_dir(extract_dir: Path, dir_name: str) -> Path | None:
    for p in sorted(extract_dir.rglob(dir_name)):
        if p.is_dir():
            return p
    return None



def find_first_named_file(extract_dir: Path, file_prefix: str) -> Path | None:
    pattern = f"{file_prefix}*.xml"
    candidates = sorted(extract_dir.rglob(pattern))
    return candidates[0] if candidates else None



def collect_support_files(extract_dir: Path) -> dict[str, Path | None]:
    return {
        "xsd_dir": find_first_named_dir(extract_dir, "XSD"),
        "ix08_path": find_first_named_file(extract_dir, "ix08"),
        "su08_path": find_first_named_file(extract_dir, "su08"),
    }


# ============================================================
# DB query
# ============================================================


def get_delivery_target_rows(cur, insurer_number: str, dl_date: str, zip_name: str) -> list[dict]:
    """
    指定ZIPの納品対象 XML を返す。

    処理順:
    1. 指定ZIPを母集団にする
    2. 過去登場済み person_year を除外する
    3. hia_delivery_exclusion_rules (facility_code) を適用する
    4. xml_sha256 重複を除外する

    NOTE:
    - ix08 / su08 は h*.xml 収集段階で対象外のため SQL では扱わない
    - 同一人判定は hia_person_years の identity 軸
      (person_id_custom, name_kana_norm, gender_code, exam_year)
    """
    sql = """
    WITH current_dl AS (
        SELECT
            xe.xml_event_id,
            xe.zip_id,
            xe.person_year_id,
            xe.xml_filename,
            xe.xml_sha256,
            xe.exam_date,
            xe.facility_code,
            xe.facility_name,
            py.person_id_custom,
            py.name_kana_norm,
            py.gender_code,
            py.exam_year,
            z.insurer_number,
            z.dl_date,
            z.zip_name
        FROM hia_xml_events xe
        JOIN hia_person_years py
          ON py.person_year_id = xe.person_year_id
        JOIN hia_import_zips z
          ON z.zip_id = xe.zip_id
        WHERE z.insurer_number = %s
          AND z.dl_date = %s
          AND z.zip_name = %s
    ),
    prior_same_person_year AS (
        SELECT DISTINCT
            py.person_id_custom,
            py.name_kana_norm,
            py.gender_code,
            py.exam_year
        FROM hia_xml_events xe
        JOIN hia_person_years py
          ON py.person_year_id = xe.person_year_id
        JOIN hia_import_zips z
          ON z.zip_id = xe.zip_id
        WHERE z.insurer_number = %s
          AND z.dl_date < %s
    ),
    after_prior_exclusion AS (
        SELECT
            c.*
        FROM current_dl c
        LEFT JOIN prior_same_person_year p
          ON p.person_id_custom = c.person_id_custom
         AND p.name_kana_norm   = c.name_kana_norm
         AND p.gender_code      = c.gender_code
         AND p.exam_year        = c.exam_year
        WHERE p.person_id_custom IS NULL
    ),
    after_rule_exclusion AS (
        SELECT
            a.*
        FROM after_prior_exclusion a
        LEFT JOIN hia_delivery_exclusion_rules r
          ON r.insurer_number = a.insurer_number
         AND r.target_schema  = 'work_other'
         AND r.target_table   = 'hia_xml_events'
         AND r.target_column  = 'facility_code'
         AND r.match_type     = 'EQUAL'
         AND r.match_value    = a.facility_code
         AND r.is_enabled     = 1
        WHERE r.exclusion_rule_id IS NULL
    ),
    deduped AS (
        SELECT *
        FROM (
            SELECT
                a.*,
                ROW_NUMBER() OVER (
                    PARTITION BY a.xml_sha256
                    ORDER BY a.xml_event_id
                ) AS rn
            FROM after_rule_exclusion a
        ) x
        WHERE x.rn = 1
    )
    SELECT
        xml_event_id,
        zip_id,
        person_year_id,
        xml_filename,
        xml_sha256,
        exam_date,
        facility_code,
        facility_name,
        person_id_custom,
        name_kana_norm,
        gender_code,
        exam_year,
        insurer_number,
        dl_date,
        zip_name
    FROM deduped
    ORDER BY
        name_kana_norm,
        exam_date,
        xml_filename
    """
    cur.execute(sql, (insurer_number, dl_date, zip_name, insurer_number, dl_date))
    rows = cur.fetchall()
    return rows or []


# ============================================================
# XML / XSD 再構成
# ============================================================


def copy_selected_xmls(target_rows: list[dict], xml_map: dict[str, Path], selected_data_dir: Path) -> list[Path]:
    copied_paths: list[Path] = []

    for row in target_rows:
        xml_filename = row["xml_filename"]
        src = xml_map.get(xml_filename)
        if src is None:
            raise FileNotFoundError(f"Source XML not found in ZIP: {xml_filename}")

        dst = selected_data_dir / xml_filename
        shutil.copy2(src, dst)
        copied_paths.append(dst)

    return copied_paths



def copy_xsd_dir(xsd_dir: Path | None, selected_root: Path):
    if xsd_dir is None:
        return

    dst = selected_root / "XSD"
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(xsd_dir, dst)



def _collect_xml_filename_refs(elem: ET.Element) -> set[str]:
    refs: set[str] = set()

    if elem.text:
        text = elem.text.strip()
        if XML_FILENAME_PATTERN.match(text):
            refs.add(text)

    for value in elem.attrib.values():
        text = str(value).strip()
        if XML_FILENAME_PATTERN.match(text):
            refs.add(text)

    for child in list(elem):
        refs.update(_collect_xml_filename_refs(child))

    return refs



def _prune_non_selected_filename_subtrees(elem: ET.Element, selected_filenames: set[str]):
    for child in list(elem):
        refs = _collect_xml_filename_refs(child)

        if refs and refs.isdisjoint(selected_filenames):
            elem.remove(child)
            continue

        _prune_non_selected_filename_subtrees(child, selected_filenames)



def _update_header_dates(root: ET.Element, dl_date: str):
    yyyymmdd = dl_date.replace("-", "")

    doc_effective = root.find("hl7:effectiveTime", NS)
    if doc_effective is not None:
        doc_effective.set("value", yyyymmdd)

    author_time = root.find(".//hl7:author/hl7:time", NS)
    if author_time is not None:
        author_time.set("value", yyyymmdd)



def rewrite_summary_xml(source_path: Path | None, output_path: Path, selected_filenames: set[str], dl_date: str):
    """
    ix08 / su08 をコピーし、選択された h*.xml に関係する部分だけ残す。

    v1 方針:
    - 元XMLをベースにする
    - h*.xml ファイル名を参照している部分木を選別する
    - ヘッダ日付は再生成対象DL日に合わせる

    NOTE:
    - 施設実データに合わせた集計仕様の厳密再構成が必要になった場合は、
      今後この関数を正式仕様ベースで拡張する
    """
    if source_path is None:
        return

    tree = ET.parse(source_path)
    root = tree.getroot()

    _prune_non_selected_filename_subtrees(root, selected_filenames)
    _update_header_dates(root, dl_date)

    tree.write(output_path, encoding="utf-8", xml_declaration=True)



def build_delivery_zip(output_zip_path: Path, selected_root: Path):
    if output_zip_path.exists():
        output_zip_path.unlink()

    with zipfile.ZipFile(output_zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(selected_root.rglob("*")):
            if not path.is_file():
                continue
            arcname = path.relative_to(selected_root).as_posix()
            zf.write(path, arcname)


# ============================================================
# main
# ============================================================


def main():
    run_id = build_run_id()
    mysql_params = load_mysql_params()

    insurer_dirs = find_input_insurer_dirs()
    if not insurer_dirs:
        print(f"No insurer dirs found: {FUND_INPUT_DIR}")
        return

    for insurer_dir in insurer_dirs:
        insurer_number = insurer_dir.name
        zip_files = find_input_zip_files(insurer_dir)

        for zip_path in zip_files:
            zip_ctx = parse_zip_context(zip_path)

            if zip_ctx["insurer_number"] != insurer_number:
                print(
                    f"Skip ZIP insurer mismatch: {zip_path.name} "
                    f"(dir={insurer_number}, zip={zip_ctx['insurer_number']})"
                )
                continue

            dirs = ensure_delivery_dirs(run_id, insurer_number, str(zip_ctx["zip_stem"]))
            print(f"Building delivery ZIP from: {zip_path}")

            extract_zip(zip_path, dirs["extract_dir"])
            xml_map = collect_data_xml_files(dirs["extract_dir"])
            support_files = collect_support_files(dirs["extract_dir"])

            with connect_ctx(mysql_params, autocommit=False) as conn:
                cur = dict_cursor(conn)
                target_rows = get_delivery_target_rows(
                    cur,
                    insurer_number=insurer_number,
                    dl_date=str(zip_ctx["dl_date"]),
                    zip_name=str(zip_ctx["zip_name"]),
                )
                conn.rollback()

            if not target_rows:
                print(f"No delivery targets after exclusion: {zip_path.name}")
                continue

            copied_paths = copy_selected_xmls(target_rows, xml_map, dirs["selected_data_dir"])
            if not copied_paths:
                print(f"No XML copied: {zip_path.name}")
                continue

            selected_filenames = {path.name for path in copied_paths}

            copy_xsd_dir(support_files["xsd_dir"], dirs["selected_root"])
            rewrite_summary_xml(
                support_files["ix08_path"],
                dirs["selected_root"] / "ix08_V08.xml",
                selected_filenames,
                str(zip_ctx["dl_date"]),
            )
            rewrite_summary_xml(
                support_files["su08_path"],
                dirs["selected_root"] / "su08_V08.xml",
                selected_filenames,
                str(zip_ctx["dl_date"]),
            )

            output_zip_path = dirs["output_dir"] / zip_path.name
            build_delivery_zip(output_zip_path, dirs["selected_root"])

            print(
                f"Built delivery ZIP: {output_zip_path} "
                f"(selected_xml_count={len(copied_paths)})"
            )


if __name__ == "__main__":
    main()
