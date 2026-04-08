

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
check_shg_result_xml.py

SHG結果XMLチェック（fase1.0: 旧スクリプトからの横移行）

目的:
- 旧 `scripts/tokuho_xml_check/check_tokuho_xml.py` の構造をベースに、
  新実行環境 `scripts/shg/` へ移行する。
- fase1.0 では入力方式は旧前提（展開済みXML）とし、
  まずは新キー体系・新DB定義・新CSV出力方針へ載せ替える。
  - 入出力の固定パスは `data/hia_export_shg/` を使用する。
- fase1.1 で ZIP 直読みに改修する。

fase1.0 方針:
- `export_shg_report`
  - 既存列は維持
  - `identity_hash` を追加
- `export_outcome_report`
  - `person_id` 列は `identity_hash` に変更
  - 他の既存列は維持
- `person_id_custom` / `identity_hash` は新 generator で生成
- `shg_result` は新定義を参照
  - `shg_year`
  - `exam_waist_cm`
  - `exam_weight_kg`
  - `identity_hash`
- 内部束ねキーは `identity_hash` を優先
- `person_key` は CSV の目視確認用として保持する

注意:
- このファイルは fase1.0 用の骨格であり、旧スクリプトの全ロジックはまだ移植していない。
- 旧スクリプトの XML 抽出関数群を順次移植する前提。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
import argparse
import csv
import sys
import xml.etree.ElementTree as ET

# ------------------------------------------------------------
# VSCode Run ボタン / file実行 対応
# ------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import WORK_OTHER
from scripts.lib.identity.generator import generate_identity_bundle
from scripts.lib.shg.xml.basic import extract_basic
from scripts.lib.shg.xml.section_90030_initial import extract_initial_goals
from scripts.lib.shg.xml.section_90060_final import extract_final_outcomes, extract_final_measurements
from scripts.lib.shg.xml.role import resolve_shg_role
from scripts.lib.shg.xml.section_90070_support_summary import extract_support_summary
from scripts.lib.shg.xml.section_90040_support_detail import extract_process_events

# ------------------------------------------------------------
# XML namespace / OID constants (fase1.0)
# ------------------------------------------------------------
NS = {
    "cda": "urn:hl7-org:v3",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
}


# ------------------------------------------------------------
# phase1.0 migration memo
# ------------------------------------------------------------
# 旧スクリプトから移植対象（XML抽出系）:
# - read_xml
# - _text_or
# - _get_number
# - scan_xmls（fase1.0では旧前提の展開済みXML、fase1.1でZIP直読みに置換）
# - extract_basic
# - extract_initial_interview_mode
# - extract_initial_goals
# - extract_motivation_goals_from_final
# - extract_final_outcomes
# - extract_final_measurements
# - extract_process_aggregate_final
# - extract_process_events
# - compute_duration_verdict
#
# fase1.0 で旧仕様から差し替える箇所:
# - gen_custom_id_external → generate_identity_bundle
# - load_shg_result_from_mysql → 新 shg_result 定義対応
# - people[...] の内部束ねキー → identity_hash 優先
# - export_shg_report → identity_hash 追加
# - export_outcome_report → person_id を identity_hash へ変更
# - 健診時腹囲 / 健診時体重 → shg_result.exam_waist_cm / exam_weight_kg を反映


# ------------------------------------------------------------
# Args
# ------------------------------------------------------------
def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="SHG結果XMLチェック（fase1.0: 新実行環境への横移行）"
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        help="展開済みXML入力ディレクトリ（fase1.0）",
    )
    parser.add_argument(
        "--out-dir",
        required=True,
        help="CSV出力先ディレクトリ",
    )
    return parser


#
# ------------------------------------------------------------
# Helpers / XML
# ------------------------------------------------------------
def dbg(*args: Any) -> None:
    print(*args)




def read_xml(xml_path: Path) -> ET.Element:
    tree = ET.parse(xml_path)
    return tree.getroot()


def scan_xmls(input_dir: Path) -> list[Path]:
    """fase1.0: 展開済みXMLを走査する。

    想定:
    - input_dir 配下の XML
    - input_dir/DATA 配下の XML
    - XSD系は対象外
    """
    candidates: list[Path] = []

    if (input_dir / "DATA").exists():
        candidates.extend(sorted((input_dir / "DATA").rglob("*.xml")))
    else:
        candidates.extend(sorted(input_dir.rglob("*.xml")))

    result: list[Path] = []
    for p in candidates:
        name = p.name.lower()
        if name.startswith("ix08") or name.startswith("su08"):
            continue
        result.append(p)
    return result


def make_person_key(
    *,
    insurer: str,
    symbol: str,
    number: str,
    name: str,
    birth: str,
    gender: str,
) -> str:
    """CSV目視確認用の内部表示キー。突合主キーではない。"""
    parts = [
        (insurer or "").strip(),
        (symbol or "").strip(),
        (number or "").strip(),
        (name or "").strip(),
        (birth or "").strip(),
        (gender or "").strip(),
    ]
    return "|".join(parts)





# ------------------------------------------------------------
# DB
# ------------------------------------------------------------
def _normalize_db_row(row: Any) -> dict[str, Any]:
    """DB row を plain dict[str, Any] に正規化する。"""
    if isinstance(row, dict):
        return {str(k): v for k, v in row.items()}

    if hasattr(row, "items"):
        try:
            return {str(k): v for k, v in row.items()}
        except Exception:
            pass

    return {}

def load_shg_result_from_mysql() -> dict[str, dict[str, Any]]:
    """新定義の work_other.shg_result を読み込む。

    fase1.0 方針:
    - 返却キーは identity_hash 優先
    - person_id_custom / person_key は CSV表示・橋渡し用途で別途保持可
    - ここでは最低限、CSV出力と突合に必要な項目を返す
    """
    params = load_mysql_base_params()
    result: dict[str, dict[str, Any]] = {}

    sql = """
        SELECT
            id,
            insurer_number_raw,
            insurance_symbol_raw,
            insurance_number_raw,
            name_kana_full_raw,
            birthdate,
            gender_code,
            shg_year,
            usage_ticket_number,
            expiration_date,
            health_checkup_date,
            exam_waist_cm,
            exam_weight_kg,
            received_date,
            person_id_custom,
            identity_hash
        FROM shg_result
        WHERE identity_hash IS NOT NULL
    """

    with connect_ctx(params, database=WORK_OTHER, autocommit=False) as conn:
        cursor = dict_cursor(conn)
        cursor.execute(sql)
        rows = cursor.fetchall()

    for row in rows:
        row_dict = _normalize_db_row(row)
        identity_hash_raw = row_dict.get("identity_hash")
        identity_hash = str(identity_hash_raw).strip() if identity_hash_raw is not None else ""
        if not identity_hash:
            continue
        result[identity_hash] = row_dict

    return result


# ------------------------------------------------------------
# Identity
# ------------------------------------------------------------
def build_xml_identity_from_basic(basic: dict[str, Any]) -> dict[str, Any]:
    """XMLの basic 情報から person_id_custom / identity_hash を生成する。

    旧スクリプトの gen_custom_id_external 置換。
    fase1.0 では generator を正とする。
    """
    return generate_identity_bundle(
        birthdate=basic.get("birth"),
        insurer_number_raw=basic.get("insurer"),
        insurance_symbol_raw=basic.get("symbol"),
        insurance_number_raw=basic.get("number"),
        name_kana_full_raw=basic.get("name"),
        gender_code=basic.get("gender"),
    )


# ------------------------------------------------------------
# CSV skeleton
# ------------------------------------------------------------
def write_export_shg_report_csv(
    out_csv: Path,
    rows: list[dict[str, Any]],
) -> None:
    """fase1.0 の export_shg_report を出力する。

    方針:
    - 既存列は旧スクリプト準拠で維持
    - identity_hash を追加
    - 詳細列構成は旧スクリプトから移植時に確定する
    """
    if not rows:
        return

    headers = list(rows[0].keys())
    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def write_export_outcome_report_csv(
    out_csv: Path,
    rows: list[dict[str, Any]],
) -> None:
    """fase1.0 の export_outcome_report を出力する。

    方針:
    - 既存列は旧スクリプト準拠で維持
    - person_id 列は identity_hash へ変更
    - 健診時腹囲 / 健診時体重は shg_result 由来に置換
    """
    if not rows:
        return

    headers = list(rows[0].keys())
    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


# ------------------------------------------------------------
# Main (fase1.0 skeleton)
# ------------------------------------------------------------
def main() -> None:
    # ------------------------------------------------------------
    # Fixed paths (VSCode Run前提)
    # ------------------------------------------------------------
    # data/hia_export_shg/
    #   input/<insurer_number>/DATA/*.xml
    #   output/<yyyymmdd_hhmmss>/export_shg_report.csv
    #   output/<yyyymmdd_hhmmss>/export_outcome_report.csv
    BASE_DIR = PROJECT_ROOT / "data" / "hia_export_shg"
    input_root_dir = BASE_DIR / "input"
    output_root_dir = BASE_DIR / "output"

    # ディレクトリ生成
    input_root_dir.mkdir(parents=True, exist_ok=True)
    output_root_dir.mkdir(parents=True, exist_ok=True)

    shg_result_map = load_shg_result_from_mysql()

    # input は input/<insurer_number>/DATA/*.xml を前提とする
    insurer_dirs = sorted([p for p in input_root_dir.iterdir() if p.is_dir()])
    if not insurer_dirs:
        print("[WARN] input insurer directory not found")
        print(f"[INFO] expected input root={input_root_dir}")
        return

    from datetime import datetime
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = output_root_dir / run_ts
    out_dir.mkdir(parents=True, exist_ok=True)

    # NOTE:
    # ここから下は fase1.0 の骨格のみ。
    # 旧スクリプトの以下を順次移植する前提:
    # - XML列挙
    # - read_xml
    # - extract_basic
    # - 初回/最終の集約
    # - 旧CSV列の再現
    #
    # 現時点では、shg_result 参照と generator 置換の入口だけを固定する。

    export_shg_rows: list[dict[str, Any]] = []
    export_outcome_rows: list[dict[str, Any]] = []
    people: dict[str, dict[str, Any]] = {}

    xml_paths: list[Path] = []
    for insurer_dir in insurer_dirs:
        xml_paths.extend(scan_xmls(insurer_dir))

    # fase1.0:
    # - XML単位の report を先に組み立てる
    # - people 集約 / outcome_report 本体は次段で移植する
    for xml_path in xml_paths:
        try:
            root = read_xml(xml_path)
            basic = extract_basic(root)
            role = resolve_shg_role(str(basic.get("report_code") or ""))
            identity_res = build_xml_identity_from_basic(basic)

            identity_hash = identity_res.get("identity_hash") or ""
            person_id_custom = identity_res.get("person_id_custom") or ""
            identity_reason = identity_res.get("reason") or ""

            person_key = make_person_key(
                insurer=str(basic.get("insurer") or ""),
                symbol=str(basic.get("symbol") or ""),
                number=str(basic.get("number") or ""),
                name=str(basic.get("name") or ""),
                birth=str(basic.get("birth") or ""),
                gender=str(basic.get("gender") or ""),
            )

            db_row = shg_result_map.get(str(identity_hash), {}) if identity_hash else {}

            initial_goals = extract_initial_goals(root)
            final_outs, outcome_pts, belly_text = extract_final_outcomes(root)
            final_waist_cm, final_weight_kg = extract_final_measurements(root)
            support_summary = extract_support_summary(root)
            process_events = extract_process_events(root)

            if identity_hash:
                bucket = people.setdefault(
                    str(identity_hash),
                    {
                        "identity_hash": str(identity_hash),
                        "person_key": person_key,
                        "person_id_custom": str(person_id_custom),
                        "db_row": db_row,
                        "initial": None,
                        "final": None,
                    },
                )

                rec = {
                    "xml_file": xml_path.name,
                    "basic": basic,
                    "initial_goals": initial_goals,
                    "final_outs": final_outs,
                    "outcome_pts": outcome_pts,
                    "belly_text": belly_text,
                    "final_waist_cm": final_waist_cm,
                    "final_weight_kg": final_weight_kg,
                    "support_summary": support_summary,
                    "process_events": process_events,
                }

                if role == "initial":
                    bucket["initial"] = rec
                elif role == "final":
                    bucket["final"] = rec

            export_shg_rows.append(
                {
                    "xml_file": xml_path.name,
                    "person_key": person_key,
                    "person_id_custom": person_id_custom,
                    "identity_hash": identity_hash,
                    "identity_reason": identity_reason,
                    "role": role or "",
                    "insurer": basic.get("insurer", ""),
                    "symbol": basic.get("symbol", ""),
                    "number": basic.get("number", ""),
                    "name": basic.get("name", ""),
                    "gender": basic.get("gender", ""),
                    "birth": basic.get("birth", ""),
                    "xml_ticket_no": basic.get("ticket_no", ""),
                    "xml_ticket_exp": basic.get("ticket_exp", ""),
                    "db_ticket_no": db_row.get("usage_ticket_number", "") if db_row else "",
                    "db_ticket_exp": db_row.get("expiration_date", "") if db_row else "",
                }
            )
        except Exception as e:
            export_shg_rows.append(
                {
                    "xml_file": xml_path.name,
                    "person_key": "",
                    "person_id_custom": "",
                    "identity_hash": "",
                    "identity_reason": f"xml_parse_error: {e}",
                    "role": "",
                    "insurer": "",
                    "symbol": "",
                    "number": "",
                    "name": "",
                    "gender": "",
                    "birth": "",
                    "xml_ticket_no": "",
                    "xml_ticket_exp": "",
                    "db_ticket_no": "",
                    "db_ticket_exp": "",
                }
            )

    # fase1.0 outcome rows (最小構成)
    for identity_hash, info in sorted(people.items()):
        initial = info.get("initial")
        final = info.get("final")
        db_info = info.get("db_row") or {}

        init_goals = (initial or {}).get("initial_goals") or {}
        final_outs = (final or {}).get("final_outs") or {}
        outcome_pts = (final or {}).get("outcome_pts") or 0
        belly_text = (final or {}).get("belly_text") or ""
        final_waist_cm = (final or {}).get("final_waist_cm")
        final_weight_kg = (final or {}).get("final_weight_kg")
        support_summary = (final or {}).get("support_summary") or {}
        process_events = (final or {}).get("process_events") or {}
        support_counts = support_summary.get("counts") or {}
        support_durations = support_summary.get("durations_min") or {}
        events_90040 = process_events.get("events") or []
        total_points_90040 = int(process_events.get("_total_points") or 0)
        total_minutes_90040 = int(process_events.get("_total_minutes") or 0)

        total_points_90070 = int(support_summary.get("_total_points") or 0)
        grand_total_points = int(support_summary.get("_grand_total") or 0)
        total_minutes_90070 = int(sum(support_durations.values()) if support_durations else 0)

        if events_90040:
            process_source = "90040"
            process_total_points = total_points_90040
            process_total_minutes = total_minutes_90040
        elif final:
            process_source = "90070"
            process_total_points = total_points_90070
            process_total_minutes = total_minutes_90070
        else:
            process_source = ""
            process_total_points = 0
            process_total_minutes = 0

        export_outcome_rows.append(
            {
                "person_key": info.get("person_key", ""),
                "person_id": identity_hash,
                "person_id_custom": info.get("person_id_custom", ""),
                "db_ticket_no": db_info.get("usage_ticket_number", ""),
                "db_ticket_exp": db_info.get("expiration_date", ""),
                "initial_xml": (initial or {}).get("xml_file", ""),
                "final_xml": (final or {}).get("xml_file", ""),
                "initial_exists": "Yes" if initial else "No",
                "健診時_腹囲(cm)": db_info.get("exam_waist_cm", ""),
                "最終_腹囲(cm)": final_waist_cm if final_waist_cm is not None else "",
                "健診時_体重(kg)": db_info.get("exam_weight_kg", ""),
                "最終_体重(kg)": final_weight_kg if final_weight_kg is not None else "",
                "initial_goal_summary": ";".join(
                    [f"{k}:{'目標' if v else '非目標'}" for k, v in init_goals.items()]
                ),
                "final_outcome_summary": ";".join(
                    [f"{k}:{'達成' if v else '未'}" for k, v in final_outs.items()]
                ) if final else "",
                "outcome_total_points": int(outcome_pts or 0),
                "achieve_腹囲体重_内容": belly_text,
                "process_source": process_source,
                "process_total_points": process_total_points,
                "process_total_minutes": process_total_minutes,
                "grand_total_points": grand_total_points,
                "proc_個別支援(対面)_回数": support_counts.get("個別支援(対面)", 0),
                "proc_個別支援(対面)_分": support_durations.get("個別支援(対面)", 0),
                "proc_個別支援(遠隔)_回数": support_counts.get("個別支援(遠隔)", 0),
                "proc_個別支援(遠隔)_分": support_durations.get("個別支援(遠隔)", 0),
                "proc_グループ支援(対面)_回数": support_counts.get("グループ支援(対面)", 0),
                "proc_グループ支援(対面)_分": support_durations.get("グループ支援(対面)", 0),
                "proc_グループ支援(遠隔)_回数": support_counts.get("グループ支援(遠隔)", 0),
                "proc_グループ支援(遠隔)_分": support_durations.get("グループ支援(遠隔)", 0),
                "proc_電話_回数": support_counts.get("電話", 0),
                "proc_電話_分": support_durations.get("電話", 0),
                "proc_電子メール等_回数": support_counts.get("電子メール等", 0),
            }
        )

    write_export_shg_report_csv(
        out_dir / "export_shg_report.csv",
        export_shg_rows,
    )
    write_export_outcome_report_csv(
        out_dir / "export_outcome_report.csv",
        export_outcome_rows,
    )

    print("[OK] fase1.0 skeleton ready")
    print(f"[INFO] input_root_dir={input_root_dir}")
    print(f"[INFO] insurer_dirs={len(insurer_dirs)}")
    print(f"[INFO] out_dir={out_dir}")
    print(f"[INFO] shg_result loaded={len(shg_result_map)}")
    print(f"[INFO] xml scanned={len(xml_paths)}")
    print(f"[INFO] export_shg_rows={len(export_shg_rows)}")

    print(f"[INFO] people aggregated={len(people)}")
    print(f"[INFO] export_outcome_rows={len(export_outcome_rows)}")


if __name__ == "__main__":
    main()