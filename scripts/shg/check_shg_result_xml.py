

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
check_shg_result_xml.py

SHG結果XMLチェック / outcome集計スクリプト

目的:
- SHG結果XMLを収集し、初回/最終情報を抽出する
- identity_hash単位で初回XML・最終XMLを束ねる
- 利用券情報の整合確認および必要最小限の自動補正を行う
- outcome判定 / 腹囲体重判定 / 継続日数判定を生成する
- CSVレポートを出力する

現在の主な処理:
- XML収集
- XML読込
- basic情報抽出
- identity_bundle生成
  - person_id_custom
  - identity_hash
- shg_result 参照
- 利用券fix判定
- 利用券XML更新
- 90010 指導情報抽出
- 90030 初回面談/目標抽出
- 90040 支援イベント抽出
- 90060 最終評価抽出
- 90070 支援集計抽出
- initial/final 集約
- outcome判定
- 腹囲体重チェック
- 継続期間チェック
- processポイント集計
- CSV出力

出力:
- export_shg_report.csv
  - XML単位チェック結果
- export_outcome_report.csv
  - identity_hash単位 outcome集計結果

内部キー方針:
- identity_hash を主束ねキーとして使用
- person_id_custom は補助識別子として保持
- person_key は目視確認用として保持

注意:
- 本スクリプトは現行 SHG XMLチェック本体として運用する
- 旧 tokuho_xml_check は参照用アーカイブ扱いとする
- XML補正は「機械的に確定できる内容のみ」を対象とする
- 推定補正は禁止
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
import argparse
import csv
import sys

# ------------------------------------------------------------
# VSCode Run ボタン / file実行 対応
# ------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from scripts.lib.identity.generator import generate_identity_bundle
from scripts.lib.db.lookup.fund import (
    FundLookupError,
    get_fund_name_from_insurer_number,
)
from scripts.lib.shg.xml.basic import extract_basic
from scripts.lib.shg.xml.section_90030_initial import (
    extract_initial_goals,
    extract_initial_date,
    extract_initial_goal_levels,
    extract_initial_interview_mode,
)
from scripts.lib.shg.xml.section_90060_final import (
    extract_final_outcomes,
    extract_final_outcome_levels,
    extract_final_measurements,
)
from scripts.lib.shg.xml.role import resolve_shg_role
from scripts.lib.shg.xml.section_90070_support_summary import extract_support_summary
from scripts.lib.shg.xml.section_90040_support_detail import extract_process_events
from scripts.lib.shg.xml.section_90010_guidance_info import extract_90010_guidance
from scripts.lib.shg.xml.outcome_checks import (
    build_conflict_result,
    build_waist_weight_check_result,
    compute_duration_days,
)

from scripts.lib.shg.xml.update import save_xml

# ------------------------------------------------------------
# Use shared XML I/O helpers
# ------------------------------------------------------------
from scripts.shg.script_lib.xml_io import (
    collect_input_xml_paths,
    read_xml,
)

# ------------------------------------------------------------
# Use shared SHG result loader
# ------------------------------------------------------------
from scripts.shg.script_lib.shg_result_loader import (
    load_shg_result_from_mysql,
)

# ------------------------------------------------------------
# Ticket fix import
# ------------------------------------------------------------
# ------------------------------------------------------------
# Ticket fix import
# ------------------------------------------------------------
from scripts.shg.script_lib.ticket_fix import (
    build_ticket_fix_result,
)

# ------------------------------------------------------------
# XML ticket update import
# ------------------------------------------------------------

from scripts.shg.script_lib.xml_ticket_writer import (
    update_usage_ticket_xml_file,
)

# ------------------------------------------------------------
# Outcome policy import
# ------------------------------------------------------------
from scripts.shg.script_lib.outcome_policy import (
    apply_final_only_motivation_conflict_policy,
)

# ------------------------------------------------------------
# Outcome point block fix import
# ------------------------------------------------------------
from scripts.shg.script_lib.outcome_point_block_fix import (
    apply_outcome_total_point_block_fix,
)

# ------------------------------------------------------------
# XML namespace / OID constants
# ------------------------------------------------------------
NS = {
    "cda": "urn:hl7-org:v3",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
}


# ------------------------------------------------------------
# Current implementation memo
# ------------------------------------------------------------
# 現行実装:
# - XML / ZIP入力対応
# - identity_hash 集約
# - 利用券fix
# - outcome判定
# - 腹囲体重チェック
# - 継続期間チェック
# - processポイント集計
# - export_shg_report 出力
# - export_outcome_report 出力
#
# 現在の主な共通化:
# - XML I/O → script_lib/xml_io.py
# - 利用券fix → script_lib/ticket_fix.py
# - XML更新 → script_lib/xml_ticket_writer.py
# - outcome policy → script_lib/outcome_policy.py
# - identity生成 → scripts.lib.identity.generator


# ------------------------------------------------------------
# Args
# ------------------------------------------------------------
def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="SHG結果XMLチェック / outcome集計"
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        help="XML / ZIP入力ディレクトリ",
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


def resolve_insurer_name(insurer_number: Any) -> str:
    """XML由来の保険者番号から健康保険組合名を解決する。"""
    raw = str(insurer_number or "").strip()
    if not raw:
        return ""
    try:
        return get_fund_name_from_insurer_number(raw)
    except (ValueError, FundLookupError):
        return ""




# ------------------------------------------------------------
# Identity
# ------------------------------------------------------------
def build_xml_identity_from_basic(basic: dict[str, Any]) -> dict[str, Any]:
    """XMLの basic 情報から person_id_custom / identity_hash を生成する。

    identity生成は scripts.lib.identity.generator を正とする。
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


    from datetime import datetime
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = output_root_dir / run_ts
    out_dir.mkdir(parents=True, exist_ok=True)
    work_dir = out_dir / "_work_zip_extract"

    # NOTE:
    # 現行 SHG XMLチェック本体。
    # XML / ZIP収集、section抽出、利用券fix、
    # outcome判定、CSV出力までを実装済み。

    export_shg_rows: list[dict[str, Any]] = []
    export_outcome_rows: list[dict[str, Any]] = []
    people: dict[str, dict[str, Any]] = {}

    xml_paths, _xml_to_extract_dir = collect_input_xml_paths(input_root_dir, work_dir)

    # XML単位 report を構築しつつ、
    # identity_hash 単位で people 集約を行う。
    for xml_path in xml_paths:
        try:
            root = read_xml(xml_path)
            basic = extract_basic(root)
            insurer_name = resolve_insurer_name(basic.get("insurer"))
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

            ticket_fix_result = build_ticket_fix_result(
                xml_ticket_no=basic.get("ticket_no", ""),
                xml_ticket_exp=basic.get("ticket_exp", ""),
                db_ticket_no=db_row.get("usage_ticket_number", "") if db_row else "",
                db_ticket_exp=db_row.get("expiration_date", "") if db_row else "",
            )
            ticket_update_result = update_usage_ticket_xml_file(
                xml_path=xml_path,
                root=root,
                ticket_fix_result=ticket_fix_result,
            )

            initial_goals = extract_initial_goals(root)
            initial_date = extract_initial_date(root)
            initial_goal_levels = extract_initial_goal_levels(root)
            initial_interview_mode = extract_initial_interview_mode(root)
            final_outs, outcome_pts, belly_text = extract_final_outcomes(root)
            final_outcome_levels = extract_final_outcome_levels(root)
            final_waist_cm, final_weight_kg = extract_final_measurements(root)
            support_summary = extract_support_summary(root)
            process_events = extract_process_events(root)
            guidance = extract_90010_guidance(root)

            outcome_point_block_fix_result = apply_outcome_total_point_block_fix(
                root=root,
                report_code=str(basic.get("report_code") or ""),
                level_code=str(guidance.get("guidance_type_code") or "") if guidance else "",
                outcome_total_points=outcome_pts,
                insurer_number=str(basic.get("insurer") or ""),
            )
            outcome_point_block_fix_save_result: dict[str, Any] | None = None
            if outcome_point_block_fix_result.applied:
                outcome_point_block_fix_save_result = save_xml(
                    xml_path=xml_path,
                    root=root,
                )

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
                    "guidance": guidance,
                    "initial_date": initial_date,
                    "initial_goals": initial_goals,
                    "initial_goal_levels": initial_goal_levels,
                    "initial_interview_mode": initial_interview_mode,
                    "final_outs": final_outs,
                    "final_outcome_levels": final_outcome_levels,
                    "outcome_pts": outcome_pts,
                    "belly_text": belly_text,
                    "final_waist_cm": final_waist_cm,
                    "final_weight_kg": final_weight_kg,
                    "support_summary": support_summary,
                    "process_events": process_events,
                    "outcome_point_block_fix_result": outcome_point_block_fix_result,
                    "outcome_point_block_fix_save_result": outcome_point_block_fix_save_result,
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
                    "report_code": basic.get("report_code", ""),
                    "initial_date": initial_date or "",
                    "final_date": basic.get("final_date", ""),
                    "insurer": basic.get("insurer", ""),
                    "健康保険組合名": insurer_name,
                    "symbol": basic.get("symbol", ""),
                    "number": basic.get("number", ""),
                    "name": basic.get("name", ""),
                    "gender": basic.get("gender", ""),
                    "birth": basic.get("birth", ""),
                    "xml_ticket_no": basic.get("ticket_no", ""),
                    "xml_ticket_exp": basic.get("ticket_exp", ""),
                    "db_ticket_no": db_row.get("usage_ticket_number", "") if db_row else "",
                    "db_ticket_exp": db_row.get("expiration_date", "") if db_row else "",
                    "ticket_fix_status": ticket_fix_result.status,
                    "ticket_fix_fields": ",".join(ticket_fix_result.fix_fields),
                    "ticket_fix_reason": ticket_fix_result.reason,
                    "ticket_update_applied": "Yes" if ticket_update_result.updated else "No",
                    "ticket_update_fields": ",".join(ticket_update_result.updated_fields),
                    "ticket_update_reason": ticket_update_result.reason,
                    "guidance_type_code": guidance.get("guidance_type_code") if guidance else "",
                    "guidance_type_name": guidance.get("guidance_type_name") if guidance else "",
                    "outcome_point_block_fix_applied": "Yes" if outcome_point_block_fix_result.applied else "No",
                    "outcome_point_block_fix_status": outcome_point_block_fix_result.status,
                    "outcome_point_block_fix_reason": outcome_point_block_fix_result.reason,
                    "outcome_point_block_fix_save_status": (
                        outcome_point_block_fix_save_result.get("status", "")
                        if outcome_point_block_fix_save_result
                        else ""
                    ),
                    "outcome_point_block_fix_save_message": (
                        outcome_point_block_fix_save_result.get("message", "")
                        if outcome_point_block_fix_save_result
                        else ""
                    ),
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
                    "健康保険組合名": "",
                    "symbol": "",
                    "number": "",
                    "name": "",
                    "gender": "",
                    "birth": "",
                    "xml_ticket_no": "",
                    "xml_ticket_exp": "",
                    "db_ticket_no": "",
                    "db_ticket_exp": "",
                    "ticket_fix_status": "",
                    "ticket_fix_fields": "",
                    "ticket_fix_reason": "",
                    "ticket_update_applied": "",
                    "ticket_update_fields": "",
                    "ticket_update_reason": "",
                    "guidance_type_code": "",
                    "guidance_type_name": "",
                    "outcome_point_block_fix_applied": "",
                    "outcome_point_block_fix_status": "",
                    "outcome_point_block_fix_reason": "",
                    "outcome_point_block_fix_save_status": "",
                    "outcome_point_block_fix_save_message": "",
                }
            )

    # fase1.0 outcome rows (最小構成)
    for identity_hash, info in sorted(people.items()):
        initial = info.get("initial")
        final = info.get("final")
        db_info = info.get("db_row") or {}
        insurer_number_value = (
            ((final or {}).get("basic") or {}).get("insurer")
            or ((initial or {}).get("basic") or {}).get("insurer")
            or ""
        )
        insurer_name = resolve_insurer_name(insurer_number_value)
        initial_guidance = (initial or {}).get("guidance") or {}
        final_guidance = (final or {}).get("guidance") or {}
        level_code = (
            final_guidance.get("guidance_type_code")
            or initial_guidance.get("guidance_type_code")
            or ""
        )
        level_text = (
            final_guidance.get("guidance_type_name")
            or initial_guidance.get("guidance_type_name")
            or ""
        )

        initial_date_value = (
            (final or {}).get("initial_date")
            or (initial or {}).get("initial_date")
            or ""
        )
        final_date_value = (
            ((final or {}).get("basic") or {}).get("final_date")
            or ""
        )
        duration_days = compute_duration_days(initial_date_value, final_date_value)
        duration_mode = "days"
        duration_threshold = "93日以上"
        if duration_days is None:
            duration_verdict = ""
        elif duration_days >= 93:
            duration_verdict = "OK"
        else:
            duration_verdict = "NG"

        init_goals = (
            (final or {}).get("initial_goals")
            or (initial or {}).get("initial_goals")
            or {}
        )
        initial_goal_levels = (
            (final or {}).get("initial_goal_levels")
            or (initial or {}).get("initial_goal_levels")
            or {}
        )
        final_outs = (final or {}).get("final_outs") or {}
        final_outcome_levels = (final or {}).get("final_outcome_levels") or {}
        initial_interview_mode_initial = (initial or {}).get("initial_interview_mode") or {}
        initial_interview_mode_final = (final or {}).get("initial_interview_mode") or {}
        plan_goal_map = {
            "腹囲・体重の改善": init_goals.get("腹囲・体重の改善", False),
            "生活習慣の改善(食習慣)": init_goals.get("生活習慣の改善(食習慣)", False),
            "生活習慣の改善(運動習慣)": init_goals.get("生活習慣の改善(運動習慣)", False),
            "生活習慣の改善(喫煙習慣)": init_goals.get("生活習慣の改善(喫煙習慣)", False),
            "生活習慣の改善(休養習慣)": init_goals.get("生活習慣の改善(休養習慣)", False),
            "生活習慣の改善(その他)": init_goals.get("生活習慣の改善(その他)", False),
        }
        outcome_map = {
            "腹囲・体重の改善": final_outs.get("腹囲・体重の改善", False),
            "生活習慣の改善(食習慣)": final_outs.get("生活習慣の改善(食習慣)", False),
            "生活習慣の改善(運動習慣)": final_outs.get("生活習慣の改善(運動習慣)", False),
            "生活習慣の改善(喫煙習慣)": final_outs.get("生活習慣の改善(喫煙習慣)", False),
            "生活習慣の改善(休養習慣)": final_outs.get("生活習慣の改善(休養習慣)", False),
            "生活習慣の改善(その他)": final_outs.get("生活習慣の改善(その他の生活習慣)", False),
        }

        outcome_pts = (final or {}).get("outcome_pts") or 0
        belly_text = (final or {}).get("belly_text") or ""
        if not belly_text:
            belly_level_raw = final_outcome_levels.get("腹囲・体重の改善")
            try:
                belly_level = int(belly_level_raw) if belly_level_raw is not None else None
            except (TypeError, ValueError):
                belly_level = None

            belly_text_map: dict[int, str] = {
                0: "未達成",
                1: "1cm/1kg",
                2: "2cm/2kg",
            }
            belly_text = belly_text_map.get(belly_level, "") if belly_level is not None else ""

        waist_plan_level_raw = initial_goal_levels.get("腹囲・体重の改善")
        try:
            waist_plan_level = int(waist_plan_level_raw) if waist_plan_level_raw is not None else None
        except (TypeError, ValueError):
            waist_plan_level = None

        waist_plan_text_map: dict[int, str] = {
            0: "計画なし",
            1: "1cm・1kg",
            2: "2cm・2kg",
        }
        waist_plan_text = waist_plan_text_map.get(waist_plan_level, "") if waist_plan_level is not None else ""
        final_waist_cm = (final or {}).get("final_waist_cm")
        final_weight_kg = (final or {}).get("final_weight_kg")

        general_conflict_result = build_conflict_result(
            plan_goal_map=plan_goal_map,
            outcome_map=outcome_map,
            has_final=bool(final),
        )
        final_report_code = ((final or {}).get("basic") or {}).get("report_code", "")
        has_initial = bool(initial)
        for short_name in ["食", "運動", "喫煙", "休養", "その他"]:
            general_conflict_result[short_name] = apply_final_only_motivation_conflict_policy(
                conflict_result=general_conflict_result.get(short_name, ""),
                report_code=final_report_code,
                has_initial=has_initial,
                level_text=level_text,
                category=short_name,
            )

        waist_weight_check_result = build_waist_weight_check_result(
            plan_level=initial_goal_levels.get("腹囲・体重の改善"),
            report_level=final_outcome_levels.get("腹囲・体重の改善"),
            exam_waist_cm=db_info.get("exam_waist_cm", ""),
            final_waist_cm=final_waist_cm,
            exam_weight_kg=db_info.get("exam_weight_kg", ""),
            final_weight_kg=final_weight_kg,
            has_final=bool(final),
        )

        conflict_items: list[str] = []
        if waist_weight_check_result.get("summary") == "NG":
            conflict_items.append("腹囲体重")
        for short_name in ["食", "運動", "喫煙", "休養", "その他"]:
            if general_conflict_result.get(short_name) == "NG":
                conflict_items.append(short_name)

        if not final:
            overall_conflict_summary = ""
        elif conflict_items:
            overall_conflict_summary = f"Yes: {', '.join(conflict_items)}"
        else:
            overall_conflict_summary = "No"
        support_summary = (final or {}).get("support_summary") or {}
        process_events = (final or {}).get("process_events") or {}
        support_counts = support_summary.get("counts") or {}
        support_durations = support_summary.get("durations_min") or {}
        events_90040 = process_events.get("events") or []
        total_points_90040 = int(process_events.get("_total_points") or 0)
        total_minutes_90040 = int(process_events.get("_total_minutes") or 0)

        total_points_90070 = int(support_summary.get("_total_points") or 0)
        total_minutes_90070 = int(sum(support_durations.values()) if support_durations else 0)
        has_90070_values = any(
            [
                total_points_90070 > 0,
                total_minutes_90070 > 0,
                any(int(v or 0) > 0 for v in support_counts.values()) if support_counts else False,
                any(int(v or 0) > 0 for v in support_durations.values()) if support_durations else False,
            ]
        )

        if events_90040:
            process_source = "90040"
            process_total_points = total_points_90040
            process_total_minutes = total_minutes_90040
        elif has_90070_values:
            process_source = "90070_evn"
            process_total_points = total_points_90070
            process_total_minutes = total_minutes_90070
        else:
            process_source = "none"
            process_total_points = 0
            process_total_minutes = 0

        grand_total_points = int(outcome_pts or 0) + int(process_total_points or 0)

        export_outcome_rows.append(
            {
                "person_key": info.get("person_key", ""),
                "person_id": identity_hash,
                "person_id_custom": info.get("person_id_custom", ""),
                "insurer": insurer_number_value,
                "健康保険組合名": insurer_name,
                "db_ticket_no": db_info.get("usage_ticket_number", ""),
                "db_ticket_exp": db_info.get("expiration_date", ""),
                "initial_xml": (initial or {}).get("xml_file", ""),
                "final_xml": (final or {}).get("xml_file", ""),
                "initial_exists": "Yes" if initial else "No",
                "初回面談方式_初回XML_コード": initial_interview_mode_initial.get("code", ""),
                "初回面談方式_初回XML_内容": initial_interview_mode_initial.get("display", ""),
                "初回面談方式_最終XML_コード": initial_interview_mode_final.get("code", ""),
                "初回面談方式_最終XML_内容": initial_interview_mode_final.get("display", ""),
                "level_code": level_code,
                "level_text": level_text,
                "initial_date": initial_date_value,
                "final_date": final_date_value,
                "継続日数": duration_days if duration_days is not None else "",
                "継続判定モード": duration_mode,
                "継続しきい値": duration_threshold,
                "継続期間_XML判定": duration_verdict,
                "矛盾(目標なし達成あり)": overall_conflict_summary,
                "健診時_腹囲(cm)": db_info.get("exam_waist_cm", ""),
                "最終_腹囲(cm)": final_waist_cm if final_waist_cm is not None else "",
                "健診時_体重(kg)": db_info.get("exam_weight_kg", ""),
                "最終_体重(kg)": final_weight_kg if final_weight_kg is not None else "",
                "計_腹囲体重": waist_plan_text,
                "結_腹囲体重": belly_text,
                "achieve_腹囲体重_内容": belly_text,
                "conflict_腹囲体重_XML判定": waist_weight_check_result.get("summary", ""),
                "腹囲体重_判定ソース": waist_weight_check_result.get("source", ""),
                "腹囲体重_計画値": waist_weight_check_result.get("plan_level", ""),
                "腹囲体重_報告値": waist_weight_check_result.get("report_level", ""),
                "腹囲体重_実測判定": waist_weight_check_result.get("measured_level", ""),
                "計_食": "目標" if plan_goal_map["生活習慣の改善(食習慣)"] else "非目標",
                "結_食": "達成" if outcome_map["生活習慣の改善(食習慣)"] else "未達成",
                "conflict_食_XML判定": general_conflict_result.get("食", ""),
                "計_運動": "目標" if plan_goal_map["生活習慣の改善(運動習慣)"] else "非目標",
                "結_運動": "達成" if outcome_map["生活習慣の改善(運動習慣)"] else "未達成",
                "conflict_運動_XML判定": general_conflict_result.get("運動", ""),
                "計_喫煙": "目標" if plan_goal_map["生活習慣の改善(喫煙習慣)"] else "非目標",
                "結_喫煙": "達成" if outcome_map["生活習慣の改善(喫煙習慣)"] else "未達成",
                "conflict_喫煙_XML判定": general_conflict_result.get("喫煙", ""),
                "計_休養": "目標" if plan_goal_map["生活習慣の改善(休養習慣)"] else "非目標",
                "結_休養": "達成" if outcome_map["生活習慣の改善(休養習慣)"] else "未達成",
                "conflict_休養_XML判定": general_conflict_result.get("休養", ""),
                "計_その他": "目標" if plan_goal_map["生活習慣の改善(その他)"] else "非目標",
                "結_その他": "達成" if outcome_map["生活習慣の改善(その他)"] else "未達成",
                "conflict_その他_XML判定": general_conflict_result.get("その他", ""),
                "initial_goal_summary": ";".join(
                    [f"{k}:{'目標' if v else '非目標'}" for k, v in init_goals.items()]
                ),
                "final_outcome_summary": ";".join(
                    [f"{k}:{'達成' if v else '未'}" for k, v in final_outs.items()]
                ) if final else "",
                "outcome_total_points": int(outcome_pts or 0),
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
                "proc_電子メール等_分": 0,
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

    print("[OK] SHG XML check completed")
    print(f"[INFO] input_root_dir={input_root_dir}")
    print(f"[INFO] out_dir={out_dir}")
    print(f"[INFO] shg_result loaded={len(shg_result_map)}")
    print(f"[INFO] xml scanned={len(xml_paths)}")
    print(f"[INFO] export_shg_rows={len(export_shg_rows)}")

    print(f"[INFO] people aggregated={len(people)}")
    print(f"[INFO] export_outcome_rows={len(export_outcome_rows)}")


if __name__ == "__main__":
    main()
