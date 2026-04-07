

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


# ------------------------------------------------------------
# DB
# ------------------------------------------------------------
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
        identity_hash = (row.get("identity_hash") or "").strip()
        if not identity_hash:
            continue
        result[identity_hash] = dict(row)

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
    args = build_arg_parser().parse_args()

    input_dir = Path(args.input_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    shg_result_map = load_shg_result_from_mysql()

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

    # TODO fase1.0:
    # 1. scan_xmls(input_dir) を旧スクリプトから移植
    # 2. read_xml / extract_basic を移植
    # 3. build_xml_identity_from_basic() で person_id_custom / identity_hash を生成
    # 4. shg_result_map を identity_hash キーで引く
    # 5. export_shg_rows / export_outcome_rows を旧CSV構造に合わせて構築
    # 6. export_outcome_report では person_id 列を identity_hash に変更
    # 7. 健診時_腹囲(cm) / 健診時_体重(kg) に shg_result の値を反映

    write_export_shg_report_csv(
        out_dir / "export_shg_report.csv",
        export_shg_rows,
    )
    write_export_outcome_report_csv(
        out_dir / "export_outcome_report.csv",
        export_outcome_rows,
    )

    print("[OK] fase1.0 skeleton ready")
    print(f"[INFO] input_dir={input_dir}")
    print(f"[INFO] out_dir={out_dir}")
    print(f"[INFO] shg_result loaded={len(shg_result_map)}")


if __name__ == "__main__":
    main()