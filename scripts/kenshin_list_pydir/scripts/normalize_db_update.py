# -*- coding: utf-8 -*-
"""
scripts/normalize_db_update.py

DB 内の既存データを「照合用派生列」へ正規化して UPDATE するためのオーケストレーター。
（例: カナ正規化、保険証記号/番号の照合用キー生成 など）

このスクリプトは “固定化フェーズ” の前提として、挙動を変えるリファクタは行わず、
運用・再現・事故防止に必要な情報をコメントとして明文化する。

運用方針
- Job（更新処理）は build_job_specs() に列挙し、.env の ON/OFF で実行対象を制御する。
- 未設定の Job は default=False 扱い（意図せず全件更新が走る事故を防ぐ）。
- DRY_RUN は .env（NORMALIZE_DRY_RUN）を既定とし、CLI の --dry-run があればそちらを優先する。
- VS Code Run / CLI どちらでも動くように、プロジェクトルートを sys.path に追加する。

必要な環境変数（例）
- NORMALIZE_DRY_RUN=1|0
- NORMALIZE_JOB_*=1|0  （下記 “Job一覧” の各 env_key を参照）

実行例
- 全体は .env のスイッチで制御（推奨）:
    python scripts/normalize_db_update.py --dotenv .env
- ドライラン（件数/対象だけ確認）:
    python scripts/normalize_db_update.py --dotenv .env --dry-run
- 緊急時の部分実行（job.name の部分一致で絞り込み）:
    python scripts/normalize_db_update.py --dotenv .env --name-contains ledger_

注意
- ここで更新するのは “派生列（*_match 等）” が主目的。原本列を破壊しない。
- 大量更新になるため、まずは DRY_RUN で対象件数を確認してから実行する。
"""

from __future__ import annotations

# -----------------------------
# VS Code Run / CLI 両対応
# -----------------------------
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# -----------------------------
# imports
# -----------------------------
import os
import argparse
from dataclasses import dataclass
from typing import Callable, Any

from kenshin_lib.db_value_update import UpdateJob, run_update_job, load_env
from kenshin_lib.kana_match_normalizer import normalize_kana_for_match
from kenshin_lib.insurance_number_match_normalizer import normalize_insurance_number_for_match
from kenshin_lib.insurance_symbol_match_normalizer import normalize_insurance_symbol_for_match


def _env_flag(name: str, default: bool = True) -> bool:
    """
    envフラグの解釈:
      "1", "true", "on", "yes" -> True
      "0", "false", "off", "no", "" -> False
    未設定の場合は default を採用。
    変な値なら default（事故防止）。
    """
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in ("1", "true", "on", "yes", "y"):
        return True
    if s in ("0", "false", "off", "no", "n", ""):
        return False
    return default


@dataclass(frozen=True)
class JobSpec:
    env_key: str
    job: UpdateJob
    transform: Callable[[Any], Any]


def build_job_specs() -> list[JobSpec]:
    """
    ここに “同じ仕組みで更新したい処理” を全部列挙する。

    env_key:
      .env の ON/OFF スイッチ名（NORMALIZE_JOB_* を推奨）

    Job一覧（env_key → job.name）
      - NORMALIZE_JOB_TARGETS_NAME_KANA_MATCH → targets_name_kana_match
      - NORMALIZE_JOB_FIND_INSURANCE_NUMBER_MATCH → find_insurance_number_match
      - NORMALIZE_JOB_FIND_INSURANCE_SYMBOL_MATCH → find_insurance_symbol_match
      - NORMALIZE_JOB_LEDGER_NAME_KANA_MATCH → ledger_name_kana_match
      - NORMALIZE_JOB_LEDGER_INSURANCE_NUMBER_MATCH → ledger_insurance_number_match
      - NORMALIZE_JOB_LEDGER_INSURANCE_SYMBOL_MATCH → ledger_insurance_symbol_match
      - NORMALIZE_JOB_EXAM_LEDGER_NAME_KANA_MATCH → exam_ledger_name_kana_match
      - NORMALIZE_JOB_EXAM_LEDGER_INSURANCE_CARD_NUMBER_MATCH → exam_ledger_insurance_card_number_match
      - NORMALIZE_JOB_EXAM_LEDGER_INSURANCE_CARD_SYMBOL_MATCH → exam_ledger_insurance_card_symbol_match
    """
    specs: list[JobSpec] = []

    # ============================================================
    # find_xml_subscribers_list_20260128 (targets)
    # ============================================================

    # ① 対象者テーブル：照合用カナ
    specs.append(
        JobSpec(
            env_key="NORMALIZE_JOB_TARGETS_NAME_KANA_MATCH",
            job=UpdateJob(
                name="targets_name_kana_match",
                db_name="work_other",
                table="find_xml_subscribers_list_20260128",
                key_cols=["subscriber_row_id"],
                src_col="name_kana_raw",
                dst_col="name_kana_match",
                where_sql="`name_kana_match` IS NULL OR `name_kana_match` = ''",
                chunk_size=2000,
            ),
            transform=normalize_kana_for_match,
        )
    )

    # ② 対象者テーブル：保険証番号（照合用・半角数字）
    specs.append(
        JobSpec(
            env_key="NORMALIZE_JOB_FIND_INSURANCE_NUMBER_MATCH",
            job=UpdateJob(
                name="find_insurance_number_match",
                db_name="work_other",
                table="find_xml_subscribers_list_20260128",
                key_cols=["subscriber_row_id"],
                src_col="insurance_number_raw",
                dst_col="insurance_number_match",
                where_sql="`insurance_number_match` IS NULL OR `insurance_number_match` = ''",
                chunk_size=2000,
            ),
            transform=normalize_insurance_number_for_match,
        )
    )

    # ③ 対象者テーブル：保険証記号（照合用・全角正規化）
    specs.append(
        JobSpec(
            env_key="NORMALIZE_JOB_FIND_INSURANCE_SYMBOL_MATCH",
            job=UpdateJob(
                name="find_insurance_symbol_match",
                db_name="work_other",
                table="find_xml_subscribers_list_20260128",
                key_cols=["subscriber_row_id"],
                src_col="insurance_symbol_raw",
                dst_col="insurance_symbol_match",
                where_sql="`insurance_symbol_match` IS NULL OR `insurance_symbol_match` = ''",
                chunk_size=2000,
            ),
            transform=normalize_insurance_symbol_for_match,
        )
    )

    # ============================================================
    # medi_xml_ledger
    # ============================================================

    # ④ ledger：照合用カナ
    specs.append(
        JobSpec(
            env_key="NORMALIZE_JOB_LEDGER_NAME_KANA_MATCH",
            job=UpdateJob(
                name="ledger_name_kana_match",
                db_name="work_other",
                table="medi_xml_ledger",
                key_cols=["xml_ledger_id"],
                src_col="name_kana_full",
                dst_col="name_kana_match",
                where_sql="`name_kana_match` IS NULL OR `name_kana_match` = ''",
                chunk_size=2000,
            ),
            transform=normalize_kana_for_match,
        )
    )

    # ⑤ ledger：保険証番号（照合用・半角数字）
    specs.append(
        JobSpec(
            env_key="NORMALIZE_JOB_LEDGER_INSURANCE_NUMBER_MATCH",
            job=UpdateJob(
                name="ledger_insurance_number_match",
                db_name="work_other",
                table="medi_xml_ledger",
                key_cols=["xml_ledger_id"],
                src_col="insurance_number",
                dst_col="insurance_number_match",
                where_sql="`insurance_number_match` IS NULL OR `insurance_number_match` = ''",
                chunk_size=5000,  # ledgerは大きいので少し増やしてもOK
            ),
            transform=normalize_insurance_number_for_match,
        )
    )

    # ⑥ ledger：保険証記号（照合用・全角正規化）
    specs.append(
        JobSpec(
            env_key="NORMALIZE_JOB_LEDGER_INSURANCE_SYMBOL_MATCH",
            job=UpdateJob(
                name="ledger_insurance_symbol_match",
                db_name="work_other",
                table="medi_xml_ledger",
                key_cols=["xml_ledger_id"],
                src_col="insurance_symbol",
                dst_col="insurance_symbol_match",
                where_sql="`insurance_symbol_match` IS NULL OR `insurance_symbol_match` = ''",
                chunk_size=5000,
            ),
            transform=normalize_insurance_symbol_for_match,
        )
    )

    # ============================================================
    # medi_exam_result_ledger  ★今回追加
    # ============================================================

    # ⑦ exam_result_ledger：照合用カナ
    specs.append(
        JobSpec(
            env_key="NORMALIZE_JOB_EXAM_LEDGER_NAME_KANA_MATCH",
            job=UpdateJob(
                name="exam_ledger_name_kana_match",
                db_name="work_other",
                table="medi_exam_result_ledger",
                key_cols=["ledger_id"],
                src_col="name_kana",
                dst_col="name_kana_match",
                where_sql="`name_kana_match` IS NULL OR `name_kana_match` = ''",
                chunk_size=2000,
            ),
            transform=normalize_kana_for_match,
        )
    )

    # ⑧ exam_result_ledger：保険証番号match（半角数字寄せ）
    # ※ transformは既存の normalize_insurance_number_for_match を使い回す
    specs.append(
        JobSpec(
            env_key="NORMALIZE_JOB_EXAM_LEDGER_INSURANCE_CARD_NUMBER_MATCH",
            job=UpdateJob(
                name="exam_ledger_insurance_card_number_match",
                db_name="work_other",
                table="medi_exam_result_ledger",
                key_cols=["ledger_id"],
                src_col="insurance_card_number",
                dst_col="insurance_card_number_match",
                where_sql="`insurance_card_number_match` IS NULL OR `insurance_card_number_match` = ''",
                chunk_size=2000,
            ),
            transform=normalize_insurance_number_for_match,
        )
    )

    # ⑨ exam_result_ledger：保険証記号match（全角正規化寄せ）
    specs.append(
        JobSpec(
            env_key="NORMALIZE_JOB_EXAM_LEDGER_INSURANCE_CARD_SYMBOL_MATCH",
            job=UpdateJob(
                name="exam_ledger_insurance_card_symbol_match",
                db_name="work_other",
                table="medi_exam_result_ledger",
                key_cols=["ledger_id"],
                src_col="insurance_card_symbol",
                dst_col="insurance_card_symbol_match",
                where_sql="`insurance_card_symbol_match` IS NULL OR `insurance_card_symbol_match` = ''",
                chunk_size=2000,
            ),
            transform=normalize_insurance_symbol_for_match,
        )
    )

    return specs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dotenv", default=".env", help=".env path")
    # dry-run は env をデフォルトに。CLI指定があればそっち優先（store_true）
    ap.add_argument("--dry-run", action="store_true", help="更新せず件数のみ確認")
    # 追加：env制御が基本だが、緊急で部分一致実行したいとき用（未指定なら無効）
    ap.add_argument("--name-contains", default="", help="job.name 部分一致で絞る（任意）")
    args = ap.parse_args()

    # .env を読み込み（job ON/OFF も DRY_RUN もここで読む）
    load_env(args.dotenv)

    env_dry_run = _env_flag("NORMALIZE_DRY_RUN", default=False)
    dry_run = True if args.dry_run else env_dry_run

    specs = build_job_specs()

    enabled: list[JobSpec] = []
    for spec in specs:
        # 未設定は default=False（意図せず全部走らないように）
        if _env_flag(spec.env_key, default=False):
            enabled.append(spec)

    # 追加絞り込み（緊急用）
    if args.name_contains:
        enabled = [s for s in enabled if args.name_contains in s.job.name]

    if not enabled:
        print("No jobs enabled. Set NORMALIZE_JOB_* = 1 in .env")
        return 0

    print("Enabled jobs:")
    for s in enabled:
        print(f"  - {s.job.name}  ({s.env_key}=1)")

    if dry_run:
        print("DRY RUN mode: ON (no DB updates)")

    for s in enabled:
        run_update_job(
            s.job,
            transform=s.transform,
            dotenv_path=args.dotenv,
            dry_run=dry_run,
            verbose=True,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
