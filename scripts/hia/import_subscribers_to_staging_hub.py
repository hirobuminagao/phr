# -*- coding: utf-8 -*-
r"""
============================================================
Script : import_subscribers_to_staging_hub.py
Path   : scripts/hia/import_subscribers_to_staging_hub.py
Project: PHR

Purpose:
    - Hub 由来の「加入者（subscribers）」CSV を
      MySQL の staging_subscribers_hub に取り込む。

Design:
    - ETL ログは scripts.lib.etl（etl_runs / etl_errors）に一元化し、run の開始は先に commit する
    - 本体取込の成否は finish_run で確定し、成功時は staging への INSERT も含めて commit
    - 取込中の行エラーは log_error 経由で etl_errors に記録し、処理は継続（行スキップ）
    - dry-run の場合は staging への INSERT を実行せず、最後に rollback（実質 no-op。run/err は残る）
    - 対象フォルダは `data/hia_export/input_subscribers_csv/<8桁保険者番号>/` をデフォルトとする
    - 進捗ログは ProgressLogger（RunMetrics参照専用）を利用（rows_seen が真実）

V1.1.0 Contract:
    - Scope: Hub CSV → `staging_subscribers_hub` import + current snapshot update まで（`subscribers` 本表への反映は本スクリプトの対象外）
    - Inputs: `data/hia_export/input_subscribers_csv/<8桁保険者番号>/` 配下の *.csv（8桁フォルダは自動列挙）
    - Outputs:
        - `staging_subscribers_hub`（dry-run の場合は INSERT しない）
        - `etl_runs` / `etl_errors`（start_run 直後に commit するため、dry-run / 失敗でも証跡は残る）
    - DB I/O (dev_phr):
        - READS:
            - current snapshot update で `subscribers` / `subscriber_addresses` / `subscriber_contact_points` を参照する
            - current snapshot lookup は import_run_id 単位で staging 行を対象にする
        - WRITES:
            - `staging_subscribers_hub`（主成果物。dry-run 時は INSERT しない）
            - `staging_subscribers_hub.current_*`（current snapshot update で更新予定）
            - `etl_runs` / `etl_errors`（証跡。start_run 直後 commit のため dry-run/失敗でも残る）
        - DB Actions (Fact):
            - start_run: `etl_runs` に INSERT → 直後に `conn.commit()`（dry-run/失敗でも run_id の証跡を残す）
            - per-row error:
                - 正規化エラー / 例外: `log_error` → `etl_errors` に INSERT（行スキップで継続）
            - staging insert: `staging_subscribers_hub` に明示カラム指定 INSERT（dry-run 時は実行しない）
                - columns:
                    - person_id_custom
                    - identity_hash
                    - compare_identity_norm_hash
                    - compare_other_hash
                    - current_subscriber_id
                    - current_hia_subscriber_id
                    - current_identity_hash
                    - current_compare_identity_norm_hash
                    - current_compare_other_hash
                    - current_name_kana_full_match
                    - current_address_id
                    - current_address_hash
                    - current_phone_contact_point_id
                    - current_email_contact_point_id
                    - current_lookup_status
                    - current_lookup_checked_at
                    - hia_subscriber_id
                    - name_kana_full
                    - name_kana_full_match
                    - name_kanji_full
                    - name_kanji_full_match
                    - name_kanji_family
                    - name_kanji_middle
                    - name_kanji_given
                    - name_kana_family
                    - name_kana_middle
                    - name_kana_given
                    - gender_code
                    - birth
                    - insured_attribute_name
                    - relationship_name
                    - insurer_number
                    - insurance_symbol
                    - insurance_symbol_digits
                    - insurance_number
                    - insurance_branchnumber
                    - qualification_acquired_date
                    - qualification_lost_date
                    - postal_code
                    - address_line
                    - building
                    - address_hash
                    - phone
                    - email
                    - employer_code
                    - department_code
                    - distribution_code
                    - employee_code
                    - connect_id
                    - created_at
                    - loaded_at
                    - processed_at
                    - src_file
                    - src_row_no
                    - src_line_no
                    - import_run_id
            - finish_run:
                - 正常系: `finish_run` 実行後、dry-run は `conn.rollback()` / 本番は `conn.commit()`
                - 異常系: 例外時は `conn.rollback()` → `finish_run(status=failed)` → `conn.commit()`
            - current snapshot run:
                - import run 成功後に別 etl_runs として start_run / finish_run する
                - import_run_id の staging 行を対象に current_* を更新する
                - current_* は current実データそのものではなく、review / compare candidate filtering 用の ID / hash / status に絞る
                - dry-run 時は current snapshot update も実行しない
    - File I/O:
        - READS: `data/hia_export/input_subscribers_csv/<8桁保険者番号>/*.csv`
        - READS (config): `scripts.lib.identity.generator` 側の設定解決に従う
    - Key generation:
        - `person_id_custom` / `identity_hash` は `scripts.lib.identity.generator.generate_identity_bundle()` で生成する
    - Identity / field policy:
        - DB格納用 canonical 値は `scripts.lib.identity.field.*` を利用する
        - `person_id_custom` / `identity_hash` は `scripts.lib.identity.generator` を利用する
        - `compare_identity_norm_hash` / `compare_other_hash` / `address_hash` は import-side compare hash として生成し staging に保存する
        - current snapshot 由来の hash は `hub_subscriber_current_snapshot` 側で staging.current_* に保存する
        - 同じ raw 値であっても、DB格納用 field normalize と identity generator は責務を分けて呼び出す
        - generator の `field_results` は identity 生成過程の内部結果であり、DB格納値の一次情報としては扱わない
    - Idempotency (v1.0 現状):
        - 本スクリプト単体では staging の重複排除/UPSERT は行わない（同一 person_id_custom の再投入制御は下流設計に委譲）
        - `src_file/src_row_no/src_line_no/import_run_id` は証跡として保持し、後段での突合・検証に使用する
    - Name parts policy (v1.1.0):
        - 氏名 parts（name_kanji_family/middle/given, name_kana_family/middle/given）は、split 可能な場合のみ格納する
        - split 不可（1トークン）の場合、parts は空文字のままとし、full を正本として保持する
        - したがって、parts 列は「分割済みの確定値」のみを表し、暫定的に full を parts へ流し込まない
    - Non-goals (v1.1.0 対象外):
        - fund 差分ロジック、喪失/異動の確定反映、名寄せ精度の改善、apply_action 判定、正本（subscribers / addresses / contact points）更新
============================================================
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

#
# sys.path 調整
# - このスクリプトは scripts/hia 配下に置く
# - `scripts.lib.*` / `scripts.hia.*` import を直実行でも解決できるように、repo root を sys.path に追加する
# ------------------------------------------------------------
WORK_ROOT = Path(__file__).resolve().parents[2]
if str(WORK_ROOT) not in sys.path:
    sys.path.insert(0, str(WORK_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import DEV_PHR
from scripts.lib.etl import (
    RunMetrics,
    ProgressLogger,
    start_run,
    finish_run,
)
from scripts.lib.io.directory_discovery import (
    list_target_directories,
    has_files_by_suffix,
    estimate_csv_rows_in_directories,
)

from scripts.hia.script_lib.hub_subscriber_import import process_csv_dir
from scripts.hia.script_lib.hub_subscriber_current_snapshot import (
    CurrentSnapshotMetrics,
    update_current_snapshot,
)


# ============================================================
# 基本設定
# ============================================================

JOB_NAME = "subscribers_hub"
BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_BASE = BASE_DIR / "data" / "hia_export" / "input_subscribers_csv"


def normalize_insurer_folder_name_to_int(folder: Path) -> int:
    """8桁保険者番号フォルダ名を int に変換する。"""

    name = folder.name.strip()
    if not name.isdigit() or len(name) != 8:
        raise ValueError(f"invalid insurer folder name: {folder}")

    return int(name)


# ============================================================
# 実行仕様（固定化ポイント）
# - 対象フォルダは 8桁ディレクトリを自動列挙。--input 指定時はそのディレクトリのみ
# - run_id は start_run 後に即 commit（進捗/失敗の証跡を残す）
# - 取込中の行エラーは etl_errors に記録し、行スキップで継続
# - import run の状態は finish_run で確定する
# - import run 成功後、dry-run でなければ current snapshot run を続けて実行する
# ============================================================

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Hub CSV を MySQL の staging_subscribers_hub に取り込む",
    )
    ap.add_argument("--base", default=str(DEFAULT_INPUT_BASE))
    ap.add_argument("--input", help="単一の 8 桁フォルダを直接指定したい場合に利用")
    ap.add_argument("--schema", default=None, help="接続先 DB スキーマ名")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="処理する行数の上限 (0 = 無制限)")
    ap.add_argument("--progress-interval", type=int, default=1000, help="進捗ログをN件ごとに出力（0で無効）")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    base_dir = Path(args.base)
    raw_target_dirs = list_target_directories(base_dir, args.input)

    active_pairs: list[tuple[Path, int]] = []
    for d in raw_target_dirs:
        has_csv = has_files_by_suffix(d, ".csv")
        if not has_csv:
            print(f"[INFO] skip folder (no CSV): {d.name}")
            continue
        insurer_num = normalize_insurer_folder_name_to_int(d)
        active_pairs.append((d, insurer_num))

    if not active_pairs:
        print("[ERR] 有効な CSV を含むフォルダがありませんでした。処理を中止します。")
        return 7

    target_dirs: list[Path] = [p for (p, _) in active_pairs]
    insurer_ids: list[int] = [i for (_, i) in active_pairs]

    if len(insurer_ids) == 1:
        run_insurer_number = f"{insurer_ids[0]:08d}"
        run_input_file = target_dirs[0].name
    else:
        run_insurer_number = None
        run_input_file = None

    insurers_summary = ",".join(f"{i:08d}" for i in insurer_ids)

    params = load_mysql_base_params()
    schema_name = DEV_PHR
    db_path_str = f"{params.host}:{params.port}/{schema_name}"

    print(f"[INFO] BASE      = {base_dir}")
    print(f"[INFO] TARGETS   = {[d.name for d in target_dirs]}")
    print(f"[INFO] DB_SCHEMA = {schema_name} (forced)")
    print(f"[INFO] DRY_RUN   = {args.dry_run}")
    print(f"[INFO] LIMIT     = {args.limit}")
    print(f"[INFO] PROGRESS  = {args.progress_interval}")
    print(f"[INFO] INSURERS  = {insurers_summary}")

    try:
        with connect_ctx(params, database=DEV_PHR) as conn:
            with dict_cursor(conn) as cur:
                metrics_all = RunMetrics()

                total_rows = estimate_csv_rows_in_directories(
                    target_dirs,
                    header_count=1,
                    limit=args.limit,
                )
                plog = ProgressLogger(
                    total=total_rows,
                    metrics=metrics_all,
                    interval=args.progress_interval,
                    label="IMPORT",
                    logger=logging.getLogger(__name__),
                )

                run_id = start_run(
                    cur,
                    phase="import",
                    source="import_subscribers_to_staging_hub",
                    db_schema=schema_name,
                    db_path=db_path_str,
                    input_base=str(base_dir),
                    input_file=run_input_file,
                    insurer_number=run_insurer_number,
                    dry_run=args.dry_run,
                    limit_rows=args.limit,
                )
                print(f"[INFO] run_id = {run_id}")
                conn.commit()

                try:
                    for folder, insurer_number in active_pairs:
                        if args.limit:
                            remaining = args.limit - (
                                metrics_all.rows_inserted
                                + metrics_all.rows_skipped
                            )
                            if remaining <= 0:
                                break
                        else:
                            remaining = 0

                        f_metrics = process_csv_dir(
                            cur,
                            run_id=run_id,
                            insurer_number=insurer_number,
                            folder=folder,
                            metrics_all=metrics_all,
                            plog=plog,
                            limit=remaining,
                            dry_run=args.dry_run,
                        )

                        print(
                            f"[OK] insurer={insurer_number:08d} folder={folder.name} "
                            f"files={f_metrics.files} rows={f_metrics.rows_seen} "
                            f"inserted={f_metrics.rows_inserted} skipped={f_metrics.rows_skipped} "
                            f"errors={f_metrics.errors}"
                        )

                        if args.limit and (
                            metrics_all.rows_inserted + metrics_all.rows_skipped
                        ) >= args.limit:
                            break

                    plog.finalize()

                    finish_run(
                        cur,
                        run_id,
                        metrics_all,
                        extra_notes=f"insurers={insurers_summary}",
                    )
                    if args.dry_run:
                        conn.rollback()
                        print(
                            f"[DONE] import_run_id={run_id} total_files={metrics_all.files} "
                            f"rows={metrics_all.rows_seen} inserted={metrics_all.rows_inserted} "
                            f"skipped={metrics_all.rows_skipped} errors={metrics_all.errors} "
                            f"current_snapshot=skipped(dry-run)"
                        )
                    else:
                        conn.commit()

                        current_metrics = CurrentSnapshotMetrics()
                        current_run_metrics = RunMetrics()

                        current_run_id = start_run(
                            cur,
                            phase="import",
                            source="hub_subscriber_current_snapshot",
                            db_schema=schema_name,
                            db_path=db_path_str,
                            input_base=str(base_dir),
                            input_file=run_input_file,
                            insurer_number=run_insurer_number,
                            dry_run=False,
                            limit_rows=args.limit,
                        )
                        print(f"[INFO] current_snapshot_run_id = {current_run_id}")
                        conn.commit()

                        try:
                            update_current_snapshot(
                                cur,
                                import_run_id=run_id,
                                metrics=current_metrics,
                                plog=None,
                            )

                            current_run_metrics.rows_seen = current_metrics.rows_seen
                            current_run_metrics.rows_inserted = current_metrics.updated
                            current_run_metrics.rows_skipped = (
                                current_metrics.not_found
                                + current_metrics.multiple_match
                                + current_metrics.review
                            )
                            current_run_metrics.errors = current_metrics.errors

                            finish_run(
                                cur,
                                current_run_id,
                                current_run_metrics,
                                extra_notes=(
                                    f"import_run_id={run_id}, "
                                    f"hia_id_matched={current_metrics.hia_id_matched}, "
                                    f"identity_hash_matched={current_metrics.identity_hash_matched}, "
                                    f"person_id_custom_matched={current_metrics.person_id_custom_matched}, "
                                    f"not_found={current_metrics.not_found}, "
                                    f"multiple_match={current_metrics.multiple_match}, "
                                    f"review={current_metrics.review}"
                                ),
                            )
                            conn.commit()

                        except Exception as e:
                            conn.rollback()
                            current_run_metrics.errors += 1
                            finish_run(
                                cur,
                                current_run_id,
                                current_run_metrics,
                                status_override="failed",
                                extra_notes=f"import_run_id={run_id}, error={e}",
                            )
                            conn.commit()
                            raise

                        print(
                            f"[DONE] import_run_id={run_id} total_files={metrics_all.files} "
                            f"rows={metrics_all.rows_seen} inserted={metrics_all.rows_inserted} "
                            f"skipped={metrics_all.rows_skipped} errors={metrics_all.errors}"
                        )
                        print(
                            f"[DONE] current_snapshot_run_id={current_run_id} "
                            f"rows={current_metrics.rows_seen} updated={current_metrics.updated} "
                            f"hia_id_matched={current_metrics.hia_id_matched} "
                            f"identity_hash_matched={current_metrics.identity_hash_matched} "
                            f"person_id_custom_matched={current_metrics.person_id_custom_matched} "
                            f"not_found={current_metrics.not_found} "
                            f"multiple_match={current_metrics.multiple_match} "
                            f"review={current_metrics.review} "
                            f"errors={current_metrics.errors}"
                        )

                except Exception as e:
                    # staging への未確定 INSERT を取り消し。run/err の最終状態はこの後 commit する
                    conn.rollback()
                    print(f"[ERR] 取込中に例外発生: {e}")
                    metrics_all.errors += 1
                    finish_run(
                        cur,
                        run_id,
                        metrics_all,
                        status_override="failed",
                        extra_notes=f"insurers={insurers_summary}, error={e}",
                    )
                    conn.commit()
                    return 7

    except Exception as e:
        # DB 接続失敗など start_run 前の致命。etl_runs にも残らない可能性がある
        print(f"[FATAL] DB 接続または実行時エラー: {e}")
        return 7

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
