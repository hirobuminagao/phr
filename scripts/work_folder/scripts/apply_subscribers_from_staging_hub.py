# -*- coding: utf-8 -*-
r"""
============================================================
Script : apply_subscribers_from_staging_hub.py
Path   : scripts/work_folder/scripts/apply_subscribers_from_staging_hub.py
Project: PHR / work_folder/phr

Purpose (v1.0 as-is):
    - 【注意】ファイル名は apply だが、v1.0 現状コードは Hub 由来の加入者CSVを
      MySQL の staging_subscribers_hub に取り込む（= import 相当）。
    - `subscribers` 本表への反映（apply 相当）は本ファイル内では未実装/未到達。

Design (v1.0 as-is):
    - ETL ログは lib.etl（etl_runs / etl_errors）に一元化し、run の開始は先に commit する
    - 取込中の行エラー（NormalizeError 等）は etl_errors に記録し、処理は継続（行スキップ）
    - dry-run の場合は staging への INSERT を実行せず、最後に rollback（実質 no-op。run/err は残る）
    - 対象フォルダは `PHR_ROOT/input/subscribers_hub/active/<8桁保険者番号>/` をデフォルトとする
    - 進捗ログは ProgressLogger（RunMetrics参照専用）を利用（rows_seen が真実）

V1.0 Freeze (Scope / Contract):
    - Scope: Hub CSV → `staging_subscribers_hub` まで（本ファイル名は apply だが v1.0 現状は import 相当。`subscribers` 更新は対象外）
    - Inputs: `PHR_ROOT/input/subscribers_hub/active/<8桁保険者番号>/` 配下の *.csv（8桁フォルダは自動列挙）
    - Outputs:
        - `staging_subscribers_hub`（dry-run の場合は INSERT しない）
        - `etl_runs` / `etl_errors`（start_run 直後に commit するため、dry-run / 失敗でも証跡は残る）
    - DB I/O (dev_phr):
        - READS: なし（参照テーブル無し。正規化はローカル処理）
        - WRITES:
            - `staging_subscribers_hub`（主成果物。dry-run 時は INSERT しない）
            - `etl_runs` / `etl_errors`（start_run 直後 commit のため dry-run/失敗でも残る）
    - DB Actions (Fact):
        - start_run: `etl_runs` に INSERT → 直後に `conn.commit()`（dry-run/失敗でも run_id の証跡を残す）
        - per-row error:
            - 正規化エラー: `log_normalize_error` → `etl_errors` に INSERT（行スキップで継続）
            - 例外: `log_error` → `etl_errors` に INSERT（行スキップで継続）
        - staging insert: `staging_subscribers_hub` に明示カラム指定 INSERT（dry-run 時は実行しない）
            - columns:
                - person_id_custom
                - name_kana_full
                - name_kanji_full
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
    - Idempotency (v1.0 現状):
        - 本スクリプト単体では staging の重複排除/UPSERT は行わない（再投入制御は下流設計に委譲）
        - `src_file/src_row_no/src_line_no/import_run_id` は証跡として保持し、後段での突合・検証に使用する
    - Non-goals (v1.0 対象外):
        - staging から subscribers への確定反映（apply）、喪失/異動の確定反映、名寄せ精度の改善

Notes:
    - staging_subscribers_hub の birth / qualification_* は DATE 型
      → ここでは ISO 'YYYY-MM-DD' を渡す（MySQL が DATE に変換する）
============================================================
"""

from __future__ import annotations

import sys
import argparse
import csv
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Any

#
# sys.path 調整
# - このスクリプトは work_folder 配下に置くが、import は `phr.*` を使う
# - そのため WORK_ROOT（= scripts/work_folder）を sys.path に追加して解決する
#
WORK_ROOT = Path(__file__).resolve().parents[2]
if str(WORK_ROOT) not in sys.path:
    sys.path.insert(0, str(WORK_ROOT))

from phr.config.settings import PHR_ROOT  # type: ignore[import]
from phr.lib.config_db import load_mysql_params  # type: ignore[import]
from phr.lib.db_mysql import connect_ctx, dict_cursor, MySQLParams  # type: ignore[import]

from phr.lib.etl import (  # type: ignore[import]
    RunMetrics,
    ProgressLogger,
    start_run,
    finish_run,
    log_error,
    log_normalize_error,
)
from phr.lib.errors import NormalizeError  # type: ignore[import]

# ★ normalize の統合先
from phr.lib.normalize import common as ntypes  # type: ignore[import]
from phr.lib.normalize import subscriber as nsub  # type: ignore[import]


# ============================================================
# 基本設定
# ============================================================

JOB_NAME = "subscribers_hub"
DEFAULT_INPUT_BASE = PHR_ROOT / "input" / JOB_NAME / "active"

MAP: Dict[str, str] = {
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


@dataclass
class FolderMetrics:
    """1 保険者フォルダ分の表示用集計（RunMetricsとは別、表示目的のみ）"""
    files: int = 0
    rows_seen: int = 0
    rows_inserted: int = 0
    rows_skipped: int = 0
    errors: int = 0


# ============================================================
# 対象フォルダ列挙
# ============================================================

def list_target_dirs(base_dir: Path, single_dir: Optional[str]) -> List[Path]:
    if single_dir:
        p = Path(single_dir)
        if not p.is_dir():
            raise NotADirectoryError(f"--input がディレクトリではありません: {p}")
        return [p]

    if not base_dir.exists():
        raise FileNotFoundError(f"ベースフォルダが見つかりません: {base_dir}")

    dirs = [
        d for d in base_dir.iterdir()
        if d.is_dir() and d.name.isdigit() and len(d.name) == 8
    ]
    if not dirs:
        raise RuntimeError(f"8桁フォルダが見つかりません: {base_dir}")

    return sorted(dirs)


def count_csv_data_rows(csv_path: Path) -> int:
    """
    CSVの「データ行数」を数える（ヘッダ除外）。
    進捗の分母用。速度優先でざっくりでOK（進捗の分母用）。
    """
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        try:
            next(f)  # header skip
        except StopIteration:
            return 0
        return sum(1 for _ in f)


def estimate_total_rows(active_pairs: list[tuple[Path, int]], limit: int) -> int:
    total = 0
    for folder, _ins in active_pairs:
        for csv_path in sorted(folder.glob("*.csv")):
            total += count_csv_data_rows(csv_path)
            if limit and total >= limit:
                return limit
    return total


# ============================================================
# CSV フォルダ処理（1 保険者分）
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
    csv_files = sorted(folder.glob("*.csv"))

    if not csv_files:
        print(f"[WARN] CSV が見つかりません: {folder}")
        return m

    for csv_path in csv_files:
        m.files += 1
        metrics_all.files += 1

        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            rdr = csv.DictReader(f)
            line_no = 1
            csv_row_no = 0

            for row in rdr:
                line_no += 1
                csv_row_no += 1

                # 進捗の根っこ：rows_seen は RunMetrics が真実（FolderMetrics は表示用）
                m.rows_seen += 1
                metrics_all.rows_seen += 1

                try:
                    # --- 1) ヘッダ名 → 内部キーへマッピング（未知キーはそのまま残す） ---
                    src = {MAP.get(k, k): (row.get(k, "") or "") for k in row.keys()}

                    # --- 2) 必須キーの正規化（insurance_number / birth / gender / symbol） ---
                    try:
                        insurance_number_text = ntypes.normalize_insurance_number_required(
                            src.get("insurance_number", ""),
                            field="insurance_number",
                            src=csv_path.name,
                            line_no=line_no,
                        )
                        branchnum_text = ntypes.normalize_branchnumber_optional(
                            src.get("insurance_branchnumber", "")
                        )

                        # birth: YYYYMMDD → ISO(YYYY-MM-DD) にして DATE に入れる
                        birth_yyyymmdd = ntypes.normalize_birth_yyyymmdd(
                            src.get("birth", "") or row.get("生年月日", ""),
                            src=csv_path.name,
                            line_no=line_no,
                        )
                        birth_iso = ntypes.yyyymmdd_to_iso_date(
                            birth_yyyymmdd,
                            field="birth",
                            src=csv_path.name,
                            line_no=line_no,
                        )

                        gender_code = ntypes.normalize_gender_code(
                            src.get("gender_code", "") or row.get("性別", "")
                        )
                        insurance_symbol_norm, sym_digits = ntypes.normalize_insurance_symbol(
                            src.get("insurance_symbol", "")
                        )
                    except NormalizeError as ne:
                        m.rows_skipped += 1
                        m.errors += 1
                        metrics_all.rows_skipped += 1
                        metrics_all.errors += 1
                        log_normalize_error(
                            cur,
                            run_id,
                            phase="import",
                            source="import_subscribers_to_staging_hub",
                            insurer_number=f"{insurer_number:08d}",
                            src_file=csv_path.name,
                            row_no=csv_row_no,
                            line_no=line_no,
                            err=ne,
                        )
                        plog.tick()
                        continue

                    # --- 3) 氏名の正規化（カナ必須。分割結果は staging へ格納） ---
                    kanji_full_raw = (src.get("name_kanji_full", "") or row.get("対象者氏名（漢字）", "")).strip()
                    kana_full_raw = (src.get("name_kana_full", "") or row.get("対象者氏名（カナ）", "")).strip()

                    if not kana_full_raw:
                        raise NormalizeError(
                            field="name_kana_full",
                            code="required",
                            raw_value="",
                            message=f"必須フィールド欠損: name_kana_full file={csv_path.name} line={line_no}",
                        )

                    try:
                        name_parts = nsub.normalize_name_fields(
                            kanji_full=kanji_full_raw,
                            kana_full=kana_full_raw,
                        )
                    except NormalizeError as ne:
                        m.rows_skipped += 1
                        m.errors += 1
                        metrics_all.rows_skipped += 1
                        metrics_all.errors += 1
                        log_normalize_error(
                            cur,
                            run_id,
                            phase="import",
                            source="import_subscribers_to_staging_hub",
                            insurer_number=f"{insurer_number:08d}",
                            src_file=csv_path.name,
                            row_no=csv_row_no,
                            line_no=line_no,
                            err=ne,
                        )
                        plog.tick()
                        continue

                    # --- 4) person_id_custom 生成（insurer+symbol+number+birth 由来の固定キー） ---
                    try:
                        person_id_custom = nsub.generate_person_id_custom(
                            insurer_number=insurer_number,
                            insurance_symbol=insurance_symbol_norm,
                            insurance_number=insurance_number_text,
                            birth_yyyymmdd=birth_yyyymmdd,
                        )
                    except NormalizeError as ne:
                        m.rows_skipped += 1
                        m.errors += 1
                        metrics_all.rows_skipped += 1
                        metrics_all.errors += 1
                        log_normalize_error(
                            cur,
                            run_id,
                            phase="import",
                            source="import_subscribers_to_staging_hub",
                            insurer_number=f"{insurer_number:08d}",
                            src_file=csv_path.name,
                            row_no=csv_row_no,
                            line_no=line_no,
                            err=ne,
                        )
                        plog.tick()
                        continue

                    # --- 5) 日付 → ISO（空は空のまま。validate は normalize_date_iso 側に委譲） ---
                    qualification_acquired_date_iso = ntypes.normalize_date_iso(
                        src.get("qualification_acquired_date", ""),
                        field="qualification_acquired_date",
                        src=csv_path.name,
                        line_no=line_no,
                    )
                    qualification_lost_date_iso = ntypes.normalize_date_iso(
                        src.get("qualification_lost_date", ""),
                        field="qualification_lost_date",
                        src=csv_path.name,
                        line_no=line_no,
                    )

                    # --- 6) INSERT 用 dict（src_* は証跡。import_run_id は etl_runs と紐付け） ---
                    vals: Dict[str, Any] = {
                        "person_id_custom": person_id_custom,
                        "name_kana_full": name_parts["name_kana_full"],
                        "name_kanji_full": kanji_full_raw,
                        "name_kanji_family": name_parts["name_kanji_family"],
                        "name_kanji_middle": name_parts["name_kanji_middle"],
                        "name_kanji_given": name_parts["name_kanji_given"],
                        "name_kana_family": name_parts["name_kana_family"],
                        "name_kana_middle": name_parts["name_kana_middle"],
                        "name_kana_given": name_parts["name_kana_given"],
                        "gender_code": gender_code,
                        "birth": birth_iso,  # DATE列
                        "insured_attribute_name": src.get("insured_attribute_name", ""),
                        "relationship_name": src.get("relationship_name", ""),
                        "insurer_number": f"{insurer_number:08d}",
                        "insurance_symbol": insurance_symbol_norm,
                        "insurance_symbol_digits": sym_digits,
                        "insurance_number": insurance_number_text,
                        "insurance_branchnumber": branchnum_text,
                        "qualification_acquired_date": qualification_acquired_date_iso,
                        "qualification_lost_date": qualification_lost_date_iso,
                        "postal_code": src.get("postal_code", ""),
                        "address_line": src.get("address_line", ""),
                        "building": src.get("building", ""),
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

                    # --- 7) INSERT（dry-run の場合は実行しない） ---
                    if not dry_run:
                        cur.execute(
                            """
                            INSERT INTO staging_subscribers_hub (
                                person_id_custom,
                                name_kana_full, name_kanji_full,
                                name_kanji_family, name_kanji_middle, name_kanji_given,
                                name_kana_family, name_kana_middle, name_kana_given,
                                gender_code, birth,
                                insured_attribute_name, relationship_name,
                                insurer_number, insurance_symbol, insurance_symbol_digits,
                                insurance_number, insurance_branchnumber,
                                qualification_acquired_date, qualification_lost_date,
                                postal_code, address_line, building,
                                phone, email,
                                employer_code, department_code, distribution_code,
                                employee_code, connect_id,
                                created_at, loaded_at, processed_at,
                                src_file, src_row_no, src_line_no, import_run_id
                            )
                            VALUES (
                                %(person_id_custom)s,
                                %(name_kana_full)s, %(name_kanji_full)s,
                                %(name_kanji_family)s, %(name_kanji_middle)s, %(name_kanji_given)s,
                                %(name_kana_family)s, %(name_kana_middle)s, %(name_kana_given)s,
                                %(gender_code)s, %(birth)s,
                                %(insured_attribute_name)s, %(relationship_name)s,
                                %(insurer_number)s, %(insurance_symbol)s, %(insurance_symbol_digits)s,
                                %(insurance_number)s, %(insurance_branchnumber)s,
                                %(qualification_acquired_date)s, %(qualification_lost_date)s,
                                %(postal_code)s, %(address_line)s, %(building)s,
                                %(phone)s, %(email)s,
                                %(employer_code)s, %(department_code)s, %(distribution_code)s,
                                %(employee_code)s, %(connect_id)s,
                                NOW(3), NOW(3), NULL,
                                %(src_file)s, %(src_row_no)s, %(src_line_no)s, %(import_run_id)s
                            )
                            """,
                            vals,
                        )

                    m.rows_inserted += 1
                    metrics_all.rows_inserted += 1

                except NormalizeError as ne:
                    m.rows_skipped += 1
                    m.errors += 1
                    metrics_all.rows_skipped += 1
                    metrics_all.errors += 1
                    log_normalize_error(
                        cur,
                        run_id,
                        phase="import",
                        source="import_subscribers_to_staging_hub",
                        insurer_number=f"{insurer_number:08d}",
                        src_file=csv_path.name,
                        row_no=csv_row_no,
                        line_no=line_no,
                        err=ne,
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

                # 進捗表示（RunMetrics参照専用）
                plog.tick()

                if limit and (metrics_all.rows_inserted + metrics_all.rows_skipped) >= limit:
                    break

        if limit and (metrics_all.rows_inserted + metrics_all.rows_skipped) >= limit:
            break

    return m


# ============================================================
# main
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
    raw_target_dirs = list_target_dirs(base_dir, args.input)

    active_pairs: list[tuple[Path, int]] = []
    for d in raw_target_dirs:
        has_csv = any(d.glob("*.csv"))
        if not has_csv:
            print(f"[INFO] skip folder (no CSV): {d.name}")
            continue
        insurer_num = ntypes.normalize_insurer_folder_name_to_int(d)
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

    params: MySQLParams = load_mysql_params()
    if args.schema:
        params.database = args.schema

    db_path_str = f"{params.host}:{params.port}/{params.database}"

    print(f"[INFO] BASE      = {base_dir}")
    print(f"[INFO] TARGETS   = {[d.name for d in target_dirs]}")
    print(f"[INFO] DB_SCHEMA = {params.database}")
    print(f"[INFO] DRY_RUN   = {args.dry_run}")
    print(f"[INFO] LIMIT     = {args.limit}")
    print(f"[INFO] PROGRESS  = {args.progress_interval}")
    print(f"[INFO] INSURERS  = {insurers_summary}")

    try:
        with connect_ctx(params) as conn:
            cur = dict_cursor(conn)

            metrics_all = RunMetrics()

            total_rows = estimate_total_rows(active_pairs, args.limit)
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
                db_schema=params.database,
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
                        remaining = args.limit - (metrics_all.rows_inserted + metrics_all.rows_skipped)
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

                    if args.limit and (metrics_all.rows_inserted + metrics_all.rows_skipped) >= args.limit:
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
                else:
                    conn.commit()

                print(
                    f"[DONE] run_id={run_id} total_files={metrics_all.files} "
                    f"rows={metrics_all.rows_seen} inserted={metrics_all.rows_inserted} "
                    f"skipped={metrics_all.rows_skipped} errors={metrics_all.errors}"
                )

            except Exception as e:
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

# -*- coding: utf-8 -*-
r"""
============================================================
Script : apply_subscribers_from_staging_hub.py
Path   : scripts/work_folder/scripts/apply_subscribers_from_staging_hub.py
Project: PHR / work_folder/phr

Purpose (PHR v1.0.1):
    - `staging_subscribers_hub` の未処理行を `subscribers` に反映する（apply 相当）。
    - 既存 subscriber は `person_id_custom` を主キー的に扱って照合する。
    - v1.0.1 追加の identity match columns を生成して `subscribers` に保存する。

Design:
    - ETL ログは lib.etl（etl_runs / etl_errors）に一元化する
    - 取込対象は `processed_run_id IS NULL` の staging 行のみ
    - 1行ごとに insert / update / noop を判定する
    - dry-run の場合は subscribers 更新を rollback する（run / error は残る）
    - match columns は apply 時に生成する

V1.0.1 Scope:
    - READS:
        - `staging_subscribers_hub`
        - `subscribers`
    - WRITES:
        - `subscribers`
        - `staging_subscribers_hub.processed_run_id / processed_at`
        - `etl_runs` / `etl_errors`
============================================================
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from scripts.work_folder.lib.config_db import load_mysql_params  # type: ignore[import]
from scripts.work_folder.lib.db_mysql import connect_ctx, dict_cursor, MySQLParams  # type: ignore[import]
from scripts.work_folder.lib.etl import (  # type: ignore[import]
    RunMetrics,
    start_run,
    finish_run,
    log_error,
)
from scripts.work_folder.lib.normalize.common import (  # type: ignore[import]
    normalize_insurance_number_match,
    normalize_insurance_symbol_match,
)


JOB_NAME = "apply_subscribers_from_staging_hub"


# ============================================================
# match fields
# ============================================================

def normalize_name_full_match(value: str) -> str:
    """漢字氏名 match 用: 半角/全角スペース除去。"""
    return (value or "").replace(" ", "").replace("　", "").strip()



def normalize_name_kana_full_match(value: str) -> str:
    """カナ氏名 match 用: 半角/全角スペース + 中点除去。"""
    return (
        (value or "")
        .replace(" ", "")
        .replace("　", "")
        .replace("・", "")
        .replace("･", "")
        .strip()
    )


# ============================================================
# subscriber row helpers
# ============================================================

def build_subscriber_vals(srow: dict[str, Any]) -> Dict[str, Any]:
    """staging 1行から subscribers 反映用の値 dict を作る。"""

    insurance_symbol = srow.get("insurance_symbol") or ""
    insurance_number = srow.get("insurance_number") or ""
    name_kanji_full = srow.get("name_kanji_full") or ""
    name_kana_full = srow.get("name_kana_full") or ""

    return {
        "person_id_custom": srow.get("person_id_custom"),
        "name_kana_full": name_kana_full,
        "name_kanji_full": name_kanji_full,
        "name_kanji_family": srow.get("name_kanji_family"),
        "name_kanji_middle": srow.get("name_kanji_middle"),
        "name_kanji_given": srow.get("name_kanji_given"),
        "name_kana_family": srow.get("name_kana_family"),
        "name_kana_middle": srow.get("name_kana_middle"),
        "name_kana_given": srow.get("name_kana_given"),
        "name_kana_full_match": normalize_name_kana_full_match(name_kana_full),
        "name_full_match": normalize_name_full_match(name_kanji_full),
        "gender_code": srow.get("gender_code"),
        "birth": srow.get("birth"),
        "insured_attribute_name": srow.get("insured_attribute_name"),
        "relationship_name": srow.get("relationship_name"),
        "insurer_number": srow.get("insurer_number"),
        "insurance_symbol": insurance_symbol,
        "insurance_symbol_digits": srow.get("insurance_symbol_digits"),
        "insurance_symbol_match": normalize_insurance_symbol_match(insurance_symbol),
        "insurance_number": insurance_number,
        "insurance_number_match": normalize_insurance_number_match(insurance_number),
        "insurance_branchnumber": srow.get("insurance_branchnumber"),
        "qualification_acquired_date": srow.get("qualification_acquired_date"),
        "qualification_lost_date": srow.get("qualification_lost_date"),
        "postal_code": srow.get("postal_code"),
        "address_line": srow.get("address_line"),
        "building": srow.get("building"),
        "phone": srow.get("phone"),
        "email": srow.get("email"),
        "employer_code": srow.get("employer_code"),
        "department_code": srow.get("department_code"),
        "distribution_code": srow.get("distribution_code"),
        "employee_code": srow.get("employee_code"),
        "connect_id": srow.get("connect_id"),
    }



def fetch_existing_subscriber(cur, person_id_custom: str) -> Optional[dict[str, Any]]:
    sql = """
        SELECT
            id,
            person_id_custom,
            name_kana_full,
            name_kanji_full,
            name_kanji_family,
            name_kanji_middle,
            name_kanji_given,
            name_kana_family,
            name_kana_middle,
            name_kana_given,
            name_kana_full_match,
            name_full_match,
            gender_code,
            birth,
            insured_attribute_name,
            relationship_name,
            insurer_number,
            insurance_symbol,
            insurance_symbol_digits,
            insurance_symbol_match,
            insurance_number,
            insurance_number_match,
            insurance_branchnumber,
            qualification_acquired_date,
            qualification_lost_date,
            postal_code,
            address_line,
            building,
            phone,
            email,
            employer_code,
            department_code,
            distribution_code,
            employee_code,
            connect_id
        FROM subscribers
        WHERE person_id_custom = %s
        LIMIT 1
    """
    cur.execute(sql, (person_id_custom,))
    return cur.fetchone()


COMPARE_COLUMNS = [
    "name_kana_full",
    "name_kanji_full",
    "name_kanji_family",
    "name_kanji_middle",
    "name_kanji_given",
    "name_kana_family",
    "name_kana_middle",
    "name_kana_given",
    "name_kana_full_match",
    "name_full_match",
    "gender_code",
    "birth",
    "insured_attribute_name",
    "relationship_name",
    "insurer_number",
    "insurance_symbol",
    "insurance_symbol_digits",
    "insurance_symbol_match",
    "insurance_number",
    "insurance_number_match",
    "insurance_branchnumber",
    "qualification_acquired_date",
    "qualification_lost_date",
    "postal_code",
    "address_line",
    "building",
    "phone",
    "email",
    "employer_code",
    "department_code",
    "distribution_code",
    "employee_code",
    "connect_id",
]



def subscriber_differs(existing: dict[str, Any], vals: Dict[str, Any]) -> bool:
    for col in COMPARE_COLUMNS:
        if existing.get(col) != vals.get(col):
            return True
    return False



def insert_subscriber(cur, vals: Dict[str, Any], run_id: int) -> int:
    sql = """
        INSERT INTO subscribers (
            person_id_custom,
            name_kana_full,
            name_kanji_full,
            name_kanji_family,
            name_kanji_middle,
            name_kanji_given,
            name_kana_family,
            name_kana_middle,
            name_kana_given,
            name_kana_full_match,
            name_full_match,
            gender_code,
            birth,
            insured_attribute_name,
            relationship_name,
            insurer_number,
            insurance_symbol,
            insurance_symbol_digits,
            insurance_symbol_match,
            insurance_number,
            insurance_number_match,
            insurance_branchnumber,
            qualification_acquired_date,
            qualification_lost_date,
            postal_code,
            address_line,
            building,
            phone,
            email,
            employer_code,
            department_code,
            distribution_code,
            employee_code,
            connect_id,
            first_import_run_id,
            last_change_run_id,
            created_at,
            updated_at
        ) VALUES (
            %(person_id_custom)s,
            %(name_kana_full)s,
            %(name_kanji_full)s,
            %(name_kanji_family)s,
            %(name_kanji_middle)s,
            %(name_kanji_given)s,
            %(name_kana_family)s,
            %(name_kana_middle)s,
            %(name_kana_given)s,
            %(name_kana_full_match)s,
            %(name_full_match)s,
            %(gender_code)s,
            %(birth)s,
            %(insured_attribute_name)s,
            %(relationship_name)s,
            %(insurer_number)s,
            %(insurance_symbol)s,
            %(insurance_symbol_digits)s,
            %(insurance_symbol_match)s,
            %(insurance_number)s,
            %(insurance_number_match)s,
            %(insurance_branchnumber)s,
            %(qualification_acquired_date)s,
            %(qualification_lost_date)s,
            %(postal_code)s,
            %(address_line)s,
            %(building)s,
            %(phone)s,
            %(email)s,
            %(employer_code)s,
            %(department_code)s,
            %(distribution_code)s,
            %(employee_code)s,
            %(connect_id)s,
            %(first_import_run_id)s,
            %(last_change_run_id)s,
            NOW(3),
            NOW(3)
        )
    """
    params = dict(vals)
    params["first_import_run_id"] = run_id
    params["last_change_run_id"] = run_id
    cur.execute(sql, params)
    return int(cur.lastrowid)



def update_subscriber(cur, subscriber_id: int, vals: Dict[str, Any], run_id: int) -> None:
    sql = """
        UPDATE subscribers
        SET
            name_kana_full = %(name_kana_full)s,
            name_kanji_full = %(name_kanji_full)s,
            name_kanji_family = %(name_kanji_family)s,
            name_kanji_middle = %(name_kanji_middle)s,
            name_kanji_given = %(name_kanji_given)s,
            name_kana_family = %(name_kana_family)s,
            name_kana_middle = %(name_kana_middle)s,
            name_kana_given = %(name_kana_given)s,
            name_kana_full_match = %(name_kana_full_match)s,
            name_full_match = %(name_full_match)s,
            gender_code = %(gender_code)s,
            birth = %(birth)s,
            insured_attribute_name = %(insured_attribute_name)s,
            relationship_name = %(relationship_name)s,
            insurer_number = %(insurer_number)s,
            insurance_symbol = %(insurance_symbol)s,
            insurance_symbol_digits = %(insurance_symbol_digits)s,
            insurance_symbol_match = %(insurance_symbol_match)s,
            insurance_number = %(insurance_number)s,
            insurance_number_match = %(insurance_number_match)s,
            insurance_branchnumber = %(insurance_branchnumber)s,
            qualification_acquired_date = %(qualification_acquired_date)s,
            qualification_lost_date = %(qualification_lost_date)s,
            postal_code = %(postal_code)s,
            address_line = %(address_line)s,
            building = %(building)s,
            phone = %(phone)s,
            email = %(email)s,
            employer_code = %(employer_code)s,
            department_code = %(department_code)s,
            distribution_code = %(distribution_code)s,
            employee_code = %(employee_code)s,
            connect_id = %(connect_id)s,
            last_change_run_id = %(last_change_run_id)s,
            updated_at = NOW(3)
        WHERE id = %(id)s
    """
    params = dict(vals)
    params["last_change_run_id"] = run_id
    params["id"] = subscriber_id
    cur.execute(sql, params)



def mark_staging_processed(cur, stg_id: int, run_id: int) -> None:
    sql = """
        UPDATE staging_subscribers_hub
        SET processed_run_id = %s,
            processed_at = NOW(3)
        WHERE id = %s
    """
    cur.execute(sql, (run_id, stg_id))



def fetch_pending_staging_rows(cur, limit: int) -> list[dict[str, Any]]:
    sql = """
        SELECT
            id,
            person_id_custom,
            name_kana_full,
            name_kanji_full,
            name_kanji_family,
            name_kanji_middle,
            name_kanji_given,
            name_kana_family,
            name_kana_middle,
            name_kana_given,
            gender_code,
            birth,
            insured_attribute_name,
            relationship_name,
            insurer_number,
            insurance_symbol,
            insurance_symbol_digits,
            insurance_number,
            insurance_branchnumber,
            qualification_acquired_date,
            qualification_lost_date,
            postal_code,
            address_line,
            building,
            phone,
            email,
            employer_code,
            department_code,
            distribution_code,
            employee_code,
            connect_id,
            src_file,
            src_row_no,
            src_line_no,
            import_run_id,
            processed_run_id
        FROM staging_subscribers_hub
        WHERE processed_run_id IS NULL
        ORDER BY id ASC
    """
    if limit > 0:
        sql += " LIMIT %s"
        cur.execute(sql, (limit,))
    else:
        cur.execute(sql)
    return list(cur.fetchall())


# ============================================================
# apply core
# ============================================================

def apply_once(cur, srow: dict[str, Any], run_id: int) -> str:
    """return: insert | update | noop"""
    vals = build_subscriber_vals(srow)

    existing = fetch_existing_subscriber(cur, vals["person_id_custom"])

    if existing is None:
        insert_subscriber(cur, vals, run_id)
        return "insert"

    if subscriber_differs(existing, vals):
        update_subscriber(cur, int(existing["id"]), vals, run_id)
        return "update"

    return "noop"


# ============================================================
# main
# ============================================================

def main() -> int:
    ap = argparse.ArgumentParser(description="Apply staging_subscribers_hub rows into subscribers")
    ap.add_argument("--schema", default=None, help="接続先 DB スキーマ名")
    ap.add_argument("--limit", type=int, default=0, help="処理する staging 行数の上限 (0 = 無制限)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    params: MySQLParams = load_mysql_params()
    if args.schema:
        params.database = args.schema

    db_path_str = f"{params.host}:{params.port}/{params.database}"

    print(f"[INFO] DB_SCHEMA = {params.database}")
    print(f"[INFO] DRY_RUN   = {args.dry_run}")
    print(f"[INFO] LIMIT     = {args.limit}")

    try:
        with connect_ctx(params) as conn:
            cur = dict_cursor(conn)
            metrics = RunMetrics()

            run_id = start_run(
                cur,
                phase="apply",
                source=JOB_NAME,
                db_schema=params.database,
                db_path=db_path_str,
                input_base="staging_subscribers_hub",
                input_file=None,
                insurer_number=None,
                dry_run=args.dry_run,
                limit_rows=args.limit,
            )
            conn.commit()
            print(f"[INFO] run_id = {run_id}")

            try:
                rows = fetch_pending_staging_rows(cur, args.limit)
                print(f"[INFO] staging rows to apply = {len(rows)}")

                for srow in rows:
                    metrics.rows_seen += 1
                    try:
                        op = apply_once(cur, srow, run_id)

                        if op == "insert":
                            metrics.rows_inserted += 1
                        elif op == "update":
                            metrics.rows_updated += 1
                        else:
                            metrics.rows_unchanged += 1

                        if not args.dry_run:
                            mark_staging_processed(cur, int(srow["id"]), run_id)

                    except Exception as e:
                        metrics.errors += 1
                        log_error(
                            cur,
                            run_id,
                            phase="apply",
                            source=JOB_NAME,
                            insurer_number=(srow.get("insurer_number") or None),
                            src_file=(srow.get("src_file") or None),
                            row_no=srow.get("src_row_no"),
                            line_no=srow.get("src_line_no"),
                            field=None,
                            field_value=None,
                            error_code=type(e).__name__,
                            message=str(e),
                        )
                        continue

                finish_run(
                    cur,
                    run_id,
                    metrics,
                    extra_notes="apply from staging_subscribers_hub",
                )
                if args.dry_run:
                    conn.rollback()
                    print(
                        f"[DRY-RUN] inserted={metrics.rows_inserted} "
                        f"updated={metrics.rows_updated} unchanged={metrics.rows_unchanged} "
                        f"errors={metrics.errors} run_id={run_id}"
                    )
                else:
                    conn.commit()
                    print(
                        f"[OK] inserted={metrics.rows_inserted} "
                        f"updated={metrics.rows_updated} unchanged={metrics.rows_unchanged} "
                        f"errors={metrics.errors} run_id={run_id}"
                    )

            except Exception as e:
                conn.rollback()
                metrics.errors += 1
                finish_run(
                    cur,
                    run_id,
                    metrics,
                    status_override="failed",
                    extra_notes=f"error={e}",
                )
                conn.commit()
                print(f"[ERR] apply 中に例外発生: {e}")
                return 7

    except Exception as e:
        print(f"[FATAL] DB 接続または実行時エラー: {e}")
        return 7

    return 0


if __name__ == "__main__":
    raise SystemExit(main())