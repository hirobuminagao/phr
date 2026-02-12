# -*- coding: utf-8 -*-
r"""
============================================================
Script : import_subscribers_to_staging_hub.py
Path   : scripts/work_folder/scripts/import_subscribers_to_staging_hub.py
Project: PHR / work_folder/phr

Purpose:
    - Hub 由来の「加入者（subscribers）」CSV を
      MySQL の staging_subscribers_hub に取り込む。

Design:
    - ETL ログは lib.etl（etl_runs / etl_errors）に一元化し、run の開始は先に commit する
    - 本体取込の成否は finish_run で確定し、成功時は staging への INSERT も含めて commit
    - 取込中の行エラー（NormalizeError 等）は etl_errors に記録し、処理は継続（行スキップ）
    - dry-run の場合は staging への INSERT を実行せず、最後に rollback（実質 no-op。run/err は残る）
    - 対象フォルダは `PHR_ROOT/input/subscribers_hub/active/<8桁保険者番号>/` をデフォルトとする
    - 進捗ログは ProgressLogger（RunMetrics参照専用）を利用（rows_seen が真実）

V1.0 Freeze (Scope / Contract):
    - Scope: Hub CSV → `staging_subscribers_hub` まで（`subscribers` 本表への反映は本スクリプトの対象外）
    - Inputs: `PHR_ROOT/input/subscribers_hub/active/<8桁保険者番号>/` 配下の *.csv（8桁フォルダは自動列挙）
    - Outputs:
        - `staging_subscribers_hub`（dry-run の場合は INSERT しない）
        - `etl_runs` / `etl_errors`（start_run 直後に commit するため、dry-run / 失敗でも証跡は残る）
    - Idempotency (v1.0 現状):
        - 本スクリプト単体では staging の重複排除/UPSERT は行わない（同一 person_id_custom の再投入制御は下流設計に委譲）
        - `src_file/src_row_no/src_line_no/import_run_id` は証跡として保持し、後段での突合・検証に使用する
    - Non-goals (v1.0 対象外):
        - fund 差分ロジック、喪失/異動の確定反映、名寄せ精度の改善、正本（subscribers）更新
============================================================
"""

from __future__ import annotations

import sys
import argparse
import csv
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

#
# sys.path 調整
# - このスクリプトは work_folder 配下に置くが、import は `phr.*` を使う
# - そのため WORK_ROOT（= scripts/work_folder）を sys.path に追加して解決する
# ------------------------------------------------------------
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

from phr.lib import normalize_common_types as ntypes  # type: ignore[import]
from phr.lib import normalize_subscriber_fields as nsub  # type: ignore[import]


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
        # 1行目ヘッダを読み飛ばし
        try:
            next(f)
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
                        birth = ntypes.normalize_birth_yyyymmdd(
                            src.get("birth", "") or row.get("生年月日", ""),
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
                    kanji_full_raw = (src.get("name_kanji_full", "") or
                                      row.get("対象者氏名（漢字）", "")).strip()
                    kana_full_raw = (src.get("name_kana_full", "") or
                                     row.get("対象者氏名（カナ）", "")).strip()

                    if not kana_full_raw:
                        raise NormalizeError(
                            field="name_kana_full",
                            code="required",
                            raw_value="",
                            message=(
                                f"必須フィールド欠損: name_kana_full "
                                f"file={csv_path.name} line={line_no}"
                            ),
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
                            birth_yyyymmdd=birth,
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
                    vals = {
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
                        "birth": birth,
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
                                NOW(), NOW(), NULL,
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
# 実行仕様（固定化ポイント）
# - 対象フォルダは 8桁ディレクトリを自動列挙。--input 指定時はそのディレクトリのみ
# - run_id は start_run 後に即 commit（進捗/失敗の証跡を残す）
# - 取込中の行エラーは etl_errors に記録し、行スキップで継続
# - 最終的な run の状態は finish_run で確定する
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
