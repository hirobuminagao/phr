# -*- coding: utf-8 -*-
r"""
============================================================
Script : import_subscribers_to_staging_fund.py
Path   : work_folder/phr/scripts/import_subscribers_to_staging_fund.py
Project: PHR / work_folder/phr
============================================================

目的:
- 健保CSV（fund）を staging_subscribers_fund に取り込む（受領履歴の入口）

fund側ルール:
- 成功した src_file は同名で再投入禁止（重複NG）
- エラーだった src_file は同名で再投入OK
- 1件でもエラーがあれば staging への INSERT は全件 rollback
  （etl_errors は rollback されない）

入力フォルダ（今回の前提）:
- <WORK_ROOT>/phr/input/subscribers_fund/active/<insurer(8桁)>/*.csv

デフォ動作（VSCode Run想定）:
- --insurer 未指定なら active 配下の全保険者番号フォルダを走査（CSVがあるものだけ）
- --input 未指定なら env/デフォルトパスを使う

実装:
- conn_log : etl_runs / etl_errors（必ず commit）
- conn_data: staging_subscribers_fund（エラー時 rollback）
"""

from __future__ import annotations

import argparse
import csv
import inspect
import logging
import os
import sys
from dataclasses import dataclass, is_dataclass, replace
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from dotenv import load_dotenv

print("PHR_MYSQL_USER=", os.getenv("PHR_MYSQL_USER"))

# ------------------------------------------------------------
# project root
# ------------------------------------------------------------
WORK_ROOT = Path(__file__).resolve().parents[2]  # .../work_folder

env_path = WORK_ROOT / ".env"
print("WORK_ROOT =", WORK_ROOT)
print(".env path =", env_path)
print(".env exists =", env_path.exists())

# ★ Runボタンでも `import phr` が通るようにする（最重要）
if str(WORK_ROOT) not in sys.path:
    sys.path.insert(0, str(WORK_ROOT))

# ------------------------------------------------------------
# imports (lib)
# ------------------------------------------------------------
from phr.lib.db import MySQLParams, load_mysql_params, connect_ctx, dict_cursor
from phr.lib.etl import RunMetrics, start_run, finish_run, log_error
from phr.lib.errors import NormalizeError

from phr.lib.normalize.common import (
    normalize_date_iso,
    normalize_gender_code,
    normalize_birth_yyyymmdd,
    yyyymmdd_to_iso_date,
    normalize_insurance_symbol,
    normalize_insurance_number_required,
    normalize_branchnumber_optional,
    normalize_insurer_folder_name_to_int,
)
from phr.lib.normalize.subscriber import (
    normalize_name_fields,
    generate_person_id_custom,
)

# ============================================================
# env keys (work_folder 相対で扱う)
# ============================================================

ENV_INPUT_BASE = "FUND_SUBSCRIBERS_INPUT_BASE"   # 例: "phr/input/subscribers_fund"
ENV_ACTIVE_DIR = "FUND_SUBSCRIBERS_ACTIVE_DIR"   # 例: "active"
ENV_DEFAULT_SCHEMA = "MYSQL_SCHEMA"              # 例: "dev_phr"（load_mysql_params側と併用OK）

# （任意）成功/失敗でファイルを移動したいなら使えるようにしておく
ENV_MOVE_ON_SUCCESS = "FUND_SUBSCRIBERS_MOVE_ON_SUCCESS"  # "1" で past へ
ENV_MOVE_ON_FAIL    = "FUND_SUBSCRIBERS_MOVE_ON_FAIL"     # "1" で error へ
ENV_PAST_DIR         = "FUND_SUBSCRIBERS_PAST_DIR"         # 例: "past"
ENV_ERROR_DIR        = "FUND_SUBSCRIBERS_ERROR_DIR"        # 例: "error"

# ============================================================
# small utils
# ============================================================

def _env_bool(key: str, default: bool = False) -> bool:
    v = (os.getenv(key) or "").strip().lower()
    if v == "":
        return default
    return v not in {"0", "false", "no", "off"}


def resolve_input_base() -> Path:
    """
    work_folder 相対のパスを env で受ける。
    """
    base = os.getenv(ENV_INPUT_BASE) or "phr/input/subscribers_fund"
    p = Path(base)
    if not p.is_absolute():
        p = WORK_ROOT / p
    return p


def resolve_active_dir(base: Path) -> Path:
    active = os.getenv(ENV_ACTIVE_DIR) or "active"
    return base / active


def list_csvs_under(folder: Path) -> List[Path]:
    if folder.is_file() and folder.suffix.lower() == ".csv":
        return [folder]
    if folder.is_dir():
        return sorted([p for p in folder.glob("*.csv") if p.is_file()])
    return []


def detect_csv_encoding(fp: Path) -> str:
    candidates = ["utf-8-sig", "utf-8", "cp932"]
    head = fp.read_bytes()[:4096]
    for enc in candidates:
        try:
            head.decode(enc)
            return enc
        except UnicodeDecodeError:
            pass
    return "cp932"


def normalize_fieldnames(fn: Sequence[str]) -> List[str]:
    out: List[str] = []
    for h in fn:
        s = (h or "").strip()
        if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
            s = s[1:-1]
        s = s.replace("\u3000", " ").strip()
        out.append(s)
    return out


# ============================================================
# MySQLParams schema override (immutable / no with_database対応)
# ============================================================

def clone_mysql_params_with_schema(params: MySQLParams, schema: Optional[str]) -> MySQLParams:
    if not schema:
        return params

    # 1) with_database があるなら最優先
    if hasattr(params, "with_database") and callable(getattr(params, "with_database")):
        return getattr(params, "with_database")(schema)  # type: ignore

    # 2) dataclassなら replace を試す
    if is_dataclass(params):
        try:
            return replace(params, database=schema)  # type: ignore
        except Exception:
            pass

    # 3) 最後に「同型再構築」を試す（frozenでもOKになりやすい）
    if hasattr(params, "database"):
        try:
            data = dict(vars(params))
            data["database"] = schema
            return type(params)(**data)  # type: ignore
        except Exception:
            pass

    # ここまで来たら override 不可
    raise RuntimeError("MySQLParams の schema(database) を上書きできません（実装を確認して）")


# ============================================================
# ETL wrappers (引数揺れ吸収)
# ============================================================

def _call_with_supported(fn, **kwargs):
    sig = inspect.signature(fn)
    supported = {}
    for k, v in kwargs.items():
        if k in sig.parameters:
            supported[k] = v
    return fn(**supported)


def etl_start(cur_log, **kwargs) -> int:
    run_id = _call_with_supported(start_run, cur=cur_log, **kwargs)  # type: ignore
    if isinstance(run_id, int):
        return run_id
    return int(run_id)


def etl_error(cur_log, **kwargs) -> None:
    try:
        _call_with_supported(log_error, cur=cur_log, **kwargs)  # type: ignore
        return
    except TypeError:
        log_error(cur_log, kwargs.get("run_id"))  # type: ignore


def etl_finish(cur_log, **kwargs) -> None:
    _call_with_supported(finish_run, cur=cur_log, **kwargs)  # type: ignore


def metrics_set(m: RunMetrics, **vals) -> RunMetrics:
    for k, v in vals.items():
        if hasattr(m, k):
            setattr(m, k, v)
    return m


# ============================================================
# DB helpers
# ============================================================

def find_fund_id_by_insurer(cur, insurer_number_8: str) -> int:
    """
    insurer_number(8桁) から fund_id を解決する。

    - fund_insurer_numbers.fund_id -> funds.id
    - 期間（valid_from/valid_to）で現行を判定（is_current に依存しない）
    """
    cur.execute(
        """
        SELECT fin.fund_id AS fund_id
          FROM fund_insurer_numbers fin
          JOIN funds f ON f.id = fin.fund_id
         WHERE fin.insurer_number = %s
           AND fin.valid_from <= CURDATE()
           AND (fin.valid_to IS NULL OR fin.valid_to >= CURDATE())
         ORDER BY fin.valid_from DESC
         LIMIT 1
        """,
        (insurer_number_8,),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"fund not found for insurer={insurer_number_8}")
    return int(row["fund_id"])


def pick_template_version(cur, fund_id: int, explicit: Optional[int]) -> int:
    if explicit:
        return int(explicit)
    cur.execute("SELECT MAX(version) AS v FROM templates WHERE fund_id=%s", (fund_id,))
    row = cur.fetchone()
    if not row or row["v"] is None:
        raise RuntimeError(f"template not found (fund_id={fund_id})")
    return int(row["v"])


def get_template_mapping(cur, fund_id: int, version: int) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT col_order, csv_header, target_column, rule, required
          FROM template_mappings
         WHERE fund_id=%s AND version=%s
         ORDER BY col_order
        """,
        (fund_id, version),
    )
    rows = cur.fetchall()
    if not rows:
        raise RuntimeError(f"template_mappings empty (fund_id={fund_id}, version={version})")
    return rows


def detect_duplicate_success_files(
    cur,
    *,
    fund_id: int,
    template_ver: int,
    insurer_number: str,
    filenames: List[str],
) -> List[str]:
    """
    成功扱い=stagingに行が存在すること。
    失敗時は rollback されるので stagingに残らず、同名再投入OKになる。
    """
    if not filenames:
        return []
    ph = ", ".join(["%s"] * len(filenames))
    sql = f"""
        SELECT DISTINCT src_file
          FROM staging_subscribers_fund
         WHERE fund_id=%s
           AND template_ver=%s
           AND insurer_number=%s
           AND src_file IN ({ph})
    """
    cur.execute(sql, [fund_id, template_ver, insurer_number, *filenames])
    return sorted({r["src_file"] for r in cur.fetchall()})


# ============================================================
# mapping apply (テンプレルールを解釈して raw→vals を作る)
# ============================================================

def apply_template_mapping(
    *,
    csv_row: Dict[str, Any],
    mapping_defs: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    template_mappings を適用して “素のvals” を作る。
    ここでは、最低限の変換だけ（重いドメイン正規化は subscriber/common に任せる）。
    """
    def get_src(header: str) -> str:
        v = csv_row.get(header, "")
        if isinstance(v, str):
            s = v.strip()
            if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
                s = s[1:-1].strip()
            return s
        return str(v or "")

    vals: Dict[str, Any] = {}

    for m in mapping_defs:
        src = (m.get("csv_header") or "").strip()
        tgt = (m.get("target_column") or "").strip()
        rule = (m.get("rule") or "").strip()
        required = int(m.get("required") or 0)

        raw = get_src(src)

        if rule == "direct":
            vals[tgt] = raw if raw != "" else None
        elif rule.startswith("rename:"):
            alt = rule.split(":", 1)[1].strip()
            v2 = get_src(alt)
            vals[tgt] = v2 if v2 != "" else None
        elif rule.startswith("const:"):
            vals[tgt] = rule.split(":", 1)[1]
        else:
            vals[tgt] = raw if raw != "" else None

        if required and (vals.get(tgt) is None or str(vals.get(tgt) or "").strip() == ""):
            raise NormalizeError(
                field=tgt,
                code="required",
                raw_value=raw or "",
                message=f"必須項目が空です: {tgt} (src={src})",
            )

    return vals


# ============================================================
# one row normalize (common/subscriber に集約)
# ============================================================

def normalize_one_row(
    *,
    vals: Dict[str, Any],
    insurer_number_8: str,
    src_file: str,
    line_no: int,
) -> Dict[str, Any]:
    """
    staging insert 直前の1行を確定させる。
    - 証記号/保険証番号/枝番/生年月日/性別/氏名/person_id_custom/続柄/資格日付
    """
    out = dict(vals)

    out["insurer_number"] = insurer_number_8

    kana_full = (out.get("name_kana_full") or "").strip()
    kanji_full = (out.get("name_kanji_full") or "").strip()
    name_pack = normalize_name_fields(kanji_full=kanji_full, kana_full=kana_full)
    out.update(name_pack)

    out["gender_code"] = normalize_gender_code(str(out.get("gender_code") or ""))

    birth_yyyymmdd = normalize_birth_yyyymmdd(
        str(out.get("birth") or ""),
        src=src_file,
        line_no=line_no,
    )
    out["birth"] = yyyymmdd_to_iso_date(
        birth_yyyymmdd,
        field="birth",
        src=src_file,
        line_no=line_no,
    )

    sym_norm, sym_digits = normalize_insurance_symbol(str(out.get("insurance_symbol") or ""))
    out["insurance_symbol"] = sym_norm
    out["insurance_symbol_digits"] = sym_digits

    out["insurance_number"] = normalize_insurance_number_required(
        str(out.get("insurance_number") or ""),
        field="insurance_number",
        src=src_file,
        line_no=line_no,
    )

    out["insurance_branchnumber"] = normalize_branchnumber_optional(
        str(out.get("insurance_branchnumber") or "")
    )

    out["qualification_acquired_date"] = normalize_date_iso(
        out.get("qualification_acquired_date"),
        field="qualification_acquired_date",
        src=src_file,
        line_no=line_no,
    )
    out["qualification_lost_date"] = normalize_date_iso(
        out.get("qualification_lost_date"),
        field="qualification_lost_date",
        src=src_file,
        line_no=line_no,
    )

    out["person_id_custom"] = generate_person_id_custom(
        insurer_number=int(insurer_number_8),
        insurance_symbol=str(out.get("insurance_symbol") or ""),
        insurance_number=str(out.get("insurance_number") or ""),
        birth_yyyymmdd=birth_yyyymmdd,
    )

    rc = str(out.get("relationship_code") or "").strip()
    rn = str(out.get("relationship_name") or "").strip()
    if rn == "" and rc != "":
        out["relationship_name"] = "本人" if rc in {"0", "00"} else rc

    return out


# ============================================================
# file processing
# ============================================================

@dataclass
class ImportResult:
    inserted: int = 0
    seen: int = 0
    errors: int = 0


def process_one_file(
    *,
    cur_data,
    cur_log,
    run_id: int,
    fund_id: int,
    template_ver: int,
    insurer_number_8: str,
    fp: Path,
    mapping_defs: List[Dict[str, Any]],
    limit_rows: int,
) -> ImportResult:
    res = ImportResult()
    enc = detect_csv_encoding(fp)
    logging.info(f"[OPEN] {fp.name} enc={enc}")

    with fp.open("r", encoding=enc, newline="") as f:
        rdr = csv.DictReader(f)
        rdr.fieldnames = normalize_fieldnames(rdr.fieldnames or [])

        row_no = 0
        for row in rdr:
            row_no += 1
            res.seen += 1
            if limit_rows > 0 and res.inserted >= limit_rows:
                break

            try:
                base_vals = apply_template_mapping(csv_row=row, mapping_defs=mapping_defs)
                final_vals = normalize_one_row(
                    vals=base_vals,
                    insurer_number_8=insurer_number_8,
                    src_file=fp.name,
                    line_no=row_no + 1,
                )

                cur_data.execute(
                    """
                    INSERT INTO staging_subscribers_fund (
                        fund_id, template_ver,
                        person_id_custom,
                        name_kana_full, name_kanji_full,
                        name_kanji_family, name_kanji_middle, name_kanji_given,
                        name_kana_family, name_kana_middle, name_kana_given,
                        gender_code, birth,
                        insurer_number, insurance_symbol, insurance_symbol_digits,
                        insurance_number, insurance_branchnumber,
                        qualification_acquired_date, qualification_lost_date,
                        postal_code, address_line, building,
                        phone, email,
                        employer_code, department_code, distribution_code,
                        employee_code, connect_id,
                        relationship_code, relationship_name,
                        src_file, src_row_no, src_line_no,
                        import_run_id,
                        loaded_at
                    )
                    VALUES (
                        %(fund_id)s, %(template_ver)s,
                        %(person_id_custom)s,
                        %(name_kana_full)s, %(name_kanji_full)s,
                        %(name_kanji_family)s, %(name_kanji_middle)s, %(name_kanji_given)s,
                        %(name_kana_family)s, %(name_kana_middle)s, %(name_kana_given)s,
                        %(gender_code)s, %(birth)s,
                        %(insurer_number)s, %(insurance_symbol)s, %(insurance_symbol_digits)s,
                        %(insurance_number)s, %(insurance_branchnumber)s,
                        %(qualification_acquired_date)s, %(qualification_lost_date)s,
                        %(postal_code)s, %(address_line)s, %(building)s,
                        %(phone)s, %(email)s,
                        %(employer_code)s, %(department_code)s, %(distribution_code)s,
                        %(employee_code)s, %(connect_id)s,
                        %(relationship_code)s, %(relationship_name)s,
                        %(src_file)s, %(src_row_no)s, %(src_line_no)s,
                        %(import_run_id)s,
                        NOW(3)
                    )
                    """,
                    {
                        **final_vals,
                        "fund_id": fund_id,
                        "template_ver": template_ver,
                        "src_file": fp.name,
                        "src_row_no": row_no,
                        "src_line_no": row_no + 1,
                        "import_run_id": run_id,
                    },
                )

                res.inserted += 1

            except NormalizeError as ne:
                res.errors += 1
                etl_error(
                    cur_log,
                    run_id=run_id,
                    phase="import",
                    source="import_subscribers_to_staging_fund",
                    insurer_number=insurer_number_8,
                    src_file=fp.name,
                    row_no=row_no,
                    line_no=row_no + 1,
                    field=getattr(ne, "field", None),
                    field_value=getattr(ne, "raw_value", None),
                    error_code=getattr(ne, "code", type(ne).__name__),
                    message=str(ne),
                )

            except Exception as e:
                res.errors += 1
                etl_error(
                    cur_log,
                    run_id=run_id,
                    phase="import",
                    source="import_subscribers_to_staging_fund",
                    insurer_number=insurer_number_8,
                    src_file=fp.name,
                    row_no=row_no,
                    line_no=row_no + 1,
                    field=None,
                    field_value=None,
                    error_code=type(e).__name__,
                    message=str(e),
                )

    return res


# ============================================================
# optional file moves
# ============================================================

def move_files_on_result(
    *,
    insurer_dir: Path,
    files: List[Path],
    success: bool,
) -> None:
    if success and not _env_bool(ENV_MOVE_ON_SUCCESS, False):
        return
    if (not success) and not _env_bool(ENV_MOVE_ON_FAIL, False):
        return

    dst_name = os.getenv(ENV_PAST_DIR) or "past"
    if not success:
        dst_name = os.getenv(ENV_ERROR_DIR) or "error"

    dst = insurer_dir.parent.parent / dst_name / insurer_dir.name  # .../subscribers_fund/<past|error>/<insurer>/
    dst.mkdir(parents=True, exist_ok=True)

    for fp in files:
        try:
            fp.rename(dst / fp.name)
        except Exception as e:
            logging.warning(f"[WARN] move failed: {fp} -> {dst} ({e})")


# ============================================================
# main
# ============================================================

def main() -> int:
    # ★ここを最初に（load_mysql_paramsより前）
    env_path = WORK_ROOT / ".env"
    load_dotenv(env_path, override=True)

    print("WORK_ROOT =", WORK_ROOT)
    print(".env path =", env_path)
    print(".env exists =", env_path.exists())
    print("PHR_MYSQL_USER =", os.getenv("PHR_MYSQL_USER"))
    print("PHR_MYSQL_HOST =", os.getenv("PHR_MYSQL_HOST"))
    print("PHR_MYSQL_DB   =", os.getenv("PHR_MYSQL_DB"))

    base_params: MySQLParams = load_mysql_params()

    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", default=None, help="MySQL schema（dev_phr等）。未指定なら env/設定に従う")
    ap.add_argument("--input", default=None, help="base入力（未指定なら env/デフォルト）")
    ap.add_argument("--insurer", default=None, help="8桁保険者番号（未指定ならactive配下を全走査）")
    ap.add_argument("--version", type=int, default=None, help="template version（YYYYMMDD）。未指定ならMAX(version)")
    ap.add_argument("--limit", type=int, default=0, help="最大取り込み行数（0=無制限）")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(message)s",
    )

    base = Path(args.input) if args.input else resolve_input_base()
    if not base.is_absolute():
        base = WORK_ROOT / base

    active_dir = resolve_active_dir(base)

    schema = args.schema or os.getenv(ENV_DEFAULT_SCHEMA) or None
    params: MySQLParams = clone_mysql_params_with_schema(base_params, schema)

    logging.info(f"[INFO] WORK_ROOT = {WORK_ROOT}")
    logging.info(f"[INFO] INPUT_BASE = {base}")
    logging.info(f"[INFO] ACTIVE_DIR = {active_dir}")
    logging.info(f"[INFO] SCHEMA = {getattr(params, 'database', '(unknown)')}")

    if not active_dir.exists():
        logging.error(f"[ERR] active dir not found: {active_dir}")
        return 3

    insurer_dirs: List[Path] = []
    if args.insurer:
        p = active_dir / str(args.insurer).zfill(8)
        insurer_dirs = [p]
    else:
        insurer_dirs = [p for p in active_dir.iterdir() if p.is_dir()]

    with connect_ctx(params) as conn_log, connect_ctx(params) as conn_data:
        cur_log = dict_cursor(conn_log)
        cur_data = dict_cursor(conn_data)

        processed_any = False

        for insurer_dir in insurer_dirs:
            if not insurer_dir.exists() or not insurer_dir.is_dir():
                continue

            try:
                insurer_int = normalize_insurer_folder_name_to_int(insurer_dir)
            except NormalizeError as ne:
                logging.warning(f"[SKIP] {insurer_dir.name}: {ne}")
                continue
            insurer = f"{insurer_int:08d}"

            files = list_csvs_under(insurer_dir)
            if not files:
                continue

            processed_any = True
            filenames = [f.name for f in files]

            logging.info("")
            logging.info(f"[INSURER] {insurer} files={len(files)}")

            try:
                fund_id = find_fund_id_by_insurer(cur_log, insurer)
                template_ver = pick_template_version(cur_log, fund_id, args.version)
                mapping = get_template_mapping(cur_log, fund_id, template_ver)
            except Exception as e:
                logging.error(f"[FAIL] precheck failed insurer={insurer}: {e}")
                continue

            run_id = etl_start(
                cur_log,
                phase="import",
                source="import_subscribers_to_staging_fund",
                db_path=None,
                db_schema=getattr(params, "database", None),
                input_base=str(insurer_dir),
                input_file=None,
                insurer_number=insurer,
                dry_run=False,
                limit_rows=args.limit,
            )
            conn_log.commit()
            logging.info(f"[RUN] run_id={run_id} fund_id={fund_id} template_ver={template_ver}")

            dup = detect_duplicate_success_files(
                cur_log,
                fund_id=fund_id,
                template_ver=template_ver,
                insurer_number=insurer,
                filenames=filenames,
            )
            if dup:
                msg = "duplicate_files=" + ",".join(dup)
                logging.error(f"[FAIL] {msg}")

                m = metrics_set(RunMetrics(), files=len(files), errors=1, rows_seen=0, rows_inserted=0)
                etl_finish(cur_log, run_id=run_id, metrics=m, status_override="failed", extra_notes=msg)
                conn_log.commit()
                continue

            total = ImportResult()
            for fp in files:
                logging.info(f"[LOAD] {fp.name}")
                r = process_one_file(
                    cur_data=cur_data,
                    cur_log=cur_log,
                    run_id=run_id,
                    fund_id=fund_id,
                    template_ver=template_ver,
                    insurer_number_8=insurer,
                    fp=fp,
                    mapping_defs=mapping,
                    limit_rows=args.limit if args.limit > 0 else 0,
                )
                total.inserted += r.inserted
                total.seen += r.seen
                total.errors += r.errors
                logging.info(f"[FILE] {fp.name} inserted={r.inserted} seen={r.seen} errors={r.errors}")

            m = metrics_set(
                RunMetrics(),
                files=len(files),
                rows_seen=total.seen,
                rows_inserted=total.inserted,
                errors=total.errors,
            )

            if total.errors > 0:
                conn_data.rollback()
                msg = f"errors={total.errors} (staging rollback; see etl_errors)"
                logging.error(f"[FAIL] {msg}")
                etl_finish(cur_log, run_id=run_id, metrics=m, status_override="failed", extra_notes=msg)
                conn_log.commit()

                move_files_on_result(insurer_dir=insurer_dir, files=files, success=False)
                continue

            conn_data.commit()
            etl_finish(cur_log, run_id=run_id, metrics=m, status_override="success")
            conn_log.commit()
            logging.info(f"[OK] insurer={insurer} inserted={total.inserted}")

            move_files_on_result(insurer_dir=insurer_dir, files=files, success=True)

        if not processed_any:
            logging.info("[INFO] no target CSVs found under active/")
            return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
