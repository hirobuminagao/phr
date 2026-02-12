# -*- coding: utf-8 -*-
r"""
fund_enrollee_loader/import_fund_subscribers.py
【位置づけ（v1.0整理）】
- 本スクリプトは fund_enrollee_loader 系（SQLite/hub_stg/hub_prod 前提）の取込処理。
- work_folder/scripts/import_subscribers_to_staging_fund.py（dev_phr/MySQL系）とは別系統。
- v1.0 現行の正規運用ルートは work_folder 側であり、本ファイルは legacy 位置づけとする。
- 仕様凍結対象はあるが、今後の拡張・新規機能追加は原則 work_folder 系で行う。


機能概要:
- VS Code の「Run ▶」で実行可能（work_folder/.env を自動読込）
- 入力: work_folder/fund_enrollee_loader/input/<保険者番号8桁>/<ファイル>.csv
  * --input 未指定時は上記パスを自動探索（保険者番号=フォルダ名で推測）
- insurer/fund_id 解決:
  * .env(FUND_INSURER) > --insurer > 入力フォルダ名(8桁) の順で決定
  * fund_id は DB の funds/fund_insurer_numbers から解決
- テンプレ ver:
  * .env(FUND_TEMPLATE_VERSION) > --version > .env(FUND_TEMPLATE_DATE) の順
- custom_id_gen:
  * 既定: work_folder/lib/custom_id_gen.py
  * .env(FUND_CUSTOM_ID_ENABLE=0) で無効化可
- 取込先: staging_subscribers_fund
- バッチコミット: 既定 .env(FUND_LOADER_BATCH) または 2000 行ごと
- 全体上限: .env(FUND_LIMIT) または --limit で「全ファイル合計」の最大処理行数（0=無制限）
- コミットログ:
  * [COMMIT] +N rows @ <filename> (total X) | at HH:MM:SS | +Δs since last | Ts total
  * [FILE DONE] start/end/total を HH:MM:SS でサマる

追記（2025-11-06）:
- etl_runs / etl_errors に記帳（phase='import'）
- 続柄の取り扱いを「原本尊重+フォールバック」に統一
  * relationship_code: CSV→`digits_or_null` でそのまま格納
  * relationship_name: CSVに無ければフォールバックで自動補完
      - name 空 & code あり → code が 0/00 なら「本人」、それ以外は code 文字列
      - 両方空 → 両方 NULL
      - name のみ → name を維持、code は NULL のまま
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
import unicodedata

# =========================
# .env ローダ（work_folder/.env）
# =========================
ROOT = Path(__file__).resolve().parents[1]  # work_folder
ENV_PATH = ROOT / ".env"


def _load_env_loose(path: Path) -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(path)
        return
    except Exception:
        pass
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, v = s.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_env_loose(ENV_PATH)

# =========================
# エンコーディング（SJIS/UTF-8判定）
# =========================
def detect_csv_encoding(fp: Path) -> str:
    """
    CSV のエンコーディング判定:
    - まず utf-8-sig
    - ダメなら utf-8
    - ダメなら cp932（Windows Shift-JIS）
    """
    candidates = ["utf-8-sig", "utf-8", "cp932"]

    with fp.open("rb") as bf:
        head = bf.read(4096)  # 先頭だけで十分

    for enc in candidates:
        try:
            head.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue

    # 最後は cp932 にフォールバック
    return "cp932"


# =========================
# custom_id_gen の import 設定
# =========================
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    # work_folder/lib/custom_id_gen.py の generate_id を直接呼び出す
    from lib.custom_id_gen import generate_id as generate_custom_id  # type: ignore
except Exception:
    generate_custom_id = None  # type: ignore

# =========================
# DB パス解決
# =========================
APP_ENV = (os.getenv("APP_ENV") or "stg").lower()
DB_STG = ROOT / "db" / "stg" / "hub_stg.sqlite"
DB_PROD = ROOT / "db" / "prod" / "hub_prod.sqlite"


def resolve_db_path() -> Path:
    env_db = os.getenv("HUB_DB_PATH")
    if env_db:
        return Path(env_db)
    if APP_ENV == "prod":
        return DB_PROD
    if APP_ENV == "stg":
        return DB_STG
    raise RuntimeError("DBパス未決定: HUB_DB_PATH を指定 or APP_ENV を stg/prod にしてください。")


# =========================
# 文字・正規化ユーティリティ
# =========================
FW_DIGITS = str.maketrans("０１２３４５６７８９", "0123456789")


def to_half_digits(s: str) -> str:
    return (s or "").translate(FW_DIGITS)


def only_digits(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())


def norm_birth_any(raw: Optional[str]) -> str:
    s = to_half_digits((raw or "").strip())
    if len(s) == 8 and s.isdigit():
        return s
    nums = [n for n in re.split(r"[^\d]+", s) if n]
    if len(nums) == 3:
        if len(nums[0]) == 4:
            y, m, d = int(nums[0]), int(nums[1]), int(nums[2])
        else:
            y, m, d = int(nums[2]), int(nums[0]), int(nums[1])
        return f"{y:04d}{m:02d}{d:02d}"
    return only_digits(s)


def split_by_space(s: str) -> Tuple[str, str, str]:
    """姓/中間/名 を空白分割。1語=名のみ、2語=姓/名、3+語=姓/中間/名"""
    if not s:
        return "", "", ""
    t = (s or "").replace("\u3000", " ").strip()
    toks = [x for x in re.split(r"\s+", t) if x]
    if not toks:
        return "", "", ""
    if len(toks) == 1:
        return "", "", toks[0]
    if len(toks) == 2:
        return toks[0], "", toks[1]
    return toks[0], " ".join(toks[1:-1]), toks[-1]


def kana_to_fullwidth_katakana(s: str) -> str:
    t = unicodedata.normalize("NFKC", s or "")
    out = []
    for ch in t:
        o = ord(ch)
        if 0x3041 <= o <= 0x3096:  # ひらがな→カタカナ
            out.append(chr(o + 0x60))
        else:
            out.append(ch)
    return "".join(out)


# =========================
# CSV utils
# =========================
def normalize_fieldnames(fn: Sequence[str]) -> List[str]:
    out: List[str] = []
    for h in fn:
        s = (h or "").strip()
        if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
            s = s[1:-1]
        s = s.replace("\u3000", " ").strip()
        out.append(s)
    return out


# =========================
# custom_id_gen 呼び出し（in-process版）
# =========================
def gen_custom_id(
    insurer_number_text: str,
    symbol_text: str,
    insurance_number_text: str,
    birth_yyyymmdd: str,
    *,
    cid_script: Path,  # 互換のために残すが、generate_id を優先
    mat_dir: Path,
) -> str:
    """
    custom_id_gen.py の generate_id(...) を in-process で呼び出す。
    - custom_id_gen.py 本体は変更しない。
    """
    if not cid_script.exists():
        raise FileNotFoundError(f"custom_id_gen.py が見つかりません: {cid_script}")

    if generate_custom_id is None:
        raise RuntimeError("lib.custom_id_gen.generate_id を import できませんでした。PYTHONPATH/配置を確認してください。")

    cid, _meta = generate_custom_id(
        insurer_number=insurer_number_text,
        symbol=symbol_text or "",
        insurance_number=insurance_number_text or "",
        birth_yyyymmdd=birth_yyyymmdd or "",
        mat_dir=mat_dir,
    )
    cid = (cid or "").strip()
    if not cid:
        raise RuntimeError("person_id_custom 空出力")
    return cid


# =========================
# DB helpers
# =========================
def connect_db(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(f"DBが見つかりません: {path}")
    con = sqlite3.connect(f"file:{path.as_posix()}?mode=rw", uri=True)
    con.row_factory = sqlite3.Row
    return con


def find_fund_id_by_insurer(cur: sqlite3.Cursor, insurer_number: str | int) -> int:
    ins_str = str(insurer_number).strip()
    row = cur.execute(
        """
        SELECT fin.fund_id
        FROM fund_insurer_numbers AS fin
        JOIN funds AS f ON f.fund_id = fin.fund_id
        WHERE fin.insurer_number = ?
        LIMIT 1
        """,
        (ins_str,),
    ).fetchone()
    if row is None:
        raise ValueError(f"insurer_number not found in fund_insurer_numbers: {ins_str}")
    return int(row[0])


def get_template_mapping(cur: sqlite3.Cursor, fund_id: int, version: int) -> List[sqlite3.Row]:
    cur.execute(
        """
        SELECT col_order, csv_header, target_column, rule, required
        FROM template_mappings
        WHERE fund_id = ? AND version = ?
        ORDER BY col_order ASC
    """,
        (fund_id, version),
    )
    rows = cur.fetchall()
    if not rows:
        raise RuntimeError(f"template_mappings が見つかりません (fund_id={fund_id}, version={version})")
    return rows


# =========================
# ETL logging
# =========================
ETL_SOURCE_NAME = "fund_enrollee_loader/import_fund_subscribers.py"


def etl_run_start(
    con: sqlite3.Connection,
    *,
    phase: str,
    source: str,
    db_path: str | None,
    input_base: str | None,
    input_file: str | None,
    insurer_number: str | None,
    dry_run: int | None,
    limit_rows: int | None,
) -> int:
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO etl_runs (
            phase,
            source,
            status,
            db_path,
            input_base,
            input_file,
            insurer_number,
            dry_run,
            limit_rows
        ) VALUES (
            ?, ?, 'running', ?, ?, ?, ?, ?, ?
        )
    """,
        (phase, source, db_path, input_base, input_file, insurer_number, dry_run, limit_rows),
    )
    con.commit()
    run_id = cur.lastrowid
    if run_id is None:
        raise RuntimeError("etl_runs の run_id を取得できませんでした")
    return int(run_id)


def etl_run_finish(
    con: sqlite3.Connection,
    run_id: int,
    *,
    status: str,
    files: int,
    rows_seen: int,
    rows_inserted: int,
    rows_updated: int = 0,
    rows_skipped: int = 0,
    errors: int = 0,
    notes: str | None = None,
) -> None:
    con.execute(
        """
        UPDATE etl_runs
           SET status        = ?,
               finished_at   = strftime('%Y-%m-%d %H:%M:%f','now','localtime'),
               files         = ?,
               rows_seen     = ?,
               rows_inserted = ?,
               rows_updated  = ?,
               rows_skipped  = ?,
               errors        = ?,
               notes         = COALESCE(notes, ?)
         WHERE run_id = ?
    """,
        (
            status,
            files,
            rows_seen,
            rows_inserted,
            rows_updated,
            rows_skipped,
            errors,
            notes,
            run_id,
        ),
    )
    con.commit()


def etl_log_error(
    con: sqlite3.Connection,
    *,
    run_id: int,
    phase: str,
    source: str,
    insurer_number: str | None,
    src_file: str | None,
    src_row_no: int | None,
    src_line_no: int | None,
    staging_rowid: int | None,
    person_id_custom: str | None,
    field: str | None,
    field_value: str | None,
    error_code: str | None,
    message: str,
) -> None:
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO etl_errors (
            run_id,
            phase,
            source,
            insurer_number,
            src_file,
            src_row_no,
            src_line_no,
            staging_rowid,
            person_id_custom,
            field,
            field_value,
            error_code,
            message
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """,
        (
            run_id,
            phase,
            source,
            insurer_number,
            src_file,
            src_row_no,
            src_line_no,
            staging_rowid,
            person_id_custom,
            field,
            field_value,
            error_code,
            message,
        ),
    )
    con.commit()


# =========================
# 入力ディレクトリ推測
# =========================
def auto_input_root() -> Path:
    base = ROOT / "fund_enrollee_loader" / "input"
    if not base.exists():
        base.mkdir(parents=True, exist_ok=True)
    folders = [d for d in base.iterdir() if d.is_dir() and d.name.isdigit() and len(d.name) == 8]
    return folders[0] if folders else base


def infer_insurer_from_path(p: Path) -> Optional[str]:
    parts = list(p.resolve().parts)
    for i in range(len(parts) - 1, -1, -1):
        name = parts[i]
        if len(name) == 8 and name.isdigit():
            return name
    return None


# =========================
# 1ファイル処理
# =========================
def process_file(
    con: sqlite3.Connection,
    cur: sqlite3.Cursor,
    *,
    fund_id: int,
    version: int,
    fp: Path,
    mapping_rows: List[sqlite3.Row],
    batch_size: int,
    cid_enable: bool,
    cid_script: Path,
    mat_dir: Path,
    insurer_number_text: str,
    max_rows: Optional[int] = None,
    trace: bool = False,
    import_run_id: Optional[int] = None,
) -> tuple[int, int, int, str]:
    """1 CSV を staging_subscribers_fund へ流し込み。戻り値は (inserted, seen, errors, encoding)"""
    inserted = 0
    seen = 0
    errors = 0
    t0 = time.perf_counter()
    start_dt = datetime.now()
    last = t0

    enc = detect_csv_encoding(fp)
    print(f"[INFO] open {fp.name} with encoding = {enc}")

    with fp.open("r", encoding=enc, newline="") as f:
        rdr = csv.DictReader(f)
        rdr.fieldnames = normalize_fieldnames(rdr.fieldnames or [])
        header_map = {h: h for h in (rdr.fieldnames or [])}



        # マッピング事前解析
        mdefs = []
        for r in mapping_rows:
            mdefs.append(
                {
                    "col_order": r["col_order"],
                    "src": r["csv_header"],
                    "tgt": r["target_column"],
                    "rule": (r["rule"] or "").strip(),
                    "required": int(r["required"] or 0),
                }
            )

        rowno = 0
        for row in rdr:
            rowno += 1
            seen += 1
            if max_rows is not None and inserted >= max_rows:
                break

            def get_src(name: str) -> str:
                key = header_map.get(name, name)
                val = row.get(key, "")
                if isinstance(val, str):
                    v = val.strip()
                    if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
                        v = v[1:-1].strip()
                    return v
                return str(val or "")

            vals: Dict[str, Any] = {
                "fund_id": fund_id,
                "template_ver": version,
                "src_file": fp.name,
                "src_row_no": rowno,
                "src_line_no": rowno + 1,
                "import_run_id": import_run_id,
            }

            # name_* 初期化
            for key in [
                "name_kana_family",
                "name_kana_middle",
                "name_kana_given",
                "name_kanji_family",
                "name_kanji_middle",
                "name_kanji_given",
            ]:
                vals[key] = None

            # ルール適用
            for m in mdefs:
                src = m["src"]
                tgt = m["tgt"]
                rule = m["rule"]

                def setval(column: str, value: Any):
                    vals[column] = value

                if rule == "direct":
                    setval(tgt, get_src(src))
                elif rule == "digits_only":
                    setval(tgt, only_digits(get_src(src)))
                elif rule == "birth_norm" or rule == "birth_yyyymmdd":
                    setval(tgt, norm_birth_any(get_src(src)))
                elif rule.startswith("rename:"):
                    setval(tgt, get_src(rule.split(":", 1)[1]))
                elif rule.startswith("const:"):
                    setval(tgt, rule.split(":", 1)[1])
                elif rule in ("gender_code_norm", "gender_code_from"):
                    g = (get_src(src) or "").strip()
                    if g in {"1", "男", "male", "m", "M"}:
                        setval(tgt, "1")
                    elif g in {"2", "女", "female", "f", "F"}:
                        setval(tgt, "2")
                    else:
                        setval(tgt, "9")
                elif rule in ("split_kana_full", "split_family_kana", "split_middle_kana", "split_given_kana"):
                    full = get_src(src)
                    fam, mid, giv = split_by_space(full)
                    vals["name_kana_full"] = kana_to_fullwidth_katakana(full).replace(" ", "")
                    vals["name_kana_family"] = kana_to_fullwidth_katakana(fam)
                    vals["name_kana_middle"] = kana_to_fullwidth_katakana(mid)
                    vals["name_kana_given"] = kana_to_fullwidth_katakana(giv)
                elif rule in ("kana_full_no_space",):
                    full = get_src(src)
                    vals["name_kana_full"] = kana_to_fullwidth_katakana(full).replace(" ", "")
                elif rule in ("split_kanji_full", "split_family", "split_middle", "split_given", "split_kanji:given"):
                    full = get_src(src)
                    fam, mid, giv = split_by_space(full)
                    vals["name_kanji_full"] = full
                    vals["name_kanji_family"] = fam
                    vals["name_kanji_middle"] = mid
                    vals["name_kanji_given"] = giv
                elif rule in ("relcode_to_name", "relationship_map"):
                    # CSVに name がある場合は direct で入り得るが、ここでは一律の置換はしない
                    # name 不在時のフォールバックは後段で処理
                    setval(tgt, get_src(src))
                elif rule in ("symbol_norm",):
                    s = get_src(src).strip()
                    s = s.replace("－", "-").replace("―", "-").replace("ｰ", "-")
                    setval(tgt, s)
                elif rule in ("symbol_digits", "digits_or_null"):
                    d = only_digits(get_src(src))
                    setval(tgt, d if d else None)
                elif rule in ("digits_required",):
                    d = only_digits(get_src(src))
                    setval(tgt, d)
                elif rule in ("date_or_null",):
                    d = norm_birth_any(get_src(src))
                    setval(tgt, d if len(d) == 8 else None)
                else:
                    setval(tgt, get_src(src))

            # insurer_number 既定
            vals.setdefault("insurer_number", insurer_number_text)

            # 証記号の数字（INT列）抽出
            sym = vals.get("insurance_symbol")
            if sym is not None:
                sym_digits_txt = only_digits(str(sym))
                try:
                    vals["insurance_symbol_digits"] = int(sym_digits_txt) if sym_digits_txt != "" else None
                except Exception:
                    vals["insurance_symbol_digits"] = None
            else:
                vals["insurance_symbol_digits"] = None

            # person_id_custom 生成（in-process）
            if cid_enable:
                try:
                    cid = gen_custom_id(
                        insurer_number_text=insurer_number_text,
                        symbol_text=str(vals.get("insurance_symbol") or ""),
                        insurance_number_text=str(vals.get("insurance_number") or ""),
                        birth_yyyymmdd=str(vals.get("birth") or ""),
                        cid_script=cid_script,
                        mat_dir=mat_dir,
                    )
                    vals["person_id_custom"] = cid
                except Exception as e:
                    print(f"[WARN] custom_id_gen 失敗 row={rowno}: {e}")

            # ===== 続柄のフォールバック（ここで最終決定） =====
            rc = (vals.get("relationship_code") or "").strip()
            rn = (vals.get("relationship_name") or "").strip()

            if rn == "" and rc != "":
                # code が 0/00 → 本人、それ以外 → code 文字列
                if rc in {"0", "00"}:
                    vals["relationship_name"] = "本人"
                else:
                    vals["relationship_name"] = rc
            elif rn != "" and rc == "":
                # name のみ → そのまま（code は NULL のまま）
                pass
            elif rn == "" and rc == "":
                # 両方空 → 何もしない（NULLのまま）
                pass
            # name と code の両方ある場合は CSV優先でそのまま

            # 未設定の可能性がある列を明示（NULLで良い）
            defaults = {
                "name_kanji_full": None,
                "name_kanji_family": None,
                "name_kanji_middle": None,
                "name_kanji_given": None,
                "name_kana_family": None,
                "name_kana_middle": None,
                "name_kana_given": None,
                "insurance_branchnumber": None,
                "qualification_acquired_date": None,
                "qualification_lost_date": None,
                "postal_code": None,
                "address_line": None,
                "building": None,
                "phone": None,
                "email": None,
                "employer_code": None,
                "department_code": None,
                "distribution_code": None,
                "employee_code": None,
                "connect_id": None,
                "loaded_at": None,
                "processed_at": None,
                "person_id_custom": None,
                "relationship_code": None,
                "relationship_name": None,
            }
            for k, v in defaults.items():
                vals.setdefault(k, v)

            try:
                cur.execute(
                    """
                    INSERT INTO staging_subscribers_fund(
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
                        src_file, src_row_no, src_line_no, import_run_id,
                        created_at, loaded_at, processed_at
                    )
                    VALUES (
                        :fund_id, :template_ver,
                        :person_id_custom,
                        :name_kana_full, :name_kanji_full,
                        :name_kanji_family, :name_kanji_middle, :name_kanji_given,
                        :name_kana_family, :name_kana_middle, :name_kana_given,
                        :gender_code, :birth,
                        :insurer_number, :insurance_symbol, :insurance_symbol_digits,
                        :insurance_number, :insurance_branchnumber,
                        :qualification_acquired_date, :qualification_lost_date,
                        :postal_code, :address_line, :building,
                        :phone, :email,
                        :employer_code, :department_code, :distribution_code,
                        :employee_code, :connect_id,
                        :relationship_code, :relationship_name,
                        :src_file, :src_row_no, :src_line_no, :import_run_id,
                        strftime('%Y-%m-%d %H:%M:%f','now','localtime'),
                        :loaded_at, :processed_at
                    )
                """,
                    vals,
                )
                inserted += 1
            except Exception as e:
                errors += 1
                if import_run_id is not None:
                    etl_log_error(
                        con,
                        run_id=import_run_id,
                        phase="import",
                        source=ETL_SOURCE_NAME,
                        insurer_number=insurer_number_text,
                        src_file=fp.name,
                        src_row_no=rowno,
                        src_line_no=rowno + 1,
                        staging_rowid=None,
                        person_id_custom=vals.get("person_id_custom"),
                        field=None,
                        field_value=None,
                        error_code=type(e).__name__,
                        message=str(e),
                    )
                continue

            if batch_size and (inserted % batch_size) == 0:
                con.commit()
                now = datetime.now().strftime("%H:%M:%S")
                t_now = time.perf_counter()
                print(
                    f"[COMMIT] +{batch_size} rows @ {fp.name} (total {inserted}) "
                    f"| at {now} | +{t_now - last:.1f}s since last | {t_now - t0:.1f}s total"
                )
                last = t_now

            if trace and (rowno % 10000 == 0):
                print(f"[TRACE] {fp.name}: scanned {rowno} rows, inserted {inserted}, errors={errors}")

    con.commit()
    end_dt = datetime.now()
    total_sec = time.perf_counter() - t0

    def fmt_hms(dt: datetime) -> str:
        return dt.strftime("%Y/%m/%d %H:%M:%S")

    def fmt_td(sec: float) -> str:
        td = timedelta(seconds=int(sec))
        return str(td)

    print(
        f"[FILE DONE] {fp.name}  rows={inserted}  errors={errors}  "
        f"start={fmt_hms(start_dt)}  end={fmt_hms(end_dt)}  total={fmt_td(total_sec)}"
    )
    return inserted, seen, errors, enc


# =========================
# メイン
# =========================
def list_csvs_under(base: Path) -> List[Path]:
    if base.is_file() and base.suffix.lower() == ".csv":
        return [base]
    if base.is_dir():
        return sorted([p for p in base.glob("*.csv")])
    return []


def pick_template_version(
    cur: sqlite3.Cursor,
    fund_id: int,
    explicit_version: Optional[int],
    explicit_date: Optional[str],
) -> int:
    if explicit_version is not None:
        return int(explicit_version)
    if explicit_date:
        v = int(explicit_date.replace("-", ""))
        return v
    row = cur.execute("""SELECT MAX(version) AS v FROM templates WHERE fund_id = ?""", (fund_id,)).fetchone()
    if not row or row["v"] is None:
        raise RuntimeError(f"templates が未登録です (fund_id={fund_id})")
    return int(row["v"])


def main() -> int:
    ENV_BATCH = int(os.getenv("FUND_LOADER_BATCH") or "2000")
    ENV_LIMIT = int(os.getenv("FUND_LIMIT") or "0")
    ENV_TRACE = (os.getenv("FUND_LOADER_TRACE") or "0").strip() not in {"0", "false", "False"}

    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(resolve_db_path()))
    ap.add_argument("--input", help="入力ファイル or フォルダ（未指定なら fund_enrollee_loader/input/<insurer> を自動）")
    ap.add_argument("--insurer", help="8桁の保険者番号。省略時は入力パスから推測")
    ap.add_argument("--version", type=int, help="テンプレート version（YYYYMMDD 数値想定）")
    ap.add_argument("--date", help="テンプレート日付 YYYY-MM-DD（--version より優先度低）")
    ap.add_argument("--truncate", type=int, default=0, help="1=staging_subscribers_fund を事前クリア")
    ap.add_argument("--batch", type=int, default=ENV_BATCH, help="コミット件数（.env:FUND_LOADER_BATCH）")
    ap.add_argument("--limit", type=int, default=ENV_LIMIT, help="全体上限（0=無制限, .env:FUND_LIMIT）")
    ap.add_argument("--trace", type=int, default=1 if ENV_TRACE else 0, help="1=進捗トレース（.env:FUND_LOADER_TRACE）")
    args = ap.parse_args()

    db_path = Path(args.db)
    input_arg = args.input or ""
    base = Path(input_arg) if input_arg else auto_input_root()

    insurer = (os.getenv("FUND_INSURER") or args.insurer or infer_insurer_from_path(base))
    if not insurer:
        print("insurer が未指定です。--insurer か .env(FUND_INSURER) か入力パスで推測できる構成にしてください。")
        return 2
    insurer = "".join(ch for ch in str(insurer) if ch.isdigit())
    if len(insurer) != 8:
        print(f"insurer が8桁ではありません: {insurer}")
        return 2

    cid_enable = (os.getenv("FUND_CUSTOM_ID_ENABLE") or "1").strip() not in {"0", "false", "False"}
    cid_script = Path(os.getenv("FUND_CUSTOM_ID_GEN_PATH") or (ROOT / "lib" / "custom_id_gen.py"))

    print(f"[INFO] APP_ENV   = {APP_ENV}")
    print(f"[INFO] HUB_DB    = {db_path}")
    print(f"[INFO] --input   = {base if input_arg else '(auto)'}")
    print(f"[INFO] limit     = {args.limit if args.limit > 0 else 'unlimited'}")

    con = connect_db(db_path)
    cur = con.cursor()

    fund_id = find_fund_id_by_insurer(cur, insurer)
    print(f"[INFO] insurer={insurer} -> fund_id={fund_id}")

    env_ver = os.getenv("FUND_TEMPLATE_VERSION")
    env_date = os.getenv("FUND_TEMPLATE_DATE")
    version = pick_template_version(
        cur,
        fund_id,
        int(env_ver) if env_ver and env_ver.isdigit() else (args.version if args.version else None),
        env_date if env_date else (args.date if args.date else None),
    )
    print(f"[INFO] template version = {version}")
    print(f"[INFO] batch commit size = {args.batch}")
    print(f"[INFO] custom_id_gen = {cid_script} (enable={cid_enable})")

    # マッピング取得
    mapping = get_template_mapping(cur, fund_id, version)

    files: List[Path] = []
    if base.is_file():
        files = [base]
    else:
        p1 = base / insurer
        if p1.exists():
            files = list_csvs_under(p1)
        if not files:
            files = list_csvs_under(base)

    if not files:
        print(f"CSV が見つかりません: {base}")
        return 3

    if args.truncate:
        cur.execute("DELETE FROM staging_subscribers_fund")
        con.commit()
        print("[TRUNCATE] staging_subscribers_fund cleared.")

    run_id = etl_run_start(
        con,
        phase="import",
        source=ETL_SOURCE_NAME,
        db_path=str(db_path),
        input_base=str(base if base.is_dir() else base.parent),
        input_file=str(base if base.is_file() else ""),
        insurer_number=insurer,
        dry_run=0,
        limit_rows=args.limit,
    )
    print(f"[INFO] etl_runs.run_id = {run_id}")
    inserted_total = 0
    seen_total = 0
    error_total = 0
    file_count = 0
    encodings: List[str] = []  # 読み込んだファイルの encoding 情報を溜める

    try:
        for fp in files:
            file_count += 1
            print(f"[LOAD] {fp}")

            remaining = (args.limit - inserted_total) if args.limit > 0 else None
            if remaining is not None and remaining <= 0:
                print(f"[LIMIT] reached limit={args.limit}, stop further files.")
                break

            # ★ ここが重要：= が抜けてたのと enc を受け取る
            inserted, seen, errors, enc = process_file(
                con,
                cur,
                fund_id=fund_id,
                version=version,
                fp=fp,
                mapping_rows=mapping,
                batch_size=args.batch,
                cid_enable=cid_enable,
                cid_script=cid_script,
                mat_dir=ROOT / "mat",
                insurer_number_text=insurer,
                max_rows=remaining,
                trace=bool(args.trace),
                import_run_id=run_id,
            )

            inserted_total += inserted
            seen_total += seen
            error_total += errors

            # ファイルごとの encoding を覚えておく
            encodings.append(f"{fp.name}={enc}")

            print(f"[OK] loaded file={fp.name} inserted={inserted} seen={seen} errors={errors}")

            if args.limit > 0 and inserted_total >= args.limit:
                print(f"[LIMIT] reached limit={args.limit}, stop further files.")
                break

        # encoding 情報を notes 用にまとめる
        enc_note = ", ".join(encodings) if encodings else "n/a"
        base_notes = f"insurer={insurer}, fund_id={fund_id}, version={version}, encodings={enc_note}"

        etl_run_finish(
            con,
            run_id,
            status="success" if error_total == 0 else "partial",
            files=file_count,
            rows_seen=seen_total,
            rows_inserted=inserted_total,
            errors=error_total,
            notes=base_notes,
        )
    except Exception as e:
        # 例外時も encoding 情報があれば付けておく
        enc_note = ", ".join(encodings) if encodings else "n/a"
        base_notes = f"insurer={insurer}, fund_id={fund_id}, version={version}, encodings={enc_note}, exception={e}"

        etl_run_finish(
            con,
            run_id,
            status="failed",
            files=file_count,
            rows_seen=seen_total,
            rows_inserted=inserted_total,
            errors=error_total + 1,
            notes=base_notes,
        )
        raise


    print(
        f"[DONE] files={file_count} inserted={inserted_total} seen={seen_total} errors={error_total} "
        f"fund_id={fund_id} version={version}"
    )
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
