# -*- coding: utf-8 -*-
r"""
import_enrollees_to_staging.py

概要:
- VS Code の「Run ▶」からそのまま実行できるように、work_folder/.env を自動読込
- DB 接続先は環境変数で決定（レガシーへは戻らない）:
    1) HUB_DB_PATH があれば **強制採用**
    2) なければ APP_ENV=stg|prod で固定パスを採用
       - stg : <work_folder>/db/stg/hub_stg.sqlite
       - prod: <work_folder>/db/prod/hub_prod.sqlite
- 取り込み元フォルダは HUB_INPUT_BASE が優先。未指定なら <repo>/enrollee-info-registration/input/from_hub
- 実行時に選択されたパス等を [INFO] ログで表示

取り込み仕様（要点）:
- Hub の CSV を staging_subscribers_hub に投入（重複 OK）
- person_id_custom は lib/custom_id_gen.py で毎行生成（不足/異常は etl_errors に記録しスキップ）
- 氏名カナは全角カタカナへ正規化＋空白除去 → name_kana_full に格納
- 氏名/氏名カナは空白区切りで 姓/中間名/名 に分解（*_family/*_middle/*_given）
- 性別: '1'(男)/'2'(女)/'9'(不明)
- 生年月日: YYYYMMDD へ正規化 → staging.birth
- 保険証番号 insurance_number: 半角数字必須（空ならエラー）
- 保険証枝番 insurance_branchnumber: 半角数字 or NULL
- 保険証記号 insurance_symbol: 半角主体に正規化（ダッシュ '-'、中点 '･'、空白除去）
  → 記号中の数字連結を insurance_symbol_digits(INT or NULL) に格納
- 元 CSV の位置情報: src_file, src_row_no(1〜), src_line_no(header=1基準で2〜) を保持
- 実行管理: etl_runs/etl_errors に run 単位で記録

前提:
- DB に staging_subscribers_hub テーブルが存在（不足カラムは警告ログ）
- lib/custom_id_gen.py と mat/ が work_folder 直下に存在
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sqlite3
import sys
import unicodedata
from pathlib import Path
from typing import Optional, Dict, List, Tuple

# ===== .env を常時自動読込（work_folder/.env） =====
PKG_ROOT = Path(__file__).resolve().parents[1]  # => .../work_folder
_ENV_PATH = PKG_ROOT / ".env"

# lib を import パスに通す（PHP の require 的に）
LIB_DIR = PKG_ROOT / "lib"
if str(LIB_DIR) not in sys.path:
    sys.path.insert(0, str(LIB_DIR))

import custom_id_gen  # lib/custom_id_gen.py

def _load_env_loose(path: Path) -> None:
    """python-dotenv があれば使い、無ければ KEY=VALUE を簡易読込"""
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

_load_env_loose(_ENV_PATH)

# ===== 既定パス（環境変数で切替、レガシーには戻らない） =====
SCRIPT_DIR = Path(__file__).resolve().parent

_APP_ENV = (os.getenv("APP_ENV") or "stg").lower()
_DB_STG  = SCRIPT_DIR.parent / "db" / "stg"  / "hub_stg.sqlite"
_DB_PROD = SCRIPT_DIR.parent / "db" / "prod" / "hub_prod.sqlite"

def _resolve_default_db() -> Path:
    env_db = os.getenv("HUB_DB_PATH")
    if env_db:
        return Path(env_db)
    if _APP_ENV == "prod":
        return _DB_PROD
    if _APP_ENV == "stg":
        return _DB_STG
    raise RuntimeError(
        "DBパス未決定: HUB_DB_PATH を設定するか、APP_ENV を 'stg' か 'prod' にしてください。"
    )

DEFAULT_DB   = _resolve_default_db()
DEFAULT_BASE = Path(os.getenv("HUB_INPUT_BASE") or (SCRIPT_DIR / "input" / "from_hub"))
DEFAULT_LIB  = SCRIPT_DIR.parent / "lib" / "custom_id_gen.py"
DEFAULT_MAT  = SCRIPT_DIR.parent / "mat"

# ===== staging 必須列（存在チェック用） =====
REQUIRED_STAGING_COLUMNS = {
    "person_id_custom",
    "name_kana_full",
    "name_kanji_full",
    "name_kanji_family", "name_kanji_middle", "name_kanji_given",
    "name_kana_family",  "name_kana_middle",  "name_kana_given",
    "gender_code", "birth",
    "insured_attribute_name", "relationship_name",
    "insurer_number", "insurance_symbol", "insurance_symbol_digits",
    "insurance_number", "insurance_branchnumber",
    "qualification_acquired_date", "qualification_lost_date",
    "postal_code", "address_line", "building",
    "phone", "email",
    "employer_code", "department_code", "distribution_code",
    "employee_code", "connect_id",
    "created_at", "loaded_at", "processed_at",
    "src_file", "src_row_no", "src_line_no", "import_run_id",
}

# 進捗ログを出す間隔（何行ごとか）
PROGRESS_EVERY_ROWS = 1000

# ===== ヘッダーマッピング（DL → 採用名） =====
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

    # 原本そのまま入れる
    "続柄名称": "relationship_name",
    "被保険者属性名": "insured_attribute_name",
}

# ===== 正規化ユーティリティ =====
FW_DIGITS = str.maketrans("０１２３４５６７８９", "0123456789")
FW2HW = str.maketrans(
    "０１２３４５６７８９"
    "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ"
    "ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ"
    "－　・，．／＼＿（）［］｛｝：；＠！？”’＋＊＝＜＞｜＾～",
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz"
    "- ･,./\\_()[]{}:;@!\"' +*=<>|^~"
)
DASHES  = {"ー","―","—","ｰ","－"}
MIDDOTS = {"・","･"}

def to_half_digits(s: str) -> str:
    return (s or "").translate(FW_DIGITS)

def only_digits_text_or_none(s: str) -> Optional[str]:
    d = "".join(ch for ch in to_half_digits(s) if ch.isdigit())
    return d if d != "" else None

def only_digits_text_required(s: str, *, field: str, file: str, line_no: int) -> str:
    d = "".join(ch for ch in to_half_digits(s) if ch.isdigit())
    if d == "":
        raise ValueError(f"必須フィールド欠損: {field} (数字空) file={file} line={line_no}")
    return d

def norm_birth_required(s: str, *, file: str, line_no: int) -> str:
    raw = s or ""
    t = to_half_digits(raw).strip()
    if len(t) == 8 and t.isdigit():
        return t
    parts = [p for p in re.split(r"[^\d]+", t) if p]
    if len(parts) == 3:
        if len(parts[0]) == 4:
            y, m, d = parts[0], parts[1], parts[2]
        else:
            y, m, d = parts[2], parts[0], parts[1]
        try:
            return f"{int(y):04d}{int(m):02d}{int(d):02d}"
        except Exception:
            pass
    raise ValueError(f"必須フィールド不正: birth file={file} line={line_no} raw={raw}")

def norm_gender_to_code_text(s: str) -> str:
    t = (s or "").strip().lower()
    if t in {"1","男","male","m"}: return "1"
    if t in {"2","女","female","f"}: return "2"
    return "9"

def normalize_name_kana_fullwidth_no_space(s: str) -> str:
    t = unicodedata.normalize('NFKC', s or '')
    t = re.sub(r'\s+', '', t.replace("\u3000"," "))
    out = []
    for ch in t:
        o = ord(ch)
        if 0x3041 <= o <= 0x3096:  # ひらがな→カタカナ
            out.append(chr(o + 0x60))
        else:
            out.append(ch)
    return ''.join(out)

def normalize_kana_token_fullwidth(s: str) -> str:
    t = unicodedata.normalize('NFKC', s or '')
    out = []
    for ch in t:
        o = ord(ch)
        if 0x3041 <= o <= 0x3096:
            out.append(chr(o + 0x60))
        else:
            out.append(ch)
    return ''.join(out)

def split_name_by_space(s: str) -> Tuple[str, str, str]:
    if not s:
        return ("", "", "")
    t = s.replace("\u3000", " ")
    toks = [tok for tok in re.split(r"\s+", t.strip()) if tok]
    if not toks:
        return ("", "", "")
    if len(toks) == 1:
        return ("", "", toks[0])
    if len(toks) == 2:
        return (toks[0], "", toks[1])
    return (toks[0], " ".join(toks[1:-1]), toks[-1])

def normalize_insurance_symbol_for_db(raw: str) -> tuple[str, int | None]:
    s = (raw or "").strip()
    if not s:
        return "", None
    s = s.translate(FW2HW)
    s = re.sub(r"\s+", "", s.replace("\u3000"," "))
    buf = []
    for ch in s:
        if ch in DASHES:
            buf.append("-")
        elif ch in MIDDOTS:
            buf.append("･")
        else:
            buf.append(ch)
    s_norm = "".join(buf)
    digits = re.findall(r"\d+", s_norm)
    digits_val = int("".join(digits)) if digits else None
    return s_norm, digits_val

# ===== custom_id（必須生成） =====
def gen_custom_id(insurer_number: int, symbol_text: str, insured_num_text: str, birth_yyyymmdd: str,
                  *, file: str, line_no: int, custom_id_script: Path, mat_dir: Path) -> str:
    """
    custom_id_gen.py を「ライブラリ」として import し、
    generate_id() を同一プロセス内で呼び出して person_id_custom を生成する。
    custom_id_gen.py 本体には一切手を加えない。
    """
    if not custom_id_script.exists():
        raise FileNotFoundError(f"custom_id_gen.py が見つかりません: {custom_id_script}")

    try:
        final_id, _meta = custom_id_gen.generate_id(
            insurer_number=f"{insurer_number:08d}",
            symbol=symbol_text or "",
            insurance_number=insured_num_text or "",
            birth_yyyymmdd=birth_yyyymmdd or "",
            mat_dir=Path(mat_dir),
        )
    except Exception as e:
        raise RuntimeError(
            f"person_id_custom 生成失敗: file={file} line={line_no} ({e})"
        ) from e

    if not final_id:
        raise RuntimeError(f"person_id_custom 空出力: file={file} line={line_no}")
    return final_id

# ===== DB存在 & スキーマ存在チェック =====
def require_db_and_schema(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise FileNotFoundError(f"DBが見つかりません: {db_path}")
    con = sqlite3.connect(f"file:{db_path.as_posix()}?mode=rw", uri=True)
    cur = con.cursor()
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='staging_subscribers_hub'")
    if cur.fetchone() is None:
        con.close()
        raise RuntimeError(f"必須テーブル 'staging_subscribers_hub' がありません: {db_path}")
    # カラムチェック（警告のみ）
    cur.execute("PRAGMA table_info(staging_subscribers_hub)")
    have = {row[1] for row in cur.fetchall()}
    missing = sorted(REQUIRED_STAGING_COLUMNS - have)
    if missing:
        print(f"[WARN] staging_subscribers_hub に不足カラム: {missing}", file=sys.stderr)
    return con

# ===== 実行管理（etl_runs / etl_errors） =====
RUNS_DDL = """
CREATE TABLE IF NOT EXISTS etl_runs (
  run_id        INTEGER PRIMARY KEY AUTOINCREMENT,
  phase         TEXT NOT NULL CHECK (phase IN ('import','apply')),
  source        TEXT NOT NULL,
  status        TEXT NOT NULL DEFAULT 'running'
               CHECK (status IN ('running','success','partial','failed')),
  started_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now','localtime')),
  finished_at   TEXT,
  db_path       TEXT,
  input_base    TEXT,
  input_file    TEXT,
  insurer_number TEXT,
  dry_run       INTEGER,
  limit_rows    INTEGER,
  files         INTEGER DEFAULT 0,
  rows_seen     INTEGER DEFAULT 0,
  rows_inserted INTEGER DEFAULT 0,
  rows_updated  INTEGER DEFAULT 0,
  rows_skipped  INTEGER DEFAULT 0,
  errors        INTEGER DEFAULT 0,
  notes         TEXT
);
CREATE INDEX IF NOT EXISTS ix_etl_runs_phase_time ON etl_runs(phase, started_at DESC);
"""
ERRORS_DDL = """
CREATE TABLE IF NOT EXISTS etl_errors (
  error_id        INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id          INTEGER,
  phase           TEXT NOT NULL CHECK (phase IN ('import','apply')),
  source          TEXT NOT NULL,
  insurer_number  TEXT,
  src_file        TEXT,
  src_row_no      INTEGER,
  src_line_no     INTEGER,
  staging_rowid   INTEGER,
  person_id_custom TEXT,
  field           TEXT,
  field_value     TEXT,
  error_code      TEXT,
  message         TEXT,
  created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now','localtime'))
);
CREATE INDEX IF NOT EXISTS ix_etl_errors_run ON etl_errors(run_id, created_at);
CREATE INDEX IF NOT EXISTS ix_etl_errors_src ON etl_errors(src_file, src_line_no);
"""

def ensure_run_and_error_tables(cur: sqlite3.Cursor):
    cur.executescript(RUNS_DDL + ERRORS_DDL)

def start_run(cur: sqlite3.Cursor, *, input_base:str, input_file:str|None,
              insurer_number:str|None, db_path:str, dry_run:int, limit_rows:int|None) -> int:
    cur.execute("""INSERT INTO etl_runs
        (phase, source, status, input_base, input_file, insurer_number, db_path, dry_run, limit_rows)
        VALUES ('import','import_enrollees','running',?,?,?,?,?,?)""",
        (input_base, input_file, insurer_number, db_path, int(dry_run), limit_rows or 0))
    return int(cur.execute("SELECT last_insert_rowid()").fetchone()[0])

def bump_metrics(cur: sqlite3.Cursor, run_id:int, *, files:int=0, rows_seen:int=0, rows_inserted:int=0, rows_skipped:int=0):
    cur.execute("""UPDATE etl_runs
      SET files=files+?, rows_seen=rows_seen+?, rows_inserted=rows_inserted+?, rows_skipped=rows_skipped+?
      WHERE run_id=?""", (files, rows_seen, rows_inserted, rows_skipped, run_id))

def add_error(cur: sqlite3.Cursor, run_id:int):
    cur.execute("UPDATE etl_runs SET errors=errors+1 WHERE run_id=?", (run_id,))

def finish_run(cur: sqlite3.Cursor, run_id:int, *, status:str, notes:str|None=None):
    cur.execute("""UPDATE etl_runs
      SET status=?, finished_at=strftime('%Y-%m-%d %H:%M:%f','now','localtime'),
          notes=COALESCE(notes,'') || ?
      WHERE run_id=?""", (status, notes or "", run_id))

def log_import_error(cur: sqlite3.Cursor, run_id:int, *, insurer:str|None, src_file:str,
                     row_no:int, line_no:int, field:str|None, field_val:str|None,
                     code:str, msg:str):
    cur.execute("""INSERT INTO etl_errors
      (run_id, phase, source, insurer_number, src_file, src_row_no, src_line_no,
       field, field_value, error_code, message)
      VALUES (?, 'import','import_enrollees',?,?,?,?,?,?,?,?)""",
      (run_id, insurer, src_file, row_no, line_no, field, field_val, code, msg))
    add_error(cur, run_id)

# ===== 保険者番号 = フォルダ名解決 =====
def resolve_insurer_from_dir(dir_path: Path) -> int:
    d = "".join(ch for ch in dir_path.name if ch.isdigit())
    if len(d) != 8:
        raise ValueError(f"フォルダ名から8桁の保険者番号を取得できません: {dir_path}")
    iv = int(d)
    if not (0 <= iv <= 99999999):
        raise ValueError(f"保険者番号が範囲外: {iv}（dir={dir_path}）")
    return iv

# ===== 対象ディレクトリ列挙 =====
def list_target_dirs(input_arg: Optional[str], base_dir: Path) -> List[Path]:
    if input_arg:
        p = Path(input_arg)
        if not p.is_dir():
            raise NotADirectoryError(f"--input がディレクトリではありません: {p}")
        return [p]
    if not base_dir.exists():
        raise FileNotFoundError(f"ベースフォルダが見つかりません: {base_dir}")
    dirs = [d for d in base_dir.iterdir() if d.is_dir() and d.name.isdigit() and len(d.name) == 8]
    if not dirs:
        raise RuntimeError(f"8桁フォルダが見つかりません: {base_dir}")
    return sorted(dirs)

# ===== CSVフォルダ処理（staging へ INSERT） =====
def process_csv_dir(cur: sqlite3.Cursor, run_id:int, insurer_number: int, folder: Path,
                    limit: int, dry_run: bool, custom_id_script: Path, mat_dir: Path) -> Tuple[int,int,int,int]:
    """
    returns: (files, total_rows, inserted, skipped)
    """
    csv_files = sorted(folder.glob("*.csv"))
    if not csv_files:
        print(f"[WARN] CSVが見つかりません: {folder}", file=sys.stderr)
        return (0, 0, 0, 0)

    total_rows = 0
    inserted = 0
    skipped = 0

    for csv_path in csv_files:
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
            rdr = csv.DictReader(f)
            line_no = 1  # header=1
            csv_row_no = 0
            for row in rdr:
                line_no += 1
                csv_row_no += 1
                total_rows += 1

                try:
                    # --- ヘッダーマッピング（未知キーはそのまま） ---
                    src = {MAP.get(k, k): (row.get(k, "") or "") for k in row.keys()}

                    # --- 必須の正規化 ---
                    insurance_number_text = only_digits_text_required(
                        src.get("insurance_number",""),
                        field="insurance_number", file=csv_path.name, line_no=line_no
                    )
                    branchnum_text = only_digits_text_or_none(src.get("insurance_branchnumber",""))
                    birth = norm_birth_required(
                        src.get("birth","") or row.get("生年月日",""),
                        file=csv_path.name, line_no=line_no
                    )
                    gender_code_txt = norm_gender_to_code_text(src.get("gender_code","") or row.get("性別",""))

                    # 記号：半角主体に正規化 + 数字抽出
                    insurance_symbol_norm, sym_digits = normalize_insurance_symbol_for_db(src.get("insurance_symbol",""))

                    # 氏名（漢字）
                    kanji_full_raw = (src.get("name_kanji_full","") or row.get("対象者氏名（漢字）","")).strip()
                    kfam, kmid, kgiv = split_name_by_space(kanji_full_raw)

                    # 氏名カナ
                    kana_full_raw = (src.get("name_kana_full","") or row.get("対象者氏名（カナ）","")).strip()
                    if kana_full_raw == "":
                        raise ValueError(f"必須フィールド欠損: name_kana_full file={csv_path.name} line={line_no}")
                    kf, km, kg = split_name_by_space(kana_full_raw)
                    kf = normalize_kana_token_fullwidth(kf)
                    km = normalize_kana_token_fullwidth(km)
                    kg = normalize_kana_token_fullwidth(kg)
                    kana_full_norm = normalize_name_kana_fullwidth_no_space(kana_full_raw)

                    # person_id_custom 生成
                    pid = gen_custom_id(
                        insurer_number=insurer_number,
                        symbol_text=insurance_symbol_norm,
                        insured_num_text=insurance_number_text,
                        birth_yyyymmdd=birth,
                        file=csv_path.name, line_no=line_no,
                        custom_id_script=Path(custom_id_script),
                        mat_dir=Path(mat_dir),
                    )

                    # --- INSERT (staging_subscribers_hub) ---
                    cols = (
                        "person_id_custom, "
                        "name_kana_full, name_kanji_full, "
                        "name_kanji_family, name_kanji_middle, name_kanji_given, "
                        "name_kana_family, name_kana_middle, name_kana_given, "
                        "gender_code, birth, "
                        "insured_attribute_name, relationship_name, "
                        "insurer_number, insurance_symbol, insurance_symbol_digits, "
                        "insurance_number, insurance_branchnumber, "
                        "qualification_acquired_date, qualification_lost_date, "
                        "postal_code, address_line, building, "
                        "phone, email, "
                        "employer_code, department_code, distribution_code, "
                        "employee_code, connect_id, "
                        "created_at, loaded_at, processed_at, "
                        "src_file, src_row_no, src_line_no, import_run_id"
                    )
                    qmarks = (
                        ":person_id_custom, "
                        ":name_kana_full, :name_kanji_full, "
                        ":name_kanji_family, :name_kanji_middle, :name_kanji_given, "
                        ":name_kana_family, :name_kana_middle, :name_kana_given, "
                        ":gender_code, :birth, "
                        ":insured_attribute_name, :relationship_name, "
                        ":insurer_number, :insurance_symbol, :insurance_symbol_digits, "
                        ":insurance_number, :insurance_branchnumber, "
                        ":qualification_acquired_date, :qualification_lost_date, "
                        ":postal_code, :address_line, :building, "
                        ":phone, :email, "
                        ":employer_code, :department_code, :distribution_code, "
                        ":employee_code, :connect_id, "
                        "strftime('%Y-%m-%d %H:%M:%f','now','localtime'), "
                        "strftime('%Y-%m-%d %H:%M:%f','now','localtime'), "
                        "NULL, "
                        ":src_file, :src_row_no, :src_line_no, :import_run_id"
                    )

                    vals = {
                        "person_id_custom": pid,
                        "name_kana_full": kana_full_norm,
                        "name_kanji_full": kanji_full_raw,
                        "name_kanji_family": kfam, "name_kanji_middle": kmid, "name_kanji_given": kgiv,
                        "name_kana_family": kf,  "name_kana_middle": km,   "name_kana_given": kg,
                        "gender_code": gender_code_txt,
                        "birth":       birth,
                        "insured_attribute_name": src.get("insured_attribute_name",""),
                        "relationship_name":      src.get("relationship_name",""),
                        "insurer_number": f"{insurer_number:08d}",
                        "insurance_symbol": insurance_symbol_norm,
                        "insurance_symbol_digits": sym_digits,
                        "insurance_number": insurance_number_text,
                        "insurance_branchnumber": branchnum_text,
                        "qualification_acquired_date": src.get("qualification_acquired_date",""),
                        "qualification_lost_date":     src.get("qualification_lost_date",""),
                        "postal_code": src.get("postal_code",""),
                        "address_line": src.get("address_line",""),
                        "building":     src.get("building",""),
                        "phone": src.get("phone",""),
                        "email": src.get("email",""),
                        "employer_code":   src.get("employer_code",""),
                        "department_code": src.get("department_code",""),
                        "distribution_code": src.get("distribution_code",""),
                        "employee_code": src.get("employee_code",""),
                        "connect_id": src.get("connect_id",""),
                        "src_file": csv_path.name,
                        "src_row_no": csv_row_no,
                        "src_line_no": line_no,
                        "import_run_id": run_id,
                    }

                    if not dry_run:
                        cur.execute(f"INSERT INTO staging_subscribers_hub ({cols}) VALUES ({qmarks})", vals)
                    inserted += 1

                except Exception as e:
                    log_import_error(
                        cur, run_id,
                        insurer=f"{insurer_number:08d}",
                        src_file=csv_path.name, row_no=csv_row_no, line_no=line_no,
                        field=None, field_val=None, code=type(e).__name__, msg=str(e)
                    )
                    skipped += 1

                # ---- 進捗ログ ----
                if PROGRESS_EVERY_ROWS and (total_rows % PROGRESS_EVERY_ROWS == 0):
                    print(
                        f"[PROGRESS] insurer={insurer_number:08d} "
                        f"file={csv_path.name} "
                        f"rows_seen={total_rows} inserted={inserted} skipped={skipped}",
                        flush=True,
                    )

                # limit 到達チェック（insert/skip 合算）
                if limit and (inserted + skipped) >= limit:
                    break

        # ファイル単位でメトリクス加算
        bump_metrics(cur, run_id, files=1, rows_seen=total_rows, rows_inserted=inserted, rows_skipped=skipped)
        if limit and (inserted + skipped) >= limit:
            break

    return (len(csv_files), total_rows, inserted, skipped)

# ===== メイン =====
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", help="単一の8桁フォルダを指定。省略時は --base 直下の全8桁フォルダを処理")
    ap.add_argument("--base", default=str(DEFAULT_BASE), help="8桁フォルダが並ぶベース（env HUB_INPUT_BASE 優先）")
    ap.add_argument("--db",   default=str(DEFAULT_DB),   help="DBパス（優先度: --db > env HUB_DB_PATH > APP_ENV=stg|prod）")
    ap.add_argument("--custom-id", default=str(DEFAULT_LIB), help="custom_id_gen.py のパス（既定: <work_folder>/lib/custom_id_gen.py）")
    ap.add_argument("--mat",  default=str(DEFAULT_MAT),  help="mat ディレクトリ（既定: <work_folder>/mat）")
    ap.add_argument("--dry-run", type=int, default=0)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    base_dir = Path(args.base)
    db_path = Path(args.db)

    print(f"[INFO] APP_ENV = {_APP_ENV}")
    print(f"[INFO] HUB_DB_PATH = {os.getenv('HUB_DB_PATH')}")
    print(f"[INFO] DB = {db_path.resolve()}")
    print(f"[INFO] INPUT_BASE = {base_dir.resolve()}")
    print(f"[INFO] custom_id_gen = {Path(args.custom_id).resolve()}")
    print(f"[INFO] mat = {Path(args.mat).resolve()}")

    target_dirs = list_target_dirs(args.input, base_dir)
    con = require_db_and_schema(db_path)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # 実行管理テーブル
    ensure_run_and_error_tables(cur)
    run_id = start_run(
        cur,
        input_base=str(base_dir),
        input_file=(target_dirs[0].name if len(target_dirs)==1 else None),
        insurer_number=None,
        db_path=str(db_path),
        dry_run=int(args.dry_run),
        limit_rows=int(args.limit),
    )

    grand_files = grand_rows = grand_inserted = grand_skipped = 0
    try:
        for folder in target_dirs:
            insurer_number = resolve_insurer_from_dir(folder)
            files, rows, ins, skip = process_csv_dir(
                cur=cur, run_id=run_id,
                insurer_number=insurer_number,
                folder=folder,
                limit=int(args.limit),
                dry_run=bool(args.dry_run),
                custom_id_script=Path(args.custom_id),
                mat_dir=Path(args.mat),
            )
            grand_files += files; grand_rows += rows
            grand_inserted += ins; grand_skipped += skip
            print(f"[OK] insurer={insurer_number:08d} folder={folder} files={files} rows={rows} inserted={ins} skipped={skip}")
            if args.limit and (grand_inserted + grand_skipped) >= args.limit:
                break

        status = 'success' if grand_skipped == 0 else ('partial' if grand_inserted > 0 else 'failed')
        finish_run(cur, run_id, status=status,
                   notes=f"files={grand_files}, rows={grand_rows}, inserted={grand_inserted}, skipped={grand_skipped}")
        if not args.dry_run:
            con.commit()
        else:
            con.rollback()
        print(f"[DONE] dirs={len(target_dirs)} total_files={grand_files} total_rows={grand_rows} "
              f"inserted={grand_inserted} skipped={grand_skipped} run_id={run_id} db={db_path}")
    except Exception as e:
        finish_run(cur, run_id, status='failed', notes=str(e))
        con.rollback()
        print(f"[ERR] 取込失敗: {e}")
        con.close()
        return 7

    con.close()
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
