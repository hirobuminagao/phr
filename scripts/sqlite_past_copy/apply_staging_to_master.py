# -*- coding: utf-8 -*-
r"""
apply_staging_to_master.py  （staging→master 反映／住所ロジック外出し／実行管理）

概要:
- VS Code の「Run ▶」からそのまま実行できるように、work_folder/.env を自動読込
- DB 接続先は環境変数で決定（レガシーへは戻らない）:
    1) HUB_DB_PATH があれば **強制採用**
    2) なければ APP_ENV=stg|prod で固定パスを採用
       - stg : <work_folder>/db/stg/hub_stg.sqlite
       - prod: <work_folder>/db/prod/hub_prod.sqlite
- 実行時に選択されたパス等を [INFO] ログで表示

処理内容:
- audit_context.source を 'apply_staging' に設定（監査トリガで使用）
- staging_subscribers_hub を読み、subscribers へ UPSERT
  - 照合キー: (person_id_custom, name_kana_full, gender_code)
  - INSERT/UPDATE 時に created_at/updated_at/last_change_run_id を管理
  - relationship_name（続柄名称）を staging の値でそのまま反映
  - 企業系フィールド（employer_code, department_code, distribution_code, employee_code, connect_id）も反映
  - 資格日付（qualification_acquired_date, qualification_lost_date）も反映（YYYY-MM-DD へ軽整形）
- 住所・連絡先は「現用(is_current=1)のみ保持」ポリシー
  - 差分があれば現用を終了(valid_to=当日)→新規現用を追加
  - 住所の都道府県推定は lib/address_resolver.resolve_prefecture() を使用
    - 判定できたら address_line は「都道府県を除いた残り」を保存
- 成功した staging 行には processed_run_id / processed_at を刻印
  - processed_run_id には apply 実行の run_id を入れる
  - これにより「processed_run_id IS NULL の行だけ apply 対象」にできる
- 実行管理: etl_runs に run を記録、行レベルの例外は etl_errors に run_id 紐付で保存
- 任意の書込み停止フラグ _write_lock.enabled=1 があれば中断

前提:
- subscribers / subscriber_addresses / subscriber_contacts / prefectures が定義済み
- subscribers に last_change_run_id カラムが存在
- staging_subscribers_hub に必要列が存在（import_run_id, processed_run_id, processed_at など）
- 監査トリガ（trg_subscribers_audit_all など）が必要に応じて有効
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path
from typing import Optional

# ===== .env を常時自動読込（work_folder/.env） =====
PKG_ROOT = Path(__file__).resolve().parents[1]  # => .../work_folder
_ENV_PATH = PKG_ROOT / ".env"

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

# ====== パス解決 & 住所ライブラリの import ======
SCRIPT_DIR = Path(__file__).resolve().parent
if str(PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(PKG_ROOT))
from lib.address_resolver import resolve_prefecture  # work_folder/lib/address_resolver.py

# --- HUB_DB_PATH or APP_ENV で自動切替（レガシーへは戻らない） ---
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

DEFAULT_DB = _resolve_default_db()

# ====== 汎用ユーティリティ ======
def get_now(cur: sqlite3.Cursor) -> str:
    return cur.execute("SELECT strftime('%Y-%m-%d %H:%M:%f','now','localtime')").fetchone()[0]

def as_int_or_none(x) -> Optional[int]:
    if x is None:
        return None
    try:
        s = str(x).strip()
        return int(s) if s != "" else None
    except Exception:
        return None

def nz(s: Optional[str]) -> str:
    return "" if s is None else str(s)

def row_differs(old_row: tuple, new_row: tuple) -> bool:
    return old_row != new_row

def row_get(r: sqlite3.Row, key: str):
    try:
        return r[key]
    except Exception:
        return None

def norm_ymd_or_keep(s: Optional[str]) -> Optional[str]:
    """YYYYMMDD -> YYYY-MM-DD にする軽整形。空/NoneはNone。その他はそのまま。"""
    if not s:
        return None
    t = str(s).strip()
    if len(t) == 8 and t.isdigit():
        return f"{t[0:4]}-{t[4:6]}-{t[6:8]}"
    return t

def canon(v) -> str:
    """差分比較用: None/空/数値を全部文字列にそろえる"""
    if v is None:
        return ""
    return str(v)

# ====== DB接続 ======
def connect_db(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise FileNotFoundError(f"DBが見つかりません: {db_path}")
    con = sqlite3.connect(f"file:{db_path.as_posix()}?mode=rw", uri=True)
    con.row_factory = sqlite3.Row
    return con

# ====== 監査コンテキスト ======
def ensure_audit_context(cur: sqlite3.Cursor, source: str) -> None:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS audit_context(
            id INTEGER PRIMARY KEY,
            source TEXT
        )
    """)
    cur.execute("""
        INSERT INTO audit_context(id, source)
        VALUES(1, ?)
        ON CONFLICT(id) DO UPDATE SET source=excluded.source
    """, (source,))

# ====== 実行管理（etl_runs / etl_errors） ======
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

def start_run_apply(cur: sqlite3.Cursor, *, db_path:str, dry_run:int, limit_rows:int|None) -> int:
    cur.execute("""INSERT INTO etl_runs
       (phase, source, status, db_path, dry_run, limit_rows)
       VALUES ('apply','apply_staging','running',?,?,?)""",
       (db_path, int(dry_run), limit_rows or 0))
    return int(cur.execute("SELECT last_insert_rowid()").fetchone()[0])

def add_error(cur: sqlite3.Cursor, run_id:int):
    cur.execute("UPDATE etl_runs SET errors=errors+1 WHERE run_id=?", (run_id,))

def finish_run(cur: sqlite3.Cursor, run_id:int, *, status:str,
               rows_inserted:int=0, rows_updated:int=0):
    cur.execute("""UPDATE etl_runs
        SET status=?,
            finished_at=strftime('%Y-%m-%d %H:%M:%f','now','localtime'),
            rows_inserted=rows_inserted+?,
            rows_updated =rows_updated +?
        WHERE run_id=?""", (status, rows_inserted, rows_updated, run_id))

def log_apply_error(cur: sqlite3.Cursor, run_id:int, *, srow: sqlite3.Row, err: Exception):
    cur.execute("""INSERT INTO etl_errors
        (run_id, phase, source, insurer_number, src_file, src_row_no, src_line_no,
         staging_rowid, person_id_custom, error_code, message)
        VALUES (?, 'apply','apply_staging', ?, ?, ?, ?, ?, ?, ?, ?)""",
        (run_id,
         row_get(srow, "insurer_number"),
         row_get(srow, "src_file"), row_get(srow, "src_row_no"), row_get(srow, "src_line_no"),
         row_get(srow, "stg_rowid"), row_get(srow, "person_id_custom"),
         type(err).__name__, str(err)))
    add_error(cur, run_id)

# ====== subscribers UPSERT ======
SUBSCRIBER_COLS = (
    "insurer_number, insurance_symbol, insurance_symbol_digits, "
    "insurance_number, insurance_branchnumber, "
    "birth, gender_code, "
    "name_kana_full, name_kanji_full, "
    "name_kanji_family, name_kanji_middle, name_kanji_given, "
    "name_kana_family, name_kana_middle, name_kana_given, "
    "relationship_name, "
    "qualification_acquired_date, qualification_lost_date, "
    "employer_code, department_code, distribution_code, employee_code, connect_id, "
    "person_id_custom"
)

# 比較対象のカラムと valsキーの対応表
COMPARE_COLS = [
    ("insurer_number",              "insurer_number"),
    ("insurance_symbol",            "insurance_symbol"),
    ("insurance_symbol_digits",     "insurance_symbol_digits"),
    ("insurance_number",            "insurance_number"),
    ("insurance_branchnumber",      "insurance_branchnumber"),
    ("birth",                       "birth"),
    ("gender_code",                 "gender_code"),
    ("name_kana_full",              "name_kana_full"),
    ("name_kanji_full",             "name_kanji_full"),
    ("name_kanji_family",           "name_kanji_family"),
    ("name_kanji_middle",           "name_kanji_middle"),
    ("name_kanji_given",            "name_kanji_given"),
    ("name_kana_family",            "name_kana_family"),
    ("name_kana_middle",            "name_kana_middle"),
    ("name_kana_given",             "name_kana_given"),
    ("relationship_name",           "relationship_name"),
    ("qualification_acquired_date", "qualification_acquired_date"),
    ("qualification_lost_date",     "qualification_lost_date"),
    ("employer_code",               "employer_code"),
    ("department_code",             "department_code"),
    ("distribution_code",           "distribution_code"),
    ("employee_code",               "employee_code"),
    ("connect_id",                  "connect_id"),
    ("person_id_custom",            "person_id_custom"),
]

def find_subscriber_row(cur: sqlite3.Cursor, person_id_custom: str,
                        name_kana_full: str, gender_code: int|None) -> Optional[sqlite3.Row]:
    return cur.execute("""
        SELECT *
        FROM subscribers
        WHERE person_id_custom = ?
          AND name_kana_full   = ?
          AND gender_code IS ?
        LIMIT 1
    """, (person_id_custom, name_kana_full, gender_code)).fetchone()

def subscriber_differs(existing: sqlite3.Row, vals: dict) -> bool:
    old_tuple = []
    new_tuple = []
    for col, key in COMPARE_COLS:
        old_tuple.append(canon(existing[col]))
        new_tuple.append(canon(vals.get(key)))
    return tuple(old_tuple) != tuple(new_tuple)

def insert_subscriber(cur: sqlite3.Cursor, vals: dict, run_id: int) -> int:
    now = get_now(cur)
    cur.execute(f"""
        INSERT INTO subscribers ({SUBSCRIBER_COLS}, created_at, updated_at, last_change_run_id)
        VALUES (:insurer_number, :insurance_symbol, :insurance_symbol_digits,
                :insurance_number, :insurance_branchnumber,
                :birth, :gender_code,
                :name_kana_full, :name_kanji_full,
                :name_kanji_family, :name_kanji_middle, :name_kanji_given,
                :name_kana_family, :name_kana_middle, :name_kana_given,
                :relationship_name,
                :qualification_acquired_date, :qualification_lost_date,
                :employer_code, :department_code, :distribution_code, :employee_code, :connect_id,
                :person_id_custom,
                :created_at, NULL, :last_change_run_id)
    """, {**vals, "created_at": now, "last_change_run_id": run_id})
    sid = cur.execute("SELECT last_insert_rowid()").fetchone()[0]
    return int(sid)

def update_subscriber(cur: sqlite3.Cursor, subscriber_id: int, vals: dict, run_id: int) -> None:
    now = get_now(cur)
    cur.execute("""
        UPDATE subscribers SET
            insurer_number = :insurer_number,
            insurance_symbol = :insurance_symbol,
            insurance_symbol_digits = :insurance_symbol_digits,
            insurance_number = :insurance_number,
            insurance_branchnumber = :insurance_branchnumber,
            birth = :birth,
            gender_code = :gender_code,
            name_kana_full = :name_kana_full,
            name_kanji_full = :name_kanji_full,
            name_kanji_family = :name_kanji_family,
            name_kanji_middle = :name_kanji_middle,
            name_kanji_given = :name_kanji_given,
            name_kana_family = :name_kana_family,
            name_kana_middle = :name_kana_middle,
            name_kana_given = :name_kana_given,
            relationship_name = :relationship_name,
            qualification_acquired_date = :qualification_acquired_date,
            qualification_lost_date     = :qualification_lost_date,
            employer_code = :employer_code,
            department_code = :department_code,
            distribution_code = :distribution_code,
            employee_code = :employee_code,
            connect_id = :connect_id,
            person_id_custom = :person_id_custom,
            updated_at = :updated_at,
            last_change_run_id = :last_change_run_id
        WHERE id = :id
    """, {**vals, "id": subscriber_id, "updated_at": now, "last_change_run_id": run_id})

# ====== 住所の現用差し替え ======
def get_current_address(cur: sqlite3.Cursor, subscriber_id: int) -> Optional[sqlite3.Row]:
    return cur.execute("""
        SELECT address_id AS aid, postal_code, prefecture_code, prefecture, city, address_line, building
        FROM subscriber_addresses
        WHERE subscriber_id = ? AND is_current = 1
        LIMIT 1
    """, (subscriber_id,)).fetchone()

def end_current_address(cur: sqlite3.Cursor, subscriber_id: int) -> None:
    cur.execute("""
        UPDATE subscriber_addresses
        SET is_current = 0, valid_to = date('now','localtime')
        WHERE subscriber_id = ? AND is_current = 1
    """, (subscriber_id,))

def insert_address(cur: sqlite3.Cursor, subscriber_id: int,
                   postal_code: Optional[str], address_line: Optional[str], building: Optional[str]) -> None:
    pref = resolve_prefecture(postal_code, address_line, cur.connection)
    clean_line = pref["rest_address"] if pref["prefecture_name"] else address_line
    cur.execute("""
        INSERT INTO subscriber_addresses(
            subscriber_id, postal_code, prefecture_code, prefecture, city,
            address_line, building,
            is_current, valid_from, valid_to, source, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, NULL,
                ?, ?,
                1, date('now','localtime'), NULL, 'apply_staging',
                strftime('%Y-%m-%d %H:%M:%f','now','localtime'), NULL)
    """, (
        subscriber_id, postal_code,
        pref["prefecture_code"], pref["prefecture_name"],
        clean_line, building
    ))

def address_apply(cur: sqlite3.Cursor, subscriber_id: int,
                  postal_code: Optional[str], address_line: Optional[str], building: Optional[str]) -> None:
    if all(nz(x) == "" for x in (postal_code, address_line, building)):
        return
    pref = resolve_prefecture(postal_code, address_line, cur.connection)
    clean_line = pref["rest_address"] if pref["prefecture_name"] else address_line
    cur_row = get_current_address(cur, subscriber_id)
    new_tuple = (nz(postal_code), pref["prefecture_code"], pref["prefecture_name"], nz(clean_line), nz(building))
    if cur_row is None:
        insert_address(cur, subscriber_id, postal_code, address_line, building)
        return
    old_tuple = (nz(cur_row["postal_code"]), cur_row["prefecture_code"], cur_row["prefecture"],
                 nz(cur_row["address_line"]), nz(cur_row["building"]))
    if row_differs(old_tuple, new_tuple):
        end_current_address(cur, subscriber_id)
        insert_address(cur, subscriber_id, postal_code, address_line, building)

# ====== 連絡先の現用差し替え ======
def get_current_contact(cur: sqlite3.Cursor, subscriber_id: int) -> Optional[sqlite3.Row]:
    return cur.execute("""
        SELECT contact_id AS cid, phone, email
        FROM subscriber_contacts
        WHERE subscriber_id = ? AND is_current = 1
        LIMIT 1
    """, (subscriber_id,)).fetchone()

def end_current_contact(cur: sqlite3.Cursor, subscriber_id: int) -> None:
    cur.execute("""
        UPDATE subscriber_contacts
        SET is_current = 0, valid_to = date('now','localtime')
        WHERE subscriber_id = ? AND is_current = 1
    """, (subscriber_id,))

def insert_contact(cur: sqlite3.Cursor, subscriber_id: int,
                   phone: Optional[str], email: Optional[str]) -> None:
    cur.execute("""
        INSERT INTO subscriber_contacts(
            subscriber_id, phone, email, is_current, valid_from, valid_to, source, created_at, updated_at
        )
        VALUES (?, ?, ?, 1, date('now','localtime'), NULL, 'apply_staging',
                strftime('%Y-%m-%d %H:%M:%f','now','localtime'), NULL)
    """, (subscriber_id, phone, email))

def contact_apply(cur: sqlite3.Cursor, subscriber_id: int,
                  phone: Optional[str], email: Optional[str]) -> None:
    if nz(phone) == "" and nz(email) == "":
        return
    cur_row = get_current_contact(cur, subscriber_id)
    new_tuple = (nz(phone), nz(email))
    if cur_row is None:
        insert_contact(cur, subscriber_id, phone, email)
        return
    old_tuple = (nz(cur_row["phone"]), nz(cur_row["email"]))
    if row_differs(old_tuple, new_tuple):
        end_current_contact(cur, subscriber_id)
        insert_contact(cur, subscriber_id, phone, email)

# ====== 1行適用 ======
def apply_once(cur: sqlite3.Cursor, srow: sqlite3.Row, run_id: int) -> str:
    """
    return: 'insert' | 'update' | 'noop'
    """
    vals = {
        "insurer_number":              as_int_or_none(srow["insurer_number"]),
        "insurance_symbol":            srow["insurance_symbol"],
        "insurance_symbol_digits":     as_int_or_none(srow["insurance_symbol_digits"]),
        "insurance_number":            srow["insurance_number"],
        "insurance_branchnumber":      srow["insurance_branchnumber"],
        "birth":                       srow["birth"],
        "gender_code":                 as_int_or_none(srow["gender_code"]),
        "name_kana_full":              srow["name_kana_full"],
        "name_kanji_full":             srow["name_kanji_full"],
        "name_kanji_family":           srow["name_kanji_family"],
        "name_kanji_middle":           srow["name_kanji_middle"],
        "name_kanji_given":            srow["name_kanji_given"],
        "name_kana_family":            srow["name_kana_family"],
        "name_kana_middle":            srow["name_kana_middle"],
        "name_kana_given":             srow["name_kana_given"],
        "relationship_name":           (srow["relationship_name"] or None),
        "qualification_acquired_date": norm_ymd_or_keep(srow["qualification_acquired_date"]),
        "qualification_lost_date":     norm_ymd_or_keep(srow["qualification_lost_date"]),
        "employer_code":               srow["employer_code"],
        "department_code":             srow["department_code"],
        "distribution_code":           srow["distribution_code"],
        "employee_code":               srow["employee_code"],
        "connect_id":                  srow["connect_id"],
        "person_id_custom":            srow["person_id_custom"],
    }

    existing = find_subscriber_row(
        cur,
        vals["person_id_custom"],
        vals["name_kana_full"],
        vals["gender_code"]
    )

    if existing is None:
        sid = insert_subscriber(cur, vals, run_id)
        # sid は今後の address/contact 用なので使っておく
        address_apply(cur, sid, srow["postal_code"], srow["address_line"], srow["building"])
        contact_apply(cur, sid, srow["phone"], srow["email"])
        return "insert"

    # 既存レコードがある → 差分チェック
    if subscriber_differs(existing, vals):
        sid = int(existing["id"])
        update_subscriber(cur, sid, vals, run_id)
        address_apply(cur, sid, srow["postal_code"], srow["address_line"], srow["building"])
        contact_apply(cur, sid, srow["phone"], srow["email"])
        return "update"

    # フィールド値に差分なし → last_change_run_id も触らない
    # ただし住所・連絡先側は staging と差分があれば入れ替えるので、そのまま呼ぶ
    sid = int(existing["id"])
    address_apply(cur, sid, srow["postal_code"], srow["address_line"], srow["building"])
    contact_apply(cur, sid, srow["phone"], srow["email"])
    return "noop"

# ====== メイン ======
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(DEFAULT_DB), help="DBパス（優先度: --db > env HUB_DB_PATH > APP_ENV=stg|prod）")
    ap.add_argument("--limit", type=int, default=0, help="0=無制限")
    ap.add_argument("--dry-run", type=int, default=0)
    ap.add_argument("--source", default="apply_staging")
    args = ap.parse_args()

    db_path = Path(args.db)

    print(f"[INFO] APP_ENV = {_APP_ENV}")
    print(f"[INFO] HUB_DB_PATH = {os.getenv('HUB_DB_PATH')}")
    print(f"[INFO] DB = {db_path.resolve()}")

    con = connect_db(db_path)
    cur = con.cursor()

    # 監査コンテキスト & 実行管理
    ensure_audit_context(cur, args.source)
    ensure_run_and_error_tables(cur)
    run_id = start_run_apply(cur, db_path=str(db_path), dry_run=int(args.dry_run), limit_rows=int(args.limit))

    # 書込みロック尊重（存在すれば）
    try:
        row = cur.execute("SELECT enabled FROM _write_lock WHERE id=1").fetchone()
        if row and int(row[0]) == 1:
            print("[ABORT] _write_lock.enabled=1（書込み停止中）")
            con.close()
            return 8
    except sqlite3.Error:
        pass

    # ステージング読み出し（rowid と src_* を含める）
    # ★ processed_run_id IS NULL の行だけ対象（未処理キュー扱い）
    cur.execute("""
        SELECT
            rowid AS stg_rowid,
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
            src_file, src_row_no, src_line_no,
            created_at, loaded_at, processed_at,
            import_run_id, processed_run_id
        FROM staging_subscribers_hub
        WHERE processed_run_id IS NULL
        ORDER BY rowid ASC
    """)
    rows = cur.fetchall()

    total = len(rows)
    print(f"[INFO] staging rows to apply = {total}")

    inserted = updated = errors = 0
    try:
        for i, srow in enumerate(rows, start=1):
            try:
                op = apply_once(cur, srow, run_id)
                if op == "insert":
                    inserted += 1
                elif op == "update":
                    updated += 1
                # "noop" は件数カウントなし（進捗だけ）

                # 成功刻印（列が無いDBでも落ちないよう try）
                try:
                    cur.execute("""
                        UPDATE staging_subscribers_hub
                        SET processed_run_id = ?, processed_at = strftime('%Y-%m-%d %H:%M:%f','now','localtime')
                        WHERE rowid = ?
                    """, (run_id, srow["stg_rowid"]))
                except sqlite3.Error:
                    pass

                if args.limit and (inserted + updated + errors) >= args.limit:
                    break

                if i % 1000 == 0:
                    print(f"[PROGRESS] applied {i}/{total} rows (ins={inserted}, upd={updated}, err={errors})")

            except Exception as e:
                errors += 1
                log_apply_error(cur, run_id, srow=srow, err=e)
                continue

        status = 'success' if errors == 0 else ('partial' if (inserted + updated) > 0 else 'failed')
        finish_run(cur, run_id, status=status, rows_inserted=inserted, rows_updated=updated)
        if args.dry_run:
            con.rollback()
            print(f"[DRY-RUN] inserted={inserted} updated={updated} errors={errors} run_id={run_id}")
        else:
            con.commit()
            print(f"[OK] inserted={inserted} updated={updated} errors={errors} run_id={run_id}")
    except Exception as e:
        finish_run(cur, run_id, status='failed')
        con.rollback()
        print(f"[ERR] 反映中に例外: {e}")
        con.close()
        return 7

    con.close()
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
