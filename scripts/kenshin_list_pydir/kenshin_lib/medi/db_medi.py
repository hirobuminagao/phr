# -*- coding: utf-8 -*-
"""
kenshin_lib/medi/db_medi.py

medi_* テーブル向けのDBアクセス（SQL）を集約。
scripts側はオーケストレーションに寄せる。

SAFE-GUARDS:
- enum/短い型で落ちない（stepなど）
- zip_inner_path_sha256 等 “空欄で落ちる列” を DB層で埋める（列が存在する場合）
- receipt_runs の FK 変更（xml_receipt_id）にも追随（列があれば埋める）
- スキーマ差分（列追加/未追加）に耐えるため information_schema を参照（キャッシュ）
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional, Any
import hashlib


def now_str() -> str:
    # scripts側と同等（microsecondまで）
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


# ============================================================
# schema introspection helpers (safe + cached)
# ============================================================

_SCHEMA_HAS_COL_CACHE: dict[tuple[str, str], bool] = {}
_SCHEMA_COLTYPE_CACHE: dict[tuple[str, str], Optional[str]] = {}


def db_get_column_type(cur, table: str, column: str) -> Optional[str]:
    """
    information_schema から COLUMN_TYPE を取得（キャッシュあり）。
    例: enum('A','B') / varchar(32) / text ...
    """
    key = (table, column)
    if key in _SCHEMA_COLTYPE_CACHE:
        return _SCHEMA_COLTYPE_CACHE[key]

    cur.execute(
        """
        SELECT COLUMN_TYPE
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND COLUMN_NAME = %s
        """,
        (table, column),
    )
    row = cur.fetchone()
    if not row:
        _SCHEMA_COLTYPE_CACHE[key] = None
        return None

    v = row.get("COLUMN_TYPE") if isinstance(row, dict) else None
    out = str(v) if v else None
    _SCHEMA_COLTYPE_CACHE[key] = out
    return out


def db_has_column(cur, table: str, column: str) -> bool:
    """
    information_schema で列存在確認（キャッシュあり）
    """
    key = (table, column)
    if key in _SCHEMA_HAS_COL_CACHE:
        return _SCHEMA_HAS_COL_CACHE[key]

    cur.execute(
        """
        SELECT 1
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND COLUMN_NAME = %s
        LIMIT 1
        """,
        (table, column),
    )
    ok = cur.fetchone() is not None
    _SCHEMA_HAS_COL_CACHE[key] = ok
    return ok


def _parse_enum_values(column_type: str) -> list[str]:
    """
    column_type例: "enum('WELLFORMED','CDA_INDEX',...)"
    -> ["WELLFORMED","CDA_INDEX",...]
    """
    s = (column_type or "").strip()
    if not s.lower().startswith("enum("):
        return []

    body = s[s.find("(") + 1 : s.rfind(")")]
    vals: list[str] = []

    buf = ""
    in_q = False
    esc = False
    for ch in body:
        if esc:
            buf += ch
            esc = False
            continue
        if ch == "\\":
            esc = True
            continue
        if ch == "'":
            in_q = not in_q
            if not in_q:
                vals.append(buf)
                buf = ""
            continue
        if in_q:
            buf += ch

    return vals


def _fallback_enum(value: str, enums: list[str]) -> str:
    """
    enums に value が無ければ安全な値へフォールバック。
    優先: OTHER -> UNKNOWN -> 先頭
    """
    if not enums:
        return value
    if value in enums:
        return value
    if "OTHER" in enums:
        return "OTHER"
    if "UNKNOWN" in enums:
        return "UNKNOWN"
    return enums[0]


def _guard_enum_value(cur, table: str, column: str, value: str) -> str:
    """
    column が enum の場合、許可されていない値をフォールバックする。
    enum ではない場合はそのまま返す。
    """
    col_type = db_get_column_type(cur, table, column)
    if not col_type:
        return value
    enums = _parse_enum_values(col_type)
    if not enums:
        return value
    return _fallback_enum(value, enums)


# ============================================================
# common hashing / normalization for “never NULL” columns
# ============================================================
def _sha256_text(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


def _norm_inner_path(p: str) -> str:
    # ZIP内パスは / に寄せて同一化（念のため）
    return (p or "").replace("\\", "/").lstrip("/")


def _ensure_inner_sha(inner_path: str, inner_sha: Optional[str]) -> str:
    """
    inner_sha が None/空 なら inner_path から必ず算出して返す
    """
    inner = _norm_inner_path(inner_path)
    v = (inner_sha or "").strip()
    return v if v else _sha256_text(inner)


def _clip_text(s: Optional[str], limit: int) -> Optional[str]:
    """
    想定外に長い例外文字列でDBやログが壊れないように軽く上限。
    """
    if s is None:
        return None
    t = str(s)
    return t[:limit] if len(t) > limit else t


# -----------------------------
# DB helpers (runs)
# -----------------------------
def db_insert_run(cur, input_root: str, note: Optional[str]) -> int:
    cur.execute(
        """
        INSERT INTO medi_import_runs (started_at, input_root, note)
        VALUES (%s, %s, %s)
        """,
        (now_str(), input_root, note),
    )
    return int(cur.lastrowid)


def db_finish_run(cur, run_id: int, note: Optional[str]) -> None:
    cur.execute(
        """
        UPDATE medi_import_runs
        SET finished_at=%s, note=%s
        WHERE run_id=%s
        """,
        (now_str(), note, run_id),
    )


# -----------------------------
# DB helpers (ZIP)
# -----------------------------
def db_get_zip_receipt_id_by_sha(cur, zip_sha256: str) -> Optional[int]:
    cur.execute("SELECT zip_receipt_id FROM medi_zip_receipts WHERE zip_sha256=%s", (zip_sha256,))
    row = cur.fetchone()
    return int(row["zip_receipt_id"]) if row else None


def db_get_zip_receipt_row_by_sha(cur, zip_sha256: str) -> Optional[dict]:
    """
    xml_extract が必要とする情報一式を返す（zip_path, zip_receipt_id, facility情報など）
    """
    cur.execute(
        """
        SELECT
          zip_receipt_id,
          facility_folder_name, facility_code, facility_name,
          zip_name, zip_path, zip_sha256
        FROM medi_zip_receipts
        WHERE zip_sha256=%s
        """,
        (zip_sha256,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def db_upsert_zip_receipt(
    cur,
    *,
    run_id: int,
    facility_folder_name: str,
    facility_code: str,
    facility_name: str,
    zip_name: str,
    zip_path: str,
    zip_sha256: str,
    structure_status: str,
    error_code: Optional[str],
    structure_message: Optional[str],
    data_dir_count: Optional[int],
    data_xml_count: Optional[int],
    # ★今回追加: ZIP側エラー詳細（列があれば格納）
    error_message: Optional[str] = None,
) -> int:
    """
    既存仕様に追加で error_message を扱う。
    ただし環境差分があり得るので、列が存在する場合のみ INSERT/UPDATE に含める。
    """
    has_err_msg = db_has_column(cur, "medi_zip_receipts", "error_message")
    error_message = _clip_text(error_message, 8000)

    if has_err_msg:
        cur.execute(
            """
            INSERT INTO medi_zip_receipts
            (
              run_id,
              first_seen_run_id, first_seen_at,
              last_seen_run_id,  last_seen_at,
              facility_folder_name, facility_code, facility_name,
              zip_name, zip_path, zip_sha256,
              structure_status, error_code, error_message, structure_message, data_dir_count, data_xml_count
            )
            VALUES
            (
              %s,
              %s, CURRENT_TIMESTAMP(6),
              %s, CURRENT_TIMESTAMP(6),
              %s, %s, %s,
              %s, %s, %s,
              %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
              run_id=VALUES(run_id),
              last_seen_run_id=VALUES(last_seen_run_id),
              last_seen_at=CURRENT_TIMESTAMP(6),
              facility_folder_name=VALUES(facility_folder_name),
              facility_code=VALUES(facility_code),
              facility_name=VALUES(facility_name),
              zip_name=VALUES(zip_name),
              zip_path=VALUES(zip_path),
              structure_status=VALUES(structure_status),
              error_code=VALUES(error_code),
              error_message=VALUES(error_message),
              structure_message=VALUES(structure_message),
              data_dir_count=VALUES(data_dir_count),
              data_xml_count=VALUES(data_xml_count),
              zip_receipt_id=LAST_INSERT_ID(zip_receipt_id)
            """,
            (
                run_id,
                run_id,
                run_id,
                facility_folder_name,
                facility_code,
                facility_name,
                zip_name,
                zip_path,
                zip_sha256,
                structure_status,
                error_code,
                error_message,
                structure_message,
                data_dir_count,
                data_xml_count,
            ),
        )
    else:
        cur.execute(
            """
            INSERT INTO medi_zip_receipts
            (
              run_id,
              first_seen_run_id, first_seen_at,
              last_seen_run_id,  last_seen_at,
              facility_folder_name, facility_code, facility_name,
              zip_name, zip_path, zip_sha256,
              structure_status, error_code, structure_message, data_dir_count, data_xml_count
            )
            VALUES
            (
              %s,
              %s, CURRENT_TIMESTAMP(6),
              %s, CURRENT_TIMESTAMP(6),
              %s, %s, %s,
              %s, %s, %s,
              %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
              run_id=VALUES(run_id),
              last_seen_run_id=VALUES(last_seen_run_id),
              last_seen_at=CURRENT_TIMESTAMP(6),
              facility_folder_name=VALUES(facility_folder_name),
              facility_code=VALUES(facility_code),
              facility_name=VALUES(facility_name),
              zip_name=VALUES(zip_name),
              zip_path=VALUES(zip_path),
              structure_status=VALUES(structure_status),
              error_code=VALUES(error_code),
              structure_message=VALUES(structure_message),
              data_dir_count=VALUES(data_dir_count),
              data_xml_count=VALUES(data_xml_count),
              zip_receipt_id=LAST_INSERT_ID(zip_receipt_id)
            """,
            (
                run_id,
                run_id,
                run_id,
                facility_folder_name,
                facility_code,
                facility_name,
                zip_name,
                zip_path,
                zip_sha256,
                structure_status,
                error_code,
                structure_message,
                data_dir_count,
                data_xml_count,
            ),
        )

    return int(cur.lastrowid)


def db_insert_zip_receipt_run(
    cur,
    *,
    run_id: int,
    zip_receipt_id: int,
    zip_sha256: str,
    action: str,
    message: Optional[str] = None,
) -> None:
    """
    互換維持：message列が存在するなら入れる（無ければ無視）
    """
    has_msg = db_has_column(cur, "medi_zip_receipt_runs", "message")
    message = _clip_text(message, 4000)

    if has_msg:
        cur.execute(
            """
            INSERT INTO medi_zip_receipt_runs (run_id, zip_receipt_id, zip_sha256, action, message)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              zip_receipt_id=VALUES(zip_receipt_id),
              action=VALUES(action),
              message=VALUES(message),
              seen_at=CURRENT_TIMESTAMP(6)
            """,
            (run_id, zip_receipt_id, zip_sha256, action, message),
        )
    else:
        cur.execute(
            """
            INSERT INTO medi_zip_receipt_runs (run_id, zip_receipt_id, zip_sha256, action)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              zip_receipt_id=VALUES(zip_receipt_id),
              action=VALUES(action),
              seen_at=CURRENT_TIMESTAMP(6)
            """,
            (run_id, zip_receipt_id, zip_sha256, action),
        )


# -----------------------------
# DB helpers (XML receipts)
# -----------------------------
def db_get_xml_receipt_id_by_sha(cur, xml_sha256: str) -> Optional[int]:
    cur.execute("SELECT xml_receipt_id FROM medi_xml_receipts WHERE xml_sha256=%s", (xml_sha256,))
    row = cur.fetchone()
    return int(row["xml_receipt_id"]) if row else None


def db_upsert_xml_receipt(
    cur,
    *,
    run_id: int,
    zip_sha256: str,
    zip_inner_path: str,
    xml_sha256: str,
    file_size: Optional[int],
    file_mtime: Optional[str],
    status: str,
    error_code: Optional[str],
    error_message: Optional[str],
    facility_code: Optional[str],
    facility_name: Optional[str],
    zip_inner_path_sha256: Optional[str] = None,
) -> int:
    """
    受領台帳は環境差が出やすいので、列があれば “空欄にならない” ようにDB層で補正する。
    """
    zip_inner_path_norm = _norm_inner_path(zip_inner_path)

    has_inner_sha = db_has_column(cur, "medi_xml_receipts", "zip_inner_path_sha256")
    inner_sha_safe = None
    if has_inner_sha:
        inner_sha_safe = _ensure_inner_sha(zip_inner_path_norm, zip_inner_path_sha256)

    cols = [
        "zip_sha256", "zip_inner_path", "xml_sha256",
        "file_size", "file_mtime",
        "status", "error_code", "error_message",
        "facility_code", "facility_name",
        "first_seen_run_id", "first_seen_at",
        "last_seen_run_id", "last_seen_at",
    ]
    vals = [
        "%s", "%s", "%s",
        "%s", "%s",
        "%s", "%s", "%s",
        "%s", "%s",
        "%s", "CURRENT_TIMESTAMP(6)",
        "%s", "CURRENT_TIMESTAMP(6)",
    ]
    params: list[Any] = [
        zip_sha256, zip_inner_path_norm, xml_sha256,
        file_size, file_mtime,
        status, error_code, _clip_text(error_message, 8000),
        facility_code, facility_name,
        run_id,
        run_id,
    ]

    if has_inner_sha:
        cols.insert(2, "zip_inner_path_sha256")
        vals.insert(2, "%s")
        params.insert(2, inner_sha_safe)

    insert_cols = ", ".join(cols)
    insert_vals = ", ".join(vals)

    updates = [
        "last_seen_run_id=VALUES(last_seen_run_id)",
        "last_seen_at=CURRENT_TIMESTAMP(6)",
        "file_size=VALUES(file_size)",
        "file_mtime=VALUES(file_mtime)",
        "status=VALUES(status)",
        "error_code=VALUES(error_code)",
        "error_message=VALUES(error_message)",
        "facility_code=VALUES(facility_code)",
        "facility_name=VALUES(facility_name)",
    ]
    if has_inner_sha:
        updates.insert(1, "zip_inner_path_sha256=VALUES(zip_inner_path_sha256)")
    updates.append("xml_receipt_id=LAST_INSERT_ID(xml_receipt_id)")

    sql = f"""
        INSERT INTO medi_xml_receipts
        ({insert_cols})
        VALUES
        ({insert_vals})
        ON DUPLICATE KEY UPDATE
          {", ".join(updates)}
    """
    cur.execute(sql, tuple(params))
    return int(cur.lastrowid)


def db_insert_xml_receipt_run(
    cur,
    *,
    run_id: int,
    xml_sha256: str,
    action: str,
    message: Optional[str],
) -> None:
    """
    旧: xml_sha256 参照
    新: xml_receipt_id 参照（FK変更に追随）
    → 列があれば xml_receipt_id も埋めて記帳（他スクリプト変更なしで追随）
    """
    has_receipt_id = db_has_column(cur, "medi_xml_receipt_runs", "xml_receipt_id")
    xml_receipt_id = None
    if has_receipt_id:
        xml_receipt_id = db_get_xml_receipt_id_by_sha(cur, xml_sha256)

    msg = _clip_text(message, 4000)

    if has_receipt_id:
        cur.execute(
            """
            INSERT INTO medi_xml_receipt_runs (run_id, xml_sha256, xml_receipt_id, action, message)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              xml_receipt_id=VALUES(xml_receipt_id),
              action=VALUES(action),
              message=VALUES(message),
              created_at=CURRENT_TIMESTAMP(6)
            """,
            (run_id, xml_sha256, xml_receipt_id, action, msg),
        )
    else:
        cur.execute(
            """
            INSERT INTO medi_xml_receipt_runs (run_id, xml_sha256, action, message)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              action=VALUES(action),
              message=VALUES(message),
              created_at=CURRENT_TIMESTAMP(6)
            """,
            (run_id, xml_sha256, action, msg),
        )


# -----------------------------
# DB helpers (process logs)
# -----------------------------
def db_insert_xml_process_log(
    cur,
    *,
    run_id: int,
    xml_sha256: str,
    step: str,
    result: str,
    message: Optional[str],
) -> None:
    """
    安全弁:
    - step が ENUM でも未知値を DB 側の許可値へフォールバックして落ちないようにする
    """
    step_safe = _guard_enum_value(cur, "medi_xml_process_logs", "step", step)

    cur.execute(
        """
        INSERT INTO medi_xml_process_logs (run_id, xml_sha256, step, result, message)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          result=VALUES(result),
          message=VALUES(message),
          processed_at=CURRENT_TIMESTAMP(6)
        """,
        (run_id, xml_sha256, step_safe, result, _clip_text(message, 8000)),
    )


def db_update_xml_index_fields(
    cur,
    *,
    xml_sha256: str,
    status: str,
    error_code: Optional[str],
    error_message: Optional[str],
    document_id: Optional[str],
    extracted_run_id: Optional[int],
    extracted_at_now: bool,
    xml_receipt_id: Optional[int] = None,
) -> None:
    has_extracted_at = db_has_column(cur, "medi_xml_receipts", "extracted_at")
    has_extracted_run = db_has_column(cur, "medi_xml_receipts", "extracted_run_id")

    sets = [
        "status=%s",
        "error_code=%s",
        "error_message=%s",
        "document_id=%s",
    ]
    params: list[Any] = [status, error_code, _clip_text(error_message, 8000), document_id]

    if has_extracted_run:
        sets.append("extracted_run_id=%s")
        params.append(extracted_run_id)

    if extracted_at_now and has_extracted_at:
        sets.append("extracted_at=CURRENT_TIMESTAMP(6)")

    where = "xml_sha256=%s"
    params.append(xml_sha256)

    sql = f"UPDATE medi_xml_receipts SET {', '.join(sets)} WHERE {where}"
    cur.execute(sql, tuple(params))


# -----------------------------
# XML_EXTRACT: select helpers
# -----------------------------
def db_select_pending_xmls(cur, *, status: str, limit: int) -> list[dict]:
    """
    zip_inner_path_sha256 がある環境では取れるようにしておく（無ければNULL）
    """
    has_inner_sha = db_has_column(cur, "medi_xml_receipts", "zip_inner_path_sha256")
    inner_sha_col = ", x.zip_inner_path_sha256" if has_inner_sha else ""

    cur.execute(
        f"""
        SELECT x.xml_receipt_id, x.xml_sha256, x.zip_sha256, x.zip_inner_path{inner_sha_col}
        FROM medi_xml_receipts x
        WHERE x.status=%s
        ORDER BY x.updated_at ASC
        LIMIT %s
        """,
        (status, limit),
    )
    return [dict(r) for r in cur.fetchall()]


def db_get_zip_path_by_sha(cur, zip_sha256: str) -> Optional[str]:
    cur.execute("SELECT zip_path FROM medi_zip_receipts WHERE zip_sha256=%s", (zip_sha256,))
    row = cur.fetchone()
    return str(row["zip_path"]) if row and row.get("zip_path") else None


# -----------------------------
# XML ledger (medi_xml_ledger)
# -----------------------------
def db_upsert_xml_ledger(
    cur,
    *,
    run_id: int,
    zip_receipt_id: int,
    facility_folder_name: Optional[str],
    facility_code: Optional[str],
    facility_name: Optional[str],
    zip_name: str,
    zip_sha256: str,
    xml_filename: str,
    zip_inner_path: str,
    insurer_number: Optional[str],
    insurance_symbol: Optional[str],
    insurance_number: Optional[str],
    insurance_branch_number: Optional[str],
    birth_date,      # date | None
    kenshin_date,    # date | None
    gender_code: Optional[str],
    name_kana_full: Optional[str],
    postal_code: Optional[str],
    address: Optional[str],
    org_name_in_xml: Optional[str],
    org_code_in_xml: Optional[str],
    report_category_code: Optional[str],
    program_type_code: Optional[str],
    guidance_level_code: Optional[str],
    metabo_code: Optional[str],
    xsd_valid: Optional[int],
    error_content: Optional[str],
    zip_inner_path_sha256: Optional[str] = None,
) -> int:
    """
    medi_xml_ledger は UNIQUE KEY (zip_sha256, zip_inner_path_sha256) を持つ想定（新DDL）。
    旧環境互換のため列の存在を見て分岐する。
    さらに “NOT NULL の可能性” があるので、列があるなら DB層で必ず sha を埋める。
    """
    zip_inner_path_norm = _norm_inner_path(zip_inner_path)

    has_inner_sha = db_has_column(cur, "medi_xml_ledger", "zip_inner_path_sha256")
    inner_sha_safe = None
    if has_inner_sha:
        inner_sha_safe = _ensure_inner_sha(zip_inner_path_norm, zip_inner_path_sha256)

    cols = [
        "run_id", "zip_receipt_id",
        "facility_folder_name", "facility_code", "facility_name",
        "zip_name", "zip_sha256",
        "xml_filename", "zip_inner_path",
        "insurer_number", "insurance_symbol", "insurance_number", "insurance_branch_number",
        "birth_date", "kenshin_date", "gender_code", "name_kana_full",
        "postal_code", "address",
        "org_name_in_xml", "org_code_in_xml",
        "report_category_code", "program_type_code", "guidance_level_code", "metabo_code",
        "xsd_valid", "error_content",
    ]
    vals = ["%s"] * len(cols)

    params: list[Any] = [
        run_id, zip_receipt_id,
        facility_folder_name, facility_code, facility_name,
        zip_name, zip_sha256,
        xml_filename, zip_inner_path_norm,
        insurer_number, insurance_symbol, insurance_number, insurance_branch_number,
        birth_date, kenshin_date, gender_code, name_kana_full,
        postal_code, address,
        org_name_in_xml, org_code_in_xml,
        report_category_code, program_type_code, guidance_level_code, metabo_code,
        xsd_valid, _clip_text(error_content, 8000),
    ]

    if has_inner_sha:
        cols.insert(9, "zip_inner_path_sha256")
        vals.insert(9, "%s")
        params.insert(9, inner_sha_safe)

    insert_cols = ", ".join(cols)
    insert_vals = ", ".join(vals)

    updates = [
        "run_id=VALUES(run_id)",
        "zip_receipt_id=VALUES(zip_receipt_id)",
        "facility_folder_name=VALUES(facility_folder_name)",
        "facility_code=VALUES(facility_code)",
        "facility_name=VALUES(facility_name)",
        "zip_name=VALUES(zip_name)",
        "xml_filename=VALUES(xml_filename)",
        "insurer_number=VALUES(insurer_number)",
        "insurance_symbol=VALUES(insurance_symbol)",
        "insurance_number=VALUES(insurance_number)",
        "insurance_branch_number=VALUES(insurance_branch_number)",
        "birth_date=VALUES(birth_date)",
        "kenshin_date=VALUES(kenshin_date)",
        "gender_code=VALUES(gender_code)",
        "name_kana_full=VALUES(name_kana_full)",
        "postal_code=VALUES(postal_code)",
        "address=VALUES(address)",
        "org_name_in_xml=VALUES(org_name_in_xml)",
        "org_code_in_xml=VALUES(org_code_in_xml)",
        "report_category_code=VALUES(report_category_code)",
        "program_type_code=VALUES(program_type_code)",
        "guidance_level_code=VALUES(guidance_level_code)",
        "metabo_code=VALUES(metabo_code)",
        "xsd_valid=VALUES(xsd_valid)",
        "error_content=VALUES(error_content)",
    ]
    if has_inner_sha:
        updates.insert(7, "zip_inner_path_sha256=VALUES(zip_inner_path_sha256)")
    updates.append("xml_ledger_id=LAST_INSERT_ID(xml_ledger_id)")

    sql = f"""
        INSERT INTO medi_xml_ledger
        ({insert_cols})
        VALUES
        ({insert_vals})
        ON DUPLICATE KEY UPDATE
          {", ".join(updates)}
    """
    cur.execute(sql, tuple(params))
    return int(cur.lastrowid)

# ============================================================
# XML item values (work_other.medi_xml_item_values)
# ============================================================

def db_select_target_xmls_for_item_extract(
    cur,
    *,
    limit: int,
    target_status: str = "OK",
) -> list[dict]:
    """
    item values 抽出の対象XMLを medi_xml_receipts から取る。

    条件:
    - status = target_status (基本OK)
    - items_extract_status が NULL または 'ERROR' または 'SKIP'（=未完扱い）
      ※ 'OK' は対象外
    """
    cur.execute(
        """
        SELECT
          xml_receipt_id,
          xml_sha256,
          zip_sha256,
          zip_inner_path,
          file_size,
          file_mtime,
          status,
          items_extract_status,
          items_extracted_run_id,
          items_extracted_at
        FROM medi_xml_receipts
        WHERE status=%s
          AND (items_extract_status IS NULL OR items_extract_status <> 'OK')
        ORDER BY updated_at ASC
        LIMIT %s
        """,
        (target_status, limit),
    )
    return [dict(r) for r in cur.fetchall()]


def db_update_items_extract_fields(
    cur,
    *,
    xml_receipt_id: int,
    items_extract_status: str,
    items_extracted_run_id: int,
    items_extracted_at_now: bool = True,
) -> None:
    """
    medi_xml_receipts の item values 抽出結果を記帳する。
    """
    sets = [
        "items_extract_status=%s",
        "items_extracted_run_id=%s",
    ]
    params = [items_extract_status, items_extracted_run_id]

    if items_extracted_at_now:
        sets.append("items_extracted_at=CURRENT_TIMESTAMP(6)")

    params.append(xml_receipt_id)

    cur.execute(
        f"""
        UPDATE medi_xml_receipts
        SET {", ".join(sets)}
        WHERE xml_receipt_id=%s
        """,
        tuple(params),
    )


def db_upsert_xml_item_value(
    cur,
    *,
    xml_sha256: str,
    zip_sha256: str,
    zip_inner_path: str,
    zip_inner_path_sha256: str,
    namecode: str,
    occurrence_no: int,
    value_raw: str | None,
    value_type: str | None,
    unit: str | None,
    code_system: str | None,
    code_value: str | None,
    code_display: str | None,
    extracted_run_id: int | None,
) -> int:
    """
    medi_xml_item_values へ UPSERT。
    UNIQUE(xml_sha256, namecode, occurrence_no)
    """
    cur.execute(
        """
        INSERT INTO medi_xml_item_values
        (
          xml_sha256,
          zip_sha256,
          zip_inner_path,
          zip_inner_path_sha256,
          namecode,
          occurrence_no,
          value_raw,
          value_type,
          unit,
          code_system,
          code_value,
          code_display,
          extracted_run_id,
          extracted_at
        )
        VALUES
        (
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          CURRENT_TIMESTAMP(6)
        )
        ON DUPLICATE KEY UPDATE
          zip_sha256=VALUES(zip_sha256),
          zip_inner_path=VALUES(zip_inner_path),
          zip_inner_path_sha256=VALUES(zip_inner_path_sha256),
          value_raw=VALUES(value_raw),
          value_type=VALUES(value_type),
          unit=VALUES(unit),
          code_system=VALUES(code_system),
          code_value=VALUES(code_value),
          code_display=VALUES(code_display),
          extracted_run_id=VALUES(extracted_run_id),
          extracted_at=CURRENT_TIMESTAMP(6),
          xml_item_value_id=LAST_INSERT_ID(xml_item_value_id)
        """,
        (
            xml_sha256,
            zip_sha256,
            zip_inner_path,
            zip_inner_path_sha256,
            namecode,
            int(occurrence_no),
            value_raw,
            value_type,
            unit,
            code_system,
            code_value,
            code_display,
            extracted_run_id,
        ),
    )
    return int(cur.lastrowid)
