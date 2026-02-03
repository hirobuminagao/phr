# -*- coding: utf-8 -*-
"""
scripts/normalize_item_values.py

目的:
- work_other.medi_exam_result_item_values の RAW を正規化して value に入れるだけ。
- 走査・更新のみ（run記帳/台帳/トークン生成など一切しない）

ルール:
- ST: raw_value をそのまま value へ
- PQ: raw_value.strip() を value へ（trimのみ）+ 数値チェック（float化）
- CD: norm_variants に「完全一致」で当たれば normalized_code を value へ
      当たらなければ ERROR
- CO: まず exam_item_master.result_code_oid を確認
      - result_code_oid がある場合：CDと同じ（辞書で完全一致→normalized_code）
      - result_code_oid がない場合：ERROR（後で仕様化）

重要:
- 推測でコード割当しない（'-'→0 など禁止）
- raw_value の trim/トークン生成/変換は「一切しない」
  ※PQのみ strip()。CD/COは raw_value をそのまま完全一致で照合。

ENV:
  # work_other
  MEDI_IMPORT_DB_HOST
  MEDI_IMPORT_DB_PORT
  MEDI_IMPORT_DB_USER
  MEDI_IMPORT_DB_PASSWORD
  MEDI_IMPORT_DB_NAME (default: work_other)

  # dev_phr
  PHR_MYSQL_HOST (空なら MEDI_IMPORT_DB_HOST を流用)
  PHR_MYSQL_PORT (空なら MEDI_IMPORT_DB_PORT を流用)
  PHR_MYSQL_USER
  PHR_MYSQL_PASSWORD
  PHR_MYSQL_DB (default: dev_phr)

  # runtime
  NORMALIZE_LIMIT (default 500)  # 0以下なら全件相当（大きい数で取る）
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Optional
from pathlib import Path

from dotenv import load_dotenv

import mysql.connector
from mysql.connector.cursor import MySQLCursorDict


# --- path bootstrap (medi_xml_item_extract と同じ) ---
BASE_DIR = Path(__file__).resolve().parents[1]  # = kenshin_list_pydir
# ----------------------------------------------------


# -----------------------------
# env utils
# -----------------------------
def env_required(key: str) -> str:
    v = os.getenv(key)
    if v is None or v.strip() == "":
        raise RuntimeError(f"必須環境変数 {key} が設定されていません")
    return v.strip()


def env_int(key: str, default: int) -> int:
    v = os.getenv(key)
    if v is None or v.strip() == "":
        return default
    try:
        return int(v.strip())
    except Exception:
        return default


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


# -----------------------------
# DB connect
# -----------------------------
def load_work_other_params() -> dict:
    host = env_required("MEDI_IMPORT_DB_HOST")
    port = int(env_required("MEDI_IMPORT_DB_PORT"))
    name = os.getenv("MEDI_IMPORT_DB_NAME", "work_other").strip() or "work_other"
    user = env_required("MEDI_IMPORT_DB_USER")
    password = env_required("MEDI_IMPORT_DB_PASSWORD")
    return {
        "host": host,
        "port": port,
        "database": name,
        "user": user,
        "password": password,
        "autocommit": False,
        "use_pure": True,
    }


def load_dev_phr_params(*, fallback_host: str, fallback_port: int) -> dict:
    host = os.getenv("PHR_MYSQL_HOST", "").strip() or fallback_host
    port = int((os.getenv("PHR_MYSQL_PORT", str(fallback_port)).strip() or str(fallback_port)))
    name = os.getenv("PHR_MYSQL_DB", "dev_phr").strip() or "dev_phr"
    user = env_required("PHR_MYSQL_USER")
    password = env_required("PHR_MYSQL_PASSWORD")
    return {
        "host": host,
        "port": port,
        "database": name,
        "user": user,
        "password": password,
        "autocommit": False,
        "use_pure": True,
    }


def connect_mysql(params: dict):
    return mysql.connector.connect(**params)


def dict_cursor(conn) -> MySQLCursorDict:
    return conn.cursor(dictionary=True, buffered=True)


# -----------------------------
# core
# -----------------------------
def select_targets(cur, limit: int) -> list[dict]:
    if limit <= 0:
        limit = 1_000_000
    cur.execute(
        """
        SELECT
          item_value_id,
          ledger_id,
          namecode,
          raw_value
        FROM medi_exam_result_item_values
        WHERE normalize_status='RAW'
          AND (value IS NULL OR value = '')
        ORDER BY item_value_id ASC
        LIMIT %s
        """,
        (limit,),
    )
    return [dict(r) for r in cur.fetchall()]


def get_master(cur_phr, namecode: str) -> Optional[dict]:
    cur_phr.execute(
        """
        SELECT namecode, xml_value_type, result_code_oid
        FROM exam_item_master
        WHERE namecode = %s
        """,
        (namecode,),
    )
    r = cur_phr.fetchone()
    return dict(r) if r else None


def lookup_code_like(cur_phr, result_code_oid: str, raw_value: str) -> Optional[dict]:
    """
    CD/CO の「コード系」照合。
    ★完全一致のみ。trim/トークン生成/変換は一切しない。
    ★canonicalに寄せる：is_canonical DESC, priority ASC の ORDER BY で担保。
    """
    cur_phr.execute(
        """
        SELECT normalized_code, code_system, display_name
        FROM norm_variants
        WHERE result_code_oid = %s
          AND raw_value_utf8 = %s
          AND is_active = 1
        ORDER BY is_canonical DESC, priority ASC, variant_id ASC
        LIMIT 1
        """,
        (result_code_oid, raw_value),
    )
    r = cur_phr.fetchone()
    return dict(r) if r else None


def update_ok(cur, item_value_id: int, value: str) -> None:
    cur.execute(
        """
        UPDATE medi_exam_result_item_values
        SET value=%s,
            normalize_status='OK',
            normalized_at=%s,
            normalize_error=NULL
        WHERE item_value_id=%s
        """,
        (value, now_str(), item_value_id),
    )


def update_error(cur, item_value_id: int, msg: str) -> None:
    cur.execute(
        """
        UPDATE medi_exam_result_item_values
        SET normalize_status='ERROR',
            normalized_at=%s,
            normalize_error=%s
        WHERE item_value_id=%s
        """,
        (now_str(), msg, item_value_id),
    )


def normalize_code_like(
    *,
    cur_work,
    cur_phr,
    item_value_id: int,
    xml_value_type: str,
    result_code_oid: str,
    raw_value_str: str,
) -> bool:
    """
    CD/CO（コード系）共通ルート。
    成功: update_ok して True
    失敗: update_error して False
    """
    if result_code_oid == "":
        update_error(
            cur_work,
            item_value_id,
            f"{xml_value_type} but result_code_oid is NULL/empty in exam_item_master",
        )
        return False

    hit = lookup_code_like(cur_phr, result_code_oid, raw_value_str)
    if not hit:
        update_error(
            cur_work,
            item_value_id,
            f"{xml_value_type} no match in norm_variants: result_code_oid='{result_code_oid}', raw_value='{raw_value_str}'",
        )
        return False

    update_ok(cur_work, item_value_id, str(hit["normalized_code"]))
    return True


def main() -> int:
    # ---- .env load (ここが今回の修正点) ----
    env_path = BASE_DIR / ".env"
    if env_path.exists():
        load_dotenv(env_path)
        print(f".env loaded: {env_path}")
    else:
        # 念のため: カレントも見る（保険）。ただし見つからない場合は出力で分かる
        load_dotenv()
        print(f".env NOT found at: {env_path} (also tried default load_dotenv())")
    # ---------------------------------------

    limit = env_int("NORMALIZE_LIMIT", 500)

    work_params = load_work_other_params()
    phr_params = load_dev_phr_params(fallback_host=work_params["host"], fallback_port=work_params["port"])

    print(f"[NORMALIZE] work_other={work_params['host']}:{work_params['port']}/{work_params['database']}")
    print(f"[NORMALIZE] dev_phr   ={phr_params['host']}:{phr_params['port']}/{phr_params['database']}")
    print(f"[NORMALIZE] LIMIT={limit if limit > 0 else 'FULL'}")

    conn_work = connect_mysql(work_params)
    conn_phr = connect_mysql(phr_params)

    try:
        cur_work = dict_cursor(conn_work)
        cur_phr = dict_cursor(conn_phr)

        targets = select_targets(cur_work, limit=limit)
        if not targets:
            print("[NORMALIZE] no targets")
            return 0

        ok = 0
        err = 0

        for t in targets:
            item_value_id = int(t["item_value_id"])
            namecode = str(t["namecode"] or "")
            raw_value = t.get("raw_value")
            raw_value_str = "" if raw_value is None else str(raw_value)

            if not namecode:
                update_error(cur_work, item_value_id, "namecode is empty")
                err += 1
                continue

            master = get_master(cur_phr, namecode)
            if not master:
                update_error(cur_work, item_value_id, f"exam_item_master not found: namecode={namecode}")
                err += 1
                continue

            xml_value_type = (master.get("xml_value_type") or "").strip().upper()
            result_code_oid = (master.get("result_code_oid") or "").strip()

            # ST
            if xml_value_type == "ST" or xml_value_type == "":
                if raw_value is None:
                    update_error(cur_work, item_value_id, "ST raw_value is NULL")
                    err += 1
                else:
                    update_ok(cur_work, item_value_id, raw_value_str)
                    ok += 1
                continue

            # PQ
            if xml_value_type == "PQ":
                v = raw_value_str.strip()
                if v == "":
                    update_error(cur_work, item_value_id, "PQ raw_value becomes empty after trim")
                    err += 1
                else:
                    # 最低限チェック（不要なら削除OK）
                    try:
                        _ = float(v)
                    except Exception:
                        update_error(cur_work, item_value_id, f"PQ not numeric: raw_value='{raw_value_str}'")
                        err += 1
                        continue
                    update_ok(cur_work, item_value_id, v)
                    ok += 1
                continue

            # CD（コード系）
            if xml_value_type == "CD":
                if normalize_code_like(
                    cur_work=cur_work,
                    cur_phr=cur_phr,
                    item_value_id=item_value_id,
                    xml_value_type="CD",
                    result_code_oid=result_code_oid,
                    raw_value_str=raw_value_str,
                ):
                    ok += 1
                else:
                    err += 1
                continue

            # CO（今回の方針：result_code_oid があるならコード系として扱う）
            if xml_value_type == "CO":
                if normalize_code_like(
                    cur_work=cur_work,
                    cur_phr=cur_phr,
                    item_value_id=item_value_id,
                    xml_value_type="CO",
                    result_code_oid=result_code_oid,
                    raw_value_str=raw_value_str,
                ):
                    ok += 1
                else:
                    err += 1
                continue

            # その他（未対応）
            update_error(cur_work, item_value_id, f"unsupported xml_value_type='{xml_value_type}'")
            err += 1

        conn_work.commit()
        print(f"[NORMALIZE] done. ok={ok} err={err} targets={len(targets)}")
        return 0 if err == 0 else 2

    finally:
        try:
            conn_phr.close()
        except Exception:
            pass
        try:
            conn_work.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
