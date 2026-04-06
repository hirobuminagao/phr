# -*- coding: utf-8 -*-
r"""
db/config.py — DB接続設定ロード（MySQLBaseParams 生成）

Path   : scripts/lib/db/config.py
Project: PHR

Purpose:
    - scripts/.env および環境変数から MySQL 接続の基盤情報を読み込む。
    - MySQLBaseParams（不変データクラス）を生成する。

Design (v1.1.0 as-is):
    - dotenv が利用可能なら load_dotenv を使用
    - 利用不可でも .env を簡易パースして os.environ に設定
    - 接続先 schema はここでは扱わない（呼び出し側が指定する）
    - 接続自体は行わない（mysql.py 側の責務）

V1.1.0 Contract:
    - 必須環境変数:
        - PHR_DB_USER
        - PHR_DB_PASSWORD
    - 任意環境変数:
        - PHR_DB_HOST（既定: localhost）
        - PHR_DB_PORT（既定: 3306）
    - load_mysql_base_params() は prefix を変更可能（既定: "PHR_DB_"）
    - 返却値は frozen=True の dataclass（実行中に変更不可）

Non-goals:
    - 接続確立
    - schema 決定
    - 接続プール管理
    - トランザクション制御

Reference:
    - DB接続方針: docs/spec/common/db_connection.md
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# ============================================================
# .env 読込
# ============================================================

SCRIPTS_ROOT = Path(__file__).resolve().parents[2]
_ENV_PATH = SCRIPTS_ROOT / ".env"


# v1.1.0: dotenv が無い環境でも最低限の .env 読込を保証する簡易ローダ
# - 既に設定済みの環境変数は上書きしない（setdefault）
def _load_env_loose(path: Path) -> None:
    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(path)
        return
    except Exception:
        pass

    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_env_loose(_ENV_PATH)


# ============================================================
# Params
# ============================================================

@dataclass(frozen=True)
class MySQLBaseParams:
    host: str
    port: int
    user: str
    password: str


# v1.1.0: 必須環境変数チェック
# - 未設定の場合は RuntimeError で即停止
def _require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"必須環境変数 {name} が設定されていません")
    return v


# v1.1.0: MySQLBaseParams 生成関数
# - prefix で複数 host 設定（dev/stg/prod 等）を切り替え可能
# - host/port は既定値あり、user/password は必須
def load_mysql_base_params(prefix: str = "PHR_DB_") -> MySQLBaseParams:
    return MySQLBaseParams(
        host=os.getenv(prefix + "HOST", "localhost"),
        port=int(os.getenv(prefix + "PORT", "3306")),
        user=_require_env(prefix + "USER"),
        password=_require_env(prefix + "PASSWORD"),
    )
# -*- coding: utf-8 -*-
r"""
db/mysql.py — MySQL接続ユーティリティ（接続生成のみ）

Path   : scripts/lib/db/mysql.py
Project: PHR

Purpose:
    - MySQL への接続オブジェクトを生成する。
    - カーソル生成および contextmanager を提供する。

Design (v1.1.0 as-is):
    - 接続基盤情報は MySQLBaseParams を受け取る（config.py 側で生成）
    - 接続先 schema は database 引数で呼び出し側が指定する
    - autocommit の制御は呼び出し側が明示指定
    - トランザクション（commit/rollback）は呼び出し側責務

V1.1.0 Contract:
    - connect_mysql():
        - mysql.connector.connect を直接呼び出す
        - 接続確立のみを責務とする
    - dict_cursor():
        - dictionary=True のカーソルを返す簡易ヘルパー
    - connect_ctx():
        - with 文で利用可能な接続ラッパー
        - ブロック終了時に close() を必ず実行
    - Non-goals:
        - 接続プール管理
        - 再接続制御
        - 自動commit/rollback
        - SQL実行ラッパー化
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator, TypeAlias

import mysql.connector
from mysql.connector.abstracts import MySQLConnectionAbstract, MySQLCursorAbstract
from mysql.connector.pooling import PooledMySQLConnection

from ..db.config import MySQLBaseParams

# ============================================================
# Types
# ============================================================

Connection: TypeAlias = MySQLConnectionAbstract | PooledMySQLConnection
Cursor: TypeAlias = MySQLCursorAbstract


# ============================================================
# Connect
# ============================================================

# v1.1.0: MySQL接続生成関数
# - database(schema) は呼び出し側が明示指定
# - autocommit は明示指定
# - 例外はそのまま上位へ伝播
def connect_mysql(
    params: MySQLBaseParams,
    *,
    database: str,
    autocommit: bool = False,
) -> Connection:
    return mysql.connector.connect(
        host=params.host,
        port=params.port,
        user=params.user,
        password=params.password,
        database=database,
        autocommit=autocommit,
    )


# v1.1.0: dictionary=True カーソル生成ヘルパー
# - 行結果を dict として扱う前提のETLで使用
def dict_cursor(conn: Connection) -> Cursor:
    return conn.cursor(dictionary=True)


# v1.1.0: with 構文用接続ラッパー
# - finally で必ず close() を実行
# - commit/rollback は呼び出し側が明示制御
@contextmanager
def connect_ctx(
    params: MySQLBaseParams,
    *,
    database: str,
    autocommit: bool = False,
) -> Iterator[Connection]:
    conn = connect_mysql(params, database=database, autocommit=autocommit)
    try:
        yield conn
    finally:
        conn.close()
