# -*- coding: utf-8 -*-
r"""
db/mysql.py — MySQL接続ユーティリティ（接続生成のみ）

Path   : scripts/work_folder/lib/db/mysql.py
Project: PHR / work_folder/phr

Purpose:
    - MySQL への接続オブジェクトを生成する。
    - カーソル生成および contextmanager を提供する。

Design (v1.0 as-is):
    - 接続パラメータは MySQLParams を受け取る（config.py 側で生成）
    - autocommit の制御は呼び出し側が明示指定
    - トランザクション（commit/rollback）は呼び出し側責務

V1.0 Freeze (Scope / Contract):
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

from .config import MySQLParams

# ============================================================
# Types
# ============================================================

Connection: TypeAlias = MySQLConnectionAbstract | PooledMySQLConnection
Cursor: TypeAlias = MySQLCursorAbstract


# ============================================================
# Connect
# ============================================================

# v1.0: MySQL接続生成関数
# - autocommit は明示指定
# - 例外はそのまま上位へ伝播
def connect_mysql(params: MySQLParams, *, autocommit: bool = False) -> Connection:
    return mysql.connector.connect(
        host=params.host,
        port=params.port,
        user=params.user,
        password=params.password,
        database=params.database,
        autocommit=autocommit,
    )


# v1.0: dictionary=True カーソル生成ヘルパー
# - 行結果を dict として扱う前提のETLで使用
def dict_cursor(conn: Connection) -> Cursor:
    return conn.cursor(dictionary=True)


# v1.0: with 構文用接続ラッパー
# - finally で必ず close() を実行
# - commit/rollback は呼び出し側が明示制御
@contextmanager
def connect_ctx(params: MySQLParams, *, autocommit: bool = False) -> Iterator[Connection]:
    conn = connect_mysql(params, autocommit=autocommit)
    try:
        yield conn
    finally:
        conn.close()
