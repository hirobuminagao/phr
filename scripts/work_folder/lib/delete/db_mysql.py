# -*- coding: utf-8 -*-
r"""
============================================================
Script: db_mysql.py
Path  : phr/lib/db_mysql.py
Project: PHR / work_folder/phr
Purpose:
    - MySQL 接続/カーソル生成の共通ヘルパー
    - autocommit=False の統一運用
    - connect() / dict_cursor() / connect_ctx() を外部に提供

Notes:
    - .env は config_db.py 側でロードされるため本モジュール単体では不要
    - mysql-connector-python を利用（import エラー時は pip install）
============================================================
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator, TypeAlias

import mysql.connector

# 正しいインポート
from mysql.connector.abstracts import MySQLConnectionAbstract, MySQLCursorAbstract
from mysql.connector.pooling import PooledMySQLConnection


# ================================
# Type Aliases
# ================================
Connection: TypeAlias = MySQLConnectionAbstract | PooledMySQLConnection
Cursor: TypeAlias = MySQLCursorAbstract


# ================================
# Param dataclass
# ================================
@dataclass
class MySQLParams:
    host: str
    port: int
    user: str
    password: str
    database: str


# ================================
# Connect
# ================================
def connect(params: MySQLParams) -> Connection:
    conn = mysql.connector.connect(
        host=params.host,
        port=params.port,
        user=params.user,
        password=params.password,
        database=params.database,
        autocommit=False,
    )
    return conn


# ================================
# Cursor
# ================================
def dict_cursor(conn: Connection) -> Cursor:
    cur = conn.cursor(dictionary=True)
    return cur


# ================================
# Context Manager
# ================================
@contextmanager
def connect_ctx(params: MySQLParams) -> Iterator[Connection]:
    conn = connect(params)
    try:
        yield conn
    finally:
        conn.close()
