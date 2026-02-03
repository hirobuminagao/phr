# -*- coding: utf-8 -*-
r"""
MySQL 接続ユーティリティ（kenshin_list_pydir用）

Path: kenshin_lib/db/mysql.py
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

def connect_mysql(params: MySQLParams, *, autocommit: bool = False) -> Connection:
    return mysql.connector.connect(
        host=params.host,
        port=params.port,
        user=params.user,
        password=params.password,
        database=params.database,
        autocommit=autocommit,
    )


def dict_cursor(conn: Connection) -> Cursor:
    return conn.cursor(dictionary=True)


@contextmanager
def connect_ctx(params: MySQLParams, *, autocommit: bool = False) -> Iterator[Connection]:
    conn = connect_mysql(params, autocommit=autocommit)
    try:
        yield conn
    finally:
        conn.close()
