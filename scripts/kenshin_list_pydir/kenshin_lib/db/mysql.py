# -*- coding: utf-8 -*-
r"""
MySQL 接続ユーティリティ（kenshin_list_pydir 共通）

役割:
- MySQL への接続生成を一箇所に集約する
- スクリプト側からは「接続方法」を意識させない
- DB種別や認証方式が変わっても、この層で吸収する

設計方針（固定）:
- 接続パラメータは MySQLParams に集約する（環境変数の直接参照は禁止）
- 返却する connection / cursor は mysql-connector 標準のものをそのまま使う
- トランザクション制御の責務は呼び出し側に置く
- 本モジュールでは SQL 実行ロジックを持たない

対象:
- kenshin_list_pydir 配下の scripts / kenshin_lib から利用される共通部品
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
