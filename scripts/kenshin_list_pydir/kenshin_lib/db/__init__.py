r"""
kenshin_lib.db

【目的】
kenshin_list_pydir 配下で利用する DB 接続ユーティリティの公開口（re-export）。
他モジュールは原則として `kenshin_lib.db` から import し、内部実装のファイル構成
（config/mysql 等）に直接依存しない。

【提供するもの】
- MySQLParams / load_mysql_params : .env から接続情報を読み込む
- connect_mysql / dict_cursor     : mysql-connector の接続・辞書カーソル
- connect_ctx                     : 接続コンテキスト（with管理）

【固定方針】
- ここは API の入口のみを扱い、機能追加やリファクタは行わない。
- 変更が必要な場合は、互換性（import 先）を最優先にする。
"""

from .config import MySQLParams, load_mysql_params
from .mysql import connect_mysql, dict_cursor, connect_ctx

__all__ = [
    "MySQLParams",
    "load_mysql_params",
    "connect_mysql",
    "dict_cursor",
    "connect_ctx",
]
