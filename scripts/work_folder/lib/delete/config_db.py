# -*- coding: utf-8 -*-
r"""
============================================================
Script: config_db.py
Path  : phr/lib/config_db.py
Project: PHR / work_folder/phr
Purpose:
    - .env 読込と MySQLParams の生成を担当
    - load_mysql_params() を中央集約し、全スクリプトが共通形式で
      MySQL 接続設定を取得できるようにする。

Notes:
    - python-dotenv があれば利用、無くても簡易読込で動作
    - 必須パラメータ（USER/PASSWORD/DB）は _require_env() で保証
============================================================
"""

from __future__ import annotations

import os
from pathlib import Path

from phr.lib.db_mysql import MySQLParams

# work_folder/.env を見る前提
PKG_ROOT = Path(__file__).resolve().parents[1]
_ENV_PATH = PKG_ROOT / ".env"


def _load_env_loose(path: Path) -> None:
    """
    python-dotenv があればそれを使い、
    無ければ KEY=VALUE をざっくり読む簡易版。
    """
    try:
        from dotenv import load_dotenv  # type: ignore[import-not-found]
        load_dotenv(path)
        return
    except Exception:
        # ライブラリ無し or 読み込み失敗 → 自前でパース
        pass

    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        os.environ.setdefault(
            k.strip(),
            v.strip().strip('"').strip("'"),
        )


# モジュール import 時に一度だけ .env を読む
_load_env_loose(_ENV_PATH)


def _require_env(name: str) -> str:
    """
    必須環境変数を取得。
    未設定なら RuntimeError にする（型的にも str 保証）。
    """
    v = os.getenv(name)
    if v is None or v == "":
        raise RuntimeError(f"必須環境変数 {name} が設定されていません。")
    return v


def load_mysql_params(prefix: str = "PHR_MYSQL_") -> MySQLParams:
    """
    .env / 環境変数から MySQLParams を組み立てる。
    host/port はデフォルトを持ち、それ以外は必須。
    """
    host = os.getenv(prefix + "HOST", "localhost")
    port_str = os.getenv(prefix + "PORT", "3306")

    return MySQLParams(
        host=host,
        port=int(port_str),
        user=_require_env(prefix + "USER"),
        password=_require_env(prefix + "PASSWORD"),
        database=_require_env(prefix + "DB"),
    )
