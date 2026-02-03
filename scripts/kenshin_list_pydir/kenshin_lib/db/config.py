# -*- coding: utf-8 -*-
r"""
DB 設定（kenshin_list_pydir用）

Path: kenshin_lib/db/config.py

役割:
  - .env 読込（ゆるい）
  - MySQLParams の生成
  - 環境変数の必須チェックを一元化

このプロジェクトでは MEDI_IMPORT_DB_* を採用
例:
  MEDI_IMPORT_DB_HOST=localhost
  MEDI_IMPORT_DB_PORT=3306
  MEDI_IMPORT_DB_USER=...
  MEDI_IMPORT_DB_PASSWORD=...
  MEDI_IMPORT_DB_NAME=work_other
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# ============================================================
# .env 読込
# ============================================================

# kenshin_list_pydir を PKG_ROOT とする
PKG_ROOT = Path(__file__).resolve().parents[2]  # .../kenshin_list_pydir
_ENV_PATH = PKG_ROOT / ".env"


def _load_env_loose(path: Path) -> None:
    # python-dotenv があればそれを使う（なければ手動パース）
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
class MySQLParams:
    host: str
    port: int
    user: str
    password: str
    database: str


def _require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"必須環境変数 {name} が設定されていません")
    return v


def load_mysql_params(prefix: str = "MEDI_IMPORT_DB_") -> MySQLParams:
    """
    既定は MEDI_IMPORT_DB_* を読む。
    database は suffix 'NAME' を採用（PHR側の 'DB' と違う点に注意）
    """
    return MySQLParams(
        host=os.getenv(prefix + "HOST", "localhost"),
        port=int(os.getenv(prefix + "PORT", "3306")),
        user=_require_env(prefix + "USER"),
        password=_require_env(prefix + "PASSWORD"),
        database=_require_env(prefix + "NAME"),
    )
