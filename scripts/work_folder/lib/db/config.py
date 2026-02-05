# -*- coding: utf-8 -*-
r"""
DB 設定（正本）

Path: phr/lib/db/config.py

役割:
  - .env 読込
  - MySQLParams の生成
  - 環境変数の必須チェックを一元化
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# ============================================================
# .env 読込
# ============================================================

PKG_ROOT = Path(__file__).resolve().parents[2]
_ENV_PATH = PKG_ROOT / ".env"


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


def load_mysql_params(prefix: str = "PHR_MYSQL_") -> MySQLParams:
    return MySQLParams(
        host=os.getenv(prefix + "HOST", "localhost"),
        port=int(os.getenv(prefix + "PORT", "3306")),
        user=_require_env(prefix + "USER"),
        password=_require_env(prefix + "PASSWORD"),
        database=_require_env(prefix + "DB"),
    )
