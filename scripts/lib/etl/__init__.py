# -*- coding: utf-8 -*-
r"""
etl/__init__.py

Path   : scripts/lib/etl/__init__.py
Project: PHR

Notes:
    - scripts/work_folder/lib/etl/ から scripts/lib/etl/ へコピーして共通化した版
    - import パスは scripts.lib.etl.* を正とする
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class NormalizeError(Exception):
    field: str
    code: str
    raw_value: str
    message: str

    def __str__(self) -> str:
        return f"[{self.field}/{self.code}] {self.message} (raw={self.raw_value!r})"



from .metrics import RunMetrics
from .progress import ProgressLogger
from .ddl import ensure_tables
from .runs import start_run, finish_run
from .errors import log_error, log_normalize_error

__all__ = [
    "RunMetrics",
    "ProgressLogger",
    "ensure_tables",
    "start_run",
    "finish_run",
    "log_error",
    "log_normalize_error",
]
