# -*- coding: utf-8 -*-
r"""
Path: work_folder/phr/lib/etl/__init__.py
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
