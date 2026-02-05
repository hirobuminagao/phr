# -*- coding: utf-8 -*-
r"""
Path: work_folder/phr/lib/etl/metrics.py
"""

from __future__ import annotations
from dataclasses import dataclass

@dataclass
class RunMetrics:
    """1 run 全体の集計値"""
    files: int = 0
    rows_seen: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_unchanged: int = 0
    rows_skipped: int = 0
    errors: int = 0
