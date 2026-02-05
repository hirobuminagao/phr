# -*- coding: utf-8 -*-
r"""
Path: work_folder/phr/lib/errors/normalize.py
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
