# -*- coding: utf-8 -*-
r"""
errors/normalize.py — 正規化エラー定義（NormalizeError）

Path   : scripts/work_folder/lib/errors/normalize.py
Project: PHR / work_folder/phr

Purpose:
    - 正規化処理中に発生した「行単位の妥当性エラー」を構造化して表現する。
    - import/apply 側で捕捉され、etl_errors に記録される前提の例外型。

Design (v1.0 as-is):
    - dataclass + Exception 継承
    - field / code / raw_value / message を必須属性として保持
    - __str__ はログ出力・etl_errors.message 保存用の可読形式

V1.0 Freeze (Scope / Contract):
    - field:
        - エラー対象フィールド名（内部キー名）
    - code:
        - エラー種別コード（例: required / invalid_format / generate_failed など）
    - raw_value:
        - 入力元の未加工値（証跡）
    - message:
        - 人間可読な説明
    - 本例外は「想定内エラー」として扱う
        - 呼び出し側はこれを捕捉し、行スキップ + etl_errors 記録を行う

Non-goals:
    - ログ出力
    - ステータス判定
    - DB更新
"""

from __future__ import annotations

from dataclasses import dataclass


# v1.0: 正規化専用例外
# - 想定内エラーとして扱い、処理全体は停止させない前提


@dataclass
class NormalizeError(Exception):
    field: str
    code: str
    raw_value: str
    message: str

    def __str__(self) -> str:
        return f"[{self.field}/{self.code}] {self.message} (raw={self.raw_value!r})"
