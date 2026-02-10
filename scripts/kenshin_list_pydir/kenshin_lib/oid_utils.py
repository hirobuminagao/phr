# -*- coding: utf-8 -*-
"""
kenshin_lib/oid_utils.py

OIDコード定義CSVを読み込み、参照用の辞書構造としてロードするためのユーティリティ。

目的:
- OIDコード体系（OID × 値コード × 表示名）をCSVから読み込み、
  プログラム内で参照可能な辞書構造に変換する。
- 判定・変換・正規化処理は本モジュールでは行わない（参照専用）。

入出力仕様:
- 入力: CSVファイル
  - ヘッダー例:
      OID_code
      OID_code_value
      OID_code_value_name
- 出力: dict
  {
    "<OID_code>": {
        "<OID_code_value>": "<OID_code_value_name>",
        ...
    },
    ...
  }

仕様・注意:
- CSVは utf-8-sig を前提とする（BOM付き想定）。
- 必須カラムが欠けている行は無視する。
- ヘッダー行や不正値（"OID_code", "value" 等）は除外する。
- 値は strip() して格納する。
- OIDが重複する場合は、同一OID内で code → name の辞書として統合する。

運用上の位置づけ:
- 本モジュールは「固定データ参照」のための補助関数のみを提供する。
- OID体系の意味解釈、業務ロジックへの適用は呼び出し側の責務とする。
- 将来的なOID定義変更時は、CSV差し替えで対応する想定。

固定化方針:
- 本ファイルは現状仕様の固定を目的としており、リファクタリングや機能拡張は行わない。
"""
import csv

def load_oid_library(csv_path):
    oid_library = {}
    with open(csv_path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            oid = row.get("OID_code")
            code = row.get("OID_code_value")
            name = row.get("OID_code_value_name")
            if oid and code and name and oid != "OID_code" and code != "value":
                if oid not in oid_library:
                    oid_library[oid] = {}
                oid_library[oid][code.strip()] = name.strip()
    return oid_library
