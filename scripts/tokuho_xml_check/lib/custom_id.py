# -*- coding: utf-8 -*-
"""
custom_id.py
Excelで使っている「足す→掛ける→乱数表に当てる」独自ID生成ロジックを再現。
- 加算・乗算係数は設定ファイルで可変
- 乱数表（置換表）は 0-9 を任意の文字へ写像する辞書を使う
"""
from typing import Dict

def _map_digits(num_str: str, table: Dict[str, str]) -> str:
    return ''.join(table.get(ch, ch) for ch in num_str)

def _ensure_digits(s: str) -> str:
    return ''.join(ch for ch in s if ch.isdigit())

def generate_custom_id(*, insurer_number: str, symbol: str, insured_number: str, birth_yyyymmdd: str,
                       add_params, mul_params, mapping_tables: Dict[str, Dict[str, str]]) -> str:
    ins = int(_ensure_digits(insurer_number) or 0)
    sym = int(_ensure_digits(symbol) or 0)
    num = int(_ensure_digits(insured_number) or 0)
    bir = int(_ensure_digits(birth_yyyymmdd) or 0)

    ins = (ins + int(add_params.get("insurer", 0))) * int(mul_params.get("insurer", 1))
    sym = (sym + int(add_params.get("symbol", 0))) * int(mul_params.get("symbol", 1))
    num = (num + int(add_params.get("insured_number", 0))) * int(mul_params.get("insured_number", 1))
    bir = (bir + int(add_params.get("birth", 0))) * int(mul_params.get("birth", 1))

    s_ins = _map_digits(str(ins), mapping_tables.get("insurer", {}))
    s_sym = _map_digits(str(sym), mapping_tables.get("symbol", {}))
    s_num = _map_digits(str(num), mapping_tables.get("insured_number", {}))
    s_bir = _map_digits(str(bir), mapping_tables.get("birth", {}))

    return f"{s_ins}{s_sym}{s_num}{s_bir}"
