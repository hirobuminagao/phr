# -*- coding: utf-8 -*-
"""
custom_id_gen.py — 各フィールド個別にカスタムID化（ロジック1～2→ロジック3置換）→ 指定順で結合
- 外部ライブラリ不使用
- 正規出力(stdout): 最終IDのみ（--jsonout 指定時は JSON ）
- デバッグ/トレースは stderr 限定（stdout を一切汚さない）

使い方:
  python custom_id_gen.py --insurer 06080162 --symbol 30 --insured 2000956 --birth 1967/5/14 --mat ../mat
  # 人が読む詳細は:
  python custom_id_gen.py ... --trace
  # 機械で読む詳細は:
  python custom_id_gen.py ... --jsonout
"""

from __future__ import annotations
import argparse, json, os, sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List

# ===== Debug / Trace =====
DEBUG = os.getenv("TOKUHO_DEBUG", "").strip().lower() in {"1","true","t","yes","y","on"}
TRACE = os.getenv("TOKUHO_TRACE", "").strip().lower() in {"1","true","t","yes","y","on"}

def dbg(*args, **kwargs):
    if DEBUG:
        print("[CID][DBG]", *args, **kwargs, file=sys.stderr)

def trace(title: str, data: Dict[str, Any]):
    if TRACE:
        print(f"[CID][TRACE] {title}", file=sys.stderr)
        for k, v in data.items():
            print(f"  - {k}: {v}", file=sys.stderr)

def trace_field(name: str, raw: str, normalized: str, num12: str, token: str):
    if TRACE:
        print(f"[CID][FIELD:{name}]", file=sys.stderr)
        print(f"  1) raw        : {raw}",        file=sys.stderr)
        print(f"  2) normalized : {normalized}", file=sys.stderr)
        print(f"  3) (v+add)*mul: {num12}",      file=sys.stderr)
        print(f"  4) mapped     : {token}",      file=sys.stderr)

# ===== Normalizers =====
FW_DIGITS = str.maketrans("0123456789", "０１２３４５６７８９")
HW_DIGITS = str.maketrans("０１２３４５６７８９", "0123456789")

def to_half_digits(s: str) -> str:
    return "".join(ch.translate(HW_DIGITS) for ch in s or "")

def norm_number(raw: Optional[str]) -> str:
    return to_half_digits((raw or "").strip())

def norm_symbol(raw: Optional[str]) -> str:
    s = (raw or "").strip()
    if s == "":
        return ""
    if all(('０' <= ch <= '９') or ('0' <= ch <= '9') for ch in s):
        return to_half_digits(s)
    return "".join(ch.translate(FW_DIGITS) for ch in s)

def normalize_birth_any(raw: Optional[str]) -> str:
    s = (raw or "").strip()
    if len(s) == 8 and s.isdigit():
        return s
    import re
    nums = [n for n in re.split(r"[^\d]+", s) if n]
    if len(nums) == 3:
        if len(nums[0]) == 4:
            y, m, d = int(nums[0]), int(nums[1]), int(nums[2])
        else:
            y, m, d = int(nums[2]), int(nums[0]), int(nums[1])
        return f"{y:04d}{m:02d}{d:02d}"
    return s

# ===== Config =====
DEFAULT_COMPOSE_ORDER = ["birth_yyyymmdd", "insured_number", "insurer_number", "symbol"]

def load_cfg(mat_dir: Path) -> Dict[str, Any]:
    cfg_path = mat_dir / "custom_id_config.json"
    if not cfg_path.exists():
        dbg("custom_id_config.json が無いので既定値で進行")
        return {
            "add": {}, "mul": {},
            "compose_order": DEFAULT_COMPOSE_ORDER,
            "mapping": {},
            "strict_mapping": False,
            "debug": False,
        }
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    mapping: Dict[str, Any] = {}
    mfile = cfg.get("mapping_file")
    if mfile:
        mp = mat_dir / mfile
        if mp.exists():
            mapping = json.loads(mp.read_text(encoding="utf-8"))
        else:
            print(f"[CID][WARN] mapping_file が見つかりません: {mp}", file=sys.stderr)
    out = {
        "add": cfg.get("add", {}),
        "mul": cfg.get("mul", {}),
        "compose_order": cfg.get("compose_order") or DEFAULT_COMPOSE_ORDER,
        "mapping": mapping,
        "strict_mapping": bool(cfg.get("strict_mapping", False)),
        "debug": bool(cfg.get("debug", False)),
    }
    if out["debug"]:
        global DEBUG; DEBUG = True
    trace("load_cfg", {
        "compose_order": out["compose_order"],
        "strict_mapping": out["strict_mapping"],
        "mapping_fields": list(mapping.keys()) if isinstance(mapping, dict) else [],
    })
    return out

# ===== Logic 1–2 =====
def logic12(value_digits: str, add: int, mul: int) -> str:
    v = int(value_digits) if value_digits.isdigit() else 0
    return str((v + int(add or 0)) * int(mul or 1))

# ===== Logic 3 (mapping) =====
FIELDS = ["insurer_number", "symbol", "insured_number", "birth_yyyymmdd"]

def map_digits(num_str: str, table: Dict[str, str], strict: bool) -> str:
    out_chars: List[str] = []
    missing: List[str] = []
    for ch in num_str:
        if ch not in "0123456789":
            out_chars.append(ch);  continue
        if ch in table:
            out_chars.append(str(table[ch]))
        else:
            missing.append(ch)
            out_chars.append("" if strict else ch)
    if strict and missing:
        raise ValueError(f"mapping未定義の桁: {sorted(set(missing))}")
    return "".join(out_chars)

# ===== Core =====
def generate_id(
    insurer_number: str, symbol: str, insured_number: str, birth_yyyymmdd: str, mat_dir: Path
) -> Tuple[str, Dict[str, Any]]:
    cfg = load_cfg(mat_dir)
    compose_order = [k for k in (cfg.get("compose_order") or DEFAULT_COMPOSE_ORDER) if k in FIELDS]
    if len(compose_order) != 4:
        compose_order = DEFAULT_COMPOSE_ORDER

    # normalize
    vals = {
        "insurer_number": norm_number(insurer_number),
        "symbol":         norm_symbol(symbol),
        "insured_number": norm_number(insured_number),
        "birth_yyyymmdd": normalize_birth_any(birth_yyyymmdd),
    }
    trace("normalized", vals)

    # logic 1–2
    add_cfg, mul_cfg = cfg.get("add", {}), cfg.get("mul", {})
    nums12 = {
        "insurer_number": logic12(vals["insurer_number"], int(add_cfg.get("insurer", 0)), int(mul_cfg.get("insurer", 1))),
        "symbol":         logic12(vals["symbol"],         int(add_cfg.get("symbol", 0)),  int(mul_cfg.get("symbol", 1))),
        "insured_number": logic12(vals["insured_number"], int(add_cfg.get("insured_number", 0)), int(mul_cfg.get("insured_number", 1))),
        "birth_yyyymmdd": logic12(vals["birth_yyyymmdd"], int(add_cfg.get("birth", 0)),  int(mul_cfg.get("birth", 1))),
    }
    trace("logic12", nums12)

    # logic 3 (mapping)
    tokens: Dict[str, str] = {}
    maptbl, strict = (cfg.get("mapping") or {}), bool(cfg.get("strict_mapping", False))
    for key in FIELDS:
        table = maptbl.get(key) or {}
        if not isinstance(table, dict):
            table = {}
        token = map_digits(nums12[key], table, strict)
        tokens[key] = token
        trace_field(key, vals[key], vals[key], nums12[key], token)

    # compose
    parts = [tokens[k] for k in compose_order]
    final_id = "".join(parts)
    trace("compose", {"order": compose_order, "final_len": len(final_id), "head": final_id[:12], "tail": final_id[-12:]})

    meta = {
        "compose_order": compose_order,
        "values_normalized": vals,
        "logic12": nums12,
        "tokens": tokens,
        "strict_mapping": strict,
    }
    return final_id, meta

# ===== CLI =====
def main():
    global DEBUG, TRACE
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--insurer", required=True)
    ap.add_argument("--symbol",  required=True)
    ap.add_argument("--insured", required=True)
    ap.add_argument("--birth",   required=True)
    ap.add_argument("--mat",     default=str(Path(__file__).parent.parent / "mat"))
    ap.add_argument("--jsonout", action="store_true", help="標準出力をJSONにする（stderrのTRACEはそのまま）")
    ap.add_argument("--debug",   action="store_true", help="雑ログをstderrへ")
    ap.add_argument("--trace",   action="store_true", help="見やすい段階トレースをstderrへ")
    args = ap.parse_args()

    if args.debug: DEBUG = True
    if args.trace: TRACE = True

    final_id, meta = generate_id(args.insurer, args.symbol, args.insured, args.birth, Path(args.mat))

    # ★ stdout は ID だけ or JSON
    if args.jsonout:
        print(json.dumps({"id": final_id, "meta": meta}, ensure_ascii=False))
    else:
        print(final_id)

if __name__ == "__main__":
    main()
