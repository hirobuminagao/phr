# -*- coding: utf-8 -*-
"""
custom_id_gen.py — 正規化(固定幅上限) → 加算 → 乗算 → 1:1マッピング

仕様:
- 各フィールドは「数字のみ」に整えた後、幅(=最大桁)にフィット:
    * 入力桁が幅を超えたらエラー（桁切りはしない）
    * 不足は左ゼロ詰めでちょうど幅
- (v + add) * mul を適用し、結果の桁数が幅を超えたらエラー、未満は左ゼロ詰めでちょうど幅
- マッピングは 1 桁 → 1 文字（mapping_one_to_one を既定 True、strict_mapping で 0-9 漏れ検知可）
- 出力長は各フィールド幅の総和（例: 11+11+11+10=43）
- stdout: 最終IDのみ（--jsonout なら {"id": "...", "meta": {...}}）
- --trace: raw / fit / after_add / after_mul / mapped を stderr に出力

運用メモ:
- mat_dir は「設定ファイル(custom_id_config.json) と mapping JSON」を置くディレクトリ
- mat_dir を省略した場合は本ファイル位置から ../mat (= phr/mat) を既定にする
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


# ============================================================
# Debug / Trace
# ============================================================

DEBUG = os.getenv("TOKUHO_DEBUG", "").strip().lower() in {
    "1", "true", "t", "yes", "y", "on"
}
TRACE = os.getenv("TOKUHO_TRACE", "").strip().lower() in {
    "1", "true", "t", "yes", "y", "on"
}


def dbg(*args, **kwargs) -> None:
    if DEBUG:
        print("[CID][DBG]", *args, **kwargs, file=sys.stderr)


def warn(*args, **kwargs) -> None:
    print("[CID][WARN]", *args, **kwargs, file=sys.stderr)


def trace(title: str, data: Dict[str, Any]) -> None:
    if TRACE:
        print(f"[CID][TRACE] {title}", file=sys.stderr)
        for k, v in data.items():
            print(f"  - {k}: {v}", file=sys.stderr)


def trace_field(name: str, payload: Dict[str, Any]) -> None:
    if TRACE:
        print(f"[CID][FIELD:{name}]", file=sys.stderr)
        for k in ["raw", "fit", "after_add", "after_mul", "mapped"]:
            if k in payload:
                print(f"  {k:>10}: {payload[k]}", file=sys.stderr)


# ============================================================
# Normalizers
# ============================================================

HW_DIGITS = str.maketrans("０１２３４５６７８９", "0123456789")


def to_half_digits(s: str) -> str:
    return (s or "").translate(HW_DIGITS)


def digits_only(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())


def norm_number(raw: Optional[str]) -> str:
    return digits_only(to_half_digits((raw or "").strip()))


def norm_symbol_digits_only(raw: Optional[str]) -> str:
    # 記号は digits-only 運用（仕様）
    return digits_only(to_half_digits((raw or "").strip()))


def normalize_birth_any(raw: Optional[str]) -> str:
    """
    生年月日を YYYYMMDD 相当に寄せる。
    例:
      - '19750307'
      - '1975-03-07'
      - '1975/3/7'
      - '03/07/1975'
    """
    s = to_half_digits((raw or "").strip())
    if len(s) == 8 and s.isdigit():
        return s

    nums = re.split(r"[^\d]+", s)
    nums = [n for n in nums if n]
    if len(nums) == 3:
        if len(nums[0]) == 4:
            y, m, d = int(nums[0]), int(nums[1]), int(nums[2])
        else:
            y, m, d = int(nums[2]), int(nums[0]), int(nums[1])
        return f"{y:04d}{m:02d}{d:02d}"

    # フォールバック：数字だけ残す（最終的には fit_width_max 側で幅判定される）
    return digits_only(s)


# ============================================================
# 設定
# ============================================================

DEFAULT_COMPOSE_ORDER = ["birth_yyyymmdd", "insurance_number", "insurer_number", "symbol"]

EXPECTED_FIELDS = ("insurer_number", "symbol", "insurance_number", "birth_yyyymmdd")
REQ_ADD_KEYS = ("insurer", "symbol", "insurance_number", "birth")
REQ_LEN_KEYS = ("insurer", "symbol", "insurance_number", "birth")


def default_mat_dir() -> Path:
    """
    mat_dir を省略した場合の既定値。
    想定配置:
      - this file: phr/lib/custom_id_gen.py
      - mat dir : phr/mat
    """
    # .../phr/lib/custom_id_gen.py -> parents[1] == .../phr
    phr_root = Path(__file__).resolve().parents[1]
    return phr_root / "mat"


def load_cfg(mat_dir: Path) -> Dict[str, Any]:
    cfg_path = mat_dir / "custom_id_config.json"
    if not cfg_path.exists():
        raise FileNotFoundError(f"custom_id_config.json が見つかりません: {cfg_path}")

    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))

    # 既定値
    cfg.setdefault("compose_order", DEFAULT_COMPOSE_ORDER)
    cfg.setdefault("strict_mapping", True)         # 0-9 完全定義を必須
    cfg.setdefault("mapping_one_to_one", True)     # 1桁→1文字のみ
    cfg.setdefault("symbol_digits_only", True)
    cfg.setdefault("debug", False)

    # add/mul/lengths 必須検証
    for k in ("add", "mul", "lengths"):
        if k not in cfg or not isinstance(cfg[k], dict):
            raise ValueError(f"{k} が未定義/不正です(custom_id_config.json)")

    for k in REQ_ADD_KEYS:
        if k not in cfg["add"] or k not in cfg["mul"]:
            raise ValueError(f"add/mul のキー不足: {k}")

    for k in REQ_LEN_KEYS:
        if k not in cfg["lengths"]:
            raise ValueError(f"lengths のキー不足: {k}")

    # mapping 読み込み
    mapping: Dict[str, Any] = cfg.get("mapping", {})
    mfile = cfg.get("mapping_file")
    if mfile:
        mp = mat_dir / mfile
        if mp.exists():
            mapping = json.loads(mp.read_text(encoding="utf-8"))
        else:
            raise FileNotFoundError(f"mapping_file が見つかりません: {mp}")

    # マッピング検証（全フィールド×0-9 必須）
    strict = bool(cfg.get("strict_mapping", True))
    one2one = bool(cfg.get("mapping_one_to_one", True))

    for fld in EXPECTED_FIELDS:
        tbl = mapping.get(fld)
        if not isinstance(tbl, dict):
            if strict:
                raise ValueError(f"mapping[{fld}] が未定義です")
            mapping[fld] = {str(i): str(i) for i in range(10)}
            continue

        miss = [d for d in "0123456789" if d not in tbl]
        if miss and strict:
            raise ValueError(f"mapping[{fld}] に 0-9 の不足: {miss}")

        if one2one:
            bad_type = [d for d in "0123456789" if d in tbl and not isinstance(tbl[d], str)]
            if bad_type:
                raise ValueError(f"mapping[{fld}] の値が文字列ではありません: {bad_type}")

            bad_len = [d for d in "0123456789" if d in tbl and isinstance(tbl[d], str) and len(tbl[d]) != 1]
            if bad_len and strict:
                raise ValueError(f"mapping[{fld}] は 1桁→1文字のみ許可（長さ不一致: {bad_len}）")

    out = {
        "add": cfg["add"],
        "mul": cfg["mul"],
        "lengths": cfg["lengths"],
        "compose_order": [k for k in (cfg.get("compose_order") or []) if k in EXPECTED_FIELDS] or DEFAULT_COMPOSE_ORDER,
        "mapping": mapping,
        "strict_mapping": strict,
        "mapping_one_to_one": one2one,
        "symbol_digits_only": bool(cfg.get("symbol_digits_only")),
        "debug": bool(cfg.get("debug")),
    }

    if out["debug"]:
        global DEBUG
        DEBUG = True

    trace("load_cfg", {
        "mat_dir": str(mat_dir),
        "compose_order": out["compose_order"],
        "lengths": out["lengths"],
        "strict_mapping": out["strict_mapping"],
        "one_to_one": out["mapping_one_to_one"],
        "mapping_fields": list(mapping.keys()),
    })
    return out


# ============================================================
# Core helpers
# ============================================================

def fit_width_max(s: str, width: int) -> str:
    """数字以外除去 → 幅を超えたらエラー、未満は左ゼロ詰めでちょうど幅。"""
    d = digits_only(s)
    if len(d) > width:
        raise ValueError(f"正規化後の桁数が幅を超えました(width={width}, got={len(d)}, value={d})")
    return d.zfill(width)


def apply_add_mul_no_cut(value_digits: str, add: int, mul: int, width: int) -> Tuple[str, Dict[str, Any]]:
    """(v + add) * mul を適用。結果が幅超ならエラー、未満は左ゼロ詰め。"""
    v = int(value_digits) if value_digits.isdigit() else 0
    n_add = v + int(add)
    n_mul = n_add * int(mul)

    s = str(abs(n_mul))
    if len(s) > width:
        raise ValueError(
            f"(v+add)*mul の結果桁が幅を超えました(field_width={width}, after_add={n_add}, after_mul={n_mul}, str_len={len(s)})"
        )
    s = s.zfill(width)
    return s, {"after_add": n_add, "after_mul": n_mul}


def map_one_to_one(num_str: str, table: Dict[str, str]) -> str:
    out = []
    for ch in num_str:
        if ch not in "0123456789":
            out.append(ch)
        else:
            out.append(table.get(ch, ch))
    return "".join(out)


# ============================================================
# 生成
# ============================================================

FIELDS_ORDERED = ("insurer_number", "symbol", "insurance_number", "birth_yyyymmdd")


def generate_id(
    insurer_number: str,
    symbol: str,
    insurance_number: str,
    birth_yyyymmdd: str,
    mat_dir: Optional[Path] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    ID生成のエントリポイント。

    mat_dir:
      - 省略可（省略時は default_mat_dir() = phr/mat）
      - 呼び出し側が settings などを持つなら明示的に渡してもOK
    """
    mat_dir = mat_dir or default_mat_dir()
    cfg = load_cfg(mat_dir)

    A, M, L = cfg["add"], cfg["mul"], cfg["lengths"]
    mapping: Dict[str, Dict[str, str]] = cfg["mapping"]

    # 1) 正規化（固定幅上限）
    raw = {
        "insurer_number": norm_number(insurer_number),
        "symbol": norm_symbol_digits_only(symbol) if cfg["symbol_digits_only"] else to_half_digits(symbol or ""),
        "insurance_number": norm_number(insurance_number),
        "birth_yyyymmdd": normalize_birth_any(birth_yyyymmdd),
    }
    fit = {
        "insurer_number": fit_width_max(raw["insurer_number"], int(L["insurer"])),
        "symbol": fit_width_max(raw["symbol"], int(L["symbol"])),
        "insurance_number": fit_width_max(raw["insurance_number"], int(L["insurance_number"])),
        "birth_yyyymmdd": fit_width_max(raw["birth_yyyymmdd"], int(L["birth"])),
    }

    # 2)+3) 加算→乗算（幅厳守／ノーカット）
    num12: Dict[str, str] = {}
    metas: Dict[str, Dict[str, Any]] = {}

    v, meta = apply_add_mul_no_cut(fit["insurer_number"], int(A["insurer"]), int(M["insurer"]), int(L["insurer"]))
    num12["insurer_number"], metas["insurer_number"] = v, meta

    v, meta = apply_add_mul_no_cut(fit["symbol"], int(A["symbol"]), int(M["symbol"]), int(L["symbol"]))
    num12["symbol"], metas["symbol"] = v, meta

    v, meta = apply_add_mul_no_cut(
        fit["insurance_number"],
        int(A["insurance_number"]),
        int(M["insurance_number"]),
        int(L["insurance_number"]),
    )
    num12["insurance_number"], metas["insurance_number"] = v, meta

    v, meta = apply_add_mul_no_cut(fit["birth_yyyymmdd"], int(A["birth"]), int(M["birth"]), int(L["birth"]))
    num12["birth_yyyymmdd"], metas["birth_yyyymmdd"] = v, meta

    # 4) 1:1 マッピング
    tokens: Dict[str, str] = {}
    for key in FIELDS_ORDERED:
        tbl = mapping.get(key) or {str(i): str(i) for i in range(10)}
        token = map_one_to_one(num12[key], tbl)
        tokens[key] = token

        trace_field(key, {
            "raw": raw[key],
            "fit": fit[key],
            "after_add": metas[key]["after_add"],
            "after_mul": metas[key]["after_mul"],
            "mapped": token,
        })

    # compose
    order = [k for k in (cfg.get("compose_order") or []) if k in EXPECTED_FIELDS] or list(DEFAULT_COMPOSE_ORDER)
    final_id = "".join(tokens[k] for k in order)

    trace("compose", {
        "order": order,
        "final_len": len(final_id),
        "head": final_id[:16],
        "tail": final_id[-16:],
    })

    meta_all = {
        "mat_dir": str(mat_dir),
        "compose_order": order,
        "values_fit": fit,
        "logic12": num12,
        "after_add_mul": metas,
        "mapping_used": {k: sorted((mapping.get(k) or {}).keys()) for k in EXPECTED_FIELDS},
    }
    return final_id, meta_all


# ============================================================
# CLI
# ============================================================

def main() -> None:
    global DEBUG, TRACE

    ap = argparse.ArgumentParser()
    ap.add_argument("--insurer", required=True)
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--insured", dest="insurance_compat", required=False)  # 互換
    ap.add_argument("--insurance", dest="insurance", required=False)
    ap.add_argument("--birth", required=True)

    # 既定は phr/mat（本ファイルが phr/lib にある想定）
    ap.add_argument("--mat", default=str(default_mat_dir()))

    ap.add_argument("--jsonout", action="store_true")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--trace", action="store_true")
    args = ap.parse_args()

    if args.debug:
        DEBUG = True
    if args.trace:
        TRACE = True

    insurance_number = args.insurance if args.insurance is not None else args.insurance_compat
    if insurance_number is None:
        ap.error("--insurance もしくは互換の --insured が必要です")

    final_id, meta = generate_id(
        insurer_number=args.insurer,
        symbol=args.symbol,
        insurance_number=insurance_number,
        birth_yyyymmdd=args.birth,
        mat_dir=Path(args.mat),
    )

    if args.jsonout:
        print(json.dumps({"id": final_id, "meta": meta}, ensure_ascii=False))
    else:
        print(final_id)


if __name__ == "__main__":
    main()
