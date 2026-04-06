# -*- coding: utf-8 -*-
"""
check_tokuho_xml.py v1.3.5 — 特定保健指導XMLを解析してCSV出力

変更点（v1.3.5）
- MySQL(work_other.shg_result) から person_id_custom に紐づく
  usage_ticket_number / expiration_date を取得し、
  export_shg_report / export_outcome_report に db_ticket_no / db_ticket_exp として出力

変更点（v1.3.4）
- outcome_report_* のカラムに initial_xml を追加し、final_xml の前に配置

変更点（v1.3.3）
- 90040 が存在しても、90070 集計の「回数/分」列は常に出力するように変更
  （以前は process_source == "90070_evn" の時のみ埋めていた）
- 比較系のカラム（90040/90060/90070の差分など）は引き続き出力しない（v1.3.2のまま）

前提：
- 代表ソースは 90040 を優先（無ければ 90070_evn）。minutesは 90040 はイベント集計、90070 は durations の総和。
"""

from __future__ import annotations

import argparse
import csv
import datetime
import json
import os
import shutil
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from lxml import etree

# ====== Debug ======
DEBUG = os.getenv("TOKUHO_DEBUG", "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def dbg(*args, **kwargs):
    if DEBUG:
        print("[CHK][DBG]", *args, **kwargs, file=sys.stderr)


# ====== Namespaces ======
CDA_NS = {"cda": "urn:hl7-org:v3"}

# ====== Normalizers ======
FW_DIGITS = str.maketrans("0123456789", "０１２３４５６７８９")
HW_DIGITS = str.maketrans("０１２３４５６７８９", "0123456789")


def to_half_digits(s: str) -> str:
    return "".join(ch.translate(HW_DIGITS) for ch in s or "")


def norm_number(raw: Optional[str]) -> str:
    return to_half_digits((raw or "").strip())


def norm_symbol_human(raw: Optional[str]) -> str:
    """表示用：全角→半角（数字/ハイフン/スペース）、日本語はそのまま。"""
    if raw is None:
        return ""
    s = (raw or "").strip()
    s = to_half_digits(s)
    s = s.replace("－", "-").replace("ー", "-").replace("―", "-").replace("—", "-")
    s = s.replace("　", " ")
    return s


def extract_symbol_digits(raw: Optional[str]) -> str:
    """digits-only 抽出（例: '０１－平'→'01', '埼１００'→'100'）。"""
    s = to_half_digits((raw or "").strip())
    return "".join(ch for ch in s if ch.isdigit())


def norm_kana(raw: Optional[str]) -> str:
    if raw is None:
        return ""
    return "".join((raw or "").split())


def _normalize_birth_any(raw: Optional[str]) -> str:
    if not raw:
        return ""
    s = raw.strip()
    if len(s) == 8 and s.isdigit():
        return s
    try:
        import re

        nums = [n for n in re.split(r"[^\d]+", s) if n]
        if len(nums) == 3:
            if len(nums[0]) == 4:
                y, m, d = int(nums[0]), int(nums[1]), int(nums[2])
            else:
                y, m, d = int(nums[2]), int(nums[0]), int(nums[1])
            return f"{y:04d}{m:02d}{d:02d}"
    except Exception:
        pass
    return s


def make_person_key(
    insurer: Optional[str],
    symbol: Optional[str],
    number: Optional[str],
    name_kana: Optional[str],
    birth: Optional[str],
    gender: Optional[str],
) -> str:
    insurer_s = to_half_digits(insurer or "")
    symbol_s = norm_symbol_human(symbol or "")
    number_s = norm_number(number or "")
    name_k = norm_kana(name_kana or "")
    birth_s = (birth or "").strip()
    gender_s = (gender or "").strip()
    return "|".join([insurer_s, symbol_s, number_s, name_k, birth_s, gender_s])


# ====== XML I/O ======
def read_xml(path: Path):
    parser = etree.XMLParser(remove_blank_text=True, recover=True)
    with open(path, "rb") as f:
        return etree.fromstring(f.read(), parser=parser)


def _text_or(listlike, default: str = "") -> str:
    if not listlike:
        return default
    v = listlike[0]
    if hasattr(v, "itertext"):
        return "".join(v.itertext()).strip()
    return str(v)


def _get_number(tree_or_elem, xps: List[str]) -> float:
    for xp in (xps or []):
        try:
            vals = tree_or_elem.xpath(xp, namespaces=CDA_NS)
            s = _text_or(vals, "")
            ds = "".join(ch for ch in s if (ch.isdigit() or ch in ".-"))
            if ds == "":
                continue
            return float(ds) if "." in ds else int(ds)  # type: ignore[return-value]
        except Exception:
            continue
    return 0  # type: ignore[return-value]


# ====== 設定ファイル ======
def load_cfg(mat_dir: Path) -> Dict[str, Any]:
    p = mat_dir / "outcome_process_config.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}


def load_custom_cfg(mat_dir: Path) -> Dict[str, Any]:
    p = mat_dir / "custom_id_config.json"
    if not p.exists():
        return {}
    try:
        cfg = json.loads(p.read_text(encoding="utf-8"))
        return {"debug": bool(cfg.get("debug", False))}
    except Exception:
        return {}


# ====== OID: oid_code_master.csv から (system, code)→名称 を構築 ======
OID_LOOKUP: Dict[Tuple[str, str], str] = {}


def load_oid_master_from_csv(mat_dir: Path) -> Dict[Tuple[str, str], str]:
    path = mat_dir / "oid_code_master.csv"
    lookup: Dict[Tuple[str, str], str] = {}
    if not path.exists():
        dbg("oid_code_master.csv not found", {"path": str(path)})
        return lookup
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            rdr = csv.DictReader(f)

            def pick(d: Dict[str, str], names: List[str]) -> str:
                for k in d.keys():
                    for n in names:
                        if k.strip().lower() == n:
                            return d[k]
                return ""

            for row in rdr:
                if not row:
                    continue
                system = (pick(row, ["oid_code"]) or "").strip()
                code = (pick(row, ["oid_code_value"]) or "").strip()
                name = (pick(row, ["oid_code_value_name"]) or "").strip()
                if system and code and name:
                    lookup[(system, code)] = name
        dbg("oid_code_master loaded", {"count": len(lookup), "file": str(path)})
    except Exception as e:
        dbg("oid_code_master.csv load error", {"error": str(e), "file": str(path)})
    return lookup


def oid_display(system: str, code: str) -> str:
    if not system or not code:
        return ""
    return OID_LOOKUP.get((system.strip(), code.strip()), "")



# ====== MySQL: work_other.shg_result から利用券情報を取得 ======
def load_shg_result_from_mysql() -> Dict[str, Dict[str, Any]]:
    """
    work_other.shg_result から person_id_custom→{usage_ticket_number, expiration_date}
    のマップを構築する。失敗した場合は空 dict を返す。
    """
    try:
        import mysql.connector  # type: ignore
    except Exception as e:
        dbg("mysql connector import error", {"error": str(e)})
        return {}

    cfg = {
        "host": os.getenv("DEV_DB_HOST", "10.0.10.201"),
        "port": int(os.getenv("DEV_DB_PORT", "3306")),
        "user": os.getenv("DEV_DB_USER", "devadmin"),
        "password": os.getenv("DEV_DB_PASSWORD", "Ksmd!1189"),
        "database": "work_other",
    }

    mapping: Dict[str, Dict[str, Any]] = {}

    try:
        conn = mysql.connector.connect(**cfg)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                person_id_custom,
                usage_ticket_number,
                expiration_date
            FROM shg_result
            """
        )
        rows = cur.fetchall()  # Pylance対策：カーソルを明示的にリストにする

        for pid, ticket, exp in rows:
            if not pid:
                continue
            mapping[str(pid)] = {
                "usage_ticket_number": ticket or "",
                "expiration_date": str(exp) if exp else "",
            }

        cur.close()
        conn.close()
        dbg("shg_result loaded", {"count": len(mapping)})
    except Exception as e:
        dbg("load_shg_result_from_mysql error", {"error": str(e)})
        return {}

    return mapping


# ====== 外部カスタムID生成 呼び出し ======
def gen_custom_id_external(
    vals: Dict[str, str],
    mat_dir: str,
    debug: bool = False,
    jsonout: bool = False,
) -> Tuple[str, str]:
    candidates = [
        Path(__file__).parents[1] / "lib" / "custom_id_gen.py",
        Path(__file__).parent / "custom_id_gen.py",
    ]
    gen_path = next((p for p in candidates if p.exists()), None)
    if not gen_path:
        return "", f"custom_id_gen.py が見つかりません: {candidates}"
    py = shutil.which("python") or sys.executable
    cmd = [
        py,
        str(gen_path),
        "--insurer",
        vals.get("insurer_number", ""),
        "--symbol",
        vals.get("symbol", ""),
        "--insurance",
        vals.get("insurance_number", ""),
        "--birth",
        vals.get("birth_yyyymmdd", ""),
        "--mat",
        mat_dir,
    ]
    if debug:
        cmd.append("--trace")
    if jsonout:
        cmd.append("--jsonout")

    dbg("call_custom_id_gen", {"cmd": cmd})
    cp = subprocess.run(cmd, capture_output=True, text=True)
    if cp.returncode != 0:
        err = (cp.stderr or cp.stdout or "").strip()
        return "", f"custom_id_gen 失敗: {err}"

    out = (cp.stdout or "").strip()
    if jsonout and out.startswith("{") and out.endswith("}"):
        try:
            j = json.loads(out)
            return j.get("id", ""), ""
        except Exception as e:
            return "", f"JSON parse error: {e} / out={out[:200]}..."
    return out, ""


# ====== 基本項目抽出 ======
def extract_basic(tree) -> Dict[str, str]:
    report = _text_or(
        tree.xpath(
            "//cda:code[@codeSystem='1.2.392.200119.6.1001']/@code",
            namespaces=CDA_NS,
        ),
        "",
    )
    insurer = _text_or(
        tree.xpath(
            "//cda:recordTarget//cda:patientRole/cda:id[@root='1.2.392.200119.6.101']/@extension",
            namespaces=CDA_NS,
        ),
        "",
    )
    symbol = _text_or(
        tree.xpath(
            "//cda:recordTarget//cda:patientRole/cda:id[@root='1.2.392.200119.6.204']/@extension",
            namespaces=CDA_NS,
        ),
        "",
    )
    number = _text_or(
        tree.xpath(
            "//cda:recordTarget//cda:patientRole/cda:id[@root='1.2.392.200119.6.205']/@extension",
            namespaces=CDA_NS,
        ),
        "",
    )
    name = _text_or(
        tree.xpath(
            "//cda:recordTarget//cda:patientRole//cda:patient/cda:name",
            namespaces=CDA_NS,
        ),
        "",
    )
    gender = _text_or(
        tree.xpath(
            "//cda:recordTarget//cda:patient//cda:administrativeGenderCode/@code",
            namespaces=CDA_NS,
        ),
        "",
    )
    birth = _text_or(
        tree.xpath(
            "//cda:recordTarget//cda:patient//cda:birthTime/@value",
            namespaces=CDA_NS,
        ),
        "",
    )
    ticket_no = _text_or(
        tree.xpath(
            "//cda:participant[@typeCode='HLD'][cda:functionCode[@code='2' and @codeSystem='1.2.392.200119.6.208']]//cda:associatedEntity/cda:id/@extension",
            namespaces=CDA_NS,
        ),
        "",
    )
    ticket_exp = _text_or(
        tree.xpath(
            "//cda:participant[@typeCode='HLD'][cda:functionCode[@code='2' and @codeSystem='1.2.392.200119.6.208']]//cda:time/cda:high/@value",
            namespaces=CDA_NS,
        ),
        "",
    )
    final_date = _text_or(
        tree.xpath(
            "//cda:section[cda:code/@code='90060']//cda:act/cda:effectiveTime/@value",
            namespaces=CDA_NS,
        ),
        "",
    )
    if final_date == "":
        final_date = _text_or(
            tree.xpath(
                "//cda:documentationOf//cda:serviceEvent/cda:effectiveTime/@value",
                namespaces=CDA_NS,
            ),
            "",
        )
    init_date = _text_or(
        tree.xpath(
            "//cda:section[cda:code/@code='90030']//cda:act/cda:effectiveTime/@value",
            namespaces=CDA_NS,
        ),
        "",
    )
    level_code = _text_or(
        tree.xpath(
            "//cda:section[cda:code/@code='90010']//cda:observation[cda:code/@code='1020000001']/cda:value/@code",
            namespaces=CDA_NS,
        ),
        "",
    )
    level_map = {"1": "積極的支援", "2": "動機付け支援"}
    level_text = level_map.get(level_code, "")
    return {
        "report_code": report,
        "insurer": insurer,
        "symbol": symbol,
        "number": number,
        "name": name,
        "gender": gender,
        "birth": birth,
        "ticket_no": ticket_no,
        "ticket_exp": ticket_exp,
        "initial_date": init_date,
        "final_date": final_date,
        "level_code": level_code,
        "level_text": level_text,
    }


# ====== robust 真偽 ======
def _robust_bool_from_value_nodes(nodes) -> bool:
    truthy = {"1", "true", "t", "yes", "y", "on"}
    falsy = {"0", "false", "f", "no", "n", "off", ""}
    saw_false = False
    for n in nodes:
        try:
            nf = n.get("nullFlavor")
            if nf is not None:
                saw_false = True
                continue
            vals: List[str] = []
            c = n.get("code")
            if c is not None:
                vals.append(c)
            v = n.get("value")
            if v is not None:
                vals.append(v)
            txt = "".join(n.itertext()).strip()
            if txt != "":
                vals.append(txt)
            for s in vals:
                s = str(s).strip()
                sl = s.lower()
                if sl in truthy:
                    return True
                if sl in falsy:
                    saw_false = True
                    continue
                sn = "".join(ch for ch in s if (ch.isdigit() or ch in ".-"))
                if sn != "":
                    try:
                        if float(sn) != 0.0:
                            return True
                        else:
                            saw_false = True
                            continue
                    except Exception:
                        pass
                if s != "":
                    return True
        except Exception:
            continue
    return False if saw_false else False


# ====== 90030 サポート ======
def _pick_time_value(elem) -> str:
    v = _text_or(
        elem.xpath("./cda:effectiveTime/@value", namespaces=CDA_NS),
        "",
    )
    if v:
        return v
    v = _text_or(
        elem.xpath(
            "./cda:effectiveTime/cda:low/@value",
            namespaces=CDA_NS,
        ),
        "",
    )
    if v:
        return v
    v = _text_or(
        elem.xpath(
            "./cda:effectiveTime/cda:high/@value",
            namespaces=CDA_NS,
        ),
        "",
    )
    if v:
        return v
    v = _text_or(elem.xpath("./*[local-name()='effectiveTime']/@value"), "")
    if v:
        return v
    v = _text_or(
        elem.xpath(
            "./*[local-name()='effectiveTime']/*[local-name()='low']/@value"
        ),
        "",
    )
    if v:
        return v
    v = _text_or(
        elem.xpath(
            "./*[local-name()='effectiveTime']/*[local-name()='high']/@value"
        ),
        "",
    )
    return v or ""


def extract_initial_interview_mode(tree, cfg: Dict[str, Any]) -> Tuple[str, str]:
    section = tree.xpath(
        "//cda:section[cda:code/@code='90030']",
        namespaces=CDA_NS,
    )
    if not section:
        section = tree.xpath(
            "//*[local-name()='section' and *[local-name()='code' and @code='90030']]"
        )
    if not section:
        return "", ""
    sec = section[0]

    nodes = sec.xpath(
        ".//cda:entry/*[self::cda:act or self::cda:encounter or self::cda:procedure or self::cda:observation]",
        namespaces=CDA_NS,
    )
    nodes += sec.xpath(
        "./*[self::cda:act or self::cda:encounter or self::cda:procedure or self::cda:observation]",
        namespaces=CDA_NS,
    )
    nodes += sec.xpath(
        ".//*[local-name()='entry']/*[local-name()='act' or local-name()='encounter' or local-name()='procedure' or local-name()='observation']"
    )
    nodes += sec.xpath(
        "./*[local-name()='act' or local-name()='encounter' or local-name()='procedure' or local-name()='observation']"
    )

    candidates: List[Tuple[str, str, str]] = []  # (date, system, code)
    for n in nodes:
        system = _text_or(
            n.xpath("./cda:code/@codeSystem", namespaces=CDA_NS),
            "",
        ) or _text_or(n.xpath("./*[local-name()='code']/@codeSystem"), "")
        code = _text_or(
            n.xpath("./cda:code/@code", namespaces=CDA_NS),
            "",
        ) or _text_or(n.xpath("./*[local-name()='code']/@code"), "")
        date = _pick_time_value(n)
        if not (system and code):
            system = system or _text_or(
                n.xpath(".//cda:code/@codeSystem", namespaces=CDA_NS),
                "",
            ) or _text_or(
                n.xpath(".//*[local-name()='code']/@codeSystem"),
                "",
            )
            code = code or _text_or(
                n.xpath(".//cda:code/@code", namespaces=CDA_NS),
                "",
            ) or _text_or(
                n.xpath(".//*[local-name()='code']/@code"),
                "",
            )
        if (system and code) or date:
            candidates.append((date or "", system, code))

    if not candidates:
        return "", ""
    candidates.sort(key=lambda t: (t[0] == "", t[0]))
    _, system, code = candidates[0]

    name = oid_display(system, code)
    return code, name


# ====== 目標抽出 ======
def extract_initial_goals(tree) -> Dict[str, bool]:
    def flag(code: str) -> bool:
        base = f"//cda:section[cda:code/@code='90030']//cda:observation[cda:code/@code='{code}']/cda:value"
        nodes = tree.xpath(base, namespaces=CDA_NS)
        return _robust_bool_from_value_nodes(nodes)

    goals: Dict[str, bool] = {}
    goals["腹囲・体重の改善"] = flag("1021001053")
    goals["生活習慣の改善(食習慣)"] = flag("1021001054")
    goals["生活習慣の改善(運動習慣)"] = flag("1021001055")
    goals["生活習慣の改善(喫煙習慣)"] = flag("1021001056")
    goals["生活習慣の改善(休養習慣)"] = flag("1021001057")
    goals["生活習慣の改善(その他)"] = flag("1021001058")
    return goals


def extract_motivation_goals_from_final(tree) -> Dict[str, bool]:
    def flag_in_90060_by_code(code: str) -> bool:
        base = f"//cda:section[cda:code/@code='90060']//cda:observation[cda:code/@code='{code}']/cda:value"
        nodes = tree.xpath(base, namespaces=CDA_NS)
        return _robust_bool_from_value_nodes(nodes)

    out: Dict[str, bool] = {
        "腹囲・体重の改善": flag_in_90060_by_code("1021001053"),
        "生活習慣の改善(食習慣)": flag_in_90060_by_code("1021001054"),
        "生活習慣の改善(運動習慣)": flag_in_90060_by_code("1021001055"),
        "生活習慣の改善(喫煙習慣)": flag_in_90060_by_code("1021001056"),
        "生活習慣の改善(休養習慣)": flag_in_90060_by_code("1021001057"),
        "生活習慣の改善(その他の生活習慣)": flag_in_90060_by_code("1042001046"),
    }
    fallback_map = {
        "腹囲・体重の改善": "1042001044",
        "生活習慣の改善(食習慣)": "1042001042",
        "生活習慣の改善(運動習慣)": "1042001041",
        "生活習慣の改善(喫煙習慣)": "1042001043",
        "生活習慣の改善(休養習慣)": "1042001045",
        "生活習慣の改善(その他の生活習慣)": "1042001046",
    }
    for k in list(out.keys()):
        if not out[k]:
            code = fallback_map.get(k, None)
            if code:
                nodes = tree.xpath(
                    f"//cda:section[cda:code/@code='90060']//cda:observation[cda:code/@code='{code}']/cda:value",
                    namespaces=CDA_NS,
                )
                out[k] = _robust_bool_from_value_nodes(nodes)
    return out


# ====== 最終90060: 達成・ポイント ======
def extract_final_outcomes(tree) -> Tuple[Dict[str, bool], int, str]:
    def _code(xp: str) -> str:
        return _text_or(tree.xpath(xp, namespaces=CDA_NS), "").strip()

    belly_code = _code(
        "//cda:section[cda:code/@code='90060']//cda:observation[cda:code/@code='1042001044']/cda:value/@code"
    )
    belly_ok = belly_code in {"1", "2"}
    belly_text_map = {"1": "1cm/1kg", "2": "2cm/2kg"}
    belly_text = belly_text_map.get(belly_code, "未達成")

    def ok1(xp: str) -> bool:
        return _code(xp) == "1"

    outs: Dict[str, bool] = {}
    outs["腹囲・体重の改善"] = belly_ok
    outs["生活習慣の改善(食習慣)"] = ok1(
        "//cda:section[cda:code/@code='90060']//cda:observation[cda:code/@code='1042001042']/cda:value/@code"
    )
    outs["生活習慣の改善(運動習慣)"] = ok1(
        "//cda:section[cda:code/@code='90060']//cda:observation[cda:code/@code='1042001041']/cda:value/@code"
    )
    outs["生活習慣の改善(喫煙習慣)"] = ok1(
        "//cda:section[cda:code/@code='90060']//cda:observation[cda:code/@code='1042001043']/cda:value/@code"
    )
    outs["生活習慣の改善(休養習慣)"] = ok1(
        "//cda:section[cda:code/@code='90060']//cda:observation[cda:code/@code='1042001045']/cda:value/@code"
    )
    outs["生活習慣の改善(その他の生活習慣)"] = ok1(
        "//cda:section[cda:code/@code='90060']//cda:observation[cda:code/@code='1042001046']/cda:value/@code"
    )
    total_pts = _get_number(
        tree,
        [
            "//cda:section[cda:code/@code='90060']//cda:observation[cda:code/@code='1042001060']/cda:value/@value",
            "//*[local-name()='section' and *[local-name()='code' and @code='90060']]"
            "//*[local-name()='observation' and *[local-name()='code' and @code='1042001060']]"
            "/*[local-name()='value']/@value",
        ],
    )
    return outs, int(total_pts or 0), belly_text


# ====== 最終90060: 最終測定値（腹囲・体重） ======
def _get_pq_float_or_none(tree, xps: List[str]) -> Optional[float]:
    for xp in (xps or []):
        try:
            vals = tree.xpath(xp, namespaces=CDA_NS)
            if not vals:
                continue
            s = _text_or(vals, "")
            ds = "".join(ch for ch in s if (ch.isdigit() or ch in ".-"))
            if ds != "":
                try:
                    return float(ds)
                except Exception:
                    continue
        except Exception:
            continue
    return None


def extract_final_measurements(tree) -> Tuple[Optional[float], Optional[float]]:
    waist_cm = _get_pq_float_or_none(
        tree,
        [
            "//cda:section[cda:code/@code='90060']//cda:observation[cda:code/@code='1042001031']/cda:value/@value",
            "//*[local-name()='section' and *[local-name()='code' and @code='90060']]"
            "//*[local-name()='observation' and *[local-name()='code' and @code='1042001031']]"
            "/*[local-name()='value']/@value",
        ],
    )
    weight_kg = _get_pq_float_or_none(
        tree,
        [
            "//cda:section[cda:code/@code='90060']//cda:observation[cda:code/@code='1042001032']/cda:value/@value",
            "//*[local-name()='section' and *[local-name()='code' and @code='90060']]"
            "//*[local-name()='observation' and *[local-name()='code' and @code='1042001032']]"
            "/*[local-name()='value']/@value",
        ],
    )
    return waist_cm, weight_kg


# ====== 90070（集計） ======
def extract_process_aggregate_final(tree, cfg: Dict[str, Any]) -> Dict[str, Any]:
    ag = (cfg.get("process_aggregate_90070_evn") or {}) if isinstance(cfg, dict) else {}
    counts_map = ag.get("counts") or {}
    durs_map = ag.get("durations_min") or {}
    pts_map = ag.get("points") or {}

    def pick(mp: Dict[str, str]) -> Dict[str, int]:
        out: Dict[str, int] = {}
        for k, xp in mp.items():
            val = _get_number(tree, [xp])
            out[k] = int(val or 0)
        return out

    counts = pick(counts_map)
    durs = pick(durs_map)
    cont = int(_get_number(tree, [pts_map.get("process_continuous_total", "")]) if pts_map else 0)
    grand = int(_get_number(tree, [pts_map.get("process_grand_total", "")]) if pts_map else 0)
    return {"counts": counts, "durations_min": durs, "_total_points": cont, "_grand_total": grand}


# ====== 90040（イベント） ======
def extract_process_events(tree) -> Dict[str, Any]:
    evts: List[Dict[str, Any]] = []
    acts = tree.xpath(
        "//cda:section[cda:code/@code='90040']//cda:entry/cda:act",
        namespaces=CDA_NS,
    )
    for act in acts:
        mode = _text_or(act.xpath("./cda:code/@code", namespaces=CDA_NS), "")
        date = _text_or(act.xpath("./cda:effectiveTime/@value", namespaces=CDA_NS), "")
        pts = _get_number(act, [".//cda:observation[cda:code/@code='1032300014']/cda:value/@value"])
        mins = _get_number(
            act,
            [
                ".//cda:observation[cda:code/@code='1032300013']/cda:effectiveTime/cda:width/@value"
            ],
        )
        evts.append(
            {"mode_code": mode, "date": date, "minutes": int(mins or 0), "points": int(pts or 0)}
        )
    total_pts = sum(e["points"] for e in evts)
    total_min = sum(e["minutes"] for e in evts)
    return {"events": evts, "_total_points": total_pts, "_total_minutes": total_min}


# ====== 期間判定 ======
def _parse_yyyymmdd(s: str) -> Optional[datetime.date]:
    try:
        return datetime.date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
    except Exception:
        return None


def _last_day_of_month(y: int, m: int) -> datetime.date:
    if m == 12:
        return datetime.date(y, 12, 31)
    d = datetime.date(y, m + 1, 1) - datetime.timedelta(days=1)
    return d


def _add_months(d: datetime.date, months: int, eom_clamp: bool = True) -> datetime.date:
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    day = d.day
    try:
        return datetime.date(y, m, day)
    except ValueError:
        if eom_clamp:
            ld = _last_day_of_month(y, m)
            return ld
        raise


def compute_duration_verdict(
    policy: Dict[str, Any],
    init_s: str,
    final_s: str,
) -> Tuple[Optional[int], str, str, str]:
    d0 = _parse_yyyymmdd(init_s)
    d1 = _parse_yyyymmdd(final_s)
    if not (d0 and d1):
        return None, "", "", "N/A(欠損)"
    days = (d1 - d0).days
    mode = (policy.get("mode") or "days").lower()
    if mode == "calendar":
        months = int(policy.get("months", 3))
        eom = bool(policy.get("eom_clamp", True))
        thr_date = _add_months(d0, months, eom)
        verdict = "OK" if d1 >= thr_date else "NG"
        return days, "calendar", f"{months}カ月(暦)以上", verdict
    else:
        thr = int(policy.get("threshold_days", 93))
        verdict = "OK" if days >= thr else "NG"
        return days, "days", f"{thr}日以上", verdict


# ====== スキャン ======
def scan_xmls(xml_dir: Path) -> List[Path]:
    return list(xml_dir.rglob("DATA/*.xml"))


# ====== メイン処理 ======
def main() -> None:
    global DEBUG, OID_LOOKUP

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--xml_dir",
        default=str(Path(__file__).parent / "input"),
        help="input ディレクトリ（バンドル/ DATA/*.xml を読む） (default: <script_dir>/input)",
    )
    ap.add_argument("--outdir", default=str(Path(__file__).parent / "out"))
    ap.add_argument(
        "--mat",
        default=str(Path(__file__).parent.parent / "mat"),
        help="各種設定（custom_id_config.json 等）を置くディレクトリ (default: <script_dir>/../mat)",
    )
    ap.add_argument(
        "--debug",
        action="store_true",
        help="デバッグログを出力する（外部ジェネレータはstderrのみ）",
    )
    args = ap.parse_args()

    DEBUG = bool(args.debug) or DEBUG

    xml_dir = Path(args.xml_dir)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    out_shg = outdir / "export_shg_report"
    out_out = outdir / "export_outcome_report"
    out_shg.mkdir(exist_ok=True)
    out_out.mkdir(exist_ok=True)

    # 設定ロード
    cfg = load_cfg(Path(args.mat))
    custom_cfg = load_custom_cfg(Path(args.mat))
    if custom_cfg.get("debug"):
        DEBUG = True

    # OID マスター読み込み
    OID_LOOKUP = load_oid_master_from_csv(Path(args.mat))

    dbg(
        "env",
        {
            "CLI_debug": args.debug,
            "ENV_debug": os.getenv("TOKUHO_DEBUG"),
            "JSON_debug": bool(custom_cfg.get("debug")),
            "FINAL_DEBUG": DEBUG,
            "OID_count": len(OID_LOOKUP),
        },
    )

    people: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
    shg_rows: List[Dict[str, Any]] = []

    # ---- XML 読み込みループ ----
    for xml_path in scan_xmls(xml_dir):
        try:
            tree = read_xml(xml_path)
        except Exception as e:
            shg_rows.append(
                {
                    "folder": str(xml_path.parents[1].name),
                    "file": xml_path.name,
                    "person_id_custom": "",
                    "person_id_custom_error": "",
                    "report_code": "",
                    "insurer": "",
                    "symbol": "",
                    "number": "",
                    "birth_raw": "",
                    "birth": "",
                    "initial_date": "",
                    "final_date": "",
                    "level_code": "",
                    "level_text": "",
                    "ticket_no": "",
                    "ticket_exp": "",
                    "init_mode_code_initial": "",
                    "init_mode_text_initial": "",
                    "init_mode_code_from_final": "",
                    "init_mode_text_from_final": "",
                    "error": f"XML parse error: {e}",
                }
            )
            continue

        b = extract_basic(tree)
        birth_norm = _normalize_birth_any(b["birth"])

        # ---- カスタムID（外部）----
        vals_for_id = {
            "insurer_number": to_half_digits(b["insurer"] or ""),
            "symbol": norm_symbol_human(b["symbol"] or ""),
            "insurance_number": norm_number(b["number"] or ""),
            "birth_yyyymmdd": (birth_norm or "").strip(),
        }
        dbg("record_vals_for_id", {"file": xml_path.name, **vals_for_id})
        person_id_custom, person_id_custom_error = gen_custom_id_external(
            vals_for_id,
            str(args.mat),
            debug=DEBUG,
            jsonout=False,
        )

        # グルーピング用キー
        person_key = make_person_key(
            insurer=b["insurer"],
            symbol=b["symbol"],
            number=b["number"],
            name_kana=b["name"],
            birth=b["birth"],
            gender=b["gender"],
        )

        # 初回面談方式
        init_mode_code_initial = ""
        init_mode_text_initial = ""
        init_mode_code_from_final = ""
        init_mode_text_from_final = ""
        role = (
            "initial"
            if b["report_code"] == "21"
            else ("final" if b["report_code"] == "22" else None)
        )
        if role == "initial":
            c, t = extract_initial_interview_mode(tree, cfg)
            init_mode_code_initial, init_mode_text_initial = c, t
        elif role == "final":
            c, t = extract_initial_interview_mode(tree, cfg)
            init_mode_code_from_final, init_mode_text_from_final = c, t

        # サマリー行（shg）
        birth_fmt = b["birth"]
        if len(birth_fmt) == 8 and birth_fmt.isdigit():
            birth_fmt = f"{birth_fmt[:4]}-{birth_fmt[4:6]}-{birth_fmt[6:]}"

        shg_rows.append(
            {
                "folder": str(xml_path.parents[1].name),
                "file": xml_path.name,
                "person_id_custom": person_id_custom,
                "person_id_custom_error": person_id_custom_error,
                "report_code": b["report_code"],
                "insurer": to_half_digits(b["insurer"]),
                "symbol": norm_symbol_human(b["symbol"]),
                "number": norm_number(b["number"]),
                "birth_raw": b["birth"],
                "birth": birth_fmt,
                "initial_date": b["initial_date"],
                "final_date": b["final_date"],
                "level_code": b["level_code"],
                "level_text": b["level_text"],
                "ticket_no": b["ticket_no"],
                "ticket_exp": b["ticket_exp"],
                "init_mode_code_initial": init_mode_code_initial,
                "init_mode_text_initial": init_mode_text_initial,
                "init_mode_code_from_final": init_mode_code_from_final,
                "init_mode_text_from_final": init_mode_text_from_final,
                "error": "",
            }
        )

        if role:
            rec = {
                "path": str(xml_path),
                "xml": xml_path.name,
                "basic": b,
                "folder": str(xml_path.parents[1].name),
                "init_mode_code": (
                    init_mode_code_initial if role == "initial" else init_mode_code_from_final
                ),
                "init_mode_text": (
                    init_mode_text_initial if role == "initial" else init_mode_text_from_final
                ),
            }
            people[person_key][role] = rec

    # ---- XML 読み込みループ終了 ----

    # MySQL(work_other.shg_result) から利用券情報を取得
    shg_result_map: Dict[str, Dict[str, Any]] = {}
    try:
        shg_result_map = load_shg_result_from_mysql()
    except Exception as e:
        dbg("shg_result_map load failed", {"error": str(e)})
        shg_result_map = {}

    # ---- export_shg_report ----
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    shg_cols = [
        "folder",
        "file",
        "person_id_custom",
        "person_id_custom_error",
        "report_code",
        "insurer",
        "symbol",
        "number",
        "birth_raw",
        "birth",
        "initial_date",
        "final_date",
        "level_code",
        "level_text",
        "ticket_no",
        "ticket_exp",
        "init_mode_code_initial",
        "init_mode_text_initial",
        "init_mode_code_from_final",
        "init_mode_text_from_final",
        "db_ticket_no",
        "db_ticket_exp",
        "error",
    ]
    out_shg = outdir / "export_shg_report"
    out_shg.mkdir(exist_ok=True)
    shg_csv = out_shg / f"shg_report_{ts}.csv"

    # shg_rows に MySQL の利用券情報をマージ
    for row in shg_rows:
        pid = (row.get("person_id_custom") or "").strip()
        info = shg_result_map.get(pid, {})
        row["db_ticket_no"] = info.get("usage_ticket_number", "")
        row["db_ticket_exp"] = info.get("expiration_date", "")

    with open(shg_csv, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=shg_cols)
        w.writeheader()
        w.writerows(shg_rows)

    # ---- export_outcome_report ----
    cols = [
        "person_key",
        "person_id",
        "person_id_custom",
        "person_id_custom_error",
        "db_ticket_no",
        "db_ticket_exp",
        "initial_xml",
        "final_xml",  # ★ v1.3.4: initial_xml を final_xml の前に追加
        "initial_exists",
        "level_code",
        "level_text",
        "initial_date",
        "final_date",
        "継続日数",
        "継続判定モード",
        "継続しきい値",
        "継続期間_XML判定",
        "initial_same_folder",
        "矛盾(目標なし達成あり)",
        "conflict_腹囲体重_XML判定",
        "conflict_食_XML判定",
        "conflict_運動_XML判定",
        "conflict_喫煙_XML判定",
        "conflict_休養_XML判定",
        "conflict_その他_XML判定",
        "goal_腹囲体重",
        "goal_食",
        "goal_運動",
        "goal_喫煙",
        "goal_休養",
        "goal_その他",
        "achieve_腹囲体重",
        "achieve_腹囲体重_内容",
        "achieve_食",
        "achieve_運動",
        "achieve_喫煙",
        "achieve_休養",
        "achieve_その他",
        "健診時_腹囲(cm)",
        "最終_腹囲(cm)",
        "健診時_体重(kg)",
        "最終_体重(kg)",
        "final_outcome_summary",
        "initial_goal_summary",
        "outcome_total_points",
        # 代表ソース（90040優先）の概要
        "process_source",
        "process_total_points",
        "process_total_minutes",
        "proc_個別支援(対面)_回数",
        "proc_個別支援(対面)_分",
        "proc_個別支援(遠隔)_回数",
        "proc_個別支援(遠隔)_分",
        "proc_グループ支援(対面)_回数",
        "proc_グループ支援(対面)_分",
        "proc_グループ支援(遠隔)_回数",
        "proc_グループ支援(遠隔)_分",
        "proc_電話_回数",
        "proc_電話_分",
        "proc_電子メール等_回数",
        "grand_total_points",
        "初回面談方式_初回XML_コード",
        "初回面談方式_初回XML_内容",
        "初回面談方式_最終XML_コード",
        "初回面談方式_最終XML_内容",
    ]
    out_out = outdir / "export_outcome_report"
    out_out.mkdir(exist_ok=True)
    out_csv = out_out / f"outcome_report_{ts}.csv"
    with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()

        duration_policy = cfg.get("duration_policy", {}) if isinstance(cfg, dict) else {}

        for person, data in sorted(people.items()):
            final = data.get("final")
            initial = data.get("initial")

            t_final = read_xml(Path(final["path"])) if final else None

            def _folder_of(rec: Optional[Dict[str, Any]]) -> str:
                return (rec or {}).get("folder", "")

            same_folder_init_exists = bool(initial and final) and (
                _folder_of(initial) == _folder_of(final)
            )

            init_goals: Dict[str, bool] = {}
            final_outs: Dict[str, bool] = {}
            outcome_pts: int = 0
            belly_text: str = ""

            if not final:
                if initial:
                    try:
                        t_init = read_xml(Path(initial["path"]))
                        init_goals = extract_initial_goals(t_init)
                    except Exception:
                        init_goals = {}
            else:
                lvl_code_for_goals = (final["basic"] or {}).get("level_code", "")
                if lvl_code_for_goals == "2":
                    init_goals = extract_motivation_goals_from_final(t_final)
                else:
                    if same_folder_init_exists and initial:
                        try:
                            t_init = read_xml(Path(initial["path"]))
                            init_goals = extract_initial_goals(t_init)
                        except Exception:
                            init_goals = {}
                    else:
                        init_goals = extract_motivation_goals_from_final(t_final)
                final_outs, outcome_pts, belly_text = extract_final_outcomes(t_final)

            # ---- 90040 / 90070 の双方を取得 ----
            process_source = "none"
            process_total_points = 0
            process_total_minutes = 0

            ev_90040 = {}
            ag_90070 = {"counts": {}, "durations_min": {}, "_total_points": 0, "_grand_total": 0}

            if final and t_final is not None:
                # 90040（詳細イベント）
                ev_90040 = extract_process_events(t_final)
                sum90040 = int(ev_90040.get("_total_points", 0) or 0)
                min90040 = int(ev_90040.get("_total_minutes", 0) or 0)

                # 90070（集計）— points が 0 でも counts/durations を使うので常に抽出
                ag_90070 = extract_process_aggregate_final(t_final, cfg)
                sum90070 = int(ag_90070.get("_total_points", 0) or 0)
                durs90070 = (ag_90070.get("durations_min", {}) or {})

                # 代表ソースの決定（従来どおり 90040 優先）
                if sum90040:
                    process_source = "90040"
                    process_total_points = sum90040
                    process_total_minutes = min90040
                elif sum90070:
                    process_source = "90070_evn"
                    process_total_points = sum90070
                    process_total_minutes = sum(int(v or 0) for v in durs90070.values())
                else:
                    process_source = "none"
                    process_total_points = 0
                    process_total_minutes = 0

            # 総ポイント = プロセス(代表ソース) + アウトカム(90060)
            grand = int(process_total_points + (outcome_pts or 0))

            bfin = final["basic"] if final else {}
            binv = initial["basic"] if initial else {}
            initial_date = (
                bfin.get("initial_date", "")
                if final
                else (binv.get("initial_date", "") if initial else "")
            )
            final_date = bfin.get("final_date", "") if final else ""

            # 90060から最終の腹囲/体重を抽出
            final_waist_cm = ""
            final_weight_kg = ""
            if final and t_final is not None:
                wcm, wkg = extract_final_measurements(t_final)
                final_waist_cm = f"{wcm:.1f}" if isinstance(wcm, (int, float)) else ""
                final_weight_kg = f"{wkg:.1f}" if isinstance(wkg, (int, float)) else ""

            # 90030 由来の初回面談方式
            init_mode_code_from_init = ""
            init_mode_text_from_init = ""
            init_mode_code_from_fin = ""
            init_mode_text_from_fin = ""
            try:
                if initial:
                    tin = read_xml(Path(initial["path"]))
                    (
                        init_mode_code_from_init,
                        init_mode_text_from_init,
                    ) = extract_initial_interview_mode(tin, cfg)
            except Exception:
                pass
            try:
                if final and t_final is not None:
                    (
                        init_mode_code_from_fin,
                        init_mode_text_from_fin,
                    ) = extract_initial_interview_mode(t_final, cfg)
            except Exception:
                pass

            # 外部ID生成は最終XMLがあれば最終側から、無ければ初回側から
            bsrc = final["basic"] if final else (initial["basic"] if initial else {})
            vals_for_id_src = {
                "insurer_number": to_half_digits((bsrc or {}).get("insurer", "")),
                "symbol": norm_symbol_human((bsrc or {}).get("symbol", "")),
                "insurance_number": norm_number((bsrc or {}).get("number", "")),
                "birth_yyyymmdd": _normalize_birth_any((bsrc or {}).get("birth", "")),
            }
            dbg(
                "outcome_vals_for_id",
                {
                    "final_xml": final.get("xml") if final else "",
                    **vals_for_id_src,
                },
            )
            person_id_custom, person_id_custom_error = gen_custom_id_external(
                vals_for_id_src,
                str(Path(args.mat)),
                debug=DEBUG,
                jsonout=False,
            )

            # MySQL の shg_result から利用券情報
            db_info = shg_result_map.get((person_id_custom or "").strip(), {})

            days_diff: Optional[int] = None
            mode_label = ""
            thr_label = ""
            verdict = ""
            if final:
                bfin2 = final["basic"]
                days_diff, mode_label, thr_label, verdict = compute_duration_verdict(
                    duration_policy,
                    bfin2.get("initial_date", ""),
                    bfin2.get("final_date", ""),
                )
                if bfin2.get("level_code", "") == "2":
                    verdict = "N/A(動機付け)"
                    mode_label = ""
                    thr_label = ""

            # 目標/達成のカテゴリ整理
            def cat(label: Optional[str]) -> str:
                if label is None:
                    return ""
                if "腹囲" in label or "体重" in label:
                    return "腹囲体重"
                if "食" in label and "習慣" in label:
                    return "食"
                if "運動" in label and "習慣" in label:
                    return "運動"
                if "喫煙" in label:
                    return "喫煙"
                if "休養" in label:
                    return "休養"
                if "その他" in label:
                    return "その他"
                return label

            cats = ["腹囲体重", "食", "運動", "喫煙", "休養", "その他"]
            gmap = {c: False for c in cats}
            amap = {c: False for c in cats}
            for k, v in (init_goals or {}).items():
                gmap[cat(k)] = bool(v)
            for k, v in (final_outs or {}).items():
                amap[cat(k)] = bool(v)
            cmap = {c: (not gmap[c]) and amap[c] for c in cats}

            conflicts_list = [c for c, flg in cmap.items() if flg] if final else []
            conflict_overall = (
                "Yes: " + ",".join(conflicts_list)
                if final and conflicts_list
                else ("" if not final else "No")
            )

            # ★ 90070 の counts/durations は常に CSV に出す
            pc = (ag_90070.get("counts", {}) or {}) if (final and t_final is not None) else {}
            pd = (ag_90070.get("durations_min", {}) or {}) if (final and t_final is not None) else {}

            row: Dict[str, Any] = {
                "person_key": person,
                "person_id": person,
                "person_id_custom": person_id_custom,
                "person_id_custom_error": person_id_custom_error,
                "db_ticket_no": db_info.get("usage_ticket_number", ""),
                "db_ticket_exp": db_info.get("expiration_date", ""),
                "initial_xml": initial.get("xml") if initial else "",
                "final_xml": final.get("xml") if final else "",
                "initial_exists": "Yes" if initial else "No",
                "level_code": bfin.get("level_code", "") if final else "",
                "level_text": bfin.get("level_text", "") if final else "",
                "initial_date": initial_date,
                "final_date": final_date,
                "継続日数": days_diff if (final and isinstance(days_diff, int)) else "",
                "継続判定モード": mode_label if final else "",
                "継続しきい値": thr_label if final else "",
                "継続期間_XML判定": verdict if final else "",
                "initial_same_folder": ("Yes" if same_folder_init_exists else "No") if final else "",
                "矛盾(目標なし達成あり)": conflict_overall,
                "conflict_腹囲体重_XML判定": "NG"
                if (final and cmap["腹囲体重"])
                else ("OK" if final else ""),
                "conflict_食_XML判定": "NG"
                if (final and cmap["食"])
                else ("OK" if final else ""),
                "conflict_運動_XML判定": "NG"
                if (final and cmap["運動"])
                else ("OK" if final else ""),
                "conflict_喫煙_XML判定": "NG"
                if (final and cmap["喫煙"])
                else ("OK" if final else ""),
                "conflict_休養_XML判定": "NG"
                if (final and cmap["休養"])
                else ("OK" if final else ""),
                "conflict_その他_XML判定": "NG"
                if (final and cmap["その他"])
                else ("OK" if final else ""),
                "goal_腹囲体重": "目標" if gmap["腹囲体重"] else "非目標",
                "goal_食": "目標" if gmap["食"] else "非目標",
                "goal_運動": "目標" if gmap["運動"] else "非目標",
                "goal_喫煙": "目標" if gmap["喫煙"] else "非目標",
                "goal_休養": "目標" if gmap["休養"] else "非目標",
                "goal_その他": "目標" if gmap["その他"] else "非目標",
                "achieve_腹囲体重": "達成"
                if (final and amap["腹囲体重"])
                else ("" if not final else "未"),
                "achieve_腹囲体重_内容": belly_text if final else "",
                "achieve_食": "達成"
                if (final and amap["食"])
                else ("" if not final else "未"),
                "achieve_運動": "達成"
                if (final and amap["運動"])
                else ("" if not final else "未"),
                "achieve_喫煙": "達成"
                if (final and amap["喫煙"])
                else ("" if not final else "未"),
                "achieve_休養": "達成"
                if (final and amap["休養"])
                else ("" if not final else "未"),
                "achieve_その他": "達成"
                if (final and amap["その他"])
                else ("" if not final else "未"),
                "健診時_腹囲(cm)": "",
                "最終_腹囲(cm)": f"{final_waist_cm}",
                "健診時_体重(kg)": "",
                "最終_体重(kg)": f"{final_weight_kg}",
                "final_outcome_summary": ";".join(
                    [f"{k}:{'達成' if v else '未'}" for k, v in (final_outs or {}).items()]
                )
                if final
                else "",
                "initial_goal_summary": ";".join(
                    [f"{k}:{'目標' if v else '非目標'}" for k, v in (init_goals or {}).items()]
                ),
                "outcome_total_points": int(outcome_pts or 0),
                # 代表ソースの概要（90040優先）
                "process_source": process_source if final else "",
                "process_total_points": int(process_total_points if final else 0),
                "process_total_minutes": int(process_total_minutes if final else 0),
                # ★ ここは 90070 を常に出力
                "proc_個別支援(対面)_回数": int(pc.get("個別支援(対面)") or 0),
                "proc_個別支援(対面)_分": int(pd.get("個別支援(対面)") or 0),
                "proc_個別支援(遠隔)_回数": int(pc.get("個別支援(遠隔)") or 0),
                "proc_個別支援(遠隔)_分": int(pd.get("個別支援(遠隔)") or 0),
                "proc_グループ支援(対面)_回数": int(pc.get("グループ支援(対面)") or 0),
                "proc_グループ支援(対面)_分": int(pd.get("グループ支援(対面)") or 0),
                "proc_グループ支援(遠隔)_回数": int(pc.get("グループ支援(遠隔)") or 0),
                "proc_グループ支援(遠隔)_分": int(pd.get("グループ支援(遠隔)") or 0),
                "proc_電話_回数": int(pc.get("電話") or 0),
                "proc_電話_分": int(pd.get("電話") or 0),
                "proc_電子メール等_回数": int(pc.get("電子メール等") or 0),
                # 総ポイント = プロセス(代表ソース) + アウトカム(90060)
                "grand_total_points": int(grand) if final else 0,
                "初回面談方式_初回XML_コード": init_mode_code_from_init,
                "初回面談方式_初回XML_内容": init_mode_text_from_init,
                "初回面談方式_最終XML_コード": init_mode_code_from_fin,
                "初回面談方式_最終XML_内容": init_mode_text_from_fin,
            }
            w.writerow(row)

    print(f"[OK] export_shg_report: {shg_csv}")
    print(f"[OK] export_outcome_report: {out_csv}")


if __name__ == "__main__":
    main()
