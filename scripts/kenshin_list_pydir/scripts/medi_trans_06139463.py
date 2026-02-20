# -*- coding: utf-8 -*-
"""
scripts/medi_trans_06139463.py

【固定化（freeze）】
本ファイルは「既に運用で必要になった変換処理」を凍結し、後続の一般化/再設計と混ぜないための仕様メモである。
このコミットでは、対象保険者（06139463）向けの変換仕様を“そのまま”実装し、過剰な一般化はしない。

【目的】
医療機関から受領した ZIP（厚労省 指定ファイル構成）内の DATA/*.xml を対象に、
HIA 取り込み前に “決まった正規化” を一括適用し、再ZIP化して出力する。
（DB更新は行わない。入力ZIPを直接書き換えず、出力ZIPを作る。）

【対象データ】
- 入力: 厚労省 指定ファイル構成の ZIP
  - ZIPのどこかに DATA/ が存在すること（通常はルートフォルダ配下に DATA/）
  - 処理対象は DATA/*.xml のみ
- 出力: 変換後ZIP（元ZIPと同じ構成で出力）

【運用前提（今回の決定）】
- 解凍〜再ZIP化までスクリプトで実施する（手作業解凍はしない）
- 対象ZIPは「保険者番号 06139463 の結果だけ」が入っている前提で運用する

【運用フォルダ（固定）】
本スクリプトは `kenshin_list_pydir` 配下で完結する。

  kenshin_list_pydir/
    medi_trans_06139463/
      in/                     # ここに入力ZIPを置く（複数OK）
      out/
        out_YYYYMMDD_HHMMSS/  # 変換後ZIP出力
      done/
        done_YYYYMMDD_HHMMSS/ # 処理済み元ZIP保管

【実行方針（今回の決定）】
- in/ 内のZIPを名前順に処理する
- 1つでも失敗したら「全停止」する
  - 失敗したZIPは in/ に残す
  - それ以前に成功したZIPは out/done に移動済み（ロールバックしない）
  - 失敗原因は out_*/ERROR_*.log に残す（スタックトレース付き）

【変換仕様（固定）】
1) 保険者番号（root=1.2.392.200119.6.101 の id/@extension）
   - 値が指定の 06139463 でない場合は 06139463 に上書きする

2) 被保険者証 記号（root=1.2.392.200119.6.204 の id/@extension）
   - 今回の対象は「半角数字のみ」で運用する（HIAの加入者情報がそう登録されているため）
   - 先頭の 0 はすべて削除（桁数不問）
     - 例: "000123" -> "123", "0" -> ""（空になったら空のまま。運用で弾く）

3) 被保険者証 番号（root=1.2.392.200119.6.205 の id/@extension）
   - 半角数字へ正規化（非数字は除去する）
   - 先頭の 0 はすべて削除（桁数不問）

4) XMLの namespace prefix 由来の表記（HIA側が判別できない問題の回避）
   - 受領XMLに含まれる
       "<ns0:" / "xmlns:ns0=" / "</ns0:"
     を、再シリアライズにより “ns0 という接頭辞が出ない形” に正規化する
   - これは単純置換ではなく、XMLとして parse -> namespace を登録 -> 再出力する

5) 電話番号（telecom/@value が "tel:" で始まる場合）
   - HIAアップロードで "tel:03-1234-5678" のようなハイフン付きがエラーになるため、電話番号部分を数字のみに正規化する
   - 手順:
      - value が "tel:"（大文字小文字は無視）で始まる場合のみ対象
      - "tel:" 以降の文字列から、";" 以降（パラメータ部）があれば切り捨てる
      - 電話番号部分について、全角数字→半角数字→数字以外を除去（結果は数字のみ）
      - 正規化後は "tel:" + 数字のみ をセットする（"+" は保持しない）

6) 住所（addr配下の state/city/streetAddressLine 等のテキスト）
   - HIA側のチェックに合わせ、住所テキストは「空白なし・全角・最大80バイト（cp932換算）」へ正規化する
   - 対象（postalCode は除外）:
     - recordTarget/patientRole/addr 配下
     - representedOrganization/addr 配下
   - 手順:
      - 半角/全角スペース・改行・タブを除去
      - 半角英数字を全角へ寄せ、半角ハイフンは全角ハイフンへ寄せる
      - cp932換算で80バイト以内に切り詰め

7) 受診券（participant/@typeCode="HLD" ブロック）
   - 受診券の必須値が欠損（空文字/未設定）のブロックは、HIA側でエラーになるためブロックごと削除する
   - 対象: participant[@typeCode='HLD']
   - 判定（いずれか欠損で削除）:
      - associatedEntity/id/@extension（受診券整理番号）
      - scopingOrganization/id[@root='1.2.392.200119.6.101']/@extension（保険者番号）

"""

from __future__ import annotations

import io
import os
import re
import shutil
import tempfile
import traceback
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, cast


import xml.etree.ElementTree as ET

# -----------------------------
# XML parser (keep comments)
# -----------------------------
# Requirement (2): keep XML comments even after ET re-serialize.
# NOTE: This does NOT guarantee identical whitespace/indentation vs original bytes.
# IMPORTANT: ElementTree XMLParser is stateful; do NOT reuse the same parser instance.
#            Create a fresh parser per XML to avoid ParseError like:
#            "parsing finished: line X, column Y".

def make_parser_keep_comments() -> Optional[ET.XMLParser]:
    try:
        return ET.XMLParser(target=ET.TreeBuilder(insert_comments=True))
    except TypeError:
        # Fallback for older Python/ET that doesn't support insert_comments.
        return None


# -----------------------------
# constants / namespaces
# -----------------------------
NS_HL7 = "urn:hl7-org:v3"
NS_XSI = "http://www.w3.org/2001/XMLSchema-instance"
NS_MHLW_INDEX = "https://www.mhlw.go.jp/stf/seisakunitsuite/bunya/0000161103.html"

OID_ROOT_INSURER = "1.2.392.200119.6.101"
OID_ROOT_SYMBOL = "1.2.392.200119.6.204"
OID_ROOT_NUMBER = "1.2.392.200119.6.205"


# -----------------------------
# env utils
# -----------------------------
def env_optional(key: str, default: str = "") -> str:
    v = os.getenv(key)
    if v is None:
        return default
    return v.strip()


def now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def is_truthy_env(key: str) -> bool:
    return env_optional(key, "").strip() in ("1", "true", "TRUE", "yes", "YES")


# -----------------------------
# folder config (fixed)
# -----------------------------
def kenshin_list_root_dir() -> Path:
    # .../scripts/kenshin_list_pydir/scripts/medi_trans_06139463.py
    # => parents[1] == .../scripts/kenshin_list_pydir
    return Path(__file__).resolve().parents[1]


def trans_root_dir() -> Path:
    # default: <kenshin_list_pydir>/medi_trans_06139463
    root = env_optional("TRANS_ROOT_DIR", "")
    if root:
        return Path(root)
    return kenshin_list_root_dir() / "medi_trans_06139463"


@dataclass
class TransFolders:
    root: Path
    in_dir: Path
    out_dir: Path
    done_dir: Path


def ensure_trans_folders() -> TransFolders:
    root = trans_root_dir()
    in_dir = root / "in"
    out_dir = root / "out"
    done_dir = root / "done"
    in_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    done_dir.mkdir(parents=True, exist_ok=True)
    return TransFolders(root=root, in_dir=in_dir, out_dir=out_dir, done_dir=done_dir)


# -----------------------------
# normalization helpers
# -----------------------------
def digits_only(s: str) -> str:
    """Return digits only.

    NOTE:
    - Incoming XML sometimes contains full-width digits (e.g. "９９９９９９").
    - Regex class [0-9] matches only ASCII digits, so full-width digits would be dropped.
    - We first normalize full-width digits to ASCII, then strip non-digits.
    """
    if not s:
        return ""
    # Convert full-width digits to ASCII digits
    s2 = s.translate(str.maketrans("０１２３４５６７８９", "0123456789"))
    return re.sub(r"[^0-9]", "", s2)


def normalize_symbol_digits_strip_leading_zeros(ext: str) -> str:
    d = digits_only(ext)
    # strip all leading zeros (any length)
    d = d.lstrip("0")
    return d


def normalize_number_digits_strip_leading_zeros(ext: str) -> str:
    d = digits_only(ext)
    d = d.lstrip("0")
    return d


def normalize_tel_value(value: str) -> str:
    """Normalize HL7 telecom/@value for tel: URIs.

    - Only applies to values that start with 'tel:' (case-insensitive).
    - Drops any parameter part after ';' (RFC3966 params are not used for HIA/MHLW here).
    - Normalizes the phone-number part to digits only (full-width digits -> ASCII digits -> strip non-digits).
    - Does NOT keep a leading '+'.
    """
    if not value:
        return value

    v = value.strip()
    if not v.lower().startswith("tel:"):
        return value

    rest = v[4:]  # after 'tel:'
    number_part = rest.split(";", 1)[0].strip()

    d = digits_only(number_part)
    return f"tel:{d}"



def patch_telecom_tel_values(root: ET.Element) -> int:
    """Patch telecom/@value that starts with tel: by removing non-digits in the number part."""
    cnt = 0
    for te in root.findall(f".//{{{NS_HL7}}}telecom"):
        before = te.get("value", "")
        if not before:
            continue
        after = normalize_tel_value(before)
        if after != before:
            te.set("value", after)
            cnt += 1
    return cnt


# --- 住所の正規化（厚労省 14P） ---
def _to_fullwidth_basic(s: str) -> str:
    """最低限の半角→全角（英数字・ハイフン）。"""
    if not s:
        return s

    fw_digits = "０１２３４５６７８９"
    hw_digits = "0123456789"
    fw_upper = "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ"
    hw_upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    fw_lower = "ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ"
    hw_lower = "abcdefghijklmnopqrstuvwxyz"

    trans_map = {
        **{hw_digits[i]: fw_digits[i] for i in range(10)},
        **{hw_upper[i]: fw_upper[i] for i in range(26)},
        **{hw_lower[i]: fw_lower[i] for i in range(26)},
        "-": "－",
    }
    return s.translate(str.maketrans(trans_map))


def normalize_address_text(value: str) -> str:
    """住所テキスト: 空白なし・全角・最大80バイト（cp932換算）。
    postalCode は対象外。
    """
    if not value:
        return value

    s = value

    # 空白除去（半角/全角/改行/タブ）
    s = re.sub(r"[ \t\r\n　]", "", s)

    # 半角→全角（最低限）
    s = _to_fullwidth_basic(s)

    # 80バイト制限（cp932）
    b = s.encode("cp932", errors="ignore")
    if len(b) > 80:
        b = b[:80]
        s = b.decode("cp932", errors="ignore")

    return s



def patch_address_texts(root: ET.Element) -> int:
    """patientRole/addr と representedOrganization/addr の住所テキストを正規化（postalCode除外）。"""
    cnt = 0

    def _patch_addr(addr_elem: ET.Element) -> None:
        nonlocal cnt

        # addr直下にテキストが入るケース（例: <addr>住所...</addr>）
        before_addr_text = addr_elem.text or ""
        if before_addr_text:
            after_addr_text = normalize_address_text(before_addr_text)
            if after_addr_text != before_addr_text:
                addr_elem.text = after_addr_text
                cnt += 1

        for e in list(addr_elem):
            # postalCode 自体の text は対象外（仕様どおり）
            if e.tag != f"{{{NS_HL7}}}postalCode":
                before = e.text or ""
                if before:
                    after = normalize_address_text(before)
                    if after != before:
                        e.text = after
                        cnt += 1

            # 子要素の直後に入る tail（例: <postalCode/>住所...）は住所本文なので正規化する
            before_tail = e.tail or ""
            if before_tail:
                after_tail = normalize_address_text(before_tail)
                if after_tail != before_tail:
                    e.tail = after_tail
                    cnt += 1

    for addr in root.findall(f".//{{{NS_HL7}}}patientRole//{{{NS_HL7}}}addr"):
        _patch_addr(addr)

    for addr in root.findall(f".//{{{NS_HL7}}}representedOrganization//{{{NS_HL7}}}addr"):
        _patch_addr(addr)

    return cnt

# --- 受診券(HLD)ブロックの削除 ---
def _is_blank(v: Optional[str]) -> bool:
    return v is None or str(v).strip() == ""


def remove_invalid_hld_participants(root: ET.Element) -> int:
    """Remove invalid HLD participant blocks.

    HIA側で弾かれるケースがあるため、受診券の必須値が欠損している
    participant(typeCode='HLD') をブロックごと削除する。

    判定（いずれか欠損で削除）:
      - associatedEntity/id/@extension （受診券整理番号）
      - scopingOrganization/id[@root='1.2.392.200119.6.101']/@extension （保険者番号）

    NOTE:
      - ElementTreeは子から親参照が無いので、親を走査し list(parent) から remove する。
    """

    removed = 0

    for parent in root.iter():
        for child in list(parent):
            if child.tag != f"{{{NS_HL7}}}participant":
                continue
            if (child.get("typeCode") or "") != "HLD":
                continue

            # --- 券面種別コードチェック（受診券のみ残す） ---
            fc = child.find(f".//{{{NS_HL7}}}functionCode")
            func_code = (fc.get("code", "") if fc is not None else "") or ""

            # code=1 以外は利用券（特定保健指導）なので削除
            if func_code != "1":
                parent.remove(child)
                removed += 1
                continue

            # --- 有効期限チェック ---
            # high が存在するなら value 必須。空はHIAエラーになるため削除
            high = child.find(f".//{{{NS_HL7}}}time/{{{NS_HL7}}}high")
            if high is not None:
                high_val = (high.get("value", "") or "").strip()
                if high_val == "":
                    parent.remove(child)
                    removed += 1
                    continue

            # 受診券整理番号: associatedEntity/id
            ticket_ext = ""
            ae = child.find(f".//{{{NS_HL7}}}associatedEntity")
            if ae is not None:
                ide = ae.find(f"./{{{NS_HL7}}}id")
                if ide is not None:
                    ticket_ext = ide.get("extension", "") or ""

            # 保険者番号: scopingOrganization/id(root=...101)
            insurer_ext = ""
            so_id = child.find(
                f".//{{{NS_HL7}}}scopingOrganization/{{{NS_HL7}}}id[@root='{OID_ROOT_INSURER}']"
            )
            if so_id is not None:
                insurer_ext = so_id.get("extension", "") or ""

            if _is_blank(ticket_ext) or _is_blank(insurer_ext):
                parent.remove(child)
                removed += 1

    return removed


# -----------------------------
# XML transformation
# -----------------------------
def collect_namespaces(xml_bytes: bytes) -> List[Tuple[str, str]]:
    """Collect (prefix, uri) namespace declarations from XML bytes."""
    out: List[Tuple[str, str]] = []
    # iterparse yields ('start-ns', (prefix, uri))
    for _event, ns in ET.iterparse(io.BytesIO(xml_bytes), events=("start-ns",)):
        prefix, uri = cast(Tuple[str, str], ns)
        out.append((prefix or "", uri))
    return out


def register_namespaces_for_reserialize(found: List[Tuple[str, str]]) -> None:
    """Register namespaces so ElementTree will not emit auto-generated ns0 prefixes.

    Policy:
    - HL7 CDA is default namespace ("")
    - XSI keeps 'xsi'
    - MHLW index keeps 'ix'
    - Any other namespace URIs are assigned deterministic 'ns1', 'ns2', ... prefixes (never 'ns0')
    """
    ET.register_namespace("", NS_HL7)
    ET.register_namespace("xsi", NS_XSI)
    ET.register_namespace("ix", NS_MHLW_INDEX)

    seen_uris = {NS_HL7, NS_XSI, NS_MHLW_INDEX}
    # deterministically assign non-ns0 prefixes for any other URIs
    idx = 1
    for _pfx, uri in found:
        if not uri or uri in seen_uris:
            continue
        # assign ns{idx} (start from 1 so we never use ns0)
        ET.register_namespace(f"ns{idx}", uri)
        seen_uris.add(uri)
        idx += 1


def indent_xml(elem: ET.Element, level: int = 0, space: str = "  ") -> None:
    """Pretty-print indentation for ElementTree output (Python 3.9+ uses ET.indent)."""
    if hasattr(ET, "indent"):
        # type: ignore[attr-defined]
        ET.indent(elem, space=space, level=level)
        return

    i = "\n" + level * space
    if len(elem):
        if not (elem.text and elem.text.strip()):
            elem.text = i + space
        for e in elem:
            indent_xml(e, level + 1, space)
        if not (elem.tail and elem.tail.strip()):
            elem.tail = i
    else:
        if level and not (elem.tail and elem.tail.strip()):
            elem.tail = i


def iter_all_id_elements(root: ET.Element) -> Iterable[ET.Element]:
    # id elements are in HL7 namespace
    return root.findall(f".//{{{NS_HL7}}}id")


def patch_patient_ids(root: ET.Element, insurer_no: str) -> Dict[str, int]:
    """Patch id/@extension for specific roots. Returns counts per category."""
    counts = {"insurer": 0, "symbol": 0, "number": 0, "tel": 0, "addr": 0, "hld_removed": 0}

    for ide in iter_all_id_elements(root):
        r = ide.get("root")
        if not r:
            continue

        if r == OID_ROOT_INSURER:
            before = ide.get("extension", "")
            if before != insurer_no:
                ide.set("extension", insurer_no)
            counts["insurer"] += 1

        elif r == OID_ROOT_SYMBOL:
            before = ide.get("extension", "")
            after = normalize_symbol_digits_strip_leading_zeros(before)
            if after != before:
                ide.set("extension", after)
            counts["symbol"] += 1

        elif r == OID_ROOT_NUMBER:
            before = ide.get("extension", "")
            after = normalize_number_digits_strip_leading_zeros(before)
            if after != before:
                ide.set("extension", after)
            counts["number"] += 1

    # telecom/tel: normalization
    counts["tel"] = patch_telecom_tel_values(root)
    counts["addr"] = patch_address_texts(root)
    counts["hld_removed"] = remove_invalid_hld_participants(root)

    return counts


def transform_xml_bytes(xml_bytes: bytes, insurer_no: str) -> Tuple[bytes, Dict[str, int]]:
    """Parse -> patch -> reserialize. Returns (new_bytes, patch_counts)."""
    found_ns = collect_namespaces(xml_bytes)
    register_namespaces_for_reserialize(found_ns)

    # parse
    parser = make_parser_keep_comments()
    if parser is not None:
        root = ET.fromstring(xml_bytes, parser=parser)
    else:
        root = ET.fromstring(xml_bytes)

    # patch
    counts = patch_patient_ids(root, insurer_no)

    # reserialize (this is what removes ns0 prefixes)
    # NOTE: keep original declaration style simple; pretty printing is not required for HIA parsing.
    indent_xml(root)
    out = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    return out, counts


# -----------------------------
# ZIP transformation
# -----------------------------
def find_data_xml_paths(extract_dir: Path) -> List[Path]:
    """Find DATA/*.xml in extracted ZIP. Supports both root/DATA/*.xml and nested."""
    out: List[Path] = []
    # common: <root_dir>/DATA/*.xml
    for p in extract_dir.rglob("*.xml"):
        try:
            if p.is_file() and p.parent.name == "DATA":
                out.append(p)
        except OSError:
            continue
    return sorted(out)


def unzip_to_dir(zip_path: Path, dst_dir: Path) -> None:
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(dst_dir)


def zip_dir_as_same_structure(src_dir: Path, out_zip_path: Path) -> None:
    out_zip_path.parent.mkdir(parents=True, exist_ok=True)
    if out_zip_path.exists():
        out_zip_path.unlink()

    with zipfile.ZipFile(out_zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for fp in src_dir.rglob("*"):
            if fp.is_file():
                arcname = fp.relative_to(src_dir).as_posix()
                zf.write(fp, arcname)


# -----------------------------
# logging
# -----------------------------
def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def build_success_log(zip_name: str, xml_count: int, per_file_counts: List[Tuple[str, Dict[str, int]]]) -> str:
    lines: List[str] = []
    lines.append(f"ZIP: {zip_name}")
    lines.append(f"XML files processed: {xml_count}")
    lines.append("Counts per XML (id roots matched):")
    for rel, cnt in per_file_counts:
        lines.append(
            f"  - {rel}: insurer={cnt['insurer']} symbol={cnt['symbol']} number={cnt['number']} tel={cnt.get('tel', 0)} addr={cnt.get('addr', 0)} hld_removed={cnt.get('hld_removed', 0)}"
        )
    return "\n".join(lines) + "\n"


def build_error_log(zip_name: str, exc: BaseException) -> str:
    tb = traceback.format_exc()
    return (
        f"ZIP: {zip_name}\n"
        f"ERROR: {type(exc).__name__}: {exc}\n\n"
        f"TRACEBACK:\n{tb}\n"
    )


# -----------------------------
# main
# -----------------------------
def main() -> int:
    insurer_no = env_optional("TRANS_INSURER_NO", "06139463") or "06139463"
    dry_run = is_truthy_env("TRANS_DRY_RUN")
    keep_temp = is_truthy_env("TRANS_KEEP_TEMP")

    folders = ensure_trans_folders()
    ts = now_ts()

    out_batch_dir = folders.out_dir / f"out_{ts}"
    done_batch_dir = folders.done_dir / f"done_{ts}"
    out_batch_dir.mkdir(parents=True, exist_ok=True)
    done_batch_dir.mkdir(parents=True, exist_ok=True)

    in_zips = sorted([p for p in folders.in_dir.glob("*.zip") if p.is_file()])
    if not in_zips:
        print(f"[INFO] No input ZIPs. Put ZIPs into: {folders.in_dir}")
        return 0

    print(f"[INFO] TRANS_ROOT_DIR = {folders.root}")
    print(f"[INFO] in_zips = {len(in_zips)} (sorted by name)")
    print(f"[INFO] out_batch = {out_batch_dir}")
    print(f"[INFO] done_batch = {done_batch_dir}")
    print(f"[INFO] insurer_no = {insurer_no} dry_run={dry_run} keep_temp={keep_temp}")

    for zip_path in in_zips:
        zip_name = zip_path.name
        print(f"\n[RUN] {zip_name}")

        temp_dir_obj = tempfile.TemporaryDirectory(prefix=f"trans_06139463_{ts}_")
        temp_dir = Path(temp_dir_obj.name)

        try:
            unzip_to_dir(zip_path, temp_dir)

            data_xmls = find_data_xml_paths(temp_dir)
            if not data_xmls:
                raise RuntimeError("DATA/*.xml が見つかりません（厚労省の指定構成を確認）")

            per_file_counts: List[Tuple[str, Dict[str, int]]] = []
            for xml_file in data_xmls:
                rel = xml_file.relative_to(temp_dir).as_posix()
                b = xml_file.read_bytes()
                new_b, cnt = transform_xml_bytes(b, insurer_no)
                per_file_counts.append((rel, cnt))

                if not dry_run:
                    xml_file.write_bytes(new_b)

            # output zip
            out_zip_path = out_batch_dir / zip_name
            if not dry_run:
                zip_dir_as_same_structure(temp_dir, out_zip_path)

            # logs
            ok_log_path = out_batch_dir / f"OK_{zip_name}.log"
            write_text(ok_log_path, build_success_log(zip_name, len(data_xmls), per_file_counts))

            # move original to done
            if not dry_run:
                shutil.move(str(zip_path), str(done_batch_dir / zip_name))

            print(f"[OK] xml={len(data_xmls)} -> out={out_zip_path if not dry_run else '(dry-run)'}")

        except Exception as e:
            err_log_path = out_batch_dir / f"ERROR_{zip_name}.log"
            write_text(err_log_path, build_error_log(zip_name, e))
            print(f"[ERROR] {zip_name} failed. See: {err_log_path}")
            print("[STOP] One failure occurred, stopping the whole batch as decided.")
            return 1

        finally:
            if keep_temp:
                print(f"[INFO] keep temp dir: {temp_dir}")
                # do not cleanup
                temp_dir_obj.cleanup = lambda: None  # type: ignore[assignment]
            else:
                temp_dir_obj.cleanup()

    print("\n[DONE] All ZIPs processed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())