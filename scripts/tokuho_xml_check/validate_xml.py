# -*- coding: utf-8 -*-
"""
validate_xml.py  (script-local defaults, multi-root, relative-path logs)
特定保健指導XML（第4期） バリデーションスクリプト
"""

from __future__ import annotations
import argparse
from pathlib import Path
from lxml import etree
from datetime import datetime
import csv
import sys
import glob as _glob
from typing import List, Tuple, Optional  # ★ Optional を追加


def find_batches(roots: List[str]) -> List[Path]:
    batch_dirs = set()
    for r in roots:
        matches = list(_glob.glob(str(r)))
        if not matches:
            matches = [r]
        for m in matches:
            p = Path(m).resolve()
            if p.is_dir():
                if (p / "DATA").is_dir() and (p / "XSD").is_dir():
                    batch_dirs.add(p)
                    continue
                for child in p.iterdir():
                    if child.is_dir() and (child / "DATA").is_dir() and (child / "XSD").is_dir():
                        batch_dirs.add(child.resolve())
    return sorted(batch_dirs)


def autodetect_xsd(xsd_dir: Path, preferred: str = "hg08_V08.xsd") -> Optional[Path]:  # ★ Optional
    cand = xsd_dir / preferred
    if cand.exists():
        return cand
    hg_list = sorted(xsd_dir.glob("hg08_V*.xsd"))
    if hg_list:
        return hg_list[-1]
    for f in sorted(xsd_dir.glob("*.xsd")):
        if "hg08" in f.name.lower():
            return f
    return None


def validate_one(schema: etree.XMLSchema, xml_path: Path) -> Optional[str]:  # ★ Optional[str]
    try:
        xml_bytes = xml_path.read_bytes()
        parser = etree.XMLParser(remove_blank_text=False, encoding="utf-8")
        doc = etree.fromstring(xml_bytes, parser)
        schema.assertValid(doc)
        return None
    except Exception as e:
        return str(e)


def rel_to_any(path: Path, bases: List[Path]) -> Tuple[str, Optional[Path]]:  # ★ Optional[Path]
    """
    path を、bases のいずれかからの相対で文字列化。
    戻り値: (表示用相対パス文字列, 採用したベースフォルダ Path または None)
    """
    p = path.resolve()
    for b in bases:
        b = b.resolve()
        try:
            rel = p.relative_to(b)
            return str(rel).replace("\\", "/"), b
        except ValueError:
            continue
    return p.name, None


def write_batch_logs(batch: Path, rows, outdir: Path, ts: str, base_for_display: Path):
    txt = outdir / f"{batch.name}_VALIDATION_LOG_{ts}.txt"
    with open(txt, "w", encoding="utf-8") as f:
        f.write(f"バッチ（相対）: {batch.resolve().relative_to(base_for_display).as_posix()}\n")
        f.write(f"検証日時: {datetime.now()}\n\n")
        ok = sum(1 for _, s, _ in rows if s == "OK")
        f.write(f"結果: OK {ok} / {len(rows)}\n\n")
        for file_rel, status, msg in rows:
            if status == "OK":
                f.write(f"[OK] {file_rel}\n")
            else:
                f.write(f"[NG] {file_rel} … {msg}\n")

    csvp = outdir / f"{batch.name}_VALIDATION_RESULT_{ts}.csv"
    with open(csvp, "w", newline="", encoding="utf-8") as cf:
        w = csv.writer(cf)
        w.writerow(["file_rel_from_input", "status", "message"])
        w.writerows(rows)


def main():
    script_dir = Path(__file__).resolve().parent

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input",
        nargs="*",
        default=None,
        help="検証ルート（複数可）。省略時は <スクリプト>/input を使用。"
    )
    ap.add_argument(
        "--glob",
        nargs="*",
        default=[],
        help="globパターン（例: /data/*/2025*）。--input と併用可。"
    )
    ap.add_argument(
        "--xsd_name",
        type=str,
        default="hg08_V08.xsd",
        help="優先的に使用するXSDファイル名（自動検出の第一候補）"
    )
    args = ap.parse_args()

    if args.input is None or len(args.input) == 0:
        roots = [str((script_dir / "input").resolve())]
    else:
        roots = [str((script_dir / r).resolve()) if not Path(r).is_absolute() else r for r in args.input]

    glob_roots = []
    for g in args.glob:
        gp = (script_dir / g) if not Path(g).is_absolute() else Path(g)
        glob_roots.append(str(gp.resolve()))

    roots = roots + glob_roots
    batches = find_batches(roots)

    outdir = (script_dir / "out" / "export_xsd_validation_result")
    outdir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%d%M")  # ご指定どおり（yyyymmdd_hhddmm）

    base_candidates = [Path(r).resolve() for r in roots]

    log_path = outdir / f"validation_log_{ts}.txt"
    csv_path = outdir / f"validation_result_{ts}.csv"

    all_rows = []
    with open(log_path, "w", encoding="utf-8") as log:
        log.write(f"XML妥当性検証ログ  {datetime.now()}\n")
        log.write("探索ルート: (script_dir)/input 等（※パス出力は全て input からの相対）\n\n")

        if not batches:
            log.write("バッチフォルダが見つかりませんでした。（<root>/<batch>/(DATA,XSD)）\n")
            print("バッチが見つかりませんでした。", file=sys.stderr)
            return 0

        for b in batches:
            b_rel, base_used = rel_to_any(b, base_candidates)
            if base_used is None:
                b_rel = b.name
                base_used = b  # フォールバック

            xsd_dir = b / "XSD"
            data_dir = b / "DATA"
            log.write(f"=== バッチ: {b_rel} ===\n")

            main_xsd = autodetect_xsd(xsd_dir, args.xsd_name)
            if not main_xsd or not main_xsd.exists():
                msg = f"XSDが見つかりません（期待: {args.xsd_name} または hg08_V*.xsd）: { (xsd_dir.resolve().relative_to(base_used)).as_posix() }"
                log.write(msg + "\n\n")
                all_rows.append([b_rel, "", "NG", msg])
                write_batch_logs(b, [], outdir, ts, base_used)
                continue

            try:
                xmlschema_doc = etree.parse(str(main_xsd))
                schema = etree.XMLSchema(xmlschema_doc)
                log.write(f"XSD読込OK: { (main_xsd.resolve().relative_to(base_used)).as_posix() }\n")
            except Exception as e:
                msg = f"XSD読込エラー: {e}"
                log.write(msg + "\n\n")
                all_rows.append([b_rel, "", "NG", msg])
                write_batch_logs(b, [], outdir, ts, base_used)
                continue

            xml_files = sorted(p for p in data_dir.rglob("*.xml") if p.is_file())
            if not xml_files:
                msg = "DATA 配下に XML が見つかりません。"
                log.write(msg + "\n\n")
                all_rows.append([b_rel, "", "NG", msg])
                write_batch_logs(b, [], outdir, ts, base_used)
                continue

            batch_rows = []
            ok_count = 0
            for xf in xml_files:
                err = validate_one(schema, xf)
                xf_rel = (xf.resolve().relative_to(base_used)).as_posix()
                if err is None:
                    log.write(f"[OK] {xf_rel}\n")
                    all_rows.append([b_rel, xf_rel, "OK", ""])
                    batch_rows.append([xf_rel, "OK", ""])
                    ok_count += 1
                else:
                    log.write(f"[NG] {xf_rel} … {err}\n")
                    all_rows.append([b_rel, xf_rel, "NG", err])
                    batch_rows.append([xf_rel, "NG", err])

            log.write(f"結果: OK {ok_count} / {len(xml_files)}\n\n")
            write_batch_logs(b, batch_rows, outdir, ts, base_used)

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["batch_rel_from_input", "file_rel_from_input", "status", "message"])
        w.writerows(all_rows)

    print(f"検証完了。出力は {outdir} を確認してください。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
