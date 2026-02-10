# -*- coding: utf-8 -*-
"""
kenshin_lib/medi/zip_inspect.py

【目的】
ZIPを展開せずに、ZIP内にXMLが含まれるかを軽量に判定する。

【方針】
- zipfile の中央ディレクトリ（infolist）を走査し、拡張子 .xml のメンバー数を数える
- 実体ファイルの読み取りや解凍は行わない（＝パスワード付きでも一覧取得できるケースが多い）

【用途】
- 共有フォルダ観測（medi_shared_files）で「健診候補ZIPっぽいか」を粗く振り分ける前処理

【注意】
- ZIP自体が壊れている/途中までしかコピーされていない場合は ok=False で返す
- UNC/ネットワーク由来のIO例外も ok=False 扱い（note に理由を入れる）
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import zipfile


@dataclass(frozen=True)
class ZipXmlProbeResult:
    ok: bool                 # 判定処理自体が成功したか
    has_xml: bool            # XMLが1つでもあるか（ok=False の場合は常に False）
    xml_count: int           # XML個数（ok=False の場合は常に 0）
    note: str | None = None  # 失敗理由/補足（短文化推奨）


def _is_xml_member(name: str) -> bool:
    n = (name or "").strip()
    if not n:
        return False
    # ディレクトリ除外
    if n.endswith("/") or n.endswith("\\"):
        return False
    return n.lower().endswith(".xml")


def probe_zip_has_xml(zip_path: str | Path) -> ZipXmlProbeResult:
    p = Path(zip_path).expanduser()
    if not p.exists():
        return ZipXmlProbeResult(ok=False, has_xml=False, xml_count=0, note="zip not found")
    if not p.is_file():
        return ZipXmlProbeResult(ok=False, has_xml=False, xml_count=0, note="zip is not a file")

    try:
        with zipfile.ZipFile(p, "r") as zf:
            cnt = 0
            for info in zf.infolist():
                # info.filename は文字化けすることがあるが拡張子判定だけなら大抵OK
                if _is_xml_member(getattr(info, "filename", "")):
                    cnt += 1
            return ZipXmlProbeResult(ok=True, has_xml=(cnt > 0), xml_count=cnt, note=None)

    except zipfile.BadZipFile:
        return ZipXmlProbeResult(ok=False, has_xml=False, xml_count=0, note="bad zip file")
    except PermissionError as e:
        return ZipXmlProbeResult(ok=False, has_xml=False, xml_count=0, note=f"permission error: {e}")
    except OSError as e:
        # UNCやネットワーク由来のIOエラーもここに入る
        return ZipXmlProbeResult(ok=False, has_xml=False, xml_count=0, note=f"os error: {e}")
    except Exception as e:
        return ZipXmlProbeResult(ok=False, has_xml=False, xml_count=0, note=f"unexpected {type(e).__name__}: {e}")
