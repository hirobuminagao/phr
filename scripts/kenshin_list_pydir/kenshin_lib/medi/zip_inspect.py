# -*- coding: utf-8 -*-
"""
kenshin_lib/medi/zip_inspect.py

ZIPを解凍せず、ZIP内にXMLが存在するかを軽量に判定する。

- zipfile の中央ディレクトリ（infolist）を参照するだけ
- encryptedでも一覧取得はできることが多い（読取は不要）
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import zipfile


@dataclass(frozen=True)
class ZipXmlProbeResult:
    ok: bool                 # 判定処理自体が成功したか
    has_xml: bool            # xmlが1つでもあるか（ok=Falseの時はFalse）
    xml_count: int           # xml個数（ok=Falseの時は0）
    note: str | None = None  # 失敗理由や補足


def _is_xml_member(name: str) -> bool:
    n = (name or "").strip()
    if not n:
        return False
    # ディレクトリ除外
    if n.endswith("/") or n.endswith("\\"):
        return False
    return n.lower().endswith(".xml")


def probe_zip_has_xml(zip_path: str | Path) -> ZipXmlProbeResult:
    p = Path(zip_path)
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
        return ZipXmlProbeResult(ok=False, has_xml=False, xml_count=0, note=f"unexpected: {e}")
