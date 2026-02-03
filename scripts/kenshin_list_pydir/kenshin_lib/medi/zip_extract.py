# -*- coding: utf-8 -*-
"""
kenshin_lib/medi/zip_extract.py

ZIP展開を担当。パスワード要否判定・候補試行・エラーコード正規化。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil
import zipfile
from typing import Iterable, Optional, List


@dataclass
class ZipExtractResult:
    ok: bool
    error_code: Optional[str] = None
    message: Optional[str] = None
    used_password_text: Optional[str] = None  # 成功時のみ（必要なら、監査/ログ用）


def _safe_rmtree(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _zip_has_encrypted_member(zf: zipfile.ZipFile) -> bool:
    # ZipInfo.flag_bits の bit0 が暗号化フラグ
    for info in zf.infolist():
        if info.flag_bits & 0x1:
            return True
    return False


def _to_pwd_bytes(pw_text: Optional[str]) -> Optional[bytes]:
    if pw_text is None:
        return None
    # zipfile は bytes pwd を要求
    # 基本utf-8でOK。もし運用で shift-jis が混じるならここだけ差し替えで対応できる。
    return pw_text.encode("utf-8", errors="strict")


def extract_zip_to_temp(
    zip_path: Path,
    temp_dir: Path,
    *,
    pwd_candidates: Optional[Iterable[str]] = None,
) -> ZipExtractResult:
    """
    展開前に temp_dir を作り直し、展開に成功したら ok=True を返す。
    失敗時は error_code と message を返す。

    error_code:
      - ZIP_PASSWORD
      - ZIP_LONG_PATH
      - ZIP_UNEXPECTED
    """
    _safe_rmtree(temp_dir)
    _ensure_dir(temp_dir)

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            encrypted = _zip_has_encrypted_member(zf)

            # パスワード不要なら即展開
            if not encrypted:
                zf.extractall(temp_dir)
                return ZipExtractResult(ok=True)

            # 暗号あり: 候補を順に試す
            candidates: List[Optional[str]] = []
            if pwd_candidates:
                # 重複は呼び出し側で消しててもいいけど、ここでも軽くケア
                seen = set()
                for pw in pwd_candidates:
                    pw2 = (pw or "").strip()
                    if not pw2:
                        continue
                    if pw2 in seen:
                        continue
                    seen.add(pw2)
                    candidates.append(pw2)

            # 最後に None も一応（暗号判定ミス/一部だけ暗号などの保険）
            candidates.append(None)

            last_err: Optional[Exception] = None
            last_runtime_msg: str = ""

            for pw_text in candidates:
                try:
                    pw_bytes = _to_pwd_bytes(pw_text)
                    zf.extractall(temp_dir, pwd=pw_bytes)
                    return ZipExtractResult(ok=True, used_password_text=pw_text)

                except RuntimeError as e:
                    # "is encrypted, password required for extraction"
                    # "Bad password for file" など
                    last_err = e
                    msg = str(e)
                    last_runtime_msg = msg

                    m = msg.lower()
                    if "bad password" in m:
                        # 次候補へ
                        continue
                    if "password required" in m or "encrypted" in m:
                        # 次候補へ（候補が尽きたら最後にZIP_PASSWORDで返す）
                        continue
                    # それ以外の RuntimeError は一旦「次」へ（最後にまとめて返す）
                    continue

                except FileNotFoundError as e:
                    # Windowsのパス長/ディレクトリ生成失敗など
                    return ZipExtractResult(ok=False, error_code="ZIP_LONG_PATH", message=str(e)[:2000])

                except Exception as e:
                    # 予期しない例外も候補を変えて通る可能性があるので最後まで試す
                    last_err = e
                    continue

            # 候補が尽きた
            msg = str(last_err)[:2000] if last_err else (last_runtime_msg[:2000] if last_runtime_msg else "encrypted zip: password required")
            return ZipExtractResult(ok=False, error_code="ZIP_PASSWORD", message=msg)

    except zipfile.BadZipFile as e:
        return ZipExtractResult(ok=False, error_code="ZIP_UNEXPECTED", message=f"File is not a zip file: {e}")
    except FileNotFoundError as e:
        return ZipExtractResult(ok=False, error_code="ZIP_LONG_PATH", message=str(e)[:2000])
    except Exception as e:
        return ZipExtractResult(ok=False, error_code="ZIP_UNEXPECTED", message=str(e)[:2000])
