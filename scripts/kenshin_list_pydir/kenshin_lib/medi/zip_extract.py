# -*- coding: utf-8 -*-
"""
kenshin_lib/medi/zip_extract.py

【役割】
ZIPファイルを一時ディレクトリへ展開するユーティリティ。
- ZIP内に暗号化メンバが存在するかを判定し、必要な場合のみパスワード候補を順に試行する。
- 失敗時は運用で扱いやすい error_code に正規化して返す。

【I/O】
- 入力:
  - zip_path: 対象ZIP
  - temp_dir: 展開先（一時ディレクトリ）
  - pwd_candidates: パスワード候補（文字列）。重複・空白は内部で軽く除去する。
- 出力: ZipExtractResult
  - ok: 展開成功なら True
  - error_code: 失敗時の種別
  - message: 失敗理由（最大 2000 文字で短文化）
  - used_password_text: 成功時に使用したパスワード（監査/ログ用途。不要なら呼び出し側で破棄）

【動作仕様】
- 実行前に temp_dir を必ず作り直す（既存があれば削除→再作成）。
- 暗号化が無い場合は即 extractall。
- 暗号化がある場合は、pwd_candidates を順に extractall(pwd=...) で試す。
  - 候補の最後に None を追加して試行する（暗号判定の揺れ/一部だけ暗号などの保険）。
- 例外は可能な限り候補試行を継続し、最終的に正規化した error_code を返す。

【error_code】
- ZIP_PASSWORD: 暗号化ZIPでパスワードが必要/不一致（候補尽き）
- ZIP_LONG_PATH: 展開中のパス長/生成失敗等（主に Windows 想定）
- ZIP_UNEXPECTED: それ以外の予期しないZIPエラー（壊れたZIP等を含む）

【注意】
- zipfile の pwd は bytes を要求するため、基本は UTF-8 で bytes 化して渡す。
  運用上 Shift-JIS 等が混在する場合は `_to_pwd_bytes()` のみ差し替えで吸収する。
- temp_dir の削除を伴うため、呼び出し側は temp_dir のパスを安全に設計すること。
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
