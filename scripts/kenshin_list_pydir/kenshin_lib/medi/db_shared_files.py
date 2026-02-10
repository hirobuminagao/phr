# -*- coding: utf-8 -*-
"""
kenshin_lib/medi/db_shared_files.py

【目的 / 役割】
共有フォルダ観測台帳 `medi_shared_files` に対する DB アクセス関数を集約する。
本モジュールは「DB I/O のみ」を責務とし、ZIP の解析・ハッシュ計算などの重い処理や
ビジネス判定ロジックは持たない。

【このモジュールが提供するもの】
- `SharedFileRow` : scan/upsert フェーズで使用する入力モデル
- `db_upsert_shared_file` : path_hash 一意キーでの UPSERT
- judge 支援（DB側の読み書きのみ）
  - `db_select_new_zip_files_for_judge` : 判定対象（NEW + zip + sha256あり + manualなし）を取得
  - `db_update_zip_xml_probe` : ZIP内XML有無の結果（zip_has_xml / zip_xml_count / checked_at）を保存
  - `db_update_auto_judgement` : `auto_judgement` を更新
  - `db_mark_stage_status` : `stage_status` を更新

【明示的に行わないこと（責務外）】
- ZIP を開く／XML を抽出・検査する
- sha256 を計算する
- 判定ロジック（KENSHIN / NON_KENSHIN などの分類）を実装する

【UPSERT 契約（固定仕様）】
- 一意キー: `path_hash = SHA1(path)`
- `first_seen_at`: 初回 INSERT 時のみ設定し、既存行では更新しない
- `last_seen_at`: 観測のたびに更新する
- `sha256`: NULL で既存値を上書きしない
- `manual_judgement`: 既存値がある場合は維持（運用上の正）

【補足】
- mysql-connector の返却型（dictキーが bytes になる等）の差異を吸収するため、
  `row_to_strkey_dict` で str キー dict に正規化する。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Any, Mapping, cast
import hashlib


# -----------------------------
# small utils
# -----------------------------
def sha1_text(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()


def clip_text(s: Optional[str], limit: int) -> Optional[str]:
    if s is None:
        return None
    t = str(s)
    return t[:limit] if len(t) > limit else t


def as_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", errors="replace")
    return str(v)


def row_to_strkey_dict(r: Any) -> dict[str, Any]:
    """
    mysql-connector の型スタブが環境により
      - dictキーがbytesかも
      - rowがNoneかも
    を疑うので、strキーdictへ正規化する。
    """
    if r is None:
        return {}
    m = cast(Mapping[Any, Any], r)
    out: dict[str, Any] = {}
    for k, v in m.items():
        if isinstance(k, (bytes, bytearray)):
            kk = k.decode("utf-8", errors="ignore")
        else:
            kk = str(k)
        out[kk] = v
    return out


# -----------------------------
# Row model (scan/upsert用)
# -----------------------------
@dataclass
class SharedFileRow:
    path: str
    src_folder_raw: Optional[str]
    dst_folder_norm: Optional[str]
    facility_hint: Optional[str]
    file_name: str
    ext: str
    file_size: int
    mtime: Optional[str]  # datetime文字列でもOK（cursor側で入れる）
    sha256: Optional[str]
    auto_judgement: str  # 'KENSHIN'|'NON_KENSHIN'|'UNREADABLE'|'UNKNOWN'
    manual_judgement: Optional[str]  # 'KENSHIN'|'NON_KENSHIN'|'UNREADABLE'|NULL
    stage_status: str  # 'NEW'|'INPUT_COPIED'|'IMPORTED'|'SKIPPED'
    note: Optional[str]
    first_seen_at: str
    last_seen_at: str


def db_upsert_shared_file(cur, row: SharedFileRow) -> int:
    """
    path_hash を一意キーに UPSERT。
    - first_seen_at は初回のみ維持
    - last_seen_at は毎回更新
    """
    path_hash = sha1_text(row.path)

    cur.execute(
        """
        INSERT INTO medi_shared_files
        (
          path_hash, path,
          src_folder_raw, dst_folder_norm, facility_hint,
          file_name, ext, file_size, mtime, sha256,
          auto_judgement, manual_judgement, stage_status, note,
          first_seen_at, last_seen_at
        )
        VALUES
        (
          %s, %s,
          %s, %s, %s,
          %s, %s, %s, %s, %s,
          %s, %s, %s, %s,
          %s, %s
        )
        ON DUPLICATE KEY UPDATE
          path=VALUES(path),
          src_folder_raw=VALUES(src_folder_raw),
          dst_folder_norm=VALUES(dst_folder_norm),
          facility_hint=VALUES(facility_hint),
          file_name=VALUES(file_name),
          ext=VALUES(ext),
          file_size=VALUES(file_size),
          mtime=VALUES(mtime),
          sha256=COALESCE(VALUES(sha256), sha256),  -- NULLで上書きしない
          auto_judgement=VALUES(auto_judgement),
          -- manual は既に入ってるなら維持（運用で手で入れるため）
          manual_judgement=COALESCE(manual_judgement, VALUES(manual_judgement)),
          stage_status=VALUES(stage_status),
          note=VALUES(note),
          last_seen_at=VALUES(last_seen_at),
          shared_file_id=LAST_INSERT_ID(shared_file_id)
        """,
        (
            path_hash,
            row.path,
            row.src_folder_raw,
            row.dst_folder_norm,
            row.facility_hint,
            row.file_name,
            row.ext,
            int(row.file_size),
            row.mtime,
            row.sha256,
            row.auto_judgement,
            row.manual_judgement,
            row.stage_status,
            clip_text(row.note, 1024),
            row.first_seen_at,
            row.last_seen_at,
        ),
    )
    return int(cur.lastrowid)


# -----------------------------
# judge support (NEW zip rows)
# -----------------------------
def db_select_new_zip_files_for_judge(
    cur,
    *,
    limit: int = 500,
    only_stage: str = "NEW",
) -> list[dict[str, Any]]:
    """
    judge対象を取る。

    条件:
    - ext='zip'
    - stage_status=only_stage(既定NEW)
    - sha256 がある（hash_zipが先）
    - manual_judgement が入ってる行は judge上書きしない方針なので除外（運用の正）
    """
    if limit and limit > 0:
        cur.execute(
            """
            SELECT
              shared_file_id,
              path,
              file_name,
              ext,
              sha256,
              src_folder_raw,
              facility_hint,
              auto_judgement,
              manual_judgement,
              stage_status,
              zip_has_xml,
              zip_xml_count,
              zip_xml_checked_at,
              note,
              first_seen_at
            FROM medi_shared_files
            WHERE ext='zip'
              AND stage_status=%s
              AND (sha256 IS NOT NULL AND sha256<>'')
              AND manual_judgement IS NULL
            ORDER BY first_seen_at ASC
            LIMIT %s
            """,
            (only_stage, int(limit)),
        )
    else:
        # limit=0 → 無制限
        cur.execute(
            """
            SELECT
              shared_file_id,
              path,
              file_name,
              ext,
              sha256,
              src_folder_raw,
              facility_hint,
              auto_judgement,
              manual_judgement,
              stage_status,
              zip_has_xml,
              zip_xml_count,
              zip_xml_checked_at,
              note,
              first_seen_at
            FROM medi_shared_files
            WHERE ext='zip'
              AND stage_status=%s
              AND (sha256 IS NOT NULL AND sha256<>'')
              AND manual_judgement IS NULL
            ORDER BY first_seen_at ASC
            """,
            (only_stage,),
        )

    raw_rows = cur.fetchall() or []
    return [row_to_strkey_dict(r) for r in raw_rows if r is not None]


def db_update_zip_xml_probe(
    cur,
    *,
    shared_file_id: int,
    zip_has_xml: Optional[int],
    zip_xml_count: Optional[int],
    note: Optional[str],
) -> None:
    """
    zip内xml判定結果を保存し、checked_atを必ず埋める。
    - zip_has_xml: 0/1（不明ならNULLでも可）
    - zip_xml_count: 0以上（不明ならNULLでも可）
    - note: 失敗理由など（上書きOK。長い場合はclip）
    """
    cur.execute(
        """
        UPDATE medi_shared_files
        SET
          zip_has_xml=%s,
          zip_xml_count=%s,
          zip_xml_checked_at=CURRENT_TIMESTAMP(6),
          note=COALESCE(%s, note),
          updated_at=CURRENT_TIMESTAMP(6)
        WHERE shared_file_id=%s
        """,
        (
            int(zip_has_xml) if zip_has_xml is not None else None,
            int(zip_xml_count) if zip_xml_count is not None else None,
            clip_text(note, 1024) if note else None,
            int(shared_file_id),
        ),
    )


def db_update_auto_judgement(
    cur,
    *,
    shared_file_id: int,
    auto_judgement: str,
    note: Optional[str],
) -> None:
    """
    auto_judgement を更新。
    manual_judgement が入ってる行は運用上の正なので、呼び出し側で除外すること。
    """
    cur.execute(
        """
        UPDATE medi_shared_files
        SET
          auto_judgement=%s,
          note=%s,
          updated_at=CURRENT_TIMESTAMP(6)
        WHERE shared_file_id=%s
        """,
        (auto_judgement, clip_text(note, 1024), int(shared_file_id)),
    )


def db_mark_stage_status(
    cur,
    *,
    shared_file_id: int,
    stage_status: str,
    note: Optional[str] = None,
) -> None:
    cur.execute(
        """
        UPDATE medi_shared_files
        SET stage_status=%s,
            note=COALESCE(%s, note),
            updated_at=CURRENT_TIMESTAMP(6)
        WHERE shared_file_id=%s
        """,
        (stage_status, clip_text(note, 1024) if note else None, int(shared_file_id)),
    )
