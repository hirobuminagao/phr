# -*- coding: utf-8 -*-
"""
============================================================
Module : directory_discovery.py
Path   : scripts/lib/io/directory_discovery.py
Project: PHR

Purpose:
    共通 directory discovery utility。

Responsibility:
    - directory existence check
    - 8桁ディレクトリ列挙
    - suffix別ファイル列挙
    - file existence check
    - CSV row estimate helper

Non-goals:
    - business logic
    - normalize / match
    - identity generation
    - DB access
    - ETL orchestration
============================================================
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional

from scripts.lib.csv.csv_loader import load_csv


# ============================================================
# directory
# ============================================================


def ensure_directory_exists(directory: Path) -> None:
    """directory が存在しない場合は例外を送出する。"""
    if not directory.exists():
        raise FileNotFoundError(f"directory not found: {directory}")

    if not directory.is_dir():
        raise NotADirectoryError(f"not a directory: {directory}")


# ============================================================
# discovery
# ============================================================


def list_8digit_directories(base_directory: Path) -> List[Path]:
    """
    8桁数字ディレクトリを列挙する。

    Example:
        06139463
        99999999
    """
    ensure_directory_exists(base_directory)

    return sorted(
        d
        for d in base_directory.iterdir()
        if d.is_dir()
        and d.name.isdigit()
        and len(d.name) == 8
    )


def list_target_directories(
    base_directory: Path,
    single_directory: Optional[str] = None,
) -> List[Path]:
    """
    import対象ディレクトリを返す。

    single_directory が指定された場合は、そのディレクトリのみを対象にする。
    未指定の場合は base_directory 配下の8桁数字ディレクトリを列挙する。
    """
    if single_directory:
        directory = Path(single_directory)
        ensure_directory_exists(directory)
        return [directory]

    directories = list_8digit_directories(base_directory)
    if not directories:
        raise RuntimeError(f"8 digit directories not found: {base_directory}")

    return directories



def list_files_by_suffix(
    directory: Path,
    suffix: str,
) -> List[Path]:
    """suffix一致ファイルを列挙する。"""
    ensure_directory_exists(directory)

    return sorted(
        p
        for p in directory.iterdir()
        if p.is_file()
        and p.suffix.lower() == suffix.lower()
    )



def has_files_by_suffix(
    directory: Path,
    suffix: str,
) -> bool:
    """suffix一致ファイルが1件以上存在するか返す。"""
    return any(list_files_by_suffix(directory, suffix))


# ============================================================
# CSV helper
# ============================================================


def estimate_csv_rows(
    csv_files: Iterable[Path],
    *,
    header_count: int = 1,
    limit: int = 0,
) -> int:
    """
    CSV群の概算行数を返す。

    ProgressLogger の total 見積もり用途。
    """
    total = 0

    for csv_path in csv_files:
        loader = load_csv(
            path=str(csv_path),
            header_count=header_count,
        )

        total += loader.count_rows()

        if limit and total >= limit:
            return limit

    return total


def estimate_csv_rows_in_directories(
    directories: Iterable[Path],
    *,
    suffix: str = ".csv",
    header_count: int = 1,
    limit: int = 0,
) -> int:
    """
    複数ディレクトリ配下のCSV行数を見積もる。

    ProgressLogger の total 見積もり用途。
    """
    total = 0

    for directory in directories:
        csv_files = list_files_by_suffix(directory, suffix)
        total += estimate_csv_rows(
            csv_files,
            header_count=header_count,
            limit=(limit - total) if limit else 0,
        )

        if limit and total >= limit:
            return limit

    return total
