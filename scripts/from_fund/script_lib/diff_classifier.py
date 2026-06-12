#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

DIFF_STATUS_NO_CHANGE = "no_change"
DIFF_STATUS_ADD = "add"
DIFF_STATUS_UPDATE = "update"
DIFF_STATUS_UNKNOWN = "unknown"


@dataclass(frozen=True)
class DiffClassifyResult:
    diff_status: str
    diff_reason: str


def _norm_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _same(a: Any, b: Any) -> bool:
    return _norm_text(a) == _norm_text(b)


def _date_same(a: Any, b: Any) -> bool:
    # MySQL DATE / str / None の比較揺れを避けるため文字列化して比較する。
    return _norm_text(a) == _norm_text(b)


def classify_staging_row(
    staging_row: dict[str, Any],
    subscribers_by_id: dict[int, dict[str, Any]],
) -> DiffClassifyResult:
    """staging行を no_change / add / update / unknown に分類する。

    現時点では、identity_hash が既存 subscribers に一致しない行は add に寄せる。
    major変更候補の探索（identity構成要素による候補検索）は将来対応とする。
    """
    matched_subscriber_id = staging_row.get("matched_subscriber_id")
    if matched_subscriber_id is None:
        return DiffClassifyResult(
            DIFF_STATUS_ADD,
            "identity_hash not matched; treated as add",
        )

    subscriber = subscribers_by_id.get(int(matched_subscriber_id))
    if subscriber is None:
        return DiffClassifyResult(
            DIFF_STATUS_ADD,
            f"matched_subscriber_id not found; treated as add: {matched_subscriber_id}",
        )

    differences: list[str] = []

    checks = [
        ("insurance_symbol_match", staging_row.get("insurance_symbol_match"), subscriber.get("insurance_symbol_match")),
        ("insurance_number_match", staging_row.get("insurance_number_match"), subscriber.get("insurance_number_match")),
        ("name_kana_family_match", staging_row.get("name_kana_family_match"), subscriber.get("name_kana_family_match")),
        ("name_kana_middle_match", staging_row.get("name_kana_middle_match"), subscriber.get("name_kana_middle_match")),
        ("name_kana_given_match", staging_row.get("name_kana_given_match"), subscriber.get("name_kana_given_match")),
        ("name_kanji_family_match", staging_row.get("name_kanji_family_match"), subscriber.get("name_kanji_family_match")),
        ("name_kanji_middle_match", staging_row.get("name_kanji_middle_match"), subscriber.get("name_kanji_middle_match")),
        ("name_kanji_given_match", staging_row.get("name_kanji_given_match"), subscriber.get("name_kanji_given_match")),
        ("gender_code", staging_row.get("gender_code_norm"), subscriber.get("gender_code")),
        ("relationship_name", staging_row.get("relationship_name_norm"), subscriber.get("relationship_name")),
        ("postal_code_match", staging_row.get("postal_code_match"), subscriber.get("postal_code")),
        ("address_match", staging_row.get("address_match"), subscriber.get("address_match")),
        ("phone", staging_row.get("phone_norm"), subscriber.get("phone")),
        ("email", staging_row.get("email_norm"), subscriber.get("email")),
        ("employer_code", staging_row.get("mapped_employer_code"), subscriber.get("employer_code")),
        ("department_code", staging_row.get("mapped_department_code"), subscriber.get("department_code")),
    ]

    for label, left, right in checks:
        if not _same(left, right):
            differences.append(label)

    date_checks = [
        ("birth", staging_row.get("birth_norm"), subscriber.get("birth")),
        ("qualification_acquired_date", staging_row.get("qualification_acquired_date_norm"), subscriber.get("qualification_acquired_date")),
        ("qualification_lost_date", staging_row.get("qualification_lost_date_norm"), subscriber.get("qualification_lost_date")),
    ]

    for label, left, right in date_checks:
        if not _date_same(left, right):
            differences.append(label)

    if differences:
        return DiffClassifyResult(
            DIFF_STATUS_UPDATE,
            "minor: " + ",".join(differences),
        )

    return DiffClassifyResult(
        DIFF_STATUS_NO_CHANGE,
        "no differences",
    )