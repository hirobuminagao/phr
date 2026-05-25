

# -*- coding: utf-8 -*-
"""
============================================================
Module : apply_action_subscriber.py
Path   : scripts/hia/script_lib/apply_action_subscriber.py
Project: PHR

Purpose:
    Apply one staging subscriber row to subscribers root table.

Responsibility:
    - apply subscribers root insert/update for one staging row
    - split subscriber root updates into identity fields and other fields
    - return target subscribers.id for child resources

Non-goals:
    - subscriber_addresses apply
    - subscriber_contact_points apply
    - subscriber_audit insert
    - staging processed mark
    - prepare / compare decision

Notes:
    `subscribers` is the root resource.

    identity fields:
        - insurance / name / birth / gender
        - identity_hash
        - compare_identity_norm_hash

    other fields:
        - insured / relationship / qualification / organization
        - compare_other_hash
============================================================
"""

from __future__ import annotations

from typing import Any


# ============================================================
# helpers
# ============================================================


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _split_diff_columns(value: Any) -> set[str]:
    text = _as_text(value)
    if not text:
        return set()
    return {part.strip() for part in text.split(",") if part.strip()}


def _to_date_or_none(value: Any) -> Any:
    """DB driver に渡す日付値。空文字は None として扱う。"""

    if value in (None, ""):
        return None
    return value


# ============================================================
# column mapping
# ============================================================


SUBSCRIBER_IDENTITY_COLUMNS = {
    "hia_subscriber_id": "hia_subscriber_id",
    "person_id_custom": "person_id_custom",
    "identity_hash": "identity_hash",
    "compare_identity_norm_hash": "compare_identity_norm_hash",
    "insurer_number": "insurer_number",
    "insurance_symbol": "insurance_symbol",
    "insurance_symbol_digits": "insurance_symbol_digits",
    "insurance_number": "insurance_number",
    "insurance_branchnumber": "insurance_branchnumber",
    "birth": "birth",
    "gender_code": "gender_code",
    "name_kana_full": "name_kana_full",
    "name_kana_full_match": "name_kana_full_match",
    "name_kanji_full": "name_kanji_full",
    "name_full_match": "name_kanji_full_match",
    "name_kana_family": "name_kana_family",
    "name_kana_middle": "name_kana_middle",
    "name_kana_given": "name_kana_given",
    "name_kanji_family": "name_kanji_family",
    "name_kanji_middle": "name_kanji_middle",
    "name_kanji_given": "name_kanji_given",
    "name_kana_family_match": "name_kana_family",
    "name_kana_middle_match": "name_kana_middle",
    "name_kana_given_match": "name_kana_given",
    "name_kanji_family_match": "name_kanji_family",
    "name_kanji_middle_match": "name_kanji_middle",
    "name_kanji_given_match": "name_kanji_given",
}


SUBSCRIBER_OTHER_COLUMNS = {
    "compare_other_hash": "compare_other_hash",
    "insured_attribute_name": "insured_attribute_name",
    "relationship_name": "relationship_name",
    "qualification_acquired_date": "qualification_acquired_date",
    "qualification_lost_date": "qualification_lost_date",
    "employer_code": "employer_code",
    "department_code": "department_code",
    "distribution_code": "distribution_code",
    "employee_code": "employee_code",
    "connect_id": "connect_id",
}


# ============================================================
# value builders
# ============================================================


def build_subscriber_identity_values(row: dict[str, Any]) -> dict[str, Any]:
    """staging row から subscribers identity fields の値を作る。"""

    values: dict[str, Any] = {}

    for target_column, source_key in SUBSCRIBER_IDENTITY_COLUMNS.items():
        value = row.get(source_key)
        if target_column == "birth":
            value = _to_date_or_none(value)
        values[target_column] = value

    return values


def build_subscriber_other_values(row: dict[str, Any]) -> dict[str, Any]:
    """staging row から subscribers other fields の値を作る。"""

    values: dict[str, Any] = {}

    for target_column, source_key in SUBSCRIBER_OTHER_COLUMNS.items():
        value = row.get(source_key)
        if target_column in {"qualification_acquired_date", "qualification_lost_date"}:
            value = _to_date_or_none(value)
        values[target_column] = value

    return values


def build_subscriber_insert_values(
    row: dict[str, Any],
    *,
    apply_run_id: int,
) -> dict[str, Any]:
    """subscribers INSERT 用 values を作る。"""

    values: dict[str, Any] = {}
    values.update(build_subscriber_identity_values(row))
    values.update(build_subscriber_other_values(row))
    values["last_change_run_id"] = apply_run_id
    return values


# ============================================================
# insert / update
# ============================================================


def insert_subscriber_root(
    cur,
    *,
    row: dict[str, Any],
    apply_run_id: int,
) -> int:
    """subscribers に新規 root row を INSERT し、subscribers.id を返す。"""

    values = build_subscriber_insert_values(row, apply_run_id=apply_run_id)

    cur.execute(
        """
        INSERT INTO subscribers (
            hia_subscriber_id,
            person_id_custom,
            identity_hash,
            compare_identity_norm_hash,
            compare_other_hash,
            insurer_number,
            insurance_symbol,
            insurance_symbol_digits,
            insurance_number,
            insurance_branchnumber,
            birth,
            gender_code,
            name_kana_full,
            name_kana_full_match,
            name_kanji_full,
            name_full_match,
            name_kana_family,
            name_kana_middle,
            name_kana_given,
            name_kanji_family,
            name_kanji_middle,
            name_kanji_given,
            name_kana_family_match,
            name_kana_middle_match,
            name_kana_given_match,
            name_kanji_family_match,
            name_kanji_middle_match,
            name_kanji_given_match,
            insured_attribute_name,
            relationship_name,
            qualification_acquired_date,
            qualification_lost_date,
            employer_code,
            department_code,
            distribution_code,
            employee_code,
            connect_id,
            last_change_run_id
        )
        VALUES (
            %(hia_subscriber_id)s,
            %(person_id_custom)s,
            %(identity_hash)s,
            %(compare_identity_norm_hash)s,
            %(compare_other_hash)s,
            %(insurer_number)s,
            %(insurance_symbol)s,
            %(insurance_symbol_digits)s,
            %(insurance_number)s,
            %(insurance_branchnumber)s,
            %(birth)s,
            %(gender_code)s,
            %(name_kana_full)s,
            %(name_kana_full_match)s,
            %(name_kanji_full)s,
            %(name_full_match)s,
            %(name_kana_family)s,
            %(name_kana_middle)s,
            %(name_kana_given)s,
            %(name_kanji_family)s,
            %(name_kanji_middle)s,
            %(name_kanji_given)s,
            %(name_kana_family_match)s,
            %(name_kana_middle_match)s,
            %(name_kana_given_match)s,
            %(name_kanji_family_match)s,
            %(name_kanji_middle_match)s,
            %(name_kanji_given_match)s,
            %(insured_attribute_name)s,
            %(relationship_name)s,
            %(qualification_acquired_date)s,
            %(qualification_lost_date)s,
            %(employer_code)s,
            %(department_code)s,
            %(distribution_code)s,
            %(employee_code)s,
            %(connect_id)s,
            %(last_change_run_id)s
        )
        """,
        values,
    )

    return int(cur.lastrowid)


def apply_subscriber_identity_fields(
    cur,
    *,
    subscriber_id: int,
    row: dict[str, Any],
    apply_run_id: int,
) -> None:
    """既存 subscribers の identity fields を staging 値へ更新する。"""

    values = build_subscriber_identity_values(row)
    values["subscriber_id"] = subscriber_id
    values["last_change_run_id"] = apply_run_id

    cur.execute(
        """
        UPDATE subscribers
        SET
            hia_subscriber_id = %(hia_subscriber_id)s,
            person_id_custom = %(person_id_custom)s,
            identity_hash = %(identity_hash)s,
            compare_identity_norm_hash = %(compare_identity_norm_hash)s,
            insurer_number = %(insurer_number)s,
            insurance_symbol = %(insurance_symbol)s,
            insurance_symbol_digits = %(insurance_symbol_digits)s,
            insurance_number = %(insurance_number)s,
            insurance_branchnumber = %(insurance_branchnumber)s,
            birth = %(birth)s,
            gender_code = %(gender_code)s,
            name_kana_full = %(name_kana_full)s,
            name_kana_full_match = %(name_kana_full_match)s,
            name_kanji_full = %(name_kanji_full)s,
            name_full_match = %(name_full_match)s,
            name_kana_family = %(name_kana_family)s,
            name_kana_middle = %(name_kana_middle)s,
            name_kana_given = %(name_kana_given)s,
            name_kanji_family = %(name_kanji_family)s,
            name_kanji_middle = %(name_kanji_middle)s,
            name_kanji_given = %(name_kanji_given)s,
            name_kana_family_match = %(name_kana_family_match)s,
            name_kana_middle_match = %(name_kana_middle_match)s,
            name_kana_given_match = %(name_kana_given_match)s,
            name_kanji_family_match = %(name_kanji_family_match)s,
            name_kanji_middle_match = %(name_kanji_middle_match)s,
            name_kanji_given_match = %(name_kanji_given_match)s,
            last_change_run_id = %(last_change_run_id)s
        WHERE id = %(subscriber_id)s
        """,
        values,
    )


def apply_subscriber_other_fields(
    cur,
    *,
    subscriber_id: int,
    row: dict[str, Any],
    apply_run_id: int,
) -> None:
    """既存 subscribers の other fields を staging 値へ更新する。"""

    values = build_subscriber_other_values(row)
    values["subscriber_id"] = subscriber_id
    values["last_change_run_id"] = apply_run_id

    cur.execute(
        """
        UPDATE subscribers
        SET
            compare_other_hash = %(compare_other_hash)s,
            insured_attribute_name = %(insured_attribute_name)s,
            relationship_name = %(relationship_name)s,
            qualification_acquired_date = %(qualification_acquired_date)s,
            qualification_lost_date = %(qualification_lost_date)s,
            employer_code = %(employer_code)s,
            department_code = %(department_code)s,
            distribution_code = %(distribution_code)s,
            employee_code = %(employee_code)s,
            connect_id = %(connect_id)s,
            last_change_run_id = %(last_change_run_id)s
        WHERE id = %(subscriber_id)s
        """,
        values,
    )


# ============================================================
# public entry
# ============================================================


def apply_subscriber_root(
    cur,
    *,
    row: dict[str, Any],
    apply_run_id: int,
) -> int | None:
    """
    1 staging row に対する subscribers root apply を行う。

    Returns:
        対象 subscribers.id
    """

    action = _as_text(row.get("apply_action"))

    if action == "insert":
        return insert_subscriber_root(
            cur,
            row=row,
            apply_run_id=apply_run_id,
        )

    subscriber_id = row.get("current_subscriber_id")
    if subscriber_id is None:
        return None

    diff_columns = _split_diff_columns(row.get("apply_diff_columns"))

    if "compare_identity_norm_hash" in diff_columns or "identity_hash" in diff_columns:
        apply_subscriber_identity_fields(
            cur,
            subscriber_id=int(subscriber_id),
            row=row,
            apply_run_id=apply_run_id,
        )

    if "compare_other_hash" in diff_columns:
        apply_subscriber_other_fields(
            cur,
            subscriber_id=int(subscriber_id),
            row=row,
            apply_run_id=apply_run_id,
        )

    return int(subscriber_id)