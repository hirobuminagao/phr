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
    - write field-level subscriber_audit rows for subscriber root changes

Non-goals:
    - subscriber_addresses apply
    - subscriber_contact_points apply
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

    Audit policy:
        - 1 changed field = 1 subscriber_audit row
        - audit rows are inserted after successful subscribers INSERT/UPDATE
============================================================
"""

from __future__ import annotations

from typing import Any
from scripts.hia.script_lib.apply_action_subscriber_audit import (
    build_subscriber_audit_rows_from_fields,
    insert_subscriber_audit_rows,
)


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


def _name_part_or_none(*, full_value: Any, part_value: Any) -> Any:
    """全文と同じ値しか入っていない name part は未分割扱いとして None にする。"""

    if part_value in (None, ""):
        return None
    if _as_text(full_value) == _as_text(part_value):
        return None
    return part_value


# ============================================================
# audit field definitions
# ============================================================


IDENTITY_AUDIT_FIELDS = [
    "hia_subscriber_id",
    "person_id_custom",
    "identity_hash",
    "compare_identity_norm_hash",
    "insurer_number",
    "insurance_symbol",
    "insurance_symbol_digits",
    "insurance_number",
    "insurance_branchnumber",
    "birth",
    "gender_code",
    "name_kana_full",
    "name_kana_full_match",
    "name_kanji_full",
    "name_full_match",
    "name_kana_family",
    "name_kana_middle",
    "name_kana_given",
    "name_kanji_family",
    "name_kanji_middle",
    "name_kanji_given",
    "name_kana_family_match",
    "name_kana_middle_match",
    "name_kana_given_match",
    "name_kanji_family_match",
    "name_kanji_middle_match",
    "name_kanji_given_match",
]


OTHER_AUDIT_FIELDS = [
    "compare_other_hash",
    "insured_attribute_name",
    "relationship_name",
    "qualification_acquired_date",
    "qualification_lost_date",
    "employer_code",
    "department_code",
    "distribution_code",
    "employee_code",
    "connect_id",
]


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

    values = {
        "hia_subscriber_id": row.get("hia_subscriber_id"),
        "person_id_custom": row.get("person_id_custom"),
        "identity_hash": row.get("identity_hash"),
        "compare_identity_norm_hash": row.get("compare_identity_norm_hash"),
        "compare_other_hash": row.get("compare_other_hash"),
        "insurer_number": row.get("insurer_number"),
        "insurance_symbol": row.get("insurance_symbol"),
        "insurance_symbol_digits": row.get("insurance_symbol_digits"),
        "insurance_number": row.get("insurance_number"),
        "insurance_branchnumber": row.get("insurance_branchnumber"),
        "birth": _to_date_or_none(row.get("birth")),
        "gender_code": row.get("gender_code"),
        "name_kana_full": row.get("name_kana_full"),
        "name_kana_full_match": row.get("name_kana_full_match"),
        "name_kanji_full": row.get("name_kanji_full"),
        "name_full_match": row.get("name_kanji_full_match"),
        "name_kana_family": _name_part_or_none(
            full_value=row.get("name_kana_full"),
            part_value=row.get("name_kana_family"),
        ),
        "name_kana_middle": _name_part_or_none(
            full_value=row.get("name_kana_full"),
            part_value=row.get("name_kana_middle"),
        ),
        "name_kana_given": _name_part_or_none(
            full_value=row.get("name_kana_full"),
            part_value=row.get("name_kana_given"),
        ),
        "name_kanji_family": row.get("name_kanji_family"),
        "name_kanji_middle": row.get("name_kanji_middle"),
        "name_kanji_given": row.get("name_kanji_given"),
        "name_kana_family_match": None,
        "name_kana_middle_match": None,
        "name_kana_given_match": None,
        "name_kanji_family_match": None,
        "name_kanji_middle_match": None,
        "name_kanji_given_match": None,
        "insured_attribute_name": row.get("insured_attribute_name"),
        "relationship_name": row.get("relationship_name"),
        "qualification_acquired_date": _to_date_or_none(row.get("qualification_acquired_date")),
        "qualification_lost_date": _to_date_or_none(row.get("qualification_lost_date")),
        "employer_code": row.get("employer_code"),
        "department_code": row.get("department_code"),
        "distribution_code": row.get("distribution_code"),
        "employee_code": row.get("employee_code"),
        "connect_id": row.get("connect_id"),
        "last_change_run_id": apply_run_id,
    }

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

    subscriber_id = int(cur.lastrowid)

    audit_rows = build_subscriber_audit_rows_from_fields(
        subscriber_id=subscriber_id,
        fields=[*IDENTITY_AUDIT_FIELDS, *OTHER_AUDIT_FIELDS],
        old_values={},
        new_values=values,
        source="hia_apply",
        note="subscriber insert",
        change_run_id=apply_run_id,
    )

    insert_subscriber_audit_rows(
        cur,
        audit_rows=audit_rows,
    )

    return subscriber_id


def load_current_subscriber_values(
    cur,
    *,
    subscriber_id: int,
) -> dict[str, Any]:
    """audit比較用に現在の subscribers row を取得する。"""

    cur.execute(
        """
        SELECT *
        FROM subscribers
        WHERE id = %(subscriber_id)s
        """,
        {
            "subscriber_id": subscriber_id,
        },
    )

    row = cur.fetchone()
    if not row:
        return {}

    return dict(row)


def apply_subscriber_identity_fields(
    cur,
    *,
    subscriber_id: int,
    row: dict[str, Any],
    apply_run_id: int,
) -> None:
    """既存 subscribers の identity fields を staging 値へ更新する。"""

    current_values = load_current_subscriber_values(
        cur,
        subscriber_id=subscriber_id,
    )

    values = {
        "subscriber_id": subscriber_id,
        "hia_subscriber_id": row.get("hia_subscriber_id"),
        "person_id_custom": row.get("person_id_custom"),
        "identity_hash": row.get("identity_hash"),
        "compare_identity_norm_hash": row.get("compare_identity_norm_hash"),
        "insurer_number": row.get("insurer_number"),
        "insurance_symbol": row.get("insurance_symbol"),
        "insurance_symbol_digits": row.get("insurance_symbol_digits"),
        "insurance_number": row.get("insurance_number"),
        "insurance_branchnumber": row.get("insurance_branchnumber"),
        "birth": _to_date_or_none(row.get("birth")),
        "gender_code": row.get("gender_code"),
        "name_kana_full": row.get("name_kana_full"),
        "name_kana_full_match": row.get("name_kana_full_match"),
        "name_kanji_full": row.get("name_kanji_full"),
        "name_full_match": row.get("name_kanji_full_match"),
        "name_kana_family": _name_part_or_none(
            full_value=row.get("name_kana_full"),
            part_value=row.get("name_kana_family"),
        ),
        "name_kana_middle": _name_part_or_none(
            full_value=row.get("name_kana_full"),
            part_value=row.get("name_kana_middle"),
        ),
        "name_kana_given": _name_part_or_none(
            full_value=row.get("name_kana_full"),
            part_value=row.get("name_kana_given"),
        ),
        "name_kanji_family": row.get("name_kanji_family"),
        "name_kanji_middle": row.get("name_kanji_middle"),
        "name_kanji_given": row.get("name_kanji_given"),
        "name_kana_family_match": None,
        "name_kana_middle_match": None,
        "name_kana_given_match": None,
        "name_kanji_family_match": None,
        "name_kanji_middle_match": None,
        "name_kanji_given_match": None,
        "last_change_run_id": apply_run_id,
    }

    audit_rows = build_subscriber_audit_rows_from_fields(
        subscriber_id=subscriber_id,
        fields=IDENTITY_AUDIT_FIELDS,
        old_values=current_values,
        new_values=values,
        source="hia_apply",
        note="subscriber identity update",
        change_run_id=apply_run_id,
    )

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

    insert_subscriber_audit_rows(
        cur,
        audit_rows=audit_rows,
    )


def apply_subscriber_other_fields(
    cur,
    *,
    subscriber_id: int,
    row: dict[str, Any],
    apply_run_id: int,
) -> None:
    """既存 subscribers の other fields を staging 値へ更新する。"""

    current_values = load_current_subscriber_values(
        cur,
        subscriber_id=subscriber_id,
    )

    values = {
        "subscriber_id": subscriber_id,
        "compare_other_hash": row.get("compare_other_hash"),
        "insured_attribute_name": row.get("insured_attribute_name"),
        "relationship_name": row.get("relationship_name"),
        "qualification_acquired_date": _to_date_or_none(row.get("qualification_acquired_date")),
        "qualification_lost_date": _to_date_or_none(row.get("qualification_lost_date")),
        "employer_code": row.get("employer_code"),
        "department_code": row.get("department_code"),
        "distribution_code": row.get("distribution_code"),
        "employee_code": row.get("employee_code"),
        "connect_id": row.get("connect_id"),
        "last_change_run_id": apply_run_id,
    }

    audit_rows = build_subscriber_audit_rows_from_fields(
        subscriber_id=subscriber_id,
        fields=OTHER_AUDIT_FIELDS,
        old_values=current_values,
        new_values=values,
        source="hia_apply",
        note="subscriber other update",
        change_run_id=apply_run_id,
    )

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

    insert_subscriber_audit_rows(
        cur,
        audit_rows=audit_rows,
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