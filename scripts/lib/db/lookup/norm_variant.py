"""Lookup helpers for phr_master.norm_variants."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from scripts.lib.db.schemas import PHR_MASTER


def _compact_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def get_norm_variant(
    cur: Any,
    *,
    result_code_oid: str | None,
    raw_value_utf8: str | None,
    master_db: str = PHR_MASTER,
) -> dict[str, Any] | None:
    """Return one active norm variant by OID and raw value."""

    oid = _compact_text(result_code_oid)
    raw_value = _compact_text(raw_value_utf8)
    if oid is None or raw_value is None:
        return None

    cur.execute(
        f"""
        SELECT
            variant_id,
            result_code_oid,
            raw_token_norm,
            raw_value_utf8,
            normalized_code,
            code_system,
            display_name,
            is_canonical,
            priority
        FROM `{master_db}`.`norm_variants`
        WHERE result_code_oid = %s
          AND BINARY raw_value_utf8 = BINARY %s
          AND is_active = 1
        ORDER BY priority, variant_id
        LIMIT 1
        """,
        (oid, raw_value),
    )
    row = cur.fetchone()
    return dict(row) if row is not None else None


def get_canonical_norm_variant(
    cur: Any,
    *,
    result_code_oid: str | None,
    normalized_code: str | None,
    master_db: str = PHR_MASTER,
) -> dict[str, Any] | None:
    """Return the canonical row for an already-decided result code."""

    oid = _compact_text(result_code_oid)
    code = _compact_text(normalized_code)
    if oid is None or code is None:
        return None

    cur.execute(
        f"""
        SELECT
            variant_id,
            result_code_oid,
            raw_token_norm,
            raw_value_utf8,
            normalized_code,
            code_system,
            display_name,
            is_canonical,
            priority
        FROM `{master_db}`.`norm_variants`
        WHERE result_code_oid = %s
          AND BINARY normalized_code = BINARY %s
          AND is_canonical = 1
          AND is_active = 1
        ORDER BY priority, variant_id
        LIMIT 1
        """,
        (oid, code),
    )
    row = cur.fetchone()
    return dict(row) if row is not None else None


def get_norm_variants(
    cur: Any,
    keys: Iterable[tuple[str | None, str | None]],
    *,
    master_db: str = PHR_MASTER,
) -> dict[tuple[str, str], dict[str, Any]]:
    """Return active norm variants keyed by `(result_code_oid, raw_value_utf8)`."""

    normalized_keys: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for raw_oid, raw_value in keys:
        oid = _compact_text(raw_oid)
        value = _compact_text(raw_value)
        if oid is None or value is None:
            continue
        key = (oid, value)
        if key in seen:
            continue
        seen.add(key)
        normalized_keys.append(key)

    if not normalized_keys:
        return {}

    conditions = " OR ".join(["(result_code_oid = %s AND BINARY raw_value_utf8 = BINARY %s)"] * len(normalized_keys))
    params: list[str] = []
    for oid, value in normalized_keys:
        params.extend([oid, value])

    cur.execute(
        f"""
        SELECT
            variant_id,
            result_code_oid,
            raw_token_norm,
            raw_value_utf8,
            normalized_code,
            code_system,
            display_name,
            is_canonical,
            priority
        FROM `{master_db}`.`norm_variants`
        WHERE is_active = 1
          AND ({conditions})
        ORDER BY result_code_oid, raw_value_utf8, priority, variant_id
        """,
        tuple(params),
    )

    result: dict[tuple[str, str], dict[str, Any]] = {}
    for row in cur.fetchall():
        item = dict(row)
        key = (str(item["result_code_oid"]), str(item["raw_value_utf8"]))
        result.setdefault(key, item)
    return result
