"""Load Article 44 required namecode definitions."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from scripts.from_medical.script_lib.article44_models import (
    ExpectedValueType,
    RequiredNamecode,
)
from scripts.lib.examination.lookup import qname


ARTICLE44_GROUP_CODE = "v2_2026_ARTICLE44_CHECK_ITEMS"


def fetch_article44_required_namecodes(
    cursor: Any,
    *,
    dev_db: str = "dev_phr",
) -> tuple[RequiredNamecode, ...]:
    """Fetch and build Article 44 required namecodes from group members."""

    cursor.execute(
        f"""
        SELECT
          namecode,
          value_type,
          method,
          identity_code,
          priority
        FROM {qname(dev_db)}.exam_item_group_members
        WHERE group_code = %s
        ORDER BY priority, namecode
        """,
        (ARTICLE44_GROUP_CODE,),
    )
    return _build_required_namecodes(cursor.fetchall())


def _build_required_namecodes(
    rows: Iterable[Mapping[str, object]],
) -> tuple[RequiredNamecode, ...]:
    """Build required namecodes from already-fetched DB rows."""

    rows = tuple(rows)
    if not rows:
        raise ValueError(f"group_code={ARTICLE44_GROUP_CODE}: no Article44 group members found")

    results: list[RequiredNamecode] = []
    seen: dict[str, tuple[ExpectedValueType, str, str]] = {}
    for row in rows:
        namecode = _required_text(row, "namecode", namecode=None)
        value_type = _required_text(row, "value_type", namecode=namecode)
        method = _required_text(row, "method", namecode=namecode)
        identity_code = _required_text(row, "identity_code", namecode=namecode)
        _required_present(row, "priority", namecode=namecode)

        expected_value_type = _expected_value_type(value_type, namecode=namecode)
        current = (expected_value_type, method, identity_code)
        previous = seen.get(namecode)
        if previous is None:
            seen[namecode] = current
            results.append(
                RequiredNamecode(
                    namecode=namecode,
                    expected_value_type=expected_value_type,
                )
            )
            continue

        if previous != current:
            raise ValueError(
                "group_code="
                f"{ARTICLE44_GROUP_CODE}: duplicate namecode definition mismatch "
                f"namecode={namecode} previous={previous!r} current={current!r}"
            )

    return tuple(results)


def _required_text(
    row: Mapping[str, object],
    column: str,
    *,
    namecode: str | None,
) -> str:
    value = _required_present(row, column, namecode=namecode)
    if not isinstance(value, str):
        raise ValueError(
            f"group_code={ARTICLE44_GROUP_CODE}: namecode={namecode} "
            f"column={column} invalid value={value!r}"
        )
    stripped = value.strip()
    if stripped == "":
        raise ValueError(
            f"group_code={ARTICLE44_GROUP_CODE}: namecode={namecode} "
            f"column={column} invalid value={value!r}"
        )
    return stripped


def _required_present(
    row: Mapping[str, object],
    column: str,
    *,
    namecode: str | None,
) -> object:
    value = row.get(column)
    if value is None:
        raise ValueError(
            f"group_code={ARTICLE44_GROUP_CODE}: namecode={namecode} "
            f"column={column} invalid value={value!r}"
        )
    return value


def _expected_value_type(
    value_type: str,
    *,
    namecode: str,
) -> ExpectedValueType:
    if value_type != value_type.strip() or value_type.upper() != value_type:
        raise ValueError(
            f"group_code={ARTICLE44_GROUP_CODE}: namecode={namecode} "
            f"column=value_type invalid value={value_type!r}"
        )

    if value_type == "PQ":
        return ExpectedValueType.PQ
    if value_type in {"CD", "CO"}:
        return ExpectedValueType.CD
    if value_type == "ST":
        return ExpectedValueType.ST

    raise ValueError(
        f"group_code={ARTICLE44_GROUP_CODE}: namecode={namecode} "
        f"column=value_type invalid value={value_type!r}"
    )
