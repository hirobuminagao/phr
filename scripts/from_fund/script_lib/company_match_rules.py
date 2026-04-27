

from __future__ import annotations

from typing import Any, Iterable

from scripts.from_fund.import_staging_subscribers_fund import base_normalize


def _to_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (int, float)):
        return str(v)
    return str(v)


def _norm(v: Any) -> str:
    s = _to_str(v)
    return base_normalize(s) or ""


def rule_as_is(values: list[Any]) -> str | None:
    v = _to_str(values[0]) if values else ""
    v = v.strip()
    return v or None


def rule_text_norm(values: list[Any]) -> str | None:
    v = values[0] if values else None
    s = _norm(v)
    return s or None


def rule_left3(values: list[Any]) -> str | None:
    v = _norm(values[0] if values else None)
    if not v:
        return None
    return v[:3]


def rule_left3_before_colon(values: list[Any]) -> str | None:
    v = _norm(values[0] if values else None)
    if not v:
        return None
    # 例: "100：社長室" / "100:社長室"
    for sep in ("：", ":"):
        if sep in v:
            v = v.split(sep, 1)[0]
            break
    return v[:3] if v else None


def rule_concat_with_pipe(values: list[Any]) -> str | None:
    if not values:
        return None
    parts = [_norm(v) for v in values if _norm(v)]
    if not parts:
        return None
    return "|".join(parts)


RULES = {
    "as_is": rule_as_is,
    "text_norm": rule_text_norm,
    "left3": rule_left3,
    "left3_before_colon": rule_left3_before_colon,
    "concat_with_pipe": rule_concat_with_pipe,
}


def apply_company_match_rule(rule: str, values: Iterable[Any]) -> str | None:
    fn = RULES.get(rule)
    if fn is None:
        raise ValueError(f"Unsupported company match rule: {rule}")
    return fn(list(values))