

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class MajorCandidateResult:
    status: str  # "major_candidate" or "add"
    pattern: Optional[str]
    candidate_subscriber_id: Optional[int]
    reason: str


# ------------------------------------------------------------
# Utility
# ------------------------------------------------------------

def _eq(a: Any, b: Any) -> bool:
    return a is not None and b is not None and a == b


# ------------------------------------------------------------
# Main finder
# ------------------------------------------------------------

def find_major_candidate(
    staging_row: Dict[str, Any],
    subscribers: List[Dict[str, Any]],
) -> MajorCandidateResult:
    """
    identity_hash 不一致の staging_row に対して
    subscribers から候補を探す
    """

    birth = staging_row.get("birth_norm")
    gender = staging_row.get("gender_code_norm")

    kana_full = staging_row.get("name_kana_full_match")

    symbol = staging_row.get("insurance_symbol_match")
    number = staging_row.get("insurance_number_match")

    kana_given = staging_row.get("name_kana_given_match")
    kanji_given = staging_row.get("name_kanji_given_match")

    # --------------------------------------------------------
    # 1. 転籍候補
    # --------------------------------------------------------
    for sub in subscribers:
        if (
            _eq(kana_full, sub.get("name_kana_full_match"))
            and _eq(birth, sub.get("birth"))
            and _eq(gender, sub.get("gender_code"))
        ):
            # 記号番号が違う → 転籍
            if not (
                _eq(symbol, sub.get("insurance_symbol_match"))
                and _eq(number, sub.get("insurance_number_match"))
            ):
                return MajorCandidateResult(
                    status="major_candidate",
                    pattern="transfer",
                    candidate_subscriber_id=sub.get("id"),
                    reason="kana_full+birth+gender一致, 記号番号差分",
                )

    # --------------------------------------------------------
    # 1.5 名字変更（強）候補（フルparts一致ベース）
    # --------------------------------------------------------
    for sub in subscribers:
        if (
            _eq(birth, sub.get("birth"))
            and _eq(gender, sub.get("gender_code"))
        ):
            # カナparts一致（family除く）
            if (
                _eq(staging_row.get("name_kana_given_match"), sub.get("name_kana_given_match"))
                and _eq(staging_row.get("name_kana_middle_match"), sub.get("name_kana_middle_match"))
            ):
                # 漢字parts一致（family除く）
                if (
                    _eq(staging_row.get("name_kanji_given_match"), sub.get("name_kanji_given_match"))
                    and _eq(staging_row.get("name_kanji_middle_match"), sub.get("name_kanji_middle_match"))
                ):
                    return MajorCandidateResult(
                        status="major_candidate",
                        pattern="name_change_strong",
                        candidate_subscriber_id=sub.get("id"),
                        reason="given+middle（kana/kanji）+birth+gender一致",
                    )

    # --------------------------------------------------------
    # 2. 名字変更候補
    # --------------------------------------------------------
    for sub in subscribers:
        if (
            _eq(symbol, sub.get("insurance_symbol_match"))
            and _eq(number, sub.get("insurance_number_match"))
            and _eq(birth, sub.get("birth"))
            and _eq(gender, sub.get("gender_code"))
        ):
            # given一致 → 名字変更の可能性
            if _eq(kana_given, sub.get("name_kana_given_match")):
                return MajorCandidateResult(
                    status="major_candidate",
                    pattern="name_change_kana",
                    candidate_subscriber_id=sub.get("id"),
                    reason="記号番号+birth+gender一致, kana_given一致",
                )

            if _eq(kanji_given, sub.get("name_kanji_given_match")):
                return MajorCandidateResult(
                    status="major_candidate",
                    pattern="name_change_kanji",
                    candidate_subscriber_id=sub.get("id"),
                    reason="記号番号+birth+gender一致, kanji_given一致",
                )

            return MajorCandidateResult(
                status="major_candidate",
                pattern="name_change_loose",
                candidate_subscriber_id=sub.get("id"),
                reason="記号番号+birth+gender一致",
            )

    # --------------------------------------------------------
    # 3. 該当なし → add
    # --------------------------------------------------------
    return MajorCandidateResult(
        status="add",
        pattern=None,
        candidate_subscriber_id=None,
        reason="no candidate",
    )