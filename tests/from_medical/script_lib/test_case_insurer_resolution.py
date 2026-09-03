from __future__ import annotations

import pytest

from scripts.from_medical.script_lib.case_insurer_resolution import (
    EventInsurerContext,
    InsurerResolutionError,
    canonical_insurer_number,
    resolve_case_insurer_number,
)


def context(*numbers: str) -> EventInsurerContext:
    return EventInsurerContext(
        event_insurer_number=numbers[0],
        fund_id=1,
        allowed_insurer_numbers=frozenset(numbers),
    )


def test_canonical_insurer_rejects_all_zero() -> None:
    assert canonical_insurer_number("00000000") is None


def test_subscriber_insurer_wins_with_multiple_allowed_numbers() -> None:
    result = resolve_case_insurer_number(
        context=context("06139463", "06345678"),
        subscriber_insurer_number="6345678",
        ledgers=[{"insurer_number": "06139463"}],
    )
    assert result == "06345678"


def test_corrected_ledger_value_wins_without_subscriber_value() -> None:
    result = resolve_case_insurer_number(
        context=context("06139463", "06345678"),
        subscriber_insurer_number=None,
        ledgers=[{"insurer_number": "00000000", "insurer_number_export_value": "06345678"}],
    )
    assert result == "06345678"


def test_single_event_candidate_fills_invalid_received_value() -> None:
    result = resolve_case_insurer_number(
        context=context("06139463"),
        subscriber_insurer_number=None,
        ledgers=[{"insurer_number": "00000000"}],
    )
    assert result == "06139463"


def test_multiple_allowed_numbers_without_evidence_are_unresolved() -> None:
    with pytest.raises(InsurerResolutionError, match="INSURER_NUMBER_UNRESOLVED"):
        resolve_case_insurer_number(
            context=context("06139463", "06345678"),
            subscriber_insurer_number=None,
            ledgers=[{"insurer_number": "00000000"}],
        )


def test_subscriber_insurer_outside_event_fund_is_rejected() -> None:
    with pytest.raises(InsurerResolutionError, match="SUBSCRIBER_INSURER_NOT_ALLOWED"):
        resolve_case_insurer_number(
            context=context("06139463"),
            subscriber_insurer_number="99999999",
            ledgers=[],
        )
