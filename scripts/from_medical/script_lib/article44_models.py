"""Type contracts shared by Article 44 examination check layers.

This module defines data contracts for layers 1 through 3. It does not perform
DB access, value loading, normalization, checking, or persistence.

Invariants:
- When invalid_reason is DUPLICATE_NAMECODE, duplicate_count is at least 2.
- For reasons other than DUPLICATE_NAMECODE, duplicate_count is None.
- NOT_FOUND, NULL, and EMPTY values have is_valid=False.
- ValueMap keys are namecodes.
- Article44Result keys are legal detail numbers.
"""

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import TypeAlias


class ExpectedValueType(str, Enum):
    """Expected value type returned by the Article 44 namecode layer."""

    PQ = "PQ"
    CD = "CD"
    ST = "ST"


@dataclass(frozen=True)
class RequiredNamecode:
    """One required namecode and its expected value type."""

    namecode: str
    expected_value_type: ExpectedValueType


class ValueState(str, Enum):
    """Presence state of a normalized examination value."""

    NOT_FOUND = "NOT_FOUND"
    NULL = "NULL"
    EMPTY = "EMPTY"
    PRESENT = "PRESENT"


class ValueInvalidReason(str, Enum):
    """Reason why a present value cannot be used."""

    TYPE_MISMATCH = "TYPE_MISMATCH"
    PARSE_ERROR = "PARSE_ERROR"
    FORMAT_ERROR = "FORMAT_ERROR"
    DUPLICATE_NAMECODE = "DUPLICATE_NAMECODE"


@dataclass(frozen=True)
class PQValue:
    """Normalized numeric physical quantity value."""

    value_state: ValueState
    raw_value: str | None
    numeric_value: Decimal | None
    unit: str | None
    is_valid: bool
    invalid_reason: ValueInvalidReason | None
    duplicate_count: int | None


@dataclass(frozen=True)
class CDValue:
    """Normalized coded value."""

    value_state: ValueState
    raw_value: str | None
    code_value: str | None
    is_valid: bool
    invalid_reason: ValueInvalidReason | None
    duplicate_count: int | None


@dataclass(frozen=True)
class STValue:
    """Normalized text value."""

    value_state: ValueState
    raw_text: str | None
    text: str | None
    is_valid: bool
    invalid_reason: ValueInvalidReason | None
    duplicate_count: int | None


@dataclass(frozen=True)
class CheckResult:
    """The status/reason pair for one Article 44 detail item."""

    status: str
    reason: str | None = None


ValueMap: TypeAlias = dict[str, PQValue | CDValue | STValue]

Article44Result: TypeAlias = dict[str, CheckResult]
