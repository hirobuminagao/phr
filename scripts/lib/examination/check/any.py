"""Common ANY checks for normalized examination values."""

from collections.abc import Iterable
from typing import Protocol


class ValidatableValue(Protocol):
    """Minimal contract required by has_any_valid()."""

    is_valid: bool


def has_any_valid(values: Iterable[ValidatableValue]) -> bool:
    """
    Return True when at least one value is valid.

    Empty iterables return False.

    This function intentionally depends only on the
    `is_valid` contract.
    """

    return any(value.is_valid for value in values)
