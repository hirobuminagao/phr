"""Common ALL checks for normalized examination values."""

from collections.abc import Iterable
from typing import Protocol


class ValidatableValue(Protocol):
    """Minimal contract required by has_all_valid()."""

    is_valid: bool


def has_all_valid(values: Iterable[ValidatableValue]) -> bool:
    """
    Return True when every value is valid.

    Empty iterables return True, following Python's built-in all() behavior.

    This function intentionally depends only on the
    `is_valid` contract.
    """

    return all(value.is_valid for value in values)
