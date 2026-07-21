"""Common text finding checks for normalized examination values."""

from typing import Protocol


class SearchableTextValue(Protocol):
    """Minimal contract required by text finding checks."""

    is_valid: bool
    text: str | None


def has_text(
    value: SearchableTextValue,
) -> bool:
    """Return True when the value has valid non-empty text."""

    return value.is_valid and value.text is not None and value.text != ""


def contains_any_keyword(
    value: SearchableTextValue,
    keywords: tuple[str, ...],
) -> bool:
    """Return True when the value text contains at least one keyword."""

    if not has_text(value):
        return False

    text = value.text
    assert text is not None

    return any(keyword in text for keyword in keywords)
