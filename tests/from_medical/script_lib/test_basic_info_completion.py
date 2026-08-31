from scripts.from_medical.script_lib.basic_info_completion import (
    INSURER_NUMBER_SOURCE_EVENT,
    INSURER_NUMBER_SOURCE_SOURCE,
    INSURER_NUMBER_STATUS_FILLED_FROM_EVENT,
    INSURER_NUMBER_STATUS_NOT_NEEDED,
    resolve_insurer_number_completion,
)


def test_all_zero_insurer_number_is_completed_from_event() -> None:
    result = resolve_insurer_number_completion(
        source_value="00000000",
        event_value="06139463",
    )

    assert result.status == INSURER_NUMBER_STATUS_FILLED_FROM_EVENT
    assert result.source == INSURER_NUMBER_SOURCE_EVENT
    assert result.export_value == "06139463"
    assert result.reason == "SOURCE_INSURER_NUMBER_ALL_ZERO"


def test_legitimate_insurer_number_with_leading_zero_is_preserved() -> None:
    result = resolve_insurer_number_completion(
        source_value="06139463",
        event_value="06139463",
    )

    assert result.status == INSURER_NUMBER_STATUS_NOT_NEEDED
    assert result.source == INSURER_NUMBER_SOURCE_SOURCE
    assert result.export_value == "06139463"
