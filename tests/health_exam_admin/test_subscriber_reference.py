from apps.health_exam_admin.main import mask_subscriber_text, subscriber_reference_pii_level


def test_mask_subscriber_text_keeps_only_requested_suffix() -> None:
    assert mask_subscriber_text("12345678", keep_end=4) == "****5678"
    assert mask_subscriber_text("ABCD", keep_end=2) == "**CD"
    assert mask_subscriber_text(None) == "未登録"


def test_subscriber_reference_pii_level_defaults_to_hidden() -> None:
    assert subscriber_reference_pii_level({"permissions": []}) == "HIDDEN"


def test_subscriber_reference_pii_level_uses_most_restrictive_permission() -> None:
    assert subscriber_reference_pii_level(
        {
            "permissions": [
                "subscriber_reference.pii.full",
                "subscriber_reference.pii.masked",
                "subscriber_reference.pii.hidden",
            ]
        }
    ) == "HIDDEN"


def test_system_manager_can_use_full_display() -> None:
    assert subscriber_reference_pii_level({"permissions": ["users.manage"]}) == "FULL"


def test_admin_all_permissions_still_resolves_to_full() -> None:
    assert subscriber_reference_pii_level(
        {
            "permissions": [
                "users.manage",
                "subscriber_reference.pii.full",
                "subscriber_reference.pii.masked",
                "subscriber_reference.pii.hidden",
            ]
        }
    ) == "FULL"
