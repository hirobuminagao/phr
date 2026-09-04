from pathlib import Path
from types import SimpleNamespace

import pytest

from apps.health_exam_admin.main import (
    build_hia_dashboard_import_command,
    normalize_hia_dashboard_insurer_number,
    run_hia_dashboard_csv_import,
)


def test_dashboard_insurer_number_is_normalized_to_eight_digits() -> None:
    assert normalize_hia_dashboard_insurer_number("6139463") == "06139463"
    assert normalize_hia_dashboard_insurer_number("06139463") == "06139463"


@pytest.mark.parametrize("value", [None, "", "00000000", "保険者なし"])
def test_dashboard_insurer_number_rejects_missing_or_zero(value: object) -> None:
    with pytest.raises(ValueError, match="有効な保険者番号"):
        normalize_hia_dashboard_insurer_number(value)


def test_dashboard_import_command_uses_isolated_insurer_directory() -> None:
    command = build_hia_dashboard_import_command(
        input_dir=Path("/tmp/import/06139463"),
        dry_run=True,
        partial_import=True,
    )

    assert command[1].endswith("scripts/hia/import_dashboard_csv.py")
    assert command[command.index("--input") + 1] == "/tmp/import/06139463"
    assert "--dry-run" in command
    assert "--partial-import" in command


def test_full_dashboard_import_omits_partial_flag() -> None:
    command = build_hia_dashboard_import_command(
        input_dir=Path("/tmp/import/06139463"),
        dry_run=False,
        partial_import=False,
    )

    assert "--dry-run" not in command
    assert "--partial-import" not in command


def test_dashboard_import_stages_one_file_under_insurer_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    upload_path = tmp_path / "dashboard.csv"
    upload_path.write_text("加入者ID,状態\n1,未予約\n", encoding="utf-8")
    observed: dict[str, object] = {}

    def fake_run(command: list[str], **kwargs: object) -> SimpleNamespace:
        input_dir = Path(command[command.index("--input") + 1])
        observed["input_dir_name"] = input_dir.name
        observed["files"] = [path.name for path in input_dir.iterdir()]
        observed["kwargs"] = kwargs
        return SimpleNamespace(returncode=0, stdout="rows=1", stderr="")

    monkeypatch.setattr("apps.health_exam_admin.main.subprocess.run", fake_run)

    result = run_hia_dashboard_csv_import(
        upload_path=upload_path,
        insurer_number="06139463",
        dry_run=True,
        partial_import=False,
    )

    assert result["ok"] is True
    assert observed["input_dir_name"] == "06139463"
    assert observed["files"] == ["dashboard.csv"]
    assert observed["kwargs"]["cwd"].name == "phr"
