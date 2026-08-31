from unittest.mock import Mock, patch

from apps.health_exam_admin.main import run_hia_xml_export_from_list


@patch("apps.health_exam_admin.main.subprocess.run")
def test_official_export_can_include_already_exported_cases(run: Mock) -> None:
    run.return_value = Mock(returncode=0, stdout="[OK] count=1", stderr="")

    run_hia_xml_export_from_list(
        xml_export_list_id=72,
        output_mode="official",
        include_exported=True,
    )

    command = run.call_args.args[0]
    assert "--include-exported" in command
    assert command[command.index("--xml-export-list-id") + 1] == "72"


@patch("apps.health_exam_admin.main.subprocess.run")
def test_official_export_does_not_reexport_without_explicit_choice(run: Mock) -> None:
    run.return_value = Mock(returncode=0, stdout="[OK] count=1", stderr="")

    run_hia_xml_export_from_list(xml_export_list_id=72, output_mode="official")

    assert "--include-exported" not in run.call_args.args[0]
