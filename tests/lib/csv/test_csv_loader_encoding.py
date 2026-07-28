from pathlib import Path

from scripts.lib.csv.csv_loader import load_csv_result
from scripts.lib.csv.exam_result_format_matcher import load_csv_matching_registered_header


def write_csv(path: Path, text: str, encoding: str) -> None:
    path.write_bytes(text.encode(encoding))


def test_quote_char_is_used_for_parsing(tmp_path: Path) -> None:
    csv_path = tmp_path / "quoted.csv"
    write_csv(csv_path, '"項目,名",値\n"血糖,空腹時",100\n', "utf-8")

    result = load_csv_result(csv_path.as_posix(), encoding="utf-8", quote_char='"')

    assert result.header_set.header_rows == [["項目,名", "値"]]
    assert result.rows == [["血糖,空腹時", "100"]]


def test_common_encoding_fallback_requires_registered_header_match(tmp_path: Path) -> None:
    cp932_path = tmp_path / "registered.csv"
    utf8_path = tmp_path / "received.csv"
    text = "氏名,健診日\n山田太郎,2026-07-01\n"
    write_csv(cp932_path, text, "cp932")
    write_csv(utf8_path, text, "utf-8-sig")
    registered = load_csv_result(cp932_path.as_posix(), encoding="cp932")
    fmt = {
        "character_encoding": "CP932",
        "encoding_fallback_policy": "ALLOW_COMMON_ENCODINGS",
        "delimiter": ",",
        "quote_char": '"',
        "data_start_row_no": 2,
        "header_sha256": registered.header_set.header_sha256,
    }

    result, actual_header_sha256 = load_csv_matching_registered_header(utf8_path.as_posix(), fmt)

    assert result is not None
    assert result.encoding == "utf-8-sig"
    assert actual_header_sha256 == fmt["header_sha256"]


def test_strict_encoding_does_not_fallback(tmp_path: Path) -> None:
    cp932_path = tmp_path / "registered.csv"
    utf8_path = tmp_path / "received.csv"
    text = "氏名,健診日\n山田太郎,2026-07-01\n"
    write_csv(cp932_path, text, "cp932")
    write_csv(utf8_path, text, "utf-8-sig")
    registered = load_csv_result(cp932_path.as_posix(), encoding="cp932")
    fmt = {
        "character_encoding": "CP932",
        "encoding_fallback_policy": "STRICT",
        "delimiter": ",",
        "quote_char": '"',
        "data_start_row_no": 2,
        "header_sha256": registered.header_set.header_sha256,
    }

    result, _ = load_csv_matching_registered_header(utf8_path.as_posix(), fmt)

    assert result is None
