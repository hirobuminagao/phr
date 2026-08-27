from decimal import Decimal
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.csv_mapping_lab.analyze_csv import candidate_for_header, normalize_header


def test_normalize_header_ignores_leading_decoration_marks() -> None:
    assert normalize_header("●身長") == normalize_header("身長")
    assert normalize_header("▼ ＨＤＬ－ｃ") == normalize_header("HDL-c")
    assert normalize_header("※問診1-1血圧を下げる薬") == normalize_header("問診1-1血圧を下げる薬")


def test_candidate_for_header_uses_decoration_insensitive_header() -> None:
    assert candidate_for_header("●身長") == (
        "EXAM_ITEM_VALUE",
        "9N001000000000001",
        None,
        Decimal("0.9500"),
    )
    assert candidate_for_header("▼体重") == (
        "EXAM_ITEM_VALUE",
        "9N006000000000001",
        None,
        Decimal("0.9500"),
    )
