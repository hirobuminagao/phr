"""Read case ID manifests passed between scoped processing steps."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_case_id_file(path_text: str | None, *, event_id: int) -> tuple[int, ...]:
    if not path_text:
        return ()
    path = Path(path_text).expanduser()
    payload: Any = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or int(payload.get("event_id") or 0) != event_id:
        raise ValueError("case ID file event_id does not match")
    raw_ids = payload.get("case_ids")
    if not isinstance(raw_ids, list):
        raise ValueError("case ID file must contain case_ids array")
    case_ids = tuple(sorted({int(value) for value in raw_ids if int(value) > 0}))
    if len(case_ids) != len(raw_ids):
        raise ValueError("case ID file contains duplicate or invalid IDs")
    if not case_ids:
        raise ValueError("case ID file contains no target cases")
    return case_ids
