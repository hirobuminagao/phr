

from __future__ import annotations


def resolve_shg_role(report_code: str) -> str | None:
    """SHG XML の報告区分コードから集約用 role を返す。

    現時点の対応:
    - 21: initial
    - 22: final
    """
    code = (report_code or "").strip()
    if code == "21":
        return "initial"
    if code == "22":
        return "final"
    return None