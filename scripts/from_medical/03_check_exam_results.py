#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Compatibility entry point for Article 44 exam result checks.

Normal operations should use:
- 03_00_check_imported_exam_ledgers.py
- 03_04_check_exam_export_cases.py
"""

from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.from_medical.script_lib.check_exam_results import main as check_main


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("03_check_exam_results.py is a compatibility entry point.")
        print("Use scripts/from_medical/03_00_check_imported_exam_ledgers.py for imported exam_ledgers.")
        print("Use scripts/from_medical/03_04_check_exam_export_cases.py for final export cases.")
        raise SystemExit(2)
    raise SystemExit(check_main())
