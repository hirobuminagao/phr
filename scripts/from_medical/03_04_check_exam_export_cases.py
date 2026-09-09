#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Check person-level export cases after adopted values are built."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.from_medical.script_lib.check_exam_results import CheckConfig
from scripts.from_medical.script_lib.check_exam_results import DEV_PHR_DB
from scripts.from_medical.script_lib.check_exam_results import HEALTH_EXAM_RESULT_DB
from scripts.from_medical.script_lib.check_exam_results import LEDGER_TYPE_EXPORT_CASE
from scripts.from_medical.script_lib.check_exam_results import run
from scripts.from_medical.script_lib.case_id_file import load_case_id_file


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check person-level exam_export_cases. This is the final Article 44 check before XML export."
    )
    parser.add_argument("--event-id", type=int, default=2)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--case-id", type=int, action="append", default=[])
    parser.add_argument("--case-id-file", default=None)
    parser.add_argument("--db-prefix", default="PHR_DB_")
    parser.add_argument("--health-db", default=HEALTH_EXAM_RESULT_DB)
    parser.add_argument("--dev-db", default=DEV_PHR_DB)
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    file_case_ids = load_case_id_file(args.case_id_file, event_id=args.event_id)
    explicit_case_ids = tuple(args.case_id or ())
    if file_case_ids and explicit_case_ids:
        raise ValueError("case-id and case-id-file cannot be combined")
    config = CheckConfig(
        event_id=args.event_id,
        health_db=args.health_db,
        dev_db=args.dev_db,
        dry_run=bool(args.dry_run),
        limit=int(args.limit or 0),
        verbose=bool(args.verbose),
        ledger_type=LEDGER_TYPE_EXPORT_CASE,
        case_ids=file_case_ids or explicit_case_ids,
    )
    summary = run(config, db_prefix=args.db_prefix)
    summary.print()
    return 0 if summary.errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
