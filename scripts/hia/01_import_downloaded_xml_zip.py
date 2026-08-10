#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Mapping

if __name__ == "__main__" and __package__ is None:
    project_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(project_root))

from scripts.hia.script_lib.hia_download_importer import (  # noqa: E402
    HiaDownloadImportConfig,
    import_hia_download_zips,
)
from scripts.hia.script_lib.config_loader import config_bool, config_value, load_yaml_config  # noqa: E402
from scripts.lib.db.config import load_mysql_base_params  # noqa: E402
from scripts.lib.db.mysql import connect_ctx, dict_cursor  # noqa: E402
from scripts.lib.etl.metrics import RunMetrics  # noqa: E402
from scripts.lib.etl.runs import finish_run, start_run  # noqa: E402


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
HIA_EXPORT_DIR = DATA_DIR / "hia_export"
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config" / "fund_delivery.yml"


def _path_from_config(value: Any, default: Path) -> Path:
    if value in (None, ""):
        return default
    path = Path(str(value)).expanduser()
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def parse_args() -> argparse.Namespace:
    bootstrap = argparse.ArgumentParser(add_help=False)
    bootstrap.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    known, _ = bootstrap.parse_known_args()
    data = load_yaml_config(Path(known.config))
    section = data.get("download_import") or {}
    if not isinstance(section, Mapping):
        raise ValueError("download_import must be a mapping in fund_delivery.yml")
    parser = argparse.ArgumentParser(
        description="Import HIA downloaded XML ZIP files into health_exam_result ledgers.",
        parents=[bootstrap],
    )
    parser.add_argument("--database", default=config_value(data, "database", "health_exam_result"))
    event_id = config_value(data, "event_id", None)
    parser.add_argument("--event-id", type=int, default=None if event_id in (None, "") else int(event_id))
    parser.add_argument("--input-zip-dir", type=Path, default=_path_from_config(section.get("input_zip_dir"), HIA_EXPORT_DIR / "input_zip"))
    parser.add_argument("--archive-zip-dir", type=Path, default=_path_from_config(section.get("archive_zip_dir"), HIA_EXPORT_DIR / "archive_zip"))
    parser.add_argument("--work-dir", type=Path, default=_path_from_config(section.get("work_dir"), HIA_EXPORT_DIR / "work"))
    parser.add_argument(
        "--archive-mode",
        choices=("copy", "move", "none"),
        default=str(config_value(section, "archive_mode", "copy")),
        help="copy keeps the source ZIP in input_zip, move mimics the old workflow.",
    )
    parser.add_argument("--dry-run", action="store_true", default=config_bool(section, "dry_run", False))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = HiaDownloadImportConfig(
        project_root=PROJECT_ROOT,
        input_zip_dir=args.input_zip_dir,
        archive_zip_dir=args.archive_zip_dir,
        work_dir=args.work_dir,
        event_id=args.event_id,
        archive_mode=args.archive_mode,
        dry_run=args.dry_run,
    )

    mysql_params = load_mysql_base_params()
    with connect_ctx(mysql_params, database=args.database, autocommit=False) as conn:
        cur = dict_cursor(conn)
        run_id = start_run(
            cur,
            phase="HIA_IMPORT_DOWNLOADED_XML_ZIP",
            source="HIA",
            db_schema=args.database,
            db_path=None,
            input_base=str(config.input_zip_dir),
            input_file=None,
            insurer_number=None,
            dry_run=args.dry_run,
            limit_rows=None,
        )

        try:
            summary = import_hia_download_zips(cur, config=config, run_id=run_id)
            metrics = RunMetrics()
            metrics.files = summary.files_seen
            metrics.rows_seen = summary.xml_seen
            metrics.rows_inserted = summary.xml_inserted + summary.person_years_upserted + summary.person_xml_events_upserted
            metrics.rows_updated = summary.xml_updated
            metrics.rows_skipped = summary.files_skipped
            metrics.errors = summary.errors

            if args.dry_run:
                conn.rollback()
                status_override = "success"
            else:
                status_override = None

            finish_run(
                cur,
                run_id,
                metrics,
                status_override=status_override,
                extra_notes=(
                    f"files_imported={summary.files_imported} "
                    f"files_skipped={summary.files_skipped} "
                    f"xml_inserted={summary.xml_inserted} "
                    f"xml_updated={summary.xml_updated} "
                    f"person_years_upserted={summary.person_years_upserted} "
                    f"person_xml_events_upserted={summary.person_xml_events_upserted}"
                ),
            )
            if args.dry_run:
                conn.rollback()
            else:
                conn.commit()
        except Exception:
            conn.rollback()
            raise

    print(
        "hia_import_downloaded_xml_zip "
        f"files={summary.files_seen} imported={summary.files_imported} skipped={summary.files_skipped} "
        f"xml_seen={summary.xml_seen} xml_inserted={summary.xml_inserted} xml_updated={summary.xml_updated} "
        f"errors={summary.errors} dry_run={1 if args.dry_run else 0}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
