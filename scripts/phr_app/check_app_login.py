#!/usr/bin/env python3
from __future__ import annotations

import argparse
import getpass
from pathlib import Path
import sys

SCRIPT_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = SCRIPT_ROOT.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.phr_app.script_lib.app_auth import authenticate_user


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check PHR app login and create a session.")
    parser.add_argument("--app-db", default="phr_app")
    parser.add_argument("--db-prefix", default="PHR_DB_")
    parser.add_argument("--employee-no", required=True)
    parser.add_argument("--client-ip")
    parser.add_argument("--user-agent", default="phr_app_cli")
    parser.add_argument("--password")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    password = args.password or getpass.getpass("password: ")
    params = load_mysql_base_params(args.db_prefix)

    with connect_ctx(params, database=args.app_db, autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            result = authenticate_user(
                cur,
                app_db=args.app_db,
                employee_no=args.employee_no,
                password=password,
                client_ip=args.client_ip,
                user_agent=args.user_agent,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    if not result.success:
        print(f"login=NG reason={result.failure_reason}")
        return 1

    print(f"login=OK app_user_id={result.app_user_id}")
    print("permissions=" + ",".join(result.permissions))
    print(f"session_token={result.session_token}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
