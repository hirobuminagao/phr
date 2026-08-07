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
from scripts.phr_app.script_lib.app_auth import hash_password


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create or update a PHR app user.")
    parser.add_argument("--app-db", default="phr_app")
    parser.add_argument("--db-prefix", default="PHR_DB_")
    parser.add_argument("--employee-no", required=True)
    parser.add_argument("--display-name", required=True)
    parser.add_argument("--display-name-kana")
    parser.add_argument("--department-name")
    parser.add_argument("--email")
    parser.add_argument("--role-code", action="append", default=["VIEWER"])
    parser.add_argument("--allowed-ip", action="append", default=[])
    parser.add_argument("--password")
    parser.add_argument("--inactive", action="store_true")
    parser.add_argument("--no-must-change-password", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    password = args.password or getpass.getpass("password: ")
    if not password:
        raise RuntimeError("password is required")

    params = load_mysql_base_params(args.db_prefix)
    with connect_ctx(params, database=args.app_db, autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            cur.execute(
                """
                INSERT INTO app_users (
                  employee_no,
                  display_name,
                  display_name_kana,
                  department_name,
                  email,
                  password_hash,
                  password_hash_algorithm,
                  password_changed_at,
                  must_change_password,
                  is_active
                )
                VALUES (%s, %s, %s, %s, %s, %s, 'pbkdf2_sha256', CURRENT_TIMESTAMP(3), %s, %s)
                ON DUPLICATE KEY UPDATE
                  display_name = VALUES(display_name),
                  display_name_kana = VALUES(display_name_kana),
                  department_name = VALUES(department_name),
                  email = VALUES(email),
                  password_hash = VALUES(password_hash),
                  password_hash_algorithm = VALUES(password_hash_algorithm),
                  password_changed_at = VALUES(password_changed_at),
                  must_change_password = VALUES(must_change_password),
                  is_active = VALUES(is_active)
                """,
                (
                    args.employee_no,
                    args.display_name,
                    args.display_name_kana,
                    args.department_name,
                    args.email,
                    hash_password(password),
                    int(not args.no_must_change_password),
                    int(not args.inactive),
                ),
            )
            cur.execute("SELECT app_user_id FROM app_users WHERE employee_no = %s", (args.employee_no,))
            user_row = cur.fetchone()
            if not user_row:
                raise RuntimeError("created user was not found")
            app_user_id = int(user_row["app_user_id"])

            for role_code in args.role_code:
                cur.execute("SELECT app_role_id FROM app_roles WHERE role_code = %s", (role_code,))
                role_row = cur.fetchone()
                if not role_row:
                    raise RuntimeError(f"role not found: {role_code}")
                cur.execute(
                    """
                    INSERT INTO app_user_roles (app_user_id, app_role_id, valid_from, is_active)
                    VALUES (%s, %s, CURRENT_DATE(), 1)
                    ON DUPLICATE KEY UPDATE is_active = 1
                    """,
                    (app_user_id, int(role_row["app_role_id"])),
                )

            for allowed_ip in args.allowed_ip:
                cur.execute(
                    """
                    INSERT INTO app_user_allowed_ips (app_user_id, allowed_ip, is_active)
                    VALUES (%s, %s, 1)
                    ON DUPLICATE KEY UPDATE is_active = 1
                    """,
                    (app_user_id, allowed_ip),
                )

            conn.commit()
        except Exception:
            conn.rollback()
            raise

    print(f"app_user_id={app_user_id} employee_no={args.employee_no} roles={','.join(args.role_code)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
