#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import hmac
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from scripts.lib.db.mysql import Cursor


HASH_ALGORITHM = "pbkdf2_sha256"
HASH_ITERATIONS = 260000


@dataclass(frozen=True)
class LoginResult:
    success: bool
    failure_reason: str | None
    app_user_id: int | None = None
    employee_no: str | None = None
    session_token: str | None = None
    permissions: tuple[str, ...] = ()


def hash_password(password: str, *, iterations: int = HASH_ITERATIONS) -> str:
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        bytes.fromhex(salt),
        iterations,
    ).hex()
    return f"{HASH_ALGORITHM}${iterations}${salt}${digest}"


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        algorithm, iterations_text, salt, expected = stored_hash.split("$", 3)
        iterations = int(iterations_text)
    except ValueError:
        return False

    if algorithm != HASH_ALGORITHM:
        return False

    actual = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        bytes.fromhex(salt),
        iterations,
    ).hex()
    return hmac.compare_digest(actual, expected)


def _schema(schema_name: str) -> str:
    if not schema_name.replace("_", "").isalnum():
        raise ValueError(f"invalid schema name: {schema_name}")
    return f"`{schema_name}`"


def get_user_by_employee_no(cur: Cursor, *, app_db: str, employee_no: str) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT *
        FROM {_schema(app_db)}.`app_users`
        WHERE `employee_no` = %s
        """,
        (employee_no,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def get_user_permissions(cur: Cursor, *, app_db: str, app_user_id: int) -> tuple[str, ...]:
    cur.execute(
        f"""
        SELECT DISTINCT p.`permission_code`
        FROM {_schema(app_db)}.`app_user_roles` ur
        JOIN {_schema(app_db)}.`app_roles` r
          ON r.`app_role_id` = ur.`app_role_id`
         AND r.`is_active` = 1
        JOIN {_schema(app_db)}.`app_role_permissions` rp
          ON rp.`app_role_id` = r.`app_role_id`
         AND rp.`is_allowed` = 1
        JOIN {_schema(app_db)}.`app_permissions` p
          ON p.`app_permission_id` = rp.`app_permission_id`
         AND p.`is_active` = 1
        WHERE ur.`app_user_id` = %s
          AND ur.`is_active` = 1
          AND (ur.`valid_from` IS NULL OR ur.`valid_from` <= CURRENT_DATE())
          AND (ur.`valid_to` IS NULL OR ur.`valid_to` >= CURRENT_DATE())
        ORDER BY p.`permission_code`
        """,
        (app_user_id,),
    )
    return tuple(str(row["permission_code"]) for row in cur.fetchall())


def is_ip_allowed(cur: Cursor, *, app_db: str, app_user_id: int, client_ip: str | None) -> bool:
    cur.execute(
        f"""
        SELECT `allowed_ip`
        FROM {_schema(app_db)}.`app_user_allowed_ips`
        WHERE `app_user_id` = %s
          AND `is_active` = 1
        """,
        (app_user_id,),
    )
    rows = cur.fetchall()
    if not rows:
        return True
    if not client_ip:
        return False
    return any(str(row["allowed_ip"]) == client_ip for row in rows)


def record_login_attempt(
    cur: Cursor,
    *,
    app_db: str,
    employee_no: str,
    app_user_id: int | None,
    success: bool,
    failure_reason: str | None,
    client_ip: str | None,
    user_agent: str | None,
) -> None:
    cur.execute(
        f"""
        INSERT INTO {_schema(app_db)}.`app_login_attempts` (
          `employee_no`,
          `app_user_id`,
          `success`,
          `failure_reason`,
          `client_ip`,
          `user_agent`
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (employee_no, app_user_id, int(success), failure_reason, client_ip, user_agent),
    )


def create_session(
    cur: Cursor,
    *,
    app_db: str,
    app_user_id: int,
    client_ip: str | None,
    user_agent: str | None,
    lifetime_hours: int = 12,
) -> str:
    token = secrets.token_urlsafe(32)
    token_sha256 = hashlib.sha256(token.encode("utf-8")).hexdigest()
    expires_at = datetime.now() + timedelta(hours=lifetime_hours)
    cur.execute(
        f"""
        INSERT INTO {_schema(app_db)}.`app_sessions` (
          `session_token_sha256`,
          `app_user_id`,
          `client_ip`,
          `user_agent`,
          `expires_at`,
          `last_seen_at`
        )
        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP(3))
        """,
        (token_sha256, app_user_id, client_ip, user_agent, expires_at),
    )
    return token


def authenticate_user(
    cur: Cursor,
    *,
    app_db: str,
    employee_no: str,
    password: str,
    client_ip: str | None,
    user_agent: str | None = None,
) -> LoginResult:
    user = get_user_by_employee_no(cur, app_db=app_db, employee_no=employee_no)
    if not user:
        record_login_attempt(
            cur,
            app_db=app_db,
            employee_no=employee_no,
            app_user_id=None,
            success=False,
            failure_reason="USER_NOT_FOUND",
            client_ip=client_ip,
            user_agent=user_agent,
        )
        return LoginResult(success=False, failure_reason="USER_NOT_FOUND", employee_no=employee_no)

    app_user_id = int(user["app_user_id"])
    if not int(user["is_active"]):
        record_login_attempt(
            cur,
            app_db=app_db,
            employee_no=employee_no,
            app_user_id=app_user_id,
            success=False,
            failure_reason="USER_INACTIVE",
            client_ip=client_ip,
            user_agent=user_agent,
        )
        return LoginResult(False, "USER_INACTIVE", app_user_id=app_user_id, employee_no=employee_no)

    locked_until = user.get("locked_until")
    if locked_until and locked_until > datetime.now():
        record_login_attempt(
            cur,
            app_db=app_db,
            employee_no=employee_no,
            app_user_id=app_user_id,
            success=False,
            failure_reason="USER_LOCKED",
            client_ip=client_ip,
            user_agent=user_agent,
        )
        return LoginResult(False, "USER_LOCKED", app_user_id=app_user_id, employee_no=employee_no)

    if not is_ip_allowed(cur, app_db=app_db, app_user_id=app_user_id, client_ip=client_ip):
        record_login_attempt(
            cur,
            app_db=app_db,
            employee_no=employee_no,
            app_user_id=app_user_id,
            success=False,
            failure_reason="IP_NOT_ALLOWED",
            client_ip=client_ip,
            user_agent=user_agent,
        )
        return LoginResult(False, "IP_NOT_ALLOWED", app_user_id=app_user_id, employee_no=employee_no)

    if not verify_password(password, str(user["password_hash"])):
        cur.execute(
            f"""
            UPDATE {_schema(app_db)}.`app_users`
            SET `failed_login_count` = `failed_login_count` + 1
            WHERE `app_user_id` = %s
            """,
            (app_user_id,),
        )
        record_login_attempt(
            cur,
            app_db=app_db,
            employee_no=employee_no,
            app_user_id=app_user_id,
            success=False,
            failure_reason="PASSWORD_MISMATCH",
            client_ip=client_ip,
            user_agent=user_agent,
        )
        return LoginResult(False, "PASSWORD_MISMATCH", app_user_id=app_user_id, employee_no=employee_no)

    permissions = get_user_permissions(cur, app_db=app_db, app_user_id=app_user_id)
    session_token = create_session(
        cur,
        app_db=app_db,
        app_user_id=app_user_id,
        client_ip=client_ip,
        user_agent=user_agent,
    )
    cur.execute(
        f"""
        UPDATE {_schema(app_db)}.`app_users`
        SET
          `failed_login_count` = 0,
          `last_login_at` = CURRENT_TIMESTAMP(3),
          `last_login_ip` = %s
        WHERE `app_user_id` = %s
        """,
        (client_ip, app_user_id),
    )
    record_login_attempt(
        cur,
        app_db=app_db,
        employee_no=employee_no,
        app_user_id=app_user_id,
        success=True,
        failure_reason=None,
        client_ip=client_ip,
        user_agent=user_agent,
    )
    return LoginResult(
        True,
        None,
        app_user_id=app_user_id,
        employee_no=employee_no,
        session_token=session_token,
        permissions=permissions,
    )
