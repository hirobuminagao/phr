from __future__ import annotations

import os
import secrets
import string
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.phr_app.script_lib.app_auth import (
    authenticate_user,
    get_authenticated_session,
    hash_password,
    revoke_session,
    verify_password,
)


APP_ROOT = Path(__file__).resolve().parent
SESSION_COOKIE_NAME = "phr_app_session"
LOGIN_ERROR_MESSAGES = {
    "USER_NOT_FOUND": "社員番号またはパスワードが違います。",
    "PASSWORD_MISMATCH": "社員番号またはパスワードが違います。",
    "USER_INACTIVE": "このアカウントは無効です。",
    "USER_LOCKED": "このアカウントは一時的にロックされています。",
    "IP_NOT_ALLOWED": "この端末からのログインは許可されていません。",
    "USER_NOT_APPROVED": "承認待ちです。管理者の承認後にログインできます。",
}

app = FastAPI(title="PHR Health Exam Admin")
app.mount("/static", StaticFiles(directory=APP_ROOT / "static"), name="static")
templates = Jinja2Templates(directory=APP_ROOT / "templates")


def approval_status_label(status: str | None, *, is_active: bool = True) -> str:
    labels = {
        "APPROVED": "有効",
        "PENDING": "承認待ち",
        "REJECTED": "却下",
    }
    if not is_active:
        return "無効"
    return labels.get(status or "APPROVED", status or "有効")


templates.env.globals["approval_status_label"] = approval_status_label


def app_db() -> str:
    return os.getenv("PHR_APP_DB", "phr_app")


def db_prefix() -> str:
    return os.getenv("PHR_DB_PREFIX", "PHR_DB_")


def client_ip(request: Request) -> str | None:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",", 1)[0].strip()
    if request.client:
        return request.client.host
    return None


async def read_form(request: Request) -> dict[str, str]:
    body = await request.body()
    parsed = parse_qs(body.decode("utf-8"), keep_blank_values=True)
    return {key: values[-1] if values else "" for key, values in parsed.items()}


def current_user(request: Request) -> dict[str, Any] | None:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            user = get_authenticated_session(cur, app_db=app_db(), session_token=token)
            conn.commit()
            return user
        except Exception:
            conn.rollback()
            raise


def has_permission(user: dict[str, Any], permission_code: str) -> bool:
    return permission_code in set(user.get("permissions") or ())


def require_user(request: Request) -> dict[str, Any] | RedirectResponse:
    user = current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    return user


def generate_temporary_password(length: int = 12) -> str:
    alphabet = string.ascii_letters + string.digits
    while True:
        password = "".join(secrets.choice(alphabet) for _ in range(length))
        if (
            any(ch.islower() for ch in password)
            and any(ch.isupper() for ch in password)
            and any(ch.isdigit() for ch in password)
        ):
            return password


def load_admin_user_rows(cur: Any, *, filters: dict[str, str] | None = None) -> list[dict[str, Any]]:
    filters = filters or {}
    where_parts: list[str] = []
    params: list[Any] = []
    status = filters.get("status", "").strip()
    role_code = filters.get("role_code", "").strip()
    query = filters.get("q", "").strip()
    if status == "active":
        where_parts.append("u.approval_status = 'APPROVED' AND u.is_active = 1")
    elif status == "pending":
        where_parts.append("u.approval_status = 'PENDING'")
    elif status == "inactive":
        where_parts.append("u.is_active = 0")
    elif status == "rejected":
        where_parts.append("u.approval_status = 'REJECTED'")
    if role_code:
        where_parts.append(
            """
            EXISTS (
              SELECT 1
              FROM app_user_roles fur
              JOIN app_roles fr
                ON fr.app_role_id = fur.app_role_id
               AND fr.is_active = 1
               AND fr.role_code = %s
              WHERE fur.app_user_id = u.app_user_id
                AND fur.is_active = 1
                AND (fur.valid_from IS NULL OR fur.valid_from <= CURRENT_DATE())
                AND (fur.valid_to IS NULL OR fur.valid_to >= CURRENT_DATE())
            )
            """
        )
        params.append(role_code)
    if query:
        like = f"%{query}%"
        where_parts.append(
            """
            (
              u.employee_no LIKE %s
              OR u.display_name LIKE %s
              OR u.display_name_kana LIKE %s
            )
            """
        )
        params.extend([like, like, like])
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    cur.execute(
        f"""
        SELECT
          u.app_user_id,
          u.employee_no,
          u.display_name,
          u.display_name_kana,
          u.department_name,
          u.email,
          u.approval_status,
          u.is_active,
          u.approval_requested_at,
          u.approved_at,
          u.must_change_password,
          u.last_login_at,
          GROUP_CONCAT(r.role_code ORDER BY r.role_code SEPARATOR ',') AS role_codes,
          GROUP_CONCAT(r.role_name ORDER BY r.role_code SEPARATOR ', ') AS role_names
        FROM app_users u
        LEFT JOIN app_user_roles ur
          ON ur.app_user_id = u.app_user_id
         AND ur.is_active = 1
         AND (ur.valid_from IS NULL OR ur.valid_from <= CURRENT_DATE())
         AND (ur.valid_to IS NULL OR ur.valid_to >= CURRENT_DATE())
        LEFT JOIN app_roles r
          ON r.app_role_id = ur.app_role_id
         AND r.is_active = 1
        {where_sql}
        GROUP BY
          u.app_user_id,
          u.employee_no,
          u.display_name,
          u.display_name_kana,
          u.department_name,
          u.email,
          u.approval_status,
          u.is_active,
          u.approval_requested_at,
          u.approved_at,
          u.must_change_password,
          u.last_login_at
        ORDER BY
          CASE u.approval_status WHEN 'PENDING' THEN 0 WHEN 'APPROVED' THEN 1 ELSE 2 END,
          u.app_user_id
        """,
        tuple(params),
    )
    return [dict(row) for row in cur.fetchall()]


def load_manageable_roles(cur: Any) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT app_role_id, role_code, role_name
        FROM app_roles
        WHERE is_active = 1
          AND role_code IN ('VIEWER', 'EDITOR', 'ADMIN')
        ORDER BY FIELD(role_code, 'VIEWER', 'EDITOR', 'ADMIN'), role_code
        """
    )
    return [dict(row) for row in cur.fetchall()]


def admin_user_filters_from_request(request: Request) -> dict[str, str]:
    return {
        "q": (request.query_params.get("q") or "").strip(),
        "status": (request.query_params.get("status") or "").strip(),
        "role_code": (request.query_params.get("role_code") or "").strip(),
    }


def load_user_search_suggestions(cur: Any) -> list[str]:
    cur.execute(
        """
        SELECT employee_no, display_name, display_name_kana
        FROM app_users
        ORDER BY app_user_id
        LIMIT 300
        """
    )
    suggestions: list[str] = []
    seen: set[str] = set()
    for row in cur.fetchall():
        for key in ("employee_no", "display_name", "display_name_kana"):
            value = str(row.get(key) or "").strip()
            if value and value not in seen:
                suggestions.append(value)
                seen.add(value)
    return suggestions


def load_admin_page_data(cur: Any, *, filters: dict[str, str] | None = None) -> dict[str, Any]:
    return {
        "users": load_admin_user_rows(cur, filters=filters),
        "roles": load_manageable_roles(cur),
        "suggestions": load_user_search_suggestions(cur),
        "filters": filters or {},
    }


def load_issue_page_data(cur: Any, *, issue_form: dict[str, str] | None = None) -> dict[str, Any]:
    return {
        "roles": load_manageable_roles(cur),
        "issue_form": issue_form or {},
    }


def load_admin_user_detail(cur: Any, *, app_user_id: int) -> dict[str, Any] | None:
    rows = load_admin_user_rows(cur, filters={})
    for row in rows:
        if int(row["app_user_id"]) == app_user_id:
            return row
    return None


def admin_user_form_values(row: dict[str, Any]) -> dict[str, str]:
    role_codes = str(row.get("role_codes") or "")
    first_role = role_codes.split(",", 1)[0] if role_codes else "VIEWER"
    return {
        "employee_no": str(row.get("employee_no") or ""),
        "display_name": str(row.get("display_name") or ""),
        "display_name_kana": str(row.get("display_name_kana") or ""),
        "department_name": str(row.get("department_name") or ""),
        "email": str(row.get("email") or ""),
        "role_code": first_role or "VIEWER",
    }


def count_remaining_active_user_managers(cur: Any, *, excluding_app_user_id: int) -> int:
    cur.execute(
        """
        SELECT COUNT(DISTINCT u.app_user_id) AS cnt
        FROM app_users u
        JOIN app_user_roles ur
          ON ur.app_user_id = u.app_user_id
         AND ur.is_active = 1
         AND (ur.valid_from IS NULL OR ur.valid_from <= CURRENT_DATE())
         AND (ur.valid_to IS NULL OR ur.valid_to >= CURRENT_DATE())
        JOIN app_roles r
          ON r.app_role_id = ur.app_role_id
         AND r.is_active = 1
        JOIN app_role_permissions rp
          ON rp.app_role_id = r.app_role_id
         AND rp.is_allowed = 1
        JOIN app_permissions p
          ON p.app_permission_id = rp.app_permission_id
         AND p.is_active = 1
         AND p.permission_code = 'users.manage'
        WHERE u.app_user_id <> %s
          AND u.is_active = 1
          AND u.approval_status = 'APPROVED'
        """,
        (excluding_app_user_id,),
    )
    row = cur.fetchone() or {}
    return int(row.get("cnt") or 0)


def role_has_user_manage(cur: Any, *, role_code: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM app_roles r
        JOIN app_role_permissions rp
          ON rp.app_role_id = r.app_role_id
         AND rp.is_allowed = 1
        JOIN app_permissions p
          ON p.app_permission_id = rp.app_permission_id
         AND p.is_active = 1
         AND p.permission_code = 'users.manage'
        WHERE r.role_code = %s
          AND r.is_active = 1
        """,
        (role_code,),
    )
    row = cur.fetchone() or {}
    return int(row.get("cnt") or 0) > 0


def assign_user_role(cur: Any, *, app_user_id: int, role_code: str, assigned_by_app_user_id: int | None) -> None:
    cur.execute("SELECT app_role_id FROM app_roles WHERE role_code = %s AND is_active = 1", (role_code,))
    role = cur.fetchone()
    if not role:
        return
    cur.execute(
        """
        INSERT INTO app_user_roles (
          app_user_id,
          app_role_id,
          valid_from,
          is_active,
          assigned_by_app_user_id,
          note
        )
        VALUES (%s, %s, CURRENT_DATE(), 1, %s, %s)
        ON DUPLICATE KEY UPDATE
          is_active = VALUES(is_active),
          valid_to = NULL,
          assigned_by_app_user_id = VALUES(assigned_by_app_user_id),
          note = VALUES(note)
        """,
        (app_user_id, int(role["app_role_id"]), assigned_by_app_user_id, f"assigned by {role_code} registration flow"),
    )


def role_exists(cur: Any, *, role_code: str) -> bool:
    cur.execute("SELECT 1 FROM app_roles WHERE role_code = %s AND is_active = 1", (role_code,))
    return cur.fetchone() is not None


def replace_user_role(cur: Any, *, app_user_id: int, role_code: str, assigned_by_app_user_id: int) -> bool:
    cur.execute("SELECT app_role_id FROM app_roles WHERE role_code = %s AND is_active = 1", (role_code,))
    role = cur.fetchone()
    if not role:
        return False
    cur.execute(
        """
        UPDATE app_user_roles
           SET is_active = 0,
               valid_to = CURRENT_DATE()
         WHERE app_user_id = %s
           AND is_active = 1
        """,
        (app_user_id,),
    )
    cur.execute(
        """
        INSERT INTO app_user_roles (
          app_user_id,
          app_role_id,
          valid_from,
          is_active,
          assigned_by_app_user_id,
          note
        )
        VALUES (%s, %s, CURRENT_DATE(), 1, %s, 'role changed from admin screen')
        ON DUPLICATE KEY UPDATE
          is_active = VALUES(is_active),
          valid_to = NULL,
          assigned_by_app_user_id = VALUES(assigned_by_app_user_id),
          note = VALUES(note)
        """,
        (app_user_id, int(role["app_role_id"]), assigned_by_app_user_id),
    )
    return True


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    return templates.TemplateResponse("dashboard.html", {"request": request, "user": user})


@app.get("/login", response_class=HTMLResponse)
def login_form(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.get("/register", response_class=HTMLResponse)
def register_form(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "register.html",
        {"request": request, "message": None, "error": None, "form": {}},
    )


@app.post("/register", response_class=HTMLResponse)
async def register_user(request: Request) -> Response:
    form = await read_form(request)
    employee_no = form.get("employee_no", "").strip()
    display_name = form.get("display_name", "").strip()
    display_name_kana = form.get("display_name_kana", "").strip() or None
    department_name = form.get("department_name", "").strip() or None
    email = form.get("email", "").strip() or None
    password = form.get("password", "")
    password_confirm = form.get("password_confirm", "")
    form_values = {
        "employee_no": employee_no,
        "display_name": display_name,
        "display_name_kana": display_name_kana or "",
        "department_name": department_name or "",
        "email": email or "",
    }

    if not employee_no or not display_name:
        return templates.TemplateResponse(
            "register.html",
            {"request": request, "message": None, "error": "社員番号と氏名は必須です。", "form": form_values},
            status_code=400,
        )
    if len(password) < 8:
        return templates.TemplateResponse(
            "register.html",
            {"request": request, "message": None, "error": "パスワードは8文字以上にしてください。", "form": form_values},
            status_code=400,
        )
    if password != password_confirm:
        return templates.TemplateResponse(
            "register.html",
            {"request": request, "message": None, "error": "パスワードが一致しません。", "form": form_values},
            status_code=400,
        )

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            cur.execute("SELECT app_user_id FROM app_users WHERE employee_no = %s", (employee_no,))
            if cur.fetchone():
                conn.rollback()
                return templates.TemplateResponse(
                    "register.html",
                    {"request": request, "message": None, "error": "この社員番号はすでに登録されています。", "form": form_values},
                    status_code=400,
                )

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
                  approval_status,
                  approval_requested_at,
                  is_active,
                  note
                )
                VALUES (
                  %s, %s, %s, %s, %s,
                  %s, 'pbkdf2_sha256', CURRENT_TIMESTAMP(3), 0,
                  'PENDING', CURRENT_TIMESTAMP(3), 1,
                  'self registration from login screen'
                )
                """,
                (
                    employee_no,
                    display_name,
                    display_name_kana,
                    department_name,
                    email,
                    hash_password(password),
                ),
            )
            app_user_id = int(cur.lastrowid)
            assign_user_role(cur, app_user_id=app_user_id, role_code="VIEWER", assigned_by_app_user_id=None)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    return templates.TemplateResponse(
        "register.html",
        {
            "request": request,
            "message": "登録申請を受け付けました。管理者の承認後にログインできます。",
            "error": None,
            "form": {},
        },
    )


@app.post("/login", response_class=HTMLResponse)
async def login(request: Request) -> Response:
    form = await read_form(request)
    employee_no = form.get("employee_no", "").strip()
    password = form.get("password", "")

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            result = authenticate_user(
                cur,
                app_db=app_db(),
                employee_no=employee_no,
                password=password,
                client_ip=client_ip(request),
                user_agent=request.headers.get("user-agent"),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    if not result.success:
        reason = result.failure_reason or "LOGIN_FAILED"
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": LOGIN_ERROR_MESSAGES.get(reason, "ログインできませんでした。")},
            status_code=401,
        )

    response = RedirectResponse("/", status_code=303)
    response.set_cookie(
        SESSION_COOKIE_NAME,
        result.session_token or "",
        httponly=True,
        samesite="lax",
        secure=False,
        max_age=60 * 60 * 12,
    )
    return response


@app.post("/logout")
def logout(request: Request) -> RedirectResponse:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            revoke_session(cur, app_db=app_db(), session_token=token)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie(SESSION_COOKIE_NAME)
    return response


@app.get("/export-lists", response_class=HTMLResponse)
def export_lists(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    return templates.TemplateResponse("export_lists.html", {"request": request, "user": user})


@app.get("/account", response_class=HTMLResponse)
def account_form(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    return templates.TemplateResponse(
        "account.html",
        {"request": request, "user": user, "message": None, "error": None},
    )


@app.post("/account", response_class=HTMLResponse)
async def update_account(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user

    form = await read_form(request)
    display_name = form.get("display_name", "").strip()
    display_name_kana = form.get("display_name_kana", "").strip() or None
    department_name = form.get("department_name", "").strip() or None
    email = form.get("email", "").strip() or None
    if not display_name:
        return templates.TemplateResponse(
            "account.html",
            {"request": request, "user": user, "message": None, "error": "表示名は必須です。"},
            status_code=400,
        )

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            cur.execute(
                """
                UPDATE app_users
                   SET display_name = %s,
                       display_name_kana = %s,
                       department_name = %s,
                       email = %s
                 WHERE app_user_id = %s
                """,
                (display_name, display_name_kana, department_name, email, int(user["app_user_id"])),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    refreshed = current_user(request) or user
    return templates.TemplateResponse(
        "account.html",
        {"request": request, "user": refreshed, "message": "登録情報を更新しました。", "error": None},
    )


@app.get("/account/password", response_class=HTMLResponse)
def password_form(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    return templates.TemplateResponse(
        "password.html",
        {"request": request, "user": user, "message": None, "error": None},
    )


@app.post("/account/password", response_class=HTMLResponse)
async def update_password(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user

    form = await read_form(request)
    current_password = form.get("current_password", "")
    new_password = form.get("new_password", "")
    new_password_confirm = form.get("new_password_confirm", "")

    if len(new_password) < 8:
        return templates.TemplateResponse(
            "password.html",
            {"request": request, "user": user, "message": None, "error": "新しいパスワードは8文字以上にしてください。"},
            status_code=400,
        )
    if new_password != new_password_confirm:
        return templates.TemplateResponse(
            "password.html",
            {"request": request, "user": user, "message": None, "error": "新しいパスワードが一致しません。"},
            status_code=400,
        )

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            cur.execute(
                "SELECT password_hash FROM app_users WHERE app_user_id = %s",
                (int(user["app_user_id"]),),
            )
            row = cur.fetchone()
            if not row or not verify_password(current_password, str(row["password_hash"])):
                conn.rollback()
                return templates.TemplateResponse(
                    "password.html",
                    {"request": request, "user": user, "message": None, "error": "現在のパスワードが違います。"},
                    status_code=400,
                )

            cur.execute(
                """
                UPDATE app_users
                   SET password_hash = %s,
                       password_hash_algorithm = 'pbkdf2_sha256',
                       password_changed_at = CURRENT_TIMESTAMP(3),
                       must_change_password = 0,
                       failed_login_count = 0,
                       locked_until = NULL
                 WHERE app_user_id = %s
                """,
                (hash_password(new_password), int(user["app_user_id"])),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    refreshed = current_user(request) or user
    return templates.TemplateResponse(
        "password.html",
        {"request": request, "user": refreshed, "message": "パスワードを変更しました。", "error": None},
    )


@app.get("/admin/users", response_class=HTMLResponse)
def admin_users(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "users.manage"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    filters = admin_user_filters_from_request(request)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        page_data = load_admin_page_data(cur, filters=filters)
        cur.close()
    return templates.TemplateResponse(
        "admin_users.html",
        {
            "request": request,
            "user": user,
            **page_data,
            "message": None,
            "temporary_password": None,
            "error": None,
        },
    )


@app.get("/admin/users/new", response_class=HTMLResponse)
def issue_user_form(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "users.manage"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        page_data = load_issue_page_data(cur)
        cur.close()
    return templates.TemplateResponse(
        "admin_user_new.html",
        {
            "request": request,
            "user": user,
            **page_data,
            "message": None,
            "temporary_password": None,
            "error": None,
        },
    )


@app.post("/admin/users/issue")
async def issue_user(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "users.manage"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    form = await read_form(request)
    employee_no = form.get("employee_no", "").strip()
    display_name = form.get("display_name", "").strip()
    display_name_kana = form.get("display_name_kana", "").strip() or None
    department_name = form.get("department_name", "").strip() or None
    email = form.get("email", "").strip() or None
    role_code = form.get("role_code", "VIEWER").strip() or "VIEWER"
    issue_form = {
        "employee_no": employee_no,
        "display_name": display_name,
        "display_name_kana": display_name_kana or "",
        "department_name": department_name or "",
        "email": email or "",
        "role_code": role_code,
    }
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            if not employee_no or not display_name:
                page_data = load_issue_page_data(cur, issue_form=issue_form)
                conn.rollback()
                return templates.TemplateResponse(
                    "admin_user_new.html",
                    {
                        "request": request,
                        "user": user,
                        **page_data,
                        "issue_form": issue_form,
                        "message": None,
                        "temporary_password": None,
                        "error": "社員番号と氏名は必須です。",
                    },
                    status_code=400,
                )
            if not role_exists(cur, role_code=role_code):
                page_data = load_issue_page_data(cur, issue_form=issue_form)
                conn.rollback()
                return templates.TemplateResponse(
                    "admin_user_new.html",
                    {
                        "request": request,
                        "user": user,
                        **page_data,
                        "issue_form": issue_form,
                        "message": None,
                        "temporary_password": None,
                        "error": "指定されたロールが見つかりません。",
                    },
                    status_code=400,
                )
            cur.execute("SELECT app_user_id FROM app_users WHERE employee_no = %s", (employee_no,))
            if cur.fetchone():
                page_data = load_issue_page_data(cur, issue_form=issue_form)
                conn.rollback()
                return templates.TemplateResponse(
                    "admin_user_new.html",
                    {
                        "request": request,
                        "user": user,
                        **page_data,
                        "issue_form": issue_form,
                        "message": None,
                        "temporary_password": None,
                        "error": "この社員番号はすでに登録されています。",
                    },
                    status_code=400,
                )

            temporary_password = generate_temporary_password()
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
                  approval_status,
                  approved_at,
                  approved_by_app_user_id,
                  is_active,
                  note
                )
                VALUES (
                  %s, %s, %s, %s, %s,
                  %s, 'pbkdf2_sha256', CURRENT_TIMESTAMP(3), 1,
                  'APPROVED', CURRENT_TIMESTAMP(3), %s, 1,
                  'issued by admin screen'
                )
                """,
                (
                    employee_no,
                    display_name,
                    display_name_kana,
                    department_name,
                    email,
                    hash_password(temporary_password),
                    int(user["app_user_id"]),
                ),
            )
            app_user_id = int(cur.lastrowid)
            assign_user_role(
                cur,
                app_user_id=app_user_id,
                role_code=role_code,
                assigned_by_app_user_id=int(user["app_user_id"]),
            )
            page_data = load_issue_page_data(cur)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    return templates.TemplateResponse(
        "admin_user_new.html",
        {
            "request": request,
            "user": user,
            **page_data,
            "message": "アカウントを発行しました。初期パスワードを本人に伝えてください。",
            "temporary_password": temporary_password,
            "error": None,
        },
    )


@app.post("/admin/users/{app_user_id}/approve")
def approve_user(request: Request, app_user_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "users.manage"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            cur.execute(
                """
                UPDATE app_users
                   SET approval_status = 'APPROVED',
                       is_active = 1,
                       approved_at = CURRENT_TIMESTAMP(3),
                       approved_by_app_user_id = %s
                 WHERE app_user_id = %s
                """,
                (int(user["app_user_id"]), app_user_id),
            )
            cur.execute(
                """
                UPDATE app_user_roles
                   SET is_active = 0,
                       valid_to = CURRENT_DATE()
                 WHERE app_user_id = %s
                   AND is_active = 1
                   AND app_role_id IN (
                     SELECT app_role_id FROM app_roles WHERE role_code = 'PENDING'
                   )
                """,
                (app_user_id,),
            )
            cur.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM app_user_roles ur
                JOIN app_roles r
                  ON r.app_role_id = ur.app_role_id
                 AND r.is_active = 1
                 AND r.role_code <> 'PENDING'
                WHERE ur.app_user_id = %s
                  AND ur.is_active = 1
                  AND (ur.valid_from IS NULL OR ur.valid_from <= CURRENT_DATE())
                  AND (ur.valid_to IS NULL OR ur.valid_to >= CURRENT_DATE())
                """,
                (app_user_id,),
            )
            role_count = int((cur.fetchone() or {}).get("cnt") or 0)
            if role_count == 0:
                assign_user_role(
                    cur,
                    app_user_id=app_user_id,
                    role_code="VIEWER",
                    assigned_by_app_user_id=int(user["app_user_id"]),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return RedirectResponse("/admin/users", status_code=303)


@app.get("/admin/users/{app_user_id}/edit", response_class=HTMLResponse)
def edit_user_form(request: Request, app_user_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "users.manage"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        row = load_admin_user_detail(cur, app_user_id=app_user_id)
        roles = load_manageable_roles(cur)
        cur.close()
    if row is None:
        return RedirectResponse("/admin/users", status_code=303)
    return templates.TemplateResponse(
        "admin_user_edit.html",
        {
            "request": request,
            "user": user,
            "target_user": row,
            "roles": roles,
            "form": admin_user_form_values(row),
            "message": None,
            "error": None,
        },
    )


@app.post("/admin/users/{app_user_id}/edit", response_class=HTMLResponse)
async def update_admin_user(request: Request, app_user_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "users.manage"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    form = await read_form(request)
    employee_no = form.get("employee_no", "").strip()
    display_name = form.get("display_name", "").strip()
    display_name_kana = form.get("display_name_kana", "").strip() or None
    department_name = form.get("department_name", "").strip() or None
    email = form.get("email", "").strip() or None
    role_code = form.get("role_code", "").strip()
    form_values = {
        "employee_no": employee_no,
        "display_name": display_name,
        "display_name_kana": display_name_kana or "",
        "department_name": department_name or "",
        "email": email or "",
        "role_code": role_code,
    }
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            row = load_admin_user_detail(cur, app_user_id=app_user_id)
            roles = load_manageable_roles(cur)
            if row is None:
                conn.rollback()
                return RedirectResponse("/admin/users", status_code=303)
            if not employee_no or not display_name:
                conn.rollback()
                return templates.TemplateResponse(
                    "admin_user_edit.html",
                    {
                        "request": request,
                        "user": user,
                        "target_user": row,
                        "roles": roles,
                        "form": form_values,
                        "message": None,
                        "error": "社員番号と氏名は必須です。",
                    },
                    status_code=400,
                )
            cur.execute(
                """
                SELECT app_user_id
                FROM app_users
                WHERE employee_no = %s
                  AND app_user_id <> %s
                """,
                (employee_no, app_user_id),
            )
            if cur.fetchone():
                conn.rollback()
                return templates.TemplateResponse(
                    "admin_user_edit.html",
                    {
                        "request": request,
                        "user": user,
                        "target_user": row,
                        "roles": roles,
                        "form": form_values,
                        "message": None,
                        "error": "この社員番号はすでに登録されています。",
                    },
                    status_code=400,
                )
            if not role_exists(cur, role_code=role_code):
                conn.rollback()
                return templates.TemplateResponse(
                    "admin_user_edit.html",
                    {
                        "request": request,
                        "user": user,
                        "target_user": row,
                        "roles": roles,
                        "form": form_values,
                        "message": None,
                        "temporary_password": None,
                        "error": "指定されたロールが見つかりません。",
                    },
                    status_code=400,
                )
            if not role_has_user_manage(cur, role_code=role_code):
                if count_remaining_active_user_managers(cur, excluding_app_user_id=app_user_id) <= 0:
                    conn.rollback()
                    return templates.TemplateResponse(
                        "admin_user_edit.html",
                        {
                            "request": request,
                            "user": user,
                            "target_user": row,
                            "roles": roles,
                            "form": form_values,
                            "message": None,
                            "temporary_password": None,
                            "error": "有効な管理者が0人になるためロールを変更できません。",
                        },
                        status_code=400,
                    )
            cur.execute(
                """
                UPDATE app_users
                   SET employee_no = %s,
                       display_name = %s,
                       display_name_kana = %s,
                       department_name = %s,
                       email = %s
                 WHERE app_user_id = %s
                """,
                (employee_no, display_name, display_name_kana, department_name, email, app_user_id),
            )
            replace_user_role(
                cur,
                app_user_id=app_user_id,
                role_code=role_code,
                assigned_by_app_user_id=int(user["app_user_id"]),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return RedirectResponse(f"/admin/users/{app_user_id}/edit", status_code=303)


@app.post("/admin/users/{app_user_id}/reset-password")
def reset_user_password(request: Request, app_user_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "users.manage"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    temporary_password = generate_temporary_password()
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            cur.execute(
                """
                UPDATE app_users
                   SET password_hash = %s,
                       password_hash_algorithm = 'pbkdf2_sha256',
                       password_changed_at = CURRENT_TIMESTAMP(3),
                       must_change_password = 1,
                       failed_login_count = 0,
                       locked_until = NULL
                 WHERE app_user_id = %s
                """,
                (hash_password(temporary_password), app_user_id),
            )
            page_data = load_admin_page_data(cur)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    return templates.TemplateResponse(
        "admin_users.html",
        {
            "request": request,
            "user": user,
            **page_data,
            "message": "初期パスワードを発行しました。本人に伝えてください。",
            "temporary_password": temporary_password,
            "error": None,
        },
    )


@app.post("/admin/users/{app_user_id}/disable")
def disable_user(request: Request, app_user_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "users.manage"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            if count_remaining_active_user_managers(cur, excluding_app_user_id=app_user_id) <= 0:
                page_data = load_admin_page_data(cur)
                conn.rollback()
                return templates.TemplateResponse(
                    "admin_users.html",
                    {
                        "request": request,
                        "user": user,
                        **page_data,
                        "message": None,
                        "temporary_password": None,
                        "error": "有効な管理者が0人になるため無効化できません。",
                    },
                    status_code=400,
                )
            cur.execute("UPDATE app_users SET is_active = 0 WHERE app_user_id = %s", (app_user_id,))
            cur.execute(
                """
                UPDATE app_sessions
                   SET revoked_at = CURRENT_TIMESTAMP(3)
                 WHERE app_user_id = %s
                   AND revoked_at IS NULL
                """,
                (app_user_id,),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return RedirectResponse("/admin/users", status_code=303)


@app.post("/admin/users/{app_user_id}/enable")
def enable_user(request: Request, app_user_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "users.manage"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            cur.execute(
                """
                UPDATE app_users
                   SET is_active = 1
                 WHERE app_user_id = %s
                   AND approval_status = 'APPROVED'
                """,
                (app_user_id,),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return RedirectResponse("/admin/users", status_code=303)
