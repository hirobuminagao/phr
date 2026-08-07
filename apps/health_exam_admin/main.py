from __future__ import annotations

import os
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
    revoke_session,
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


@app.get("/admin/users", response_class=HTMLResponse)
def admin_users(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "users.manage"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        cur.execute(
            """
            SELECT
              app_user_id,
              employee_no,
              display_name,
              display_name_kana,
              department_name,
              email,
              approval_status,
              is_active,
              approval_requested_at,
              approved_at,
              last_login_at
            FROM app_users
            ORDER BY
              CASE approval_status WHEN 'PENDING' THEN 0 WHEN 'APPROVED' THEN 1 ELSE 2 END,
              app_user_id
            """
        )
        users = cur.fetchall()
        cur.close()
    return templates.TemplateResponse(
        "admin_users.html",
        {"request": request, "user": user, "users": users, "message": None},
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
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return RedirectResponse("/admin/users", status_code=303)


@app.post("/admin/users/{app_user_id}/disable")
def disable_user(request: Request, app_user_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "users.manage"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    if int(user["app_user_id"]) == app_user_id:
        return RedirectResponse("/admin/users", status_code=303)

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            cur.execute("UPDATE app_users SET is_active = 0 WHERE app_user_id = %s", (app_user_id,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return RedirectResponse("/admin/users", status_code=303)
