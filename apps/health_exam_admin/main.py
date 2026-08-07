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


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    user = current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
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
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": result.failure_reason or "LOGIN_FAILED"},
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
    user = current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    return templates.TemplateResponse("export_lists.html", {"request": request, "user": user})
