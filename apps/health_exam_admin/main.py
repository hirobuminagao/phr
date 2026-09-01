from __future__ import annotations

import argparse
import csv
import io
import os
import hashlib
import json
import logging
import re
import secrets
import shutil
import string
import subprocess
import sys
import tempfile
import traceback
import zipfile
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import parse_qs, quote, urlencode

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from mysql.connector import IntegrityError
from starlette.background import BackgroundTask

from scripts.lib.csv.csv_loader import load_csv_result
from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.schemas import CSV_MAPPING_LAB
from scripts.lib.db.lookup.event import get_event_insurer_number, get_event_year
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.etl.metrics import RunMetrics
from scripts.lib.etl.runs import finish_run, start_run
from scripts.lib.examination.lookup import qname
from scripts.lib.examination.models import RESULT_NG, RESULT_OK, STATUS_OK
from scripts.lib.examination.report_classification import fiscal_year_end_date
from scripts.from_medical.script_lib.article44_checker import check_article44
from scripts.from_medical.script_lib.article44_required_namecodes import fetch_article44_required_namecodes
from scripts.from_medical.script_lib.article44_value_loader import _build_value_map as build_article44_value_map
from scripts.from_medical.script_lib.check_exam_results import (
    ARTICLE44_DETAIL_NAMES,
    aggregate_article44_legal_result,
    article44_result_columns,
    validate_article44_result,
)
from scripts.from_medical.script_lib.export_case_readiness import refresh_export_case_readiness
from scripts.from_medical.script_lib.hia_xml_export_loader import (
    ExportSelectors,
    decide_candidate,
    fetch_candidates,
)
from scripts.from_medical.script_lib.specific_health_checker import (
    RESULT_NOT_APPLICABLE,
    RESULT_UNDETERMINABLE,
    SPECIFIC_DETAIL_CODE_BY_NAMECODE,
    SPECIFIC_ITEM_NAMES,
    SPECIFIC_REQUIRED_NAMECODES,
    aggregate_specific_result_with_details,
)
from scripts.from_medical.script_lib.specific_health_required_namecodes import fetch_specific_health_required_namecodes
from scripts.csv_mapping_lab.analyze_csv import insert_analysis as insert_csv_mapping_analysis, normalize_header
from scripts.csv_mapping_lab.export_llm_prompt import (
    build_prompt as build_csv_mapping_prompt,
    fetch_analysis as fetch_csv_mapping_analysis,
    filter_columns as filter_csv_mapping_columns,
    json_default as csv_mapping_json_default,
)
from scripts.csv_mapping_lab.rule_engine import apply_rules_to_analysis
from scripts.hia.script_lib.config_loader import config_bool, config_value, load_yaml_config
from scripts.hia.script_lib.fund_delivery_list_builder import (
    FundDeliveryListConfig,
    build_fund_delivery_list,
)
from scripts.hia.script_lib.fund_delivery_submission_marker import (
    FundDeliverySubmissionConfig,
    mark_fund_delivery_submitted,
)
from scripts.hia.script_lib.fund_delivery_zip_exporter import (
    FundDeliveryZipExportConfig,
    export_fund_delivery_zip,
)
from scripts.hia.script_lib.hia_download_importer import (
    HiaDownloadImportConfig,
    import_hia_download_zips,
)
from scripts.hia.dev_tools.check_hia_xml_zip import (
    DEFAULT_REPORT_DIR as XML_ZIP_CHECK_REPORT_DIR,
    DEFAULT_XSD_DIR as XML_ZIP_CHECK_XSD_DIR,
    ManualTextFix,
    check_zip as check_hia_xml_zip_file,
    write_report as write_hia_xml_zip_check_report,
)
from scripts.lib.identity.field.address import normalize_address_export, normalize_postal_code_export
from scripts.lib.identity.field.date_field import normalize_date_to_ymd_and_compact
from scripts.lib.identity.field.gender_code import normalize_gender_code
from scripts.lib.identity.field.insurance_number import normalize_insurance_number
from scripts.lib.identity.field.insurance_symbol import normalize_insurance_symbol
from scripts.lib.identity.field.insurer_number import normalize_insurer_number
from scripts.lib.identity.field.name_kana import normalize_name_kana_full
from scripts.lib.identity.field.ticket_identifier import normalize_ticket_identifier
from scripts.lib.identity.generator import generate_identity_bundle
from scripts.lib.identity.primitive.digits import zero_pad
from scripts.phr_app.script_lib.app_auth import (
    authenticate_user,
    get_authenticated_session,
    get_app_setting,
    get_app_setting_int,
    hash_password,
    revoke_session,
    verify_password,
)


APP_ROOT = Path(__file__).resolve().parent
REPO_ROOT = APP_ROOT.parents[1]
CSV_MAPPING_LAB_REGULATION_PATH = REPO_ROOT / "docs" / "spec" / "csv_mapping_lab" / "ai_mapping_exchange_regulation.md"
SYSTEM_SQL_EXPORT_ROOT = REPO_ROOT / "sql" / "system_exports"
CSV_MAPPING_AI_INSTRUCTION = """添付ZIPを解析してください。
ZIP内の REGULATION.md を必ず読んで、そのルールに従って analysis_prompt.json の列マッピング候補をAIレビューしてください。

各列のヘッダー、サンプル値、型、周辺列を確認し、関連列がある場合は related_column_nos に入れてください。
所見、問診、判定、検査方法違い、左右別、連番列は、安易にDIRECTにせず、必要に応じて MULTI_COLUMN_JOIN / METHOD_SELECTION / NEEDS_CONFIRMATION を使ってください。
ヘッダーとして意味があるが今回の取込対象にはしない列で、将来値が入ったら気づきたいものは WATCH / WATCH_IF_PRESENT にしてください。
サンプル値が空、または non_blank_count が0という理由だけで IGNORE にしないでください。

候補namecodeは、analysis_prompt.json 内の既存候補または同梱された根拠から判断できるものだけにしてください。
存在確認できないnamecodeは作らず、REVIEW または NEEDS_CONFIRMATION にしてください。

返却JSONは suggestions ではなく updates を使ってください。
ai_review_status は REVIEWED / SKIPPED / FAILED のいずれかにしてください。
人の最終判断である decision_status は変更対象にしないでください。

返却は REGULATION.md の返却形式に従って、JSONだけにしてください。
Markdown説明、コードフェンス、補足文章は不要です。"""
LOGGER = logging.getLogger("health_exam_admin")
SESSION_COOKIE_NAME = "phr_app_session"
CSRF_COOKIE_NAME = "phr_app_csrf"
CSRF_FIELD_NAME = "_csrf_token"
ARTICLE44_GROUP_CODE = "v2_2026_ARTICLE44_CHECK_ITEMS"
CDA_SECTION_CODE_SYSTEM = "1.2.392.200119.6.1010"
CDA_SECTION_NAMES = {
    "01010": "特定健診・問診結果セクション",
    "01020": "広域連合保健事業セクション",
    "01030": "労働安全衛生法健診結果セクション",
    "01040": "学校保健安全法健診結果セクション",
    "01060": "がん検診セクション",
    "01090": "肝炎検診セクション",
    "01990": "任意追加項目セクション",
}
BUSINESS_SETTINGS_VIEW_PERMISSION = "business_settings.view"
BUSINESS_SETTINGS_PERMISSION = "business_settings.manage"
SYSTEM_SETTINGS_PERMISSION = "users.manage"
MANUAL_EXAM_ENTRY_EDIT_PERMISSION = "manual_exam_entry.edit"
MANUAL_EXAM_ENTRY_MANAGE_PERMISSION = "manual_exam_entry.manage"
SUBSCRIBER_REFERENCE_VIEW_PERMISSION = "subscriber_reference.view"
SUBSCRIBER_REFERENCE_HISTORY_PERMISSION = "subscriber_reference.history_view"
SUBSCRIBER_REFERENCE_EDIT_PERMISSION = "subscriber_reference.edit"
SUBSCRIBER_REFERENCE_PII_FULL_PERMISSION = "subscriber_reference.pii.full"
SUBSCRIBER_REFERENCE_PII_MASKED_PERMISSION = "subscriber_reference.pii.masked"
SUBSCRIBER_REFERENCE_PII_HIDDEN_PERMISSION = "subscriber_reference.pii.hidden"
MANUAL_EXAM_FLAG_TOGGLE_IDENTITY_CODES = {"9N056", "9N061", "9N066"}
LOGIN_ERROR_MESSAGES = {
    "USER_NOT_FOUND": "社員番号またはパスワードが違います。",
    "PASSWORD_MISMATCH": "社員番号またはパスワードが違います。",
    "USER_INACTIVE": "このアカウントは無効です。",
    "USER_LOCKED": "このアカウントは一時的にロックされています。",
    "IP_NOT_ALLOWED": "この端末からのログインは許可されていません。",
    "USER_NOT_APPROVED": "承認待ちです。管理者の承認後にログインできます。",
}
WORK_PERMISSION_ITEMS = (
    {
        "key": "export_list",
        "name": "出力リスト",
        "description": "出力対象リストの確認・作成を担当する",
        "view_codes": ("export_lists.view",),
        "edit_codes": ("export_lists.edit",),
    },
    {
        "key": "xml_export",
        "name": "XML出力",
        "description": "確認出力と本番出力を担当する",
        "view_codes": ("xml_export.review",),
        "edit_codes": ("xml_export.official",),
    },
    {
        "key": "hia_upload",
        "name": "HIAアップロード",
        "description": "出力済みZIPのアップロード作業と結果記帳を担当する",
        "view_codes": ("hia_upload.perform",),
        "edit_codes": ("hia_upload_status.edit",),
    },
    {
        "key": "business_settings",
        "name": "管理カテゴリ",
        "description": "イベント、健診機関、受領フォルダなど業務側の管理を担当する",
        "view_codes": ("business_settings.view",),
        "edit_codes": ("business_settings.manage",),
    },
    {
        "key": "manual_exam_entry",
        "name": "健診結果手入力",
        "description": "表示=手入力draftの作成・更新、編集=削除や正式ledger管理を担当する",
        "view_codes": (MANUAL_EXAM_ENTRY_EDIT_PERMISSION,),
        "edit_codes": (MANUAL_EXAM_ENTRY_MANAGE_PERMISSION,),
    },
    {
        "key": "case_csv_download",
        "name": "case CSV出力",
        "description": "個人case一覧の絞り込み結果を、加入者情報付きCSVとして出力する",
        "view_codes": ("exam_export_cases.csv_download",),
        "edit_codes": ("exam_export_cases.csv_download",),
    },
    {
        "key": "subscriber_reference",
        "name": "加入者情報確認",
        "description": "表示=加入者・イベント・履歴の参照、編集=将来の加入者情報変更を許可する",
        "view_codes": (SUBSCRIBER_REFERENCE_VIEW_PERMISSION, SUBSCRIBER_REFERENCE_HISTORY_PERMISSION),
        "edit_codes": (SUBSCRIBER_REFERENCE_EDIT_PERMISSION,),
    },
)

BASIC_INFO_CORRECTION_FIELDS: dict[str, dict[str, Any]] = {
    "report_codes": {
        "label": "報告区分 / プログラムコード",
        "case_value_column": "health_exam_report_category",
        "case_secondary_value_column": "program_code",
        "case_source_column": "report_code_resolution_source",
        "case_reason_column": "report_code_resolution_reason",
        "composite": True,
    },
    "exam_date": {
        "label": "健診実施日",
        "case_value_column": "exam_date_export_value",
        "case_source_column": "exam_date_export_source",
        "case_reason_column": "exam_date_export_reason",
    },
    "name_kana": {
        "label": "氏名カナ",
        "case_value_column": "name_kana_export_value",
        "case_source_column": "name_kana_export_source",
        "case_reason_column": "name_kana_export_reason",
    },
    "insurance_symbol": {
        "label": "記号",
        "case_value_column": "insurance_symbol_export_value",
        "case_source_column": "insurance_symbol_export_source",
        "case_reason_column": "insurance_symbol_export_reason",
    },
    "insurance_number": {
        "label": "番号",
        "case_value_column": "insurance_number_export_value",
        "case_source_column": "insurance_number_export_source",
        "case_reason_column": "insurance_number_export_reason",
    },
    "insurance_branch_number": {
        "label": "枝番",
        "case_value_column": "insurance_branch_number_export_value",
        "case_source_column": "insurance_branch_number_export_source",
        "case_reason_column": "insurance_branch_number_export_reason",
    },
    "exam_ticket_number": {
        "label": "受診券番号",
        "case_value_column": "exam_ticket_number_export_value",
        "case_source_column": "exam_ticket_number_export_source",
        "case_reason_column": "exam_ticket_number_export_reason",
    },
    "exam_ticket_expires_on": {
        "label": "受診券有効期限",
        "case_value_column": "exam_ticket_expires_on_export_value",
        "case_source_column": "exam_ticket_expires_on_export_source",
        "case_reason_column": "exam_ticket_expires_on_export_reason",
    },
    "insurer_number": {
        "label": "保険者番号",
        "case_value_column": "insurer_number_export_value",
        "case_source_column": None,
        "case_reason_column": None,
    },
    "postal_code": {
        "label": "郵便番号",
        "case_value_column": "postal_code_completed_value",
        "case_source_column": None,
        "case_reason_column": "address_completion_reason",
    },
    "address": {
        "label": "住所",
        "case_value_column": "address_completed_value",
        "case_source_column": "address_source",
        "case_reason_column": "address_completion_reason",
    },
}
app = FastAPI(title="PHR Health Exam Admin")
app.mount("/static", StaticFiles(directory=APP_ROOT / "static"), name="static")
templates = Jinja2Templates(directory=APP_ROOT / "templates")
templates.env.filters["url_quote"] = lambda value: quote(str(value or ""), safe="")
templates.env.globals["static_asset_version"] = max(
    (APP_ROOT / "static" / "app.css").stat().st_mtime_ns,
    (APP_ROOT / "static" / "app.js").stat().st_mtime_ns,
)


def admin_allowed_client_ips() -> set[str]:
    raw = os.getenv("PHR_ADMIN_ALLOWED_CLIENT_IPS", "")
    values = {
        item.strip()
        for item in raw.replace("\n", ",").split(",")
        if item.strip()
    }
    if not values:
        return set()
    values.update({"127.0.0.1", "::1"})
    return values


def request_client_ip(request: Request) -> str | None:
    forwarded = None
    if os.getenv("PHR_ADMIN_TRUST_PROXY_HEADERS", "0") == "1":
        forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",", 1)[0].strip()
    if request.client:
        return request.client.host
    return None


@app.middleware("http")
async def restrict_admin_client_ip(request: Request, call_next: Any) -> Response:
    allowed_ips = admin_allowed_client_ips()
    if allowed_ips:
        request_ip = request_client_ip(request)
        if request_ip not in allowed_ips:
            return Response("Forbidden: client IP is not allowed.", status_code=403)
    return await call_next(request)


def is_csrf_exempt_path(path: str) -> bool:
    return path in {"/login", "/logout", "/register"} or path.startswith("/static/")


async def request_csrf_token(request: Request) -> str | None:
    header_value = request.headers.get("x-csrf-token")
    if header_value:
        return header_value
    content_type = request.headers.get("content-type", "")
    body = await request.body()
    if content_type.startswith("application/x-www-form-urlencoded"):
        values = parse_qs(body.decode("utf-8", errors="replace"), keep_blank_values=True)
        token_values = values.get(CSRF_FIELD_NAME)
        return token_values[0] if token_values else None
    if content_type.startswith("multipart/form-data"):
        match = re.search(
            rb'name="' + re.escape(CSRF_FIELD_NAME.encode("utf-8")) + rb'"\r?\n\r?\n([^\r\n]*)',
            body,
        )
        if match:
            return match.group(1).decode("utf-8", errors="replace")
    return None


@app.middleware("http")
async def protect_post_with_csrf(request: Request, call_next: Any) -> Response:
    if request.method in {"POST", "PUT", "PATCH", "DELETE"} and not is_csrf_exempt_path(request.url.path):
        cookie_token = request.cookies.get(CSRF_COOKIE_NAME)
        submitted_token = await request_csrf_token(request)
        if not cookie_token or not submitted_token or not secrets.compare_digest(cookie_token, submitted_token):
            return Response("Forbidden: invalid CSRF token.", status_code=403)

    response = await call_next(request)
    if not request.cookies.get(CSRF_COOKIE_NAME):
        response.set_cookie(
            CSRF_COOKIE_NAME,
            secrets.token_urlsafe(32),
            httponly=False,
            samesite="lax",
            secure=False,
            max_age=60 * 60 * 12,
        )
    return response


@app.middleware("http")
async def force_utf8_html_response(request: Request, call_next: Any) -> Response:
    response = await call_next(request)
    content_type = response.headers.get("content-type", "")
    if content_type.startswith("text/html") and "charset=" not in content_type.lower():
        response.headers["content-type"] = "text/html; charset=utf-8"
    return response


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


def health_db() -> str:
    return os.getenv("PHR_HEALTH_DB", "health_exam_result")


def master_db() -> str:
    return os.getenv("PHR_MASTER_DB", "phr_master")


def dev_db() -> str:
    return os.getenv("PHR_DEV_DB", "dev_phr")


def work_other_db() -> str:
    return os.getenv("PHR_WORK_OTHER_DB", "work_other")


def db_prefix() -> str:
    return os.getenv("PHR_DB_PREFIX", "PHR_DB_")


def client_ip(request: Request) -> str | None:
    return request_client_ip(request)


async def read_form(request: Request) -> dict[str, str]:
    form = await request.form()
    return {key: str(value) for key, value in form.multi_items()}


def current_user(request: Request) -> dict[str, Any] | None:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if not token:
        return None
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            idle_timeout = get_app_setting_int(
                cur,
                app_db=app_db(),
                setting_key="session_idle_timeout_minutes",
                default=60,
                minimum=0,
                maximum=24 * 60,
            )
            user = get_authenticated_session(
                cur,
                app_db=app_db(),
                session_token=token,
                idle_timeout_minutes=idle_timeout,
            )
            conn.commit()
            return user
        except Exception:
            conn.rollback()
            raise


def has_permission(user: dict[str, Any], permission_code: str) -> bool:
    return permission_code in set(user.get("permissions") or ())


def has_any_permission(user: dict[str, Any], permission_codes: tuple[str, ...]) -> bool:
    permissions = set(user.get("permissions") or ())
    return any(permission_code in permissions for permission_code in permission_codes)


def can_manage_business_settings(user: dict[str, Any]) -> bool:
    return has_any_permission(user, (BUSINESS_SETTINGS_PERMISSION, SYSTEM_SETTINGS_PERMISSION))


def can_view_business_settings(user: dict[str, Any]) -> bool:
    return has_any_permission(
        user,
        (BUSINESS_SETTINGS_VIEW_PERMISSION, BUSINESS_SETTINGS_PERMISSION, SYSTEM_SETTINGS_PERMISSION),
    )


def can_run_exam_processing(user: dict[str, Any]) -> bool:
    return has_any_permission(user, ("export_lists.edit", SYSTEM_SETTINGS_PERMISSION))


def can_edit_manual_exam_entry(user: dict[str, Any]) -> bool:
    return has_any_permission(
        user,
        (MANUAL_EXAM_ENTRY_EDIT_PERMISSION, MANUAL_EXAM_ENTRY_MANAGE_PERMISSION, SYSTEM_SETTINGS_PERMISSION),
    )


def can_manage_manual_exam_entry(user: dict[str, Any]) -> bool:
    return has_any_permission(user, (MANUAL_EXAM_ENTRY_MANAGE_PERMISSION, SYSTEM_SETTINGS_PERMISSION))


def can_view_subscriber_reference(user: dict[str, Any]) -> bool:
    return has_any_permission(user, (SUBSCRIBER_REFERENCE_VIEW_PERMISSION, SYSTEM_SETTINGS_PERMISSION))


def can_view_subscriber_history(user: dict[str, Any]) -> bool:
    return has_any_permission(user, (SUBSCRIBER_REFERENCE_HISTORY_PERMISSION, SYSTEM_SETTINGS_PERMISSION))


def subscriber_reference_pii_level(user: dict[str, Any]) -> str:
    if has_permission(user, SYSTEM_SETTINGS_PERMISSION):
        return "FULL"
    if has_permission(user, SUBSCRIBER_REFERENCE_PII_HIDDEN_PERMISSION):
        return "HIDDEN"
    if has_permission(user, SUBSCRIBER_REFERENCE_PII_MASKED_PERMISSION):
        return "MASKED"
    if has_permission(user, SUBSCRIBER_REFERENCE_PII_FULL_PERMISSION):
        return "FULL"
    return "HIDDEN"


def require_user(request: Request) -> dict[str, Any] | RedirectResponse:
    user = current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    return user


@app.exception_handler(Exception)
async def unhandled_app_exception(request: Request, exc: Exception) -> Response:
    log_id = generate_app_error_log_id()
    user: dict[str, Any] | None = None
    try:
        user = current_user(request)
    except Exception:
        user = None
    try:
        log_unhandled_app_error(request, exc, log_id=log_id, user=user)
    except Exception:
        LOGGER.exception("failed to persist app error log log_id=%s", log_id)
    LOGGER.exception("unhandled app error log_id=%s path=%s", log_id, request.url.path)
    wants_json = request.url.path.startswith("/api/") or "application/json" in request.headers.get("accept", "")
    if wants_json:
        return JSONResponse(
            {
                "error": "INTERNAL_ERROR",
                "message": "処理中にエラーが発生しました。",
                "log_id": log_id,
                "error_detail_url": f"/admin/error-logs/{log_id}",
            },
            status_code=500,
        )
    return templates.TemplateResponse(
        "internal_error.html",
        {
            "request": request,
            "user": user,
            "log_id": log_id,
        },
        status_code=500,
    )


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
          (
            SELECT GROUP_CONCAT(aip.allowed_ip ORDER BY aip.allowed_ip SEPARATOR ', ')
            FROM app_user_allowed_ips aip
            WHERE aip.app_user_id = u.app_user_id
              AND aip.is_active = 1
              AND (aip.label = '申請元IP' OR aip.note = 'self registration source ip')
          ) AS registration_ip,
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


def load_app_user_options(cur: Any) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          app_user_id,
          employee_no,
          display_name,
          display_name_kana,
          department_name,
          is_active,
          approval_status
        FROM {qname(app_db())}.app_users
        ORDER BY
          CASE WHEN approval_status = 'APPROVED' AND is_active = 1 THEN 0 ELSE 1 END,
          COALESCE(display_name_kana, display_name, employee_no),
          app_user_id
        """
    )
    return [dict(row) for row in cur.fetchall()]


def load_manageable_roles(cur: Any) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT app_role_id, role_code, role_name
        FROM app_roles
        WHERE is_active = 1
          AND role_code IN ('VIEWER', 'EDITOR', 'FIELD_MANAGER', 'ADMIN')
        ORDER BY FIELD(role_code, 'VIEWER', 'EDITOR', 'FIELD_MANAGER', 'ADMIN'), role_code
        """
    )
    return [dict(row) for row in cur.fetchall()]


def load_permission_matrix(cur: Any) -> dict[str, Any]:
    roles = load_manageable_roles(cur)
    cur.execute(
        """
        SELECT app_permission_id, permission_code, permission_name, permission_group, description
        FROM app_permissions
        WHERE is_active = 1
        ORDER BY
          FIELD(permission_group, 'users', 'business', 'health_exam', 'xml_export', 'hia', 'audit'),
          permission_group,
          app_permission_id
        """
    )
    permissions = [dict(row) for row in cur.fetchall()]
    cur.execute(
        """
        SELECT r.role_code, p.permission_code, rp.is_allowed
        FROM app_role_permissions rp
        JOIN app_roles r
          ON r.app_role_id = rp.app_role_id
         AND r.is_active = 1
        JOIN app_permissions p
          ON p.app_permission_id = rp.app_permission_id
         AND p.is_active = 1
        WHERE r.role_code IN ('ADMIN', 'EDITOR', 'VIEWER')
        """
    )
    allowed: dict[str, bool] = {}
    for row in cur.fetchall():
        allowed[f"{row['role_code']}||{row['permission_code']}"] = bool(row["is_allowed"])
    return {"roles": roles, "permissions": permissions, "allowed": allowed}


def upsert_role_permission(cur: Any, *, role_code: str, permission_code: str, is_allowed: bool) -> None:
    cur.execute(
        """
        INSERT INTO app_role_permissions (app_role_id, app_permission_id, is_allowed)
        SELECT r.app_role_id, p.app_permission_id, %s
        FROM app_roles r
        JOIN app_permissions p
        WHERE r.role_code = %s
          AND r.is_active = 1
          AND p.permission_code = %s
          AND p.is_active = 1
        ON DUPLICATE KEY UPDATE
          is_allowed = VALUES(is_allowed)
        """,
        (1 if is_allowed else 0, role_code, permission_code),
    )


def load_work_permission_rows(cur: Any, *, app_user_id: int | None = None) -> list[dict[str, Any]]:
    permission_codes = tuple(
        permission_code
        for item in WORK_PERMISSION_ITEMS
        for permission_code in (*item["view_codes"], *item["edit_codes"])
    )
    placeholders = ", ".join(["%s"] * len(permission_codes))
    cur.execute(
        f"""
        SELECT
          p.permission_code,
          p.permission_name,
          p.permission_group,
          p.description,
          up.is_allowed AS user_is_allowed
        FROM app_permissions p
        LEFT JOIN app_user_permissions up
          ON up.app_permission_id = p.app_permission_id
         AND up.app_user_id = %s
        WHERE p.is_active = 1
          AND p.permission_code IN ({placeholders})
        """,
        (app_user_id or 0, *permission_codes),
    )
    values = {str(row["permission_code"]): row for row in cur.fetchall()}
    rows: list[dict[str, Any]] = []
    for item in WORK_PERMISSION_ITEMS:
        view_codes = tuple(item["view_codes"])
        edit_codes = tuple(item["edit_codes"])
        rows.append(
            {
                "key": item["key"],
                "name": item["name"],
                "description": item["description"],
                "view_codes": view_codes,
                "edit_codes": edit_codes,
                "view_is_allowed": all(bool(values.get(code, {}).get("user_is_allowed")) for code in view_codes),
                "edit_is_allowed": all(bool(values.get(code, {}).get("user_is_allowed")) for code in edit_codes),
            }
        )
    return rows


def replace_user_work_permissions(
    cur: Any,
    *,
    app_user_id: int,
    allowed_work_permissions: set[tuple[str, str]],
    assigned_by_app_user_id: int,
) -> None:
    for item in WORK_PERMISSION_ITEMS:
        keyed_codes = (
            *[(str(item["key"]), "view", code) for code in item["view_codes"]],
            *[(str(item["key"]), "edit", code) for code in item["edit_codes"]],
        )
        for item_key, action_key, permission_code in keyed_codes:
            cur.execute(
                """
                INSERT INTO app_user_permissions (
                  app_user_id,
                  app_permission_id,
                  is_allowed,
                  assigned_by_app_user_id,
                  note
                )
                SELECT %s, p.app_permission_id, %s, %s, 'work permission set by admin screen'
                FROM app_permissions p
                WHERE p.permission_code = %s
                  AND p.is_active = 1
                ON DUPLICATE KEY UPDATE
                  is_allowed = VALUES(is_allowed),
                  assigned_by_app_user_id = VALUES(assigned_by_app_user_id),
                  note = VALUES(note)
                """,
                (
                    app_user_id,
                    1 if (item_key, action_key) in allowed_work_permissions else 0,
                    assigned_by_app_user_id,
                    permission_code,
                ),
            )


def load_user_subscriber_pii_level(cur: Any, *, app_user_id: int) -> str:
    cur.execute(
        """
        SELECT p.permission_code, up.is_allowed
        FROM app_permissions p
        LEFT JOIN app_user_permissions up
          ON up.app_permission_id = p.app_permission_id
         AND up.app_user_id = %s
        WHERE p.permission_code IN (%s, %s, %s)
        """,
        (
            app_user_id,
            SUBSCRIBER_REFERENCE_PII_FULL_PERMISSION,
            SUBSCRIBER_REFERENCE_PII_MASKED_PERMISSION,
            SUBSCRIBER_REFERENCE_PII_HIDDEN_PERMISSION,
        ),
    )
    allowed = {str(row["permission_code"]) for row in cur.fetchall() if bool(row.get("is_allowed"))}
    if SUBSCRIBER_REFERENCE_PII_HIDDEN_PERMISSION in allowed:
        return "HIDDEN"
    if SUBSCRIBER_REFERENCE_PII_MASKED_PERMISSION in allowed:
        return "MASKED"
    if SUBSCRIBER_REFERENCE_PII_FULL_PERMISSION in allowed:
        return "FULL"
    return "HIDDEN"


def replace_user_subscriber_pii_level(
    cur: Any,
    *,
    app_user_id: int,
    pii_level: str,
    assigned_by_app_user_id: int,
) -> None:
    permission_by_level = {
        "FULL": SUBSCRIBER_REFERENCE_PII_FULL_PERMISSION,
        "MASKED": SUBSCRIBER_REFERENCE_PII_MASKED_PERMISSION,
        "HIDDEN": SUBSCRIBER_REFERENCE_PII_HIDDEN_PERMISSION,
    }
    selected_code = permission_by_level.get(pii_level, SUBSCRIBER_REFERENCE_PII_HIDDEN_PERMISSION)
    for permission_code in permission_by_level.values():
        cur.execute(
            """
            INSERT INTO app_user_permissions (
              app_user_id, app_permission_id, is_allowed, assigned_by_app_user_id, note
            )
            SELECT %s, p.app_permission_id, %s, %s, 'subscriber PII level set by admin screen'
            FROM app_permissions p
            WHERE p.permission_code = %s
              AND p.is_active = 1
            ON DUPLICATE KEY UPDATE
              is_allowed = VALUES(is_allowed),
              assigned_by_app_user_id = VALUES(assigned_by_app_user_id),
              note = VALUES(note)
            """,
            (app_user_id, 1 if permission_code == selected_code else 0, assigned_by_app_user_id, permission_code),
        )


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


def load_allowed_ip_rows(cur: Any, *, app_user_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT app_user_allowed_ip_id, allowed_ip, label, is_active, note, updated_at
        FROM app_user_allowed_ips
        WHERE app_user_id = %s
        ORDER BY is_active DESC, allowed_ip
        """,
        (app_user_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def replace_allowed_ips(
    cur: Any,
    *,
    app_user_id: int,
    allowed_ips_text: str,
) -> None:
    values = []
    seen = set()
    for line in allowed_ips_text.replace(",", "\n").splitlines():
        text = line.strip()
        if not text or text in seen:
            continue
        values.append(text)
        seen.add(text)
    cur.execute(
        """
        UPDATE app_user_allowed_ips
           SET is_active = 0
         WHERE app_user_id = %s
        """,
        (app_user_id,),
    )
    for allowed_ip in values:
        cur.execute(
            """
            INSERT INTO app_user_allowed_ips (app_user_id, allowed_ip, is_active, note)
            VALUES (%s, %s, 1, 'updated from admin screen')
            ON DUPLICATE KEY UPDATE
              is_active = VALUES(is_active),
              note = VALUES(note)
            """,
            (app_user_id, allowed_ip),
        )


def add_registration_allowed_ip(cur: Any, *, app_user_id: int, request_ip: str | None) -> None:
    if not request_ip:
        return
    cur.execute(
        """
        INSERT INTO app_user_allowed_ips (app_user_id, allowed_ip, label, is_active, note)
        VALUES (%s, %s, '申請元IP', 1, 'self registration source ip')
        ON DUPLICATE KEY UPDATE
          label = VALUES(label),
          is_active = VALUES(is_active),
          note = VALUES(note)
        """,
        (app_user_id, request_ip),
    )


def allowed_ips_text(rows: list[dict[str, Any]]) -> str:
    return "\n".join(str(row["allowed_ip"]) for row in rows if bool(row.get("is_active")))


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


def load_app_security_settings(cur: Any) -> dict[str, str]:
    keys = {
        "session_lifetime_minutes": "720",
        "session_idle_timeout_minutes": "60",
        "personal_info_audit_enabled": "1",
    }
    return {
        key: get_app_setting(cur, app_db=app_db(), setting_key=key, default=default)
        for key, default in keys.items()
    }


def load_audit_log_rows(cur: Any, *, limit: int = 100) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
          app_audit_log_id,
          app_user_id,
          employee_no,
          action_code,
          target_schema,
          target_table,
          target_id,
          after_json,
          client_ip,
          created_at
        FROM app_audit_logs
        ORDER BY app_audit_log_id DESC
        LIMIT %s
        """,
        (limit,),
    )
    return [dict(row) for row in cur.fetchall()]


def generate_app_error_log_id() -> str:
    return f"ERR-{datetime.now().strftime('%Y%m%d%H%M%S')}-{secrets.token_hex(3)}"


def log_unhandled_app_error(request: Request, exc: Exception, *, log_id: str, user: dict[str, Any] | None) -> None:
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        try:
            cur.execute(
                f"""
                INSERT INTO {qname(app_db())}.app_error_logs (
                  log_id,
                  status_code,
                  method,
                  path,
                  query_string,
                  app_user_id,
                  employee_no,
                  client_ip,
                  user_agent,
                  exception_type,
                  exception_message,
                  traceback_text
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    log_id,
                    500,
                    request.method,
                    request.url.path,
                    request.url.query,
                    None if not user else user.get("app_user_id"),
                    None if not user else user.get("employee_no"),
                    client_ip(request),
                    request.headers.get("user-agent"),
                    type(exc).__name__,
                    str(exc)[:2000],
                    "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[:60000],
                ),
            )
        finally:
            cur.close()


def load_app_error_log_rows(cur: Any, *, limit: int = 200) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          app_error_log_id,
          log_id,
          status_code,
          method,
          path,
          query_string,
          employee_no,
          client_ip,
          exception_type,
          exception_message,
          resolved_at,
          created_at
        FROM {qname(app_db())}.app_error_logs
        ORDER BY app_error_log_id DESC
        LIMIT %s
        """,
        (limit,),
    )
    return [dict(row) for row in cur.fetchall()]


def load_app_error_log_detail(cur: Any, *, log_id: str) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT
          app_error_log_id,
          log_id,
          status_code,
          method,
          path,
          query_string,
          app_user_id,
          employee_no,
          client_ip,
          user_agent,
          exception_type,
          exception_message,
          traceback_text,
          admin_note,
          resolved_at,
          created_at
        FROM {qname(app_db())}.app_error_logs
        WHERE log_id = %s
        LIMIT 1
        """,
        (log_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def upsert_app_setting(
    cur: Any,
    *,
    setting_key: str,
    setting_value: str,
    value_type: str,
    setting_group: str,
    description: str,
    updated_by_app_user_id: int,
) -> None:
    cur.execute(
        """
        INSERT INTO app_settings (
          setting_key,
          setting_value,
          value_type,
          setting_group,
          description,
          updated_by_app_user_id
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          setting_value = VALUES(setting_value),
          value_type = VALUES(value_type),
          setting_group = VALUES(setting_group),
          description = VALUES(description),
          updated_by_app_user_id = VALUES(updated_by_app_user_id)
        """,
        (setting_key, setting_value, value_type, setting_group, description, updated_by_app_user_id),
    )


def audit_enabled(cur: Any) -> bool:
    return get_app_setting(cur, app_db=app_db(), setting_key="personal_info_audit_enabled", default="1") == "1"


def log_audit(
    cur: Any,
    *,
    request: Request,
    user: dict[str, Any] | None,
    action_code: str,
    target_schema: str | None = None,
    target_table: str | None = None,
    target_id: str | None = None,
    after: dict[str, Any] | None = None,
) -> None:
    cur.execute(
        f"""
        INSERT INTO {qname(app_db())}.app_audit_logs (
          app_user_id,
          employee_no,
          action_code,
          target_schema,
          target_table,
          target_id,
          after_json,
          client_ip,
          user_agent
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            None if not user else int(user["app_user_id"]),
            None if not user else str(user.get("employee_no") or ""),
            action_code,
            target_schema,
            target_table,
            target_id,
            json.dumps(after or {}, ensure_ascii=False),
            client_ip(request),
            request.headers.get("user-agent"),
        ),
    )


def log_app_operation(
    *,
    request: Request,
    user: dict[str, Any],
    action_code: str,
    target_schema: str | None = None,
    target_table: str | None = None,
    target_id: str | None = None,
    after: dict[str, Any] | None = None,
) -> None:
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            if audit_enabled(cur):
                log_audit(
                    cur,
                    request=request,
                    user=user,
                    action_code=action_code,
                    target_schema=target_schema,
                    target_table=target_table,
                    target_id=target_id,
                    after=after,
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def log_personal_info_view(
    cur: Any,
    *,
    request: Request,
    user: dict[str, Any],
    action_code: str,
    cases: list[dict[str, Any]],
    list_id: int | None = None,
) -> None:
    if not audit_enabled(cur):
        return
    for case in cases:
        log_audit(
            cur,
            request=request,
            user=user,
            action_code=action_code,
            target_schema=health_db(),
            target_table="exam_export_cases",
            target_id=str(case.get("exam_export_case_id") or ""),
            after={
                "xml_export_list_id": list_id,
                "exam_export_case_id": case.get("exam_export_case_id"),
                "hia_subscriber_id": case.get("hia_subscriber_id"),
                "person_id_custom": case.get("person_id_custom"),
                "exam_date": str(case.get("exam_date") or ""),
                "exam_facility_id": case.get("exam_facility_id"),
            },
        )


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


def list_status_label(status: str | None) -> str:
    labels = {
        "DRAFT": "下書き",
        "READY": "出力待ち",
        "EXPORTING": "出力中",
        "EXPORTED": "出力済み",
        "PARTIAL": "一部出力",
        "ERROR": "エラー",
        "CANCELLED": "取消",
    }
    return labels.get(status or "", status or "")


def readiness_label(status: str | None) -> str:
    labels = {
        "EXPORT_READY": "READY",
        "APPROVED_WITH_REASON": "理由ありOK",
        "PENDING": "確認待ち",
        "BLOCKED": "BLOCKED",
        "EXPORTED": "出力済み",
    }
    return labels.get(status or "", status or "")


def check_result_label(status: str | None) -> str:
    labels = {
        "OK": "OK",
        "NG": "NG",
        "PENDING": "未判定",
        "NOT_APPLICABLE": "対象外",
        "UNDETERMINABLE": "判定不能",
    }
    return labels.get(status or "", status or "")


def case_review_status_label(status: str | None) -> str:
    labels = {
        "NONE": "未処理",
        "NEEDS_CONFIRMATION": "確認待ち",
        "APPROVED_WITH_REASON": "理由ありOK",
        "WAITING_RESUBMISSION": "再提出待ち",
        "RESUBMITTED": "再提出済み",
        "EXCLUDED": "除外",
        "RESOLVED_BY_SOURCE_VALUE": "受領値で解決",
    }
    return labels.get(status or "", status or "")


def case_check_scope_label(scope: str | None) -> str:
    labels = {
        "ARTICLE44": "法定チェック",
        "SPECIFIC_HEALTH_CHECKUP": "特定健診チェック",
        "FUND_DELIVERY": "健保納品チェック",
        "HIA_UPLOAD": "HIAアップロード確認",
    }
    return labels.get(scope or "", scope or "")


def normalize_specific_check_result(status: str | None, reason: str | None = None) -> str:
    raw_status = str(status or "").strip()
    reason_text = str(reason or "")
    if raw_status == "OK" and reason_text.startswith("対象外:"):
        return "NOT_APPLICABLE"
    if raw_status in {"OK", "NG", "NOT_APPLICABLE", "UNDETERMINABLE", "PENDING"}:
        return raw_status
    if not raw_status:
        return "PENDING"
    return "UNDETERMINABLE"


def specific_check_result_sql(alias: str = "ecr") -> str:
    """Normalize DB specific check status while keeping old summary fallback."""

    return f"""
    CASE
      WHEN {alias}.specific_check_result = 'OK'
       AND COALESCE({alias}.specific_reason_summary, '') LIKE '対象外:%'
        THEN 'NOT_APPLICABLE'
      WHEN COALESCE({alias}.specific_check_result, '') IN ('OK', 'NG', 'NOT_APPLICABLE', 'UNDETERMINABLE', 'PENDING')
        THEN COALESCE({alias}.specific_check_result, 'PENDING')
      WHEN {alias}.specific_check_result IS NULL OR {alias}.specific_check_result = ''
        THEN 'PENDING'
      ELSE 'UNDETERMINABLE'
    END
    """


def check_result_status_class(status: str | None) -> str:
    classes = {
        "OK": "status-ready",
        "NG": "status-danger",
        "NOT_APPLICABLE": "status-muted",
        "UNDETERMINABLE": "status-danger",
        "PENDING": "status-pending",
    }
    return classes.get(status or "", "status-pending")


def split_filter_values(value: str | None) -> list[str]:
    seen: set[str] = set()
    values: list[str] = []
    for item in re.split(r"[\s,，、]+", str(value or "")):
        text = item.strip()
        if not text or text in seen:
            continue
        seen.add(text)
        values.append(text)
    return values


templates.env.globals["list_status_label"] = list_status_label
templates.env.globals["readiness_label"] = readiness_label
templates.env.globals["check_result_label"] = check_result_label
templates.env.globals["case_review_status_label"] = case_review_status_label
templates.env.globals["case_check_scope_label"] = case_check_scope_label
templates.env.globals["check_result_status_class"] = check_result_status_class
templates.env.globals["normalize_specific_check_result"] = normalize_specific_check_result


def fund_delivery_status_label(status: str | None) -> str:
    labels = {
        "DRAFT": "下書き",
        "READY": "出力待ち",
        "CREATED": "作成済み",
        "SUBMITTED": "提出済み",
        "PARTIAL_SUBMITTED": "一部提出済み",
        "PENDING": "保留",
        "SUBMISSION_ERROR": "提出エラー",
        "PARTIAL_ERROR": "一部エラー",
        "ERROR": "エラー",
        "IMPORTED": "取込済み",
        "PROCESSING": "処理中",
        "PARSED": "読取OK",
    }
    return labels.get(status or "", status or "")


templates.env.globals["fund_delivery_status_label"] = fund_delivery_status_label


def hia_upload_status_label(status: str | None) -> str:
    labels = {
        "PENDING": "未アップロード",
        "UPLOADED": "アップロード済み",
        "UPLOAD_ERROR": "アップロードエラー",
        "PARTIAL": "一部エラー",
        "CONFIRMED": "確認済み",
        "EXCLUDED": "対象外",
    }
    return labels.get(status or "", status or "")


templates.env.globals["hia_upload_status_label"] = hia_upload_status_label


def external_feedback_status_label(status: str | None) -> str:
    labels = {
        "OPEN": "未対応",
        "IN_PROGRESS": "対応中",
        "RESOLVED": "解決済み",
        "CLOSED": "クローズ",
        "CANCELLED": "取消",
        "CONFIRMED": "確認済み",
        "FIX_PLANNED": "修正予定",
        "WAITING_RESUBMISSION": "再提出待ち",
        "RESUBMITTED": "再提出済み",
        "WONT_FIX": "対応しない",
    }
    return labels.get(status or "", status or "")


def external_feedback_source_label(source: str | None) -> str:
    labels = {
        "HIA_UPLOAD": "HIAアップロード",
        "FUND_DELIVERY": "健保納品",
        "EMPLOYER_DELIVERY": "事業所納品",
        "MANUAL": "手動記帳",
    }
    return labels.get(source or "", source or "")


def external_feedback_category_label(category: str | None) -> str:
    labels = {
        "SUBSCRIBER": "加入者",
        "BASIC_INFO": "基本情報",
        "EXAM_ITEM": "検査項目",
        "XML_SCHEMA": "XMLスキーマ",
        "UPLOAD": "アップロード",
        "DELIVERY": "納品",
        "OTHER": "その他",
    }
    return labels.get(category or "", category or "")


templates.env.globals["external_feedback_status_label"] = external_feedback_status_label
templates.env.globals["external_feedback_source_label"] = external_feedback_source_label
templates.env.globals["external_feedback_category_label"] = external_feedback_category_label

PREFECTURE_OPTIONS = [
    "北海道",
    "青森県",
    "岩手県",
    "宮城県",
    "秋田県",
    "山形県",
    "福島県",
    "茨城県",
    "栃木県",
    "群馬県",
    "埼玉県",
    "千葉県",
    "東京都",
    "神奈川県",
    "新潟県",
    "富山県",
    "石川県",
    "福井県",
    "山梨県",
    "長野県",
    "岐阜県",
    "静岡県",
    "愛知県",
    "三重県",
    "滋賀県",
    "京都府",
    "大阪府",
    "兵庫県",
    "奈良県",
    "和歌山県",
    "鳥取県",
    "島根県",
    "岡山県",
    "広島県",
    "山口県",
    "徳島県",
    "香川県",
    "愛媛県",
    "高知県",
    "福岡県",
    "佐賀県",
    "長崎県",
    "熊本県",
    "大分県",
    "宮崎県",
    "鹿児島県",
    "沖縄県",
]


FUND_DELIVERY_CONFIG_PATH = REPO_ROOT / "scripts" / "hia" / "config" / "fund_delivery.yml"
HIA_EXPORT_DIR = REPO_ROOT / "data" / "hia_export"
APP_DATA_DIR = REPO_ROOT / "data"
HIA_XML_REVIEW_EXPORT_ROOT_DIR = REPO_ROOT / "data" / "hia_xml_review_exports"
HIA_XML_ZIP_CHECK_UPLOAD_DIR = REPO_ROOT / "data" / "hia_xml_zip_checks" / "uploads"
HIA_XML_ZIP_CHECK_ROOT_DIR = REPO_ROOT / "data" / "hia_xml_zip_checks"


def _config_path(value: Any, default: Path) -> Path:
    if value in (None, ""):
        return default
    path = Path(str(value)).expanduser()
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def load_fund_delivery_page_config() -> dict[str, Any]:
    return load_yaml_config(FUND_DELIVERY_CONFIG_PATH)


def fund_delivery_section(config: dict[str, Any], key: str) -> dict[str, Any]:
    section = config.get(key) or {}
    if not isinstance(section, dict):
        raise ValueError(f"{key} must be a mapping in fund_delivery.yml")
    return section


def latest_fund_delivery_list_id(cur: Any) -> int | None:
    cur.execute(
        """
        SELECT delivery_list_id
          FROM fund_delivery_lists
         WHERE list_status IN ('READY', 'CREATED')
         ORDER BY delivery_list_id DESC
         LIMIT 1
        """
    )
    row = cur.fetchone()
    return int(row["delivery_list_id"]) if row else None


def ready_fund_delivery_list_ids(cur: Any) -> list[int]:
    cur.execute(
        """
        SELECT delivery_list_id
          FROM fund_delivery_lists
         WHERE list_status IN ('READY', 'CREATED')
         ORDER BY
           CASE WHEN exam_month IS NULL THEN 1 ELSE 0 END,
           exam_month,
           delivery_list_id
        """
    )
    return [int(row["delivery_list_id"]) for row in cur.fetchall() or []]


def latest_fund_delivery_exported_list_id(cur: Any) -> int | None:
    cur.execute(
        """
        SELECT l.delivery_list_id
          FROM fund_delivery_lists l
          JOIN fund_delivery_runs r
            ON r.delivery_list_id = l.delivery_list_id
         WHERE r.delivery_status IN ('CREATED', 'PARTIAL_SUBMITTED', 'PENDING', 'SUBMISSION_ERROR')
         ORDER BY r.delivery_run_id DESC
         LIMIT 1
        """
    )
    row = cur.fetchone()
    return int(row["delivery_list_id"]) if row else None


def load_fund_delivery_summary(cur: Any) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for table_name, key in (
        ("hia_download_zips", "download_zips"),
        ("hia_download_xmls", "download_xmls"),
        ("fund_delivery_lists", "delivery_lists"),
        ("fund_delivery_runs", "delivery_runs"),
        ("fund_delivery_members", "delivery_members"),
    ):
        cur.execute(f"SELECT COUNT(*) AS cnt FROM {table_name}")
        summary[key] = int((cur.fetchone() or {}).get("cnt") or 0)
    return summary


def load_fund_delivery_zip_rows(cur: Any, *, limit: int = 20) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
          download_zip_id,
          event_id,
          insurer_number,
          facility_code,
          folder_name,
          zip_name,
          dl_date,
          send_seq,
          import_status,
          import_reason,
          xml_count_total,
          xml_count_success,
          xml_count_error,
          updated_at
        FROM hia_download_zips
        ORDER BY download_zip_id DESC
        LIMIT %s
        """,
        (limit,),
    )
    return [dict(row) for row in cur.fetchall()]


def load_fund_delivery_list_rows(cur: Any, *, limit: int = 20) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
          l.delivery_list_id,
          l.event_id,
          l.insurer_number,
          l.list_name,
          l.list_status,
          l.output_mode,
          l.exam_month,
          l.delivery_policy,
          l.same_exam_date_policy,
          l.created_at,
          COUNT(lm.delivery_list_member_id) AS member_count
        FROM fund_delivery_lists l
        LEFT JOIN fund_delivery_list_members lm
          ON lm.delivery_list_id = l.delivery_list_id
        GROUP BY
          l.delivery_list_id,
          l.event_id,
          l.insurer_number,
          l.list_name,
          l.list_status,
          l.output_mode,
          l.exam_month,
          l.delivery_policy,
          l.same_exam_date_policy,
          l.created_at
        ORDER BY l.delivery_list_id DESC
        LIMIT %s
        """,
        (limit,),
    )
    return [dict(row) for row in cur.fetchall()]


def load_fund_delivery_run_rows(cur: Any, *, limit: int = 20) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
          r.delivery_run_id,
          r.delivery_list_id,
          l.list_name,
          r.event_id,
          r.insurer_number,
          r.output_mode,
          r.exam_month,
          r.output_zip_name,
          r.output_zip_path,
          r.delivery_status,
          r.delivery_xml_count,
          r.delivery_person_count,
          r.created_at,
          r.updated_at
        FROM fund_delivery_runs r
        LEFT JOIN fund_delivery_lists l
          ON l.delivery_list_id = r.delivery_list_id
        ORDER BY r.delivery_run_id DESC
        LIMIT %s
        """,
        (limit,),
    )
    return [dict(row) for row in cur.fetchall()]


def load_fund_delivery_member_rows(cur: Any, *, limit: int = 120) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
          m.delivery_member_id,
          m.delivery_run_id,
          r.delivery_list_id,
          l.list_name,
          m.person_year_id,
          py.person_id_custom,
          py.name_kana_norm,
          py.gender_code,
          py.birthdate,
          py.insurance_symbol_match,
          py.insurance_number_match,
          m.xml_filename,
          m.facility_code,
          m.facility_name,
          m.exam_date,
          m.exam_month,
          m.member_status,
          m.member_reason,
          m.submitted_at,
          m.submitted_by,
          m.submission_note,
          m.updated_at
        FROM fund_delivery_members m
        JOIN fund_delivery_runs r
          ON r.delivery_run_id = m.delivery_run_id
        LEFT JOIN fund_delivery_lists l
          ON l.delivery_list_id = r.delivery_list_id
        LEFT JOIN hia_person_years py
          ON py.person_year_id = m.person_year_id
        ORDER BY m.delivery_member_id DESC
        LIMIT %s
        """,
        (limit,),
    )
    return [dict(row) for row in cur.fetchall()]


def load_hia_upload_summary(cur: Any) -> dict[str, int]:
    cur.execute(
        f"""
        SELECT
          COUNT(*) AS zip_count,
          SUM(CASE WHEN hia_upload_status = 'UPLOADED' THEN 1 ELSE 0 END) AS zip_uploaded_count,
          SUM(CASE WHEN hia_upload_status = 'UPLOAD_ERROR' THEN 1 ELSE 0 END) AS zip_error_count,
          COALESCE(SUM(member_count), 0) AS member_count
        FROM {qname(health_db())}.xml_export_zips
        """
    )
    zip_row = cur.fetchone() or {}
    cur.execute(
        f"""
        SELECT
          COUNT(*) AS member_total,
          SUM(CASE WHEN hia_upload_status = 'UPLOADED' THEN 1 ELSE 0 END) AS member_uploaded_count,
          SUM(CASE WHEN hia_upload_status = 'UPLOAD_ERROR' THEN 1 ELSE 0 END) AS member_error_count
        FROM {qname(health_db())}.xml_export_members
        """
    )
    member_row = cur.fetchone() or {}
    return {
        "zip_count": int(zip_row.get("zip_count") or 0),
        "zip_uploaded_count": int(zip_row.get("zip_uploaded_count") or 0),
        "zip_error_count": int(zip_row.get("zip_error_count") or 0),
        "member_count": int(member_row.get("member_total") or zip_row.get("member_count") or 0),
        "member_uploaded_count": int(member_row.get("member_uploaded_count") or 0),
        "member_error_count": int(member_row.get("member_error_count") or 0),
    }


def load_hia_upload_zip_rows(cur: Any, *, limit: int = 80) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          zez.xml_export_zip_id,
          zez.xml_export_list_id,
          xel.list_name AS xml_export_list_name,
          zez.event_id,
          zez.exam_facility_id,
          zez.facility_code,
          zez.facility_name,
          zez.facility_folder_name,
          zez.insurer_number,
          zez.file_date,
          zez.split_no,
          zez.zip_file_name,
          zez.zip_path,
          zez.member_count,
          zez.hia_upload_status,
          zez.hia_uploaded_at,
          zez.hia_uploaded_by,
          zez.hia_upload_checked_at,
          zez.hia_upload_checked_by,
          zez.hia_upload_error_summary,
          zez.hia_upload_note,
          zez.created_at,
          COUNT(zem.xml_export_member_id) AS member_rows,
          SUM(CASE WHEN zem.hia_upload_status = 'UPLOADED' THEN 1 ELSE 0 END) AS uploaded_members,
          SUM(CASE WHEN zem.hia_upload_status = 'UPLOAD_ERROR' THEN 1 ELSE 0 END) AS error_members
        FROM {qname(health_db())}.xml_export_zips AS zez
        LEFT JOIN {qname(health_db())}.ops_xml_export_lists AS xel
          ON xel.xml_export_list_id = zez.xml_export_list_id
        LEFT JOIN {qname(health_db())}.xml_export_members AS zem
          ON zem.xml_export_zip_id = zez.xml_export_zip_id
        GROUP BY
          zez.xml_export_zip_id,
          zez.xml_export_list_id,
          xel.list_name,
          zez.event_id,
          zez.exam_facility_id,
          zez.facility_code,
          zez.facility_name,
          zez.facility_folder_name,
          zez.insurer_number,
          zez.file_date,
          zez.split_no,
          zez.zip_file_name,
          zez.zip_path,
          zez.member_count,
          zez.hia_upload_status,
          zez.hia_uploaded_at,
          zez.hia_uploaded_by,
          zez.hia_upload_checked_at,
          zez.hia_upload_checked_by,
          zez.hia_upload_error_summary,
          zez.hia_upload_note,
          zez.created_at
        ORDER BY zez.xml_export_zip_id DESC
        LIMIT %s
        """,
        (limit,),
    )
    rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        row["zip_dir_path"] = parent_path_text(row.get("zip_path"))
    return rows


def load_hia_upload_member_rows(cur: Any, *, limit: int = 240) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          zem.xml_export_member_id,
          zem.xml_export_zip_id,
          zez.xml_export_list_id,
          xel.list_name AS xml_export_list_name,
          zem.event_id,
          zem.ledger_type,
          zem.ledger_id,
          zem.source_file_receipt_id,
          zem.subscriber_id,
          zem.hia_subscriber_id,
          zem.person_xml_file_name,
          zem.report_category_code,
          zem.program_type_code,
          zem.hia_upload_status,
          zem.hia_upload_error_code,
          zem.hia_upload_error_message,
          zem.hia_upload_note,
          zem.hia_uploaded_at,
          zem.hia_uploaded_by,
          zem.created_at,
          zez.zip_file_name,
          zez.facility_code,
          zez.facility_name,
          zez.file_date,
          eec.exam_export_case_id,
          eec.name_kana_export_value,
          eec.name_full_raw,
          eec.insurance_symbol_export_value,
          eec.insurance_number_export_value,
          eec.exam_date,
          eec.export_readiness_status
        FROM {qname(health_db())}.xml_export_members AS zem
        INNER JOIN {qname(health_db())}.xml_export_zips AS zez
          ON zez.xml_export_zip_id = zem.xml_export_zip_id
        LEFT JOIN {qname(health_db())}.ops_xml_export_lists AS xel
          ON xel.xml_export_list_id = zez.xml_export_list_id
        LEFT JOIN {qname(health_db())}.exam_export_cases AS eec
          ON zem.ledger_type = 'CASE'
         AND zem.ledger_id = eec.exam_export_case_id
        ORDER BY zem.xml_export_member_id DESC
        LIMIT %s
        """,
        (limit,),
    )
    return [dict(row) for row in cur.fetchall()]


def load_hia_upload_page_data(cur: Any) -> dict[str, Any]:
    zips = load_hia_upload_zip_rows(cur)
    members = load_hia_upload_member_rows(cur)
    members_by_zip: dict[int, list[dict[str, Any]]] = {}
    for member in members:
        zip_id = int(member.get("xml_export_zip_id") or 0)
        members_by_zip.setdefault(zip_id, []).append(member)
    return {
        "summary": load_hia_upload_summary(cur),
        "zips": zips,
        "members": members,
        "members_by_zip": members_by_zip,
    }


def _optional_int(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    return int(text)


def parent_path_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    separator_index = max(text.rfind("/"), text.rfind("\\"))
    if separator_index <= 0:
        return ""
    return text[:separator_index]


PERSON_SELECTION_COLUMNS = (
    ("unused", "未使用"),
    ("insurer_number", "保険者番号"),
    ("insurance_symbol", "記号"),
    ("insurance_number", "番号"),
    ("insurance_branch_number", "枝番"),
    ("name_kana", "氏名カナ"),
    ("name_full", "氏名"),
    ("birthdate", "生年月日"),
    ("gender", "性別"),
    ("hia_subscriber_id", "HIA加入者ID"),
    ("subscriber_id", "subscriber_id"),
    ("case_id", "case_id"),
    ("exam_date", "健診実施日"),
    ("facility_code", "健診機関コード"),
    ("employee_code", "社員番号"),
)


PERSON_SELECTION_STATUS_LABELS = {
    "READY": "追加OK",
    "ALREADY_ADDED": "既に追加済み",
    "MULTIPLE": "候補複数",
    "NOT_FOUND": "未突合",
    "INSUFFICIENT": "入力不足",
    "CASE_NOT_FOUND": "caseなし",
    "PARSE_ERROR": "解析エラー",
}


templates.env.globals["person_selection_status_label"] = lambda status: PERSON_SELECTION_STATUS_LABELS.get(
    status or "", status or ""
)


def event_insurer_number_from_options(events: list[Mapping[str, Any]], event_id: int) -> str:
    for event in events:
        if int(event.get("event_id") or 0) == event_id:
            return str(event.get("insurer_number") or "").strip()
    return ""


def split_person_selection_line(line: str, *, delimiter: str, custom_delimiter: str = "") -> list[str]:
    if delimiter == "tab":
        return line.split("\t")
    if delimiter == "comma":
        return line.split(",")
    if delimiter == "hyphen":
        return line.split("-")
    if delimiter == "space":
        return re.split(r"\s+", line.strip()) if line.strip() else []
    if delimiter == "custom" and custom_delimiter:
        return line.split(custom_delimiter)
    return line.split("\t")


def normalize_person_selection_raw(row: Mapping[str, Any], *, fixed_insurer_number: str) -> dict[str, Any]:
    insurer_raw = str(row.get("insurer_number") or fixed_insurer_number or "").strip()
    symbol_raw = str(row.get("insurance_symbol") or "").strip()
    number_raw = str(row.get("insurance_number") or "").strip()
    kana_raw = str(row.get("name_kana") or "").strip()
    birth_raw = str(row.get("birthdate") or "").strip()
    gender_raw = str(row.get("gender") or "").strip()

    insurer = normalize_insurer_number(insurer_raw) if insurer_raw else {"ok": False, "match": None, "reason": "missing"}
    symbol = normalize_insurance_symbol(symbol_raw) if symbol_raw else {"ok": False, "match": None, "reason": "missing"}
    number = normalize_insurance_number(number_raw) if number_raw else {"ok": False, "match": None, "reason": "missing"}
    kana = normalize_name_kana_full(kana_raw) if kana_raw else {"ok": False, "match": None, "reason": "missing"}
    birth = normalize_date_to_ymd_and_compact(birth_raw, purpose="birthdate") if birth_raw else {
        "ok": False,
        "match": None,
        "reason": "missing",
    }
    gender = normalize_gender_code(gender_raw) if gender_raw else {"ok": False, "match": None, "reason": "missing"}

    normalized = {
        "insurer_number": insurer.get("match"),
        "insurance_symbol": symbol.get("match"),
        "insurance_number": number.get("match"),
        "name_kana": kana.get("match"),
        "birthdate": birth.get("match"),
        "gender": gender.get("match"),
        "normalization_errors": [],
    }
    for key, result in (
        ("保険者番号", insurer),
        ("記号", symbol),
        ("番号", number),
        ("氏名カナ", kana),
        ("生年月日", birth),
        ("性別", gender),
    ):
        if str(row.get({
            "保険者番号": "insurer_number",
            "記号": "insurance_symbol",
            "番号": "insurance_number",
            "氏名カナ": "name_kana",
            "生年月日": "birthdate",
            "性別": "gender",
        }[key]) or (fixed_insurer_number if key == "保険者番号" else "")).strip() and not result.get("ok"):
            normalized["normalization_errors"].append(f"{key}:{result.get('reason')}")

    if all(
        normalized.get(field)
        for field in ("insurer_number", "insurance_symbol", "insurance_number", "name_kana", "birthdate", "gender")
    ):
        bundle = generate_identity_bundle(
            birthdate=normalized["birthdate"],
            insurer_number_raw=normalized["insurer_number"],
            insurance_symbol_raw=normalized["insurance_symbol"],
            insurance_number_raw=normalized["insurance_number"],
            name_kana_full_raw=normalized["name_kana"],
            gender_code=normalized["gender"],
        )
        normalized["person_id_custom"] = bundle.get("person_id_custom")
        normalized["identity_hash"] = bundle.get("identity_hash")
        if not bundle.get("ok"):
            normalized["normalization_errors"].append(str(bundle.get("reason") or "identity生成失敗"))
    else:
        normalized["person_id_custom"] = None
        normalized["identity_hash"] = None
    return normalized


def parse_person_selection_paste(
    *,
    raw_text: str,
    delimiter: str,
    custom_delimiter: str,
    has_header: bool,
    column_map: list[str],
    fixed_insurer_number: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    lines = [line for line in raw_text.splitlines() if line.strip()]
    if has_header and lines:
        lines = lines[1:]
    for index, line in enumerate(lines, start=1):
        values = [part.strip() for part in split_person_selection_line(line, delimiter=delimiter, custom_delimiter=custom_delimiter)]
        parsed: dict[str, Any] = {}
        for col_index, value in enumerate(values):
            field = column_map[col_index] if col_index < len(column_map) else "unused"
            if field == "unused" or value == "":
                continue
            if field in parsed and parsed[field]:
                parsed[f"{field}_duplicate"] = value
                continue
            parsed[field] = value
        normalized = normalize_person_selection_raw(parsed, fixed_insurer_number=fixed_insurer_number)
        rows.append(
            {
                "row_no": index,
                "raw_line": line,
                "values": values,
                "parsed": parsed,
                "normalized": normalized,
                "status": "INSUFFICIENT",
                "reason": "",
                "candidates": [],
                "case_candidates": [],
            }
        )
    return rows


def build_person_selection_single_row(*, form: Mapping[str, Any], fixed_insurer_number: str) -> list[dict[str, Any]]:
    parsed = {
        "case_id": str(form.get("single_case_id") or "").strip(),
        "subscriber_id": str(form.get("single_subscriber_id") or "").strip(),
        "hia_subscriber_id": str(form.get("single_hia_subscriber_id") or "").strip(),
        "employee_code": str(form.get("single_employee_code") or "").strip(),
        "insurer_number": str(form.get("single_insurer_number") or fixed_insurer_number or "").strip(),
        "insurance_symbol": str(form.get("single_insurance_symbol") or "").strip(),
        "insurance_number": str(form.get("single_insurance_number") or "").strip(),
        "name_kana": str(form.get("single_name_kana") or "").strip(),
        "birthdate": str(form.get("single_birthdate") or "").strip(),
        "gender": str(form.get("single_gender") or "").strip(),
    }
    parsed = {key: value for key, value in parsed.items() if value}
    normalized = normalize_person_selection_raw(parsed, fixed_insurer_number=fixed_insurer_number)
    return [
        {
            "row_no": 1,
            "raw_line": " / ".join(f"{key}={value}" for key, value in parsed.items()),
            "values": list(parsed.values()),
            "parsed": parsed,
            "normalized": normalized,
            "status": "INSUFFICIENT",
            "reason": "",
            "candidates": [],
            "case_candidates": [],
        }
    ]


def load_person_selection_case(cur: Any, *, case_id: int) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT
          eec.exam_export_case_id,
          eec.event_id,
          eec.subscriber_id,
          eec.hia_subscriber_id,
          eec.person_id_custom,
          eec.identity_hash,
          eec.name_full_raw,
          eec.name_kana_raw,
          eec.birthdate,
          eec.gender_code,
          eec.facility_name,
          eec.facility_code,
          eec.exam_date,
          eec.export_readiness_status,
          eec.xml_export_status
        FROM {qname(health_db())}.exam_export_cases AS eec
        WHERE eec.exam_export_case_id = %s
        """,
        (case_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def load_person_selection_cases_for_subscriber(
    cur: Any,
    *,
    event_id: int,
    subscriber_id: int,
    limit: int = 20,
) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          exam_export_case_id,
          event_id,
          subscriber_id,
          hia_subscriber_id,
          person_id_custom,
          identity_hash,
          name_full_raw,
          name_kana_raw,
          birthdate,
          gender_code,
          facility_name,
          facility_code,
          exam_date,
          export_readiness_status,
          xml_export_status
        FROM {qname(health_db())}.exam_export_cases
        WHERE event_id = %s
          AND subscriber_id = %s
        ORDER BY exam_date DESC, exam_export_case_id DESC
        LIMIT %s
        """,
        (event_id, subscriber_id, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def load_manual_exam_entry_cases_for_subscriber(
    cur: Any,
    *,
    event_id: int,
    subscriber_id: int,
    limit: int = 10,
) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          eec.exam_export_case_id,
          eec.event_id,
          eec.subscriber_id,
          eec.hia_subscriber_id,
          eec.name_full_raw,
          eec.name_kana_raw,
          eec.facility_name,
          eec.facility_code,
          eec.exam_date,
          eec.source_mode,
          eec.case_status,
          eec.merge_status,
          eec.value_build_status,
          eec.case_value_count,
          eec.export_readiness_status,
          eec.xml_export_status,
          COALESCE(ecr.legal_check_result, 'PENDING') AS legal_check_result,
          ecr.legal_reason_summary,
          COALESCE(ecr.specific_check_result, 'PENDING') AS specific_check_result,
          ecr.specific_reason_summary,
          COALESCE(src.source_count, 0) AS source_count,
          COALESCE(src.xml_count, 0) AS xml_count,
          COALESCE(src.csv_count, 0) AS csv_count,
          COALESCE(src.paper_count, 0) AS paper_count
        FROM {qname(health_db())}.exam_export_cases AS eec
        LEFT JOIN (
          SELECT r1.*
          FROM {qname(health_db())}.exam_check_results AS r1
          INNER JOIN (
            SELECT exam_export_case_id, MAX(id) AS max_id
            FROM {qname(health_db())}.exam_check_results
            WHERE ledger_type = 'EXPORT_CASE'
              AND exam_export_case_id IS NOT NULL
            GROUP BY exam_export_case_id
          ) AS latest
            ON latest.max_id = r1.id
        ) AS ecr
          ON ecr.exam_export_case_id = eec.exam_export_case_id
        LEFT JOIN (
          SELECT
            exam_export_case_id,
            COUNT(*) AS source_count,
            SUM(CASE WHEN source_type = 'XML' THEN 1 ELSE 0 END) AS xml_count,
            SUM(CASE WHEN source_type = 'CSV' THEN 1 ELSE 0 END) AS csv_count,
            SUM(CASE WHEN source_type = 'PAPER' THEN 1 ELSE 0 END) AS paper_count
          FROM {qname(health_db())}.exam_export_case_sources
          GROUP BY exam_export_case_id
        ) AS src
          ON src.exam_export_case_id = eec.exam_export_case_id
        WHERE eec.event_id = %s
          AND eec.subscriber_id = %s
        ORDER BY eec.exam_date DESC, eec.exam_export_case_id DESC
        LIMIT %s
        """,
        (event_id, subscriber_id, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def search_person_selection_subscribers(
    cur: Any,
    *,
    row: Mapping[str, Any],
    normalized: Mapping[str, Any],
    event_id: int,
    limit: int = 20,
) -> tuple[list[dict[str, Any]], str]:
    parsed = row.get("parsed", {}) if isinstance(row.get("parsed"), Mapping) else {}
    where_parts: list[str] = []
    params: list[Any] = []
    matched_parts: list[str] = []
    subscriber_id = str(parsed.get("subscriber_id") or "").strip()
    case_id = str(parsed.get("case_id") or "").strip()
    if case_id:
        return [], "case_id"
    if subscriber_id.isdigit():
        where_parts.append("s.id = %s")
        params.append(int(subscriber_id))
        matched_parts.append("subscriber_id")
    elif str(parsed.get("hia_subscriber_id") or "").strip():
        where_parts.append("s.hia_subscriber_id = %s")
        params.append(str(parsed.get("hia_subscriber_id")).strip())
        matched_parts.append("hia_subscriber_id")
    elif str(parsed.get("employee_code") or "").strip():
        where_parts.append("s.employee_code = %s")
        params.append(str(parsed.get("employee_code")).strip())
        matched_parts.append("employee_code")
    else:
        if normalized.get("insurance_symbol"):
            where_parts.append("(s.insurance_symbol_match = %s OR s.insurance_symbol = %s)")
            params.extend([normalized.get("insurance_symbol"), normalized.get("insurance_symbol")])
            matched_parts.append("記号")
        if normalized.get("insurance_number"):
            where_parts.append("(s.insurance_number_match = %s OR s.insurance_number = %s)")
            params.extend([normalized.get("insurance_number"), normalized.get("insurance_number")])
            matched_parts.append("番号")
        if normalized.get("name_kana"):
            where_parts.append("s.name_kana_full_match LIKE %s")
            params.append(f"%{normalized.get('name_kana')}%")
            matched_parts.append("氏名カナ")
        if normalized.get("birthdate"):
            where_parts.append("s.birth = %s")
            params.append(normalized.get("birthdate"))
            matched_parts.append("生年月日")
        if normalized.get("gender"):
            where_parts.append("s.gender_code = %s")
            params.append(normalized.get("gender"))
            matched_parts.append("性別")
        if not where_parts and normalized.get("identity_hash"):
            where_parts.append("s.identity_hash = %s")
            params.append(normalized.get("identity_hash"))
            matched_parts.append("identity_hash")
        elif not where_parts and normalized.get("person_id_custom"):
            where_parts.append("s.person_id_custom = %s")
            params.append(normalized.get("person_id_custom"))
            matched_parts.append("person_id_custom")

    if not where_parts:
        return [], ""

    subscriber_columns = manual_exam_entry_existing_columns(cur, dev_db(), "subscribers")
    subscriber_name_full_expr = "s.name_kanji_full" if "name_kanji_full" in subscriber_columns else "s.name_full_match"
    subscriber_select = lambda column, alias=None: (
        f"s.{column} AS {alias or column}" if column in subscriber_columns else f"NULL AS {alias or column}"
    )
    subscriber_group = lambda column: f"s.{column}" if column in subscriber_columns else "NULL"
    cur.execute(
        f"""
        SELECT
          s.id AS subscriber_id,
          s.hia_subscriber_id,
          s.person_id_custom,
          s.identity_hash,
          {subscriber_name_full_expr} AS name_full,
          s.name_kana_full,
          s.name_kana_full_match,
          s.birth,
          s.gender_code,
          s.insurance_symbol,
          s.insurance_symbol_match,
          s.insurance_number,
          s.insurance_number_match,
          s.insurance_branchnumber,
          {subscriber_select("employee_code")},
          {subscriber_select("relationship_name")},
          {subscriber_select("qualification_lost_date")},
          COUNT(eec.exam_export_case_id) AS event_case_count,
          MAX(eec.exam_export_case_id) AS latest_case_id
        FROM {qname(dev_db())}.subscribers AS s
        LEFT JOIN {qname(health_db())}.exam_export_cases AS eec
          ON eec.event_id = %s
         AND eec.subscriber_id = s.id
        WHERE {" AND ".join(where_parts)}
        GROUP BY
          s.id,
          s.hia_subscriber_id,
          s.person_id_custom,
          s.identity_hash,
          {subscriber_name_full_expr},
          s.name_kana_full,
          s.name_kana_full_match,
          s.birth,
          s.gender_code,
          s.insurance_symbol,
          s.insurance_symbol_match,
          s.insurance_number,
          s.insurance_number_match,
          s.insurance_branchnumber,
          {subscriber_group("employee_code")},
          {subscriber_group("relationship_name")},
          {subscriber_group("qualification_lost_date")}
        ORDER BY event_case_count DESC, s.id DESC
        LIMIT %s
        """,
        (event_id, *params, limit),
    )
    return [dict(item) for item in cur.fetchall()], "+".join(matched_parts)


def resolve_person_selection_rows(cur: Any, *, event_id: int, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for row in rows:
        parsed = row.get("parsed", {})
        normalized = row.get("normalized", {})
        case_id = str(parsed.get("case_id") or "").strip() if isinstance(parsed, Mapping) else ""
        if case_id:
            if not case_id.isdigit():
                row["status"] = "PARSE_ERROR"
                row["reason"] = "case_idが数値ではありません"
                continue
            case = load_person_selection_case(cur, case_id=int(case_id))
            if not case:
                row["status"] = "NOT_FOUND"
                row["reason"] = "case_idに一致するcaseがありません"
                continue
            row["status"] = "READY"
            row["reason"] = "case_id一致"
            row["case_candidates"] = [case]
            continue

        subscribers, matched_by = search_person_selection_subscribers(
            cur,
            row=row,
            normalized=normalized,
            event_id=event_id,
        )
        row["matched_by"] = matched_by
        row["candidates"] = subscribers
        if not subscribers:
            if normalized.get("normalization_errors"):
                row["status"] = "PARSE_ERROR"
                row["reason"] = " / ".join(normalized.get("normalization_errors") or [])
            else:
                row["status"] = "NOT_FOUND" if matched_by else "INSUFFICIENT"
                row["reason"] = "候補が見つかりません" if matched_by else "突合キーが不足しています"
            continue
        if len(subscribers) > 1:
            row["status"] = "MULTIPLE"
            row["reason"] = f"{matched_by}で候補が複数あります"
            continue
        subscriber = subscribers[0]
        cases = load_person_selection_cases_for_subscriber(
            cur,
            event_id=event_id,
            subscriber_id=int(subscriber["subscriber_id"]),
        )
        row["case_candidates"] = cases
        if not cases:
            row["status"] = "CASE_NOT_FOUND"
            row["reason"] = "加入者は見つかりましたが、このeventのcaseがありません"
        else:
            row["status"] = "READY"
            row["reason"] = f"{matched_by}一致"
    return rows


def load_external_feedback_summary(cur: Any) -> dict[str, int]:
    cur.execute(
        f"""
        SELECT
          COUNT(*) AS report_count,
          SUM(CASE WHEN report_status IN ('OPEN', 'IN_PROGRESS') THEN 1 ELSE 0 END) AS active_report_count
        FROM {qname(health_db())}.ops_external_feedback_reports
        """
    )
    report_row = cur.fetchone() or {}
    cur.execute(
        f"""
        SELECT
          COUNT(*) AS item_count,
          SUM(CASE WHEN handling_status IN ('OPEN', 'CONFIRMED', 'FIX_PLANNED', 'WAITING_RESUBMISSION') THEN 1 ELSE 0 END) AS active_item_count,
          SUM(CASE WHEN issue_level = 'ERROR' THEN 1 ELSE 0 END) AS error_item_count,
          SUM(CASE WHEN exam_export_case_id IS NULL THEN 1 ELSE 0 END) AS unlinked_item_count
        FROM {qname(health_db())}.ops_external_feedback_items
        """
    )
    item_row = cur.fetchone() or {}
    return {
        "report_count": int(report_row.get("report_count") or 0),
        "active_report_count": int(report_row.get("active_report_count") or 0),
        "item_count": int(item_row.get("item_count") or 0),
        "active_item_count": int(item_row.get("active_item_count") or 0),
        "error_item_count": int(item_row.get("error_item_count") or 0),
        "unlinked_item_count": int(item_row.get("unlinked_item_count") or 0),
    }


def load_external_feedback_report_rows(cur: Any, *, limit: int = 80) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          r.external_feedback_report_id,
          r.event_id,
          r.feedback_source,
          r.feedback_scope,
          r.report_status,
          r.received_at,
          r.received_from,
          r.channel,
          r.summary,
          r.source_file_name,
          r.xml_export_list_id,
          xel.list_name AS xml_export_list_name,
          r.xml_export_zip_id,
          zez.zip_file_name AS xml_export_zip_name,
          r.fund_delivery_list_id,
          fdl.list_name AS fund_delivery_list_name,
          r.fund_delivery_run_id,
          fdr.output_zip_name AS fund_delivery_zip_name,
          r.created_by,
          r.created_at,
          COUNT(i.external_feedback_item_id) AS item_count,
          SUM(CASE WHEN i.handling_status IN ('OPEN', 'CONFIRMED', 'FIX_PLANNED', 'WAITING_RESUBMISSION') THEN 1 ELSE 0 END) AS active_item_count
        FROM {qname(health_db())}.ops_external_feedback_reports AS r
        LEFT JOIN {qname(health_db())}.ops_external_feedback_items AS i
          ON i.external_feedback_report_id = r.external_feedback_report_id
        LEFT JOIN {qname(health_db())}.ops_xml_export_lists AS xel
          ON xel.xml_export_list_id = r.xml_export_list_id
        LEFT JOIN {qname(health_db())}.xml_export_zips AS zez
          ON zez.xml_export_zip_id = r.xml_export_zip_id
        LEFT JOIN {qname(health_db())}.fund_delivery_lists AS fdl
          ON fdl.delivery_list_id = r.fund_delivery_list_id
        LEFT JOIN {qname(health_db())}.fund_delivery_runs AS fdr
          ON fdr.delivery_run_id = r.fund_delivery_run_id
        GROUP BY
          r.external_feedback_report_id,
          r.event_id,
          r.feedback_source,
          r.feedback_scope,
          r.report_status,
          r.received_at,
          r.received_from,
          r.channel,
          r.summary,
          r.source_file_name,
          r.xml_export_list_id,
          xel.list_name,
          r.xml_export_zip_id,
          zez.zip_file_name,
          r.fund_delivery_list_id,
          fdl.list_name,
          r.fund_delivery_run_id,
          fdr.output_zip_name,
          r.created_by,
          r.created_at
        ORDER BY r.external_feedback_report_id DESC
        LIMIT %s
        """,
        (limit,),
    )
    return [dict(row) for row in cur.fetchall()]


def load_external_feedback_item_rows(cur: Any, *, limit: int = 240) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          i.external_feedback_item_id,
          i.external_feedback_report_id,
          r.feedback_source,
          r.report_status,
          i.event_id,
          i.exam_export_case_id,
          i.xml_export_member_id,
          i.xml_export_zip_id,
          i.fund_delivery_member_id,
          i.issue_level,
          i.issue_category,
          i.handling_status,
          i.external_error_code,
          i.external_message,
          i.namecode,
          i.check_item_code,
          i.source_xml_file_name,
          i.source_zip_file_name,
          i.reported_value,
          i.resolution_note,
          i.assigned_to,
          i.resolved_at,
          i.created_by,
          i.created_at,
          eec.name_kana_export_value,
          eec.name_full_raw,
          eec.hia_subscriber_id,
          eec.exam_date,
          ef.exam_facility_code,
          ef.exam_facility_name AS facility_name
        FROM {qname(health_db())}.ops_external_feedback_items AS i
        JOIN {qname(health_db())}.ops_external_feedback_reports AS r
          ON r.external_feedback_report_id = i.external_feedback_report_id
        LEFT JOIN {qname(health_db())}.exam_export_cases AS eec
          ON eec.exam_export_case_id = i.exam_export_case_id
        LEFT JOIN {qname(master_db())}.exam_facilities AS ef
          ON ef.exam_facility_id = eec.exam_facility_id
        ORDER BY i.external_feedback_item_id DESC
        LIMIT %s
        """,
        (limit,),
    )
    return [dict(row) for row in cur.fetchall()]


def load_hia_upload_member_prefill(cur: Any, *, xml_export_member_id: int) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT
          zem.xml_export_member_id,
          zem.xml_export_zip_id,
          zez.xml_export_list_id,
          zem.event_id,
          zem.ledger_type,
          zem.ledger_id,
          zem.person_xml_file_name,
          zem.hia_upload_error_code,
          zem.hia_upload_error_message,
          zem.hia_upload_note,
          zez.zip_file_name,
          zez.hia_upload_error_summary,
          eec.exam_export_case_id
        FROM {qname(health_db())}.xml_export_members AS zem
        JOIN {qname(health_db())}.xml_export_zips AS zez
          ON zez.xml_export_zip_id = zem.xml_export_zip_id
        LEFT JOIN {qname(health_db())}.exam_export_cases AS eec
          ON zem.ledger_type = 'CASE'
         AND zem.ledger_id = eec.exam_export_case_id
        WHERE zem.xml_export_member_id = %s
        """,
        (xml_export_member_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def search_hia_upload_members_for_feedback(
    cur: Any,
    *,
    event_id: int | None,
    query: str,
    name_kana: str,
    xml_export_member_id: int | None,
    exam_export_case_id: int | None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    where_parts: list[str] = []
    params: list[Any] = []
    if event_id:
        where_parts.append("zem.event_id = %s")
        params.append(event_id)
    if xml_export_member_id:
        where_parts.append("zem.xml_export_member_id = %s")
        params.append(xml_export_member_id)
    if exam_export_case_id:
        where_parts.append("eec.exam_export_case_id = %s")
        params.append(exam_export_case_id)
    if name_kana:
        where_parts.append("eec.name_kana_export_value LIKE %s")
        params.append(f"%{name_kana}%")
    if query:
        like = f"%{query}%"
        where_parts.append(
            """
            (
              zem.hia_subscriber_id LIKE %s
              OR eec.name_kana_export_value LIKE %s
              OR eec.name_full_raw LIKE %s
              OR zem.person_xml_file_name LIKE %s
              OR zez.zip_file_name LIKE %s
              OR zez.facility_name LIKE %s
            )
            """
        )
        params.extend([like, like, like, like, like, like])
    if not where_parts:
        return []
    params.append(limit)
    cur.execute(
        f"""
        SELECT
          zem.xml_export_member_id,
          zem.xml_export_zip_id,
          zez.xml_export_list_id,
          zem.event_id,
          zem.ledger_type,
          zem.ledger_id,
          zem.subscriber_id,
          zem.hia_subscriber_id,
          zem.person_xml_file_name,
          zem.hia_upload_status,
          zem.hia_upload_error_code,
          zem.hia_upload_error_message,
          zez.zip_file_name,
          zez.facility_code,
          zez.facility_name,
          eec.exam_export_case_id,
          eec.name_kana_export_value,
          eec.name_full_raw,
          eec.insurance_symbol_export_value,
          eec.insurance_number_export_value,
          eec.exam_date,
          eec.export_readiness_status,
          eec.xml_export_status
        FROM {qname(health_db())}.xml_export_members AS zem
        INNER JOIN {qname(health_db())}.xml_export_zips AS zez
          ON zez.xml_export_zip_id = zem.xml_export_zip_id
        LEFT JOIN {qname(health_db())}.exam_export_cases AS eec
          ON zem.ledger_type = 'CASE'
         AND zem.ledger_id = eec.exam_export_case_id
        WHERE {" AND ".join(where_parts)}
        ORDER BY zem.xml_export_member_id DESC
        LIMIT %s
        """,
        tuple(params),
    )
    return [dict(row) for row in cur.fetchall()]


def load_external_feedback_page_data(cur: Any, *, query_params: Mapping[str, Any]) -> dict[str, Any]:
    prefill: dict[str, Any] = {
        "feedback_source": str(query_params.get("feedback_source") or "HIA_UPLOAD"),
        "feedback_scope": str(query_params.get("feedback_scope") or "CASE"),
        "issue_level": str(query_params.get("issue_level") or "ERROR"),
        "issue_category": str(query_params.get("issue_category") or "OTHER"),
        "handling_status": str(query_params.get("handling_status") or "OPEN"),
        "xml_export_member_id": str(query_params.get("xml_export_member_id") or ""),
        "xml_export_zip_id": str(query_params.get("xml_export_zip_id") or ""),
        "xml_export_list_id": str(query_params.get("xml_export_list_id") or ""),
        "exam_export_case_id": str(query_params.get("exam_export_case_id") or ""),
    }
    if prefill["xml_export_member_id"]:
        member = load_hia_upload_member_prefill(cur, xml_export_member_id=int(prefill["xml_export_member_id"]))
        if member:
            prefill.update(
                {
                    "event_id": str(member.get("event_id") or ""),
                    "xml_export_zip_id": str(member.get("xml_export_zip_id") or ""),
                    "xml_export_list_id": str(member.get("xml_export_list_id") or ""),
                    "exam_export_case_id": str(member.get("exam_export_case_id") or ""),
                    "source_xml_file_name": str(member.get("person_xml_file_name") or ""),
                    "source_zip_file_name": str(member.get("zip_file_name") or ""),
                    "external_error_code": str(member.get("hia_upload_error_code") or ""),
                    "external_message": str(member.get("hia_upload_error_message") or member.get("hia_upload_error_summary") or ""),
                    "resolution_note": str(member.get("hia_upload_note") or ""),
                }
            )
    return {
        "summary": load_external_feedback_summary(cur),
        "reports": load_external_feedback_report_rows(cur),
        "items": load_external_feedback_item_rows(cur),
        "prefill": prefill,
    }


def create_external_feedback_from_form(cur: Any, *, form: Mapping[str, Any], user: dict[str, Any]) -> tuple[int, int]:
    actor = fund_delivery_actor(user)
    received_at_text = str(form.get("received_at") or "").strip()
    received_at = received_at_text or None
    event_id = _optional_int(form.get("event_id"))
    xml_export_list_id = _optional_int(form.get("xml_export_list_id"))
    xml_export_zip_id = _optional_int(form.get("xml_export_zip_id"))
    xml_export_member_id = _optional_int(form.get("xml_export_member_id"))
    exam_export_case_id = _optional_int(form.get("exam_export_case_id"))
    fund_delivery_list_id = _optional_int(form.get("fund_delivery_list_id"))
    fund_delivery_run_id = _optional_int(form.get("fund_delivery_run_id"))
    fund_delivery_list_member_id = _optional_int(form.get("fund_delivery_list_member_id"))
    fund_delivery_member_id = _optional_int(form.get("fund_delivery_member_id"))
    if xml_export_member_id:
        member = load_hia_upload_member_prefill(cur, xml_export_member_id=xml_export_member_id)
        if member:
            event_id = event_id or _optional_int(member.get("event_id"))
            xml_export_zip_id = xml_export_zip_id or _optional_int(member.get("xml_export_zip_id"))
            xml_export_list_id = xml_export_list_id or _optional_int(member.get("xml_export_list_id"))
            exam_export_case_id = exam_export_case_id or _optional_int(member.get("exam_export_case_id"))
    cur.execute(
        f"""
        INSERT INTO {qname(health_db())}.ops_external_feedback_reports (
          event_id, feedback_source, feedback_scope, report_status,
          received_at, received_from, channel, summary,
          source_file_name, source_file_path,
          xml_export_list_id, xml_export_zip_id,
          fund_delivery_list_id, fund_delivery_run_id,
          created_by, updated_by
        )
        VALUES (%s, %s, %s, 'OPEN', %s, NULLIF(%s, ''), NULLIF(%s, ''), NULLIF(%s, ''),
                NULLIF(%s, ''), NULLIF(%s, ''), %s, %s, %s, %s, %s, %s)
        """,
        (
            event_id,
            str(form.get("feedback_source") or "HIA_UPLOAD"),
            str(form.get("feedback_scope") or "CASE"),
            received_at,
            str(form.get("received_from") or ""),
            str(form.get("channel") or ""),
            str(form.get("summary") or ""),
            str(form.get("source_file_name") or ""),
            str(form.get("source_file_path") or ""),
            xml_export_list_id,
            xml_export_zip_id,
            fund_delivery_list_id,
            fund_delivery_run_id,
            actor,
            actor,
        ),
    )
    report_id = int(cur.lastrowid)
    cur.execute(
        f"""
        INSERT INTO {qname(health_db())}.ops_external_feedback_items (
          external_feedback_report_id, event_id, exam_export_case_id,
          xml_export_list_case_id, xml_export_member_id, xml_export_zip_id,
          fund_delivery_list_member_id, fund_delivery_member_id,
          issue_level, issue_category, handling_status,
          external_error_code, external_message, namecode, check_item_code,
          source_xml_file_name, source_zip_file_name, reported_value,
          resolution_note, assigned_to, created_by, updated_by
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                NULLIF(%s, ''), NULLIF(%s, ''), NULLIF(%s, ''), NULLIF(%s, ''),
                NULLIF(%s, ''), NULLIF(%s, ''), NULLIF(%s, ''),
                NULLIF(%s, ''), NULLIF(%s, ''), %s, %s)
        """,
        (
            report_id,
            event_id,
            exam_export_case_id,
            _optional_int(form.get("xml_export_list_case_id")),
            xml_export_member_id,
            xml_export_zip_id,
            fund_delivery_list_member_id,
            fund_delivery_member_id,
            str(form.get("issue_level") or "ERROR"),
            str(form.get("issue_category") or "OTHER"),
            str(form.get("handling_status") or "OPEN"),
            str(form.get("external_error_code") or ""),
            str(form.get("external_message") or ""),
            str(form.get("namecode") or ""),
            str(form.get("check_item_code") or ""),
            str(form.get("source_xml_file_name") or ""),
            str(form.get("source_zip_file_name") or ""),
            str(form.get("reported_value") or ""),
            str(form.get("resolution_note") or ""),
            str(form.get("assigned_to") or ""),
            actor,
            actor,
        ),
    )
    item_id = int(cur.lastrowid)
    cur.execute(
        f"""
        INSERT INTO {qname(health_db())}.ops_external_feedback_item_audit_logs (
          external_feedback_item_id, action_type, after_status, after_json, changed_by
        )
        VALUES (%s, 'CREATE', %s, %s, %s)
        """,
        (
            item_id,
            str(form.get("handling_status") or "OPEN"),
            json.dumps({"report_id": report_id, "item_id": item_id}, ensure_ascii=False),
            actor,
        ),
    )
    return report_id, item_id


def build_hia_download_import_config(raw: dict[str, Any]) -> HiaDownloadImportConfig:
    section = fund_delivery_section(raw, "download_import")
    event_id = config_value(raw, "event_id", None)
    return HiaDownloadImportConfig(
        project_root=REPO_ROOT,
        input_zip_dir=_config_path(section.get("input_zip_dir"), HIA_EXPORT_DIR / "input_zip"),
        archive_zip_dir=_config_path(section.get("archive_zip_dir"), HIA_EXPORT_DIR / "archive_zip"),
        work_dir=_config_path(section.get("work_dir"), HIA_EXPORT_DIR / "work"),
        event_id=None if event_id in (None, "") else int(event_id),
        archive_mode=str(config_value(section, "archive_mode", "copy")),
        dry_run=config_bool(section, "dry_run", False),
    )


def _string_list(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    if isinstance(value, list | tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()]


def fund_delivery_actor(user: dict[str, Any]) -> str:
    employee_number = str(user.get("employee_number") or "").strip()
    display_name = str(user.get("display_name") or "").strip()
    if employee_number and display_name:
        return f"{employee_number} {display_name}"
    return employee_number or display_name or "health_exam_admin"


def safe_upload_file_name(filename: str | None, *, default: str = "upload.zip") -> str:
    base_name = Path(filename or default).name
    sanitized = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", base_name).strip(" .")
    return sanitized or default


def is_path_under(path: Path, base_dir: Path) -> bool:
    try:
        path_abs = os.path.normcase(os.path.abspath(path.resolve()))
        base_abs = os.path.normcase(os.path.abspath(base_dir.resolve()))
        return os.path.commonpath([path_abs, base_abs]) == base_abs
    except (OSError, ValueError):
        return False


def app_data_path_from_form_value(value: str) -> Path:
    normalized = value.strip().replace("¥", os.sep)
    if os.sep != "\\":
        normalized = normalized.replace("\\", os.sep)
    path = Path(normalized)
    if path.is_absolute():
        return path
    parts = path.parts
    if parts and parts[0] == "data":
        return REPO_ROOT / path
    return APP_DATA_DIR / path


def xml_zip_check_allowed(user: dict[str, Any]) -> bool:
    return has_any_permission(user, ("hia_upload.perform", "hia_upload_status.edit", "users.manage"))


def xml_zip_check_input_allowed(path: Path) -> bool:
    return is_path_under(path, APP_DATA_DIR)


def path_debug_payload(*, submitted_value: str, resolved_path: Path, allowed_base: Path) -> dict[str, Any]:
    def _resolve_text(path: Path) -> str:
        try:
            return str(path.resolve())
        except OSError as exc:
            return f"<resolve_error:{type(exc).__name__}:{exc}>"

    return {
        "submitted_value": submitted_value,
        "resolved_path": str(resolved_path),
        "resolved_absolute_path": _resolve_text(resolved_path),
        "allowed_base": str(allowed_base),
        "allowed_base_absolute_path": _resolve_text(allowed_base),
        "exists": resolved_path.exists(),
        "is_file": resolved_path.is_file(),
        "is_under_data": is_path_under(resolved_path, APP_DATA_DIR),
        "repo_root": str(REPO_ROOT),
        "cwd": str(Path.cwd()),
    }


def safe_form_debug(form: dict[str, str]) -> dict[str, str]:
    safe: dict[str, str] = {}
    for key, value in form.items():
        if key == CSRF_FIELD_NAME:
            safe[key] = "<csrf>"
        else:
            safe[key] = value
    return safe


def path_debug_message(prefix: str, debug: dict[str, Any]) -> str:
    submitted = str(debug.get("submitted_value") or "(空)")
    resolved = str(debug.get("resolved_path") or "(不明)")
    try:
        resolved_short = str(Path(resolved).relative_to(APP_DATA_DIR))
    except (OSError, ValueError):
        resolved_short = resolved
    return (
        f"{prefix} "
        f"送信値={submitted} / 解決後={resolved_short} / "
        f"exists={debug.get('exists')} / is_file={debug.get('is_file')} / "
        f"data配下={debug.get('is_under_data')}"
    )


def serialize_xml_zip_findings(findings: list[Any], *, limit: int = 200) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for finding in findings[:limit]:
        rows.append(
            {
                "severity": finding.severity,
                "check_type": finding.check_type,
                "xml_inner_path": finding.xml_inner_path,
                "namecode": finding.namecode,
                "item_display_name": finding.item_display_name,
                "message": finding.message,
                "value_preview": finding.value_preview,
                "xsd_element": getattr(finding, "xsd_element", None),
                "xsd_attribute": getattr(finding, "xsd_attribute", None),
                "mhlw_byte_length": finding.mhlw_byte_length,
                "max_byte_length": finding.max_byte_length,
                "fixed": finding.fixed,
            }
        )
    return rows


def xml_zip_check_message_label(finding: Any) -> str:
    message = str(finding.message or "")
    if finding.check_type == "XSD":
        if "minLength" in message:
            return "XSD: 必須文字数不足(minLength)"
        if "maxLength" in message:
            return "XSD: 最大文字数超過(maxLength)"
        if "No matching global declaration" in message:
            return "XSD: ルート要素不一致"
        if "This element is not expected" in message:
            return "XSD: 要素位置/順序不一致"
        if "attribute" in message and "not allowed" in message:
            return "XSD: 許可されない属性"
        return "XSD: その他"
    if finding.check_type == "ST_MAX_BYTE_LENGTH_EXCEEDED":
        return "ST/TX文字数超過"
    if finding.check_type == "CODE_SYSTEM_EMPTY":
        return "codeSystem空"
    if finding.check_type == "DISPLAY_NAME_EMPTY":
        return "displayName空"
    if finding.check_type == "ZIP_STRUCTURE":
        return "ZIP構造"
    if finding.check_type == "XML_PARSE":
        return "XML構文エラー"
    return str(finding.check_type or "その他")


def xml_zip_namecode_source_label(source: str | None) -> str:
    labels = {
        "VALUE_PARENT_OBSERVATION": "該当valueの親",
        "MESSAGE_CODE": "XSDメッセージ内",
        "ERROR_LINE_ELEMENT": "XSDエラー行",
        "NEAREST_PREVIOUS_ELEMENT": "XSD直前要素推定",
    }
    return labels.get(str(source or ""), "未特定")


def xml_zip_namecode_source_class(source: str | None) -> str:
    if source in {"VALUE_PARENT_OBSERVATION", "MESSAGE_CODE", "ERROR_LINE_ELEMENT"}:
        return "status-ok"
    if source == "NEAREST_PREVIOUS_ELEMENT":
        return "status-pending"
    return "status-neutral"


def serialize_xml_zip_display_groups(findings: list[Any]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    manual_fix_index = 0
    for finding in findings:
        if finding.severity not in {"ERROR", "WARNING"}:
            continue
        key = str(finding.xml_inner_path or "")
        row = grouped.setdefault(
            key,
            {
                "xml_inner_path": key,
                "finding_count": 0,
                "error_count": 0,
                "warning_count": 0,
                "labels": {},
                "findings": [],
            },
        )
        severity = str(finding.severity or "")
        severity_label = "エラー" if severity == "ERROR" else "警告"
        status_class = "status-danger" if severity == "ERROR" else "status-pending"
        can_fix = bool(getattr(finding, "can_fix", False))
        namecode_source = getattr(finding, "namecode_source", None)
        row["finding_count"] += 1
        if severity == "ERROR":
            row["error_count"] += 1
        elif severity == "WARNING":
            row["warning_count"] += 1
        label = xml_zip_check_message_label(finding)
        label_key = f"{severity_label}: {label}"
        row["labels"][label_key] = row["labels"].get(label_key, 0) + 1
        manual_fix_field = None
        if finding.check_type == "ST_MAX_BYTE_LENGTH_EXCEEDED":
            manual_fix_index += 1
            manual_fix_field = f"manual_text_fix_{manual_fix_index}"
        row["findings"].append(
            {
                "severity": severity,
                "severity_label": severity_label,
                "status_class": status_class,
                "label": label,
                "check_type": finding.check_type,
                "namecode": finding.namecode,
                "item_display_name": finding.item_display_name,
                "namecode_source": namecode_source,
                "namecode_source_label": xml_zip_namecode_source_label(namecode_source),
                "namecode_source_class": xml_zip_namecode_source_class(namecode_source),
                "message": finding.message,
                "value_preview": finding.value_preview,
                "xsd_element": getattr(finding, "xsd_element", None),
                "xsd_attribute": getattr(finding, "xsd_attribute", None),
                "mhlw_byte_length": finding.mhlw_byte_length,
                "max_byte_length": finding.max_byte_length,
                "can_fix": can_fix,
                "fixability": "FIXABLE" if can_fix else "MANUAL",
                "fixability_label": "修正可" if can_fix else "手動確認",
                "fixability_class": "status-ok" if can_fix else "status-neutral",
                "fix_note": getattr(finding, "fix_note", None),
                "manual_fix_field": manual_fix_field,
            }
        )
    rows = sorted(grouped.values(), key=lambda item: (-int(item["error_count"]), -int(item["warning_count"]), item["xml_inner_path"]))
    for row in rows:
        row["label_summary"] = " / ".join(
            f"{label} {count}件"
            for label, count in sorted(row["labels"].items(), key=lambda item: (-int(item[1]), str(item[0])))
        )
    return rows


def serialize_xml_zip_error_summaries(findings: list[Any]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for finding in findings:
        check_type = str(getattr(finding, "check_type", "") or "")
        namecode = str(getattr(finding, "namecode", "") or "").strip()
        item_name = str(getattr(finding, "item_display_name", "") or "").strip()
        xsd_element = str(getattr(finding, "xsd_element", "") or "").strip()
        xsd_attribute = str(getattr(finding, "xsd_attribute", "") or "").strip()
        value_preview = getattr(finding, "value_preview", None)
        if check_type == "DISPLAY_NAME_EMPTY":
            if item_name:
                key = "display_name_fixable"
                title = "displayName空 / 項目名あり"
                description = "exam_item_masterから項目名を補完できます。修正版ZIP作成の対象です。"
            else:
                key = "display_name_remove_empty"
                title = "displayName空 / 項目名未登録"
                description = "displayName属性は任意のため、空属性を削除してXSDエラーを解消できます。項目自体の扱いは確認対象に残します。"
            status_class = "status-ok"
        elif (
            check_type == "XSD"
            and xsd_element == "code"
            and xsd_attribute == "displayName"
            and value_preview == ""
            and namecode
            and not item_name
        ):
            key = "display_name_item_missing"
            title = "displayName空 / 項目名未登録"
            description = "namecodeは取れていますが、exam_item_masterで項目名が見つかりません。マスタ追加判断が必要です。"
            status_class = "status-danger"
        else:
            label = xml_zip_check_message_label(finding)
            key = f"{check_type}:{label}:{bool(getattr(finding, 'can_fix', False))}"
            title = label
            description = "同じ種類の検出内容です。詳細は下のファイル別一覧で確認します。"
            status_class = "status-ok" if getattr(finding, "can_fix", False) else "status-neutral"

        row = grouped.setdefault(
            key,
            {
                "key": key,
                "title": title,
                "description": description,
                "status_class": status_class,
                "count": 0,
                "namecodes": {},
            },
        )
        row["count"] += 1
        if namecode:
            names = row["namecodes"].setdefault(namecode, {"namecode": namecode, "item_name": item_name, "count": 0})
            names["count"] += 1
            if item_name:
                names["item_name"] = item_name

    result: list[dict[str, Any]] = []
    for row in grouped.values():
        namecodes = sorted(row["namecodes"].values(), key=lambda item: (-int(item["count"]), str(item["namecode"])))
        row["namecode_count"] = len(namecodes)
        row["namecodes"] = namecodes[:40]
        row["has_more_namecodes"] = len(namecodes) > 40
        result.append(row)
    return sorted(result, key=lambda item: (-int(item["count"]), str(item["title"])))


def build_xml_zip_check_result(
    *,
    upload_path: Path,
    original_filename: str,
    fix: bool,
    manual_text_fixes: list[ManualTextFix] | None = None,
) -> dict[str, Any]:
    item_names = load_exam_item_names_for_xml_zip_check()
    item_result_code_oids = load_exam_item_result_code_oids_for_xml_zip_check()
    summary, findings = check_hia_xml_zip_file(
        upload_path,
        xsd_dir=XML_ZIP_CHECK_XSD_DIR,
        fix=fix,
        fixed_output_dir=XML_ZIP_CHECK_REPORT_DIR / "fixed",
        item_names=item_names,
        item_result_code_oids=item_result_code_oids,
        manual_text_fixes=manual_text_fixes,
    )
    report_csv_path = write_hia_xml_zip_check_report(findings, XML_ZIP_CHECK_REPORT_DIR)
    display_name_options = sorted(
        {
            str(item.item_display_name).strip()
            for item in findings
            if item.severity in {"ERROR", "WARNING"} and str(item.item_display_name or "").strip()
        }
    )
    is_uploaded_zip = is_path_under(upload_path, HIA_XML_ZIP_CHECK_UPLOAD_DIR)
    fixable_count = sum(1 for item in findings if getattr(item, "can_fix", False))
    return {
        "original_filename": original_filename,
        "upload_path": str(upload_path),
        "is_uploaded_zip": is_uploaded_zip,
        "report_csv_path": str(report_csv_path),
        "fixed_zip_path": summary.fixed_zip_path,
        "fix": fix,
        "zip_files_seen": summary.zip_files_seen,
        "xml_files_seen": summary.xml_files_seen,
        "findings": len(findings),
        "errors": sum(1 for item in findings if item.severity == "ERROR"),
        "warnings": sum(1 for item in findings if item.severity == "WARNING"),
        "fixed": sum(1 for item in findings if item.fixed),
        "fixable_count": fixable_count,
        "can_create_fixed": is_uploaded_zip and fixable_count > 0,
        "can_delete_upload": is_uploaded_zip,
        "error_summaries": serialize_xml_zip_error_summaries(findings),
        "display_groups": serialize_xml_zip_display_groups(findings),
        "display_name_options": display_name_options,
    }


def manual_text_fixes_from_form(form: dict[str, str]) -> list[ManualTextFix]:
    fixes: list[ManualTextFix] = []
    prefixes = sorted(
        {
            key.removesuffix("__replacement")
            for key in form
            if key.startswith("manual_text_fix_") and key.endswith("__replacement")
        }
    )
    for prefix in prefixes:
        replacement = str(form.get(f"{prefix}__replacement") or "").strip()
        if not replacement:
            continue
        fixes.append(
            ManualTextFix(
                xml_inner_path=str(form.get(f"{prefix}__xml_inner_path") or ""),
                namecode=str(form.get(f"{prefix}__namecode") or "") or None,
                original_text=str(form.get(f"{prefix}__original_text") or ""),
                replacement_text=replacement,
            )
        )
    return fixes


def load_exam_item_names_for_xml_zip_check() -> dict[str, str]:
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=dev_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        cur.execute(
            f"""
            SELECT
                namecode,
                item_name
            FROM {qname(dev_db())}.exam_item_master
            WHERE namecode IS NOT NULL
              AND namecode <> ''
            """
        )
        return {
            str(row["namecode"]).strip(): str(row.get("item_name") or "").strip()
            for row in cur.fetchall()
            if str(row.get("namecode") or "").strip() and str(row.get("item_name") or "").strip()
        }


def load_exam_item_result_code_oids_for_xml_zip_check() -> dict[str, str]:
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=dev_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        cur.execute(
            f"""
            SELECT
                namecode,
                result_code_oid
            FROM {qname(dev_db())}.exam_item_master
            WHERE namecode IS NOT NULL
              AND namecode <> ''
              AND result_code_oid IS NOT NULL
              AND result_code_oid <> ''
            """
        )
        return {
            str(row["namecode"]).strip(): str(row.get("result_code_oid") or "").strip()
            for row in cur.fetchall()
            if str(row.get("namecode") or "").strip() and str(row.get("result_code_oid") or "").strip()
        }


def load_xml_zip_uploaded_files() -> list[dict[str, Any]]:
    if not HIA_XML_ZIP_CHECK_ROOT_DIR.exists():
        return []
    rows: list[dict[str, Any]] = []
    for path in HIA_XML_ZIP_CHECK_ROOT_DIR.rglob("*.zip"):
        if not path.is_file() or not is_path_under(path, HIA_XML_ZIP_CHECK_ROOT_DIR):
            continue
        stat = path.stat()
        can_delete = is_path_under(path, HIA_XML_ZIP_CHECK_UPLOAD_DIR)
        rows.append(
            {
                "name": path.name,
                "path": str(path),
                "check_path": str(path.relative_to(APP_DATA_DIR)),
                "relative_path": str(path.relative_to(APP_DATA_DIR)),
                "size_mb": round(stat.st_size / 1024 / 1024, 2),
                "modified_at": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                "kind": "アップロード" if can_delete else "修正版/確認用",
                "can_delete": can_delete,
            }
        )
    return sorted(rows, key=lambda row: str(row["modified_at"]), reverse=True)


def build_fund_delivery_list_configs(raw: dict[str, Any], *, actor: str | None = None) -> list[FundDeliveryListConfig]:
    section = fund_delivery_section(raw, "list")
    event_id = config_value(raw, "event_id", None)
    insurer_number = str(config_value(raw, "insurer_number", "06139463"))
    output_mode = str(config_value(section, "output_mode", "EXAM_MONTH"))
    if output_mode == "EXAM_MONTH":
        exam_months = _string_list(section.get("exam_months")) or _string_list(config_value(section, "exam_month", None))
        if not exam_months:
            raise ValueError("list.exam_months または list.exam_month が必要です。")
    else:
        exam_months = [None]
    configs = []
    for exam_month in exam_months:
        list_name = config_value(section, "list_name", None)
        if list_name and output_mode == "EXAM_MONTH" and len(exam_months) > 1:
            list_name = f"{exam_month}_{list_name}"
        if not list_name:
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            list_name = f"{exam_month}_健保納品リスト_{stamp}" if output_mode == "EXAM_MONTH" else f"全件_健保納品リスト_{stamp}"
        configs.append(FundDeliveryListConfig(
            event_id=None if event_id in (None, "") else int(event_id),
            insurer_number=insurer_number,
            list_name=str(list_name),
            output_mode=output_mode,
            exam_month=None if exam_month in (None, "") else str(exam_month),
            delivery_policy=str(config_value(section, "delivery_policy", "NOT_DELIVERED_ONLY")),
            same_exam_date_policy=str(config_value(section, "same_exam_date_policy", "LATEST_DOWNLOAD")),
            grouping_mode=str(config_value(section, "grouping_mode", "ALL")),
            sender_code=str(config_value(section, "sender_code", "1322100106")),
            sender_name=config_value(section, "sender_name", None),
            created_by=config_value(section, "created_by", actor),
            dry_run=not config_bool(section, "confirm", False),
        ))
    return configs


def build_fund_delivery_list_config(raw: dict[str, Any], *, actor: str | None = None) -> FundDeliveryListConfig:
    configs = build_fund_delivery_list_configs(raw, actor=actor)
    if len(configs) != 1:
        raise ValueError("複数月設定です。build_fund_delivery_list_configs を使ってください。")
    return configs[0]


def _delivery_date(value: str | None) -> str:
    if value:
        text = value.strip()
        if len(text) != 8 or not text.isdigit():
            raise ValueError(f"export.delivery_date は YYYYMMDD で指定してください: {value}")
        return text
    return datetime.now().strftime("%Y%m%d")


def next_fund_delivery_output_seq(
    cur: Any,
    *,
    sender_code: str,
    insurer_number: str,
    delivery_date: str,
    service_event_type_code: str,
) -> int:
    prefix = f"{sender_code}_{insurer_number}_{delivery_date}"
    suffix = f"_{service_event_type_code}.zip"
    cur.execute(
        """
        SELECT output_zip_name
          FROM fund_delivery_runs
         WHERE output_zip_name LIKE %s
        """,
        (prefix + "%",),
    )
    max_seq = -1
    found = False
    for row in cur.fetchall() or []:
        name = str(row["output_zip_name"])
        if name.startswith(prefix) and name.endswith(suffix):
            seq_text = name[len(prefix) : -len(suffix)]
            if seq_text.isdigit():
                found = True
                max_seq = max(max_seq, int(seq_text))
    return max_seq + 1 if found else 0


def validate_fund_delivery_output_digits(*, output_seq: int, service_event_type_code: str) -> None:
    if not 0 <= output_seq <= 9:
        raise ValueError(f"export.output_seq は 0-9 の一桁で指定してください: {output_seq}")
    if len(service_event_type_code) != 1 or not service_event_type_code.isdigit():
        raise ValueError(f"export.service_event_type_code は 0-9 の一桁で指定してください: {service_event_type_code}")


def fund_delivery_list_header(cur: Any, delivery_list_id: int) -> dict[str, Any]:
    cur.execute(
        """
        SELECT delivery_list_id, sender_code, insurer_number
          FROM fund_delivery_lists
         WHERE delivery_list_id = %s
        """,
        (delivery_list_id,),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError(f"fund_delivery_list not found: delivery_list_id={delivery_list_id}")
    return dict(row)


def build_fund_delivery_zip_configs(cur: Any, raw: dict[str, Any], *, actor: str | None = None) -> list[FundDeliveryZipExportConfig]:
    section = fund_delivery_section(raw, "export")
    delivery_list_id = config_value(section, "delivery_list_id", None)
    if delivery_list_id in (None, ""):
        delivery_list_ids = ready_fund_delivery_list_ids(cur)
    else:
        delivery_list_ids = [int(delivery_list_id)]
    if not delivery_list_ids:
        raise ValueError("出力待ちの健保納品リストがありません。先にリストを作成してください。")
    output_seq_raw = str(config_value(section, "output_seq", 0)).strip()
    service_event_type_code = str(config_value(section, "service_event_type_code", 1)).strip()
    auto_output_seq = output_seq_raw.lower() == "auto"
    next_output_seq: int | None = None
    configs = []
    for list_id in delivery_list_ids:
        if auto_output_seq:
            header = fund_delivery_list_header(cur, list_id)
            if next_output_seq is None:
                next_output_seq = next_fund_delivery_output_seq(
                    cur,
                    sender_code=str(header["sender_code"]),
                    insurer_number=str(header["insurer_number"]),
                    delivery_date=_delivery_date(config_value(section, "delivery_date", None)),
                    service_event_type_code=service_event_type_code,
                )
            output_seq = next_output_seq
            next_output_seq += 1
        else:
            output_seq = int(output_seq_raw)
        validate_fund_delivery_output_digits(
            output_seq=output_seq,
            service_event_type_code=service_event_type_code,
        )
        configs.append(FundDeliveryZipExportConfig(
            delivery_list_id=list_id,
            output_base_dir=_config_path(section.get("output_base_dir"), REPO_ROOT / "data" / "fund_delivery" / "output"),
            xsd_dir=_config_path(section.get("xsd_dir"), REPO_ROOT / "scripts" / "from_medical" / "source" / "XSD" / "mhlw_v4_20230331_v08"),
            delivery_date=config_value(section, "delivery_date", None),
            output_seq=output_seq,
            service_event_type_code=service_event_type_code,
            created_by=config_value(section, "created_by", actor),
            dry_run=not config_bool(section, "confirm", False),
        ))
    return configs


def build_fund_delivery_zip_config(cur: Any, raw: dict[str, Any], *, actor: str | None = None) -> FundDeliveryZipExportConfig:
    configs = build_fund_delivery_zip_configs(cur, raw, actor=actor)
    if len(configs) != 1:
        raise ValueError("複数リストが対象です。build_fund_delivery_zip_configs を使ってください。")
    return configs[0]


def build_fund_delivery_submission_config(cur: Any, raw: dict[str, Any], *, actor: str | None = None) -> FundDeliverySubmissionConfig:
    section = fund_delivery_section(raw, "submission")
    delivery_list_id = config_value(section, "delivery_list_id", None)
    resolved_id = None if delivery_list_id in (None, "") else int(delivery_list_id)
    if resolved_id is None:
        resolved_id = latest_fund_delivery_exported_list_id(cur)
    if resolved_id is None:
        raise ValueError("提出済みにできる健保納品出力がありません。先にZIP出力してください。")
    member_ids_raw = config_value(section, "delivery_member_ids", [])
    if isinstance(member_ids_raw, str):
        member_ids = tuple(int(part.strip()) for part in member_ids_raw.split(",") if part.strip())
    else:
        member_ids = tuple(int(item) for item in (member_ids_raw or []))
    submitted_at_raw = config_value(section, "submitted_at", None)
    submitted_at = None
    if submitted_at_raw:
        text = str(submitted_at_raw)
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                submitted_at = datetime.strptime(text, fmt)
                break
            except ValueError:
                pass
        if submitted_at is None:
            raise ValueError("submitted_at は YYYY-MM-DD、YYYY-MM-DD HH:MM:SS、YYYY-MM-DDTHH:MM:SS のいずれかで指定してください。")
    return FundDeliverySubmissionConfig(
        delivery_list_id=resolved_id,
        delivery_member_ids=member_ids,
        all_members=config_bool(section, "all_members", True),
        target_status=str(config_value(section, "status", "SUBMITTED")),
        submitted_at=submitted_at,
        submitted_by=config_value(section, "submitted_by", actor),
        submission_note=config_value(section, "note", None),
        dry_run=not config_bool(section, "confirm", False),
    )


def build_fund_delivery_submission_config_from_form(
    cur: Any,
    form: dict[str, str],
    *,
    actor: str | None = None,
) -> FundDeliverySubmissionConfig:
    delivery_list_id = int(form.get("delivery_list_id") or "0")
    delivery_run_id = int(form.get("delivery_run_id") or "0")
    delivery_member_id = int(form.get("delivery_member_id") or "0")
    target_scope = (form.get("target_scope") or "member").strip()
    target_status = (form.get("target_status") or "SUBMITTED").strip()
    note = (form.get("note") or "").strip() or None
    if delivery_list_id <= 0:
        raise ValueError("delivery_list_id が不正です。")
    member_ids: tuple[int, ...]
    if target_scope == "run":
        if delivery_run_id <= 0:
            raise ValueError("delivery_run_id が不正です。")
        cur.execute(
            """
            SELECT delivery_member_id
              FROM fund_delivery_members
             WHERE delivery_run_id = %s
             ORDER BY delivery_member_id
            """,
            (delivery_run_id,),
        )
        member_ids = tuple(int(row["delivery_member_id"]) for row in (cur.fetchall() or []))
        if not member_ids:
            raise ValueError(f"delivery_run_id={delivery_run_id} の納品XMLがありません。")
    elif target_scope == "member":
        if delivery_member_id <= 0:
            raise ValueError("delivery_member_id が不正です。")
        member_ids = (delivery_member_id,)
    else:
        raise ValueError(f"target_scope が不正です: {target_scope}")
    return FundDeliverySubmissionConfig(
        delivery_list_id=delivery_list_id,
        delivery_member_ids=member_ids,
        all_members=False,
        target_status=target_status,
        submitted_at=None,
        submitted_by=actor,
        submission_note=note,
        dry_run=False,
    )


def run_hia_fund_delivery_step(cur: Any, *, action: str, raw: dict[str, Any], user: dict[str, Any]) -> tuple[str, bool]:
    database = str(config_value(raw, "database", health_db()))
    actor = fund_delivery_actor(user)
    if action == "import_download":
        config = build_hia_download_import_config(raw)
        run_id = start_run(
            cur,
            phase="HIA_IMPORT_DOWNLOADED_XML_ZIP",
            source="HIA",
            db_schema=database,
            db_path=None,
            input_base=str(config.input_zip_dir),
            input_file=None,
            insurer_number=None,
            dry_run=config.dry_run,
            limit_rows=None,
        )
        summary = import_hia_download_zips(cur, config=config, run_id=run_id)
        metrics = RunMetrics(
            files=summary.files_seen,
            rows_seen=summary.xml_seen,
            rows_inserted=summary.xml_inserted + summary.person_years_upserted + summary.person_xml_events_upserted,
            rows_updated=summary.xml_updated,
            rows_skipped=summary.files_skipped,
            errors=summary.errors,
        )
        finish_run(
            cur,
            run_id,
            metrics,
            status_override="success" if config.dry_run else None,
            extra_notes=f"files_imported={summary.files_imported} xml_inserted={summary.xml_inserted} xml_updated={summary.xml_updated}",
        )
        return (
            f"HIA ZIP取込: files={summary.files_seen} imported={summary.files_imported} "
            f"xml={summary.xml_seen} errors={summary.errors} dry_run={int(config.dry_run)}",
            config.dry_run,
        )

    if action == "create_list":
        configs = build_fund_delivery_list_configs(raw, actor=actor)
        dry_run = configs[0].dry_run if configs else True
        run_id = start_run(
            cur,
            phase="HIA_CREATE_FUND_DELIVERY_LIST",
            source="HIA",
            db_schema=database,
            db_path=None,
            input_base=None,
            input_file=None,
            insurer_number=str(config_value(raw, "insurer_number", "")),
            dry_run=dry_run,
            limit_rows=None,
        )
        summaries = [build_fund_delivery_list(cur, config) for config in configs]
        metrics = RunMetrics(
            rows_seen=sum(item.valid_xmls_seen for item in summaries),
            rows_inserted=sum(item.list_members_inserted + item.list_created for item in summaries),
            rows_updated=sum(item.candidates_upserted + item.person_status_upserted for item in summaries),
            rows_skipped=sum(item.skipped_by_delivery_policy for item in summaries),
        )
        finish_run(
            cur,
            run_id,
            metrics,
            status_override="success" if dry_run else None,
            extra_notes=(
                f"list_ids={','.join(str(item.list_id) for item in summaries)} "
                f"selected={sum(item.selected_candidates for item in summaries)} "
                f"members={sum(item.list_members_seen for item in summaries)}"
            ),
        )
        return (
            f"納品リスト作成: lists={len(summaries)} selected={sum(item.selected_candidates for item in summaries)} "
            f"members={sum(item.list_members_seen for item in summaries)} skipped={sum(item.skipped_by_delivery_policy for item in summaries)} "
            f"dry_run={int(dry_run)}",
            dry_run,
        )

    if action == "export_zip":
        configs = build_fund_delivery_zip_configs(cur, raw, actor=actor)
        dry_run = configs[0].dry_run if configs else True
        run_id = start_run(
            cur,
            phase="HIA_EXPORT_FUND_DELIVERY_ZIP",
            source="HIA",
            db_schema=database,
            db_path=None,
            input_base=None,
            input_file=None,
            insurer_number=None,
            dry_run=dry_run,
            limit_rows=None,
        )
        summaries = [export_fund_delivery_zip(cur, config=config, etl_run_id=run_id) for config in configs]
        metrics = RunMetrics(
            rows_seen=sum(item.members_seen for item in summaries),
            rows_inserted=sum(item.members_written for item in summaries),
            errors=sum(item.errors for item in summaries),
        )
        finish_run(
            cur,
            run_id,
            metrics,
            status_override="success" if dry_run else None,
            extra_notes=(
                f"delivery_list_ids={','.join(str(item.delivery_list_id) for item in summaries)} "
                f"output_zips={','.join(str(item.output_zip_name) for item in summaries)} "
                f"summary_csvs={','.join(str(item.summary_csv_path) for item in summaries if item.summary_csv_path)}"
            ),
        )
        return (
            f"健保納品ZIP出力: lists={len(summaries)} members={sum(item.members_seen for item in summaries)} "
            f"outputs={','.join(str(item.output_zip_name) for item in summaries)} "
            f"summary_csvs={','.join(str(item.summary_csv_path) for item in summaries if item.summary_csv_path)} "
            f"dry_run={int(dry_run)}",
            dry_run,
        )

    if action == "mark_submitted":
        config = build_fund_delivery_submission_config(cur, raw, actor=actor)
        run_id = start_run(
            cur,
            phase="HIA_MARK_FUND_DELIVERY_SUBMITTED",
            source="HIA",
            db_schema=database,
            db_path=None,
            input_base=None,
            input_file=None,
            insurer_number=None,
            dry_run=config.dry_run,
            limit_rows=None,
        )
        summary = mark_fund_delivery_submitted(cur, config)
        metrics = RunMetrics(
            rows_seen=summary.members_seen,
            rows_updated=summary.members_updated + summary.runs_updated + summary.person_status_updated,
            errors=summary.errors,
        )
        finish_run(
            cur,
            run_id,
            metrics,
            status_override="success" if config.dry_run else None,
            extra_notes=f"delivery_list_id={summary.delivery_list_id} list_status={summary.list_status}",
        )
        return (
            f"提出済み反映: list_id={summary.delivery_list_id} status={summary.list_status} "
            f"members={summary.members_seen} updated={summary.members_updated} dry_run={int(config.dry_run)}",
            config.dry_run,
        )

    raise ValueError(f"未対応の操作です: {action}")


def load_fund_delivery_page_data(cur: Any) -> dict[str, Any]:
    raw = load_fund_delivery_page_config()
    return {
        "config": raw,
        "summary": load_fund_delivery_summary(cur),
        "download_zips": load_fund_delivery_zip_rows(cur),
        "delivery_lists": load_fund_delivery_list_rows(cur),
        "delivery_runs": load_fund_delivery_run_rows(cur),
        "delivery_members": load_fund_delivery_member_rows(cur),
    }


def load_ops_xml_export_lists(cur: Any, *, limit: int = 30) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          xel.xml_export_list_id,
          xel.event_id,
          xel.list_name,
          xel.list_status,
          xel.requested_exam_month,
          xel.requested_facility_codes,
          xel.include_exported,
          xel.requested_file_date,
          xel.requested_split_no,
          xel.export_etl_run_id,
          xel.export_started_at,
          xel.export_finished_at,
          xel.exported_zip_count,
          xel.exported_member_count,
          xel.created_by,
          xel.confirmed_by,
          xel.confirmed_at,
          xel.created_at,
          COUNT(xelc.xml_export_list_case_id) AS case_count,
          SUM(CASE WHEN xelc.list_case_status = 'READY' THEN 1 ELSE 0 END) AS ready_count,
          SUM(CASE WHEN xelc.list_case_status = 'EXPORTED' THEN 1 ELSE 0 END) AS exported_count,
          SUM(CASE WHEN xelc.list_case_status = 'EXPORT_ERROR' THEN 1 ELSE 0 END) AS error_count
        FROM {qname(health_db())}.ops_xml_export_lists xel
        LEFT JOIN {qname(health_db())}.ops_xml_export_list_cases xelc
          ON xelc.xml_export_list_id = xel.xml_export_list_id
         AND xelc.removed_at IS NULL
        GROUP BY
          xel.xml_export_list_id,
          xel.event_id,
          xel.list_name,
          xel.list_status,
          xel.requested_exam_month,
          xel.requested_facility_codes,
          xel.include_exported,
          xel.requested_file_date,
          xel.requested_split_no,
          xel.export_etl_run_id,
          xel.export_started_at,
          xel.export_finished_at,
          xel.exported_zip_count,
          xel.exported_member_count,
          xel.created_by,
          xel.confirmed_by,
          xel.confirmed_at,
          xel.created_at
        ORDER BY xel.xml_export_list_id DESC
        LIMIT %s
        """,
        (limit,),
    )
    return [dict(row) for row in cur.fetchall()]


def parse_positive_int(value: str | None, *, default: int, maximum: int) -> int:
    try:
        parsed = int(str(value or "").strip())
    except ValueError:
        return default
    if parsed <= 0:
        return default
    return min(parsed, maximum)


def load_event_options(cur: Any) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          event_id,
          event_name,
          event_year,
          event_type,
          insurer_number
        FROM {qname(dev_db())}.event
        WHERE is_active = 1
        ORDER BY event_year DESC, event_id DESC
        """
    )
    return [dict(row) for row in cur.fetchall()]


def load_workload_estimate_actuals(cur: Any, *, event_id: int) -> dict[str, int]:
    cur.execute(
        f"""
        SELECT COUNT(*) AS case_count,
          COUNT(DISTINCT exam_facility_id) AS facility_count,
          SUM(CASE WHEN check_status = 'NG' THEN 1 ELSE 0 END) AS legal_error_case_count,
          COUNT(DISTINCT CASE WHEN check_status = 'NG' THEN exam_facility_id END) AS legal_error_facility_count,
          SUM(CASE WHEN export_readiness_status = 'APPROVED_WITH_REASON' THEN 1 ELSE 0 END) AS approved_with_reason_count
        FROM {qname(health_db())}.exam_export_cases
        WHERE event_id = %s
        """,
        (event_id,),
    )
    case_row = dict(cur.fetchone() or {})
    cur.execute(
        f"""
        SELECT
          SUM(CASE WHEN entry_purpose = 'PAPER_ONLY' AND applied_exam_ledger_id IS NOT NULL THEN 1 ELSE 0 END) AS paper_only_count,
          SUM(CASE WHEN entry_purpose = 'SUPPLEMENT' AND applied_exam_ledger_id IS NOT NULL THEN 1 ELSE 0 END) AS supplement_count
        FROM {qname(health_db())}.manual_exam_entry_drafts
        WHERE event_id = %s
        """,
        (event_id,),
    )
    values = {**case_row, **dict(cur.fetchone() or {})}
    return {key: int(value or 0) for key, value in values.items()}


def load_monthly_exam_ng_report(cur: Any, *, event_id: str, exam_month: str) -> dict[str, Any]:
    facility_rows = load_facility_summary_rows(
        cur,
        filters={"event_id": event_id, "exam_month": exam_month, "q": ""},
        limit=2000,
    )
    month_clause = ""
    params: list[Any] = [event_id]
    if exam_month:
        month_clause = " AND DATE_FORMAT(eec.exam_date, '%Y-%m') = %s"
        params.append(exam_month)

    cur.execute(
        f"""
        SELECT source_mix.facility_code,
          SUM(source_mix.has_csv) AS csv_adopted_case_count,
          SUM(source_mix.has_manual) AS manual_adopted_case_count,
          SUM(CASE WHEN source_mix.source_type_count >= 2 THEN 1 ELSE 0 END) AS multi_source_case_count,
          SUM(source_mix.csv_value_count) AS csv_adopted_value_count,
          SUM(source_mix.manual_value_count) AS manual_adopted_value_count
        FROM (
          SELECT eec.exam_export_case_id, eec.facility_code,
            MAX(CASE WHEN el.source_type = 'CSV' THEN 1 ELSE 0 END) AS has_csv,
            MAX(CASE WHEN el.source_type IN ('MANUAL', 'PAPER') THEN 1 ELSE 0 END) AS has_manual,
            COUNT(DISTINCT CASE WHEN el.source_type IN ('XML', 'CSV', 'MANUAL', 'PAPER') THEN el.source_type END) AS source_type_count,
            SUM(CASE WHEN el.source_type = 'CSV' THEN 1 ELSE 0 END) AS csv_value_count,
            SUM(CASE WHEN el.source_type IN ('MANUAL', 'PAPER') THEN 1 ELSE 0 END) AS manual_value_count
          FROM {qname(health_db())}.exam_export_cases AS eec
          LEFT JOIN {qname(health_db())}.exam_export_case_values AS eecv
            ON eecv.exam_export_case_id = eec.exam_export_case_id
          LEFT JOIN {qname(health_db())}.exam_item_values AS eiv
            ON eiv.id = eecv.source_exam_item_value_id
          LEFT JOIN {qname(health_db())}.exam_ledgers AS el
            ON el.exam_ledger_id = eiv.ledger_id AND eiv.ledger_type = 'EXAM'
          WHERE eec.event_id = %s {month_clause}
          GROUP BY eec.exam_export_case_id, eec.facility_code
        ) AS source_mix
        GROUP BY source_mix.facility_code
        """,
        tuple(params),
    )
    source_by_facility = {str(row.get("facility_code") or ""): dict(row) for row in cur.fetchall()}
    for row in facility_rows:
        source = source_by_facility.get(str(row.get("facility_code") or ""), {})
        for field in ("csv_adopted_case_count", "manual_adopted_case_count", "multi_source_case_count", "csv_adopted_value_count", "manual_adopted_value_count"):
            row[field] = int(source.get(field) or 0)

    cur.execute(
        f"""
        SELECT cri.check_scope, cri.check_item_code,
          COALESCE(NULLIF(MAX(cri.check_item_name), ''), cri.check_item_code) AS check_item_name,
          COALESCE(NULLIF(cri.validation_reason, ''), '理由なし') AS validation_reason,
          COUNT(DISTINCT cri.exam_export_case_id) AS case_count,
          COUNT(DISTINCT eec.facility_code) AS facility_count,
          SUM(CASE WHEN cri.review_status IN ('NONE', 'NEEDS_CONFIRMATION') THEN 1 ELSE 0 END) AS open_count,
          SUM(CASE WHEN cri.review_status = 'WAITING_RESUBMISSION' THEN 1 ELSE 0 END) AS waiting_count,
          SUM(CASE WHEN cri.review_status = 'APPROVED_WITH_REASON' THEN 1 ELSE 0 END) AS approved_count,
          SUM(CASE WHEN cri.review_status = 'EXCLUDED' THEN 1 ELSE 0 END) AS excluded_count
        FROM {qname(health_db())}.exam_case_check_review_items AS cri
        INNER JOIN {qname(health_db())}.exam_export_cases AS eec
          ON eec.exam_export_case_id = cri.exam_export_case_id
        WHERE eec.event_id = %s
          AND cri.review_status <> 'RESOLVED_BY_SOURCE_VALUE'
          {month_clause}
        GROUP BY cri.check_scope, cri.check_item_code,
          COALESCE(NULLIF(cri.validation_reason, ''), '理由なし')
        ORDER BY case_count DESC, facility_count DESC, cri.check_scope, cri.check_item_code
        LIMIT 200
        """,
        tuple(params),
    )
    ng_item_rows = [dict(row) for row in cur.fetchall()]
    for row in ng_item_rows:
        for field in ("case_count", "facility_count", "open_count", "waiting_count", "approved_count", "excluded_count"):
            row[field] = int(row.get(field) or 0)

    summary = {
        "facility_count": len([row for row in facility_rows if int(row.get("case_count") or 0) > 0]),
        "case_count": sum(int(row.get("case_count") or 0) for row in facility_rows),
        "ready_count": sum(int(row.get("case_ready_count") or 0) for row in facility_rows),
        "approved_count": sum(int(row.get("case_approved_count") or 0) for row in facility_rows),
        "blocked_count": sum(int(row.get("case_blocked_count") or 0) for row in facility_rows),
        "exported_count": sum(int(row.get("case_exported_count") or 0) for row in facility_rows),
        "legal_ng_count": sum(int(row.get("legal_ng_count") or 0) for row in facility_rows),
        "specific_ng_count": sum(int(row.get("specific_ng_count") or 0) for row in facility_rows),
        "specific_subject_count": sum(int(row.get("specific_subject_count") or 0) for row in facility_rows),
        "source_ng_count": sum(int(row.get("source_ng_count") or 0) for row in facility_rows),
        "source_count": sum(int(row.get("source_count") or 0) for row in facility_rows),
        "csv_adopted_case_count": sum(int(row.get("csv_adopted_case_count") or 0) for row in facility_rows),
        "manual_adopted_case_count": sum(int(row.get("manual_adopted_case_count") or 0) for row in facility_rows),
        "csv_adopted_value_count": sum(int(row.get("csv_adopted_value_count") or 0) for row in facility_rows),
        "manual_adopted_value_count": sum(int(row.get("manual_adopted_value_count") or 0) for row in facility_rows),
    }
    summary["legal_ng_rate"] = pct_label(summary["legal_ng_count"], summary["case_count"])
    summary["specific_ng_rate"] = pct_label(summary["specific_ng_count"], summary["specific_subject_count"])
    summary["source_ng_rate"] = pct_label(summary["source_ng_count"], summary["source_count"])
    return {"summary": summary, "facility_rows": facility_rows, "ng_item_rows": ng_item_rows}


def load_admin_event_rows(cur: Any) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          event_id,
          insurer_number,
          event_year,
          event_type,
          event_name,
          age_rule_type,
          age_reference_date,
          result_root_path,
          is_active,
          updated_at
        FROM {qname(dev_db())}.event
        ORDER BY event_year DESC, event_id DESC
        """
    )
    return [dict(row) for row in cur.fetchall()]


def normalize_event_form(form: dict[str, str]) -> dict[str, Any]:
    insurer_number = form.get("insurer_number", "").strip()
    event_year = form.get("event_year", "").strip()
    event_type = form.get("event_type", "").strip() or "HEALTH_EXAM"
    event_name = form.get("event_name", "").strip() or None
    age_rule_type = form.get("age_rule_type", "").strip() or "FIXED_DATE"
    age_reference_date = form.get("age_reference_date", "").strip() or None
    result_root_path = form.get("result_root_path", "").strip() or None
    is_active = 1 if form.get("is_active") == "1" else 0
    if not insurer_number:
        raise ValueError("保険者番号は必須です。")
    if not event_year:
        raise ValueError("年度は必須です。")
    return {
        "insurer_number": insurer_number,
        "event_year": int(event_year),
        "event_type": event_type,
        "event_name": event_name,
        "age_rule_type": age_rule_type,
        "age_reference_date": age_reference_date,
        "result_root_path": result_root_path,
        "is_active": is_active,
    }


def _form_text(form: dict[str, str], key: str) -> str | None:
    text = (form.get(key) or "").strip()
    return text or None


def load_alias_facility_admin_rows(cur: Any, *, limit: int = 300) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          ef.exam_facility_id,
          ef.exam_facility_code,
          ef.exam_facility_name,
          ef.exam_facility_display_name,
          ef.exam_facility_type,
          ef.medical_institution_code,
          ef.reservation_system_medical_institution_code,
          ef.postal_code,
          ef.address,
          ef.phone_number,
          ef.data_source_name,
          ef.note,
          ef.is_active,
          ef.updated_at,
          COUNT(DISTINCT mfa.alias_id) AS alias_count,
          MAX(mfa.updated_at) AS alias_last_updated_at
        FROM {qname(master_db())}.exam_facilities ef
        JOIN {qname(master_db())}.medical_folder_aliases mfa
          ON mfa.exam_facility_id = ef.exam_facility_id
        GROUP BY
          ef.exam_facility_id,
          ef.exam_facility_code,
          ef.exam_facility_name,
          ef.exam_facility_display_name,
          ef.exam_facility_type,
          ef.medical_institution_code,
          ef.reservation_system_medical_institution_code,
          ef.postal_code,
          ef.address,
          ef.phone_number,
          ef.data_source_name,
          ef.note,
          ef.is_active,
          ef.updated_at
        ORDER BY ef.is_active DESC, ef.exam_facility_code IS NULL, ef.exam_facility_code, ef.exam_facility_name
        LIMIT %s
        """,
        (limit,),
    )
    return [dict(row) for row in cur.fetchall()]


def facility_master_search_where(
    *,
    keyword: str | None = None,
    code: str | None = None,
    code_match: str = "exact",
    prefecture: str | None = None,
) -> tuple[str, list[Any]]:
    where_parts: list[str] = []
    params: list[Any] = []
    keyword = (keyword or "").strip()
    code = (code or "").strip()
    prefecture = (prefecture or "").strip()
    if prefecture:
        where_parts.append("address LIKE %s")
        params.append(f"{prefecture}%")
    if code:
        code_operator = "LIKE" if code_match == "partial" else "="
        code_value = f"%{code}%" if code_match == "partial" else code
        where_parts.append(
            f"""(
              exam_facility_code {code_operator} %s
              OR medical_institution_code {code_operator} %s
              OR reservation_system_medical_institution_code {code_operator} %s
            )"""
        )
        params.extend([code_value, code_value, code_value])
    if keyword:
        like = f"%{keyword}%"
        where_parts.append(
            """(
          exam_facility_name LIKE %s
          OR exam_facility_display_name LIKE %s
          OR postal_code LIKE %s
          OR address LIKE %s
          OR phone_number LIKE %s
          OR note LIKE %s
        )"""
        )
        params.extend([like] * 6)
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    return where_sql, params


def count_facility_master_admin_rows(
    cur: Any,
    *,
    keyword: str | None = None,
    code: str | None = None,
    code_match: str = "exact",
    prefecture: str | None = None,
) -> int:
    where_sql, params = facility_master_search_where(
        keyword=keyword,
        code=code,
        code_match=code_match,
        prefecture=prefecture,
    )
    cur.execute(
        f"""
        SELECT COUNT(*) AS cnt
        FROM {qname(master_db())}.exam_facilities
        {where_sql}
        """,
        params,
    )
    row = cur.fetchone() or {}
    return int(row.get("cnt") or 0)


def load_facility_master_admin_rows(
    cur: Any,
    *,
    limit: int = 500,
    keyword: str | None = None,
    code: str | None = None,
    code_match: str = "exact",
    prefecture: str | None = None,
    prefer_alias_registered: bool = False,
) -> list[dict[str, Any]]:
    where_sql, params = facility_master_search_where(
        keyword=keyword,
        code=code,
        code_match=code_match,
        prefecture=prefecture,
    )
    cur.execute(
        f"""
        SELECT
          ef.exam_facility_id,
          ef.exam_facility_code,
          ef.exam_facility_name,
          ef.exam_facility_display_name,
          ef.exam_facility_type,
          ef.medical_institution_code,
          ef.reservation_system_medical_institution_code,
          ef.postal_code,
          ef.address,
          ef.phone_number,
          ef.data_source_name,
          ef.note,
          ef.is_active,
          ef.updated_at,
          COALESCE(alias_counts.alias_count, 0) AS alias_count,
          COALESCE(alias_counts.active_alias_count, 0) AS active_alias_count
        FROM {qname(master_db())}.exam_facilities AS ef
        LEFT JOIN (
          SELECT
            exam_facility_id,
            COUNT(*) AS alias_count,
            SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_alias_count
          FROM {qname(master_db())}.medical_folder_aliases
          WHERE exam_facility_id IS NOT NULL
          GROUP BY exam_facility_id
        ) AS alias_counts
          ON alias_counts.exam_facility_id = ef.exam_facility_id
        {where_sql}
        ORDER BY
          {"COALESCE(alias_counts.alias_count, 0) > 0 DESC," if prefer_alias_registered else ""}
          ef.is_active DESC,
          ef.exam_facility_code IS NULL,
          ef.exam_facility_code,
          ef.exam_facility_name
        LIMIT %s
        """,
        [*params, limit],
    )
    return [dict(row) for row in cur.fetchall()]


def facility_master_search_item(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "exam_facility_id": row.get("exam_facility_id"),
        "exam_facility_code": row.get("exam_facility_code"),
        "medical_institution_code": row.get("medical_institution_code"),
        "reservation_system_medical_institution_code": row.get(
            "reservation_system_medical_institution_code"
        ),
        "exam_facility_name": row.get("exam_facility_name"),
        "exam_facility_display_name": row.get("exam_facility_display_name"),
        "postal_code": row.get("postal_code"),
        "address": row.get("address"),
        "phone_number": row.get("phone_number"),
        "is_active": row.get("is_active"),
        "alias_count": int(row.get("alias_count") or 0),
        "active_alias_count": int(row.get("active_alias_count") or 0),
    }


@app.get("/admin/facility-master/search", response_class=JSONResponse)
def search_facility_master(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse({"items": []}, status_code=401)
    if not can_view_business_settings(user):
        return JSONResponse({"items": []}, status_code=403)
    code = request.query_params.get("code", "").strip()
    keyword = request.query_params.get("q", "").strip()
    prefecture = request.query_params.get("prefecture", "").strip()
    code_match = request.query_params.get("code_match", "").strip()
    prefer_alias_registered = request.query_params.get("prefer_alias_registered", "").strip() == "1"
    if code_match == "partial" and code and len(code) < 2:
        return JSONResponse({"items": [], "message": "コードは2桁以上で検索してください。"})
    if code_match != "partial":
        code_match = "exact"
    if prefecture and prefecture not in PREFECTURE_OPTIONS:
        prefecture = ""
    if not code and not keyword and not prefecture:
        return JSONResponse({"items": []})
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        total_count = count_facility_master_admin_rows(
            cur,
            keyword=keyword,
            code=code,
            code_match=code_match,
            prefecture=prefecture,
        )
        rows = load_facility_master_admin_rows(
            cur,
            limit=50,
            keyword=keyword,
            code=code,
            code_match=code_match,
            prefecture=prefecture,
            prefer_alias_registered=prefer_alias_registered,
        )
        cur.close()
    return JSONResponse(
        {
            "total_count": total_count,
            "limit": 50,
            "has_more": total_count > 50,
            "items": [
                facility_master_search_item(row)
                for row in rows
            ],
        }
    )


def load_folder_alias_admin_rows(cur: Any, *, limit: int = 400) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          mfa.alias_id,
          mfa.event_id,
          ev.event_name,
          ev.event_year,
          mfa.src_folder_raw,
          mfa.dst_folder_norm,
          mfa.exam_facility_id,
          mfa.expected_source_mode,
          mfa.csv_format_version_id,
          ef.exam_facility_code,
          ef.medical_institution_code,
          ef.reservation_system_medical_institution_code,
          ef.exam_facility_name,
          ef.exam_facility_display_name,
          ef_by_folder.exam_facility_id AS folder_code_exam_facility_id,
          ef_by_folder.exam_facility_code AS folder_code_exam_facility_code,
          ef_by_folder.medical_institution_code AS folder_code_medical_institution_code,
          ef_by_folder.exam_facility_name AS folder_code_exam_facility_name,
          cfv.mapping_version AS csv_mapping_version,
          cfv.format_name AS csv_format_name,
          cfv.is_active AS csv_format_is_active,
          receipt_counts.xml_file_count,
          receipt_counts.csv_file_count,
          mfa.manual_judgement,
          mfa.note,
          mfa.is_active,
          mfa.updated_at
        FROM {qname(master_db())}.medical_folder_aliases mfa
        LEFT JOIN {qname(master_db())}.exam_facilities ef
          ON ef.exam_facility_id = mfa.exam_facility_id
        LEFT JOIN {qname(master_db())}.exam_facilities ef_by_folder
          ON ef_by_folder.medical_institution_code = SUBSTRING_INDEX(mfa.src_folder_raw, '_', 1)
        LEFT JOIN {qname(master_db())}.csv_format_versions cfv
          ON cfv.csv_format_version_id = mfa.csv_format_version_id
        LEFT JOIN (
          SELECT
            event_id,
            exam_facility_id,
            SUM(CASE WHEN file_type = 'XML' THEN 1 ELSE 0 END) AS xml_file_count,
            SUM(CASE WHEN file_type = 'CSV' THEN 1 ELSE 0 END) AS csv_file_count
          FROM {qname(health_db())}.file_receipts
          WHERE exam_facility_id IS NOT NULL
          GROUP BY event_id, exam_facility_id
        ) receipt_counts
          ON receipt_counts.event_id = mfa.event_id
         AND receipt_counts.exam_facility_id = mfa.exam_facility_id
        LEFT JOIN {qname(dev_db())}.event ev
          ON ev.event_id = mfa.event_id
        ORDER BY mfa.is_active DESC, mfa.event_id DESC, mfa.src_folder_raw
        LIMIT %s
        """,
        (limit,),
    )
    rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        row["expected_source_mode_label"] = source_mode_label(row.get("expected_source_mode"))
        row["source_mode_filter_tokens"] = source_mode_filter_tokens(row.get("expected_source_mode"))
        row["receipt_source_mode_label"] = receipt_source_mode_label(
            row.get("xml_file_count"),
            row.get("csv_file_count"),
        )
    return rows


def enrich_folder_alias_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for row in rows:
        row["expected_source_mode_label"] = source_mode_label(row.get("expected_source_mode"))
        row["source_mode_filter_tokens"] = source_mode_filter_tokens(row.get("expected_source_mode"))
        row["receipt_source_mode_label"] = receipt_source_mode_label(
            row.get("xml_file_count"),
            row.get("csv_file_count"),
        )
    return rows


def load_received_folder_alias_rows(cur: Any, *, limit: int = 2000) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        WITH receipt_counts AS (
          SELECT
            event_id,
            exam_facility_id,
            SUM(CASE WHEN file_type = 'XML' THEN 1 ELSE 0 END) AS xml_file_count,
            SUM(CASE WHEN file_type = 'CSV' THEN 1 ELSE 0 END) AS csv_file_count
          FROM {qname(health_db())}.file_receipts
          WHERE exam_facility_id IS NOT NULL
          GROUP BY event_id, exam_facility_id
        ),
        alias_ranked AS (
          SELECT
            mfa.*,
            ROW_NUMBER() OVER (
              PARTITION BY mfa.event_id, mfa.exam_facility_id
              ORDER BY mfa.is_active DESC, mfa.updated_at DESC, mfa.alias_id DESC
            ) AS alias_rank
          FROM {qname(master_db())}.medical_folder_aliases mfa
          WHERE mfa.exam_facility_id IS NOT NULL
        )
        SELECT
          mfa.alias_id,
          receipt_counts.event_id,
          ev.event_name,
          ev.event_year,
          mfa.src_folder_raw,
          mfa.dst_folder_norm,
          receipt_counts.exam_facility_id,
          mfa.expected_source_mode,
          mfa.csv_format_version_id,
          ef.exam_facility_code,
          ef.exam_facility_name,
          ef.exam_facility_display_name,
          cfv.mapping_version AS csv_mapping_version,
          cfv.format_name AS csv_format_name,
          cfv.is_active AS csv_format_is_active,
          receipt_counts.xml_file_count,
          receipt_counts.csv_file_count,
          mfa.manual_judgement,
          mfa.note,
          COALESCE(mfa.is_active, 0) AS is_active,
          mfa.updated_at
        FROM receipt_counts
        LEFT JOIN alias_ranked mfa
          ON mfa.event_id = receipt_counts.event_id
         AND mfa.exam_facility_id = receipt_counts.exam_facility_id
         AND mfa.alias_rank = 1
        LEFT JOIN {qname(master_db())}.exam_facilities ef
          ON ef.exam_facility_id = receipt_counts.exam_facility_id
        LEFT JOIN {qname(master_db())}.csv_format_versions cfv
          ON cfv.csv_format_version_id = mfa.csv_format_version_id
        LEFT JOIN {qname(dev_db())}.event ev
          ON ev.event_id = receipt_counts.event_id
        ORDER BY COALESCE(mfa.is_active, 0) DESC, receipt_counts.event_id DESC, COALESCE(mfa.src_folder_raw, ef.exam_facility_name)
        LIMIT %s
        """,
        (limit,),
    )
    return enrich_folder_alias_rows([dict(row) for row in cur.fetchall()])


def load_subscriber_match_issue_folder_alias_rows(
    cur: Any,
    *,
    filters: dict[str, str],
    limit: int = 2000,
) -> list[dict[str, Any]]:
    issue_parts, issue_params = subscriber_match_issue_where_parts(filters, include_facility=False)
    issue_parts.append("el.exam_facility_id IS NOT NULL")
    issue_where_sql = " AND ".join(issue_parts)
    cur.execute(
        f"""
        WITH issue_facilities AS (
          SELECT DISTINCT el.event_id, el.exam_facility_id
          FROM {qname(health_db())}.exam_ledgers AS el
          WHERE {issue_where_sql}
        ),
        alias_ranked AS (
          SELECT
            mfa.*,
            ROW_NUMBER() OVER (
              PARTITION BY mfa.event_id, mfa.exam_facility_id
              ORDER BY mfa.is_active DESC, mfa.updated_at DESC, mfa.alias_id DESC
            ) AS alias_rank
          FROM {qname(master_db())}.medical_folder_aliases mfa
          WHERE mfa.exam_facility_id IS NOT NULL
        ),
        receipt_counts AS (
          SELECT
            event_id,
            exam_facility_id,
            SUM(CASE WHEN file_type = 'XML' THEN 1 ELSE 0 END) AS xml_file_count,
            SUM(CASE WHEN file_type = 'CSV' THEN 1 ELSE 0 END) AS csv_file_count
          FROM {qname(health_db())}.file_receipts
          WHERE exam_facility_id IS NOT NULL
          GROUP BY event_id, exam_facility_id
        )
        SELECT
          mfa.alias_id,
          issue_facilities.event_id,
          ev.event_name,
          ev.event_year,
          mfa.src_folder_raw,
          mfa.dst_folder_norm,
          issue_facilities.exam_facility_id,
          mfa.expected_source_mode,
          mfa.csv_format_version_id,
          ef.exam_facility_code,
          ef.exam_facility_name,
          ef.exam_facility_display_name,
          cfv.mapping_version AS csv_mapping_version,
          cfv.format_name AS csv_format_name,
          cfv.is_active AS csv_format_is_active,
          receipt_counts.xml_file_count,
          receipt_counts.csv_file_count,
          mfa.manual_judgement,
          mfa.note,
          COALESCE(mfa.is_active, 0) AS is_active,
          mfa.updated_at
        FROM issue_facilities
        LEFT JOIN alias_ranked mfa
          ON mfa.event_id = issue_facilities.event_id
         AND mfa.exam_facility_id = issue_facilities.exam_facility_id
         AND mfa.alias_rank = 1
        LEFT JOIN receipt_counts
          ON receipt_counts.event_id = issue_facilities.event_id
         AND receipt_counts.exam_facility_id = issue_facilities.exam_facility_id
        LEFT JOIN {qname(master_db())}.exam_facilities ef
          ON ef.exam_facility_id = issue_facilities.exam_facility_id
        LEFT JOIN {qname(master_db())}.csv_format_versions cfv
          ON cfv.csv_format_version_id = mfa.csv_format_version_id
        LEFT JOIN {qname(dev_db())}.event ev
          ON ev.event_id = issue_facilities.event_id
        ORDER BY COALESCE(mfa.is_active, 0) DESC, issue_facilities.event_id DESC, COALESCE(mfa.src_folder_raw, ef.exam_facility_name)
        LIMIT %s
        """,
        (*issue_params, limit),
    )
    return enrich_folder_alias_rows([dict(row) for row in cur.fetchall()])


def load_subscriber_match_issue_month_options(
    cur: Any,
    *,
    filters: dict[str, str],
    limit: int = 36,
) -> list[dict[str, Any]]:
    where_parts, params = subscriber_match_issue_where_parts(filters, include_exam_month=False)
    where_sql = " AND ".join(where_parts)
    cur.execute(
        f"""
        SELECT
          CASE WHEN el.exam_date IS NULL THEN '不明' ELSE DATE_FORMAT(el.exam_date, '%Y-%m') END AS exam_month,
          CASE WHEN el.exam_date IS NULL THEN '不明' ELSE DATE_FORMAT(el.exam_date, '%Y-%m') END AS exam_month_label,
          COUNT(*) AS ledger_count
        FROM {qname(health_db())}.exam_ledgers AS el
        WHERE {where_sql}
        GROUP BY CASE WHEN el.exam_date IS NULL THEN '不明' ELSE DATE_FORMAT(el.exam_date, '%Y-%m') END
        ORDER BY
          CASE WHEN exam_month = '不明' THEN 1 ELSE 0 END,
          exam_month DESC
        LIMIT %s
        """,
        (*params, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def load_csv_format_options(cur: Any, *, limit: int = 500) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          cfv.csv_format_version_id,
          cfv.exam_facility_id,
          cfv.mapping_version,
          cfv.format_name,
          cfv.is_default_for_facility,
          cfv.is_active,
          ef.exam_facility_code,
          ef.exam_facility_name,
          ef.exam_facility_display_name
        FROM {qname(master_db())}.csv_format_versions cfv
        LEFT JOIN {qname(master_db())}.exam_facilities ef
          ON ef.exam_facility_id = cfv.exam_facility_id
        ORDER BY cfv.is_active DESC, cfv.is_default_for_facility DESC, ef.exam_facility_code, cfv.mapping_version
        LIMIT %s
        """,
        (limit,),
    )
    return [dict(row) for row in cur.fetchall()]


def load_csv_mapping_template_rows(cur: Any, *, keyword: str = "", limit: int = 500) -> list[dict[str, Any]]:
    where_parts = ["1=1"]
    params: list[Any] = []
    if keyword:
        like = f"%{keyword}%"
        where_parts.append(
            """
            (
              cfv.`mapping_version` LIKE %s
              OR cfv.`format_name` LIKE %s
              OR cfv.`note` LIKE %s
              OR ef.`exam_facility_code` LIKE %s
              OR ef.`medical_institution_code` LIKE %s
              OR ef.`exam_facility_name` LIKE %s
              OR ef.`exam_facility_display_name` LIKE %s
            )
            """
        )
        params.extend([like, like, like, like, like, like, like])
    where_sql = " AND ".join(where_parts)
    has_rule_origin_type = manual_exam_entry_column_exists(
        cur, master_db(), "csv_exam_result_mapping_rules", "rule_origin_type"
    )
    has_edit_capability = manual_exam_entry_column_exists(
        cur, master_db(), "csv_exam_result_mapping_rules", "edit_capability"
    )
    edit_capability_expr = "r.`edit_capability`" if has_edit_capability else "'VIEW_ONLY'"
    cur.execute(
        f"""
        SELECT
          cfv.`csv_format_version_id`,
          cfv.`exam_facility_id`,
          cfv.`mapping_version`,
          cfv.`file_type`,
          cfv.`format_name`,
          cfv.`has_header`,
          cfv.`header_mode`,
          cfv.`header_structure_type`,
          cfv.`data_start_row_no`,
          cfv.`header_hash_status`,
          cfv.`header_mismatch_policy`,
          cfv.`character_encoding`,
          cfv.`delimiter`,
          cfv.`is_default_for_facility`,
          cfv.`is_active`,
          cfv.`valid_from`,
          cfv.`valid_to`,
          cfv.`updated_at`,
          ef.`exam_facility_code`,
          ef.`medical_institution_code`,
          ef.`exam_facility_name`,
          ef.`exam_facility_display_name`,
          COALESCE(rule_counts.`rule_count`, 0) AS `rule_count`,
          COALESCE(rule_counts.`active_rule_count`, 0) AS `active_rule_count`,
          COALESCE(rule_counts.`editable_rule_count`, 0) AS `editable_rule_count`,
          COALESCE(rule_counts.`view_only_rule_count`, 0) AS `view_only_rule_count`,
          COALESCE(rule_counts.`condition_count`, 0) AS `condition_count`,
          COALESCE(receipt_counts.`receipt_count`, 0) AS `receipt_count`,
          COALESCE(ledger_counts.`ledger_count`, 0) AS `ledger_count`,
          COALESCE(alias_counts.`alias_count`, 0) AS `alias_count`,
          COALESCE(alias_counts.`active_alias_count`, 0) AS `active_alias_count`,
          alias_counts.`alias_summary`
        FROM {qname(master_db())}.`csv_format_versions` AS cfv
        LEFT JOIN {qname(master_db())}.`exam_facilities` AS ef
          ON ef.`exam_facility_id` = cfv.`exam_facility_id`
        LEFT JOIN (
          SELECT
            r.`csv_format_version_id`,
            COUNT(*) AS `rule_count`,
            SUM(CASE WHEN r.`is_active` = 1 THEN 1 ELSE 0 END) AS `active_rule_count`,
            SUM(CASE WHEN {edit_capability_expr} = 'BASIC_SIMPLE' THEN 1 ELSE 0 END) AS `editable_rule_count`,
            SUM(CASE WHEN {edit_capability_expr} <> 'BASIC_SIMPLE' THEN 1 ELSE 0 END) AS `view_only_rule_count`,
            COUNT(c.`csv_exam_result_mapping_condition_id`) AS `condition_count`
          FROM {qname(master_db())}.`csv_exam_result_mapping_rules` AS r
          LEFT JOIN {qname(master_db())}.`csv_exam_result_mapping_conditions` AS c
            ON c.`csv_exam_result_mapping_rule_id` = r.`csv_exam_result_mapping_rule_id`
          GROUP BY r.`csv_format_version_id`
        ) AS rule_counts
          ON rule_counts.`csv_format_version_id` = cfv.`csv_format_version_id`
        LEFT JOIN (
          SELECT `matched_csv_format_version_id`, COUNT(*) AS `receipt_count`
          FROM {qname(health_db())}.`file_receipts`
          WHERE `matched_csv_format_version_id` IS NOT NULL
          GROUP BY `matched_csv_format_version_id`
        ) AS receipt_counts
          ON receipt_counts.`matched_csv_format_version_id` = cfv.`csv_format_version_id`
        LEFT JOIN (
          SELECT `mapping_version`, COUNT(*) AS `ledger_count`
          FROM {qname(health_db())}.`exam_ledgers`
          WHERE `source_type` = 'CSV'
            AND `mapping_version` IS NOT NULL
          GROUP BY `mapping_version`
        ) AS ledger_counts
          ON ledger_counts.`mapping_version` = cfv.`mapping_version`
        LEFT JOIN (
          SELECT
            mfa.`csv_format_version_id`,
            COUNT(*) AS `alias_count`,
            SUM(CASE WHEN mfa.`is_active` = 1 THEN 1 ELSE 0 END) AS `active_alias_count`,
            GROUP_CONCAT(mfa.`src_folder_raw` ORDER BY mfa.`is_active` DESC, mfa.`updated_at` DESC, mfa.`alias_id` DESC SEPARATOR '\n') AS `alias_summary`
          FROM {qname(master_db())}.`medical_folder_aliases` AS mfa
          WHERE mfa.`csv_format_version_id` IS NOT NULL
          GROUP BY mfa.`csv_format_version_id`
        ) AS alias_counts
          ON alias_counts.`csv_format_version_id` = cfv.`csv_format_version_id`
        WHERE {where_sql}
        ORDER BY cfv.`is_active` DESC, cfv.`is_default_for_facility` DESC, ef.`exam_facility_code`, cfv.`mapping_version`
        LIMIT %s
        """,
        (*params, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def load_csv_mapping_template_detail(cur: Any, *, csv_format_version_id: int) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT
          cfv.*,
          ef.`exam_facility_code`,
          ef.`medical_institution_code`,
          ef.`exam_facility_name`,
          ef.`exam_facility_display_name`,
          ef.`address` AS `exam_facility_address`,
          ef.`phone_number` AS `exam_facility_phone_number`
        FROM {qname(master_db())}.`csv_format_versions` AS cfv
        LEFT JOIN {qname(master_db())}.`exam_facilities` AS ef
          ON ef.`exam_facility_id` = cfv.`exam_facility_id`
        WHERE cfv.`csv_format_version_id` = %s
        """,
        (csv_format_version_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def load_csv_mapping_template_alias_rows(
    cur: Any, *, csv_format_version_id: int, exam_facility_id: int | None
) -> list[dict[str, Any]]:
    if not exam_facility_id:
        return []
    cur.execute(
        f"""
        SELECT
          mfa.`alias_id`,
          mfa.`event_id`,
          ev.`event_name`,
          mfa.`src_folder_raw`,
          mfa.`dst_folder_norm`,
          mfa.`expected_source_mode`,
          mfa.`csv_format_version_id`,
          cfv.`mapping_version`,
          cfv.`format_name`,
          cfv.`is_active` AS `csv_format_is_active`,
          mfa.`is_active`,
          mfa.`updated_at`
        FROM {qname(master_db())}.`medical_folder_aliases` AS mfa
        LEFT JOIN {qname(dev_db())}.`event` AS ev
          ON ev.`event_id` = mfa.`event_id`
        LEFT JOIN {qname(master_db())}.`csv_format_versions` AS cfv
          ON cfv.`csv_format_version_id` = mfa.`csv_format_version_id`
        WHERE mfa.`exam_facility_id` = %s
        ORDER BY
          CASE WHEN mfa.`csv_format_version_id` = %s THEN 0 ELSE 1 END,
          mfa.`is_active` DESC,
          mfa.`event_id` DESC,
          mfa.`src_folder_raw`
        """,
        (exam_facility_id, csv_format_version_id),
    )
    rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        row["expected_source_mode_label"] = source_mode_label(row.get("expected_source_mode"))
        row["is_current_template"] = int(row.get("csv_format_version_id") or 0) == int(csv_format_version_id)
    return rows


def load_csv_mapping_template_rule_summary(cur: Any, *, csv_format_version_id: int) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          r.`target_kind`,
          r.`target_resolution_type`,
          r.`selection_mode`,
          COUNT(*) AS `rule_count`,
          SUM(CASE WHEN r.`is_active` = 1 THEN 1 ELSE 0 END) AS `active_rule_count`,
          SUM(CASE WHEN r.`is_required` = 1 THEN 1 ELSE 0 END) AS `required_rule_count`,
          COUNT(c.`csv_exam_result_mapping_condition_id`) AS `condition_count`
        FROM {qname(master_db())}.`csv_exam_result_mapping_rules` AS r
        LEFT JOIN {qname(master_db())}.`csv_exam_result_mapping_conditions` AS c
          ON c.`csv_exam_result_mapping_rule_id` = r.`csv_exam_result_mapping_rule_id`
        WHERE r.`csv_format_version_id` = %s
        GROUP BY r.`target_kind`, r.`target_resolution_type`, r.`selection_mode`
        ORDER BY r.`target_kind`, r.`target_resolution_type`, r.`selection_mode`
        """,
        (csv_format_version_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def load_csv_mapping_template_rules(cur: Any, *, csv_format_version_id: int, limit: int = 1000) -> list[dict[str, Any]]:
    has_rule_origin_type = manual_exam_entry_column_exists(
        cur, master_db(), "csv_exam_result_mapping_rules", "rule_origin_type"
    )
    has_edit_capability = manual_exam_entry_column_exists(
        cur, master_db(), "csv_exam_result_mapping_rules", "edit_capability"
    )
    rule_origin_select = "r.`rule_origin_type`" if has_rule_origin_type else "'SEED'"
    edit_capability_select = "r.`edit_capability`" if has_edit_capability else "'VIEW_ONLY'"
    cur.execute(
        f"""
        SELECT
          r.`csv_exam_result_mapping_rule_id`,
          r.`rule_key`,
          r.`target_kind`,
          r.`target_resolution_type`,
          r.`selection_mode`,
          r.`selection_group_code`,
          r.`target_namecode`,
          eim.`item_name` AS `target_item_name`,
          eim.`category_name` AS `target_category_name`,
          r.`target_identity_item_code`,
          r.`target_field`,
          r.`method_structure_type`,
          r.`value_source_type`,
          r.`fixed_value`,
          r.`value_join_separator`,
          r.`value_exclude_values`,
          r.`raw_value_type`,
          r.`raw_unit`,
          {rule_origin_select} AS `rule_origin_type`,
          {edit_capability_select} AS `edit_capability`,
          r.`is_required`,
          r.`priority`,
          r.`note`,
          r.`is_active`,
          COUNT(c.`csv_exam_result_mapping_condition_id`) AS `condition_count`,
          SUM(CASE WHEN c.`locator_type` IN ('HEADER_NAME', 'HEADER_CONTEXT_AND_NAME') THEN 1 ELSE 0 END) AS `header_name_condition_count`,
          SUM(CASE WHEN c.`locator_type` = 'COLUMN_NO' THEN 1 ELSE 0 END) AS `column_no_condition_count`,
          SUM(CASE WHEN c.`locator_type` = 'HEADER_AND_COLUMN' THEN 1 ELSE 0 END) AS `header_and_column_condition_count`,
          GROUP_CONCAT(DISTINCT NULLIF(c.`header_name`, '') ORDER BY c.`priority`, c.`csv_exam_result_mapping_condition_id` SEPARATOR ' / ') AS `condition_header_names`,
          GROUP_CONCAT(DISTINCT c.`column_no` ORDER BY c.`column_no` SEPARATOR ' / ') AS `condition_column_nos`
        FROM {qname(master_db())}.`csv_exam_result_mapping_rules` AS r
        LEFT JOIN {qname(master_db())}.`csv_exam_result_mapping_conditions` AS c
          ON c.`csv_exam_result_mapping_rule_id` = r.`csv_exam_result_mapping_rule_id`
        LEFT JOIN {qname(dev_db())}.`exam_item_master` AS eim
          ON eim.`namecode` = r.`target_namecode`
        WHERE r.`csv_format_version_id` = %s
        GROUP BY r.`csv_exam_result_mapping_rule_id`
        ORDER BY r.`is_active` DESC, r.`priority`, r.`rule_key`
        LIMIT %s
        """,
        (csv_format_version_id, limit),
    )
    rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        row["locator_summary"] = csv_mapping_locator_summary(row)
        inferred_screen_simple = is_csv_mapping_screen_simple_rule(row)
        if not has_rule_origin_type and inferred_screen_simple:
            row["rule_origin_type"] = "SCREEN"
        if not has_edit_capability and inferred_screen_simple:
            row["edit_capability"] = "BASIC_SIMPLE"
        row["rule_origin_label"] = csv_mapping_rule_origin_label(row.get("rule_origin_type"))
        row["edit_capability_label"] = csv_mapping_edit_capability_label(row.get("edit_capability"))
        row["is_screen_editable"] = (
            str(row.get("edit_capability") or "").upper() == "BASIC_SIMPLE"
            or inferred_screen_simple
        )
    return rows


def load_csv_mapping_template_conditions(cur: Any, *, csv_format_version_id: int, limit: int = 2000) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          c.`csv_exam_result_mapping_condition_id`,
          c.`csv_exam_result_mapping_rule_id`,
          r.`rule_key`,
          r.`target_kind`,
          r.`target_namecode`,
          r.`target_field`,
          c.`condition_group_no`,
          c.`condition_type`,
          c.`locator_type`,
          c.`header_context`,
          c.`header_name`,
          c.`header_occurrence`,
          c.`column_no`,
          c.`operator`,
          c.`expected_value`,
          c.`expected_value_normalized`,
          c.`source_role`,
          c.`priority`,
          c.`note`,
          c.`is_active`
        FROM {qname(master_db())}.`csv_exam_result_mapping_conditions` AS c
        JOIN {qname(master_db())}.`csv_exam_result_mapping_rules` AS r
          ON r.`csv_exam_result_mapping_rule_id` = c.`csv_exam_result_mapping_rule_id`
        WHERE r.`csv_format_version_id` = %s
        ORDER BY r.`priority`, r.`rule_key`, c.`condition_group_no`, c.`priority`, c.`csv_exam_result_mapping_condition_id`
        LIMIT %s
        """,
        (csv_format_version_id, limit),
    )
    rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        row["locator_type_label"] = csv_mapping_locator_type_label(row.get("locator_type"))
    return rows


def csv_mapping_locator_type_label(locator_type: Any) -> str:
    value = str(locator_type or "").strip().upper()
    labels = {
        "HEADER_NAME": "ヘッダー名",
        "HEADER_CONTEXT_AND_NAME": "context+ヘッダー",
        "COLUMN_NO": "CSV列番",
        "HEADER_AND_COLUMN": "ヘッダー+列番",
    }
    return labels.get(value, value or "-")


def csv_mapping_locator_summary(row: Mapping[str, Any]) -> str:
    parts: list[str] = []
    header_name_count = int(row.get("header_name_condition_count") or 0)
    column_no_count = int(row.get("column_no_condition_count") or 0)
    header_and_column_count = int(row.get("header_and_column_condition_count") or 0)
    if header_name_count:
        parts.append(f"ヘッダー名 {header_name_count}")
    if column_no_count:
        parts.append(f"CSV列番 {column_no_count}")
    if header_and_column_count:
        parts.append(f"両方 {header_and_column_count}")
    return " / ".join(parts) or "-"


def csv_mapping_condition_locator_text(condition: Mapping[str, Any]) -> str:
    header_name = str(condition.get("header_name") or "").strip()
    column_no = condition.get("column_no")
    if header_name and column_no:
        return f"{header_name}（CSV列{column_no}）"
    if header_name:
        return header_name
    if column_no:
        return f"CSV列{column_no}"
    return "-"


def csv_mapping_selection_mode_label(selection_mode: Any) -> str:
    value = str(selection_mode or "").strip().upper()
    labels = {
        "DIRECT": "単一ルール",
        "EXCLUSIVE_ONE": "候補のいずれか1つ",
        "MULTI_ENTRY": "複数entry作成",
    }
    return labels.get(value, value or "-")


def csv_mapping_source_role_label(source_role: Any) -> str:
    value = str(source_role or "VALUE").strip().upper()
    labels = {
        "VALUE": "値",
        "LOWER_LIMIT": "下限",
        "UPPER_LIMIT": "上限",
        "JUDGEMENT": "判定",
        "METHOD": "方式",
        "QUALIFIER": "補助条件",
    }
    return labels.get(value, value or "-")


def csv_mapping_rule_origin_label(value: Any) -> str:
    key = str(value or "SEED").strip().upper()
    labels = {
        "SEED": "裏投入",
        "MIGRATION": "migration",
        "SCREEN": "画面作成",
        "IMPORT": "取込作成",
    }
    return labels.get(key, key or "-")


def csv_mapping_edit_capability_label(value: Any) -> str:
    key = str(value or "VIEW_ONLY").strip().upper()
    labels = {
        "BASIC_SIMPLE": "編集可",
        "VIEW_ONLY": "表示のみ",
        "UNSUPPORTED": "未実装",
    }
    return labels.get(key, key or "-")


def is_csv_mapping_screen_simple_rule(row: Mapping[str, Any]) -> bool:
    method = str(row.get("method_structure_type") or "").upper()
    value_source = str(row.get("value_source_type") or "").upper()
    selection_mode = str(row.get("selection_mode") or "").upper()
    target_kind = str(row.get("target_kind") or "").upper()
    if method not in {"SINGLE_COLUMN", "MULTI_COLUMN_JOIN"}:
        return False
    if value_source != "SOURCE" or selection_mode != "DIRECT":
        return False
    if target_kind not in {"LEDGER_FIELD", "EXAM_ITEM_VALUE"}:
        return False
    if row.get("fixed_value") not in (None, ""):
        return False
    return True


def csv_mapping_operator_label(operator: Any) -> str:
    value = str(operator or "").strip().upper()
    labels = {
        "PRESENT": "存在",
        "NOT_EMPTY": "空でない",
        "EMPTY": "空",
        "EQUALS": "=",
        "NOT_EQUALS": "!=",
        "IN": "いずれか",
        "NOT_IN": "含まない",
    }
    return labels.get(value, value or "-")


def enrich_csv_mapping_template_rules_with_conditions(
    rules: list[dict[str, Any]],
    conditions: list[Mapping[str, Any]],
) -> None:
    conditions_by_rule_id: dict[int, list[Mapping[str, Any]]] = {}
    for condition in conditions:
        rule_id = _optional_int(condition.get("csv_exam_result_mapping_rule_id"))
        if rule_id:
            conditions_by_rule_id.setdefault(rule_id, []).append(condition)

    for rule in rules:
        rule_id = _optional_int(rule.get("csv_exam_result_mapping_rule_id"))
        rule_conditions = conditions_by_rule_id.get(rule_id or 0, [])
        groups: dict[int, list[Mapping[str, Any]]] = {}
        for condition in rule_conditions:
            groups.setdefault(_optional_int(condition.get("condition_group_no")) or 1, []).append(condition)

        value_conditions = [
            condition
            for condition in rule_conditions
            if str(condition.get("source_role") or "VALUE").upper() == "VALUE"
            and str(condition.get("condition_type") or "").upper() in {"HEADER_MATCH", "SOURCE_COLUMN"}
        ]
        role_labels = sorted(
            {
                csv_mapping_source_role_label(condition.get("source_role"))
                for condition in rule_conditions
            }
        )
        detail_parts: list[str] = [csv_mapping_selection_mode_label(rule.get("selection_mode"))]
        if str(rule.get("value_source_type") or "").upper() == "FIXED":
            detail_parts.append(f"固定値: {rule.get('fixed_value') or '-'}")
        elif len(value_conditions) > 1:
            separator = rule.get("value_join_separator")
            detail_parts.append(f"値列{len(value_conditions)}列を結合")
            detail_parts.append(f"区切り: {separator if separator is not None else '未設定'}")
        elif len(value_conditions) == 1:
            detail_parts.append("値列1列")
        else:
            detail_parts.append("値列なし")

        if len(groups) > 1:
            detail_parts.append(f"{len(groups)}パターンのいずれか")
        elif groups:
            condition_count = len(next(iter(groups.values())))
            detail_parts.append("条件なし" if condition_count <= 1 else f"{condition_count}条件AND")
        if role_labels:
            detail_parts.append("役割: " + " / ".join(role_labels))

        group_summaries: list[str] = []
        for group_no in sorted(groups):
            group_conditions = sorted(
                groups[group_no],
                key=lambda condition: (
                    _optional_int(condition.get("priority")) or 0,
                    _optional_int(condition.get("csv_exam_result_mapping_condition_id")) or 0,
                ),
            )
            condition_texts: list[str] = []
            for condition in group_conditions:
                source_role = csv_mapping_source_role_label(condition.get("source_role"))
                locator = csv_mapping_condition_locator_text(condition)
                operator = csv_mapping_operator_label(condition.get("operator"))
                expected = str(condition.get("expected_value") or "").strip()
                if expected:
                    condition_texts.append(f"{source_role}:{locator} {operator} {expected}")
                else:
                    condition_texts.append(f"{source_role}:{locator} {operator}")
            group_summaries.append(f"group {group_no}: " + " AND ".join(condition_texts))

        rule["rule_content_summary"] = " / ".join(detail_parts)
        rule["rule_condition_summary"] = " OR ".join(group_summaries) or "-"
        rule["value_condition_summary"] = " / ".join(
            csv_mapping_condition_locator_text(condition) for condition in value_conditions
        )
        rule["screen_edit_payload"] = {
            "ruleId": rule_id,
            "mode": "many" if str(rule.get("method_structure_type") or "").upper() == "MULTI_COLUMN_JOIN" else "one",
            "targetKind": str(rule.get("target_kind") or ""),
            "targetName": str(rule.get("target_item_name") or rule.get("target_field") or ""),
            "targetCode": str(rule.get("target_namecode") or rule.get("target_field") or ""),
            "targetMeta": str(rule.get("target_category_name") or rule.get("target_resolution_type") or ""),
            "targetCategory": str(rule.get("target_category_name") or ""),
            "targetValueType": str(rule.get("raw_value_type") or ""),
            "headers": [
                {
                    "columnNo": str(condition.get("column_no") or ""),
                    "headerName": str(condition.get("header_name") or ""),
                    "headerContext": str(condition.get("header_context") or ""),
                }
                for condition in value_conditions
            ],
        }


def build_csv_mapping_template_target_groups(rules: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    group_by_key: dict[tuple[str, str], dict[str, Any]] = {}

    for rule in rules:
        target_kind = str(rule.get("target_kind") or "-")
        target_code = str(
            rule.get("target_namecode")
            or rule.get("target_field")
            or rule.get("target_identity_item_code")
            or "-"
        )
        key = (target_kind, target_code)
        if key not in group_by_key:
            group = {
                "target_kind": target_kind,
                "target_code": target_code,
                "target_item_name": rule.get("target_item_name"),
                "target_category_name": rule.get("target_category_name"),
                "target_resolution_type": rule.get("target_resolution_type"),
                "selection_mode": rule.get("selection_mode"),
                "rows": [],
            }
            group_by_key[key] = group
            groups.append(group)

        value_source_type = str(rule.get("value_source_type") or "-")
        fixed_value = str(rule.get("fixed_value") or "").strip()
        if value_source_type.upper() == "FIXED":
            registered_value = fixed_value or "-"
        else:
            registered_value = rule.get("value_condition_summary") or rule.get("condition_header_names") or "-"

        group_by_key[key]["rows"].append(
            {
                "rule_key": rule.get("rule_key"),
                "rule_id": rule.get("csv_exam_result_mapping_rule_id"),
                "priority": rule.get("priority"),
                "source_columns": rule.get("value_condition_summary") or rule.get("condition_header_names") or "-",
                "condition_summary": rule.get("rule_condition_summary") or "-",
                "registered_value": registered_value,
                "value_source_type": value_source_type,
                "note": rule.get("note"),
                "is_active": rule.get("is_active"),
                "is_required": rule.get("is_required"),
            }
        )

    return groups


def build_csv_mapping_template_header_columns(
    template: Mapping[str, Any],
    conditions: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    snapshot_raw = template.get("header_snapshot_json")
    if not snapshot_raw:
        return []
    if isinstance(snapshot_raw, str):
        try:
            snapshot = json.loads(snapshot_raw)
        except json.JSONDecodeError:
            return []
    elif isinstance(snapshot_raw, Mapping):
        snapshot = snapshot_raw
    else:
        return []

    normalized_columns = snapshot.get("normalized_columns")
    if not isinstance(normalized_columns, list):
        return []

    mapped_by_column: dict[int, list[str]] = {}
    mapped_by_header: dict[tuple[str, str, int], list[str]] = {}
    for condition in conditions:
        if not condition.get("is_active"):
            continue
        rule_key = str(condition.get("rule_key") or "-")
        column_no = _optional_int(condition.get("column_no"))
        if column_no:
            mapped_by_column.setdefault(column_no, []).append(rule_key)
        header_name = str(condition.get("header_name") or "").strip()
        if header_name:
            header_context = str(condition.get("header_context") or "").strip()
            occurrence = _optional_int(condition.get("header_occurrence")) or 1
            mapped_by_header.setdefault((header_context, header_name, occurrence), []).append(rule_key)

    rows: list[dict[str, Any]] = []
    for column in normalized_columns:
        if not isinstance(column, Mapping):
            continue
        column_no = _optional_int(column.get("column_no"))
        if not column_no:
            continue
        context = str(column.get("context") or "").strip()
        name = str(column.get("name") or column.get("header_name") or "").strip()
        occurrence = _optional_int(column.get("occurrence")) or 1
        mapped_rules = [
            *mapped_by_column.get(column_no, []),
            *mapped_by_header.get((context, name, occurrence), []),
        ]
        rows.append(
            {
                "column_no": column_no,
                "context": context,
                "name": name,
                "occurrence": occurrence,
                "mapped_rules": sorted(set(mapped_rules)),
                "is_mapped": bool(mapped_rules),
            }
        )
    return rows


def build_csv_mapping_template_header_preview_payload(
    template: Mapping[str, Any] | None,
    header_columns: list[Mapping[str, Any]],
) -> dict[str, Any]:
    if not template:
        return {"header_rows": [], "columns": []}
    snapshot_raw = template.get("header_snapshot_json")
    snapshot: Mapping[str, Any] = {}
    if isinstance(snapshot_raw, str) and snapshot_raw:
        try:
            parsed = json.loads(snapshot_raw)
            if isinstance(parsed, Mapping):
                snapshot = parsed
        except json.JSONDecodeError:
            snapshot = {}
    elif isinstance(snapshot_raw, Mapping):
        snapshot = snapshot_raw
    header_rows = snapshot.get("header_rows")
    if not isinstance(header_rows, list):
        header_rows = []
    cleaned_header_rows = [
        [str(cell or "") for cell in row] for row in header_rows if isinstance(row, list)
    ]
    return {
        "header_rows": cleaned_header_rows,
        "columns": [
            {
                "column_no": _optional_int(column.get("column_no")) or index + 1,
                "name": str(column.get("name") or column.get("header_name") or ""),
                "context": str(column.get("context") or column.get("header_context") or ""),
                "is_mapped": bool(column.get("is_mapped")),
            }
            for index, column in enumerate(header_columns)
        ],
    }


def csv_mapping_screen_rule_key_part(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9_]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text[:80] or "field"


def load_csv_mapping_template_header_columns(
    cur: Any,
    *,
    template: Mapping[str, Any],
    conditions: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    csv_format_version_id = _optional_int(template.get("csv_format_version_id"))
    if not csv_format_version_id:
        return build_csv_mapping_template_header_columns(template, conditions)
    if not manual_exam_entry_table_exists(cur, master_db(), "csv_format_header_columns"):
        return build_csv_mapping_template_header_columns(template, conditions)

    mapped_by_column: dict[int, list[str]] = {}
    mapped_by_header: dict[tuple[str, str, int], list[str]] = {}
    for condition in conditions:
        if not condition.get("is_active"):
            continue
        rule_key = str(condition.get("rule_key") or "-")
        column_no = _optional_int(condition.get("column_no"))
        if column_no:
            mapped_by_column.setdefault(column_no, []).append(rule_key)
        header_name = str(condition.get("header_name") or "").strip()
        if header_name:
            header_context = str(condition.get("header_context") or "").strip()
            occurrence = _optional_int(condition.get("header_occurrence")) or 1
            mapped_by_header.setdefault((header_context, header_name, occurrence), []).append(rule_key)

    cur.execute(
        f"""
        SELECT
          `csv_format_header_column_id`,
          `csv_format_version_id`,
          `column_no`,
          `header_context`,
          `header_name`,
          `normalized_header_name`,
          `header_occurrence`
        FROM {qname(master_db())}.`csv_format_header_columns`
        WHERE `csv_format_version_id` = %s
        ORDER BY `column_no`
        """,
        (csv_format_version_id,),
    )
    rows: list[dict[str, Any]] = []
    for row in cur.fetchall():
        column = dict(row)
        column_no = _optional_int(column.get("column_no")) or 0
        context = str(column.get("header_context") or "").strip()
        name = str(column.get("header_name") or "").strip()
        occurrence = _optional_int(column.get("header_occurrence")) or 1
        mapped_rules = [
            *mapped_by_column.get(column_no, []),
            *mapped_by_header.get((context, name, occurrence), []),
        ]
        column["context"] = context
        column["name"] = name
        column["occurrence"] = occurrence
        column["mapped_rules"] = sorted(set(mapped_rules))
        column["is_mapped"] = bool(mapped_rules)
        rows.append(column)

    return rows or build_csv_mapping_template_header_columns(template, conditions)


def summarize_csv_header_context(column: Mapping[str, Any], *, neighbor_count: int = 2) -> str:
    parts = []
    context = str(column.get("context") or column.get("header_context") or "").strip()
    if context:
        parts.append(f"context: {context}")
    previous_names = column.get("previous_names")
    next_names = column.get("next_names")
    if previous_names:
        parts.append("前: " + " / ".join(str(value) for value in previous_names if value))
    if next_names:
        parts.append("後: " + " / ".join(str(value) for value in next_names if value))
    return " | ".join(parts) or "-"


def attach_csv_header_neighbors(columns: list[dict[str, Any]], *, neighbor_count: int = 2) -> list[dict[str, Any]]:
    for index, column in enumerate(columns):
        previous_columns = columns[max(0, index - neighbor_count):index]
        next_columns = columns[index + 1:index + 1 + neighbor_count]
        column["previous_names"] = [str(item.get("name") or item.get("header_name") or "") for item in previous_columns]
        column["next_names"] = [str(item.get("name") or item.get("header_name") or "") for item in next_columns]
        column["context_summary"] = summarize_csv_header_context(column, neighbor_count=neighbor_count)
    return columns


def build_csv_mapping_header_compare(uploaded_columns: list[dict[str, Any]], registered_columns: list[dict[str, Any]]) -> dict[str, Any]:
    registered_by_exact: dict[tuple[str, str, int], dict[str, Any]] = {}
    registered_by_normalized: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for column in registered_columns:
        column_no = _optional_int(column.get("column_no")) or 0
        context = str(column.get("context") or column.get("header_context") or "").strip()
        name = str(column.get("name") or column.get("header_name") or "").strip()
        occurrence = _optional_int(column.get("occurrence") or column.get("header_occurrence")) or 1
        normalized = normalize_header(name) or ""
        registered_by_exact[(context, name, occurrence)] = column
        registered_by_normalized.setdefault((normalized, occurrence), []).append(column)

    matched_by_registered_column: dict[int, dict[str, Any]] = {}
    match_type_by_registered_column: dict[int, str] = {}
    unmatched_uploads: list[dict[str, Any]] = []
    for upload in uploaded_columns:
        column_no = _optional_int(upload.get("column_no")) or 0
        context = str(upload.get("context") or "").strip()
        name = str(upload.get("name") or "").strip()
        occurrence = _optional_int(upload.get("occurrence")) or 1
        normalized = normalize_header(name) or ""
        matched = registered_by_exact.get((context, name, occurrence))
        match_type = "EXACT"
        if not matched:
            normalized_matches = registered_by_normalized.get((normalized, occurrence), [])
            if len(normalized_matches) == 1:
                matched = normalized_matches[0]
                match_type = "NORMALIZED"
        if not matched:
            unmatched_uploads.append(upload)
            continue
        matched_no = _optional_int(matched.get("column_no")) or 0
        if matched_no and matched_no not in matched_by_registered_column:
            matched_by_registered_column[matched_no] = upload
            match_type_by_registered_column[matched_no] = match_type
        else:
            unmatched_uploads.append(upload)

    rows: list[dict[str, Any]] = []
    for column in registered_columns:
        column_no = _optional_int(column.get("column_no")) or 0
        uploaded = matched_by_registered_column.get(column_no)
        rows.append(
            {
                "uploaded": uploaded,
                "registered": column,
                "status": "MATCH" if uploaded else "REGISTERED_ONLY",
                "match_type": match_type_by_registered_column.get(column_no, "-") if uploaded else "-",
            }
        )
    return {
        "rows": rows,
        "unmatched_uploads": unmatched_uploads,
        "matched_count": sum(1 for row in rows if row["status"] == "MATCH"),
        "column_only_count": 0,
        "upload_only_count": len(unmatched_uploads),
        "registered_only_count": sum(1 for row in rows if row["status"] == "REGISTERED_ONLY"),
    }


def csv_header_snapshot_for_result(csv_result: Any) -> dict[str, Any]:
    return {
        "active_header_row_no": csv_result.header_set.active_header_row_no,
        "header_rows": csv_result.header_set.header_rows,
        "normalized_columns": [
            {
                "column_no": column.column_no,
                "context": column.context,
                "header_name": column.header_name,
                "occurrence": column.occurrence,
            }
            for column in csv_result.header_set.normalized_columns
        ],
    }


def save_csv_mapping_template_header_columns(cur: Any, *, csv_format_version_id: int, csv_result: Any) -> None:
    if not manual_exam_entry_table_exists(cur, master_db(), "csv_format_header_columns"):
        raise ValueError("csv_format_header_columnsテーブルがありません。migrationを適用してください。")
    snapshot = csv_header_snapshot_for_result(csv_result)
    cur.execute(
        f"""
        UPDATE {qname(master_db())}.`csv_format_versions`
           SET `header_sha256` = %s,
               `header_snapshot_json` = %s,
               `header_hash_status` = 'VERIFIED'
         WHERE `csv_format_version_id` = %s
        """,
        (
            csv_result.header_set.header_sha256,
            json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")),
            csv_format_version_id,
        ),
    )
    cur.execute(
        f"""
        DELETE FROM {qname(master_db())}.`csv_format_header_columns`
        WHERE `csv_format_version_id` = %s
        """,
        (csv_format_version_id,),
    )
    rows = [
        (
            csv_format_version_id,
            column.column_no,
            column.context,
            column.header_name,
            normalize_header(column.header_name),
            column.occurrence,
        )
        for column in csv_result.header_set.normalized_columns
    ]
    if rows:
        cur.executemany(
            f"""
            INSERT INTO {qname(master_db())}.`csv_format_header_columns` (
              `csv_format_version_id`,
              `column_no`,
              `header_context`,
              `header_name`,
              `normalized_header_name`,
              `header_occurrence`
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            rows,
        )


async def import_csv_mapping_template_header_upload(
    cur: Any,
    *,
    request: Request,
    user: dict[str, Any],
    csv_format_version_id: int,
    template: Mapping[str, Any],
    csv_file: UploadFile,
) -> dict[str, Any]:
    tmp_path: str | None = None
    try:
        uploaded_bytes = await csv_file.read()
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            tmp.write(uploaded_bytes)
            tmp_path = tmp.name
        header_count = max(int(template.get("data_start_row_no") or 2) - 1, 0)
        csv_result = load_csv_result(
            tmp_path,
            header_count=header_count,
            delimiter=str(template.get("delimiter") or ","),
            encoding=str(template.get("character_encoding") or "") or None,
            quote_char=str(template.get("quote_char") or '"'),
            active_header_row_no=(
                int(template["active_header_row_no"])
                if template.get("active_header_row_no") is not None
                else None
            ),
            data_start_row_no=int(template.get("data_start_row_no") or header_count + 1),
        )
        save_csv_mapping_template_header_columns(
            cur,
            csv_format_version_id=csv_format_version_id,
            csv_result=csv_result,
        )
        result = {
            "csv_format_version_id": csv_format_version_id,
            "file_name": csv_file.filename,
            "column_count": len(csv_result.header_set.normalized_columns),
            "header_sha256": csv_result.header_set.header_sha256,
        }
        log_audit(
            cur,
            request=request,
            user=user,
            action_code="CSV_MAPPING_TEMPLATE_HEADER_IMPORT",
            target_schema=master_db(),
            target_table="csv_format_header_columns",
            target_id=str(csv_format_version_id),
            after=result,
        )
        return result
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


def source_mode_label(value: Any) -> str:
    labels = {
        "UNKNOWN": "未設定",
        "XML_ONLY": "XMLのみ",
        "CSV_ONLY": "CSVのみ",
        "XML_CSV_MERGE": "XML+CSV結合",
        "PAPER_ONLY": "紙のみ",
        "XML_PAPER_MERGE": "XML+紙結合",
        "CSV_PAPER_MERGE": "CSV+紙結合",
        "XML_CSV_PAPER_MERGE": "XML+CSV+紙結合",
    }
    return labels.get(str(value or "UNKNOWN"), str(value or "未設定"))


def source_mode_is_configured(value: Any) -> bool:
    return str(value or "").strip().upper() not in ("", "UNKNOWN")


def preferred_alias_source_mode_sql() -> str:
    return """
            SUBSTRING_INDEX(
              GROUP_CONCAT(
                expected_source_mode
                ORDER BY
                  CASE
                    WHEN expected_source_mode IS NULL OR expected_source_mode = '' OR expected_source_mode = 'UNKNOWN' THEN 1
                    ELSE 0
                  END,
                  expected_source_mode
                SEPARATOR ','
              ),
              ',',
              1
            ) AS expected_source_mode
    """


def alias_source_mode_by_facility_code_sql() -> str:
    return f"""
          SELECT
            mfa.event_id,
            COALESCE(ef.exam_facility_code, SUBSTRING_INDEX(mfa.src_folder_raw, '_', 1)) AS facility_code,
{preferred_alias_source_mode_sql()}
          FROM {qname(master_db())}.medical_folder_aliases AS mfa
          LEFT JOIN {qname(master_db())}.exam_facilities AS ef
            ON ef.exam_facility_id = mfa.exam_facility_id
          WHERE mfa.is_active = 1
          GROUP BY
            mfa.event_id,
            COALESCE(ef.exam_facility_code, SUBSTRING_INDEX(mfa.src_folder_raw, '_', 1))
    """


def source_mode_options() -> list[dict[str, str]]:
    return [
        {"value": "UNKNOWN", "label": "未設定"},
        {"value": "XML_ONLY", "label": "XMLのみ"},
        {"value": "CSV_ONLY", "label": "CSVのみ"},
        {"value": "XML_CSV_MERGE", "label": "XML+CSV"},
        {"value": "PAPER_ONLY", "label": "紙のみ"},
        {"value": "XML_PAPER_MERGE", "label": "XML+紙"},
        {"value": "CSV_PAPER_MERGE", "label": "CSV+紙"},
        {"value": "XML_CSV_PAPER_MERGE", "label": "XML+CSV+紙"},
    ]


def source_mode_values() -> set[str]:
    return {option["value"] for option in source_mode_options()}


def source_mode_filter_tokens(value: Any) -> str:
    mode = str(value or "UNKNOWN")
    tokens_by_mode = {
        "UNKNOWN": "UNKNOWN",
        "XML_ONLY": "XML",
        "CSV_ONLY": "CSV",
        "XML_CSV_MERGE": "XML CSV",
        "PAPER_ONLY": "PAPER",
        "XML_PAPER_MERGE": "XML PAPER",
        "CSV_PAPER_MERGE": "CSV PAPER",
        "XML_CSV_PAPER_MERGE": "XML CSV PAPER",
    }
    return tokens_by_mode.get(mode, "UNKNOWN")


def receipt_source_mode_label(xml_count: Any, csv_count: Any) -> str:
    xml_num = int(xml_count or 0)
    csv_num = int(csv_count or 0)
    if xml_num and csv_num:
        return f"XML+CSV受領 ({xml_num}/{csv_num})"
    if xml_num:
        return f"XML受領 ({xml_num})"
    if csv_num:
        return f"CSV受領 ({csv_num})"
    return "受領実績なし"


def normalize_facility_form(form: dict[str, str]) -> dict[str, Any]:
    values = {
        "exam_facility_code": _form_text(form, "exam_facility_code"),
        "exam_facility_name": _form_text(form, "exam_facility_name"),
        "exam_facility_display_name": _form_text(form, "exam_facility_display_name"),
        "exam_facility_type": _form_text(form, "exam_facility_type"),
        "medical_institution_code": _form_text(form, "medical_institution_code"),
        "reservation_system_medical_institution_code": _form_text(
            form, "reservation_system_medical_institution_code"
        ),
        "postal_code": _form_text(form, "postal_code"),
        "address": _form_text(form, "address"),
        "phone_number": _form_text(form, "phone_number"),
        "note": _form_text(form, "note"),
        "is_active": 1 if form.get("is_active") == "1" else 0,
    }
    if not values["exam_facility_name"]:
        raise ValueError("健診機関名は必須です。")
    if not values["exam_facility_display_name"]:
        values["exam_facility_display_name"] = values["exam_facility_name"]
    return values


def resolve_exam_facility_selector(cur: Any, selector: str | None) -> int | None:
    text = (selector or "").strip()
    if not text:
        return None
    cur.execute(
        f"""
        SELECT exam_facility_id
        FROM {qname(master_db())}.exam_facilities
        WHERE exam_facility_code = %s
           OR medical_institution_code = %s
           OR reservation_system_medical_institution_code = %s
        LIMIT 1
        """,
        (text, text, text),
    )
    row = cur.fetchone()
    if row:
        return int(row["exam_facility_id"])
    if text.isdigit():
        cur.execute(
            f"""
            SELECT exam_facility_id
            FROM {qname(master_db())}.exam_facilities
            WHERE exam_facility_id = %s
            LIMIT 1
            """,
            (int(text),),
        )
        row = cur.fetchone()
        if row:
            return int(row["exam_facility_id"])
    raise ValueError("指定された健診機関ID/コードが見つかりません。")


def resolve_csv_format_version_selector(cur: Any, selector: str | None) -> int | None:
    text = (selector or "").strip()
    if not text:
        return None
    if not text.isdigit():
        raise ValueError("CSVテンプレートはIDで指定してください。")
    cur.execute(
        f"""
        SELECT csv_format_version_id
        FROM {qname(master_db())}.csv_format_versions
        WHERE csv_format_version_id = %s
        LIMIT 1
        """,
        (int(text),),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError("指定されたCSVテンプレートが見つかりません。")
    return int(row["csv_format_version_id"])


def normalize_folder_alias_form(cur: Any, form: dict[str, str]) -> dict[str, Any]:
    event_id_text = (form.get("event_id") or "").strip()
    src_folder_raw = _form_text(form, "src_folder_raw")
    dst_folder_norm = _form_text(form, "dst_folder_norm") or src_folder_raw
    expected_source_mode = _form_text(form, "expected_source_mode") or "UNKNOWN"
    if not event_id_text:
        raise ValueError("イベントは必須です。")
    if not src_folder_raw:
        raise ValueError("フォルダ名は必須です。")
    if expected_source_mode not in source_mode_values():
        raise ValueError("受領モードの指定が不正です。")
    return {
        "event_id": int(event_id_text),
        "src_folder_raw": src_folder_raw,
        "dst_folder_norm": dst_folder_norm,
        "exam_facility_id": resolve_exam_facility_selector(cur, form.get("exam_facility_selector")),
        "expected_source_mode": expected_source_mode,
        "csv_format_version_id": resolve_csv_format_version_selector(cur, form.get("csv_format_version_id")),
        "manual_judgement": 1 if form.get("manual_judgement") == "1" else 0,
        "note": _form_text(form, "note"),
        "is_active": 1 if form.get("is_active") == "1" else 0,
    }


def scan_folder_name_from_path(path_text: str | None) -> str:
    text = str(path_text or "").strip().rstrip("/\\")
    if not text:
        return ""
    return re.split(r"[\\/]+", text)[-1]


def join_display_path(root_path: str | None, child_name: str | None) -> str:
    root = str(root_path or "").strip().rstrip("/\\")
    child = str(child_name or "").strip().strip("/\\")
    if not root:
        return child
    if not child:
        return root
    separator = "\\" if "\\" in root and "/" not in root else "/"
    return f"{root}{separator}{child}"


def load_unknown_scan_folder_rows(cur: Any, *, event_id: str, limit: int = 50) -> list[dict[str, Any]]:
    excluded_folder_names = {"xml作成_出力履歴"}
    where_parts = [
        "ee.phase = 'SCAN_FILES'",
        "ee.error_code = 'UNKNOWN_MEDICAL_FOLDER'",
    ]
    params: list[Any] = []
    if event_id:
        where_parts.append("er.input_base = %s")
        params.append(f"event_id={event_id}")
    for folder_name in excluded_folder_names:
        where_parts.append("ee.field_value NOT LIKE %s")
        params.append(f"%{folder_name}%")
    params.append(limit)
    cur.execute(
        f"""
        SELECT
          MAX(ee.error_id) AS error_id,
          MAX(ee.run_id) AS run_id,
          MAX(er.started_at) AS started_at,
          MAX(er.finished_at) AS finished_at,
          er.input_base,
          ee.field_value,
          COUNT(*) AS occurrence_count
        FROM {qname(health_db())}.etl_errors AS ee
        JOIN {qname(health_db())}.etl_runs AS er
          ON er.run_id = ee.run_id
        WHERE {" AND ".join(where_parts)}
        GROUP BY er.input_base, ee.field_value
        ORDER BY MAX(ee.error_id) DESC
        LIMIT %s
        """,
        tuple(params),
    )
    rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        input_base = str(row.get("input_base") or "")
        match = re.search(r"event_id=(\d+)", input_base)
        row["event_id"] = match.group(1) if match else event_id
        row["src_folder_raw"] = scan_folder_name_from_path(str(row.get("field_value") or ""))
    rows = [row for row in rows if str(row.get("src_folder_raw") or "").strip() not in excluded_folder_names]
    folder_names = sorted({str(row.get("src_folder_raw") or "").strip() for row in rows if row.get("src_folder_raw")})
    event_ids = sorted({str(row.get("event_id") or "").strip() for row in rows if row.get("event_id")})
    if not folder_names or not event_ids:
        return rows
    event_placeholders = ", ".join(["%s"] * len(event_ids))
    folder_placeholders = ", ".join(["%s"] * len(folder_names))
    cur.execute(
        f"""
        SELECT event_id, src_folder_raw
          FROM {qname(master_db())}.medical_folder_aliases
         WHERE event_id IN ({event_placeholders})
           AND src_folder_raw IN ({folder_placeholders})
        """,
        tuple([int(value) for value in event_ids] + folder_names),
    )
    registered = {
        (str(row.get("event_id") or ""), str(row.get("src_folder_raw") or "").strip())
        for row in cur.fetchall()
    }
    return [
        row
        for row in rows
        if (str(row.get("event_id") or ""), str(row.get("src_folder_raw") or "").strip()) not in registered
    ]


def load_file_receipt_rows(cur: Any, *, filters: dict[str, str], limit: int = 200) -> list[dict[str, Any]]:
    where_parts: list[str] = []
    params: list[Any] = []
    event_id = filters.get("event_id", "").strip()
    file_type = filters.get("file_type", "").strip()
    status = filters.get("status", "").strip()
    receipt_check = filters.get("receipt_check", "").strip()
    query = filters.get("q", "").strip()
    if event_id:
        where_parts.append("fr.event_id = %s")
        params.append(event_id)
    if file_type:
        where_parts.append("fr.file_type = %s")
        params.append(file_type)
    if status:
        where_parts.append("fr.status = %s")
        params.append(status)
    if receipt_check == "HAS_NG":
        where_parts.append("COALESCE(ledger_counts.ng_count, 0) > 0")
    elif receipt_check == "OK_ONLY":
        where_parts.append(
            """
            COALESCE(ledger_counts.source_count, 0) > 0
            AND COALESCE(ledger_counts.ng_count, 0) = 0
            AND COALESCE(ledger_counts.pending_count, 0) = 0
            """
        )
    elif receipt_check == "HAS_PENDING":
        where_parts.append("COALESCE(ledger_counts.pending_count, 0) > 0")
    if query:
        like = f"%{query}%"
        where_parts.append(
            """
            (
              fr.file_name LIKE %s
              OR fr.relative_path LIKE %s
              OR fr.facility_name LIKE %s
              OR fr.facility_code LIKE %s
            )
            """
        )
        params.extend([like, like, like, like])
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    cur.execute(
        f"""
        SELECT
          fr.id,
          fr.event_id,
          fr.file_type,
          fr.file_name,
          fr.relative_path,
          fr.file_sha256,
          fr.processable_count,
          COALESCE(ledger_counts.source_count, 0) AS source_count,
          COALESCE(ledger_counts.ok_count, 0) AS ok_count,
          COALESCE(ledger_counts.ng_count, 0) AS ng_count,
          COALESCE(ledger_counts.pending_count, 0) AS pending_count,
          fr.facility_code,
          fr.facility_name,
          fr.exam_facility_id,
          fr.matched_csv_format_version_id,
          fr.status,
          fr.summary_message,
          fr.etl_run_id,
          fr.first_seen_at,
          fr.last_seen_at,
          fr.processed_at,
          fr.updated_at
        FROM {qname(health_db())}.file_receipts AS fr
        LEFT JOIN (
          SELECT
            file_receipt_id,
            COUNT(*) AS source_count,
            SUM(CASE WHEN check_status = 'OK' THEN 1 ELSE 0 END) AS ok_count,
            SUM(CASE WHEN check_status = 'NG' THEN 1 ELSE 0 END) AS ng_count,
            SUM(CASE WHEN check_status NOT IN ('OK', 'NG') OR check_status IS NULL THEN 1 ELSE 0 END) AS pending_count
          FROM {qname(health_db())}.exam_ledgers
          WHERE file_receipt_id IS NOT NULL
          GROUP BY file_receipt_id
        ) AS ledger_counts
          ON ledger_counts.file_receipt_id = fr.id
        {where_sql}
        ORDER BY fr.updated_at DESC, fr.id DESC
        LIMIT %s
        """,
        (*params, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def load_exam_ledger_rows(cur: Any, *, filters: dict[str, str], limit: int = 200) -> list[dict[str, Any]]:
    where_parts: list[str] = []
    params: list[Any] = []
    event_id = filters.get("event_id", "").strip()
    source_type = filters.get("source_type", "").strip()
    subscriber_match_filter = filters.get("subscriber_match_filter", "").strip()
    check_status = filters.get("check_status", "").strip()
    file_receipt_id = filters.get("file_receipt_id", "").strip()
    query = filters.get("q", "").strip()
    name_kana = filters.get("name_kana", "").strip()
    name_kana_match = None
    if name_kana:
        kana_result = normalize_name_kana_full(name_kana)
        name_kana_match = kana_result.get("match")
    insurance_symbol = filters.get("insurance_symbol", "").strip()
    insurance_number = filters.get("insurance_number", "").strip()
    hia_subscriber_id = filters.get("hia_subscriber_id", "").strip()
    facility_query = filters.get("facility_q", "").strip()
    facility_codes = split_filter_values(filters.get("facility_codes", ""))
    if event_id:
        where_parts.append("event_id = %s")
        params.append(event_id)
    if file_receipt_id:
        where_parts.append("file_receipt_id = %s")
        params.append(file_receipt_id)
    if source_type:
        where_parts.append("source_type = %s")
        params.append(source_type)
    if subscriber_match_filter == "MATCHED":
        where_parts.append("subscriber_match_status = 'MATCHED' AND subscriber_match_method = 'identity_hash'")
    elif subscriber_match_filter == "PARTIAL_MATCHED":
        where_parts.append(
            """
            (
              subscriber_match_status IN ('CANDIDATE', 'MULTIPLE_MATCH')
              OR (
                subscriber_match_status = 'MATCHED'
                AND (subscriber_match_method IS NULL OR subscriber_match_method <> 'identity_hash')
              )
            )
            """
        )
    elif subscriber_match_filter == "UNMATCHED":
        where_parts.append("subscriber_match_status = 'NOT_FOUND'")
    elif subscriber_match_filter == "NEEDS_REVIEW":
        where_parts.append("subscriber_match_status = 'IDENTITY_ERROR'")
    elif subscriber_match_filter == "MISSING":
        where_parts.append(
            """
            (
              subscriber_match_status IS NULL
              OR subscriber_match_status = 'NOT_EXECUTED'
            )
            """
        )
    if check_status:
        where_parts.append("check_status = %s")
        params.append(check_status)
    if query:
        like = f"%{query}%"
        where_parts.append(
            """
            (
              hia_subscriber_id LIKE %s
              OR person_id_custom LIKE %s
              OR name_full_raw LIKE %s
              OR xml_file_name LIKE %s
            )
            """
        )
        params.extend([like, like, like, like])
    if name_kana:
        kana_like = f"%{name_kana}%"
        if name_kana_match:
            match_like = f"%{name_kana_match}%"
            where_parts.append("(name_kana_raw LIKE %s OR name_kana_match LIKE %s)")
            params.extend([kana_like, match_like])
        else:
            where_parts.append("name_kana_raw LIKE %s")
            params.append(kana_like)
    if facility_query:
        like = f"%{facility_query}%"
        where_parts.append("(facility_code LIKE %s OR facility_name LIKE %s)")
        params.extend([like, like])
    if facility_codes:
        where_parts.append(f"facility_code IN ({', '.join(['%s'] * len(facility_codes))})")
        params.extend(facility_codes)
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    cur.execute(
        f"""
        SELECT
          exam_ledger_id,
          event_id,
          source_type,
          file_receipt_id,
          src_row_no,
          hia_subscriber_id,
          person_id_custom,
          subscriber_match_status,
          subscriber_match_method,
          facility_code,
          facility_name,
          exam_date,
          postal_code,
          address,
          name_full_raw,
          name_kana_raw,
          health_exam_report_category,
          program_code,
          mapping_version,
          exam_item_count,
          exam_item_error_count,
          check_status,
          check_reason,
          xml_export_status,
          merge_status,
          xml_file_name,
          updated_at
        FROM {qname(health_db())}.exam_ledgers
        {where_sql}
        ORDER BY updated_at DESC, exam_ledger_id DESC
        LIMIT %s
        """,
        (*params, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def load_admin_manual_exam_ledger_rows(
    cur: Any,
    *,
    filters: dict[str, str],
    limit: int = 200,
) -> list[dict[str, Any]]:
    where_parts = ["el.source_type IN ('PAPER', 'MANUAL')"]
    params: list[Any] = []
    event_id = filters.get("event_id", "").strip()
    draft_status = filters.get("draft_status", "").strip().upper()
    apply_state = filters.get("apply_state", "").strip().upper()
    worker_user_id = filters.get("worker_user_id", "").strip()
    query = filters.get("q", "").strip()
    if event_id:
        where_parts.append("el.event_id = %s")
        params.append(event_id)
    if draft_status:
        where_parts.append("UPPER(COALESCE(d.draft_status, '')) = %s")
        params.append(draft_status)
    if apply_state == "APPLIED_WITH_DRAFT":
        where_parts.append("d.manual_exam_entry_draft_id IS NOT NULL")
    elif apply_state == "NO_DRAFT_LINK":
        where_parts.append("d.manual_exam_entry_draft_id IS NULL")
    if worker_user_id:
        where_parts.append(
            """
            (
              d.created_by_app_user_id = %s
              OR d.updated_by_app_user_id = %s
              OR d.applied_by_app_user_id = %s
            )
            """
        )
        params.extend([worker_user_id, worker_user_id, worker_user_id])
    if query:
        like = f"%{query}%"
        where_parts.append(
            """
            (
              el.exam_ledger_id LIKE %s
              OR d.manual_exam_entry_draft_id LIKE %s
              OR el.hia_subscriber_id LIKE %s
              OR el.person_id_custom LIKE %s
              OR el.name_kana_raw LIKE %s
              OR el.name_full_raw LIKE %s
              OR el.facility_code LIKE %s
              OR el.facility_name LIKE %s
              OR el.facility_document_id LIKE %s
              OR el.xml_file_name LIKE %s
            )
            """
        )
        params.extend([like] * 10)
    where_sql = f"WHERE {' AND '.join(where_parts)}"
    cur.execute(
        f"""
        SELECT
          el.exam_ledger_id,
          el.event_id,
          el.source_type,
          el.hia_subscriber_id,
          el.person_id_custom,
          el.subscriber_id,
          el.subscriber_match_status,
          el.subscriber_match_method,
          el.facility_code,
          el.facility_name,
          el.facility_document_id,
          el.exam_date,
          el.name_full_raw,
          el.name_kana_raw,
          el.insurance_symbol_raw,
          el.insurance_number_raw,
          el.insurance_branch_number_raw,
          el.birthdate,
          el.gender_code,
          el.exam_item_count,
          el.exam_item_error_count,
          el.row_status,
          el.check_status,
          el.check_reason,
          el.xml_export_status,
          el.merge_status,
          el.created_at,
          el.updated_at,
          d.manual_exam_entry_draft_id,
          d.draft_status,
          d.entry_purpose,
          d.applied_at,
          d.created_by_app_user_id,
          d.updated_by_app_user_id,
          d.applied_by_app_user_id,
          COALESCE(dv.value_count, 0) AS draft_value_count,
          COALESCE(eiv.item_value_count, 0) AS item_value_count,
          COALESCE(src.case_source_count, 0) AS case_source_count,
          COALESCE(adopted.adopted_value_count, 0) AS adopted_value_count,
          COALESCE(listed.list_case_count, 0) AS list_case_count,
          COALESCE(cu.display_name, cu.employee_no, CONCAT('user ', d.created_by_app_user_id)) AS draft_created_by_name,
          COALESCE(uu.display_name, uu.employee_no, CONCAT('user ', d.updated_by_app_user_id)) AS draft_updated_by_name,
          COALESCE(au.display_name, au.employee_no, CONCAT('user ', d.applied_by_app_user_id)) AS draft_applied_by_name
        FROM {qname(health_db())}.exam_ledgers AS el
        LEFT JOIN {qname(health_db())}.manual_exam_entry_drafts AS d
          ON d.applied_exam_ledger_id = el.exam_ledger_id
          OR d.manual_exam_entry_draft_id = CAST(JSON_UNQUOTE(JSON_EXTRACT(el.raw_row_json, '$.manual_exam_entry_draft_id')) AS UNSIGNED)
        LEFT JOIN (
          SELECT manual_exam_entry_draft_id, COUNT(*) AS value_count
          FROM {qname(health_db())}.manual_exam_entry_draft_values
          GROUP BY manual_exam_entry_draft_id
        ) AS dv
          ON dv.manual_exam_entry_draft_id = d.manual_exam_entry_draft_id
        LEFT JOIN (
          SELECT ledger_id, COUNT(*) AS item_value_count
          FROM {qname(health_db())}.exam_item_values
          WHERE ledger_type = 'EXAM'
          GROUP BY ledger_id
        ) AS eiv
          ON eiv.ledger_id = el.exam_ledger_id
        LEFT JOIN (
          SELECT source_exam_ledger_id, COUNT(*) AS case_source_count
          FROM {qname(health_db())}.exam_export_case_sources
          GROUP BY source_exam_ledger_id
        ) AS src
          ON src.source_exam_ledger_id = el.exam_ledger_id
        LEFT JOIN (
          SELECT source_exam_ledger_id, COUNT(*) AS adopted_value_count
          FROM {qname(health_db())}.exam_export_case_values
          GROUP BY source_exam_ledger_id
        ) AS adopted
          ON adopted.source_exam_ledger_id = el.exam_ledger_id
        LEFT JOIN (
          SELECT eecs.source_exam_ledger_id, COUNT(*) AS list_case_count
          FROM {qname(health_db())}.exam_export_case_sources AS eecs
          INNER JOIN {qname(health_db())}.ops_xml_export_list_cases AS oelc
            ON oelc.exam_export_case_id = eecs.exam_export_case_id
           AND oelc.removed_at IS NULL
          GROUP BY eecs.source_exam_ledger_id
        ) AS listed
          ON listed.source_exam_ledger_id = el.exam_ledger_id
        LEFT JOIN {qname(app_db())}.app_users AS cu
          ON cu.app_user_id = d.created_by_app_user_id
        LEFT JOIN {qname(app_db())}.app_users AS uu
          ON uu.app_user_id = d.updated_by_app_user_id
        LEFT JOIN {qname(app_db())}.app_users AS au
          ON au.app_user_id = d.applied_by_app_user_id
        {where_sql}
        ORDER BY el.updated_at DESC, el.exam_ledger_id DESC
        LIMIT %s
        """,
        (*params, limit),
    )
    rows = [dict(row) for row in cur.fetchall()]
    ledger_ids = [int(row["exam_ledger_id"]) for row in rows]
    refs_by_ledger: dict[int, list[dict[str, Any]]] = {ledger_id: [] for ledger_id in ledger_ids}
    if ledger_ids:
        placeholders = ", ".join(["%s"] * len(ledger_ids))
        cur.execute(
            f"""
            SELECT DISTINCT
              eecs.source_exam_ledger_id,
              oel.xml_export_list_id,
              oel.list_name,
              oel.list_status,
              oelc.xml_export_list_case_id,
              oelc.list_case_status
            FROM {qname(health_db())}.exam_export_case_sources AS eecs
            INNER JOIN {qname(health_db())}.ops_xml_export_list_cases AS oelc
              ON oelc.exam_export_case_id = eecs.exam_export_case_id
             AND oelc.removed_at IS NULL
            INNER JOIN {qname(health_db())}.ops_xml_export_lists AS oel
              ON oel.xml_export_list_id = oelc.xml_export_list_id
            WHERE eecs.source_exam_ledger_id IN ({placeholders})
            ORDER BY oel.xml_export_list_id, oelc.xml_export_list_case_id
            """,
            tuple(ledger_ids),
        )
        for ref in cur.fetchall():
            ledger_id = int(ref["source_exam_ledger_id"])
            refs_by_ledger.setdefault(ledger_id, []).append(dict(ref))
    for row in rows:
        row["export_list_refs"] = refs_by_ledger.get(int(row["exam_ledger_id"]), [])
    return rows


SUBSCRIBER_MATCH_ISSUE_FILTERS = {
    "PARTIAL_MATCHED": """
        (
          el.subscriber_match_status IN ('CANDIDATE', 'MULTIPLE_MATCH')
          OR (
            el.subscriber_match_status = 'MATCHED'
            AND (
              el.subscriber_match_method IS NULL
              OR el.subscriber_match_method NOT IN ('identity_hash', 'manual')
            )
          )
        )
    """,
    "UNMATCHED": "el.subscriber_match_status = 'NOT_FOUND'",
    "NEEDS_REVIEW": "el.subscriber_match_status = 'IDENTITY_ERROR'",
    "MISSING": "(el.subscriber_match_status IS NULL OR el.subscriber_match_status = 'NOT_EXECUTED')",
    "WAITING_RESUBMISSION": "el.subscriber_match_status = 'WAITING_RESUBMISSION'",
    "RESUBMITTED": "el.subscriber_match_status = 'RESUBMITTED'",
    "ALREADY_UPLOADED": "el.subscriber_match_status = 'ALREADY_UPLOADED'",
    "EXCLUDED": "el.subscriber_match_status = 'EXCLUDED'",
}


def subscriber_match_status_label(status: str | None, method: str | None = None) -> str:
    if status == "MATCHED" and method == "identity_hash":
        return "MATCHED"
    if status == "MATCHED" and method == "manual":
        return "手動確定"
    if status == "MATCHED":
        return "一部MATCHED"
    labels = {
        "CANDIDATE": "一部MATCHED",
        "MULTIPLE_MATCH": "一部MATCHED",
        "NOT_FOUND": "UNMATCHED",
        "IDENTITY_ERROR": "要確認",
        "NOT_EXECUTED": "MISSING",
        "WAITING_RESUBMISSION": "再提出待ち",
        "RESUBMITTED": "再提出済み",
        "ALREADY_UPLOADED": "アップロード済み",
        "EXCLUDED": "除外",
        None: "MISSING",
        "": "MISSING",
    }
    return labels.get(status, status or "MISSING")


templates.env.globals["subscriber_match_status_label"] = subscriber_match_status_label


def gender_code_label(gender_code: Any) -> str:
    code = str(gender_code or "").strip()
    labels = {
        "1": "男",
        "2": "女",
        "M": "男",
        "F": "女",
        "male": "男",
        "female": "女",
    }
    return labels.get(code, code or "-")


templates.env.globals["gender_code_label"] = gender_code_label


def load_subscriber_match_issue_rows(
    cur: Any,
    *,
    filters: dict[str, str],
    limit: int = 200,
) -> list[dict[str, Any]]:
    where_parts, params = subscriber_match_issue_where_parts(filters)
    where_sql = f"WHERE {' AND '.join(where_parts)}"
    cur.execute(
        f"""
        SELECT
          el.exam_ledger_id,
          el.event_id,
          el.source_type,
          el.file_receipt_id,
          el.src_row_no,
          el.subscriber_id,
          el.hia_subscriber_id,
          el.person_id_custom,
          el.identity_hash,
          el.subscriber_match_status,
          el.subscriber_match_method,
          el.subscriber_match_reason,
          el.facility_code,
          el.facility_name,
          el.exam_date,
          el.name_full_raw,
          el.name_kana_raw,
          el.name_kana_match,
          el.insurer_number,
          el.insurance_symbol_raw,
          el.insurance_symbol_match,
          el.insurance_number_raw,
          el.insurance_number_match,
          el.insurance_branch_number_raw,
          el.insurance_branch_number_match,
          el.birthdate,
          el.gender_code,
          el.xml_file_name,
          el.mapping_version,
          el.updated_at,
          latest.changed_at AS last_manual_matched_at,
          latest.note AS last_manual_match_note
        FROM {qname(health_db())}.exam_ledgers AS el
        LEFT JOIN (
          SELECT a1.*
          FROM {qname(health_db())}.exam_ledger_subscriber_match_audit_logs AS a1
          JOIN (
            SELECT exam_ledger_id, MAX(exam_ledger_subscriber_match_audit_log_id) AS max_id
            FROM {qname(health_db())}.exam_ledger_subscriber_match_audit_logs
            GROUP BY exam_ledger_id
          ) AS latest_ids
            ON latest_ids.max_id = a1.exam_ledger_subscriber_match_audit_log_id
        ) AS latest
          ON latest.exam_ledger_id = el.exam_ledger_id
        {where_sql}
        ORDER BY
          FIELD(el.subscriber_match_status, 'IDENTITY_ERROR', 'MULTIPLE_MATCH', 'CANDIDATE', 'NOT_FOUND', 'NOT_EXECUTED', 'MATCHED'),
          el.updated_at DESC,
          el.exam_ledger_id DESC
        LIMIT %s
        """,
        (*params, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def subscriber_match_issue_where_parts(
    filters: dict[str, str],
    *,
    include_query: bool = True,
    include_facility: bool = True,
    include_exam_month: bool = True,
) -> tuple[list[str], list[Any]]:
    where_parts: list[str] = []
    params: list[Any] = []
    event_id = filters.get("event_id", "").strip()
    status_filter = filters.get("status_filter", "").strip()
    query = filters.get("q", "").strip()
    facility_query = filters.get("facility_q", "").strip()
    facility_codes = split_filter_values(filters.get("facility_codes", ""))
    exam_months = split_filter_values(filters.get("exam_month", ""))
    if event_id:
        where_parts.append("el.event_id = %s")
        params.append(event_id)
    if status_filter in SUBSCRIBER_MATCH_ISSUE_FILTERS:
        where_parts.append(SUBSCRIBER_MATCH_ISSUE_FILTERS[status_filter])
    else:
        where_parts.append(
            """
            (
              el.subscriber_match_status IS NULL
              OR el.subscriber_match_status <> 'MATCHED'
              OR el.subscriber_match_method IS NULL
              OR el.subscriber_match_method NOT IN ('identity_hash', 'manual')
            )
            """
        )
    if include_query and query:
        like = f"%{query}%"
        where_parts.append(
            """
            (
              el.hia_subscriber_id LIKE %s
              OR el.person_id_custom LIKE %s
              OR el.name_full_raw LIKE %s
              OR el.name_kana_raw LIKE %s
              OR el.insurance_symbol_raw LIKE %s
              OR el.insurance_number_raw LIKE %s
              OR el.xml_file_name LIKE %s
            )
            """
        )
        params.extend([like] * 7)
    if include_facility and facility_query:
        like = f"%{facility_query}%"
        where_parts.append("(el.facility_code LIKE %s OR el.facility_name LIKE %s)")
        params.extend([like, like])
    if include_facility and facility_codes:
        where_parts.append(f"el.facility_code IN ({', '.join(['%s'] * len(facility_codes))})")
        params.extend(facility_codes)
    if include_exam_month and exam_months:
        known_months = [
            month for month in exam_months
            if month.upper() != "UNKNOWN" and month != "不明"
        ]
        month_parts: list[str] = []
        if known_months:
            month_parts.append(f"DATE_FORMAT(el.exam_date, '%Y-%m') IN ({', '.join(['%s'] * len(known_months))})")
            params.extend(known_months)
        if any(month.upper() == "UNKNOWN" or month == "不明" for month in exam_months):
            month_parts.append("el.exam_date IS NULL")
        if month_parts:
            where_parts.append("(" + " OR ".join(month_parts) + ")")
    return where_parts, params


def load_subscriber_match_candidate_rows(
    cur: Any,
    *,
    ledger: Mapping[str, Any] | None,
    event_id: Any | None = None,
    query: str = "",
    candidate_filters: Mapping[str, str] | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    candidate_filters = candidate_filters or {}
    if ledger is None and not query.strip() and not any(str(v or "").strip() for v in candidate_filters.values()):
        return []
    subscriber_columns = manual_exam_entry_existing_columns(cur, dev_db(), "subscribers")
    subscriber_name_full_expr = "s.name_kanji_full" if "name_kanji_full" in subscriber_columns else "s.name_full_match"
    subscriber_select = lambda column, alias=None: (
        f"s.{column} AS {alias or column}" if column in subscriber_columns else f"NULL AS {alias or column}"
    )
    where_parts: list[str] = []
    filter_parts: list[str] = []
    params: list[Any] = []
    filter_params: list[Any] = []
    score_parts: list[str] = []
    score_params: list[Any] = []
    if ledger:
        if ledger.get("hia_subscriber_id"):
            where_parts.append("s.hia_subscriber_id = %s")
            params.append(ledger.get("hia_subscriber_id"))
            score_parts.append("CASE WHEN s.hia_subscriber_id = %s THEN 100 ELSE 0 END")
            score_params.append(ledger.get("hia_subscriber_id"))
        if ledger.get("identity_hash"):
            where_parts.append("s.identity_hash = %s")
            params.append(ledger.get("identity_hash"))
            score_parts.append("CASE WHEN s.identity_hash = %s THEN 90 ELSE 0 END")
            score_params.append(ledger.get("identity_hash"))
        if ledger.get("person_id_custom"):
            where_parts.append("s.person_id_custom = %s")
            params.append(ledger.get("person_id_custom"))
            score_parts.append("CASE WHEN s.person_id_custom = %s THEN 70 ELSE 0 END")
            score_params.append(ledger.get("person_id_custom"))
        if ledger.get("insurer_number") and ledger.get("insurance_number_match"):
            where_parts.append("(s.insurer_number = %s AND s.insurance_number_match = %s)")
            params.extend([ledger.get("insurer_number"), ledger.get("insurance_number_match")])
            score_parts.append("CASE WHEN s.insurer_number = %s AND s.insurance_number_match = %s THEN 50 ELSE 0 END")
            score_params.extend([ledger.get("insurer_number"), ledger.get("insurance_number_match")])
        if ledger.get("birthdate") and ledger.get("name_kana_match"):
            where_parts.append("(s.birth = %s AND s.name_kana_full_match = %s)")
            params.extend([ledger.get("birthdate"), ledger.get("name_kana_match")])
            score_parts.append("CASE WHEN s.birth = %s AND s.name_kana_full_match = %s THEN 40 ELSE 0 END")
            score_params.extend([ledger.get("birthdate"), ledger.get("name_kana_match")])
    query = query.strip()
    if query:
        like = f"%{query}%"
        employee_code_expr = "s.employee_code" if "employee_code" in subscriber_columns else "''"
        where_parts.append(
            f"""
            (
              s.hia_subscriber_id LIKE %s
              OR s.person_id_custom LIKE %s
              OR {subscriber_name_full_expr} LIKE %s
              OR s.name_kana_full LIKE %s
              OR s.name_kana_full_match LIKE %s
              OR s.insurance_number LIKE %s
              OR s.insurance_number_match LIKE %s
              OR {employee_code_expr} LIKE %s
            )
            """
        )
        params.extend([like] * 8)
    candidate_kana = str(candidate_filters.get("name_kana") or "").strip()
    if candidate_kana:
        like = f"%{candidate_kana}%"
        filter_parts.append("(s.name_kana_full LIKE %s OR s.name_kana_full_match LIKE %s)")
        filter_params.extend([like, like])
    candidate_symbol = str(candidate_filters.get("insurance_symbol") or "").strip()
    if candidate_symbol:
        like = f"%{candidate_symbol}%"
        filter_parts.append("(s.insurance_symbol LIKE %s OR s.insurance_symbol_export LIKE %s OR s.insurance_symbol_match LIKE %s)")
        filter_params.extend([like, like, like])
    candidate_number = str(candidate_filters.get("insurance_number") or "").strip()
    if candidate_number:
        like = f"%{candidate_number}%"
        filter_parts.append("(s.insurance_number LIKE %s OR s.insurance_number_match LIKE %s)")
        filter_params.extend([like, like])
    candidate_employee_code = str(candidate_filters.get("employee_code") or "").strip()
    if candidate_employee_code:
        like = f"%{candidate_employee_code}%"
        filter_parts.append("s.employee_code LIKE %s")
        filter_params.append(like)
    candidate_birth = str(candidate_filters.get("birth") or "").strip()
    if candidate_birth:
        filter_parts.append("CAST(s.birth AS CHAR) LIKE %s")
        filter_params.append(f"%{candidate_birth}%")
    candidate_hia_subscriber_id = str(candidate_filters.get("hia_subscriber_id") or "").strip()
    if candidate_hia_subscriber_id:
        filter_parts.append("s.hia_subscriber_id LIKE %s")
        filter_params.append(f"%{candidate_hia_subscriber_id}%")
    candidate_subscriber_ids = tuple(
        int(value)
        for value in (candidate_filters.get("subscriber_ids") or ())
        if str(value).isdigit() and int(value) > 0
    )
    if candidate_subscriber_ids:
        filter_parts.append(f"s.id IN ({', '.join(['%s'] * len(candidate_subscriber_ids))})")
        filter_params.extend(candidate_subscriber_ids)
    if not where_parts and not filter_parts:
        return []
    where_sql_parts: list[str] = []
    if where_parts:
        where_sql_parts.append("(" + " OR ".join(f"({part})" for part in where_parts) + ")")
    if filter_parts:
        where_sql_parts.extend(f"({part})" for part in filter_parts)
    score_sql = " + ".join(score_parts) if score_parts else "0"
    case_event_id = event_id or (ledger.get("event_id") if ledger else None)
    has_subscriber_addresses = manual_exam_entry_table_exists(cur, dev_db(), "subscriber_addresses")
    address_select_sql = (
        "a.postal_code AS subscriber_postal_code, "
        "a.address_line AS subscriber_address_line, "
        "a.building AS subscriber_building"
        if has_subscriber_addresses
        else "NULL AS subscriber_postal_code, NULL AS subscriber_address_line, NULL AS subscriber_building"
    )
    address_join_sql = (
        f"LEFT JOIN {qname(dev_db())}.subscriber_addresses AS a "
        "ON a.subscriber_id = s.id AND a.is_current = 1"
        if has_subscriber_addresses
        else ""
    )
    address_order_sql = "a.address_id DESC," if has_subscriber_addresses else ""
    cur.execute(
        f"""
        SELECT
          s.id AS subscriber_id,
          s.hia_subscriber_id,
          s.person_id_custom,
          s.identity_hash,
          s.insurer_number,
          s.insurance_symbol,
          s.insurance_symbol_export,
          s.insurance_symbol_match,
          s.insurance_number,
          s.insurance_number_match,
          s.insurance_branchnumber,
          s.name_kana_full,
          s.name_kana_full_match,
          {subscriber_name_full_expr} AS name_kanji_full,
          s.birth,
          s.gender_code,
          {subscriber_select("relationship_name")},
          {subscriber_select("qualification_lost_date")},
          {subscriber_select("employee_code")},
          {address_select_sql},
          hds.status AS hia_dashboard_status,
          hds.medical_institution AS hia_dashboard_medical_institution,
          hds.reservation_date AS hia_dashboard_reservation_date,
          hds.exam_date AS hia_dashboard_exam_date,
          hds.course_name AS hia_dashboard_course_name,
          case_summary.case_count AS candidate_case_count,
          latest_case.exam_export_case_id AS candidate_latest_case_id,
          latest_case.facility_name AS candidate_latest_case_facility_name,
          latest_case.exam_date AS candidate_latest_case_exam_date,
          latest_case.source_mode AS candidate_latest_case_source_mode,
          latest_case.export_readiness_status AS candidate_latest_case_export_readiness_status,
          latest_case.xml_export_status AS candidate_latest_case_xml_export_status,
          latest_case.legal_check_result AS candidate_latest_case_legal_check_result,
          latest_case.specific_check_result AS candidate_latest_case_specific_check_result,
          ({score_sql}) AS match_score
        FROM {qname(dev_db())}.subscribers AS s
        {address_join_sql}
        LEFT JOIN {qname(work_other_db())}.hia_dashboard_status AS hds
          ON hds.hia_subscriber_id = s.hia_subscriber_id
         AND hds.is_active = 1
        LEFT JOIN (
          SELECT
            subscriber_id,
            COUNT(*) AS case_count
          FROM {qname(health_db())}.exam_export_cases
          WHERE event_id = %s
          GROUP BY subscriber_id
        ) AS case_summary
          ON case_summary.subscriber_id = s.id
        LEFT JOIN (
          SELECT *
          FROM (
            SELECT
              eec.exam_export_case_id,
              eec.subscriber_id,
              eec.facility_name,
              eec.exam_date,
              eec.source_mode,
              eec.export_readiness_status,
              eec.xml_export_status,
              COALESCE(ecr.legal_check_result, 'PENDING') AS legal_check_result,
              COALESCE(ecr.specific_check_result, 'PENDING') AS specific_check_result,
              ROW_NUMBER() OVER (
                PARTITION BY eec.subscriber_id
                ORDER BY eec.exam_date DESC, eec.exam_export_case_id DESC
              ) AS row_num
            FROM {qname(health_db())}.exam_export_cases AS eec
            LEFT JOIN (
              SELECT r1.*
              FROM {qname(health_db())}.exam_check_results AS r1
              INNER JOIN (
                SELECT exam_export_case_id, MAX(id) AS max_id
                FROM {qname(health_db())}.exam_check_results
                WHERE ledger_type = 'EXPORT_CASE'
                  AND exam_export_case_id IS NOT NULL
                GROUP BY exam_export_case_id
              ) AS latest_result
                ON latest_result.max_id = r1.id
            ) AS ecr
              ON ecr.exam_export_case_id = eec.exam_export_case_id
            WHERE eec.event_id = %s
          ) AS ranked_cases
          WHERE row_num = 1
        ) AS latest_case
          ON latest_case.subscriber_id = s.id
        WHERE {" AND ".join(where_sql_parts)}
        ORDER BY match_score DESC, s.id, {address_order_sql} hds.hia_dashboard_person_id DESC
        LIMIT %s
        """,
        (*score_params, case_event_id, case_event_id, *params, *filter_params, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def load_subscriber_row(cur: Any, *, subscriber_id: int) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT
          s.id AS subscriber_id,
          s.hia_subscriber_id,
          s.person_id_custom,
          s.identity_hash,
          s.insurer_number,
          s.insurance_symbol,
          s.insurance_symbol_export,
          s.insurance_symbol_match,
          s.insurance_number,
          s.insurance_number_match,
          s.insurance_branchnumber,
          s.name_kana_full,
          s.name_kana_full_match,
          a.postal_code AS subscriber_postal_code,
          a.address_line AS subscriber_address_line,
          a.building AS subscriber_building
        FROM {qname(dev_db())}.subscribers AS s
        LEFT JOIN {qname(dev_db())}.subscriber_addresses AS a
          ON a.subscriber_id = s.id
         AND a.is_current = 1
        WHERE s.id = %s
        ORDER BY a.address_id DESC
        LIMIT 1
        """,
        (subscriber_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def _subscriber_export_update_values(subscriber: Mapping[str, Any]) -> tuple[dict[str, Any], list[str]]:
    updates: dict[str, Any] = {}
    applied_fields: list[str] = []

    kana = normalize_name_kana_full(str(subscriber.get("name_kana_full") or "")) if subscriber.get("name_kana_full") else {}
    if kana.get("ok") and kana.get("field_norm"):
        updates["name_kana_export_value"] = kana.get("field_norm")
        updates["name_kana_export_source"] = "SUBSCRIBER"
        updates["name_kana_export_reason"] = "subscriber search apply"
        applied_fields.append("name_kana")

    symbol_raw = subscriber.get("insurance_symbol_export") or subscriber.get("insurance_symbol")
    symbol = normalize_insurance_symbol(str(symbol_raw or "")) if symbol_raw else {}
    if symbol.get("ok") and symbol.get("export"):
        updates["insurance_symbol_export_value"] = symbol.get("export")
        updates["insurance_symbol_export_source"] = "SUBSCRIBER"
        updates["insurance_symbol_export_reason"] = "subscriber search apply"
        applied_fields.append("insurance_symbol")

    number = normalize_insurance_number(str(subscriber.get("insurance_number") or "")) if subscriber.get("insurance_number") else {}
    if number.get("ok") and number.get("field_norm"):
        updates["insurance_number_export_value"] = number.get("field_norm")
        updates["insurance_number_export_source"] = "SUBSCRIBER"
        updates["insurance_number_export_reason"] = "subscriber search apply"
        applied_fields.append("insurance_number")

    branch_number = str(subscriber.get("insurance_branchnumber") or "").strip()
    if branch_number:
        updates["insurance_branch_number_export_value"] = branch_number
        updates["insurance_branch_number_export_source"] = "SUBSCRIBER"
        updates["insurance_branch_number_export_reason"] = "subscriber search apply"
        applied_fields.append("insurance_branch_number")

    postal_code = normalize_postal_code_export(subscriber.get("subscriber_postal_code"))
    address_parts = [
        str(subscriber.get("subscriber_address_line") or "").strip(),
        str(subscriber.get("subscriber_building") or "").strip(),
    ]
    address = normalize_address_export("".join(part for part in address_parts if part))
    if postal_code:
        updates["postal_code_completed_value"] = postal_code
        applied_fields.append("postal_code")
    if address:
        updates["address_completed_value"] = address
        updates["address_source"] = "SUBSCRIBER"
        updates["address_completion_status"] = "SUBSCRIBER"
        updates["address_completion_reason"] = "subscriber search apply"
        applied_fields.append("address")

    return updates, applied_fields


def confirm_exam_ledger_subscriber_match(
    cur: Any,
    *,
    exam_ledger_id: int,
    subscriber_id: int,
    note: str,
    app_user_id: int,
    apply_subscriber_values: bool = False,
) -> dict[str, Any] | None:
    ledger = load_exam_ledger_detail(cur, exam_ledger_id=exam_ledger_id)
    subscriber = load_subscriber_row(cur, subscriber_id=subscriber_id)
    if ledger is None or subscriber is None:
        return None
    new_reason = note or "手動で加入者を確定"
    export_updates, applied_fields = (
        _subscriber_export_update_values(subscriber) if apply_subscriber_values else ({}, [])
    )
    cur.execute(
        f"""
        INSERT INTO {qname(health_db())}.exam_ledger_subscriber_match_audit_logs (
          event_id,
          exam_ledger_id,
          old_subscriber_id,
          new_subscriber_id,
          old_hia_subscriber_id,
          new_hia_subscriber_id,
          old_person_id_custom,
          new_person_id_custom,
          old_identity_hash,
          new_identity_hash,
          old_subscriber_match_status,
          new_subscriber_match_status,
          old_subscriber_match_method,
          new_subscriber_match_method,
          old_subscriber_match_reason,
          new_subscriber_match_reason,
          applied_subscriber_export_values,
          applied_fields_json,
          note,
          changed_by_app_user_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'MATCHED', %s, 'manual', %s, %s, %s, %s, %s, %s)
        """,
        (
            ledger.get("event_id"),
            exam_ledger_id,
            ledger.get("subscriber_id"),
            subscriber_id,
            ledger.get("hia_subscriber_id"),
            subscriber.get("hia_subscriber_id"),
            ledger.get("person_id_custom"),
            subscriber.get("person_id_custom"),
            ledger.get("identity_hash"),
            subscriber.get("identity_hash"),
            ledger.get("subscriber_match_status"),
            ledger.get("subscriber_match_method"),
            ledger.get("subscriber_match_reason"),
            new_reason,
            1 if apply_subscriber_values else 0,
            json.dumps(applied_fields, ensure_ascii=False),
            note,
            app_user_id,
        ),
    )
    update_columns = [
        "subscriber_id = %s",
        "hia_subscriber_id = COALESCE(%s, hia_subscriber_id)",
        "identity_hash = COALESCE(%s, identity_hash)",
        "person_id_custom = COALESCE(%s, person_id_custom)",
        "name_kana_match = COALESCE(%s, name_kana_match)",
        "insurance_symbol_match = COALESCE(%s, insurance_symbol_match)",
        "insurance_number_match = COALESCE(%s, insurance_number_match)",
        "insurance_branch_number_match = COALESCE(%s, insurance_branch_number_match)",
        "subscriber_match_status = 'MATCHED'",
        "subscriber_match_method = 'manual'",
        "subscriber_match_reason = %s",
    ]
    update_params: list[Any] = [
        subscriber_id,
        subscriber.get("hia_subscriber_id"),
        subscriber.get("identity_hash"),
        subscriber.get("person_id_custom"),
        subscriber.get("name_kana_full_match"),
        subscriber.get("insurance_symbol_match"),
        subscriber.get("insurance_number_match"),
        subscriber.get("insurance_branchnumber"),
        new_reason,
    ]
    for column, value in export_updates.items():
        update_columns.append(f"{column} = %s")
        update_params.append(value)
    update_params.append(exam_ledger_id)
    cur.execute(
        f"""
        UPDATE {qname(health_db())}.exam_ledgers
        SET {", ".join(update_columns)}
        WHERE exam_ledger_id = %s
        """,
        tuple(update_params),
    )
    return {
        "exam_ledger_id": exam_ledger_id,
        "subscriber_id": subscriber_id,
        "old_subscriber_id": ledger.get("subscriber_id"),
        "new_hia_subscriber_id": subscriber.get("hia_subscriber_id"),
        "applied_subscriber_export_values": bool(apply_subscriber_values),
        "applied_fields": applied_fields,
    }


SUBSCRIBER_MATCH_WORKFLOW_STATUSES = {
    "WAITING_RESUBMISSION": "再提出待ち",
    "RESUBMITTED": "再提出済み",
    "ALREADY_UPLOADED": "アップロード済み",
    "EXCLUDED": "除外",
    "NOT_FOUND": "UNMATCHEDに戻す",
}


def update_exam_ledger_subscriber_match_workflow_status(
    cur: Any,
    *,
    exam_ledger_id: int,
    new_status: str,
    note: str,
    app_user_id: int,
) -> dict[str, Any] | None:
    if new_status not in SUBSCRIBER_MATCH_WORKFLOW_STATUSES:
        raise ValueError(f"unsupported subscriber match workflow status: {new_status}")
    ledger = load_exam_ledger_detail(cur, exam_ledger_id=exam_ledger_id)
    if ledger is None:
        return None
    new_reason = note or SUBSCRIBER_MATCH_WORKFLOW_STATUSES[new_status]
    cur.execute(
        f"""
        INSERT INTO {qname(health_db())}.exam_ledger_subscriber_match_audit_logs (
          event_id,
          exam_ledger_id,
          old_subscriber_id,
          new_subscriber_id,
          old_hia_subscriber_id,
          new_hia_subscriber_id,
          old_person_id_custom,
          new_person_id_custom,
          old_identity_hash,
          new_identity_hash,
          old_subscriber_match_status,
          new_subscriber_match_status,
          old_subscriber_match_method,
          new_subscriber_match_method,
          old_subscriber_match_reason,
          new_subscriber_match_reason,
          applied_subscriber_export_values,
          applied_fields_json,
          note,
          changed_by_app_user_id
        )
        VALUES (%s, %s, %s, NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'manual_review', %s, %s, 0, JSON_ARRAY(), %s, %s)
        """,
        (
            ledger.get("event_id"),
            exam_ledger_id,
            ledger.get("subscriber_id"),
            ledger.get("hia_subscriber_id"),
            ledger.get("hia_subscriber_id"),
            ledger.get("person_id_custom"),
            ledger.get("person_id_custom"),
            ledger.get("identity_hash"),
            ledger.get("identity_hash"),
            ledger.get("subscriber_match_status"),
            new_status,
            ledger.get("subscriber_match_method"),
            ledger.get("subscriber_match_reason"),
            new_reason,
            note,
            app_user_id,
        ),
    )
    cur.execute(
        f"""
        UPDATE {qname(health_db())}.exam_ledgers
        SET
          subscriber_id = NULL,
          subscriber_match_status = %s,
          subscriber_match_method = 'manual_review',
          subscriber_match_reason = %s
        WHERE exam_ledger_id = %s
        """,
        (new_status, new_reason, exam_ledger_id),
    )
    return {"exam_ledger_id": exam_ledger_id, "status": new_status, "reason": new_reason}


def load_exam_ledger_detail(cur: Any, *, exam_ledger_id: int) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT *
        FROM {qname(health_db())}.exam_ledgers
        WHERE exam_ledger_id = %s
        LIMIT 1
        """,
        (exam_ledger_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def load_exam_item_value_rows(cur: Any, *, exam_ledger_id: int) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          id,
          namecode,
          namecode_display_name,
          section_code,
          section_name,
          occurrence_no,
          raw_value,
          raw_value_type,
          raw_unit,
          normalized_value,
          normalized_unit,
          nullflavor,
          code_system,
          code_value,
          code_display,
          interpretation_code,
          interpretation_name,
          source_reference_lower,
          source_reference_upper,
          negation_ind,
          normalize_status,
          normalize_reason,
          validation_status,
          validation_reason,
          extracted_run_id,
          updated_at
        FROM {qname(health_db())}.exam_item_values
        WHERE ledger_type = 'EXAM'
          AND ledger_id = %s
        ORDER BY
          CASE WHEN validation_status = 'INVALID' THEN 0 ELSE 1 END,
          COALESCE(jun_no, 999999),
          namecode,
          occurrence_no,
          id
        """,
        (exam_ledger_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def search_exam_ledger_candidates(
    cur: Any,
    *,
    event_id: str,
    name_kana: str,
    hia_subscriber_id: str,
    insurance_symbol: str,
    insurance_number: str,
    limit: int = 50,
) -> list[dict[str, Any]]:
    clauses = ["event_id = %s"]
    params: list[Any] = [event_id]
    if name_kana:
        clauses.append("name_kana_raw LIKE %s")
        params.append(f"%{name_kana}%")
    if hia_subscriber_id:
        clauses.append("hia_subscriber_id LIKE %s")
        params.append(f"%{hia_subscriber_id}%")
    if insurance_number:
        clauses.append(
            "(insurance_number_raw LIKE %s OR insurance_number_match LIKE %s OR insurance_number_export_value LIKE %s)"
        )
        params.extend([f"%{insurance_number}%", f"%{insurance_number}%", f"%{insurance_number}%"])
        if insurance_symbol:
            clauses.append(
                "(insurance_symbol_raw LIKE %s OR insurance_symbol_match LIKE %s OR insurance_symbol_export_value LIKE %s)"
            )
            params.extend([f"%{insurance_symbol}%", f"%{insurance_symbol}%", f"%{insurance_symbol}%"])
    if not name_kana and not hia_subscriber_id and not insurance_number:
        return []
    where_sql = " AND ".join(clauses)
    cur.execute(
        f"""
        SELECT
          exam_ledger_id,
          event_id,
          source_type,
          file_receipt_id,
          src_row_no,
          hia_subscriber_id,
          insurance_symbol_raw,
          insurance_symbol_export_value,
          insurance_number_raw,
          insurance_number_export_value,
          name_full_raw,
          name_kana_raw,
          facility_name,
          facility_code,
          exam_date,
          check_status,
          xml_export_status,
          exam_item_error_count,
          xml_file_name,
          mapping_version,
          updated_at
        FROM {qname(health_db())}.exam_ledgers
        WHERE {where_sql}
        ORDER BY updated_at DESC, exam_ledger_id DESC
        LIMIT %s
        """,
        (*params, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def build_exam_export_case_where(filters: dict[str, str]) -> tuple[str, list[Any]]:
    where_parts: list[str] = []
    params: list[Any] = []
    event_id = filters.get("event_id", "").strip()
    legal_check_result = filters.get("legal_check_result", "").strip()
    specific_check_result = filters.get("specific_check_result", "").strip()
    export_readiness_status = filters.get("export_readiness_status", "").strip()
    source_mode = filters.get("source_mode", "").strip()
    exam_months = split_filter_values(filters.get("exam_month", ""))
    query = filters.get("q", "").strip()
    case_id = filters.get("case_id", "").strip()
    name_full = filters.get("name_full", "").strip()
    name_kana = filters.get("name_kana", "").strip()
    insurance_symbol = filters.get("insurance_symbol", "").strip()
    insurance_number = filters.get("insurance_number", "").strip()
    hia_subscriber_id = filters.get("hia_subscriber_id", "").strip()
    subscriber_id = filters.get("subscriber_id", "").strip()
    qualification_lost_status = filters.get("qualification_lost_status", "").strip()
    qualification_lost_date = filters.get("qualification_lost_date", "").strip()
    facility_query = filters.get("facility_q", "").strip()
    facility_codes = split_filter_values(filters.get("facility_codes", ""))
    duplicate_subscriber = filters.get("duplicate_subscriber", "").strip()
    author_import_status = filters.get("author_import_status", "").strip()
    exam_item_namecodes = split_filter_values(filters.get("exam_item_namecodes", ""))
    exam_item_match_mode = filters.get("exam_item_match_mode", "any").strip().lower()
    if exam_item_match_mode not in {"any", "all"}:
        exam_item_match_mode = "any"
    if event_id:
        where_parts.append("eec.event_id = %s")
        params.append(event_id)
    if legal_check_result:
        where_parts.append("COALESCE(ecr.legal_check_result, 'PENDING') = %s")
        params.append(legal_check_result)
    if specific_check_result:
        where_parts.append(f"({specific_check_result_sql('ecr')}) = %s")
        params.append(specific_check_result)
    if export_readiness_status:
        where_parts.append("eec.export_readiness_status = %s")
        params.append(export_readiness_status)
    if source_mode:
        where_parts.append("eec.source_mode = %s")
        params.append(source_mode)
    if exam_months:
        where_parts.append(f"DATE_FORMAT(eec.exam_date, '%Y-%m') IN ({', '.join(['%s'] * len(exam_months))})")
        params.extend(exam_months)
    if query:
        like = f"%{query}%"
        where_parts.append(
            """
            (
              CAST(eec.exam_export_case_id AS CHAR) = %s
              OR eec.hia_subscriber_id LIKE %s
              OR eec.person_id_custom LIKE %s
              OR eec.name_full_raw LIKE %s
            )
            """
        )
        params.extend([query, like, like, like])
    if case_id:
        where_parts.append("CAST(eec.exam_export_case_id AS CHAR) = %s")
        params.append(case_id)
    if name_full:
        like = f"%{name_full}%"
        where_parts.append("(eec.name_full_raw LIKE %s OR eec.name_full_export_value LIKE %s)")
        params.extend([like, like])
    if name_kana:
        like = f"%{name_kana}%"
        where_parts.append("(eec.name_kana_raw LIKE %s OR eec.name_kana_export_value LIKE %s)")
        params.extend([like, like])
    if hia_subscriber_id:
        like = f"%{hia_subscriber_id}%"
        where_parts.append("eec.hia_subscriber_id LIKE %s")
        params.append(like)
    if subscriber_id:
        where_parts.append("CAST(eec.subscriber_id AS CHAR) = %s")
        params.append(subscriber_id)
    if qualification_lost_status == "LOST":
        where_parts.append("s.qualification_lost_date IS NOT NULL")
    elif qualification_lost_status == "ACTIVE":
        where_parts.append("s.qualification_lost_date IS NULL")
    if qualification_lost_date:
        if re.fullmatch(r"\d{4}-\d{2}", qualification_lost_date):
            where_parts.append("DATE_FORMAT(s.qualification_lost_date, '%Y-%m') = %s")
            params.append(qualification_lost_date)
        else:
            where_parts.append("s.qualification_lost_date = %s")
            params.append(qualification_lost_date)
    if insurance_symbol:
        like = f"%{insurance_symbol}%"
        where_parts.append("(eec.insurance_symbol_raw LIKE %s OR eec.insurance_symbol_export_value LIKE %s)")
        params.extend([like, like])
    if insurance_number:
        like = f"%{insurance_number}%"
        where_parts.append("(eec.insurance_number_raw LIKE %s OR eec.insurance_number_export_value LIKE %s)")
        params.extend([like, like])
    if facility_query:
        like = f"%{facility_query}%"
        where_parts.append(
            """
            (
              eec.facility_name LIKE %s
              OR eec.facility_code LIKE %s
              OR mfa.expected_source_mode LIKE %s
            )
            """
        )
        params.extend([like, like, like])
    if facility_codes:
        where_parts.append(f"eec.facility_code IN ({', '.join(['%s'] * len(facility_codes))})")
        params.extend(facility_codes)
    if duplicate_subscriber == "1":
        where_parts.append("COALESCE(subcase.subscriber_case_count, 0) >= 2")
    if author_import_status == "RECOVERED":
        where_parts.append(
            f"""
            EXISTS (
              SELECT 1
              FROM {qname(health_db())}.exam_export_case_values AS author_case_value
              INNER JOIN {qname(health_db())}.exam_item_values AS author_value
                ON author_value.id = author_case_value.source_exam_item_value_id
              WHERE author_case_value.exam_export_case_id = eec.exam_export_case_id
                AND author_value.normalize_reason = 'XML_AUTHOR_ELEMENT'
            )
            """
        )
    elif author_import_status == "MISSING_CANDIDATE":
        where_parts.append(author_missing_candidate_exists_sql("eec"))
    if exam_item_namecodes:
        placeholders = ", ".join(["%s"] * len(exam_item_namecodes))
        match_count = len(exam_item_namecodes) if exam_item_match_mode == "all" else 1
        match_operator = "=" if exam_item_match_mode == "all" else ">="
        where_parts.append(
            f"""
            (
              SELECT COUNT(DISTINCT item_filter.namecode)
              FROM {qname(health_db())}.exam_export_case_values AS item_filter
              WHERE item_filter.exam_export_case_id = eec.exam_export_case_id
                AND item_filter.namecode IN ({placeholders})
            ) {match_operator} %s
            """
        )
        params.extend([*exam_item_namecodes, match_count])
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    return where_sql, params


def author_missing_candidate_exists_sql(case_alias: str) -> str:
    return f"""
        EXISTS (
          SELECT 1
          FROM {qname(health_db())}.exam_export_case_sources AS author_source
          INNER JOIN {qname(health_db())}.exam_item_values AS author_parent_value
            ON author_parent_value.ledger_type = 'EXAM'
           AND author_parent_value.ledger_id = author_source.source_exam_ledger_id
          INNER JOIN {qname(dev_db())}.exam_item_master AS author_parent_item
            ON author_parent_item.namecode = author_parent_value.namecode
           AND author_parent_item.annex2_author_item_code IS NOT NULL
          WHERE author_source.exam_export_case_id = {case_alias}.exam_export_case_id
            AND author_source.source_type = 'XML'
            AND NOT EXISTS (
              SELECT 1
              FROM {qname(health_db())}.exam_item_values AS expected_author_value
              WHERE expected_author_value.ledger_type = 'EXAM'
                AND expected_author_value.ledger_id = author_source.source_exam_ledger_id
                AND expected_author_value.namecode = author_parent_item.annex2_author_item_code
            )
        )
    """


def author_recovered_count_sql(case_alias: str) -> str:
    return f"""
        (
          SELECT COUNT(*)
          FROM {qname(health_db())}.exam_export_case_values AS author_case_value
          INNER JOIN {qname(health_db())}.exam_item_values AS author_value
            ON author_value.id = author_case_value.source_exam_item_value_id
          WHERE author_case_value.exam_export_case_id = {case_alias}.exam_export_case_id
            AND author_value.normalize_reason = 'XML_AUTHOR_ELEMENT'
        )
    """


def load_exam_export_case_count(cur: Any, *, filters: dict[str, str]) -> int:
    where_sql, params = build_exam_export_case_where(filters)
    cur.execute(
        f"""
        SELECT COUNT(*) AS total_count
        FROM {qname(health_db())}.exam_export_cases AS eec
        LEFT JOIN (
          SELECT r1.*
          FROM {qname(health_db())}.exam_check_results AS r1
          INNER JOIN (
            SELECT exam_export_case_id, MAX(id) AS max_id
            FROM {qname(health_db())}.exam_check_results
            WHERE ledger_type = 'EXPORT_CASE'
              AND exam_export_case_id IS NOT NULL
            GROUP BY exam_export_case_id
          ) AS latest
            ON latest.max_id = r1.id
        ) AS ecr
          ON ecr.exam_export_case_id = eec.exam_export_case_id
        LEFT JOIN {qname(dev_db())}.subscribers AS s
          ON s.id = eec.subscriber_id
        LEFT JOIN (
          SELECT
            event_id,
            exam_facility_id,
            MAX(expected_source_mode) AS expected_source_mode
          FROM {qname(master_db())}.medical_folder_aliases
          WHERE is_active = 1
          GROUP BY event_id, exam_facility_id
        ) AS mfa
          ON mfa.event_id = eec.event_id
         AND mfa.exam_facility_id = eec.exam_facility_id
        LEFT JOIN (
          SELECT event_id, subscriber_id, COUNT(*) AS subscriber_case_count
          FROM {qname(health_db())}.exam_export_cases
          WHERE subscriber_id IS NOT NULL
          GROUP BY event_id, subscriber_id
        ) AS subcase
          ON subcase.event_id = eec.event_id
         AND subcase.subscriber_id = eec.subscriber_id
        {where_sql}
        """,
        tuple(params),
    )
    row = cur.fetchone()
    return int((row or {}).get("total_count") or 0)


def exam_export_case_base_filters(filters: dict[str, str]) -> dict[str, str]:
    return {key: (filters.get(key, "") if key == "event_id" else "") for key in filters}


def has_exam_export_case_detail_filters(filters: dict[str, str]) -> bool:
    ignored_keys = {"event_id", "limit", "page"}
    return any(str(value or "").strip() for key, value in filters.items() if key not in ignored_keys)


def load_exam_export_case_summary(cur: Any, *, filters: dict[str, str]) -> dict[str, int]:
    where_sql, params = build_exam_export_case_where(filters)
    cur.execute(
        f"""
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN eec.export_readiness_status = 'EXPORT_READY' THEN 1 ELSE 0 END) AS ready,
          SUM(CASE WHEN eec.export_readiness_status = 'APPROVED_WITH_REASON' THEN 1 ELSE 0 END) AS approved_with_reason,
          SUM(CASE WHEN eec.export_readiness_status = 'BLOCKED' THEN 1 ELSE 0 END) AS blocked,
          SUM(CASE WHEN eec.export_readiness_status NOT IN ('EXPORT_READY', 'APPROVED_WITH_REASON', 'BLOCKED')
                    OR eec.export_readiness_status IS NULL THEN 1 ELSE 0 END) AS waiting,
          SUM(CASE WHEN eec.xml_export_status = 'EXPORTED' THEN 1 ELSE 0 END) AS exported,
          SUM(CASE WHEN COALESCE(ecr.legal_check_result, 'PENDING') = 'NG' THEN 1 ELSE 0 END) AS legal_ng,
          SUM(CASE WHEN ({specific_check_result_sql('ecr')}) = 'NG' THEN 1 ELSE 0 END) AS specific_ng,
          SUM(CASE WHEN COALESCE(src.source_count, 0) >= 2 THEN 1 ELSE 0 END) AS multi_source,
          SUM(CASE WHEN COALESCE(subcase.subscriber_case_count, 0) >= 2 THEN 1 ELSE 0 END) AS duplicate_subscriber
        FROM {qname(health_db())}.exam_export_cases AS eec
        LEFT JOIN (
          SELECT r1.*
          FROM {qname(health_db())}.exam_check_results AS r1
          INNER JOIN (
            SELECT exam_export_case_id, MAX(id) AS max_id
            FROM {qname(health_db())}.exam_check_results
            WHERE ledger_type = 'EXPORT_CASE'
              AND exam_export_case_id IS NOT NULL
            GROUP BY exam_export_case_id
          ) AS latest
            ON latest.max_id = r1.id
        ) AS ecr
          ON ecr.exam_export_case_id = eec.exam_export_case_id
        LEFT JOIN {qname(dev_db())}.subscribers AS s
          ON s.id = eec.subscriber_id
        LEFT JOIN (
          SELECT
            event_id,
            exam_facility_id,
            MAX(expected_source_mode) AS expected_source_mode
          FROM {qname(master_db())}.medical_folder_aliases
          WHERE is_active = 1
          GROUP BY event_id, exam_facility_id
        ) AS mfa
          ON mfa.event_id = eec.event_id
         AND mfa.exam_facility_id = eec.exam_facility_id
        LEFT JOIN (
          SELECT exam_export_case_id, COUNT(*) AS source_count
          FROM {qname(health_db())}.exam_export_case_sources
          GROUP BY exam_export_case_id
        ) AS src
          ON src.exam_export_case_id = eec.exam_export_case_id
        LEFT JOIN (
          SELECT event_id, subscriber_id, COUNT(*) AS subscriber_case_count
          FROM {qname(health_db())}.exam_export_cases
          WHERE subscriber_id IS NOT NULL
          GROUP BY event_id, subscriber_id
        ) AS subcase
          ON subcase.event_id = eec.event_id
         AND subcase.subscriber_id = eec.subscriber_id
        {where_sql}
        """,
        tuple(params),
    )
    row = dict(cur.fetchone() or {})
    return {key: int(row.get(key) or 0) for key in (
        "total",
        "ready",
        "approved_with_reason",
        "blocked",
        "waiting",
        "exported",
        "legal_ng",
        "specific_ng",
        "multi_source",
        "duplicate_subscriber",
    )}


def load_exam_export_case_rows(
    cur: Any,
    *,
    filters: dict[str, str],
    limit: int = 200,
    offset: int = 0,
) -> list[dict[str, Any]]:
    where_sql, params = build_exam_export_case_where(filters)
    order_sql = "eec.updated_at DESC, eec.exam_export_case_id DESC"
    if filters.get("duplicate_subscriber") == "1":
        order_sql = "eec.subscriber_id, eec.exam_date DESC, eec.exam_facility_id, eec.exam_export_case_id DESC"
    subscriber_columns = manual_exam_entry_existing_columns(cur, dev_db(), "subscribers")
    subscriber_name_full_expr = "s.name_kanji_full" if "name_kanji_full" in subscriber_columns else "s.name_full_match"
    subscriber_select = lambda column, alias=None: (
        f"s.{column} AS {alias or column}" if column in subscriber_columns else f"NULL AS {alias or column}"
    )
    cur.execute(
        f"""
        SELECT
          eec.exam_export_case_id,
          eec.event_id,
          eec.subscriber_id,
          eec.hia_subscriber_id,
          eec.subscriber_match_status,
          eec.person_id_custom,
          eec.insurer_number,
          eec.exam_facility_id,
          eec.facility_code,
          eec.facility_name,
          mfa.expected_source_mode,
          eec.exam_date,
          eec.health_exam_report_category,
          eec.program_code,
          eec.insurance_symbol_raw,
          eec.insurance_symbol_export_value,
          eec.insurance_number_raw,
          eec.insurance_number_export_value,
          eec.insurance_branch_number_raw,
          eec.insurance_branch_number_export_value,
          eec.postal_code,
          eec.postal_code_completed_value,
          eec.name_full_raw,
          eec.name_kana_raw,
          eec.birthdate,
          eec.gender_code,
          hds.status AS hia_dashboard_status,
          hds.medical_institution AS hia_dashboard_medical_institution,
          hds.reservation_date AS hia_dashboard_reservation_date,
          hds.exam_date AS hia_dashboard_exam_date,
          hds.course_name AS hia_dashboard_course_name,
          s.insurer_number AS subscriber_insurer_number,
          s.insurance_symbol AS subscriber_insurance_symbol,
          s.insurance_symbol_export AS subscriber_insurance_symbol_export,
          s.insurance_number AS subscriber_insurance_number,
          s.insurance_branchnumber AS subscriber_insurance_branchnumber,
          s.birth AS subscriber_birth,
          s.gender_code AS subscriber_gender_code,
          s.name_kana_full AS subscriber_name_kana_full,
          {subscriber_name_full_expr} AS subscriber_name_kanji_full,
          s.person_id_custom AS subscriber_person_id_custom,
          s.hia_subscriber_id AS subscriber_hia_subscriber_id,
          {subscriber_select("insured_attribute_name")},
          {subscriber_select("relationship_name")},
          {subscriber_select("qualification_acquired_date")},
          {subscriber_select("qualification_lost_date")},
          {subscriber_select("employer_code")},
          {subscriber_select("department_code")},
          {subscriber_select("distribution_code")},
          {subscriber_select("employee_code")},
          {subscriber_select("connect_id")},
          eec.source_mode,
          eec.case_status,
          eec.case_reason,
          eec.merge_status,
          eec.merge_reason,
          eec.value_build_status,
          eec.value_build_reason,
          eec.case_value_count,
          eec.check_status,
          eec.check_reason,
          eec.xml_export_status,
          eec.output_zip_file_name,
          eec.output_xml_file_name,
          eec.manual_export_approved,
          eec.export_readiness_status,
          eec.export_readiness_reason,
          eec.correction_status,
          eec.updated_at,
          COALESCE(ecr.legal_check_result, 'PENDING') AS legal_check_result,
          ecr.legal_reason_summary,
          COALESCE(ecr.specific_check_result, 'PENDING') AS specific_check_result,
          ecr.specific_reason_summary,
          COALESCE(src.source_count, 0) AS source_count,
          COALESCE(src.xml_count, 0) AS xml_count,
          COALESCE(src.csv_count, 0) AS csv_count,
          COALESCE(src.paper_count, 0) AS paper_count,
          COALESCE(subcase.subscriber_case_count, 0) AS subscriber_case_count,
          {author_recovered_count_sql('eec')} AS author_recovered_count,
          CASE WHEN {author_missing_candidate_exists_sql('eec')} THEN 1 ELSE 0 END AS author_missing_candidate
        FROM {qname(health_db())}.exam_export_cases AS eec
        LEFT JOIN (
          SELECT r1.*
          FROM {qname(health_db())}.exam_check_results AS r1
          INNER JOIN (
            SELECT exam_export_case_id, MAX(id) AS max_id
            FROM {qname(health_db())}.exam_check_results
            WHERE ledger_type = 'EXPORT_CASE'
              AND exam_export_case_id IS NOT NULL
            GROUP BY exam_export_case_id
          ) AS latest
            ON latest.max_id = r1.id
        ) AS ecr
          ON ecr.exam_export_case_id = eec.exam_export_case_id
        LEFT JOIN {qname(dev_db())}.subscribers AS s
          ON s.id = eec.subscriber_id
        LEFT JOIN (
          SELECT *
          FROM (
            SELECT
              hds1.*,
              ROW_NUMBER() OVER (
                PARTITION BY hds1.hia_subscriber_id
                ORDER BY hds1.updated_at DESC, hds1.hia_dashboard_person_id DESC
              ) AS dashboard_rank
            FROM {qname(work_other_db())}.hia_dashboard_status AS hds1
            WHERE hds1.is_active = 1
              AND hds1.hia_subscriber_id IS NOT NULL
              AND hds1.hia_subscriber_id <> ''
          ) AS ranked_dashboard
          WHERE ranked_dashboard.dashboard_rank = 1
        ) AS hds
          ON hds.hia_subscriber_id = COALESCE(NULLIF(eec.hia_subscriber_id, ''), s.hia_subscriber_id)
        LEFT JOIN (
          SELECT
            exam_export_case_id,
            COUNT(*) AS source_count,
            SUM(CASE WHEN source_type = 'XML' THEN 1 ELSE 0 END) AS xml_count,
            SUM(CASE WHEN source_type = 'CSV' THEN 1 ELSE 0 END) AS csv_count,
            SUM(CASE WHEN source_type = 'PAPER' THEN 1 ELSE 0 END) AS paper_count
          FROM {qname(health_db())}.exam_export_case_sources
          GROUP BY exam_export_case_id
        ) AS src
          ON src.exam_export_case_id = eec.exam_export_case_id
        LEFT JOIN (
          SELECT
            event_id,
            exam_facility_id,
            MAX(expected_source_mode) AS expected_source_mode
          FROM {qname(master_db())}.medical_folder_aliases
          WHERE is_active = 1
          GROUP BY event_id, exam_facility_id
        ) AS mfa
          ON mfa.event_id = eec.event_id
         AND mfa.exam_facility_id = eec.exam_facility_id
        LEFT JOIN (
          SELECT event_id, subscriber_id, COUNT(*) AS subscriber_case_count
          FROM {qname(health_db())}.exam_export_cases
          WHERE subscriber_id IS NOT NULL
          GROUP BY event_id, subscriber_id
        ) AS subcase
          ON subcase.event_id = eec.event_id
         AND subcase.subscriber_id = eec.subscriber_id
        {where_sql}
        ORDER BY {order_sql}
        LIMIT %s OFFSET %s
        """,
        (*params, limit, offset),
    )
    rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        row["expected_source_mode_label"] = source_mode_label(row.get("expected_source_mode"))
        row["specific_check_result_display"] = normalize_specific_check_result(
            row.get("specific_check_result"),
            row.get("specific_reason_summary"),
        )
    return rows


EXAM_EXPORT_CASE_CSV_PERMISSION = "exam_export_cases.csv_download"

EXAM_EXPORT_CASE_CSV_FIELD_GROUPS: list[dict[str, Any]] = [
    {
        "group": "case基本",
        "fields": [
            ("exam_export_case_id", "case_id"),
            ("event_id", "event_id"),
            ("subscriber_id", "case_subscriber_id"),
            ("hia_subscriber_id", "case_hia_id"),
            ("person_id_custom", "case_person_id_custom"),
            ("subscriber_match_status", "突合状態"),
            ("subscriber_match_reason", "突合理由"),
        ],
    },
    {
        "group": "受診者(case)",
        "fields": [
            ("name_kana_raw", "case氏名カナ"),
            ("name_full_raw", "case氏名"),
            ("birthdate", "case生年月日"),
            ("gender_code", "case性別"),
            ("insurer_number", "case保険者番号"),
            ("insurance_symbol_raw", "case記号"),
            ("insurance_symbol_export_value", "case記号_出力"),
            ("insurance_number_raw", "case番号"),
            ("insurance_number_export_value", "case番号_出力"),
            ("insurance_branch_number_raw", "case枝番"),
            ("insurance_branch_number_export_value", "case枝番_出力"),
        ],
    },
    {
        "group": "加入者(subscriber)",
        "fields": [
            ("subscriber_hia_subscriber_id", "加入者HIA ID"),
            ("subscriber_person_id_custom", "加入者person_id_custom"),
            ("subscriber_name_kana_full", "加入者氏名カナ"),
            ("subscriber_name_kanji_full", "加入者氏名"),
            ("subscriber_birth", "加入者生年月日"),
            ("subscriber_gender_code", "加入者性別"),
            ("subscriber_insurer_number", "加入者保険者番号"),
            ("subscriber_insurance_symbol", "加入者記号"),
            ("subscriber_insurance_symbol_export", "加入者記号_出力"),
            ("subscriber_insurance_number", "加入者番号"),
            ("subscriber_insurance_branchnumber", "加入者枝番"),
            ("insured_attribute_name", "本人家族区分"),
            ("relationship_name", "続柄"),
            ("qualification_acquired_date", "資格取得日"),
            ("qualification_lost_date", "資格喪失日"),
            ("employer_code", "事業所コード"),
            ("department_code", "部署コード"),
            ("distribution_code", "配布コード"),
            ("employee_code", "社員番号"),
            ("connect_id", "connect_id"),
        ],
    },
    {
        "group": "受診",
        "fields": [
            ("exam_facility_id", "健診機関ID"),
            ("facility_code", "健診機関コード"),
            ("facility_name", "健診機関名"),
            ("expected_source_mode", "想定受領方式"),
            ("exam_date", "受診日"),
            ("health_exam_report_category", "報告区分"),
            ("program_code", "プログラムコード"),
            ("report_code_resolution_source", "報告コード確定元"),
            ("report_code_resolution_reason", "報告コード確定理由"),
        ],
    },
    {
        "group": "ダッシュボード",
        "fields": [
            ("hia_dashboard_status", "ダッシュボード状態"),
            ("hia_dashboard_medical_institution", "ダッシュボード病院名"),
            ("hia_dashboard_reservation_date", "ダッシュボード予約日"),
            ("hia_dashboard_exam_date", "ダッシュボード受診日"),
            ("hia_dashboard_course_name", "ダッシュボードコース名"),
        ],
    },
    {
        "group": "チェック/出力",
        "fields": [
            ("source_mode", "source"),
            ("case_value_count", "採用値数"),
            ("legal_check_result", "法定チェック"),
            ("legal_reason_summary", "法定理由"),
            ("specific_check_result_display", "特定健診チェック"),
            ("specific_reason_summary", "特定健診理由"),
            ("export_readiness_status", "出力可否"),
            ("export_readiness_reason", "出力不可理由"),
            ("xml_export_status", "XML出力状態"),
            ("output_zip_file_name", "出力ZIP"),
            ("output_xml_file_name", "出力XML"),
            ("updated_at", "case更新日時"),
        ],
    },
    {
        "group": "source数",
        "fields": [
            ("source_count", "source数"),
            ("xml_count", "XML数"),
            ("csv_count", "CSV数"),
            ("paper_count", "紙数"),
        ],
    },
]

EXAM_EXPORT_CASE_CSV_FIELD_LABELS: dict[str, str] = {
    field: label
    for group in EXAM_EXPORT_CASE_CSV_FIELD_GROUPS
    for field, label in group["fields"]
}

EXAM_EXPORT_CASE_CSV_DEFAULT_FIELDS = [
    "exam_export_case_id",
    "event_id",
    "subscriber_id",
    "hia_subscriber_id",
    "person_id_custom",
    "subscriber_match_status",
    "name_kana_raw",
    "name_full_raw",
    "birthdate",
    "gender_code",
    "insurer_number",
    "insurance_symbol_raw",
    "insurance_number_raw",
    "insurance_branch_number_raw",
    "subscriber_name_kana_full",
    "subscriber_name_kanji_full",
    "subscriber_birth",
    "subscriber_gender_code",
    "subscriber_insurance_symbol",
    "subscriber_insurance_number",
    "subscriber_insurance_branchnumber",
    "relationship_name",
    "qualification_lost_date",
    "facility_code",
    "facility_name",
    "exam_date",
    "source_mode",
    "legal_check_result",
    "legal_reason_summary",
    "specific_check_result_display",
    "specific_reason_summary",
    "export_readiness_status",
    "export_readiness_reason",
    "output_zip_file_name",
    "output_xml_file_name",
]


def can_download_exam_export_case_csv(user: Mapping[str, Any]) -> bool:
    return has_any_permission(user, (EXAM_EXPORT_CASE_CSV_PERMISSION, SYSTEM_SETTINGS_PERMISSION))


def normalize_exam_export_case_csv_fields(raw_fields: Sequence[str]) -> list[str]:
    allowed = set(EXAM_EXPORT_CASE_CSV_FIELD_LABELS)
    fields = [field for field in raw_fields if field in allowed]
    if not fields:
        fields = list(EXAM_EXPORT_CASE_CSV_DEFAULT_FIELDS)
    return list(dict.fromkeys(fields))


def csv_cell_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def build_exam_export_case_csv(rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> str:
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow([EXAM_EXPORT_CASE_CSV_FIELD_LABELS[field] for field in fields])
    for row in rows:
        writer.writerow([csv_cell_value(row.get(field)) for field in fields])
    return output.getvalue()


def build_exam_export_case_pagination(
    filters: dict[str, str],
    *,
    total_count: int,
    row_count: int,
    page: int,
    limit: int,
) -> dict[str, Any]:
    page_count = max(1, (total_count + limit - 1) // limit)
    page = min(max(1, page), page_count)
    start = ((page - 1) * limit) + 1 if total_count else 0
    end = min(total_count, start + row_count - 1) if total_count else 0

    def page_url(target_page: int) -> str:
        query = {key: value for key, value in filters.items() if value and key != "page"}
        query["limit"] = str(limit)
        query["page"] = str(target_page)
        return f"/exam-export-cases?{urlencode(query)}"

    window_pages = {1, page_count}
    for candidate in range(page - 2, page + 3):
        if 1 <= candidate <= page_count:
            window_pages.add(candidate)

    pages: list[dict[str, Any]] = []
    previous_page = 0
    for page_number in sorted(window_pages):
        pages.append(
            {
                "page": page_number,
                "url": page_url(page_number),
                "is_current": page_number == page,
                "gap_before": previous_page > 0 and page_number > previous_page + 1,
            }
        )
        previous_page = page_number

    return {
        "page": page,
        "limit": limit,
        "page_count": page_count,
        "total_count": total_count,
        "start": start,
        "end": end,
        "has_previous": page > 1,
        "has_next": page < page_count,
        "previous_url": page_url(page - 1) if page > 1 else "",
        "next_url": page_url(page + 1) if page < page_count else "",
        "pages": pages,
    }


def build_exam_export_case_summary_filter_urls(filters: dict[str, str], *, limit: int) -> dict[str, str]:
    def filter_url(**overrides: str) -> str:
        query = {key: value for key, value in filters.items() if value and key != "page"}
        query.update(overrides)
        query["limit"] = str(limit)
        query["page"] = "1"
        return f"/exam-export-cases?{urlencode(query)}"

    return {
        "ready": filter_url(export_readiness_status="EXPORT_READY"),
        "approved_with_reason": filter_url(export_readiness_status="APPROVED_WITH_REASON"),
        "blocked": filter_url(export_readiness_status="BLOCKED"),
        "exported": filter_url(export_readiness_status="EXPORTED"),
        "multi_source": filter_url(source_mode="XML_CSV"),
        "duplicate_subscriber": filter_url(duplicate_subscriber="1"),
    }


def load_exam_export_case_month_options(cur: Any, *, event_id: str | None = None, limit: int = 36) -> list[dict[str, Any]]:
    where_parts = ["exam_date IS NOT NULL"]
    params: list[Any] = []
    event_text = str(event_id or "").strip()
    if event_text:
        where_parts.append("event_id = %s")
        params.append(event_text)
    where_sql = " AND ".join(where_parts)
    cur.execute(
        f"""
        SELECT
          DATE_FORMAT(exam_date, '%Y-%m') AS exam_month,
          COUNT(*) AS case_count
        FROM {qname(health_db())}.exam_export_cases
        WHERE {where_sql}
        GROUP BY DATE_FORMAT(exam_date, '%Y-%m')
        ORDER BY exam_month DESC
        LIMIT %s
        """,
        (*params, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def load_exam_export_case_facility_options(cur: Any, *, event_id: str | None = None, limit: int = 2000) -> list[dict[str, Any]]:
    where_parts = ["eec.exam_facility_id IS NOT NULL"]
    params: list[Any] = []
    event_text = str(event_id or "").strip()
    if event_text:
        where_parts.append("eec.event_id = %s")
        params.append(event_text)
    cur.execute(
        f"""
        SELECT
          eec.event_id,
          eec.exam_facility_id,
          COALESCE(ef.exam_facility_code, eec.facility_code) AS exam_facility_code,
          COALESCE(ef.exam_facility_name, eec.facility_name) AS exam_facility_name,
          ef.exam_facility_display_name,
          MAX(mfa.expected_source_mode) AS expected_source_mode,
          COUNT(*) AS case_count,
          SUM(CASE WHEN eec.source_mode LIKE '%%XML%%' THEN 1 ELSE 0 END) AS xml_case_count,
          SUM(CASE WHEN eec.source_mode LIKE '%%CSV%%' THEN 1 ELSE 0 END) AS csv_case_count,
          SUM(CASE WHEN eec.source_mode LIKE '%%PAPER%%' OR eec.source_mode LIKE '%%MANUAL%%' THEN 1 ELSE 0 END) AS paper_case_count,
          MIN(eec.exam_date) AS first_exam_date,
          MAX(eec.exam_date) AS latest_exam_date
        FROM {qname(health_db())}.exam_export_cases AS eec
        LEFT JOIN {qname(master_db())}.exam_facilities AS ef
          ON ef.exam_facility_id = eec.exam_facility_id
        LEFT JOIN (
          SELECT
            event_id,
            exam_facility_id,
            MAX(expected_source_mode) AS expected_source_mode
          FROM {qname(master_db())}.medical_folder_aliases
          WHERE is_active = 1
          GROUP BY event_id, exam_facility_id
        ) AS mfa
          ON mfa.event_id = eec.event_id
         AND mfa.exam_facility_id = eec.exam_facility_id
        WHERE {' AND '.join(where_parts)}
        GROUP BY
          eec.event_id,
          eec.exam_facility_id,
          COALESCE(ef.exam_facility_code, eec.facility_code),
          COALESCE(ef.exam_facility_name, eec.facility_name),
          ef.exam_facility_display_name
        HAVING exam_facility_code IS NOT NULL
           AND exam_facility_code <> ''
        ORDER BY case_count DESC, exam_facility_code, exam_facility_name
        LIMIT %s
        """,
        (*params, limit),
    )
    rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        row["expected_source_mode_label"] = source_mode_label(row.get("expected_source_mode"))
    return rows


def load_facility_summary_month_options(cur: Any, *, event_id: str | None = None, limit: int = 36) -> list[dict[str, Any]]:
    params: list[Any] = []
    event_text = str(event_id or "").strip()
    ledger_event_clause = ""
    case_event_clause = ""
    if event_text:
        ledger_event_clause = "AND event_id = %s"
        case_event_clause = "AND event_id = %s"
        params.extend([event_text, event_text])
    cur.execute(
        f"""
        SELECT
          exam_month,
          SUM(source_count) AS source_count,
          SUM(case_count) AS case_count
        FROM (
          SELECT
            DATE_FORMAT(exam_date, '%Y-%m') AS exam_month,
            COUNT(*) AS source_count,
            0 AS case_count
          FROM {qname(health_db())}.exam_ledgers
          WHERE exam_date IS NOT NULL
            {ledger_event_clause}
          GROUP BY DATE_FORMAT(exam_date, '%Y-%m')
          UNION ALL
          SELECT
            DATE_FORMAT(exam_date, '%Y-%m') AS exam_month,
            0 AS source_count,
            COUNT(*) AS case_count
          FROM {qname(health_db())}.exam_export_cases
          WHERE exam_date IS NOT NULL
            {case_event_clause}
          GROUP BY DATE_FORMAT(exam_date, '%Y-%m')
        ) AS months
        GROUP BY exam_month
        ORDER BY exam_month DESC
        LIMIT %s
        """,
        (*params, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def load_exam_export_case_detail(cur: Any, *, exam_export_case_id: int) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT
          eec.*,
          mfa.expected_source_mode,
          COALESCE(ecr.legal_check_result, 'PENDING') AS legal_check_result,
          ecr.legal_reason_summary,
          COALESCE(ecr.specific_check_result, 'PENDING') AS specific_check_result,
          ecr.specific_reason_summary,
          {author_recovered_count_sql('eec')} AS author_recovered_count,
          CASE WHEN {author_missing_candidate_exists_sql('eec')} THEN 1 ELSE 0 END AS author_missing_candidate
        FROM {qname(health_db())}.exam_export_cases AS eec
        LEFT JOIN (
          SELECT r1.*
          FROM {qname(health_db())}.exam_check_results AS r1
          INNER JOIN (
            SELECT exam_export_case_id, MAX(id) AS max_id
            FROM {qname(health_db())}.exam_check_results
            WHERE ledger_type = 'EXPORT_CASE'
              AND exam_export_case_id IS NOT NULL
            GROUP BY exam_export_case_id
          ) AS latest
            ON latest.max_id = r1.id
        ) AS ecr
          ON ecr.exam_export_case_id = eec.exam_export_case_id
        LEFT JOIN (
          SELECT
            event_id,
            exam_facility_id,
            MAX(expected_source_mode) AS expected_source_mode
          FROM {qname(master_db())}.medical_folder_aliases
          WHERE is_active = 1
          GROUP BY event_id, exam_facility_id
        ) AS mfa
          ON mfa.event_id = eec.event_id
         AND mfa.exam_facility_id = eec.exam_facility_id
        WHERE eec.exam_export_case_id = %s
        LIMIT 1
        """,
        (exam_export_case_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    result = dict(row)
    result["expected_source_mode_label"] = source_mode_label(result.get("expected_source_mode"))
    result["specific_check_result_display"] = normalize_specific_check_result(
        result.get("specific_check_result"),
        result.get("specific_reason_summary"),
    )
    return result


def load_exam_export_case_sources(cur: Any, *, exam_export_case_id: int) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          eecs.*,
          el.facility_name,
          el.facility_code,
          el.exam_date,
          el.name_kana_raw,
          el.hia_subscriber_id,
          el.subscriber_match_status,
          el.check_status,
          el.check_reason,
          el.exam_item_count,
          el.exam_item_error_count,
          el.xml_file_name,
          el.mapping_version,
          fr.file_name,
          fr.relative_path,
          fr.status AS file_receipt_status
        FROM {qname(health_db())}.exam_export_case_sources AS eecs
        INNER JOIN {qname(health_db())}.exam_ledgers AS el
          ON el.exam_ledger_id = eecs.source_exam_ledger_id
        LEFT JOIN {qname(health_db())}.file_receipts AS fr
          ON fr.id = eecs.file_receipt_id
        WHERE eecs.exam_export_case_id = %s
        ORDER BY eecs.source_priority, eecs.source_type, eecs.source_exam_ledger_id
        """,
        (exam_export_case_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def load_exam_export_case_values(
    cur: Any,
    *,
    exam_export_case_id: int,
    source_headers: Sequence[Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          eecv.*,
          COALESCE(eiv.namecode_display_name, eim.item_name, eecv.namecode) AS item_name,
          eiv.raw_value AS adopted_raw_value,
          eiv.raw_value_type AS adopted_raw_value_type,
          eiv.raw_unit AS adopted_raw_unit,
          eiv.normalize_status AS adopted_normalize_status,
          eiv.validation_status AS adopted_validation_status,
          eiv.review_status AS adopted_review_status,
          el.source_type AS adopted_source_type,
          el.facility_name AS adopted_source_facility_name
        FROM {qname(health_db())}.exam_export_case_values AS eecv
        LEFT JOIN {qname(health_db())}.exam_item_values AS eiv
          ON eiv.id = eecv.source_exam_item_value_id
        LEFT JOIN {qname(dev_db())}.exam_item_master AS eim
          ON eim.namecode = eecv.namecode
        LEFT JOIN {qname(health_db())}.exam_ledgers AS el
          ON el.exam_ledger_id = eecv.source_exam_ledger_id
        WHERE eecv.exam_export_case_id = %s
        ORDER BY eecv.namecode, eecv.occurrence_no, eecv.exam_export_case_value_id
        """,
        (exam_export_case_id,),
    )
    adopted_rows = [dict(row) for row in cur.fetchall()]
    if not adopted_rows:
        return []

    cur.execute(
        f"""
        SELECT
          eiv.id AS source_exam_item_value_id,
          eiv.namecode,
          eiv.occurrence_no,
          eiv.raw_value,
          eiv.raw_value_type,
          eiv.raw_unit,
          eiv.normalized_value,
          eiv.normalized_unit,
          eiv.nullflavor,
          eiv.code_value,
          eiv.code_display,
          eiv.interpretation_code,
          eiv.interpretation_name,
          eiv.normalize_status,
          eiv.validation_status,
          eiv.review_status,
          src.source_role,
          src.source_type,
          src.source_priority,
          src.source_exam_ledger_id,
          el.facility_name AS source_facility_name,
          COALESCE(fr.file_name, el.xml_file_name, el.mapping_version) AS source_file_name,
          fr.relative_path AS source_relative_path,
          COALESCE(eiv.namecode_display_name, eim.item_name, eiv.namecode) AS item_name
        FROM {qname(health_db())}.exam_export_case_sources AS src
        INNER JOIN {qname(health_db())}.exam_item_values AS eiv
          ON eiv.ledger_type = 'EXAM'
         AND eiv.ledger_id = src.source_exam_ledger_id
        LEFT JOIN {qname(dev_db())}.exam_item_master AS eim
          ON eim.namecode = eiv.namecode
        LEFT JOIN {qname(health_db())}.exam_ledgers AS el
          ON el.exam_ledger_id = src.source_exam_ledger_id
        LEFT JOIN {qname(health_db())}.file_receipts AS fr
          ON fr.id = src.file_receipt_id
        WHERE src.exam_export_case_id = %s
          AND src.source_status = 'ACTIVE'
          AND eiv.namecode IS NOT NULL
        ORDER BY eiv.namecode, eiv.occurrence_no, src.source_priority, eiv.id
        """,
        (exam_export_case_id,),
    )
    candidates_by_item: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for row in cur.fetchall():
        item = dict(row)
        key = (str(item.get("namecode") or ""), int(item.get("occurrence_no") or 1))
        candidates_by_item.setdefault(key, []).append(item)

    def compact_value(row: Mapping[str, Any] | None) -> str:
        if not row:
            return "-"
        for key in ("normalized_value", "code_display", "code_value", "nullflavor"):
            value = row.get(key)
            if value not in (None, ""):
                return str(value)
        return "-"

    values: list[dict[str, Any]] = []
    for adopted in adopted_rows:
        key = (str(adopted.get("namecode") or ""), int(adopted.get("occurrence_no") or 1))
        candidates = candidates_by_item.get(key, [])
        adopted_source_item_id = adopted.get("source_exam_item_value_id")
        if adopted_source_item_id is not None and not any(
            str(candidate.get("source_exam_item_value_id") or "") == str(adopted_source_item_id)
            for candidate in candidates
        ):
            adopted_candidate = dict(adopted)
            adopted_candidate["source_role"] = adopted.get("adopted_source_role")
            adopted_candidate["source_type"] = adopted.get("adopted_source_type")
            adopted_candidate["source_exam_ledger_id"] = adopted.get("source_exam_ledger_id")
            candidates = [adopted_candidate, *candidates]

        source_slots: list[dict[str, Any]] = []
        if source_headers:
            for source in source_headers[:3]:
                source_exam_ledger_id = source.get("source_exam_ledger_id")
                matched = next(
                    (
                        candidate
                        for candidate in candidates
                        if str(candidate.get("source_exam_ledger_id") or "") == str(source_exam_ledger_id or "")
                    ),
                    None,
                )
                if matched:
                    source_slots.append(matched)
                else:
                    source_slots.append(
                        {
                            "source_role": source.get("source_role"),
                            "source_type": source.get("source_type"),
                            "source_exam_ledger_id": source_exam_ledger_id,
                            "source_file_name": source.get("file_name")
                            or source.get("xml_file_name")
                            or source.get("mapping_version"),
                            "source_relative_path": source.get("relative_path"),
                        }
                    )
        else:
            source_slots = candidates[:3]
            while len(source_slots) < 3:
                source_slots.append({})

        for index, candidate in enumerate(source_slots, start=1):
            candidate["slot_no"] = index
            candidate["value_text"] = compact_value(candidate)
            candidate["is_adopted"] = bool(
                adopted_source_item_id is not None
                and str(candidate.get("source_exam_item_value_id") or "") == str(adopted_source_item_id)
            )
        adopted["source_slots"] = source_slots
        adopted["candidate_values_text"] = " ".join(
            str(candidate.get("value_text") or "") for candidate in source_slots if candidate
        )
        adopted["adopted_value_text"] = compact_value(adopted)
        values.append(adopted)
    return values


def load_exam_export_case_placeholders(cur: Any, *, exam_export_case_id: int) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          cri.exam_case_check_review_item_id AS id,
          cri.event_id,
          'EXPORT_CASE' AS ledger_type,
          cri.exam_export_case_id AS ledger_id,
          cri.check_scope,
          cri.check_item_code AS namecode,
          cri.check_item_name AS namecode_display_name,
          COALESCE(cri.check_item_name, cri.check_item_code) AS item_name,
          1 AS occurrence_no,
          cri.raw_value_type,
          NULL AS normalize_status,
          CONCAT(cri.check_scope, '_MISSING_PLACEHOLDER') AS normalize_reason,
          'INVALID' AS validation_status,
          cri.validation_reason,
          'MISSING_PLACEHOLDER' AS value_source_role,
          cri.review_status,
          cri.review_note,
          cri.reviewed_at,
          cri.reviewed_by_app_user_id,
          latest_audit.note AS latest_note,
          latest_audit.changed_at AS latest_note_at
        FROM {qname(health_db())}.exam_case_check_review_items AS cri
        LEFT JOIN (
          SELECT a1.*
          FROM {qname(health_db())}.exam_case_check_review_item_audit_logs AS a1
          INNER JOIN (
            SELECT
              exam_case_check_review_item_id,
              MAX(exam_case_check_review_item_audit_log_id) AS max_id
            FROM {qname(health_db())}.exam_case_check_review_item_audit_logs
            GROUP BY exam_case_check_review_item_id
          ) AS latest
            ON latest.max_id = a1.exam_case_check_review_item_audit_log_id
        ) AS latest_audit
          ON latest_audit.exam_case_check_review_item_id = cri.exam_case_check_review_item_id
        WHERE cri.exam_export_case_id = %s
        ORDER BY
          CASE cri.review_status
            WHEN 'NEEDS_CONFIRMATION' THEN 0
            WHEN 'WAITING_RESUBMISSION' THEN 1
            WHEN 'NONE' THEN 2
            WHEN 'APPROVED_WITH_REASON' THEN 3
            WHEN 'RESUBMITTED' THEN 4
            ELSE 5
          END,
          cri.check_scope,
          cri.check_item_code,
          cri.exam_case_check_review_item_id
        """,
        (exam_export_case_id,),
    )
    rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        row["related_namecodes"] = load_placeholder_related_namecodes(
            cur,
            exam_export_case_id=exam_export_case_id,
            placeholder=row,
        )
    return rows


def load_placeholder_related_namecodes(
    cur: Any,
    *,
    exam_export_case_id: int,
    placeholder: Mapping[str, Any],
) -> list[dict[str, Any]]:
    validation_reason = str(placeholder.get("validation_reason") or "")
    match = re.match(r"^ARTICLE44:(?P<detail_no>44\d{8}):", validation_reason)
    if not match:
        return []
    detail_no = match.group("detail_no")
    cur.execute(
        f"""
        SELECT
          gm.namecode,
          COALESCE(eim.item_name, gm.namecode) AS item_name,
          gm.value_type,
          ecv.exam_export_case_value_id,
          ecv.adopted_source_role,
          ecv.source_exam_item_value_id,
          GROUP_CONCAT(
            DISTINCT CONCAT(
              ecs.source_role,
              '/',
              ecs.source_type,
              ':',
              COALESCE(eiv.normalize_status, '-'),
              '/',
              COALESCE(eiv.validation_status, '-')
            )
            ORDER BY ecs.source_priority, ecs.source_type
            SEPARATOR ', '
          ) AS source_states
        FROM {qname(dev_db())}.exam_item_group_members AS gm
        LEFT JOIN {qname(dev_db())}.exam_item_master AS eim
          ON eim.namecode = gm.namecode
        LEFT JOIN {qname(health_db())}.exam_export_case_values AS ecv
          ON ecv.exam_export_case_id = %s
         AND ecv.namecode = gm.namecode
        LEFT JOIN {qname(health_db())}.exam_export_case_sources AS ecs
          ON ecs.exam_export_case_id = %s
        LEFT JOIN {qname(health_db())}.exam_item_values AS eiv
          ON eiv.ledger_type = 'EXAM'
         AND eiv.ledger_id = ecs.source_exam_ledger_id
         AND eiv.namecode = gm.namecode
        WHERE gm.group_code = 'v2_2026_ARTICLE44_CHECK_ITEMS'
          AND gm.notes LIKE %s
        GROUP BY
          gm.namecode,
          eim.item_name,
          gm.value_type,
          ecv.exam_export_case_value_id,
          ecv.adopted_source_role,
          ecv.source_exam_item_value_id
        ORDER BY gm.priority, gm.namecode
        """,
        (exam_export_case_id, exam_export_case_id, f"%Article44 {detail_no}:%"),
    )
    return [dict(row) for row in cur.fetchall()]


def load_exam_export_case_check_rows(cur: Any, *, exam_export_case_id: int, limit: int = 10) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT *
        FROM {qname(health_db())}.exam_check_results
        WHERE ledger_type = 'EXPORT_CASE'
          AND exam_export_case_id = %s
        ORDER BY id DESC
        LIMIT %s
        """,
        (exam_export_case_id, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def _case_basic_info_display_value(case: Mapping[str, Any], field_code: str) -> str | None:
    if field_code == "report_codes":
        report_category = case.get("health_exam_report_category")
        program_code = case.get("program_code")
        if report_category in (None, "") and program_code in (None, ""):
            return None
        return f"{report_category or ''}|{program_code or ''}"
    if field_code == "exam_date":
        value = case.get("exam_date_export_value") or case.get("exam_date")
        return None if value in (None, "") else str(value)
    if field_code == "insurer_number":
        return case.get("insurer_number_export_value") or case.get("insurer_number")
    if field_code == "insurance_branch_number":
        return case.get("insurance_branch_number_export_value") or case.get("insurance_branch_number_raw")
    if field_code == "postal_code":
        return case.get("postal_code_completed_value") or case.get("postal_code")
    if field_code == "address":
        return case.get("address_completed_value") or case.get("address")
    config = BASIC_INFO_CORRECTION_FIELDS[field_code]
    value = case.get(str(config["case_value_column"]))
    return None if value in (None, "") else str(value)


def normalize_basic_info_correction_value(
    field_code: str,
    raw_value: str,
    *,
    case: Mapping[str, Any] | None = None,
) -> tuple[str | None, str, str | None]:
    value = raw_value.strip()
    if not value:
        return None, "ERROR", "VALUE_REQUIRED"
    if field_code == "report_codes":
        values = [part.strip() for part in value.split("|", 1)]
        if len(values) != 2 or not values[0] or not values[1]:
            return None, "ERROR", "REPORT_CATEGORY_AND_PROGRAM_CODE_REQUIRED"
        if not re.fullmatch(r"[0-9]{2}", values[0]):
            return None, "ERROR", "INVALID_REPORT_CATEGORY"
        if not re.fullmatch(r"[0-9]{3}", values[1]):
            return None, "ERROR", "INVALID_PROGRAM_CODE"
        return f"{values[0]}|{values[1]}", "OK", None
    if field_code == "exam_date":
        result = normalize_date_to_ymd_and_compact(value, purpose="exam_date")
        normalized = result.get("field_norm")
    elif field_code == "name_kana":
        result = normalize_name_kana_full(value)
        normalized = result.get("field_norm")
    elif field_code == "insurance_symbol":
        result = normalize_insurance_symbol(value)
        normalized = result.get("export")
    elif field_code == "insurance_number":
        result = normalize_insurance_number(value)
        normalized = result.get("field_norm")
    elif field_code == "insurance_branch_number":
        result = normalize_insurance_number(value)
        normalized = result.get("field_norm")
    elif field_code == "exam_ticket_number":
        issuer_insurer_number = None
        if case is not None:
            issuer_insurer_number = case.get("insurer_number_export_value") or case.get("insurer_number")
        result = normalize_ticket_identifier(
            value,
            ticket_kind="exam_ticket",
            issuer_insurer_number=issuer_insurer_number,
        )
        normalized = result.get("field_norm")
    elif field_code == "exam_ticket_expires_on":
        result = normalize_date_to_ymd_and_compact(value, purpose="exam_ticket_expires_on")
        normalized = result.get("field_norm")
    elif field_code == "insurer_number":
        result = normalize_insurer_number(value)
        normalized = result.get("field_norm")
        if result.get("ok") and normalized not in (None, ""):
            normalized = zero_pad(str(normalized), 8)
            if normalized is None or len(normalized) != 8:
                return None, "ERROR", "INVALID_INSURER_NUMBER_LENGTH"
    elif field_code == "postal_code":
        normalized = normalize_postal_code_export(value)
        if not normalized:
            return None, "ERROR", "INVALID_POSTAL_CODE"
        return normalized, "OK", None
    elif field_code == "address":
        normalized = normalize_address_export(value)
        if not normalized:
            return None, "ERROR", "INVALID_ADDRESS"
        return normalized, "OK", None
    else:
        return None, "ERROR", "UNKNOWN_FIELD"

    if not result.get("ok") or normalized in (None, ""):
        return None, "ERROR", str(result.get("reason") or "NORMALIZE_FAILED")
    return str(normalized), "OK", None


def load_exam_case_basic_info_corrections(cur: Any, *, exam_export_case_id: int) -> dict[str, dict[str, Any]]:
    cur.execute(
        f"""
        SELECT *
        FROM {qname(health_db())}.exam_case_basic_info_corrections
        WHERE exam_export_case_id = %s
        """,
        (exam_export_case_id,),
    )
    return {str(row["field_code"]): dict(row) for row in cur.fetchall()}


def build_basic_info_correction_rows(
    case: Mapping[str, Any],
    corrections: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for field_code, config in BASIC_INFO_CORRECTION_FIELDS.items():
        correction = dict(corrections.get(field_code) or {})
        current_value = _case_basic_info_display_value(case, field_code)
        rows.append(
            {
                "field_code": field_code,
                "label": config["label"],
                "current_value": current_value,
                "current_values": current_value.split("|", 1) if field_code == "report_codes" and current_value else [],
                "correction": correction,
                "has_active_correction": correction.get("correction_status") == "ACTIVE",
            }
        )
    return rows


def update_exam_case_basic_info_correction(
    cur: Any,
    *,
    exam_export_case_id: int,
    field_code: str,
    corrected_value: str,
    correction_reason: str,
    app_user_id: int,
) -> dict[str, Any] | None:
    if field_code not in BASIC_INFO_CORRECTION_FIELDS:
        return None
    cur.execute(
        f"""
        SELECT *
        FROM {qname(health_db())}.exam_export_cases
        WHERE exam_export_case_id = %s
        LIMIT 1
        """,
        (exam_export_case_id,),
    )
    case = cur.fetchone()
    if not case:
        return None
    case_row = dict(case)
    normalized_value, status, reason = normalize_basic_info_correction_value(
        field_code,
        corrected_value,
        case=case_row,
    )
    if status != "OK" or normalized_value is None:
        return {
            "ok": False,
            "reason": reason or "NORMALIZE_FAILED",
            "exam_export_case_id": exam_export_case_id,
            "field_code": field_code,
        }

    config = BASIC_INFO_CORRECTION_FIELDS[field_code]
    field_label = str(config["label"])
    source_value = _case_basic_info_display_value(case_row, field_code)
    cur.execute(
        f"""
        INSERT INTO {qname(health_db())}.exam_case_basic_info_corrections (
          event_id,
          exam_export_case_id,
          field_code,
          field_label,
          source_value,
          corrected_value,
          normalized_value,
          normalization_status,
          normalization_reason,
          correction_status,
          correction_reason,
          corrected_at,
          corrected_by_app_user_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, 'OK', NULL, 'ACTIVE', %s, CURRENT_TIMESTAMP(3), %s)
        ON DUPLICATE KEY UPDATE
          field_label = VALUES(field_label),
          corrected_value = VALUES(corrected_value),
          normalized_value = VALUES(normalized_value),
          normalization_status = VALUES(normalization_status),
          normalization_reason = VALUES(normalization_reason),
          correction_status = 'ACTIVE',
          correction_reason = VALUES(correction_reason),
          corrected_at = CURRENT_TIMESTAMP(3),
          corrected_by_app_user_id = VALUES(corrected_by_app_user_id)
        """,
        (
            case_row.get("event_id"),
            exam_export_case_id,
            field_code,
            field_label,
            source_value,
            corrected_value,
            normalized_value,
            correction_reason,
            app_user_id,
        ),
    )
    cur.execute(
        f"""
        SELECT exam_case_basic_info_correction_id
        FROM {qname(health_db())}.exam_case_basic_info_corrections
        WHERE exam_export_case_id = %s
          AND field_code = %s
        LIMIT 1
        """,
        (exam_export_case_id, field_code),
    )
    correction = cur.fetchone()
    if not correction:
        return None
    correction_id = int(correction["exam_case_basic_info_correction_id"])

    if config.get("composite"):
        primary_value, secondary_value = normalized_value.split("|", 1)
        update_columns = [
            f"`{config['case_value_column']}` = %s",
            f"`{config['case_secondary_value_column']}` = %s",
            "`correction_status` = 'CORRECTED'",
        ]
        update_params: list[Any] = [primary_value, secondary_value]
    else:
        update_columns = [f"`{config['case_value_column']}` = %s", "`correction_status` = 'CORRECTED'"]
        update_params = [normalized_value]
    if config.get("case_source_column"):
        update_columns.append(f"`{config['case_source_column']}` = 'MANUAL_CORRECTION'")
    if config.get("case_reason_column"):
        update_columns.append(f"`{config['case_reason_column']}` = %s")
        update_params.append(correction_reason or "MANUAL_CORRECTION")
    if field_code == "address":
        update_columns.append("`address_completion_status` = 'MANUAL_CORRECTION'")
    elif field_code == "postal_code":
        update_columns.append("`address_completion_status` = 'MANUAL_CORRECTION'")
    update_params.append(exam_export_case_id)
    cur.execute(
        f"""
        UPDATE {qname(health_db())}.exam_export_cases
        SET {", ".join(update_columns)}
        WHERE exam_export_case_id = %s
        """,
        tuple(update_params),
    )
    cur.execute(
        f"""
        INSERT INTO {qname(health_db())}.exam_case_basic_info_correction_audit_logs (
          exam_case_basic_info_correction_id,
          event_id,
          exam_export_case_id,
          field_code,
          field_name,
          old_value,
          new_value,
          source,
          note,
          changed_by_app_user_id
        )
        VALUES (%s, %s, %s, %s, 'normalized_value', %s, %s, 'ADMIN_UI', %s, %s)
        """,
        (
            correction_id,
            case_row.get("event_id"),
            exam_export_case_id,
            field_code,
            source_value,
            normalized_value,
            correction_reason,
            app_user_id,
        ),
    )
    return {
        "ok": True,
        "exam_export_case_id": exam_export_case_id,
        "field_code": field_code,
        "field_label": field_label,
        "old_value": source_value,
        "new_value": normalized_value,
    }


def clear_exam_case_basic_info_correction(
    cur: Any,
    *,
    exam_export_case_id: int,
    field_code: str,
    note: str,
    app_user_id: int,
) -> dict[str, Any] | None:
    if field_code not in BASIC_INFO_CORRECTION_FIELDS:
        return None
    cur.execute(
        f"""
        SELECT *
        FROM {qname(health_db())}.exam_case_basic_info_corrections
        WHERE exam_export_case_id = %s
          AND field_code = %s
        LIMIT 1
        """,
        (exam_export_case_id, field_code),
    )
    correction = cur.fetchone()
    if not correction:
        return None
    item = dict(correction)
    config = BASIC_INFO_CORRECTION_FIELDS[field_code]
    restore_value = item.get("source_value")
    if config.get("composite"):
        restore_values = str(restore_value or "|").split("|", 1)
        update_columns = [
            f"`{config['case_value_column']}` = %s",
            f"`{config['case_secondary_value_column']}` = %s",
        ]
        update_params: list[Any] = [restore_values[0] or None, restore_values[1] or None]
    else:
        update_columns = [f"`{config['case_value_column']}` = %s"]
        update_params = [restore_value]
    if config.get("case_source_column"):
        update_columns.append(f"`{config['case_source_column']}` = NULL")
    if config.get("case_reason_column"):
        update_columns.append(f"`{config['case_reason_column']}` = NULL")
    if field_code in {"postal_code", "address"}:
        update_columns.append("`address_completion_status` = NULL")
    update_params.append(exam_export_case_id)
    cur.execute(
        f"""
        UPDATE {qname(health_db())}.exam_export_cases
        SET {", ".join(update_columns)}
        WHERE exam_export_case_id = %s
        """,
        tuple(update_params),
    )
    cur.execute(
        f"""
        UPDATE {qname(health_db())}.exam_case_basic_info_corrections
        SET correction_status = 'CLEARED',
            correction_reason = %s,
            corrected_at = CURRENT_TIMESTAMP(3),
            corrected_by_app_user_id = %s
        WHERE exam_case_basic_info_correction_id = %s
        """,
        (note or "補正解除", app_user_id, item["exam_case_basic_info_correction_id"]),
    )
    cur.execute(
        f"""
        SELECT COUNT(*) AS active_count
        FROM {qname(health_db())}.exam_case_basic_info_corrections
        WHERE exam_export_case_id = %s
          AND correction_status = 'ACTIVE'
        """,
        (exam_export_case_id,),
    )
    active_count_row = cur.fetchone() or {}
    if int(active_count_row.get("active_count") or 0) == 0:
        cur.execute(
            f"""
            UPDATE {qname(health_db())}.exam_export_cases
            SET correction_status = 'NONE'
            WHERE exam_export_case_id = %s
            """,
            (exam_export_case_id,),
        )
    cur.execute(
        f"""
        INSERT INTO {qname(health_db())}.exam_case_basic_info_correction_audit_logs (
          exam_case_basic_info_correction_id,
          event_id,
          exam_export_case_id,
          field_code,
          field_name,
          old_value,
          new_value,
          source,
          note,
          changed_by_app_user_id
        )
        VALUES (%s, %s, %s, %s, 'correction_status', %s, 'CLEARED', 'ADMIN_UI', %s, %s)
        """,
        (
            item["exam_case_basic_info_correction_id"],
            item.get("event_id"),
            exam_export_case_id,
            field_code,
            item.get("correction_status"),
            note or "補正解除",
            app_user_id,
        ),
    )
    return item


def update_exam_case_check_review(
    cur: Any,
    *,
    review_item_id: int,
    review_status: str,
    note: str,
    app_user_id: int,
) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT
          exam_case_check_review_item_id,
          event_id,
          exam_export_case_id,
          check_scope,
          check_item_code,
          review_status
        FROM {qname(health_db())}.exam_case_check_review_items
        WHERE exam_case_check_review_item_id = %s
        LIMIT 1
        """,
        (review_item_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    item = dict(row)
    old_status = str(item.get("review_status") or "NONE")
    cur.execute(
        f"""
        UPDATE {qname(health_db())}.exam_case_check_review_items
        SET review_status = %s,
            review_note = NULLIF(%s, ''),
            reviewed_at = CURRENT_TIMESTAMP(3),
            reviewed_by_app_user_id = %s
        WHERE exam_case_check_review_item_id = %s
        """,
        (review_status, note, app_user_id, review_item_id),
    )
    cur.execute(
        f"""
        INSERT INTO {qname(health_db())}.exam_case_check_review_item_audit_logs (
          exam_case_check_review_item_id,
          event_id,
          exam_export_case_id,
          check_scope,
          check_item_code,
          field_name,
          old_value,
          new_value,
          source,
          note,
          changed_by_app_user_id
        )
        VALUES (%s, %s, %s, %s, %s, 'review_status', %s, %s, 'ADMIN_UI', %s, %s)
        """,
        (
            review_item_id,
            item.get("event_id"),
            item.get("exam_export_case_id"),
            item.get("check_scope"),
            item.get("check_item_code"),
            old_status,
            review_status,
            note,
            app_user_id,
        ),
    )
    item["old_review_status"] = old_status
    item["new_review_status"] = review_status
    return item


def load_exam_case_check_review_item_ids(
    cur: Any,
    *,
    exam_export_case_id: int,
    target_scope: str,
) -> list[int]:
    if target_scope == "all":
        status_clause = ""
        params: tuple[Any, ...] = (exam_export_case_id,)
    else:
        status_clause = """
          AND review_status IN ('NONE', 'NEEDS_CONFIRMATION', 'WAITING_RESUBMISSION')
        """
        params = (exam_export_case_id,)
    cur.execute(
        f"""
        SELECT exam_case_check_review_item_id
        FROM {qname(health_db())}.exam_case_check_review_items
        WHERE exam_export_case_id = %s
        {status_clause}
        ORDER BY exam_case_check_review_item_id
        """,
        params,
    )
    return [int(row["exam_case_check_review_item_id"]) for row in cur.fetchall()]


def summarize_exam_export_cases(rows: list[dict[str, Any]]) -> dict[str, int]:
    summary = {
        "total": len(rows),
        "ready": 0,
        "approved_with_reason": 0,
        "blocked": 0,
        "waiting": 0,
        "exported": 0,
        "legal_ng": 0,
        "specific_ng": 0,
        "multi_source": 0,
        "duplicate_subscriber": 0,
    }
    for row in rows:
        readiness = str(row.get("export_readiness_status") or "")
        if readiness == "EXPORT_READY":
            summary["ready"] += 1
        elif readiness == "APPROVED_WITH_REASON":
            summary["approved_with_reason"] += 1
        elif readiness == "BLOCKED":
            summary["blocked"] += 1
        elif readiness:
            summary["waiting"] += 1
        if str(row.get("xml_export_status") or "") == "EXPORTED":
            summary["exported"] += 1
        if str(row.get("legal_check_result") or "") == "NG":
            summary["legal_ng"] += 1
        if normalize_specific_check_result(row.get("specific_check_result"), row.get("specific_reason_summary")) == "NG":
            summary["specific_ng"] += 1
        if int(row.get("source_count") or 0) >= 2:
            summary["multi_source"] += 1
        if int(row.get("subscriber_case_count") or 0) >= 2:
            summary["duplicate_subscriber"] += 1
    return summary


def pct_label(part: Any, total: Any) -> str:
    total_num = int(total or 0)
    if total_num <= 0:
        return "-"
    return f"{(int(part or 0) / total_num) * 100:.1f}%"


def _facility_key(row: Mapping[str, Any]) -> str | None:
    facility_id = row.get("exam_facility_id")
    if facility_id is not None:
        return f"id:{facility_id}"
    facility_code = str(row.get("facility_code") or "").strip()
    if facility_code:
        return f"code:{facility_code}"
    facility_name = str(row.get("facility_name") or "").strip()
    if facility_name:
        return f"name:{facility_name}"
    return None


def _ensure_facility_summary_row(rows: dict[str, dict[str, Any]], source: Mapping[str, Any]) -> dict[str, Any] | None:
    key = _facility_key(source)
    if key is None:
        return None
    row = rows.setdefault(
        key,
        {
            "event_id": source.get("event_id"),
            "exam_facility_id": source.get("exam_facility_id"),
            "facility_code": source.get("facility_code"),
            "facility_name": source.get("facility_name"),
            "expected_source_mode": source.get("expected_source_mode"),
            "expected_source_mode_label": source_mode_label(source.get("expected_source_mode")),
            "file_total": 0,
            "xml_file_count": 0,
            "csv_file_count": 0,
            "zip_file_count": 0,
            "file_imported_count": 0,
            "file_waiting_count": 0,
            "source_count": 0,
            "source_ok_count": 0,
            "source_ng_count": 0,
            "source_pending_count": 0,
            "source_error_count": 0,
            "manual_source_count": 0,
            "subscriber_match_issue_count": 0,
            "case_count": 0,
            "case_ready_count": 0,
            "case_blocked_count": 0,
            "case_approved_count": 0,
            "case_exported_count": 0,
            "legal_ok_count": 0,
            "legal_ng_count": 0,
            "legal_pending_count": 0,
            "specific_ok_count": 0,
            "specific_ng_count": 0,
            "specific_not_applicable_count": 0,
            "specific_pending_count": 0,
            "item_error_count": 0,
            "top_error_items": [],
        },
    )
    for field in ("event_id", "exam_facility_id", "facility_code", "facility_name"):
        if row.get(field) in (None, "") and source.get(field) not in (None, ""):
            row[field] = source.get(field)
    if (
        not source_mode_is_configured(row.get("expected_source_mode"))
        and source_mode_is_configured(source.get("expected_source_mode"))
    ):
        row["expected_source_mode"] = source.get("expected_source_mode")
    row["expected_source_mode_label"] = source_mode_label(row.get("expected_source_mode"))
    return row


def load_facility_summary_rows(cur: Any, *, filters: dict[str, str], limit: int = 200) -> list[dict[str, Any]]:
    event_id = str(filters.get("event_id") or "").strip()
    query = str(filters.get("q") or "").strip()
    exam_months = split_filter_values(filters.get("exam_month", ""))
    fr_where_parts: list[str] = []
    fr_params: list[Any] = []
    el_where_parts: list[str] = []
    el_params: list[Any] = []
    eec_where_parts: list[str] = []
    eec_params: list[Any] = []
    eiv_where_parts = ["(eiv.normalize_status = 'ERROR' OR eiv.validation_status = 'INVALID')"]
    eiv_params: list[Any] = []
    if event_id:
        fr_where_parts.append("fr.event_id = %s")
        fr_params.append(event_id)
        el_where_parts.append("el.event_id = %s")
        el_params.append(event_id)
        eec_where_parts.append("eec.event_id = %s")
        eec_params.append(event_id)
        eiv_where_parts.append("el.event_id = %s")
        eiv_params.append(event_id)
    if exam_months:
        month_placeholders = ", ".join(["%s"] * len(exam_months))
        fr_where_parts.append(
            f"""
            EXISTS (
              SELECT 1
              FROM {qname(health_db())}.exam_ledgers AS fr_el
              WHERE fr_el.file_receipt_id = fr.id
                AND DATE_FORMAT(fr_el.exam_date, '%Y-%m') IN ({month_placeholders})
            )
            """
        )
        fr_params.extend(exam_months)
        el_where_parts.append(f"DATE_FORMAT(el.exam_date, '%Y-%m') IN ({month_placeholders})")
        el_params.extend(exam_months)
        eec_where_parts.append(f"DATE_FORMAT(eec.exam_date, '%Y-%m') IN ({month_placeholders})")
        eec_params.extend(exam_months)
        eiv_where_parts.append(f"DATE_FORMAT(el.exam_date, '%Y-%m') IN ({month_placeholders})")
        eiv_params.extend(exam_months)
    fr_event_clause = f"WHERE {' AND '.join(fr_where_parts)}" if fr_where_parts else ""
    el_event_clause = f"WHERE {' AND '.join(el_where_parts)}" if el_where_parts else ""
    eec_event_clause = f"WHERE {' AND '.join(eec_where_parts)}" if eec_where_parts else ""
    eiv_where_clause = f"WHERE {' AND '.join(eiv_where_parts)}"
    rows: dict[str, dict[str, Any]] = {}

    cur.execute(
        f"""
        SELECT
          fr.event_id,
          fr.exam_facility_id,
          fr.facility_code,
          fr.facility_name,
          mfa.expected_source_mode,
          COUNT(*) AS file_total,
          SUM(CASE WHEN fr.file_type = 'XML' THEN 1 ELSE 0 END) AS xml_file_count,
          SUM(CASE WHEN fr.file_type = 'CSV' THEN 1 ELSE 0 END) AS csv_file_count,
          SUM(CASE WHEN fr.file_type = 'ZIP' THEN 1 ELSE 0 END) AS zip_file_count,
          SUM(CASE WHEN fr.status = 'IMPORTED' THEN 1 ELSE 0 END) AS file_imported_count,
          SUM(CASE WHEN fr.status = 'WAITING_CONFIRM' THEN 1 ELSE 0 END) AS file_waiting_count
        FROM {qname(health_db())}.file_receipts AS fr
        LEFT JOIN (
{alias_source_mode_by_facility_code_sql()}
        ) AS mfa
          ON mfa.event_id = fr.event_id
         AND mfa.facility_code = fr.facility_code
        {fr_event_clause}
        GROUP BY
          fr.event_id,
          fr.exam_facility_id,
          fr.facility_code,
          fr.facility_name,
          mfa.expected_source_mode
        """,
        tuple(fr_params),
    )
    for source in (dict(row) for row in cur.fetchall()):
        item = _ensure_facility_summary_row(rows, source)
        if item is None:
            continue
        for field in (
            "file_total",
            "xml_file_count",
            "csv_file_count",
            "zip_file_count",
            "file_imported_count",
            "file_waiting_count",
        ):
            item[field] = int(source.get(field) or 0)

    cur.execute(
        f"""
        SELECT
          el.event_id,
          el.exam_facility_id,
          el.facility_code,
          el.facility_name,
          COUNT(*) AS source_count,
          SUM(CASE WHEN el.check_status = 'OK' THEN 1 ELSE 0 END) AS source_ok_count,
          SUM(CASE WHEN el.check_status = 'NG' THEN 1 ELSE 0 END) AS source_ng_count,
          SUM(CASE WHEN el.check_status NOT IN ('OK', 'NG') OR el.check_status IS NULL THEN 1 ELSE 0 END) AS source_pending_count,
          SUM(COALESCE(el.exam_item_error_count, 0)) AS source_error_count,
          SUM(CASE WHEN el.source_type IN ('PAPER', 'MANUAL') THEN 1 ELSE 0 END) AS manual_source_count,
          SUM(
            CASE
              WHEN el.subscriber_match_status = 'MATCHED'
               AND el.subscriber_match_method = 'identity_hash'
                THEN 0
              ELSE 1
            END
          ) AS subscriber_match_issue_count
        FROM {qname(health_db())}.exam_ledgers AS el
        {el_event_clause}
        GROUP BY el.event_id, el.exam_facility_id, el.facility_code, el.facility_name
        """,
        tuple(el_params),
    )
    for source in (dict(row) for row in cur.fetchall()):
        item = _ensure_facility_summary_row(rows, source)
        if item is None:
            continue
        for field in (
            "source_count",
            "source_ok_count",
            "source_ng_count",
            "source_pending_count",
            "source_error_count",
            "manual_source_count",
            "subscriber_match_issue_count",
        ):
            item[field] = int(source.get(field) or 0)

    cur.execute(
        f"""
        SELECT
          eec.event_id,
          eec.exam_facility_id,
          eec.facility_code,
          eec.facility_name,
          COUNT(*) AS case_count,
          SUM(CASE WHEN eec.export_readiness_status = 'EXPORT_READY' THEN 1 ELSE 0 END) AS case_ready_count,
          SUM(CASE WHEN eec.export_readiness_status = 'BLOCKED' THEN 1 ELSE 0 END) AS case_blocked_count,
          SUM(CASE WHEN eec.export_readiness_status = 'APPROVED_WITH_REASON' THEN 1 ELSE 0 END) AS case_approved_count,
          SUM(CASE WHEN eec.xml_export_status = 'EXPORTED' THEN 1 ELSE 0 END) AS case_exported_count,
          SUM(CASE WHEN COALESCE(ecr.legal_check_result, 'PENDING') = 'OK' THEN 1 ELSE 0 END) AS legal_ok_count,
          SUM(CASE WHEN COALESCE(ecr.legal_check_result, 'PENDING') = 'NG' THEN 1 ELSE 0 END) AS legal_ng_count,
          SUM(CASE WHEN COALESCE(ecr.legal_check_result, 'PENDING') NOT IN ('OK', 'NG') THEN 1 ELSE 0 END) AS legal_pending_count,
          SUM(CASE WHEN ({specific_check_result_sql('ecr')}) = 'OK' THEN 1 ELSE 0 END) AS specific_ok_count,
          SUM(CASE WHEN ({specific_check_result_sql('ecr')}) = 'NG' THEN 1 ELSE 0 END) AS specific_ng_count,
          SUM(CASE WHEN ({specific_check_result_sql('ecr')}) = 'NOT_APPLICABLE' THEN 1 ELSE 0 END) AS specific_not_applicable_count,
          SUM(CASE WHEN ({specific_check_result_sql('ecr')}) IN ('PENDING', 'UNDETERMINABLE') THEN 1 ELSE 0 END) AS specific_pending_count
        FROM {qname(health_db())}.exam_export_cases AS eec
        LEFT JOIN (
          SELECT r1.*
          FROM {qname(health_db())}.exam_check_results AS r1
          INNER JOIN (
            SELECT exam_export_case_id, MAX(id) AS max_id
            FROM {qname(health_db())}.exam_check_results
            WHERE ledger_type = 'EXPORT_CASE'
              AND exam_export_case_id IS NOT NULL
            GROUP BY exam_export_case_id
          ) AS latest
            ON latest.max_id = r1.id
        ) AS ecr
          ON ecr.exam_export_case_id = eec.exam_export_case_id
        {eec_event_clause}
        GROUP BY eec.event_id, eec.exam_facility_id, eec.facility_code, eec.facility_name
        """,
        tuple(eec_params),
    )
    for source in (dict(row) for row in cur.fetchall()):
        item = _ensure_facility_summary_row(rows, source)
        if item is None:
            continue
        for field in (
            "case_count",
            "case_ready_count",
            "case_blocked_count",
            "case_approved_count",
            "case_exported_count",
            "legal_ok_count",
            "legal_ng_count",
            "legal_pending_count",
            "specific_ok_count",
            "specific_ng_count",
            "specific_not_applicable_count",
            "specific_pending_count",
        ):
            item[field] = int(source.get(field) or 0)

    cur.execute(
        f"""
        SELECT
          el.event_id,
          el.exam_facility_id,
          el.facility_code,
          el.facility_name,
          eiv.namecode,
          COALESCE(eiv.namecode_display_name, eiv.namecode) AS item_name,
          COUNT(*) AS error_count
        FROM {qname(health_db())}.exam_item_values AS eiv
        INNER JOIN {qname(health_db())}.exam_ledgers AS el
          ON el.exam_ledger_id = eiv.ledger_id
         AND eiv.ledger_type = 'EXAM'
        {eiv_where_clause}
        GROUP BY
          el.event_id,
          el.exam_facility_id,
          el.facility_code,
          el.facility_name,
          eiv.namecode,
          COALESCE(eiv.namecode_display_name, eiv.namecode)
        ORDER BY error_count DESC, eiv.namecode
        """,
        tuple(eiv_params),
    )
    error_total_by_key: dict[str, int] = {}
    for source in (dict(row) for row in cur.fetchall()):
        item = _ensure_facility_summary_row(rows, source)
        if item is None:
            continue
        key = _facility_key(item)
        if key is None:
            continue
        count = int(source.get("error_count") or 0)
        error_total_by_key[key] = error_total_by_key.get(key, 0) + count
        if len(item["top_error_items"]) < 5:
            item["top_error_items"].append(
                {
                    "namecode": source.get("namecode"),
                    "item_name": source.get("item_name"),
                    "error_count": count,
                }
            )
    for key, count in error_total_by_key.items():
        if key in rows:
            rows[key]["item_error_count"] = count

    result = list(rows.values())
    if query:
        q = query.lower()
        result = [
            row for row in result
            if q in str(row.get("facility_code") or "").lower()
            or q in str(row.get("facility_name") or "").lower()
            or q in str(row.get("expected_source_mode") or "").lower()
            or q in str(row.get("expected_source_mode_label") or "").lower()
        ]
    for row in result:
        row["legal_ng_rate"] = pct_label(row.get("legal_ng_count"), row.get("case_count"))
        row["specific_subject_count"] = int(row.get("specific_ok_count") or 0) + int(row.get("specific_ng_count") or 0)
        row["specific_ng_rate"] = pct_label(row.get("specific_ng_count"), row.get("specific_subject_count"))
        row["source_ng_rate"] = pct_label(row.get("source_ng_count"), row.get("source_count"))
        row["case_ng_rate"] = pct_label(row.get("case_blocked_count"), row.get("case_count"))
        row["receipt_source_mode_label"] = receipt_source_mode_label(
            int(row.get("xml_file_count") or 0) + int(row.get("zip_file_count") or 0),
            row.get("csv_file_count"),
        )
        if source_mode_is_configured(row.get("expected_source_mode")):
            row["facility_source_mode_summary"] = f"想定 {row.get('expected_source_mode_label')}"
        else:
            row["facility_source_mode_summary"] = f"想定未設定 / 実績 {row.get('receipt_source_mode_label')}"
        row["facility_filter_value"] = row.get("facility_code") or row.get("facility_name") or ""
        row["risk_score"] = (
            int(row.get("legal_ng_count") or 0) * 5
            + int(row.get("specific_ng_count") or 0) * 4
            + int(row.get("source_ng_count") or 0) * 3
            + int(row.get("item_error_count") or 0)
        )
    result.sort(
        key=lambda row: (
            int(row.get("risk_score") or 0),
            int(row.get("case_count") or 0),
            int(row.get("source_count") or 0),
            int(row.get("file_total") or 0),
        ),
        reverse=True,
    )
    return result[:limit]


HEARING_JUDGEMENT_ITEMS = {
    "9D100163100000011": "右 1000Hz",
    "9D100163500000011": "左 1000Hz",
    "9D100163200000011": "右 4000Hz",
    "9D100163600000011": "左 4000Hz",
}


def build_hearing_judgement_summary(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    item_rows: dict[str, dict[str, Any]] = {
        namecode: {
            "item_label": item_label,
            "namecode": namecode,
            "normal_count": 0,
            "abnormal_count": 0,
            "unknown_count": 0,
            "total_count": 0,
            "abnormal_rate": None,
        }
        for namecode, item_label in HEARING_JUDGEMENT_ITEMS.items()
    }
    ledger_states: dict[int, set[str]] = {}
    for source in rows:
        namecode = str(source.get("namecode") or "")
        item = item_rows.get(namecode)
        if item is None:
            continue
        code_value = str(source.get("code_value") or "").strip()
        if not code_value:
            continue
        ledger_id = int(source["exam_ledger_id"])
        state = "ABNORMAL" if code_value == "1" else "NORMAL" if code_value == "2" else "UNKNOWN"
        ledger_states.setdefault(ledger_id, set()).add(state)
        item["total_count"] += 1
        item[f"{state.lower()}_count"] += 1

    normal_ledgers = 0
    abnormal_ledgers = 0
    unknown_ledgers = 0
    for states in ledger_states.values():
        if "ABNORMAL" in states:
            abnormal_ledgers += 1
        elif "UNKNOWN" in states:
            unknown_ledgers += 1
        else:
            normal_ledgers += 1

    for item in item_rows.values():
        judged_count = item["normal_count"] + item["abnormal_count"]
        if judged_count:
            item["abnormal_rate"] = round(item["abnormal_count"] * 100 / judged_count, 1)

    judged_ledgers = normal_ledgers + abnormal_ledgers
    return {
        "available": bool(ledger_states),
        "ledger_total": len(ledger_states),
        "normal_ledger_count": normal_ledgers,
        "abnormal_ledger_count": abnormal_ledgers,
        "unknown_ledger_count": unknown_ledgers,
        "abnormal_ledger_rate": round(abnormal_ledgers * 100 / judged_ledgers, 1) if judged_ledgers else None,
        "item_rows": list(item_rows.values()),
    }


def load_facility_summary_detail(
    cur: Any,
    *,
    event_id: str,
    facility_code: str,
    exam_month: str = "",
) -> dict[str, Any]:
    event_id = str(event_id or "").strip()
    facility_code = str(facility_code or "").strip()
    exam_months = split_filter_values(exam_month)
    if not event_id or not facility_code:
        raise ValueError("イベントと健診機関コードは必須です。")

    month_clause = ""
    month_params: list[Any] = []
    if exam_months:
        placeholders = ", ".join(["%s"] * len(exam_months))
        month_clause = f" AND DATE_FORMAT(el.exam_date, '%Y-%m') IN ({placeholders})"
        month_params.extend(exam_months)

    cur.execute(
        f"""
        SELECT
          COALESCE(MAX(fr.facility_name), MAX(el.facility_name), MAX(ef.exam_facility_display_name), MAX(ef.exam_facility_name)) AS facility_name,
          COALESCE(MAX(fr.facility_code), MAX(el.facility_code), MAX(ef.exam_facility_code)) AS facility_code,
          MAX(mfa.expected_source_mode) AS expected_source_mode,
          MAX(ev.result_root_path) AS result_root_path
        FROM {qname(health_db())}.file_receipts AS fr
        LEFT JOIN {qname(health_db())}.exam_ledgers AS el
          ON el.file_receipt_id = fr.id
        LEFT JOIN {qname(master_db())}.exam_facilities AS ef
          ON ef.exam_facility_code = fr.facility_code
        LEFT JOIN {qname(dev_db())}.event AS ev
          ON ev.event_id = fr.event_id
        LEFT JOIN (
{alias_source_mode_by_facility_code_sql()}
        ) AS mfa
          ON mfa.event_id = fr.event_id
         AND mfa.facility_code = fr.facility_code
        WHERE fr.event_id = %s
          AND fr.facility_code = %s
        """,
        (event_id, facility_code),
    )
    header = dict(cur.fetchone() or {})
    header["facility_code"] = header.get("facility_code") or facility_code
    header["expected_source_mode_label"] = source_mode_label(header.get("expected_source_mode"))
    cur.execute(
        f"""
        SELECT
          mfa.alias_id,
          mfa.src_folder_raw,
          mfa.dst_folder_norm,
          mfa.expected_source_mode
        FROM {qname(master_db())}.medical_folder_aliases AS mfa
        LEFT JOIN {qname(master_db())}.exam_facilities AS ef
          ON ef.exam_facility_id = mfa.exam_facility_id
        WHERE mfa.event_id = %s
          AND mfa.is_active = 1
          AND COALESCE(ef.exam_facility_code, SUBSTRING_INDEX(mfa.src_folder_raw, '_', 1)) = %s
        ORDER BY mfa.updated_at DESC, mfa.alias_id DESC
        LIMIT 1
        """,
        (event_id, facility_code),
    )
    alias_header = dict(cur.fetchone() or {})
    header["alias_id"] = alias_header.get("alias_id")
    header["src_folder_raw"] = alias_header.get("src_folder_raw")
    header["dst_folder_norm"] = alias_header.get("dst_folder_norm")
    header["facility_folder_name"] = header.get("src_folder_raw") or header.get("dst_folder_norm")
    header["facility_folder_copy_value"] = join_display_path(
        header.get("result_root_path"),
        header.get("facility_folder_name"),
    )
    if not header.get("expected_source_mode") and alias_header.get("expected_source_mode"):
        header["expected_source_mode"] = alias_header.get("expected_source_mode")
        header["expected_source_mode_label"] = source_mode_label(header.get("expected_source_mode"))

    cur.execute(
        f"""
        SELECT
          COALESCE(DATE_FORMAT(el.exam_date, '%Y-%m'), '不明') AS exam_month,
          COUNT(DISTINCT fr.id) AS file_count,
          COUNT(DISTINCT el.exam_ledger_id) AS source_count,
          SUM(CASE WHEN el.check_status = 'OK' THEN 1 ELSE 0 END) AS source_ok_count,
          SUM(CASE WHEN el.check_status = 'NG' THEN 1 ELSE 0 END) AS source_ng_count,
          SUM(
            CASE
              WHEN el.subscriber_match_status = 'MATCHED'
               AND el.subscriber_match_method = 'identity_hash'
                THEN 0
              ELSE 1
            END
          ) AS subscriber_issue_count
        FROM {qname(health_db())}.file_receipts AS fr
        LEFT JOIN {qname(health_db())}.exam_ledgers AS el
          ON el.file_receipt_id = fr.id
        WHERE fr.event_id = %s
          AND fr.facility_code = %s
          {month_clause}
        GROUP BY COALESCE(DATE_FORMAT(el.exam_date, '%Y-%m'), '不明')
        ORDER BY exam_month
        """,
        (event_id, facility_code, *month_params),
    )
    monthly_rows = [dict(row) for row in cur.fetchall()]
    for row in monthly_rows:
        for field in ("file_count", "source_count", "source_ok_count", "source_ng_count", "subscriber_issue_count"):
            row[field] = int(row.get(field) or 0)

    cur.execute(
        f"""
        SELECT
          fr.id AS file_receipt_id,
          fr.file_type,
          fr.file_name,
          fr.relative_path,
          fr.status,
          COUNT(el.exam_ledger_id) AS source_count,
          SUM(CASE WHEN el.check_status = 'OK' THEN 1 ELSE 0 END) AS source_ok_count,
          SUM(CASE WHEN el.check_status = 'NG' THEN 1 ELSE 0 END) AS source_ng_count
        FROM {qname(health_db())}.file_receipts AS fr
        LEFT JOIN {qname(health_db())}.exam_ledgers AS el
          ON el.file_receipt_id = fr.id
        WHERE fr.event_id = %s
          AND fr.facility_code = %s
          {month_clause}
        GROUP BY fr.id, fr.file_type, fr.file_name, fr.relative_path, fr.status
        ORDER BY fr.first_seen_at DESC, fr.id DESC
        LIMIT 80
        """,
        (event_id, facility_code, *month_params),
    )
    file_rows = [dict(row) for row in cur.fetchall()]
    for row in file_rows:
        for field in ("source_count", "source_ok_count", "source_ng_count"):
            row[field] = int(row.get(field) or 0)

    cur.execute(
        f"""
        SELECT
          COALESCE(el.check_reason, '理由なし') AS reason,
          COUNT(*) AS cnt
        FROM {qname(health_db())}.exam_ledgers AS el
        WHERE el.event_id = %s
          AND el.facility_code = %s
          AND el.check_status = 'NG'
          AND el.source_type IN ('XML', 'CSV')
          {month_clause}
        GROUP BY COALESCE(el.check_reason, '理由なし')
        ORDER BY cnt DESC
        LIMIT 20
        """,
        (event_id, facility_code, *month_params),
    )
    source_ng_reasons = [dict(row) for row in cur.fetchall()]

    cur.execute(
        f"""
        SELECT
          COALESCE(ecr.legal_check_result, 'PENDING') AS legal_check_result,
          COALESCE(ecr.specific_check_result, 'PENDING') AS specific_check_result,
          COALESCE(ecr.legal_reason_summary, '') AS legal_reason_summary,
          COALESCE(ecr.specific_reason_summary, '') AS specific_reason_summary,
          COUNT(*) AS cnt
        FROM {qname(health_db())}.exam_export_cases AS eec
        LEFT JOIN (
          SELECT r1.*
          FROM {qname(health_db())}.exam_check_results AS r1
          INNER JOIN (
            SELECT exam_export_case_id, MAX(id) AS max_id
            FROM {qname(health_db())}.exam_check_results
            WHERE ledger_type = 'EXPORT_CASE'
              AND exam_export_case_id IS NOT NULL
            GROUP BY exam_export_case_id
          ) AS latest
            ON latest.max_id = r1.id
        ) AS ecr
          ON ecr.exam_export_case_id = eec.exam_export_case_id
        WHERE eec.event_id = %s
          AND eec.facility_code = %s
          {month_clause.replace('el.exam_date', 'eec.exam_date')}
        GROUP BY
          COALESCE(ecr.legal_check_result, 'PENDING'),
          COALESCE(ecr.specific_check_result, 'PENDING'),
          COALESCE(ecr.legal_reason_summary, ''),
          COALESCE(ecr.specific_reason_summary, '')
        ORDER BY cnt DESC
        LIMIT 30
        """,
        (event_id, facility_code, *month_params),
    )
    case_check_rows = [dict(row) for row in cur.fetchall()]

    cur.execute(
        f"""
        SELECT
          cri.check_item_code AS namecode,
          COALESCE(NULLIF(cri.check_item_name, ''), cri.check_item_code) AS item_name,
          COALESCE(NULLIF(cri.validation_reason, ''), '理由なし') AS validation_reason,
          COUNT(DISTINCT cri.exam_export_case_id) AS case_count,
          SUM(CASE WHEN cri.review_status = 'NEEDS_CONFIRMATION' THEN 1 ELSE 0 END) AS needs_confirmation_count,
          SUM(CASE WHEN cri.review_status = 'WAITING_RESUBMISSION' THEN 1 ELSE 0 END) AS waiting_resubmission_count,
          SUM(CASE WHEN cri.review_status = 'APPROVED_WITH_REASON' THEN 1 ELSE 0 END) AS approved_count,
          SUM(CASE WHEN cri.review_status = 'EXCLUDED' THEN 1 ELSE 0 END) AS excluded_count
        FROM {qname(health_db())}.exam_case_check_review_items AS cri
        INNER JOIN {qname(health_db())}.exam_export_cases AS eec
          ON eec.exam_export_case_id = cri.exam_export_case_id
        WHERE eec.event_id = %s
          AND eec.facility_code = %s
          AND cri.check_scope = 'SPECIFIC_HEALTH'
          AND cri.review_status <> 'RESOLVED_BY_SOURCE_VALUE'
          {month_clause.replace('el.exam_date', 'eec.exam_date')}
        GROUP BY
          cri.check_item_code,
          COALESCE(NULLIF(cri.check_item_name, ''), cri.check_item_code),
          COALESCE(NULLIF(cri.validation_reason, ''), '理由なし')
        ORDER BY case_count DESC, cri.check_item_code
        LIMIT 100
        """,
        (event_id, facility_code, *month_params),
    )
    specific_ng_item_rows = [dict(row) for row in cur.fetchall()]
    for row in specific_ng_item_rows:
        for field in (
            "case_count",
            "needs_confirmation_count",
            "waiting_resubmission_count",
            "approved_count",
            "excluded_count",
        ):
            row[field] = int(row.get(field) or 0)

    cur.execute(
        f"""
        SELECT
          eiv.namecode,
          COALESCE(eiv.namecode_display_name, eiv.namecode) AS item_name,
          eiv.normalize_status,
          eiv.validation_status,
          eiv.normalize_reason,
          eiv.validation_reason,
          COUNT(*) AS cnt
        FROM {qname(health_db())}.exam_item_values AS eiv
        INNER JOIN {qname(health_db())}.exam_ledgers AS el
          ON el.exam_ledger_id = eiv.ledger_id
         AND eiv.ledger_type = 'EXAM'
        WHERE el.event_id = %s
          AND el.facility_code = %s
          AND (eiv.normalize_status = 'ERROR' OR eiv.validation_status = 'INVALID')
          {month_clause}
        GROUP BY
          eiv.namecode,
          COALESCE(eiv.namecode_display_name, eiv.namecode),
          eiv.normalize_status,
          eiv.validation_status,
          eiv.normalize_reason,
          eiv.validation_reason
        ORDER BY cnt DESC, eiv.namecode
        LIMIT 50
        """,
        (event_id, facility_code, *month_params),
    )
    item_error_rows = [dict(row) for row in cur.fetchall()]

    hearing_namecodes = tuple(HEARING_JUDGEMENT_ITEMS)
    hearing_namecode_placeholders = ", ".join(["%s"] * len(hearing_namecodes))
    cur.execute(
        f"""
        SELECT
          el.exam_ledger_id,
          eiv.namecode,
          eiv.code_value
        FROM {qname(health_db())}.exam_ledgers AS el
        INNER JOIN {qname(health_db())}.exam_item_values AS eiv
          ON eiv.ledger_id = el.exam_ledger_id
         AND eiv.ledger_type = 'EXAM'
         AND eiv.namecode IN ({hearing_namecode_placeholders})
        WHERE el.event_id = %s
          AND el.facility_code = %s
          AND el.source_type = 'CSV'
          {month_clause}
        ORDER BY el.exam_ledger_id, eiv.namecode
        """,
        (*hearing_namecodes, event_id, facility_code, *month_params),
    )
    hearing_summary = build_hearing_judgement_summary(dict(row) for row in cur.fetchall())

    return {
        "header": header,
        "monthly_rows": monthly_rows,
        "file_rows": file_rows,
        "source_ng_reasons": source_ng_reasons,
        "case_check_rows": case_check_rows,
        "specific_ng_item_rows": specific_ng_item_rows,
        "item_error_rows": item_error_rows,
        "hearing_summary": hearing_summary,
        "summary": {
            "file_total": sum(int(row.get("file_count") or 0) for row in monthly_rows),
            "source_total": sum(int(row.get("source_count") or 0) for row in monthly_rows),
            "source_ng_total": sum(int(row.get("source_ng_count") or 0) for row in monthly_rows),
            "subscriber_issue_total": sum(int(row.get("subscriber_issue_count") or 0) for row in monthly_rows),
            "item_error_total": sum(int(row.get("cnt") or 0) for row in item_error_rows),
        },
    }


def load_xml_export_list_detail(cur: Any, *, xml_export_list_id: int) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT *
        FROM {qname(health_db())}.ops_xml_export_lists
        WHERE xml_export_list_id = %s
        """,
        (xml_export_list_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def load_ops_xml_export_list_cases(cur: Any, *, xml_export_list_id: int) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          xelc.xml_export_list_case_id,
          xelc.xml_export_list_id,
          xelc.exam_export_case_id,
          xelc.list_case_status,
          xelc.export_readiness_status_snapshot,
          xelc.export_readiness_reason_snapshot,
          xelc.export_error_reason,
          xelc.added_by,
          xelc.added_at,
          xelc.exported_at,
          eec.hia_subscriber_id,
          eec.person_id_custom,
          eec.insurance_symbol_export_value AS insured_card_symbol,
          eec.insurance_number_export_value AS insured_card_number,
          eec.name_kana_export_value AS name_kana,
          eec.birthdate AS birth_date,
          eec.exam_date,
          eec.exam_facility_id,
          eec.export_readiness_status,
          eec.export_readiness_reason,
          eec.check_status,
          eec.check_reason,
          eec.xml_export_status,
          ef.exam_facility_code,
          ef.exam_facility_name
        FROM {qname(health_db())}.ops_xml_export_list_cases xelc
        INNER JOIN {qname(health_db())}.exam_export_cases eec
          ON eec.exam_export_case_id = xelc.exam_export_case_id
        LEFT JOIN {qname(master_db())}.exam_facilities ef
          ON ef.exam_facility_id = eec.exam_facility_id
        WHERE xelc.xml_export_list_id = %s
          AND xelc.removed_at IS NULL
        ORDER BY ef.exam_facility_code, eec.exam_date, eec.name_kana_export_value, xelc.xml_export_list_case_id
        """,
        (xml_export_list_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def load_export_case_add_candidates(
    cur: Any,
    *,
    xml_export_list_id: int,
    event_id: int,
    filters: Mapping[str, Any],
    limit: int = 80,
) -> list[dict[str, Any]]:
    where_parts = ["eec.event_id = %s"]
    params: list[Any] = [event_id]
    query = filters.get("case_q", "").strip()
    facility_query = filters.get("facility_q", "").strip()
    facility_codes = tuple(
        item.strip()
        for item in (filters.get("facility_codes") or "").replace(",", "\n").splitlines()
        if item.strip()
    )
    exam_month = filters.get("exam_month", "").strip()
    exam_item_namecodes = tuple(
        dict.fromkeys(
            str(value).strip()
            for value in (filters.get("exam_item_namecodes") or [])
            if str(value).strip()
        )
    )
    exam_item_match_mode = str(filters.get("exam_item_match_mode") or "any").strip().lower()
    if exam_item_match_mode not in {"any", "all"}:
        exam_item_match_mode = "any"
    readiness_values = tuple(
        value
        for value in ("EXPORT_READY", "APPROVED_WITH_REASON", "EXPORTED")
        if filters.get(f"include_{value.lower()}") == "1"
    ) or ("EXPORT_READY", "APPROVED_WITH_REASON")

    if query:
        like = f"%{query}%"
        where_parts.append(
            """
            (
              CAST(eec.exam_export_case_id AS CHAR) = %s
              OR eec.hia_subscriber_id LIKE %s
              OR eec.person_id_custom LIKE %s
              OR eec.name_kana_export_value LIKE %s
              OR eec.insurance_symbol_export_value LIKE %s
              OR eec.insurance_number_export_value LIKE %s
            )
            """
        )
        params.extend([query, like, like, like, like, like])
    if facility_query:
        like = f"%{facility_query}%"
        where_parts.append("(ef.exam_facility_code LIKE %s OR ef.exam_facility_name LIKE %s)")
        params.extend([like, like])
    if facility_codes:
        where_parts.append(f"ef.exam_facility_code IN ({', '.join(['%s'] * len(facility_codes))})")
        params.extend(facility_codes)
    if exam_month:
        where_parts.append("DATE_FORMAT(eec.exam_date, '%Y-%m') = %s")
        params.append(exam_month)
    if exam_item_namecodes:
        placeholders = ", ".join(["%s"] * len(exam_item_namecodes))
        match_count = len(exam_item_namecodes) if exam_item_match_mode == "all" else 1
        match_operator = "=" if exam_item_match_mode == "all" else ">="
        where_parts.append(
            f"""
            (
              SELECT COUNT(DISTINCT eecv_filter.namecode)
              FROM {qname(health_db())}.exam_export_case_values AS eecv_filter
              WHERE eecv_filter.exam_export_case_id = eec.exam_export_case_id
                AND eecv_filter.namecode IN ({placeholders})
            ) {match_operator} %s
            """
        )
        params.extend([*exam_item_namecodes, match_count])
    where_parts.append(f"eec.export_readiness_status IN ({', '.join(['%s'] * len(readiness_values))})")
    params.extend(readiness_values)

    where_sql = " AND ".join(where_parts)
    cur.execute(
        f"""
        SELECT
          eec.exam_export_case_id,
          eec.hia_subscriber_id,
          eec.person_id_custom,
          eec.insurance_symbol_export_value AS insured_card_symbol,
          eec.insurance_number_export_value AS insured_card_number,
          eec.name_kana_export_value AS name_kana,
          eec.birthdate AS birth_date,
          eec.exam_date,
          eec.export_readiness_status,
          eec.export_readiness_reason,
          eec.check_status,
          eec.check_reason,
          eec.xml_export_status,
          ef.exam_facility_code,
          ef.exam_facility_name,
          xelc.xml_export_list_case_id AS existing_list_case_id,
          xelc.removed_at AS existing_removed_at
        FROM {qname(health_db())}.exam_export_cases eec
        LEFT JOIN {qname(master_db())}.exam_facilities ef
          ON ef.exam_facility_id = eec.exam_facility_id
        LEFT JOIN {qname(health_db())}.ops_xml_export_list_cases xelc
          ON xelc.xml_export_list_id = %s
         AND xelc.exam_export_case_id = eec.exam_export_case_id
        WHERE {where_sql}
        ORDER BY
          CASE WHEN xelc.removed_at IS NULL AND xelc.xml_export_list_case_id IS NOT NULL THEN 0 ELSE 1 END,
          ef.exam_facility_code,
          eec.exam_date,
          eec.name_kana_export_value,
          eec.exam_export_case_id
        LIMIT %s
        """,
        (xml_export_list_id, *params, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def load_export_candidate_exam_items(cur: Any, *, namecodes: Sequence[str]) -> list[dict[str, Any]]:
    selected = tuple(dict.fromkeys(str(value).strip() for value in namecodes if str(value).strip()))
    if not selected:
        return []
    cur.execute(
        f"""
        SELECT namecode, item_name, category_name, xml_value_type, display_unit
        FROM {qname(dev_db())}.exam_item_master
        WHERE namecode IN ({', '.join(['%s'] * len(selected))})
        """,
        selected,
    )
    rows_by_namecode = {str(row["namecode"]): dict(row) for row in cur.fetchall()}
    return [
        rows_by_namecode.get(namecode, {"namecode": namecode, "item_name": namecode})
        for namecode in selected
    ]


def add_export_case_to_list(
    cur: Any,
    *,
    xml_export_list_id: int,
    exam_export_case_id: int,
    user: dict[str, Any],
) -> str:
    operator = str(user.get("employee_no") or user.get("display_name") or "")
    cur.execute(
        f"""
        SELECT eec.export_readiness_status, eec.export_readiness_reason
        FROM {qname(health_db())}.exam_export_cases eec
        INNER JOIN {qname(health_db())}.ops_xml_export_lists xel
          ON xel.xml_export_list_id = %s
         AND xel.event_id = eec.event_id
        WHERE eec.exam_export_case_id = %s
        LIMIT 1
        """,
        (xml_export_list_id, exam_export_case_id),
    )
    case = cur.fetchone()
    if not case:
        raise ValueError("CASE_NOT_FOUND")
    cur.execute(
        f"""
        SELECT xml_export_list_case_id, removed_at
        FROM {qname(health_db())}.ops_xml_export_list_cases
        WHERE xml_export_list_id = %s
          AND exam_export_case_id = %s
        LIMIT 1
        """,
        (xml_export_list_id, exam_export_case_id),
    )
    existing = cur.fetchone()
    if existing and existing.get("removed_at") is None:
        return "already"
    if existing:
        cur.execute(
            f"""
            UPDATE {qname(health_db())}.ops_xml_export_list_cases
            SET
              list_case_status = 'READY',
              export_readiness_status_snapshot = %s,
              export_readiness_reason_snapshot = %s,
              added_by = %s,
              added_at = CURRENT_TIMESTAMP(3),
              removed_by = NULL,
              removed_at = NULL,
              remove_reason = NULL,
              updated_at = CURRENT_TIMESTAMP(3)
            WHERE xml_export_list_case_id = %s
            """,
            (
                case.get("export_readiness_status"),
                case.get("export_readiness_reason"),
                operator,
                existing["xml_export_list_case_id"],
            ),
        )
        return "readded"
    cur.execute(
        f"""
        INSERT INTO {qname(health_db())}.ops_xml_export_list_cases (
          xml_export_list_id, exam_export_case_id, list_case_status,
          export_readiness_status_snapshot, export_readiness_reason_snapshot,
          added_by
        ) VALUES (%s, %s, 'READY', %s, %s, %s)
        """,
        (
            xml_export_list_id,
            exam_export_case_id,
            case.get("export_readiness_status"),
            case.get("export_readiness_reason"),
            operator,
        ),
    )
    return "added"


def remove_export_list_case(
    cur: Any,
    *,
    xml_export_list_id: int,
    xml_export_list_case_id: int,
    user: dict[str, Any],
    reason: str | None,
) -> int:
    operator = str(user.get("employee_no") or user.get("display_name") or "")
    cur.execute(
        f"""
        UPDATE {qname(health_db())}.ops_xml_export_list_cases
        SET
          list_case_status = 'REMOVED',
          removed_by = %s,
          removed_at = CURRENT_TIMESTAMP(3),
          remove_reason = NULLIF(%s, ''),
          updated_at = CURRENT_TIMESTAMP(3)
        WHERE xml_export_list_id = %s
          AND xml_export_list_case_id = %s
          AND removed_at IS NULL
        """,
        (operator, reason or "", xml_export_list_id, xml_export_list_case_id),
    )
    return int(cur.rowcount or 0)


LEDGER_REVERT_LIST_REMOVE_REASON_PREFIX = "SOURCE_LEDGER_REVERTED:"
EXPORT_LIST_AUTO_RESTORE_READY_STATUSES = {"EXPORT_READY", "APPROVED_WITH_REASON"}


def reconcile_reverted_export_list_cases_after_recheck(
    cur: Any,
    *,
    exam_export_case_ids: Sequence[int],
    operator: str,
) -> dict[str, Any]:
    case_ids = tuple(dict.fromkeys(int(case_id) for case_id in exam_export_case_ids if int(case_id) > 0))
    if not case_ids:
        return {"considered": 0, "restored": 0, "kept_removed": 0, "list_ids": []}
    placeholders = ", ".join(["%s"] * len(case_ids))
    cur.execute(
        f"""
        SELECT
          oelc.xml_export_list_case_id,
          oelc.xml_export_list_id,
          oelc.exam_export_case_id,
          oelc.remove_reason,
          oelc.removed_at,
          eec.export_readiness_status,
          eec.export_readiness_reason
        FROM {qname(health_db())}.ops_xml_export_list_cases AS oelc
        INNER JOIN {qname(health_db())}.exam_export_cases AS eec
          ON eec.exam_export_case_id = oelc.exam_export_case_id
        WHERE oelc.exam_export_case_id IN ({placeholders})
          AND oelc.removed_at IS NOT NULL
          AND oelc.remove_reason LIKE %s
        ORDER BY oelc.xml_export_list_case_id
        """,
        (*case_ids, f"{LEDGER_REVERT_LIST_REMOVE_REASON_PREFIX}%"),
    )
    rows = [dict(row) for row in cur.fetchall()]
    restored = 0
    kept_removed = 0
    for row in rows:
        readiness_status = str(row.get("export_readiness_status") or "")
        readiness_reason = str(row.get("export_readiness_reason") or "")
        if readiness_status in EXPORT_LIST_AUTO_RESTORE_READY_STATUSES:
            cur.execute(
                f"""
                UPDATE {qname(health_db())}.ops_xml_export_list_cases
                SET list_case_status = 'READY',
                    export_readiness_status_snapshot = %s,
                    export_readiness_reason_snapshot = NULLIF(%s, ''),
                    added_by = %s,
                    added_at = CURRENT_TIMESTAMP(3),
                    list_case_note = CONCAT_WS(
                      '\n',
                      NULLIF(list_case_note, ''),
                      CONCAT('[AUTO_RESTORED_AFTER_CASE_RECHECK] ', COALESCE(remove_reason, ''))
                    ),
                    removed_by = NULL,
                    removed_at = NULL,
                    remove_reason = NULL,
                    export_error_reason = NULL,
                    updated_at = CURRENT_TIMESTAMP(3)
                WHERE xml_export_list_case_id = %s
                  AND removed_at IS NOT NULL
                """,
                (
                    readiness_status,
                    readiness_reason,
                    operator,
                    row["xml_export_list_case_id"],
                ),
            )
            restored += int(cur.rowcount or 0)
            continue
        cur.execute(
            f"""
            UPDATE {qname(health_db())}.ops_xml_export_list_cases
            SET export_readiness_status_snapshot = NULLIF(%s, ''),
                export_readiness_reason_snapshot = NULLIF(%s, ''),
                export_error_reason = %s,
                updated_at = CURRENT_TIMESTAMP(3)
            WHERE xml_export_list_case_id = %s
              AND removed_at IS NOT NULL
            """,
            (
                readiness_status,
                readiness_reason,
                f"CASE_RECHECK_NOT_READY: {readiness_status or 'UNKNOWN'} / {readiness_reason or '-'}",
                row["xml_export_list_case_id"],
            ),
        )
        kept_removed += int(cur.rowcount or 0)
    return {
        "considered": len(rows),
        "restored": restored,
        "kept_removed": kept_removed,
        "list_ids": sorted({int(row["xml_export_list_id"]) for row in rows}),
    }


def run_hia_xml_export_from_list(
    *,
    xml_export_list_id: int,
    output_mode: str,
    package_mode: str = "standard",
    submission_facility_code: str = "",
    include_exported: bool = False,
) -> str:
    if output_mode not in {"review", "official"}:
        raise ValueError("OUTPUT_MODE_INVALID")
    if output_mode == "official":
        package_mode = "standard"
    if package_mode not in {"standard", "single_data"}:
        raise ValueError("PACKAGE_MODE_INVALID")
    if package_mode == "single_data" and not submission_facility_code.strip():
        submission_facility_code = "1322100106"
    script_path = REPO_ROOT / "scripts" / "from_medical" / "04_export_hia_xml.py"
    cmd = [
        sys.executable,
        str(script_path),
        "--xml-export-list-id",
        str(xml_export_list_id),
        "--output-mode",
        output_mode,
        "--no-latest-xml-export-list",
        "--package-mode",
        package_mode,
    ]
    if package_mode == "single_data":
        cmd.extend(["--submission-facility-code", submission_facility_code.strip()])
    if include_exported or output_mode == "review":
        cmd.append("--include-exported")
    if output_mode == "review":
        cmd.extend(["--review-output-root", str(HIA_XML_REVIEW_EXPORT_ROOT_DIR)])
    completed = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        timeout=60 * 30,
        check=False,
    )
    output = "\n".join(part.strip() for part in (completed.stdout, completed.stderr) if part and part.strip())
    if completed.returncode != 0:
        raise RuntimeError(output or f"XML export failed: returncode={completed.returncode}")
    if output_mode == "review" and "[OK]" not in output:
        raise RuntimeError(output or "確認用ZIPは作成されませんでした。出力対象が0件の可能性があります。")
    return output or "XML出力が完了しました。"


EXAM_PROCESSING_STEPS: tuple[dict[str, str], ...] = (
    {
        "key": "scan_files",
        "label": "01 スキャン",
        "script": "01_scan_files.py",
        "description": "eventの受領ルートをスキャンし、CSV/XML/ZIPを受領ファイル一覧に登録します。",
        "phase": "SCAN_FILES",
    },
    {
        "key": "import_xml",
        "label": "02 XML取り込み",
        "script": "02_import_xml.py",
        "description": "受領ファイル一覧のXML/ZIPを取り込み、source単位の健診結果を登録します。",
        "phase": "IMPORT_XML",
    },
    {
        "key": "import_csv",
        "label": "02 CSV取り込み",
        "script": "02_02_exam_result_csv_import.py",
        "description": "受領ファイル一覧のCSVをmappingに沿って取り込み、source単位の健診結果を登録します。",
        "phase": "IMPORT_CSV_EXAM_RESULTS",
    },
    {
        "key": "check_sources",
        "label": "03_00 受領単位チェック",
        "script": "03_00_check_imported_exam_ledgers.py",
        "description": "取り込み済みCSV/XMLのsource単位で法定チェックを更新します。",
        "phase": "CHECK_EXAM_RESULTS",
    },
    {
        "key": "build_cases",
        "label": "03_01 case更新",
        "script": "03_01_build_exam_export_cases.py",
        "description": "source単位の台帳から人単位の出力caseとsource紐付けを更新します。",
        "phase": "BUILD_EXAM_EXPORT_CASES",
    },
    {
        "key": "build_values",
        "label": "03_02 case値更新",
        "script": "03_02_build_exam_export_case_values.py",
        "description": "XML/CSVなど複数sourceから、出力用の採用値を作成します。",
        "phase": "BUILD_EXAM_EXPORT_CASE_VALUES",
    },
    {
        "key": "check_cases",
        "label": "03_04 case単位チェック",
        "script": "03_04_check_exam_export_cases.py",
        "description": "人単位の採用値で法定チェックを行い、出力可否summaryを更新します。",
        "phase": "CHECK_EXAM_RESULTS",
    },
)
EXAM_PROCESSING_STEP_MAP = {step["key"]: step for step in EXAM_PROCESSING_STEPS}


def run_exam_processing_step(
    *,
    step_key: str,
    event_id: int,
    dry_run: bool,
    limit: int = 0,
    include_imported: bool = False,
    case_ids: tuple[int, ...] = (),
    subscriber_ids: tuple[int, ...] = (),
) -> dict[str, Any]:
    step = EXAM_PROCESSING_STEP_MAP.get(step_key)
    if not step:
        raise ValueError("処理ステップが不正です。")
    script_path = REPO_ROOT / "scripts" / "from_medical" / step["script"]
    cmd = [
        sys.executable,
        str(script_path),
        "--event-id",
        str(event_id),
        "--db-prefix",
        db_prefix(),
        "--health-db",
        health_db(),
    ]
    if step_key in {
        "scan_files",
        "import_xml",
        "import_csv",
        "check_sources",
        "build_cases",
        "check_cases",
    }:
        cmd.extend(["--dev-db", dev_db()])
    if step_key in {"scan_files", "import_xml", "import_csv", "build_values"}:
        cmd.extend(["--master-db", master_db()])
    if dry_run:
        cmd.append("--dry-run")
    if include_imported and step_key in {"import_xml", "import_csv"}:
        cmd.append("--include-imported")
    if case_ids and step_key in {"build_cases", "build_values", "check_cases"}:
        for case_id in case_ids:
            cmd.extend(["--case-id", str(case_id)])
    if subscriber_ids and step_key == "build_cases":
        for subscriber_id in subscriber_ids:
            cmd.extend(["--subscriber-id", str(subscriber_id)])
    if limit > 0:
        limit_arg = "--limit-cases" if step_key == "build_values" else "--limit-groups" if step_key == "build_cases" else "--limit"
        cmd.extend([limit_arg, str(limit)])
    completed = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        timeout=60 * 30,
        check=False,
    )
    output = "\n".join(part.strip() for part in (completed.stdout, completed.stderr) if part and part.strip())
    return {
        "step_key": step_key,
        "label": step["label"],
        "returncode": completed.returncode,
        "ok": completed.returncode == 0,
        "output": output or "(出力なし)",
    }


def load_case_rebuild_cases(cur: Any, *, event_id: int, subscriber_id: int) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          eec.exam_export_case_id,
          eec.exam_date,
          eec.exam_facility_id,
          eec.facility_code,
          eec.facility_name,
          eec.source_mode,
          eec.case_status,
          eec.merge_status,
          eec.value_build_status,
          eec.check_status,
          eec.export_readiness_status,
          eec.xml_export_status,
          COUNT(DISTINCT src.exam_export_case_source_id) AS source_count,
          COUNT(DISTINCT cv.exam_export_case_value_id) AS value_count
        FROM {qname(health_db())}.exam_export_cases AS eec
        LEFT JOIN {qname(health_db())}.exam_export_case_sources AS src
          ON src.exam_export_case_id = eec.exam_export_case_id
         AND src.source_status = 'ACTIVE'
        LEFT JOIN {qname(health_db())}.exam_export_case_values AS cv
          ON cv.exam_export_case_id = eec.exam_export_case_id
        WHERE eec.event_id = %s
          AND eec.subscriber_id = %s
        GROUP BY eec.exam_export_case_id
        ORDER BY eec.exam_date DESC, eec.exam_facility_id, eec.exam_export_case_id DESC
        """,
        (event_id, subscriber_id),
    )
    return [dict(row) for row in cur.fetchall()]


def load_case_rebuild_subscriber(cur: Any, *, subscriber_id: int) -> dict[str, Any] | None:
    columns = manual_exam_entry_existing_columns(cur, dev_db(), "subscribers")
    name_expr = "s.name_kanji_full" if "name_kanji_full" in columns else "s.name_full_match"
    employee_expr = "s.employee_code" if "employee_code" in columns else "NULL"
    cur.execute(
        f"""
        SELECT
          s.id AS subscriber_id,
          s.hia_subscriber_id,
          s.person_id_custom,
          s.insurer_number,
          s.insurance_number,
          s.name_kana_full,
          {name_expr} AS name_kanji_full,
          s.birth,
          {employee_expr} AS employee_code
        FROM {qname(dev_db())}.subscribers AS s
        WHERE s.id = %s
        LIMIT 1
        """,
        (subscriber_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def load_case_rebuild_ledgers(
    cur: Any,
    *,
    event_id: int,
    subscriber_id: int,
    hia_subscriber_id: str | None,
) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          el.exam_ledger_id,
          el.source_type,
          el.exam_date,
          el.exam_facility_id,
          el.facility_code,
          el.facility_name,
          el.insurer_number,
          el.row_status,
          el.check_status,
          el.xml_file_name,
          fr.file_name,
          src.exam_export_case_id
        FROM {qname(health_db())}.exam_ledgers AS el
        LEFT JOIN {qname(health_db())}.manual_exam_entry_drafts AS d
          ON el.source_type IN ('PAPER', 'MANUAL')
         AND CAST(d.manual_exam_entry_draft_id AS CHAR) = JSON_UNQUOTE(
           JSON_EXTRACT(el.raw_row_json, '$.manual_exam_entry_draft_id')
         )
        LEFT JOIN {qname(health_db())}.file_receipts AS fr
          ON fr.id = el.file_receipt_id
        LEFT JOIN {qname(health_db())}.exam_export_case_sources AS src
          ON src.source_exam_ledger_id = el.exam_ledger_id
         AND src.source_status = 'ACTIVE'
        WHERE el.event_id = %s
          AND el.source_type IN ('XML', 'CSV', 'PAPER', 'MANUAL')
          AND COALESCE(el.row_status, '') <> 'REVERTED_TO_DRAFT'
          AND (
            el.subscriber_match_status = 'MATCHED'
            OR (
              el.source_type IN ('PAPER', 'MANUAL')
              AND el.subscriber_match_status IN ('MANUAL_ENTRY', 'MANUAL_CONFIRMED')
            )
          )
          AND (
            el.subscriber_id = %s
            OR d.subscriber_id = %s
            OR (
              el.source_type IN ('PAPER', 'MANUAL')
              AND %s <> ''
              AND el.hia_subscriber_id = %s
            )
          )
        ORDER BY el.exam_date DESC, el.exam_facility_id, el.source_type, el.exam_ledger_id
        """,
        (event_id, subscriber_id, subscriber_id, hia_subscriber_id or "", hia_subscriber_id or ""),
    )
    return [dict(row) for row in cur.fetchall()]


def load_case_rebuild_scope_subscriber_ids(
    cur: Any,
    *,
    event_id: int,
    exam_facility_id: int | None,
    exam_month: str,
) -> tuple[int, ...]:
    case_filters = ["eec.event_id = %s", "eec.subscriber_id IS NOT NULL"]
    ledger_filters = [
        "el.event_id = %s",
        "el.source_type IN ('XML', 'CSV', 'PAPER', 'MANUAL')",
        "COALESCE(el.row_status, '') <> 'REVERTED_TO_DRAFT'",
        "(el.subscriber_match_status = 'MATCHED' OR "
        "(el.source_type IN ('PAPER', 'MANUAL') AND "
        "el.subscriber_match_status IN ('MANUAL_ENTRY', 'MANUAL_CONFIRMED')))",
    ]
    case_params: list[Any] = [event_id]
    ledger_params: list[Any] = [event_id]
    if exam_facility_id is not None:
        case_filters.append("eec.exam_facility_id = %s")
        ledger_filters.append("el.exam_facility_id = %s")
        case_params.append(exam_facility_id)
        ledger_params.append(exam_facility_id)
    if exam_month:
        case_filters.append("DATE_FORMAT(eec.exam_date, '%Y-%m') = %s")
        ledger_filters.append("DATE_FORMAT(el.exam_date, '%Y-%m') = %s")
        case_params.append(exam_month)
        ledger_params.append(exam_month)
    cur.execute(
        f"""
        SELECT DISTINCT scoped.subscriber_id
        FROM (
          SELECT eec.subscriber_id
          FROM {qname(health_db())}.exam_export_cases AS eec
          WHERE {' AND '.join(case_filters)}

          UNION

          SELECT COALESCE(el.subscriber_id, d.subscriber_id, s.id) AS subscriber_id
          FROM {qname(health_db())}.exam_ledgers AS el
          LEFT JOIN {qname(health_db())}.manual_exam_entry_drafts AS d
            ON el.source_type IN ('PAPER', 'MANUAL')
           AND CAST(d.manual_exam_entry_draft_id AS CHAR) = JSON_UNQUOTE(
             JSON_EXTRACT(el.raw_row_json, '$.manual_exam_entry_draft_id')
           )
          LEFT JOIN {qname(dev_db())}.subscribers AS s
            ON s.hia_subscriber_id = el.hia_subscriber_id
          WHERE {' AND '.join(ledger_filters)}
        ) AS scoped
        WHERE scoped.subscriber_id IS NOT NULL
        ORDER BY scoped.subscriber_id
        """,
        (*case_params, *ledger_params),
    )
    return tuple(int(row["subscriber_id"]) for row in cur.fetchall())


def load_case_rebuild_month_options(cur: Any, *, event_id: int, limit: int = 36) -> list[str]:
    cur.execute(
        f"""
        SELECT exam_month
        FROM (
          SELECT DATE_FORMAT(exam_date, '%Y-%m') AS exam_month
          FROM {qname(health_db())}.exam_export_cases
          WHERE event_id = %s AND exam_date IS NOT NULL
          UNION
          SELECT DATE_FORMAT(exam_date, '%Y-%m') AS exam_month
          FROM {qname(health_db())}.exam_ledgers
          WHERE event_id = %s AND exam_date IS NOT NULL
        ) AS months
        ORDER BY exam_month DESC
        LIMIT %s
        """,
        (event_id, event_id, limit),
    )
    return [str(row["exam_month"]) for row in cur.fetchall()]


def load_recent_exam_processing_runs(cur: Any, *, event_id: int, limit: int = 20) -> list[dict[str, Any]]:
    phases = tuple(dict.fromkeys(step["phase"] for step in EXAM_PROCESSING_STEPS))
    placeholders = ", ".join(["%s"] * len(phases))
    cur.execute(
        f"""
        SELECT
          run_id,
          phase,
          status,
          started_at,
          finished_at,
          dry_run,
          rows_seen,
          rows_inserted,
          rows_updated,
          rows_skipped,
          errors,
          notes
        FROM {qname(health_db())}.etl_runs
        WHERE phase IN ({placeholders})
          AND (input_base = %s OR notes LIKE %s)
        ORDER BY started_at DESC, run_id DESC
        LIMIT %s
        """,
        (*phases, f"event_id={event_id}", f"%event_id={event_id}%", limit),
    )
    return [dict(row) for row in cur.fetchall()]


def load_etl_run_detail(cur: Any, *, run_id: int) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT
          run_id,
          phase,
          source,
          db_schema,
          status,
          started_at,
          finished_at,
          db_path,
          input_base,
          input_file,
          insurer_number,
          dry_run,
          limit_rows,
          files,
          rows_seen,
          rows_inserted,
          rows_updated,
          rows_unchanged,
          rows_skipped,
          errors,
          notes,
          admin_note
        FROM {qname(health_db())}.etl_runs
        WHERE run_id = %s
        """,
        (run_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def load_etl_run_errors(cur: Any, *, run_id: int, limit: int = 500) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          error_id,
          run_id,
          phase,
          source,
          insurer_number,
          src_file,
          src_row_no,
          src_line_no,
          staging_rowid,
          person_id_custom,
          field,
          field_value,
          error_code,
          message,
          created_at
        FROM {qname(health_db())}.etl_errors
        WHERE run_id = %s
        ORDER BY created_at DESC, error_id DESC
        LIMIT %s
        """,
        (run_id, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def load_running_exam_processing_runs(cur: Any, *, event_id: int, limit: int = 20) -> list[dict[str, Any]]:
    phases = tuple(dict.fromkeys(step["phase"] for step in EXAM_PROCESSING_STEPS))
    placeholders = ", ".join(["%s"] * len(phases))
    cur.execute(
        f"""
        SELECT
          run_id,
          phase,
          source,
          db_schema,
          status,
          started_at,
          dry_run,
          limit_rows,
          rows_seen,
          rows_inserted,
          rows_updated,
          rows_skipped,
          errors,
          notes,
          TIMESTAMPDIFF(MINUTE, started_at, CURRENT_TIMESTAMP(3)) AS running_minutes
        FROM {qname(health_db())}.etl_runs
        WHERE status = 'running'
          AND phase IN ({placeholders})
          AND (input_base = %s OR notes LIKE %s)
        ORDER BY started_at DESC, run_id DESC
        LIMIT %s
        """,
        (*phases, f"event_id={event_id}", f"%event_id={event_id}%", limit),
    )
    return [dict(row) for row in cur.fetchall()]


def load_running_etl_runs(cur: Any, *, limit: int = 100) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          run_id,
          phase,
          source,
          db_schema,
          status,
          started_at,
          dry_run,
          limit_rows,
          files,
          rows_seen,
          rows_inserted,
          rows_updated,
          rows_skipped,
          errors,
          input_base,
          input_file,
          notes,
          admin_note,
          TIMESTAMPDIFF(MINUTE, started_at, CURRENT_TIMESTAMP(3)) AS running_minutes
        FROM {qname(health_db())}.etl_runs
        WHERE status = 'running'
        ORDER BY started_at DESC, run_id DESC
        LIMIT %s
        """,
        (limit,),
    )
    return [dict(row) for row in cur.fetchall()]


def mark_etl_run_stopped(cur: Any, *, run_id: int, operator: str, reason: str) -> int:
    note = f"[admin_stop] {datetime.now().isoformat(timespec='seconds')} {operator}: {reason or 'reason not specified'}"
    cur.execute(
        f"""
        UPDATE {qname(health_db())}.etl_runs
        SET
          status = 'failed',
          finished_at = CURRENT_TIMESTAMP(3),
          errors = COALESCE(errors, 0) + 1,
          notes = CONCAT_WS('\n', NULLIF(notes, ''), %s),
          admin_note = CONCAT_WS('\n', NULLIF(admin_note, ''), %s)
        WHERE run_id = %s
          AND status = 'running'
        """,
        (note, note, run_id),
    )
    return int(cur.rowcount or 0)


def review_export_event_root(event_id: int) -> Path:
    return HIA_XML_REVIEW_EXPORT_ROOT_DIR / f"event_{event_id}"


def load_review_xml_export_downloads(*, event_id: int, limit: int = 50) -> list[dict[str, Any]]:
    root = review_export_event_root(event_id)
    if not root.exists():
        return []
    rows: list[dict[str, Any]] = []
    for path in root.rglob("*.zip"):
        if not path.is_file() or not is_path_under(path, root):
            continue
        stat = path.stat()
        rows.append(
            {
                "name": path.name,
                "download_path": str(path.relative_to(root)),
                "relative_path": str(path.relative_to(REPO_ROOT)),
                "size_mb": round(stat.st_size / 1024 / 1024, 2),
                "modified_at": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    return sorted(rows, key=lambda row: str(row["modified_at"]), reverse=True)[:limit]


def resolve_review_xml_export_zip(*, event_id: int, relative_path: str) -> Path:
    root = review_export_event_root(event_id)
    normalized = relative_path.strip().replace("¥", os.sep)
    if os.sep != "\\":
        normalized = normalized.replace("\\", os.sep)
    path = root / normalized
    if not is_path_under(path, root):
        raise ValueError("REVIEW_EXPORT_PATH_OUTSIDE_ROOT")
    if not path.exists() or not path.is_file() or path.suffix.lower() != ".zip":
        raise FileNotFoundError("REVIEW_EXPORT_ZIP_NOT_FOUND")
    return path


def delete_file_and_empty_parents(path: Path, *, stop_at: Path) -> None:
    try:
        path.unlink(missing_ok=True)
        stop_at_resolved = stop_at.resolve()
        current = path.parent.resolve()
        while current != stop_at_resolved and is_path_under(current, stop_at_resolved):
            try:
                current.rmdir()
            except OSError:
                break
            current = current.parent
    except OSError:
        LOGGER.exception("failed to delete review export zip: %s", path)


def create_xml_export_list_from_form(cur: Any, *, form: dict[str, str], user: dict[str, Any]) -> tuple[int, int, int]:
    event_id = int(form.get("event_id") or 2)
    list_name = (form.get("list_name") or "").strip()
    if not list_name:
        raise ValueError("LIST_NAME_REQUIRED")
    exam_month = (form.get("exam_month") or "").strip() or None
    facility_codes = tuple(
        item.strip()
        for item in (form.get("facility_codes") or "").replace(",", "\n").splitlines()
        if item.strip()
    )
    include_exported = form.get("include_exported") == "1"
    add_mode = form.get("add_mode") or "candidates"
    include_ready = form.get("include_ready") == "1"
    include_approved = form.get("include_approved") == "1"
    requested_file_date = (form.get("requested_file_date") or "").strip() or None
    requested_split_no_text = (form.get("requested_split_no") or "").strip()
    requested_split_no = int(requested_split_no_text) if requested_split_no_text else None
    selectors = ExportSelectors(
        event_id=event_id,
        facility_codes=facility_codes,
        exam_month=exam_month,
        include_exported=include_exported,
    )
    candidates = fetch_candidates(cur, selectors=selectors, health_db=health_db(), dev_db=dev_db(), master_db=master_db())
    selected = []
    for row in candidates:
        decision = decide_candidate(row)
        if not decision.allowed:
            continue
        if row.get("export_readiness_status") == "EXPORT_READY" and include_ready:
            selected.append(row)
        elif row.get("export_readiness_status") == "APPROVED_WITH_REASON" and include_approved:
            selected.append(row)
        elif row.get("export_readiness_status") == "EXPORTED" and include_exported:
            selected.append(row)
    if add_mode == "empty":
        selected = []

    status = "READY"
    created_by = str(user.get("employee_no") or user.get("display_name") or "")
    cur.execute(
        f"""
        INSERT INTO {qname(health_db())}.ops_xml_export_lists (
          event_id, list_name, list_status, selector_summary,
          requested_exam_month, requested_facility_codes, include_exported,
          requested_file_date, requested_split_no, created_by, confirmed_by, confirmed_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP(3))
        """,
        (
            event_id,
            list_name,
            status,
            "\n".join(
                [
                    f"event_id={event_id}",
                    f"exam_month={exam_month or ''}",
                    f"facility_codes={','.join(facility_codes)}",
                    f"include_ready={int(include_ready)}",
                    f"include_approved={int(include_approved)}",
                    f"include_exported={int(include_exported)}",
                    f"add_mode={add_mode}",
                ]
            ),
            exam_month,
            "\n".join(facility_codes) if facility_codes else None,
            include_exported,
            requested_file_date,
            requested_split_no,
            created_by,
            created_by,
        ),
    )
    xml_export_list_id = int(cur.lastrowid)
    for row in selected:
        cur.execute(
            f"""
            INSERT INTO {qname(health_db())}.ops_xml_export_list_cases (
              xml_export_list_id, exam_export_case_id, list_case_status,
              export_readiness_status_snapshot, export_readiness_reason_snapshot,
              added_by
            ) VALUES (%s, %s, 'READY', %s, %s, %s)
            """,
            (
                xml_export_list_id,
                row["exam_export_case_id"],
                row.get("export_readiness_status"),
                row.get("export_readiness_reason"),
                created_by,
            ),
        )
    return xml_export_list_id, len(candidates), len(selected)


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


def load_manual_exam_entry_items(cur: Any, *, limit: int = 5000) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          namecode,
          item_name,
          xml_value_type,
          result_code_oid,
          display_unit,
          ucum_unit,
          method_name,
          category_name,
          data_type_label,
          xml_method_code,
          identity_item_code,
          identity_item_name,
          annex2_exec_requirement,
          annex2_legal_report_flag,
          cda_section_code_default,
          annex2_series_group_identifier,
          annex2_series_group_relation_code
        FROM {qname(dev_db())}.exam_item_master
        ORDER BY
          COALESCE(kubun_no, 999999),
          COALESCE(jun_no, 999999),
          COALESCE(category_name, ''),
          COALESCE(identity_item_code, namecode),
          namecode
        LIMIT %s
        """,
        (limit,),
    )
    rows = [dict(row) for row in cur.fetchall()]
    code_options = load_manual_exam_cd_options(cur, rows)
    article44_flags = load_manual_exam_article44_flags(cur, rows)
    method_group_counts: dict[str, int] = {}
    for row in rows:
        group_key = str(row.get("identity_item_code") or row.get("namecode") or "")
        if group_key:
            method_group_counts[group_key] = method_group_counts.get(group_key, 0) + 1
    for row in rows:
        row["manual_original_category_name"] = row.get("category_name")
        if manual_exam_is_blood_collection_time(row):
            row["category_name"] = "生化学検査"
            row["manual_blood_time_item"] = True
        else:
            row["manual_blood_time_item"] = False
        row["manual_random_time_required_trigger"] = manual_exam_time_series_key(row) == "RANDOM"
        group_key = str(row.get("identity_item_code") or row.get("namecode") or "")
        row["manual_method_group_key"] = group_key
        row["manual_method_group_count"] = method_group_counts.get(group_key, 0)
        row["manual_input_type"] = manual_exam_input_type(row.get("xml_value_type"))
        result_code_oid = str(row.get("result_code_oid") or "")
        row["manual_code_options"] = code_options.get(result_code_oid, [])
        row["manual_code_toggle_options"] = (
            str(row.get("xml_value_type") or "").upper() in {"CD", "CO"}
            and str(row.get("identity_item_code") or "") in MANUAL_EXAM_FLAG_TOGGLE_IDENTITY_CODES
            and len(row["manual_code_options"]) == 2
        )
        namecode = str(row.get("namecode") or "")
        row["manual_article44_items"] = article44_flags.get(namecode, [])
    return rows


def manual_exam_entry_table_exists(cur: Any, schema_name: str, table_name: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name = %s
        """,
        (schema_name, table_name),
    )
    row = cur.fetchone()
    return bool(row and int(row.get("cnt") or 0) > 0)


def manual_exam_entry_column_exists(cur: Any, schema_name: str, table_name: str, column_name: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND column_name = %s
        """,
        (schema_name, table_name, column_name),
    )
    row = cur.fetchone()
    return bool(row and int(row.get("cnt") or 0) > 0)


def manual_exam_entry_existing_columns(cur: Any, schema_name: str, table_name: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name AS column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        """,
        (schema_name, table_name),
    )
    columns: set[str] = set()
    for row in cur.fetchall():
        value = row.get("column_name") or row.get("COLUMN_NAME")
        if value:
            columns.add(str(value))
    return columns


def load_manual_exam_entry_draft_rows(cur: Any, *, limit: int = 200, status_filter: str = "") -> list[dict[str, Any]]:
    value_join = ""
    value_select = "0 AS value_count"
    if manual_exam_entry_table_exists(cur, health_db(), "manual_exam_entry_draft_values"):
        value_select = "COALESCE(v.value_count, 0) AS value_count"
        value_join = f"""
        LEFT JOIN (
          SELECT manual_exam_entry_draft_id, COUNT(*) AS value_count
          FROM {qname(health_db())}.manual_exam_entry_draft_values
          GROUP BY manual_exam_entry_draft_id
        ) v
          ON v.manual_exam_entry_draft_id = d.manual_exam_entry_draft_id
        """
    article44_check_columns = ",\n          ".join(
        f"NULL AS a44_{detail_no}_status, NULL AS a44_{detail_no}_reason"
        for detail_no in ARTICLE44_DETAIL_NAMES
    )
    specific_detail_codes = sorted(SPECIFIC_DETAIL_CODE_BY_NAMECODE.values())
    specific_check_columns = ",\n          ".join(
        f"NULL AS sp_{detail_code}_status, NULL AS sp_{detail_code}_reason"
        for detail_code in specific_detail_codes
    )
    check_join = ""
    check_select = f"""
          NULL AS draft_legal_check_result,
          NULL AS draft_legal_reason_summary,
          NULL AS draft_specific_check_result,
          NULL AS draft_specific_reason_summary,
          NULL AS draft_checked_at,
          NULL AS draft_updated_at_snapshot,
          NULL AS draft_checked_by_name,
          {article44_check_columns},
          {specific_check_columns}
    """
    if manual_exam_entry_table_exists(cur, health_db(), "manual_exam_entry_draft_check_results"):
        existing_check_columns = manual_exam_entry_existing_columns(
            cur,
            health_db(),
            "manual_exam_entry_draft_check_results",
        )
        article44_check_columns = ",\n          ".join(
            f"dcr.a44_{detail_no}_status, dcr.a44_{detail_no}_reason"
            for detail_no in ARTICLE44_DETAIL_NAMES
        )
        specific_check_columns = ",\n          ".join(
            (
                f"dcr.sp_{detail_code}_status, dcr.sp_{detail_code}_reason"
                if f"sp_{detail_code}_status" in existing_check_columns
                and f"sp_{detail_code}_reason" in existing_check_columns
                else f"NULL AS sp_{detail_code}_status, NULL AS sp_{detail_code}_reason"
            )
            for detail_code in specific_detail_codes
        )
        check_select = f"""
          dcr.legal_check_result AS draft_legal_check_result,
          dcr.legal_reason_summary AS draft_legal_reason_summary,
          dcr.specific_check_result AS draft_specific_check_result,
          dcr.specific_reason_summary AS draft_specific_reason_summary,
          dcr.checked_at AS draft_checked_at,
          dcr.draft_updated_at_snapshot AS draft_updated_at_snapshot,
          COALESCE(cbu.display_name, cbu.employee_no, CONCAT('user ', dcr.checked_by_app_user_id)) AS draft_checked_by_name,
          {article44_check_columns},
          {specific_check_columns}
        """
        check_join = f"""
        LEFT JOIN {qname(health_db())}.manual_exam_entry_draft_check_results dcr
          ON dcr.manual_exam_entry_draft_id = d.manual_exam_entry_draft_id
        LEFT JOIN {qname(app_db())}.app_users cbu
          ON cbu.app_user_id = dcr.checked_by_app_user_id
        """

    status_filter = status_filter.upper().strip()
    where_clause = ""
    sql_params: list[Any] = []
    if status_filter == "ERROR":
        where_clause = "WHERE UPPER(COALESCE(d.draft_status, 'DRAFT')) IN ('ERROR', 'FAILED')"
    elif status_filter in {"DRAFT", "READY", "APPLIED"}:
        where_clause = "WHERE UPPER(COALESCE(d.draft_status, 'DRAFT')) = %s"
        sql_params.append(status_filter)

    cur.execute(
        f"""
        SELECT
          d.manual_exam_entry_draft_id,
          d.event_id,
          d.draft_status,
          d.entry_purpose,
          d.exam_export_case_id,
          d.subscriber_id,
          COALESCE(NULLIF(d.hia_subscriber_id, ''), s.hia_subscriber_id) AS hia_subscriber_id,
          COALESCE(NULLIF(d.person_id_custom, ''), s.person_id_custom) AS person_id_custom,
          COALESCE(NULLIF(d.insurer_number, ''), s.insurer_number) AS insurer_number,
          d.insurance_symbol,
          d.insurance_number,
          d.insurance_branch_number,
          d.postal_code,
          d.address,
          d.name_full,
          d.name_kana,
          d.birthdate,
          d.gender_code,
          d.facility_code,
          d.facility_name,
          d.facility_document_id,
          d.exam_date,
          d.created_by_app_user_id,
          d.updated_by_app_user_id,
          d.applied_exam_ledger_id,
          d.created_at,
          d.updated_at,
          d.applied_at,
          {value_select},
          {check_select},
          COALESCE(cu.display_name, cu.employee_no, CONCAT('user ', d.created_by_app_user_id)) AS created_by_name,
          COALESCE(uu.display_name, uu.employee_no, CONCAT('user ', d.updated_by_app_user_id)) AS updated_by_name
        FROM {qname(health_db())}.manual_exam_entry_drafts d
        LEFT JOIN {qname(dev_db())}.subscribers s
          ON s.id = d.subscriber_id
        {value_join}
        {check_join}
        LEFT JOIN {qname(app_db())}.app_users cu
          ON cu.app_user_id = d.created_by_app_user_id
        LEFT JOIN {qname(app_db())}.app_users uu
          ON uu.app_user_id = d.updated_by_app_user_id
        {where_clause}
        ORDER BY
          d.updated_at DESC,
          d.manual_exam_entry_draft_id DESC
        LIMIT %s
        """,
        (*sql_params, limit),
    )
    rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        row["draft_check_display_status"] = manual_exam_draft_check_display_status(row)
        row["draft_check_details"] = manual_exam_draft_check_details(row)
    return rows


def manual_exam_draft_check_display_status(row: Mapping[str, Any]) -> str:
    if not row.get("draft_checked_at"):
        return "UNCHECKED"
    snapshot = row.get("draft_updated_at_snapshot")
    updated = row.get("updated_at")
    if snapshot and updated and updated > snapshot:
        return "STALE"
    legal = str(row.get("draft_legal_check_result") or "").upper()
    specific = str(row.get("draft_specific_check_result") or "").upper()
    if legal == RESULT_NG or specific == RESULT_NG:
        return "NG"
    if legal in {RESULT_UNDETERMINABLE, "PENDING"} or specific in {RESULT_UNDETERMINABLE, "PENDING"}:
        return "UNDETERMINABLE"
    if legal == RESULT_OK and specific in {RESULT_OK, RESULT_NOT_APPLICABLE}:
        return "OK"
    return "UNDETERMINABLE"


def manual_exam_draft_check_details(row: Mapping[str, Any]) -> list[dict[str, str | None]]:
    if not row.get("draft_checked_at"):
        return []
    details: list[dict[str, str | None]] = []
    for detail_no, detail_name in ARTICLE44_DETAIL_NAMES.items():
        status = _manual_text(row.get(f"a44_{detail_no}_status"))
        reason = _manual_text(row.get(f"a44_{detail_no}_reason"))
        details.append(
            {
                "scope": "法定",
                "detail_no": detail_no,
                "name": detail_name,
                "status": status,
                "reason": reason,
            }
        )
    specific_detail_found = False
    for namecode, detail_code in sorted(SPECIFIC_DETAIL_CODE_BY_NAMECODE.items(), key=lambda item: item[1]):
        status = _manual_text(row.get(f"sp_{detail_code}_status"))
        reason = _manual_text(row.get(f"sp_{detail_code}_reason"))
        if not status and not reason:
            continue
        specific_detail_found = True
        details.append(
            {
                "scope": "特定",
                "detail_no": detail_code,
                "name": SPECIFIC_ITEM_NAMES.get(namecode, namecode),
                "status": status,
                "reason": reason,
            }
        )
    specific_status = _manual_text(row.get("draft_specific_check_result"))
    specific_reason = _manual_text(row.get("draft_specific_reason_summary"))
    if not specific_detail_found and (specific_status or specific_reason):
        details.append(
            {
                "scope": "特定",
                "detail_no": "summary",
                "name": "特定健診チェック",
                "status": specific_status,
                "reason": specific_reason,
            }
        )
    return details


def load_manual_exam_entry_draft_by_id(cur: Any, draft_id: int) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT
          d.manual_exam_entry_draft_id,
          d.event_id,
          d.draft_status,
          d.entry_purpose,
          d.exam_export_case_id,
          d.subscriber_id,
          d.hia_subscriber_id,
          d.person_id_custom,
          COALESCE(NULLIF(d.insurer_number, ''), s.insurer_number) AS insurer_number,
          d.insurance_symbol,
          d.insurance_number,
          d.insurance_branch_number,
          d.name_full,
          d.name_kana,
          d.birthdate,
          d.gender_code,
          d.exam_facility_id,
          d.facility_code,
          d.facility_name,
          d.facility_document_id,
          d.exam_date,
          d.note,
          d.created_at,
          d.updated_at
        FROM {qname(health_db())}.manual_exam_entry_drafts d
        LEFT JOIN {qname(dev_db())}.subscribers s
          ON s.id = d.subscriber_id
        WHERE d.manual_exam_entry_draft_id = %s
        LIMIT 1
        """,
        (draft_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    draft = dict(row)
    if manual_exam_entry_table_exists(cur, health_db(), "manual_exam_entry_draft_values"):
        cur.execute(
            f"""
            SELECT
              manual_exam_entry_draft_value_id,
              namecode,
              namecode_display_name,
              identity_item_code,
              identity_item_name,
              xml_value_type,
              raw_value,
              normalized_value,
              code_system,
              code_value,
              code_display,
              display_unit,
              ucum_unit,
              method_code,
              method_name,
              occurrence_no,
              include_flag,
              input_status,
              note
            FROM {qname(health_db())}.manual_exam_entry_draft_values
            WHERE manual_exam_entry_draft_id = %s
            ORDER BY manual_exam_entry_draft_value_id
            """,
            (draft_id,),
        )
        draft["values"] = [dict(value_row) for value_row in cur.fetchall()]
    else:
        draft["values"] = []
    return draft


def load_manual_exam_draft_check_value_map(
    cur: Any,
    *,
    draft_id: int,
    required_namecodes: tuple[Any, ...],
) -> dict[str, Any]:
    namecodes = tuple(required.namecode for required in required_namecodes)
    if not namecodes:
        raise ValueError("required_namecodes_empty")
    placeholders = ", ".join(["%s"] * len(namecodes))
    cur.execute(
        f"""
        SELECT
          dv.manual_exam_entry_draft_id AS ledger_id,
          dv.manual_exam_entry_draft_value_id AS id,
          dv.namecode,
          COALESCE(dv.xml_value_type, im.xml_value_type, 'ST') AS raw_value_type,
          dv.raw_value,
          dv.display_unit AS raw_unit,
          COALESCE(dv.normalized_value, dv.raw_value, dv.code_value) AS normalized_value,
          COALESCE(dv.ucum_unit, dv.display_unit, im.ucum_unit, im.display_unit) AS normalized_unit,
          dv.code_value,
          COALESCE(im.cda_section_code_default, '01030') AS section_code
        FROM {qname(health_db())}.manual_exam_entry_draft_values AS dv
        LEFT JOIN {qname(dev_db())}.exam_item_master AS im
          ON im.namecode = dv.namecode
        WHERE dv.manual_exam_entry_draft_id = %s
          AND dv.include_flag = 1
          AND dv.namecode IN ({placeholders})
        ORDER BY dv.namecode, dv.manual_exam_entry_draft_value_id
        """,
        (draft_id, *namecodes),
    )
    return build_article44_value_map(required_namecodes, cur.fetchall())


def insert_manual_exam_draft_check_result(
    cur: Any,
    *,
    draft: Mapping[str, Any],
    article44_result: Mapping[str, Any],
    legal_result: str,
    legal_summary: str | None,
    specific_result: str,
    specific_summary: str | None,
    specific_detail_results: Mapping[str, Any],
    user_id: int,
) -> None:
    draft_id = int(draft["manual_exam_entry_draft_id"])
    cur.execute(
        f"""
        DELETE FROM {qname(health_db())}.manual_exam_entry_draft_check_results
         WHERE manual_exam_entry_draft_id = %s
        """,
        (draft_id,),
    )
    row: dict[str, Any] = {
        "manual_exam_entry_draft_id": draft_id,
        "event_id": _optional_int(draft.get("event_id")) or 2,
        "subscriber_id": _optional_int(draft.get("subscriber_id")),
        "hia_subscriber_id": _manual_text(draft.get("hia_subscriber_id")),
        "legal_check_result": legal_result,
        "specific_check_result": specific_result,
        "legal_reason_summary": legal_summary,
        "specific_reason_summary": specific_summary,
        "draft_updated_at_snapshot": draft.get("updated_at"),
        "checked_by_app_user_id": user_id,
    }
    row.update(article44_result_columns(article44_result))
    existing_columns = manual_exam_entry_existing_columns(
        cur,
        health_db(),
        "manual_exam_entry_draft_check_results",
    )
    for detail_code, result in specific_detail_results.items():
        status_column = f"sp_{detail_code}_status"
        reason_column = f"sp_{detail_code}_reason"
        if status_column not in existing_columns or reason_column not in existing_columns:
            continue
        row[status_column] = result.status
        row[reason_column] = result.reason
    columns = list(row.keys())
    placeholders = ", ".join(["%s"] * len(columns))
    column_sql = ", ".join(f"`{column}`" for column in columns)
    cur.execute(
        f"""
        INSERT INTO {qname(health_db())}.manual_exam_entry_draft_check_results ({column_sql})
        VALUES ({placeholders})
        """,
        tuple(row[column] for column in columns),
    )
    cur.execute(
        f"""
        INSERT INTO {qname(health_db())}.manual_exam_entry_draft_audit_logs (
          manual_exam_entry_draft_id,
          event_id,
          action_code,
          field_name,
          old_value,
          new_value,
          source,
          changed_by_app_user_id
        ) VALUES (
          %s,
          %s,
          'RUN_DRAFT_CHECK',
          'draft_check',
          NULL,
          %s,
          'ADMIN_UI',
          %s
        )
        """,
        (
            draft_id,
            _optional_int(draft.get("event_id")) or 2,
            json.dumps(
                {
                    "legal_check_result": legal_result,
                    "specific_check_result": specific_result,
                    "legal_reason_summary": legal_summary,
                    "specific_reason_summary": specific_summary,
                },
                ensure_ascii=False,
                default=manual_exam_json_default,
            ),
            user_id,
        ),
    )


def check_manual_exam_entry_draft(cur: Any, *, draft_id: int, user_id: int) -> dict[str, Any]:
    draft = load_manual_exam_entry_draft_by_id(cur, draft_id)
    if draft is None:
        raise ValueError("draft_not_found")
    if not manual_exam_entry_table_exists(cur, health_db(), "manual_exam_entry_draft_check_results"):
        raise ValueError("draft_check_table_not_found")
    if not draft.get("values"):
        raise ValueError("draft_has_no_values")

    event_id = _optional_int(draft.get("event_id")) or 2
    article44_required = fetch_article44_required_namecodes(cur, dev_db=dev_db())
    article44_value_map = load_manual_exam_draft_check_value_map(
        cur,
        draft_id=draft_id,
        required_namecodes=article44_required,
    )
    article44_result = check_article44(article44_value_map)
    validate_article44_result(article44_result)
    legal_result, legal_summary = aggregate_article44_legal_result(article44_result)

    specific_required = fetch_specific_health_required_namecodes(
        cur,
        dev_db=dev_db(),
        fallback=SPECIFIC_REQUIRED_NAMECODES,
    )
    specific_value_map = load_manual_exam_draft_check_value_map(
        cur,
        draft_id=draft_id,
        required_namecodes=specific_required,
    )
    event_year = get_event_year(cur, event_id=event_id, dev_db=dev_db())
    fiscal_end = fiscal_year_end_date(event_year) if event_year is not None else None
    specific_result, specific_summary, specific_detail_results = aggregate_specific_result_with_details(
        value_map=specific_value_map,
        required_namecodes=specific_required,
        birthdate=draft.get("birthdate"),
        age_reference_date=fiscal_end,
        legal_result=legal_result,
    )
    insert_manual_exam_draft_check_result(
        cur,
        draft=draft,
        article44_result=article44_result,
        legal_result=legal_result,
        legal_summary=legal_summary,
        specific_result=specific_result,
        specific_summary=specific_summary,
        specific_detail_results=specific_detail_results,
        user_id=user_id,
    )
    return {
        "draft_id": draft_id,
        "legal_check_result": legal_result,
        "legal_reason_summary": legal_summary,
        "specific_check_result": specific_result,
        "specific_reason_summary": specific_summary,
    }


def manual_exam_json_default(value: Any) -> str:
    if isinstance(value, (date, datetime, Decimal)):
        return str(value)
    return str(value)


def _manual_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _manual_date_text(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = normalize_date_to_ymd_and_compact(text, purpose="manual_exam_entry")
    return normalized.get("field_norm") or text


def load_subscriber_insurer_number(cur: Any, subscriber_id: int | None) -> str | None:
    if not subscriber_id:
        return None
    cur.execute(
        f"""
        SELECT insurer_number
        FROM {qname(dev_db())}.subscribers
        WHERE id = %s
        LIMIT 1
        """,
        (subscriber_id,),
    )
    row = cur.fetchone()
    return _manual_text(row.get("insurer_number") if row else None)


def _manual_effective_insurer_number(value: Any) -> str | None:
    result = normalize_insurer_number(None if value is None else str(value))
    normalized = result.get("field_norm")
    if not result.get("ok") or normalized in (None, "", "0"):
        return None
    return zero_pad(str(normalized), 8)


def resolve_manual_exam_entry_insurer_number(
    cur: Any,
    *,
    event_id: int,
    source_value: Any,
    exam_export_case_id: int | None = None,
    subscriber_id: int | None = None,
) -> tuple[str | None, str]:
    source_number = _manual_effective_insurer_number(source_value)
    if source_number:
        return source_number, "SOURCE"
    if exam_export_case_id:
        cur.execute(
            f"""
            SELECT insurer_number_export_value, insurer_number
            FROM {qname(health_db())}.exam_export_cases
            WHERE exam_export_case_id = %s
              AND event_id = %s
            LIMIT 1
            """,
            (exam_export_case_id, event_id),
        )
        case = cur.fetchone() or {}
        case_number = _manual_effective_insurer_number(
            case.get("insurer_number_export_value") or case.get("insurer_number")
        )
        if case_number:
            return case_number, "LINKED_CASE"
    event_number = _manual_effective_insurer_number(
        get_event_insurer_number(cur, event_id=event_id, dev_db=dev_db())
    )
    if event_number:
        return event_number, "EVENT"
    subscriber_number = _manual_effective_insurer_number(load_subscriber_insurer_number(cur, subscriber_id))
    if subscriber_number:
        return subscriber_number, "SUBSCRIBER"
    return None, "UNRESOLVED"


def insert_manual_exam_entry_draft(
    cur: Any,
    *,
    event_id: int,
    entry_purpose: str,
    action_code: str,
    source_payload: Mapping[str, Any],
    user_id: int,
) -> int:
    subscriber_id = _optional_int(source_payload.get("subscriber_id"))
    event_id = int(event_id)
    exam_export_case_id = _optional_int(source_payload.get("exam_export_case_id") or source_payload.get("case_id"))
    insurer_number, _insurer_source = resolve_manual_exam_entry_insurer_number(
        cur,
        event_id=event_id,
        source_value=source_payload.get("insurer_number"),
        exam_export_case_id=exam_export_case_id,
        subscriber_id=subscriber_id,
    )
    cur.execute(
        f"""
        INSERT INTO {qname(health_db())}.manual_exam_entry_drafts (
          event_id,
          draft_status,
          entry_purpose,
          exam_export_case_id,
          subscriber_id,
          hia_subscriber_id,
          person_id_custom,
          insurer_number,
          insurance_symbol,
          insurance_number,
          insurance_branch_number,
          postal_code,
          address,
          name_full,
          name_kana,
          birthdate,
          gender_code,
          exam_facility_id,
          facility_code,
          facility_name,
          facility_document_id,
          exam_date,
          created_by_app_user_id,
          updated_by_app_user_id
        ) VALUES (
          %s,
          'DRAFT',
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s
        )
        """,
        (
            event_id,
            entry_purpose,
            exam_export_case_id,
            subscriber_id,
            _manual_text(source_payload.get("hia_subscriber_id")),
            _manual_text(source_payload.get("person_id_custom")),
            insurer_number,
            _manual_text(source_payload.get("insurance_symbol")),
            _manual_text(source_payload.get("insurance_number")),
            _manual_text(source_payload.get("insurance_branch_number")),
            normalize_postal_code_export(source_payload.get("postal_code") or source_payload.get("subscriber_postal_code")),
            normalize_address_export(source_payload.get("address") or source_payload.get("subscriber_address_line")),
            _manual_text(source_payload.get("name_full")),
            _manual_text(source_payload.get("name_kana")),
            _manual_date_text(source_payload.get("birthdate") or source_payload.get("birth")),
            _manual_text(source_payload.get("gender_code") or source_payload.get("gender_label")),
            _optional_int(source_payload.get("exam_facility_id")),
            _manual_text(source_payload.get("facility_code")),
            _manual_text(source_payload.get("facility_name")),
            _manual_text(source_payload.get("facility_document_id")),
            _manual_date_text(source_payload.get("exam_date")),
            user_id,
            user_id,
        ),
    )
    draft_id = int(cur.lastrowid)
    cur.execute(
        f"""
        INSERT INTO {qname(health_db())}.manual_exam_entry_draft_audit_logs (
          manual_exam_entry_draft_id,
          event_id,
          action_code,
          field_name,
          old_value,
          new_value,
          source,
          changed_by_app_user_id
        ) VALUES (
          %s,
          %s,
          %s,
          'draft',
          NULL,
          %s,
          'ADMIN_UI',
          %s
        )
        """,
        (
            draft_id,
            event_id,
            action_code,
            json.dumps(source_payload, ensure_ascii=False, default=manual_exam_json_default),
            user_id,
        ),
    )
    return draft_id


def update_manual_exam_entry_draft_basic_info(
    cur: Any,
    *,
    draft_id: int,
    event_id: int,
    facility_code: str | None,
    facility_name: str | None,
    exam_date: str | None,
    user_id: int,
) -> None:
    before = load_manual_exam_entry_draft_by_id(cur, draft_id)
    if before is None:
        raise ValueError("draft_not_found")
    cur.execute(
        f"""
        UPDATE {qname(health_db())}.manual_exam_entry_drafts
           SET facility_code = %s,
               facility_name = %s,
               exam_date = %s,
               updated_by_app_user_id = %s
         WHERE manual_exam_entry_draft_id = %s
        """,
        (facility_code, facility_name, exam_date, user_id, draft_id),
    )
    cur.execute(
        f"""
        INSERT INTO {qname(health_db())}.manual_exam_entry_draft_audit_logs (
          manual_exam_entry_draft_id,
          event_id,
          action_code,
          field_name,
          old_value,
          new_value,
          source,
          changed_by_app_user_id
        ) VALUES (
          %s,
          %s,
          'UPDATE_BASIC_INFO',
          'basic_info',
          %s,
          %s,
          'ADMIN_UI',
          %s
        )
        """,
        (
            draft_id,
            event_id,
            json.dumps(
                {
                    "facility_code": before.get("facility_code"),
                    "facility_name": before.get("facility_name"),
                    "exam_date": before.get("exam_date"),
                },
                ensure_ascii=False,
                default=manual_exam_json_default,
            ),
            json.dumps(
                {
                    "facility_code": facility_code,
                    "facility_name": facility_name,
                    "exam_date": exam_date,
                },
                ensure_ascii=False,
                default=manual_exam_json_default,
            ),
            user_id,
        ),
    )


def delete_manual_exam_entry_draft(cur: Any, *, draft_id: int) -> dict[str, Any]:
    before = load_manual_exam_entry_draft_by_id(cur, draft_id)
    if before is None:
        raise ValueError("draft_not_found")
    if str(before.get("draft_status") or "DRAFT").upper() == "APPLIED":
        raise ValueError("draft_already_applied")
    cur.execute(
        f"""
        DELETE FROM {qname(health_db())}.manual_exam_entry_drafts
         WHERE manual_exam_entry_draft_id = %s
        """,
        (draft_id,),
    )
    return before


def manual_exam_entry_section_name(section_code: Any) -> str | None:
    code = str(section_code or "").strip()
    if not code:
        return None
    return CDA_SECTION_NAMES.get(code)


def manual_exam_entry_source_type(entry_purpose: Any) -> str:
    purpose = str(entry_purpose or "").strip().upper()
    if purpose == "PAPER_ONLY":
        return "PAPER"
    return "MANUAL"


def manual_exam_entry_identity_from_draft(draft: Mapping[str, Any]) -> dict[str, Any]:
    result = {
        "person_id_custom": _manual_text(draft.get("person_id_custom")),
        "identity_hash": None,
        "reason": None,
    }
    if not all(
        _manual_text(draft.get(key))
        for key in (
            "insurer_number",
            "insurance_symbol",
            "insurance_number",
            "name_kana",
            "birthdate",
            "gender_code",
        )
    ):
        result["reason"] = "identity source fields incomplete"
        return result
    bundle = generate_identity_bundle(
        birthdate=draft.get("birthdate"),
        insurer_number_raw=draft.get("insurer_number"),
        insurance_symbol_raw=draft.get("insurance_symbol"),
        insurance_number_raw=draft.get("insurance_number"),
        name_kana_full_raw=draft.get("name_kana"),
        gender_code=draft.get("gender_code"),
    )
    result["person_id_custom"] = bundle.get("person_id_custom") or result["person_id_custom"]
    result["identity_hash"] = bundle.get("identity_hash")
    result["reason"] = None if bundle.get("ok") else str(bundle.get("reason") or "identity generation failed")
    return result


def resolve_manual_exam_entry_facility_id(cur: Any, draft: Mapping[str, Any]) -> int | None:
    facility_id = _optional_int(draft.get("exam_facility_id"))
    if facility_id is not None:
        return facility_id
    facility_code = _manual_text(draft.get("facility_code"))
    if not facility_code:
        return None
    cur.execute(
        f"""
        SELECT exam_facility_id
        FROM {qname(master_db())}.exam_facilities
        WHERE exam_facility_code = %s
        LIMIT 1
        """,
        (facility_code,),
    )
    row = cur.fetchone()
    return _optional_int(row.get("exam_facility_id")) if row else None


def build_manual_exam_draft_case_match_check(cur: Any, *, draft_id: int) -> dict[str, Any] | None:
    draft = load_manual_exam_entry_draft_by_id(cur, draft_id)
    if draft is None:
        return None

    facility_id = resolve_manual_exam_entry_facility_id(cur, draft)
    effective_insurer_number, insurer_source = resolve_manual_exam_entry_insurer_number(
        cur,
        event_id=int(draft.get("event_id") or 0),
        source_value=draft.get("insurer_number"),
        exam_export_case_id=_optional_int(draft.get("exam_export_case_id")),
        subscriber_id=_optional_int(draft.get("subscriber_id")),
    )
    criteria = [
        {
            "key": "event_id",
            "label": "イベント",
            "value": json_safe_value(draft.get("event_id")),
            "required": True,
        },
        {
            "key": "subscriber_id",
            "label": "subscriber_id",
            "value": json_safe_value(draft.get("subscriber_id")),
            "required": True,
        },
        {
            "key": "exam_date",
            "label": "健診実施日",
            "value": json_safe_value(draft.get("exam_date")),
            "required": True,
        },
        {
            "key": "exam_facility_id",
            "label": "健診機関ID",
            "value": json_safe_value(facility_id),
            "required": True,
            "note": f"コード {draft.get('facility_code') or '-'} から解決" if draft.get("facility_code") else None,
        },
        {
            "key": "insurer_number",
            "label": "保険者番号",
            "value": json_safe_value(effective_insurer_number),
            "required": True,
            "note": f"確定元: {insurer_source}",
        },
    ]
    missing = [
        item
        for item in criteria
        if item["required"] and str(item.get("value") or "").strip() == ""
    ]
    matches: list[dict[str, Any]] = []
    nearby_cases: list[dict[str, Any]] = []
    if not missing:
        cur.execute(
            f"""
            SELECT
              exam_export_case_id,
              event_id,
              subscriber_id,
              hia_subscriber_id,
              exam_date,
              exam_facility_id,
              facility_code,
              facility_name,
              insurer_number,
              insurer_number_export_value,
              source_mode,
              updated_at
            FROM {qname(health_db())}.exam_export_cases
            WHERE event_id = %s
              AND subscriber_id = %s
              AND exam_date = %s
              AND exam_facility_id = %s
              AND COALESCE(insurer_number_export_value, insurer_number) = %s
            ORDER BY exam_export_case_id DESC
            LIMIT 10
            """,
            (
                draft.get("event_id"),
                draft.get("subscriber_id"),
                draft.get("exam_date"),
                facility_id,
                effective_insurer_number,
            ),
        )
        matches = [json_safe_mapping(row) for row in cur.fetchall()]

    if draft.get("event_id") and draft.get("subscriber_id"):
        cur.execute(
            f"""
            SELECT
              exam_export_case_id,
              event_id,
              subscriber_id,
              hia_subscriber_id,
              exam_date,
              exam_facility_id,
              facility_code,
              facility_name,
              insurer_number,
              insurer_number_export_value,
              source_mode,
              updated_at
            FROM {qname(health_db())}.exam_export_cases
            WHERE event_id = %s
              AND subscriber_id = %s
            ORDER BY exam_date DESC, exam_export_case_id DESC
            LIMIT 10
            """,
            (draft.get("event_id"), draft.get("subscriber_id")),
        )
        nearby_cases = [json_safe_mapping(row) for row in cur.fetchall()]
        for case in nearby_cases:
            case["mismatches"] = []
            if str(case.get("exam_date") or "") != str(draft.get("exam_date") or ""):
                case["mismatches"].append("健診実施日")
            if str(case.get("exam_facility_id") or "") != str(facility_id or ""):
                case["mismatches"].append("健診機関ID")
            case_insurer_number = case.get("insurer_number_export_value") or case.get("insurer_number")
            if str(case_insurer_number or "") != str(effective_insurer_number or ""):
                case["mismatches"].append("保険者番号")

    if missing:
        status = "MISSING_KEYS"
        message = "case作成に必要なキーが不足しています。"
    elif matches:
        status = "MATCHED"
        message = f"完全一致するcaseが {len(matches)} 件あります。"
    else:
        status = "NO_MATCH"
        message = "必要キーは揃っていますが、完全一致するcaseはありません。case作成未実行、または既存case側のキー差異を確認してください。"

    return {
        "draft_id": draft_id,
        "status": status,
        "message": message,
        "criteria": criteria,
        "missing_keys": [item["key"] for item in missing],
        "matches": matches,
        "nearby_cases": nearby_cases,
    }


def apply_manual_exam_entry_draft(cur: Any, *, draft_id: int, user_id: int) -> dict[str, Any]:
    draft = load_manual_exam_entry_draft_by_id(cur, draft_id)
    if draft is None:
        raise ValueError("draft_not_found")
    if str(draft.get("draft_status") or "DRAFT").upper() == "APPLIED" or draft.get("applied_exam_ledger_id"):
        raise ValueError("draft_already_applied")

    values = [
        value
        for value in draft.get("values", [])
        if int(value.get("include_flag") or 0) == 1
        and (_manual_text(value.get("raw_value")) or _manual_text(value.get("code_value")))
    ]
    if not values:
        raise ValueError("draft_has_no_values")

    event_id = _optional_int(draft.get("event_id")) or 2
    source_type = manual_exam_entry_source_type(draft.get("entry_purpose"))
    document_id = f"manual-draft-{draft_id}"
    cur.execute(
        f"""
        SELECT COUNT(*) AS applied_count
        FROM {qname(health_db())}.exam_ledgers
        WHERE document_id = %s
           OR JSON_UNQUOTE(JSON_EXTRACT(raw_row_json, '$.manual_exam_entry_draft_id')) = %s
        """,
        (document_id, str(draft_id)),
    )
    applied_count_row = cur.fetchone() or {}
    apply_sequence = int(applied_count_row.get("applied_count") or 0) + 1
    row_payload = {
        "source": "MANUAL_EXAM_ENTRY_DRAFT",
        "manual_exam_entry_draft_id": draft_id,
        "apply_sequence": apply_sequence,
        "entry_purpose": draft.get("entry_purpose"),
        "value_count": len(values),
    }
    row_sha256 = hashlib.sha256(
        f"manual_exam_entry_draft:{draft_id}:apply:{apply_sequence}".encode("utf-8")
    ).hexdigest()
    effective_insurer_number, insurer_source = resolve_manual_exam_entry_insurer_number(
        cur,
        event_id=event_id,
        source_value=draft.get("insurer_number"),
        exam_export_case_id=_optional_int(draft.get("exam_export_case_id")),
        subscriber_id=_optional_int(draft.get("subscriber_id")),
    )
    effective_draft = dict(draft)
    effective_draft["insurer_number"] = effective_insurer_number
    identity = manual_exam_entry_identity_from_draft(effective_draft)
    person_id_custom = identity.get("person_id_custom") or _manual_text(draft.get("person_id_custom"))
    identity_hash = identity.get("identity_hash")
    match_status = "MANUAL_CONFIRMED" if draft.get("subscriber_id") else "MANUAL_ENTRY"
    match_reason = "manual exam entry draft applied"
    if identity.get("reason"):
        match_reason = f"{match_reason}; {identity['reason']}"
    exam_facility_id = resolve_manual_exam_entry_facility_id(cur, draft)

    cur.execute(
        f"""
        INSERT INTO {qname(health_db())}.exam_ledgers (
          event_id,
          source_type,
          row_sha256,
          raw_row_json,
          subscriber_id,
          hia_subscriber_id,
          identity_hash,
          person_id_custom,
          subscriber_match_status,
          subscriber_match_method,
          subscriber_match_reason,
          document_id,
          facility_document_id,
          insurer_number,
          exam_facility_id,
          facility_code,
          facility_name,
          exam_date,
          postal_code,
          address,
          name_full_raw,
          name_kana_raw,
          name_kana_match,
          name_kana_export_value,
          name_kana_export_source,
          name_kana_export_reason,
          insurance_symbol_raw,
          insurance_symbol_match,
          insurance_symbol_export_value,
          insurance_symbol_export_source,
          insurance_symbol_export_reason,
          insurance_number_raw,
          insurance_number_match,
          insurance_number_export_value,
          insurance_number_export_source,
          insurance_number_export_reason,
          insurance_branch_number_raw,
          insurance_branch_number_match,
          birthdate,
          gender_code,
          gender_raw,
          basic_info_status,
          basic_info_reason,
          exam_item_status,
          exam_item_count,
          exam_item_error_count,
          exam_item_reason,
          xml_status,
          xml_reason,
          row_status,
          row_reason,
          check_status,
          xml_export_status,
          merge_status,
          merge_reason,
          source_created_at,
          source_updated_at
        ) VALUES (
          %s, %s, %s, CAST(%s AS JSON),
          %s, %s, %s, %s,
          %s, %s, %s,
          %s, %s, %s,
          %s, %s, %s, %s,
          %s, %s, %s, %s, %s, %s, 'MANUAL_ENTRY', 'manual exam entry draft applied',
          %s, %s, %s, 'MANUAL_ENTRY', 'manual exam entry draft applied',
          %s, %s, %s, 'MANUAL_ENTRY', 'manual exam entry draft applied',
          %s, %s,
          %s, %s, %s,
          'READY', 'manual exam entry draft applied',
          'READY', %s, 0, 'manual exam entry values applied',
          'READY', 'manual exam entry source',
          'READY', 'manual exam entry draft applied',
          'PENDING',
          'PENDING',
          'SOURCE_SINGLE',
          'manual exam entry source',
          CURRENT_TIMESTAMP(3),
          CURRENT_TIMESTAMP(3)
        )
        """,
        (
            event_id,
            source_type,
            row_sha256,
            json.dumps(row_payload, ensure_ascii=False, default=manual_exam_json_default),
            _optional_int(draft.get("subscriber_id")),
            _manual_text(draft.get("hia_subscriber_id")),
            identity_hash,
            person_id_custom,
            match_status,
            "manual_exam_entry",
            match_reason,
            document_id,
            _manual_text(draft.get("facility_document_id")),
            effective_insurer_number,
            exam_facility_id,
            _manual_text(draft.get("facility_code")),
            _manual_text(draft.get("facility_name")),
            _manual_date_text(draft.get("exam_date")),
            normalize_postal_code_export(draft.get("postal_code")),
            normalize_address_export(draft.get("address")),
            _manual_text(draft.get("name_full")),
            _manual_text(draft.get("name_kana")),
            _manual_text(draft.get("name_kana")),
            _manual_text(draft.get("name_kana")),
            _manual_text(draft.get("insurance_symbol")),
            _manual_text(draft.get("insurance_symbol")),
            _manual_text(draft.get("insurance_symbol")),
            _manual_text(draft.get("insurance_number")),
            _manual_text(draft.get("insurance_number")),
            _manual_text(draft.get("insurance_number")),
            _manual_text(draft.get("insurance_branch_number")),
            _manual_text(draft.get("insurance_branch_number")),
            _manual_date_text(draft.get("birthdate")),
            _manual_text(draft.get("gender_code")),
            _manual_text(draft.get("gender_code")),
            len(values),
        ),
    )
    exam_ledger_id = int(cur.lastrowid)

    rows: list[tuple[Any, ...]] = []
    for value in values:
        namecode = _manual_text(value.get("namecode")) or ""
        cur.execute(
            f"""
            SELECT
              item_name,
              xml_value_type,
              result_code_oid,
              display_unit,
              ucum_unit,
              xml_method_code,
              cda_section_code_default,
              identity_item_code,
              jun_no
            FROM {qname(dev_db())}.exam_item_master
            WHERE namecode = %s
            LIMIT 1
            """,
            (namecode,),
        )
        master = cur.fetchone() or {}
        section_code = _manual_text(master.get("cda_section_code_default"))
        raw_value_type = _manual_text(value.get("xml_value_type")) or _manual_text(master.get("xml_value_type")) or "ST"
        code_system = (
            _manual_text(value.get("code_system"))
            or (_manual_text(master.get("result_code_oid")) if raw_value_type.upper() in {"CD", "CO"} else None)
        )
        code_value = _manual_text(value.get("code_value"))
        code_display = _manual_text(value.get("code_display"))
        raw_value = _manual_text(value.get("raw_value")) or code_value
        is_code_value = raw_value_type.upper() in {"CD", "CO"}
        normalized_value = None if is_code_value else (_manual_text(value.get("normalized_value")) or raw_value)
        rows.append(
            (
                event_id,
                "EXAM",
                exam_ledger_id,
                _optional_int(draft.get("subscriber_id")),
                _manual_text(draft.get("hia_subscriber_id")),
                namecode,
                section_code,
                CDA_SECTION_CODE_SYSTEM if section_code else None,
                manual_exam_entry_section_name(section_code),
                _optional_int(value.get("occurrence_no")) or 1,
                raw_value,
                raw_value_type,
                _manual_text(value.get("display_unit")) or _manual_text(master.get("display_unit")),
                normalized_value,
                _manual_text(value.get("ucum_unit")) or _manual_text(master.get("ucum_unit")) or _manual_text(value.get("display_unit")),
                code_system,
                code_value,
                code_display,
                _manual_text(value.get("namecode_display_name")) or _manual_text(master.get("item_name")),
                _manual_text(value.get("identity_item_code")) or _manual_text(master.get("identity_item_code")),
                _optional_int(master.get("jun_no")),
                source_type,
                exam_ledger_id,
                "PRIMARY",
            )
        )

    cur.executemany(
        f"""
        INSERT INTO {qname(health_db())}.exam_item_values (
          event_id,
          ledger_type,
          ledger_id,
          subscriber_id,
          hia_subscriber_id,
          namecode,
          section_code,
          section_code_system,
          section_name,
          occurrence_no,
          raw_value,
          raw_value_type,
          raw_unit,
          normalized_value,
          normalized_unit,
          code_system,
          code_value,
          code_display,
          namecode_display_name,
          identity_item_code,
          jun_no,
          normalize_status,
          normalize_reason,
          validation_status,
          validation_reason,
          source_ledger_type,
          source_ledger_id,
          value_source_role,
          extracted_at,
          normalized_at
        ) VALUES (
          %s, %s, %s, %s, %s,
          %s, %s, %s, %s, %s,
          %s, %s, %s, %s, %s,
          %s, %s, %s, %s, %s,
          %s,
          'OK',
          'MANUAL_ENTRY',
          'VALID',
          NULL,
          %s, %s, %s,
          CURRENT_TIMESTAMP(3),
          CURRENT_TIMESTAMP(3)
        )
        """,
        rows,
    )

    cur.execute(
        f"""
        UPDATE {qname(health_db())}.manual_exam_entry_drafts
           SET draft_status = 'APPLIED',
               applied_by_app_user_id = %s,
               applied_at = CURRENT_TIMESTAMP(3),
               applied_exam_ledger_id = %s,
               updated_by_app_user_id = %s
         WHERE manual_exam_entry_draft_id = %s
        """,
        (user_id, exam_ledger_id, user_id, draft_id),
    )
    cur.execute(
        f"""
        INSERT INTO {qname(health_db())}.manual_exam_entry_draft_audit_logs (
          manual_exam_entry_draft_id,
          event_id,
          action_code,
          field_name,
          old_value,
          new_value,
          source,
          changed_by_app_user_id
        ) VALUES (
          %s,
          %s,
          'APPLY_TO_EXAM_LEDGER',
          'exam_ledger',
          NULL,
          %s,
          'ADMIN_UI',
          %s
        )
        """,
        (
            draft_id,
            event_id,
            json.dumps(
                {"exam_ledger_id": exam_ledger_id, "value_count": len(rows), "source_type": source_type},
                ensure_ascii=False,
            ),
            user_id,
        ),
    )
    return {"exam_ledger_id": exam_ledger_id, "value_count": len(rows), "source_type": source_type}


def revert_manual_exam_ledger_to_draft(
    cur: Any,
    *,
    exam_ledger_id: int,
    user: Mapping[str, Any],
) -> dict[str, Any]:
    user_id = int(user["app_user_id"])
    operator = str(user.get("employee_no") or user.get("display_name") or f"app_user_id:{user_id}")
    cur.execute(
        f"""
        SELECT
          el.exam_ledger_id,
          el.event_id,
          el.source_type,
          el.row_status,
          el.xml_export_status,
          d.manual_exam_entry_draft_id,
          d.draft_status,
          COALESCE(eiv.item_value_count, 0) AS item_value_count,
          COALESCE(src.case_source_count, 0) AS case_source_count,
          COALESCE(adopted.adopted_value_count, 0) AS adopted_value_count,
          COALESCE(listed.list_case_count, 0) AS list_case_count
        FROM {qname(health_db())}.exam_ledgers AS el
        LEFT JOIN {qname(health_db())}.manual_exam_entry_drafts AS d
          ON d.applied_exam_ledger_id = el.exam_ledger_id
        LEFT JOIN (
          SELECT ledger_id, COUNT(*) AS item_value_count
          FROM {qname(health_db())}.exam_item_values
          WHERE ledger_type = 'EXAM'
          GROUP BY ledger_id
        ) AS eiv
          ON eiv.ledger_id = el.exam_ledger_id
        LEFT JOIN (
          SELECT source_exam_ledger_id, COUNT(*) AS case_source_count
          FROM {qname(health_db())}.exam_export_case_sources
          WHERE source_status = 'ACTIVE'
          GROUP BY source_exam_ledger_id
        ) AS src
          ON src.source_exam_ledger_id = el.exam_ledger_id
        LEFT JOIN (
          SELECT source_exam_ledger_id, COUNT(*) AS adopted_value_count
          FROM {qname(health_db())}.exam_export_case_values
          GROUP BY source_exam_ledger_id
        ) AS adopted
          ON adopted.source_exam_ledger_id = el.exam_ledger_id
        LEFT JOIN (
          SELECT eecs.source_exam_ledger_id, COUNT(*) AS list_case_count
          FROM {qname(health_db())}.exam_export_case_sources AS eecs
          INNER JOIN {qname(health_db())}.ops_xml_export_list_cases AS oelc
            ON oelc.exam_export_case_id = eecs.exam_export_case_id
           AND oelc.removed_at IS NULL
          GROUP BY eecs.source_exam_ledger_id
        ) AS listed
          ON listed.source_exam_ledger_id = el.exam_ledger_id
        WHERE el.exam_ledger_id = %s
        LIMIT 1
        """,
        (exam_ledger_id,),
    )
    row = cur.fetchone()
    if row is None:
        raise ValueError("manual_exam_ledger_not_found")
    if str(row.get("source_type") or "").upper() not in {"PAPER", "MANUAL"}:
        raise ValueError("manual_exam_ledger_source_type_not_allowed")
    draft_id = _optional_int(row.get("manual_exam_entry_draft_id"))
    if draft_id is None:
        raise ValueError("manual_exam_ledger_has_no_draft")
    if str(row.get("draft_status") or "").upper() != "APPLIED":
        raise ValueError("manual_exam_draft_is_not_applied")

    cur.execute(
        f"""
        SELECT DISTINCT
          oelc.xml_export_list_case_id,
          oelc.xml_export_list_id,
          oelc.list_case_status,
          oel.list_name
        FROM {qname(health_db())}.exam_export_case_sources AS eecs
        INNER JOIN {qname(health_db())}.ops_xml_export_list_cases AS oelc
          ON oelc.exam_export_case_id = eecs.exam_export_case_id
         AND oelc.removed_at IS NULL
        INNER JOIN {qname(health_db())}.ops_xml_export_lists AS oel
          ON oel.xml_export_list_id = oelc.xml_export_list_id
        WHERE eecs.source_exam_ledger_id = %s
        ORDER BY oelc.xml_export_list_id, oelc.xml_export_list_case_id
        """,
        (exam_ledger_id,),
    )
    removed_list_refs = [dict(ref) for ref in cur.fetchall()]

    old_value = {
        "exam_ledger_id": exam_ledger_id,
        "row_status": row.get("row_status"),
        "xml_export_status": row.get("xml_export_status"),
        "case_source_count": int(row.get("case_source_count") or 0),
        "adopted_value_count": int(row.get("adopted_value_count") or 0),
        "item_value_count": int(row.get("item_value_count") or 0),
        "export_list_refs": removed_list_refs,
    }
    cur.execute(
        f"""
        UPDATE {qname(health_db())}.ops_xml_export_list_cases AS oelc
        INNER JOIN {qname(health_db())}.exam_export_case_sources AS eecs
          ON eecs.exam_export_case_id = oelc.exam_export_case_id
        SET oelc.list_case_status = 'REMOVED',
            oelc.removed_by = %s,
            oelc.removed_at = CURRENT_TIMESTAMP(3),
            oelc.remove_reason = %s,
            oelc.updated_at = CURRENT_TIMESTAMP(3)
        WHERE eecs.source_exam_ledger_id = %s
          AND oelc.removed_at IS NULL
        """,
        (
            operator,
            f"{LEDGER_REVERT_LIST_REMOVE_REASON_PREFIX} ledger_id={exam_ledger_id}",
            exam_ledger_id,
        ),
    )
    removed_list_case_count = int(cur.rowcount or 0)
    cur.execute(
        f"""
        UPDATE {qname(health_db())}.exam_export_case_sources
           SET source_status = 'REVERTED_TO_DRAFT',
               source_reason = 'manual exam ledger reverted to draft',
               updated_at = CURRENT_TIMESTAMP(3)
         WHERE source_exam_ledger_id = %s
        """,
        (exam_ledger_id,),
    )
    cur.execute(
        f"""
        DELETE FROM {qname(health_db())}.exam_export_case_values
         WHERE source_exam_ledger_id = %s
        """,
        (exam_ledger_id,),
    )
    cur.execute(
        f"""
        UPDATE {qname(health_db())}.exam_ledgers
           SET row_status = 'REVERTED_TO_DRAFT',
               row_reason = 'manual exam ledger reverted to draft',
               check_status = 'PENDING',
               check_reason = NULL,
               xml_export_status = 'PENDING',
               merge_status = 'REVERTED_TO_DRAFT',
               merge_reason = 'manual exam ledger reverted to draft',
               updated_at = CURRENT_TIMESTAMP(3)
         WHERE exam_ledger_id = %s
        """,
        (exam_ledger_id,),
    )
    cur.execute(
        f"""
        UPDATE {qname(health_db())}.manual_exam_entry_drafts
           SET draft_status = 'DRAFT',
               applied_by_app_user_id = NULL,
               applied_at = NULL,
               applied_exam_ledger_id = NULL,
               updated_by_app_user_id = %s
         WHERE manual_exam_entry_draft_id = %s
        """,
        (user_id, draft_id),
    )
    cur.execute(
        f"""
        INSERT INTO {qname(health_db())}.manual_exam_entry_draft_audit_logs (
          manual_exam_entry_draft_id,
          event_id,
          action_code,
          field_name,
          old_value,
          new_value,
          source,
          changed_by_app_user_id
        ) VALUES (
          %s,
          %s,
          'REVERT_FROM_EXAM_LEDGER',
          'exam_ledger',
          %s,
          %s,
          'ADMIN_UI',
          %s
        )
        """,
        (
            draft_id,
            _optional_int(row.get("event_id")) or 2,
            json.dumps(old_value, ensure_ascii=False, default=manual_exam_json_default),
            json.dumps(
                {
                    "draft_status": "DRAFT",
                    "exam_ledger_row_status": "REVERTED_TO_DRAFT",
                    "removed_export_list_cases": removed_list_case_count,
                },
                ensure_ascii=False,
            ),
            user_id,
        ),
    )
    return {
        "exam_ledger_id": exam_ledger_id,
        "manual_exam_entry_draft_id": draft_id,
        "item_value_count": int(row.get("item_value_count") or 0),
        "removed_export_list_case_count": removed_list_case_count,
        "removed_export_list_ids": sorted({int(ref["xml_export_list_id"]) for ref in removed_list_refs}),
    }


def update_manual_exam_entry_draft_from_basic(
    cur: Any,
    *,
    draft_id: int,
    event_id: int,
    basic: Mapping[str, Any],
    user_id: int,
) -> None:
    before = load_manual_exam_entry_draft_by_id(cur, draft_id)
    if before is None:
        raise ValueError("draft_not_found")
    subscriber_id = _optional_int(basic.get("subscriber_id"))
    insurer_number = _manual_text(basic.get("insurer_number")) or load_subscriber_insurer_number(
        cur,
        subscriber_id,
    )
    cur.execute(
        f"""
        UPDATE {qname(health_db())}.manual_exam_entry_drafts
           SET event_id = %s,
               entry_purpose = %s,
               exam_export_case_id = %s,
               subscriber_id = %s,
               hia_subscriber_id = %s,
               person_id_custom = %s,
               insurer_number = %s,
               insurance_symbol = %s,
               insurance_number = %s,
               insurance_branch_number = %s,
               postal_code = %s,
               address = %s,
               name_full = %s,
               name_kana = %s,
               birthdate = %s,
               gender_code = %s,
               exam_facility_id = %s,
               facility_code = %s,
               facility_name = %s,
               facility_document_id = %s,
               exam_date = %s,
               updated_by_app_user_id = %s
         WHERE manual_exam_entry_draft_id = %s
        """,
        (
            event_id,
            _manual_text(basic.get("entry_purpose")) or "PAPER_ONLY",
            _optional_int(basic.get("exam_export_case_id")),
            subscriber_id,
            _manual_text(basic.get("hia_subscriber_id")),
            _manual_text(basic.get("person_id_custom")),
            insurer_number,
            _manual_text(basic.get("insurance_symbol")),
            _manual_text(basic.get("insurance_number")),
            _manual_text(basic.get("insurance_branch_number")),
            normalize_postal_code_export(basic.get("postal_code") or basic.get("subscriber_postal_code")),
            normalize_address_export(basic.get("address") or basic.get("subscriber_address_line")),
            _manual_text(basic.get("name_full")),
            _manual_text(basic.get("name_kana")),
            _manual_date_text(basic.get("birthdate")),
            _manual_text(basic.get("gender_code") or basic.get("gender_label") or basic.get("gender")),
            _optional_int(basic.get("exam_facility_id")),
            _manual_text(basic.get("facility_code")),
            _manual_text(basic.get("facility_name")),
            _manual_text(basic.get("facility_document_id")),
            _manual_date_text(basic.get("exam_date")),
            user_id,
            draft_id,
        ),
    )
    cur.execute(
        f"""
        INSERT INTO {qname(health_db())}.manual_exam_entry_draft_audit_logs (
          manual_exam_entry_draft_id,
          event_id,
          action_code,
          field_name,
          old_value,
          new_value,
          source,
          changed_by_app_user_id
        ) VALUES (
          %s,
          %s,
          'UPDATE_DRAFT_BASIC',
          'draft_basic',
          %s,
          %s,
          'ADMIN_UI',
          %s
        )
        """,
        (
            draft_id,
            event_id,
            json.dumps(before, ensure_ascii=False, default=manual_exam_json_default),
            json.dumps(basic, ensure_ascii=False, default=manual_exam_json_default),
            user_id,
        ),
    )


def replace_manual_exam_entry_draft_values(
    cur: Any,
    *,
    draft_id: int,
    event_id: int,
    values: Sequence[Mapping[str, Any]],
    user_id: int,
) -> int:
    cur.execute(
        f"""
        DELETE FROM {qname(health_db())}.manual_exam_entry_draft_values
         WHERE manual_exam_entry_draft_id = %s
        """,
        (draft_id,),
    )
    rows: list[tuple[Any, ...]] = []
    for value in values:
        raw_value = _manual_text(value.get("raw_value"))
        code_value = _manual_text(value.get("code_value"))
        if not raw_value and not code_value:
            continue
        rows.append(
            (
                draft_id,
                _manual_text(value.get("namecode")) or "",
                _manual_text(value.get("namecode_display_name")),
                _manual_text(value.get("identity_item_code")),
                _manual_text(value.get("identity_item_name")),
                _manual_text(value.get("xml_value_type")),
                raw_value or code_value,
                None
                if str(value.get("xml_value_type") or "").upper() in {"CD", "CO"}
                else (_manual_text(value.get("normalized_value")) or raw_value or code_value),
                _manual_text(value.get("code_system")),
                code_value,
                _manual_text(value.get("code_display")),
                _manual_text(value.get("display_unit")),
                _manual_text(value.get("ucum_unit")),
                _manual_text(value.get("method_code")),
                _manual_text(value.get("method_name")),
                _optional_int(value.get("occurrence_no")) or 1,
                1,
                "DRAFT",
                _manual_text(value.get("note")),
                user_id,
            )
        )
    if rows:
        cur.executemany(
            f"""
            INSERT INTO {qname(health_db())}.manual_exam_entry_draft_values (
              manual_exam_entry_draft_id,
              namecode,
              namecode_display_name,
              identity_item_code,
              identity_item_name,
              xml_value_type,
              raw_value,
              normalized_value,
              code_system,
              code_value,
              code_display,
              display_unit,
              ucum_unit,
              method_code,
              method_name,
              occurrence_no,
              include_flag,
              input_status,
              note,
              updated_by_app_user_id
            ) VALUES (
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            rows,
        )
    cur.execute(
        f"""
        UPDATE {qname(health_db())}.manual_exam_entry_drafts
           SET updated_by_app_user_id = %s
         WHERE manual_exam_entry_draft_id = %s
        """,
        (user_id, draft_id),
    )
    cur.execute(
        f"""
        INSERT INTO {qname(health_db())}.manual_exam_entry_draft_audit_logs (
          manual_exam_entry_draft_id,
          event_id,
          action_code,
          field_name,
          old_value,
          new_value,
          source,
          changed_by_app_user_id
        ) VALUES (
          %s,
          %s,
          'SAVE_VALUES',
          'draft_values',
          NULL,
          %s,
          'ADMIN_UI',
          %s
        )
        """,
        (
            draft_id,
            event_id,
            json.dumps({"value_count": len(rows)}, ensure_ascii=False),
            user_id,
        ),
    )
    return len(rows)


def summarize_manual_exam_entry_drafts(rows: list[dict[str, Any]]) -> dict[str, int]:
    summary = {
        "total": len(rows),
        "draft": 0,
        "ready": 0,
        "applied": 0,
        "error": 0,
        "duplicate": 0,
        "duplicate_groups": 0,
    }
    for row in rows:
        status = str(row.get("draft_status") or "DRAFT").upper()
        if status == "READY":
            summary["ready"] += 1
        elif status == "APPLIED":
            summary["applied"] += 1
        elif status in {"ERROR", "FAILED"}:
            summary["error"] += 1
        else:
            summary["draft"] += 1
    duplicate_group_keys = {
        str(row.get("draft_duplicate_group") or "")
        for row in rows
        if int(row.get("draft_duplicate_count") or 0) > 1
    }
    summary["duplicate"] = sum(1 for row in rows if int(row.get("draft_duplicate_count") or 0) > 1)
    summary["duplicate_groups"] = len(duplicate_group_keys)
    return summary


def manual_exam_draft_identity_keys(row: Mapping[str, Any]) -> tuple[str, ...]:
    event_id = str(row.get("event_id") or "")
    keys: list[str] = []

    def add(kind: str, *values: Any) -> None:
        normalized = tuple(re.sub(r"[\s　]+", "", str(value or "")).casefold() for value in values)
        if all(normalized):
            keys.append(f"{event_id}:{kind}:" + ":".join(normalized))

    add("subscriber", row.get("subscriber_id"))
    add("hia", row.get("hia_subscriber_id"))
    add("person", row.get("person_id_custom"))
    add(
        "insurance",
        row.get("insurer_number"),
        row.get("insurance_symbol") or "-",
        row.get("insurance_number"),
        row.get("insurance_branch_number") or "-",
        row.get("birthdate"),
    )
    add("kana_birth", row.get("name_kana"), row.get("birthdate"))
    return tuple(dict.fromkeys(keys))


def annotate_manual_exam_draft_duplicates(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    parent = list(range(len(rows)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    first_by_key: dict[str, int] = {}
    for index, row in enumerate(rows):
        for identity_key in manual_exam_draft_identity_keys(row):
            first_index = first_by_key.setdefault(identity_key, index)
            union(index, first_index)

    groups: dict[int, list[int]] = {}
    for index in range(len(rows)):
        groups.setdefault(find(index), []).append(index)
    for indexes in groups.values():
        minimum_draft_id = min(int(rows[index]["manual_exam_entry_draft_id"]) for index in indexes)
        duplicate_refs = [
            {
                "manual_exam_entry_draft_id": int(rows[index]["manual_exam_entry_draft_id"]),
                "draft_status": str(rows[index].get("draft_status") or "DRAFT"),
                "name": str(rows[index].get("name_kana") or rows[index].get("name_full") or "氏名未設定"),
            }
            for index in indexes
        ]
        for index in indexes:
            current_id = int(rows[index]["manual_exam_entry_draft_id"])
            rows[index]["draft_duplicate_group"] = f"draft:{minimum_draft_id}"
            rows[index]["draft_duplicate_count"] = len(indexes)
            rows[index]["draft_duplicate_refs"] = [
                ref for ref in duplicate_refs if ref["manual_exam_entry_draft_id"] != current_id
            ]


def filter_manual_exam_entry_draft_rows(
    rows: Sequence[dict[str, Any]],
    *,
    status_filter: str,
    hia_subscriber_id: str,
    case_id: str,
    duplicates_only: bool,
    limit: int = 200,
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    hia_query = hia_subscriber_id.strip().casefold()
    case_query = case_id.strip().casefold()
    for row in rows:
        status = str(row.get("draft_status") or "DRAFT").upper()
        if status_filter == "ERROR":
            if status not in {"ERROR", "FAILED"}:
                continue
        elif status_filter and status != status_filter:
            continue
        if hia_query and hia_query not in str(row.get("hia_subscriber_id") or "").casefold():
            continue
        if case_query and case_query not in str(row.get("exam_export_case_id") or "").casefold():
            continue
        if duplicates_only and int(row.get("draft_duplicate_count") or 0) <= 1:
            continue
        filtered.append(row)
        if len(filtered) >= limit:
            break
    return filtered


def load_manual_exam_article44_flags(cur: Any, rows: list[dict[str, Any]]) -> dict[str, list[dict[str, str]]]:
    namecodes = sorted({str(row.get("namecode") or "").strip() for row in rows if str(row.get("namecode") or "").strip()})
    if not namecodes:
        return {}

    placeholders = ", ".join(["%s"] * len(namecodes))
    cur.execute(
        f"""
        SELECT
          namecode,
          value_type,
          notes
        FROM {qname(dev_db())}.exam_item_group_members
        WHERE group_code = %s
          AND namecode IN ({placeholders})
        ORDER BY
          priority,
          namecode,
          value_type
        """,
        (ARTICLE44_GROUP_CODE, *namecodes),
    )

    flags: dict[str, list[dict[str, str]]] = {}
    seen: set[tuple[str, str]] = set()
    for row in cur.fetchall():
        namecode = str(row.get("namecode") or "").strip()
        notes = str(row.get("notes") or "")
        match = re.search(r"Article44\s+(?P<detail_no>44\d{8})\s*:\s*(?P<detail_name>.+)", notes)
        if not namecode or not match:
            continue
        detail_no = match.group("detail_no")
        detail_name = match.group("detail_name").strip()
        key = (namecode, detail_no)
        if key in seen:
            continue
        seen.add(key)
        flags.setdefault(namecode, []).append(
            {
                "detail_no": detail_no,
                "detail_name": detail_name,
                "label": f"{detail_no} / {detail_name}" if detail_name else detail_no,
            }
        )
    return flags


def load_manual_exam_cd_options(cur: Any, rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    result_code_oids = sorted(
        {
            str(row.get("result_code_oid") or "").strip()
            for row in rows
            if str(row.get("xml_value_type") or "").upper() == "CD"
            and str(row.get("result_code_oid") or "").strip()
        }
    )
    if not result_code_oids:
        return {}

    placeholders = ", ".join(["%s"] * len(result_code_oids))
    cur.execute(
        f"""
        SELECT
          result_code_oid,
          normalized_code,
          display_name,
          priority,
          variant_id
        FROM {qname(master_db())}.norm_variants
        WHERE is_active = 1
          AND is_canonical = 1
          AND result_code_oid IN ({placeholders})
          AND normalized_code IS NOT NULL
          AND normalized_code <> ''
          AND normalized_code <> '<<CODE>>'
        ORDER BY
          result_code_oid,
          CASE
            WHEN normalized_code REGEXP '^[0-9]+$' THEN CAST(normalized_code AS UNSIGNED)
            ELSE 999999
          END,
          normalized_code,
          priority,
          variant_id
        """,
        tuple(result_code_oids),
    )

    options_by_oid: dict[str, list[dict[str, Any]]] = {}
    seen: set[tuple[str, str]] = set()
    for row in cur.fetchall():
        item = dict(row)
        oid = str(item.get("result_code_oid") or "").strip()
        code = str(item.get("normalized_code") or "").strip()
        if not oid or not code:
            continue
        key = (oid, code)
        if key in seen:
            continue
        seen.add(key)
        label = str(item.get("display_name") or "").strip() or code
        options_by_oid.setdefault(oid, []).append(
            {
                "code": code,
                "label": label,
                "option_label": f"{code}: {label}" if label != code else code,
            }
        )
    return options_by_oid


def group_manual_exam_entry_items(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    group_index: dict[str, dict[str, Any]] = {}
    item_index: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    item_names_by_method_key: dict[tuple[str, str, str], set[str]] = {}
    for row in rows:
        category = str(row.get("category_name") or "未分類")
        identity_key = str(row.get("identity_item_code") or row.get("namecode") or "")
        value_type = str(row.get("xml_value_type") or "")
        item_name = str(row.get("item_name") or "")
        if category and identity_key and value_type:
            item_names_by_method_key.setdefault((category, identity_key, value_type), set()).add(item_name)
    for row in rows:
        category = str(row.get("category_name") or "未分類")
        if category not in group_index:
            group = {"category_name": category, "items": []}
            group_index[category] = group
            groups.append(group)
        identity_key = str(row.get("identity_item_code") or row.get("namecode") or "")
        value_type = str(row.get("xml_value_type") or "")
        entry_identity_key = manual_exam_entry_identity_key(row, identity_key=identity_key)
        expand_methods = manual_exam_should_expand_method_rows(
            row,
            same_identity_item_names=item_names_by_method_key.get((category, identity_key, value_type), set()),
        )
        entry_key = str(row.get("namecode") if expand_methods else entry_identity_key)
        item_key = (category, entry_key, value_type, str(row.get("namecode") or "") if expand_methods else "")
        method_option = {
            "namecode": str(row.get("namecode") or ""),
            "method_name": str(row.get("method_name") or ""),
            "xml_method_code": str(row.get("xml_method_code") or ""),
        }
        if item_key not in item_index:
            item = dict(row)
            item["manual_entry_key"] = entry_key
            item["manual_filter_text"] = " ".join(
                str(row.get(field) or "")
                for field in (
                    "namecode",
                    "item_name",
                    "category_name",
                    "identity_item_code",
                    "identity_item_name",
                    "method_name",
                    "xml_method_code",
                    "result_code_oid",
                )
            )
            item["manual_method_options"] = [method_option]
            item["manual_method_option_count"] = 1
            item["manual_method_required"] = False
            item["manual_st_required_hint"] = False
            item_index[item_key] = item
            group_index[category]["items"].append(item)
        else:
            item = item_index[item_key]
            item["manual_method_options"].append(method_option)
            item["manual_method_option_count"] = len(item["manual_method_options"])
            item["manual_method_required"] = True
            filter_parts = [
                str(item.get("manual_filter_text") or ""),
                str(row.get("namecode") or ""),
                str(row.get("item_name") or ""),
                str(row.get("method_name") or ""),
                str(row.get("xml_method_code") or ""),
            ]
            item["manual_filter_text"] = " ".join(part for part in filter_parts if part)
    for item in item_index.values():
        item["manual_method_options"].sort(key=manual_exam_method_option_sort_key)
        if item["manual_method_options"]:
            default_method = item["manual_method_options"][0]
            item["method_name"] = default_method.get("method_name") or item.get("method_name")
            item["xml_method_code"] = default_method.get("xml_method_code") or item.get("xml_method_code")
    for group in groups:
        items_by_entry_key: dict[str, list[dict[str, Any]]] = {}
        for item in group["items"]:
            items_by_entry_key.setdefault(str(item.get("manual_entry_key") or ""), []).append(item)
        for item in group["items"]:
            if str(item.get("xml_value_type") or "").upper() != "ST":
                continue
            key = str(item.get("manual_entry_key") or "")
            has_related_cd = any(
                str(other.get("xml_value_type") or "").upper() == "CD"
                for other in items_by_entry_key.get(key, [])
            )
            item["manual_st_required_hint"] = has_related_cd
    return groups


def manual_exam_should_expand_method_rows(row: dict[str, Any], *, same_identity_item_names: set[str]) -> bool:
    if manual_exam_time_series_key(row):
        return False
    if len({name for name in same_identity_item_names if name}) > 1:
        return True
    text = " ".join(
        str(row.get(field) or "")
        for field in ("item_name", "identity_item_name", "category_name", "method_name")
    )
    return "血圧" in text or "心電図" in text


def manual_exam_time_series_key(row: dict[str, Any]) -> str:
    text = " ".join(
        str(row.get(field) or "")
        for field in ("item_name", "identity_item_name")
    )
    if "空腹" in text:
        return "FASTING"
    if "随時" in text:
        return "RANDOM"
    return ""


def manual_exam_entry_identity_key(row: dict[str, Any], *, identity_key: str) -> str:
    time_series_key = manual_exam_time_series_key(row)
    if time_series_key:
        return f"{identity_key}:{time_series_key}"
    return identity_key


def manual_exam_is_blood_collection_time(row: dict[str, Any]) -> bool:
    namecode = str(row.get("namecode") or "")
    identity_code = str(row.get("identity_item_code") or "")
    item_name = str(row.get("item_name") or "")
    return namecode.startswith("9N141") or identity_code == "9N141" or "採血時間" in item_name


def manual_exam_method_option_sort_key(option: dict[str, Any]) -> tuple[int, str, str]:
    method_name = str(option.get("method_name") or "")
    xml_method_code = str(option.get("xml_method_code") or "")
    preferred = any(keyword in method_name for keyword in ("一般", "その他"))
    return (0 if preferred else 1, method_name, xml_method_code)


def manual_exam_input_type(xml_value_type: Any) -> str:
    value_type = str(xml_value_type or "").upper()
    if value_type == "PQ":
        return "number"
    return "text"


def json_int(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, Decimal):
        return int(value)
    return int(value or 0)


@app.get("/api/manual-exam-entry/subscribers")
def manual_exam_entry_subscribers(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse({"items": []}, status_code=401)
    if not can_edit_manual_exam_entry(user):
        return JSONResponse({"items": []}, status_code=403)

    event_id = request.query_params.get("event_id", "2")
    query = request.query_params.get("q", "").strip()
    filters = {
        "name_kana": request.query_params.get("name_kana", "").strip(),
        "insurance_symbol": request.query_params.get("insurance_symbol", "").strip(),
        "insurance_number": request.query_params.get("insurance_number", "").strip(),
        "employee_code": request.query_params.get("employee_code", "").strip(),
    }
    if not query and not any(filters.values()):
        return JSONResponse({"items": []})

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=dev_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            rows = load_subscriber_match_candidate_rows(
                cur,
                ledger=None,
                event_id=event_id,
                query=query,
                candidate_filters=filters,
                limit=50,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    items: list[dict[str, Any]] = []
    for row in rows:
        items.append(
            {
                "subscriber_id": row.get("subscriber_id"),
                "hia_subscriber_id": row.get("hia_subscriber_id"),
                "person_id_custom": row.get("person_id_custom"),
                "name_full": row.get("name_kanji_full"),
                "name_kana": row.get("name_kana_full"),
                "birth": str(row.get("birth") or ""),
                "gender_code": row.get("gender_code"),
                "gender_label": gender_code_label(row.get("gender_code")),
                "insurer_number": row.get("insurer_number"),
                "insurance_symbol": row.get("insurance_symbol_export") or row.get("insurance_symbol"),
                "insurance_number": row.get("insurance_number"),
                "insurance_branch_number": row.get("insurance_branchnumber"),
                "postal_code": row.get("subscriber_postal_code"),
                "employee_code": row.get("employee_code"),
                "relationship_name": row.get("relationship_name"),
                "qualification_lost_date": str(row.get("qualification_lost_date") or ""),
                "hia_dashboard_status": row.get("hia_dashboard_status"),
                "hia_dashboard_medical_institution": row.get("hia_dashboard_medical_institution"),
                "hia_dashboard_reservation_date": str(row.get("hia_dashboard_reservation_date") or ""),
                "hia_dashboard_exam_date": str(row.get("hia_dashboard_exam_date") or ""),
                "hia_dashboard_course_name": row.get("hia_dashboard_course_name"),
                "candidate_case_count": row.get("candidate_case_count") or 0,
                "candidate_latest_case_id": row.get("candidate_latest_case_id"),
                "candidate_latest_case_facility_name": row.get("candidate_latest_case_facility_name"),
                "candidate_latest_case_exam_date": str(row.get("candidate_latest_case_exam_date") or ""),
                "candidate_latest_case_export_readiness_status": row.get("candidate_latest_case_export_readiness_status"),
            }
        )
    return JSONResponse({"items": items})


@app.get("/api/manual-exam-entry/cases")
def manual_exam_entry_cases(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse({"items": []}, status_code=401)
    if not can_edit_manual_exam_entry(user):
        return JSONResponse({"items": []}, status_code=403)

    event_id = _optional_int(request.query_params.get("event_id")) or 2
    subscriber_id = _optional_int(request.query_params.get("subscriber_id"))
    if subscriber_id is None:
        return JSONResponse({"items": []})

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            rows = load_manual_exam_entry_cases_for_subscriber(
                cur,
                event_id=event_id,
                subscriber_id=subscriber_id,
                limit=10,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    for row in rows:
        row["exam_date"] = str(row.get("exam_date") or "")
        row["legal_reason_summary"] = str(row.get("legal_reason_summary") or "")
        row["specific_reason_summary"] = str(row.get("specific_reason_summary") or "")
    return JSONResponse({"items": rows})


@app.get("/api/manual-exam-entry/case-candidates")
def manual_exam_entry_case_candidates(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse({"items": []}, status_code=401)
    if not can_edit_manual_exam_entry(user):
        return JSONResponse({"items": []}, status_code=403)

    filters = {
        "event_id": request.query_params.get("event_id", "2"),
        "facility_q": request.query_params.get("facility_q", "").strip(),
        "facility_codes": request.query_params.get("facility_codes", "").strip(),
        "exam_month": request.query_params.get("exam_month", "").strip(),
        "name_full": request.query_params.get("name_full", "").strip(),
        "name_kana": request.query_params.get("name_kana", "").strip(),
        "hia_subscriber_id": request.query_params.get("hia_subscriber_id", "").strip(),
        "insurance_symbol": request.query_params.get("insurance_symbol", "").strip(),
        "insurance_number": request.query_params.get("insurance_number", "").strip(),
        "qualification_lost_status": request.query_params.get("qualification_lost_status", "").strip(),
        "qualification_lost_date": request.query_params.get("qualification_lost_date", "").strip(),
        "limit": request.query_params.get("limit", "50").strip(),
    }
    if not any(
        filters[key]
        for key in (
            "facility_q",
            "facility_codes",
            "exam_month",
            "name_full",
            "name_kana",
            "hia_subscriber_id",
            "insurance_number",
            "qualification_lost_status",
            "qualification_lost_date",
        )
    ):
        return JSONResponse({"items": []})
    limit = parse_positive_int(filters["limit"], default=50, maximum=500)

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            rows = load_exam_export_case_rows(cur, filters=filters, limit=limit, offset=0)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    items: list[dict[str, Any]] = []
    for row in rows:
        items.append(
            {
                "exam_export_case_id": row.get("exam_export_case_id"),
                "event_id": row.get("event_id"),
                "subscriber_id": row.get("subscriber_id"),
                "hia_subscriber_id": row.get("hia_subscriber_id"),
                "person_id_custom": row.get("person_id_custom"),
                "name_full": row.get("name_full_raw"),
                "name_kana": row.get("name_kana_raw"),
                "birthdate": str(row.get("birthdate") or ""),
                "gender_code": row.get("gender_code"),
                "gender_label": gender_code_label(row.get("gender_code")),
                "insurer_number": row.get("insurer_number"),
                "insurance_symbol": row.get("insurance_symbol_export_value") or row.get("insurance_symbol_raw"),
                "insurance_number": row.get("insurance_number_export_value") or row.get("insurance_number_raw"),
                "insurance_branch_number": row.get("insurance_branch_number_export_value")
                or row.get("insurance_branch_number_raw"),
                "postal_code": row.get("postal_code_completed_value") or row.get("postal_code"),
                "facility_code": row.get("facility_code"),
                "facility_name": row.get("facility_name"),
                "expected_source_mode": row.get("expected_source_mode"),
                "expected_source_mode_label": source_mode_label(row.get("expected_source_mode")),
                "exam_date": str(row.get("exam_date") or ""),
                "source_mode": row.get("source_mode"),
                "case_value_count": json_int(row.get("case_value_count")),
                "source_count": json_int(row.get("source_count")),
                "xml_count": json_int(row.get("xml_count")),
                "csv_count": json_int(row.get("csv_count")),
                "paper_count": json_int(row.get("paper_count")),
                "legal_check_result": row.get("legal_check_result") or "PENDING",
                "legal_reason_summary": row.get("legal_reason_summary") or "",
                "specific_check_result": row.get("specific_check_result") or "PENDING",
                "specific_reason_summary": row.get("specific_reason_summary") or "",
                "export_readiness_status": row.get("export_readiness_status"),
                "xml_export_status": row.get("xml_export_status"),
            }
        )
    return JSONResponse({"items": items})


@app.post("/api/manual-exam-entry-drafts/from-person")
async def create_manual_exam_entry_draft_from_person(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse({"message": "ログインしてください。"}, status_code=401)
    if not can_edit_manual_exam_entry(user):
        return JSONResponse({"message": "権限がありません。"}, status_code=403)

    payload = await request.json()
    person = payload.get("person") if isinstance(payload, dict) else None
    if not isinstance(person, dict):
        return JSONResponse({"message": "加入者情報がありません。"}, status_code=400)
    event_id = _optional_int(payload.get("event_id") or person.get("event_id")) or 2

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            if not manual_exam_entry_table_exists(cur, health_db(), "manual_exam_entry_drafts"):
                return JSONResponse({"message": "仮登録テーブルが未適用です。"}, status_code=400)
            draft_id = insert_manual_exam_entry_draft(
                cur,
                event_id=event_id,
                entry_purpose="PAPER_ONLY",
                action_code="CREATE_FROM_PERSON",
                source_payload=person,
                user_id=int(user["app_user_id"]),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return JSONResponse(
        {
            "draft_id": draft_id,
            "redirect_url": f"/manual-exam-entry?draft_id={draft_id}",
            "message": f"仮登録 draft {draft_id} を作成しました。",
        }
    )


@app.post("/api/manual-exam-entry-drafts/from-case")
async def create_manual_exam_entry_draft_from_case(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse({"message": "ログインしてください。"}, status_code=401)
    if not can_edit_manual_exam_entry(user):
        return JSONResponse({"message": "権限がありません。"}, status_code=403)

    payload = await request.json()
    case = payload.get("case") if isinstance(payload, dict) else None
    if not isinstance(case, dict):
        return JSONResponse({"message": "case情報がありません。"}, status_code=400)
    event_id = _optional_int(payload.get("event_id") or case.get("event_id")) or 2

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            if not manual_exam_entry_table_exists(cur, health_db(), "manual_exam_entry_drafts"):
                return JSONResponse({"message": "仮登録テーブルが未適用です。"}, status_code=400)
            draft_id = insert_manual_exam_entry_draft(
                cur,
                event_id=event_id,
                entry_purpose="SUPPLEMENT",
                action_code="CREATE_FROM_CASE",
                source_payload=case,
                user_id=int(user["app_user_id"]),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return JSONResponse(
        {
            "draft_id": draft_id,
            "redirect_url": f"/manual-exam-entry?draft_id={draft_id}",
            "message": f"仮登録 draft {draft_id} を作成しました。",
        }
    )


@app.post("/api/manual-exam-entry-drafts/{draft_id}/basic-info")
async def update_manual_exam_entry_draft_basic_info_api(draft_id: int, request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse({"message": "ログインしてください。"}, status_code=401)
    if not can_edit_manual_exam_entry(user):
        return JSONResponse({"message": "権限がありません。"}, status_code=403)

    payload = await request.json()
    if not isinstance(payload, dict):
        return JSONResponse({"message": "基本情報がありません。"}, status_code=400)
    facility_code = _manual_text(payload.get("facility_code"))
    facility_name = _manual_text(payload.get("facility_name"))
    exam_date = _manual_date_text(payload.get("exam_date"))
    event_id = _optional_int(payload.get("event_id")) or 2
    if not facility_name:
        facility_name = facility_code
    if not facility_code and not exam_date:
        return JSONResponse({"message": "健診機関または健診実施日を指定してください。"}, status_code=400)

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            if not manual_exam_entry_table_exists(cur, health_db(), "manual_exam_entry_drafts"):
                return JSONResponse({"message": "仮登録テーブルが未適用です。"}, status_code=400)
            try:
                update_manual_exam_entry_draft_basic_info(
                    cur,
                    draft_id=draft_id,
                    event_id=event_id,
                    facility_code=facility_code,
                    facility_name=facility_name,
                    exam_date=exam_date,
                    user_id=int(user["app_user_id"]),
                )
            except ValueError:
                return JSONResponse({"message": "対象の仮登録が見つかりません。"}, status_code=404)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return JSONResponse({"message": f"draft {draft_id} の基本情報を更新しました。"})


@app.post("/api/manual-exam-entry-drafts/{draft_id}/delete")
async def delete_manual_exam_entry_draft_api(draft_id: int, request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse({"message": "ログインしてください。"}, status_code=401)
    if not can_manage_manual_exam_entry(user):
        return JSONResponse({"message": "権限がありません。"}, status_code=403)

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            if not manual_exam_entry_table_exists(cur, health_db(), "manual_exam_entry_drafts"):
                return JSONResponse({"message": "仮登録テーブルが未適用です。"}, status_code=400)
            try:
                delete_manual_exam_entry_draft(cur, draft_id=draft_id)
            except ValueError as exc:
                if str(exc) == "draft_already_applied":
                    return JSONResponse({"message": "本データ反映済みの仮登録は削除できません。"}, status_code=400)
                return JSONResponse({"message": "対象の仮登録が見つかりません。"}, status_code=404)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return JSONResponse({"message": f"draft {draft_id} の仮登録を削除しました。"})


@app.post("/api/manual-exam-entry-drafts/{draft_id}/apply")
async def apply_manual_exam_entry_draft_api(draft_id: int, request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse({"message": "ログインしてください。"}, status_code=401)
    if not can_manage_manual_exam_entry(user):
        return JSONResponse({"message": "権限がありません。"}, status_code=403)

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            for table_name in (
                "manual_exam_entry_drafts",
                "manual_exam_entry_draft_values",
                "manual_exam_entry_draft_audit_logs",
                "exam_ledgers",
                "exam_item_values",
            ):
                if not manual_exam_entry_table_exists(cur, health_db(), table_name):
                    return JSONResponse({"message": f"{table_name} テーブルが未適用です。"}, status_code=400)
            try:
                result = apply_manual_exam_entry_draft(
                    cur,
                    draft_id=draft_id,
                    user_id=int(user["app_user_id"]),
                )
            except ValueError as exc:
                error_code = str(exc)
                if error_code == "draft_already_applied":
                    return JSONResponse({"message": "この仮登録はすでに本データ反映済みです。"}, status_code=400)
                if error_code == "draft_has_no_values":
                    return JSONResponse({"message": "入力済みの検査値がないため本データ反映できません。"}, status_code=400)
                return JSONResponse({"message": "対象の仮登録が見つかりません。"}, status_code=404)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return JSONResponse(
        {
            "message": (
                f"draft {draft_id} を本データ反映しました。"
                f" ledger {result['exam_ledger_id']} / {result['value_count']}件"
            ),
            **result,
        }
    )


@app.post("/api/manual-exam-entry-drafts/{draft_id}/check")
async def check_manual_exam_entry_draft_api(draft_id: int, request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse({"message": "ログインしてください。"}, status_code=401)
    if not can_edit_manual_exam_entry(user):
        return JSONResponse({"message": "権限がありません。"}, status_code=403)

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            for table_name in (
                "manual_exam_entry_drafts",
                "manual_exam_entry_draft_values",
                "manual_exam_entry_draft_check_results",
            ):
                if not manual_exam_entry_table_exists(cur, health_db(), table_name):
                    return JSONResponse({"message": f"{table_name} テーブルが未適用です。"}, status_code=400)
            try:
                result = check_manual_exam_entry_draft(
                    cur,
                    draft_id=draft_id,
                    user_id=int(user["app_user_id"]),
                )
            except ValueError as exc:
                error_code = str(exc)
                if error_code == "draft_has_no_values":
                    return JSONResponse({"message": "入力済みの検査値がないため参考チェックできません。"}, status_code=400)
                if error_code == "draft_check_table_not_found":
                    return JSONResponse({"message": "仮登録チェック結果テーブルが未適用です。"}, status_code=400)
                return JSONResponse({"message": "対象の仮登録が見つかりません。"}, status_code=404)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return JSONResponse(
        {
            "message": (
                f"draft {draft_id} の参考チェックを実行しました。"
                f" 法定 {result['legal_check_result']} / 特定 {check_result_label(result['specific_check_result'])}"
            ),
            **result,
        }
    )


@app.post("/api/manual-exam-entry-drafts/check")
async def check_manual_exam_entry_drafts_api(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse({"message": "ログインしてください。"}, status_code=401)
    if not can_edit_manual_exam_entry(user):
        return JSONResponse({"message": "権限がありません。"}, status_code=403)

    payload = await request.json()
    draft_ids_raw = payload.get("draft_ids") if isinstance(payload, dict) else None
    draft_ids = [
        int(draft_id)
        for draft_id in (draft_ids_raw if isinstance(draft_ids_raw, list) else [])
        if _optional_int(draft_id) is not None
    ]
    draft_ids = list(dict.fromkeys(draft_ids))
    if not draft_ids:
        return JSONResponse({"message": "参考チェック対象がありません。"}, status_code=400)

    checked = 0
    errors: list[dict[str, Any]] = []
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            for table_name in (
                "manual_exam_entry_drafts",
                "manual_exam_entry_draft_values",
                "manual_exam_entry_draft_check_results",
            ):
                if not manual_exam_entry_table_exists(cur, health_db(), table_name):
                    return JSONResponse({"message": f"{table_name} テーブルが未適用です。"}, status_code=400)
            for draft_id in draft_ids:
                try:
                    check_manual_exam_entry_draft(cur, draft_id=draft_id, user_id=int(user["app_user_id"]))
                    checked += 1
                except ValueError as exc:
                    errors.append({"draft_id": draft_id, "error": str(exc)})
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return JSONResponse(
        {
            "message": f"参考チェック checked={checked} errors={len(errors)}",
            "checked": checked,
            "errors": errors,
        }
    )


@app.get("/api/manual-exam-entry-drafts/{draft_id}/case-match")
def manual_exam_entry_draft_case_match_api(draft_id: int, request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse({"message": "ログインしてください。"}, status_code=401)
    if not can_edit_manual_exam_entry(user):
        return JSONResponse({"message": "権限がありません。"}, status_code=403)

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        if not manual_exam_entry_table_exists(cur, health_db(), "manual_exam_entry_drafts"):
            return JSONResponse({"message": "仮登録テーブルが未適用です。"}, status_code=400)
        result = build_manual_exam_draft_case_match_check(cur, draft_id=draft_id)
        if result is None:
            return JSONResponse({"message": "対象の仮登録が見つかりません。"}, status_code=404)
    return JSONResponse(result)


@app.post("/api/manual-exam-entry-drafts/save")
async def save_manual_exam_entry_draft_api(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse({"message": "ログインしてください。"}, status_code=401)
    if not can_edit_manual_exam_entry(user):
        return JSONResponse({"message": "権限がありません。"}, status_code=403)

    payload = await request.json()
    if not isinstance(payload, dict):
        return JSONResponse({"message": "保存内容がありません。"}, status_code=400)
    basic = payload.get("basic") if isinstance(payload.get("basic"), dict) else {}
    values = payload.get("values") if isinstance(payload.get("values"), list) else []
    draft_id = _optional_int(payload.get("draft_id"))
    event_id = _optional_int(basic.get("event_id")) or 2
    if not values and draft_id is None:
        return JSONResponse({"message": "入力された検査値がありません。"}, status_code=400)
    basic_has_value = any(
        _manual_text(basic.get(key))
        for key in (
            "facility_code",
            "facility_name",
            "exam_date",
            "hia_subscriber_id",
            "insurance_symbol",
            "insurance_number",
            "name_full",
            "name_kana",
        )
    )
    if not basic_has_value:
        return JSONResponse({"message": "下書き保存には基本情報が必要です。"}, status_code=400)

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            if not manual_exam_entry_table_exists(cur, health_db(), "manual_exam_entry_drafts"):
                return JSONResponse({"message": "仮登録テーブルが未適用です。"}, status_code=400)
            if draft_id is None:
                draft_id = insert_manual_exam_entry_draft(
                    cur,
                    event_id=event_id,
                    entry_purpose=_manual_text(basic.get("entry_purpose")) or "PAPER_ONLY",
                    action_code="CREATE_FROM_MANUAL_ENTRY",
                    source_payload=basic,
                    user_id=int(user["app_user_id"]),
                )
            else:
                update_manual_exam_entry_draft_from_basic(
                    cur,
                    draft_id=draft_id,
                    event_id=event_id,
                    basic=basic,
                    user_id=int(user["app_user_id"]),
                )
            value_count = replace_manual_exam_entry_draft_values(
                cur,
                draft_id=draft_id,
                event_id=event_id,
                values=values,
                user_id=int(user["app_user_id"]),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return JSONResponse(
        {
            "draft_id": draft_id,
            "value_count": value_count,
            "message": f"draft {draft_id} に {value_count}件を下書き保存しました。",
        }
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    return templates.TemplateResponse("dashboard.html", {"request": request, "user": user})


@app.get("/support/mhlw-zip-format", response_class=HTMLResponse)
def support_mhlw_zip_format(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    return templates.TemplateResponse("support_mhlw_zip_format.html", {"request": request, "user": user})


@app.get("/manual-exam-entry", response_class=HTMLResponse)
def manual_exam_entry(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_edit_manual_exam_entry(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    params = load_mysql_base_params(db_prefix())
    initial_draft: dict[str, Any] | None = None
    draft_id = _optional_int(request.query_params.get("draft_id"))
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            event_options = load_event_options(cur)
            folder_aliases = load_received_folder_alias_rows(cur)
            item_rows = load_manual_exam_entry_items(cur)
            if draft_id is not None and manual_exam_entry_table_exists(cur, health_db(), "manual_exam_entry_drafts"):
                initial_draft = load_manual_exam_entry_draft_by_id(cur, draft_id)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return templates.TemplateResponse(
        "manual_exam_entry.html",
        {
            "request": request,
            "user": user,
            "event_options": event_options,
            "folder_aliases": folder_aliases,
            "item_groups": group_manual_exam_entry_items(item_rows),
            "item_count": len(item_rows),
            "initial_draft": initial_draft,
            "initial_draft_json": json.dumps(
                initial_draft or {},
                ensure_ascii=False,
                default=manual_exam_json_default,
            ).replace("</", "<\\/"),
            "filters": {
                "event_id": str((initial_draft or {}).get("event_id") or request.query_params.get("event_id", "2")),
            },
        },
    )


@app.get("/manual-exam-entry-drafts", response_class=HTMLResponse)
def manual_exam_entry_drafts(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_edit_manual_exam_entry(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            event_options = load_event_options(cur)
            folder_aliases = load_received_folder_alias_rows(cur)
            schema_ready = manual_exam_entry_table_exists(cur, health_db(), "manual_exam_entry_drafts")
            status_filter = str(request.query_params.get("status", "") or "").upper().strip()
            if status_filter not in {"DRAFT", "READY", "APPLIED", "ERROR"}:
                status_filter = ""
            draft_filters = {
                "hia_subscriber_id": str(request.query_params.get("hia_subscriber_id", "") or "").strip(),
                "case_id": str(request.query_params.get("case_id", "") or "").strip(),
                "duplicates": "1" if request.query_params.get("duplicates") == "1" else "",
            }
            summary_rows = load_manual_exam_entry_draft_rows(cur, limit=100000) if schema_ready else []
            annotate_manual_exam_draft_duplicates(summary_rows)
            draft_rows = filter_manual_exam_entry_draft_rows(
                summary_rows,
                status_filter=status_filter,
                hia_subscriber_id=draft_filters["hia_subscriber_id"],
                case_id=draft_filters["case_id"],
                duplicates_only=draft_filters["duplicates"] == "1",
            )
            summary = summarize_manual_exam_entry_drafts(summary_rows)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return templates.TemplateResponse(
        "manual_exam_entry_drafts.html",
        {
            "request": request,
            "user": user,
            "event_options": event_options,
            "folder_aliases": folder_aliases,
            "schema_ready": schema_ready,
            "draft_rows": draft_rows,
            "summary": summary,
            "status_filter": status_filter,
            "draft_filters": draft_filters,
            "can_manage_manual_entry": can_manage_manual_exam_entry(user),
            "message": request.query_params.get("message", ""),
        },
    )


@app.get("/login", response_class=HTMLResponse)
def login_form(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("login.html", {"request": request, "error": None, "request_ip": client_ip(request)})


@app.get("/register", response_class=HTMLResponse)
def register_form(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "register.html",
        {"request": request, "message": None, "error": None, "form": {}, "request_ip": client_ip(request)},
    )


@app.post("/register", response_class=HTMLResponse)
async def register_user(request: Request) -> Response:
    form = await read_form(request)
    request_ip = client_ip(request)
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
            {
                "request": request,
                "message": None,
                "error": "社員番号と氏名は必須です。",
                "form": form_values,
                "request_ip": request_ip,
            },
            status_code=400,
        )
    if len(password) < 8:
        return templates.TemplateResponse(
            "register.html",
            {
                "request": request,
                "message": None,
                "error": "パスワードは8文字以上にしてください。",
                "form": form_values,
                "request_ip": request_ip,
            },
            status_code=400,
        )
    if password != password_confirm:
        return templates.TemplateResponse(
            "register.html",
            {
                "request": request,
                "message": None,
                "error": "パスワードが一致しません。",
                "form": form_values,
                "request_ip": request_ip,
            },
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
                    {
                        "request": request,
                        "message": None,
                        "error": "この社員番号はすでに登録されています。",
                        "form": form_values,
                        "request_ip": request_ip,
                    },
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
            add_registration_allowed_ip(cur, app_user_id=app_user_id, request_ip=request_ip)
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
            "request_ip": request_ip,
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
            session_lifetime = get_app_setting_int(
                cur,
                app_db=app_db(),
                setting_key="session_lifetime_minutes",
                default=720,
                minimum=5,
                maximum=24 * 60,
            )
            result = authenticate_user(
                cur,
                app_db=app_db(),
                employee_no=employee_no,
                password=password,
                client_ip=client_ip(request),
                user_agent=request.headers.get("user-agent"),
                session_lifetime_minutes=session_lifetime,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    if not result.success:
        reason = result.failure_reason or "LOGIN_FAILED"
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": LOGIN_ERROR_MESSAGES.get(reason, "ログインできませんでした。"),
                "request_ip": client_ip(request),
            },
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


@app.get("/exam-processing", response_class=HTMLResponse)
def exam_processing(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_run_exam_processing(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    selected_event_id = parse_positive_int(request.query_params.get("event_id"), default=2, maximum=999999)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            events = load_event_options(cur)
            recent_runs = load_recent_exam_processing_runs(cur, event_id=selected_event_id)
            running_runs = load_running_exam_processing_runs(cur, event_id=selected_event_id)
            unknown_scan_folders = load_unknown_scan_folder_rows(cur, event_id=str(selected_event_id))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return templates.TemplateResponse(
        "exam_processing.html",
        {
            "request": request,
            "user": user,
            "events": events,
            "selected_event_id": selected_event_id,
            "steps": EXAM_PROCESSING_STEPS,
            "recent_runs": recent_runs,
            "running_runs": running_runs,
            "unknown_scan_folders": unknown_scan_folders,
            "results": [],
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
        },
    )


@app.get("/exam-case-rebuild", response_class=HTMLResponse)
def exam_case_rebuild(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_run_exam_processing(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    event_id = parse_positive_int(request.query_params.get("event_id"), default=2, maximum=999999)
    search_filters = {
        "insurance_symbol": str(request.query_params.get("insurance_symbol") or "").strip(),
        "insurance_number": str(request.query_params.get("insurance_number") or "").strip(),
        "name_kana": str(request.query_params.get("name_kana") or "").strip(),
        "birth": str(request.query_params.get("birth") or "").strip(),
        "hia_subscriber_id": str(request.query_params.get("hia_subscriber_id") or "").strip(),
        "exam_facility_id": str(request.query_params.get("exam_facility_id") or "").strip(),
        "exam_facility_display": str(request.query_params.get("exam_facility_display") or "").strip(),
        "exam_month": str(request.query_params.get("exam_month") or "").strip(),
    }
    has_search_filters = any(search_filters.values())
    search_query = urlencode({key: value for key, value in search_filters.items() if value})
    subscriber_id = _optional_int(request.query_params.get("subscriber_id"))
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            events = load_event_options(cur)
            exam_facility_id = _optional_int(search_filters["exam_facility_id"])
            has_exam_scope = exam_facility_id is not None or bool(search_filters["exam_month"])
            scoped_subscriber_ids = (
                load_case_rebuild_scope_subscriber_ids(
                    cur,
                    event_id=event_id,
                    exam_facility_id=exam_facility_id,
                    exam_month=search_filters["exam_month"],
                )
                if has_exam_scope
                else ()
            )
            candidate_filters = dict(search_filters)
            if has_exam_scope:
                candidate_filters["subscriber_ids"] = scoped_subscriber_ids
            candidates = load_subscriber_match_candidate_rows(
                cur,
                ledger=None,
                event_id=event_id,
                candidate_filters=candidate_filters,
                limit=50,
            ) if has_search_filters and (not has_exam_scope or scoped_subscriber_ids) else []
            exam_month_options = load_case_rebuild_month_options(cur, event_id=event_id)
            selected_subscriber = (
                load_case_rebuild_subscriber(cur, subscriber_id=subscriber_id)
                if subscriber_id is not None
                else None
            )
            cases = (
                load_case_rebuild_cases(cur, event_id=event_id, subscriber_id=subscriber_id)
                if selected_subscriber and subscriber_id is not None
                else []
            )
            ledgers = (
                load_case_rebuild_ledgers(
                    cur,
                    event_id=event_id,
                    subscriber_id=subscriber_id,
                    hia_subscriber_id=selected_subscriber.get("hia_subscriber_id"),
                )
                if selected_subscriber and subscriber_id is not None
                else []
            )
            running_runs = load_running_exam_processing_runs(cur, event_id=event_id)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return templates.TemplateResponse(
        "exam_case_rebuild.html",
        {
            "request": request,
            "user": user,
            "events": events,
            "selected_event_id": event_id,
            "search_filters": search_filters,
            "search_query": search_query,
            "exam_month_options": exam_month_options,
            "prefecture_options": PREFECTURE_OPTIONS,
            "candidates": candidates,
            "selected_subscriber": selected_subscriber,
            "cases": cases,
            "ledgers": ledgers,
            "running_runs": running_runs,
            "results": [],
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
        },
    )


@app.post("/exam-case-rebuild/run", response_class=HTMLResponse)
async def run_exam_case_rebuild(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_run_exam_processing(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await request.form()
    event_id = parse_positive_int(str(form.get("event_id") or ""), default=2, maximum=999999)
    subscriber_id = _optional_int(form.get("subscriber_id"))
    mode = str(form.get("mode") or "full").strip()
    requested_case_ids = tuple(
        int(value)
        for value in form.getlist("case_ids")
        if str(value).strip().isdigit() and int(value) > 0
    )
    if subscriber_id is None:
        return RedirectResponse(
            f"/exam-case-rebuild?event_id={event_id}&error={quote('対象の加入者を選択してください。')}",
            status_code=303,
        )
    if mode not in {"full", "values", "check"}:
        return RedirectResponse(
            f"/exam-case-rebuild?event_id={event_id}&subscriber_id={subscriber_id}&error={quote('再実行範囲が不正です。')}",
            status_code=303,
        )

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            events = load_event_options(cur)
            selected_subscriber = load_case_rebuild_subscriber(cur, subscriber_id=subscriber_id)
            cases_before = load_case_rebuild_cases(cur, event_id=event_id, subscriber_id=subscriber_id)
            ledgers = load_case_rebuild_ledgers(
                cur,
                event_id=event_id,
                subscriber_id=subscriber_id,
                hia_subscriber_id=(selected_subscriber or {}).get("hia_subscriber_id"),
            ) if selected_subscriber else []
            exam_month_options = load_case_rebuild_month_options(cur, event_id=event_id)
            running_runs = load_running_exam_processing_runs(cur, event_id=event_id)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    error: str | None = None
    results: list[dict[str, Any]] = []
    available_case_ids = {int(row["exam_export_case_id"]) for row in cases_before}
    if selected_subscriber is None:
        error = "加入者が見つかりません。"
    elif running_runs:
        error = "このeventで別の健診結果処理が実行中です。"
    elif available_case_ids:
        if not requested_case_ids:
            error = "再実行するcaseを1件以上選択してください。"
        elif not set(requested_case_ids).issubset(available_case_ids):
            error = "選択した加入者に属さないcaseが含まれています。"
    elif not ledgers:
        error = "この加入者にはcase作成元にできるledgerがありません。"
    elif mode != "full":
        error = "caseがまだないため、case作成から実行してください。"

    target_case_ids = requested_case_ids
    if error is None and not available_case_ids:
        result = run_exam_processing_step(
            step_key="build_cases",
            event_id=event_id,
            dry_run=False,
            subscriber_ids=(subscriber_id,),
        )
        results.append(result)
        if not result["ok"]:
            error = "case作成で停止しました。"
        else:
            with connect_ctx(params, database=health_db(), autocommit=False) as conn:
                cur = dict_cursor(conn)
                try:
                    cases_after = load_case_rebuild_cases(cur, event_id=event_id, subscriber_id=subscriber_id)
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
            target_case_ids = tuple(int(row["exam_export_case_id"]) for row in cases_after)
            if not target_case_ids:
                error = "ledgerは見つかりましたがcaseを作成できませんでした。実行結果を確認してください。"

    if error is None:
        steps = {
            "full": ("build_cases", "build_values", "check_cases") if available_case_ids else ("build_values", "check_cases"),
            "values": ("build_values", "check_cases"),
            "check": ("check_cases",),
        }[mode]
        for step_key in steps:
            result = run_exam_processing_step(
                step_key=step_key,
                event_id=event_id,
                dry_run=False,
                case_ids=target_case_ids,
            )
            results.append(result)
            if not result["ok"]:
                error = f"{result['label']}で停止しました。"
                break

    recovery_result: dict[str, Any] | None = None
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            check_completed = any(
                result.get("step_key") == "check_cases" and result.get("ok")
                for result in results
            )
            if error is None and check_completed:
                operator = str(
                    user.get("employee_no")
                    or user.get("display_name")
                    or f"app_user_id:{user.get('app_user_id')}"
                )
                recovery_result = reconcile_reverted_export_list_cases_after_recheck(
                    cur,
                    exam_export_case_ids=target_case_ids,
                    operator=operator,
                )
                if recovery_result["considered"] and audit_enabled(cur):
                    log_audit(
                        cur,
                        request=request,
                        user=user,
                        action_code="RESTORE_EXPORT_LIST_CASES_AFTER_PERSON_RECHECK",
                        target_schema=health_db(),
                        target_table="ops_xml_export_list_cases",
                        target_id=",".join(str(case_id) for case_id in target_case_ids),
                        after=recovery_result,
                    )
            cases = load_case_rebuild_cases(cur, event_id=event_id, subscriber_id=subscriber_id)
            ledgers = load_case_rebuild_ledgers(
                cur,
                event_id=event_id,
                subscriber_id=subscriber_id,
                hia_subscriber_id=(selected_subscriber or {}).get("hia_subscriber_id"),
            ) if selected_subscriber else []
            running_runs = load_running_exam_processing_runs(cur, event_id=event_id)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    if recovery_result and recovery_result["considered"]:
        results.append(
            {
                "step_key": "restore_export_list_cases",
                "label": "出力リスト復帰判定",
                "returncode": 0,
                "ok": True,
                "output": (
                    f"対象 {recovery_result['considered']}件 / "
                    f"復帰 {recovery_result['restored']}件 / "
                    f"除外継続 {recovery_result['kept_removed']}件"
                ),
            }
        )
    return templates.TemplateResponse(
        "exam_case_rebuild.html",
        {
            "request": request,
            "user": user,
            "events": events,
            "selected_event_id": event_id,
            "search_filters": {
                "insurance_symbol": "",
                "insurance_number": "",
                "name_kana": "",
                "birth": "",
                "hia_subscriber_id": "",
                "exam_facility_id": "",
                "exam_facility_display": "",
                "exam_month": "",
            },
            "search_query": "",
            "exam_month_options": exam_month_options,
            "prefecture_options": PREFECTURE_OPTIONS,
            "candidates": [],
            "selected_subscriber": selected_subscriber,
            "cases": cases,
            "ledgers": ledgers,
            "running_runs": running_runs,
            "results": results,
            "message": f"{sum(1 for result in results if result['ok'])}件の処理が完了しました。" if results else None,
            "error": error,
        },
    )


@app.get("/exam-processing/runs/{run_id}", response_class=HTMLResponse)
def exam_processing_run_detail(request: Request, run_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_run_exam_processing(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            run = load_etl_run_detail(cur, run_id=run_id)
            errors = load_etl_run_errors(cur, run_id=run_id) if run else []
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    if not run:
        return templates.TemplateResponse(
            "exam_processing_run_detail.html",
            {
                "request": request,
                "user": user,
                "run": None,
                "errors": [],
                "message": None,
                "error": f"run_id={run_id} の実行履歴が見つかりません。",
            },
            status_code=404,
        )
    return templates.TemplateResponse(
        "exam_processing_run_detail.html",
        {
            "request": request,
            "user": user,
            "run": run,
            "errors": errors,
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
        },
    )


@app.post("/exam-processing/run", response_class=HTMLResponse)
async def run_exam_processing(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_run_exam_processing(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await request.form()
    event_id = parse_positive_int(str(form.get("event_id") or ""), default=2, maximum=999999)
    dry_run = str(form.get("dry_run") or "") == "1"
    include_imported = str(form.get("include_imported") or "") == "1"
    limit = parse_positive_int(str(form.get("limit") or ""), default=0, maximum=100000)
    action = str(form.get("action") or "").strip()
    selected_step_keys = [str(value) for value in form.getlist("step_keys")]
    if action == "run_selected":
        valid_step_keys = {step["key"] for step in EXAM_PROCESSING_STEPS}
        step_keys = [step["key"] for step in EXAM_PROCESSING_STEPS if step["key"] in selected_step_keys and step["key"] in valid_step_keys]
        if not step_keys:
            return RedirectResponse(
                f"/exam-processing?event_id={event_id}&error={quote('実行する処理を1つ以上選択してください。')}",
                status_code=303,
            )
    elif action == "run_all":
        step_keys = [step["key"] for step in EXAM_PROCESSING_STEPS]
    else:
        step_keys = [action]
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            running_runs = load_running_exam_processing_runs(cur, event_id=event_id)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    if running_runs:
        return RedirectResponse(
            f"/exam-processing?event_id={event_id}&error={quote('このeventで別の健診結果処理が実行中です。管理者の実行中処理画面で確認してください。')}",
            status_code=303,
        )
    results: list[dict[str, Any]] = []
    for step_key in step_keys:
        result = run_exam_processing_step(
            step_key=step_key,
            event_id=event_id,
            dry_run=dry_run,
            limit=limit,
            include_imported=include_imported,
        )
        results.append(result)
        if not result["ok"]:
            break
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            events = load_event_options(cur)
            recent_runs = load_recent_exam_processing_runs(cur, event_id=event_id)
            running_runs = load_running_exam_processing_runs(cur, event_id=event_id)
            unknown_scan_folders = load_unknown_scan_folder_rows(cur, event_id=str(event_id))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    ok_count = sum(1 for result in results if result["ok"])
    failed = [result for result in results if not result["ok"]]
    return templates.TemplateResponse(
        "exam_processing.html",
        {
            "request": request,
            "user": user,
            "events": events,
            "selected_event_id": event_id,
            "steps": EXAM_PROCESSING_STEPS,
            "recent_runs": recent_runs,
            "running_runs": running_runs,
            "unknown_scan_folders": unknown_scan_folders,
            "results": results,
            "message": f"{ok_count}件の処理が完了しました。",
            "error": f"{failed[0]['label']}で停止しました。" if failed else None,
        },
        status_code=500 if failed else 200,
    )


@app.get("/admin/etl-runs", response_class=HTMLResponse)
def admin_etl_runs(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "users.manage"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            running_runs = load_running_etl_runs(cur)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return templates.TemplateResponse(
        "admin_etl_runs.html",
        {
            "request": request,
            "user": user,
            "running_runs": running_runs,
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
        },
    )


@app.get("/admin/workload-estimate", response_class=HTMLResponse)
def admin_workload_estimate(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_manage_business_settings(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    selected_event_id = _optional_int(request.query_params.get("event_id"))
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            events = load_event_options(cur)
            if selected_event_id is None and events:
                selected_event_id = int(events[0]["event_id"])
            actuals = load_workload_estimate_actuals(cur, event_id=selected_event_id) if selected_event_id else {}
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return templates.TemplateResponse(
        "admin_workload_estimate.html",
        {"request": request, "user": user, "events": events, "selected_event_id": selected_event_id, "actuals": actuals},
    )


@app.get("/reports/monthly-exam-ng-summary", response_class=HTMLResponse)
def monthly_exam_ng_summary(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_manage_business_settings(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    event_id = request.query_params.get("event_id", "").strip()
    exam_month = request.query_params.get("exam_month", "").strip()
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            events = load_event_options(cur)
            if not event_id and events:
                event_id = str(events[0]["event_id"])
            month_options = load_facility_summary_month_options(cur, event_id=event_id) if event_id else []
            if not exam_month and month_options:
                exam_month = str(month_options[0].get("exam_month") or "")
            report = load_monthly_exam_ng_report(cur, event_id=event_id, exam_month=exam_month) if event_id else {
                "summary": {}, "facility_rows": [], "ng_item_rows": []
            }
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return templates.TemplateResponse(
        "monthly_exam_ng_summary.html",
        {
            "request": request,
            "user": user,
            "events": events,
            "month_options": month_options,
            "filters": {"event_id": event_id, "exam_month": exam_month},
            "report": report,
        },
    )


@app.get("/admin/manual-exam-ledgers", response_class=HTMLResponse)
def admin_manual_exam_ledgers(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_manage_manual_exam_entry(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    filters = {
        "event_id": request.query_params.get("event_id", "2"),
        "q": request.query_params.get("q", ""),
        "worker_user_id": request.query_params.get("worker_user_id", ""),
        "draft_status": request.query_params.get("draft_status", ""),
        "apply_state": request.query_params.get("apply_state", ""),
        "limit": request.query_params.get("limit", "200"),
    }
    limit = parse_positive_int(filters["limit"], default=200, maximum=1000)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            event_options = load_event_options(cur)
            worker_options = load_app_user_options(cur)
            rows = load_admin_manual_exam_ledger_rows(cur, filters=filters, limit=limit)
            if audit_enabled(cur):
                for row in rows:
                    log_audit(
                        cur,
                        request=request,
                        user=user,
                        action_code="PERSONAL_INFO_VIEW_ADMIN_MANUAL_EXAM_LEDGER",
                        target_schema=health_db(),
                        target_table="exam_ledgers",
                        target_id=str(row.get("exam_ledger_id") or ""),
                        after={
                            "exam_ledger_id": row.get("exam_ledger_id"),
                            "manual_exam_entry_draft_id": row.get("manual_exam_entry_draft_id"),
                            "hia_subscriber_id": row.get("hia_subscriber_id"),
                            "person_id_custom": row.get("person_id_custom"),
                            "exam_date": str(row.get("exam_date") or ""),
                            "facility_code": row.get("facility_code"),
                        },
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return templates.TemplateResponse(
        "admin_manual_exam_ledgers.html",
        {
            "request": request,
            "user": user,
            "rows": rows,
            "filters": filters,
            "limit": limit,
            "event_options": event_options,
            "worker_options": worker_options,
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
        },
    )


@app.post("/admin/manual-exam-ledgers/{exam_ledger_id}/revert")
async def admin_revert_manual_exam_ledger(exam_ledger_id: int, request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_manage_manual_exam_entry(user):
        return JSONResponse({"ok": False, "message": "権限がありません。"}, status_code=403)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            result = revert_manual_exam_ledger_to_draft(cur, exam_ledger_id=exam_ledger_id, user=user)
            if audit_enabled(cur):
                log_audit(
                    cur,
                    request=request,
                    user=user,
                    action_code="REVERT_ADMIN_MANUAL_EXAM_LEDGER_TO_DRAFT",
                    target_schema=health_db(),
                    target_table="exam_ledgers",
                    target_id=str(exam_ledger_id),
                    after=result,
                )
            conn.commit()
        except ValueError as exc:
            conn.rollback()
            return JSONResponse({"ok": False, "message": f"戻しできません: {exc}"}, status_code=400)
        except Exception:
            conn.rollback()
            raise
    message = (
        f"ledger {result['exam_ledger_id']} をdraft {result['manual_exam_entry_draft_id']} へ戻しました。"
        f"出力リストから{result['removed_export_list_case_count']}件を履歴付きで除外しました。"
        "必要に応じて健診結果処理 step5〜7 を再実行してください。"
    )
    return JSONResponse({"ok": True, "message": message, **result})


@app.post("/admin/etl-runs/{run_id}/stop")
async def admin_stop_etl_run(request: Request, run_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "users.manage"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await request.form()
    reason = str(form.get("reason") or "").strip()
    operator = f"{user.get('employee_no') or '-'}:{user.get('display_name') or '-'}"
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            updated = mark_etl_run_stopped(cur, run_id=run_id, operator=operator, reason=reason)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    if updated:
        message = f"run_id={run_id} を停止扱いにしました。"
        return RedirectResponse(f"/admin/etl-runs?message={quote(message)}", status_code=303)
    return RedirectResponse(
        f"/admin/etl-runs?error={quote('対象runはrunningではありません。最新状態を確認してください。')}",
        status_code=303,
    )


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    if isinstance(value, (date, datetime)):
        return "'" + str(value).replace("'", "''") + "'"
    text = str(value)
    return "'" + text.replace("\\", "\\\\").replace("'", "''") + "'"


def load_norm_variants_for_sql_export(cur: Any, *, canonical_only: bool) -> list[dict[str, Any]]:
    where = "WHERE is_canonical = 1" if canonical_only else ""
    cur.execute(
        f"""
        SELECT
          result_code_oid,
          raw_token_norm,
          raw_value_utf8,
          normalized_code,
          code_system,
          display_name,
          is_canonical,
          priority,
          is_active,
          note
        FROM {qname(master_db())}.norm_variants
        {where}
        ORDER BY result_code_oid, is_canonical DESC, priority, normalized_code, raw_value_utf8
        """
    )
    return [dict(row) for row in cur.fetchall()]


def build_norm_variants_upsert_sql(rows: Sequence[Mapping[str, Any]], *, source_label: str, canonical_only: bool) -> str:
    columns = [
        "result_code_oid",
        "raw_token_norm",
        "raw_value_utf8",
        "normalized_code",
        "code_system",
        "display_name",
        "is_canonical",
        "priority",
        "is_active",
        "note",
    ]
    lines = [
        "-- System export: phr_master.norm_variants",
        f"-- source: {source_label}",
        f"-- exported_at: {datetime.now().isoformat(timespec='seconds')}",
        f"-- scope: {'canonical_only' if canonical_only else 'all_rows'}",
        "-- sync key: UNIQUE(result_code_oid, raw_value_utf8)",
        "",
        "INSERT INTO `phr_master`.`norm_variants` (",
        "  " + ",\n  ".join(f"`{column}`" for column in columns),
        ") VALUES",
    ]
    value_lines = []
    for row in rows:
        values = ", ".join(sql_literal(row.get(column)) for column in columns)
        value_lines.append(f"  ({values})")
    lines.append(",\n".join(value_lines))
    lines.extend(
        [
            "ON DUPLICATE KEY UPDATE",
            "  `raw_token_norm` = VALUES(`raw_token_norm`),",
            "  `normalized_code` = VALUES(`normalized_code`),",
            "  `code_system` = VALUES(`code_system`),",
            "  `display_name` = VALUES(`display_name`),",
            "  `is_canonical` = VALUES(`is_canonical`),",
            "  `priority` = VALUES(`priority`),",
            "  `is_active` = VALUES(`is_active`),",
            "  `note` = VALUES(`note`),",
            "  `updated_at` = CURRENT_TIMESTAMP(6);",
            "",
        ]
    )
    return "\n".join(lines)


@app.get("/admin/master-sql-export", response_class=HTMLResponse)
def admin_master_sql_export(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, (BUSINESS_SETTINGS_PERMISSION, SYSTEM_SETTINGS_PERMISSION)):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=master_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        summary = load_norm_variant_summary(cur)
        cur.close()
    return templates.TemplateResponse(
        "admin_master_sql_export.html",
        {
            "request": request,
            "user": user,
            "summary": summary,
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
        },
    )


@app.post("/admin/master-sql-export/norm-variants")
async def admin_export_norm_variants_sql(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, (BUSINESS_SETTINGS_PERMISSION, SYSTEM_SETTINGS_PERMISSION)):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await read_form(request)
    canonical_only = str(form.get("canonical_only") or "") == "1"
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=master_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            rows = load_norm_variants_for_sql_export(cur, canonical_only=canonical_only)
            if not rows:
                raise ValueError("出力対象のnorm_variantsがありません。")
            source_label = f"{user.get('employee_no') or '-'}:{user.get('display_name') or '-'}"
            sql_text = build_norm_variants_upsert_sql(rows, source_label=source_label, canonical_only=canonical_only)
            export_dir = SYSTEM_SQL_EXPORT_ROOT / "phr_master"
            export_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            scope = "canonical" if canonical_only else "all"
            export_path = export_dir / f"{timestamp}_phr_master_norm_variants_{scope}_upsert.sql"
            export_path.write_text(sql_text, encoding="utf-8")
            if audit_enabled(cur):
                log_audit(
                    cur,
                    request=request,
                    user=user,
                    action_code="EXPORT_MASTER_SQL_NORM_VARIANTS",
                    target_schema=master_db(),
                    target_table="norm_variants",
                    target_id=str(export_path),
                    after={"row_count": len(rows), "canonical_only": canonical_only, "path": str(export_path)},
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
    return FileResponse(
        export_path,
        media_type="application/sql",
        filename=export_path.name,
    )


@app.get("/file-receipts", response_class=HTMLResponse)
def file_receipts(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, ("export_lists.view", "export_lists.edit", "users.manage")):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    filters = {
        "event_id": request.query_params.get("event_id", "2"),
        "file_type": request.query_params.get("file_type", ""),
        "status": request.query_params.get("status", ""),
        "receipt_check": request.query_params.get("receipt_check", ""),
        "q": request.query_params.get("q", ""),
        "limit": request.query_params.get("limit", "500"),
    }
    limit = parse_positive_int(filters["limit"], default=500, maximum=5000)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            event_options = load_event_options(cur)
            rows = load_file_receipt_rows(cur, filters=filters, limit=limit)
            unknown_scan_folders = load_unknown_scan_folder_rows(cur, event_id=filters["event_id"])
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return templates.TemplateResponse(
        "file_receipts.html",
        {
            "request": request,
            "user": user,
            "rows": rows,
            "filters": filters,
            "limit": limit,
            "event_options": event_options,
            "unknown_scan_folders": unknown_scan_folders,
        },
    )


@app.get("/hia/fund-delivery", response_class=HTMLResponse)
def hia_fund_delivery(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, ("hia_upload.perform", "hia_upload_status.edit", "users.manage")):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        page_data = load_fund_delivery_page_data(cur)
    return templates.TemplateResponse(
        "hia_fund_delivery.html",
        {
            "request": request,
            "user": user,
            **page_data,
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
            "can_submit": has_any_permission(user, ("hia_upload_status.edit", "users.manage")),
        },
    )


@app.get("/hia/upload-work", response_class=HTMLResponse)
def hia_upload_work(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, ("hia_upload.perform", "hia_upload_status.edit", "users.manage")):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        page_data = load_hia_upload_page_data(cur)
    return templates.TemplateResponse(
        "hia_upload_work.html",
        {
            "request": request,
            "user": user,
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
            "can_edit": has_any_permission(user, ("hia_upload_status.edit", "users.manage")),
            **page_data,
        },
    )


@app.get("/external-feedback", response_class=HTMLResponse)
def external_feedback_work(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, ("hia_upload.perform", "hia_upload_status.edit", "users.manage")):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        page_data = load_external_feedback_page_data(cur, query_params=request.query_params)
    return templates.TemplateResponse(
        "external_feedback.html",
        {
            "request": request,
            "user": user,
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
            "can_edit": has_any_permission(user, ("hia_upload_status.edit", "users.manage")),
            **page_data,
        },
    )


@app.get("/api/external-feedback/hia-members")
def external_feedback_hia_members(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse({"items": []}, status_code=401)
    if not has_any_permission(user, ("hia_upload_status.edit", "users.manage")):
        return JSONResponse({"items": []}, status_code=403)

    event_id = _optional_int(request.query_params.get("event_id"))
    query = request.query_params.get("q", "").strip()
    name_kana = request.query_params.get("name_kana", "").strip()
    xml_export_member_id = _optional_int(request.query_params.get("xml_export_member_id"))
    exam_export_case_id = _optional_int(request.query_params.get("exam_export_case_id"))
    if not any((query, name_kana, xml_export_member_id, exam_export_case_id)):
        return JSONResponse({"items": []})

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        rows = search_hia_upload_members_for_feedback(
            cur,
            event_id=event_id,
            query=query,
            name_kana=name_kana,
            xml_export_member_id=xml_export_member_id,
            exam_export_case_id=exam_export_case_id,
            limit=50,
        )

    items: list[dict[str, Any]] = []
    for row in rows:
        items.append(
            {
                "xml_export_member_id": row.get("xml_export_member_id"),
                "xml_export_zip_id": row.get("xml_export_zip_id"),
                "xml_export_list_id": row.get("xml_export_list_id"),
                "event_id": row.get("event_id"),
                "exam_export_case_id": row.get("exam_export_case_id"),
                "subscriber_id": row.get("subscriber_id"),
                "hia_subscriber_id": row.get("hia_subscriber_id"),
                "name_kana": row.get("name_kana_export_value"),
                "name_full": row.get("name_full_raw"),
                "insurance_symbol": row.get("insurance_symbol_export_value"),
                "insurance_number": row.get("insurance_number_export_value"),
                "exam_date": str(row.get("exam_date") or ""),
                "facility_code": row.get("facility_code"),
                "facility_name": row.get("facility_name"),
                "person_xml_file_name": row.get("person_xml_file_name"),
                "zip_file_name": row.get("zip_file_name"),
                "hia_upload_status": row.get("hia_upload_status"),
                "hia_upload_error_code": row.get("hia_upload_error_code"),
                "hia_upload_error_message": row.get("hia_upload_error_message"),
                "export_readiness_status": row.get("export_readiness_status"),
                "xml_export_status": row.get("xml_export_status"),
            }
        )
    return JSONResponse({"items": items})


@app.post("/external-feedback", response_class=HTMLResponse)
async def create_external_feedback(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, ("hia_upload_status.edit", "users.manage")):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await read_form(request)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            report_id, item_id = create_external_feedback_from_form(cur, form=form, user=user)
            conn.commit()
        except Exception as exc:
            conn.rollback()
            return RedirectResponse(f"/external-feedback?error={quote(str(exc))}", status_code=303)
    log_app_operation(
        request=request,
        user=user,
        action_code="EXTERNAL_FEEDBACK_CREATE",
        target_schema=health_db(),
        target_table="ops_external_feedback_items",
        target_id=str(item_id),
        after={"external_feedback_report_id": report_id, "external_feedback_item_id": item_id},
    )
    return RedirectResponse(
        f"/external-feedback?message={quote(f'外部指摘を登録しました。report={report_id} item={item_id}')}",
        status_code=303,
    )


def load_csv_mapping_lab_files(cur: Any, *, limit: int = 50) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          `analysis_file_id`, `source_file_name`, `facility_code`, `facility_name`,
          `encoding`, `row_count`, `column_count`, `parse_status`, `analysis_status`,
          `created_at`, `updated_at`
        FROM {qname(CSV_MAPPING_LAB)}.`analysis_files`
        ORDER BY `analysis_file_id` DESC
        LIMIT %s
        """,
        (limit,),
    )
    return [dict(row) for row in cur.fetchall()]


def load_csv_mapping_lab_file(cur: Any, *, analysis_file_id: int) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT *
        FROM {qname(CSV_MAPPING_LAB)}.`analysis_files`
        WHERE `analysis_file_id` = %s
        """,
        (analysis_file_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def load_csv_mapping_lab_summary(cur: Any) -> dict[str, int]:
    cur.execute(
        f"""
        SELECT
          COUNT(*) AS file_count,
          COALESCE(SUM(`column_count`), 0) AS column_count,
          COALESCE(SUM(`row_count`), 0) AS row_count
        FROM {qname(CSV_MAPPING_LAB)}.`analysis_files`
        """
    )
    row = cur.fetchone() or {}
    return {
        "file_count": int(row.get("file_count") or 0),
        "column_count": int(row.get("column_count") or 0),
        "row_count": int(row.get("row_count") or 0),
    }


def load_csv_mapping_lab_column_count(cur: Any, *, analysis_file_id: int) -> int:
    cur.execute(
        f"""
        SELECT COUNT(*) AS `column_count`
        FROM {qname(CSV_MAPPING_LAB)}.`analysis_columns`
        WHERE `analysis_file_id` = %s
        """,
        (analysis_file_id,),
    )
    row = cur.fetchone() or {}
    return int(row.get("column_count") or 0)


def csv_mapping_lab_column_filter_from_query(query: Mapping[str, str]) -> dict[str, str]:
    return {
        "decision_status": (query.get("decision_status") or "").strip(),
        "candidate_target_kind": (query.get("candidate_target_kind") or "").strip(),
        "keyword": (query.get("keyword") or "").strip(),
        "has_namecode": (query.get("has_namecode") or "").strip(),
        "has_ledger_field": (query.get("has_ledger_field") or "").strip(),
        "ai_review_status": (query.get("ai_review_status") or "").strip(),
        "sensitive_hint": (query.get("sensitive_hint") or "").strip(),
        "low_confidence": (query.get("low_confidence") or "").strip(),
    }


def build_csv_mapping_lab_column_where(
    *,
    analysis_file_id: int,
    filters: Mapping[str, str],
    alias: str = "analysis_columns",
) -> tuple[str, list[Any]]:
    clauses = [f"`{alias}`.`analysis_file_id` = %s"]
    params: list[Any] = [analysis_file_id]
    decision_status = filters.get("decision_status") or ""
    if decision_status:
        clauses.append(f"`{alias}`.`decision_status` = %s")
        params.append(decision_status)
    candidate_target_kind = filters.get("candidate_target_kind") or ""
    if candidate_target_kind:
        clauses.append(f"`{alias}`.`candidate_target_kind` = %s")
        params.append(candidate_target_kind)
    keyword = filters.get("keyword") or ""
    if keyword:
        like = f"%{keyword}%"
        clauses.append(
            "("
            f"`{alias}`.`header_name` LIKE %s OR "
            f"`{alias}`.`normalized_header_name` LIKE %s OR "
            f"`{alias}`.`analysis_note` LIKE %s OR "
            f"`{alias}`.`candidate_namecode` LIKE %s OR "
            f"`{alias}`.`candidate_ledger_field` LIKE %s"
            ")"
        )
        params.extend([like, like, like, like, like])
    if filters.get("has_namecode") == "1":
        clauses.append(f"`{alias}`.`candidate_namecode` IS NOT NULL AND `{alias}`.`candidate_namecode` <> ''")
    elif filters.get("has_namecode") == "0":
        clauses.append(f"(`{alias}`.`candidate_namecode` IS NULL OR `{alias}`.`candidate_namecode` = '')")
    if filters.get("has_ledger_field") == "1":
        clauses.append(f"`{alias}`.`candidate_ledger_field` IS NOT NULL AND `{alias}`.`candidate_ledger_field` <> ''")
    elif filters.get("has_ledger_field") == "0":
        clauses.append(f"(`{alias}`.`candidate_ledger_field` IS NULL OR `{alias}`.`candidate_ledger_field` = '')")
    ai_review_status = filters.get("ai_review_status") or ""
    if ai_review_status:
        clauses.append(f"`{alias}`.`ai_review_status` = %s")
        params.append(ai_review_status)
    if filters.get("sensitive_hint") == "1":
        clauses.append(f"`{alias}`.`sensitive_hint` = 1")
    elif filters.get("sensitive_hint") == "0":
        clauses.append(f"(`{alias}`.`sensitive_hint` IS NULL OR `{alias}`.`sensitive_hint` = 0)")
    if filters.get("low_confidence") == "1":
        clauses.append(
            f"(`{alias}`.`candidate_confidence` IS NULL OR `{alias}`.`candidate_confidence` < 0.85)"
        )
    return " AND ".join(clauses), params


def load_csv_mapping_lab_filtered_column_count(
    cur: Any,
    *,
    analysis_file_id: int,
    filters: Mapping[str, str],
) -> int:
    where_sql, params = build_csv_mapping_lab_column_where(analysis_file_id=analysis_file_id, filters=filters)
    cur.execute(
        f"""
        SELECT COUNT(*) AS `column_count`
        FROM {qname(CSV_MAPPING_LAB)}.`analysis_columns` AS `analysis_columns`
        WHERE {where_sql}
        """,
        params,
    )
    row = cur.fetchone() or {}
    return int(row.get("column_count") or 0)


def load_csv_mapping_lab_column_status_summary(
    cur: Any,
    *,
    analysis_file_id: int,
) -> dict[str, int]:
    cur.execute(
        f"""
        SELECT
          COUNT(*) AS total,
          COALESCE(SUM(CASE WHEN `decision_status` = 'UNREVIEWED' THEN 1 ELSE 0 END), 0) AS unreviewed,
          COALESCE(SUM(CASE WHEN `candidate_target_kind` = 'REVIEW' THEN 1 ELSE 0 END), 0) AS review,
          COALESCE(SUM(CASE WHEN `candidate_target_kind` = 'EXAM_ITEM_VALUE' THEN 1 ELSE 0 END), 0) AS exam_item,
          COALESCE(SUM(CASE WHEN `candidate_target_kind` = 'LEDGER_FIELD' THEN 1 ELSE 0 END), 0) AS ledger_field,
          COALESCE(SUM(CASE WHEN `candidate_target_kind` = 'IGNORE' THEN 1 ELSE 0 END), 0) AS ignore_kind,
          COALESCE(SUM(CASE WHEN `candidate_target_kind` = 'WATCH' THEN 1 ELSE 0 END), 0) AS watch,
          COALESCE(SUM(CASE WHEN `candidate_target_kind` = 'WATCH' AND `non_blank_count` > 0 THEN 1 ELSE 0 END), 0) AS watch_hit,
          COALESCE(SUM(CASE WHEN `ai_review_status` = 'REVIEWED' THEN 1 ELSE 0 END), 0) AS ai_reviewed,
          COALESCE(SUM(CASE WHEN `ai_review_status` = 'FAILED' THEN 1 ELSE 0 END), 0) AS ai_failed,
          COALESCE(SUM(CASE WHEN `candidate_confidence` IS NULL OR `candidate_confidence` < 0.85 THEN 1 ELSE 0 END), 0) AS low_confidence
        FROM {qname(CSV_MAPPING_LAB)}.`analysis_columns`
        WHERE `analysis_file_id` = %s
        """,
        (analysis_file_id,),
    )
    row = cur.fetchone() or {}
    return {
        key: int(row.get(key) or 0)
        for key in ("total", "unreviewed", "review", "exam_item", "ledger_field", "ignore_kind", "watch", "watch_hit", "ai_reviewed", "ai_failed", "low_confidence")
    }


def load_csv_mapping_lab_columns(
    cur: Any,
    *,
    analysis_file_id: int,
    filters: Mapping[str, str] | None = None,
    limit: int = 120,
    offset: int = 0,
) -> list[dict[str, Any]]:
    filters = filters or {}
    where_sql, params = build_csv_mapping_lab_column_where(analysis_file_id=analysis_file_id, filters=filters)
    try:
        cur.execute(
            f"""
            SELECT
              `analysis_column_id`, `column_no`, `header_name`, `normalized_header_name`, `inferred_value_type`,
              `inferred_format`, `blank_count`, `non_blank_count`, `sensitive_hint`,
              `candidate_target_kind`, `candidate_namecode`, `candidate_ledger_field`,
              `candidate_confidence`, `analysis_note`, `decision_status`, `sample_values_json`,
              `ai_review_status`, `ai_review_note`, `ai_reviewed_by`, `ai_reviewed_at`,
              (
                SELECT COUNT(*)
                FROM {qname(CSV_MAPPING_LAB)}.`csv_mapping_rule_hits` AS `hit`
                WHERE `hit`.`analysis_column_id` = `analysis_columns`.`analysis_column_id`
              ) AS `rule_hit_count`
            FROM {qname(CSV_MAPPING_LAB)}.`analysis_columns` AS `analysis_columns`
            WHERE {where_sql}
            ORDER BY `column_no`
            LIMIT %s OFFSET %s
            """,
            params + [limit, offset],
        )
    except Exception:
        where_sql, params = build_csv_mapping_lab_column_where(analysis_file_id=analysis_file_id, filters=filters)
        cur.execute(
            f"""
            SELECT
              `analysis_column_id`, `column_no`, `header_name`, `normalized_header_name`, `inferred_value_type`,
              `inferred_format`, `blank_count`, `non_blank_count`, `sensitive_hint`,
              `candidate_target_kind`, `candidate_namecode`, `candidate_ledger_field`,
              `candidate_confidence`, `analysis_note`, `decision_status`, `sample_values_json`,
              'NOT_REVIEWED' AS `ai_review_status`, NULL AS `ai_review_note`, NULL AS `ai_reviewed_by`, NULL AS `ai_reviewed_at`,
              0 AS `rule_hit_count`
            FROM {qname(CSV_MAPPING_LAB)}.`analysis_columns`
            WHERE {where_sql}
            ORDER BY `column_no`
            LIMIT %s OFFSET %s
            """,
            params + [limit, offset],
        )
    return [dict(row) for row in cur.fetchall()]


def csv_mapping_decision_status_for_target_kind(target_kind: str) -> str:
    if target_kind == "IGNORE":
        return "IGNORE"
    if target_kind == "REVIEW":
        return "NEEDS_CONFIRMATION"
    if target_kind == "WATCH":
        return "WATCH"
    if target_kind in {"LEDGER_FIELD", "EXAM_ITEM_VALUE"}:
        return "ADOPT"
    return "UNREVIEWED"


def load_csv_mapping_lab_filtered_columns_for_bulk(
    cur: Any,
    *,
    analysis_file_id: int,
    filters: Mapping[str, str],
    maximum: int = 5000,
) -> list[dict[str, Any]]:
    where_sql, params = build_csv_mapping_lab_column_where(analysis_file_id=analysis_file_id, filters=filters)
    cur.execute(
        f"""
        SELECT
          `analysis_column_id`,
          `column_no`,
          `header_name`,
          `normalized_header_name`,
          `inferred_value_type`
        FROM {qname(CSV_MAPPING_LAB)}.`analysis_columns` AS `analysis_columns`
        WHERE {where_sql}
        ORDER BY `column_no`
        LIMIT %s
        """,
        params + [maximum],
    )
    return [dict(row) for row in cur.fetchall()]


def load_csv_mapping_lab_columns_by_numbers(
    cur: Any,
    *,
    analysis_file_id: int,
    column_numbers: Sequence[int],
) -> list[dict[str, Any]]:
    unique_numbers = sorted({int(number) for number in column_numbers if int(number) > 0})
    if not unique_numbers:
        return []
    placeholders = ", ".join(["%s"] * len(unique_numbers))
    cur.execute(
        f"""
        SELECT
          `analysis_column_id`, `column_no`, `header_name`, `normalized_header_name`, `inferred_value_type`,
          `candidate_target_kind`, `candidate_namecode`, `candidate_ledger_field`
        FROM {qname(CSV_MAPPING_LAB)}.`analysis_columns`
        WHERE `analysis_file_id` = %s AND `column_no` IN ({placeholders})
        ORDER BY `column_no`
        """,
        [analysis_file_id] + unique_numbers,
    )
    return [dict(row) for row in cur.fetchall()]


def load_csv_mapping_lab_rule_summary(cur: Any) -> dict[str, int]:
    try:
        cur.execute(
            f"""
            SELECT
              COUNT(*) AS rule_count,
              COALESCE(SUM(CASE WHEN `active` = 1 THEN 1 ELSE 0 END), 0) AS active_rule_count
            FROM {qname(CSV_MAPPING_LAB)}.`csv_mapping_rules`
            """
        )
    except Exception:
        return {"rule_count": 0, "active_rule_count": 0}
    row = cur.fetchone() or {}
    return {
        "rule_count": int(row.get("rule_count") or 0),
        "active_rule_count": int(row.get("active_rule_count") or 0),
    }


def csv_mapping_lab_default_context(
    request: Request,
    user: Mapping[str, Any],
    *,
    message: str | None = None,
    error: str | None = None,
    selected_file_id: int | None = None,
    column_page: int = 1,
    prompt_json: str | None = None,
    prompt_form: dict[str, Any] | None = None,
) -> dict[str, Any]:
    column_page_size = 120
    column_page = max(1, column_page)
    column_filters = csv_mapping_lab_column_filter_from_query(request.query_params)
    column_query_params = {
        "analysis_file_id": str(selected_file_id or ""),
        **{key: value for key, value in column_filters.items() if value},
    }
    column_filter_query = urlencode(column_query_params)
    column_filter_active = any(column_filters.values())
    column_offset = (column_page - 1) * column_page_size
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=CSV_MAPPING_LAB, autocommit=True) as conn:
        cur = dict_cursor(conn)
        files = load_csv_mapping_lab_files(cur)
        summary = load_csv_mapping_lab_summary(cur)
        rule_summary = load_csv_mapping_lab_rule_summary(cur)
        selected_file = load_csv_mapping_lab_file(cur, analysis_file_id=selected_file_id) if selected_file_id else None
        total_column_count = load_csv_mapping_lab_column_count(cur, analysis_file_id=selected_file_id) if selected_file_id else 0
        column_count = (
            load_csv_mapping_lab_filtered_column_count(cur, analysis_file_id=selected_file_id, filters=column_filters)
            if selected_file_id
            else 0
        )
        column_status_summary = (
            load_csv_mapping_lab_column_status_summary(cur, analysis_file_id=selected_file_id)
            if selected_file_id
            else {
                "total": 0,
                "unreviewed": 0,
                "review": 0,
                "exam_item": 0,
                "ledger_field": 0,
                "ignore_kind": 0,
                "watch": 0,
                "watch_hit": 0,
                "ai_reviewed": 0,
                "ai_failed": 0,
                "low_confidence": 0,
            }
        )
        max_column_page = max(1, ((column_count - 1) // column_page_size) + 1) if column_count else 1
        if column_page > max_column_page:
            column_page = max_column_page
            column_offset = (column_page - 1) * column_page_size
        columns = (
            load_csv_mapping_lab_columns(
                cur,
                analysis_file_id=selected_file_id,
                filters=column_filters,
                limit=column_page_size,
                offset=column_offset,
            )
            if selected_file_id
            else []
        )
        cur.close()
    return {
        "request": request,
        "user": user,
        "message": message,
        "error": error,
        "summary": summary,
        "rule_summary": rule_summary,
        "files": files,
        "selected_file": selected_file,
        "selected_file_id": selected_file_id,
        "columns": columns,
        "column_filters": column_filters,
        "column_filter_active": column_filter_active,
        "column_filter_query": column_filter_query,
        "column_status_summary": column_status_summary,
        "column_pagination": {
            "page": column_page,
            "page_size": column_page_size,
            "total": column_count,
            "unfiltered_total": total_column_count,
            "max_page": max_column_page,
            "start": column_offset + 1 if column_count else 0,
            "end": min(column_offset + column_page_size, column_count),
            "prev_page": column_page - 1 if column_page > 1 else None,
            "next_page": column_page + 1 if column_page < max_column_page else None,
        },
        "prompt_json": prompt_json,
        "prompt_form": prompt_form
        or {
            "analysis_file_id": selected_file_id or "",
            "column_start": "",
            "column_end": "",
            "only_unreviewed": "",
            "include_sensitive": "",
            "nearby_window": "3",
        },
        "ai_instruction_text": CSV_MAPPING_AI_INSTRUCTION,
        "ledger_field_options": CSV_MAPPING_LAB_LEDGER_FIELD_OPTIONS,
        "prefecture_options": PREFECTURE_OPTIONS,
    }


CSV_MAPPING_LAB_LEDGER_FIELD_OPTIONS = [
    {"value": "insurer_number", "label": "保険者番号", "hint": "event側で固定できない場合の保険者番号"},
    {"value": "insurance_symbol_raw", "label": "記号", "hint": "保険証記号"},
    {"value": "insurance_number_raw", "label": "番号", "hint": "保険証番号"},
    {"value": "insurance_branch_number_raw", "label": "枝番", "hint": "保険証枝番"},
    {"value": "name_full_raw", "label": "氏名", "hint": "漢字などの氏名"},
    {"value": "name_kana_raw", "label": "氏名かな", "hint": "カナ氏名"},
    {"value": "birthdate", "label": "生年月日", "hint": "YYYY-MM-DDへ正規化される値"},
    {"value": "gender_raw", "label": "性別", "hint": "男/女/1/2など"},
    {"value": "exam_date", "label": "健診実施日", "hint": "受診日"},
    {"value": "facility_code", "label": "健診機関コード", "hint": "受領フォルダより行の値を優先したい場合"},
    {"value": "facility_name", "label": "健診機関名", "hint": "行に施設名がある場合"},
    {"value": "health_exam_report_category", "label": "報告区分", "hint": "健診結果報告区分"},
    {"value": "program_code", "label": "プログラムコード", "hint": "コース/プログラムコード"},
    {"value": "postal_code", "label": "郵便番号", "hint": "受診者住所の郵便番号"},
    {"value": "address", "label": "住所", "hint": "受診者住所"},
]


CSV_MAPPING_LAB_RETURN_QUERY_KEYS = {
    "analysis_file_id",
    "column_page",
    "decision_status",
    "candidate_target_kind",
    "keyword",
    "has_namecode",
    "has_ledger_field",
    "sensitive_hint",
    "low_confidence",
}


def csv_mapping_lab_return_url(
    form: Mapping[str, str],
    *,
    analysis_file_id: int,
    message: str | None = None,
    error: str | None = None,
) -> str:
    params: dict[str, str] = {"analysis_file_id": str(analysis_file_id)}
    raw_return_query = str(form.get("return_query") or "")
    for key, values in parse_qs(raw_return_query, keep_blank_values=False).items():
        if key in CSV_MAPPING_LAB_RETURN_QUERY_KEYS and values:
            params[key] = values[0]
    params["analysis_file_id"] = str(analysis_file_id)
    if message:
        params["message"] = message
    if error:
        params["error"] = error
    return f"/utilities/csv-mapping-lab?{urlencode(params)}"


@app.get("/api/csv-mapping-lab/exam-items")
def api_csv_mapping_lab_exam_items(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    keyword = str(request.query_params.get("keyword") or "").strip()
    value_type = str(request.query_params.get("value_type") or "").strip().upper()
    if value_type not in {"PQ", "CD", "ST", "CO"}:
        value_type = ""
    like = f"%{keyword}%"
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=dev_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        cur.execute(
            f"""
            SELECT
              `namecode`,
              `item_name`,
              `category_name`,
              `xml_value_type`,
              `display_unit`,
              `method_name`,
              `identity_item_code`,
              `identity_item_name`,
              `result_code_oid`,
              `item_code_oid`,
              `ucum_unit`,
              `data_type_label`,
              `xml_method_code`,
              `value_method`,
              `nullflavor_allowed`,
              `notes`,
              `kubun_no`,
              `kubun_name`,
              `jun_no`,
              `annex2_exec_requirement`,
              `annex2_legal_report_flag`,
              `cda_section_code_default`,
              `annex2_series_group_identifier`,
              `annex2_series_group_relation_code`
            FROM {qname(dev_db())}.`exam_item_master`
            WHERE `namecode` IS NOT NULL
              AND `namecode` <> ''
              AND (%s = '' OR `xml_value_type` = %s)
              AND (
                %s = ''
                OR CONVERT(`namecode` USING utf8mb4) COLLATE utf8mb4_0900_ai_ci LIKE %s
                OR `item_name` LIKE %s
                OR `category_name` LIKE %s
                OR CONVERT(`identity_item_code` USING utf8mb4) COLLATE utf8mb4_0900_ai_ci LIKE %s
                OR `identity_item_name` LIKE %s
                OR `method_name` LIKE %s
              )
            ORDER BY
              CASE
                WHEN CONVERT(`namecode` USING utf8mb4) COLLATE utf8mb4_0900_ai_ci = %s THEN 0
                WHEN `item_name` = %s THEN 1
                WHEN CONVERT(`namecode` USING utf8mb4) COLLATE utf8mb4_0900_ai_ci LIKE %s THEN 2
                WHEN `item_name` LIKE %s THEN 3
                ELSE 4
              END,
              COALESCE(`kubun_no`, 999999),
              COALESCE(`jun_no`, 999999),
              COALESCE(`category_name`, ''),
              COALESCE(`identity_item_code`, `namecode`),
              `namecode`
            LIMIT 80
            """,
            (
                value_type,
                value_type,
                keyword,
                like,
                like,
                like,
                like,
                like,
                like,
                keyword,
                keyword,
                f"{keyword}%",
                f"{keyword}%",
            ),
        )
        rows = [dict(row) for row in cur.fetchall()]
        norm_variants_by_oid = load_norm_variants_by_oid(
            cur,
            [str(row.get("result_code_oid") or "") for row in rows],
        )
        cur.close()
    for row in rows:
        oid = str(row.get("result_code_oid") or "").strip()
        norm_rows = norm_variants_by_oid.get(oid, []) if oid else []
        row["standard_code_rows"] = [
            json_safe_mapping(variant)
            for variant in norm_rows
            if int(variant.get("is_canonical") or 0) == 1 and int(variant.get("is_active") or 0) == 1
        ]
        row["norm_variant_rows"] = [json_safe_mapping(variant) for variant in norm_rows]
    return JSONResponse({"items": [json_safe_mapping(row) for row in rows], "limit": 80})


@app.get("/api/export-lists/exam-items")
def api_export_list_exam_items(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, ("export_lists.view", "export_lists.edit", "users.manage")):
        return JSONResponse({"error": "FORBIDDEN"}, status_code=403)

    keyword = str(request.query_params.get("keyword") or "").strip()
    if not keyword:
        return JSONResponse({"items": [], "limit": 60})
    like = f"%{keyword}%"
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=dev_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        cur.execute(
            f"""
            SELECT
              namecode,
              item_name,
              category_name,
              xml_value_type,
              display_unit
            FROM {qname(dev_db())}.exam_item_master
            WHERE namecode IS NOT NULL
              AND namecode <> ''
              AND (
                CONVERT(namecode USING utf8mb4) COLLATE utf8mb4_0900_ai_ci LIKE %s
                OR item_name LIKE %s
                OR category_name LIKE %s
              )
            ORDER BY
              CASE
                WHEN CONVERT(namecode USING utf8mb4) COLLATE utf8mb4_0900_ai_ci = %s THEN 0
                WHEN item_name = %s THEN 1
                WHEN CONVERT(namecode USING utf8mb4) COLLATE utf8mb4_0900_ai_ci LIKE %s THEN 2
                WHEN item_name LIKE %s THEN 3
                ELSE 4
              END,
              COALESCE(category_name, ''),
              COALESCE(item_name, ''),
              namecode
            LIMIT 60
            """,
            (like, like, like, keyword, keyword, f"{keyword}%", f"{keyword}%"),
        )
        rows = [json_safe_mapping(dict(row)) for row in cur.fetchall()]
        cur.close()
    return JSONResponse({"items": rows, "limit": 60})


def exam_item_master_filters_from_query(query: Mapping[str, str]) -> dict[str, str]:
    return {
        "keyword": str(query.get("keyword") or "").strip(),
        "result_code_oid": str(query.get("result_code_oid") or "").strip(),
        "xml_value_type": str(query.get("xml_value_type") or "").strip().upper(),
        "category_name": str(query.get("category_name") or "").strip(),
        "legal_report": str(query.get("legal_report") or "").strip(),
    }


def load_exam_item_master_categories(cur: Any) -> list[str]:
    cur.execute(
        f"""
        SELECT DISTINCT category_name
        FROM {qname(dev_db())}.exam_item_master
        WHERE category_name IS NOT NULL
          AND category_name <> ''
        ORDER BY category_name
        """
    )
    return [str(row.get("category_name") or "") for row in cur.fetchall() if row.get("category_name")]


def load_exam_item_master_rows(cur: Any, filters: Mapping[str, str], *, limit: int = 200) -> list[dict[str, Any]]:
    where = ["1 = 1"]
    params: list[Any] = []
    keyword = str(filters.get("keyword") or "").strip()
    if keyword:
        like = f"%{keyword}%"
        where.append(
            """(
              CONVERT(namecode USING utf8mb4) COLLATE utf8mb4_unicode_ci LIKE %s
              OR item_name COLLATE utf8mb4_unicode_ci LIKE %s
              OR category_name COLLATE utf8mb4_unicode_ci LIKE %s
              OR CONVERT(identity_item_code USING utf8mb4) COLLATE utf8mb4_unicode_ci LIKE %s
              OR identity_item_name COLLATE utf8mb4_unicode_ci LIKE %s
              OR method_name COLLATE utf8mb4_unicode_ci LIKE %s
              OR CONVERT(result_code_oid USING utf8mb4) COLLATE utf8mb4_unicode_ci LIKE %s
              OR notes COLLATE utf8mb4_unicode_ci LIKE %s
            )"""
        )
        params.extend([like] * 8)

    result_code_oid = str(filters.get("result_code_oid") or "").strip()
    if result_code_oid:
        where.append("CONVERT(result_code_oid USING utf8mb4) COLLATE utf8mb4_unicode_ci LIKE %s")
        params.append(f"%{result_code_oid}%")

    xml_value_type = str(filters.get("xml_value_type") or "").strip().upper()
    if xml_value_type in {"PQ", "CD", "CO", "ST"}:
        where.append("xml_value_type = %s")
        params.append(xml_value_type)

    category_name = str(filters.get("category_name") or "").strip()
    if category_name:
        where.append("category_name = %s")
        params.append(category_name)

    legal_report = str(filters.get("legal_report") or "").strip()
    if legal_report in {"1", "0"}:
        where.append("annex2_legal_report_flag = %s")
        params.append(int(legal_report))

    params.append(limit)
    cur.execute(
        f"""
        SELECT
          namecode,
          item_name,
          xml_value_type,
          item_code_oid,
          result_code_oid,
          display_unit,
          ucum_unit,
          method_name,
          category_name,
          data_type_label,
          xml_method_code,
          value_method,
          nullflavor_allowed,
          notes,
          kubun_no,
          kubun_name,
          jun_no,
          identity_item_code,
          identity_item_name,
          annex2_exec_requirement,
          annex2_legal_report_flag,
          cda_section_code_default,
          annex2_series_group_identifier,
          annex2_series_group_relation_code
        FROM {qname(dev_db())}.exam_item_master
        WHERE {" AND ".join(where)}
        ORDER BY
          COALESCE(kubun_no, 999999),
          COALESCE(jun_no, 999999),
          COALESCE(category_name, ''),
          COALESCE(identity_item_code, namecode),
          namecode
        LIMIT %s
        """,
        tuple(params),
    )
    return [dict(row) for row in cur.fetchall()]


def load_norm_variants_by_oid(cur: Any, result_code_oids: Sequence[str]) -> dict[str, list[dict[str, Any]]]:
    oids = [oid for oid in dict.fromkeys(str(oid or "").strip() for oid in result_code_oids) if oid]
    if not oids:
        return {}
    placeholders = ", ".join(["%s"] * len(oids))
    cur.execute(
        f"""
        SELECT
          result_code_oid,
          raw_value_utf8,
          normalized_code,
          code_system,
          display_name,
          is_canonical,
          priority,
          is_active,
          note
        FROM {qname(master_db())}.norm_variants
        WHERE result_code_oid IN ({placeholders})
        ORDER BY result_code_oid, is_canonical DESC, priority, normalized_code, raw_value_utf8
        LIMIT 2000
        """,
        tuple(oids),
    )
    variants: dict[str, list[dict[str, Any]]] = {}
    for row in cur.fetchall():
        item = dict(row)
        oid = str(item.get("result_code_oid") or "").strip()
        if oid:
            variants.setdefault(oid, []).append(item)
    return variants


def load_norm_variant_summary(cur: Any) -> dict[str, int]:
    cur.execute(
        f"""
        SELECT
          COUNT(*) AS variant_count,
          COALESCE(SUM(CASE WHEN is_canonical = 1 THEN 1 ELSE 0 END), 0) AS canonical_count,
          COUNT(DISTINCT result_code_oid) AS oid_count,
          COUNT(DISTINCT CASE WHEN is_canonical = 1 THEN result_code_oid END) AS canonical_oid_count
        FROM {qname(master_db())}.norm_variants
        WHERE is_active = 1
        """
    )
    row = cur.fetchone() or {}
    return {
        "variant_count": int(row.get("variant_count") or 0),
        "canonical_count": int(row.get("canonical_count") or 0),
        "oid_count": int(row.get("oid_count") or 0),
        "canonical_oid_count": int(row.get("canonical_oid_count") or 0),
    }


def json_safe_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def json_safe_mapping(row: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): json_safe_value(value) for key, value in row.items()}


def build_exam_item_master_lookup_copy_payload(item: Mapping[str, Any]) -> str:
    payload = {
        "debug_scope": "exam_item_lookup",
        "source": "health_exam_admin.exam_item_master",
        "namecode": item.get("namecode"),
        "item_name": item.get("item_name"),
        "identity_item_code": item.get("identity_item_code"),
        "identity_item_name": item.get("identity_item_name"),
        "xml_value_type": item.get("xml_value_type"),
        "item_code_oid": item.get("item_code_oid"),
        "result_code_oid": item.get("result_code_oid"),
        "lookup": {
            "master": {
                "table": f"{dev_db()}.exam_item_master",
                "where": {"namecode": item.get("namecode")},
                "also_check": {"identity_item_code": item.get("identity_item_code")},
            },
            "values": {
                "table": f"{health_db()}.exam_item_values",
                "where": {"namecode": item.get("namecode")},
            },
            "cases": {
                "table": f"{health_db()}.exam_export_cases",
                "how": "exam_item_valuesからcase_id/ledger_id/event_idで突き合わせる",
            },
            "norm": {
                "table": f"{master_db()}.norm_variants",
                "where": {"result_code_oid": item.get("result_code_oid")},
                "when": "result_code_oidがあるCD/CO系の値だけ確認する",
            },
        },
    }
    return json.dumps({key: json_safe_value(value) for key, value in payload.items()}, ensure_ascii=False, indent=2)


def build_exam_item_master_detail_copy_payload(
    item: Mapping[str, Any],
    *,
    scope: str,
    oid_peer_items: Sequence[Mapping[str, Any]] | None = None,
) -> str:
    norm_rows = [json_safe_mapping(row) for row in item.get("norm_variant_rows", [])]
    standard_rows = [row for row in norm_rows if int(row.get("is_canonical") or 0) == 1 and int(row.get("is_active") or 0) == 1]
    item_payload = {
        "namecode": item.get("namecode"),
        "item_name": item.get("item_name"),
        "category_name": item.get("category_name"),
        "kubun_no": item.get("kubun_no"),
        "kubun_name": item.get("kubun_name"),
        "jun_no": item.get("jun_no"),
        "identity_item_code": item.get("identity_item_code"),
        "identity_item_name": item.get("identity_item_name"),
        "xml_value_type": item.get("xml_value_type"),
        "data_type_label": item.get("data_type_label"),
        "display_unit": item.get("display_unit"),
        "ucum_unit": item.get("ucum_unit"),
        "value_method": item.get("value_method"),
        "nullflavor_allowed": item.get("nullflavor_allowed"),
        "item_code_oid": item.get("item_code_oid"),
        "result_code_oid": item.get("result_code_oid"),
        "xml_method_code": item.get("xml_method_code"),
        "cda_section_code_default": item.get("cda_section_code_default"),
        "annex2_legal_report_flag": item.get("annex2_legal_report_flag"),
        "annex2_exec_requirement": item.get("annex2_exec_requirement"),
        "annex2_series_group_identifier": item.get("annex2_series_group_identifier"),
        "annex2_series_group_relation_code": item.get("annex2_series_group_relation_code"),
        "notes": item.get("notes"),
    }
    payload = {
        "copy_scope": scope,
        "source": "health_exam_admin.exam_item_master",
        "lookup_keys": {
            "namecode": item.get("namecode"),
            "identity_item_code": item.get("identity_item_code"),
            "result_code_oid": item.get("result_code_oid"),
            "item_code_oid": item.get("item_code_oid"),
            "xml_value_type": item.get("xml_value_type"),
        },
        "table_hints": [
            {
                "table": f"{dev_db()}.exam_item_master",
                "purpose": "健診項目定義。namecode、値型、単位、法定出力、OID、同一性項目を確認する。",
                "lookup": {
                    "primary": {"namecode": item.get("namecode")},
                    "related": {"identity_item_code": item.get("identity_item_code")},
                    "oid": {
                        "item_code_oid": item.get("item_code_oid"),
                        "result_code_oid": item.get("result_code_oid"),
                    },
                },
            },
            {
                "table": f"{master_db()}.norm_variants",
                "purpose": "CD/COなど結果コードOIDを持つ値の標準コードとraw値の揺れ登録を確認する。",
                "lookup": {"result_code_oid": item.get("result_code_oid")},
                "note": "result_code_oidがnullのST/PQ項目では通常参照対象なし。",
            },
            {
                "table": f"{health_db()}.exam_item_values",
                "purpose": "取り込み後の値。実データ側でこのnamecodeにどんなraw/adopted値が入ったか確認する。",
                "lookup": {"namecode": item.get("namecode")},
            },
            {
                "table": f"{health_db()}.exam_export_cases",
                "purpose": "case単位の採用・出力状態を確認する。個人やeventで絞ってからexam_item_valuesと突き合わせる。",
                "lookup": {"related_item_namecode": item.get("namecode")},
            },
        ],
        "item": {key: json_safe_value(value) for key, value in item_payload.items()},
    }
    if scope == "namecode":
        payload["norm"] = {
            "standard_codes": standard_rows,
            "variants": norm_rows,
        }
    else:
        payload["result_code_oid"] = item.get("result_code_oid")
        payload["items_sharing_result_code_oid"] = [
            {
                "namecode": peer.get("namecode"),
                "item_name": peer.get("item_name"),
                "xml_value_type": peer.get("xml_value_type"),
                "display_unit": peer.get("display_unit"),
                "method_name": peer.get("method_name"),
            }
            for peer in (oid_peer_items or [item])
        ]
        payload["norm"] = {
            "standard_codes": standard_rows,
            "variants": norm_rows,
        }
    return json.dumps(payload, ensure_ascii=False, indent=2)


@app.get("/utilities/exam-item-master", response_class=HTMLResponse)
def exam_item_master_browser(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user

    filters = exam_item_master_filters_from_query(request.query_params)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=dev_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        rows = load_exam_item_master_rows(cur, filters)
        categories = load_exam_item_master_categories(cur)
        result_code_oids = [str(row.get("result_code_oid") or "").strip() for row in rows]
        norm_variants_by_oid = load_norm_variants_by_oid(cur, result_code_oids)
        norm_variant_summary = load_norm_variant_summary(cur)
        cur.close()

    rows_by_oid: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        oid = str(row.get("result_code_oid") or "").strip()
        if oid:
            rows_by_oid.setdefault(oid, []).append(row)

    for row in rows:
        oid = str(row.get("result_code_oid") or "").strip()
        row["norm_variant_rows"] = norm_variants_by_oid.get(oid, [])
        row["standard_code_rows"] = [
            variant
            for variant in row["norm_variant_rows"]
            if int(variant.get("is_canonical") or 0) == 1 and int(variant.get("is_active") or 0) == 1
        ]
        row["lookup_copy_text"] = build_exam_item_master_lookup_copy_payload(row)
        row["detail_copy_text"] = build_exam_item_master_detail_copy_payload(
            row,
            scope="result_code_oid",
            oid_peer_items=rows_by_oid.get(oid) or [row],
        )

    return templates.TemplateResponse(
        "exam_item_master.html",
        {
            "request": request,
            "user": user,
            "filters": filters,
            "rows": rows,
            "categories": categories,
            "row_limit": 200,
            "norm_variant_summary": norm_variant_summary,
            "can_copy_master_debug": can_manage_business_settings(user),
        },
    )


@app.get("/utilities/csv-mapping-lab", response_class=HTMLResponse)
def csv_mapping_lab(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    selected_file_id = parse_positive_int(request.query_params.get("analysis_file_id"), default=0, maximum=999999999)
    column_page = parse_positive_int(request.query_params.get("column_page"), default=1, maximum=999999)
    return templates.TemplateResponse(
        "csv_mapping_lab.html",
        csv_mapping_lab_default_context(
            request,
            user,
            message=request.query_params.get("message"),
            error=request.query_params.get("error"),
            selected_file_id=selected_file_id or None,
            column_page=column_page or 1,
        ),
    )


@app.post("/utilities/csv-mapping-lab/upload", response_class=HTMLResponse)
async def upload_csv_mapping_lab(
    request: Request,
    csv_file: UploadFile = File(...),
    facility_code: str = Form(""),
    facility_name: str = Form(""),
    header_row_no: int = Form(1),
    data_start_row_no: int = Form(2),
    encoding: str = Form(""),
    replace_source_sha: str | None = Form(None),
) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    safe_name = Path(csv_file.filename or "upload.csv").name
    if not safe_name.lower().endswith(".csv"):
        return RedirectResponse("/utilities/csv-mapping-lab?error=CSVファイルを選択してください。", status_code=303)
    upload_dir = Path(tempfile.gettempdir()) / "csv_mapping_lab_uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)
    upload_path = upload_dir / f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{safe_name}"
    with upload_path.open("wb") as out:
        shutil.copyfileobj(csv_file.file, out)

    args = argparse.Namespace(
        csv_path=str(upload_path),
        facility_code=facility_code.strip() or None,
        facility_name=facility_name.strip() or None,
        source_folder_name=None,
        header_row_no=max(1, min(int(header_row_no or 1), 20)),
        data_start_row_no=max(1, min(int(data_start_row_no or 2), 50)),
        encoding=encoding.strip() or None,
        delimiter=",",
        quote_char='"',
        sample_limit=20,
        db_prefix=db_prefix(),
        lab_db=CSV_MAPPING_LAB,
        created_by=str(user.get("display_name") or user.get("employee_no") or user.get("app_user_id") or ""),
        memo=None,
        replace_source_sha=bool(replace_source_sha),
        dry_run=False,
    )
    try:
        csv_result = load_csv_result(
            str(upload_path),
            header_count=args.header_row_no,
            delimiter=args.delimiter,
            encoding=args.encoding,
            quote_char=args.quote_char,
            active_header_row_no=args.header_row_no,
            data_start_row_no=args.data_start_row_no,
        )
        parse_status = "OK"
        parse_error = None
        expected_columns = len(csv_result.header_set.normalized_columns)
        row_widths = {len(row) for row in csv_result.rows}
        if any(width != expected_columns for width in row_widths):
            parse_status = "WARNING"
            parse_error = f"列数が揃っていません: header={expected_columns}, row_widths={sorted(row_widths)[:10]}"
        params = load_mysql_base_params(db_prefix())
        with connect_ctx(params, database=CSV_MAPPING_LAB, autocommit=False) as conn:
            analysis_file_id = insert_csv_mapping_analysis(
                conn,
                args=args,
                csv_result=csv_result,
                parse_status=parse_status,
                parse_error=parse_error,
            )
            try:
                apply_rules_to_analysis(
                    conn,
                    lab_db=CSV_MAPPING_LAB,
                    analysis_file_id=analysis_file_id,
                    facility_code=facility_code.strip() or None,
                )
            except Exception as exc:
                LOGGER.warning("failed to apply csv mapping rules: %s", exc)
            conn.commit()
    except Exception as exc:
        return RedirectResponse(f"/utilities/csv-mapping-lab?error={quote(str(exc))}", status_code=303)
    finally:
        try:
            upload_path.unlink(missing_ok=True)
        except OSError:
            LOGGER.warning("failed to delete csv mapping lab upload: %s", upload_path)
    return RedirectResponse(
        f"/utilities/csv-mapping-lab?analysis_file_id={analysis_file_id}&message={quote('CSVを解析しました。')}",
        status_code=303,
    )


@app.post("/utilities/csv-mapping-lab/rules/reapply")
async def reapply_csv_mapping_lab_rules(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    form = await read_form(request)
    analysis_file_id = parse_positive_int(str(form.get("analysis_file_id") or ""), default=0, maximum=999999999)
    if not analysis_file_id:
        return RedirectResponse("/utilities/csv-mapping-lab?error=解析ファイルを選択してください。", status_code=303)
    try:
        params = load_mysql_base_params(db_prefix())
        with connect_ctx(params, database=CSV_MAPPING_LAB, autocommit=False) as conn:
            cur = dict_cursor(conn)
            file_row = load_csv_mapping_lab_file(cur, analysis_file_id=analysis_file_id)
            if not file_row:
                raise ValueError("解析ファイルが見つかりません。")
            result = apply_rules_to_analysis(
                conn,
                lab_db=CSV_MAPPING_LAB,
                analysis_file_id=analysis_file_id,
                facility_code=file_row.get("facility_code"),
            )
            conn.commit()
    except Exception as exc:
        return RedirectResponse(
            csv_mapping_lab_return_url(form, analysis_file_id=analysis_file_id, error=str(exc)),
            status_code=303,
        )
    message = f"ルールを再適用しました。rules={result['rules']} hits={result['hits']} applied={result['applied_columns']}"
    return RedirectResponse(
        csv_mapping_lab_return_url(form, analysis_file_id=analysis_file_id, message=message),
        status_code=303,
    )


@app.post("/utilities/csv-mapping-lab/rules/bulk-set")
async def bulk_set_csv_mapping_lab_rules(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    form = await read_form(request)
    analysis_file_id = parse_positive_int(str(form.get("analysis_file_id") or ""), default=0, maximum=999999999)
    target_kind = str(form.get("target_kind") or "")
    if not analysis_file_id:
        return RedirectResponse("/utilities/csv-mapping-lab?error=解析ファイルを選択してください。", status_code=303)
    if target_kind not in {"IGNORE", "REVIEW", "WATCH"}:
        return RedirectResponse(
            csv_mapping_lab_return_url(form, analysis_file_id=analysis_file_id, error="一括設定できるのは未使用/要確認/監視だけです。"),
            status_code=303,
        )
    return_query = {
        key: values[0]
        for key, values in parse_qs(str(form.get("return_query") or ""), keep_blank_values=False).items()
        if values
    }
    filters = csv_mapping_lab_column_filter_from_query(return_query)
    mapping_strategy = "IGNORE" if target_kind == "IGNORE" else "WATCH_IF_PRESENT" if target_kind == "WATCH" else "NEEDS_CONFIRMATION"
    actor = str(user.get("display_name") or user.get("employee_no") or user.get("app_user_id") or "")
    try:
        params = load_mysql_base_params(db_prefix())
        with connect_ctx(params, database=CSV_MAPPING_LAB, autocommit=False) as conn:
            cur = dict_cursor(conn)
            file_row = load_csv_mapping_lab_file(cur, analysis_file_id=analysis_file_id)
            if not file_row:
                raise ValueError("解析ファイルが見つかりません。")
            columns = load_csv_mapping_lab_filtered_columns_for_bulk(cur, analysis_file_id=analysis_file_id, filters=filters)
            if not columns:
                raise ValueError("一括設定できる列がありません。")
            distinct_rules: dict[tuple[str, str, str], dict[str, Any]] = {}
            for column in columns:
                header_name = str(column.get("header_name") or "")
                normalized_header = str(column.get("normalized_header_name") or normalize_header(header_name) or "")
                value_type = str(column.get("inferred_value_type") or "")
                distinct_rules.setdefault(
                    (header_name, normalized_header, value_type),
                    {"header_name": header_name, "normalized_header": normalized_header, "value_type": value_type},
                )
            for rule in distinct_rules.values():
                cur.execute(
                    f"""
                    INSERT INTO {qname(CSV_MAPPING_LAB)}.`csv_mapping_rules` (
                      `scope`, `facility_code`, `event_id`, `condition_type`, `column_no_min`, `column_no_max`,
                      `header_pattern`, `normalized_header_pattern`, `value_type`, `target_kind`, `target_namecode`,
                      `target_ledger_field`, `mapping_strategy`, `confidence`, `reason`, `created_by`, `updated_by`
                    )
                    VALUES ('facility', %s, NULL, 'normalized_header_exact', NULL, NULL, %s, %s, %s, %s, NULL, NULL, %s, 0.9000, %s, %s, %s)
                    """,
                    (
                        file_row.get("facility_code"),
                        rule["header_name"],
                        rule["normalized_header"],
                        rule["value_type"],
                        target_kind,
                        mapping_strategy,
                        f"解析ID {analysis_file_id} の絞り込み結果から一括作成",
                        actor,
                        actor,
                    ),
                )
            where_sql, where_params = build_csv_mapping_lab_column_where(analysis_file_id=analysis_file_id, filters=filters)
            cur.execute(
                f"""
                UPDATE {qname(CSV_MAPPING_LAB)}.`analysis_columns` AS `analysis_columns`
                SET
                  `candidate_target_kind` = %s,
                  `candidate_namecode` = NULL,
                  `candidate_ledger_field` = NULL,
                  `candidate_confidence` = 1.0000,
                  `decision_status` = %s,
                  `analysis_note` = %s
                WHERE {where_sql}
                """,
                [
                    target_kind,
                    csv_mapping_decision_status_for_target_kind(target_kind),
                    f"manual bulk rule: 解析ID {analysis_file_id} の絞り込み結果から一括設定",
                ] + where_params,
            )
            conn.commit()
    except Exception as exc:
        return RedirectResponse(
            csv_mapping_lab_return_url(form, analysis_file_id=analysis_file_id, error=str(exc)),
            status_code=303,
        )
    label = "未使用" if target_kind == "IGNORE" else "監視" if target_kind == "WATCH" else "要確認"
    return RedirectResponse(
        csv_mapping_lab_return_url(form, analysis_file_id=analysis_file_id, message=f"絞り込み結果 {len(columns)}列を{label}に設定しました。"),
        status_code=303,
    )


@app.post("/utilities/csv-mapping-lab/columns/bulk-action")
async def bulk_action_csv_mapping_lab_columns(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    raw_form = await request.form()
    form = {key: str(value) for key, value in raw_form.multi_items()}
    analysis_file_id = parse_positive_int(str(form.get("analysis_file_id") or ""), default=0, maximum=999999999)
    action = str(form.get("bulk_action") or "")
    raw_column_numbers = raw_form.getlist("column_no")
    column_numbers = [
        number
        for number in (
            parse_positive_int(str(raw or ""), default=0, maximum=999999)
            for raw in raw_column_numbers
        )
        if number
    ][:500]
    if not analysis_file_id:
        return RedirectResponse("/utilities/csv-mapping-lab?error=解析ファイルを選択してください。", status_code=303)
    if not column_numbers:
        return RedirectResponse(
            csv_mapping_lab_return_url(form, analysis_file_id=analysis_file_id, error="一括処理する列を選択してください。"),
            status_code=303,
        )
    if action not in {"IGNORE", "REVIEW", "WATCH", "MACHINE_ACCEPT"}:
        return RedirectResponse(
            csv_mapping_lab_return_url(form, analysis_file_id=analysis_file_id, error="一括処理の内容を選択してください。"),
            status_code=303,
        )
    actor = str(user.get("display_name") or user.get("employee_no") or user.get("app_user_id") or "")
    try:
        params = load_mysql_base_params(db_prefix())
        with connect_ctx(params, database=CSV_MAPPING_LAB, autocommit=False) as conn:
            cur = dict_cursor(conn)
            file_row = load_csv_mapping_lab_file(cur, analysis_file_id=analysis_file_id)
            if not file_row:
                raise ValueError("解析ファイルが見つかりません。")
            columns = load_csv_mapping_lab_columns_by_numbers(cur, analysis_file_id=analysis_file_id, column_numbers=column_numbers)
            if not columns:
                raise ValueError("一括処理できる列がありません。")
            processed = 0
            skipped = 0
            if action in {"IGNORE", "REVIEW", "WATCH"}:
                target_kind = action
                mapping_strategy = "IGNORE" if target_kind == "IGNORE" else "WATCH_IF_PRESENT" if target_kind == "WATCH" else "NEEDS_CONFIRMATION"
                distinct_rules: dict[tuple[str, str, str], dict[str, Any]] = {}
                for column in columns:
                    header_name = str(column.get("header_name") or "")
                    normalized_header = str(column.get("normalized_header_name") or normalize_header(header_name) or "")
                    value_type = str(column.get("inferred_value_type") or "")
                    distinct_rules.setdefault(
                        (header_name, normalized_header, value_type),
                        {"header_name": header_name, "normalized_header": normalized_header, "value_type": value_type},
                    )
                for rule in distinct_rules.values():
                    cur.execute(
                        f"""
                        INSERT INTO {qname(CSV_MAPPING_LAB)}.`csv_mapping_rules` (
                          `scope`, `facility_code`, `event_id`, `condition_type`, `column_no_min`, `column_no_max`,
                          `header_pattern`, `normalized_header_pattern`, `value_type`, `target_kind`, `target_namecode`,
                          `target_ledger_field`, `mapping_strategy`, `confidence`, `reason`, `created_by`, `updated_by`
                        )
                        VALUES ('facility', %s, NULL, 'normalized_header_exact', NULL, NULL, %s, %s, %s, %s, NULL, NULL, %s, 0.9000, %s, %s, %s)
                        """,
                        (
                            file_row.get("facility_code"),
                            rule["header_name"],
                            rule["normalized_header"],
                            rule["value_type"],
                            target_kind,
                            mapping_strategy,
                            f"解析ID {analysis_file_id} の選択列から一括作成",
                            actor,
                            actor,
                        ),
                    )
                placeholders = ", ".join(["%s"] * len(columns))
                cur.execute(
                    f"""
                    UPDATE {qname(CSV_MAPPING_LAB)}.`analysis_columns`
                    SET
                      `candidate_target_kind` = %s,
                      `candidate_namecode` = NULL,
                      `candidate_ledger_field` = NULL,
                      `candidate_confidence` = 1.0000,
                      `decision_status` = %s,
                      `analysis_note` = %s
                    WHERE `analysis_file_id` = %s AND `column_no` IN ({placeholders})
                    """,
                    [
                        target_kind,
                        csv_mapping_decision_status_for_target_kind(target_kind),
                        f"manual selected bulk rule: 解析ID {analysis_file_id} の選択列から一括設定",
                        analysis_file_id,
                    ] + [int(column["column_no"]) for column in columns],
                )
                processed = len(columns)
            else:
                for column in columns:
                    target_kind = str(column.get("candidate_target_kind") or "")
                    target_namecode = str(column.get("candidate_namecode") or "").strip() or None
                    target_ledger_field = str(column.get("candidate_ledger_field") or "").strip() or None
                    if target_kind not in {"LEDGER_FIELD", "EXAM_ITEM_VALUE", "IGNORE", "WATCH"}:
                        skipped += 1
                        continue
                    if target_kind == "EXAM_ITEM_VALUE" and not target_namecode:
                        skipped += 1
                        continue
                    if target_kind == "LEDGER_FIELD" and not target_ledger_field:
                        skipped += 1
                        continue
                    if target_kind != "EXAM_ITEM_VALUE":
                        target_namecode = None
                    if target_kind != "LEDGER_FIELD":
                        target_ledger_field = None
                    header_name = str(column.get("header_name") or "")
                    normalized_header = str(column.get("normalized_header_name") or normalize_header(header_name) or "")
                    mapping_strategy = "IGNORE" if target_kind == "IGNORE" else "WATCH_IF_PRESENT" if target_kind == "WATCH" else "DIRECT"
                    cur.execute(
                        f"""
                        INSERT INTO {qname(CSV_MAPPING_LAB)}.`csv_mapping_rules` (
                          `scope`, `facility_code`, `event_id`, `condition_type`, `column_no_min`, `column_no_max`,
                          `header_pattern`, `normalized_header_pattern`, `value_type`, `target_kind`, `target_namecode`,
                          `target_ledger_field`, `mapping_strategy`, `confidence`, `reason`, `created_by`, `updated_by`
                        )
                        VALUES ('facility', %s, NULL, 'normalized_header_exact', NULL, NULL, %s, %s, %s, %s, %s, %s, %s, 0.9500, %s, %s, %s)
                        """,
                        (
                            file_row.get("facility_code"),
                            header_name,
                            normalized_header,
                            column.get("inferred_value_type"),
                            target_kind,
                            target_namecode,
                            target_ledger_field,
                            mapping_strategy,
                            f"機械判定を選択列から一括採用: 解析ID {analysis_file_id} 列 {column.get('column_no')}",
                            actor,
                            actor,
                        ),
                    )
                    cur.execute(
                        f"""
                        UPDATE {qname(CSV_MAPPING_LAB)}.`analysis_columns`
                        SET
                          `candidate_target_kind` = %s,
                          `candidate_namecode` = %s,
                          `candidate_ledger_field` = %s,
                          `candidate_confidence` = 1.0000,
                          `decision_status` = %s,
                          `analysis_note` = %s
                        WHERE `analysis_file_id` = %s AND `column_no` = %s
                        """,
                        (
                            target_kind,
                            target_namecode,
                            target_ledger_field,
                            csv_mapping_decision_status_for_target_kind(target_kind),
                            f"machine candidate accepted by selected bulk: 解析ID {analysis_file_id} 列 {column.get('column_no')}",
                            analysis_file_id,
                            int(column["column_no"]),
                        ),
                    )
                    processed += 1
                if processed == 0:
                    raise ValueError("採用できる機械判定が選択列にありません。")
            conn.commit()
    except Exception as exc:
        return RedirectResponse(
            csv_mapping_lab_return_url(form, analysis_file_id=analysis_file_id, error=str(exc)),
            status_code=303,
        )
    action_label = {"IGNORE": "未使用", "REVIEW": "要確認", "WATCH": "監視", "MACHINE_ACCEPT": "機械OK"}[action]
    extra = f"（スキップ {skipped}列）" if skipped else ""
    return RedirectResponse(
        csv_mapping_lab_return_url(form, analysis_file_id=analysis_file_id, message=f"選択した{processed}列を{action_label}で一括処理しました。{extra}"),
        status_code=303,
    )


@app.post("/utilities/csv-mapping-lab/candidates/accept")
async def accept_csv_mapping_lab_candidate(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    form = await read_form(request)
    analysis_file_id = parse_positive_int(str(form.get("analysis_file_id") or ""), default=0, maximum=999999999)
    column_no = parse_positive_int(str(form.get("column_no") or ""), default=0, maximum=999999)
    if not analysis_file_id or not column_no:
        return RedirectResponse("/utilities/csv-mapping-lab?error=解析列を選択してください。", status_code=303)
    scope = str(form.get("scope") or "facility")
    if scope not in {"global", "facility", "event"}:
        scope = "facility"
    condition_type = str(form.get("condition_type") or "normalized_header_exact")
    if condition_type not in {"header_exact", "normalized_header_exact", "header_contains"}:
        condition_type = "normalized_header_exact"
    actor = str(user.get("display_name") or user.get("employee_no") or user.get("app_user_id") or "")
    try:
        params = load_mysql_base_params(db_prefix())
        with connect_ctx(params, database=CSV_MAPPING_LAB, autocommit=False) as conn:
            cur = dict_cursor(conn)
            file_row = load_csv_mapping_lab_file(cur, analysis_file_id=analysis_file_id)
            if not file_row:
                raise ValueError("解析ファイルが見つかりません。")
            cur.execute(
                f"""
                SELECT
                  `header_name`, `normalized_header_name`, `inferred_value_type`,
                  `candidate_target_kind`, `candidate_namecode`, `candidate_ledger_field`
                FROM {qname(CSV_MAPPING_LAB)}.`analysis_columns`
                WHERE `analysis_file_id` = %s AND `column_no` = %s
                """,
                (analysis_file_id, column_no),
            )
            column = cur.fetchone()
            if not column:
                raise ValueError("解析列が見つかりません。")
            target_kind = str(column.get("candidate_target_kind") or "")
            target_namecode = str(column.get("candidate_namecode") or "").strip() or None
            target_ledger_field = str(column.get("candidate_ledger_field") or "").strip() or None
            if target_kind not in {"LEDGER_FIELD", "EXAM_ITEM_VALUE", "IGNORE", "WATCH"}:
                raise ValueError("採用できる機械判定がありません。")
            if target_kind == "EXAM_ITEM_VALUE" and not target_namecode:
                raise ValueError("機械判定にnamecodeがありません。")
            if target_kind == "LEDGER_FIELD" and not target_ledger_field:
                raise ValueError("機械判定にledger fieldがありません。")
            if target_kind != "EXAM_ITEM_VALUE":
                target_namecode = None
            if target_kind != "LEDGER_FIELD":
                target_ledger_field = None
            header_name = str(column.get("header_name") or "")
            normalized_header = str(column.get("normalized_header_name") or normalize_header(header_name) or "")
            mapping_strategy = "IGNORE" if target_kind == "IGNORE" else "WATCH_IF_PRESENT" if target_kind == "WATCH" else "DIRECT"
            decision_status = csv_mapping_decision_status_for_target_kind(target_kind)
            cur.execute(
                f"""
                INSERT INTO {qname(CSV_MAPPING_LAB)}.`csv_mapping_rules` (
                  `scope`, `facility_code`, `event_id`, `condition_type`, `column_no_min`, `column_no_max`, `header_pattern`,
                  `normalized_header_pattern`, `value_type`, `target_kind`, `target_namecode`,
                  `target_ledger_field`, `mapping_strategy`, `confidence`, `reason`,
                  `created_by`, `updated_by`
                )
                VALUES (%s, %s, %s, %s, NULL, NULL, %s, %s, %s, %s, %s, %s, %s, 0.9500, %s, %s, %s)
                """,
                (
                    scope,
                    file_row.get("facility_code") if scope == "facility" else None,
                    file_row.get("event_id") if scope == "event" else None,
                    condition_type,
                    header_name,
                    normalized_header,
                    column.get("inferred_value_type"),
                    target_kind,
                    target_namecode,
                    target_ledger_field,
                    mapping_strategy,
                    f"機械判定を採用: 解析ID {analysis_file_id} 列 {column_no}",
                    actor,
                    actor,
                ),
            )
            apply_rules_to_analysis(
                conn,
                lab_db=CSV_MAPPING_LAB,
                analysis_file_id=analysis_file_id,
                facility_code=file_row.get("facility_code"),
            )
            cur.execute(
                f"""
                UPDATE {qname(CSV_MAPPING_LAB)}.`analysis_columns`
                SET
                  `candidate_target_kind` = %s,
                  `candidate_namecode` = %s,
                  `candidate_ledger_field` = %s,
                  `candidate_confidence` = 1.0000,
                  `decision_status` = %s,
                  `analysis_note` = %s
                WHERE `analysis_file_id` = %s AND `column_no` = %s
                """,
                (
                    target_kind,
                    target_namecode,
                    target_ledger_field,
                    decision_status,
                    f"machine candidate accepted: 解析ID {analysis_file_id} 列 {column_no}",
                    analysis_file_id,
                    column_no,
                ),
            )
            conn.commit()
    except Exception as exc:
        return RedirectResponse(
            csv_mapping_lab_return_url(form, analysis_file_id=analysis_file_id, error=str(exc)),
            status_code=303,
        )
    return RedirectResponse(
        csv_mapping_lab_return_url(form, analysis_file_id=analysis_file_id, message="機械判定を採用しました。"),
        status_code=303,
    )


@app.post("/utilities/csv-mapping-lab/rules/create")
async def create_csv_mapping_lab_rule(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    form = await read_form(request)
    analysis_file_id = parse_positive_int(str(form.get("analysis_file_id") or ""), default=0, maximum=999999999)
    column_no = parse_positive_int(str(form.get("column_no") or ""), default=0, maximum=999999)
    if not analysis_file_id or not column_no:
        return RedirectResponse("/utilities/csv-mapping-lab?error=解析列を選択してください。", status_code=303)
    scope = str(form.get("scope") or "facility")
    if scope not in {"global", "facility", "event"}:
        scope = "facility"
    condition_type = str(form.get("condition_type") or "normalized_header_exact")
    if condition_type not in {"header_exact", "normalized_header_exact", "header_contains"}:
        condition_type = "normalized_header_exact"
    target_kind = str(form.get("target_kind") or "")
    if target_kind not in {"LEDGER_FIELD", "EXAM_ITEM_VALUE", "IGNORE", "REVIEW", "WATCH"}:
        return RedirectResponse(
            csv_mapping_lab_return_url(form, analysis_file_id=analysis_file_id, error="target kindを選択してください。"),
            status_code=303,
        )
    target_namecode = str(form.get("target_namecode") or "").strip() or None
    target_ledger_field = str(form.get("target_ledger_field") or "").strip() or None
    if target_kind == "EXAM_ITEM_VALUE" and not target_namecode:
        return RedirectResponse(
            csv_mapping_lab_return_url(form, analysis_file_id=analysis_file_id, error="namecodeを入力してください。"),
            status_code=303,
        )
    if target_kind == "LEDGER_FIELD" and not target_ledger_field:
        return RedirectResponse(
            csv_mapping_lab_return_url(form, analysis_file_id=analysis_file_id, error="ledger fieldを入力してください。"),
            status_code=303,
        )
    if target_kind != "EXAM_ITEM_VALUE":
        target_namecode = None
    if target_kind != "LEDGER_FIELD":
        target_ledger_field = None
    mapping_strategy = "IGNORE" if target_kind == "IGNORE" else "WATCH_IF_PRESENT" if target_kind == "WATCH" else "NEEDS_CONFIRMATION" if target_kind == "REVIEW" else "DIRECT"
    decision_status = csv_mapping_decision_status_for_target_kind(target_kind)
    actor = str(user.get("display_name") or user.get("employee_no") or user.get("app_user_id") or "")
    try:
        params = load_mysql_base_params(db_prefix())
        with connect_ctx(params, database=CSV_MAPPING_LAB, autocommit=False) as conn:
            cur = dict_cursor(conn)
            file_row = load_csv_mapping_lab_file(cur, analysis_file_id=analysis_file_id)
            if not file_row:
                raise ValueError("解析ファイルが見つかりません。")
            cur.execute(
                f"""
                SELECT `header_name`, `normalized_header_name`, `inferred_value_type`
                FROM {qname(CSV_MAPPING_LAB)}.`analysis_columns`
                WHERE `analysis_file_id` = %s AND `column_no` = %s
                """,
                (analysis_file_id, column_no),
            )
            column = cur.fetchone()
            if not column:
                raise ValueError("解析列が見つかりません。")
            header_name = str(column.get("header_name") or "")
            normalized_header = str(column.get("normalized_header_name") or normalize_header(header_name) or "")
            cur.execute(
                f"""
                INSERT INTO {qname(CSV_MAPPING_LAB)}.`csv_mapping_rules` (
                  `scope`, `facility_code`, `event_id`, `condition_type`, `column_no_min`, `column_no_max`, `header_pattern`,
                  `normalized_header_pattern`, `value_type`, `target_kind`, `target_namecode`,
                  `target_ledger_field`, `mapping_strategy`, `confidence`, `reason`,
                  `created_by`, `updated_by`
                )
                VALUES (%s, %s, %s, %s, NULL, NULL, %s, %s, %s, %s, %s, %s, %s, 0.9000, %s, %s, %s)
                """,
                (
                    scope,
                    file_row.get("facility_code") if scope == "facility" else None,
                    file_row.get("event_id") if scope == "event" else None,
                    condition_type,
                    header_name,
                    normalized_header,
                    column.get("inferred_value_type"),
                    target_kind,
                    target_namecode,
                    target_ledger_field,
                    mapping_strategy,
                    f"解析ID {analysis_file_id} 列 {column_no} から作成",
                    actor,
                    actor,
                ),
            )
            apply_rules_to_analysis(
                conn,
                lab_db=CSV_MAPPING_LAB,
                analysis_file_id=analysis_file_id,
                facility_code=file_row.get("facility_code"),
            )
            cur.execute(
                f"""
                UPDATE {qname(CSV_MAPPING_LAB)}.`analysis_columns`
                SET
                  `candidate_target_kind` = %s,
                  `candidate_namecode` = %s,
                  `candidate_ledger_field` = %s,
                  `candidate_confidence` = 1.0000,
                  `decision_status` = %s,
                  `analysis_note` = %s
                WHERE `analysis_file_id` = %s AND `column_no` = %s
                """,
                (
                    target_kind,
                    target_namecode,
                    target_ledger_field,
                    decision_status,
                    f"manual rule: 解析ID {analysis_file_id} 列 {column_no} から作成",
                    analysis_file_id,
                    column_no,
                ),
            )
            conn.commit()
    except Exception as exc:
        return RedirectResponse(
            csv_mapping_lab_return_url(form, analysis_file_id=analysis_file_id, error=str(exc)),
            status_code=303,
        )
    return RedirectResponse(
        csv_mapping_lab_return_url(form, analysis_file_id=analysis_file_id, message="ルールを追加して再適用しました。"),
        status_code=303,
    )


def csv_mapping_prompt_range_label(form: Mapping[str, str]) -> str:
    column_start = parse_positive_int(str(form.get("column_start") or ""), default=0, maximum=999999)
    column_end = parse_positive_int(str(form.get("column_end") or ""), default=0, maximum=999999)
    if column_start and column_end:
        return f"{column_start}-{column_end}"
    if column_start:
        return f"{column_start}-last"
    if column_end:
        return f"1-{column_end}"
    return "all"


def build_csv_mapping_prompt_from_form(form: Mapping[str, str]) -> tuple[int, str]:
    analysis_file_id = parse_positive_int(str(form.get("analysis_file_id") or ""), default=0, maximum=999999999)
    if not analysis_file_id:
        raise ValueError("解析ファイルを選択してください。")
    args = argparse.Namespace(
        analysis_file_id=analysis_file_id,
        db_prefix=db_prefix(),
        lab_db=CSV_MAPPING_LAB,
        output=None,
        column_start=parse_positive_int(str(form.get("column_start") or ""), default=0, maximum=999999) or None,
        column_end=parse_positive_int(str(form.get("column_end") or ""), default=0, maximum=999999) or None,
        only_unreviewed=str(form.get("only_unreviewed") or "") == "1",
        include_sensitive=str(form.get("include_sensitive") or "") == "1",
        nearby_window=parse_positive_int(str(form.get("nearby_window") or "3"), default=3, maximum=10),
    )
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=CSV_MAPPING_LAB, autocommit=True) as conn:
        file_row, columns = fetch_csv_mapping_analysis(conn, lab_db=CSV_MAPPING_LAB, analysis_file_id=analysis_file_id)
    selected_columns = filter_csv_mapping_columns(columns, args=args)
    prompt = build_csv_mapping_prompt(file_row, columns, selected_columns, args=args)
    return analysis_file_id, json.dumps(prompt, ensure_ascii=False, indent=2, default=csv_mapping_json_default)


@app.post("/utilities/csv-mapping-lab/prompt", response_class=HTMLResponse)
async def preview_csv_mapping_lab_prompt(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    form = await read_form(request)
    try:
        analysis_file_id, prompt_json = build_csv_mapping_prompt_from_form(form)
        error = None
    except Exception as exc:
        analysis_file_id = parse_positive_int(str(form.get("analysis_file_id") or ""), default=0, maximum=999999999)
        prompt_json = None
        error = str(exc)
    return templates.TemplateResponse(
        "csv_mapping_lab.html",
        csv_mapping_lab_default_context(
            request,
            user,
            error=error,
            selected_file_id=analysis_file_id or None,
            prompt_json=prompt_json,
            prompt_form=dict(form),
        ),
    )


@app.post("/utilities/csv-mapping-lab/prompt/download")
async def download_csv_mapping_lab_prompt(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    form = await read_form(request)
    try:
        analysis_file_id, prompt_json = build_csv_mapping_prompt_from_form(form)
    except Exception as exc:
        return RedirectResponse(f"/utilities/csv-mapping-lab?error={quote(str(exc))}", status_code=303)
    regulation_text = CSV_MAPPING_LAB_REGULATION_PATH.read_text(encoding="utf-8")
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("REGULATION.md", regulation_text)
        archive.writestr("analysis_prompt.json", prompt_json + "\n")
    zip_buffer.seek(0)
    file_name = f"csv_mapping_prompt_{analysis_file_id}_{csv_mapping_prompt_range_label(form)}.zip"
    headers = {
        "Content-Disposition": f'attachment; filename="{file_name}"',
        "X-Content-Type-Options": "nosniff",
    }
    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers=headers,
    )


def mask_subscriber_text(value: Any, *, keep_end: int = 2) -> str:
    text = str(value or "").strip()
    if not text:
        return "未登録"
    if len(text) <= keep_end:
        return "*" * len(text)
    return "*" * (len(text) - keep_end) + text[-keep_end:]


def subscriber_reference_filters(request: Request) -> dict[str, str]:
    return {
        "subscriber_id": str(request.query_params.get("subscriber_id") or "").strip(),
        "hia_subscriber_id": str(request.query_params.get("hia_subscriber_id") or "").strip(),
        "insurance_symbol": str(request.query_params.get("insurance_symbol") or "").strip(),
        "insurance_number": str(request.query_params.get("insurance_number") or "").strip(),
        "name_kana": str(request.query_params.get("name_kana") or "").strip(),
        "birth": str(request.query_params.get("birth") or "").strip(),
        "insurer_number": str(request.query_params.get("insurer_number") or "").strip(),
        "employee_code": str(request.query_params.get("employee_code") or "").strip(),
    }


def load_subscriber_reference_search_rows(cur: Any, *, filters: Mapping[str, str], pii_level: str) -> list[dict[str, Any]]:
    subscriber_columns = manual_exam_entry_existing_columns(cur, dev_db(), "subscribers")
    where: list[str] = []
    params: list[Any] = []
    field_map = {
        "subscriber_id": "CAST(s.id AS CHAR)",
        "hia_subscriber_id": "s.hia_subscriber_id",
        "insurance_symbol": "s.insurance_symbol",
        "insurance_number": "s.insurance_number",
        "name_kana": "COALESCE(s.name_kana_full_match, s.name_kana_full)",
        "birth": "CAST(s.birth AS CHAR)",
        "insurer_number": "s.insurer_number",
        "employee_code": "s.employee_code" if "employee_code" in subscriber_columns else "NULL",
    }
    for key, expression in field_map.items():
        value = str(filters.get(key) or "").strip()
        if value:
            if expression == "NULL":
                return []
            where.append(f"{expression} LIKE %s")
            params.append(f"%{value}%")
    if not where:
        return []
    event_count_expr = (
        f"(SELECT COUNT(*) FROM {qname(dev_db())}.person_event pe WHERE pe.subscriber_id = s.id)"
        if manual_exam_entry_table_exists(cur, dev_db(), "person_event")
        else "0"
    )
    cur.execute(
        f"""
        SELECT
          s.id AS subscriber_id,
          s.hia_subscriber_id,
          s.name_kana_full,
          s.birth,
          s.insurer_number,
          s.insurance_symbol,
          s.insurance_number,
          {('s.employee_code' if 'employee_code' in subscriber_columns else 'NULL')} AS employee_code,
          {('s.qualification_lost_date' if 'qualification_lost_date' in subscriber_columns else 'NULL')} AS qualification_lost_date,
          s.updated_at,
          (SELECT MAX(eec.exam_date) FROM {qname(health_db())}.exam_export_cases eec WHERE eec.subscriber_id = s.id) AS latest_exam_date,
          {event_count_expr} AS event_count,
          (SELECT COUNT(*) FROM {qname(health_db())}.exam_export_cases eec WHERE eec.subscriber_id = s.id) AS case_count
        FROM {qname(dev_db())}.subscribers s
        WHERE {' AND '.join(where)}
        ORDER BY latest_exam_date DESC, s.id DESC
        LIMIT 100
        """,
        tuple(params),
    )
    rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        if pii_level == "HIDDEN":
            row["name_display"] = "非表示"
            row["birth_display"] = "非表示"
            row["insurance_display"] = "非表示"
        elif pii_level == "FULL":
            row["name_display"] = str(row.get("name_kana_full") or "未登録")
            row["birth_display"] = str(row.get("birth") or "未登録")
            row["insurance_display"] = f"{row.get('insurance_symbol') or '-'} / {row.get('insurance_number') or '-'}"
        else:
            row["name_display"] = mask_subscriber_text(row.get("name_kana_full"), keep_end=2)
            birth = str(row.get("birth") or "")
            row["birth_display"] = f"{birth[:4]}-**-**" if len(birth) >= 4 else "未登録"
            row["insurance_display"] = f"{mask_subscriber_text(row.get('insurance_symbol'))} / {mask_subscriber_text(row.get('insurance_number'), keep_end=4)}"
        row.pop("name_kana_full", None)
        row.pop("birth", None)
        row.pop("insurance_symbol", None)
        row.pop("insurance_number", None)
    return rows


def load_subscriber_reference_detail(cur: Any, *, subscriber_id: int, display_mode: str) -> dict[str, Any] | None:
    subscriber_columns = manual_exam_entry_existing_columns(cur, dev_db(), "subscribers")
    optional_select = lambda column: f"s.{column}" if column in subscriber_columns else f"NULL AS {column}"
    cur.execute(
        f"""
        SELECT
          s.id AS subscriber_id, s.hia_subscriber_id, s.person_id_custom, s.identity_hash,
          s.insurer_number, s.insurance_symbol, s.insurance_number, s.insurance_branchnumber,
          {optional_select('name_kanji_full')}, s.name_kana_full, s.birth, s.gender_code,
          {optional_select('employee_code')}, {optional_select('employer_code')}, {optional_select('department_code')},
          {optional_select('qualification_acquired_date')}, {optional_select('qualification_lost_date')},
          {optional_select('last_change_run_id')}, s.created_at, s.updated_at,
          a.postal_code, a.address_line, a.building
        FROM {qname(dev_db())}.subscribers s
        LEFT JOIN {qname(dev_db())}.subscriber_addresses a
          ON a.subscriber_id = s.id AND a.is_current = 1
        WHERE s.id = %s
        ORDER BY a.address_id DESC
        LIMIT 1
        """,
        (subscriber_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    detail = dict(row)
    contacts: list[dict[str, Any]] = []
    if manual_exam_entry_table_exists(cur, dev_db(), "subscriber_contact_points"):
        cur.execute(
            f"""
            SELECT contact_type, contact_value
            FROM {qname(dev_db())}.subscriber_contact_points
            WHERE subscriber_id = %s AND is_current = 1
            ORDER BY contact_type, contact_point_id DESC
            """,
            (subscriber_id,),
        )
        contacts = [dict(item) for item in cur.fetchall()]
    sensitive_keys = (
        "insurance_symbol", "insurance_number", "insurance_branchnumber", "name_kanji_full",
        "name_kana_full", "birth", "postal_code", "address_line", "building",
    )
    if display_mode == "HIDDEN":
        for key in sensitive_keys:
            detail[key] = None
        detail["contacts"] = []
    elif display_mode == "MASKED":
        detail["name_kanji_full"] = mask_subscriber_text(detail.get("name_kanji_full"), keep_end=1)
        detail["name_kana_full"] = mask_subscriber_text(detail.get("name_kana_full"), keep_end=2)
        birth = str(detail.get("birth") or "")
        detail["birth"] = f"{birth[:4]}-**-**" if len(birth) >= 4 else None
        detail["insurance_symbol"] = mask_subscriber_text(detail.get("insurance_symbol"))
        detail["insurance_number"] = mask_subscriber_text(detail.get("insurance_number"), keep_end=4)
        detail["insurance_branchnumber"] = mask_subscriber_text(detail.get("insurance_branchnumber"), keep_end=1)
        detail["postal_code"] = mask_subscriber_text(detail.get("postal_code"), keep_end=2)
        detail["address_line"] = "登録あり（マスク）" if detail.get("address_line") else None
        detail["building"] = "登録あり（マスク）" if detail.get("building") else None
        detail["contacts"] = [
            {"contact_type": item.get("contact_type"), "contact_value": mask_subscriber_text(item.get("contact_value"), keep_end=4)}
            for item in contacts
        ]
    else:
        detail["contacts"] = contacts
    return detail


def load_subscriber_reference_events(cur: Any, *, subscriber_id: int) -> list[dict[str, Any]]:
    has_person_event = manual_exam_entry_table_exists(cur, dev_db(), "person_event")
    has_status_items = manual_exam_entry_table_exists(cur, dev_db(), "person_event_status_items")
    person_event_select = (
        "pe.person_event_id, pe.result_received_count, pe.delivery_detected_count, "
        "pe.hia_status_code, pe.delivery_target_flag, pe.delivery_exported_flag, "
        "pe.gap_flag, pe.gap_reason, pe.updated_at AS person_event_updated_at"
        if has_person_event
        else "NULL AS person_event_id, 0 AS result_received_count, 0 AS delivery_detected_count, "
             "NULL AS hia_status_code, NULL AS delivery_target_flag, NULL AS delivery_exported_flag, "
             "NULL AS gap_flag, NULL AS gap_reason, NULL AS person_event_updated_at"
    )
    person_event_join = (
        f"LEFT JOIN {qname(dev_db())}.person_event pe ON pe.event_id = ev.event_id AND pe.subscriber_id = %s"
        if has_person_event else ""
    )
    person_event_where = "pe.person_event_id IS NOT NULL OR" if has_person_event else ""
    cur.execute(
        f"""
        SELECT
          ev.event_id, ev.event_name, ev.event_year, ev.event_type,
          {person_event_select},
          (SELECT COUNT(*) FROM {qname(health_db())}.exam_ledgers el WHERE el.event_id = ev.event_id AND el.subscriber_id = %s) AS ledger_count,
          (SELECT GROUP_CONCAT(DISTINCT el.source_type ORDER BY el.source_type SEPARATOR ' / ') FROM {qname(health_db())}.exam_ledgers el WHERE el.event_id = ev.event_id AND el.subscriber_id = %s) AS source_types,
          (SELECT COUNT(*) FROM {qname(health_db())}.exam_export_cases eec WHERE eec.event_id = ev.event_id AND eec.subscriber_id = %s) AS case_count,
          (SELECT MAX(eec.exam_date) FROM {qname(health_db())}.exam_export_cases eec WHERE eec.event_id = ev.event_id AND eec.subscriber_id = %s) AS latest_exam_date,
          (SELECT GROUP_CONCAT(DISTINCT eec.export_readiness_status ORDER BY eec.export_readiness_status SEPARATOR ' / ') FROM {qname(health_db())}.exam_export_cases eec WHERE eec.event_id = ev.event_id AND eec.subscriber_id = %s) AS readiness_statuses,
          (SELECT GROUP_CONCAT(DISTINCT eec.check_status ORDER BY eec.check_status SEPARATOR ' / ') FROM {qname(health_db())}.exam_export_cases eec WHERE eec.event_id = ev.event_id AND eec.subscriber_id = %s) AS check_statuses,
          (SELECT GROUP_CONCAT(DISTINCT eec.xml_export_status ORDER BY eec.xml_export_status SEPARATOR ' / ') FROM {qname(health_db())}.exam_export_cases eec WHERE eec.event_id = ev.event_id AND eec.subscriber_id = %s) AS xml_statuses
        FROM {qname(dev_db())}.event ev
        {person_event_join}
        WHERE {person_event_where}
           EXISTS (SELECT 1 FROM {qname(health_db())}.exam_ledgers el WHERE el.event_id = ev.event_id AND el.subscriber_id = %s)
           OR EXISTS (SELECT 1 FROM {qname(health_db())}.exam_export_cases eec WHERE eec.event_id = ev.event_id AND eec.subscriber_id = %s)
        ORDER BY ev.event_year DESC, ev.event_id DESC
        """,
        (subscriber_id,) * (10 if has_person_event else 9),
    )
    rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        if not has_status_items:
            row["status_items"] = []
            continue
        cur.execute(
            f"""
            SELECT item_code, value_type, value_bool, value_number, value_text, value_code,
                   value_date, value_datetime, value_ref_type, value_ref_id, reason, source_run_id, refreshed_at
            FROM {qname(dev_db())}.person_event_status_items
            WHERE event_id = %s AND subscriber_id = %s
            ORDER BY item_code
            """,
            (row["event_id"], subscriber_id),
        )
        row["status_items"] = [dict(item) for item in cur.fetchall()]
    return rows


def load_subscriber_reference_dashboard(cur: Any, *, subscriber_id: int) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT hia_dashboard_person_id, status, reservation_date, exam_date,
               medical_institution, course_name, first_seen_run_id, last_seen_run_id, updated_at
        FROM {qname(work_other_db())}.hia_dashboard_status
        WHERE subscribers_id = %s AND is_active = 1
        ORDER BY updated_at DESC, hia_dashboard_person_id DESC
        LIMIT 20
        """,
        (subscriber_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def load_subscriber_reference_history(cur: Any, *, subscriber_id: int) -> list[dict[str, Any]]:
    if not manual_exam_entry_table_exists(cur, dev_db(), "subscriber_audit"):
        return []
    cur.execute(
        f"""
        SELECT audit_id, field, changed_at, source,
               CASE WHEN note IS NULL OR note = '' THEN 0 ELSE 1 END AS has_note,
               change_run_id
        FROM {qname(dev_db())}.subscriber_audit
        WHERE subscriber_id = %s
        ORDER BY audit_id DESC
        LIMIT 200
        """,
        (subscriber_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def load_subscriber_reference_view_history(cur: Any, *, subscriber_id: int) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT app_audit_log_id, app_user_id, employee_no, action_code,
               target_schema, target_table, target_id, created_at
        FROM {qname(app_db())}.app_audit_logs
        WHERE target_table = 'subscribers'
          AND target_id = %s
          AND action_code LIKE 'PERSONAL_INFO_%SUBSCRIBER%UTILITY'
        ORDER BY app_audit_log_id DESC
        LIMIT 200
        """,
        (str(subscriber_id),),
    )
    return [dict(row) for row in cur.fetchall()]


def render_subscriber_reference_detail(request: Request, *, subscriber_id: int, full: bool) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_view_subscriber_reference(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    pii_level = subscriber_reference_pii_level(user)
    if full and pii_level != "FULL":
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    display_mode = "FULL" if full else ("HIDDEN" if pii_level == "HIDDEN" else "MASKED")
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=dev_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        detail = load_subscriber_reference_detail(cur, subscriber_id=subscriber_id, display_mode=display_mode)
        if not detail:
            conn.rollback()
            return RedirectResponse("/utilities/subscribers?error=加入者が見つかりません。", status_code=303)
        events = load_subscriber_reference_events(cur, subscriber_id=subscriber_id)
        dashboard_rows = load_subscriber_reference_dashboard(cur, subscriber_id=subscriber_id)
        history_rows = load_subscriber_reference_history(cur, subscriber_id=subscriber_id) if can_view_subscriber_history(user) else []
        view_history_rows = load_subscriber_reference_view_history(cur, subscriber_id=subscriber_id) if can_view_subscriber_history(user) else []
        if audit_enabled(cur):
            log_audit(
                cur,
                request=request,
                user=user,
                action_code="PERSONAL_INFO_VIEW_SUBSCRIBER_FULL_UTILITY" if full else "PERSONAL_INFO_VIEW_SUBSCRIBER_UTILITY",
                target_schema=dev_db(),
                target_table="subscribers",
                target_id=str(subscriber_id),
                after={"subscriber_id": subscriber_id, "display_mode": display_mode},
            )
        conn.commit()
    return templates.TemplateResponse(
        "subscriber_reference_detail.html",
        {
            "request": request, "user": user, "subscriber": detail, "events": events,
            "dashboard_rows": dashboard_rows, "history_rows": history_rows,
            "view_history_rows": view_history_rows, "display_mode": display_mode,
            "can_open_full": pii_level == "FULL", "can_view_history": can_view_subscriber_history(user),
        },
    )


@app.get("/utilities/subscribers", response_class=HTMLResponse)
def subscriber_reference_search(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_view_subscriber_reference(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    filters = subscriber_reference_filters(request)
    searched = any(filters.values())
    pii_level = subscriber_reference_pii_level(user)
    requested_display_mode = str(request.query_params.get("display_mode") or "MASKED").strip().upper()
    display_mode = "FULL" if requested_display_mode == "FULL" and pii_level == "FULL" else ("HIDDEN" if pii_level == "HIDDEN" else "MASKED")
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=dev_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        rows = load_subscriber_reference_search_rows(cur, filters=filters, pii_level=display_mode) if searched else []
        if searched and audit_enabled(cur):
            log_audit(
                cur, request=request, user=user,
                action_code="PERSONAL_INFO_SEARCH_SUBSCRIBER_FULL_UTILITY" if display_mode == "FULL" else "PERSONAL_INFO_SEARCH_SUBSCRIBER_UTILITY",
                target_schema=dev_db(), target_table="subscribers", target_id=None,
                after={"filter_fields": [key for key, value in filters.items() if value], "result_count": len(rows), "display_mode": display_mode},
            )
        conn.commit()
    return templates.TemplateResponse(
        "subscriber_reference_search.html",
        {"request": request, "user": user, "filters": filters, "rows": rows, "searched": searched,
         "pii_level": pii_level, "display_mode": display_mode,
         "can_open_full": pii_level == "FULL", "error": request.query_params.get("error")},
    )


@app.get("/utilities/subscribers/{subscriber_id}", response_class=HTMLResponse)
def subscriber_reference_detail(request: Request, subscriber_id: int) -> Response:
    return render_subscriber_reference_detail(request, subscriber_id=subscriber_id, full=False)


@app.post("/utilities/subscribers/{subscriber_id}/full", response_class=HTMLResponse)
def subscriber_reference_full_detail(request: Request, subscriber_id: int) -> Response:
    return render_subscriber_reference_detail(request, subscriber_id=subscriber_id, full=True)


@app.get("/utilities/person-selection", response_class=HTMLResponse)
def person_selection_utility(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        events = load_event_options(cur)
    default_event_id = str(events[0]["event_id"]) if events else "2"
    default_insurer_number = event_insurer_number_from_options(events, int(default_event_id))
    return templates.TemplateResponse(
        "person_selection_utility.html",
        {
            "request": request,
            "user": user,
            "events": events,
            "column_options": PERSON_SELECTION_COLUMNS,
            "form": {
                "input_mode": "bulk",
                "event_id": default_event_id,
                "fixed_insurer_number": default_insurer_number,
                "delimiter": "tab",
                "custom_delimiter": "",
                "has_header": "0",
                "raw_text": "",
                "single": {},
                "columns": [
                    "insurer_number",
                    "insurance_symbol",
                    "insurance_number",
                    "name_kana",
                    "birthdate",
                    "gender",
                    "unused",
                    "unused",
                    "unused",
                    "unused",
                    "unused",
                    "unused",
                ],
            },
            "rows": [],
            "summary": {},
        },
    )


@app.post("/utilities/person-selection", response_class=HTMLResponse)
async def resolve_person_selection_utility(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    form = await read_form(request)
    event_id = parse_positive_int(str(form.get("event_id") or "2"), default=2, maximum=999999)
    input_mode = str(form.get("input_mode") or "bulk")
    raw_text = str(form.get("raw_text") or "")
    delimiter = str(form.get("delimiter") or "tab")
    custom_delimiter = str(form.get("custom_delimiter") or "")
    requested_fixed_insurer_number = str(form.get("fixed_insurer_number") or "").strip()
    has_header = str(form.get("has_header") or "") == "1"
    columns = [str(form.get(f"col_{index}") or "unused") for index in range(12)]
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        events = load_event_options(cur)
        fixed_insurer_number = event_insurer_number_from_options(events, event_id) or requested_fixed_insurer_number
    if input_mode == "single":
        rows = build_person_selection_single_row(form=form, fixed_insurer_number=fixed_insurer_number)
    else:
        rows = parse_person_selection_paste(
            raw_text=raw_text,
            delimiter=delimiter,
            custom_delimiter=custom_delimiter,
            has_header=has_header,
            column_map=columns,
            fixed_insurer_number=fixed_insurer_number,
        )
    with connect_ctx(params, database=health_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        rows = resolve_person_selection_rows(cur, event_id=event_id, rows=rows)
    summary: dict[str, int] = {}
    for row in rows:
        status = str(row.get("status") or "UNKNOWN")
        summary[status] = summary.get(status, 0) + 1
    return templates.TemplateResponse(
        "person_selection_utility.html",
        {
            "request": request,
            "user": user,
            "events": events,
            "column_options": PERSON_SELECTION_COLUMNS,
            "form": {
                "input_mode": input_mode,
                "event_id": str(event_id),
                "fixed_insurer_number": fixed_insurer_number,
                "delimiter": delimiter,
                "custom_delimiter": custom_delimiter,
                "has_header": "1" if has_header else "0",
                "raw_text": raw_text,
                "single": {
                    "case_id": str(form.get("single_case_id") or "").strip(),
                    "subscriber_id": str(form.get("single_subscriber_id") or "").strip(),
                    "hia_subscriber_id": str(form.get("single_hia_subscriber_id") or "").strip(),
                    "employee_code": str(form.get("single_employee_code") or "").strip(),
                    "insurer_number": str(form.get("single_insurer_number") or "").strip(),
                    "insurance_symbol": str(form.get("single_insurance_symbol") or "").strip(),
                    "insurance_number": str(form.get("single_insurance_number") or "").strip(),
                    "name_kana": str(form.get("single_name_kana") or "").strip(),
                    "birthdate": str(form.get("single_birthdate") or "").strip(),
                    "gender": str(form.get("single_gender") or "").strip(),
                },
                "columns": columns,
            },
            "rows": rows,
            "summary": summary,
        },
    )


@app.get("/hia/xml-zip-check", response_class=HTMLResponse)
def hia_xml_zip_check(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not xml_zip_check_allowed(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    return templates.TemplateResponse(
        "hia_xml_zip_check.html",
        {
            "request": request,
            "user": user,
            "result": None,
            "error": request.query_params.get("error"),
            "message": request.query_params.get("message"),
            "xsd_dir": str(XML_ZIP_CHECK_XSD_DIR),
            "report_dir": str(XML_ZIP_CHECK_REPORT_DIR),
            "uploaded_files": load_xml_zip_uploaded_files(),
        },
    )


@app.post("/hia/xml-zip-check", response_class=HTMLResponse)
async def run_hia_xml_zip_check(
    request: Request,
    zip_file: UploadFile = File(...),
) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not xml_zip_check_allowed(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    original_filename = safe_upload_file_name(zip_file.filename)
    if not original_filename.lower().endswith(".zip"):
        return templates.TemplateResponse(
            "hia_xml_zip_check.html",
            {
                "request": request,
                "user": user,
                "result": None,
                "error": "ZIPファイルを指定してください。",
                "message": None,
                "xsd_dir": str(XML_ZIP_CHECK_XSD_DIR),
                "report_dir": str(XML_ZIP_CHECK_REPORT_DIR),
                "uploaded_files": load_xml_zip_uploaded_files(),
            },
            status_code=400,
        )

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    upload_dir = HIA_XML_ZIP_CHECK_UPLOAD_DIR / stamp
    upload_dir.mkdir(parents=True, exist_ok=True)
    upload_path = upload_dir / original_filename
    with upload_path.open("wb") as fp:
        while chunk := await zip_file.read(1024 * 1024):
            fp.write(chunk)

    try:
        result = build_xml_zip_check_result(
            upload_path=upload_path,
            original_filename=original_filename,
            fix=False,
        )
        log_app_operation(
            request=request,
            user=user,
            action_code="HIA_XML_ZIP_CHECK",
            target_schema="file",
            target_table="hia_xml_zip_check_uploads",
            target_id=str(upload_path),
            after={
                "original_filename": original_filename,
                "xml_files_seen": result.get("xml_files_seen"),
                "findings": result.get("findings"),
                "errors": result.get("errors"),
                "warnings": result.get("warnings"),
                "report_csv_path": result.get("report_csv_path"),
            },
        )
    except Exception as exc:
        return templates.TemplateResponse(
            "hia_xml_zip_check.html",
            {
                "request": request,
                "user": user,
                "result": None,
                "error": str(exc),
                "message": None,
                "xsd_dir": str(XML_ZIP_CHECK_XSD_DIR),
                "report_dir": str(XML_ZIP_CHECK_REPORT_DIR),
                "uploaded_files": load_xml_zip_uploaded_files(),
            },
            status_code=500,
        )

    return templates.TemplateResponse(
        "hia_xml_zip_check.html",
        {
            "request": request,
            "user": user,
            "result": result,
            "error": None,
            "message": "チェックが完了しました。",
            "xsd_dir": str(XML_ZIP_CHECK_XSD_DIR),
            "report_dir": str(XML_ZIP_CHECK_REPORT_DIR),
            "uploaded_files": load_xml_zip_uploaded_files(),
        },
    )


@app.post("/hia/xml-zip-check/create-fixed", response_class=HTMLResponse)
async def create_fixed_hia_xml_zip(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not xml_zip_check_allowed(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    form = await read_form(request)
    upload_path_text = str(form.get("upload_path") or request.query_params.get("upload_path") or "").strip()
    upload_path = Path(upload_path_text)
    debug = path_debug_payload(submitted_value=upload_path_text, resolved_path=upload_path, allowed_base=HIA_XML_ZIP_CHECK_UPLOAD_DIR)
    debug["form"] = safe_form_debug(form)
    if not upload_path_text or not is_path_under(upload_path, HIA_XML_ZIP_CHECK_UPLOAD_DIR):
        LOGGER.warning("XML ZIP create-fixed rejected outside upload root: %s", json.dumps(debug, ensure_ascii=False))
        return RedirectResponse(
            f"/hia/xml-zip-check?error={quote(path_debug_message('修正できるのはアップロード済みZIPだけです。', debug))}",
            status_code=303,
        )
    if not upload_path.exists() or not upload_path.is_file():
        LOGGER.warning("XML ZIP create-fixed target not found: %s", json.dumps(debug, ensure_ascii=False))
        return RedirectResponse(
            f"/hia/xml-zip-check?error={quote(path_debug_message('アップロード済みZIPが見つかりません。', debug))}",
            status_code=303,
        )

    try:
        manual_text_fixes = manual_text_fixes_from_form(form)
        result = build_xml_zip_check_result(
            upload_path=upload_path,
            original_filename=upload_path.name,
            fix=True,
            manual_text_fixes=manual_text_fixes,
        )
        log_app_operation(
            request=request,
            user=user,
            action_code="HIA_XML_ZIP_CREATE_FIXED",
            target_schema="file",
            target_table="hia_xml_zip_check_uploads",
            target_id=str(upload_path),
            after={
                "original_filename": upload_path.name,
                "fixed_zip_path": result.get("fixed_zip_path"),
                "fixed": result.get("fixed"),
                "findings": result.get("findings"),
                "errors": result.get("errors"),
                "warnings": result.get("warnings"),
                "report_csv_path": result.get("report_csv_path"),
                "manual_text_fixes": len(manual_text_fixes),
            },
        )
    except Exception as exc:
        return templates.TemplateResponse(
            "hia_xml_zip_check.html",
            {
                "request": request,
                "user": user,
                "result": None,
                "error": str(exc),
                "message": None,
                "xsd_dir": str(XML_ZIP_CHECK_XSD_DIR),
                "report_dir": str(XML_ZIP_CHECK_REPORT_DIR),
                "uploaded_files": load_xml_zip_uploaded_files(),
            },
            status_code=500,
        )

    return templates.TemplateResponse(
        "hia_xml_zip_check.html",
        {
            "request": request,
            "user": user,
            "result": result,
            "error": None,
            "message": "修正版ZIPを作成しました。",
            "xsd_dir": str(XML_ZIP_CHECK_XSD_DIR),
            "report_dir": str(XML_ZIP_CHECK_REPORT_DIR),
            "uploaded_files": load_xml_zip_uploaded_files(),
        },
    )


@app.post("/hia/xml-zip-check/recheck", response_class=HTMLResponse)
async def recheck_hia_xml_zip(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not xml_zip_check_allowed(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    form = await read_form(request)
    zip_path_text = str(form.get("zip_path") or request.query_params.get("zip_path") or "").strip()
    zip_path = app_data_path_from_form_value(zip_path_text)
    debug = path_debug_payload(submitted_value=zip_path_text, resolved_path=zip_path, allowed_base=APP_DATA_DIR)
    debug["form"] = safe_form_debug(form)
    if not zip_path_text or not xml_zip_check_input_allowed(zip_path):
        LOGGER.warning("XML ZIP recheck rejected outside data root: %s", json.dumps(debug, ensure_ascii=False))
        return RedirectResponse(
            f"/hia/xml-zip-check?error={quote(path_debug_message('再チェックできるのはdata配下のZIPだけです。', debug))}",
            status_code=303,
        )
    if not zip_path.exists() or not zip_path.is_file():
        LOGGER.warning("XML ZIP recheck target not found: %s", json.dumps(debug, ensure_ascii=False))
        return RedirectResponse(
            f"/hia/xml-zip-check?error={quote(path_debug_message('再チェック対象のZIPが見つかりません。', debug))}",
            status_code=303,
        )

    try:
        LOGGER.info("XML ZIP recheck accepted: %s", json.dumps(debug, ensure_ascii=False))
        result = build_xml_zip_check_result(
            upload_path=zip_path,
            original_filename=zip_path.name,
            fix=False,
        )
        log_app_operation(
            request=request,
            user=user,
            action_code="HIA_XML_ZIP_RECHECK",
            target_schema="file",
            target_table="hia_xml_zip_check_uploads",
            target_id=str(zip_path),
            after={
                "original_filename": zip_path.name,
                "xml_files_seen": result.get("xml_files_seen"),
                "findings": result.get("findings"),
                "errors": result.get("errors"),
                "warnings": result.get("warnings"),
                "report_csv_path": result.get("report_csv_path"),
            },
        )
    except Exception as exc:
        return templates.TemplateResponse(
            "hia_xml_zip_check.html",
            {
                "request": request,
                "user": user,
                "result": None,
                "error": str(exc),
                "message": None,
                "xsd_dir": str(XML_ZIP_CHECK_XSD_DIR),
                "report_dir": str(XML_ZIP_CHECK_REPORT_DIR),
                "uploaded_files": load_xml_zip_uploaded_files(),
            },
            status_code=500,
        )

    return templates.TemplateResponse(
        "hia_xml_zip_check.html",
        {
            "request": request,
            "user": user,
            "result": result,
            "error": None,
            "message": "再チェックが完了しました。",
            "xsd_dir": str(XML_ZIP_CHECK_XSD_DIR),
            "report_dir": str(XML_ZIP_CHECK_REPORT_DIR),
            "uploaded_files": load_xml_zip_uploaded_files(),
        },
    )


@app.post("/hia/xml-zip-check/delete-upload")
async def delete_hia_xml_zip_upload(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not xml_zip_check_allowed(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    form = await read_form(request)
    upload_path_text = str(form.get("upload_path") or request.query_params.get("upload_path") or "").strip()
    upload_path = Path(upload_path_text)
    debug = path_debug_payload(submitted_value=upload_path_text, resolved_path=upload_path, allowed_base=HIA_XML_ZIP_CHECK_UPLOAD_DIR)
    debug["form"] = safe_form_debug(form)
    if not upload_path_text or not is_path_under(upload_path, HIA_XML_ZIP_CHECK_UPLOAD_DIR):
        LOGGER.warning("XML ZIP delete rejected outside upload root: %s", json.dumps(debug, ensure_ascii=False))
        return RedirectResponse(
            f"/hia/xml-zip-check?error={quote(path_debug_message('削除できるのはアップロード済みZIPだけです。', debug))}",
            status_code=303,
        )
    if not upload_path.exists():
        return RedirectResponse(
            f"/hia/xml-zip-check?message={quote('アップロードファイルは既にありません。')}",
            status_code=303,
        )
    if not upload_path.is_file():
        return RedirectResponse(
            f"/hia/xml-zip-check?error={quote('削除対象がファイルではありません。')}",
            status_code=303,
        )

    file_size = upload_path.stat().st_size
    upload_path.unlink()
    try:
        parent = upload_path.parent
        if parent != HIA_XML_ZIP_CHECK_UPLOAD_DIR and is_path_under(parent, HIA_XML_ZIP_CHECK_UPLOAD_DIR):
            parent.rmdir()
    except OSError:
        pass
    log_app_operation(
        request=request,
        user=user,
        action_code="HIA_XML_ZIP_DELETE_UPLOAD",
        target_schema="file",
        target_table="hia_xml_zip_check_uploads",
        target_id=str(upload_path),
        after={"deleted_path": str(upload_path), "size_bytes": file_size},
    )

    return RedirectResponse(
        f"/hia/xml-zip-check?message={quote('アップロードファイルを削除しました。CSVレポートは残しています。')}",
        status_code=303,
    )


@app.post("/hia/fund-delivery/run", response_class=HTMLResponse)
async def run_hia_fund_delivery(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, ("hia_upload.perform", "hia_upload_status.edit", "users.manage")):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await read_form(request)
    action = form.get("action", "").strip()
    if action == "mark_submitted" and not has_any_permission(user, ("hia_upload_status.edit", "users.manage")):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            raw = load_fund_delivery_page_config()
            message, dry_run = run_hia_fund_delivery_step(cur, action=action, raw=raw, user=user)
            if dry_run:
                conn.rollback()
            else:
                conn.commit()
        except Exception as exc:
            conn.rollback()
            return RedirectResponse(f"/hia/fund-delivery?error={quote(str(exc))}", status_code=303)
    return RedirectResponse(f"/hia/fund-delivery?message={quote(message)}", status_code=303)


@app.post("/hia/upload-work/zips/{xml_export_zip_id}/status", response_class=HTMLResponse)
async def update_hia_upload_zip_status(request: Request, xml_export_zip_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, ("hia_upload_status.edit", "users.manage")):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await read_form(request)
    target_status = str(form.get("target_status") or "").strip()
    if target_status not in {"PENDING", "UPLOADED", "UPLOAD_ERROR", "PARTIAL", "CONFIRMED"}:
        return RedirectResponse(f"/hia/upload-work?error={quote('未対応のZIP状態です。')}", status_code=303)
    note = str(form.get("note") or "").strip()
    error_summary = str(form.get("error_summary") or "").strip()
    actor = fund_delivery_actor(user)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            cur.execute(
                f"""
                UPDATE {qname(health_db())}.xml_export_zips
                   SET hia_upload_status = %s,
                       hia_uploaded_at = CASE WHEN %s IN ('UPLOADED', 'CONFIRMED') THEN CURRENT_TIMESTAMP(3) ELSE hia_uploaded_at END,
                       hia_uploaded_by = CASE WHEN %s IN ('UPLOADED', 'CONFIRMED') THEN %s ELSE hia_uploaded_by END,
                       hia_upload_checked_at = CURRENT_TIMESTAMP(3),
                       hia_upload_checked_by = %s,
                       hia_upload_error_summary = NULLIF(%s, ''),
                       hia_upload_note = NULLIF(%s, '')
                 WHERE xml_export_zip_id = %s
                """,
                (target_status, target_status, target_status, actor, actor, error_summary, note, xml_export_zip_id),
            )
            member_status = {
                "UPLOADED": "UPLOADED",
                "CONFIRMED": "UPLOADED",
                "UPLOAD_ERROR": "UPLOAD_ERROR",
                "PARTIAL": "UPLOAD_ERROR",
                "PENDING": "PENDING",
            }[target_status]
            cur.execute(
                f"""
                UPDATE {qname(health_db())}.xml_export_members
                   SET hia_upload_status = %s,
                       hia_upload_error_code = CASE
                         WHEN %s IN ('UPLOAD_ERROR', 'PARTIAL') THEN 'ZIP_STATUS'
                         WHEN %s = 'PENDING' THEN NULL
                         ELSE hia_upload_error_code
                       END,
                       hia_upload_error_message = CASE
                         WHEN %s IN ('UPLOAD_ERROR', 'PARTIAL') THEN NULLIF(%s, '')
                         WHEN %s = 'PENDING' THEN NULL
                         ELSE hia_upload_error_message
                       END,
                       hia_upload_note = NULLIF(%s, ''),
                       hia_uploaded_at = CASE WHEN %s = 'UPLOADED' THEN CURRENT_TIMESTAMP(3) ELSE hia_uploaded_at END,
                       hia_uploaded_by = CASE WHEN %s = 'UPLOADED' THEN %s ELSE hia_uploaded_by END
                 WHERE xml_export_zip_id = %s
                """,
                (
                    member_status,
                    target_status,
                    target_status,
                    target_status,
                    error_summary,
                    target_status,
                    note,
                    member_status,
                    member_status,
                    actor,
                    xml_export_zip_id,
                ),
            )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            return RedirectResponse(f"/hia/upload-work?error={quote(str(exc))}", status_code=303)
    log_app_operation(
        request=request,
        user=user,
        action_code="HIA_UPLOAD_ZIP_STATUS_UPDATE",
        target_schema=health_db(),
        target_table="xml_export_zips",
        target_id=str(xml_export_zip_id),
        after={"status": target_status, "note": note, "error_summary": error_summary},
    )
    return RedirectResponse(f"/hia/upload-work?message={quote('ZIPのHIAアップロード状態を更新しました。')}", status_code=303)


@app.post("/hia/upload-work/members/{xml_export_member_id}/status", response_class=HTMLResponse)
async def update_hia_upload_member_status(request: Request, xml_export_member_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, ("hia_upload_status.edit", "users.manage")):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await read_form(request)
    target_status = str(form.get("target_status") or "").strip()
    if target_status not in {"PENDING", "UPLOADED", "UPLOAD_ERROR", "EXCLUDED"}:
        return RedirectResponse(f"/hia/upload-work?error={quote('未対応の個人XML状態です。')}", status_code=303)
    error_code = str(form.get("error_code") or "").strip()
    error_message = str(form.get("error_message") or "").strip()
    note = str(form.get("note") or "").strip()
    actor = fund_delivery_actor(user)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            cur.execute(
                f"""
                UPDATE {qname(health_db())}.xml_export_members
                   SET hia_upload_status = %s,
                       hia_upload_error_code = NULLIF(%s, ''),
                       hia_upload_error_message = NULLIF(%s, ''),
                       hia_upload_note = NULLIF(%s, ''),
                       hia_uploaded_at = CASE WHEN %s = 'UPLOADED' THEN CURRENT_TIMESTAMP(3) ELSE hia_uploaded_at END,
                       hia_uploaded_by = CASE WHEN %s = 'UPLOADED' THEN %s ELSE hia_uploaded_by END
                 WHERE xml_export_member_id = %s
                """,
                (target_status, error_code, error_message, note, target_status, target_status, actor, xml_export_member_id),
            )
            cur.execute(
                f"""
                UPDATE {qname(health_db())}.xml_export_zips z
                JOIN (
                  SELECT
                    xml_export_zip_id,
                    SUM(CASE WHEN hia_upload_status = 'UPLOAD_ERROR' THEN 1 ELSE 0 END) AS error_count,
                    SUM(CASE WHEN hia_upload_status = 'UPLOADED' THEN 1 ELSE 0 END) AS uploaded_count,
                    COUNT(*) AS total_count
                  FROM {qname(health_db())}.xml_export_members
                  WHERE xml_export_zip_id = (
                    SELECT xml_export_zip_id
                    FROM {qname(health_db())}.xml_export_members
                    WHERE xml_export_member_id = %s
                  )
                  GROUP BY xml_export_zip_id
                ) s
                  ON s.xml_export_zip_id = z.xml_export_zip_id
                   SET z.hia_upload_status = CASE
                         WHEN s.error_count > 0 AND s.uploaded_count > 0 THEN 'PARTIAL'
                         WHEN s.error_count > 0 THEN 'UPLOAD_ERROR'
                         WHEN s.uploaded_count = s.total_count THEN 'UPLOADED'
                         ELSE z.hia_upload_status
                       END,
                       z.hia_upload_checked_at = CURRENT_TIMESTAMP(3),
                       z.hia_upload_checked_by = %s
                """,
                (xml_export_member_id, actor),
            )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            return RedirectResponse(f"/hia/upload-work?error={quote(str(exc))}", status_code=303)
    log_app_operation(
        request=request,
        user=user,
        action_code="HIA_UPLOAD_MEMBER_STATUS_UPDATE",
        target_schema=health_db(),
        target_table="xml_export_members",
        target_id=str(xml_export_member_id),
        after={
            "status": target_status,
            "error_code": error_code,
            "error_message": error_message,
            "note": note,
        },
    )
    return RedirectResponse(f"/hia/upload-work?message={quote('個人XMLのHIAアップロード状態を更新しました。')}", status_code=303)


@app.post("/hia/fund-delivery/members/status", response_class=HTMLResponse)
async def update_hia_fund_delivery_member_status(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, ("hia_upload_status.edit", "users.manage")):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await read_form(request)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            config = build_fund_delivery_submission_config_from_form(
                cur,
                form,
                actor=fund_delivery_actor(user),
            )
            run_id = start_run(
                cur,
                phase="HIA_MARK_FUND_DELIVERY_SUBMITTED",
                source="HIA",
                db_schema=health_db(),
                db_path=None,
                input_base=None,
                input_file=None,
                insurer_number=None,
                dry_run=False,
                limit_rows=None,
            )
            summary = mark_fund_delivery_submitted(cur, config)
            metrics = RunMetrics(
                rows_seen=summary.members_seen,
                rows_updated=summary.members_updated + summary.runs_updated + summary.person_status_updated,
                errors=summary.errors,
            )
            finish_run(
                cur,
                run_id,
                metrics,
                extra_notes=(
                    f"delivery_list_id={summary.delivery_list_id} "
                    f"target_status={config.target_status} list_status={summary.list_status}"
                ),
            )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            return RedirectResponse(f"/hia/fund-delivery?error={quote(str(exc))}", status_code=303)
    return RedirectResponse(
        f"/hia/fund-delivery?message={quote(f'納品状態を更新しました: {config.target_status} {summary.members_seen}件')}",
        status_code=303,
    )


@app.get("/admin/events", response_class=HTMLResponse)
def admin_events(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_view_business_settings(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            events = load_admin_event_rows(cur)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return templates.TemplateResponse(
        "admin_events.html",
        {
            "request": request,
            "user": user,
            "events": events,
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
        },
    )


@app.post("/admin/events", response_class=HTMLResponse)
async def create_admin_event(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_manage_business_settings(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await read_form(request)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            values = normalize_event_form(form)
            cur.execute(
                f"""
                INSERT INTO {qname(dev_db())}.event (
                  insurer_number,
                  event_year,
                  event_type,
                  event_name,
                  age_rule_type,
                  age_reference_date,
                  result_root_path,
                  is_active
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    values["insurer_number"],
                    values["event_year"],
                    values["event_type"],
                    values["event_name"],
                    values["age_rule_type"],
                    values["age_reference_date"],
                    values["result_root_path"],
                    values["is_active"],
                ),
            )
            log_audit(
                cur,
                request=request,
                user=user,
                action_code="ADMIN_EVENT_CREATE",
                target_schema=dev_db(),
                target_table="event",
                target_id=str(cur.lastrowid or ""),
                after=values,
            )
            conn.commit()
        except ValueError as exc:
            conn.rollback()
            return RedirectResponse(f"/admin/events?error={quote(str(exc))}", status_code=303)
        except Exception:
            conn.rollback()
            raise
    return RedirectResponse("/admin/events?message=イベントを作成しました。", status_code=303)


@app.post("/admin/events/{event_id}", response_class=HTMLResponse)
async def update_admin_event(request: Request, event_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_manage_business_settings(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await read_form(request)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            values = normalize_event_form(form)
            cur.execute(
                f"""
                UPDATE {qname(dev_db())}.event
                   SET insurer_number = %s,
                       event_year = %s,
                       event_type = %s,
                       event_name = %s,
                       age_rule_type = %s,
                       age_reference_date = %s,
                       result_root_path = %s,
                       is_active = %s
                 WHERE event_id = %s
                """,
                (
                    values["insurer_number"],
                    values["event_year"],
                    values["event_type"],
                    values["event_name"],
                    values["age_rule_type"],
                    values["age_reference_date"],
                    values["result_root_path"],
                    values["is_active"],
                    event_id,
                ),
            )
            log_audit(
                cur,
                request=request,
                user=user,
                action_code="ADMIN_EVENT_UPDATE",
                target_schema=dev_db(),
                target_table="event",
                target_id=str(event_id),
                after=values,
            )
            conn.commit()
        except ValueError as exc:
            conn.rollback()
            return RedirectResponse(f"/admin/events?error={quote(str(exc))}", status_code=303)
        except Exception:
            conn.rollback()
            raise
    return RedirectResponse("/admin/events?message=イベントを更新しました。", status_code=303)


@app.get("/admin/folder-aliases", response_class=HTMLResponse)
@app.get("/admin/facilities", response_class=HTMLResponse)
def admin_folder_aliases(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_view_business_settings(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            event_options = load_event_options(cur)
            alias_facility_rows = load_alias_facility_admin_rows(cur)
            alias_rows = load_folder_alias_admin_rows(cur)
            csv_format_options = load_csv_format_options(cur)
            alias_count_by_event: dict[str, int] = {}
            for row in alias_rows:
                key = str(row.get("event_id") or "")
                alias_count_by_event[key] = alias_count_by_event.get(key, 0) + 1
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return templates.TemplateResponse(
        "admin_facilities.html",
        {
            "request": request,
            "user": user,
            "event_options": event_options,
            "alias_count_by_event": alias_count_by_event,
            "alias_facility_rows": alias_facility_rows,
            "alias_rows": alias_rows,
            "csv_format_options": csv_format_options,
            "source_mode_options": source_mode_options(),
            "prefecture_options": PREFECTURE_OPTIONS,
            "filters": {
                "q": request.query_params.get("q", ""),
            },
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
        },
    )


@app.get("/admin/facility-master", response_class=HTMLResponse)
def admin_facility_master(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_view_business_settings(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        facility_keyword = request.query_params.get("q", "").strip()
        facility_code = request.query_params.get("code", "").strip()
        facility_prefecture = request.query_params.get("prefecture", "").strip()
        if facility_prefecture and facility_prefecture not in PREFECTURE_OPTIONS:
            facility_prefecture = ""
        try:
            facility_rows = load_facility_master_admin_rows(
                cur,
                limit=2000 if facility_keyword or facility_code or facility_prefecture else 500,
                keyword=facility_keyword,
                code=facility_code,
                prefecture=facility_prefecture,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return templates.TemplateResponse(
        "admin_facility_master.html",
        {
            "request": request,
            "user": user,
            "facility_rows": facility_rows,
            "filters": {"q": facility_keyword, "code": facility_code, "prefecture": facility_prefecture},
            "prefecture_options": PREFECTURE_OPTIONS,
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
        },
    )


@app.get("/admin/csv-mapping-templates", response_class=HTMLResponse)
def admin_csv_mapping_templates(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_view_business_settings(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    keyword = request.query_params.get("q", "").strip()
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            rows = load_csv_mapping_template_rows(cur, keyword=keyword, limit=1000)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return templates.TemplateResponse(
        "admin_csv_mapping_templates.html",
        {
            "request": request,
            "user": user,
            "rows": rows,
            "filters": {"q": keyword},
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
        },
    )


@app.get("/api/admin/csv-mapping-templates/facility-missing-items")
def api_csv_mapping_template_facility_missing_items(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_view_business_settings(user):
        return JSONResponse({"error": "権限がありません。"}, status_code=403)
    exam_facility_id = _optional_int(request.query_params.get("exam_facility_id"))
    csv_format_version_id = _optional_int(request.query_params.get("csv_format_version_id"))
    if not exam_facility_id:
        return JSONResponse(
            {
                "subject_case_count": 0,
                "specific_subject_case_count": 0,
                "legal_subject_case_count": 0,
                "items": [],
            }
        )

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            cur.execute(
                f"""
                SELECT exam_facility_code
                FROM {qname(master_db())}.exam_facilities
                WHERE exam_facility_id = %s
                LIMIT 1
                """,
                (exam_facility_id,),
            )
            facility = cur.fetchone()
            facility_code = str((facility or {}).get("exam_facility_code") or "").strip()
            if not facility_code:
                return JSONResponse(
                    {
                        "subject_case_count": 0,
                        "specific_subject_case_count": 0,
                        "legal_subject_case_count": 0,
                        "items": [],
                    }
                )

            mapped_namecodes: set[str] = set()
            if csv_format_version_id:
                cur.execute(
                    f"""
                    SELECT DISTINCT target_namecode
                    FROM {qname(master_db())}.csv_exam_result_mapping_rules
                    WHERE csv_format_version_id = %s
                      AND target_kind = 'EXAM_ITEM_VALUE'
                      AND is_active = 1
                      AND target_namecode IS NOT NULL
                    """,
                    (csv_format_version_id,),
                )
                mapped_namecodes = {str(row.get("target_namecode") or "") for row in cur.fetchall()}

            article44_status_columns = ",\n                  ".join(
                f"r1.`a44_{detail_no}_status`" for detail_no in ARTICLE44_DETAIL_NAMES
            )
            specific_result_columns = ",\n                  ".join(
                (
                    f"r1.`sp_{detail_code}_status`,\n"
                    f"                  r1.`sp_{detail_code}_reason`"
                )
                for detail_code in SPECIFIC_DETAIL_CODE_BY_NAMECODE.values()
            )
            cur.execute(
                f"""
                SELECT
                  r1.exam_ledger_id,
                  r1.legal_check_result,
                  r1.specific_check_result,
                  el.source_type,
                  {article44_status_columns},
                  {specific_result_columns}
                FROM {qname(health_db())}.exam_check_results AS r1
                INNER JOIN (
                  SELECT exam_ledger_id, MAX(id) AS max_id
                  FROM {qname(health_db())}.exam_check_results
                  WHERE ledger_type = 'EXAM'
                    AND exam_ledger_id IS NOT NULL
                  GROUP BY exam_ledger_id
                ) AS latest ON latest.max_id = r1.id
                INNER JOIN {qname(health_db())}.exam_ledgers AS el
                  ON el.exam_ledger_id = r1.exam_ledger_id
                WHERE el.facility_code = %s
                  AND el.source_type IN ('XML', 'CSV')
                  AND (
                    r1.legal_check_result IN ('OK', 'NG')
                    OR r1.specific_check_result IN ('OK', 'NG')
                  )
                """,
                (facility_code,),
            )
            source_check_rows = [dict(row) for row in cur.fetchall()]
            legal_check_rows = [row for row in source_check_rows if row.get("legal_check_result") in {"OK", "NG"}]
            specific_check_rows = [row for row in source_check_rows if row.get("specific_check_result") in {"OK", "NG"}]
            legal_xml_ledger_count = sum(1 for row in legal_check_rows if row.get("source_type") == "XML")
            legal_csv_ledger_count = sum(1 for row in legal_check_rows if row.get("source_type") == "CSV")
            legal_subject_case_count = legal_xml_ledger_count + legal_csv_ledger_count
            specific_xml_ledger_count = sum(1 for row in specific_check_rows if row.get("source_type") == "XML")
            specific_csv_ledger_count = sum(1 for row in specific_check_rows if row.get("source_type") == "CSV")
            specific_subject_case_count = specific_xml_ledger_count + specific_csv_ledger_count
            missing_rows: list[dict[str, Any]] = []
            specific_missing_reasons = {
                "NOT_FOUND", "NULL", "EMPTY", "CODE_VALUE_MISSING", "TEXT_VALUE_MISSING"
            }
            for namecode, detail_code in SPECIFIC_DETAIL_CODE_BY_NAMECODE.items():
                source_counts: dict[str, dict[str, int]] = {}
                for source_type in ("XML", "CSV"):
                    rows = [row for row in specific_check_rows if row.get("source_type") == source_type]
                    statuses = [str(row.get(f"sp_{detail_code}_status") or "").strip() for row in rows]
                    reasons = [str(row.get(f"sp_{detail_code}_reason") or "").strip() for row in rows]
                    subject_count = sum(
                        1 for status in statuses if status and status not in {RESULT_NOT_APPLICABLE, RESULT_UNDETERMINABLE}
                    )
                    missing_count = sum(
                        1
                        for status, reason in zip(statuses, reasons)
                        if status in {"MISSING", "INVALID"} and reason in specific_missing_reasons
                    )
                    source_counts[source_type] = {"subject": subject_count, "missing": missing_count}
                xml_subject_count = source_counts["XML"]["subject"]
                xml_missing_count = source_counts["XML"]["missing"]
                csv_subject_count = source_counts["CSV"]["subject"]
                csv_missing_count = source_counts["CSV"]["missing"]
                xml_missing_rate = round(xml_missing_count * 100 / xml_subject_count, 1) if xml_subject_count else None
                csv_missing_rate = round(csv_missing_count * 100 / csv_subject_count, 1) if csv_subject_count else None
                if xml_missing_rate == 100 or csv_missing_rate == 100:
                    missing_rows.append(
                        {
                            "check_scope": "SPECIFIC_HEALTH",
                            "namecode": namecode,
                            "item_name": SPECIFIC_ITEM_NAMES.get(namecode, namecode),
                            "missing_case_count": xml_missing_count + csv_missing_count,
                            "subject_case_count": xml_subject_count + csv_subject_count,
                            "xml_subject_count": xml_subject_count,
                            "xml_missing_count": xml_missing_count,
                            "xml_missing_rate": xml_missing_rate,
                            "csv_subject_count": csv_subject_count,
                            "csv_missing_count": csv_missing_count,
                            "csv_missing_rate": csv_missing_rate,
                        }
                    )
            legal_detail_rows: list[dict[str, Any]] = []
            for detail_no, detail_name in ARTICLE44_DETAIL_NAMES.items():
                source_counts: dict[str, dict[str, int]] = {}
                for source_type in ("XML", "CSV"):
                    statuses = [
                        str(row.get(f"a44_{detail_no}_status") or "").strip()
                        for row in legal_check_rows
                        if row.get("source_type") == source_type
                    ]
                    source_counts[source_type] = {
                        "subject": sum(1 for status in statuses if status),
                        "missing": sum(1 for status in statuses if status == "MISSING"),
                    }
                xml_subject_count = source_counts["XML"]["subject"]
                xml_missing_count = source_counts["XML"]["missing"]
                csv_subject_count = source_counts["CSV"]["subject"]
                csv_missing_count = source_counts["CSV"]["missing"]
                xml_missing_rate = round(xml_missing_count * 100 / xml_subject_count, 1) if xml_subject_count else None
                csv_missing_rate = round(csv_missing_count * 100 / csv_subject_count, 1) if csv_subject_count else None
                if xml_missing_rate == 100 or csv_missing_rate == 100:
                    legal_detail_rows.append(
                        {
                            "legal_detail_no": detail_no,
                            "legal_detail_name": f"法定チェック: {detail_name}",
                            "missing_case_count": xml_missing_count + csv_missing_count,
                            "subject_case_count": xml_subject_count + csv_subject_count,
                            "xml_subject_count": xml_subject_count,
                            "xml_missing_count": xml_missing_count,
                            "xml_missing_rate": xml_missing_rate,
                            "csv_subject_count": csv_subject_count,
                            "csv_missing_count": csv_missing_count,
                            "csv_missing_rate": csv_missing_rate,
                        }
                    )
            if legal_detail_rows:
                cur.execute(
                    f"""
                    SELECT
                      gm.namecode,
                      COALESCE(NULLIF(eim.item_name, ''), gm.namecode) AS item_name,
                      gm.notes,
                      gm.priority
                    FROM {qname(dev_db())}.exam_item_group_members AS gm
                    LEFT JOIN {qname(dev_db())}.exam_item_master AS eim
                      ON eim.namecode = gm.namecode
                    WHERE gm.group_code = 'v2_2026_ARTICLE44_CHECK_ITEMS'
                    ORDER BY gm.priority, gm.namecode
                    """
                )
                legal_targets_by_detail: dict[str, list[dict[str, Any]]] = {}
                for target in cur.fetchall():
                    match = re.search(r"Article44\s+(44\d{8})\s*:", str(target.get("notes") or ""))
                    if match:
                        legal_targets_by_detail.setdefault(match.group(1), []).append(dict(target))
                for detail in legal_detail_rows:
                    detail_no = str(detail.get("legal_detail_no") or "")
                    for target in legal_targets_by_detail.get(detail_no, []):
                        missing_rows.append(
                            {
                                "check_scope": "ARTICLE44",
                                "namecode": target.get("namecode"),
                                "item_name": target.get("item_name"),
                                "legal_detail_no": detail_no,
                                "legal_detail_name": detail.get("legal_detail_name"),
                                "missing_case_count": detail.get("missing_case_count"),
                                "subject_case_count": detail.get("subject_case_count"),
                                "xml_subject_count": detail.get("xml_subject_count"),
                                "xml_missing_count": detail.get("xml_missing_count"),
                                "xml_missing_rate": detail.get("xml_missing_rate"),
                                "csv_subject_count": detail.get("csv_subject_count"),
                                "csv_missing_count": detail.get("csv_missing_count"),
                                "csv_missing_rate": detail.get("csv_missing_rate"),
                            }
                        )
            xml_counts: dict[str, int] = {}
            missing_namecodes = [str(row.get("namecode") or "") for row in missing_rows if row.get("namecode")]
            if missing_namecodes:
                placeholders = ", ".join(["%s"] * len(missing_namecodes))
                cur.execute(
                    f"""
                    SELECT
                      eiv.namecode,
                      COUNT(DISTINCT el.exam_ledger_id) AS xml_ledger_count
                    FROM {qname(health_db())}.exam_item_values AS eiv
                    INNER JOIN {qname(health_db())}.exam_ledgers AS el
                      ON el.exam_ledger_id = eiv.ledger_id
                     AND eiv.ledger_type = 'EXAM'
                    WHERE el.facility_code = %s
                      AND el.source_type = 'XML'
                      AND eiv.namecode IN ({placeholders})
                    GROUP BY eiv.namecode
                    """,
                    (facility_code, *missing_namecodes),
                )
                xml_counts = {
                    str(row.get("namecode") or ""): int(row.get("xml_ledger_count") or 0)
                    for row in cur.fetchall()
                }

            items = []
            for row in missing_rows:
                missing_case_count = int(row.get("missing_case_count") or 0)
                check_scope = str(row.get("check_scope") or "")
                subject_case_count = int(row.get("subject_case_count") or 0)
                missing_rate = round(missing_case_count * 100 / subject_case_count, 1) if subject_case_count else None
                namecode = str(row.get("namecode") or "")
                items.append(
                    {
                        "namecode": namecode,
                        "item_name": row.get("item_name") or namecode,
                        "check_scope": check_scope,
                        "check_scope_label": "法定" if check_scope == "ARTICLE44" else "特定健診",
                        "legal_detail_no": row.get("legal_detail_no"),
                        "legal_detail_name": row.get("legal_detail_name"),
                        "missing_case_count": missing_case_count,
                        "missing_rate": missing_rate,
                        "xml_subject_count": row.get("xml_subject_count"),
                        "xml_missing_count": row.get("xml_missing_count"),
                        "xml_missing_rate": row.get("xml_missing_rate"),
                        "csv_subject_count": row.get("csv_subject_count"),
                        "csv_missing_count": row.get("csv_missing_count"),
                        "csv_missing_rate": row.get("csv_missing_rate"),
                        "xml_ledger_count": xml_counts.get(namecode, 0),
                        "is_mapped": namecode in mapped_namecodes,
                    }
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return JSONResponse(
        {
            "facility_code": facility_code,
            "subject_case_count": specific_subject_case_count,
            "specific_subject_case_count": specific_subject_case_count,
            "legal_subject_case_count": legal_subject_case_count,
            "legal_xml_ledger_count": legal_xml_ledger_count,
            "legal_csv_ledger_count": legal_csv_ledger_count,
            "specific_xml_ledger_count": specific_xml_ledger_count,
            "specific_csv_ledger_count": specific_csv_ledger_count,
            "items": items,
        }
    )


@app.get("/admin/csv-mapping-templates/new", response_class=HTMLResponse)
def admin_csv_mapping_template_new(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_view_business_settings(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    return templates.TemplateResponse(
        "admin_csv_mapping_template_edit.html",
        {
            "request": request,
            "user": user,
            "mode": "new",
            "template": None,
            "prefecture_options": PREFECTURE_OPTIONS,
            "rules": [],
            "target_groups": [],
            "header_columns": [],
            "header_preview": {"header_rows": [], "columns": []},
            "ledger_field_options": CSV_MAPPING_LAB_LEDGER_FIELD_OPTIONS,
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
        },
    )


@app.post("/admin/csv-mapping-templates/new", response_class=HTMLResponse)
async def create_admin_csv_mapping_template(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_manage_business_settings(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await request.form()
    exam_facility_id = _optional_int(form.get("exam_facility_id"))
    mapping_version = str(form.get("mapping_version") or "").strip()
    format_name = str(form.get("format_name") or "").strip() or None
    is_active = 1 if form.get("is_active") == "1" else 0
    data_start_row_no = _optional_int(form.get("data_start_row_no")) or 2
    csv_file = form.get("csv_file")
    csv_file_name = str(getattr(csv_file, "filename", "") or "")
    active_header_row_no = _optional_int(form.get("active_header_row_no"))
    header_structure_type = str(form.get("header_structure_type") or "SIMPLE_HEADER").strip()
    if header_structure_type not in {"SIMPLE_HEADER", "CONTEXT_HEADER"}:
        header_structure_type = "SIMPLE_HEADER"
    header_mode = "MULTI_ROW" if header_structure_type == "CONTEXT_HEADER" else "SINGLE"
    header_context_rule = "PREVIOUS_ROWS_AS_CONTEXT" if header_structure_type == "CONTEXT_HEADER" else None
    if not exam_facility_id:
        return RedirectResponse("/admin/csv-mapping-templates/new?error=健診機関を選択してください。", status_code=303)
    if not mapping_version:
        return RedirectResponse("/admin/csv-mapping-templates/new?error=mapping_versionを入力してください。", status_code=303)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            cur.execute(
                f"""
                INSERT INTO {qname(master_db())}.`csv_format_versions` (
                  `exam_facility_id`,
                  `mapping_version`,
                  `file_type`,
                  `format_name`,
                  `has_header`,
                  `header_mode`,
                  `header_structure_type`,
                  `header_context_rule`,
                  `active_header_row_no`,
                  `data_start_row_no`,
                  `header_hash_status`,
                  `header_mismatch_policy`,
                  `allow_column_no_rules`,
                  `character_encoding`,
                  `delimiter`,
                  `note`,
                  `is_active`
                )
                VALUES (%s, %s, 'CSV', %s, 1, %s, %s, %s, %s, %s, 'UNVERIFIED', 'ALLOW_AFTER_CONFIRM', 0, 'CP932', ',', %s, %s)
                """,
                (
                    exam_facility_id,
                    mapping_version,
                    format_name,
                    header_mode,
                    header_structure_type,
                    header_context_rule,
                    active_header_row_no,
                    data_start_row_no,
                    "created from admin csv mapping builder",
                    is_active,
                ),
            )
            csv_format_version_id = int(cur.lastrowid)
            template_for_header_import = {
                "data_start_row_no": data_start_row_no,
                "delimiter": ",",
                "character_encoding": "CP932",
                "quote_char": '"',
                "active_header_row_no": active_header_row_no,
            }
            header_import_result = None
            if csv_file_name:
                header_import_result = await import_csv_mapping_template_header_upload(
                    cur,
                    request=request,
                    user=user,
                    csv_format_version_id=csv_format_version_id,
                    template=template_for_header_import,
                    csv_file=csv_file,
                )
            log_audit(
                cur,
                request=request,
                user=user,
                action_code="CSV_MAPPING_TEMPLATE_CREATE",
                target_schema=master_db(),
                target_table="csv_format_versions",
                target_id=str(csv_format_version_id),
                after={
                    "exam_facility_id": exam_facility_id,
                    "mapping_version": mapping_version,
                    "format_name": format_name,
                    "header_mode": header_mode,
                    "header_structure_type": header_structure_type,
                    "header_context_rule": header_context_rule,
                    "active_header_row_no": active_header_row_no,
                    "data_start_row_no": data_start_row_no,
                    "header_import": header_import_result,
                },
            )
            conn.commit()
        except IntegrityError as exc:
            conn.rollback()
            return RedirectResponse(
                f"/admin/csv-mapping-templates/new?error={quote('同じ健診機関とmapping_versionのテンプレートが既にあります。')}",
                status_code=303,
            )
        except Exception:
            conn.rollback()
            raise
    message = "CSVマッピングテンプレートを作成しました。"
    if csv_file_name:
        message = "CSVマッピングテンプレートを作成し、基準ヘッダーを読み込みました。"
    return RedirectResponse(
        f"/admin/csv-mapping-templates/{csv_format_version_id}/edit?message={quote(message)}",
        status_code=303,
    )


@app.get("/admin/csv-mapping-templates/{csv_format_version_id}", response_class=HTMLResponse)
def admin_csv_mapping_template_detail(request: Request, csv_format_version_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_view_business_settings(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            template = load_csv_mapping_template_detail(cur, csv_format_version_id=csv_format_version_id)
            if template:
                summaries = load_csv_mapping_template_rule_summary(cur, csv_format_version_id=csv_format_version_id)
                rules = load_csv_mapping_template_rules(cur, csv_format_version_id=csv_format_version_id)
                conditions = load_csv_mapping_template_conditions(cur, csv_format_version_id=csv_format_version_id)
                enrich_csv_mapping_template_rules_with_conditions(rules, conditions)
                target_groups = build_csv_mapping_template_target_groups(rules)
                header_columns = attach_csv_header_neighbors(
                    load_csv_mapping_template_header_columns(cur, template=template, conditions=conditions)
                )
                alias_rows = load_csv_mapping_template_alias_rows(
                    cur,
                    csv_format_version_id=csv_format_version_id,
                    exam_facility_id=_optional_int(template.get("exam_facility_id")),
                )
            else:
                summaries = []
                rules = []
                conditions = []
                target_groups = []
                header_columns = []
                alias_rows = []
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    if not template:
        return templates.TemplateResponse(
            "admin_csv_mapping_template_detail.html",
            {
                "request": request,
                "user": user,
                "template": None,
                "summaries": [],
                "rules": [],
                "conditions": [],
                "message": None,
                "error": f"csv_format_version_id={csv_format_version_id} のテンプレートが見つかりません。",
            },
            status_code=404,
        )
    return templates.TemplateResponse(
        "admin_csv_mapping_template_detail.html",
        {
            "request": request,
            "user": user,
            "template": template,
            "summaries": summaries,
            "rules": rules,
            "conditions": conditions,
            "target_groups": target_groups,
            "header_columns": header_columns,
            "header_preview": build_csv_mapping_template_header_preview_payload(template, header_columns),
            "alias_rows": alias_rows,
            "ledger_field_options": CSV_MAPPING_LAB_LEDGER_FIELD_OPTIONS,
            "mapped_header_count": sum(1 for column in header_columns if column.get("is_mapped")),
            "unmapped_header_count": sum(1 for column in header_columns if not column.get("is_mapped")),
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
        },
    )


@app.get("/admin/csv-mapping-templates/{csv_format_version_id}/edit", response_class=HTMLResponse)
def admin_csv_mapping_template_edit(request: Request, csv_format_version_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_view_business_settings(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            template = load_csv_mapping_template_detail(cur, csv_format_version_id=csv_format_version_id)
            rules = load_csv_mapping_template_rules(cur, csv_format_version_id=csv_format_version_id) if template else []
            conditions = (
                load_csv_mapping_template_conditions(cur, csv_format_version_id=csv_format_version_id)
                if template
                else []
            )
            enrich_csv_mapping_template_rules_with_conditions(rules, conditions)
            target_groups = build_csv_mapping_template_target_groups(rules)
            header_columns = (
                attach_csv_header_neighbors(
                    load_csv_mapping_template_header_columns(cur, template=template, conditions=conditions)
                )
                if template
                else []
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    if not template:
        return RedirectResponse(
            f"/admin/csv-mapping-templates?error={quote(f'csv_format_version_id={csv_format_version_id} のテンプレートが見つかりません。')}",
            status_code=303,
        )
    return templates.TemplateResponse(
        "admin_csv_mapping_template_edit.html",
        {
            "request": request,
            "user": user,
            "mode": "edit",
            "template": template,
            "prefecture_options": PREFECTURE_OPTIONS,
            "rules": rules,
            "target_groups": target_groups,
            "header_columns": header_columns,
            "header_preview": build_csv_mapping_template_header_preview_payload(template, header_columns),
            "ledger_field_options": CSV_MAPPING_LAB_LEDGER_FIELD_OPTIONS,
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
        },
    )


@app.post("/admin/csv-mapping-templates/{csv_format_version_id}/edit", response_class=HTMLResponse)
async def update_admin_csv_mapping_template(request: Request, csv_format_version_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_manage_business_settings(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await request.form()
    exam_facility_id = _optional_int(form.get("exam_facility_id"))
    mapping_version = str(form.get("mapping_version") or "").strip()
    format_name = str(form.get("format_name") or "").strip() or None
    is_active = 1 if form.get("is_active") == "1" else 0
    data_start_row_no = _optional_int(form.get("data_start_row_no")) or 2
    csv_file = form.get("csv_file")
    csv_file_name = str(getattr(csv_file, "filename", "") or "")
    active_header_row_no = _optional_int(form.get("active_header_row_no"))
    header_structure_type = str(form.get("header_structure_type") or "SIMPLE_HEADER").strip()
    if header_structure_type not in {"SIMPLE_HEADER", "CONTEXT_HEADER"}:
        header_structure_type = "SIMPLE_HEADER"
    header_mode = "MULTI_ROW" if header_structure_type == "CONTEXT_HEADER" else "SINGLE"
    header_context_rule = "PREVIOUS_ROWS_AS_CONTEXT" if header_structure_type == "CONTEXT_HEADER" else None
    if not exam_facility_id:
        return RedirectResponse(
            f"/admin/csv-mapping-templates/{csv_format_version_id}/edit?error=健診機関を選択してください。",
            status_code=303,
        )
    if not mapping_version:
        return RedirectResponse(
            f"/admin/csv-mapping-templates/{csv_format_version_id}/edit?error=mapping_versionを入力してください。",
            status_code=303,
        )
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            cur.execute(
                f"""
                UPDATE {qname(master_db())}.`csv_format_versions`
                   SET `exam_facility_id` = %s,
                       `mapping_version` = %s,
                       `format_name` = %s,
                       `header_mode` = %s,
                       `header_structure_type` = %s,
                       `header_context_rule` = %s,
                       `active_header_row_no` = %s,
                       `data_start_row_no` = %s,
                       `is_active` = %s
                 WHERE `csv_format_version_id` = %s
                """,
                (
                    exam_facility_id,
                    mapping_version,
                    format_name,
                    header_mode,
                    header_structure_type,
                    header_context_rule,
                    active_header_row_no,
                    data_start_row_no,
                    is_active,
                    csv_format_version_id,
                ),
            )
            if cur.rowcount == 0:
                conn.rollback()
                return RedirectResponse(
                    f"/admin/csv-mapping-templates?error={quote(f'csv_format_version_id={csv_format_version_id} のテンプレートが見つかりません。')}",
                    status_code=303,
                )
            header_import_result = None
            if csv_file_name:
                template_for_header_import = {
                    "data_start_row_no": data_start_row_no,
                    "delimiter": ",",
                    "character_encoding": "CP932",
                    "quote_char": '"',
                    "active_header_row_no": active_header_row_no,
                }
                header_import_result = await import_csv_mapping_template_header_upload(
                    cur,
                    request=request,
                    user=user,
                    csv_format_version_id=csv_format_version_id,
                    template=template_for_header_import,
                    csv_file=csv_file,
                )
            log_audit(
                cur,
                request=request,
                user=user,
                action_code="CSV_MAPPING_TEMPLATE_UPDATE",
                target_schema=master_db(),
                target_table="csv_format_versions",
                target_id=str(csv_format_version_id),
                after={
                    "exam_facility_id": exam_facility_id,
                    "mapping_version": mapping_version,
                    "format_name": format_name,
                    "header_mode": header_mode,
                    "header_structure_type": header_structure_type,
                    "header_context_rule": header_context_rule,
                    "active_header_row_no": active_header_row_no,
                    "data_start_row_no": data_start_row_no,
                    "header_import": header_import_result,
                },
            )
            conn.commit()
        except IntegrityError:
            conn.rollback()
            return RedirectResponse(
                f"/admin/csv-mapping-templates/{csv_format_version_id}/edit?error={quote('同じ健診機関とmapping_versionのテンプレートが既にあります。')}",
                status_code=303,
            )
        except Exception:
            conn.rollback()
            raise
    message = "基本情報を保存しました。"
    if csv_file_name:
        message = "基本情報を保存し、基準ヘッダーを読み込みました。"
    return RedirectResponse(
        f"/admin/csv-mapping-templates/{csv_format_version_id}/edit?message={quote(message)}",
        status_code=303,
    )


@app.post("/admin/csv-mapping-templates/{csv_format_version_id}/toggle-active", response_class=HTMLResponse)
async def toggle_admin_csv_mapping_template_active(request: Request, csv_format_version_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_manage_business_settings(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await request.form()
    next_active = 1 if form.get("is_active") == "1" else 0
    return_to = str(form.get("return_to") or "").strip()
    if return_to not in {"list", "detail", "edit"}:
        return_to = "detail"
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            template = load_csv_mapping_template_detail(cur, csv_format_version_id=csv_format_version_id)
            if not template:
                conn.rollback()
                return RedirectResponse(
                    f"/admin/csv-mapping-templates?error={quote(f'csv_format_version_id={csv_format_version_id} のテンプレートが見つかりません。')}",
                    status_code=303,
                )
            before = {"is_active": int(template.get("is_active") or 0)}
            cur.execute(
                f"""
                UPDATE {qname(master_db())}.`csv_format_versions`
                   SET `is_active` = %s,
                       `updated_at` = CURRENT_TIMESTAMP(3)
                 WHERE `csv_format_version_id` = %s
                """,
                (next_active, csv_format_version_id),
            )
            log_audit(
                cur,
                request=request,
                user=user,
                action_code="CSV_MAPPING_TEMPLATE_ACTIVE_TOGGLE",
                target_schema=master_db(),
                target_table="csv_format_versions",
                target_id=str(csv_format_version_id),
                before=before,
                after={"is_active": next_active},
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    message = "テンプレートを有効にしました。" if next_active else "テンプレートを無効にしました。"
    if return_to == "list":
        url = "/admin/csv-mapping-templates"
    elif return_to == "edit":
        url = f"/admin/csv-mapping-templates/{csv_format_version_id}/edit"
    else:
        url = f"/admin/csv-mapping-templates/{csv_format_version_id}"
    return RedirectResponse(f"{url}?message={quote(message)}", status_code=303)


@app.post("/admin/csv-mapping-templates/{csv_format_version_id}/aliases", response_class=HTMLResponse)
async def assign_admin_csv_mapping_template_alias(request: Request, csv_format_version_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_manage_business_settings(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await request.form()
    alias_id = _optional_int(form.get("alias_id"))
    if not alias_id:
        return RedirectResponse(
            f"/admin/csv-mapping-templates/{csv_format_version_id}?error={quote('設定する受領フォルダaliasを選択してください。')}",
            status_code=303,
        )
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            template = load_csv_mapping_template_detail(cur, csv_format_version_id=csv_format_version_id)
            if not template:
                conn.rollback()
                return RedirectResponse(
                    f"/admin/csv-mapping-templates?error={quote(f'csv_format_version_id={csv_format_version_id} のテンプレートが見つかりません。')}",
                    status_code=303,
                )
            cur.execute(
                f"""
                SELECT `alias_id`, `exam_facility_id`, `csv_format_version_id`, `src_folder_raw`
                  FROM {qname(master_db())}.`medical_folder_aliases`
                 WHERE `alias_id` = %s
                """,
                (alias_id,),
            )
            alias = cur.fetchone()
            if not alias:
                conn.rollback()
                return RedirectResponse(
                    f"/admin/csv-mapping-templates/{csv_format_version_id}?error={quote('受領フォルダaliasが見つかりません。')}",
                    status_code=303,
                )
            if int(alias.get("exam_facility_id") or 0) != int(template.get("exam_facility_id") or 0):
                conn.rollback()
                return RedirectResponse(
                    f"/admin/csv-mapping-templates/{csv_format_version_id}?error={quote('テンプレートとaliasの健診機関が一致しません。')}",
                    status_code=303,
                )
            before = {
                "alias_id": alias_id,
                "csv_format_version_id": alias.get("csv_format_version_id"),
                "src_folder_raw": alias.get("src_folder_raw"),
            }
            cur.execute(
                f"""
                UPDATE {qname(master_db())}.`medical_folder_aliases`
                   SET `csv_format_version_id` = %s,
                       `updated_at` = CURRENT_TIMESTAMP(3)
                 WHERE `alias_id` = %s
                """,
                (csv_format_version_id, alias_id),
            )
            log_audit(
                cur,
                request=request,
                user=user,
                action_code="CSV_MAPPING_TEMPLATE_ALIAS_ASSIGN",
                target_schema=master_db(),
                target_table="medical_folder_aliases",
                target_id=str(alias_id),
                before=before,
                after={"csv_format_version_id": csv_format_version_id},
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return RedirectResponse(
        f"/admin/csv-mapping-templates/{csv_format_version_id}?message={quote('受領フォルダaliasにテンプレートを設定しました。')}",
        status_code=303,
    )


@app.post("/api/admin/csv-mapping-templates/{csv_format_version_id}/screen-rules", response_class=JSONResponse)
async def api_save_csv_mapping_template_screen_rules(request: Request, csv_format_version_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse({"message": "ログインしてください。"}, status_code=401)
    if not can_manage_business_settings(user):
        return JSONResponse({"message": "権限がありません。"}, status_code=403)
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"message": "保存内容を読み取れません。"}, status_code=400)
    items = payload.get("items") if isinstance(payload, Mapping) else None
    if not isinstance(items, list) or not items:
        return JSONResponse({"message": "保存するマッピングがありません。"}, status_code=400)

    params = load_mysql_base_params(db_prefix())
    saved_count = 0
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            template = load_csv_mapping_template_detail(cur, csv_format_version_id=csv_format_version_id)
            if not template:
                conn.rollback()
                return JSONResponse({"message": "対象テンプレートがありません。"}, status_code=404)
            cur.execute(
                f"""
                SELECT COALESCE(MAX(`priority`), 0) AS `max_priority`
                FROM {qname(master_db())}.`csv_exam_result_mapping_rules`
                WHERE `csv_format_version_id` = %s
                """,
                (csv_format_version_id,),
            )
            max_priority = int((cur.fetchone() or {}).get("max_priority") or 0)
            has_rule_origin_type = manual_exam_entry_column_exists(
                cur, master_db(), "csv_exam_result_mapping_rules", "rule_origin_type"
            )
            has_edit_capability = manual_exam_entry_column_exists(
                cur, master_db(), "csv_exam_result_mapping_rules", "edit_capability"
            )
            for index, item in enumerate(items, start=1):
                if not isinstance(item, Mapping):
                    continue
                rule_id = _optional_int(item.get("ruleId"))
                target_kind = str(item.get("targetKind") or "").strip()
                if target_kind not in {"LEDGER_FIELD", "EXAM_ITEM_VALUE"}:
                    continue
                mode = "many" if str(item.get("mode") or "") == "many" else "one"
                headers = item.get("headers")
                if not isinstance(headers, list) or not headers:
                    continue
                if mode == "one":
                    headers = headers[:1]
                condition_rows = []
                for header_index, header in enumerate(headers, start=1):
                    if not isinstance(header, Mapping):
                        continue
                    header_name = str(header.get("headerName") or "").strip()
                    column_no = _optional_int(header.get("columnNo"))
                    header_context = str(header.get("headerContext") or "").strip() or None
                    if not header_name and not column_no:
                        continue
                    condition_rows.append(
                        (
                            1,
                            "SOURCE_COLUMN",
                            "HEADER_CONTEXT_AND_NAME" if header_context else "HEADER_NAME",
                            header_context,
                            header_name or None,
                            1,
                            column_no,
                            None,
                            None,
                            None,
                            "VALUE",
                            header_index * 100,
                            1,
                        )
                    )
                if not condition_rows:
                    continue
                target_code = str(item.get("targetCode") or "").strip()
                if not target_code:
                    continue
                if target_kind == "EXAM_ITEM_VALUE" and not re.fullmatch(r"[0-9A-Z]{17}", target_code):
                    continue
                target_part = csv_mapping_screen_rule_key_part(target_code)
                rule_key = f"screen.{csv_format_version_id}.{target_kind.lower()}.{target_part}.{datetime.now().strftime('%Y%m%d%H%M%S%f')}.{index}"
                method_structure_type = "MULTI_COLUMN_JOIN" if mode == "many" else "SINGLE_COLUMN"
                target_resolution_type = "LEDGER_FIELD" if target_kind == "LEDGER_FIELD" else "SINGLE_NAMECODE"
                priority = max_priority + (index * 10)
                if rule_id:
                    cur.execute(
                        f"""
                        SELECT `csv_exam_result_mapping_rule_id`, `csv_format_version_id`,
                               `target_kind`, `selection_mode`, `method_structure_type`,
                               `value_source_type`, `fixed_value`,
                               {('`edit_capability`' if has_edit_capability else "'VIEW_ONLY'")} AS `edit_capability`
                        FROM {qname(master_db())}.`csv_exam_result_mapping_rules`
                        WHERE `csv_exam_result_mapping_rule_id` = %s
                          AND `csv_format_version_id` = %s
                        """,
                        (rule_id, csv_format_version_id),
                    )
                    existing_rule = cur.fetchone()
                    if not existing_rule:
                        continue
                    existing_is_editable = (
                        str(existing_rule.get("edit_capability") or "").upper() == "BASIC_SIMPLE"
                        or is_csv_mapping_screen_simple_rule(existing_rule)
                    )
                    if not existing_is_editable:
                        continue
                    update_columns = [
                        "`target_kind` = %s",
                        "`target_resolution_type` = %s",
                        "`selection_mode` = %s",
                        "`selection_group_code` = %s",
                        "`target_namecode` = %s",
                        "`target_identity_item_code` = %s",
                        "`target_field` = %s",
                        "`method_structure_type` = %s",
                        "`value_source_type` = %s",
                        "`fixed_value` = %s",
                        "`value_join_separator` = %s",
                        "`raw_value_type` = %s",
                        "`raw_unit` = %s",
                        "`note` = %s",
                    ]
                    update_values: list[Any] = [
                        target_kind,
                        target_resolution_type,
                        "DIRECT",
                        None,
                        target_code if target_kind == "EXAM_ITEM_VALUE" else None,
                        None,
                        target_code if target_kind == "LEDGER_FIELD" else None,
                        method_structure_type,
                        "SOURCE",
                        None,
                        "" if mode == "many" else None,
                        None,
                        None,
                        f"screen: {item.get('targetName') or target_code}",
                    ]
                    if has_rule_origin_type:
                        update_columns.append("`rule_origin_type` = %s")
                        update_values.append("SCREEN")
                    if has_edit_capability:
                        update_columns.append("`edit_capability` = %s")
                        update_values.append("BASIC_SIMPLE")
                    cur.execute(
                        f"""
                        UPDATE {qname(master_db())}.`csv_exam_result_mapping_rules`
                        SET {", ".join(update_columns)}
                        WHERE `csv_exam_result_mapping_rule_id` = %s
                          AND `csv_format_version_id` = %s
                        """,
                        (*update_values, rule_id, csv_format_version_id),
                    )
                    cur.execute(
                        f"""
                        DELETE FROM {qname(master_db())}.`csv_exam_result_mapping_conditions`
                        WHERE `csv_exam_result_mapping_rule_id` = %s
                        """,
                        (rule_id,),
                    )
                else:
                    rule_id = None
                columns = [
                    "`csv_format_version_id`",
                    "`rule_key`",
                    "`target_kind`",
                    "`target_resolution_type`",
                    "`selection_mode`",
                    "`selection_group_code`",
                    "`target_namecode`",
                    "`target_identity_item_code`",
                    "`target_field`",
                    "`method_structure_type`",
                    "`value_source_type`",
                    "`fixed_value`",
                    "`value_join_separator`",
                    "`raw_value_type`",
                    "`raw_unit`",
                    "`is_required`",
                    "`priority`",
                    "`is_active`",
                    "`note`",
                ]
                values: list[Any] = [
                    csv_format_version_id,
                    rule_key,
                    target_kind,
                    target_resolution_type,
                    "DIRECT",
                    None,
                    target_code if target_kind == "EXAM_ITEM_VALUE" else None,
                    None,
                    target_code if target_kind == "LEDGER_FIELD" else None,
                    method_structure_type,
                    "SOURCE",
                    None,
                    "" if mode == "many" else None,
                    None,
                    None,
                    0,
                    priority,
                    1,
                    f"screen: {item.get('targetName') or target_code}",
                ]
                if has_rule_origin_type:
                    columns.append("`rule_origin_type`")
                    values.append("SCREEN")
                if has_edit_capability:
                    columns.append("`edit_capability`")
                    values.append("BASIC_SIMPLE")
                if not rule_id:
                    cur.execute(
                        f"""
                        INSERT INTO {qname(master_db())}.`csv_exam_result_mapping_rules` (
                          {", ".join(columns)}
                        )
                        VALUES ({", ".join(["%s"] * len(values))})
                        """,
                        tuple(values),
                    )
                    rule_id = int(cur.lastrowid)
                cur.executemany(
                    f"""
                    INSERT INTO {qname(master_db())}.`csv_exam_result_mapping_conditions` (
                      `csv_exam_result_mapping_rule_id`, `condition_group_no`, `condition_type`,
                      `locator_type`, `header_context`, `header_name`, `header_occurrence`, `column_no`,
                      `operator`, `expected_value`, `expected_value_normalized`, `source_role`,
                      `priority`, `is_active`, `note`
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    [(rule_id, *condition_row, f"screen:{rule_key}") for condition_row in condition_rows],
                )
                saved_count += 1
            if saved_count == 0:
                conn.rollback()
                return JSONResponse({"message": "保存できるマッピングがありません。"}, status_code=400)
            log_audit(
                cur,
                request=request,
                user=user,
                action_code="CSV_MAPPING_TEMPLATE_SCREEN_RULES_SAVE",
                target_schema=master_db(),
                target_table="csv_exam_result_mapping_rules",
                target_id=str(csv_format_version_id),
                after={"saved_count": saved_count},
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return JSONResponse({"message": f"{saved_count}件のマッピングを保存しました。", "saved_count": saved_count})


@app.delete(
    "/api/admin/csv-mapping-templates/{csv_format_version_id}/screen-rules/{rule_id}",
    response_class=JSONResponse,
)
async def api_delete_csv_mapping_template_screen_rule(
    request: Request, csv_format_version_id: int, rule_id: int
) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse({"message": "ログインしてください。"}, status_code=401)
    if not can_manage_business_settings(user):
        return JSONResponse({"message": "権限がありません。"}, status_code=403)

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            template = load_csv_mapping_template_detail(cur, csv_format_version_id=csv_format_version_id)
            if not template:
                conn.rollback()
                return JSONResponse({"message": "対象テンプレートがありません。"}, status_code=404)
            has_edit_capability = manual_exam_entry_column_exists(
                cur, master_db(), "csv_exam_result_mapping_rules", "edit_capability"
            )
            cur.execute(
                f"""
                SELECT `csv_exam_result_mapping_rule_id`, `csv_format_version_id`,
                       `target_kind`, `selection_mode`, `method_structure_type`,
                       `value_source_type`, `fixed_value`,
                       {('`edit_capability`' if has_edit_capability else "'VIEW_ONLY'")} AS `edit_capability`
                FROM {qname(master_db())}.`csv_exam_result_mapping_rules`
                WHERE `csv_exam_result_mapping_rule_id` = %s
                  AND `csv_format_version_id` = %s
                """,
                (rule_id, csv_format_version_id),
            )
            rule = cur.fetchone()
            if not rule:
                conn.rollback()
                return JSONResponse({"message": "対象ルールがありません。"}, status_code=404)
            if str(rule.get("edit_capability") or "").upper() != "BASIC_SIMPLE" and not is_csv_mapping_screen_simple_rule(rule):
                conn.rollback()
                return JSONResponse({"message": "このルールは画面から削除できません。"}, status_code=400)
            cur.execute(
                f"""
                DELETE FROM {qname(master_db())}.`csv_exam_result_mapping_conditions`
                WHERE `csv_exam_result_mapping_rule_id` = %s
                """,
                (rule_id,),
            )
            cur.execute(
                f"""
                DELETE FROM {qname(master_db())}.`csv_exam_result_mapping_rules`
                WHERE `csv_exam_result_mapping_rule_id` = %s
                  AND `csv_format_version_id` = %s
                """,
                (rule_id, csv_format_version_id),
            )
            log_audit(
                cur,
                request=request,
                user=user,
                action_code="CSV_MAPPING_TEMPLATE_SCREEN_RULE_DELETE",
                target_schema=master_db(),
                target_table="csv_exam_result_mapping_rules",
                target_id=str(rule_id),
                before=dict(rule),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return JSONResponse({"message": "マッピングルールを削除しました。", "deleted_rule_id": rule_id})


@app.get("/admin/csv-mapping-templates/{csv_format_version_id}/headers", response_class=HTMLResponse)
def admin_csv_mapping_template_headers(request: Request, csv_format_version_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_view_business_settings(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            template = load_csv_mapping_template_detail(cur, csv_format_version_id=csv_format_version_id)
            if template:
                conditions = load_csv_mapping_template_conditions(cur, csv_format_version_id=csv_format_version_id)
                header_columns = attach_csv_header_neighbors(
                    load_csv_mapping_template_header_columns(cur, template=template, conditions=conditions)
                )
            else:
                header_columns = []
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    if not template:
        return templates.TemplateResponse(
            "admin_csv_mapping_template_headers.html",
            {
                "request": request,
                "user": user,
                "template": None,
                "header_columns": [],
                "compare_result": None,
                "message": None,
                "error": f"csv_format_version_id={csv_format_version_id} のテンプレートが見つかりません。",
            },
            status_code=404,
        )
    return templates.TemplateResponse(
        "admin_csv_mapping_template_headers.html",
        {
            "request": request,
            "user": user,
            "template": template,
            "header_columns": header_columns,
            "compare_result": None,
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
        },
    )


@app.post("/admin/csv-mapping-templates/{csv_format_version_id}/headers/compare", response_class=HTMLResponse)
async def admin_csv_mapping_template_headers_compare(
    request: Request,
    csv_format_version_id: int,
    csv_file: UploadFile = File(...),
) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_view_business_settings(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    tmp_path: str | None = None
    try:
        uploaded_bytes = await csv_file.read()
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            tmp.write(uploaded_bytes)
            tmp_path = tmp.name

        params = load_mysql_base_params(db_prefix())
        with connect_ctx(params, database=health_db(), autocommit=False) as conn:
            cur = dict_cursor(conn)
            try:
                template = load_csv_mapping_template_detail(cur, csv_format_version_id=csv_format_version_id)
                if template is None:
                    conn.commit()
                    return RedirectResponse(
                        f"/admin/csv-mapping-templates/{csv_format_version_id}/headers?error=テンプレートが見つかりません。",
                        status_code=303,
                    )
                conditions = load_csv_mapping_template_conditions(cur, csv_format_version_id=csv_format_version_id)
                header_columns = attach_csv_header_neighbors(
                    load_csv_mapping_template_header_columns(cur, template=template, conditions=conditions)
                )
                header_count = max(int(template.get("data_start_row_no") or 2) - 1, 0)
                csv_result = load_csv_result(
                    tmp_path,
                    header_count=header_count,
                    delimiter=str(template.get("delimiter") or ","),
                    encoding=str(template.get("character_encoding") or "") or None,
                    quote_char=str(template.get("quote_char") or '"'),
                    active_header_row_no=(
                        int(template["active_header_row_no"])
                        if template.get("active_header_row_no") is not None
                        else None
                    ),
                    data_start_row_no=int(template.get("data_start_row_no") or header_count + 1),
                )
                uploaded_columns = attach_csv_header_neighbors(
                    [
                        {
                            "column_no": column.column_no,
                            "context": column.context or "",
                            "name": column.header_name or "",
                            "normalized_header_name": normalize_header(column.header_name) or "",
                            "occurrence": column.occurrence,
                        }
                        for column in csv_result.header_set.normalized_columns
                    ]
                )
                compare_result = build_csv_mapping_header_compare(uploaded_columns, header_columns)
                compare_result["file_name"] = csv_file.filename or "-"
                compare_result["encoding"] = csv_result.encoding
                compare_result["header_sha256"] = csv_result.header_set.header_sha256
                compare_result["template_header_sha256"] = template.get("header_sha256")
                conn.commit()
            except Exception:
                conn.rollback()
                raise
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    return templates.TemplateResponse(
        "admin_csv_mapping_template_headers.html",
        {
            "request": request,
            "user": user,
            "template": template,
            "header_columns": header_columns,
            "compare_result": compare_result,
            "message": None,
            "error": None,
        },
    )


@app.post("/admin/csv-mapping-templates/{csv_format_version_id}/headers/import", response_class=HTMLResponse)
async def admin_csv_mapping_template_headers_import(
    request: Request,
    csv_format_version_id: int,
    csv_file: UploadFile = File(...),
) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_view_business_settings(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            template = load_csv_mapping_template_detail(cur, csv_format_version_id=csv_format_version_id)
            if template is None:
                conn.commit()
                return RedirectResponse(
                    f"/admin/csv-mapping-templates?error={quote('テンプレートが見つかりません。')}",
                    status_code=303,
                )
            await import_csv_mapping_template_header_upload(
                cur,
                request=request,
                user=user,
                csv_format_version_id=csv_format_version_id,
                template=template,
                csv_file=csv_file,
            )
            conn.commit()
        except ValueError as exc:
            conn.rollback()
            return RedirectResponse(
                f"/admin/csv-mapping-templates/{csv_format_version_id}/edit?error={quote(str(exc))}",
                status_code=303,
            )
        except Exception:
            conn.rollback()
            raise

    return RedirectResponse(
        f"/admin/csv-mapping-templates/{csv_format_version_id}/edit?message={quote('基準ヘッダーを読み込みました。')}",
        status_code=303,
    )


@app.get("/admin/facilities/new", response_class=HTMLResponse)
def new_admin_facility_form(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_view_business_settings(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    return templates.TemplateResponse(
        "admin_facility_new.html",
        {
            "request": request,
            "user": user,
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
        },
    )


@app.post("/admin/facilities", response_class=HTMLResponse)
async def create_admin_facility(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_manage_business_settings(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await read_form(request)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            values = normalize_facility_form(form)
            cur.execute(
                f"""
                INSERT INTO {qname(master_db())}.exam_facilities (
                  exam_facility_code,
                  exam_facility_name,
                  exam_facility_display_name,
                  exam_facility_type,
                  medical_institution_code,
                  reservation_system_medical_institution_code,
                  postal_code,
                  address,
                  phone_number,
                  note,
                  is_active
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    values["exam_facility_code"],
                    values["exam_facility_name"],
                    values["exam_facility_display_name"],
                    values["exam_facility_type"],
                    values["medical_institution_code"],
                    values["reservation_system_medical_institution_code"],
                    values["postal_code"],
                    values["address"],
                    values["phone_number"],
                    values["note"],
                    values["is_active"],
                ),
            )
            log_audit(
                cur,
                request=request,
                user=user,
                action_code="ADMIN_EXAM_FACILITY_CREATE",
                target_schema=master_db(),
                target_table="exam_facilities",
                target_id=str(cur.lastrowid or ""),
                after=values,
            )
            conn.commit()
        except ValueError as exc:
            conn.rollback()
            return RedirectResponse(f"/admin/facilities/new?error={quote(str(exc))}", status_code=303)
        except Exception:
            conn.rollback()
            raise
    return RedirectResponse("/admin/facility-master?message=健診機関を作成しました。", status_code=303)


@app.post("/admin/facilities/{exam_facility_id}", response_class=HTMLResponse)
async def update_admin_facility(request: Request, exam_facility_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_manage_business_settings(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await read_form(request)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            values = normalize_facility_form(form)
            cur.execute(
                f"""
                UPDATE {qname(master_db())}.exam_facilities
                   SET exam_facility_code = %s,
                       exam_facility_name = %s,
                       exam_facility_display_name = %s,
                       exam_facility_type = %s,
                       medical_institution_code = %s,
                       reservation_system_medical_institution_code = %s,
                       postal_code = %s,
                       address = %s,
                       phone_number = %s,
                       note = %s,
                       is_active = %s
                 WHERE exam_facility_id = %s
                """,
                (
                    values["exam_facility_code"],
                    values["exam_facility_name"],
                    values["exam_facility_display_name"],
                    values["exam_facility_type"],
                    values["medical_institution_code"],
                    values["reservation_system_medical_institution_code"],
                    values["postal_code"],
                    values["address"],
                    values["phone_number"],
                    values["note"],
                    values["is_active"],
                    exam_facility_id,
                ),
            )
            log_audit(
                cur,
                request=request,
                user=user,
                action_code="ADMIN_EXAM_FACILITY_UPDATE",
                target_schema=master_db(),
                target_table="exam_facilities",
                target_id=str(exam_facility_id),
                after=values,
            )
            conn.commit()
        except ValueError as exc:
            conn.rollback()
            return RedirectResponse(f"/admin/facility-master?error={quote(str(exc))}", status_code=303)
        except Exception:
            conn.rollback()
            raise
    return RedirectResponse("/admin/facility-master?message=健診機関を更新しました。", status_code=303)


@app.get("/admin/folder-aliases/new", response_class=HTMLResponse)
def new_admin_folder_alias_form(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_view_business_settings(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        event_options = load_event_options(cur)
        csv_format_options = load_csv_format_options(cur)
        cur.close()
    return templates.TemplateResponse(
        "admin_folder_alias_new.html",
        {
            "request": request,
            "user": user,
            "event_options": event_options,
            "csv_format_options": csv_format_options,
            "source_mode_options": source_mode_options(),
            "prefecture_options": PREFECTURE_OPTIONS,
            "prefill": {
                "event_id": request.query_params.get("event_id", ""),
                "src_folder_raw": request.query_params.get("src_folder_raw", ""),
                "dst_folder_norm": request.query_params.get("dst_folder_norm", ""),
                "note": request.query_params.get("note", ""),
            },
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
        },
    )


@app.post("/admin/folder-aliases", response_class=HTMLResponse)
async def create_admin_folder_alias(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_manage_business_settings(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await read_form(request)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            values = normalize_folder_alias_form(cur, form)
            cur.execute(
                f"""
                INSERT INTO {qname(master_db())}.medical_folder_aliases (
                  event_id,
                  src_folder_raw,
                  dst_folder_norm,
                  exam_facility_id,
                  expected_source_mode,
                  csv_format_version_id,
                  manual_judgement,
                  note,
                  is_active
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    values["event_id"],
                    values["src_folder_raw"],
                    values["dst_folder_norm"],
                    values["exam_facility_id"],
                    values["expected_source_mode"],
                    values["csv_format_version_id"],
                    values["manual_judgement"],
                    values["note"],
                    values["is_active"],
                ),
            )
            log_audit(
                cur,
                request=request,
                user=user,
                action_code="ADMIN_FOLDER_ALIAS_CREATE",
                target_schema=master_db(),
                target_table="medical_folder_aliases",
                target_id=str(cur.lastrowid or ""),
                after=values,
            )
            conn.commit()
        except ValueError as exc:
            conn.rollback()
            return RedirectResponse(f"/admin/folder-aliases/new?error={quote(str(exc))}", status_code=303)
        except IntegrityError as exc:
            conn.rollback()
            if getattr(exc, "errno", None) == 1062 and "uq_medical_folder_aliases_event_src" in str(exc):
                message = "同じイベントに同じ実フォルダ名の受領フォルダが既にあります。一覧の編集から更新してください。"
                return RedirectResponse(f"/admin/folder-aliases?error={quote(message)}", status_code=303)
            raise
        except Exception:
            conn.rollback()
            raise
    return RedirectResponse("/admin/folder-aliases?message=受領フォルダを作成しました。", status_code=303)


@app.post("/admin/folder-aliases/{alias_id}", response_class=HTMLResponse)
async def update_admin_folder_alias(request: Request, alias_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_manage_business_settings(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await read_form(request)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            values = normalize_folder_alias_form(cur, form)
            if values["exam_facility_id"] is None:
                cur.execute(
                    f"""
                    SELECT exam_facility_id
                    FROM {qname(master_db())}.medical_folder_aliases
                    WHERE alias_id = %s
                    LIMIT 1
                    """,
                    (alias_id,),
                )
                current_alias = cur.fetchone() or {}
                values["exam_facility_id"] = current_alias.get("exam_facility_id")
            cur.execute(
                f"""
                UPDATE {qname(master_db())}.medical_folder_aliases
                   SET event_id = %s,
                       src_folder_raw = %s,
                       dst_folder_norm = %s,
                       exam_facility_id = %s,
                       expected_source_mode = %s,
                       csv_format_version_id = %s,
                       manual_judgement = %s,
                       note = %s,
                       is_active = %s
                 WHERE alias_id = %s
                """,
                (
                    values["event_id"],
                    values["src_folder_raw"],
                    values["dst_folder_norm"],
                    values["exam_facility_id"],
                    values["expected_source_mode"],
                    values["csv_format_version_id"],
                    values["manual_judgement"],
                    values["note"],
                    values["is_active"],
                    alias_id,
                ),
            )
            log_audit(
                cur,
                request=request,
                user=user,
                action_code="ADMIN_FOLDER_ALIAS_UPDATE",
                target_schema=master_db(),
                target_table="medical_folder_aliases",
                target_id=str(alias_id),
                after=values,
            )
            conn.commit()
        except ValueError as exc:
            conn.rollback()
            return RedirectResponse(f"/admin/folder-aliases?error={quote(str(exc))}", status_code=303)
        except Exception:
            conn.rollback()
            raise
    return RedirectResponse("/admin/folder-aliases?message=受領フォルダを更新しました。", status_code=303)


@app.get("/exam-ledgers", response_class=HTMLResponse)
def exam_ledgers(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, ("export_lists.view", "export_lists.edit", "users.manage")):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    filters = {
        "event_id": request.query_params.get("event_id", "2"),
        "file_receipt_id": request.query_params.get("file_receipt_id", ""),
        "source_type": request.query_params.get("source_type", ""),
        "subscriber_match_filter": request.query_params.get("subscriber_match_filter", ""),
        "check_status": request.query_params.get("check_status", ""),
        "q": request.query_params.get("q", ""),
        "name_kana": request.query_params.get("name_kana", ""),
        "facility_q": request.query_params.get("facility_q", ""),
        "facility_codes": request.query_params.get("facility_codes", ""),
        "limit": request.query_params.get("limit", "2000"),
    }
    limit = parse_positive_int(filters["limit"], default=2000, maximum=5000)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            event_options = load_event_options(cur)
            folder_aliases = load_received_folder_alias_rows(cur)
            rows = load_exam_ledger_rows(cur, filters=filters, limit=limit)
            if audit_enabled(cur):
                for row in rows:
                    log_audit(
                        cur,
                        request=request,
                        user=user,
                        action_code="PERSONAL_INFO_VIEW_EXAM_LEDGER",
                        target_schema=health_db(),
                        target_table="exam_ledgers",
                        target_id=str(row.get("exam_ledger_id") or ""),
                        after={
                            "exam_ledger_id": row.get("exam_ledger_id"),
                            "hia_subscriber_id": row.get("hia_subscriber_id"),
                            "person_id_custom": row.get("person_id_custom"),
                            "exam_date": str(row.get("exam_date") or ""),
                            "facility_code": row.get("facility_code"),
                        },
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return templates.TemplateResponse(
        "exam_ledgers.html",
        {
            "request": request,
            "user": user,
            "rows": rows,
            "filters": filters,
            "limit": limit,
            "event_options": event_options,
            "folder_aliases": folder_aliases,
        },
    )


@app.get("/subscriber-match-review", response_class=HTMLResponse)
def subscriber_match_review(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, ("export_lists.view", "export_lists.edit", "users.manage")):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    filters = {
        "event_id": request.query_params.get("event_id", "2"),
        "status_filter": request.query_params.get("status_filter", ""),
        "q": request.query_params.get("q", ""),
        "facility_q": request.query_params.get("facility_q", ""),
        "facility_codes": request.query_params.get("facility_codes", ""),
        "exam_month": request.query_params.get("exam_month", ""),
        "limit": request.query_params.get("limit", "200"),
    }
    selected_ledger_id = parse_positive_int(request.query_params.get("ledger_id", ""), default=0, maximum=999999999999)
    candidate_query = request.query_params.get("candidate_q", "").strip()
    candidate_filters = {
        "name_kana": request.query_params.get("candidate_name_kana", "").strip(),
        "insurance_symbol": request.query_params.get("candidate_insurance_symbol", "").strip(),
        "insurance_number": request.query_params.get("candidate_insurance_number", "").strip(),
        "employee_code": request.query_params.get("candidate_employee_code", "").strip(),
    }
    limit = parse_positive_int(filters["limit"], default=200, maximum=1000)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            event_options = load_event_options(cur)
            folder_aliases = load_subscriber_match_issue_folder_alias_rows(cur, filters=filters)
            exam_month_options = load_subscriber_match_issue_month_options(cur, filters=filters)
            rows = load_subscriber_match_issue_rows(cur, filters=filters, limit=limit)
            selected_ledger = None
            if selected_ledger_id:
                selected_ledger = load_exam_ledger_detail(cur, exam_ledger_id=selected_ledger_id)
            elif rows:
                selected_ledger = load_exam_ledger_detail(cur, exam_ledger_id=int(rows[0]["exam_ledger_id"]))
            candidate_rows = load_subscriber_match_candidate_rows(
                cur,
                ledger=selected_ledger,
                event_id=filters["event_id"],
                query=candidate_query,
                candidate_filters=candidate_filters,
            )
            if audit_enabled(cur):
                for row in rows:
                    log_audit(
                        cur,
                        request=request,
                        user=user,
                        action_code="PERSONAL_INFO_VIEW_SUBSCRIBER_MATCH_REVIEW",
                        target_schema=health_db(),
                        target_table="exam_ledgers",
                        target_id=str(row.get("exam_ledger_id") or ""),
                        after={
                            "exam_ledger_id": row.get("exam_ledger_id"),
                            "hia_subscriber_id": row.get("hia_subscriber_id"),
                            "person_id_custom": row.get("person_id_custom"),
                            "subscriber_match_status": row.get("subscriber_match_status"),
                        },
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return templates.TemplateResponse(
        "subscriber_match_review.html",
        {
            "request": request,
            "user": user,
            "event_options": event_options,
            "filters": filters,
            "rows": rows,
            "folder_aliases": folder_aliases,
            "exam_month_options": exam_month_options,
            "selected_exam_months": split_filter_values(filters.get("exam_month")),
            "selected_ledger": selected_ledger,
            "candidate_rows": candidate_rows,
            "candidate_query": candidate_query,
            "candidate_filters": candidate_filters,
            "limit": limit,
            "message": request.query_params.get("message", ""),
            "error": request.query_params.get("error", ""),
        },
    )


@app.post("/subscriber-match-review/{exam_ledger_id}/confirm")
async def subscriber_match_confirm(request: Request, exam_ledger_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, ("export_lists.edit", "users.manage")):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await request.form()
    try:
        subscriber_id = int(str(form.get("subscriber_id") or "").strip())
    except ValueError:
        return RedirectResponse(f"/subscriber-match-review?ledger_id={exam_ledger_id}&error=加入者候補が不正です。", status_code=303)
    note = str(form.get("note") or "").strip()
    if not note:
        return RedirectResponse(f"/subscriber-match-review?ledger_id={exam_ledger_id}&error=確定理由を入力してください。", status_code=303)
    apply_subscriber_values = str(form.get("apply_subscriber_values") or "0").strip() == "1"
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            result = confirm_exam_ledger_subscriber_match(
                cur,
                exam_ledger_id=exam_ledger_id,
                subscriber_id=subscriber_id,
                note=note,
                app_user_id=int(user["app_user_id"]),
                apply_subscriber_values=apply_subscriber_values,
            )
            if result is None:
                conn.rollback()
                return RedirectResponse(f"/subscriber-match-review?ledger_id={exam_ledger_id}&error=ledgerまたは加入者候補が見つかりません。", status_code=303)
            if audit_enabled(cur):
                log_audit(
                    cur,
                    request=request,
                    user=user,
                    action_code="EXAM_LEDGER_SUBSCRIBER_MATCH_CONFIRM",
                    target_schema=health_db(),
                    target_table="exam_ledgers",
                    target_id=str(exam_ledger_id),
                    after=result,
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    if apply_subscriber_values:
        message = "加入者突合を確定し、加入者情報を出力用基本情報へ適用しました。case反映には健診結果処理のstep5〜7を再実行してください。"
    else:
        message = "加入者突合を手動確定しました。case反映には健診結果処理のstep5〜7を再実行してください。"
    return RedirectResponse(f"/subscriber-match-review?ledger_id={exam_ledger_id}&message={quote(message, safe='')}", status_code=303)


@app.post("/subscriber-match-review/{exam_ledger_id}/workflow-status")
async def subscriber_match_workflow_status(request: Request, exam_ledger_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, ("export_lists.edit", "users.manage")):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await request.form()
    new_status = str(form.get("new_status") or "").strip()
    note = str(form.get("note") or "").strip()
    if new_status not in SUBSCRIBER_MATCH_WORKFLOW_STATUSES:
        return RedirectResponse(f"/subscriber-match-review?ledger_id={exam_ledger_id}&error=変更先の状態が不正です。", status_code=303)
    if not note:
        return RedirectResponse(f"/subscriber-match-review?ledger_id={exam_ledger_id}&error=状態変更理由を入力してください。", status_code=303)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            result = update_exam_ledger_subscriber_match_workflow_status(
                cur,
                exam_ledger_id=exam_ledger_id,
                new_status=new_status,
                note=note,
                app_user_id=int(user["app_user_id"]),
            )
            if result is None:
                conn.rollback()
                return RedirectResponse(f"/subscriber-match-review?ledger_id={exam_ledger_id}&error=ledgerが見つかりません。", status_code=303)
            if audit_enabled(cur):
                log_audit(
                    cur,
                    request=request,
                    user=user,
                    action_code="UPDATE_SUBSCRIBER_MATCH_WORKFLOW_STATUS",
                    target_schema=health_db(),
                    target_table="exam_ledgers",
                    target_id=str(exam_ledger_id),
                    after=result,
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    label = SUBSCRIBER_MATCH_WORKFLOW_STATUSES.get(new_status, new_status)
    message = f"加入者突合の業務状態を「{label}」に変更しました。必要に応じて健診結果処理のstep5〜7を再実行してください。"
    return RedirectResponse(f"/subscriber-match-review?ledger_id={exam_ledger_id}&message={quote(message, safe='')}", status_code=303)


@app.get("/facility-summary", response_class=HTMLResponse)
def facility_summary(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, ("export_lists.view", "export_lists.edit", "users.manage")):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    filters = {
        "event_id": request.query_params.get("event_id", "2"),
        "q": request.query_params.get("q", ""),
        "exam_month": request.query_params.get("exam_month", ""),
        "limit": request.query_params.get("limit", "200"),
    }
    limit = parse_positive_int(filters["limit"], default=200, maximum=1000)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            event_options = load_event_options(cur)
            exam_month_options = load_facility_summary_month_options(cur, event_id=filters["event_id"])
            rows = load_facility_summary_rows(cur, filters=filters, limit=limit)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    summary = {
        "facility_count": len(rows),
        "file_total": sum(int(row.get("file_total") or 0) for row in rows),
        "source_total": sum(int(row.get("source_count") or 0) for row in rows),
        "case_total": sum(int(row.get("case_count") or 0) for row in rows),
        "legal_ng_total": sum(int(row.get("legal_ng_count") or 0) for row in rows),
        "specific_ng_total": sum(int(row.get("specific_ng_count") or 0) for row in rows),
        "item_error_total": sum(int(row.get("item_error_count") or 0) for row in rows),
    }
    return templates.TemplateResponse(
        "facility_summary.html",
        {
            "request": request,
            "user": user,
            "event_options": event_options,
            "exam_month_options": exam_month_options,
            "selected_exam_months": split_filter_values(filters.get("exam_month")),
            "filters": filters,
            "rows": rows,
            "summary": summary,
            "limit": limit,
        },
    )


@app.get("/facility-summary/detail", response_class=HTMLResponse)
def facility_summary_detail(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, ("export_lists.view", "export_lists.edit", "users.manage")):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    filters = {
        "event_id": request.query_params.get("event_id", "2"),
        "facility_code": request.query_params.get("facility_code", ""),
        "exam_month": request.query_params.get("exam_month", ""),
    }
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            event_options = load_event_options(cur)
            exam_month_options = load_facility_summary_month_options(cur, event_id=filters["event_id"])
            detail = load_facility_summary_detail(
                cur,
                event_id=filters["event_id"],
                facility_code=filters["facility_code"],
                exam_month=filters["exam_month"],
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return templates.TemplateResponse(
        "facility_summary_detail.html",
        {
            "request": request,
            "user": user,
            "event_options": event_options,
            "exam_month_options": exam_month_options,
            "selected_exam_months": split_filter_values(filters.get("exam_month")),
            "filters": filters,
            "detail": detail,
        },
    )


@app.get("/exam-ledgers/search", response_class=HTMLResponse)
def exam_ledger_search(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, ("export_lists.view", "export_lists.edit", "users.manage")):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    search_filters = {
        "event_id": request.query_params.get("event_id", "2"),
        "name_kana": request.query_params.get("name_kana", "").strip(),
        "hia_subscriber_id": request.query_params.get("hia_subscriber_id", "").strip(),
        "insurance_symbol": request.query_params.get("insurance_symbol", "").strip(),
        "insurance_number": request.query_params.get("insurance_number", "").strip(),
    }
    has_search = bool(
        search_filters["name_kana"]
        or search_filters["hia_subscriber_id"]
        or search_filters["insurance_number"]
        or search_filters["insurance_symbol"]
    )
    search_error = "記号だけでは検索できません。保険番号も入力してください。" if (
        search_filters["insurance_symbol"] and not search_filters["insurance_number"]
        and not search_filters["name_kana"] and not search_filters["hia_subscriber_id"]
    ) else None
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            event_options = load_event_options(cur)
            if search_error:
                search_results = []
            else:
                search_results = search_exam_ledger_candidates(
                    cur,
                    event_id=search_filters["event_id"],
                    name_kana=search_filters["name_kana"],
                    hia_subscriber_id=search_filters["hia_subscriber_id"],
                    insurance_symbol=search_filters["insurance_symbol"],
                    insurance_number=search_filters["insurance_number"],
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return templates.TemplateResponse(
        "exam_ledger_detail.html",
        {
            "request": request,
            "user": user,
            "ledger": None,
            "item_rows": [],
            "event_options": event_options,
            "search_filters": search_filters,
            "search_results": search_results,
            "has_search": has_search,
            "search_error": search_error,
        },
    )


@app.get("/exam-export-cases", response_class=HTMLResponse)
def exam_export_cases(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, ("export_lists.view", "export_lists.edit", "users.manage")):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    filters = {
        "event_id": request.query_params.get("event_id", "2"),
        "legal_check_result": request.query_params.get("legal_check_result", ""),
        "specific_check_result": request.query_params.get("specific_check_result", ""),
        "export_readiness_status": request.query_params.get("export_readiness_status", ""),
        "source_mode": request.query_params.get("source_mode", ""),
        "exam_month": request.query_params.get("exam_month", ""),
        "q": request.query_params.get("q", ""),
        "case_id": request.query_params.get("case_id", ""),
        "name_full": request.query_params.get("name_full", ""),
        "name_kana": request.query_params.get("name_kana", ""),
        "insurance_symbol": request.query_params.get("insurance_symbol", ""),
        "insurance_number": request.query_params.get("insurance_number", ""),
        "hia_subscriber_id": request.query_params.get("hia_subscriber_id", ""),
        "subscriber_id": request.query_params.get("subscriber_id", ""),
        "qualification_lost_status": request.query_params.get("qualification_lost_status", ""),
        "qualification_lost_date": request.query_params.get("qualification_lost_date", ""),
        "facility_q": request.query_params.get("facility_q", ""),
        "facility_codes": request.query_params.get("facility_codes", ""),
        "duplicate_subscriber": request.query_params.get("duplicate_subscriber", ""),
        "author_import_status": request.query_params.get("author_import_status", ""),
        "exam_item_namecodes": request.query_params.get("exam_item_namecodes", ""),
        "exam_item_match_mode": request.query_params.get("exam_item_match_mode", "any"),
        "limit": request.query_params.get("limit", "500"),
        "page": request.query_params.get("page", "1"),
    }
    limit = parse_positive_int(filters["limit"], default=500, maximum=5000)
    page = parse_positive_int(filters["page"], default=1, maximum=1000000)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            event_options = load_event_options(cur)
            case_facility_options = load_exam_export_case_facility_options(cur, event_id=filters["event_id"])
            exam_month_options = load_exam_export_case_month_options(cur, event_id=filters["event_id"])
            selected_case_exam_items = load_export_candidate_exam_items(
                cur,
                namecodes=split_filter_values(filters["exam_item_namecodes"]),
            )
            unfiltered_total_count = load_exam_export_case_count(
                cur,
                filters=exam_export_case_base_filters(filters),
            )
            total_count = load_exam_export_case_count(cur, filters=filters)
            page_count = max(1, (total_count + limit - 1) // limit)
            page = min(page, page_count)
            offset = (page - 1) * limit
            rows = load_exam_export_case_rows(cur, filters=filters, limit=limit, offset=offset)
            summary = load_exam_export_case_summary(cur, filters=filters)
            pagination = build_exam_export_case_pagination(
                filters,
                total_count=total_count,
                row_count=len(rows),
                page=page,
                limit=limit,
            )
            summary_filter_urls = build_exam_export_case_summary_filter_urls(filters, limit=limit)
            if audit_enabled(cur):
                for row in rows:
                    log_audit(
                        cur,
                        request=request,
                        user=user,
                        action_code="PERSONAL_INFO_VIEW_EXAM_EXPORT_CASE",
                        target_schema=health_db(),
                        target_table="exam_export_cases",
                        target_id=str(row.get("exam_export_case_id") or ""),
                        after={
                            "exam_export_case_id": row.get("exam_export_case_id"),
                            "hia_subscriber_id": row.get("hia_subscriber_id"),
                            "subscriber_id": row.get("subscriber_id"),
                            "exam_date": str(row.get("exam_date") or ""),
                        },
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return templates.TemplateResponse(
        "exam_export_cases.html",
        {
            "request": request,
            "user": user,
            "event_options": event_options,
            "filters": filters,
            "rows": rows,
            "summary": summary,
            "summary_filter_urls": summary_filter_urls,
            "total_count": total_count,
            "unfiltered_total_count": unfiltered_total_count,
            "has_detail_filters": has_exam_export_case_detail_filters(filters),
            "limit": limit,
            "pagination": pagination,
            "case_facility_options": case_facility_options,
            "exam_month_options": exam_month_options,
            "selected_exam_months": split_filter_values(filters.get("exam_month")),
            "selected_case_exam_items": selected_case_exam_items,
            "can_download_csv": can_download_exam_export_case_csv(user),
            "csv_field_groups": EXAM_EXPORT_CASE_CSV_FIELD_GROUPS,
            "csv_default_fields": set(EXAM_EXPORT_CASE_CSV_DEFAULT_FIELDS),
        },
    )


@app.post("/exam-export-cases/download.csv")
async def exam_export_cases_csv_download(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not can_download_exam_export_case_csv(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await request.form()
    filters = {
        "event_id": str(form.get("event_id") or "2"),
        "legal_check_result": str(form.get("legal_check_result") or ""),
        "specific_check_result": str(form.get("specific_check_result") or ""),
        "export_readiness_status": str(form.get("export_readiness_status") or ""),
        "source_mode": str(form.get("source_mode") or ""),
        "exam_month": str(form.get("exam_month") or ""),
        "q": str(form.get("q") or ""),
        "case_id": str(form.get("case_id") or ""),
        "name_full": str(form.get("name_full") or ""),
        "name_kana": str(form.get("name_kana") or ""),
        "insurance_symbol": str(form.get("insurance_symbol") or ""),
        "insurance_number": str(form.get("insurance_number") or ""),
        "hia_subscriber_id": str(form.get("hia_subscriber_id") or ""),
        "subscriber_id": str(form.get("subscriber_id") or ""),
        "qualification_lost_status": str(form.get("qualification_lost_status") or ""),
        "qualification_lost_date": str(form.get("qualification_lost_date") or ""),
        "facility_q": str(form.get("facility_q") or ""),
        "facility_codes": str(form.get("facility_codes") or ""),
        "exam_item_namecodes": str(form.get("exam_item_namecodes") or ""),
        "exam_item_match_mode": str(form.get("exam_item_match_mode") or "any"),
        "limit": "5000",
        "page": "1",
    }
    fields = normalize_exam_export_case_csv_fields([str(value) for value in form.getlist("fields")])
    export_limit = parse_positive_int(str(form.get("export_limit") or ""), default=30000, maximum=50000)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            total_count = load_exam_export_case_count(cur, filters=filters)
            rows = load_exam_export_case_rows(cur, filters=filters, limit=min(total_count, export_limit), offset=0)
            if audit_enabled(cur):
                log_audit(
                    cur,
                    request=request,
                    user=user,
                    action_code="DOWNLOAD_EXAM_EXPORT_CASES_CSV",
                    target_schema=health_db(),
                    target_table="exam_export_cases",
                    target_id=f"event_id={filters.get('event_id') or '-'}",
                    after={
                        "filters": {key: value for key, value in filters.items() if value and key not in {"limit", "page"}},
                        "selected_fields": fields,
                        "row_count": len(rows),
                        "total_count": total_count,
                        "export_limit": export_limit,
                    },
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
    csv_text = build_exam_export_case_csv(rows, fields)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    event_part = re.sub(r"[^0-9A-Za-z_-]+", "", filters.get("event_id") or "all") or "all"
    filename = f"exam_export_cases_event{event_part}_{timestamp}.csv"
    return StreamingResponse(
        iter([csv_text.encode("utf-8")]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/exam-export-cases/{exam_export_case_id}", response_class=HTMLResponse)
def exam_export_case_detail(request: Request, exam_export_case_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, ("export_lists.view", "export_lists.edit", "users.manage")):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            case = load_exam_export_case_detail(cur, exam_export_case_id=exam_export_case_id)
            if case is None:
                conn.commit()
                return RedirectResponse("/exam-export-cases?error=caseが見つかりません。", status_code=303)
            sources = load_exam_export_case_sources(cur, exam_export_case_id=exam_export_case_id)
            source_headers = sources[:3]
            while len(source_headers) < 3:
                source_headers.append({})
            values = load_exam_export_case_values(
                cur,
                exam_export_case_id=exam_export_case_id,
                source_headers=source_headers,
            )
            placeholders = load_exam_export_case_placeholders(cur, exam_export_case_id=exam_export_case_id)
            check_rows = load_exam_export_case_check_rows(cur, exam_export_case_id=exam_export_case_id)
            basic_info_corrections = load_exam_case_basic_info_corrections(
                cur,
                exam_export_case_id=exam_export_case_id,
            )
            basic_info_correction_rows = build_basic_info_correction_rows(case, basic_info_corrections)
            if audit_enabled(cur):
                log_audit(
                    cur,
                    request=request,
                    user=user,
                    action_code="PERSONAL_INFO_VIEW_EXAM_EXPORT_CASE_DETAIL",
                    target_schema=health_db(),
                    target_table="exam_export_cases",
                    target_id=str(exam_export_case_id),
                    after={
                        "exam_export_case_id": exam_export_case_id,
                        "hia_subscriber_id": case.get("hia_subscriber_id"),
                        "subscriber_id": case.get("subscriber_id"),
                        "exam_date": str(case.get("exam_date") or ""),
                        "facility_code": case.get("facility_code"),
                    },
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return templates.TemplateResponse(
        "exam_export_case_detail.html",
        {
            "request": request,
            "user": user,
            "case": case,
            "sources": sources,
            "source_headers": source_headers,
            "values": values,
            "placeholders": placeholders,
            "check_rows": check_rows,
            "basic_info_correction_rows": basic_info_correction_rows,
            "message": request.query_params.get("message", ""),
            "error": request.query_params.get("error", ""),
        },
    )


@app.post("/exam-export-cases/{exam_export_case_id}/basic-info-correction")
async def exam_export_case_basic_info_correction(request: Request, exam_export_case_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, ("export_lists.edit", "users.manage")):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await request.form()
    field_code = str(form.get("field_code") or "").strip()
    action = str(form.get("action") or "save").strip()
    corrected_value = str(form.get("corrected_value") or "").strip()
    if field_code == "report_codes":
        corrected_value = f"{corrected_value}|{str(form.get('corrected_value_secondary') or '').strip()}"
    correction_reason = str(form.get("correction_reason") or "").strip()
    if field_code not in BASIC_INFO_CORRECTION_FIELDS:
        return RedirectResponse(f"/exam-export-cases/{exam_export_case_id}?error=補正対象が不正です。", status_code=303)
    if action == "save":
        if not corrected_value:
            return RedirectResponse(f"/exam-export-cases/{exam_export_case_id}?error=補正値を入力してください。", status_code=303)
        if not correction_reason:
            return RedirectResponse(f"/exam-export-cases/{exam_export_case_id}?error=補正理由を入力してください。", status_code=303)
    elif action != "clear":
        return RedirectResponse(f"/exam-export-cases/{exam_export_case_id}?error=補正操作が不正です。", status_code=303)

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            if action == "clear":
                result = clear_exam_case_basic_info_correction(
                    cur,
                    exam_export_case_id=exam_export_case_id,
                    field_code=field_code,
                    note=correction_reason,
                    app_user_id=int(user["app_user_id"]),
                )
                if result is None:
                    conn.rollback()
                    return RedirectResponse(f"/exam-export-cases/{exam_export_case_id}?error=解除する補正が見つかりません。", status_code=303)
                message = "基本情報補正を解除しました。step5〜7を再実行するとcase一覧へ反映されます。"
                audit_after = {
                    "exam_export_case_id": exam_export_case_id,
                    "field_code": field_code,
                    "action": "clear",
                }
            else:
                result = update_exam_case_basic_info_correction(
                    cur,
                    exam_export_case_id=exam_export_case_id,
                    field_code=field_code,
                    corrected_value=corrected_value,
                    correction_reason=correction_reason,
                    app_user_id=int(user["app_user_id"]),
                )
                if result is None:
                    conn.rollback()
                    return RedirectResponse(f"/exam-export-cases/{exam_export_case_id}?error=補正対象caseが見つかりません。", status_code=303)
                if not result.get("ok"):
                    conn.rollback()
                    reason = result.get("reason") or "正規化できませんでした。"
                    return RedirectResponse(f"/exam-export-cases/{exam_export_case_id}?error=補正値を正規化できません: {reason}", status_code=303)
                message = "基本情報補正を保存しました。step5〜7を再実行するとcase一覧へ反映されます。"
                audit_after = {
                    "exam_export_case_id": exam_export_case_id,
                    "field_code": field_code,
                    "action": "save",
                    "new_value": result.get("new_value"),
                }
            if audit_enabled(cur):
                log_audit(
                    cur,
                    request=request,
                    user=user,
                    action_code="EXAM_CASE_BASIC_INFO_CORRECTION_UPDATE",
                    target_schema=health_db(),
                    target_table="exam_case_basic_info_corrections",
                    target_id=str(exam_export_case_id),
                    after=audit_after,
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return RedirectResponse(f"/exam-export-cases/{exam_export_case_id}?message={quote(message, safe='')}", status_code=303)


@app.post("/exam-export-cases/{exam_export_case_id}/item-review")
async def exam_export_case_item_review(request: Request, exam_export_case_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, ("export_lists.edit", "users.manage")):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await request.form()
    try:
        review_item_id = int(str(form.get("review_item_id") or "").strip())
    except ValueError:
        return RedirectResponse(f"/exam-export-cases/{exam_export_case_id}?error=対象確認項目が不正です。", status_code=303)
    review_status = str(form.get("review_status") or "").strip()
    note = str(form.get("note") or "").strip()
    allowed_statuses = {
        "NEEDS_CONFIRMATION",
        "APPROVED_WITH_REASON",
        "EXCLUDED",
        "WAITING_RESUBMISSION",
        "RESUBMITTED",
        "RESOLVED_BY_SOURCE_VALUE",
        "NONE",
    }
    if review_status not in allowed_statuses:
        return RedirectResponse(f"/exam-export-cases/{exam_export_case_id}?error=状態が不正です。", status_code=303)
    if review_status in {"APPROVED_WITH_REASON", "EXCLUDED"} and not note:
        label = "理由ありOK" if review_status == "APPROVED_WITH_REASON" else "除外"
        return RedirectResponse(f"/exam-export-cases/{exam_export_case_id}?error={label}には理由が必須です。", status_code=303)

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            item = update_exam_case_check_review(
                cur,
                review_item_id=review_item_id,
                review_status=review_status,
                note=note,
                app_user_id=int(user["app_user_id"]),
            )
            if item is None or int(item.get("exam_export_case_id") or 0) != exam_export_case_id:
                conn.rollback()
                return RedirectResponse(f"/exam-export-cases/{exam_export_case_id}?error=対象確認項目がcaseに紐づいていません。", status_code=303)
            if audit_enabled(cur):
                log_audit(
                    cur,
                    request=request,
                    user=user,
                    action_code="EXAM_CASE_CHECK_REVIEW_UPDATE",
                    target_schema=health_db(),
                    target_table="exam_case_check_review_items",
                    target_id=str(review_item_id),
                    after={
                        "exam_export_case_id": exam_export_case_id,
                        "exam_case_check_review_item_id": review_item_id,
                        "check_scope": item.get("check_scope"),
                        "check_item_code": item.get("check_item_code"),
                        "old_review_status": item.get("old_review_status"),
                        "new_review_status": review_status,
                    },
                )
            refresh_export_case_readiness(
                cur,
                health_db=health_db(),
                event_id=int(item.get("event_id") or 0),
                exam_export_case_id=exam_export_case_id,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return RedirectResponse(
        f"/exam-export-cases/{exam_export_case_id}?message=確認状態を保存しました。caseの出力可否へ反映しました。",
        status_code=303,
    )


@app.post("/exam-export-cases/{exam_export_case_id}/item-review-bulk")
async def exam_export_case_item_review_bulk(request: Request, exam_export_case_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, ("export_lists.edit", "users.manage")):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await request.form()
    review_status = str(form.get("review_status") or "").strip()
    note = str(form.get("note") or "").strip()
    target_scope = str(form.get("target_scope") or "unresolved").strip()
    allowed_statuses = {
        "NEEDS_CONFIRMATION",
        "APPROVED_WITH_REASON",
        "EXCLUDED",
        "WAITING_RESUBMISSION",
        "RESUBMITTED",
        "RESOLVED_BY_SOURCE_VALUE",
        "NONE",
    }
    allowed_scopes = {"unresolved", "all"}
    if review_status not in allowed_statuses:
        return RedirectResponse(f"/exam-export-cases/{exam_export_case_id}?error=状態が不正です。", status_code=303)
    if target_scope not in allowed_scopes:
        return RedirectResponse(f"/exam-export-cases/{exam_export_case_id}?error=一括対象が不正です。", status_code=303)
    if review_status in {"APPROVED_WITH_REASON", "EXCLUDED"} and not note:
        label = "理由ありOK" if review_status == "APPROVED_WITH_REASON" else "除外"
        return RedirectResponse(f"/exam-export-cases/{exam_export_case_id}?error={label}には理由が必須です。", status_code=303)

    params = load_mysql_base_params(db_prefix())
    updated = 0
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            review_item_ids = load_exam_case_check_review_item_ids(
                cur,
                exam_export_case_id=exam_export_case_id,
                target_scope=target_scope,
            )
            for review_item_id in review_item_ids:
                item = update_exam_case_check_review(
                    cur,
                    review_item_id=review_item_id,
                    review_status=review_status,
                    note=note,
                    app_user_id=int(user["app_user_id"]),
                )
                if item is None or int(item.get("exam_export_case_id") or 0) != exam_export_case_id:
                    conn.rollback()
                    return RedirectResponse(f"/exam-export-cases/{exam_export_case_id}?error=一括更新対象に不正な確認項目があります。", status_code=303)
                updated += 1
            if audit_enabled(cur):
                log_audit(
                    cur,
                    request=request,
                    user=user,
                    action_code="EXAM_CASE_CHECK_REVIEW_BULK_UPDATE",
                    target_schema=health_db(),
                    target_table="exam_case_check_review_items",
                    target_id=str(exam_export_case_id),
                    after={
                        "exam_export_case_id": exam_export_case_id,
                        "target_scope": target_scope,
                        "review_status": review_status,
                        "updated": updated,
                    },
                )
            if review_item_ids:
                refresh_export_case_readiness(
                    cur,
                    health_db=health_db(),
                    event_id=int(item.get("event_id") or 0),
                    exam_export_case_id=exam_export_case_id,
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return RedirectResponse(
        f"/exam-export-cases/{exam_export_case_id}?message=確認状態を{updated}件まとめて保存しました。caseの出力可否へ反映しました。",
        status_code=303,
    )


@app.get("/exam-ledgers/{exam_ledger_id}", response_class=HTMLResponse)
def exam_ledger_detail(request: Request, exam_ledger_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, ("export_lists.view", "export_lists.edit", "users.manage")):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            ledger = load_exam_ledger_detail(cur, exam_ledger_id=exam_ledger_id)
            if ledger is None:
                conn.commit()
                return RedirectResponse("/exam-ledgers?error=健診結果が見つかりません。", status_code=303)
            item_rows = load_exam_item_value_rows(cur, exam_ledger_id=exam_ledger_id)
            event_options = load_event_options(cur)
            search_filters = {
                "event_id": str(ledger.get("event_id") or "2"),
                "name_kana": request.query_params.get("name_kana", "").strip(),
                "hia_subscriber_id": request.query_params.get("hia_subscriber_id", "").strip(),
                "insurance_symbol": request.query_params.get("insurance_symbol", "").strip(),
                "insurance_number": request.query_params.get("insurance_number", "").strip(),
            }
            has_search = bool(
                search_filters["name_kana"]
                or search_filters["hia_subscriber_id"]
                or search_filters["insurance_number"]
                or search_filters["insurance_symbol"]
            )
            search_error = "記号だけでは検索できません。保険番号も入力してください。" if (
                search_filters["insurance_symbol"] and not search_filters["insurance_number"]
                and not search_filters["name_kana"] and not search_filters["hia_subscriber_id"]
            ) else None
            if search_error:
                search_results = []
            else:
                search_results = search_exam_ledger_candidates(
                    cur,
                    event_id=search_filters["event_id"],
                    name_kana=search_filters["name_kana"],
                    hia_subscriber_id=search_filters["hia_subscriber_id"],
                    insurance_symbol=search_filters["insurance_symbol"],
                    insurance_number=search_filters["insurance_number"],
                )
            if audit_enabled(cur):
                log_audit(
                    cur,
                    request=request,
                    user=user,
                    action_code="PERSONAL_INFO_VIEW_EXAM_LEDGER_DETAIL",
                    target_schema=health_db(),
                    target_table="exam_ledgers",
                    target_id=str(exam_ledger_id),
                    after={
                        "exam_ledger_id": ledger.get("exam_ledger_id"),
                        "hia_subscriber_id": ledger.get("hia_subscriber_id"),
                        "person_id_custom": ledger.get("person_id_custom"),
                        "exam_date": str(ledger.get("exam_date") or ""),
                        "facility_code": ledger.get("facility_code"),
                        "item_count": len(item_rows),
                    },
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return templates.TemplateResponse(
        "exam_ledger_detail.html",
        {
            "request": request,
            "user": user,
            "ledger": ledger,
            "item_rows": item_rows,
            "event_options": event_options,
            "search_filters": search_filters,
            "search_results": search_results,
            "has_search": has_search,
            "search_error": search_error,
        },
    )


@app.get("/export-lists", response_class=HTMLResponse)
def export_lists(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(
        user,
        (
            "export_lists.view",
            "export_lists.edit",
            "xml_export.review",
            "xml_export.official",
            "hia_upload.perform",
            "hia_upload_status.edit",
        ),
    ):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            lists = load_ops_xml_export_lists(cur)
            folder_aliases = load_received_folder_alias_rows(cur)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return templates.TemplateResponse(
        "export_lists.html",
        {
            "request": request,
            "user": user,
            "lists": lists,
            "folder_aliases": folder_aliases,
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
            "can_edit": has_permission(user, "export_lists.edit"),
        },
    )


@app.post("/export-lists", response_class=HTMLResponse)
async def create_export_list(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "export_lists.edit"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await read_form(request)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            xml_export_list_id, candidates, selected = create_xml_export_list_from_form(cur, form=form, user=user)
            conn.commit()
        except ValueError as exc:
            conn.rollback()
            message = "リスト名は必須です。" if str(exc) == "LIST_NAME_REQUIRED" else str(exc)
            return RedirectResponse(f"/export-lists?error={quote(message)}", status_code=303)
        except Exception:
            conn.rollback()
            raise
    return RedirectResponse(
        f"/export-lists/{xml_export_list_id}?message={quote(f'候補{candidates}件から{selected}件を追加しました。')}",
        status_code=303,
    )


@app.get("/export-lists/{xml_export_list_id}", response_class=HTMLResponse)
def export_list_detail(request: Request, xml_export_list_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(
        user,
        (
            "export_lists.view",
            "export_lists.edit",
            "xml_export.official",
            "hia_upload.perform",
            "hia_upload_status.edit",
        ),
    ):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            export_list = load_xml_export_list_detail(cur, xml_export_list_id=xml_export_list_id)
            if not export_list:
                conn.commit()
                return RedirectResponse("/export-lists?error=出力リストが見つかりません。", status_code=303)
            cases = load_ops_xml_export_list_cases(cur, xml_export_list_id=xml_export_list_id)
            candidate_filters = {
                "case_q": request.query_params.get("case_q", ""),
                "facility_q": request.query_params.get("facility_q", ""),
                "facility_codes": request.query_params.get("facility_codes", ""),
                "exam_month": request.query_params.get("exam_month", ""),
                "exam_item_namecodes": request.query_params.getlist("exam_item_namecode"),
                "exam_item_match_mode": request.query_params.get("exam_item_match_mode", "any"),
                "include_export_ready": request.query_params.get("include_export_ready", "1"),
                "include_approved_with_reason": request.query_params.get("include_approved_with_reason", "1"),
                "include_exported": request.query_params.get("include_exported", ""),
            }
            show_candidates = request.query_params.get("show_candidates") == "1"
            selected_candidate_exam_items = load_export_candidate_exam_items(
                cur,
                namecodes=candidate_filters["exam_item_namecodes"],
            )
            add_candidates = (
                load_export_case_add_candidates(
                    cur,
                    xml_export_list_id=xml_export_list_id,
                    event_id=int(export_list["event_id"]),
                    filters=candidate_filters,
                )
                if show_candidates
                else []
            )
            review_downloads = load_review_xml_export_downloads(event_id=int(export_list["event_id"]))
            folder_aliases = load_received_folder_alias_rows(cur)
            log_personal_info_view(
                cur,
                request=request,
                user=user,
                action_code="PERSONAL_INFO_VIEW_EXPORT_LIST",
                cases=cases,
                list_id=xml_export_list_id,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return templates.TemplateResponse(
        "export_list_detail.html",
        {
            "request": request,
            "user": user,
            "export_list": export_list,
            "cases": cases,
            "add_candidates": add_candidates,
            "addable_candidate_count": sum(
                1
                for candidate in add_candidates
                if candidate.get("existing_list_case_id") is None
                or candidate.get("existing_removed_at") is not None
            ),
            "candidate_filters": candidate_filters,
            "selected_candidate_exam_items": selected_candidate_exam_items,
            "show_candidates": show_candidates,
            "folder_aliases": folder_aliases,
            "review_downloads": review_downloads,
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
            "can_edit": has_permission(user, "export_lists.edit"),
            "can_review_export": has_permission(user, "xml_export.review"),
            "can_export": has_permission(user, "xml_export.official"),
            "prefecture_options": PREFECTURE_OPTIONS,
        },
    )


EXPORT_LIST_CANDIDATE_QUERY_KEYS = {
    "show_candidates",
    "case_q",
    "facility_q",
    "facility_codes",
    "exam_month",
    "exam_item_namecode",
    "exam_item_match_mode",
    "include_export_ready",
    "include_approved_with_reason",
    "include_exported",
}


def export_list_candidate_return_url(
    *,
    xml_export_list_id: int,
    return_query: str,
    message: str | None = None,
    error: str | None = None,
) -> str:
    parsed = parse_qs(return_query, keep_blank_values=False)
    query = {
        key: values
        for key, values in parsed.items()
        if key in EXPORT_LIST_CANDIDATE_QUERY_KEYS and values
    }
    query["show_candidates"] = ["1"]
    if message:
        query["message"] = [message]
    if error:
        query["error"] = [error]
    return f"/export-lists/{xml_export_list_id}?{urlencode(query, doseq=True)}"


@app.post("/export-lists/{xml_export_list_id}/name", response_class=HTMLResponse)
async def export_list_name_update(request: Request, xml_export_list_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "export_lists.edit"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    form = await read_form(request)
    list_name = str(form.get("list_name") or "").strip()
    if not list_name:
        return RedirectResponse(
            f"/export-lists/{xml_export_list_id}?error={quote('出力リスト名を入力してください。')}",
            status_code=303,
        )
    if len(list_name) > 255:
        return RedirectResponse(
            f"/export-lists/{xml_export_list_id}?error={quote('出力リスト名は255文字以内で入力してください。')}",
            status_code=303,
        )

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            export_list = load_xml_export_list_detail(cur, xml_export_list_id=xml_export_list_id)
            if not export_list:
                conn.rollback()
                return RedirectResponse("/export-lists?error=出力リストが見つかりません。", status_code=303)
            old_name = str(export_list.get("list_name") or "")
            cur.execute(
                f"""
                UPDATE {qname(health_db())}.ops_xml_export_lists
                SET list_name = %s
                WHERE xml_export_list_id = %s
                """,
                (list_name, xml_export_list_id),
            )
            log_audit(
                cur,
                request=request,
                user=user,
                action_code="XML_EXPORT_LIST_NAME_UPDATE",
                target_schema=health_db(),
                target_table="ops_xml_export_lists",
                target_id=str(xml_export_list_id),
                after={"old_list_name": old_name, "list_name": list_name},
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    return RedirectResponse(
        f"/export-lists/{xml_export_list_id}?message={quote('出力リスト名を変更しました。')}",
        status_code=303,
    )


@app.post("/export-lists/{xml_export_list_id}/cases/add", response_class=HTMLResponse)
async def export_list_case_add(request: Request, xml_export_list_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "export_lists.edit"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await read_form(request)
    return_query = str(form.get("return_query") or "")
    try:
        exam_export_case_id = int(str(form.get("exam_export_case_id") or "").strip())
    except ValueError:
        return RedirectResponse(
            export_list_candidate_return_url(
                xml_export_list_id=xml_export_list_id,
                return_query=return_query,
                error="追加するcaseを選択してください。",
            ),
            status_code=303,
        )
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            action = add_export_case_to_list(
                cur,
                xml_export_list_id=xml_export_list_id,
                exam_export_case_id=exam_export_case_id,
                user=user,
            )
            if audit_enabled(cur):
                log_audit(
                    cur,
                    request=request,
                    user=user,
                    action_code="XML_EXPORT_LIST_CASE_ADD",
                    target_schema=health_db(),
                    target_table="ops_xml_export_list_cases",
                    target_id=str(xml_export_list_id),
                    after={
                        "xml_export_list_id": xml_export_list_id,
                        "exam_export_case_id": exam_export_case_id,
                        "action": action,
                    },
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    labels = {"added": "追加しました。", "readded": "再追加しました。", "already": "すでに追加済みです。"}
    return RedirectResponse(
        export_list_candidate_return_url(
            xml_export_list_id=xml_export_list_id,
            return_query=return_query,
            message=labels.get(action, "更新しました。"),
        ),
        status_code=303,
    )


@app.post("/export-lists/{xml_export_list_id}/cases/add-visible", response_class=HTMLResponse)
async def export_list_visible_cases_add(request: Request, xml_export_list_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "export_lists.edit"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    form = await read_form(request)
    return_query = str(form.get("return_query") or "")
    case_ids: list[int] = []
    for raw_case_id in str(form.get("exam_export_case_ids") or "").split(","):
        try:
            case_id = int(raw_case_id.strip())
        except ValueError:
            continue
        if case_id > 0 and case_id not in case_ids:
            case_ids.append(case_id)
    if not case_ids:
        return RedirectResponse(
            export_list_candidate_return_url(
                xml_export_list_id=xml_export_list_id,
                return_query=return_query,
                error="追加できる候補がありません。",
            ),
            status_code=303,
        )

    action_counts = {"added": 0, "readded": 0, "already": 0}
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            for exam_export_case_id in case_ids:
                action = add_export_case_to_list(
                    cur,
                    xml_export_list_id=xml_export_list_id,
                    exam_export_case_id=exam_export_case_id,
                    user=user,
                )
                action_counts[action] = action_counts.get(action, 0) + 1
            if audit_enabled(cur):
                log_audit(
                    cur,
                    request=request,
                    user=user,
                    action_code="XML_EXPORT_LIST_VISIBLE_CASES_ADD",
                    target_schema=health_db(),
                    target_table="ops_xml_export_list_cases",
                    target_id=str(xml_export_list_id),
                    after={
                        "xml_export_list_id": xml_export_list_id,
                        "exam_export_case_ids": case_ids,
                        "action_counts": action_counts,
                    },
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    message_parts = [f"{action_counts['added']}件追加"]
    if action_counts["readded"]:
        message_parts.append(f"{action_counts['readded']}件再追加")
    if action_counts["already"]:
        message_parts.append(f"{action_counts['already']}件追加済み")
    return RedirectResponse(
        export_list_candidate_return_url(
            xml_export_list_id=xml_export_list_id,
            return_query=return_query,
            message="、".join(message_parts) + "しました。",
        ),
        status_code=303,
    )


@app.post("/export-lists/{xml_export_list_id}/cases/{xml_export_list_case_id}/remove", response_class=HTMLResponse)
async def export_list_case_remove(request: Request, xml_export_list_id: int, xml_export_list_case_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "export_lists.edit"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await read_form(request)
    reason = str(form.get("remove_reason") or "").strip()
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            updated = remove_export_list_case(
                cur,
                xml_export_list_id=xml_export_list_id,
                xml_export_list_case_id=xml_export_list_case_id,
                user=user,
                reason=reason,
            )
            if audit_enabled(cur):
                log_audit(
                    cur,
                    request=request,
                    user=user,
                    action_code="XML_EXPORT_LIST_CASE_REMOVE",
                    target_schema=health_db(),
                    target_table="ops_xml_export_list_cases",
                    target_id=str(xml_export_list_case_id),
                    after={
                        "xml_export_list_id": xml_export_list_id,
                        "xml_export_list_case_id": xml_export_list_case_id,
                        "remove_reason": reason,
                        "updated": updated,
                    },
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    message = "リストから外しました。" if updated else "対象はすでに外されているか、見つかりません。"
    return RedirectResponse(f"/export-lists/{xml_export_list_id}?message={quote(message)}", status_code=303)


@app.post("/export-lists/{xml_export_list_id}/export", response_class=HTMLResponse)
async def export_list_run(request: Request, xml_export_list_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    form = await read_form(request)
    output_mode = (form.get("output_mode") or "review").strip().lower()
    package_mode = (form.get("package_mode") or "standard").strip().lower()
    submission_facility_code = (form.get("submission_facility_code") or "").strip()
    include_exported = form.get("include_exported") == "1"
    required_permission = "xml_export.official" if output_mode == "official" else "xml_export.review"
    if not has_permission(user, required_permission):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    try:
        result = run_hia_xml_export_from_list(
            xml_export_list_id=xml_export_list_id,
            output_mode=output_mode,
            package_mode=package_mode,
            submission_facility_code=submission_facility_code,
            include_exported=include_exported,
        )
    except Exception as exc:
        message = str(exc).strip() or type(exc).__name__
        return RedirectResponse(
            f"/export-lists/{xml_export_list_id}?error={quote(message[:1800])}",
            status_code=303,
        )

    mode_label = "本番03フォルダ出力" if output_mode == "official" else "確認出力"
    return RedirectResponse(
        f"/export-lists/{xml_export_list_id}?message={quote(mode_label + 'が完了しました。' + result[:1200])}",
        status_code=303,
    )


@app.get("/export-lists/{xml_export_list_id}/review-zips/download")
def download_review_xml_export_zip(request: Request, xml_export_list_id: int, path: str) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "xml_export.review"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            export_list = load_xml_export_list_detail(cur, xml_export_list_id=xml_export_list_id)
            if not export_list:
                conn.commit()
                return RedirectResponse("/export-lists?error=出力リストが見つかりません。", status_code=303)
            event_id = int(export_list["event_id"])
            zip_path = resolve_review_xml_export_zip(event_id=event_id, relative_path=path)
            root = review_export_event_root(event_id)
            if audit_enabled(cur):
                log_audit(
                    cur,
                    request=request,
                    user=user,
                    action_code="HIA_XML_REVIEW_DOWNLOAD",
                    target_schema="file",
                    target_table="hia_xml_review_exports",
                    target_id=str(zip_path.relative_to(root)),
                    after={
                        "xml_export_list_id": xml_export_list_id,
                        "event_id": event_id,
                        "file_name": zip_path.name,
                        "file_size": zip_path.stat().st_size,
                    },
                )
            conn.commit()
        except FileNotFoundError:
            conn.rollback()
            return RedirectResponse(
                f"/export-lists/{xml_export_list_id}?error={quote('確認用ZIPが見つかりません。すでにダウンロード済みで削除された可能性があります。')}",
                status_code=303,
            )
        except Exception:
            conn.rollback()
            raise

    return FileResponse(
        zip_path,
        media_type="application/zip",
        filename=zip_path.name,
        background=BackgroundTask(delete_file_and_empty_parents, zip_path, stop_at=root),
    )


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


@app.get("/admin/security", response_class=HTMLResponse)
def security_settings(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "users.manage"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        settings = load_app_security_settings(cur)
        cur.close()
    return templates.TemplateResponse(
        "admin_security.html",
        {
            "request": request,
            "user": user,
            "settings": settings,
            "message": None,
            "error": None,
        },
    )


@app.post("/admin/security", response_class=HTMLResponse)
async def update_security_settings(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "users.manage"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await read_form(request)
    try:
        session_lifetime_minutes = max(5, min(24 * 60, int(form.get("session_lifetime_minutes") or "720")))
        session_idle_timeout_minutes = max(0, min(24 * 60, int(form.get("session_idle_timeout_minutes") or "60")))
    except ValueError:
        params = load_mysql_base_params(db_prefix())
        with connect_ctx(params, database=app_db(), autocommit=True) as conn:
            cur = dict_cursor(conn)
            settings = load_app_security_settings(cur)
            cur.close()
        return templates.TemplateResponse(
            "admin_security.html",
            {
                "request": request,
                "user": user,
                "settings": settings,
                "message": None,
                "error": "分数は数値で入力してください。",
            },
            status_code=400,
        )
    audit_on = "1" if form.get("personal_info_audit_enabled") == "1" else "0"
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            upsert_app_setting(
                cur,
                setting_key="session_lifetime_minutes",
                setting_value=str(session_lifetime_minutes),
                value_type="int",
                setting_group="security",
                description="ログインセッションの最大有効時間。初期値は12時間",
                updated_by_app_user_id=int(user["app_user_id"]),
            )
            upsert_app_setting(
                cur,
                setting_key="session_idle_timeout_minutes",
                setting_value=str(session_idle_timeout_minutes),
                value_type="int",
                setting_group="security",
                description="無操作状態で自動ログアウトするまでの分数。0は無効",
                updated_by_app_user_id=int(user["app_user_id"]),
            )
            upsert_app_setting(
                cur,
                setting_key="personal_info_audit_enabled",
                setting_value=audit_on,
                value_type="bool",
                setting_group="audit",
                description="個人情報を含む画面閲覧・ダウンロードを監査ログへ記録する",
                updated_by_app_user_id=int(user["app_user_id"]),
            )
            log_audit(
                cur,
                request=request,
                user=user,
                action_code="APP_SECURITY_SETTINGS_UPDATE",
                target_schema=app_db(),
                target_table="app_settings",
                target_id="security",
                after={
                    "session_lifetime_minutes": session_lifetime_minutes,
                    "session_idle_timeout_minutes": session_idle_timeout_minutes,
                    "personal_info_audit_enabled": audit_on,
                },
            )
            conn.commit()
            settings = load_app_security_settings(cur)
        except Exception:
            conn.rollback()
            raise
    return templates.TemplateResponse(
        "admin_security.html",
        {
            "request": request,
            "user": current_user(request) or user,
            "settings": settings,
            "message": "セキュリティ設定を更新しました。",
            "error": None,
        },
    )


@app.get("/admin/audit-logs", response_class=HTMLResponse)
def audit_logs(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "audit.view"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        rows = load_audit_log_rows(cur)
        cur.close()
    return templates.TemplateResponse(
        "admin_audit_logs.html",
        {
            "request": request,
            "user": user,
            "rows": rows,
        },
    )


@app.get("/admin/error-logs", response_class=HTMLResponse)
def admin_error_logs(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, SYSTEM_SETTINGS_PERMISSION):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        rows = load_app_error_log_rows(cur)
        cur.close()
    return templates.TemplateResponse(
        "admin_error_logs.html",
        {
            "request": request,
            "user": user,
            "rows": rows,
        },
    )


@app.get("/admin/error-logs/{log_id}", response_class=HTMLResponse)
def admin_error_log_detail(request: Request, log_id: str) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, SYSTEM_SETTINGS_PERMISSION):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        row = load_app_error_log_detail(cur, log_id=log_id)
        cur.close()
    if not row:
        return templates.TemplateResponse(
            "admin_error_log_detail.html",
            {
                "request": request,
                "user": user,
                "row": None,
                "error": f"{log_id} のエラーログが見つかりません。",
            },
            status_code=404,
        )
    return templates.TemplateResponse(
        "admin_error_log_detail.html",
        {
            "request": request,
            "user": user,
            "row": row,
            "error": None,
        },
    )


@app.get("/admin/permissions", response_class=HTMLResponse)
def permission_settings(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "users.manage"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=True) as conn:
        cur = dict_cursor(conn)
        matrix = load_permission_matrix(cur)
        cur.close()
    return templates.TemplateResponse(
        "admin_permissions.html",
        {
            "request": request,
            "user": user,
            **matrix,
            "message": None,
            "error": None,
        },
    )


@app.post("/admin/permissions", response_class=HTMLResponse)
async def update_permission_settings(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "users.manage"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    form = await read_form(request)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=app_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            matrix = load_permission_matrix(cur)
            permissions = matrix["permissions"]
            for permission in permissions:
                permission_code = str(permission["permission_code"])
                upsert_role_permission(cur, role_code="ADMIN", permission_code=permission_code, is_allowed=True)
                for role_code in ("EDITOR", "VIEWER"):
                    is_allowed = form.get(f"allow__{role_code}__{permission_code}") == "1"
                    if permission_code in (
                        "users.manage",
                        "export_lists.view",
                        "export_lists.edit",
                        "xml_export.review",
                        "xml_export.official",
                        "hia_upload.perform",
                        "hia_upload_status.edit",
                    ):
                        is_allowed = False
                    upsert_role_permission(
                        cur,
                        role_code=role_code,
                        permission_code=permission_code,
                        is_allowed=is_allowed,
                    )
            conn.commit()
            matrix = load_permission_matrix(cur)
        except Exception:
            conn.rollback()
            raise
    return templates.TemplateResponse(
        "admin_permissions.html",
        {
            "request": request,
            "user": current_user(request) or user,
            **matrix,
            "message": "権限設定を更新しました。",
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
        work_permissions = load_work_permission_rows(cur, app_user_id=app_user_id)
        subscriber_pii_level = load_user_subscriber_pii_level(cur, app_user_id=app_user_id)
        allowed_ips = load_allowed_ip_rows(cur, app_user_id=app_user_id)
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
            "work_permissions": work_permissions,
            "subscriber_pii_level": subscriber_pii_level,
            "allowed_ips": allowed_ips,
            "form": admin_user_form_values(row),
            "allowed_ips_text": allowed_ips_text(allowed_ips),
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
    allowed_ips_input = form.get("allowed_ips", "")
    allowed_work_permissions = {
        (str(item["key"]), action_key)
        for item in WORK_PERMISSION_ITEMS
        for action_key in ("view", "edit")
        if form.get(f"work_permission__{item['key']}__{action_key}") == "1"
    }
    subscriber_pii_level = str(form.get("subscriber_pii_level") or "HIDDEN").strip().upper()
    if subscriber_pii_level not in {"FULL", "MASKED", "HIDDEN"}:
        subscriber_pii_level = "HIDDEN"
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
            work_permissions = load_work_permission_rows(cur, app_user_id=app_user_id)
            allowed_ips = load_allowed_ip_rows(cur, app_user_id=app_user_id)
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
                        "work_permissions": work_permissions,
                        "allowed_ips": allowed_ips,
                        "form": form_values,
                        "allowed_ips_text": allowed_ips_input,
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
                        "work_permissions": work_permissions,
                        "allowed_ips": allowed_ips,
                        "form": form_values,
                        "allowed_ips_text": allowed_ips_input,
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
                        "work_permissions": work_permissions,
                        "allowed_ips": allowed_ips,
                        "form": form_values,
                        "allowed_ips_text": allowed_ips_input,
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
                            "work_permissions": work_permissions,
                            "allowed_ips": allowed_ips,
                            "form": form_values,
                            "allowed_ips_text": allowed_ips_input,
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
            replace_user_work_permissions(
                cur,
                app_user_id=app_user_id,
                allowed_work_permissions=allowed_work_permissions,
                assigned_by_app_user_id=int(user["app_user_id"]),
            )
            replace_user_subscriber_pii_level(
                cur,
                app_user_id=app_user_id,
                pii_level=subscriber_pii_level,
                assigned_by_app_user_id=int(user["app_user_id"]),
            )
            replace_allowed_ips(cur, app_user_id=app_user_id, allowed_ips_text=allowed_ips_input)
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
