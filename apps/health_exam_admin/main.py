from __future__ import annotations

import os
import json
import re
import secrets
import string
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.etl.metrics import RunMetrics
from scripts.lib.etl.runs import finish_run, start_run
from scripts.lib.examination.lookup import qname
from scripts.from_medical.script_lib.hia_xml_export_loader import (
    ExportSelectors,
    decide_candidate,
    fetch_candidates,
)
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
    check_zip as check_hia_xml_zip_file,
    write_report as write_hia_xml_zip_check_report,
)
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
SESSION_COOKIE_NAME = "phr_app_session"
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
)

app = FastAPI(title="PHR Health Exam Admin")
app.mount("/static", StaticFiles(directory=APP_ROOT / "static"), name="static")
templates = Jinja2Templates(directory=APP_ROOT / "templates")


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


def load_permission_matrix(cur: Any) -> dict[str, Any]:
    roles = load_manageable_roles(cur)
    cur.execute(
        """
        SELECT app_permission_id, permission_code, permission_name, permission_group, description
        FROM app_permissions
        WHERE is_active = 1
        ORDER BY
          FIELD(permission_group, 'users', 'health_exam', 'xml_export', 'hia', 'audit'),
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


templates.env.globals["list_status_label"] = list_status_label
templates.env.globals["readiness_label"] = readiness_label


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


FUND_DELIVERY_CONFIG_PATH = REPO_ROOT / "scripts" / "hia" / "config" / "fund_delivery.yml"
HIA_EXPORT_DIR = REPO_ROOT / "data" / "hia_export"
HIA_XML_ZIP_CHECK_UPLOAD_DIR = REPO_ROOT / "data" / "hia_xml_zip_checks" / "uploads"


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
        path.resolve().relative_to(base_dir.resolve())
    except ValueError:
        return False
    return True


def xml_zip_check_allowed(user: dict[str, Any]) -> bool:
    return has_any_permission(user, ("hia_upload.perform", "hia_upload_status.edit", "users.manage"))


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
        row["findings"].append(
            {
                "severity": severity,
                "severity_label": severity_label,
                "status_class": status_class,
                "label": label,
                "namecode": finding.namecode,
                "item_display_name": finding.item_display_name,
                "namecode_source": namecode_source,
                "namecode_source_label": xml_zip_namecode_source_label(namecode_source),
                "namecode_source_class": xml_zip_namecode_source_class(namecode_source),
                "message": finding.message,
                "value_preview": finding.value_preview,
                "mhlw_byte_length": finding.mhlw_byte_length,
                "max_byte_length": finding.max_byte_length,
                "can_fix": can_fix,
                "fixability": "FIXABLE" if can_fix else "MANUAL",
                "fixability_label": "修正可" if can_fix else "手動確認",
                "fixability_class": "status-ok" if can_fix else "status-neutral",
                "fix_note": getattr(finding, "fix_note", None),
            }
        )
    rows = sorted(grouped.values(), key=lambda item: (-int(item["error_count"]), -int(item["warning_count"]), item["xml_inner_path"]))
    for row in rows:
        row["label_summary"] = " / ".join(
            f"{label} {count}件"
            for label, count in sorted(row["labels"].items(), key=lambda item: (-int(item[1]), str(item[0])))
        )
    return rows


def build_xml_zip_check_result(
    *,
    upload_path: Path,
    original_filename: str,
    fix: bool,
) -> dict[str, Any]:
    summary, findings = check_hia_xml_zip_file(
        upload_path,
        xsd_dir=XML_ZIP_CHECK_XSD_DIR,
        fix=fix,
        fixed_output_dir=XML_ZIP_CHECK_REPORT_DIR / "fixed",
    )
    report_csv_path = write_hia_xml_zip_check_report(findings, XML_ZIP_CHECK_REPORT_DIR)
    display_name_options = sorted(
        {
            str(item.item_display_name).strip()
            for item in findings
            if item.severity in {"ERROR", "WARNING"} and str(item.item_display_name or "").strip()
        }
    )
    return {
        "original_filename": original_filename,
        "upload_path": str(upload_path),
        "report_csv_path": str(report_csv_path),
        "fixed_zip_path": summary.fixed_zip_path,
        "fix": fix,
        "zip_files_seen": summary.zip_files_seen,
        "xml_files_seen": summary.xml_files_seen,
        "findings": len(findings),
        "errors": sum(1 for item in findings if item.severity == "ERROR"),
        "warnings": sum(1 for item in findings if item.severity == "WARNING"),
        "fixed": sum(1 for item in findings if item.fixed),
        "display_groups": serialize_xml_zip_display_groups(findings),
        "display_name_options": display_name_options,
    }


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


def load_facility_admin_rows(cur: Any, *, limit: int = 300) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          exam_facility_id,
          exam_facility_code,
          exam_facility_name,
          exam_facility_display_name,
          exam_facility_type,
          medical_institution_code,
          reservation_system_medical_institution_code,
          postal_code,
          address,
          phone_number,
          data_source_name,
          note,
          is_active,
          updated_at
        FROM {qname(master_db())}.exam_facilities
        ORDER BY is_active DESC, exam_facility_code IS NULL, exam_facility_code, exam_facility_name
        LIMIT %s
        """,
        (limit,),
    )
    return [dict(row) for row in cur.fetchall()]


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
          ef.exam_facility_code,
          ef.exam_facility_name,
          ef.exam_facility_display_name,
          mfa.manual_judgement,
          mfa.note,
          mfa.is_active,
          mfa.updated_at
        FROM {qname(master_db())}.medical_folder_aliases mfa
        LEFT JOIN {qname(master_db())}.exam_facilities ef
          ON ef.exam_facility_id = mfa.exam_facility_id
        LEFT JOIN {qname(dev_db())}.event ev
          ON ev.event_id = mfa.event_id
        ORDER BY mfa.is_active DESC, mfa.event_id DESC, mfa.src_folder_raw
        LIMIT %s
        """,
        (limit,),
    )
    return [dict(row) for row in cur.fetchall()]


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
    if text.isdigit():
        return int(text)
    cur.execute(
        f"""
        SELECT exam_facility_id
        FROM {qname(master_db())}.exam_facilities
        WHERE exam_facility_code = %s
        LIMIT 1
        """,
        (text,),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError("指定された健診機関ID/コードが見つかりません。")
    return int(row["exam_facility_id"])


def normalize_folder_alias_form(cur: Any, form: dict[str, str]) -> dict[str, Any]:
    event_id_text = (form.get("event_id") or "").strip()
    src_folder_raw = _form_text(form, "src_folder_raw")
    dst_folder_norm = _form_text(form, "dst_folder_norm") or src_folder_raw
    if not event_id_text:
        raise ValueError("イベントは必須です。")
    if not src_folder_raw:
        raise ValueError("フォルダ名は必須です。")
    return {
        "event_id": int(event_id_text),
        "src_folder_raw": src_folder_raw,
        "dst_folder_norm": dst_folder_norm,
        "exam_facility_id": resolve_exam_facility_selector(cur, form.get("exam_facility_selector")),
        "manual_judgement": 1 if form.get("manual_judgement") == "1" else 0,
        "note": _form_text(form, "note"),
        "is_active": 1 if form.get("is_active") == "1" else 0,
    }


def load_file_receipt_rows(cur: Any, *, filters: dict[str, str], limit: int = 200) -> list[dict[str, Any]]:
    where_parts: list[str] = []
    params: list[Any] = []
    event_id = filters.get("event_id", "").strip()
    file_type = filters.get("file_type", "").strip()
    status = filters.get("status", "").strip()
    query = filters.get("q", "").strip()
    if event_id:
        where_parts.append("event_id = %s")
        params.append(event_id)
    if file_type:
        where_parts.append("file_type = %s")
        params.append(file_type)
    if status:
        where_parts.append("status = %s")
        params.append(status)
    if query:
        like = f"%{query}%"
        where_parts.append(
            """
            (
              file_name LIKE %s
              OR relative_path LIKE %s
              OR facility_name LIKE %s
              OR facility_code LIKE %s
            )
            """
        )
        params.extend([like, like, like, like])
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    cur.execute(
        f"""
        SELECT
          id,
          event_id,
          file_type,
          file_name,
          relative_path,
          file_sha256,
          facility_code,
          facility_name,
          exam_facility_id,
          matched_csv_format_version_id,
          status,
          summary_message,
          etl_run_id,
          first_seen_at,
          last_seen_at,
          processed_at,
          updated_at
        FROM {qname(health_db())}.file_receipts
        {where_sql}
        ORDER BY updated_at DESC, id DESC
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
    check_status = filters.get("check_status", "").strip()
    query = filters.get("q", "").strip()
    if event_id:
        where_parts.append("event_id = %s")
        params.append(event_id)
    if source_type:
        where_parts.append("source_type = %s")
        params.append(source_type)
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
              OR name_kana_raw LIKE %s
              OR facility_name LIKE %s
              OR xml_file_name LIKE %s
            )
            """
        )
        params.extend([like, like, like, like, like, like])
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
          facility_code,
          facility_name,
          exam_date,
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
          xelc.added_by,
          xelc.added_at,
          xelc.exported_at,
          eec.hia_subscriber_id,
          eec.person_id_custom,
          eec.insured_card_symbol,
          eec.insured_card_number,
          eec.name_kana,
          eec.birth_date,
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
        ORDER BY ef.exam_facility_code, eec.exam_date, eec.name_kana, xelc.xml_export_list_case_id
        """,
        (xml_export_list_id,),
    )
    return [dict(row) for row in cur.fetchall()]


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
    candidates = fetch_candidates(cur, selectors=selectors, health_db=health_db(), master_db=master_db())
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
        "q": request.query_params.get("q", ""),
        "limit": request.query_params.get("limit", "200"),
    }
    limit = parse_positive_int(filters["limit"], default=200, maximum=1000)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            event_options = load_event_options(cur)
            rows = load_file_receipt_rows(cur, filters=filters, limit=limit)
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
        },
    )


@app.post("/hia/xml-zip-check", response_class=HTMLResponse)
async def run_hia_xml_zip_check(
    request: Request,
    zip_file: UploadFile = File(...),
    fix: str | None = Form(None),
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
            fix=fix == "1",
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
        },
    )


@app.post("/hia/xml-zip-check/delete-upload")
async def delete_hia_xml_zip_upload(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not xml_zip_check_allowed(user):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)

    form = await request.form()
    upload_path_text = str(form.get("upload_path") or "").strip()
    upload_path = Path(upload_path_text)
    if not upload_path_text or not is_path_under(upload_path, HIA_XML_ZIP_CHECK_UPLOAD_DIR):
        return RedirectResponse(
            f"/hia/xml-zip-check?error={quote('削除できるのはアップロード済みZIPだけです。')}",
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

    upload_path.unlink()
    try:
        parent = upload_path.parent
        if parent != HIA_XML_ZIP_CHECK_UPLOAD_DIR and is_path_under(parent, HIA_XML_ZIP_CHECK_UPLOAD_DIR):
            parent.rmdir()
    except OSError:
        pass

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
    if not has_permission(user, "users.manage"):
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
    if not has_permission(user, "users.manage"):
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
    if not has_permission(user, "users.manage"):
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


@app.get("/admin/facilities", response_class=HTMLResponse)
def admin_facilities(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "users.manage"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            event_options = load_event_options(cur)
            facility_rows = load_facility_admin_rows(cur)
            alias_rows = load_folder_alias_admin_rows(cur)
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
            "facility_rows": facility_rows,
            "alias_rows": alias_rows,
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
        },
    )


@app.post("/admin/facilities", response_class=HTMLResponse)
async def create_admin_facility(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "users.manage"):
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
            return RedirectResponse(f"/admin/facilities?error={quote(str(exc))}", status_code=303)
        except Exception:
            conn.rollback()
            raise
    return RedirectResponse("/admin/facilities?message=健診機関を作成しました。", status_code=303)


@app.post("/admin/facilities/{exam_facility_id}", response_class=HTMLResponse)
async def update_admin_facility(request: Request, exam_facility_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "users.manage"):
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
            return RedirectResponse(f"/admin/facilities?error={quote(str(exc))}", status_code=303)
        except Exception:
            conn.rollback()
            raise
    return RedirectResponse("/admin/facilities?message=健診機関を更新しました。", status_code=303)


@app.post("/admin/folder-aliases", response_class=HTMLResponse)
async def create_admin_folder_alias(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "users.manage"):
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
                  manual_judgement,
                  note,
                  is_active
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    values["event_id"],
                    values["src_folder_raw"],
                    values["dst_folder_norm"],
                    values["exam_facility_id"],
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
            return RedirectResponse(f"/admin/facilities?error={quote(str(exc))}", status_code=303)
        except Exception:
            conn.rollback()
            raise
    return RedirectResponse("/admin/facilities?message=フォルダaliasを作成しました。", status_code=303)


@app.post("/admin/folder-aliases/{alias_id}", response_class=HTMLResponse)
async def update_admin_folder_alias(request: Request, alias_id: int) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_permission(user, "users.manage"):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    form = await read_form(request)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            values = normalize_folder_alias_form(cur, form)
            cur.execute(
                f"""
                UPDATE {qname(master_db())}.medical_folder_aliases
                   SET event_id = %s,
                       src_folder_raw = %s,
                       dst_folder_norm = %s,
                       exam_facility_id = %s,
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
            return RedirectResponse(f"/admin/facilities?error={quote(str(exc))}", status_code=303)
        except Exception:
            conn.rollback()
            raise
    return RedirectResponse("/admin/facilities?message=フォルダaliasを更新しました。", status_code=303)


@app.get("/exam-ledgers", response_class=HTMLResponse)
def exam_ledgers(request: Request) -> Response:
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not has_any_permission(user, ("export_lists.view", "export_lists.edit", "users.manage")):
        return templates.TemplateResponse("forbidden.html", {"request": request, "user": user}, status_code=403)
    filters = {
        "event_id": request.query_params.get("event_id", "2"),
        "source_type": request.query_params.get("source_type", ""),
        "check_status": request.query_params.get("check_status", ""),
        "q": request.query_params.get("q", ""),
        "limit": request.query_params.get("limit", "200"),
    }
    limit = parse_positive_int(filters["limit"], default=200, maximum=1000)
    params = load_mysql_base_params(db_prefix())
    with connect_ctx(params, database=health_db(), autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            event_options = load_event_options(cur)
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
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
            "can_edit": has_permission(user, "export_lists.edit"),
            "can_export": has_permission(user, "xml_export.official"),
        },
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
