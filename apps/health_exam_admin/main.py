from __future__ import annotations

import os
import secrets
import string
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.examination.lookup import qname
from scripts.from_medical.script_lib.hia_xml_export_loader import (
    ExportSelectors,
    decide_candidate,
    fetch_candidates,
)
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


def load_xml_export_lists(cur: Any, *, limit: int = 30) -> list[dict[str, Any]]:
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
        FROM {qname(health_db())}.xml_export_lists xel
        LEFT JOIN {qname(health_db())}.xml_export_list_cases xelc
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


def load_xml_export_list_detail(cur: Any, *, xml_export_list_id: int) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT *
        FROM {qname(health_db())}.xml_export_lists
        WHERE xml_export_list_id = %s
        """,
        (xml_export_list_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def load_xml_export_list_cases(cur: Any, *, xml_export_list_id: int) -> list[dict[str, Any]]:
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
        FROM {qname(health_db())}.xml_export_list_cases xelc
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
        INSERT INTO {qname(health_db())}.xml_export_lists (
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
            INSERT INTO {qname(health_db())}.xml_export_list_cases (
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
            lists = load_xml_export_lists(cur)
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
            cases = load_xml_export_list_cases(cur, xml_export_list_id=xml_export_list_id)
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
                        "work_permissions": work_permissions,
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
                        "work_permissions": work_permissions,
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
                            "work_permissions": work_permissions,
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
            replace_user_work_permissions(
                cur,
                app_user_id=app_user_id,
                allowed_work_permissions=allowed_work_permissions,
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
