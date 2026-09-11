from __future__ import annotations

import csv
import hashlib
import io
import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from scripts.lib.identity.field.insurance_number import normalize_insurance_number
from scripts.lib.identity.field.insurance_symbol import normalize_insurance_symbol
from scripts.lib.identity.field.insurer_number import normalize_insurer_number
from scripts.lib.identity.field.name_kana import normalize_name_kana_full


BASE_HEADERS = (
    "name", "hospital_id", "id", "course_id", "course_name", "reservation_date",
    "start_time", "end_time", "reservation_status", "cancel_date", "terminal_type",
    "tax_included_price", "second_date", "third_date", "tel_timezone",
    "insurance_assoc_id", "insurance_assoc", "created_at", "updated_at", "applicant_id",
    "hia_member_id", "applicant_fullname", "applicant_fullname_kana", "applicant_tel",
    "applicant_sex", "applicant_birthday", "postcode", "fulladdress", "insurance_char",
    "insurance_num", "canceller", "コースHIAコード", "HIAコース名", "コース総額", "コース補助額",
)
OPTION_HEADERS = ("OP HIAコード", "OP 名称", "OP 価格")
OPTION_SLOT_COUNT = 8
RECORD_COLUMNS = (
    "event_id", "reservation_hospital_id", "reservation_hospital_name", "exam_facility_id",
    "course_id", "course_name", "reservation_date", "start_time", "end_time",
    "reservation_status_raw", "cancelled_at", "terminal_type_raw", "tax_included_price",
    "second_date", "third_date", "tel_timezone_raw", "tel_timezone_hour",
    "insurance_assoc_id_raw", "insurer_number_match", "insurance_assoc_name",
    "source_created_at", "source_updated_at", "applicant_id", "hia_member_id",
    "applicant_fullname", "applicant_fullname_kana", "applicant_fullname_kana_match",
    "applicant_tel", "applicant_sex_raw", "applicant_birthday", "postcode", "fulladdress",
    "insurance_symbol", "insurance_number", "insurance_symbol_match", "insurance_number_match",
    "canceller_raw", "hia_course_code", "hia_course_name", "course_total_price",
    "course_subsidy_amount",
)


def _text(value: Any) -> str | None:
    value = str(value or "").strip()
    return value or None


def _uint(value: Any, field: str, *, maximum: int = 4_294_967_295) -> int | None:
    text = _text(value)
    if text is None:
        return None
    if not text.isdigit() or int(text) > maximum:
        raise ValueError(f"{field}: 0から{maximum}の整数ではありません")
    return int(text)


def _date(value: Any, field: str) -> str | None:
    text = _text(value)
    if text is None:
        return None
    for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    raise ValueError(f"{field}: 日付形式が不正です")


def _datetime(value: Any, field: str) -> str | None:
    text = _text(value)
    if text is None:
        return None
    normalized = text.replace("T", " ")
    for fmt in ("%Y/%m/%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d"):
        try:
            return datetime.strptime(normalized, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    raise ValueError(f"{field}: 日時形式が不正です")


def _time(value: Any, field: str) -> str | None:
    text = _text(value)
    if text is None:
        return None
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).strftime("%H:%M:%S")
        except ValueError:
            pass
    raise ValueError(f"{field}: 時刻形式が不正です")


def _money(value: Any, field: str) -> str | None:
    text = _text(value)
    if text is None:
        return None
    try:
        amount = Decimal(text.replace(",", ""))
    except InvalidOperation as exc:
        raise ValueError(f"{field}: 金額形式が不正です") from exc
    if amount != amount.to_integral_value() or amount < 0:
        raise ValueError(f"{field}: 0以上の整数金額ではありません")
    return str(amount.quantize(Decimal("1")))


def decode_csv(raw: bytes) -> tuple[str, str]:
    for encoding in ("utf-8-sig", "cp932"):
        try:
            return raw.decode(encoding), encoding
        except UnicodeDecodeError:
            pass
    raise ValueError("文字コードを判定できません。UTF-8またはShift_JISのCSVを指定してください。")


def validate_headers(headers: list[str]) -> int:
    base_count = len(BASE_HEADERS)
    if headers[:base_count] != list(BASE_HEADERS):
        raise ValueError("CSVヘッダーの固定35列が一致しません。")
    option_headers = headers[base_count:]
    if len(option_headers) % len(OPTION_HEADERS) != 0:
        raise ValueError(
            f"CSVのオプションヘッダーが3列1組ではありません（実際{len(option_headers)}列）。"
        )
    option_slot_count = len(option_headers) // len(OPTION_HEADERS)
    if option_slot_count > OPTION_SLOT_COUNT:
        raise ValueError(
            f"CSVのオプションは最大{OPTION_SLOT_COUNT}組です（実際{option_slot_count}組）。"
        )
    if option_headers != list(OPTION_HEADERS) * option_slot_count:
        raise ValueError("CSVのオプションヘッダーが一致しません。")
    return option_slot_count


def parse_csv(raw: bytes, *, event_id: int) -> dict[str, Any]:
    text, encoding = decode_csv(raw)
    rows = list(csv.reader(io.StringIO(text, newline="")))
    if not rows:
        raise ValueError("CSVファイルが空です。")
    header_option_slot_count = validate_headers(rows[0])

    parsed: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    for line_no, values in enumerate(rows[1:], 2):
        try:
            if len(values) < len(BASE_HEADERS):
                raise ValueError(
                    f"固定項目の列数が不足しています（必要{len(BASE_HEADERS)}、実際{len(values)}）"
                )
            option_values = values[len(BASE_HEADERS):]
            if len(option_values) % len(OPTION_HEADERS) != 0:
                raise ValueError(
                    f"オプション列が3列1組ではありません（実際{len(option_values)}列）"
                )
            row_option_slot_count = len(option_values) // len(OPTION_HEADERS)
            if row_option_slot_count > header_option_slot_count:
                raise ValueError(
                    "データ行のオプション数がヘッダーのオプション数を超えています"
                    f"（ヘッダー{header_option_slot_count}組、データ{row_option_slot_count}組）"
                )
            base = dict(zip(BASE_HEADERS, values[: len(BASE_HEADERS)]))
            reservation_id = _uint(base["id"], "id", maximum=18_446_744_073_709_551_615)
            if reservation_id is None:
                raise ValueError("idは必須です")
            if reservation_id in seen_ids:
                raise ValueError(f"同じ予約IDがCSV内に複数あります: {reservation_id}")
            seen_ids.add(reservation_id)
            insurer = normalize_insurer_number(base["insurance_assoc_id"])
            kana = normalize_name_kana_full(base["applicant_fullname_kana"])
            symbol = normalize_insurance_symbol(base["insurance_char"])
            number = normalize_insurance_number(base["insurance_num"])
            tel_raw = _text(base["tel_timezone"])
            tel_hour = None
            if tel_raw is not None:
                if not tel_raw.isdigit() or not 0 <= int(tel_raw) <= 23:
                    raise ValueError("tel_timezone: 0から23または空欄ではありません")
                tel_hour = int(tel_raw)
            record = {
                "reservation_id": reservation_id,
                "event_id": event_id,
                "reservation_hospital_id": _uint(base["hospital_id"], "hospital_id"),
                "reservation_hospital_name": _text(base["name"]),
                "exam_facility_id": None,
                "course_id": _uint(base["course_id"], "course_id"),
                "course_name": _text(base["course_name"]),
                "reservation_date": _date(base["reservation_date"], "reservation_date"),
                "start_time": _time(base["start_time"], "start_time"),
                "end_time": _time(base["end_time"], "end_time"),
                "reservation_status_raw": _text(base["reservation_status"]),
                "cancelled_at": _datetime(base["cancel_date"], "cancel_date"),
                "terminal_type_raw": _text(base["terminal_type"]),
                "tax_included_price": _money(base["tax_included_price"], "tax_included_price"),
                "second_date": _datetime(base["second_date"], "second_date"),
                "third_date": _datetime(base["third_date"], "third_date"),
                "tel_timezone_raw": tel_raw,
                "tel_timezone_hour": tel_hour,
                "insurance_assoc_id_raw": _text(base["insurance_assoc_id"]),
                "insurer_number_match": insurer.get("match") if insurer.get("ok") else None,
                "insurance_assoc_name": _text(base["insurance_assoc"]),
                "source_created_at": _datetime(base["created_at"], "created_at"),
                "source_updated_at": _datetime(base["updated_at"], "updated_at"),
                "applicant_id": _uint(base["applicant_id"], "applicant_id"),
                "hia_member_id": _text(base["hia_member_id"]),
                "applicant_fullname": _text(base["applicant_fullname"]),
                "applicant_fullname_kana": _text(base["applicant_fullname_kana"]),
                "applicant_fullname_kana_match": kana.get("match") if kana.get("ok") else None,
                "applicant_tel": _text(base["applicant_tel"]),
                "applicant_sex_raw": _text(base["applicant_sex"]),
                "applicant_birthday": _date(base["applicant_birthday"], "applicant_birthday"),
                "postcode": _text(base["postcode"]),
                "fulladdress": _text(base["fulladdress"]),
                "insurance_symbol": _text(base["insurance_char"]),
                "insurance_number": _text(base["insurance_num"]),
                "insurance_symbol_match": symbol.get("match") if symbol.get("ok") else None,
                "insurance_number_match": number.get("match") if number.get("ok") else None,
                "canceller_raw": _text(base["canceller"]),
                "hia_course_code": _text(base["コースHIAコード"]),
                "hia_course_name": _text(base["HIAコース名"]),
                "course_total_price": _money(base["コース総額"], "コース総額"),
                "course_subsidy_amount": _money(base["コース補助額"], "コース補助額"),
            }
            if record["reservation_hospital_id"] is None:
                raise ValueError("hospital_idは必須です")
            options = []
            for slot in range(OPTION_SLOT_COUNT):
                start = slot * len(OPTION_HEADERS)
                option_group = option_values[start : start + len(OPTION_HEADERS)]
                code, name, price = option_group if len(option_group) == len(OPTION_HEADERS) else ("", "", "")
                options.append({
                    "option_slot_no": slot + 1,
                    "option_hia_code": _text(code),
                    "option_name": _text(name),
                    "option_price": _money(price, f"OP価格{slot + 1}"),
                })
            canonical = json.dumps({"record": record, "options": options}, ensure_ascii=False, sort_keys=True)
            record["row_sha256"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
            parsed.append({"line_no": line_no, "record": record, "options": options})
        except ValueError as exc:
            errors.append({"line_no": line_no, "message": str(exc)})
    return {
        "encoding": encoding,
        "header_option_slot_count": header_option_slot_count,
        "rows": parsed,
        "errors": errors,
        "row_count": len(rows) - 1,
    }


def build_plan(cur: Any, parsed: dict[str, Any]) -> dict[str, Any]:
    ids = [row["record"]["reservation_id"] for row in parsed["rows"]]
    existing: dict[int, dict[str, Any]] = {}
    for start in range(0, len(ids), 500):
        chunk = ids[start : start + 500]
        placeholders = ",".join(["%s"] * len(chunk))
        cur.execute(f"SELECT * FROM reservation_site_records WHERE reservation_id IN ({placeholders})", tuple(chunk))
        existing.update({int(row["reservation_id"]): dict(row) for row in cur.fetchall()})
    hospital_ids = sorted({row["record"]["reservation_hospital_id"] for row in parsed["rows"]})
    mappings: dict[int, int] = {}
    if hospital_ids:
        placeholders = ",".join(["%s"] * len(hospital_ids))
        cur.execute(
            f"SELECT reservation_hospital_id, exam_facility_id FROM reservation_site_facility_mappings "
            f"WHERE is_active=1 AND reservation_hospital_id IN ({placeholders})",
            tuple(hospital_ids),
        )
        mappings = {int(row["reservation_hospital_id"]): int(row["exam_facility_id"]) for row in cur.fetchall()}
    counts = {"insert": 0, "update": 0, "unchanged": 0, "stale": 0, "error": len(parsed["errors"])}
    rows = []
    for item in parsed["rows"]:
        record = item["record"]
        record["exam_facility_id"] = mappings.get(record["reservation_hospital_id"])
        current = existing.get(record["reservation_id"])
        if current is None:
            action = "insert"
        elif int(current["event_id"]) != int(record["event_id"]):
            action = "error"
            parsed["errors"].append({"line_no": item["line_no"], "message": "予約IDが別eventに登録済みです"})
        elif current.get("source_updated_at") and record.get("source_updated_at") and str(current["source_updated_at"]) > record["source_updated_at"]:
            action = "stale"
        elif (
            str(current.get("row_sha256") or "") == record["row_sha256"]
            and current.get("exam_facility_id") == record.get("exam_facility_id")
        ):
            action = "unchanged"
        else:
            action = "update"
        counts[action] += 1
        rows.append({**item, "action": action})
    counts["error"] = len(parsed["errors"])
    unmapped = sorted({
        (row["record"]["reservation_hospital_id"], row["record"]["reservation_hospital_name"])
        for row in rows if row["record"].get("exam_facility_id") is None
    })
    return {"version": 1, "encoding": parsed["encoding"], "row_count": parsed["row_count"], "rows": rows, "errors": parsed["errors"], "counts": counts, "unmapped_facilities": unmapped}


def apply_plan(cur: Any, plan: dict[str, Any], *, run_id: int) -> dict[str, int]:
    counts = {"inserted": 0, "updated": 0, "unchanged": 0, "stale": 0}
    if plan.get("errors"):
        raise ValueError("エラーを含む事前確認結果は反映できません。")
    for item in plan["rows"]:
        record = item["record"]
        action = item["action"]
        if action in {"stale", "error"}:
            counts["stale"] += action == "stale"
            continue
        if action == "insert":
            columns = ("reservation_id",) + RECORD_COLUMNS + ("row_sha256", "first_seen_run_id", "last_seen_run_id")
            values = [record.get(column) for column in ("reservation_id",) + RECORD_COLUMNS + ("row_sha256",)] + [run_id, run_id]
            cur.execute(
                f"INSERT INTO reservation_site_records ({','.join(f'`{c}`' for c in columns)}) VALUES ({','.join(['%s'] * len(columns))})",
                tuple(values),
            )
            record_pk = int(cur.lastrowid)
            counts["inserted"] += 1
        else:
            cur.execute("SELECT * FROM reservation_site_records WHERE reservation_id=%s FOR UPDATE", (record["reservation_id"],))
            current = dict(cur.fetchone())
            record_pk = int(current["reservation_site_record_id"])
            if action == "update":
                for column in RECORD_COLUMNS + ("row_sha256",):
                    old, new = current.get(column), record.get(column)
                    if str(old or "") != str(new or ""):
                        cur.execute(
                            "INSERT INTO reservation_site_record_history (reservation_site_record_id,run_id,column_name,old_value,new_value) VALUES (%s,%s,%s,%s,%s)",
                            (record_pk, run_id, column, old, new),
                        )
                assignments = ",".join(f"`{column}`=%s" for column in RECORD_COLUMNS + ("row_sha256",))
                cur.execute(
                    f"UPDATE reservation_site_records SET {assignments},last_seen_run_id=%s WHERE reservation_site_record_id=%s",
                    tuple(record.get(column) for column in RECORD_COLUMNS + ("row_sha256",)) + (run_id, record_pk),
                )
                counts["updated"] += 1
            else:
                cur.execute("UPDATE reservation_site_records SET last_seen_run_id=%s WHERE reservation_site_record_id=%s", (run_id, record_pk))
                counts["unchanged"] += 1
        cur.execute("SELECT * FROM reservation_site_record_options WHERE reservation_site_record_id=%s", (record_pk,))
        existing_options = {int(row["option_slot_no"]): dict(row) for row in cur.fetchall()}
        for option in item["options"]:
            slot = option["option_slot_no"]
            active = any(option.get(key) is not None for key in ("option_hia_code", "option_name", "option_price"))
            old = existing_options.get(slot)
            if old is None and active:
                cur.execute(
                    "INSERT INTO reservation_site_record_options (reservation_site_record_id,option_slot_no,option_hia_code,option_name,option_price,is_active,first_seen_run_id,last_seen_run_id) VALUES (%s,%s,%s,%s,%s,1,%s,%s)",
                    (record_pk, slot, option["option_hia_code"], option["option_name"], option["option_price"], run_id, run_id),
                )
            elif old is not None:
                new_values = {**option, "is_active": 1 if active else 0}
                for column in ("option_hia_code", "option_name", "option_price", "is_active"):
                    if str(old.get(column) or "") != str(new_values.get(column) or ""):
                        cur.execute(
                            "INSERT INTO reservation_site_record_history (reservation_site_record_id,run_id,change_scope,option_slot_no,column_name,old_value,new_value) VALUES (%s,%s,'OPTION',%s,%s,%s,%s)",
                            (record_pk, run_id, slot, column, old.get(column), new_values.get(column)),
                        )
                cur.execute(
                    "UPDATE reservation_site_record_options SET option_hia_code=%s,option_name=%s,option_price=%s,is_active=%s,last_seen_run_id=%s,inactive_run_id=%s,inactive_at=CASE WHEN %s=0 THEN CURRENT_TIMESTAMP(3) ELSE NULL END WHERE reservation_site_record_option_id=%s",
                    (option["option_hia_code"], option["option_name"], option["option_price"], new_values["is_active"], run_id, None if active else run_id, new_values["is_active"], old["reservation_site_record_option_id"]),
                )
    return counts
