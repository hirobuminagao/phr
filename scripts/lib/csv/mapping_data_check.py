"""Stream a CSV through saved exam-result mapping rules without persistence."""

from __future__ import annotations

import codecs
import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from scripts.lib.csv.csv_loader import CsvHeaderColumn, CsvHeaderSet, CsvLoadResult, build_header_columns
from scripts.lib.csv.exam_result_mapping_extractor import ExtractedCsvRuleValue, extract_row_values
from scripts.lib.db.lookup.csv_exam_result_mapping import CsvMappingRule
from scripts.lib.examination.value_normalizer import NormalizedExamValue


MAX_DATA_ROWS = 10_000
MAX_SAMPLES_PER_TARGET = 100
MAX_ERROR_SAMPLES = 1_000


@dataclass(frozen=True)
class CsvStreamHeader:
    encoding: str
    header_set: CsvHeaderSet
    data_start_row_no: int

    def as_load_result(self, path: str, *, delimiter: str, quote_char: str) -> CsvLoadResult:
        return CsvLoadResult(
            path=path,
            encoding=self.encoding,
            delimiter=delimiter,
            quote_char=quote_char,
            header_set=self.header_set,
            rows=[],
            data_start_row_no=self.data_start_row_no,
        )


def detect_csv_encoding(path: str | Path) -> str:
    with open(path, "rb") as source:
        raw = source.read(64 * 1024)
    if raw.startswith(codecs.BOM_UTF8):
        return "utf-8-sig"
    try:
        raw.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        return "cp932"


def _clean_row(row: Iterable[Any]) -> list[str]:
    cleaned = [str(value or "").strip() for value in row]
    if cleaned:
        cleaned[0] = cleaned[0].lstrip("\ufeff")
    return cleaned


def _header_hash(header_rows: list[list[str]], active_header_row_no: int | None) -> str:
    import hashlib

    columns = build_header_columns(header_rows, active_header_row_no)
    payload = {
        "active_header_row_no": active_header_row_no,
        "header_rows": header_rows,
        "normalized_columns": [
            {
                "column_no": column.column_no,
                "context": column.context,
                "header_name": column.header_name,
                "occurrence": column.occurrence,
            }
            for column in columns
        ],
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def read_csv_stream_header(
    path: str | Path,
    *,
    data_start_row_no: int,
    delimiter: str = ",",
    quote_char: str = '"',
    active_header_row_no: int | None = None,
) -> CsvStreamHeader:
    encoding = detect_csv_encoding(path)
    header_count = max(data_start_row_no - 1, 0)
    header_rows: list[list[str]] = []
    with open(path, "r", encoding=encoding, newline="") as source:
        reader = csv.reader(source, delimiter=delimiter, quotechar=quote_char)
        for index, row in enumerate(reader):
            if index >= header_count:
                break
            header_rows.append(_clean_row(row))
    columns = build_header_columns(header_rows, active_header_row_no)
    header_set = CsvHeaderSet(
        header_rows=header_rows,
        active_header_row_no=active_header_row_no,
        normalized_columns=columns,
        header_sha256=_header_hash(header_rows, active_header_row_no),
    )
    return CsvStreamHeader(encoding=encoding, header_set=header_set, data_start_row_no=data_start_row_no)


def iter_csv_data_rows(
    path: str | Path,
    *,
    encoding: str,
    data_start_row_no: int,
    delimiter: str = ",",
    quote_char: str = '"',
) -> Iterable[tuple[int, list[str]]]:
    with open(path, "r", encoding=encoding, newline="") as source:
        reader = csv.reader(source, delimiter=delimiter, quotechar=quote_char)
        for line_no, row in enumerate(reader, start=1):
            if line_no < data_start_row_no:
                continue
            yield line_no, _clean_row(row)


def _column_dict(column: CsvHeaderColumn) -> dict[str, Any]:
    return {
        "column_no": column.column_no,
        "context": column.context or "",
        "name": column.header_name or "",
        "occurrence": column.occurrence,
    }


def _snapshot_columns(snapshot: Any) -> list[dict[str, Any]]:
    if isinstance(snapshot, str):
        try:
            snapshot = json.loads(snapshot)
        except json.JSONDecodeError:
            return []
    if not isinstance(snapshot, Mapping):
        return []
    rows = snapshot.get("normalized_columns")
    if not isinstance(rows, list):
        return []
    result = []
    for index, row in enumerate(rows, start=1):
        if not isinstance(row, Mapping):
            continue
        result.append(
            {
                "column_no": int(row.get("column_no") or index),
                "context": str(row.get("context") or ""),
                "name": str(row.get("name") or row.get("header_name") or ""),
                "occurrence": int(row.get("occurrence") or 1),
            }
        )
    return result


def compare_csv_headers(header: CsvStreamHeader, registered_snapshot: Any) -> dict[str, Any]:
    uploaded = [_column_dict(column) for column in header.header_set.normalized_columns]
    registered = _snapshot_columns(registered_snapshot)
    uploaded_groups: dict[tuple[str, str, int], list[dict[str, Any]]] = defaultdict(list)
    registered_groups: dict[tuple[str, str, int], list[dict[str, Any]]] = defaultdict(list)
    for row in uploaded:
        uploaded_groups[(row["context"], row["name"], row["occurrence"])].append(row)
    for row in registered:
        registered_groups[(row["context"], row["name"], row["occurrence"])].append(row)
    rows: list[dict[str, Any]] = []
    consumed_upload_columns: set[int] = set()
    for key, base_rows in registered_groups.items():
        base = base_rows[0]
        actual_rows = uploaded_groups.get(key, [])
        actual = actual_rows[0] if len(actual_rows) == 1 else None
        if len(base_rows) > 1 or len(actual_rows) > 1:
            rows.append({"status": "AMBIGUOUS", "registered": base, "uploaded": actual_rows[0] if actual_rows else None})
            consumed_upload_columns.update(int(item["column_no"]) for item in actual_rows)
            continue
        if actual:
            status = "MATCHED" if actual["column_no"] == base["column_no"] else "ORDER_ONLY"
            consumed_upload_columns.add(int(actual["column_no"]))
        else:
            same_name = [row for row in uploaded if row["name"] == base["name"] and row["occurrence"] == base["occurrence"]]
            status = "CONTEXT_CHANGED" if len(same_name) == 1 else "MISSING"
            actual = same_name[0] if len(same_name) == 1 else None
            if actual:
                consumed_upload_columns.add(int(actual["column_no"]))
        rows.append({"status": status, "registered": base, "uploaded": actual})
    for actual in uploaded:
        if int(actual["column_no"]) not in consumed_upload_columns:
            rows.append({"status": "ADDED", "registered": None, "uploaded": actual})
    counts = Counter(row["status"] for row in rows)
    return {
        "is_exact": bool(registered) and counts.get("MATCHED", 0) == len(registered) and len(uploaded) == len(registered),
        "registered_count": len(registered),
        "uploaded_count": len(uploaded),
        "counts": dict(counts),
        "rows": rows,
    }


def _target_key(rule: CsvMappingRule) -> str:
    target = rule.target_namecode if rule.target_kind == "EXAM_ITEM_VALUE" else rule.target_field
    return f"{rule.target_kind}:{target or rule.rule_key}"


def _rule_description(rule: CsvMappingRule) -> dict[str, Any]:
    return {
        "rule_id": rule.rule_id,
        "rule_key": rule.rule_key,
        "selection_mode": rule.selection_mode,
        "value_source_type": rule.value_source_type,
        "fixed_value": rule.fixed_value,
        "priority": rule.priority,
        "sources": [
            {
                "column_no": condition.resolved_column_no or condition.column_no,
                "context": condition.header_context,
                "header_name": condition.header_name,
                "source_role": condition.source_role,
                "operator": condition.operator,
                "expected_value": condition.expected_value,
            }
            for condition in rule.conditions
        ],
    }


def _new_target(rule: CsvMappingRule) -> dict[str, Any]:
    return {
        "key": _target_key(rule),
        "target_kind": rule.target_kind,
        "target_namecode": rule.target_namecode,
        "target_field": rule.target_field,
        "data_type": rule.raw_value_type,
        "rules": [],
        "processed_count": 0,
        "value_count": 0,
        "blank_count": 0,
        "success_count": 0,
        "failure_count": 0,
        "no_condition_count": 0,
        "conflict_count": 0,
        "code_counts": Counter(),
        "code_raw_counts": defaultdict(Counter),
        "combination_counts": Counter(),
        "samples": [],
        "error_sample_count": 0,
        "omitted_error_count": 0,
    }


def _percent(count: int, total: int) -> float:
    return round((count * 100.0 / total), 2) if total else 0.0


def _sample(
    target: dict[str, Any],
    *,
    line_no: int,
    raw_values: list[str | None],
    generated: list[tuple[ExtractedCsvRuleValue, NormalizedExamValue | None]],
    status: str,
    reason: str | None,
    global_error_state: dict[str, int],
) -> None:
    is_error = status in {"ERROR", "CONFLICT"}
    if is_error:
        if global_error_state["count"] >= MAX_ERROR_SAMPLES:
            target["omitted_error_count"] += 1
            return
        global_error_state["count"] += 1
        target["error_sample_count"] += 1
    elif len(target["samples"]) >= MAX_SAMPLES_PER_TARGET:
        return
    target["samples"].append(
        {
            "line_no": line_no,
            "raw_values": raw_values,
            "status": status,
            "reason": reason,
            "results": [
                {
                    "rule_key": extracted.rule.rule_key,
                    "condition_group": extracted.matched_condition_group_no,
                    "source_value": extracted.values_by_role.get("VALUE"),
                    "normalized_value": normalized.normalized_value if normalized else extracted.values_by_role.get("VALUE"),
                    "code_value": normalized.code_value if normalized else None,
                    "code_display": normalized.code_display if normalized else None,
                    "normalize_status": normalized.normalize_status if normalized else "OK",
                    "normalize_reason": normalized.normalize_reason if normalized else None,
                    "validation_status": normalized.validation_status if normalized else "VALID",
                    "validation_reason": normalized.validation_reason if normalized else None,
                }
                for extracted, normalized in generated
            ],
        }
    )


def run_csv_mapping_data_check(
    path: str | Path,
    *,
    stream_header: CsvStreamHeader,
    rules: list[CsvMappingRule],
    normalize: Callable[[CsvMappingRule, str], NormalizedExamValue],
    delimiter: str = ",",
    quote_char: str = '"',
    max_rows: int = MAX_DATA_ROWS,
) -> dict[str, Any]:
    rules_by_target: dict[str, list[CsvMappingRule]] = defaultdict(list)
    targets: dict[str, dict[str, Any]] = {}
    for rule in rules:
        key = _target_key(rule)
        rules_by_target[key].append(rule)
        target = targets.setdefault(key, _new_target(rule))
        target["rules"].append(_rule_description(rule))

    total_data_rows = 0
    processed_rows = 0
    empty_rows = 0
    omitted_rows = 0
    global_error_state = {"count": 0}

    for line_no, row in iter_csv_data_rows(
        path,
        encoding=stream_header.encoding,
        data_start_row_no=stream_header.data_start_row_no,
        delimiter=delimiter,
        quote_char=quote_char,
    ):
        total_data_rows += 1
        if all(not cell for cell in row):
            empty_rows += 1
            continue
        if processed_rows >= max_rows:
            omitted_rows += 1
            continue
        processed_rows += 1
        extracted_by_rule = {result.rule.rule_id: result for result in extract_row_values(row, rules)}

        for key, target_rules in rules_by_target.items():
            target = targets[key]
            target["processed_count"] += 1
            matched = [
                extracted_by_rule[rule.rule_id]
                for rule in target_rules
                if extracted_by_rule[rule.rule_id].matched_condition_group_no is not None
            ]
            unresolved_errors = [
                error
                for rule in target_rules
                for error in extracted_by_rule[rule.rule_id].errors
                if error.startswith("UNRESOLVED_CONDITION_COLUMN")
            ]
            matched_errors = [error for result in matched for error in result.errors]
            values = [result for result in matched if result.values_by_role.get("VALUE") not in {None, ""}]
            raw_values = [result.values_by_role.get("VALUE") for result in matched]
            if not values:
                target["blank_count"] += 1
                target["no_condition_count"] += 1 if not matched else 0
                configuration_errors = unresolved_errors or matched_errors
                if configuration_errors:
                    target["failure_count"] += 1
                    _sample(
                        target,
                        line_no=line_no,
                        raw_values=raw_values,
                        generated=[],
                        status="ERROR",
                        reason=configuration_errors[0],
                        global_error_state=global_error_state,
                    )
                continue

            target["value_count"] += 1
            allows_multiple = all(result.rule.selection_mode == "MULTI_ENTRY" for result in values)
            if len(values) > 1 and not allows_multiple:
                target["conflict_count"] += 1
                target["failure_count"] += 1
                _sample(
                    target,
                    line_no=line_no,
                    raw_values=[result.values_by_role.get("VALUE") for result in values],
                    generated=[],
                    status="CONFLICT",
                    reason="MULTIPLE_RULE_MATCH",
                    global_error_state=global_error_state,
                )
                continue

            generated: list[tuple[ExtractedCsvRuleValue, NormalizedExamValue | None]] = []
            failed = False
            combination: list[str] = []
            for result in values:
                raw_value = str(result.values_by_role.get("VALUE") or "")
                normalized = normalize(result.rule, raw_value) if result.rule.target_kind == "EXAM_ITEM_VALUE" else None
                generated.append((result, normalized))
                if normalized and not target.get("data_type"):
                    target["data_type"] = normalized.raw_value_type
                if normalized and (normalized.normalize_status == "ERROR" or normalized.validation_status == "INVALID"):
                    failed = True
                    continue
                if normalized and normalized.code_value:
                    code_key = f"{normalized.code_value}\t{normalized.code_display or ''}"
                    target["code_counts"][code_key] += 1
                    target["code_raw_counts"][code_key][raw_value] += 1
                    combination.append(code_key)
            if failed:
                target["failure_count"] += 1
            else:
                target["success_count"] += 1
                if combination:
                    target["combination_counts"][" | ".join(sorted(combination))] += 1
            reason = next(
                (
                    normalized.validation_reason or normalized.normalize_reason
                    for _, normalized in generated
                    if normalized and (normalized.normalize_status == "ERROR" or normalized.validation_status == "INVALID")
                ),
                None,
            )
            _sample(
                target,
                line_no=line_no,
                raw_values=[result.values_by_role.get("VALUE") for result in values],
                generated=generated,
                status="ERROR" if failed else "OK",
                reason=reason,
                global_error_state=global_error_state,
            )

    serialized_targets = []
    for target in targets.values():
        total = target["processed_count"]
        code_rows = []
        for key, count in target.pop("code_counts").most_common():
            code_value, code_display = key.split("\t", 1)
            code_rows.append(
                {
                    "code_value": code_value,
                    "code_display": code_display,
                    "count": count,
                    "rate": _percent(count, total),
                    "raw_values": [
                        {"raw_value": raw, "count": raw_count, "rate": _percent(raw_count, total)}
                        for raw, raw_count in target["code_raw_counts"][key].most_common()
                    ],
                }
            )
        target.pop("code_raw_counts")
        target["code_rows"] = code_rows
        target["combination_rows"] = [
            {"value": value, "count": count, "rate": _percent(count, total)}
            for value, count in target.pop("combination_counts").most_common()
        ]
        for field in ("value", "blank", "success", "failure", "no_condition", "conflict"):
            target[f"{field}_rate"] = _percent(int(target[f"{field}_count"]), total)
        serialized_targets.append(target)

    return {
        "encoding": stream_header.encoding,
        "total_data_rows": total_data_rows,
        "processed_rows": processed_rows,
        "empty_rows": empty_rows,
        "omitted_rows": omitted_rows,
        "target_count": len(serialized_targets),
        "error_samples_shown": global_error_state["count"],
        "targets": serialized_targets,
    }
