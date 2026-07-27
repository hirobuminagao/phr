#!/usr/bin/env python3
"""Build phr_master exam facility / folder alias seed SQL.

Inputs:
- 社会保険診療報酬支払基金 CSV (`Pref_00.csv`, CP932)
- Existing health_exam_result medical_folder_aliases seed

The generated SQL avoids fixed `exam_facility_id` values. Folder aliases refer
to facilities by `medical_institution_code` subqueries.
"""

from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path


DEFAULT_PREF_CSV = Path("docs/spec/exam_result_csv_import/downloads/Pref_00.csv")
DEFAULT_ALIAS_SEED = Path("sql/seed/health_exam_result/0010_health_exam_result__medical_folder_aliases_event2.sql")
DEFAULT_OUTPUT = Path("sql/seed/phr_master/0000_generated_exam_facilities_and_aliases_event2.sql")


ADOPTED_ALIAS_CODES = {
    "0415312420": "0421200015",
    "1010211041": "1020700017",
    "1110103762": None,
    "1210122986": "1220700072",
    "2310607227": "2320700061",
    "2311301861": "2320800200",
    "2719109346": "2720700059",
    "4410121711": "4420700074",
}

EXCLUDED_ALIAS_PREFIXES = {
    "202604開院": "旧仮フォルダ名。正式採番済みフォルダを使用するため初期移設seedでは対象外。",
}


@dataclass(frozen=True)
class PrefFacility:
    code: str
    facility_type: str
    name: str
    postal_code: str
    phone_number: str
    address: str
    website_url: str
    management_entity: str


@dataclass(frozen=True)
class FolderAlias:
    event_id: int
    src_folder_raw: str
    dst_folder_norm: str
    note: str | None
    manual_judgement: int
    is_active: int

    @property
    def leading_code(self) -> str | None:
        match = re.match(r"^(\d{10})_", self.src_folder_raw)
        return match.group(1) if match else None

    @property
    def display_name(self) -> str | None:
        if "_" not in self.src_folder_raw:
            return None
        return self.src_folder_raw.split("_", 1)[1] or None


def sql_string(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def read_pref_facilities(path: Path) -> dict[str, PrefFacility]:
    facilities: dict[str, PrefFacility] = {}
    with path.open("r", encoding="cp932", newline="") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            code = (row.get("機関コード") or "").strip()
            if not code:
                continue
            facilities[code] = PrefFacility(
                code=code,
                facility_type=(row.get("機関種別") or "").strip(),
                name=(row.get("機関名") or "").strip(),
                postal_code=(row.get("郵便番号") or "").strip(),
                phone_number=(row.get("電話番号") or "").strip(),
                address=(row.get("機関所在地") or "").strip(),
                website_url=(row.get("ホームページ") or "").strip(),
                management_entity=(row.get("経営主体") or "").strip(),
            )
    return facilities


def read_aliases(path: Path) -> list[FolderAlias]:
    text = path.read_text()
    pattern = re.compile(
        r"\(\s*(\d+),\s*'((?:[^']|'')*)',\s*'((?:[^']|'')*)',\s*(NULL|'(?:[^']|'')*'),\s*(\d+),\s*(\d+),",
        re.MULTILINE,
    )
    aliases: list[FolderAlias] = []
    for match in pattern.finditer(text):
        note_raw = match.group(4)
        note = None if note_raw == "NULL" else note_raw[1:-1].replace("''", "'")
        aliases.append(
            FolderAlias(
                event_id=int(match.group(1)),
                src_folder_raw=match.group(2).replace("''", "'"),
                dst_folder_norm=match.group(3).replace("''", "'"),
                note=note,
                manual_judgement=int(match.group(5)),
                is_active=int(match.group(6)),
            )
        )
    return aliases


def adopted_code_for_alias(alias: FolderAlias) -> str | None:
    leading_code = alias.leading_code
    if leading_code is None:
        return None
    if leading_code in ADOPTED_ALIAS_CODES:
        return ADOPTED_ALIAS_CODES[leading_code]
    return leading_code


def is_excluded_alias(alias: FolderAlias) -> bool:
    return any(alias.src_folder_raw.startswith(prefix) for prefix in EXCLUDED_ALIAS_PREFIXES)


def build_display_names(aliases: list[FolderAlias]) -> dict[str, str]:
    display_names: dict[str, str] = {}
    for alias in aliases:
        if is_excluded_alias(alias):
            continue
        adopted_code = adopted_code_for_alias(alias)
        display_name = alias.display_name
        if adopted_code and display_name and adopted_code not in display_names:
            display_names[adopted_code] = display_name
    return display_names


def write_insert_header(fp, table: str, columns: list[str]) -> None:
    fp.write(f"INSERT INTO `{table}` (\n")
    fp.write(",\n".join(f"  `{column}`" for column in columns))
    fp.write("\n) VALUES\n")


def write_exam_facilities(fp, facilities: dict[str, PrefFacility], display_names: dict[str, str], chunk_size: int) -> None:
    columns = [
        "exam_facility_code",
        "exam_facility_name",
        "exam_facility_display_name",
        "exam_facility_type",
        "medical_institution_code",
        "postal_code",
        "address",
        "phone_number",
        "website_url",
        "management_entity",
        "is_active",
        "created_at",
        "updated_at",
    ]
    rows = list(facilities.values())
    for start in range(0, len(rows), chunk_size):
        chunk = rows[start : start + chunk_size]
        write_insert_header(fp, "phr_master`.`exam_facilities", columns)
        values: list[str] = []
        for row in chunk:
            values.append(
                "  ("
                + ", ".join(
                    [
                        sql_string(row.code),
                        sql_string(row.name),
                        sql_string(display_names.get(row.code)),
                        sql_string(row.facility_type),
                        sql_string(row.code),
                        sql_string(row.postal_code),
                        sql_string(row.address),
                        sql_string(row.phone_number),
                        sql_string(row.website_url),
                        sql_string(row.management_entity),
                        "1",
                        "CURRENT_TIMESTAMP(3)",
                        "CURRENT_TIMESTAMP(3)",
                    ]
                )
                + ")"
            )
        fp.write(",\n".join(values))
        fp.write("\nON DUPLICATE KEY UPDATE\n")
        fp.write("  `exam_facility_name` = VALUES(`exam_facility_name`),\n")
        fp.write("  `exam_facility_display_name` = COALESCE(VALUES(`exam_facility_display_name`), `exam_facility_display_name`),\n")
        fp.write("  `exam_facility_type` = VALUES(`exam_facility_type`),\n")
        fp.write("  `postal_code` = VALUES(`postal_code`),\n")
        fp.write("  `address` = VALUES(`address`),\n")
        fp.write("  `phone_number` = VALUES(`phone_number`),\n")
        fp.write("  `website_url` = VALUES(`website_url`),\n")
        fp.write("  `management_entity` = VALUES(`management_entity`),\n")
        fp.write("  `is_active` = VALUES(`is_active`),\n")
        fp.write("  `updated_at` = CURRENT_TIMESTAMP(3);\n\n")


def alias_note(alias: FolderAlias) -> str | None:
    parts: list[str] = []
    if alias.note:
        parts.append(alias.note)
    leading_code = alias.leading_code
    adopted_code = adopted_code_for_alias(alias)
    if leading_code and adopted_code and leading_code != adopted_code:
        parts.append(f"alias先頭コード {leading_code} から確認済み採用コード {adopted_code} へ紐付け")
    if leading_code and adopted_code is None:
        parts.append(f"alias先頭コード {leading_code} は未確定。データ受領時にCSV/XML内番号で確認")
    return " / ".join(parts) if parts else None


def write_aliases(fp, aliases: list[FolderAlias]) -> None:
    columns = [
        "event_id",
        "src_folder_raw",
        "dst_folder_norm",
        "exam_facility_id",
        "note",
        "manual_judgement",
        "is_active",
        "created_at",
        "updated_at",
    ]
    write_insert_header(fp, "phr_master`.`medical_folder_aliases", columns)
    values: list[str] = []
    for alias in aliases:
        if is_excluded_alias(alias):
            continue
        adopted_code = adopted_code_for_alias(alias)
        if adopted_code is None:
            facility_expr = "NULL"
        else:
            facility_expr = (
                "(SELECT ef.exam_facility_id FROM `phr_master`.`exam_facilities` ef "
                f"WHERE ef.medical_institution_code = {sql_string(adopted_code)} LIMIT 1)"
            )
        values.append(
            "  ("
            + ", ".join(
                [
                    str(alias.event_id),
                    sql_string(alias.src_folder_raw),
                    sql_string(alias.dst_folder_norm),
                    facility_expr,
                    sql_string(alias_note(alias)),
                    str(alias.manual_judgement),
                    str(alias.is_active),
                    "CURRENT_TIMESTAMP(3)",
                    "CURRENT_TIMESTAMP(3)",
                ]
            )
            + ")"
        )
    fp.write(",\n".join(values))
    fp.write("\nON DUPLICATE KEY UPDATE\n")
    fp.write("  `dst_folder_norm` = VALUES(`dst_folder_norm`),\n")
    fp.write("  `exam_facility_id` = VALUES(`exam_facility_id`),\n")
    fp.write("  `note` = VALUES(`note`),\n")
    fp.write("  `manual_judgement` = VALUES(`manual_judgement`),\n")
    fp.write("  `is_active` = VALUES(`is_active`),\n")
    fp.write("  `updated_at` = CURRENT_TIMESTAMP(3);\n\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pref-csv", type=Path, default=DEFAULT_PREF_CSV)
    parser.add_argument("--alias-seed", type=Path, default=DEFAULT_ALIAS_SEED)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--chunk-size", type=int, default=1000)
    args = parser.parse_args()

    facilities = read_pref_facilities(args.pref_csv)
    aliases = read_aliases(args.alias_seed)
    display_names = build_display_names(aliases)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="\n") as fp:
        fp.write("-- Generated seed for phr_master exam facilities and folder aliases.\n")
        fp.write("-- Source: 支払基金 Pref_00.csv + health_exam_result medical_folder_aliases event2 seed.\n")
        fp.write("-- Review before applying.\n\n")
        fp.write("START TRANSACTION;\n\n")
        write_exam_facilities(fp, facilities, display_names, args.chunk_size)
        write_aliases(fp, aliases)
        fp.write("COMMIT;\n")

    print(f"wrote {args.output}")
    print(f"exam_facilities={len(facilities)} aliases={sum(1 for alias in aliases if not is_excluded_alias(alias))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
