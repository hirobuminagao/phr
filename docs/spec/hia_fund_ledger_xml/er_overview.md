# HIA_fund_ledger_xml ER Overview

This document describes the v2 entity relationships for HIA downloaded XML and fund delivery.

The v2 design separates these responsibilities:

- HIA downloaded ZIP/XML source ledgers
- Person-year linkage ledgers
- Fund delivery list/run/member ledgers
- Operational exclusion rules

---

## Entity Relationship Diagram

```text
+-----------------------------+
| hia_download_zips           |
+-----------------------------+
| PK download_zip_id          |
| event_id                    |
| insurer_number              |
| facility_code               |
| folder_name                 |
| zip_name                    |
| dl_date                     |
| send_seq                    |
| zip_sha256                  |
| source_zip_path             |
| archive_zip_path            |
| import_status               |
+-----------------------------+
        | 1
        |
        | N
        v
+-----------------------------+
| hia_download_xmls           |
+-----------------------------+
| PK hia_download_xml_id      |
| FK download_zip_id          |
| event_id                    |
| xml_filename                |
| xml_inner_path              |
| xml_sha256                  |
| exam_date                   |
| exam_year                   |
| exam_month                  |
| facility_code               |
| facility_name               |
| report_category_code        |
| program_type_code           |
| person_id_custom            |
| identity_hash               |
| parse_status                |
+-----------------------------+
        | 1
        |
        | N
        v
+-----------------------------+
| hia_person_xml_events       |
+-----------------------------+
| PK person_xml_event_id      |
| FK person_year_id           |
| FK hia_download_xml_id      |
| FK download_zip_id          |
| event_type                  |
| event_status                |
| is_current                  |
+-----------------------------+
        | N
        |
        | 1
        v
+-----------------------------+
| hia_person_years            |
+-----------------------------+
| PK person_year_id           |
| event_id                    |
| person_id_custom            |
| identity_hash               |
| exam_year                   |
| insurer_number              |
| insurance_symbol_match      |
| insurance_number_match      |
| birthdate                   |
| name_kana_norm              |
| dl_count                    |
| first_seen_dl_date          |
| last_seen_dl_date           |
+-----------------------------+
```

```text
+-----------------------------+
| fund_delivery_lists         |
+-----------------------------+
| PK delivery_list_id         |
| event_id                    |
| insurer_number              |
| list_name                   |
| output_mode                 |
| exam_month                  |
| grouping_mode               |
| sender_code                 |
| delivery_policy             |
| same_exam_date_policy       |
| list_status                 |
| submitted_at                |
+-----------------------------+
        | 1
        |
        | N
        v
+-----------------------------+
| fund_delivery_list_members  |
+-----------------------------+
| PK delivery_list_member_id  |
| FK delivery_list_id         |
| FK person_year_id           |
| FK selected_hia_download_xml_id |
| member_status               |
| selection_reason            |
+-----------------------------+
```

```text
+-----------------------------+
| fund_delivery_runs          |
+-----------------------------+
| PK delivery_run_id          |
| FK delivery_list_id         |
| event_id                    |
| output_mode                 |
| exam_month                  |
| grouping_mode               |
| sender_code                 |
| output_zip_name             |
| output_zip_path             |
| delivery_status             |
+-----------------------------+
        | 1
        |
        | N
        v
+-----------------------------+
| fund_delivery_members       |
+-----------------------------+
| PK delivery_member_id       |
| FK delivery_run_id          |
| FK delivery_list_member_id  |
| FK person_year_id           |
| FK hia_download_xml_id      |
| output_xml_filename         |
| member_status               |
+-----------------------------+
```

```text
+-----------------------------+
| fund_delivery_xml_candidates|
+-----------------------------+
| PK delivery_candidate_id    |
| event_id                    |
| FK person_year_id           |
| FK hia_download_xml_id      |
| FK person_xml_event_id      |
| exam_date                   |
| exam_month                  |
| dl_date                     |
| send_seq                    |
| candidate_status            |
| selection_policy            |
+-----------------------------+

+-----------------------------+
| fund_delivery_person_status |
+-----------------------------+
| PK delivery_person_status_id|
| event_id                    |
| FK person_year_id           |
| exam_year                   |
| delivery_tracking_status    |
| last_delivered_at           |
| redelivery_required         |
+-----------------------------+

+-----------------------------+
| fund_delivery_exclusion_rules |
+-------------------------------+
| PK exclusion_rule_id          |
| event_id                      |
| insurer_number                |
| target_table                  |
| target_column                 |
| match_type                    |
| match_value                   |
| exclusion_reason              |
| is_enabled                    |
+-------------------------------+
```

---

## Relationship Summary

### ZIP -> XML

`hia_download_zips` -> `hia_download_xmls`

One HIA downloaded ZIP contains multiple XML files.

### XML -> Person Year

`hia_download_xmls` -> `hia_person_xml_events` -> `hia_person_years`

XML source facts and person-year linkage history are separated.

### Candidate -> List -> Run

`fund_delivery_xml_candidates` stores selectable XML candidates.

`fund_delivery_lists` / `fund_delivery_list_members` are the human-controlled output list.

`fund_delivery_runs` / `fund_delivery_members` are the actual ZIP output history.

---

## Script Boundary

Only human-operated entry scripts are placed directly under `scripts/hia/`.

Detailed selection, dedupe, summary, and ZIP construction logic should live under `scripts/hia/script_lib/`.

| script | purpose |
| --- | --- |
| `01_import_downloaded_xml_zip.py` | import downloaded HIA ZIP/XML |
| `02_create_fund_delivery_list.py` | create fund delivery list |
| `03_export_fund_delivery_zip.py` | export fund delivery ZIP |
| `04_mark_fund_delivery_submitted.py` | mark delivery members as submitted/error/pending |
