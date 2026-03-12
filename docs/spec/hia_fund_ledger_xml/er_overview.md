# HIA_fund_ledger_xml ER Overview

This document describes the entity relationships used by the **HIA_fund_ledger_xml** pipeline.  
The pipeline manages downloaded HIA ZIP files, their XML contents, person-year identity matching, and delivery exclusion rules.

---

## Entity Relationship Diagram

```text
+-----------------------------+
| hia_import_zips             |
+-----------------------------+
| PK zip_id                   |
| insurer_number              |
| folder_name                 |
| zip_name                    |
| dl_date                     |
| send_seq                    |
| zip_sha256                  |
| xml_count_total             |
| xml_count_success           |
| xml_count_error             |
| import_status               |
| created_at                  |
| updated_at                  |
+-----------------------------+
        | 1
        | 
        | N
        v
+-----------------------------+
| hia_xml_events              |
+-----------------------------+
| PK xml_event_id             |
| FK zip_id                   |
| FK person_year_id           |
| xml_filename                |
| xml_sha256                  |
| exam_date                   |
| facility_code               |
| facility_name               |
| report_category             |
| health_program_code         |
| created_at                  |
+-----------------------------+
        | N
        |
        v
+-----------------------------+
| hia_person_years            |
+-----------------------------+
| PK person_year_id           |
| person_id_custom            |
| name_kana_norm              |
| gender_code                 |
| exam_year                   |
| insurer_number              |
| insurance_symbol            |
| insurance_number            |
| birthdate                   |
| name_kana_raw               |
| dl_count                    |
| first_seen_dl_date          |
| first_seen_zip_name         |
| first_seen_xml_filename     |
| last_seen_dl_date           |
| last_seen_zip_name          |
| last_seen_xml_filename      |
| created_at                  |
| updated_at                  |
+-----------------------------+


+-----------------------------+
| hia_import_zip_errors       |
+-----------------------------+
| PK zip_error_id             |
| FK zip_id                   |
| xml_filename                |
| error_code                  |
| error_message               |
| error_detail                |
| created_at                  |
+-----------------------------+


+----------------------------------+
| hia_delivery_exclusion_rules     |
+----------------------------------+
| PK exclusion_rule_id             |
| insurer_number                   |
| target_schema                    |
| target_table                     |
| target_column                    |
| match_type                       |
| match_value                      |
| exclusion_reason                 |
| source_note                      |
| is_enabled                       |
| created_at                       |
| updated_at                       |
+----------------------------------+
```

---

## Relationship Summary

### ZIP → XML

`hia_import_zips` → `hia_xml_events`

* One ZIP contains multiple XML files.
* Each XML record references the ZIP it originated from.

Relationship type:

```
1 ZIP : N XML
```

---

### XML → Person Year

`hia_xml_events` → `hia_person_years`

* Each XML is matched to a **person + exam_year** record.
* A person-year may appear multiple times across different downloads.

Relationship type:

```
1 person_year : N XML events
```

---

### ZIP → Error Ledger

`hia_import_zips` → `hia_import_zip_errors`

* ZIP processing may produce multiple errors.
* Errors may occur before or after XML parsing.

Relationship type:

```
1 ZIP : N errors
```

---

### Delivery Exclusion Rules

`hia_delivery_exclusion_rules` is **not linked via foreign keys**.

Instead it is used by the delivery rebuild logic to dynamically filter data.

Typical example:

```
Exclude facility_code = 'XXXX'
for insurer_number = '06139463'
```

This allows operations teams to control exclusions **without modifying application code**.

---

## Conceptual Data Flow

```
HIA ZIP Download
        |
        v
hia_import_zips
        |
        v
hia_person_years
        |
        v
hia_xml_events

Errors → hia_import_zip_errors

Delivery rebuild
        |
        v
Apply hia_delivery_exclusion_rules
```

---

## Design Intent

The structure separates responsibilities clearly:

| Layer | Responsibility |
|------|----------------|
| ZIP ledger | track downloaded packages |
| XML ledger | track individual XML files |
| Person-year ledger | identity matching and deduplication |
| Error ledger | processing failures |
| Exclusion rules | operational delivery filtering |

This design allows the system to:

* reconstruct insurer deliveries
* track historical downloads
* detect duplicate XML files
* isolate processing errors
* support operational filtering rules

---

## Future Extension Possibilities

Potential future tables may include:

* `hia_exam_events` (separate exam event abstraction)
* `hia_delivery_history` (record rebuilt delivery ZIPs)
* `hia_exclusion_hits` (log when exclusion rules match records)

These are intentionally not included in v1 to keep the initial pipeline simple.
