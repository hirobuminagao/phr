# Murakami Iin Paper-to-CSV Sample

## Source

- Original local file: `/Users/hiro/Downloads/紙からcsvサンプル.csv`
- Workspace sample: `murakami_iin_paper_sample_001.csv`
- Character encoding: UTF-8 with BOM
- Header rows: 2
- Active header row: 1
- Data start row: 3
- Columns: 120
- Data rows: 2
- Facility code in CSV: `4210225498`
- Facility name in CSV: `医療法人社団平世会村上医院`

The workspace sample keeps the paper-to-CSV header and exam values, but replaces
person-identifying fields with local test values:

- `NAME_FULL`
- `NAME_KANA`
- `POSTALCODE`
- `ADDRESS`
- `INSURANCE_CARD_SYMBOL`
- `INSURANCE_CARD_NUMBER`

## Mapping

This sample is covered by:

- `sql/seed/phr_master/0006_add_murakami_iin_paper_csv_mapping.sql`

Registered format:

- `mapping_version = MURAKAMI_IIN_PAPER_2026_05_V1`
- `header_mode = WITH_CONTEXT`
- `header_structure_type = SIMPLE_HEADER`
- `header_context_rule = LOWER_HEADER_LABEL`
- `active_header_row_no = 1`
- `data_start_row_no = 3`
- `header_sha256 = d37bc4a347f697b9ad8bf34580f12dfd292b0dab0a803dce0bb3f6621afb3875`

## Local m4 Verification

For local m4 verification only, apply:

- `sql/seed/dev_phr/9999_local_sample_subscribers_for_csv_import.sql`

The seed inserts two anonymized subscribers matching this sample:

| hia_subscriber_id | insurance_symbol | insurance_number | birth | gender | kana |
| --- | --- | --- | --- | --- | --- |
| `CSV_MURAKAMI_001` | `100` | `900001` | `1988-12-16` | `2` | `シケン ハナコ` |
| `CSV_MURAKAMI_002` | `100` | `900002` | `1993-12-25` | `2` | `シケン メグミ` |

Normal-flow verification should use scan -> CSV format match -> CSV import ->
legal check. The local subscriber seed is not part of the execution environment
setup.

## Expected Notes

- Urine qualitative `-` is normalized by
  `sql/seed/phr_master/0007_add_urine_qualitative_dash_norm_variants.sql`.
- The original paper-derived sample had blank identity fields. This workspace
  sample fills them so that the normal subscriber match path can be tested.
