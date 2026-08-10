# HIA Fund Delivery Sample Data

## Purpose

This directory contains non-sensitive sample data for the HIA downloaded XML -> fund delivery workflow.

The samples are intentionally separated from real operating data:

- Health insurer / facility identifiers use realistic public or operational-style identifiers.
- Subscriber names, insurance numbers, dates, and health exam results are synthetic.
- ZIP files are generated locally under `data/hia_export/input_zip/` and are not committed.

## Files

- `subscribers/sample_subscribers_event2.csv`
  - Synthetic subscriber rows for event `2` style verification.
  - Designed to match the sample XML identity fields.
- `hia_download_xml/*.xml`
  - Minimal HIA-downloaded XML samples used by the ZIP generator.
  - These are parser-focused fixtures, not complete conformance-test XML.
- `csv_import/murakami_iin_event2_sample_001.csv`
  - CSV import sample whose first two rows match `SAMPLE_HIA_001` and `SAMPLE_HIA_002`.
  - Header rows are copied from the Murakami Iin paper-to-CSV sample, so the existing Murakami mapping can be reused.
- `scripts/dev_tools/generate_hia_fund_delivery_sample_zips.py`
  - Generates HIA downloaded ZIP fixtures under `data/hia_export/input_zip/sample_hia_fund_delivery/`.
- `scripts/dev_tools/generate_exam_result_csv_sample_for_hia_fund_delivery.py`
  - Regenerates the CSV import sample from the existing Murakami Iin sample.

## Sample Cases

| case | purpose | expected |
| --- | --- | --- |
| `SAMPLE_HIA_001` | first-time normal XML | parse OK, unique candidate |
| `SAMPLE_HIA_002_OLD` | same person / same exam date old XML | duplicate candidate, old side |
| `SAMPLE_HIA_002_NEW` | same person / same exam date new XML | duplicate candidate, latest side |
| `SAMPLE_HIA_003_MISSING_NUMBER` | missing insurance number | parse error / not output candidate |

## Generate CSV Import Sample

```bash
python scripts/dev_tools/generate_exam_result_csv_sample_for_hia_fund_delivery.py
```

The generated CSV is:

```text
docs/spec/hia_fund_ledger_xml/samples/csv_import/murakami_iin_event2_sample_001.csv
```

This CSV keeps the Murakami Iin header fingerprint unchanged and only replaces
the first two data rows with the sample subscribers:

| sample | insurance symbol | insurance number | exam date |
| --- | --- | --- | --- |
| `SAMPLE_HIA_001` | `100` | `700001` | `2026-05-15` |
| `SAMPLE_HIA_002` | `100` | `700002` | `2026-05-20` |

## Generate ZIPs

```bash
python scripts/dev_tools/generate_hia_fund_delivery_sample_zips.py --clean
```

Then run:

```bash
python scripts/hia/01_import_downloaded_xml_zip.py --dry-run
```

For a local apply test, remove `--dry-run`.

## Notes

- Runtime ZIPs are ignored by git through `data/hia_export/`.
- If subscriber SQL fixtures are later needed, generate them from `sample_subscribers_event2.csv` and the current identity library rather than hand-maintaining hashes.
