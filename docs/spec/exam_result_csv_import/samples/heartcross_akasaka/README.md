# Heartcross Akasaka CSV Sample

## Purpose

This directory contains a second CSV sample for `02_02_exam_result_csv_import` template investigation.

The original file was provided as sample data and copied from the local Downloads directory.
The sample is intended for two-row header, namecode-driven mapping, row ledger, and normalize design checks.

## Files

- `heartcross_akasaka_sample_001.csv`

## Checked Facts

- Source file: `/Users/hiro/Downloads/ハートクロス健診プラザ赤坂駅前5月結果_sample.csv`
- Copied file: `docs/spec/exam_result_csv_import/samples/heartcross_akasaka/heartcross_akasaka_sample_001.csv`
- Encoding: CP932
- Rows: 7
- Columns: 164
- Header rows: 2 rows
- Data rows: 5
- All rows have 164 columns.
- Header row 1: Japanese display header
- Header row 2: ledger field code or exam item `namecode`
- Pattern: B / two-row header, row 2 used as the active header
- Duplicate headers: none
- Duplicate row-2 codes: none
- Blank headers/codes: none

## Notes

- The sample contains no sensitive personal information; values were replaced with sample data before placement.
- This sample differs from Hirooka Pattern A because row 2 already contains stable field codes and `namecode` values.
- Row 2 should be treated as the active header for mapping. This does not require a dedicated namecode-row mapping type.
- Some headers contain the word `判定`, but not all are facility-level A/B/C style judgements.
- `メタボリックシンドローム判定` and `保健指導レベル` are standard CD result items and must be treated as entry values.
- Facility-derived judgement columns, if any, remain separate from entry value status terms such as `未実施`, `測定不能`, and `判定不能`.
- Detailed review: `docs/spec/exam_result_csv_import/24_heartcross_akasaka_pattern_b_review.md`
