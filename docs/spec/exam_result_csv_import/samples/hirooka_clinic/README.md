# Hirooka Clinic CSV Sample

## Purpose

This directory contains the first CSV sample for `02_02_exam_result_csv_import` template investigation.

The original file was provided as sample data and copied from the local Downloads directory.
The sample is intended for header, mapping rule, row ledger, and validation design checks.

## Files

- `hirooka_clinic_sample_001.csv`

## Checked Facts

- Source file: `/Users/hiro/Downloads/ヒロオカクリニックサンプル全項目結果_202605.csv`
- Copied file: `docs/spec/exam_result_csv_import/samples/hirooka_clinic/hirooka_clinic_sample_001.csv`
- Encoding: CP932
- Rows: 8
- Columns: 830
- Header row: 1 row
- Data rows: 7
- All rows have 830 columns.
- Pattern: A / single header full-width template
- `カナ氏名` exists at column 8.
- `カナ氏名` is blank in all 7 data rows.

## Notes

- The sample contains no sensitive personal information; values were replaced with sample data before placement.
- Blank `カナ氏名` should not stop CSV import by itself.
- Blank `カナ氏名` should be detected later as an item to confirm with the exam facility.
- Detailed review: `docs/spec/exam_result_csv_import/23_hirooka_clinic_pattern_a_review.md`
