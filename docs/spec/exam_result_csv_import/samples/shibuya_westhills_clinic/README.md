# Shibuya Westhills Clinic Sample

## Source

- Original file: `/Users/hiro/Downloads/1311333301_渋谷ウエストヒルズクリニック_サンプル_5月結果.csv`
- Workspace copy: `shibuya_westhills_clinic_sample_001.csv`
- Character encoding: CP932
- Header rows: 1
- Data start row: 2
- Columns: 830
- Data rows: 5
- Facility code in CSV: `1311333301`
- Facility name in CSV: `渋谷ウエストヒルズクリニック`

The provided sample had anonymization-time column deletion. The following
columns were restored as empty columns to match the real Pattern A header:

- `氏名` and `カナ氏名` after `コース名称`
- `住所` after `郵便番号`

## Mapping

This sample uses the same 830-column Pattern A header as the Hirooka Clinic and
Healthcare Clinic Atsugi samples.

The initial seed registers a facility-specific CSV format version:

- `SHIBUYA_WESTHILLS_2026_05_PATTERN_A_V1`

The mapping rules and conditions are copied from the confirmed Hirooka Pattern A
rules. Facility judgement columns remain intentionally unmapped.

## Local Verification

Local Docker import verification result:

- Files imported: 1
- Rows seen: 5
- Rows inserted on first run: 5
- Exam item values: 316
- Script errors: 0
- Normalize/validation result: `314 OK / 2 ERROR`

The remaining normalize errors are pending decision:

| namecode | item | raw value | reason |
| --- | --- | --- | --- |
| `9E160162100000001` | 視力(右) | `<0.1` | Current PQ normalize accepts numeric values only |
| `9E160162200000001` | 視力(左) | `<0.1` | Current PQ normalize accepts numeric values only |

`<0.1` is not a dictionary alias issue. It needs a separate decision on how to
preserve or export comparator-style numeric values.
