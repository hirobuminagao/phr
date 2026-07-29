# Healthcare Clinic Atsugi Sample

## Source

- Original file: `/Users/hiro/Downloads/ヘルスケアクリニック厚木_サンプル_全項目結果_202605.csv`
- Workspace copy: `healthcare_clinic_atsugi_sample_001.csv`
- Character encoding: CP932
- Header rows: 1
- Data start row: 2
- Columns: 830
- Data rows: 13
- Facility code in CSV: `1412910586`
- Facility name in CSV: `ヘルスケアクリニック厚木`

## Mapping

This sample uses the same 830-column Pattern A header as the Hirooka Clinic
sample. The initial seed registers a facility-specific CSV format version:

- `ATSUGI_2026_05_PATTERN_A_V1`

The mapping rules and conditions are copied from the confirmed Hirooka Pattern A
rules. Facility judgement columns remain intentionally unmapped.

## Local Verification

Local Docker import verification result:

- Files imported: 1
- Rows seen: 13
- Rows inserted on first run: 13
- Exam item values: 855
- Script errors: 0
- Normalize/validation result after alias additions: `854 OK / 1 ERROR`

The remaining normalize error is intentional pending decision:

| namecode | item | raw value | reason |
| --- | --- | --- | --- |
| `1A020000000190111` | 尿糖 | `（４＋）` | No confirmed standard code/alias for 4+ in the current dictionary |

