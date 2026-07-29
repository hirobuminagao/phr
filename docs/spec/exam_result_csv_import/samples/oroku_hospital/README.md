# Oroku Hospital Joined CSV Sample

## Source

- Original file: `/Users/hiro/Downloads/202605_4710114044_06139463結合_サンプル.csv`
- Workspace copy: `oroku_hospital_joined_sample_001.csv`
- Character encoding: UTF-8 BOM
- Header rows: 1
- Data start row: 2
- Original columns: 275
- Template verification columns: 277
- Data rows: 19
- Original file SHA-256: `ee7a964be2690ef4efb6cc9e30558578cad6d9d1f29fe9f966215439f958eb55`
- Workspace file SHA-256: `78b562609d87676144910e350f90c885a8b261dfa500d5256716629fe6d11afc`
- Template header SHA-256: `d2c027cceebffc5ebc22407896cab777db3f021b1be701650413e8f3da4f609c`
- Facility code from file name: `4710114044`
- Facility name from master seed: `小禄病院`

## Structure

This sample is a joined CSV. The first source appears to occupy columns 1-123,
and the second source starts at column 124:

- Column 123: `No`
- Column 124: `氏名（漢字）`
- Column 128: `医療機関コード`
- Column 131: `健診実施日`

The import mapping should be registered after choosing which side of duplicated
items to use. The join operation itself is treated as a later utility feature,
not as part of the initial CSV import script.

For production use, `保険記号` and `保険番号` are added to the end of the joined
CSV. `保険者番号` is not added to the CSV because it can be carried from
`file_receipts.insurer_number` for the event/folder context.

## Mapping Decisions

- Use the second source as the template base.
- Use second-source `氏名（漢字）` and `氏名（カナ）`.
- Use second-source `生年月日` and `性別`.
- Use second-source result-code columns for `尿蛋白`, `尿糖`, and `尿潜血`.
- Use one side consistently for duplicated identical numeric items; this seed
  uses the second source where the second source has the same value.
- Use first-source values only where the second source has no equivalent in the
  initial mapping.
- Do not map facility judgement/category columns in the initial seed.
- Full nonblank-column review:
  `docs/spec/exam_result_csv_import/25_oroku_hospital_joined_pattern_c_review.md`

## Duplicate Header Decision List

| Header | First source | Second source | Same values | Initial note |
| --- | --- | --- | --- | --- |
| `生年月日` | col 3, `1968/5/24` | col 126, `19680524` | No | Either can work; second source is machine-friendly fixed width. |
| `性別` | col 4, `女` / `男` | col 127, `F` / `M` | No | Either can work; first source is human-readable, second needs gender conversion. |
| `身長` | col 6 | col 135 | Yes | Prefer one side consistently. |
| `体重` | col 7 | col 136 | Yes | Prefer one side consistently. |
| `標準体重` | col 8 | col 138 | Yes | Not mapped unless needed. |
| `尿蛋白` | col 36, `-` / `+-` / `ｷﾔﾝｾﾙ` | col 159, `1` / `2` / blank | No | Second source looks like result-code side; decision needed. |
| `尿潜血` | col 37, `1+` / `-` / `2+` / `3+` | col 162, `3` / `1` / `4` / `5` | No | Second source looks like result-code side; decision needed. |
| `尿糖` | col 43, `-` / `ｷﾔﾝｾﾙ` | col 160, `1` / blank | No | Second source looks like result-code side; decision needed. |
| `血小板数` | col 54 | col 175 | Yes | Prefer one side consistently. |
| `A/G比` | col 65 | col 195 | Yes, all blank | Do not map unless needed. |
| `コリンエステラーゼ` | col 71 | col 203 | Yes, all blank | Do not map unless needed. |
| `アルブミン` | col 75 | col 196 | Yes, all blank | Do not map unless needed. |
| `尿素窒素` | col 80 | col 213 | Yes | Prefer one side consistently. |
| `クレアチニン` | col 81 | col 214 | Yes | Prefer one side consistently. |
| `eGFR` | col 82 | col 215 | Yes | Prefer one side consistently. |
| `尿酸` | col 83 | col 212 | Yes | Prefer one side consistently. |
| `総コレステロール` | col 84 | col 206 | Yes | Prefer one side consistently. |
| `空腹時血糖` | col 88 | col 191 | Yes | Prefer one side consistently. |
| `non HDL-コレステロール` | col 90 | col 211 | Yes | Prefer one side consistently. |

## Local Verification

Local Docker import verification result:

- Files imported: 1
- Rows seen: 19
- Rows inserted on first run: 19
- Exam item values: 536
- Script errors: 0
- Normalize/validation result: `536 OK / 0 ERROR`

Subscriber matching is expected to stop at identity generation in this sample
because the appended `保険記号` and `保険番号` columns are currently blank:

- `insurer_number` is filled from `file_receipts.insurer_number`: `06139463`
- `insurance_symbol_raw` is blank
- `insurance_number_raw` is blank
- `subscriber_match_status = IDENTITY_ERROR`
- reason: `insurance_symbol NG: missing_raw_or_base_norm`
