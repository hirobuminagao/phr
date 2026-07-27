# CSV Exam Result Mapping Rule Structure Examples

## Status

Draft.

このドキュメントは、`20_mapping_rule_screen_mock.html` で見えてきた入力構造を、`csv_exam_result_mapping_rules` / `csv_exam_result_mapping_conditions` の候補データとして見える化する。
実DDLの最終確定ではなく、seed化・FastAPI化前に必要情報の過不足を確認するためのサンプルである。

## Basic Shape

CSVテンプレート登録の入口は1つにする。
基本情報と検査結果値は登録先が違うだけで、CSVから値を抽出するrule/condition形式は共通にする。

親rule:

| column | meaning |
|---|---|
| `csv_format_version_id` | 対象CSVテンプレート |
| `target_kind` | `LEDGER_FIELD` / `EXAM_ITEM_VALUE` |
| `target_field` | `LEDGER_FIELD` の場合の `csv_row_ledger` field。`EXAM_ITEM_VALUE` では原則NULL |
| `target_namecode` | 単一namecode投入時の投入先 |
| `target_identity_item_code` | 同一性項目から候補namecodeを選ぶ場合のキー |
| `target_resolution_type` | `SINGLE_NAMECODE` / `IDENTITY_ITEM_CANDIDATES` |
| `selection_mode` | `DIRECT` / `EXCLUSIVE_ONE` / `MULTI_ENTRY` |
| `selection_group_code` | `EXCLUSIVE_ONE` の排他対象グループ |
| `method_structure_type` | `SINGLE_COLUMN` / `MULTI_COLUMN` |
| `raw_value_type` | CSV値型 |
| `raw_unit` | 結果値・下限・上限で共通の原本単位 |
| `is_required` | テンプレート上の必須 |
| `priority` | 複数候補成立時の優先 |

子condition:

| column | meaning |
|---|---|
| `csv_exam_result_mapping_rule_id` | 親rule |
| `condition_group_no` | OR条件のgroup |
| `source_role` | `VALUE` / `LOWER_LIMIT` / `UPPER_LIMIT` / `JUDGEMENT` / `METHOD` / `QUALIFIER` |
| `condition_type` | `HEADER_MATCH` / `METHOD_MATCH` / `VALUE_PRESENT` / `VALUE_MATCH` |
| `locator_type` | `HEADER_NAME` / `COLUMN_NO` / `HEADER_AND_COLUMN` |
| `header_context` | contextありCSVのcontext |
| `header_name` | ヘッダー名 |
| `header_occurrence` | 同一ヘッダーの出現順 |
| `column_no` | 1始まり列番号 |
| `operator` | `EQUALS` / `PRESENT` / `EMPTY` など |
| `expected_value` | 条件値 |
| `priority` | 同一group内の評価順 |

## Example 1: Basic Ledger Field

健診日を `csv_row_ledger.exam_date` に登録する。
基本情報は条件なしの `VALUE` として扱う。

### Rule

| target_kind | target_field | target_namecode | source idea |
|---|---|---|---|
| `LEDGER_FIELD` | `exam_date` | `NULL` | CSVの健診日列 |

### Conditions

| group | source_role | condition_type | locator_type | context | header | column_no | operator | expected |
|---:|---|---|---|---|---|---:|---|---|
| 1 | `VALUE` | `HEADER_MATCH` | `HEADER_NAME` | `NULL` | `健診日` | `NULL` | `PRESENT` | `NULL` |

## Example 2: Single Namecode Result Value

単一namecodeにCSVの値、下限、上限、判定を取り込む。
下限/上限の単位は `raw_unit` と同じ前提で、専用単位は持たない。

### Rule

| target_kind | target_resolution_type | target_namecode | target_field | raw_unit |
|---|---|---|---|---|
| `EXAM_ITEM_VALUE` | `SINGLE_NAMECODE` | `9N141000000000011` | `raw_value` | `mg/dL` |

### Conditions

| group | source_role | condition_type | locator_type | context | header | column_no | operator | expected |
|---:|---|---|---|---|---|---:|---|---|
| 1 | `VALUE` | `HEADER_MATCH` | `HEADER_AND_COLUMN` | `血糖` | `値` | 12 | `PRESENT` | `NULL` |
| 1 | `LOWER_LIMIT` | `HEADER_MATCH` | `HEADER_NAME` | `血糖` | `下限` | `NULL` | `PRESENT` | `NULL` |
| 1 | `UPPER_LIMIT` | `HEADER_MATCH` | `HEADER_NAME` | `血糖` | `上限` | `NULL` | `PRESENT` | `NULL` |
| 1 | `JUDGEMENT` | `HEADER_MATCH` | `HEADER_NAME` | `血糖` | `判定` | `NULL` | `PRESENT` | `NULL` |

保存先:

| source_role | save to |
|---|---|
| `VALUE` | `exam_item_values.raw_value` |
| `LOWER_LIMIT` | `exam_item_values.source_reference_lower` |
| `UPPER_LIMIT` | `exam_item_values.source_reference_upper` |
| `JUDGEMENT` | 健診機関由来の健診判定列。初期実装ではPHR側判定へ使わない。原本証跡として `csv_row_ledger.raw_row_json` から復元できる状態にする |

`VALUE` が完全空セルの場合は `exam_item_values` 行を作らない。
下限/上限/判定だけが存在しても、`VALUE` が完全空セルなら行を作らない。
`未実施` / `キャンセル` / `測定不能` などの非測定値語は完全空ではないため、`exam_item_values.raw_value` に原文を残し、normalize結果のreasonで分類する。

## Example 3: Identity Item Candidates With Multi-Column Method

同一性項目 `BP` の候補namecodeから、方式列の条件で投入先を選ぶ。
CSVに `家庭` / `院内` の方式列があり、`院内 = 1` の場合に院内測定namecodeへ登録する。
この例では、同じCSV値から最終的にどれか1つのnamecodeへ寄せるため、`selection_mode = EXCLUSIVE_ONE` として扱う。
排他対象の範囲は `selection_group_code` で明示する。

### Rule

| target_kind | target_resolution_type | selection_mode | selection_group_code | target_identity_item_code | target_namecode | method_structure_type | priority |
|---|---|---|---|---|---|---|---:|
| `EXAM_ITEM_VALUE` | `IDENTITY_ITEM_CANDIDATES` | `EXCLUSIVE_ONE` | `BP_MEASURE_SITE` | `BP` | `9N051000000000011` | `MULTI_COLUMN` | 10 |

### Conditions

| group | source_role | condition_type | locator_type | context | header | column_no | operator | expected |
|---:|---|---|---|---|---|---:|---|---|
| 1 | `VALUE` | `HEADER_MATCH` | `HEADER_AND_COLUMN` | `血圧` | `値` | 4 | `PRESENT` | `NULL` |
| 1 | `METHOD` | `METHOD_MATCH` | `HEADER_NAME` | `血圧` | `院内` | `NULL` | `EQUALS` | `1` |
| 1 | `METHOD` | `METHOD_MATCH` | `HEADER_NAME` | `血圧` | `家庭` | `NULL` | `EMPTY` | `NULL` |

別候補として `家庭 = 1` のruleを、同じ `selection_group_code`、別 `target_namecode` / 別priorityで持つ。
条件数は固定せず、必要な分だけ子conditionを追加する。

## Example 3b: Glucose Exclusive Timing Group

同じCSV値列 `血糖/値` を見て、補助列 `血糖区分` により空腹時/随時のどれか1つの `namecode` へ振り分ける。

### Rules

| selection_group_code | target_namecode | source value | condition |
|---|---|---|---|
| `GLUCOSE_TIMING` | 空腹時血糖1 namecode | `血糖/値` | `血糖区分 = 1` |
| `GLUCOSE_TIMING` | 空腹時血糖2 namecode | `血糖/値` | `血糖区分 = 2` |
| `GLUCOSE_TIMING` | 随時血糖1 namecode | `血糖/値` | `血糖区分 = 4` |
| `GLUCOSE_TIMING` | 随時血糖2 namecode | `血糖/値` | `血糖区分 = 5` |

同じ `selection_group_code` 内で複数ruleが成立した場合は、`priority` で1件に絞る。
それでも一意に決まらない場合はエラーまたはwarningとして扱う。

## Example 4: Same Identity Item But Multiple Entries

`identity_item_code` は、既存check_result側では制度上の同一性や候補探索の括りとして使われている。
そのため、同じ `identity_item_code` 配下のnamecodeが常に排他候補であるとは限らない。

CSV上で同じ括りに見える項目でも、値、計算方法、補助値、左右差、原本上の別entryとして残すべき値などが混在する場合は、`EXCLUSIVE_ONE` にしない。
候補表示には `target_identity_item_code` を使ってよいが、保存ルールは使用する `target_namecode` ごとに独立して作る。

### Rule

| target_kind | target_resolution_type | selection_mode | target_identity_item_code | target_namecode | source idea |
|---|---|---|---|---|---|
| `EXAM_ITEM_VALUE` | `IDENTITY_ITEM_CANDIDATES` | `MULTI_ENTRY` | `XXX` | `namecode_A` | 値列 |
| `EXAM_ITEM_VALUE` | `IDENTITY_ITEM_CANDIDATES` | `MULTI_ENTRY` | `XXX` | `namecode_B` | 計算方法列または別entry列 |

`MULTI_ENTRY` では、同じ `target_identity_item_code` 配下で複数ruleが成立しても潰さない。
各ruleを独立評価し、`VALUE` が存在するruleごとに `exam_item_values` 行を作る。
`identity_item_code` は候補を絞り込むUI/lookupキーであり、排他性の根拠にはしない。

## Hash / Skip Notes

- `file_receipts.file_sha256` はCSVファイル全体の重複抑制に使う。
- `csv_row_ledger.row_sha256` は行単位の再取込抑制に使う。
- `row_sha256` は列順込みのセル配列から算出する。
- ヘッダー名でsortしたkey-valueからhashを作ると、列順変更を検知できないため採用しない。
- 完全空行はskipする。
- その他の基本情報不足行は取込段階ではskipせず、後続checkで扱う。
