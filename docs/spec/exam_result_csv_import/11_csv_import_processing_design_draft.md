# CSV Import Processing Design Draft

## Status

Draft.

このドキュメントは `02_02_exam_result_csv_import` の処理側設計を整理する。
`phr_master` の初期DDL案は `10_phr_master_initial_ddl_draft.md` に置き、本ドキュメントでは `health_exam_result` 側の受領台帳、CSV行台帳、`exam_item_values` 登録までの接続を扱う。

現時点ではDDL適用、migration作成、seed作成、スクリプト変更は行わない。

## Related Existing Decisions

既存の `health_exam_result` 設計では、将来的にCSV直取込へ対応する場合は `csv_row_ledger` を追加し、基本情報Ledgerと健診結果値を分離する方針が記録されている。

`exam_item_values` は由来を `ledger_type` / `ledger_id` で表すため、CSV取込でもXMLと同じ縦持ち結果値テーブルを使う。
CSVの場合は `ledger_type = 'CSV'`、`ledger_id = csv_row_ledger.csv_row_ledger_id` とする。

## Boundary

### phr_master

`phr_master` は以下を持つ。

- 健診機関マスタ
- 受領フォルダalias
- CSVフォーマットバージョン
- CSV列マッピング
- CSV結果値変換ルール

`phr_master` は受領ファイル、CSV行、取込エラー、健診結果値などの業務トランザクションを持たない。

### health_exam_result

`health_exam_result` は以下を持つ。

- 受領ファイル台帳
- CSV行台帳
- `exam_item_values`
- ETL run / error

CSV取込の処理状態や再実行制御は `health_exam_result` 側で管理する。
実行履歴の根は既存方針どおり `etl_runs` とし、CSV専用の実行履歴テーブルは初期設計では追加しない。

## Processing Tables

CSV取込側で必要になる処理テーブル案は以下とする。

- `file_receipts`
  - 既存テーブル。
  - CSVファイル単位の受領台帳として利用する。
  - `exam_facility_id` を追加する案は `10_phr_master_initial_ddl_draft.md` に記載する。
  - 既存 `facility_code` / `facility_name` は、scan時にlookupした健診機関コード・名称のスナップショットとして利用する案とする。
  - CSVファイルでは、scan時または再照合時に `actual_header_sha256` / `matched_csv_format_version_id` を保持する。
- `csv_row_ledger`
  - CSVデータ行単位の基本情報Ledger。
  - 受診者識別、健診日、加入者照合状態、行処理状態を保持する。
  - `exam_item_values.ledger_id` の参照先になる。
- `etl_runs` / `etl_errors`
  - 既存のETL実行履歴・エラー台帳。
  - CSV取込Runの開始・終了・件数・エラー数はここへ集約する。
  - `file_receipts` と `csv_row_ledger` は、処理時の `etl_run_id` を保持して実行履歴へ紐付ける。

## Execution History Policy

CSV取込専用の `csv_import_batches` は初期設計では採用しない。
既存の根本方針どおり、実行履歴は `etl_runs` を正とする。

役割分担:

- `etl_runs`
  - 1回のスクリプト実行・dry-run・再実行の履歴。
  - phase、開始終了、件数、エラー数などの実行サマリー。
- `file_receipts`
  - CSVファイル単位の現在状態。
  - scan runへの参照、format照合、header確認Go、CSVファイル単位の現在状態を持つ。
  - CSV取込runの履歴は `file_receipts` へ上書きせず、`etl_runs` / `csv_row_ledger.etl_run_id` / `etl_errors.run_id` に寄せる。
- `csv_row_ledger`
  - CSVデータ行、つまり1人分の現在状態。
  - 加入者突合、検査値処理、check/export状態を持つ。
- `etl_errors`
  - run/file/row/item単位のエラー証跡。

将来、1つの `file_receipts` に対して複数回の詳細な試行履歴を業務画面で並べる必要が出た場合のみ、`etl_runs` を親にした補助テーブルを再検討する。
ただしその場合も、実行履歴の根を `etl_runs` から置き換えない。

## csv_row_ledger Draft

CSVデータ行単位の台帳。
CSVの1行から基本情報を抽出し、加入者照合と健診結果値登録の単位にする。
`csv_row_ledger` は `xml_ledger` と対になるCSV由来の1人分台帳とし、状態管理・照合結果・check/export系の考え方は `xml_ledger` に準拠する。
基本情報は、旧 `work_other.medi_exam_result_ledger` がXML作成時に利用していた項目も参照し、CSVからXMLを作成する際に必要な1人分の基本情報を保持する。

`raw_row_json` は、CSVの該当データ行を証跡として再現するための値である。
想定内容は、文字コード変換・CSVパース後の1行分セル配列、列番号、解決済みヘッダー/contextを含むsnapshotとする。
元ファイルの完全なバイト列は `file_receipts.file_sha256` と実ファイル管理に寄せ、`raw_row_json` は「この行から何を抽出したか」を人間と処理が確認するための行単位証跡として扱う。

```sql
CREATE TABLE `health_exam_result`.`csv_row_ledger` (
  `csv_row_ledger_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `file_receipt_id` bigint unsigned NOT NULL,
  `etl_run_id` bigint unsigned DEFAULT NULL,
  `event_id` bigint NOT NULL,
  `src_row_no` int NOT NULL,
  `src_line_no` int DEFAULT NULL,
  `row_sha256` char(64) DEFAULT NULL,
  `raw_row_json` json DEFAULT NULL,
  `actual_header_sha256` char(64) DEFAULT NULL,
  `mapping_version` varchar(64) DEFAULT NULL,
  `subscriber_id` bigint unsigned DEFAULT NULL,
  `hia_subscriber_id` varchar(190) DEFAULT NULL,
  `insurer_number` varchar(20) DEFAULT NULL,
  `exam_facility_id` bigint unsigned DEFAULT NULL,
  `facility_code` varchar(64) DEFAULT NULL,
  `facility_name` varchar(255) DEFAULT NULL,
  `exam_date` date DEFAULT NULL,
  `name_full_raw` varchar(255) DEFAULT NULL,
  `name_kana_raw` varchar(255) DEFAULT NULL,
  `name_kana_match` varchar(255) DEFAULT NULL,
  `insurance_symbol_raw` varchar(190) DEFAULT NULL,
  `insurance_symbol_match` varchar(190) DEFAULT NULL,
  `insurance_number_raw` varchar(190) DEFAULT NULL,
  `insurance_number_match` varchar(190) DEFAULT NULL,
  `insurance_branch_number_raw` varchar(64) DEFAULT NULL,
  `insurance_branch_number_match` varchar(64) DEFAULT NULL,
  `birthdate` date DEFAULT NULL,
  `gender_code` varchar(16) DEFAULT NULL,
  `gender_raw` varchar(64) DEFAULT NULL,
  `health_exam_report_category` varchar(64) DEFAULT NULL,
  `program_code` varchar(64) DEFAULT NULL,
  `postal_code` varchar(16) DEFAULT NULL,
  `address` varchar(255) DEFAULT NULL,
  `exam_facility_postal_code` varchar(16) DEFAULT NULL,
  `exam_facility_address` varchar(255) DEFAULT NULL,
  `exam_facility_phone_number` varchar(32) DEFAULT NULL,
  `identity_hash` char(64) DEFAULT NULL,
  `person_id_custom` varchar(190) DEFAULT NULL,
  `subscriber_match_status` varchar(32) DEFAULT NULL,
  `subscriber_match_method` varchar(64) DEFAULT NULL,
  `subscriber_match_reason` text,
  `exam_item_status` varchar(32) DEFAULT NULL,
  `exam_item_count` int NOT NULL DEFAULT 0,
  `exam_item_error_count` int NOT NULL DEFAULT 0,
  `exam_item_reason` text,
  `row_status` varchar(32) NOT NULL DEFAULT 'PENDING',
  `row_reason` text,
  `check_status` varchar(32) NOT NULL DEFAULT 'PENDING',
  `check_reason` text,
  `xml_export_status` varchar(32) NOT NULL DEFAULT 'PENDING',
  `manual_export_approved` tinyint(1) NOT NULL DEFAULT 0,
  `manual_export_reason` text,
  `resume_approved` tinyint(1) NOT NULL DEFAULT 0,
  `resume_approved_at` datetime(3) DEFAULT NULL,
  `resume_approved_by` varchar(190) DEFAULT NULL,
  `resume_approved_reason` text,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`csv_row_ledger_id`),
  UNIQUE KEY `uq_csv_row_ledger_file_row` (`file_receipt_id`, `src_row_no`),
  KEY `idx_csv_row_ledger_file_receipt` (`file_receipt_id`),
  KEY `idx_csv_row_ledger_etl_run` (`etl_run_id`),
  KEY `idx_csv_row_ledger_event` (`event_id`),
  KEY `idx_csv_row_ledger_row_sha256` (`row_sha256`),
  KEY `idx_csv_row_ledger_subscriber` (`subscriber_id`),
  KEY `idx_csv_row_ledger_hia_subscriber` (`hia_subscriber_id`),
  KEY `idx_csv_row_ledger_insurer_number` (`insurer_number`),
  KEY `idx_csv_row_ledger_exam_facility` (`exam_facility_id`),
  KEY `idx_csv_row_ledger_facility_code` (`facility_code`),
  KEY `idx_csv_row_ledger_exam_date` (`exam_date`),
  KEY `idx_csv_row_ledger_identity_hash` (`identity_hash`),
  KEY `idx_csv_row_ledger_person_id_custom` (`person_id_custom`),
  KEY `idx_csv_row_ledger_subscriber_match_status` (`subscriber_match_status`),
  KEY `idx_csv_row_ledger_row_status` (`row_status`),
  KEY `idx_csv_row_ledger_check_status` (`check_status`),
  KEY `idx_csv_row_ledger_export_status` (`xml_export_status`),
  KEY `idx_csv_row_ledger_resume_approved` (`resume_approved`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
```

### csv_row_ledger Column Groups

`xml_ledger` 準拠:

- `event_id`
- `subscriber_id`
- `hia_subscriber_id`
- `insurer_number`
- `facility_code`
- `facility_name`
- `exam_date`
- `name_kana_raw`
- `name_kana_match`
- `insurance_symbol_raw`
- `insurance_symbol_match`
- `insurance_number_raw`
- `insurance_number_match`
- `birthdate`
- `gender_code`
- `identity_hash`
- `person_id_custom`
- `subscriber_match_status`
- `subscriber_match_method`
- `subscriber_match_reason`
- `exam_item_status`
- `exam_item_reason`
- `check_status`
- `check_reason`
- `xml_export_status`
- `manual_export_approved`
- `manual_export_reason`

旧 `work_other.medi_exam_result_ledger` 由来:

- `name_full_raw`
- `gender_raw`
- `health_exam_report_category`
- `program_code`
- `postal_code`
- `address`
- `exam_facility_postal_code`
- `exam_facility_address`
- `exam_facility_phone_number`

CSV固有:

- `file_receipt_id`
- `etl_run_id`
- `src_row_no`
- `src_line_no`
- `row_sha256`
- `raw_row_json`
- `actual_header_sha256`
- `mapping_version`
- `exam_facility_id`
- `exam_item_count`
- `exam_item_error_count`
- `row_status`
- `row_reason`
- `resume_approved`
- `resume_approved_at`
- `resume_approved_by`
- `resume_approved_reason`

## Column Mapping Role

CSVテンプレート登録という入口は1つにする。
登録先としては以下の2系統に分かれるが、CSVから値を抽出するマッピング形式は共通化する。

- 基本情報マッピング
  - CSV列を `csv_row_ledger` の基本情報カラムへ割り当てる。
  - 条件なしの単純な値抽出として扱う。
- 検査結果値マッピング
  - CSV列を `exam_item_values` の健診結果値へ割り当てる。
  - `namecode`、CSV由来項目列、検査方法条件を持つ。

`csv_column_mappings` は両者を1テーブルで扱う旧暫定案として履歴に残す。
ただし、namecode中心の検査結果値マッピングが具体化したため、初期実装では「抽出ルール形式は共通、登録先種別で分岐」する方向を優先する。
基本情報は `target_kind = LEDGER_FIELD`、条件なし、`source_role = VALUE` のruleとして表現できる。
検査結果値は `target_kind = EXAM_ITEM_VALUE`、`target_namecode` または `target_identity_item_code` を持ち、必要に応じて条件を持つruleとして表現する。
採用するテーブル名は `csv_exam_result_mapping_rules` / `csv_exam_result_mapping_conditions` とする。

加入者CSV取込では、`dev_phr.template_mappings` が `csv_header -> target_column` を基本にし、`col_order` は1始まりの定義順として使われている。
処理も `loader.iter_dict_rows()` と `source_row.get(m.csv_header)` に寄せており、列番号ではなくヘッダー名を正としている。
健診結果CSV取込もこれに合わせ、`csv_header_name` を主キー的に扱い、`csv_column_order` は1始まりの定義順・検査補助として保持する案を基本とする。

健診結果CSVでは、実施した任意項目が横方向に増減したり、健診基幹システムのテンプレート選択、施設別、健保別の出力設定によって、似たCSVでも列位置が変わることがある。
このため、`csv_column_order` を処理上の値取得キーとして使うことは避ける。
値取得は原則 `csv_header_name` で行い、列順は人間がCSV実物とmappingを照合するための補助情報として扱う。

同一ヘッダー名が繰り返されるCSVに対応するため、`csv_format_versions.header_structure_type` を持つ案とする。
さらに、contextの作り方もCSVによって異なるため、`csv_format_versions.header_context_rule` を持つ案とする。
マッピング側は `csv_header_context`, `csv_header_name`, `csv_header_occurrence` を持ち、CSVヘッダー構造に応じて列を特定する。

初期実装では、1つの `csv_format_versions` / `mapping_version` に対して登録できるヘッダーは1種類とする。
ヘッダー名の表記ゆれはシステム側で自動吸収しない。
ヘッダー名や列構造が違うCSVは、同じ施設でも別 `mapping_version` として明示登録する。
ヘッダー表記ゆれのN対N自動マッチングは、違う列を憶測で同一視するリスクがあるため初期実装では扱わない。
ルールやマッピングは完全自動生成しない。
健診機関・mapping versionごとの初回テンプレートは、人がCSV実物を確認して手動登録する。
`CARRY_FORWARD_ITEM` は自動推測エンジンではなく、手動登録済みの `header_snapshot_json.normalized_columns` に従って、持ち回りcontext形式のCSVヘッダー構造を再現する方式として扱う。
取込時にシステムが検査項目名やmappingを推測して作ることはしない。

採用する初期のヘッダー構造:

- `SIMPLE_HEADER`
  - 単純な1行ヘッダー。
  - `csv_header_name` を主キー的に使う。
- `GROUPED_VALUE_METHOD`
  - 検査項目名や上段ヘッダーの配下に `値`, `方式` などが繰り返される形式。
  - `csv_header_context` に検査項目名や上段ヘッダーを持ち、`csv_header_name` に `値` / `方式` を持つ。
  - 同一context内でさらに重複がある場合は `csv_header_occurrence` を使う。

採用する初期のcontext生成ルール:

- `UPPER_HEADER`
  - 2行ヘッダーなどで、上段ヘッダーをcontext、下段ヘッダーをnameとして扱う。
  - 例: 上段 `血圧`, 下段 `値`。
- `CARRY_FORWARD_ITEM`
  - 1行ヘッダーやExcel由来CSVで、検査項目名が出た後に `値`, `方式` が続く形式。
  - 直前の検査項目名をcontextとして持ち回る。
  - 例: `血圧, 値, 方式, 血糖, 値, 方式`。
- `NONE`
  - contextを使わない。
  - `SIMPLE_HEADER` では原則 `NONE` とする。

基本情報マッピングの場合:

- `target_field` は `csv_row_ledger` の対象カラムを表す。
- `target_namecode` は持たない。
- 受診者識別、健診日、保険者番号、氏名カナなど、加入者照合と行台帳作成に必要な値を扱う。
- 抽出ルール形式は検査結果値と同じにし、基本情報では原則として条件を持たない。

検査結果値マッピングの場合:

- `target_namecode` は `exam_item_values.namecode` を表す。
- `source_role = VALUE` は `exam_item_values.raw_value` へ反映する。
- `source_role = LOWER_LIMIT` / `UPPER_LIMIT` / `JUDGEMENT` はCSV由来の下限・上限・判定として扱う。
- `raw_value_type`, `raw_unit`, `exam_item_master`, `norm_variants` を使って結果値登録とnormalizeを行う。
- 項目別明示変換は初期実装では扱わない。必要になった時点でmapping ruleへ追加する。

ただし、健診結果値のマッピングは「CSV列から登録先へ」だけで表すと、管理画面や実CSV差分に対して扱いづらくなる可能性が高い。
実務上は `namecode` を中心に、「このCSVではどのヘッダー条件に一致したら、その `namecode` の値として採用するか」を管理する形が自然である。

そのため、`EXAM_ITEM_VALUE` は以下のような `namecode` 中心のルール評価モデルを採用する。

1. `csv_format_versions` から対象フォーマットを決定する。
2. 対象フォーマットに紐づく `csv_exam_result_mapping_rules` を取得する。
3. 各ruleの `target_resolution_type` から投入先の決め方を判定する。
4. `SINGLE_NAMECODE` の場合は `target_namecode` を投入先として固定する。
5. `IDENTITY_ITEM_CANDIDATES` の場合は `target_identity_item_code` を起点に候補 `namecode` を表示・絞り込み、各ruleの `selection_mode` と `selection_group_code` で排他選択か複数entry登録かを判定する。
6. rule配下の `csv_exam_result_mapping_conditions` を評価する。
7. `HEADER_MATCH` 条件で値列を特定する。
8. 必要に応じて `METHOD_MATCH` 条件で方式列や補助列を評価する。
9. 条件を満たした場合のみ `exam_item_values.raw_value` を作成し、normalize共通libへ渡す。

`identity_item_code` は既存check_result側では制度上の同一性や候補探索の括りであり、それだけで「候補のうち必ずどれか1つ」とは判断しない。
そのため、CSVマッピングでは `selection_mode` を明示して扱う。

- `DIRECT`
  - 固定 `target_namecode` のruleを独立評価する。
- `EXCLUSIVE_ONE`
  - 同じ `selection_group_code` 内で成立したruleのうち、`priority` により1つだけを採用する。
  - 複数成立して優先順位で決めきれない場合はwarning/errorとする。
- `MULTI_ENTRY`
  - 同じ `target_identity_item_code` 配下で複数ruleが成立しても潰さず、それぞれ `exam_item_values` entryを作る。
  - 値と計算方法のように、同じ括りに見えても別entryとして保持すべきものを想定する。

このモデルでは、単純ヘッダーも、2行ヘッダーも、`血圧, 値, 方式, 血糖, 値, 方式` のような持ち回りcontext形式も、最終的には以下の条件として表現する。

```text
target_namecode = 対象健診項目
target_resolution_type = SINGLE_NAMECODE
  condition_group_no = 1
    HEADER_MATCH: source_role = VALUE, locator_type = HEADER_NAME, context = 項目名, name = 値
    METHOD_MATCH: source_role = METHOD, locator_type = HEADER_NAME, context = 項目名, name = 方式, expected_value = 指定方式
```

`condition_group_no` はOR条件の単位として扱う。
同一rule内で同じ `condition_group_no` の条件はAND評価し、複数groupがある場合はいずれかのgroupが成立すれば採用候補とする。
同じ `selection_group_code` 内で複数候補が成立した場合は `priority` が小さいものを優先し、それでも決まらない場合はwarning/errorとして扱う。
この排他解決は `selection_mode = EXCLUSIVE_ONE` の場合だけに適用する。
`MULTI_ENTRY` の場合は、成立したruleを複数登録する。

rule/conditionの重複validateはDB制約ではなく、seed生成時および将来のFastAPI登録時に行う。
同一rule key、同一rule内の同一 `source_role + locator`、同一rule内の複数 `VALUE`、`EXCLUSIVE_ONE` の同priority衝突などはテンプレート登録エラーとして扱う。

入力画面で扱う最小項目は以下を想定する。

- CSVベース設定
  - 健診機関
  - mapping version
  - ヘッダー設定
  - context生成方式
  - データ開始行
- 投入先
  - `target_resolution_type`
  - `selection_mode`
  - `target_namecode`
  - `target_identity_item_code`
- 投入先方法
  - `SINGLE_NAMECODE`
  - `IDENTITY_ITEM_CANDIDATES`
- 方式
  - `method_structure_type`
  - `SINGLE_COLUMN`
  - `MULTI_COLUMN`
- CSV由来項目列
  - `source_role`
  - `VALUE`
  - `LOWER_LIMIT`
  - `UPPER_LIMIT`
  - `JUDGEMENT`
  - `locator_type`
  - `header_context`
  - `header_name`
  - `column_no`
  - `header_occurrence`
- 検査方法列
  - `locator_type`
  - `header_context`
  - `header_name`
  - `column_no`
  - `header_occurrence`
- 検査方法条件
  - `operator`
  - `expected_value`
  - 例: `expected_value = '1'`

値列、基準下限列、基準上限列、判定列、検査方法列はいずれも、ヘッダー名、列番号、ヘッダー名+列番号の3方式で指定できる案とする。
原則はヘッダー名を優先し、列番号のみ指定はヘッダーが不安定なCSVへの例外対応として扱う。
ヘッダー名+列番号を指定した場合は、ヘッダー名で列を探したうえで期待列番号と一致するかを検証し、ズレた場合はwarning/errorにする。

列解決では、指定条件から列が一意に決まることを必須とする。

- 0件の場合は、列未検出としてエラーにする。
- 2件以上の場合は、曖昧な列指定としてエラーにする。
- 同値ヘッダーが複数存在するCSVでは、人が `header_context`, `header_occurrence`, `COLUMN_NO`, `HEADER_AND_COLUMN` などを使って一意化条件を登録する。
- それでも1列に決まらない場合、取込処理は推測せず停止する。

CSVテンプレート登録上の `LOWER_LIMIT` / `UPPER_LIMIT` / `JUDGEMENT` は、CSVに該当列がある場合に取り込むかどうかを表す。
これは `namecode` に紐づく基準範囲マスタそのものではない。
CSVに基準下限、基準上限、判定が含まれない場合は未設定のままにし、結果値 `VALUE` のみを取り込む。

テンプレート登録画面の流れは以下を基本とする。

1. 健診機関とmapping versionを選択する。
2. CSVベース設定として、ヘッダー設定を `なし` / `単一` / `contextあり` から選ぶ。
3. `contextあり` の場合は、context生成方式を `上段ヘッダー` / `持ち回り` から選ぶ。
4. データ開始行を1始まりで入力する。
5. 同一性項目を選択する。
6. 選択した同一性項目に紐づく候補 `namecode` 一覧を表示する。
7. CSVで値を受け取る候補 `namecode` にチェックを付ける。
8. 使用する `namecode` ごとに、CSV内に存在する `値` / `下限` / `上限` / `判定` のどれを取り込むか選ぶ。
9. 選んだCSV由来項目ごとに列指定を設定する。
10. 必要に応じて検査方法列や補助条件を設定する。

`20_mapping_rule_screen_mock.html` は、画面実装ではなく、テンプレート登録に必要な構造を把握するためのサンプルモックとして扱う。
今回スコープでは、CSV取込を成立させるためのテンプレート登録はseed前提とする。
テンプレート登録は、今回スコープ完了後の次タスクでFastAPIベースの管理APIとして実装する案を検討する。
この場合、上位リソースは `exam_facilities/{exam_facility_id}/csv-format-versions/{mapping_version}` 相当とし、その配下に同一性項目別のmapping ruleを登録する。
ただし現時点では実装せず、設計調査・仕様整理に留める。

## Import Flow

1. `01_scan_files.py` がCSVを `file_receipts` に登録する。
2. `01_scan_files.py` が `phr_master.medical_folder_aliases` から `exam_facility_id` を確定し、`file_receipts` にスナップショットを持たせる。
3. `01_scan_files.py` がCSV format照合共通処理を呼び、`actual_header_sha256` / `matched_csv_format_version_id` / `status` を設定する。
4. 初回mapping未登録、複数候補、default未決定などで `WAITING_CONFIRM` になったCSVは、mapping登録後に `01_01_match_csv_format.py` でformat照合だけを再適用する。
5. `02_02_exam_result_csv_import` が新規CSVと、過去に停止したが確認Go済みのCSVを同じRunで取得する。
6. `02_02_exam_result_csv_import` は `file_receipts.matched_csv_format_version_id` があればそれを優先し、なければ `file_receipts.exam_facility_id` から `phr_master.csv_format_versions` を探索する。
7. `scripts/lib/csv/csv_loader.py` の `load_csv_result()` でCSVを読み、文字コード、delimiter、ヘッダー、行数を取得する。
8. 実CSVのヘッダー構造から `header_sha256` を算出し、採用formatの `csv_format_versions.header_sha256` と照合する。
9. ヘッダー不一致の場合は、rule/template側の許可設定と `file_receipts` 側の確認Goを確認し、未確認なら停止する。
10. `etl_runs` にCSV取込Runを開始記録する。
11. データ行ごとに `csv_row_ledger` を作成する。
12. 基本情報マッピングでCSV列を `csv_row_ledger` に反映する。
13. identity生成、加入者照合を実行し、`subscriber_match_status` を更新する。
14. 検査結果値マッピングで `exam_item_values` を登録する。
15. CSV由来raw値を入力にnormalizeし、`normalized_value`, `normalized_unit`, `normalize_status`, `normalize_reason`, `validation_status`, `validation_reason`, `normalized_at` へ反映する。
16. 行単位、file_receipts単位、etl_runs単位の状態を集約する。

## Header Fingerprint Check

健診基幹システム側のテンプレート変更による静かな欠落を防ぐため、取込前にヘッダー指紋を照合する。
ヘッダー登録済みであることを「このヘッダー構造は確認済み」とみなし、登録済みヘッダー内の未マッピング列は意図的な非取込列として扱う。

`csv_format_versions` には以下を保持する案とする。

- `header_sha256`
  - テンプレート登録時に確認した正規化済みヘッダー構造のhash。
- `header_snapshot_json`
  - hash元を人間が確認するためのヘッダー行・context・列番号・occurrenceのsnapshot。
- `header_hash_status`
  - `UNVERIFIED` / `VERIFIED` / `MISMATCH_ALLOWED` など。

取込時:

1. 実CSVから `header_mode`, `header_structure_type`, `header_context_rule`, `data_start_row_no` に従ってヘッダーを読み取る。
2. ヘッダーセルを標準化する。
3. context/occurrenceを解決した `normalized_columns` を列順込みで作る。
4. `normalized_columns` をJSON正規化し、SHA-256を算出する。
5. `csv_format_versions.header_sha256` と一致するか確認する。

不一致時の初期方針:

- 基本方針は、可能な限り取り込み、エラー・不足・警告を明確に記録する。
- ヘッダー不一致の場合、初期実装では人の確認なしに自動続行しない。
- dry-runでは差分を表示し、新規列・削除列・列順変更を確認できるようにする。
- 列番号指定を使っているruleがある場合、または必要なmapping列を解決できない場合は、誤登録リスクが高いため停止候補とする。
- `SIMPLE_HEADER` かつ全ruleがヘッダー名指定で値取得できる場合でも、続行するには人の確認Goを必要とする。
- 人は確認後に、同一format versionでそのファイルだけ続行するか、新しいmapping versionとして登録するかを選ぶ。
- rule/template側には「このmappingはヘッダー不一致でも確認後Goを許せるか」を持たせる。
- file_receipts側には「このファイルは内容確認済みでGoしてよいか」を持たせる。
- 通常Runは、新規ファイルに加えて、停止済みだがfile_receipts側で確認Goが出ているファイルを再度拾い、不足している後続処理を進める。

このcheckは「登録時に確認していないヘッダー構造のCSVを、既存mappingで取り込んでしまう」事故を防ぐための入口である。
ヘッダーが一致している場合、未マッピング列はテンプレート登録時に不要と判断した列として扱い、coverage不足エラーにはしない。

不一致時制御で決める内容:

| option | 内容 | メリット | デメリット |
|---|---|---|---|
| 解決可能ならwarning取込 | header不一致でも必要列が解決できれば取込し、差分をwarningとして記録する | まず取り込める。後続checkで不足を明確化できる | 想定外列を見逃す可能性がある。初期では採用しない |
| 常に停止 | header不一致なら必ず停止し、新version登録を求める | 最も安全。判断が単純 | 列追加だけでも止まる |
| file_receipts確認済みoverride | そのファイルだけ確認済みとして続行する | 例外がファイル単位で閉じる | 再取込時の扱いを決める必要がある |
| format側MISMATCH_ALLOWED | format version側で不一致許可を持つ | 同じ変更CSVを継続処理しやすい | 恒常的に検知が弱くなる |
| rule側allow_header_mismatch | ruleが全てヘッダー名指定等の場合だけ許可 | 列番号指定ruleを厳格に止められる | 判定ロジックが複雑 |

採用する初期値:

- 原則は `ALLOW_AFTER_CONFIRM` とし、ヘッダー不一致時は人の確認Goが出るまで止める。
- 必要なmapping列が解決できない場合、または列番号指定ruleがあり列ズレリスクが高い場合は停止する。
- rule/template側で確認後Goが許可されている場合だけ、`file_receipts` 側の「確認済みGo」で続行できる。
- `file_receipts` 側の確認済みGoは、そのファイル内容を人が確認した証跡として扱う。
- 停止済みファイルの再処理は専用Runを作らず、通常Runが新規ファイルと一緒に拾う。
- `IMPORT_RESOLVABLE_WITH_WARNING` のような、人の確認なしに解決可能なら続行する設定は、初期では採用しない。

## Run Target Selection

CSV取込Runは、基本的に毎回同じ動きにする。
新規取込、停止後の確認Go、後続処理の不足分を別コマンドや別モードに分けず、処理対象の状態で判定する。

対象候補:

1. 新規CSV
   - `file_receipts` に登録済みで、CSV取込が未開始のもの。
2. 停止済みCSV
   - ヘッダー不一致、不足設定、確認待ちなどで停止したもの。
   - rule/template側で確認後Goが許可され、かつ `file_receipts` 側で確認Goが出ている場合だけ再処理する。
3. 後続処理不足CSV
   - `csv_row_ledger` までは作成済みだが、加入者照合や `exam_item_values` 登録などの後続処理が未完了のもの。

処理方針:

- 新規CSVは通常通り先頭から処理する。
- 停止済みCSVは、停止理由が解消済みか確認し、解消されていれば再投入する。
- 既に正常完了した `row_sha256` はskipし、未完了またはerror行だけを処理対象にできるようにする。
- Runそのものの操作感は常に同じにし、状態遷移で「止める」「再開する」「後続だけ進める」を制御する。

## Extraction / Registration Strategy

基本情報マッピングと検査結果値マッピングは内部モデルとして分ける。
初期実装では、CSVの1行を1受診者・1健診結果セットとして扱い、行単位で抽出・登録する。

### Row-Oriented Extraction

CSVデータ行ごとに、基本情報と検査結果値を一括で抽出する。
その行の中で、基本情報を `csv_row_ledger` へupsertし、加入者照合後に検査結果値を `exam_item_values` へ登録する。
再処理時は、対象 `csv_row_ledger` に紐づくCSV由来 `exam_item_values` をdelete+insertで入れ替える。
これにより、現状の `exam_item_values` に新しい一意制約を追加せずに整合させる。

処理イメージ:

1. 1行分のCSVセルを読む。
2. 対象format versionに紐づく抽出項目一覧を取得する。
3. 基本情報マッピングを適用して `ledger_payload` を作る。
4. 検査結果値は、各項目ruleを1つずつ評価して `exam_item_payloads` を作る。
5. `csv_row_ledger` をupsertする。
6. identity生成・加入者照合を行う。
7. 再処理対象の場合は、既存のCSV由来 `exam_item_values` を削除する。
8. その行で必要な `exam_item_values` のinsertを組み立てる。
9. `exam_item_values` を登録し、normalize結果も反映する。
10. 行単位のstatus/errorを確定する。
11. 1行分のDB変更をcommitする。

同じCSV列でも、行ごとの補助列や方式条件によって保存先 `target_namecode` が変わる。
たとえば中性脂肪の値列が同じでも、補助列が `1` なら空腹時TGの方法1、補助列が `6` なら随時TGの方法3として `exam_item_values` を作る可能性がある。
そのため、CSVテンプレートは候補ruleを定義するだけで、実際にどの `namecode` の行を作るかはCSVデータ行単位で決定する。

行単位トランザクションの粒度:

1. row hashを算出し、skip可否を判定する。
2. 基本情報抽出結果から `csv_row_ledger` のinsert/update文を作る。
3. 再処理対象の場合は、`ledger_type = 'CSV'` かつ `ledger_id = csv_row_ledger_id` の既存 `exam_item_values` を削除する。
4. 抽出対象ruleを順番に評価し、成立したruleごとに `exam_item_values` のinsert文を作る。
5. `VALUE` が完全空セルのruleは `exam_item_values` を作らない。
6. `exam_item_values` payloadごとにnormalizeを実行し、normalize系カラムを同じpayloadへ反映する。
7. 1行分の `csv_row_ledger` と `exam_item_values` の変更をcommitする。
8. 途中で失敗した場合はその行の変更をrollbackし、行単位errorとして記録する。

利点:

- 行単位の処理状態とエラーを管理しやすい。
- 基本情報と検査結果値の紐づきが明確。
- 加入者照合結果を使って、そのまま結果値登録へ進める。
- 初期実装、ヒロオカクリニック試験、dry-runの確認に向いている。

注意点:

- 大量CSVでは1行ごとのDB書き込みが増えやすい。
- バルクinsert最適化は後から検討する。

### Future Optimization: Phase-Oriented Batch Extraction

将来の性能改善案として、CSV全体に対して基本情報だけを先に一括抽出し、その後で検査結果値だけを一括抽出する方式を検討できる。
初期実装では採用しない。

処理イメージ:

1. CSV全体に基本情報マッピングを適用する。
2. `csv_row_ledger` をまとめてupsertする。
3. identity生成・加入者照合をまとめて実行する。
4. CSV全体に検査結果値マッピングを適用する。
5. `exam_item_values` をまとめてinsertする。

利点:

- DB書き込みをバッチ化しやすい。
- 大量CSVでは性能面で有利になりやすい。
- 基本情報だけの再処理、結果値だけの再処理を分けやすい。

欠点:

- 行単位エラーと結果値エラーの関連付けが複雑になりやすい。
- 基本情報抽出後に結果値抽出で失敗した場合の中間状態管理が必要。
- 初期実装では処理分岐と再実行制御が増える。

初期実装で採用しない理由は、行単位エラーと結果値エラーの関連付け、中間状態、再処理制御が複雑になりやすいためである。
ただし、共通マッピング適用処理は、将来のバッチ処理へ転用できるよう、基本情報抽出と検査結果値抽出の関数境界は分けておく。
CSV取込ではリアルタイム性を求めないため、初期実装は1人/1行ずつ処理する方針を基本とする。

空値、非測定値語、不足基本情報の扱い:

- CSV由来の `VALUE` が完全空セルの場合は `exam_item_values` 行を作らない。
- `LOWER_LIMIT` / `UPPER_LIMIT` / `JUDGEMENT` だけが存在し、`VALUE` が完全空セルの場合も `exam_item_values` 行を作らない。
- `未実施`, `未受診`, `実施せず`, `キャンセル`, `中止`, `拒否`, `対象外` などは完全空ではないため、`exam_item_values.raw_value` に原文を残し、`normalize_reason = RAW_VALUE_NO_RESULT` として扱う。
- `測定不能`, `判定不能`, `検体不良`, `採血不可`, `測定不可` などは完全空ではないため、`exam_item_values.raw_value` に原文を残し、`normalize_reason = RAW_VALUE_UNMEASURABLE` として扱う。
- `未実施` / `測定不能` / `判定不能` などは、entry内の項目結果値として出てくる実施状態・測定可否であり、健診機関由来のABC等の健診判定とは別に扱う。
- CD/CO系で辞書一致OKになった場合は、`normalize_reason = RAW_VALUE_EXACT_MATCH` / `RAW_VALUE_NORMALIZED_MATCH` とし、原文一致か前処理後一致かを区別する。
- 型に合わない未知文字列は、`exam_item_values.raw_value` に原文を残し、`normalize_reason = INVALID_VALUE_TYPE` として扱う。
- `あり` / `なし` は共通ノイズ扱いせず、項目別ルール、CD/CO辞書、または変換ルールで扱う。
- 健診日など基本情報が不足していても、CSV取込段階ではskipしない。
- 基本情報不足の評価は、将来 `check_result` 側で追加するスコープとして扱う。
- ハートクロスCSVのようにCSV単体に健診日がないサンプルでも、別データで健診日を特定できる見込みがある場合は、暫定テンプレートとして実装検証を進める。
- 健診日未解決の行は `csv_row_ledger.exam_date = NULL` で保持し、後続checkで不足として検知できる状態にする。
- 完全空行はCSV取込段階でskipする。
- その他のフッター行、メモ行、基本情報不足行は原則skipせず、行台帳と後続checkで扱う。

既存テーブルとの関係:

- `work_other.medi_exam_result_ledger` は旧紙/Excel系の1人=1件の基本情報台帳であり、`csv_row_ledger` のカラム検討時の参照元とする。
- `health_exam_result.file_receipts` はファイル単位台帳であり、人/行単位の基本情報台帳としては共用しない。
- `health_exam_result.exam_item_values` には `interpretation_code` / `interpretation_name` はあるが、CSV由来の下限/上限専用カラムは現状ない。
- CSV由来の下限/上限は、マスタ基準値ではなく健診機関が提出した原本由来情報として扱う。
- 原本由来情報として `exam_item_values.source_reference_lower` / `source_reference_upper` を追加するmigration候補を作成する。
- CSV由来の下限/上限の単位は、結果値の `raw_unit` と同じ前提で扱う。
- 下限/上限だけ別単位で提出されるケースはかなり特殊であり、初期設計では専用単位カラムを持たない。
- ここで扱うCSV由来の判定は、法定項目の必須/不足チェックや `check_result` の評価ではなく、健診機関がCSVに出してきた検査別判定・カテゴリ総合判定を指す。
- `未実施` / `測定不能` / `判定不能` など、entry内の項目結果値として出てくる実施状態・測定可否は、この健診機関由来の健診判定とは別に扱う。
- 健診機関由来の健診判定は、健診機関ごとの判定基準、契約、事業所向け要件で意味が変わる可能性が高いため、初期実装ではPHR側の判定ロジックや納品判定には利用しない。
- XML由来の `interpretationCode` は標準コードとして扱えるが、CSV由来判定は施設固有判定である可能性が高いため、初期実装では `exam_item_values.interpretation_code` / `interpretation_code_system` / `interpretation_name` に寄せない。
- CSV原本の健診機関由来判定は証跡として保持する。最低限、`csv_row_ledger.raw_row_json` から復元できる状態にする。
- 必要になった場合は、`exam_item_values.source_judgement_raw` などの専用カラム、または健診機関別判定マスタを後続バージョンで検討する。

### Duplicate Control

CSV取込では、ファイル単位と行単位の2段階で余分な取り込みを抑制する。

- ファイル単位
  - `file_receipts.file_sha256` をCSVファイル全体のsha256として利用する。
  - 同一 `event_id` / `relative_path` / `file_sha256` のファイルは既存の `file_receipts` 一意制約で重複登録しない。
- 行単位
  - `csv_row_ledger.row_sha256` を作成する。
  - check済みでOK扱いになった同一 `row_sha256` は再取込時にskipする。
  - 同じ値でも列の並び順が変わった場合に同一sha256にならないよう、row hashは標準化セル配列を列順込みで算出する。
  - ヘッダー名だけでソートしたkey-value hashにはしない。

`row_sha256` の算出対象:

```text
encoding-normalized CSV row cells joined with delimiter including column order
```

CSV loaderが返すデータ行のセル配列を、trim等の過度な正規化はせず、BOM/改行などI/O由来の差分だけ除いた形でhash化する。
これにより、値が同じでも列順が変わったCSVは別rowとして扱える。

## Status Draft

### csv_row_ledger.row_status

- `PENDING`
- `IMPORTED`
- `WARNING`
- `ERROR`
- `SKIPPED`

`subscriber_match_status` は既存XML設計と同じく `MATCHED / CANDIDATE / NOT_FOUND / IDENTITY_ERROR / NOT_EXECUTED` を基本とする。

## Error Recording

CSV取込エラーは `health_exam_result.etl_errors` に記録する。
既存XML importと同じく、`etl_errors` の既存カラムへ寄せる。

- ファイル単位エラー:
  - `src_file = file_receipts.source_path`
  - `src_row_no` / `src_line_no` / `staging_rowid` はNULL。
  - `field` は既存XML側の `FILE` / `DB` に加え、CSV固有として `CSV`, `CSV_HEADER`, `CSV_FORMAT`, `CSV_MAPPING` を使う。
- 行単位エラー:
  - `src_row_no` / `src_line_no` を設定する。
  - `staging_rowid = csv_row_ledger.csv_row_ledger_id` として行台帳へ辿れるようにする。
- 項目単位エラー:
  - `field` に `target_namecode` または `CSV_HEADER:<header_name>` を設定する。
  - `field_value` にraw値、または `rule_id=<id>; condition_id=<id>; header=<name>` のような補足を入れる。
  - `message` に人間が読む詳細を入れる。

既存 `etl_errors` で表現できるもの:

- どのrunか: `run_id`
- どの処理か: `phase`
- どのファイルか: `src_file`
- どのCSV行か: `src_row_no` / `src_line_no`
- どの行台帳か: `staging_rowid`
- どの項目か: `field`
- raw値や補足: `field_value`
- エラー種別: `error_code`

既存 `etl_errors` だけでは構造化検索しづらいもの:

- `csv_exam_result_mapping_rule_id`
- `csv_exam_result_mapping_condition_id`
- `target_namecode` とCSVヘッダー名を同時に厳密検索する用途

初期実装ではDDL追加せず、`field` / `field_value` / `message` に寄せる。
運用上、rule単位・namecode単位のエラー集計が必要になった時点で、`etl_errors` への補助カラム追加を検討する。

CSV取込スクリプトは、`exam_item_values` 登録時に共通normalize libを同期呼び出しする。
normalize lib自体はCSV専用にせず、XML由来の後続normalizeでも使える形にする。

## Open Points
- 既存 `csv_loader` 利用スクリプトに影響を与えない形で、`docs/spec/common_lib/csv_loader.md` の `CsvLoadResult` / `CsvHeaderSet` 形式を追加APIとして拡張する。

## Existing csv_loader

CSVヘッダー読取用の共通libは既に存在する。

```text
scripts/lib/csv/csv_loader.py
```

現実装は `CSVLoader` / `load_csv()` を提供し、以下を扱える。

- UTF-8 BOM / UTF-8 / CP932 の簡易エンコーディング判定
- BOM除去
- delimiter指定
- 複数行ヘッダー読込
- 最終ヘッダー行を基準にした `header -> index` 辞書
- list行イテレータ
- dict行イテレータ
- 行数カウント

利用例:

```python
from scripts.lib.csv.csv_loader import load_csv

loader = load_csv(
    path=str(csv_path),
    header_count=1,
    delimiter=",",
)

headers = loader.get_headers()
header_map = loader.get_header_dict()
rows = loader.iter_rows()
row_count = loader.count_rows()
```

一方で、`docs/spec/common_lib/csv_loader.md` は `CsvLoadResult` / `CsvHeaderSet`、`disp_mode`、delimiter自動判定などを想定しており、現実装と一部差分がある。

`02_02_exam_result_csv_import` では、CSV読込本体スクリプトを太らせないため、`CsvLoadResult` / `CsvHeaderSet` 形式を共通lib側に追加する方針とする。
既存の `load_csv()` と `CSVLoader` は既存利用スクリプトへの影響を避けるため互換維持し、新しい関数またはオプションで構造化結果を返す。

追加API案:

```python
load_csv_result(
    path: str,
    *,
    header_count: int = 1,
    delimiter: str | None = None,
    encoding: str | None = None,
    count_rows: bool = True,
) -> CsvLoadResult
```

`CsvLoadResult` は、CSV取込本体が必要とする読込結果を1つにまとめる戻り値とする。
既存の `CSVLoader` と異なり、同名ヘッダーを辞書で潰さず、列順・context・occurrence・文字コード・delimiter・データ開始行を保持する。

想定する主な要素:

- `path`: 読み込んだCSVファイルパス。
- `encoding`: 実際に使用した文字コード。
- `delimiter`: 実際に使用した区切り文字。
- `header`: `CsvHeaderSet`。ヘッダー行、正規化列、header hash算出材料を持つ。
- `rows`: データ行のlist。初期実装では全件保持でもよいが、大容量化する場合はiterator化を検討する。
- `data_start_row_no`: CSV上のデータ開始行番号。
- `row_count`: データ行数。
- `warnings`: 文字コード推定、列数揺れなど、読込時に検出した警告。

`CsvHeaderSet` は、ヘッダー関連情報だけをまとめる。

想定する主な要素:

- `header_mode`: `NONE` / `SINGLE` / `WITH_CONTEXT`。
- `header_structure_type`: `SIMPLE_HEADER` / `GROUPED_VALUE_METHOD`。
- `header_context_rule`: `NONE` / `UPPER_HEADER` / `CARRY_FORWARD_ITEM`。
- `active_header_row_no`: 複数行ヘッダーの場合に、列指定へ使うCSV行番号。
- `header_rows`: CSV上のヘッダー行そのもの。
- `normalized_columns`: 列順込みの正規化済み列情報。各列は `column_no`, `context`, `name`, `occurrence` を持つ。
- `header_sha256`: `normalized_columns` をJSON正規化して算出したhash。

同一ヘッダー名が複数列に出る場合、`occurrence` は `context + name` ごとの左からの出現順として採番する。
このルールは `csv_format_versions.header_snapshot_json.normalized_columns` と実CSV読込時の `normalized_columns` の両方に適用する。

ハートクロスのように、1行目が表示名、2行目がfield code/namecodeのCSVでは、専用の `NAMECODE_ROW` 方式は増やさない。
`active_header_row_no = 2` とし、2行目の `INSURER_NUMBER` や `9N001000000000001` を通常の `header_name` として扱う。
1行目の表示名は `header_rows` と `normalized_columns.context` などに保持し、人間の確認用として使う。

互換方針:

- 既存 `load_csv()` の戻り値は `CSVLoader` のまま変えない。
- 既存 `CSVLoader.get_headers()`, `get_header_dict()`, `iter_rows()`, `iter_dict_rows()`, `count_rows()` は維持する。
- `CsvLoadResult` / `CsvHeaderSet` は追加dataclassとして導入する。
- delimiter自動判定は追加API側で対応する候補とし、明示指定がある場合は既存挙動と同じく指定delimiterを使う。
- header validation、mapping適用、normalizeは引き続きCSV取込側または専用libの責務とし、`csv_loader` には入れない。
