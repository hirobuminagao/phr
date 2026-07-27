# phr_master Initial DDL Draft

## Status

Draft.

このドキュメントは `02_02_exam_result_csv_import` の設計前調査として、`phr_master` に初期作成するテーブルのDDL案を整理する。
ここに記載するSQLは検討用であり、現時点ではDDL適用、migration作成、seed作成、スクリプト変更は行わない。

## Source CSV Check

社会保険診療報酬支払基金の全国CSVを、調査用に以下へ配置済みである。

- `docs/spec/exam_result_csv_import/downloads/Pref_00.csv`

`/Users/hiro/Downloads/Pref_00.csv` と project 配下のファイルは `cksum` が一致した。

確認結果は以下のとおり。

- ファイルサイズ: 8,705,832 bytes
- 行数: 54,713行
- データ行数: 54,712行
- 文字コード: CP932として読込可能。UTF-8としては読込不可。
- CSV列数: 全データ行で8列
- ヘッダー: `機関コード`, `機関種別`, `機関名`, `郵便番号`, `電話番号`, `機関所在地`, `ホームページ`, `経営主体`
- `機関種別` 件数:
  - `特定健診`: 34,418
  - `特定健診・指導`: 19,135
  - `特定保健指導`: 1,159

このCSVの `機関コード` は、`exam_facilities.medical_institution_code` の候補値として保持できる。
ただし、受領CSVの健診機関を一意に確定する親IDは `exam_facility_id` とし、外部コードとは分ける。
`機関種別`、`ホームページ`、`経営主体` も健診機関確認用の属性として `exam_facilities` に保持する。
データソースが支払基金CSVであることを表す専用カラムは、初期DDL案には含めない。

## Initial Tables

`phr_master` 初期DDL案では、CSV健診結果取込に直接必要な以下を対象とする。

- `exam_facilities`
- `medical_folder_aliases`
- `exam_item_concept_groups`
- `exam_item_concept_group_members`
- `norm_variants`
- `csv_format_versions`
- `csv_exam_result_mapping_rules`
- `csv_exam_result_mapping_conditions`
- `norm_variants`

`exam_item_master`、`exam_item_groups`、`norm_rules`、保険者系マスタは、今回の初期DDL案には含めない。
`norm_variants` は旧「紙→Excel→DB 2テーブル直接投入→normalize→export」フローで実利用されているCD/CO系結果値名寄せ辞書であり、normalize共通libから参照する。
今回のCSV健診結果取込で必要な共通マスタとして、`phr_master` 初期DDLに含める。
旧 `dev_phr.norm_variants` の廃止タイミングは今回決めず、CSV取込実装後に別途判断する。
CSVテンプレート登録では、CSVファイル内に存在する `値` / `下限` / `上限` / `判定` 列を、対象 `namecode` の取り込み対象として設定できる必要がある。
一方で、マスタとしての基準範囲・判定ルールは別論点であり、`exam_item_reference_ranges` は初期DDL案にはまだ含めない追加候補として扱う。

`exam_item_concept_groups` / `exam_item_concept_group_members` は正式テーブルとして扱う。
付属2由来の `identity_item_code` 197件は物理seedとして保持し、CSVテンプレート登録画面の候補探索に使う。
血糖・脂質などの入力支援bundleも同じテーブルで `concept_group_kind = INPUT_BUNDLE` として扱う。

## DDL Draft

```sql
CREATE DATABASE IF NOT EXISTS `phr_master`
  DEFAULT CHARACTER SET utf8mb4
  COLLATE utf8mb4_ja_0900_as_cs;
```

### exam_facilities

健診機関そのものを表す親マスタ。
支払基金CSVなどの外部コードと、内部で安定して参照する `exam_facility_id` を分ける。

```sql
CREATE TABLE `phr_master`.`exam_facilities` (
  `exam_facility_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `exam_facility_code` varchar(64) DEFAULT NULL,
  `exam_facility_name` varchar(255) NOT NULL,
  `exam_facility_display_name` varchar(255) DEFAULT NULL,
  `exam_facility_type` varchar(64) DEFAULT NULL,
  `medical_institution_code` varchar(64) DEFAULT NULL,
  `reservation_system_medical_institution_code` varchar(64) DEFAULT NULL,
  `postal_code` varchar(16) DEFAULT NULL,
  `address` varchar(512) DEFAULT NULL,
  `phone_number` varchar(64) DEFAULT NULL,
  `website_url` varchar(1024) DEFAULT NULL,
  `management_entity` varchar(255) DEFAULT NULL,
  `note` text,
  `is_active` tinyint(1) NOT NULL DEFAULT 1,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`exam_facility_id`),
  UNIQUE KEY `uq_exam_facilities_code` (`exam_facility_code`),
  KEY `idx_exam_facilities_medical_institution_code` (`medical_institution_code`),
  KEY `idx_exam_facilities_reservation_system_code` (`reservation_system_medical_institution_code`),
  KEY `idx_exam_facilities_type` (`exam_facility_type`),
  KEY `idx_exam_facilities_name` (`exam_facility_name`),
  KEY `idx_exam_facilities_active` (`is_active`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
```

### medical_folder_aliases

受領フォルダ名の揺れ、イベント別配置、フォルダ正規化名を扱う子情報。
既存テーブル名と `event_id + src_folder_raw` の一意性は維持し、`exam_facility_id` を追加する。

cross schema FK は張らず、整合性は移行SQL、検査SQL、アプリケーション側で確認する。

```sql
CREATE TABLE `phr_master`.`medical_folder_aliases` (
  `alias_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `event_id` bigint NOT NULL,
  `exam_facility_id` bigint unsigned DEFAULT NULL,
  `src_folder_raw` varchar(255) NOT NULL,
  `dst_folder_norm` varchar(255) NOT NULL,
  `manual_judgement` tinyint(1) NOT NULL DEFAULT 0,
  `note` text,
  `is_active` tinyint(1) NOT NULL DEFAULT 1,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`alias_id`),
  UNIQUE KEY `uq_medical_folder_aliases_event_src` (`event_id`, `src_folder_raw`),
  KEY `idx_medical_folder_aliases_event` (`event_id`),
  KEY `idx_medical_folder_aliases_exam_facility` (`exam_facility_id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
```

### exam_item_concept_groups

CSVテンプレート登録で候補 `namecode` を探しやすくするための上位グループ。
付属2由来の `ANNEX2_IDENTITY` は197件を物理seedとして保持する。
血糖・脂質などの入力支援bundleは `INPUT_BUNDLE` として同じテーブルに保持する。
入力支援bundleは、画面では大きい親bundleで探し、内部では小さい意味単位の子bundleへ分けられるよう親子階層を許容する。

```sql
CREATE TABLE `phr_master`.`exam_item_concept_groups` (
  `concept_group_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `concept_group_code` varchar(64) NOT NULL,
  `concept_group_name` varchar(255) NOT NULL,
  `concept_group_kind` varchar(32) NOT NULL,
  `parent_concept_group_id` bigint unsigned DEFAULT NULL,
  `parent_concept_group_code` varchar(64) DEFAULT NULL,
  `concept_group_depth` int NOT NULL DEFAULT 0,
  `concept_group_category` varchar(64) DEFAULT NULL,
  `default_selection_mode` varchar(64) NOT NULL DEFAULT 'MULTI_ENTRY',
  `coverage_required` tinyint(1) NOT NULL DEFAULT 0,
  `source_group_code` varchar(64) DEFAULT NULL,
  `sort_no` int DEFAULT NULL,
  `note` text,
  `is_active` tinyint(1) NOT NULL DEFAULT 1,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`concept_group_id`),
  UNIQUE KEY `uq_exam_item_concept_groups_code` (`concept_group_code`),
  KEY `idx_exam_item_concept_groups_kind` (`concept_group_kind`),
  KEY `idx_exam_item_concept_groups_parent` (`parent_concept_group_id`),
  KEY `idx_exam_item_concept_groups_category` (`concept_group_category`),
  KEY `idx_exam_item_concept_groups_active` (`is_active`, `sort_no`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
```

初期の `concept_group_kind` 候補:

- `ANNEX2_IDENTITY`
  - `exam_item_master.identity_item_code` ごとの最小網羅単位。
  - 197件を物理seedとして保持する。
- `INPUT_BUNDLE`
  - 血糖、脂質、血圧など、複数の `ANNEX2_IDENTITY` を束ねる入力支援用グループ。
  - 親bundleと子bundleの階層を許容する。
  - 例: `LIPID_RELATED` 配下に `TRIGLYCERIDE`, `HDL`, `LDL`, `NON_HDL`, `TOTAL_CHOLESTEROL` を置く。

### exam_item_concept_group_members

上位グループに所属する候補 `identity_item_code` / `namecode` を表す。
`ANNEX2_IDENTITY` では、同じ `identity_item_code` 配下の全 `namecode` をmemberとして保持する。
`INPUT_BUNDLE` では、束ねる `identity_item_code` を中心に保持し、必要に応じて個別 `namecode` も持つ。

```sql
CREATE TABLE `phr_master`.`exam_item_concept_group_members` (
  `concept_group_member_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `concept_group_id` bigint unsigned NOT NULL,
  `concept_group_code` varchar(64) NOT NULL,
  `identity_item_code` varchar(32) DEFAULT NULL,
  `namecode` char(17) DEFAULT NULL,
  `member_role` varchar(32) NOT NULL DEFAULT 'RESULT_VALUE',
  `selection_hint` varchar(64) DEFAULT NULL,
  `priority` int NOT NULL DEFAULT 100,
  `note` text,
  `is_active` tinyint(1) NOT NULL DEFAULT 1,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`concept_group_member_id`),
  UNIQUE KEY `uq_exam_item_concept_group_members_target` (`concept_group_code`, `identity_item_code`, `namecode`),
  KEY `idx_exam_item_concept_group_members_group` (`concept_group_id`),
  KEY `idx_exam_item_concept_group_members_identity` (`identity_item_code`),
  KEY `idx_exam_item_concept_group_members_namecode` (`namecode`),
  KEY `idx_exam_item_concept_group_members_active` (`is_active`, `priority`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
```

### norm_variants

CD/CO系の結果値名寄せ辞書。
既存 `dev_phr.norm_variants` を `phr_master` へ移設し、CSV取込と将来のXML由来normalizeで共通参照する。

初期実装では、`result_code_oid + raw_value_utf8` の完全一致で辞書を引く。
`raw_token_norm` は既存DDLに合わせて保持するが、初期の照合キー本命にはしない。

```sql
CREATE TABLE `phr_master`.`norm_variants` (
  `variant_id` bigint NOT NULL AUTO_INCREMENT COMMENT '揺れ辞書ID',
  `result_code_oid` varchar(128) NOT NULL COMMENT '結果コードOID（CD/CO系の辞書キー）',
  `raw_token_norm` varchar(190) NOT NULL COMMENT '入力値を前処理した照合トークン',
  `raw_value_utf8` varchar(190) NOT NULL DEFAULT '' COMMENT '入力値（揺れ受け止め用：照合キー本命）',
  `normalized_code` varchar(64) NOT NULL COMMENT '正規化後コード',
  `code_system` varchar(190) DEFAULT NULL COMMENT 'codeSystem',
  `display_name` varchar(255) DEFAULT NULL COMMENT '運用表示名',
  `is_canonical` tinyint NOT NULL DEFAULT 0 COMMENT '1=正規値（このOIDの代表）',
  `priority` smallint NOT NULL DEFAULT 100 COMMENT '複数マッチ時の優先度（小さいほど優先）',
  `is_active` tinyint NOT NULL DEFAULT 1 COMMENT '1=有効/0=無効',
  `note` text COMMENT '備考',
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),

  PRIMARY KEY (`variant_id`),
  UNIQUE KEY `uq_norm_variants_oid_rawvalue_utf8` (`result_code_oid`, `raw_value_utf8`),
  KEY `idx_norm_variants_oid_canonical` (`result_code_oid`, `is_canonical`, `priority`),
  KEY `idx_norm_variants_oid_normcode` (`result_code_oid`, `normalized_code`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
```

既存exportは `sql/export_sql/norm_variants.sql` にあり、812行、`result_code_oid` は94種類である。
`phr_master` 初期seed作成時はこのexportを元に、投入先schemaを `phr_master` へ置き換えて利用する。
旧 `dev_phr.norm_variants` の廃止タイミングは今回決めず、CSV取込実装後に参照切替と運用影響を確認してから別途判断する。

### csv_format_versions

健診機関ごとのCSVフォーマットバージョンを表す親テーブル。
`02_02_exam_result_csv_import` は `file_receipts.exam_facility_id` を起点に、有効なフォーマットを選択する。
CSVヘッダーの解釈方式は、健診機関単位ではなく `csv_format_versions` ごとに保持する。
同じ健診機関でも、健診基幹システムのテンプレート選択、施設別、健保別出力によりヘッダー構造が変わるためである。
画面上のベース設定としては、健診機関、mapping version、ヘッダー設定、データ開始行を上位に持つ。
ヘッダー設定は `header_mode` とし、取り込み処理向けの詳細として `header_structure_type` / `header_context_rule` を併用する案とする。
健診基幹システム側のテンプレート変更による静かな欠落を防ぐため、format versionには想定ヘッダーのhashとsnapshotを保持する。
登録済みヘッダーに含まれる未マッピング列は、テンプレート登録時に不要と判断した意図的な非取込列として扱う。

`mapping_version` は、健診機関番号など機械的な識別子を詰め込むためではなく、人がテンプレートを識別しやすい名称として付ける。
健診機関番号は `exam_facilities.medical_institution_code` / `exam_facility_code` で保持するため、`mapping_version` には施設略称、対象年月、パターン、版を含める案を基本とする。
例: `HIROOKA_2026_05_PATTERN_A_V1`, `HEARTCROSS_2026_05_PATTERN_B_V1`。

`valid_from` は明示指定を基本とし、未指定の場合は登録日を適用開始日として扱う。
`valid_to` はNULLを無期限として扱う。
フォーマット変更が分かった場合に、旧versionの `valid_to` を設定し、新versionを追加する。

```sql
CREATE TABLE `phr_master`.`csv_format_versions` (
  `csv_format_version_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `exam_facility_id` bigint unsigned NOT NULL,
  `mapping_version` varchar(64) NOT NULL,
  `file_type` varchar(32) NOT NULL DEFAULT 'CSV',
  `format_name` varchar(255) DEFAULT NULL,
  `has_header` tinyint(1) NOT NULL DEFAULT 1,
  `header_mode` varchar(64) NOT NULL DEFAULT 'SINGLE',
  `header_structure_type` varchar(64) NOT NULL DEFAULT 'SIMPLE_HEADER',
  `header_context_rule` varchar(64) DEFAULT NULL,
  `active_header_row_no` int DEFAULT NULL,
  `data_start_row_no` int NOT NULL DEFAULT 2,
  `header_sha256` char(64) DEFAULT NULL,
  `header_snapshot_json` json DEFAULT NULL,
  `header_hash_status` varchar(32) NOT NULL DEFAULT 'UNVERIFIED',
  `header_mismatch_policy` varchar(64) NOT NULL DEFAULT 'ALLOW_AFTER_CONFIRM',
  `allow_column_no_rules` tinyint(1) NOT NULL DEFAULT 0,
  `duplicate_row_policy` varchar(64) NOT NULL DEFAULT 'SKIP_CHECKED_OK',
  `missing_basic_info_policy` varchar(64) NOT NULL DEFAULT 'IMPORT_AND_CHECK_LATER',
  `character_encoding` varchar(32) NOT NULL DEFAULT 'CP932',
  `delimiter` varchar(8) NOT NULL DEFAULT ',',
  `quote_char` varchar(8) DEFAULT '"',
  `valid_from` date DEFAULT NULL,
  `valid_to` date DEFAULT NULL,
  `note` text,
  `is_active` tinyint(1) NOT NULL DEFAULT 1,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`csv_format_version_id`),
  UNIQUE KEY `uq_csv_format_versions_facility_version` (`exam_facility_id`, `mapping_version`),
  KEY `idx_csv_format_versions_header_sha256` (`header_sha256`),
  KEY `idx_csv_format_versions_header_mismatch_policy` (`header_mismatch_policy`),
  KEY `idx_csv_format_versions_facility_active` (`exam_facility_id`, `is_active`),
  KEY `idx_csv_format_versions_validity` (`valid_from`, `valid_to`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
```

採用する初期の `header_mode`:

- `NONE`
  - ヘッダーなし。
  - 列番号指定を主に使う。
- `SINGLE`
  - 単一行ヘッダー。
  - `header_structure_type = SIMPLE_HEADER`, `header_context_rule = NONE` を基本とする。
- `WITH_CONTEXT`
  - contextありヘッダー。
  - 2行ヘッダー、または `血圧, 値, 方式` のような持ち回りcontext形式を対象とする。
  - 詳細なcontext生成方法は `header_context_rule` で表す。

ハートクロスのように2行目にfield code/namecodeがあるCSVも、専用の `NAMECODE_ROW` 方式は増やさず、`WITH_CONTEXT` の複数行ヘッダーとして扱う。
2行目コード/namecodeは、通常のヘッダー名指定で使う実ヘッダーとして扱う。
そのため、複数行ヘッダーでは `active_header_row_no` に列指定へ使うCSV行番号を保持する案を基本とする。
例: 1行目が表示名、2行目がfield code/namecode、3行目からデータの場合、`active_header_row_no = 2`, `data_start_row_no = 3` とする。
専用の `header_code` / `header_namecode` カラムは初期実装では追加しない。

`data_start_row_no` は1始まりのCSV行番号とする。
例: 1行目がヘッダー、2行目からデータの場合は `2`。
2行ヘッダーで3行目からデータの場合は `3`。

`header_sha256` は、CSVテンプレート登録時に確認したヘッダー構造の指紋である。
実取込時には対象CSVから同じ手順でヘッダー指紋を算出し、`csv_format_versions.header_sha256` と照合する。
不一致の場合は、未確認の新規列・削除列・列順変更などテンプレート変更の可能性があるため、初期実装では自動続行しない。
続行する場合は、format側で確認後Goを許可し、かつ `file_receipts` 側に人が内容確認済みでGoした証跡がある場合に限る。
一致する場合、`header_snapshot_json` に含まれる未マッピング列は意図した欠落列として扱い、未マッピングであること自体はエラーにしない。

`header_snapshot_json` には、ハッシュ元を確認できるように、正規化済みヘッダー情報を保持する。
JSONは検索・制御の主軸ではなく、確認用snapshotとして使う。
取込制御に使う `header_sha256`, `header_hash_status`, `header_mode`, `header_structure_type`, `data_start_row_no` は通常カラムとして保持する。
例:

```json
{
  "header_mode": "WITH_CONTEXT",
  "header_structure_type": "GROUPED_VALUE_METHOD",
  "header_context_rule": "UPPER_HEADER",
  "data_start_row_no": 3,
  "header_rows": [
    ["基本情報", "基本情報", "血糖", "血糖"],
    ["氏名", "健診日", "値", "方式"]
  ],
  "normalized_columns": [
    {"column_no": 1, "context": "基本情報", "name": "氏名", "occurrence": 1},
    {"column_no": 2, "context": "基本情報", "name": "健診日", "occurrence": 1},
    {"column_no": 3, "context": "血糖", "name": "値", "occurrence": 1},
    {"column_no": 4, "context": "血糖", "name": "方式", "occurrence": 1}
  ]
}
```

`header_sha256` の算出対象は、文字コード変換後、前後空白除去、改行差異吸収などの標準化後に、列順込みの `normalized_columns` 配列をJSON正規化したものとする案を基本とする。
列順を含めることで、同一ヘッダー名でも列順変更を検知できる。

同一ヘッダー名が複数列に出るCSVでは、`occurrence` を `context + name` ごとの左からの出現順として採番する。
たとえば同一context内に `値` が3回出る場合、左から `occurrence = 1`, `2`, `3` とする。
`header_context_rule = CARRY_FORWARD_ITEM` の場合も、取込時にシステムが検査項目名を推測するのではなく、手動登録済みの `header_snapshot_json.normalized_columns` と同じ解釈で `context`, `name`, `occurrence` を再現する。
これにより、画面モック上の「投入先namecode / 値 / 条件 / 条件」のような複数条件指定を、`header_context`, `header_name`, `header_occurrence` で一意に列解決できる。

初期実装では、1つの `csv_format_versions` / `mapping_version` に対して登録できるヘッダーは1種類とする。
ヘッダー名の表記ゆれは自動吸収せず、ヘッダー名や列構造が違うCSVは別 `mapping_version` として明示登録する。
ルールやマッピングは完全自動生成せず、健診機関・mapping versionごとの初回テンプレートは、人がCSV実物を確認して手動登録する。
`CARRY_FORWARD_ITEM` は自動推測エンジンではなく、手動登録済みの `header_snapshot_json.normalized_columns` に従って、持ち回りcontext形式のCSVヘッダー構造を再現する方式として扱う。
将来、同一mappingに複数ヘッダーを紐づけたくなった場合は、人が確認済みのheader variantを追加する別テーブルを検討する。
ただし、初期DDLにはヘッダーaliasやN対N自動マッチング用テーブルは含めない。

`header_snapshot_json` の代替案:

| option | 内容 | メリット | デメリット |
|---|---|---|---|
| JSON snapshot | ヘッダー行と正規化列をJSONで保持 | 2行ヘッダー、context、occurrenceなど可変構造を保持しやすい | JSONを好まない場合、手で読みづらい |
| 子テーブル化 | `csv_format_header_columns` のような列単位テーブルを作る | SQLで検索・差分確認しやすい | テーブルが増え、ヘッダー行そのものの復元がやや重い |
| text snapshot | TSV/CSV風に文字列保持 | 実装が簡単 | 構造化比較に弱い |

初期は JSON snapshot とする。
理由は、制御に必要な値は通常カラムへ出し、JSONは人間確認と再hash用の構造保存に限定できるためである。

`header_mismatch_policy` は、ヘッダー不一致時にformat側として確認後Goを許せるかを表す。
採用する初期値:

- `ALLOW_AFTER_CONFIRM`
  - file_receipts側で内容確認済みGoが出ている場合だけ続行できる。
  - 初期値。
- `STOP`
  - 不一致時は停止する。
- `IMPORT_RESOLVABLE_WITH_WARNING`
  - ヘッダー不一致でも、必要なmapping列を安全に解決できる場合はwarningとして取込を進める。
  - 初期実装では採用せず、将来の緩和候補とする。

`allow_column_no_rules` は、列番号指定ruleを許可するかを表す。
初期値は `0` とし、列番号指定ruleがある場合は列ズレリスクが高いため原則停止する。

`duplicate_row_policy` は、同一 `row_sha256` の再処理方針を表す。
採用する初期値:

- `SKIP_CHECKED_OK`
  - check済みOK扱いの同一行はskipする。
- `REPROCESS`
  - 同一行でも再処理する。

`missing_basic_info_policy` は、健診日など基本情報不足時の取込方針を表す。
採用する初期値:

- `IMPORT_AND_CHECK_LATER`
  - CSV取込では止めず、後続checkで不足を扱う。
- `STOP`
  - 不足時にCSV取込段階で停止する。

血糖の `区分列で分岐` / `空腹時・随時別列` / `空腹時のみ` / `随時のみ` などの選択は、format本体の永続カラムにはしない。
これはseed生成やFastAPI入力支援の補助設定として扱い、最終的なDB表現は `csv_exam_result_mapping_rules` / `csv_exam_result_mapping_conditions` に展開されたrule群で表す。

### Mapping Rule Model

CSVテンプレート登録という入口は1つにする。
画面モックで扱う「投入先namecode、値、下限、上限、判定、複数条件」をDBへ落とすため、初期DDLでは `csv_exam_result_mapping_rules` / `csv_exam_result_mapping_conditions` を主案とする。

役割:

- `csv_format_versions`
  - 健診機関、mapping version、ヘッダー構造、データ開始行、header fingerprintを持つ。
- `csv_exam_result_mapping_rules`
  - 1つの投入先を表す親rule。
  - 基本情報なら `target_kind = LEDGER_FIELD` と `target_field`。
  - 検査値なら `target_kind = EXAM_ITEM_VALUE` と `target_namecode` / `target_identity_item_code`。
  - `selection_mode` / `selection_group_code` により、同じ値列からどれか1つへ振り分けるのか、複数entryを作るのかを表す。
- `csv_exam_result_mapping_conditions`
  - 親ruleにぶら下がるCSV上の値取得・条件判定。
  - `source_role` により、値、下限、上限、判定、方式、補助条件を表す。
  - `condition_group_no` により、同一rule内の OR/AND 条件を表す。

旧暫定案の `csv_column_mappings` は「CSV列から登録先へ」という列中心の表現であり、単純CSVでは分かりやすい。
ただし今回のモックで確認した `namecode` 中心、値/下限/上限/判定の複数source、方式条件、空腹時/随時の排他分岐を表すには親rule + 子conditionの方が自然である。
そのため、`csv_column_mappings` は実装前検討の旧案として履歴に残し、初期DDLの作成対象には含めない。

### csv_exam_result_mapping_rules

`csv_column_mappings` は「CSV列から登録先へ」寄りの定義であり、健診結果値の運用画面としては扱いづらくなる可能性がある。
健診結果CSVでは、実務上は `namecode` を中心に「このCSVではどのヘッダー条件に一致したらこの項目として採用するか」を管理したい。
そのため、登録先種別を持つ親ruleと、CSV上の探索条件を子conditionに分ける構成を採用する。
検査結果値では親ruleが `target_namecode` または `target_identity_item_code` を持つ。
基本情報では親ruleが `target_field` を持ち、条件なしの値抽出として扱う。
採用するテーブル名は `csv_exam_result_mapping_rules` / `csv_exam_result_mapping_conditions` とする。

`csv_exam_result_mapping_rules` は、CSVフォーマット内の1つの抽出・登録ルールを表す。
基本情報も検査結果値も同じ親rule形式で扱う。
rule重複の最終防止は、MySQLのUNIQUE制約ではなく、seed生成時および将来のFastAPI登録時のvalidateで行う。
これは、`target_namecode` や `target_field` などNULL許容カラムを含むrule keyをDB制約だけで安全に表しきれないためである。

```sql
CREATE TABLE `phr_master`.`csv_exam_result_mapping_rules` (
  `csv_exam_result_mapping_rule_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `csv_format_version_id` bigint unsigned NOT NULL,
  `target_kind` varchar(32) NOT NULL,
  `target_resolution_type` varchar(64) DEFAULT NULL,
  `selection_mode` varchar(64) NOT NULL DEFAULT 'DIRECT',
  `selection_group_code` varchar(64) DEFAULT NULL,
  `target_namecode` char(17) DEFAULT NULL,
  `target_identity_item_code` varchar(64) DEFAULT NULL,
  `target_field` varchar(64) DEFAULT NULL,
  `method_structure_type` varchar(64) NOT NULL DEFAULT 'SINGLE_COLUMN',
  `raw_value_type` varchar(32) DEFAULT NULL,
  `raw_unit` varchar(64) DEFAULT NULL,
  `is_required` tinyint(1) NOT NULL DEFAULT 0,
  `priority` int NOT NULL DEFAULT 100,
  `is_active` tinyint(1) NOT NULL DEFAULT 1,
  `note` text,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`csv_exam_result_mapping_rule_id`),
  UNIQUE KEY `uq_csv_exam_result_rules_format_target` (`csv_format_version_id`, `target_kind`, `target_resolution_type`, `selection_mode`, `selection_group_code`, `target_namecode`, `target_identity_item_code`, `target_field`),
  KEY `idx_csv_exam_result_rules_format` (`csv_format_version_id`),
  KEY `idx_csv_exam_result_rules_target_kind` (`target_kind`),
  KEY `idx_csv_exam_result_rules_selection_group` (`csv_format_version_id`, `selection_group_code`),
  KEY `idx_csv_exam_result_rules_namecode` (`target_namecode`),
  KEY `idx_csv_exam_result_rules_identity_item` (`target_identity_item_code`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
```

初期の `target_kind`:

- `LEDGER_FIELD`
  - `csv_row_ledger` の基本情報カラムへ登録する。
  - `target_field` を必須とし、登録先の `csv_row_ledger` カラム名を保持する。
  - 基本情報では原則 `target_resolution_type` / `target_namecode` / `target_identity_item_code` は使わない。
- `EXAM_ITEM_VALUE`
  - `exam_item_values` の検査結果値へ登録する。
  - `target_namecode` または `target_identity_item_code` を使う。
  - `target_field` は原則NULLとし、値・下限・上限・判定の区別は子conditionの `source_role` で表す。

初期の `target_resolution_type`:

- `SINGLE_NAMECODE`
  - 投入先 `target_namecode` を固定指定する。
- `IDENTITY_ITEM_CANDIDATES`
  - `target_identity_item_code` などの同一性項目を起点に、検査方法条件や補助条件から投入先 `namecode` を決める。
  - 例: 同じ検査項目でも、検査方法によって別namecodeへ振り分ける場合。

初期の `selection_mode`:

- `DIRECT`
  - `SINGLE_NAMECODE` 向け。ruleごとに固定 `target_namecode` へ登録する。
- `EXCLUSIVE_ONE`
  - 同じ候補グループ内で、条件に一致した1つの `namecode` だけを採用する。
  - 検査方式の違いにより同じCSV値をどれか1つの `namecode` へ寄せる場合に使う。
  - 排他対象の範囲は `selection_group_code` で明示する。
- `MULTI_ENTRY`
  - 同じ `target_identity_item_code` 配下でも、成立したruleをそれぞれ独立した `exam_item_values` entryとして登録する。
  - `identity_item_code` は候補探索や画面表示の括りとして使うだけで、排他性の根拠にはしない。

初期の `method_structure_type`:

- `SINGLE_COLUMN`
  - 検査方法列が1列で表現される。
- `MULTI_COLUMN`
  - 検査方法が複数列や複数条件で表現される。

### csv_exam_result_mapping_conditions

`csv_exam_result_mapping_conditions` は、親ルールに対してCSV上の列取得条件を複数持たせる。
1つの `namecode` に対して、結果値、基準下限、基準上限、判定、検査方法条件を設定できるようにする。
ここで扱う基準下限・基準上限・判定は、マスタ上の基準範囲ではなく、CSVファイル内にその列が存在する場合に取り込むCSV由来項目を指す。
単純ヘッダー、2行ヘッダー、`値` / `方式` 分解、施設テンプレート差分は条件として追加できるようにする。
ヘッダー名の表記ゆれは自動吸収せず、別 `mapping_version` として明示登録する。
基本情報の場合は原則として条件なしの `source_role = VALUE` のみを使う。
同一rule内の同一 `source_role + locator`、複数 `VALUE`、`EXCLUSIVE_ONE` の同priority衝突などは、DB制約ではなくテンプレート登録validateで検出する。

```sql
CREATE TABLE `phr_master`.`csv_exam_result_mapping_conditions` (
  `csv_exam_result_mapping_condition_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `csv_exam_result_mapping_rule_id` bigint unsigned NOT NULL,
  `condition_group_no` int NOT NULL DEFAULT 1,
  `condition_type` varchar(64) NOT NULL,
  `locator_type` varchar(64) NOT NULL DEFAULT 'HEADER_NAME',
  `header_context` varchar(255) DEFAULT NULL,
  `header_name` varchar(255) DEFAULT NULL,
  `header_occurrence` int NOT NULL DEFAULT 1,
  `column_no` int DEFAULT NULL,
  `operator` varchar(32) NOT NULL DEFAULT 'EQUALS',
  `expected_value` varchar(255) DEFAULT NULL,
  `expected_value_normalized` varchar(255) DEFAULT NULL,
  `source_role` varchar(64) NOT NULL DEFAULT 'VALUE',
  `priority` int NOT NULL DEFAULT 100,
  `is_active` tinyint(1) NOT NULL DEFAULT 1,
  `note` text,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`csv_exam_result_mapping_condition_id`),
  KEY `idx_csv_exam_result_conditions_rule` (`csv_exam_result_mapping_rule_id`),
  KEY `idx_csv_exam_result_conditions_header` (`header_context`, `header_name`, `header_occurrence`),
  KEY `idx_csv_exam_result_conditions_column_no` (`column_no`),
  KEY `idx_csv_exam_result_conditions_group` (`csv_exam_result_mapping_rule_id`, `condition_group_no`, `priority`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
```

初期の `locator_type`:

- `HEADER_NAME`
  - ヘッダー名で列を特定する。
- `COLUMN_NO`
  - 列番号で列を特定する。
  - 原則は例外用とし、列ズレ検知を強める。
- `HEADER_AND_COLUMN`
  - ヘッダー名と列番号の両方で列を特定する。
  - ヘッダー名一致に加え、期待列番号からズレていないかを検証する。

初期の `condition_type`:

- `HEADER_MATCH`
  - 指定した `header_context` / `header_name` / `header_occurrence` の列から値を取得する。
  - 指定条件から列が0件または2件以上になる場合は取込エラーとし、推測で続行しない。
  - 同値ヘッダーが複数存在するCSVでは、人が `header_context`, `header_occurrence`, `COLUMN_NO`, `HEADER_AND_COLUMN` などで一意化条件を登録する。
- `METHOD_MATCH`
  - 同じcontext内の `方式` 列などが指定値に一致する場合に採用する。
  - `expected_value = '1'` のように、検査方法条件を値として保持する。
- `VALUE_PRESENT`
  - 値列が空でない場合に採用する。
- `VALUE_MATCH`
  - 値列が指定値に一致する場合に採用する。

初期の `source_role`:

- `VALUE`
  - `exam_item_values.raw_value` に入れる値の取得元。
- `LOWER_LIMIT`
  - CSV上の基準下限列。
- `UPPER_LIMIT`
  - CSV上の基準上限列。
- `JUDGEMENT`
  - CSV上の判定列。
- `METHOD`
  - 方式判定に使う列。
- `QUALIFIER`
  - 判定補助に使う列。

この案では、処理は `namecode` ごとに有効な親ルールを取得し、子条件を評価して採用列を決める。
画面上も「健診項目namecode」単位に、値列、方式列、条件、優先順位を追加するUIにしやすい。

### CSV Value Transform Rules

`csv_value_transform_rules` は初期DDL対象から外す。

CSV raw値の機械的な前処理は、`identity_hash` と同じ思想で共通lib側へ寄せる。
つまり、DBに細かい変換ルールを持たせるのではなく、以下の責務分離を採用する。

```text
CSV raw cell
  -> value base normalize
  -> value status / no-result classification
  -> data_type specific normalize
  -> norm_variants lookup for CD/CO
  -> normalized exam item value
```

初期実装で共通lib側へ置く処理:

- NFKC
- 制御文字除去
- 前後空白除去
- 空文字を `None` へ統一
- 全角英数字などの基本吸収
- `未実施`, `未受診`, `キャンセル`, `測定不能`, `判定不能` などの非測定値語分類

DBに持つもの:

- CD/CO系の名寄せ辞書として `phr_master.norm_variants`

DBに持たないもの:

- 全角半角変換
- 空白除去
- 制御文字除去
- 単位変換
- 施設由来判定の意味変換

`csv_exam_result_mapping_rules.transform_rule_code` は初期DDLに含めない。
将来、項目別明示変換が必要になった時点で、用途名と仕様を決めてmigration追加する。

### exam_item_reference_ranges

`namecode` ごとのマスタ基準範囲・判定ルールを表す候補テーブル。
基準範囲は性別、年齢、健診機関、保険者、検査方法、年度などで変わる可能性があるため、将来追加する場合は `exam_item_master` の固定カラムではなく別テーブルを基本とする。
現時点では初期DDLに含めず、normalize/validationの詳細設計時に正式採用を判断する。
これはCSVテンプレート内で設定する `LOWER_LIMIT` / `UPPER_LIMIT` / `JUDGEMENT` とは別である。

```sql
CREATE TABLE `phr_master`.`exam_item_reference_ranges` (
  `exam_item_reference_range_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `namecode` char(17) NOT NULL,
  `sex_code` varchar(16) DEFAULT NULL,
  `age_min` int DEFAULT NULL,
  `age_max` int DEFAULT NULL,
  `exam_facility_id` bigint unsigned DEFAULT NULL,
  `insurer_number` varchar(20) DEFAULT NULL,
  `method_code` varchar(64) DEFAULT NULL,
  `reference_lower` decimal(18,6) DEFAULT NULL,
  `reference_upper` decimal(18,6) DEFAULT NULL,
  `reference_unit` varchar(64) DEFAULT NULL,
  `judgement_rule_code` varchar(64) DEFAULT NULL,
  `valid_from` date DEFAULT NULL,
  `valid_to` date DEFAULT NULL,
  `priority` int NOT NULL DEFAULT 100,
  `is_active` tinyint(1) NOT NULL DEFAULT 1,
  `note` text,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`exam_item_reference_range_id`),
  KEY `idx_exam_item_reference_ranges_namecode` (`namecode`),
  KEY `idx_exam_item_reference_ranges_facility` (`exam_facility_id`),
  KEY `idx_exam_item_reference_ranges_insurer` (`insurer_number`),
  KEY `idx_exam_item_reference_ranges_validity` (`valid_from`, `valid_to`),
  KEY `idx_exam_item_reference_ranges_priority` (`priority`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
```

初期の `judgement_rule_code` 候補:

- `NUMERIC_RANGE`
  - `reference_lower` / `reference_upper` で低値・基準範囲内・高値を判定する。
- `CODE_VALUE`
  - CD/COなどの正規化コードから判定する。
- `TEXT_MATCH`
  - テキスト値や所見文言から判定する。
- `NONE`
  - 判定しない。

## health_exam_result Impact Draft

`health_exam_result.file_receipts` は、XML側の実装と同じくファイル単位の受領・現在状態を表す。
既存の `status` / `summary_message` / `processable_count` / `content_checked_at` / `processed_at` をCSVでも利用し、CSV専用の `csv_status` は追加しない。

`health_exam_result.file_receipts` には、CSV取込のマッピング選択に必要な健診機関IDと、XML側では表現できないCSVヘッダー照合情報、停止後Goの証跡だけを追加する。
既存の `facility_code` / `facility_name` は、scan時にlookupした健診機関コード・名称のスナップショットとして利用する案とする。
この変更は `phr_master` 初期DDLとは別のmigration候補として扱い、現時点では適用しない。

### ZIP Password Lookup Impact

XML取込では、暗号ZIPのパスワード解決時に `file_receipts.facility_code` / `submitter_facility_code` / 受領フォルダ名を候補として `work_other.medi_zip_passwords` を参照している。
そのため、scan時に `facility_code` へ `exam_facility_code` を入れる変更は、ZIPパスワード解決の影響範囲に含める。

初期実装では以下を原則とする。

- XML基本情報の施設コード・施設名は、従来通りXML本文から抽出して `xml_ledger.facility_code` / `facility_name` へ保存する。
- `file_receipts.facility_code` / `facility_name` はscan時点の施設スナップショットであり、XML本文由来の正情報とは分けて扱う。
- ZIPパスワード解決は、既存互換のため `facility_folder_name` 一致を必ず残す。
- `facility_code` 一致は、`file_receipts.facility_code` と `submitter_facility_code` の候補検索を維持する。
- `exam_facility_id` によるパスワード解決は初期DDLでは追加しない。必要になった場合のみ、`medi_zip_passwords` の移設または拡張時に検討する。

これにより、旧フォルダコードと支払基金由来コードが異なる施設でも、既存のフォルダ名ベースのパスワード解決を壊さない。

```sql
ALTER TABLE `health_exam_result`.`file_receipts`
  ADD COLUMN `exam_facility_id` bigint unsigned DEFAULT NULL AFTER `facility_name`,
  ADD COLUMN `actual_header_sha256` char(64) DEFAULT NULL AFTER `exam_facility_id`,
  ADD COLUMN `matched_csv_format_version_id` bigint unsigned DEFAULT NULL AFTER `actual_header_sha256`,
  ADD COLUMN `import_resume_approved` tinyint(1) NOT NULL DEFAULT 0 AFTER `summary_message`,
  ADD COLUMN `import_resume_approved_at` datetime(3) DEFAULT NULL AFTER `import_resume_approved`,
  ADD COLUMN `import_resume_approved_by` varchar(190) DEFAULT NULL AFTER `import_resume_approved_at`,
  ADD COLUMN `import_resume_approved_reason` text AFTER `import_resume_approved_by`,
  ADD COLUMN `import_resume_scope` varchar(64) DEFAULT NULL AFTER `import_resume_approved_reason`,
  ADD KEY `idx_file_receipts_exam_facility` (`exam_facility_id`),
  ADD KEY `idx_file_receipts_actual_header` (`actual_header_sha256`),
  ADD KEY `idx_file_receipts_csv_format_version` (`matched_csv_format_version_id`),
  ADD KEY `idx_file_receipts_import_resume` (`import_resume_approved`);
```

CSV行単位の加入者突合、健診結果値処理、check/export状態は `csv_row_ledger` に持たせる。
`file_receipts` には `subscriber_match_*` / `exam_item_*` / `csv_status` / `csv_reason` を追加しない。

CSV由来の下限/上限は、マスタ基準値ではなく健診機関が提出した原本由来情報として `exam_item_values` に保持する。
現状の `exam_item_values` には下限/上限専用カラムがないため、以下のmigration候補を作成する。

```sql
ALTER TABLE `health_exam_result`.`exam_item_values`
  ADD COLUMN `source_reference_lower` text COMMENT '原本由来の基準下限。CSV等で健診機関が提出した値を保持する'
    AFTER `raw_unit`,
  ADD COLUMN `source_reference_upper` text COMMENT '原本由来の基準上限。CSV等で健診機関が提出した値を保持する'
    AFTER `source_reference_lower`;
```

CSV由来の下限/上限の単位は、結果値の `raw_unit` と同じ前提で扱う。
下限/上限だけ別単位で提出されるケースはかなり特殊であり、初期設計では専用単位カラムを持たない。

実ファイル案:

```text
sql/migrations/health_exam_result/20260723_001_health_exam_result_add_csv_reference_bounds_to_exam_item_values.sql
```

ここで扱うCSV由来の判定は、法定項目の必須/不足チェックや `check_result` の評価ではなく、健診機関がCSVに出してきた検査別判定・カテゴリ総合判定を指す。
`未実施` / `測定不能` / `判定不能` など、entry内の項目結果値として出てくる実施状態・測定可否は、この健診機関由来の健診判定とは別に扱う。
健診機関由来の健診判定は、健診機関ごとの基準、契約、事業所向け要件で意味が変わる可能性が高いため、初期実装ではPHR側の判定ロジックや納品判定には利用しない。
XML由来の `interpretationCode` は標準コードとして扱えるが、CSV由来判定は施設固有判定である可能性が高いため、初期実装では `exam_item_values.interpretation_code` / `interpretation_code_system` / `interpretation_name` に寄せない。
CSV原本の健診機関由来判定は証跡として保持する。最低限、`csv_row_ledger.raw_row_json` から復元できる状態にする。
必要になった場合は、`exam_item_values.source_judgement_raw` などの専用カラム、または健診機関別判定マスタを後続バージョンで検討する。

## Import Flow Notes

1. `01_scan_files.py` は受領フォルダ名から `phr_master.medical_folder_aliases` を引き、`exam_facility_id` を確定する。
2. `01_scan_files.py` は `health_exam_result.file_receipts` に `exam_facility_id` を登録し、既存 `facility_code` / `facility_name` にlookupした健診機関コード・名称をスナップショットとして登録する。
3. XML/ZIP取込では、暗号ZIPのパスワード解決時に既存通り `file_receipts.facility_code` / `submitter_facility_code` / 受領フォルダ名を候補にする。
4. `02_02_exam_result_csv_import` は `file_receipts` 起点で対象CSVを取得する。
5. `02_02_exam_result_csv_import` は実行開始を `etl_runs` に記録する。
6. `02_02_exam_result_csv_import` は `exam_facility_id` から `csv_format_versions` を選択する。
7. 実CSVのヘッダーから `actual_header_sha256` を算出し、`csv_format_versions.header_sha256` と照合する。
8. 対象formatに紐づく `csv_exam_result_mapping_rules` / `csv_exam_result_mapping_conditions` を取得する。
9. `target_kind = 'LEDGER_FIELD'` のruleはCSV行台帳の基本情報へ反映する。
10. `target_kind = 'EXAM_ITEM_VALUE'` のruleは条件成立時に `exam_item_values` を作成し、`source_role = VALUE` を `raw_value`、`LOWER_LIMIT` / `UPPER_LIMIT` をCSV由来下限/上限として反映する。`JUDGEMENT` は健診機関由来の健診判定列を表し、初期実装では原本証跡として扱い、PHR側判定には使わない。
11. normalize共通libを呼び、normalize系カラムへ反映する。
12. 行単位の状態は `csv_row_ledger`、ファイル単位の現在状態は `file_receipts`、実行履歴は `etl_runs` / `etl_errors` に記録する。

## Remaining Points

初期実装前に決める、またはseed作成時に確定するもの:

- `exam_facility_type` の初期コード体系。
- `reservation_system_medical_institution_code` の実データ由来と桁数。

後続バージョンで扱うもの:

- `exam_facility_type` の正式コード体系。
- 項目別明示変換が必要になった場合の追加migration方針。
- 将来、契約・請求側と接続する段階で、医療機関番号を正規管理する `medical_institutions` 相当のマスタを追加し、`exam_facilities` と紐づける移行方針。
