# 23 Phase1 Core DDL Detail

## 1. 目的

Phase1 Core DDL の対象7テーブルについて、DDL作成前にカラム、型、NULL可否、PK / FK / UNIQUE / INDEX、根拠、未決事項を整理する。

本資料はDDLそのものではなく、次工程で `sql/ddl/health_exam_result/` 配下にテーブル単位DDLを作成するための実装前詳細資料とする。

## 2. 参照資料

- `docs/refactor/health_exam_result/03_decisions.md`
- `docs/refactor/health_exam_result/12_v2_ddl_design_notes.md`
- `docs/refactor/health_exam_result/19_implementation_ready_summary.md`
- `docs/refactor/health_exam_result/20_implementation_plan.md`
- `docs/refactor/health_exam_result/21_dry_run_review.md`
- `sql/ddl/dev_phr/`
- `sql/export_sql/`

## 3. 既存DDL・SQL参照結果

### 3.1 sql/ddl/dev_phr

- `dev_phr.etl_runs` / `dev_phr.etl_errors` は既存ETL証跡テーブルとして参照できる。
- 既存ETL系DDLでは、IDは `bigint unsigned AUTO_INCREMENT`、時刻は `datetime(3)`、文字コードは `utf8mb4` / `utf8mb4_ja_0900_as_cs` が使われている。
- `dev_phr.etl_errors.run_id` は `dev_phr.etl_runs.run_id` へのFKを持ち、`ON DELETE CASCADE` が設定されている。
- `dev_phr.events.event_id` は `BIGINT`、`dev_phr.subscribers.id` は `bigint unsigned` として定義されている。
- `dev_phr.exam_item_master` は `namecode char(17)` と `identity_item_code varchar(32)` を持つ。
- `dev_phr.exam_item_group_*` 系DDLは、制度チェックマスタ参照用であり、Phase1 Core DDLの作成対象ではない。
- `0040_dev_phr__exam_item_group_indentity_members.sql` はファイル名に `indentity` のtypoがあるが、テーブル名は `exam_item_group_identity_members` である。

### 3.2 sql/export_sql

- `exam_item_master.sql` は `exam_item_master` の初期データ参照元であり、`namecode` と `identity_item_code` の対応確認に使う。
- `exam_item_groups.sql`、`exam_item_group_identity_members.sql`、`exam_item_group_members.sql`、`exam_item_group_method_members.sql` は制度チェック用マスタデータの参照元である。
- export SQL は dev_phr 側マスタの初期データ確認資料であり、Phase1 Core 7テーブルのDDLカラム定義を直接確定する資料ではない。

## 4. Phase1 Core 7テーブル確定表

未決または人間判断が必要な型・制約は、推測で確定せず「未決」または「要確認」と記載する。

### 4.1 etl_runs

| カラム名 | 型 | NULL可否 | デフォルト | PK/FK/UNIQUE/INDEX | 参照元・根拠 | 備考 |
|---|---|---|---|---|---|---|
| id | bigint unsigned | NOT NULL | AUTO_INCREMENT | PK | 12 / dev_phr.etl_runs | 既存DDLは `run_id`。health_exam_result側は12の候補に合わせ `id` とする前提。 |
| run_type | varchar(64) | NOT NULL | 未決 | INDEX候補 | 12 / 20 / 03 | `01_scan_files.py` 等の実行種別。値定義は人間判断。 |
| event_id | bigint | NULL可 | NULL | INDEX候補 | 12 / dev_phr.events / 03 | `dev_phr.events.event_id` 参照。cross schema FKは張らない。 |
| started_at | datetime(3) | NOT NULL | CURRENT_TIMESTAMP(3)候補 | INDEX候補 | 12 / dev_phr.etl_runs | 既存ETL系DDLに合わせる候補。 |
| finished_at | datetime(3) | NULL可 | NULL | - | 12 / dev_phr.etl_runs | Run終了時に更新する。 |
| status | varchar(32) | NOT NULL | 未決 | INDEX候補 | 12 / dev_phr.etl_runs / 03 | 既存DDLは `running/success/partial/failed`。health_exam_result側の正式値は未決。 |
| summary_message | text | NULL可 | NULL | - | 12 / 19 | 件数・スキップ・エラー概要を保持する。 |
| created_at | datetime(3) | NOT NULL | CURRENT_TIMESTAMP(3)候補 | - | 12 / dev_phr.etl_runs | 既存DDLの時刻精度に合わせる候補。 |
| updated_at | datetime(3) | NULL可 | ON UPDATE CURRENT_TIMESTAMP(3)候補 | - | 12 / dev_phr系DDL | 既存ETL系にはないが12候補に含まれる。採用詳細はDDL時確認。 |

### 4.2 etl_errors

| カラム名 | 型 | NULL可否 | デフォルト | PK/FK/UNIQUE/INDEX | 参照元・根拠 | 備考 |
|---|---|---|---|---|---|---|
| id | bigint unsigned | NOT NULL | AUTO_INCREMENT | PK | 12 / dev_phr.etl_errors | 既存DDLは `error_id`。health_exam_result側は12の候補に合わせ `id` とする前提。 |
| run_id | bigint unsigned | NOT NULL | - | FK / INDEX | 12 / dev_phr.etl_errors / 03 | `etl_runs.id` 参照。ON DELETEの詳細は人間判断。 |
| file_receipt_id | bigint unsigned | NULL可 | NULL | FK / INDEX | 12 / 03 | ファイル単位エラーの紐付け。 |
| xml_ledger_id | bigint unsigned | NULL可 | NULL | FK / INDEX | 12 / 03 | XML単位エラーの紐付け。 |
| item_value_id | bigint unsigned | NULL可 | NULL | FK / INDEX | 12 / 03 | 項目値単位エラーの紐付け。 |
| error_type | varchar(64) | NOT NULL候補 | 未決 | INDEX候補 | 12 / 19 | エラー分類。正式値は未決。 |
| error_code | varchar(190) | NULL可 | NULL | INDEX候補 | 12 / dev_phr.etl_errors | 既存DDLの `error_code` 型を参考。 |
| error_message | text | NULL可 | NULL | - | 12 / dev_phr.etl_errors | 既存DDLは `message text`。 |
| status | varchar(32) | NOT NULL候補 | 未決 | INDEX候補 | 12 / 03 | 未解決/解決済み等の値定義は未決。 |
| resolved_by_xml_ledger_id | bigint unsigned | NULL可 | NULL | FK / INDEX候補 | 12 / 03 | 再提出XML等による解決紐付け。 |
| created_at | datetime(3) | NOT NULL | CURRENT_TIMESTAMP(3)候補 | INDEX候補 | 12 / dev_phr.etl_errors | 既存ETL系DDLに合わせる候補。 |
| resolved_at | datetime(3) | NULL可 | NULL | - | 12 | エラー解決時刻。 |

### 4.3 medical_folder_aliases

| カラム名 | 型 | NULL可否 | デフォルト | PK/FK/UNIQUE/INDEX | 参照元・根拠 | 備考 |
|---|---|---|---|---|---|---|
| alias_id | bigint unsigned | NOT NULL | AUTO_INCREMENT | PK | 12 | 12の候補名を採用。 |
| event_id | bigint | NOT NULL | - | UNIQUE候補 / INDEX | 12 / 19 / 03 / 03_medical_folder_aliases spec | event単位のフォルダ名対応。cross schema FKは張らない。 |
| src_folder_raw | varchar(255)候補 | NOT NULL | - | UNIQUE候補 | 12 / 19 / 03_medical_folder_aliases spec | 共有フォルダ上の実フォルダ名。 |
| dst_folder_norm | varchar(255)候補 | NOT NULL | - | INDEX候補 | 12 / 19 / 03_medical_folder_aliases spec | システム内部の正規フォルダ名。初期投入は原則 `src_folder_raw = dst_folder_norm`。 |
| manual_judgement | tinyint(1)候補 | NOT NULL候補 | 0候補 | - | 12 | 手動判断の有無。型・デフォルトは未決。 |
| note | text | NULL可 | NULL | - | 12 / 19 | 仮フォルダ名等の注意事項を保持する。 |
| is_active | tinyint(1)候補 | NOT NULL候補 | 1候補 | INDEX候補 | 12 | 有効/無効管理。型・デフォルトは未決。 |
| created_at | datetime(3) | NOT NULL | CURRENT_TIMESTAMP(3)候補 | - | 12 / dev_phr系DDL | 時刻精度は既存DDL参考。 |
| updated_at | datetime(3) | NULL可 | ON UPDATE CURRENT_TIMESTAMP(3)候補 | - | 12 / dev_phr系DDL | 採用詳細はDDL時確認。 |

### 4.4 file_receipts

| カラム名 | 型 | NULL可否 | デフォルト | PK/FK/UNIQUE/INDEX | 参照元・根拠 | 備考 |
|---|---|---|---|---|---|---|
| id | bigint unsigned | NOT NULL | AUTO_INCREMENT | PK | 12 | 物理ファイル資産台帳ID。 |
| event_id | bigint | NOT NULL候補 | - | INDEX / UNIQUE構成要素 | 03 / 12 / 20 | 対象イベント。cross schema FKは張らない。 |
| file_role | varchar(32) | NOT NULL候補 | 未決 | INDEX候補 | 12 | 値定義は未決。 |
| file_type | varchar(32) | NOT NULL | 未決 | INDEX候補 | 03 / 12 | ZIPは独立テーブルにせず `file_type` で扱う。値定義は未決。 |
| file_name | varchar(255)候補 | NOT NULL | - | INDEX候補 | 12 | ファイル名。 |
| file_ext | varchar(32)候補 | NULL可 | NULL | INDEX候補 | 12 | 拡張子。 |
| source_path | text または varchar(1024)候補 | NOT NULL | - | UNIQUE候補 | 12 / 19 | 実ファイルパス。型は未決。 |
| relative_path | text または varchar(1024)候補 | NOT NULL候補 | - | UNIQUE構成要素 | 12 / 19 / 03 | `event.result_root_path` からの相対パス想定。型は未決。 |
| output_path | text または varchar(1024)候補 | NULL可 | NULL | - | 12 | 将来的な出力ファイル資産台帳用途。 |
| file_sha256 | char(64) | NOT NULL | - | INDEX / UNIQUE構成要素 | 12 / 19 / 03 | 単独UNIQUEは採用しない。重複防止は `event_id`、`relative_path`、`file_sha256` の組み合わせを基本とする。 |
| file_size | bigint unsigned | NULL可 | NULL | - | 12 | バイト数。 |
| processable_count | int | NULL可 | NULL | - | 12 | ZIP内XML件数等。 |
| insurer_number | varchar(20) | NULL可 | NULL | INDEX候補 | 12 / dev_phr.etl_runs | 保険者番号。 |
| submitter_facility_code | varchar(64)候補 | NULL可 | NULL | INDEX候補 | 12 | 提出元機関コード。 |
| facility_code | varchar(64)候補 | NULL可 | NULL | INDEX候補 | 12 | 健診機関コード。 |
| facility_name | varchar(255)候補 | NULL可 | NULL | INDEX候補 | 12 | 健診機関名。 |
| storage_folder_type | varchar(64) | NULL可 | NULL | INDEX候補 | 12 | 値定義は未決。 |
| status | varchar(32) | NOT NULL | `DISCOVERED`候補 | INDEX候補 | 03 / 12 / 19 | 値は `DISCOVERED / IMPORTING / IMPORTED / ERROR` で確定。DB enumは採用しない。 |
| summary_message | text | NULL可 | NULL | - | 12 / 19 | 処理結果サマリー。 |
| etl_run_id | bigint unsigned | NULL可 | NULL | FK / INDEX | 12 / 19 / 03 | 登録Runまたは処理Runとの紐付け。 |
| first_seen_at | datetime(3) | NOT NULL候補 | CURRENT_TIMESTAMP(3)候補 | INDEX候補 | 12 / 19 | 初回検出時刻。 |
| last_seen_at | datetime(3) | NULL可 | NULL | INDEX候補 | 12 / 19 | 最終検出時刻。 |
| content_checked_at | datetime(3) | NULL可 | NULL | - | 12 | 中身確認時刻。 |
| received_at | datetime(3) | NULL可 | NULL | INDEX候補 | 12 / 19 | 受領扱い時刻。 |
| processed_at | datetime(3) | NULL可 | NULL | INDEX候補 | 12 | 処理完了時刻。 |
| created_at | datetime(3) | NOT NULL | CURRENT_TIMESTAMP(3)候補 | - | 12 / dev_phr系DDL | 作成時刻。 |
| updated_at | datetime(3) | NULL可 | ON UPDATE CURRENT_TIMESTAMP(3)候補 | - | 12 / dev_phr系DDL | 更新時刻。 |

### 4.5 xml_ledger

| カラム名 | 型 | NULL可否 | デフォルト | PK/FK/UNIQUE/INDEX | 参照元・根拠 | 備考 |
|---|---|---|---|---|---|---|
| id | bigint unsigned | NOT NULL | AUTO_INCREMENT | PK | 12 | XML内容単位の一意台帳ID。 |
| event_id | bigint | NOT NULL候補 | - | INDEX候補 | 03 / 12 | `dev_phr.events.event_id` 参照。cross schema FKは張らない。 |
| subscriber_id | bigint unsigned | NULL可 | NULL | INDEX候補 | 12 / dev_phr.subscribers / 03 | 加入者照合後のID。未照合・照合不可時のNULL許容が必要。cross schema FKは張らない。 |
| hia_subscriber_id | varchar(190)候補 | NULL可 | NULL | INDEX候補 | 03 / 12 / dev_phr.subscribers | 検索用冗長保持。正ではない。 |
| xml_sha256 | char(64) | NOT NULL | - | UNIQUE | 03 / 12 / 19 | XML内容一意性の基準。 |
| document_id | varchar(190)候補 | NULL可 | NULL | INDEX候補 | 12 | XML内文書ID。型は未決。 |
| insurer_number | varchar(20) | NULL可 | NULL | INDEX候補 | 12 / dev_phr.etl_runs | 保険者番号。 |
| facility_code | varchar(64)候補 | NULL可 | NULL | INDEX候補 | 12 | 健診機関コード。 |
| facility_name | varchar(255)候補 | NULL可 | NULL | - | 12 | 健診機関名。 |
| exam_date | date | NULL可 | NULL | INDEX候補 | 12 | 健診日。 |
| name_kana_raw | varchar(255)候補 | NULL可 | NULL | - | 12 | XML raw値。 |
| name_kana_match | varchar(255)候補 | NULL可 | NULL | - | 12 | 照合用正規化値。 |
| insurance_symbol_raw | varchar(190)候補 | NULL可 | NULL | - | 12 | XML raw値。 |
| insurance_symbol_match | varchar(190)候補 | NULL可 | NULL | - | 12 | 照合用正規化値。 |
| insurance_number_raw | varchar(190)候補 | NULL可 | NULL | - | 12 | XML raw値。 |
| insurance_number_match | varchar(190)候補 | NULL可 | NULL | - | 12 | 照合用正規化値。 |
| birthdate | date | NULL可 | NULL | INDEX候補 | 12 / dev_phr.subscribers | 生年月日。 |
| gender_code | varchar(16)候補 | NULL可 | NULL | - | 12 | 性別コード。 |
| identity_hash | char(64)候補 | NULL可 | NULL | INDEX候補 | 12 / dev_phr.subscribers | 加入者照合補助。 |
| person_id_custom | varchar(190)候補 | NULL可 | NULL | INDEX候補 | 12 / dev_phr.subscribers | 加入者照合補助。 |
| subscriber_match_status | varchar(32) | NULL可 | NULL | INDEX候補 | 12 | 値定義は未決。 |
| subscriber_match_method | varchar(64) | NULL可 | NULL | - | 12 | 照合方式。値定義は未決。 |
| subscriber_match_reason | text | NULL可 | NULL | - | 12 | 照合理由。 |
| xml_status | varchar(32) | NOT NULL | `PENDING`候補 | INDEX候補 | 03 / 12 | 値は `PENDING / IMPORTED / ERROR / SKIPPED` で確定。DB enumは採用しない。 |
| xml_reason | text または varchar(190) | NULL可 | NULL | - | 03 / 12 | reason code詳細は未決。12では固定enumではない文字列方針。 |
| check_status | varchar(32) | NOT NULL | `PENDING`候補 | INDEX候補 | 03 / 12 | 値は `PENDING / OK / WARNING / NG` で確定。DB enumは採用しない。 |
| xml_export_status | varchar(32) | NOT NULL | `PENDING`候補 | INDEX候補 | 03 / 12 | 値は `PENDING / READY / EXPORTED / ERROR / SKIPPED`。DB enumは採用しない。 |
| manual_export_approved | tinyint(1)候補 | NOT NULL候補 | 0候補 | INDEX候補 | 12 | 手動承認で出力可とする情報。 |
| manual_export_reason | text | NULL可 | NULL | - | 12 | 手動承認理由。 |
| created_at | datetime(3) | NOT NULL | CURRENT_TIMESTAMP(3)候補 | - | 12 / dev_phr系DDL | 作成時刻。 |
| updated_at | datetime(3) | NULL可 | ON UPDATE CURRENT_TIMESTAMP(3)候補 | - | 12 / dev_phr系DDL | 更新時刻。 |

### 4.6 xml_file_links

| カラム名 | 型 | NULL可否 | デフォルト | PK/FK/UNIQUE/INDEX | 参照元・根拠 | 備考 |
|---|---|---|---|---|---|---|
| id | bigint unsigned | NOT NULL | AUTO_INCREMENT | PK | 12 | 物理ファイルとXML内容の対応ID。 |
| event_id | bigint | NOT NULL候補 | - | INDEX候補 | 03 / 12 | 検索性向上の冗長保持。cross schema FKは張らない。 |
| file_receipt_id | bigint unsigned | NOT NULL | - | FK / INDEX / UNIQUE構成要素 | 03 / 12 | `file_receipts.id` 参照。 |
| xml_ledger_id | bigint unsigned | NOT NULL | - | FK / INDEX / UNIQUE構成要素 | 03 / 12 | `xml_ledger.id` 参照。 |
| xml_inner_path | text または varchar(1024)候補 | NULL可 | NULL | UNIQUE構成要素 | 03 / 12 | ZIP内相対パス。単体XMLではNULL。UNIQUEは `file_receipt_id`、`xml_ledger_id`、`xml_inner_path` の組み合わせとする。 |
| created_at | datetime(3) | NOT NULL | CURRENT_TIMESTAMP(3)候補 | INDEX候補 | 12 / dev_phr系DDL | リンク作成時刻。 |

### 4.7 exam_item_values

| カラム名 | 型 | NULL可否 | デフォルト | PK/FK/UNIQUE/INDEX | 参照元・根拠 | 備考 |
|---|---|---|---|---|---|---|
| id | bigint unsigned | NOT NULL | AUTO_INCREMENT | PK | 12 | 健診項目値ID。 |
| event_id | bigint | NOT NULL候補 | - | INDEX候補 | 03 / 12 | 検索用冗長保持。cross schema FKは張らない。 |
| ledger_type | varchar(16) | NOT NULL | `XML`候補 | INDEX候補 | 03 / 12 | 現時点では `XML / CSV`。DB enumは採用しない。 |
| ledger_id | bigint unsigned | NOT NULL | - | INDEX候補 | 03 / 12 | `ledger_type` と組み合わせて由来Ledgerを表す。XML時は `xml_ledger.id`。 |
| subscriber_id | bigint unsigned | NULL可 | NULL | INDEX候補 | 03 / 12 | 検索用冗長保持。cross schema FKは張らない。 |
| hia_subscriber_id | varchar(190)候補 | NULL可 | NULL | INDEX候補 | 03 / 12 | 検索用冗長保持。正ではない。 |
| namecode | char(17) | NOT NULL | - | INDEX候補 | 12 / dev_phr.exam_item_master | JLAC10等の項目コード。 |
| occurrence_no | int | NOT NULL候補 | 1候補 | INDEX候補 | 12 | 同一XML内の同一namecode複数出現管理。 |
| raw_value | text | NULL可 | NULL | - | 12 | XML raw値。保持範囲は未決。 |
| raw_value_type | varchar(32)候補 | NULL可 | NULL | - | 12 | raw値種別。値定義は未決。 |
| raw_unit | varchar(64)候補 | NULL可 | NULL | - | 12 | raw単位。 |
| normalized_value | text | NULL可 | NULL | - | 03 / 12 / 20 | 正規化済み値。 |
| normalized_unit | varchar(64)候補 | NULL可 | NULL | - | 03 / 12 / 20 | 正規化単位。 |
| nullflavor | varchar(32)候補 | NULL可 | NULL | INDEX候補 | 12 | XMLのnullFlavor等。 |
| code_system | varchar(190)候補 | NULL可 | NULL | - | 12 | コード値項目用。 |
| code_value | varchar(190)候補 | NULL可 | NULL | INDEX候補 | 12 | コード値項目用。 |
| code_display | varchar(255)候補 | NULL可 | NULL | - | 12 | コード表示名。 |
| identity_item_code | varchar(32) | NULL可 | NULL | INDEX候補 | 03 / 12 / dev_phr.exam_item_master | `exam_item_master` から解決する。 |
| jun_no | int | NULL可 | NULL | INDEX候補 | 12 | 項目順序。 |
| normalize_status | varchar(32)候補 | NULL可 | NULL | INDEX候補 | 12 | 正規化状態。値定義は未決。 |
| normalize_reason | text | NULL可 | NULL | - | 12 | 正規化理由。 |
| validation_status | varchar(32)候補 | NULL可 | NULL | INDEX候補 | 12 / 21 | 正式値は未決。 |
| validation_reason | text | NULL可 | NULL | - | 12 | 値妥当性の理由。 |
| extracted_run_id | bigint unsigned | NULL可 | NULL | FK / INDEX | 12 / 03 | 抽出Run。 |
| extracted_at | datetime(3) | NULL可 | NULL | INDEX候補 | 12 | 抽出時刻。 |
| normalized_at | datetime(3) | NULL可 | NULL | - | 12 | 正規化時刻。 |
| created_at | datetime(3) | NOT NULL | CURRENT_TIMESTAMP(3)候補 | - | 12 / dev_phr系DDL | 作成時刻。 |
| updated_at | datetime(3) | NULL可 | ON UPDATE CURRENT_TIMESTAMP(3)候補 | - | 12 / dev_phr系DDL | 更新時刻。 |

## 5. テーブル間リレーション

| From | To | 関係 | DDL時の扱い |
|---|---|---|---|
| `etl_errors.run_id` | `etl_runs.id` | エラーはRunに属する | health_exam_result内FKを張る。 |
| `file_receipts.etl_run_id` | `etl_runs.id` | ファイル登録・処理Runとの紐付け | health_exam_result内FKを張る。Run削除時の扱いは未決。 |
| `xml_file_links.file_receipt_id` | `file_receipts.id` | 物理ファイルとXML内容の対応 | health_exam_result内FKを張る。 |
| `xml_file_links.xml_ledger_id` | `xml_ledger.id` | 物理ファイルとXML内容の対応 | health_exam_result内FKを張る。 |
| `exam_item_values.ledger_type + ledger_id` | `xml_ledger.id` など | 値の由来Ledger | 初期XML由来では `xml_ledger.id` を参照する。将来CSV対応を含むFK表現は未決。 |
| `etl_errors.file_receipt_id` | `file_receipts.id` | ファイル単位エラー | health_exam_result内FKを張る。 |
| `etl_errors.xml_ledger_id` | `xml_ledger.id` | XML単位エラー | health_exam_result内FKを張る。 |
| `etl_errors.item_value_id` | `exam_item_values.id` | 項目値単位エラー | health_exam_result内FKを張る。 |
| `event_id` 各列 | `dev_phr.events.event_id` | イベント参照 | cross schema FKは張らず、必要に応じてINDEXを付与する。 |
| `subscriber_id` 各列 | `dev_phr.subscribers.id` | 加入者参照 | cross schema FKは張らず、必要に応じてINDEXを付与する。 |

## 6. PK / FK / UNIQUE / INDEX 方針

- PKは各テーブルのID列に設定する方針とする。
- `xml_ledger.xml_sha256` はUNIQUEとする。
- `file_receipts.file_sha256` 単独UNIQUEは採用しない。
- `file_receipts` の重複防止は `event_id`、`relative_path`、`file_sha256` の組み合わせを基本とする。
- `xml_file_links` は `file_receipt_id`、`xml_ledger_id`、`xml_inner_path` の組み合わせをUNIQUEとする。
- `medical_folder_aliases` は `event_id` と `src_folder_raw` の組み合わせをUNIQUE候補とするが、正式制約は未決とする。
- status系、時刻系、`event_id`、`subscriber_id`、`hia_subscriber_id`、`xml_sha256`、`file_sha256` はINDEX候補とする。
- `dev_phr` など外部DB・外部スキーマへのcross schema FKは張らない。
- `event_id`、`subscriber_id`、`hia_subscriber_id` など外部参照・検索用カラムは必要に応じてINDEXを付与する。
- health_exam_result 内部のテーブル間FKは張る。

## 7. status / reason / varchar 候補

| 対象 | 値定義 | 確定状況 | 備考 |
|---|---|---|---|
| `file_receipts.status` | `DISCOVERED / IMPORTING / IMPORTED / ERROR` | 値は確定 | DB enumではなく `varchar` で定義する。 |
| `xml_ledger.xml_status` | `PENDING / IMPORTED / ERROR / SKIPPED` | 値は確定 | DB enumではなく `varchar` で定義する。XML単位の詳細ステータスは初期実装では持たない。 |
| `xml_ledger.check_status` | `PENDING / OK / WARNING / NG` | 値は確定 | DB enumではなく `varchar` で定義する。制度チェック総合判定の保持先。 |
| `xml_ledger.xml_export_status` | `PENDING / READY / EXPORTED / ERROR / SKIPPED` | 値は確定 | DB enumではなく `varchar` で定義する。04_export_hia_xml.py は後続フェーズ。 |
| `xml_ledger.xml_reason` | 未決 | reason code詳細は未決 | 12では固定enumではなく文字列カラム方針。 |
| `exam_item_values.validation_status` | 未決 | 未決 | 値そのものの妥当性を表す。制度チェックstatusではない。 |
| `etl_runs.status` | 未決 | 未決 | 既存dev_phrは `running / success / partial / failed`。採用可否は未決。 |
| `etl_errors.status` | 未決 | 未決 | 解決済み管理を含むか要判断。 |

## 8. 未決事項・人間判断事項

- `etl_runs.status` / `run_type` の正式値。
- `etl_errors.status` / `error_type` の正式値。
- `file_receipts.file_role` / `file_type` / `storage_folder_type` の値定義。
- `file_receipts.source_path` / `relative_path` / `output_path` の型を `varchar` にするか `text` にするか。
- `xml_status` / `check_status` / `xml_export_status` のreason code詳細。
- `exam_item_values.raw_value` の型と保持範囲。
- `exam_item_values.validation_status` の正式値。
- `event_id` の型を `dev_phr.events.event_id` に合わせて signed `bigint` とするか、health_exam_result内のID方針に合わせるか。
- `updated_at` のNULL可否と `ON UPDATE` 採用有無。

## 9. Phase1 DDL実装GO判定

判定: 条件付きGO。

Phase1 Core 7テーブルの責務、主カラム、status値の主要部分、テーブル間の関係は整理済みであり、DDL作成に進むための材料は揃っている。

ただし、型・INDEX・未確定status値には未決事項が残る。次工程でDDLを迷わず作成するには、少なくとも以下をDDL着手前に人間判断する必要がある。

- `etl_runs.status` / `run_type`、`etl_errors.status` / `error_type` の正式値。
- `file_receipts.file_role` / `file_type` / `storage_folder_type` の値定義。
- `medical_folder_aliases` のUNIQUE制約。
- `source_path` / `relative_path` / `output_path` の型。
- `exam_item_values.validation_status` の正式値をPhase1でDDL制約に含めるか、文字列カラムに留めるか。
