# Table DDL Summary by Codex

## 調査日時

2026-06-25 16:35 JST

---

## 全体概要

`07_current_script_specs_codex.md` に出現するテーブルと、health_exam_result v2 のDDL設計で特に確認すべき指定テーブルを対象に、`sql/ddl/*` と `sql/migrations/*` を確認した。

現行参照実装のDBは、大きく以下の領域に分かれている。

- `work_other`: 健診結果の受領・作業台帳・ZIP/XML処理ログ・XML抽出値・旧/作業用の健診結果ledgerを保持する。
- `dev_phr`: 加入者、健保、保険者番号、健診項目マスタ、正規化辞書、テンプレート系マスタを保持する。
- `phr`: 今回調査対象の `07_current_script_specs_codex.md` からは、対象テーブルのDDLを確認できなかった。

事実として、ZIP/XML受領系は `medi_shared_files`、`medi_zip_receipts`、`medi_xml_receipts`、`medi_xml_ledger`、`medi_xml_item_values` に分かれている。加えて、別系統の `medi_exam_result_ledger` / `medi_exam_result_item_values` があり、XML出力や正規化で使われている。v2では「受領ファイル」「ZIP receipt」「XML ledger」「item_values」「制度チェック台帳」「subscriber紐付け」をどこで分離・統合するかを最初に決める必要がある。

---

## DB一覧

### work_other

- `work_other.medi_shared_files`
- `work_other.medi_shared_folder_aliases`
- `work_other.medi_zip_passwords`
- `work_other.medi_import_runs`
- `work_other.medi_zip_receipts`
- `work_other.medi_zip_receipt_runs`
- `work_other.medi_xml_receipts`
- `work_other.medi_xml_receipt_runs`
- `work_other.medi_xml_process_logs`
- `work_other.medi_xml_ledger`
- `work_other.medi_xml_item_values`
- `work_other.medi_exam_result_ledger`
- `work_other.medi_exam_result_item_values`
- `work_other.etl_runs`
- `work_other.etl_errors`
- `work_other.find_xml_subscribers_list_20260128`（DDL未確認）
- `work_other.csv_header_map_submit`（DDL未確認）

### dev_phr

- `dev_phr.subscribers`
- `dev_phr.funds`
- `dev_phr.fund_insurer_numbers`
- `dev_phr.exam_item_master`
- `dev_phr.norm_variants`
- `dev_phr.etl_runs`
- `dev_phr.etl_errors`
- `dev_phr.templates`
- `dev_phr.template_mappings`

### phr

- 今回対象テーブルのDDLは確認できなかった。

---

## テーブル詳細

### work_other.medi_shared_files

#### DDL所在

`sql/ddl/work_other/0040_work_other__medi_shared_files.sql`

#### 現行での責務

共有フォルダ上で観測したファイルをファイル単位で管理する台帳。パス、サイズ、mtime、sha256、自動/手動判定、処理ステージ、ZIP内XML probe結果を保持する。

#### 主な利用スクリプト

SELECT

- `medi_shared_files_hash_zip.py`
- `medi_shared_files_auto_judge.py`
- `medi_shared_files_copy_to_input.py`

INSERT

- `medi_shared_files_scan.py`

UPDATE

- `medi_shared_files_scan.py`
- `medi_shared_files_hash_zip.py`
- `medi_shared_files_auto_judge.py`
- `medi_shared_files_copy_to_input.py`

DELETE

- 07上は確認なし。

#### Primary Key

- `shared_file_id`

#### Unique Key

- `uk_path_hash` (`path_hash`)

#### Foreign Key

- なし。

#### Index

- `idx_sha256` (`sha256`)
- `idx_stage` (`stage_status`)
- `idx_judge` (`manual_judgement`, `auto_judgement`)
- `idx_auto_judge_stage` (`auto_judgement`, `stage_status`)

#### 主要カラム

##### 識別子

- `shared_file_id`
- `path_hash`
- `sha256`

##### 原本値

- `path`
- `src_folder_raw`
- `facility_hint`
- `file_name`
- `ext`
- `file_size`
- `mtime`

##### 照合用派生値

- `dst_folder_norm`
- `sha256`
- `zip_has_xml`
- `zip_xml_count`

##### status

- `auto_judgement`
- `manual_judgement`
- `stage_status`

##### count / summary

- `file_size`
- `zip_xml_count`

##### audit / log

- `note`

##### 日時

- `first_seen_at`
- `last_seen_at`
- `created_at`
- `updated_at`
- `zip_xml_checked_at`

#### migration変更履歴

対象テーブルへの migration は確認できなかった。DDLに現行構造がまとまっている。

#### health_exam_result v2での扱い候補

分類：そのまま移設候補 / 要再設計

理由：ファイル台帳として有用。ただし `stage_status` が copy/import まで含むため、v2では File ledger と処理ステータスを分けるか検討する。

---

### work_other.medi_shared_folder_aliases

#### DDL所在

`sql/ddl/work_other/0045_work_other__medi_shared_folder_aliases.sql`

#### 現行での責務

共有フォルダ名の生値 `src_folder_raw` を、input配置用の `dst_folder_norm` に対応づける手動確定台帳。

#### 主な利用スクリプト

SELECT

- `medi_shared_files_copy_to_input.py`

INSERT

- 07上は確認なし。

UPDATE

- 07上は確認なし。運用で手作業更新される前提。

DELETE

- 07上は確認なし。

#### Primary Key

- `alias_id`

#### Unique Key

- `uk_src_folder_raw_active` (`src_folder_raw`, `is_active`)

#### Foreign Key

- なし。

#### Index

- Unique keyのみ。

#### 主要カラム

##### 識別子

- `alias_id`

##### 原本値

- `src_folder_raw`

##### 照合用派生値

- `dst_folder_norm`

##### status

- `manual_judgement`
- `is_active`

##### count / summary

- なし。

##### audit / log

- `note`

##### 日時

- `created_at`
- `updated_at`

#### migration変更履歴

対象テーブルへの migration は確認できなかった。

#### health_exam_result v2での扱い候補

分類：要再設計

理由：フォルダ名aliasは運用上必要だが、v2では医療機関・受領フォルダ・作業フォルダの正式設計と合わせて定義する必要がある。

---

### work_other.medi_zip_passwords

#### DDL所在

`sql/ddl/work_other/0046_work_other__medi_zip_passwords.sql`

注記：DDLの `CREATE TABLE` は `work_other`.`...` ではなく ``CREATE TABLE `medi_zip_passwords``` 形式。ファイル所在から `work_other` として扱う。

#### 現行での責務

暗号ZIPを開くためのパスワード候補を、施設・ZIP名・ZIP sha256単位で管理する。

#### 主な利用スクリプト

SELECT

- `medi_zip_import.py`
- `medi_xml_item_extract.py`
- `kenshin_lib/medi/xml_extract.py`

INSERT

- 07上は確認なし。

UPDATE

- 07上は確認なし。

DELETE

- 07上は確認なし。

#### Primary Key

- `zip_password_id`

#### Unique Key

- なし。

#### Foreign Key

- なし。

#### Index

- `idx_zip_pw_scope` (`scope_type`, `is_active`, `priority`)
- `idx_zip_pw_facility` (`facility_code`, `facility_folder_name`)
- `idx_zip_pw_zip_name` (`zip_name`)
- `idx_zip_pw_zip_sha` (`zip_sha256`)

#### 主要カラム

##### 識別子

- `zip_password_id`
- `facility_code`
- `zip_name`
- `zip_sha256`

##### 原本値

- `facility_folder_name`
- `password_text`

##### 照合用派生値

- なし。

##### status

- `scope_type`
- `is_active`

##### count / summary

- `priority`

##### audit / log

- `note`

##### 日時

- `created_at`
- `updated_at`

#### migration変更履歴

対象テーブルへの migration は確認できなかった。

#### health_exam_result v2での扱い候補

分類：参照のみ / 要再設計

理由：暗号ZIP対応は必要だが、平文パスワード保持は運用リスクがある。v2では保管方式・権限・監査を再設計したい。

---

### work_other.medi_import_runs

#### DDL所在

`sql/ddl/work_other/0005_work_other__medi_import_runs.sql`

#### 現行での責務

medi系ZIP/XML受領・抽出処理のrun単位ログ。

#### 主な利用スクリプト

SELECT

- `medi_xml_item_extract.py`（既存run確認）

INSERT

- `medi_zip_import.py`
- `medi_xml_item_extract.py`

UPDATE

- `medi_zip_import.py`
- `medi_xml_item_extract.py`

DELETE

- 07上は確認なし。

#### Primary Key

- `run_id`

#### Unique Key

- なし。

#### Foreign Key

- なし。

#### Index

- `idx_medi_import_runs_started_at` (`started_at`)

#### 主要カラム

##### 識別子

- `run_id`

##### 原本値

- `input_root`

##### 照合用派生値

- なし。

##### status

- `finished_at` がNULLかどうかで実行中/終了を表現。

##### count / summary

- なし。

##### audit / log

- `note`

##### 日時

- `started_at`
- `finished_at`
- `created_at`

#### migration変更履歴

対象テーブルへの migration は確認できなかった。

#### health_exam_result v2での扱い候補

分類：統合候補

理由：`etl_runs` と責務が重なる。v2では health_exam_result 用 run 管理へ統合する候補。

---

### work_other.medi_zip_receipts

#### DDL所在

`sql/ddl/work_other/0010_work_other__medi_zip_receipts.sql`

#### 現行での責務

受領ZIPの台帳。施設情報、ZIP名/パス/SHA、構造判定、DATA/XML件数、エラー情報、初回/最終検出runを保持する。

#### 主な利用スクリプト

SELECT

- `medi_shared_files_copy_to_input.py`
- `medi_zip_import.py`
- `medi_xml_item_extract.py`
- `kenshin_lib/medi/xml_extract.py`

INSERT

- `medi_zip_import.py`

UPDATE

- `medi_zip_import.py`

DELETE

- 07上は確認なし。

#### Primary Key

- `zip_receipt_id`

#### Unique Key

- `uq_medi_zip_receipts_zip_sha256` (`zip_sha256`)

#### Foreign Key

- なし。

#### Index

- `idx_medi_zip_receipts_run` (`run_id`)
- `idx_medi_zip_receipts_zip_sha256` (`zip_sha256`)
- `idx_medi_zip_receipts_facility_code` (`facility_code`)
- `idx_medi_zip_receipts_status` (`structure_status`)
- `idx_medi_zip_receipts_first_seen_run` (`first_seen_run_id`)
- `idx_medi_zip_receipts_last_seen_run` (`last_seen_run_id`)
- `idx_medi_zip_receipts_error_code` (`error_code`)

#### 主要カラム

##### 識別子

- `zip_receipt_id`
- `run_id`
- `first_seen_run_id`
- `last_seen_run_id`
- `zip_sha256`

##### 原本値

- `facility_folder_name`
- `facility_code`
- `facility_name`
- `zip_name`
- `zip_path`

##### 照合用派生値

- `zip_sha256`

##### status

- `structure_status`
- `error_code`

##### count / summary

- `data_dir_count`
- `data_xml_count`

##### audit / log

- `error_message`
- `structure_message`
- `admin_note`

##### 日時

- `first_seen_at`
- `last_seen_at`
- `created_at`
- `updated_at`

#### migration変更履歴

対象テーブルへの migration は確認できなかった。

#### health_exam_result v2での扱い候補

分類：そのまま移設候補

理由：v2初期ゴールの ZIP Receipt に直接対応する。`zip_path` は work領域パスに限定するか、共有元パスとコピー後パスを分けるか要検討。

---

### work_other.medi_zip_receipt_runs

#### DDL所在

`sql/ddl/work_other/0012_work_other_medi_zip_receipt_runs.sql`

注記：DDLの `CREATE TABLE` はDB名なし。ファイル所在から `work_other` として扱う。

#### 現行での責務

runごとにどのZIPを見たか、NEW/SEEN/UPDATED を記録する実績テーブル。

#### 主な利用スクリプト

SELECT

- 07上は確認なし。

INSERT

- `medi_zip_import.py`

UPDATE

- `medi_zip_import.py`（ON DUPLICATE KEY UPDATE）

DELETE

- 07上は確認なし。

#### Primary Key

- `zip_receipt_run_id`

#### Unique Key

- `uq_medi_zip_receipt_runs_run_sha` (`run_id`, `zip_sha256`)

#### Foreign Key

- `fk_medi_zip_receipt_runs_run`: `run_id` -> `medi_import_runs.run_id`
- `fk_medi_zip_receipt_runs_zip_receipt`: `zip_receipt_id` -> `medi_zip_receipts.zip_receipt_id`

#### Index

- `idx_medi_zip_receipt_runs_run` (`run_id`)
- `idx_medi_zip_receipt_runs_zip_receipt` (`zip_receipt_id`)

#### 主要カラム

##### 識別子

- `zip_receipt_run_id`
- `run_id`
- `zip_receipt_id`
- `zip_sha256`

##### 原本値

- なし。

##### 照合用派生値

- `zip_sha256`

##### status

- `action`

##### count / summary

- なし。

##### audit / log

- なし。

##### 日時

- `seen_at`

#### migration変更履歴

対象テーブルへの migration は確認できなかった。

#### health_exam_result v2での扱い候補

分類：統合候補

理由：run実績として有用。ただし v2のrun/event logへ統合できる可能性がある。

---

### work_other.medi_xml_receipts

#### DDL所在

`sql/ddl/work_other/0015_work_other__medi_xml_receipts.sql`

#### 現行での責務

ZIP内の個票XML単位の受領台帳。ZIP内パス、XML SHA、XML処理状態、CDA document_id、抽出メタ、item抽出状態を保持する。

#### 主な利用スクリプト

SELECT

- `medi_zip_import.py`
- `medi_xml_item_extract.py`
- `kenshin_lib/medi/xml_extract.py`

INSERT

- `medi_zip_import.py`

UPDATE

- `medi_zip_import.py`
- `kenshin_lib/medi/xml_extract.py`
- `medi_xml_item_extract.py`

DELETE

- 07上は確認なし。

#### Primary Key

- `xml_receipt_id`

#### Unique Key

- `uq_medi_xml_receipts_zip_path` (`zip_sha256`, `zip_inner_path`)

#### Foreign Key

- なし。

#### Index

- `idx_medi_xml_receipts_zip_sha256` (`zip_sha256`)
- `idx_medi_xml_receipts_status` (`status`)
- `idx_medi_xml_receipts_docid` (`document_id`)
- `idx_medi_xml_receipts_person_exam` (`person_key`, `exam_date`)
- `idx_medi_xml_receipts_first_seen_run` (`first_seen_run_id`)
- `idx_medi_xml_receipts_last_seen_run` (`last_seen_run_id`)
- `idx_medi_xml_receipts_extracted_at` (`extracted_at`)
- `idx_medi_xml_receipts_xml_sha256` (`xml_sha256`)
- `idx_medi_xml_receipts_items_extract_status` (`items_extract_status`)
- `idx_medi_xml_receipts_items_extracted_run_id` (`items_extracted_run_id`)
- `idx_items_extract_pick` (`items_extract_status`, `updated_at`)

#### 主要カラム

##### 識別子

- `xml_receipt_id`
- `zip_sha256`
- `zip_inner_path`
- `xml_sha256`
- `document_id`
- `extracted_run_id`
- `items_extracted_run_id`

##### 原本値

- `zip_inner_path`
- `file_size`
- `file_mtime`
- `facility_code`
- `facility_name`

##### 照合用派生値

- `person_key`
- `patient_name_kana`
- `insurer_number`
- `birthdate`
- `exam_date`
- `extracted_json`

##### status

- `status`
- `error_code`
- `items_extract_status`

##### count / summary

- なし。

##### audit / log

- `error_message`
- `admin_note`

##### 日時

- `file_mtime`
- `extracted_at`
- `first_seen_at`
- `last_seen_at`
- `created_at`
- `updated_at`
- `items_extracted_at`

#### migration変更履歴

対象テーブルへの migration は確認できなかった。ただし DDL上に item抽出系カラムが含まれている。

#### health_exam_result v2での扱い候補

分類：そのまま移設候補 / 要再設計

理由：XML Ledger候補の中核。ただし `medi_xml_ledger` と抽出メタが重複するため、v2では receipt と ledger の境界を定義する必要がある。

---

### work_other.medi_xml_receipt_runs

#### DDL所在

`sql/ddl/work_other/0016_work_other_medi_xml_receipt_runs.sql`

注記：DDLの `CREATE TABLE` はDB名なし。ファイル所在から `work_other` として扱う。

#### 現行での責務

runごとに検出したXMLを NEW/SEEN として記録する実績テーブル。

#### 主な利用スクリプト

SELECT

- 07上は確認なし。

INSERT

- `medi_zip_import.py`

UPDATE

- `medi_zip_import.py`（ON DUPLICATE KEY UPDATE）

DELETE

- 07上は確認なし。

#### Primary Key

- `xml_receipt_run_id`

#### Unique Key

- `uq_medi_xml_receipt_runs_run_xml` (`run_id`, `xml_sha256`)

#### Foreign Key

- `fk_medi_xml_receipt_runs_receipt`: `xml_receipt_id` -> `medi_xml_receipts.xml_receipt_id`
- `fk_medi_xml_receipt_runs_run`: `run_id` -> `medi_import_runs.run_id`

#### Index

- `idx_medi_xml_receipt_runs_run` (`run_id`)
- `idx_medi_xml_receipt_runs_xml` (`xml_sha256`)
- `idx_medi_xml_receipt_runs_receipt_id` (`xml_receipt_id`)

#### 主要カラム

##### 識別子

- `xml_receipt_run_id`
- `run_id`
- `xml_sha256`
- `xml_receipt_id`

##### 原本値

- なし。

##### 照合用派生値

- `xml_sha256`

##### status

- `action`

##### count / summary

- なし。

##### audit / log

- `message`

##### 日時

- `created_at`

#### migration変更履歴

対象テーブルへの migration は確認できなかった。

#### health_exam_result v2での扱い候補

分類：統合候補

理由：XML単位のrun履歴として有用だが、v2では process log / run event と統合できる可能性がある。

---

### work_other.medi_xml_process_logs

#### DDL所在

`sql/ddl/work_other/0017_work_other_medi_xml_process_logs.sql`

注記：DDLの `CREATE TABLE` はDB名なし。ファイル所在から `work_other` として扱う。

#### 現行での責務

XML処理ステップごとの結果ログ。well-formed、CDA index、XSD validate、item抽出、ledger反映などを記録する。

#### 主な利用スクリプト

SELECT

- 07上は確認なし。

INSERT

- `medi_zip_import.py` の XML_EXTRACT
- `medi_xml_item_extract.py`

UPDATE

- `medi_zip_import.py` の XML_EXTRACT（ON DUPLICATE KEY UPDATE）
- `medi_xml_item_extract.py`（ON DUPLICATE KEY UPDATE）

DELETE

- 07上は確認なし。

#### Primary Key

- `xml_process_log_id`

#### Unique Key

- `uq_medi_xml_process_logs_run_xml_step` (`run_id`, `xml_sha256`, `step`)

#### Foreign Key

- `fk_medi_xml_process_logs_run`: `run_id` -> `medi_import_runs.run_id`

#### Index

- `idx_medi_xml_process_logs_xml` (`xml_sha256`)
- `idx_medi_xml_process_logs_run` (`run_id`)

#### 主要カラム

##### 識別子

- `xml_process_log_id`
- `run_id`
- `xml_sha256`

##### 原本値

- なし。

##### 照合用派生値

- なし。

##### status

- `step`
- `result`

##### count / summary

- なし。

##### audit / log

- `message`

##### 日時

- `processed_at`

#### migration変更履歴

対象テーブルへの migration は確認できなかった。

#### health_exam_result v2での扱い候補

分類：そのまま移設候補

理由：XMLチェック結果と処理ログの基礎として有用。v2では制度チェックログもこの粒度に寄せるか、別台帳にするか検討する。

---

### work_other.medi_xml_ledger

#### DDL所在

`sql/ddl/work_other/0020_work_other__medi_xml_ledger.sql`

#### 現行での責務

XMLから抽出した受診者識別、保険証情報、健診日、施設情報、XSD結果、LSIO法定判定の一部を横持ちで保持するXML台帳。

#### 主な利用スクリプト

SELECT

- `normalize_db_update.py`

INSERT

- `medi_zip_import.py` の XML_EXTRACT / `kenshin_lib/medi/xml_extract.py`

UPDATE

- `medi_zip_import.py` の XML_EXTRACT / `kenshin_lib/medi/xml_extract.py`
- `normalize_db_update.py`

DELETE

- 07上は確認なし。

#### Primary Key

- `xml_ledger_id`

#### Unique Key

- `uq_medi_xml_ledger_zip_member` (`zip_sha256`, `zip_inner_path_sha256`)

#### Foreign Key

- DDL上はなし。`zip_receipt_id` へのindexはあるがFK制約は確認できない。

#### Index

- `idx_medi_xml_ledger_run` (`run_id`)
- `idx_medi_xml_ledger_zip_sha256` (`zip_sha256`)
- `idx_medi_xml_ledger_facility_code` (`facility_code`)
- `idx_medi_xml_ledger_person_hint` (`insurer_number`, `insurance_symbol`, `insurance_number`, `insurance_branch_number`, `birth_date`)
- `idx_medi_xml_ledger_person_id_custom` (`person_id_custom`)
- `idx_medi_xml_ledger_identity_hash` (`identity_hash`)
- `idx_medi_xml_ledger_kenshin_date` (`kenshin_date`)
- `idx_medi_xml_ledger_xml_sha256` (`xml_sha256`)
- `idx_medi_xml_ledger_zip_receipt_id` (`zip_receipt_id`)

#### 主要カラム

##### 識別子

- `xml_ledger_id`
- `run_id`
- `zip_receipt_id`
- `zip_sha256`
- `zip_inner_path_sha256`
- `xml_sha256`
- `person_id_custom`
- `identity_hash`

##### 原本値

- `facility_folder_name`
- `facility_code`
- `facility_name`
- `zip_name`
- `xml_filename`
- `zip_inner_path`
- `insurer_number`
- `insurance_symbol`
- `insurance_number`
- `insurance_branch_number`
- `birth_date`
- `kenshin_date`
- `gender_code`
- `name_kana_full`
- `postal_code`
- `address`
- `org_name_in_xml`
- `org_code_in_xml`
- `report_category_code`
- `program_type_code`
- `guidance_level_code`
- `metabo_code`

##### 照合用派生値

- `insurance_symbol_match`
- `insurance_number_match`
- `name_kana_match`
- `person_id_custom`
- `identity_hash`

##### status

- `xsd_valid`
- `judge_status`
- `is_exam_result`
- `is_legal_exam`
- `lsio_legal_is_complete`

##### count / summary

- `judge_score`
- `lsio_legal_required_count`
- `lsio_legal_present_count`
- `lsio_legal_missing_methods`

##### audit / log

- `error_content`
- `judge_note`
- `judged_run_id`
- `lsio_legal_judged_run_id`

##### 日時

- `created_at`
- `judged_at`
- `lsio_legal_judged_at`

#### migration変更履歴

- `sql/migrations/work_other/20260324_006_work_other_add_person_id_custom_and_identity_hash_to_medi_xml_ledger.sql`
  - `person_id_custom` を追加。
  - `identity_hash` を追加。
  - `idx_medi_xml_ledger_person_id_custom` と `idx_medi_xml_ledger_identity_hash` を追加。

#### health_exam_result v2での扱い候補

分類：要再設計

理由：XML Ledger候補だが、receipt情報、受診者索引、XSD結果、未使用判定列、LSIO判定列が混在している。v2では XML ledger、subscriber linkage、制度チェック台帳を分離する方がよい。

---

### work_other.medi_xml_item_values

#### DDL所在

`sql/ddl/work_other/0025_work_other__medi_xml_item_value.sql`

#### 現行での責務

XML observation から抽出した項目値の生抽出レイヤー。法定判定や正規化マスタは持たない。

#### 主な利用スクリプト

SELECT

- 07上は確認なし。

INSERT

- `medi_xml_item_extract.py`

UPDATE

- `medi_xml_item_extract.py`（ON DUPLICATE KEY UPDATE）

DELETE

- 07上は確認なし。

#### Primary Key

- `xml_item_value_id`

#### Unique Key

- `uq_xml_namecode_occ` (`xml_sha256`, `namecode`, `occurrence_no`)

#### Foreign Key

- なし。

#### Index

- `idx_xml_sha` (`xml_sha256`)
- `idx_namecode` (`namecode`)
- `idx_zip_inner_sha` (`zip_inner_path_sha256`)
- `idx_extract_run` (`extracted_run_id`)
- `idx_item_values_zip_inner` (`zip_sha256`, `zip_inner_path`)
- `idx_item_values_namecode` (`namecode`)

#### 主要カラム

##### 識別子

- `xml_item_value_id`
- `xml_sha256`
- `zip_sha256`
- `zip_inner_path_sha256`
- `namecode`
- `occurrence_no`
- `extracted_run_id`

##### 原本値

- `zip_inner_path`
- `value_raw`
- `value_type`
- `unit`
- `code_system`
- `code_value`
- `code_display`

##### 照合用派生値

- なし。

##### status

- なし。

##### count / summary

- `occurrence_no`

##### audit / log

- なし。

##### 日時

- `extracted_at`

#### migration変更履歴

対象テーブルへの migration は確認できなかった。

#### health_exam_result v2での扱い候補

分類：そのまま移設候補 / 統合候補

理由：item_valuesの生値保持として有用。ただし v2では `subscribers.id`、年度、XML ledger、正規化値との関係を追加する必要がある。

---

### work_other.medi_exam_result_ledger

#### DDL所在

`sql/ddl/work_other/0030_work_other__medi_exam_result_ledger.sql`

#### 現行での責務

1人=1件の健診結果基本情報を保持する作業用ledger。XML出力 `medi_export_xml.py` の入力元。

#### 主な利用スクリプト

SELECT

- `normalize_db_update.py`
- `medi_export_xml.py`

INSERT

- 07上は確認なし。

UPDATE

- `normalize_db_update.py`

DELETE

- 07上は確認なし。

#### Primary Key

- `ledger_id`

#### Unique Key

- なし。

#### Foreign Key

- なし。

#### Index

- `idx_ledger_exam_date` (`health_examination_date`)
- `idx_ledger_name_kana` (`name_kana`)
- `idx_ledger_insurance` (`insurance_card_symbol`, `insurance_card_number`)
- `idx_ledger_insurance_match` (`insurance_card_symbol_match`, `insurance_card_number_match`)

#### 主要カラム

##### 識別子

- `ledger_id`
- `insurer_number`
- `health_examination_organization_no`

##### 原本値

- `health_examination_date`
- `insurance_card_symbol`
- `insurance_card_number`
- `name_full`
- `name_kana`
- `gender_code`
- `gender`
- `birthday`
- `health_exam_report_category`
- `program_code`
- `postalcode`
- `address`
- `health_examination_organization_*`
- `input_method`
- `source_note`

##### 照合用派生値

- `insurance_card_symbol_match`
- `insurance_card_number_match`
- `name_kana_match`

##### status

- なし。

##### count / summary

- なし。

##### audit / log

- `source_note`

##### 日時

- `health_examination_date`
- `birthday`
- `created_at`
- `updated_at`

#### migration変更履歴

対象テーブルへの migration は確認できなかった。

#### health_exam_result v2での扱い候補

分類：統合候補 / 廃止候補

理由：`medi_xml_ledger` と情報が重複する。v2でXML由来の台帳を正とするなら、作業用ledgerは統合または廃止候補。

---

### work_other.medi_exam_result_item_values

#### DDL所在

`sql/ddl/work_other/0035_work_other__medi_exam_result_item_values.sql`

#### 現行での責務

`medi_exam_result_ledger` に紐づく健診項目値。raw値、正規化後value、nullFlavor、正規化status/errorを持ち、XML出力で参照される。

#### 主な利用スクリプト

SELECT

- `normalize_item_values.py`
- `medi_export_xml.py`

INSERT

- 07上は確認なし。

UPDATE

- `normalize_item_values.py`

DELETE

- 07上は確認なし。

#### Primary Key

- `item_value_id`

#### Unique Key

- `uq_ledger_namecode_seq` (`ledger_id`, `namecode`, `value_seq`)

#### Foreign Key

- なし。

#### Index

- `idx_item_values_namecode` (`namecode`)
- `idx_item_values_ledger` (`ledger_id`)
- `idx_item_values_norm` (`normalize_status`, `normalized_at`)

#### 主要カラム

##### 識別子

- `item_value_id`
- `ledger_id`
- `namecode`
- `value_seq`

##### 原本値

- `raw_value`
- `nullflavor`
- `identity_item_code`
- `jun_no`

##### 照合用派生値

- `value`

##### status

- `normalize_status`

##### count / summary

- `value_seq`

##### audit / log

- `normalize_error`

##### 日時

- `normalized_at`
- `created_at`
- `updated_at`

#### migration変更履歴

対象テーブルへの migration は確認できなかった。

#### health_exam_result v2での扱い候補

分類：統合候補

理由：`medi_xml_item_values` と責務が近い。v2では raw値、正規化値、status/error を持つ item_values に統合するか、生抽出と正規化結果を分けるか決める必要がある。

---

### work_other.etl_runs

#### DDL所在

`sql/ddl/work_other/0055_work_other__etl_runs.sql`

#### 現行での責務

汎用ETLのrunログ。phase、source、status、件数、エラー数、入力ファイル情報などを保持する。

#### 主な利用スクリプト

SELECT

- 07上の対象スクリプトでは直接利用確認なし。

INSERT

- 07上の対象スクリプトでは直接利用確認なし。

UPDATE

- 07上の対象スクリプトでは直接利用確認なし。

DELETE

- 07上は確認なし。

#### Primary Key

- `run_id`

#### Unique Key

- なし。

#### Foreign Key

- なし。

#### Index

- `idx_etl_runs_insurer_started` (`insurer_number`, `started_at`)
- `idx_etl_runs_phase_started` (`phase`, `started_at`)

#### 主要カラム

##### 識別子

- `run_id`
- `insurer_number`

##### 原本値

- `source`
- `db_schema`
- `db_path`
- `input_base`
- `input_file`

##### 照合用派生値

- なし。

##### status

- `phase`
- `status`
- `dry_run`

##### count / summary

- `limit_rows`
- `files`
- `rows_seen`
- `rows_inserted`
- `rows_updated`
- `rows_unchanged`
- `rows_skipped`
- `errors`

##### audit / log

- `notes`
- `admin_note`

##### 日時

- `started_at`
- `finished_at`

#### migration変更履歴

対象テーブルへの migration は確認できなかった。

#### health_exam_result v2での扱い候補

分類：統合候補

理由：`medi_import_runs` と統合し、v2のrun台帳として使える可能性がある。

---

### work_other.etl_errors

#### DDL所在

`sql/ddl/work_other/0056_work_other__etl_errors.sql`

#### 現行での責務

汎用ETLエラー台帳。run、phase、source、行番号、field、error_code、messageを保持する。

#### 主な利用スクリプト

SELECT

- 07上の対象スクリプトでは直接利用確認なし。

INSERT

- 07上の対象スクリプトでは直接利用確認なし。

UPDATE

- 07上の対象スクリプトでは直接利用確認なし。

DELETE

- 07上は確認なし。

#### Primary Key

- `error_id`

#### Unique Key

- なし。

#### Foreign Key

- `fk_etl_errors_run`: `run_id` -> `work_other.etl_runs.run_id` ON DELETE CASCADE

#### Index

- `idx_etl_errors_run_phase` (`run_id`, `phase`)
- `idx_etl_errors_insurer_run` (`insurer_number`, `run_id`)
- `idx_etl_errors_person` (`person_id_custom`, `run_id`)

#### 主要カラム

##### 識別子

- `error_id`
- `run_id`
- `insurer_number`
- `staging_rowid`
- `person_id_custom`

##### 原本値

- `src_file`
- `src_row_no`
- `src_line_no`
- `field`
- `field_value`

##### 照合用派生値

- `person_id_custom`

##### status

- `phase`
- `error_code`

##### count / summary

- なし。

##### audit / log

- `source`
- `message`

##### 日時

- `created_at`

#### migration変更履歴

対象テーブルへの migration は確認できなかった。

#### health_exam_result v2での扱い候補

分類：統合候補

理由：v2のエラー台帳として有用。ただし XML処理ログ `medi_xml_process_logs` との住み分けが必要。

---

### work_other.find_xml_subscribers_list_20260128

#### DDL所在

DDL未確認

#### 現行での責務

`normalize_db_update.py` の対象者テーブルとして登場する暫定テーブル。`name_kana_raw`、`insurance_number_raw`、`insurance_symbol_raw` から照合用列を作る用途。

#### 主な利用スクリプト

SELECT

- `normalize_db_update.py`

INSERT

- 07上は確認なし。

UPDATE

- `normalize_db_update.py`

DELETE

- 07上は確認なし。

#### Primary Key

DDL未確認

#### Unique Key

DDL未確認

#### Foreign Key

DDL未確認

#### Index

DDL未確認

#### 主要カラム

##### 識別子

- `subscriber_row_id`（07記載のJob定義より）

##### 原本値

- `name_kana_raw`
- `insurance_number_raw`
- `insurance_symbol_raw`

##### 照合用派生値

- `name_kana_match`
- `insurance_number_match`
- `insurance_symbol_match`

##### status

DDL未確認

##### count / summary

DDL未確認

##### audit / log

DDL未確認

##### 日時

DDL未確認

#### migration変更履歴

DDL/migration未確認。

#### health_exam_result v2での扱い候補

分類：廃止候補

理由：暫定名の対象者抽出テーブル。v2では `dev_phr.subscribers` と正式な未着管理/対象者台帳を使うべき。

---

### work_other.csv_header_map_submit

#### DDL所在

DDL未確認

#### 現行での責務

`import_submit_csv.py` がCSV列順・ヘッダー・取込先カラムを決めるために参照するマッピングテーブル。

#### 主な利用スクリプト

SELECT

- `import_submit_csv.py`

INSERT

- 07上は確認なし。

UPDATE

- 07上は確認なし。

DELETE

- 07上は確認なし。

#### Primary Key

DDL未確認

#### Unique Key

DDL未確認

#### Foreign Key

DDL未確認

#### Index

DDL未確認

#### 主要カラム

##### 識別子

- `display_order`（07記載より）

##### 原本値

- `csv_header`
- `original_header`

##### 照合用派生値

- `table_column`

##### status

DDL未確認

##### count / summary

DDL未確認

##### audit / log

DDL未確認

##### 日時

DDL未確認

#### migration変更履歴

DDL/migration未確認。

#### health_exam_result v2での扱い候補

分類：参照のみ / 廃止候補

理由：CSV提出用の補助テーブル。v2初期スコープがZIP/XML中心なら直接移設しない。

---

### dev_phr.subscribers

#### DDL所在

`sql/ddl/dev_phr/0080_dev_phr__subscribers.sql`

#### 現行での責務

加入者マスタ。保険者番号、保険証記号/番号/枝番、生年月日、性別、氏名、照合用派生値、HIA加入者ID、資格日などを保持する。

#### 主な利用スクリプト

SELECT

- 07では直接SELECT実装は未確認。ただし `subscribers.id` 紐付け先としてv2示唆に登場する。

INSERT

- 07上は確認なし。

UPDATE

- 07上は確認なし。

DELETE

- 07上は確認なし。

#### Primary Key

- `id`

#### Unique Key

- `uq_subscribers_personid_namekana` (`person_id_custom`, `name_kana_full`)

#### Foreign Key

- `fk_subscribers_last_change_run`: `last_change_run_id` -> `dev_phr.etl_runs.run_id`

#### Index

- `idx_subscribers_insurer` (`insurer_number`)
- `idx_subscribers_insurance_full` (`insurer_number`, `insurance_symbol`, `insurance_number`, `insurance_branchnumber`)
- `idx_subscribers_gender` (`gender_code`)
- `idx_subscribers_last_change_run` (`last_change_run_id`)
- `idx_subscribers_name_kana_full_match` (`name_kana_full_match`)
- `idx_subscribers_name_full_match` (`name_full_match`)
- `idx_subscribers_symbol_match` (`insurance_symbol_match`)
- `idx_subscribers_symbol_export` (`insurance_symbol_export`)
- `idx_subscribers_number_match` (`insurance_number_match`)
- `idx_subscribers_identity_hash` (`identity_hash`)
- `idx_subscribers_compare_identity_norm_hash` (`compare_identity_norm_hash`)
- `idx_subscribers_compare_other_hash` (`compare_other_hash`)

#### 主要カラム

##### 識別子

- `id`
- `insurer_number`
- `person_id_custom`
- `hia_subscriber_id`
- `last_change_run_id`

##### 原本値

- `insurance_symbol`
- `insurance_number`
- `insurance_branchnumber`
- `birth`
- `gender_code`
- `name_kana_full`
- `name_kanji_*`
- `qualification_acquired_date`
- `qualification_lost_date`
- `employee_code`
- `connect_id`

##### 照合用派生値

- `insurance_symbol_export`
- `insurance_symbol_digits`
- `name_kana_full_match`
- `name_full_match`
- `name_kana_*_match`
- `name_kanji_*_match`
- `insurance_symbol_match`
- `insurance_number_match`
- `identity_hash`
- `compare_identity_norm_hash`
- `compare_other_hash`

##### status

- 資格状態は `qualification_acquired_date` / `qualification_lost_date` で表現。

##### count / summary

- なし。

##### audit / log

- `last_change_run_id`

##### 日時

- `birth`
- `qualification_acquired_date`
- `qualification_lost_date`
- `created_at`
- `updated_at`

#### migration変更履歴

- `20260318_002_dev_phr_add_identity_hash_to_subscribers.sql`: `identity_hash` と indexを追加。
- `20260318_003_dev_phr_add_insurance_symbol_export_to_subscribers.sql`: `insurance_symbol_export` と indexを追加。
- `20260430_001_dev_phr_add_name_parts_match_columns.sql`: カナ/漢字parts match列を追加。
- `20260520_001_dev_phr_add_compare_hash_columns.sql`: `compare_identity_norm_hash`、`compare_other_hash` とindexを追加。
- その他、staging系migrationで `subscribers` 由来IDや比較列への言及あり。

#### health_exam_result v2での扱い候補

分類：参照のみ

理由：v2の正本加入者マスタとして参照し、XML Ledgerやsubject_statusから `subscribers.id` へ紐付ける。v2側で複製しない方針が自然。

---

### dev_phr.funds

#### DDL所在

`sql/ddl/dev_phr/0015_dev_phr__funds.sql`

#### 現行での責務

健保マスタ。健保コード、名称、組織種別、健診結果受領方法、特保XML納品方針などを保持する。

#### 主な利用スクリプト

SELECT

- 07上は直接利用確認なし。

INSERT

- 07上は確認なし。

UPDATE

- 07上は確認なし。

DELETE

- 07上は確認なし。

#### Primary Key

- `id`

#### Unique Key

- `uq_funds_fund_code` (`fund_code`)

#### Foreign Key

- なし。

#### Index

- Unique keyのみ。

#### 主要カラム

##### 識別子

- `id`
- `fund_code`

##### 原本値

- `name_official`
- `name_short`
- `name_kana`
- `name_display`
- `org_type`

##### 照合用派生値

- なし。

##### status

- `active`

##### count / summary

- なし。

##### audit / log

- `notes`

##### 日時

- `created_at`
- `updated_at`

#### migration変更履歴

対象テーブルへの migration は確認できなかった。

#### health_exam_result v2での扱い候補

分類：参照のみ

理由：保険者別ルール、受領/納品方針の参照元として使う。v2固有テーブルへ複製しない。

---

### dev_phr.fund_insurer_numbers

#### DDL所在

`sql/ddl/dev_phr/0018_dev_phr__fund_insurer_numbers.sql`

#### 現行での責務

健保と保険者番号を有効期間・系統種別付きで対応づけるマスタ。

#### 主な利用スクリプト

SELECT

- 07上は直接利用確認なし。

INSERT

- 07上は確認なし。

UPDATE

- 07上は確認なし。

DELETE

- 07上は確認なし。

#### Primary Key

- `id`

#### Unique Key

- `uq_insurer_number_line_valid` (`insurer_number`, `line_type_id`, `valid_from`)

#### Foreign Key

- DDL上はなし。

#### Index

- `idx_line_type_id` (`line_type_id`)
- `idx_fund_id` (`fund_id`)
- `idx_insurer_number` (`insurer_number`)

#### 主要カラム

##### 識別子

- `id`
- `fund_id`
- `insurer_number`
- `line_type_id`
- `insurer_no_id_custom`

##### 原本値

- `valid_from`
- `valid_to`

##### 照合用派生値

- なし。

##### status

- `is_current`

##### count / summary

- なし。

##### audit / log

- `notes`

##### 日時

- `valid_from`
- `valid_to`
- `created_at`
- `updated_at`

#### migration変更履歴

対象テーブルへの migration は確認できなかった。

#### health_exam_result v2での扱い候補

分類：参照のみ

理由：XMLの保険者番号から健保・制度ルールを解決する参照マスタとして利用候補。

---

### dev_phr.exam_item_master

#### DDL所在

`sql/ddl/dev_phr/0025_dev_phr__exam_item_master.sql`

#### 現行での責務

厚労省形式の健診項目マスタ。namecode、項目名、XML value type、OID、単位、XPath、値取得方法、付属2の法定/任意フラグ、CDAセクションを保持する。

#### 主な利用スクリプト

SELECT

- `medi_xml_item_extract.py`
- `normalize_item_values.py`
- `medi_export_xml.py`

INSERT

- 07上は確認なし。

UPDATE

- migrationで付属2カラム値を更新。

DELETE

- 07上は確認なし。

#### Primary Key

- `namecode`

#### Unique Key

- なし。

#### Foreign Key

- なし。

#### Index

- `idx_exam_item_category` (`category_name`)
- `idx_exam_item_xml_value_type` (`xml_value_type`)
- `idx_exam_item_result_oid` (`result_code_oid`)

#### 主要カラム

##### 識別子

- `namecode`
- `item_code_oid`
- `result_code_oid`
- `xml_method_code`
- `identity_item_code`

##### 原本値

- `item_name`
- `display_unit`
- `ucum_unit`
- `method_name`
- `category_name`
- `data_type_label`
- `xpath_template`
- `value_method`
- `importance`
- `importance_group`
- `kubun_no`
- `kubun_name`
- `jun_no`
- `identity_item_name`

##### 照合用派生値

- なし。

##### status

- `xml_value_type`
- `nullflavor_allowed`
- `annex2_exec_requirement`
- `annex2_legal_report_flag`
- `cda_section_code_default`

##### count / summary

- `jun_no`

##### audit / log

- `notes`
- `update_type`
- `update_reason`
- `source_last_updated`

##### 日時

- `source_last_updated`
- `created_at`
- `updated_at`

#### migration変更履歴

- `20260213_001_dev_phr_add_annex2_flags_to_exam_item_master.sql`: `annex2_exec_requirement`、`annex2_legal_report_flag`、`cda_section_code_default` を追加。
- `20260213_002_dev_phr_update_exam_item_master_annex2_from_mhlw.sql`: 付属2情報を namecode 単位でUPDATE。
- `20260213_003_dev_phr_set_optional_annex2_flags_for_remaining_items.sql`: 未設定の付属2掲載項目を任意扱いにUPDATE。

#### health_exam_result v2での扱い候補

分類：参照のみ / そのまま移設候補

理由：item_values抽出・正規化・不足チェックの基礎マスタ。v2では制度チェックルールを別テーブルに分けつつ、項目マスタとして参照する。

---

### dev_phr.norm_variants

#### DDL所在

`sql/ddl/dev_phr/0023_dev_phr__norm_variants.sql`

#### 現行での責務

CD/CO系の健診値を `result_code_oid + raw_value_utf8` から正規化コードへ変換する辞書。

#### 主な利用スクリプト

SELECT

- `normalize_item_values.py`

INSERT

- 07上は確認なし。

UPDATE

- 07上は確認なし。

DELETE

- 07上は確認なし。

#### Primary Key

- `variant_id`

#### Unique Key

- `uq_oid_rawvalue_utf8` (`result_code_oid`, `raw_value_utf8`)

#### Foreign Key

- なし。

#### Index

- `idx_oid_canonical` (`result_code_oid`, `is_canonical`, `priority`)
- `idx_oid_normcode` (`result_code_oid`, `normalized_code`)

#### 主要カラム

##### 識別子

- `variant_id`
- `result_code_oid`

##### 原本値

- `raw_value_utf8`
- `raw_token_norm`
- `display_name`

##### 照合用派生値

- `normalized_code`
- `code_system`

##### status

- `is_canonical`
- `is_active`

##### count / summary

- `priority`

##### audit / log

- `note`

##### 日時

- `created_at`
- `updated_at`

#### migration変更履歴

対象テーブルへの migration は確認できなかった。

#### health_exam_result v2での扱い候補

分類：参照のみ

理由：item_values正規化で必要。v2側で辞書を複製せず参照する方針が自然。

---

### dev_phr.etl_runs

#### DDL所在

`sql/ddl/dev_phr/0010_dev_phr__etl_runs.sql`

#### 現行での責務

dev_phr側の汎用ETL runログ。構造は `work_other.etl_runs` と同一。

#### 主な利用スクリプト

SELECT / INSERT / UPDATE / DELETE

- 07上の対象スクリプトでは直接利用確認なし。

#### Primary Key

- `run_id`

#### Unique Key

- なし。

#### Foreign Key

- なし。

#### Index

- `idx_etl_runs_insurer_started` (`insurer_number`, `started_at`)
- `idx_etl_runs_phase_started` (`phase`, `started_at`)

#### 主要カラム

##### 識別子

- `run_id`
- `insurer_number`

##### 原本値

- `source`
- `db_schema`
- `db_path`
- `input_base`
- `input_file`

##### 照合用派生値

- なし。

##### status

- `phase`
- `status`
- `dry_run`

##### count / summary

- `files`
- `rows_seen`
- `rows_inserted`
- `rows_updated`
- `rows_unchanged`
- `rows_skipped`
- `errors`

##### audit / log

- `notes`
- `admin_note`

##### 日時

- `started_at`
- `finished_at`

#### migration変更履歴

対象テーブルへの migration は確認できなかった。

#### health_exam_result v2での扱い候補

分類：参照のみ

理由：`dev_phr.subscribers.last_change_run_id` の参照先。v2の健診結果処理runとは直接混ぜない方がよい。

---

### dev_phr.etl_errors

#### DDL所在

`sql/ddl/dev_phr/0020_dev_phr__etl_errors.sql`

#### 現行での責務

dev_phr側の汎用ETLエラー台帳。構造は `work_other.etl_errors` と同一。

#### 主な利用スクリプト

SELECT / INSERT / UPDATE / DELETE

- 07上の対象スクリプトでは直接利用確認なし。

#### Primary Key

- `error_id`

#### Unique Key

- なし。

#### Foreign Key

- `fk_etl_errors_run`: `run_id` -> `dev_phr.etl_runs.run_id`

#### Index

- `idx_etl_errors_run_phase` (`run_id`, `phase`)
- `idx_etl_errors_insurer_run` (`insurer_number`, `run_id`)
- `idx_etl_errors_person` (`person_id_custom`, `run_id`)

#### 主要カラム

##### 識別子

- `error_id`
- `run_id`
- `insurer_number`
- `staging_rowid`
- `person_id_custom`

##### 原本値

- `src_file`
- `src_row_no`
- `src_line_no`
- `field`
- `field_value`

##### 照合用派生値

- `person_id_custom`

##### status

- `phase`
- `error_code`

##### count / summary

- なし。

##### audit / log

- `source`
- `message`

##### 日時

- `created_at`

#### migration変更履歴

対象テーブルへの migration は確認できなかった。

#### health_exam_result v2での扱い候補

分類：参照のみ

理由：dev_phr ETL用であり、健診結果v2のエラー台帳とは分ける。

---

### dev_phr.templates

#### DDL所在

`sql/ddl/dev_phr/0065_dev_phr__templates.sql`

#### 現行での責務

健保別/バージョン別のCSV取込テンプレートヘッダ。主に加入者staging取込向け。

#### 主な利用スクリプト

SELECT / INSERT / UPDATE / DELETE

- 07上の対象スクリプトでは直接利用確認なし。

#### Primary Key

- (`fund_id`, `version`)

#### Unique Key

- Primary keyのみ。

#### Foreign Key

- DDL上はなし。

#### Index

- Primary keyのみ。

#### 主要カラム

##### 識別子

- `fund_id`
- `version`

##### 原本値

- `name`
- `template_type`
- `target_table`
- `version_label`
- `created_by`

##### 照合用派生値

- なし。

##### status

- なし。

##### count / summary

- なし。

##### audit / log

- `notes`

##### 日時

- `created_at`
- `configured_on`

#### migration変更履歴

- `20260416_001_dev_phr_templates_templatemappings_add_comments.sql`: 列コメントを追加/更新。
- `20260416_003_dev_phr_fix_template_mappings_rules_for_fund_06139463.sql`: `fund_id=2` / `version=20260416` のテンプレートを upsert。

#### health_exam_result v2での扱い候補

分類：参照のみ / 廃止候補

理由：加入者CSV取込テンプレートであり、健診結果ZIP/XML v2の初期DDLには不要。ただしCSV健診結果取込を扱うなら類似概念を再利用可能。

---

### dev_phr.template_mappings

#### DDL所在

`sql/ddl/dev_phr/0066_dev_phr__template_mappings.sql`

#### 現行での責務

テンプレート別のCSV列順、CSVヘッダー、target_column、変換rule、必須フラグを保持する。

#### 主な利用スクリプト

SELECT / INSERT / UPDATE / DELETE

- 07上の対象スクリプトでは直接利用確認なし。

#### Primary Key

- DDL上、単独Primary Keyなし。

#### Unique Key

- `uq_template_mapping` (`fund_id`, `version`, `col_order`, `target_column`)

#### Foreign Key

- DDL上はなし。

#### Index

- `idx_tmplate_template` (`fund_id`, `version`)

#### 主要カラム

##### 識別子

- `fund_id`
- `version`
- `col_order`
- `target_column`

##### 原本値

- `csv_header`
- `rule`

##### 照合用派生値

- `target_column`

##### status

- `required`

##### count / summary

- `col_order`

##### audit / log

- `notes`

##### 日時

- `created_at`

#### migration変更履歴

- `20260416_001_dev_phr_templates_templatemappings_add_comments.sql`: 列コメントを追加/更新。
- `20260416_002_dev_phr_fix_template_mappings_for_staging_subscribers_fund.sql`: target_column名を新DDLに合わせてUPDATE。
- `20260416_003_dev_phr_fix_template_mappings_rules_for_fund_06139463.sql`: 06139463向けmappingを作り直し。
- `20260417_001_add_match_mappings_to_staging_subscribers_fund.sql`: match系target mappingをINSERT。

#### health_exam_result v2での扱い候補

分類：参照のみ / 廃止候補

理由：加入者CSV取込向け。健診結果CSV取込をv2で扱う場合は、`csv_header_map_submit` の代替として再設計候補。

---

## DB全体の重複

- `medi_xml_receipts` と `medi_xml_ledger`
  - 両方に XML単位の識別情報、施設情報、抽出情報がある。
  - `medi_xml_receipts` は receipt/status、`medi_xml_ledger` は業務索引と判定寄りだが、境界が曖昧。
- `medi_xml_item_values` と `medi_exam_result_item_values`
  - どちらも健診項目値を持つ。
  - 前者は XML生抽出、後者は正規化・XML出力向け。
- `medi_import_runs` と `etl_runs`
  - どちらもrunログ。
  - medi専用と汎用ETLで分かれている。
- `medi_xml_process_logs` と `etl_errors`
  - 前者はXML処理ステップログ、後者は汎用エラー台帳。
  - v2では処理ログとエラー台帳を分けるか統合するか決める必要がある。
- `medi_xml_ledger` の判定列と今後の制度チェック台帳
  - `judge_*` や `lsio_legal_*` が含まれるが、設計履歴では制度チェック結果は専用台帳へ分ける方針。

---

## 統合候補

- `medi_import_runs` + `work_other.etl_runs`
  - health_exam_result v2用 run table へ統合候補。
- `medi_zip_receipt_runs` + `medi_xml_receipt_runs` + `medi_xml_process_logs`
  - run event / process log として統合候補。ただし「検出実績」と「処理結果」は分けてもよい。
- `medi_xml_item_values` + `medi_exam_result_item_values`
  - v2 item_values として統合候補。
  - raw value、normalized value、normalize_status、normalize_error、xml source情報を一貫して持たせる設計が必要。
- `medi_xml_receipts` + `medi_xml_ledger`
  - 完全統合ではなく、receiptとledgerの責務分離が必要。
  - receiptは原本・状態、ledgerは加入者/年度/健診日/チェックサマリに寄せる案。

---

## 廃止候補

- `work_other.find_xml_subscribers_list_20260128`
  - 暫定名の対象者抽出テーブル。v2では正式な対象者・未着管理テーブルに置き換える。
- `work_other.csv_header_map_submit`
  - DDL未確認。ZIP/XML中心の初期v2では不要候補。
- `work_other.medi_exam_result_ledger`
  - `medi_xml_ledger` またはv2 ledgerへ統合するなら廃止候補。
- `work_other.medi_exam_result_item_values`
  - v2 item_valuesへ統合するなら廃止候補。
- `medi_xml_ledger` 内の `judge_*` / `is_*` / `lsio_legal_*`
  - テーブル廃止ではなく、列の責務移動候補。制度チェック台帳に分離する方針。

---

## v2 DDL設計で最初に決めるべき事項

1. ファイル台帳とZIP Receiptの境界
   - 共有フォルダ観測、workコピー、ZIP受領をどのテーブルで分けるか。

2. `data/medical/work` のパス管理
   - 共有元パス、コピー後パス、処理後アーカイブパスを別カラムにするか。

3. XML ReceiptとXML Ledgerの境界
   - XML原本単位の状態と、受診者・年度・健診日単位の業務状態を分けるか。

4. `subscribers.id` 紐付けの保持先
   - XML Ledgerに持つか、subject_status / person-year tableに持つか。

5. item_valuesの正本設計
   - raw値、正規化値、nullFlavor、単位、コード値、status、エラーを同一テーブルに持つか分けるか。

6. 制度チェック結果の専用台帳
   - item_valuesへ不足を入れない方針に合わせ、特定健診・法定健診チェック台帳を別設計する。

7. run / error / process logの統合方針
   - `medi_import_runs`、`etl_runs`、`medi_xml_process_logs`、`etl_errors` をv2でどう整理するか。

8. 既存 `dev_phr` マスタの参照範囲
   - `subscribers`、`funds`、`fund_insurer_numbers`、`exam_item_master`、`norm_variants` は参照のみとし、v2側へ複製しないか。

9. 旧作業テーブルの扱い
   - `medi_exam_result_ledger` / `medi_exam_result_item_values` を移行対象にするか、参照実装としてのみ扱うか。
