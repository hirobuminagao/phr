# Codex 調査結果

このドキュメントは Codex による調査結果を時系列で記録する。
コード・DDLの修正は行わず、調査結果のみを保存する。

---

# 第1回調査
## HIA・加入者同期・共通基盤調査

> 調査日時: 未記録

調査のみ実施しました。コード変更・DDL変更・コミットはしていません。

**対象スクリプト**
- [hia_import_zip.py](/Users/hiro/work/phr/scripts/work_folder/scripts/hia_import_zip.py:1)
  - 役割: HIA ZIPを展開し、XML人物識別情報を読み、ZIP台帳・人物年度台帳・XMLイベント台帳へ記帳。
  - 入力: `data/hia_export/input_zip/{insurer}/*.zip`
  - 出力: `hia_import_zips`, `hia_import_zip_errors`, `hia_person_years`, `hia_xml_events`, `data/hia_export/archive_zip/...`, `error.txt`
  - 呼び出し先: `hia_parse_xml.parse_hia_xml_identity`, `custom_id_gen.generate_id`, `lib.db`, `lib.normalize.common`
- [hia_parse_xml.py](/Users/hiro/work/phr/scripts/work_folder/scripts/hia_parse_xml.py:1)
  - 役割: HIA XMLから最小識別情報を抽出。
  - 入力: XMLファイル
  - 出力: dict `exam_date`, `name`, `birthdate`, `gender_code`, `insurer_number`, `insurance_symbol`, `insurance_number`, `facility_code`, `report_category`, `health_program_code` など
  - 呼び出し元: `hia_import_zip.py`
- [hia_build_delivery_zip.py](/Users/hiro/work/phr/scripts/work_folder/scripts/hia_build_delivery_zip.py:1)
  - 役割: 取込済みXML台帳から納品対象を抽出し、納品ZIPを再構成。
  - 入力: `data/fund_delivery/input/{insurer}/*.zip`, DB台帳
  - 出力: `data/fund_delivery/output/{insurer}/*.zip`
  - 参照: `hia_xml_events`, `hia_person_years`, `hia_import_zips`, `hia_delivery_exclusion_rules`
- [hia_import_dashboard_csv.py](/Users/hiro/work/phr/scripts/work_folder/scripts/hia_import_dashboard_csv.py:1)
  - 役割: HIA dashboard CSVを正規化し、現況・履歴・勧奨イベントへ反映。
  - 入力: dashboard CSV
  - 出力: `work_other.hia_dashboard_status`, `hia_dashboard_status_history`, `hia_dashboard_reminder_events`
  - 参照: `dev_phr.subscribers`, `dev_phr.identity_kanji_normalization`
- [check_tokuho_xml.py](/Users/hiro/work/phr/scripts/tokuho_xml_check/check_tokuho_xml.py:1)
  - 役割: 特定保健指導XMLの検証・集計・突合レポート。
  - 入力: XML群、mat設定、必要に応じてMySQL
  - 出力: summary/detail/log/json等
  - 参照: `work_other.shg_result`
- [validate_xml.py](/Users/hiro/work/phr/scripts/tokuho_xml_check/validate_xml.py:1)
  - 役割: XSD検証。
  - 入力: `input` 配下または指定パスの `DATA/*.xml`, `XSD`
  - 出力: `out/export_xsd_validation_result`, validation log/csv
  - DB参照なし

加入者連携として健診結果処理に接続しているもの:
- [import_subscribers_to_staging_hub.py](/Users/hiro/work/phr/scripts/work_folder/scripts/import_subscribers_to_staging_hub.py:1): HIA加入者CSV → `staging_subscribers_hub`
- [apply_subscribers_from_staging_hub.py](/Users/hiro/work/phr/scripts/work_folder/scripts/apply_subscribers_from_staging_hub.py:1): `staging_subscribers_hub` → `subscribers`, `subscriber_addresses`, `subscriber_contacts`, `subscriber_audit`
- [import_subscribers_to_staging_fund.py](/Users/hiro/work/phr/scripts/work_folder/scripts/import_subscribers_to_staging_fund.py:1): fund加入者CSV → `staging_subscribers_fund`

**実際に参照しているテーブル**
- `work_other`系: `hia_import_zips`, `hia_import_zip_errors`, `hia_person_years`, `hia_xml_events`, `hia_delivery_exclusion_rules`, `hia_dashboard_status`, `hia_dashboard_status_history`, `hia_dashboard_reminder_events`, `shg_result`, `etl_runs`, `etl_errors`
- `dev_phr`系: `subscribers`, `subscriber_addresses`, `subscriber_contacts`, `subscriber_audit`, `staging_subscribers_hub`, `staging_subscribers_fund`, `funds`, `fund_insurer_numbers`, `templates`, `template_mappings`, `identity_kanji_normalization`, `etl_runs`, `etl_errors`

**テーブル利用マップ**
- `hia_import_zips`: `hia_import_zip.py` SELECT/INSERT/UPDATE、`hia_build_delivery_zip.py` SELECT
- `hia_import_zip_errors`: `hia_import_zip.py` SELECT/INSERT
- `hia_person_years`: `hia_import_zip.py` INSERT/UPDATE/SELECT、`hia_build_delivery_zip.py` SELECT
- `hia_xml_events`: `hia_import_zip.py` SELECT/INSERT/UPDATE、`hia_build_delivery_zip.py` SELECT
- `hia_delivery_exclusion_rules`: `hia_build_delivery_zip.py` SELECT
- `hia_dashboard_status`: `hia_import_dashboard_csv.py` SELECT/INSERT/UPDATE
- `hia_dashboard_status_history`: `hia_import_dashboard_csv.py` INSERT
- `hia_dashboard_reminder_events`: `hia_import_dashboard_csv.py` INSERT IGNORE
- `shg_result`: `check_tokuho_xml.py` SELECT
- `subscribers`: `hia_import_dashboard_csv.py` SELECT、`apply_subscribers_from_staging_hub.py` SELECT/INSERT/UPDATE
- `subscriber_addresses`: `apply_subscribers_from_staging_hub.py` SELECT/INSERT/UPDATE
- `subscriber_contacts`: `apply_subscribers_from_staging_hub.py` SELECT/INSERT/UPDATE
- `subscriber_audit`: `apply_subscribers_from_staging_hub.py` INSERT
- `staging_subscribers_hub`: `import_subscribers_to_staging_hub.py` INSERT、`apply_subscribers_from_staging_hub.py` SELECT/UPDATE
- `staging_subscribers_fund`: `import_subscribers_to_staging_fund.py` SELECT/INSERT
- `fund_insurer_numbers`, `funds`: `import_subscribers_to_staging_fund.py` SELECT
- `templates`, `template_mappings`: `import_subscribers_to_staging_fund.py` SELECT
- `identity_kanji_normalization`: `lib/normalize/kanji_dict.py` SELECT
- `etl_runs`, `etl_errors`: `lib/etl` が CREATE/INSERT/UPDATE、各import/apply系から利用

**主な使用カラム**
- `hia_import_zips`: `zip_id`, `insurer_number`, `folder_name`, `zip_name`, `dl_date`, `send_seq`, `zip_sha256`, `xml_count_total`, `xml_count_success`, `xml_count_error`, `import_status`, `archived_zip_path`, `archived_at`, `updated_at`
- `hia_person_years`: `person_year_id`, `person_id_custom`, `name_kana_norm`, `name_kana_full_match`, `gender_code`, `exam_year`, `insurer_number`, `insurance_symbol`, `insurance_number`, `insurance_symbol_match`, `insurance_number_match`, `report_category`, `health_program_code`, `birthdate`, `name_kana_raw`, `identity_hash`, `dl_count`, `first_seen_*`, `last_seen_*`
- `hia_xml_events`: `xml_event_id`, `person_year_id`, `zip_id`, `xml_filename`, `xml_sha256`, `is_deleted`, `exam_date`, `facility_code`, `facility_name`, `dl_date`, `updated_at`
- `hia_dashboard_status`: `snapshot_identity_key`, 保険証/続柄/氏名match系, subscriber補完系, `status`, `reservation_date`, `exam_date`, 会社/医療機関/メール/勧奨/除外理由, `row_sha256`, `first_seen_run_id`, `last_seen_run_id`
- DDLにあるがコード参照が見えない候補（推測）: `hia_*` の `created_at` はDBデフォルト利用のみ。`hia_delivery_exclusion_rules.exclusion_reason/source_note/created_at/updated_at` はコード上SELECT条件や出力に未使用。

**ライブラリ利用状況**
- `lib/db`: `hia_import_zip.py`, `hia_build_delivery_zip.py`, `hia_import_dashboard_csv.py`, `import_subscribers_to_staging_hub.py`, `apply_subscribers_from_staging_hub.py`, `import_subscribers_to_staging_fund.py`
- `lib/etl`: subscriber import/apply、dashboard CSV importで `RunMetrics`, `start_run`, `finish_run`, `log_error`, `log_normalize_error`
- `lib/errors`: 正規化エラー `NormalizeError`
- `lib/normalize`: HIA ZIP、dashboard、subscriber import/applyで氏名・保険証・日付・性別・identity hashを利用
- `lib/progress`: 独立ディレクトリは存在せず、実体は `lib/etl/progress.py`
- `lib/metrics`: 独立ディレクトリは存在せず、実体は `lib/etl/metrics.py`

**依存関係**
```text
hia_import_zip.py
  -> hia_parse_xml.py
  -> lib/custom_id_gen.py
  -> lib/db/*
  -> lib/normalize/common.py
      -> lib/normalize/kanji_dict.py
      -> lib/errors/normalize.py

hia_import_dashboard_csv.py
  -> lib/db/*
  -> lib/etl/*
  -> lib/normalize/common.py
  -> lib/errors/normalize.py

hia_build_delivery_zip.py
  -> lib/db/*

import_subscribers_to_staging_hub.py
  -> lib/db/*
  -> lib/etl/*
  -> lib/normalize/common.py
  -> lib/normalize/subscriber.py

apply_subscribers_from_staging_hub.py
  -> lib/db/*
  -> lib/etl/*
  -> lib/normalize/common.py
```

**重複コード候補**
- ZIP名パース、ZIP探索、展開、`DATA/h*.xml` 収集: `hia_import_zip.py` と `hia_build_delivery_zip.py`
- `build_run_id`: `hia_import_zip.py`, `hia_build_delivery_zip.py`
- `custom_id_gen.py`: `scripts/work_folder/lib` と `scripts/tokuho_xml_check`
- kana/数字/date normalize: `lib/normalize/common.py`, `lib/normalize/subscriber.py`, `lib/delete/*`, `tokuho_xml_check/check_tokuho_xml.py`
- ETL基盤: `lib/etl/*` と `lib/delete/etl.py`
- DB接続: `lib/db/*`, `lib/delete/db_mysql.py`, `check_tokuho_xml.py` 内の直書き接続

**未使用候補（推測）**
- `scripts/work_folder/lib/delete/*`: 現行scriptからのimportは確認できず。
- DDL候補: `medi_*` 系DDLの多く、`exam_item_*`, `event_*`, `subscriber_contact_points`, `hia_company_master`, `fund_company_mapping` は今回対象Pythonから直接参照なし。
- `staging_subscribers_fund` の多数の compare/apply/match カラムは、`import_subscribers_to_staging_fund.py` の INSERT対象外が多い。
- `subscribers` の `compare_identity_norm_hash`, `compare_other_hash`, parts match列は今回対象のapplyコードでは直接更新なし。

**ハードコード**
- パス: `data/hia_export/input_zip`, `archive_zip`, `work`, `output_to_fund`, `data/fund_delivery/input|work|output`, `scripts/work_folder/mat`, `tokuho_xml_check/input`, `out/export_xsd_validation_result`
- ZIP形式: `{facility_code}_{insurer_number}_{yyyymmddX}_{send_seq}.zip`
- XML: `DATA`, `XSD`, `h*.xml`, `ix08`, `su08`, `hg08_V08.xsd`
- HL7/OID: namespace `urn:hl7-org:v3`, OID `1.2.392.200119.6.204`, `1.2.392.200119.6.205`
- DB接続直書き: `check_tokuho_xml.py` に `DEV_DB_HOST` default `10.0.10.201`, user `devadmin`, password default文字列あり
- 固定値: dashboard identity key構成、`target_schema='work_other'`, `target_table='hia_xml_events'`, `target_column='facility_code'`, `match_type='EQUAL'`, `report_category == "10"`

**TODO/FIXME/HACK**
- `TODO`, `FIXME`, `HACK` は対象範囲でヒットなし。
- `NOTE` は `hia_import_dashboard_csv.py` のsnapshot key説明、`hia_build_delivery_zip.py` の納品対象SQL説明にあり。

**実際に利用されているDDL**
- `sql/ddl/work_other`: `0050`, `0051`, `0052`, `0053`, `0054`, `0055`, `0056`, `0057`, `0058`, `0059`, `0023`
- `sql/ddl/dev_phr`: `0010`, `0020`, `0080`, `0082`, `0083`, `0084`, `0072`, `0070`, `0015`, `0018`, `0065`, `0066`, `0090`
- migrationsで実使用カラムに関係: dashboard subscriber/identity系、`hia_person_years.identity_hash/name_kana_full_match`, `hia_import_zips.archived_zip_path`, `hia_xml_events.is_deleted/latest snapshot`, `subscribers` match/identity/hia_subscriber_id/symbol_export系。

---

# 第2回調査
## medi（健診結果処理）詳細調査

> 調査日時: 未記録

コード・DDLは変更せず、medi系だけを読み取り調査しました。

**1. 全体概要**
medi系は大きく4層あります。

1. 共有フォルダ観測層  
   `medi_shared_files`, `medi_shared_folder_aliases`

2. ZIP/XML受領層  
   `medi_import_runs`, `medi_zip_receipts`, `medi_zip_receipt_runs`, `medi_xml_receipts`, `medi_xml_receipt_runs`, `medi_xml_process_logs`, `medi_zip_passwords`

3. XML原本由来の台帳・生抽出層  
   `medi_xml_ledger`, `medi_xml_item_values`

4. XML生成用の健診結果台帳層  
   `medi_exam_result_ledger`, `medi_exam_result_item_values`

現行コードの中心は `scripts/kenshin_list_pydir` です。`scripts/work_folder` と `scripts/tokuho_xml_check` から medi テーブルを直接利用している実装は、今回の検索範囲では見当たりませんでした。

**2. medi系テーブル一覧**
- `medi_import_runs`
  - 役割: medi取込系の実行ログ。
  - PK: `run_id`
  - 関係: `medi_zip_receipt_runs`, `medi_xml_receipt_runs`, `medi_xml_process_logs` からFK参照。
- `medi_zip_receipts`
  - 役割: ZIP受領ログ、ZIP単位の構造判定。
  - 保持: 施設フォルダ、ZIP名/パス/SHA、構造ステータス、DATA/XML件数、エラー。
  - PK: `zip_receipt_id`
  - Unique: `zip_sha256`
  - 関係: `medi_zip_receipt_runs.zip_receipt_id`, `medi_xml_ledger.zip_receipt_id` と対応。
- `medi_zip_receipt_runs`
  - 役割: run×ZIPの検出実績。
  - PK: `zip_receipt_run_id`
  - FK: `run_id -> medi_import_runs`, `zip_receipt_id -> medi_zip_receipts`
- `medi_xml_receipts`
  - 役割: ZIP内XMLの受領台帳。
  - 保持: `zip_sha256`, `zip_inner_path`, `xml_sha256`, status, document_id, 人物/施設の索引情報、item抽出ステータス。
  - PK: `xml_receipt_id`
  - Unique: `(zip_sha256, zip_inner_path)`
- `medi_xml_receipt_runs`
  - 役割: run×XMLの検出実績。
  - PK: `xml_receipt_run_id`
  - FK: `run_id -> medi_import_runs`, `xml_receipt_id -> medi_xml_receipts`
- `medi_xml_process_logs`
  - 役割: XML処理ステップログ。
  - 保持: `WELLFORMED`, `CDA_INDEX`, `XSD_VALIDATE`, `EXTRACT_ITEMS`, `LEDGER` 等。
  - PK: `xml_process_log_id`
  - FK: `run_id -> medi_import_runs`
- `medi_xml_ledger`
  - 役割: XML単位の健診XML台帳。
  - 保持: ZIP/XML識別、保険者/保険証/氏名/生年月日/健診日/施設/報告区分/メタボ等、identity系。
  - PK: `xml_ledger_id`
  - Unique: `(zip_sha256, zip_inner_path_sha256)`
  - 関係: `zip_receipt_id`, `zip_sha256`, `xml_sha256` により受領系と接続。
- `medi_xml_item_values`
  - 役割: XMLから抽出した observation 値の生抽出テーブル。
  - PK: `xml_item_value_id`
  - Unique: `(xml_sha256, namecode, occurrence_no)`
  - 関係: `xml_sha256` で `medi_xml_receipts` / `medi_xml_ledger` と接続。
- `medi_lsio_identity_presence`
  - 役割: LSIO identity presence の中間生成テーブル。
  - PK: `(xml_sha256, group_code, identity_item_code)`
  - 利用実績: 今回対象コードでは見当たらず。
- `medi_lsio_missing_items`
  - 役割: 法定健診必要項目のXML別欠損テーブル。
  - PK: `id`
  - Unique: `(xml_sha256, group_code, identity_item_code)`
  - 利用実績: 今回対象コードでは見当たらず。
- `medi_exam_result_ledger`
  - 役割: XML生成用の健診結果ヘッダ/基本情報台帳。
  - PK: `ledger_id`
  - 関係: `medi_exam_result_item_values.ledger_id` と親子。
- `medi_exam_result_item_values`
  - 役割: XML生成用の健診項目値。raw→normalize→value。
  - PK: `item_value_id`
  - Unique: `(ledger_id, namecode, value_seq)`
- `medi_shared_files`
  - 役割: 共有フォルダ上のファイル観測台帳。
  - PK: `shared_file_id`
  - Unique: `path_hash`
- `medi_shared_folder_aliases`
  - 役割: 共有側フォルダ名から `medi_input` 配置フォルダ名への対応表。
  - PK: `alias_id`
  - Unique: `(src_folder_raw, is_active)`
- `medi_zip_passwords`
  - 役割: ZIPパスワード候補管理。
  - PK: `zip_password_id`

**3. XML処理フロー**
現行の主フローは以下です。

```text
共有フォルダ
  -> medi_shared_files_scan.py
  -> medi_shared_files

medi_shared_files
  -> medi_shared_files_hash_zip.py
  -> sha256付与

medi_shared_files
  -> medi_shared_files_auto_judge.py
  -> zip_has_xml / auto_judgement 更新

medi_shared_files + medi_shared_folder_aliases
  -> medi_shared_files_copy_to_input.py
  -> medi_input/<facility>/ にコピー

medi_input/<facility>/*.zip
  -> medi_zip_import.py ZIP_IMPORT
  -> medi_import_runs
  -> medi_zip_receipts
  -> medi_zip_receipt_runs
  -> medi_xml_receipts
  -> medi_xml_receipt_runs

medi_xml_receipts(status=PENDING)
  -> medi_zip_import.py XML_EXTRACT / kenshin_lib.medi.xml_extract
  -> medi_xml_process_logs
  -> medi_xml_receipts(status=OK/ERROR, document_id等)
  -> medi_xml_ledger

medi_xml_receipts(status=OK)
  -> medi_xml_item_extract.py
  -> medi_xml_item_values
  -> medi_xml_receipts.items_extract_status

medi_exam_result_ledger + medi_exam_result_item_values
  -> medi_export_xml.py
  -> CDA XML生成

--------------------------------------------------
# 第3回調査
## kenshin_list_pydir 実行フロー・責務解析
--------------------------------------------------

### 1. 実行順序
operator が健診結果を受領してから XML 生成までの現行フローは、概ね次の順序で進む。

1. `medi_shared_files_scan.py`
   - 入力: 共有フォルダ内のファイル/ディレクトリ
   - 出力: `medi_shared_files`
   - 更新テーブル: `medi_shared_files`
   - 更新ステータス: `stage_status` 等を走査・初期登録

2. `medi_shared_files_hash_zip.py`
   - 入力: `medi_shared_files`
   - 出力: `medi_shared_files` に ZIP SHA256 と構造情報を追記
   - 更新テーブル: `medi_shared_files`
   - 更新ステータス: SHA256 付き行の更新

3. `medi_shared_files_copy_to_input.py`
   - 入力: `medi_shared_files`, `medi_shared_folder_aliases`
   - 出力: `medi_input/<facility>/*.zip`
   - 更新テーブル: `medi_shared_files` のコピー済みステータス
   - 更新ステータス: `stage_status` / `copied_at` など（スクリプト内実装依存）

4. `medi_zip_import.py` (`MEDI_IMPORT_MODE=ZIP_IMPORT`)
   - 入力: `medi_input/<facility>/*.zip`
   - 出力:
     - `medi_import_runs`
     - `medi_zip_receipts`
     - `medi_zip_receipt_runs`
     - `medi_xml_receipts`（`MEDI_IMPORT_XML_ENABLED=true` 時）
     - `medi_xml_receipt_runs`
   - 更新テーブル: `medi_import_runs`, `medi_zip_receipts`, `medi_zip_receipt_runs`, `medi_xml_receipts`, `medi_xml_receipt_runs`
   - 更新ステータス: `medi_zip_receipts.structure_status` = `OK`/`ERROR`, `medi_xml_receipts.status` = `PENDING`/`ERROR`
   - 補足: ZIP 内 XML の棚卸しは `DATA` 配下優先だが、`DATA` がなくても全体探索で XML を拾う

5. `medi_zip_import.py` (`MEDI_IMPORT_MODE=XML_EXTRACT` または `FULL` 内 XML_EXTRACT)
   - 入力: `medi_xml_receipts` (`status=PENDING` が基本、`MEDI_IMPORT_XML_TARGET_STATUS` で制御)
   - 出力:
     - `medi_import_runs` の実行ログ
     - `medi_xml_process_logs`
     - `medi_xml_ledger`
   - 更新テーブル: `medi_xml_receipts`, `medi_xml_process_logs`, `medi_xml_ledger`
   - 更新ステータス: `medi_xml_receipts.status` = `OK`/`ERROR`, `medi_xml_receipts.document_id`, `medi_xml_receipts.extracted_run_id`
   - 内容: ZIP から XML member を読み出し、well-formed/CDA index/XSD validate の検証と `medi_xml_ledger` への UPSERT を実施

6. `medi_xml_item_extract.py`
   - 入力: `medi_xml_receipts` (`status=OK`)
   - 出力: `medi_xml_item_values`, `medi_xml_process_logs`
   - 更新テーブル: `medi_xml_item_values`, `medi_xml_receipts`, `medi_xml_process_logs`, `medi_import_runs`
   - 更新ステータス: `medi_xml_receipts.items_extract_status` = `OK`/`ERROR`/`SKIP`
   - 内容: XML の `observation` を走査し、`namecode`/`value` を抽出して `medi_xml_item_values` に UPSERT

7. `normalize_item_values.py`
   - 入力: `medi_exam_result_item_values` (`normalize_status='RAW'` かつ `value IS NULL/''`)
   - 出力: `medi_exam_result_item_values` の `value`, `normalize_status`, `normalized_at`, `normalize_error`
   - 更新テーブル: `medi_exam_result_item_values`
   - 更新ステータス: `normalize_status` = `OK`/`ERROR`
   - 内容: `dev_phr.exam_item_master` を参照し、`ST`/`PQ`/`CD`/`CO` の最小正規化を実施
   - 重要: 現行コード内に `medi_xml_item_values` から `medi_exam_result_item_values` へ転送する経路は見当たらない

8. `normalize_db_update.py`
   - 入力: `medi_xml_ledger`, `medi_exam_result_ledger`, `find_xml_subscribers_list_20260128`
   - 出力: それぞれの照合用派生列（`*_match`, `name_kana_match` 等）
   - 更新テーブル: `medi_xml_ledger`, `medi_exam_result_ledger`, `find_xml_subscribers_list_20260128`
   - 更新ステータス: `*_match` 系列の埋め戻し
   - 内容: `kenshin_lib.kana_match_normalizer`, `insurance_number_match_normalizer`, `insurance_symbol_match_normalizer` を使い、既存値を照合用に正規化

9. `medi_export_xml.py`
   - 入力: `medi_exam_result_ledger`, `medi_exam_result_item_values`, `dev_phr.exam_item_master`
   - 出力: CDA/XML ファイルおよび ZIP 生成
   - 更新テーブル: なし（DB 更新は行わない）
   - 状態: 生成対象テーブルの内容依存

### 2. import_submit_csv.py
- 役割: `SUBMIT_INBOX_ROOT` 配下の1件 CSV を読み、`SUBMIT_TARGET_TABLE` にそのまま INSERT する汎用 CSV インジェスト
- 入力:
  - CSV ファイル (`SUBMIT_CSV_FILENAME` も指定可)
  - `csv_header_map_submit` のマッピング定義
- 出力:
  - `SUBMIT_TARGET_TABLE` への INSERT
  - 必要に応じて `TRUNCATE TABLE` も実行
- 更新テーブル:
  - 任意の `SUBMIT_TARGET_TABLE`
  - `csv_header_map_submit` は参照のみ
- 呼び出しライブラリ:
  - `mysql.connector`
  - `dotenv.load_dotenv`
  - 標準 CSV/Path ライブラリ
- `medi_exam_result_ledger` / `medi_exam_result_item_values` との関係:
  - コード中に直接参照はなし
  - `SUBMIT_TARGET_TABLE` に `medi_exam_result_ledger` / `medi_exam_result_item_values` を指定すれば投入可能な汎用器
  - したがって現行実装上は「これらのテーブルへ直接書き込む専用経路ではない」

### 3. normalize_db_update.py
- 役割: 既存 DB データの「照合用派生列」を UPDATE するオーケストレーター
- 正規化対象:
  - `find_xml_subscribers_list_20260128` の `name_kana_match`, `insurance_number_match`, `insurance_symbol_match`
  - `medi_xml_ledger` の `name_kana_match`, `insurance_number_match`, `insurance_symbol_match`
  - `medi_exam_result_ledger` の `name_kana_match`, `insurance_card_number_match`, `insurance_card_symbol_match`
- 更新テーブル:
  - `medi_xml_ledger`
  - `medi_exam_result_ledger`
  - `find_xml_subscribers_list_20260128`
- 特徴:
  - 原本列を破壊せず、派生列に正規化結果を埋める
  - `NORMALIZE_JOB_*` 環境変数で対象ジョブを制御
  - `normalize_*` ライブラリを利用し、カナ/保険証記号/保険証番号を整形

### 4. normalize_item_values.py
- 役割: `medi_exam_result_item_values` の `RAW` 行を最小正規化し、XML 生成用 `value` を埋める
- 反映先テーブル:
  - `medi_exam_result_item_values`
- 処理対象値:
  - `raw_value` → `value`
  - `xml_value_type` に応じた `ST`/`PQ`/`CD`/`CO` の判定
- 更新内容:
  - `value`
  - `normalize_status` (`OK`/`ERROR`)
  - `normalized_at`
  - `normalize_error`
- 参照テーブル:
  - `dev_phr.exam_item_master` (`namecode` → `xml_value_type`, `result_code_oid`)
  - `dev_phr.norm_variants` (`raw_value_utf8` と `result_code_oid` の完全一致で `normalized_code` を取得)
- ポリシー:
  - CD/CO は raw_value の完全一致のみ
  - PQ は trim のみ許可し、数値変換できない場合はエラー
  - ST は raw_value をそのまま採用

### 5. medi_xml_item_extract.py
- 役割: 受領済み XML から `observation` を抽出し、生抽出レイヤー `medi_xml_item_values` に UPSERT
- 入力:
  - `medi_xml_receipts` (`status='OK'`)
  - `medi_zip_receipts` (`zip_path` を取得)
  - `medi_zip_passwords`（暗号 ZIP のパスワード候補）
- 出力:
  - `medi_xml_item_values`
  - `medi_xml_receipts.items_extract_status`
  - `medi_xml_process_logs`
  - `medi_import_runs`
- 処理概要:
  - `medi_xml_receipts` から対象 XML を取得
  - ZIP から XML member を読み出し、`ElementTree`/`lxml` で解析
  - `//cda:observation` を順次走査し、`code/@code` を `namecode` として採用
  - `value` がなければ `text` をフォールバック
  - `medi_xml_item_values` に `UNIQUE(xml_sha256, namecode, occurrence_no)` で UPSERT
  - `medi_xml_receipts.items_extract_status` を `OK`/`ERROR`/`SKIP` で更新

### 6. medi_zip_import.py
- 役割: ZIP 受領と XML 受領の二段階処理を担う runner
- `ZIP_IMPORT` フェーズ
  - 入力: `medi_input/<facility>/*.zip`
  - 出力:
    - `medi_import_runs`
    - `medi_zip_receipts`
    - `medi_zip_receipt_runs`
    - `medi_xml_receipts`（`MEDI_IMPORT_XML_ENABLED=true` 時）
    - `medi_xml_receipt_runs`
  - 更新ステータス:
    - `medi_zip_receipts.structure_status` = `OK`/`ERROR`
    - `medi_xml_receipts.status` = `PENDING`/`ERROR`
  - 内容:
    - ZIP 展開 (暗号対応あり)
    - DATA フォルダ優先で XML を検出
    - XML の SHA256 を算出し、`medi_xml_receipts` を PENDING で作成
    - well-formed チェックはオプションで、`PENDING` を基本とする

- `XML_EXTRACT` フェーズ
  - 入力: `medi_xml_receipts` (`target_status` で指定、デフォルト `PENDING`)
  - 出力:
    - `medi_xml_process_logs`
    - `medi_xml_ledger`
    - `medi_xml_receipts` の `status`/`document_id`/`extracted_run_id`
  - 更新ステータス:
    - `medi_xml_receipts.status` = `OK`/`ERROR`
    - `medi_xml_receipts.error_code`/`error_message`
  - 内容:
    - ZIP member を開き、`ElementTree`/`lxml` で解析
    - CDA index を抽出し `document_id` を得る
    - XSD validate はログのみで抽出継続
    - `medi_xml_ledger` を UPSERT

- `FULL` モード
  - `ZIP_IMPORT` → `XML_EXTRACT` を連続実行

### 7. Ledger 生成経路
- `medi_xml_ledger`
  - INSERT/UPDATE: `kenshin_lib/medi/xml_extract.py` (`db_upsert_xml_ledger`)
  - SELECT: `normalize_db_update.py`, バックフィルスクリプト

- `medi_xml_item_values`
  - INSERT/UPDATE: `scripts/medi_xml_item_extract.py` (`db_upsert_xml_item_value`)
  - SELECT: `medi_xml_item_extract.py` は `medi_xml_receipts` から抽出対象を選択

- `medi_exam_result_ledger`
  - INSERT: 現行 `scripts/kenshin_list_pydir` 直下では確認できず
  - UPDATE: `scripts/normalize_db_update.py`
  - SELECT: `scripts/medi_export_xml.py`, `scripts/normalize_db_update.py`

- `medi_exam_result_item_values`
  - INSERT: 現行 `scripts/kenshin_list_pydir` 直下では確認できず
  - UPDATE: `scripts/normalize_item_values.py`
  - SELECT: `scripts/medi_export_xml.py`, `scripts/normalize_item_values.py`

### 8. 責務分類
- `medi_shared_files_scan.py`: 受領 90%, 共通ライブラリ 10%
- `medi_shared_files_hash_zip.py`: 受領 90%, 共通ライブラリ 10%
- `medi_shared_files_copy_to_input.py`: 受領 90%, 共通ライブラリ 10%
- `medi_shared_files_auto_judge.py`: 受領 80%, 結果管理 20%
- `medi_zip_import.py`: 受領 60%, 結果管理 30%, 共通 10%
- `kenshin_lib/medi/xml_extract.py`: 受領 40%, 結果管理 40%, 共通 20%
- `medi_xml_item_extract.py`: 結果管理 60%, 受領 20%, 共通 20%
- `normalize_item_values.py`: 結果管理 70%, 共通 30%
- `normalize_db_update.py`: 結果管理 50%, 保険者変換 30%, 共通 20%
- `medi_export_xml.py`: 納品 70%, 結果管理 30%
- `import_submit_csv.py`: 受領 30%, 共通 70%
- `kenshin_lib/*`: 共通ライブラリ 100%

### 9. 実装されていない経路
- `medi_exam_result_ledger` / `medi_exam_result_item_values` への INSERT 元が、`scripts/kenshin_list_pydir` 直下の現在の Python 実装では見当たらない。
  - 推測: 手動挿入、`import_submit_csv.py` など汎用 CSV 投入、または別運用パイプラインが存在する
- `medi_xml_item_values` から `medi_exam_result_item_values` への変換処理は現行コードに実装なし。
  - 推測: `medi_exam_result_*` は XML 生成用に外部投入されたデータを前提としている
- `medi_lsio_identity_presence`, `medi_lsio_missing_items` は DDL に存在するが、今回対象の `scripts/kenshin_list_pydir` 内の実装では更新/参照が確認できない
- `medi_shared_folder_aliases` の投入経路も今回対象コード内では見当たらない（手動設定または別処理を想定）
```

`medi_exam_result_*` は、現行のZIP/XML受領フローから直接生成される経路は確認できませんでした。推測: 手動投入、Navicat import、Excel copy 等の別経路で作られるXML生成用テーブルです。

**4. 利用状況**
- `medi_import_runs`
  - SELECT: `medi_xml_item_extract.py`
  - INSERT/UPDATE: `db_medi.py` 経由で `medi_zip_import.py`, `medi_xml_item_extract.py`
  - DELETE: なし
- `medi_zip_receipts`
  - SELECT: `db_medi.py`, `medi_shared_files_copy_to_input.py`
  - INSERT/UPDATE: `db_medi.py` 経由で `medi_zip_import.py`
  - DELETE: なし
- `medi_zip_receipt_runs`
  - INSERT/UPDATE: `db_medi.py` 経由で `medi_zip_import.py`
  - SELECT/DELETE: なし
- `medi_xml_receipts`
  - SELECT: `db_medi.py` 経由で XML抽出/item抽出
  - INSERT/UPDATE: `medi_zip_import.py`, `xml_extract.py`, `medi_xml_item_extract.py`
  - DELETE: なし
- `medi_xml_receipt_runs`
  - INSERT/UPDATE: `db_medi.py` 経由で `medi_zip_import.py`
  - SELECT/DELETE: なし
- `medi_xml_process_logs`
  - INSERT/UPDATE: `xml_extract.py`, `medi_xml_item_extract.py`
  - SELECT/DELETE: なし
- `medi_xml_ledger`
  - INSERT/UPDATE: `xml_extract.py`, `normalize_db_update.py`, backfill script
  - SELECT: backfill script, `normalize_db_update.py`
  - DELETE: なし
- `medi_xml_item_values`
  - INSERT/UPDATE: `medi_xml_item_extract.py`
  - SELECT: item抽出対象の直接SELECTは `medi_xml_receipts`、値テーブル自体の読みは今回コードでは限定的
  - DELETE: なし
- `medi_exam_result_ledger`
  - SELECT: `medi_export_xml.py`, `normalize_db_update.py`
  - UPDATE: `normalize_db_update.py`
  - INSERT/DELETE: 見当たらず
- `medi_exam_result_item_values`
  - SELECT: `medi_export_xml.py`, `normalize_item_values.py`
  - UPDATE: `normalize_item_values.py`
  - INSERT/DELETE: 見当たらず
- `medi_shared_files`
  - SELECT/INSERT/UPDATE: shared系スクリプト群
  - DELETE: なし
- `medi_shared_folder_aliases`
  - SELECT: `medi_shared_files_copy_to_input.py`
  - INSERT/UPDATE/DELETE: 今回対象コードでは見当たらず
- `medi_zip_passwords`
  - SELECT: `zip_passwords.py`, `medi_zip_import.py`, `xml_extract.py`, `medi_xml_item_extract.py`
  - INSERT/UPDATE/DELETE: 見当たらず
- `medi_lsio_identity_presence`, `medi_lsio_missing_items`
  - DDLあり、今回対象コードでの利用実績は見当たらず。

**5. テーブル責務一覧**
- Receipt: `medi_zip_receipts`, `medi_xml_receipts`
- Ledger: `medi_xml_ledger`, `medi_exam_result_ledger`
- Work: `medi_xml_item_values`, `medi_exam_result_item_values`, `medi_lsio_identity_presence`, `medi_lsio_missing_items`
- Cache: `medi_shared_files`, `medi_shared_folder_aliases`
- Master: `medi_zip_passwords`
- Log: `medi_import_runs`, `medi_zip_receipt_runs`, `medi_xml_receipt_runs`, `medi_xml_process_logs`
- Status: `medi_xml_receipts.status`, `items_extract_status`, `medi_shared_files.stage_status`, `auto_judgement`, `manual_judgement`

**6. 重複・統合候補**
- XML Ledgerが複数存在:
  - `medi_xml_ledger`: XML原本由来の台帳。
  - `medi_exam_result_ledger`: XML生成用の健診結果台帳。
  - 推測: 目的が異なるが、「健診結果ヘッダ情報」という意味では重複領域あり。
- Item Valueが複数存在:
  - `medi_xml_item_values`: XMLからの生抽出。
  - `medi_exam_result_item_values`: XML生成用、正規化後 `value` を持つ。
  - 推測: raw抽出とXML生成用正規化済み値で役割分担しているが、変換パイプラインの接続は現状コード上では薄い。
- Receiptが二重管理:
  - ZIP: `medi_zip_receipts` と `medi_zip_receipt_runs`
  - XML: `medi_xml_receipts` と `medi_xml_receipt_runs`
  - これは重複というより「現在状態」と「run別検出履歴」の分離。
- 同じ情報:
  - `medi_xml_receipts` と `medi_xml_ledger` に `zip_sha256`, `zip_inner_path`, `xml_sha256`, 施設情報が重複。
  - `medi_zip_receipts` と `medi_xml_ledger` に施設/ZIP情報が重複。
  - 推測: ledger単体で参照しやすくするための冗長保持。

**7. health_exam_resultへの移行候補**
- `medi_import_runs`: 名前変更して移設  
  理由: `import_runs` として汎用化可能。
- `medi_zip_receipts`: 名前変更して移設  
  例: `source_zip_receipts`。ZIP受領の中核。
- `medi_zip_receipt_runs`: 名前変更して移設  
  例: `source_zip_run_events`。run×ZIP履歴として有用。
- `medi_xml_receipts`: 名前変更して移設  
  例: `source_xml_receipts`。XML受領・状態管理の中核。
- `medi_xml_receipt_runs`: 名前変更して移設  
  例: `source_xml_run_events`。
- `medi_xml_process_logs`: そのまま/名前変更して移設  
  例: `xml_process_logs`。Error Queueに近い役割も兼ねる。
- `medi_xml_ledger`: upgradeすれば利用できそう  
  理由: XML単位の原本台帳として有用。ただし未使用判定列やLSIO列が混在。
- `medi_xml_item_values`: そのまま/名前変更して移設  
  理由: raw XML item抽出レイヤーとして明確。
- `medi_exam_result_ledger`: 統合または名前変更して移設  
  理由: XML生成用の正規化済み結果台帳。`medi_xml_ledger` との関係設計が必要。
- `medi_exam_result_item_values`: 統合または名前変更して移設  
  理由: raw抽出値から正規化済み値への成果物として有用。
- `medi_shared_files`: 廃止候補または別DB/ops領域  
  理由: health_exam_result本体より前段のファイル観測台帳。
- `medi_shared_folder_aliases`: 廃止候補またはops/master領域  
  理由: 共有フォルダ運用補助。
- `medi_zip_passwords`: Masterとして移設候補  
  理由: 暗号ZIP対応に現行コードが依存。
- `medi_lsio_identity_presence`, `medi_lsio_missing_items`: 役割不明/upgrade候補  
  理由: DDLはあるが現行利用実績が見当たらない。LSIO判定を実装するなら再利用候補。

**8. 利用できる既存資産**
- ZIP展開: `kenshin_lib/medi/zip_extract.py`
- ZIP内XML判定: `kenshin_lib/medi/zip_inspect.py`
- パスワード候補取得: `kenshin_lib/medi/zip_passwords.py`
- DBアクセス集約: `kenshin_lib/medi/db_medi.py`, `db_shared_files.py`
- XML抽出: `kenshin_lib/medi/xml_extract.py`
- XML item抽出: `medi_xml_item_extract.py`
- XML validation: `xml_extract.py` 内のXSD検証、`scripts/tokuho_xml_check/validate_xml.py`
- normalize: `kenshin_list_pydir/lib/normalize/common.py`, `normalize_db_update.py`, `normalize_item_values.py`
- item mapping/master参照: `dev_phr.exam_item_master`, `norm_variants`
- XML生成: `medi_export_xml.py`

**9. 不明点・要確認事項**
- `medi_exam_result_ledger` / `medi_exam_result_item_values` のINSERT元が今回対象コードから見当たりません。推測: 手動投入または別運用。
- `medi_lsio_identity_presence` / `medi_lsio_missing_items` の生成・更新コードが見当たりません。
- `medi_xml_ledger` の `judge_status`, `is_exam_result`, `is_legal_exam`, `judge_*` はDDLコメント上も未使用。
- `medi_xml_ledger` のLSIO集計列はDDLにあるが、現行コードで更新箇所は見当たりません。
- migrationで medi テーブルに変更しているものは、確認できた範囲では `20260324_006_work_other_add_person_id_custom_and_identity_hash_to_medi_xml_ledger.sql` のみです。
- 不足概念（推測）:
  - Subject Status: 個人・年度単位の現在状態。
  - Delivery Queue: 健保/納品先別の出力待ち管理。
  - Error Queue: process logとは別の再処理対象キュー。
  - Import Batch / Source File lineage: ZIP/XML/item/結果台帳を一貫して追えるバッチ概念。
  - Raw to normalized mapping status: `medi_xml_item_values` から `medi_exam_result_item_values` への変換状態。