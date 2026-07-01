# 共通lib棚卸し

## 目的

health_exam_result v2 の実装設計に入る前に、既存の共通lib資産を棚卸しし、使える共通処理・不足しているspec・health_exam_result固有libへ置くべき処理を整理する。

対象:

- `scripts/lib/`
- `scripts/common/`
- `scripts/medical/script_lib/`
- `scripts/hia/`
- `scripts/shg/`
- `scripts/**/*.py`
- `scripts/**/README.md`
- `docs/spec/common/`
- `docs/spec/common_lib/`

調査時点の注意:

- `scripts/common/` は存在しない。
- `scripts/medical/script_lib/` は存在しない。
- `scripts/lib/` が現時点の共通lib本流。
- `scripts/work_folder/lib/` に旧共通lib相当が残るが、README上は `scripts/lib/` 側を正とする方針。
- `scripts/kenshin_list_pydir/` には健診結果旧実装があり、health_exam_result v2 では参考実装として扱う。

---

## 1. 既存lib一覧

### 1.1 `scripts/lib/`

| ファイルパス | 役割 | 主な関数 / クラス | 主な利用元 | README | spec |
| --- | --- | --- | --- | --- | --- |
| `scripts/lib/db/config.py` | `.env` / 環境変数からMySQL接続基盤情報を読む。schemaは扱わない。 | `MySQLBaseParams`, `load_mysql_base_params()` | `scripts/hia/*`, `scripts/shg/*`, `scripts/from_fund/*` | `scripts/lib/db/README.md` | `docs/spec/common/db_connection.md` |
| `scripts/lib/db/mysql.py` | MySQL接続、dict cursor、context manager。transactionは呼び出し側責務。 | `connect_mysql()`, `dict_cursor()`, `connect_ctx()` | `scripts/hia/*`, `scripts/shg/*`, `scripts/from_fund/*` | あり | `docs/spec/common/db_connection.md` |
| `scripts/lib/db/schemas.py` | schema名定数。 | `DEV_PHR`, `WORK_OTHER`, `ALL_SCHEMAS` | DB利用スクリプト全般 | あり | `docs/spec/common/db_connection.md` |
| `scripts/lib/db/lookup/fund.py` | 保険者番号から `fund_id` を参照する。 | `get_fund_id_from_insurer_number()`, `FundNotFoundError`, `FundAmbiguousError` | `scripts/from_fund/import_staging_subscribers_fund.py` | `scripts/lib/db/lookup/README.md` | `docs/spec/common/db_lookup.md` |
| `scripts/lib/db/lookup/subscriber.py` | `identity_hash` 等から subscriber を参照する薄いlookup。 | `list_subscribers_by_identity_hash()`, `get_single_subscriber_id_by_identity_hash()` | `scripts/from_fund/*` | あり | `docs/spec/common/db_lookup.md` |
| `scripts/lib/db/lookup/subscriber_identity.py` | subscriber identity resolver。HIA ID / identity_hash / person_id_custom の段階的lookup。 | `SubscriberIdentityLookupResult`, `resolve_subscriber_identity()` 系 | `scripts/hia/script_lib/hub_subscriber_current_snapshot.py` | あり | `docs/spec/common/db_lookup.md` だが詳細specは要確認 |
| `scripts/lib/db/lookup/subscriber_projection.py` | subscriber current snapshot/projection取得。 | 要確認 | `scripts/hia/script_lib/hub_subscriber_current_snapshot.py` | あり | `docs/spec/common/db_lookup.md` だが詳細specは要確認 |
| `scripts/lib/db/lookup/subscriber_addresses.py` | subscriber住所の参照。 | `get_current_addresses_by_subscriber_ids()` | `scripts/from_fund/update_staging_subscriber_diff_status.py` | あり | `docs/spec/common/db_lookup.md` だが詳細specは要確認 |
| `scripts/lib/db/lookup/subscriber_contact_points.py` | subscriber連絡先の参照。 | `get_contact_point_by_id()`, `get_current_contact_points_by_subscriber_ids()` | `scripts/hia/script_lib/apply_action_subscriber_contact_point.py`, `scripts/from_fund/*` | あり | `docs/spec/common/db_lookup.md` だが詳細specは要確認 |
| `scripts/lib/db/lookup/hia_company.py` | HIA会社マスタ参照。 | `fetch_hia_company_master_rows_by_insurer_number()` | `scripts/from_fund/update_staging_company_mapping_values.py` | あり | `docs/spec/common/db_lookup.md` だが詳細specは要確認 |
| `scripts/lib/db/insert/subscriber_audit.py` | `subscriber_audit` INSERT専用共通処理。 | `insert_subscriber_audit_row()`, `insert_subscriber_audit_rows()`, `touch_subscriber_last_change_run_id()` | `scripts/from_fund/script_lib/apply_subscribers_fund_name_parts.py` | `scripts/lib/db/README.md` | `docs/spec/common/subscriber_audit_insert.md` |
| `scripts/lib/etl/runs.py` | `etl_runs` の開始/終了、Run status判定。 | `start_run()`, `finish_run()` | `scripts/hia/*`, `scripts/from_fund/*` | `scripts/lib/etl/README.md` | `docs/spec/common/etl_common_lib.md`, `docs/spec/common/etl_run_lifecycle.md` |
| `scripts/lib/etl/errors.py` | `etl_errors` への構造化エラー記録。 | `log_error()` | `scripts/from_fund/import_staging_subscribers_fund.py` | あり | あり |
| `scripts/lib/etl/metrics.py` | Run件数カウンタ。 | `RunMetrics` | ETL利用スクリプト全般 | あり | あり |
| `scripts/lib/etl/progress.py` | 進捗ログ表示。 | `ProgressLogger` | ETL利用スクリプト全般 | あり | あり |
| `scripts/lib/etl/ddl.py` | `etl_runs` / `etl_errors` の存在保証DDL。 | `ensure_tables()` | `runs.py`, `errors.py` | あり | あり。ただしDDL責務は要再確認 |
| `scripts/lib/csv/csv_loader.py` | CSV読込、文字コード判定、BOM除去、ヘッダー/行iteration。 | `CSVLoader`, `load_csv()` | `scripts/hia/script_lib/hub_subscriber_import.py`, `scripts/from_fund/import_staging_subscribers_fund.py`, `scripts/lib/io/directory_discovery.py` | `scripts/lib/csv/README.md` | `docs/spec/common_lib/csv_loader.md` |
| `scripts/lib/io/directory_discovery.py` | directory存在確認、8桁directory列挙、suffix別ファイル列挙、CSV行数見積。 | `ensure_directory_exists()`, `list_files_by_suffix()`, `estimate_csv_rows()` | `scripts/hia/import_subscribers_to_staging_hub.py`, `scripts/hia/script_lib/hub_subscriber_import.py` | `scripts/lib/io/README.md` | specなし |
| `scripts/lib/hash/compare_hash.py` | compare用SHA256 hash生成。 | `build_compare_hash()` | `scripts/hia/*`, `scripts/hia/script_lib/hub_subscriber_import.py` | `scripts/lib/hash/README.md` | specなし |
| `scripts/lib/identity/generator.py` | rawから `person_id_custom` / `identity_hash` / bundle を生成するオーケストレーター。 | `generate_person_id_custom()`, `generate_identity_hash()`, `generate_identity_bundle()` | `scripts/hia/*`, `scripts/shg/check_shg_result_xml.py`, `scripts/from_fund/import_staging_subscribers_fund.py` | `scripts/lib/identity/README.md` | identity系specは別ディレクトリ。common_lib側specはなし |
| `scripts/lib/identity/primitive/*.py` | 文字種・日付・数字・空白・記号などの純関数。 | `to_nfkc()`, `extract_digits()`, `parse_yyyymmdd()` 等 | identity field層 | あり | identity系specあり。common_lib側specはなし |
| `scripts/lib/identity/field/*.py` | 項目別 normalize / match生成。 | `normalize_birthdate()`, `normalize_insurance_symbol()`, `normalize_name_kana_full()` 等 | HIA/from_fund/SHG | あり | identity系specあり。common_lib側specはなし |
| `scripts/lib/identity/builder/*.py` | canonical inputから完成キーを生成。 | `build_person_id_custom()`, `build_identity_hash()` | `identity/generator.py` | あり | identity系specあり。common_lib側specはなし |
| `scripts/lib/transform/relationship.py` | 続柄コード/名称の正規化・解決。 | `resolve_relationship_name()`, `normalize_relationship_code_match()` | `scripts/from_fund/import_staging_subscribers_fund.py` | なし | specなし |
| `scripts/lib/xml/delete.py` | XML Element削除の汎用ヘルパ。業務判定は持たない。 | `XmlDeleteTarget`, `XmlDeleteResult`, `delete_xml_element()` | `scripts/shg/script_lib/outcome_point_block_fix.py` | `scripts/lib/xml/README.md` | specなし |
| `scripts/lib/shg/xml/*.py` | SHG XML値抽出・更新・判定補助。 | `extract_basic()`, `extract_final_outcomes()`, `save_xml()` 等 | `scripts/shg/check_shg_result_xml.py`, `scripts/shg/script_lib/*` | `scripts/lib/shg/xml/README.md` | SHG固有。common specなし |

### 1.2 `scripts/hia/`

| ファイルパス | 役割 | 主な関数 / クラス | 主な利用元 | README | spec |
| --- | --- | --- | --- | --- | --- |
| `scripts/hia/import_subscribers_to_staging_hub.py` | HIA加入者CSVをstagingへ取込むエントリースクリプト。 | `main()` 等 | 運用実行 | `scripts/hia/config/README.md` | 個別specは要確認 |
| `scripts/hia/apply_hia_subscriber_sync.py` | HIA stagingから subscribers へ同期適用するエントリースクリプト。 | `main()` 等 | 運用実行 | あり | 要確認 |
| `scripts/hia/snapshot_hia_dashboard_year_end_status.py` | HIAダッシュボード年度末状態のスナップショット。 | `main()` 等 | 運用実行 | あり | 要確認 |
| `scripts/hia/script_lib/hub_subscriber_import.py` | HIA CSV取込の業務ロジック。CSV loader、identity、hash、ETLを利用。 | `process_csv_dir()`, `FolderMetrics` | `scripts/hia/import_subscribers_to_staging_hub.py` | config READMEのみ | 要確認 |
| `scripts/hia/script_lib/hub_subscriber_prepare.py` | HIA同期前処理。 | 要確認 | `apply_hia_subscriber_sync.py` | なし | 要確認 |
| `scripts/hia/script_lib/hub_subscriber_compare.py` | stagingと現行subscriberの比較・差分判定。 | `compare_hia_subscriber_apply_actions()` 等 | `apply_hia_subscriber_sync.py` | なし | 要確認 |
| `scripts/hia/script_lib/hub_subscriber_apply.py` | compare結果に基づく適用処理のオーケストレーション。 | `apply_hia_subscriber_rows()` | `apply_hia_subscriber_sync.py` | なし | 要確認 |
| `scripts/hia/script_lib/apply_action_subscriber*.py` | subscriber / address / contact / audit の適用処理。 | `apply_subscriber_root()` 等 | HIA apply | なし | 一部 `subscriber_audit` は共通specあり |

評価:

- HIA系は「共通libの利用実例」として有用。
- health_exam_result v2 へそのまま移植するより、ETL run/error、DB接続、CSV loader、identity利用パターンの参考にする。
- `script_lib` はHIAドメイン固有のため、共通libへは原則移さない。

### 1.3 `scripts/shg/`

| ファイルパス | 役割 | 主な関数 / クラス | 主な利用元 | README | spec |
| --- | --- | --- | --- | --- | --- |
| `scripts/shg/check_shg_result_xml.py` | SHG XMLチェック/修正のエントリースクリプト。 | `main()`, `build_xml_identity_from_basic()` 等 | 運用実行 | `scripts/lib/shg/xml/README.md` | 要確認 |
| `scripts/shg/generate_identity_from_shg_result.py` | SHG結果からidentity生成。 | `main()` | 運用実行 | なし | 要確認 |
| `scripts/shg/script_lib/xml_io.py` | SHG用XML/ZIP入出力、ZIP展開、対象XML収集。 | `read_xml()`, `extract_zip()`, `collect_input_xml_paths()` | `check_shg_result_xml.py` | なし | specなし |
| `scripts/shg/script_lib/shg_result_loader.py` | SHG結果DBロード系。 | 要確認 | SHG scripts | なし | 要確認 |
| `scripts/shg/script_lib/xml_ticket_writer.py` | SHG XML利用券/受診券更新。 | 要確認 | `check_shg_result_xml.py` | なし | 要確認 |
| `scripts/shg/script_lib/outcome_*` | SHGアウトカム判定・修正。 | `apply_outcome_total_point_block_fix()` 等 | `check_shg_result_xml.py` | なし | 要確認 |

評価:

- `scripts/lib/shg/xml/` はSHG専用の抽出層として整理済み。
- `scripts/shg/script_lib/xml_io.py` はZIP展開・XML収集の参考になるが、SHG固有除外条件と `lxml` 前提があるため health_exam_result v2 へはそのまま使わない方がよい。

### 1.4 `scripts/kenshin_list_pydir/`

| ファイルパス | 役割 | 主な関数 / クラス | 主な利用元 | README | spec |
| --- | --- | --- | --- | --- | --- |
| `scripts/kenshin_list_pydir/kenshin_lib/medi/zip_extract.py` | 旧健診結果向けZIP展開。 | 要確認 | 旧 `medi_zip_import.py` | なし | specなし |
| `scripts/kenshin_list_pydir/kenshin_lib/medi/xml_extract.py` | 旧健診結果XML基本情報/値抽出。 | `xml_extract_phase()` 等 | 旧 `medi_zip_import.py` | なし | specなし |
| `scripts/kenshin_list_pydir/kenshin_lib/medi/db_medi.py` | 旧medi DB操作。 | 要確認 | 旧medi scripts | なし | specなし |
| `scripts/kenshin_list_pydir/kenshin_lib/medi/db_shared_files.py` | 旧共有ファイル台帳操作。 | `db_upsert_shared_file()` 等 | 旧medi scan/judge scripts | なし | specなし |
| `scripts/kenshin_list_pydir/kenshin_lib/exam_value_normalizer.py` | 健診値の型別正規化。 | `normalize_by_type()`, `normalize_pq()` | 旧健診値正規化 | なし | specなし |
| `scripts/kenshin_list_pydir/kenshin_lib/*_match_normalizer.py` | 旧記号/番号/カナ match正規化。 | `normalize_*_for_match()` | 旧健診処理 | なし | specなし |

評価:

- health_exam_result v2 の直接移植元というより参考実装。
- DB設計・責務が旧medi前提のため、そのまま使うのは避ける。
- XML項目抽出・ZIPパス処理・健診値正規化の仕様検討材料として読む価値は高い。

### 1.5 旧/重複lib

| ファイルパス | 役割 | 判断 |
| --- | --- | --- |
| `scripts/work_folder/lib/db/*` | 旧DB接続lib | `scripts/lib/db/` へ統合済みのため、新規利用は避ける。 |
| `scripts/work_folder/lib/etl/*` | 旧ETL common lib | README上 `scripts/lib/etl/` が正。新規利用は避ける。 |
| `scripts/work_folder/lib/normalize/*` | 旧normalize lib | identity common化前の資産。新規利用は `scripts/lib/identity/` を優先。 |
| `scripts/work_folder/lib/custom_id_gen.py` | 旧custom_id生成 | `scripts/lib/identity/` を優先。 |
| `scripts/kenshin_list_pydir/lib/*` | 旧健診/旧identity補助 | 参考のみ。 |

---

## 2. 既存spec一覧

| spec | 対応する実装ファイル | 対応状況 |
| --- | --- | --- |
| `docs/spec/common/db_connection.md` | `scripts/lib/db/config.py`, `scripts/lib/db/mysql.py`, `scripts/lib/db/schemas.py` | 概ね対応。 |
| `docs/spec/common/db_lookup.md` | `scripts/lib/db/lookup/*.py` | 大枠は対応。個別lookupごとの詳細specは不足。 |
| `docs/spec/common/etl_common_lib.md` | `scripts/lib/etl/runs.py`, `metrics.py`, `errors.py`, `ddl.py`, `progress.py` | 概ね対応。health_exam_result v2 のRun単位処理＋file単位transactionとの整合メモは追加検討。 |
| `docs/spec/common/etl_run_lifecycle.md` | `scripts/lib/etl/runs.py`, `scripts/lib/etl/metrics.py`, `scripts/lib/etl/errors.py` | 対応。ただし現specは「transaction境界はrun単位」を標準としており、health_exam_result v2 の `02_import_xml.py` は例外設計が必要。 |
| `docs/spec/common/subscriber_audit_insert.md` | `scripts/lib/db/insert/subscriber_audit.py` | 対応。health_exam_result v2 では直接利用可能性は低い。 |
| `docs/spec/common_lib/csv_loader.md` | `scripts/lib/csv/csv_loader.py` | 一部不一致。specは `CsvLoadResult` / `disp_mode` / delimiter自動判定 / `count_rows` 引数を想定するが、現実装は `CSVLoader` 返却、delimiter既定`,`、`disp_mode`なし。 |

---

## 3. health_exam_result v2 で使えそうな共通処理

| 観点 | 判定 | 使える既存資産 | コメント |
| --- | --- | --- | --- |
| DB接続 | そのまま使える | `scripts/lib/db/config.py`, `mysql.py`, `schemas.py` | schema追加として `health_exam_result` 定数が必要になる可能性あり。実装修正前にspec化推奨。 |
| トランザクション | 少し修正すれば使える | `connect_ctx()` | 接続は使える。transaction境界は呼び出し側責務なので、`file_receipt` 単位commit/rollback設計はhealth_exam_result側で実装する。 |
| ETL run / error 記録 | 少し修正すれば使える | `scripts/lib/etl/*` | 基盤は使える。ただし現DDL/APIは行単位CSV取込寄り。ファイル/XML単位エラー、phase/source値、health_exam_result schema運用はspec調整が必要。 |
| config読込 | 参考だけ | `scripts/hia/config/README.md` と各HIA YAML | 共通YAML loaderは見当たらない。health_exam_result v2 では先に設定読込specを作る方がよい。 |
| logging | 要確認 | Python標準logging利用、`ProgressLogger` | 共通logging初期化libは見当たらない。 |
| SHA256 / hash | 少し修正すれば使える | `scripts/lib/hash/compare_hash.py` | compare hash用途は使えるが、ファイルSHA256 / XML bytes SHA256 用の共通関数は見当たらない。新規spec化推奨。 |
| file scan | 少し修正すれば使える | `scripts/lib/io/directory_discovery.py` | suffix列挙等は使える。再帰スキャン、除外、相対パス、医療機関フォルダ構造はhealth_exam_result固有側で実装。 |
| path操作 | 少し修正すれば使える | `directory_discovery.py` | 汎用Path補助は薄い。出力Runディレクトリ作成やsafe pathは不足。 |
| ZIP操作 | 参考だけ | `scripts/shg/script_lib/xml_io.py`, 旧 `kenshin_lib/medi/zip_extract.py` | 共通ZIP libはない。SHG/旧mediはドメイン前提あり。health_exam_result v2前に共通ZIP spec推奨。 |
| XML parse | 参考だけ | `scripts/lib/xml/delete.py`, `scripts/lib/shg/xml/*`, `scripts/shg/script_lib/xml_io.py` | XML削除は汎用だが parse/read/discovery は共通化されていない。健診XML parse基礎specが必要。 |
| CSV loader | 少し修正すれば使える | `scripts/lib/csv/csv_loader.py` | XML中心のv2初期では優先度低め。ただし将来CSV取込では重要。specと実装差分の解消が必要。 |
| text normalize | そのまま使える | `scripts/lib/identity/primitive/*`, `base_norm.py` | 低レイヤは使える。 |
| date parse | そのまま使える | `scripts/lib/identity/field/date_field.py`, `birthdate.py`, `primitive/dates.py` | 健診日などは `date_field.py` の用途ベース関数を優先。 |
| identity_hash / person_id_custom | そのまま使える | `scripts/lib/identity/generator.py`, `builder/*` | XML基本情報から raw を渡して bundle生成できる。必要項目不足時の扱いはhealth_exam_result側で状態化する。 |
| subscriber lookup | 少し修正すれば使える | `scripts/lib/db/lookup/subscriber_identity.py`, `subscriber.py` | lookup結果形式は使える。health_exam_resultの `subscriber_match_status` / reason への変換は固有lib側。 |

---

## 4. health_exam_result 固有libに置くべき処理

以下は共通libへ寄せず、`scripts/medical/script_lib/` または health_exam_result 専用libへ置く候補とする。

| 固有lib候補 | 理由 |
| --- | --- |
| `medical_folder_aliases` 解決 | `event.result_root_path`、医療機関フォルダ、`02_健診結果（編集）` という業務構造に依存するため。 |
| `file_receipts` 登録 | health_exam_result の物理ファイル台帳DDL・ステータス・重複判定に依存するため。 |
| `xml_file_links` 登録 | `file_receipts` と `xml_ledger` の対応台帳であり、health_exam_result固有の責務。 |
| `xml_ledger` 登録 | XML内容一意台帳、ステータス、基本情報、subscriber照合結果を持つhealth_exam_result固有処理。 |
| `exam_item_values` 抽出 | 健診XMLのentry/observation/nameCode/value型に依存するため。 |
| subscriber照合のhealth_exam_result向け適用 | identity共通libとsubscriber lookupは共通利用し、`subscriber_match_status` / reason / ledger更新は固有処理にする。 |
| `exam_check_results` 生成 | 横持ち72項目、制度チェックsummary、ledger集約に依存するため。 |
| 法定健診チェック | `dev_phr.exam_item_group_*` マスタ利用方針とhealth_exam_resultの判定運用に依存するため。 |
| 特定健診チェック | warning / 参考判定の業務方針に依存するため。 |
| 異常値チェック | 健診項目マスタや運用上のエラー/警告扱いに依存するため。 |
| HIA出力 | `03_健診結果（アップロード）` 出力先、Run単位ディレクトリ、HIA XML生成仕様に依存するため。 |

---

## 5. common_lib spec不足一覧

| 区分 | 対象 | 状況 | health_exam_result v2での扱い |
| --- | --- | --- | --- |
| 実装あり・spec不足 | `scripts/lib/io/directory_discovery.py` | READMEのみ。 | file scan / path操作の一部に使う可能性があるため、先にspec化推奨。 |
| 実装あり・spec不足 | `scripts/lib/hash/compare_hash.py` | READMEのみ。 | compare hash用途は既存READMEで足りるが、ファイルSHA256とは別物。ファイルSHA256 specが必要。 |
| 実装なし・spec不足 | file SHA256 / XML SHA256 | `compare_hash.py` は用途違い。 | `file_receipts.file_sha256` / `xml_ledger.xml_sha256` の正となるため、先にspec化推奨。 |
| 実装なし・spec不足 | ZIP展開共通lib | SHG/旧mediに個別実装あり。 | `02_import_xml.py` の中核。safe extract、文字化け、ZIP内相対パス、cleanup方針をspec化推奨。 |
| 実装なし・spec不足 | XML parse基礎 | XML削除libはあるが、parse/read/discovery共通はない。 | XML原本読込、namespace、parse error、bytes hash の扱いをspec化推奨。 |
| 実装あり・spec不一致 | `scripts/lib/csv/csv_loader.py` | `docs/spec/common_lib/csv_loader.md` と実装APIが一部不一致。 | v2初期はXML中心なので優先度中。将来CSV取込前に解消。 |
| specはあるが実装調整要 | `scripts/lib/etl/*` | specはあるが `etl_run_lifecycle.md` はtransaction境界run単位を標準としている。 | health_exam_result v2 のRun単位処理＋file_receipt単位transactionを例外として追記した方がよい。 |
| 実装あり・個別spec不足 | `scripts/lib/db/lookup/subscriber_identity.py` | lookup共通specはあるが、resolver返却形式の詳細specはない。 | subscriber照合に使うため、先にspec化推奨。 |
| 実装あり・common_lib specなし | `scripts/lib/identity/*` | identity専用specは別に存在するが common_lib棚卸し上の対応は分散。 | 既存spec参照でよいが、health_exam_result向け利用手順メモがあると安全。 |
| READMEのみ | `scripts/lib/xml/delete.py` | READMEあり、specなし。 | v2初期で使う可能性は低い。 |
| READMEのみ・SHG固有 | `scripts/lib/shg/xml/*` | READMEあり、SHG固有。 | 健診XMLには流用しない。XML探索構造の参考のみ。 |
| 実装あり・旧資産 | `scripts/kenshin_list_pydir/kenshin_lib/medi/*` | specなし。 | 旧健診処理の参考。共通lib化せず、必要な仕様だけ再設計。 |

---

## 6. 推奨アクション

### 6.1 先にspecを書くべきcommon_lib

1. `file_hash` / `xml_hash`
   - ファイルSHA256、XML内容SHA256、ZIP内XML bytes の読み方、改行・encodingを変えない方針を定義する。
2. `zip_extract`
   - safe extract、ZIP内相対パス、展開先、cleanup、エラー分類、パス traversal 防止を定義する。
3. `xml_parse_basic`
   - XML読込、namespace扱い、parse error、原本bytes保持/非保持、XML単体/ZIP内XMLの扱いを定義する。
4. `etl_run_lifecycle` のhealth_exam_result例外追記
   - 1 runで複数 `file_receipts` を処理し、DB transactionは `file_receipt` 単位とする例外を明記する。
5. `subscriber_identity_lookup`
   - `SubscriberIdentityLookupResult` のstatus、matched_by、multiple扱い、health_exam_result側への変換方針を定義する。
6. `directory_discovery`
   - 再帰/非再帰、suffix、相対パス、除外、存在チェックの共通範囲を定義する。

### 6.2 先に実装修正すべきcommon_lib

| 対象 | 推奨 |
| --- | --- |
| `scripts/lib/db/schemas.py` | `HEALTH_EXAM_RESULT = "health_exam_result"` を追加するか、schema定数の拡張方針をspec化してから追加する。 |
| `scripts/lib/etl/*` | `phase` enum が `import` / `apply` 固定のため、`scan` / `check` / `export` を扱うならDDL/APIの見直しが必要。実装前にspec更新。 |
| `scripts/lib/csv/csv_loader.py` | `docs/spec/common_lib/csv_loader.md` とAPI差分がある。将来CSV取込前に `CsvLoadResult` 化またはspec側修正を決める。 |
| `scripts/lib/io/directory_discovery.py` | health_exam_resultのフルスキャンには再帰探索・相対パス返却が必要になりそう。共通化範囲を決めてから追加。 |

### 6.3 health_exam_result v2 実装時に新規作成すべきscript_lib

| 候補 | 主な依存 |
| --- | --- |
| `medical_folder_alias_service.py` | `dev_phr.event`, `health_exam_result.medical_folder_aliases`, `scripts/lib/io` |
| `file_receipt_service.py` | `health_exam_result.file_receipts`, file hash common |
| `work_file_manager.py` | path/ZIP/hash common、ただし利用は `02_import_xml.py` に限定 |
| `xml_file_link_service.py` | `health_exam_result.xml_file_links` |
| `xml_ledger_service.py` | `health_exam_result.xml_ledger`, identity common |
| `health_exam_xml_parser.py` | XML parse common、健診XML item/basic extraction |
| `exam_item_value_extractor.py` | `dev_phr.exam_item_master`, 健診XML parser |
| `subscriber_match_service.py` | identity common, subscriber lookup common |
| `exam_check_service.py` | `exam_item_values`, `dev_phr.exam_item_group_*` |
| `hia_xml_export_service.py` | `xml_ledger`, `exam_item_values`, output path policy |

### 6.4 既存libのまま使うと危なそうなもの

| 対象 | 理由 |
| --- | --- |
| `scripts/work_folder/lib/*` | 共通化前の旧実装。新規利用すると import path と仕様が分散する。 |
| `scripts/kenshin_list_pydir/kenshin_lib/medi/*` | 旧medi DB/テーブル前提。v2 DDL責務と合わない。 |
| `scripts/shg/script_lib/xml_io.py` | ZIP/XML I/Oは参考になるが、SHG固有の対象XML除外条件と `lxml` 前提を含む。 |
| `scripts/lib/etl/ddl.py` | `etl_runs.phase` enum が狭く、health_exam_resultの4本構成にそのまま合わない可能性が高い。 |
| `scripts/lib/csv/csv_loader.py` | specと実装APIが不一致。CSV中心処理では先に解消が必要。 |

---

## 7. health_exam_result v2 で優先して使う / 整備するcommon_lib

優先して使う:

- `scripts/lib/db/config.py`
- `scripts/lib/db/mysql.py`
- `scripts/lib/db/schemas.py`
- `scripts/lib/identity/generator.py`
- `scripts/lib/identity/field/*`
- `scripts/lib/db/lookup/subscriber_identity.py`
- `scripts/lib/db/lookup/subscriber.py`

優先して整備する:

- ETL run/error common
- file/XML SHA256 common
- ZIP extract common
- XML parse basic common
- directory discovery common

参考に留める:

- `scripts/shg/script_lib/xml_io.py`
- `scripts/lib/shg/xml/*`
- `scripts/kenshin_list_pydir/kenshin_lib/medi/*`

使わない方がよい:

- `scripts/work_folder/lib/*` の新規利用
- 旧 `kenshin_list_pydir` のDB操作をv2へ直接移植すること
