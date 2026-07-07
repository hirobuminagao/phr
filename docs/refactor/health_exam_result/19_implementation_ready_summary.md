# health_exam_result v2 実装前サマリー

## 1. 全体方針

本ドキュメントは、health_exam_result v2 のDDL作成・migration作成・スクリプト実装に入るため、`03_decisions.md` を正本として `01` から `04` のオーケストラスクリプト責務を実装粒度で整理したものである。

正本:

- `03_decisions.md`

参照資料:

- `05_design_history.md`
- `06_flow.md`
- `11_v2_script_design_notes.md`
- `12_v2_ddl_design_notes.md`
- `15_medi_to_v2_column_mapping.md`
- `16_legacy_legal_check_rule_summary.md`
- `17_legal_check_rule_diff.md`
- `docs/spec/health_examinations/02_exam_check_item_spec_v2_0_0.md`
- `docs/spec/health_examinations/03_medical_folder_aliases_initial_data_v2_0_0.md`
- `docs/spec/common_lib/00_inventory.md`

### v2初期スコープ

- `scripts/from_medical/config/scan_files.yml` の `event_id` から `dev_phr.event.result_root_path` を取得する。CLI引数は指定時のみconfig値を一時的に上書きする。
- `health_exam_result.medical_folder_aliases` を参照し、医療機関フォルダを解決する。
- 各医療機関フォルダ配下の `02_健診結果（編集）` を毎回フルスキャンする。
- Phase3では ZIP と健診結果本体XMLのファイル名規定に合う単体XMLの未登録ファイルのみ `file_receipts.status = DISCOVERED` で登録する。
- 単体XMLは `h*.xml` のみ登録対象とし、`ix08*.xml` / `su08*.xml` / schema関連 / XSD関連のXMLは登録しない。
- CSVは初期実装では `file_receipts` に登録せず、将来対応時にスキャン対象へ追加する。
- ZIP/XMLからXMLを検出し、XML内容を `xml_sha256` で一意判定する。
- 物理ファイルとXML内容の対応は `xml_file_links` に保持する。
- XML内容の正台帳は `xml_ledger` とし、同一 `xml_sha256` は重複作成しない。
- XMLに実際に存在した健診値を `exam_item_values` に縦持ち登録する。
- DB上の `xml_ledger` / `exam_item_values` と `dev_phr.exam_item_group_*` 系マスタから項目単位の制度チェックを実行し、`exam_check_results` を生成する方針とする。
- 制度チェックの判定フローは `XML -> exam_item_values -> exam_check_results -> check_result` とする。
- HIAアップロード用XMLをRun単位ディレクトリへ出力する。

### 初期DDL方針

- 初期DDLは `sql/ddl/health_exam_result/` 配下にテーブル単位で作成する。
- DDLファイル名は `NNNN_health_exam_result__<table_name>.sql` を基本とする。
- core DDLの初期作成対象は以下の7テーブルとする。
  - `etl_runs`
  - `etl_errors`
  - `medical_folder_aliases`
  - `file_receipts`
  - `xml_ledger`
  - `xml_file_links`
  - `exam_item_values`
- `exam_check_results` はcore DDLから一旦外し、制度チェックDDLとして別途DDL化する。
- `exam_check_results` の対象72項目、項目別 `status` / `reason` 方針、判定ルールをマスタに持たせる方針は設計済みとして扱う。

初期スコープ外:

- 人＋イベント単位の状態管理台帳の本格実装。
- 未着管理、医療機関回答待ち、再提出管理の本格運用。
- CSV直取込。
- HIAアップロード後の結果反映。
- `work` 領域の恒久保存・証跡保存。
- `zip_receipts`、legal presence中間テーブル、`csv_row_ledger`、`xml_export_logs` の初期作成。

### オーケストラスクリプト4本構成

人間が実行するスクリプトは以下の4本とする。

```text
01_scan_files.py
02_import_xml.py
03_check_exam_results.py
04_export_hia_xml.py
```

### work 一時利用方針

- `work` は恒久保存領域ではなく、処理中だけ使う一時作業領域である。
- `work` を触るのは原則 `02_import_xml.py` のみ。
- `01_scan_files.py` は `work` へコピーしない。
- `02_import_xml.py` が処理直前にコピーし、ZIPであれば展開する。
- 通常は処理完了後にコピー・展開済みファイルを削除する。
- デバッグ時のみ `--keep-work` のような明示オプションで一時保持できる。
- `file_receipts` には恒久的な `work_path` を持たせない。

### ETL共通責務

- `etl_runs` / `etl_errors` はADR準拠の実行証跡であり、業務状態の正ではない。
- 各オーケストラスクリプトは、自身のRun開始・終了・件数サマリーを `etl_runs` に記録する。
- 処理中の詳細エラーは `etl_errors` に記録する。
- 機械的な業務状態は各台帳の `status` / `xml_status` / `check_status` / `xml_export_status` に保持する。
- `02_import_xml.py` はRun単位で複数 `file_receipts` を処理するが、DBトランザクションは `file_receipt` 単位とする。

### 主要テーブル責務

| テーブル | 責務 |
| --- | --- |
| `file_receipts` | 物理ファイル単位の受領・処理サマリー台帳。人＋イベント単位の最終完了状態は持たない。 |
| `xml_file_links` | `file_receipts` と `xml_ledger` の対応台帳。ZIP内XMLパスを保持する。 |
| `xml_ledger` | XML内容単位の一意台帳。XML取込状態、加入者照合結果、XML単位の最新HIA出力状態を持つ。`file_receipt_id` は持たない。 |
| `exam_item_values` | 実際に存在した健診値の縦持ち。XML/CSV共通の将来拡張を想定し、`ledger_type` / `ledger_id` で由来を表す。 |
| `exam_check_results` | 制度チェック結果の横持ち台帳。対象72項目の `status_<item_code>` / `reason_<item_code>` を一度だけ保持し、制度単位の総合判定の入力とする。core DDLからは一旦外す。 |
| `medical_folder_aliases` | event単位の医療機関フォルダ名変換台帳。 |

### medical_folder_aliases 初期データ

- `docs/spec/health_examinations/03_medical_folder_aliases_initial_data_v2_0_0.md` を `medical_folder_aliases` 初期データの参照資料とする。
- 同資料には `event_id = 2` の医療機関フォルダ188件が初期データ候補として整理されている。
- 初期投入時点では原則 `src_folder_raw = dst_folder_norm` とする。
- `202604開院_福岡労働衛生研究所　健診スクエア博多` は仮フォルダ名の可能性があるため、初期データ投入時の注意事項として扱う。
- 上記の仮フォルダ名注意は初期実装のブロッカーではない。

## 2. `01_scan_files.py`

### 目的

対象イベントの医療機関フォルダ配下にある `02_健診結果（編集）` を毎回フルスキャンし、初期実装では ZIP / XML の未登録ファイルのみ `file_receipts.status = DISCOVERED` で登録する。

Phase3はファイル検出と `file_receipts` 登録に責務を限定し、ZIP展開・XML読込・健診値抽出は `02_import_xml.py` で実施する。CSVは初期実装では登録しない。

### 入力

- `scripts/from_medical/config/scan_files.yml`
- CLI引数（指定時のみconfig値を一時的に上書きする）
- `dev_phr.event.result_root_path`
- `health_exam_result.medical_folder_aliases`
- `<event.result_root_path>/<医療機関フォルダ>/02_健診結果（編集）` 配下のファイル

### 出力

- 新規 `file_receipts`
- スキャンRunの `etl_runs`
- 参照不可・ハッシュ計算不可などの `etl_errors`

### 主な処理順

1. `scripts/from_medical/config/scan_files.yml` を正本として読み込み、指定されたCLI引数のみ上書きする。
2. `etl_runs` に `01_scan_files.py` のRun開始を記録する。
3. `dev_phr.event` から `result_root_path` を取得する。
4. `medical_folder_aliases` を参照し、有効な医療機関フォルダを解決する。
5. 各医療機関フォルダの `02_健診結果（編集）` をフルスキャンする。
6. 対象ファイルの種別、ファイル名、`event.result_root_path` からの相対パス、サイズ、SHA256、医療機関情報を取得する。
7. 初期登録対象は ZIP と健診結果本体XMLのファイル名規定に合う単体XMLとし、CSV、隠しファイル、一時ファイル、対象外拡張子は `file_receipts` に登録しない。
8. 未知フォルダ、`is_active = 0` alias、`manual_judgement = 1` alias はスキップし、運用上対応が必要な事象として `etl_errors` に記録する。
9. 登録済みファイルを `file_receipts` で判定し、既存であれば登録しない。
10. 未登録ファイルのみ `file_receipts` に登録し、登録時の `etl_run_id` を保持する。
11. 登録件数・スキップ件数・エラー件数を標準出力へ表示し、可能な範囲で `etl_runs.notes` に集約してRun終了を記録する。

### 参照テーブル

- `dev_phr.event`
- `health_exam_result.medical_folder_aliases`
- `health_exam_result.file_receipts`

### 更新テーブル

- `health_exam_result.file_receipts`
- `etl_runs`
- `etl_errors`

### 作成/更新する主なカラム

`file_receipts`:

- `event_id`
- `file_role`
- `file_type`
- `file_name`
- `file_ext`
- `source_path`
- `relative_path`
- `file_sha256`
- `file_size`
- `insurer_number`
- `submitter_facility_code`
- `facility_code`
- `facility_name`
- `storage_folder_type`
- `status`
- `summary_message`
- `etl_run_id`
- `first_seen_at`
- `last_seen_at`
- `received_at`
- `created_at`
- `updated_at`

Phase3登録時の固定値・方針:

- `file_role = FROM_MEDICAL`
- `file_type = ZIP / XML`
- `storage_folder_type = MEDICAL_RESULT_ROOT`
- `status = DISCOVERED`
- `processable_count = NULL`
- `relative_path` は `event.result_root_path` からの相対パス
- `file_sha256` はPhase3スキャン時に計算

`file_type = OTHER` は初期実装では登録対象としない。`file_type = CSV` は将来CSV対応時に追加する。

単体XMLは `h*.xml` のみ登録対象とし、`ix08*.xml` / `su08*.xml` / schema関連 / XSD関連のXMLは登録しない。対象外XMLは `etl_errors` にも記録しない。ZIPはPhase3では中身を確認せず、ZIPファイル自体を登録する。

### status更新

- `file_receipts.status` は物理ファイル単位の機械的状態として使う。
- 値は `DISCOVERED` / `IMPORTING` / `IMPORTED` / `ERROR` とする。
- 登録直後は `DISCOVERED` とする。
- 登録済みスキップは原則エラーではなく、Runサマリーに残す。

### etl_runs / etl_errors の使い方

- `etl_runs` はスキャンRunの開始・終了・件数サマリーを保持する。
- Phase3は共通ETL構造の `phase = SCAN_FILES`、`source = FROM_MEDICAL` として記録する。
- Phase3の `etl_runs.status` は共通ETL仕様の `running / success / partial / failed` を利用する。
- `etl_errors` はフォルダ参照不可、ファイル属性取得不可、SHA256算出不可、DB登録失敗などを記録する。
- 登録済みスキップは通常エラーではないため、Runサマリーに集約する。
- 対象外ファイル（CSV、隠しファイル、一時ファイル等）は原則スキップし、`etl_errors` にも記録しない。
- `etl_errors` は運用上対応が必要な事象のみ記録する。
- Phase3固有の分類は共通ETL構造の `field` / `error_code` に寄せる。
- `notes` は人間が読みやすい短いテキストとし、JSON等の構造化データは採用しない。

### 再実行方針

- 毎回フルスキャンしてよい。
- 同一ファイルは `event_id` / `relative_path` / `file_sha256` を基準に判定し、重複登録しない。
- `work` を使わないため、一時ファイルの後始末は不要。

### エラー時の扱い

- 1ファイルの属性取得・ハッシュ計算に失敗しても、可能な限り他ファイルのスキャンを継続する。
- ルートパス未設定、医療機関フォルダ全体が参照不能など、Run継続不能なエラーはRun失敗として扱う。
- ファイル単位エラーは `etl_errors` に記録する。

### DDL作成時に必要なカラム

- `file_receipts.file_sha256`
- `file_receipts.source_path`
- `file_receipts.relative_path`
- `file_receipts.file_role`
- `file_receipts.file_type`
- `file_receipts.storage_folder_type`
- `file_receipts.status`
- `file_receipts.summary_message`
- `file_receipts.etl_run_id`
- `file_receipts.first_seen_at`
- `file_receipts.last_seen_at`
- `file_receipts.received_at`
- `medical_folder_aliases.event_id`
- `medical_folder_aliases.src_folder_raw`
- `medical_folder_aliases.dst_folder_norm`
- `dev_phr.event.result_root_path`

## 3. `02_import_xml.py`

### 目的

指定 `etl_run_id` の未処理 `file_receipts` を対象に、XML内容の取込、物理ファイルとのリンク、XML基本情報抽出、加入者照合、健診項目値抽出を一括で行う。

### 入力

- `etl_run_id`
- 未処理の `health_exam_result.file_receipts`
- 元ファイルの `source_path`
- `--keep-work` などのデバッグ用オプション

### 出力

- `xml_file_links`
- `xml_ledger`
- `exam_item_values`
- `file_receipts` の処理サマリー
- XML取込Runの `etl_runs`
- XML取込エラーの `etl_errors`

### 主な処理順

1. 既存 `scripts/from_medical/config/import_xml.yml` を正本として読み込み、指定されたCLI引数のみ上書きする。Phase4では設定項目の追加検討は行わない。
2. `etl_runs` にXML取込Run開始を記録する。
3. 通常実行は `event_id + file_receipts.status = DISCOVERED`、CLI `etl_run_id` 指定時のみ対象Runへ限定して未処理 `file_receipts` を取得する。
4. 対象 `file_receipt` ごとにDBトランザクションを開始する。
5. 対象ファイルを処理直前に `work` へ一時コピーする。
6. ZIPの場合は `02_import_xml.py` 内で展開し、XMLを列挙する。
7. ZIP内の取込対象XML件数を数え、`file_receipts.processable_count` に更新する。
   - Phase3ではZIP内件数を算出しない。
   - ZIP内対象XMLが0件の場合は `file_receipts.status = ERROR` とし、`etl_errors` に `field = ZIP`、`error_code = ZIP_NO_TARGET_XML` を基本として記録する。
8. XML単体ファイルの場合は、単体XMLとして扱う。
9. XMLごとに原本bytesから `xml_sha256` を算出する。parse不能XMLでもXMLファイル自体のSHA256から `xml_sha256` を算出する。
10. `xml_sha256` が既存の `xml_ledger` にあるか確認する。
11. 既存XMLの場合、`xml_ledger` は重複作成せず `xml_file_links` のみ追加する。
12. 新規XMLの場合、XML基本情報を抽出して `xml_ledger` を作成する。
    - parse不能XMLでも最小情報で `xml_ledger` を作成し、`xml_status = PARSE_ERROR` とする。
    - parse不能XMLではidentity系項目を設定せず、`exam_item_values` も登録しない。
    - parse不能XMLの詳細は `etl_errors` に `field = XML`、`error_code = XML_PARSE_FAILED` を基本として記録する。
13. XML基本情報から identity共通libで `person_id_custom` / `identity_hash` を生成する。
14. subscriber lookup共通libで `dev_phr.subscribers` を照合する。
15. 照合結果を `xml_ledger.subscriber_id` / `hia_subscriber_id` / `subscriber_match_*` に保持する。
16. XML内の健診項目raw値を抽出し、`exam_item_values` に登録する。
    - `exam_item_values` は `xml_ledger` 作成後に登録する。
    - XML解析が成功した場合は、identity生成に失敗しても登録する。
    - Phase4では検査値の正規化、バリデーション、`exam_item_status` 更新、`normalize_status` 更新、`validation_status` 更新を実施しない。
    - 一部検査値raw値の抽出に失敗した場合は、取得可能なraw値を登録し、不足・異常は `etl_errors` に記録して処理を継続する。
17. `file_receipts` に処理件数・処理状態を集約する。
18. その `file_receipt` 分のトランザクションをcommitする。
19. 処理完了後、通常は `work` のコピー・展開済みファイルを削除する。
20. 失敗した `file_receipt` はrollbackし、`etl_errors` に記録して次のファイルへ進む。
21. Run全体の件数サマリーを `etl_runs` に記録して終了する。

### 参照テーブル

- `health_exam_result.file_receipts`
- `health_exam_result.xml_ledger`
- `health_exam_result.xml_file_links`
- `dev_phr.subscribers`

### 更新テーブル

- `health_exam_result.file_receipts`
- `health_exam_result.xml_file_links`
- `health_exam_result.xml_ledger`
- `health_exam_result.exam_item_values`
- `etl_runs`
- `etl_errors`

### 作成/更新する主なカラム

`xml_file_links`:

- `event_id`
- `file_receipt_id`
- `xml_ledger_id`
- `xml_inner_path`
- `created_at`

`xml_ledger`:

- `event_id`
- `subscriber_id`
- `hia_subscriber_id`
- `xml_sha256`
- `document_id`
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
- `xml_status`
- `xml_reason`
- `check_status`
- `xml_export_status`
- `created_at`
- `updated_at`

`exam_item_values`:

- `event_id`
- `ledger_type`
- `ledger_id`
- `subscriber_id`
- `hia_subscriber_id`
- `namecode`
- `occurrence_no`
- `raw_value`
- `raw_value_type`
- `raw_unit`
- `normalized_value`
- `normalized_unit`
- `nullflavor`
- `code_system`
- `code_value`
- `code_display`
- `identity_item_code`
- `jun_no`
- `normalize_status`
- `normalize_reason`
- `validation_status`
- `validation_reason`
- `extracted_run_id`
- `extracted_at`
- `normalized_at`
- `created_at`
- `updated_at`

### work コピー・展開・削除方針

- コピーは対象 `file_receipt` の処理直前に行う。
- ZIP展開はこのスクリプトでのみ行う。
- XMLファイルの中身を読む処理はこのスクリプトに集約する。
- 通常は処理完了後に `work` を削除する。
- `--keep-work` 指定時のみデバッグ目的で一時保持する。

### xml_inner_path の扱い

- ZIP内XMLの場合、`xml_file_links.xml_inner_path` はZIP内相対パスを保持する。
- XML単体ファイルの場合、`xml_file_links.xml_inner_path` は `NULL` とする。

### xml_sha256 / 重複XMLの扱い

- `xml_sha256` はXML内容の一意キーである。
- 同一 `xml_sha256` のXMLは `xml_ledger` に重複作成しない。
- 同一XMLを別ZIP等で再受領した場合は、既存 `xml_ledger` を参照する `xml_file_links` のみ追加する。
- `duplicate_of_xml_ledger_id` は採用しない。
- `xml_ledger.file_receipt_id` は持たない。
- 既存XMLに対して `exam_item_values` を重複登録しない。
- 同一 `xml_sha256` の再受領時は `exam_item_values` を再登録しない。

### subscriber照合

- XML基本情報から、氏名カナ、保険証記号、保険証番号、生年月日、性別等を抽出する。
- XML基本情報のraw値からdictを作成し、`scripts.lib.identity.generator.generate_identity_bundle(**raw)` で `person_id_custom` / `identity_hash` を生成する。
- 入力キーは `birthdate`、`insurer_number_raw`、`insurance_symbol_raw`、`insurance_number_raw`、`name_kana_full_raw`、`gender_code` とする。
- Phase4が `generate_identity_bundle()` の戻り値として利用するのは、`ok`、`reason`、`person_id_custom`、`identity_hash`、`field_results` のみとする。
- XML parserはraw値抽出のみを担当し、identity用の独自正規化を実装しない。
- Phase4から `scripts/lib/identity/builder/` や `scripts/lib/identity/field/` を直接呼ばない。
- `identity_hash` を主キー相当の照合キーとして subscriber lookup を行う。
- 一致、未一致、複数一致、identity生成不可などを `subscriber_match_status` / `subscriber_match_method` / `subscriber_match_reason` に変換する。
- `hia_subscriber_id` は正ではなく、検索・調査用の冗長補助キーとする。
- XMLとして正常に読み込み可能な場合は、identity生成に失敗しても `xml_ledger` を作成し、詳細を `etl_errors` に `field = IDENTITY` として記録する。
- identity生成失敗時は `generator.reason` を代表理由、`field_results` を詳細ソース、`etl_errors.message` を人間確認用として扱う。
- identity生成失敗時の `etl_errors.error_code` は `IDENTITY_GENERATION_FAILED` を基本とする。
- identity生成失敗時の `etl_errors.message` は、`identity generation failed: <field>=NG(<reason>), <field>=NG(<reason>)` の形式を基本とし、複数fieldが失敗した場合はカンマ区切りで列挙する。

### Phase4 etl_errors 最小運用

- Phase4では `etl_errors.error_code` を必要最小限のコードセットで運用する。
- 主な `error_code` は `CONFIG_INVALID`、`FILE_NOT_FOUND`、`FILE_READ_FAILED`、`ZIP_OPEN_FAILED`、`ZIP_NO_TARGET_XML`、`XML_READ_FAILED`、`XML_PARSE_FAILED`、`XML_RAW_EXTRACT_FAILED`、`IDENTITY_GENERATION_FAILED`、`SUBSCRIBER_NOT_FOUND`、`SUBSCRIBER_LOOKUP_FAILED`、`DB_XML_LEDGER_SAVE_FAILED`、`DB_XML_FILE_LINK_SAVE_FAILED`、`DB_EXAM_ITEM_VALUES_SAVE_FAILED`、`DB_FILE_RECEIPT_STATUS_UPDATE_FAILED` を基本とする。
- `etl_errors.field` は `CONFIG`、`FILE`、`ZIP`、`XML`、`IDENTITY`、`SUBSCRIBER`、`DB` を基本とする。
- 検査値raw抽出エラーは `field = XML`、`error_code = XML_RAW_EXTRACT_FAILED` に寄せる。
- 検査値単位の詳細エラーコードと normalize / validation 専用エラーコードはPhase4では作成せず、Phase5で設計する。
- `etl_errors.message` は対象ファイル、対象XML、対象フィールド、理由を含む人間確認用テキストとする。
- parse不能XMLは `xml parse failed: path=<path>, inner_path=<inner_path>, reason=<parser_error>` を基本形式とする。
- ZIP内対象XML0件は `zip has no target xml: path=<path>, pattern=h*.xml, excludes=ix08,su08,schema,xsd` を基本形式とする。
- raw抽出失敗は `xml raw extract failed: path=<path>, inner_path=<inner_path>, field=<field>, reason=<reason>`、複数fieldの場合は `xml raw extract failed: path=<path>, inner_path=<inner_path>, fields=<field1>,<field2>, reason=<reason>` を基本形式とする。

### 状態管理

状態管理は責務単位で分離する。

- XML状態は `xml_status` で管理する。
- 加入者照合状態は `subscriber_match_status` で管理する。
- 検査値抽出・バリデーション状態は後続Phaseで `exam_item_status` として管理する。
- `xml_status` に加入者照合結果や検査値バリデーション結果を混在させない。
- 加入者照合NG時に `xml_status` は変更しない。
- `subscriber_match_status` と `exam_item_status` はそれぞれ独立した状態として扱う。ただしPhase4では `exam_item_status` を更新しない。
- `xml_ledger.exam_item_status` を追加する。必要に応じて `xml_ledger.exam_item_reason` も追加するが、更新は後続Phaseで実施する。
- `xml_ledger.exam_item_status` / `exam_item_reason` はDDL変更が必要なため、DDL更新と既存DB向けmigrationを同時に作成する。
- health_exam_result のMigrationファイル名は `YYYYMMDD_NNN_health_exam_result_<description>.sql` とし、連番はその日の `sql/migrations/health_exam_result/` 配下で採番する。
- `description` は英小文字 + snake_case とし、例は `20260707_001_health_exam_result_add_exam_item_status.sql` とする。
- DDLのみ更新してMigrationを後回しにしない。

Phase4で使用する正式コード:

- `file_receipts.status`: `DISCOVERED` / `IMPORTING` / `IMPORTED` / `WARNING` / `ERROR`
- `xml_status`: `READY` / `PARSE_ERROR`
- `subscriber_match_status`: `MATCHED` / `NOT_FOUND` / `IDENTITY_ERROR` / `NOT_EXECUTED`
- `exam_item_status`: `OK` / `WARNING` / `ERROR` / `NOT_EXECUTED` は後続正規化Phaseで使用する。Phase4では更新しない。

### exam_item_values 登録

- `exam_item_values` はPhase4（`02_import_xml.py`）で、`xml_ledger` 作成後に登録する。
- 実際にXML内に存在した健診値のみ登録する。
- XML解析が成功した場合は、identity生成に失敗しても `exam_item_values` を登録する。
- 制度チェック上の不足項目は `exam_item_values` には作らず、`exam_check_results` で判定する。
- `ledger_type = XML`、`ledger_id = xml_ledger.id` とする。
- `event_id` / `subscriber_id` / `hia_subscriber_id` は検索性向上の冗長カラムとして保持する。
- Phase4ではraw値、raw unit、nullFlavor、code系情報などを登録し、正規化済み値・正規化状態・妥当性判定は更新しない。
- Phase4では `exam_item_master` / `norm_variants` を用いた正規化・バリデーションを実施しない。
- 後続の正規化Phaseでは `exam_item_values` のraw値を入力とし、`exam_item_master`、必要に応じて `norm_variants`、`normalize_exam_item_value()` を用いて正規化・バリデーションを実施する。
- `xml_ledger.exam_item_status` や `file_receipts` への検査値サマリー反映は後続Phaseで実施する。

### file_receipt単位transaction

- `02_import_xml.py` はRun単位で複数ファイルを処理する。
- DBトランザクションは `file_receipt` 単位とする。
- 1ファイル失敗してもRun全体は止めない。
- 成功した `file_receipt` の処理結果は確定する。
- 失敗した `file_receipt` はrollbackし、`etl_errors` に記録して次のファイルへ進む。

### etl_runs / etl_errors の使い方

- `etl_runs` はXML取込Runの開始・終了・対象件数・成功件数・失敗件数を保持する。
- `etl_errors` はコピー失敗、ZIP展開失敗、XML parse失敗、XML基本情報不足、identity生成不可、subscriber照合例外、項目抽出失敗、DB登録失敗などを記録する。
- `etl_errors` は共通ETL構造を利用し、ファイル・XML・項目の補足情報は `src_file`、`field`、`field_value`、`error_code`、`message` などの既存カラムへ寄せる。
- `etl_runs` / `etl_errors` は証跡であり、処理状態の正は各台帳の状態カラムとする。
- Phase4のETL metricsは、`files` を処理対象 `file_receipts` 件数、`rows_seen` を対象XML件数、`rows_inserted` を新規 `xml_ledger` 件数、`rows_updated` を `xml_file_links` 登録件数 + `file_receipts` 更新件数、`rows_skipped` を既存 `xml_sha256` 再受領・対象外XML件数、`errors` を `etl_errors` 登録件数とする。
- `exam_item_values` 件数は `rows_inserted` に含めず、必要に応じて `etl_runs.notes` のサマリーへ記録する。

### 再実行方針

- 同一 `xml_sha256` のXMLは重複登録しない。
- 成功済みの `file_receipt` は再処理対象外にする。
- 失敗済みの `file_receipt` は状態とエラーを確認し、同じ `etl_run_id` または新しいRunで再実行する。
- 同一XMLの再受領は `xml_file_links` で受領事実を追加する。

### エラー時の扱い

- `file_receipt` 単位でrollbackする。
- エラー内容を `etl_errors` に記録する。
- 処理可能な次ファイルへ進む。
- `work` は原則削除する。`--keep-work` 指定時のみ保持する。

### DDL作成時に必要なカラム

- `xml_file_links.file_receipt_id`
- `xml_file_links.xml_ledger_id`
- `xml_file_links.xml_inner_path`
- `xml_ledger.xml_sha256`
- `xml_ledger.xml_status`
- `xml_ledger.xml_reason`
- `xml_ledger.subscriber_match_status`
- `xml_ledger.subscriber_match_method`
- `xml_ledger.subscriber_match_reason`
- `xml_ledger.exam_item_status`
- `xml_ledger.exam_item_reason`（必要に応じて）
- `xml_ledger.identity_hash`
- `xml_ledger.person_id_custom`
- `xml_ledger.subscriber_id`
- `xml_ledger.hia_subscriber_id`
- `exam_item_values.ledger_type`
- `exam_item_values.ledger_id`
- `exam_item_values.validation_status`
- `exam_item_values.validation_reason`
- `file_receipts.processable_count`
- `file_receipts.content_checked_at`
- `file_receipts.processed_at`

## 4. `03_check_exam_results.py`

### 目的

XMLファイルを再読込せず、DB上の `xml_ledger` / `exam_item_values` を入力に、項目単位の制度チェックを行い、`exam_check_results` に72項目分の `status` / `reason` を生成する。

制度チェックは「項目単位の判定」と「制度単位の総合判定」を分離する。判定フローは `XML -> exam_item_values -> exam_check_results -> check_result` とし、法定健診・特定健診の総合判定は `exam_check_results` を唯一の入力として算出する。総合判定では XML や `exam_item_values` を直接参照しない。

### 入力

- `event_id` または対象Run条件
- `health_exam_result.xml_ledger`
- `health_exam_result.exam_item_values`
- `dev_phr.exam_item_master`
- `dev_phr.exam_item_group_*` 系マスタ
- `docs/spec/health_examinations/02_exam_check_item_spec_v2_0_0.md` に定義された制度チェック対象72項目

### 出力

- `exam_check_results`
- 制度単位の `check_result`
- `xml_ledger.xml_export_status`
- チェックRunの `etl_runs`
- チェックエラーの `etl_errors`

### 主な処理順

1. `etl_runs` にチェックRun開始を記録する。
2. チェック対象の `xml_ledger` を取得する。
3. 対象XMLに紐づく `exam_item_values` を取得する。
4. `dev_phr.exam_item_group_*` 系マスタから制度チェックルールを取得する。
5. `exam_item_master` で `namecode` と `identity_item_code` の対応を解決する。
6. 共通72項目用グループに従って、項目別 `status_<item_code>` / `reason_<item_code>` を組み立てる。
7. `exam_check_results` をupsertまたは再作成する。
8. 制度単位の総合判定を行う場合は、法定健診判定用グループ・特定健診判定用グループに従って、`exam_check_results` の項目別 `status` を `check_result` へ集計する。
9. チェック結果と手動承認状態をもとに `xml_ledger.xml_export_status` を更新する。
10. `etl_runs` に件数サマリーを記録して終了する。

### 参照テーブル

- `health_exam_result.xml_ledger`
- `health_exam_result.exam_item_values`
- `dev_phr.exam_item_master`
- `dev_phr.exam_item_groups`
- `dev_phr.exam_item_group_members`
- `dev_phr.exam_item_group_method_members`
- `dev_phr.exam_item_group_identity_members`

### 更新テーブル

- `health_exam_result.exam_check_results`
- `health_exam_result.xml_ledger`
- `etl_runs`
- `etl_errors`

### 作成/更新する主なカラム

`exam_check_results`:

- `event_id`
- `ledger_type`
- `ledger_id`
- `subscriber_id`
- `hia_subscriber_id`
- `check_run_id`
- `checked_at`
- `status_<item_code>`
- `reason_<item_code>`
- `created_at`
- `updated_at`

`xml_ledger`:

- `check_status`
- `xml_export_status`
- `updated_at`

### 法定健診チェック

- 再提出・確認フローの主判定として扱う。
- 旧 `LSIO_Legal_Item` はpresence判定の土台としては使えるが、そのままではv2制度チェックとして不足する。
- v2仕様の法定健診対象は `17_legal_check_rule_diff.md` 上では35件で、旧 `LSIO_Legal_Item` の29件とは一致しない。
- `exam_check_results` の横持ち対象は制度チェック仕様の72項目を正とする。
- 旧presence中間テーブルは初期実装では作らず、`exam_item_values` とルールマスタから直接 `exam_check_results` を生成する。
- 法定健診の総合判定は、法定健診判定用グループに従って `exam_check_results` の項目別 `status` から集計する。

### 特定健診チェック

- warning / 参考判定を基本とする。
- 法定健診と同じ72項目の `status` / `reason` を利用し、特定健診専用の項目別 `status` / `reason` は二重に持たない。
- 特定健診の総合判定は、特定健診判定用グループに従って `exam_check_results` の項目別 `status` から集計する。
- 特定健診用グループは、初期実装ではマスタ未投入でも動作可能な構成とし、後でマスタを投入すれば判定できるようにする。

### 異常値チェック

- 値そのものの妥当性は `exam_item_values.validation_status` / `validation_reason` として扱う。
- 制度チェック上の影響は `exam_check_results.status_<item_code>` / `reason_<item_code>` に反映する。
- `dev_phr.exam_item_master` に異常値 min/max を追加するかは未決。

### check_result 集計

`check_result` は制度単位の最終判定を表す。

値:

- `OK`
- `WARNING`
- `NG`

基本方針:

- `check_result` は `exam_check_results` の項目別 `status` を制度グループ単位で集計した最終判定とする。
- 法定健診・特定健診の総合判定は、72項目の `status` をもとに、それぞれの制度グループ定義に従って `OK` / `WARNING` / `NG` を集計する。
- 総合判定は `exam_check_results` を唯一の入力とし、XML や `exam_item_values` を直接参照しない。
- 法定健診チェックは主判定とする。
- 特定健診チェックは原則warning / 参考判定とする。
- 制度チェック総合判定は、`exam_check_results` の制度判定結果から `xml_ledger.check_status` を生成する。
- 法定OK・特定OKの場合は `OK`、法定OK・特定WARNINGの場合は `WARNING`、法定NGの場合は `NG` とする。
- 特定健診不足は `WARNING`、法定健診不足は `NG` とする。
- システム判定結果として保持し、手動承認で変更しない。
- reason集約の詳細は未決として残す。

### xml_export_status の判定

- 出力可能なXMLは `xml_export_status = READY`。
- 出力対象外は `SKIPPED` または `PENDING` へ振り分ける。
- チェック処理例外により出力可否が決められない場合は `ERROR` の候補とする。
- 制度単位の判定が `NG` でも、医療機関確認等により正当理由が確認できた場合は、`manual_export_approved = true`、`manual_export_reason` を設定し、`xml_export_status = READY` とできる。

### exam_check_results.status_<item_code> / reason_<item_code> の扱い

- 対象項目は `02_exam_check_item_spec_v2_0_0.md` の72項目を正とする。
- カラム名は `status_<item_code>` / `reason_<item_code>` とする。
- 72項目の項目別 `status` / `reason` は `exam_check_results` に一度だけ横持ちで保持し、法定健診・特定健診で二重に持たない。
- 項目別statusは `OK` / `CALCULATED` / `ALTERNATIVE` / `MISSING` / `INVALID` とする。
- reasonは特記事項のみ保持し、`OK` の場合は `NULL` とする。
- reasonには算出元、代替元、不足理由、不正理由などを保持する。
- 判定ルール自体は `exam_check_results` に保持せず、`dev_phr.exam_item_group_*` 系マスタを利用する。
- XML処理結果ログおよび医療機関向けメッセージは、`reason` が `NULL` ではない項目を集約して生成する。

### ANY_NONEMPTY / ルールディスパッチ

- `ANY_NONEMPTY` は presence 判定のみを担当する。
- 対象 `namecode` 群のうち1つ以上に有効値が存在すれば充足とする。
- 行が存在するだけでは充足とせず、`NULL`・空値・無効値は充足扱いしない。
- 値の整合性チェック、付帯情報チェック、条件付き必須などの詳細判定は別ルールとして扱う。
- `CALCULATE` ルールは、対象同一性項目に有効値が存在しない場合のみ評価する。
- 対象同一性項目に有効値が存在する場合は、その値を採用し、項目別 `status = OK` とする。
- `CALCULATE` に必要な同一性項目がすべて揃う場合は、共通計算ライブラリを利用して値を生成し、項目別 `status = CALCULATED` とする。
- `CALCULATE` で値を確定できない場合のみ、`ALTERNATIVE` ルールを評価する。
- `ALTERNATIVE` が成立した場合は、対象項目を項目別 `status = ALTERNATIVE`、代替項目を項目別 `status = OK` とする。
- `CALCULATE` と `ALTERNATIVE` のいずれでも値を確定できない場合は、項目別 `status = MISSING` とする。
- 計算ロジックは共通ライブラリ `scripts/lib/examination/calc.py` へ実装し、制度チェック側は計算ライブラリを呼び出して `status` を決定する。
- `CALCULATE` と `ALTERNATIVE` は別ルールとして扱い、同一の処理フローへ混在させない。
- `ALTERNATIVE` は既存の identity 項目コードによる処理フローを利用する。
- `ALTERNATIVE` 共通処理は `scripts/lib/examination/alternative.py` に実装する。
- `ALTERNATIVE` 共通処理では、ケース判定と実処理関数を分離する。
- 制度チェックはルール種別をキーとして、対応する判定関数へディスパッチする構成とする。
- 新しい制度ルールは既存処理へ条件分岐を追加するのではなく、判定関数を追加して拡張できる構成とする。
- v2初期では `exam_item_group_identity_members` への追加カラムは作成しない。

### etl_runs / etl_errors の使い方

- `etl_runs` はチェックRunの開始・終了・対象件数・OK/WARNING/NG件数を保持する。
- `etl_errors` はマスタ不足、想定外値、チェック処理例外、DB更新失敗などを記録する。
- 業務状態の正は `exam_check_results`、制度単位の `check_result`、`xml_export_status` とする。

### 再実行方針

- XMLファイルは再読込しない。
- `exam_item_values` とマスタの現在状態から再計算する。
- 同一 `ledger_type` / `ledger_id` の `exam_check_results` はupsertまたは削除再作成のどちらかに統一する必要がある。

### エラー時の扱い

- XML単位で処理可能な範囲は継続する。
- チェック不能なXMLは制度単位の `check_result` または `xml_export_status` に反映し、詳細は `etl_errors` に記録する。
- マスタ不整合などRun継続不能な場合はRun失敗として扱う。

### DDL作成時に必要なカラム

- `exam_check_results.ledger_type`
- `exam_check_results.ledger_id`
- `exam_check_results.check_run_id`
- `exam_check_results.checked_at`
- 72項目分の `status_<item_code>`
- 72項目分の `reason_<item_code>`
- 制度単位の `check_result`
- `xml_ledger.xml_export_status`
- `xml_ledger.manual_export_approved`
- `xml_ledger.manual_export_reason`

## 5. `04_export_hia_xml.py`

### 目的

DB上のチェック結果・出力可否を参照し、HIAアップロード用XMLを生成する。

### 入力

- `event_id` または対象条件
- `xml_ledger.xml_export_status`
- `xml_ledger`
- `exam_item_values`
- `exam_check_results`
- 必要に応じて `file_receipts` / `xml_file_links` から辿れる元ファイル

### 出力

- HIAアップロード用XMLを含むZIP
- Run単位の出力ディレクトリ
- 出力Runの `etl_runs`
- 出力エラーの `etl_errors`
- `xml_ledger.xml_export_status` の出力済み状態

### 主な処理順

1. `etl_runs` に出力Run開始を記録する。
2. `xml_export_status = READY` の対象を取得する。
3. DB上の基本情報、健診値、チェック結果を取得する。
4. HIAアップロード用XMLを生成する。
5. 医療機関フォルダ配下の `03_健診結果（アップロード）` にRun単位ディレクトリを作成する。
6. `<event.result_root_path>/<医療機関フォルダ>/03_健診結果（アップロード）/yyyymmdd_hhmmss_<run_id>/<xxx.zip>` に書き出す。
7. 既存出力ファイルを上書きしない。
8. 出力成功時は `xml_ledger.xml_export_status = EXPORTED` に更新する。
9. 出力失敗時は `xml_ledger.xml_export_status = ERROR` とし、詳細を `etl_errors` に記録する。
10. `etl_runs` に件数サマリーを記録して終了する。

### 参照テーブル

- `dev_phr.event`
- `health_exam_result.medical_folder_aliases`
- `health_exam_result.xml_ledger`
- `health_exam_result.exam_item_values`
- `health_exam_result.exam_check_results`
- `health_exam_result.file_receipts`
- `health_exam_result.xml_file_links`

### 更新テーブル

- `health_exam_result.xml_ledger`
- `etl_runs`
- `etl_errors`

### 作成/更新する主なカラム

`xml_ledger`:

- `xml_export_status`
- `updated_at`

`etl_runs` / `etl_errors`:

- 出力Runの実行証跡
- 出力失敗詳細

### 出力先パス

出力先形式:

```text
<event.result_root_path>/<医療機関フォルダ>/03_健診結果（アップロード）/yyyymmdd_hhmmss_<run_id>/<xxx.zip>
```

### 既存出力を上書きしない方針

- `04_export_hia_xml.py` はRun単位ディレクトリを作成する。
- 既存出力ファイルは上書きしない。
- 出力済みファイルの削除・整理は運用側の責務とする。
- v2初期では出力履歴はRun単位の出力フォルダを証跡とする。
- `xml_export_logs` 等の出力台帳は、検索・監査・再出力履歴管理が必要になった場合のみ将来追加する。

### xml_export_status 更新

`xml_export_status` は `04_export_hia_xml.py` のHIA出力状態を表す。

値:

- `PENDING`
- `READY`
- `EXPORTED`
- `ERROR`
- `SKIPPED`

基本方針:

- `03_check_exam_results.py` が出力候補を `READY` にする。
- `04_export_hia_xml.py` は `READY` の対象を出力する。
- 出力成功後は `EXPORTED`。
- 出力失敗時は `ERROR`。
- 出力対象外は `SKIPPED` の候補。
- `xml_export_status` はv2初期では `xml_ledger` に保持し、XML単位の最新出力状態を管理する。

### etl_runs / etl_errors の使い方

- `etl_runs` は出力Runの開始・終了・出力件数・失敗件数を保持する。
- `etl_errors` は出力対象不整合、XML生成失敗、ZIP生成失敗、ファイル書込失敗などを記録する。
- 出力済み状態の正は `xml_ledger.xml_export_status` とする。

### 再実行方針

- 基本方針はDBの正規化済みデータから再生成する。
- XML原本を再読込する必要がある場合は、`file_receipts` / `xml_file_links` から元ファイルを辿る。
- 既存出力ファイルは上書きしない。
- すでに `EXPORTED` のXMLを再出力対象にするかは運用オプションとして要確認。

### エラー時の扱い

- 出力対象単位で失敗を `etl_errors` に記録する。
- 失敗したXMLは `xml_export_status = ERROR` とする。
- 他の出力対象を継続するか、Run全体を止めるかは実装前に要確認。

### DDL作成時に必要なカラム

- `xml_ledger.xml_export_status`
- `xml_ledger.manual_export_approved`
- `xml_ledger.manual_export_reason`
- `file_receipts.output_path` は出力ファイル台帳として使う場合のみ候補。v2初期のHIA出力履歴はRun単位フォルダを証跡とするため、XML単位出力履歴台帳は作らない。

## 6. 共通lib / script_lib 方針

### 既存common_libを使うもの

- DB接続: `scripts/lib/db/config.py`, `scripts/lib/db/mysql.py`
- subscriber lookup: `scripts/lib/db/lookup/subscriber.py`, `scripts/lib/db/lookup/subscriber_identity.py`
- identity生成: `scripts/lib/identity/generator.py`
  - `generator.py` を `identity_hash` / `person_id_custom` 生成の唯一の入口・正本とする。
  - Phase4では `generate_identity_bundle(**raw)` を利用する。
  - 利用側スクリプトから `scripts/lib/identity/field/*` / `scripts/lib/identity/builder/*` は直接呼ばない。
- ETL run/error基盤: `scripts/lib/etl/*`

### 既存common_lib側の確認が必要なもの

- `scripts/lib/db/schemas.py`
  - `HEALTH_EXAM_RESULT = "health_exam_result"` の追加候補。
- ETL run/error common
  - `scan` / `import_xml` / `check` / `export` に合うRun種別・phase定義が必要。
  - `02_import_xml.py` のRun単位処理＋file_receipt単位transaction例外をspecへ追記する。

### 新規common_lib化しないもの

- `medical_folder_aliases` 解決
- `file_receipts` 登録
- `xml_file_links` 登録
- `xml_ledger` 登録
- `exam_item_values` 抽出
- health_exam_result向けsubscriber照合結果変換
- `exam_check_results` 生成
- 法定健診チェック
- 特定健診チェック
- 異常値チェック
- HIA出力
- SHA256計算
- ZIP対象XML列挙
- XML解析
- directory discovery

これらは health_exam_result のDDL・業務状態・運用フォルダ構造に依存するため、固有 `script_lib` に置く。
SHA256計算は標準ライブラリ呼び出しで足りるため、共通ライブラリ化しない。

### health_exam_result固有script_libに置くもの

候補:

- `medical_folder_alias_service.py`
- `file_receipt_service.py`
- `work_file_manager.py`
- `xml_file_link_service.py`
- `xml_ledger_service.py`
- `health_exam_xml_parser.py`
- `exam_item_value_extractor.py`
- `subscriber_match_service.py`
- `exam_check_service.py`
- `hia_xml_export_service.py`

### 共通化を後回しにするもの

- CSV loader
  - v2初期はXML中心。既存specと実装APIに差分があるため、CSV直取込前に整備する。
- SHG XML I/O
  - ZIP/XML処理の参考にはなるが、SHG固有条件を含むため直接流用しない。
- 旧 `kenshin_list_pydir` 系
  - 旧medi DB/テーブル前提のため、仕様検討の参考に留める。

## 7. DDL作成前の最終確認リスト

DDLは固定せず、設計変更に合わせて更新する。新規環境は最新DDLから構築し、既存環境はMigrationで追従する。DDLを変更した場合、既存DBが対象となる変更についてはMigrationを同時に作成し、DDLのみ更新してMigrationを後回しにしない。health_exam_result のMigrationファイル名は `YYYYMMDD_NNN_health_exam_result_<description>.sql` とし、例は `20260707_001_health_exam_result_add_exam_item_status.sql` とする。

| 対象 | 必要になる主なカラム | 12記載 | 足りなそうなもの / 要確認 |
| --- | --- | --- | --- |
| `dev_phr.event` | `result_root_path` | あり | migration対象として確定が必要。 |
| `medical_folder_aliases` | `event_id`, `src_folder_raw`, `dst_folder_norm`, `is_active` | あり | 初期投入データは `03_medical_folder_aliases_initial_data_v2_0_0.md` を参照する。 |
| `file_receipts` | `event_id`, `file_role`, `file_type`, `source_path`, `relative_path`, `file_sha256`, `status`, `etl_run_id`, `processable_count`, `content_checked_at` | あり | 重複判定の一意制約、登録済みスキップの扱い。 |
| `xml_file_links` | `event_id`, `file_receipt_id`, `xml_ledger_id`, `xml_inner_path` | あり | 一意制約。例: `file_receipt_id + xml_inner_path`、または `file_receipt_id + xml_ledger_id + xml_inner_path`。 |
| `xml_ledger` | `xml_sha256`, 基本情報, subscriber照合結果, `xml_status`, `check_status`, `xml_export_status`, `manual_export_approved` | あり | `xml_sha256` unique、reason code、出力対象外時の `xml_export_status`。制度チェック総合判定は `check_status` に保持する。 |
| `exam_item_values` | `ledger_type`, `ledger_id`, `namecode`, raw/normalized値, `identity_item_code`, `validation_status` | あり | `ledger_type` enum、重複防止キー、値型別カラム範囲。 |
| `exam_check_results` | `ledger_type`, `ledger_id`, 72項目分の `status_` / `reason_` | あり | core DDLからは一旦外す。72項目・status/reason方針は設計済み。 |
| `etl_runs` | `run_id`, `phase`, `source`, `db_schema`, `started_at`, `finished_at`, `status`, metrics系カラム, `notes`, `admin_note` | あり | 既存 `scripts/lib/etl` の共通構造へ合わせる。 |
| `etl_errors` | `error_id`, `run_id`, `phase`, `source`, `src_file`, `field`, `field_value`, `error_code`, `message`, `created_at` | あり | 既存 `scripts/lib/etl` の共通構造へ合わせ、health_exam_result専用カラムは持たない。 |
| `dev_phr.exam_item_group_*` | 制度チェック用 group / identity member / presence rule | あり | 72項目対応のmigration / 初期データ追加が必要。v2初期では `exam_item_group_identity_members` への追加カラムは作成しない。 |
| `dev_phr.exam_item_master` | `identity_item_code`, namecode対応, 異常値min/max候補 | あり | min/max追加有無。 |

### 今回のスクリプト設計から必要になるテーブル

core DDLの初期作成対象:

- `health_exam_result.etl_runs`
- `health_exam_result.etl_errors`
- `health_exam_result.medical_folder_aliases`
- `health_exam_result.file_receipts`
- `health_exam_result.xml_ledger`
- `health_exam_result.xml_file_links`
- `health_exam_result.exam_item_values`

core DDLから一旦外し、制度チェックDDLとして別途DDL化するテーブル:

- `health_exam_result.exam_check_results`

参照する既存テーブル:

- `dev_phr.event`
- `dev_phr.subscribers`
- `dev_phr.funds`
- `dev_phr.fund_insurer_numbers`
- `dev_phr.exam_item_master`
- `dev_phr.exam_item_groups`
- `dev_phr.exam_item_group_members`
- `dev_phr.exam_item_group_method_members`
- `dev_phr.exam_item_group_identity_members`

### 未決として残すもの

- `xml_status` / `check_status` / `xml_export_status` のreason code詳細。
- `exam_item_values.validation_status` の正式値。
- XML単位の詳細ステータス（項目別・工程別）の追加要否。
- reason集約の詳細。
- `dev_phr.exam_item_group_*` 系マスタの72項目対応migration / 初期データ追加。
- `dev_phr.event.result_root_path` の既存DDL / migration確認。
- 人＋イベント台帳の正式名称。
- v2初期スコープに人＋イベント台帳を含めるか。
- 人＋イベント台帳に保持する `operation_status` の正式値。
- 再提出XMLをどの旧XMLの解決として扱うかの紐付け方法。
- HIA出力履歴台帳を将来追加するか。

## 8. 残未決事項

### 実装前に決めるべき

- `02_import_xml.py` の既存XML再受領時に、既存 `xml_ledger.xml_status` を変更しないか、`SKIPPED` をどこに記録するか。
- `02_import_xml.py` の失敗済み `file_receipt` 再実行条件。
- `03_check_exam_results.py` の `exam_check_results` 再計算方式。upsertか削除再作成か。
- reason集約の詳細。
- `03_check_exam_results.py` が `xml_export_status` を `READY` / `SKIPPED` / `PENDING` / `ERROR` へ振り分ける条件。
- `04_export_hia_xml.py` で `EXPORTED` 済みを再出力するオプション有無。
- `04_export_hia_xml.py` の出力単位失敗時にRun全体を継続するか。
- `etl_errors` に登録済みスキップを記録するか、Runサマリーに留めるか。

### DDL前に決めるべき

- `file_receipts.file_role`、`file_type`、`storage_folder_type` の値定義。
- `file_receipts` の登録済み判定キーと一意制約。
- `xml_file_links` の一意制約。
- `xml_ledger.xml_sha256` のunique制約。
- `xml_status` / `check_status` / `xml_export_status` のreason code詳細。
- `exam_item_values` の `ledger_type` enum、重複防止キー、raw/normalized保持範囲。
- `exam_item_values.validation_status` の正式値。
- 72項目分の `status_` / `reason_` カラムDDL。
- `dev_phr.event.result_root_path` migration。
- `dev_phr.exam_item_group_*` 系マスタの72項目対応migration / 初期データ追加。

### 実装しながら決めてよい

- `01_scan_files.py` の詳細な件数サマリー項目。
- `02_import_xml.py` の `--keep-work` オプション名、保持先命名。
- Phase4 `etl_errors.message` の組み立て補助関数をどこまで共通化するか。
- `work_file_manager.py` 等の固有script_lib分割粒度。
- HIA出力ZIP内ファイル名 `<xxx.zip>` の具体命名規則。ただし上書きしない方針は固定。
- ログ出力フォーマット。

### 将来スコープ

- 人＋イベント単位の状態管理台帳。
- 人間の業務確認状態 `operation_status` の本格実装。
- 再提出XMLと旧XMLの解決関係管理。
- 未着管理。
- 医療機関回答待ち管理。
- HIAアップロード後の結果反映。
- CSV直取込と `csv_row_ledger`。
- HIA出力履歴台帳 `xml_export_logs` 等。
- legal presence / missing 中間テーブル。

## 9. 初期実装GO判定

| 対象 | 判定 | 補足 |
| --- | --- | --- |
| core DDL 7テーブル | OK | `etl_runs`、`etl_errors`、`medical_folder_aliases`、`file_receipts`、`xml_ledger`、`xml_file_links`、`exam_item_values` を初期作成対象とする。 |
| `medical_folder_aliases` 初期データ準備 | OK | `03_medical_folder_aliases_initial_data_v2_0_0.md` を参照資料とする。 |
| `01_scan_files.py` | OK | `dev_phr.event.result_root_path` の既存DDL / migration確認は必要。 |
| `02_import_xml.py` | OK | Phase4はraw値登録までに責務を限定し、正規化・バリデーションは後続Phaseへ移す。 |
| `03_check_exam_results.py` の72項目別 `status` / `reason` 生成 | OK寄り | dev_phrマスタの72項目対応migration / 初期データ追加が必要。 |
| 法定健診・特定健診の `check_result` 総合判定 | OK寄り | `xml_ledger.check_status` への集約条件は確定済み。reason集約の詳細は未決。 |
| `04_export_hia_xml.py` | 後続でよい | v2初期では出力履歴台帳を作らず、Run単位フォルダを証跡とする。 |

## 10. 注意 / 要確認

- 旧 `LSIO_Legal_Item` は29件、v2仕様の法定健診対象は35件、`exam_check_results` 横持ち対象は72項目であり、粒度が異なる。DDL上の横持ちは72項目で確定だが、法定健診主判定に含める項目集合とマスタmigrationは要確認。
- `ANY_NONEMPTY` はpresence判定のみを担当し、対象 `namecode` 群のうち1つ以上に有効値が存在すれば充足とする。旧実装は行存在に近いため、v2実装では旧挙動をそのまま正としない。
- `file_receipts` に `output_path` 候補があるが、v2初期のHIA出力履歴はRun単位フォルダを証跡とする。出力ファイルを `file_receipts` に登録するかは要確認。
- `etl_runs` / `etl_errors` は既存共通libにあるが、現在のphase enumやライフサイクルspecがhealth_exam_resultの4本構成・file_receipt単位transactionにそのまま合わない可能性がある。
- `scripts/from_medical/script_lib/` は現時点で未存在。実装時に新規作成する必要がある。
