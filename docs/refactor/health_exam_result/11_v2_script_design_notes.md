# health_exam_result v2 スクリプト設計メモ

このドキュメントは、`health_exam_result v2` のスクリプト構成・責務・実行順序を整理するためのメモである。`03_decisions.md` を正本として作成・更新する設計メモであり、正式決定事項はそこに従って反映する。

DDL側の骨子は `12_v2_ddl_design_notes.md` を参照する。

---

## 1. 目的

v2 初期実装で人間が実行するオーケストラスクリプトを4本に整理し、各スクリプトの目的・入出力・更新/参照テーブル・処理順・再実行方針・エラー記録方針を明確にする。

実処理は可能な限り既存の共通基盤または `scripts/from_medical/script_lib/` に切り出し、オーケストラスクリプトは処理順を表す薄いオーケストレーターとする。

---

## 2. 基本方針

### 2.1 スクリプト配置

```text
scripts/from_medical/
  人が実行するオーケストラスクリプトのみ配置する。

scripts/from_medical/config/
  医療機関取込処理用の設定ファイルを配置する。

scripts/from_medical/script_lib/
  医療機関取込処理内で再利用する業務固有共通処理を配置する。

scripts/lib/
  全システム共通ライブラリを配置する。
```

### 2.2 人間が実行するスクリプト

```text
01_scan_files.py
02_import_xml.py
03_check_exam_results.py
04_export_hia_xml.py
```

### 2.3 責務分離

- `01_scan_files.py` は対象フォルダを毎回フルスキャンし、未登録ファイルのみ `file_receipts` に登録する。重複ファイルは新規登録せず、スキップ件数としてRunサマリーに集約する。登録時の `file_receipts.status` は `DISCOVERED` とする。
- `01_scan_files.py` は `work` へのコピーを行わない。
- `02_import_xml.py` は指定 `etl_run_id` の未処理 `file_receipts` を対象にする。処理開始時に `file_receipts.status = IMPORTING`、成功時に `IMPORTED`、失敗時に `ERROR` とする。
- `02_import_xml.py` はRun単位で複数の `file_receipts` を処理し、DBトランザクションは `file_receipt` 単位とする。
- `work` への一時コピー、ZIP展開、XMLファイル読込は `02_import_xml.py` に集約する。
- `02_import_xml.py` はXML基本情報抽出、加入者照合、健診項目値抽出、`xml_file_links` / `xml_ledger` / `exam_item_values` 登録を一括で行う。
- `03_check_exam_results.py` はXMLファイルを再読込せず、DB上の `xml_ledger` / `exam_item_values` を入力にする。
- `03_check_exam_results.py` は統合された制度チェック対象72項目について、同一性項目コード単位で項目別 `status` / `reason` を生成し、`exam_check_results` に横持ちで保持する。
- 法定健診・特定健診で項目別 `status` / `reason` を二重に持たない。
- 制度単位の `check_result` は `exam_check_results` の項目別 `status` を制度グループ単位で集計する。
- 法定健診・特定健診の総合判定は `exam_check_results` を唯一の入力とし、XMLや `exam_item_values` を直接参照しない。
- `04_export_hia_xml.py` はDB上のチェック結果・出力状態を参照し、HIAアップロード用XMLを生成する。
- `work` 領域は恒久保存領域ではなく、処理中だけ利用する一時作業領域とする。
- 既に取り込み済みの重複ファイルは `file_receipts` に新規登録せず、重複件数は `etl_runs` のスキップ件数・実行サマリーで管理する。
- 機械的な処理状態は `xml_status` / `check_status` / `xml_export_status` で表し、人間の業務確認状態は将来の `operation_status` として分離する。
- `file_receipts` / `xml_ledger` には、人＋イベント単位の最終完了状態を背負わせない。

---

## 3. v2 初期実装の処理フロー

```text
01_scan_files.py
  event_id から event.result_root_path を取得
  medical_folder_aliases を参照
  02_健診結果（編集）配下を毎回フルスキャン
  未登録ファイルのみ file_receipts へ登録
  登録時の etl_run_id を file_receipts に保持
  ↓
02_import_xml.py
  指定 etl_run_id の未処理 file_receipts を取得
  処理直前に work へ一時コピー
  ZIPなら展開、XMLなら単体として処理
  XML SHA256算出
  xml_file_links 登録
  xml_ledger 登録または既存XML内容への紐付け
  XML基本情報抽出
  subscriber lookup
  exam_item_values 登録
  処理完了後に work を削除
  ↓
03_check_exam_results.py
  xml_ledger / exam_item_values を入力
  dev_phr.exam_item_group_* 系マスタを参照
  統合された制度チェック対象72項目の項目別 status / reason を生成
  exam_check_results 登録・更新
  exam_check_results から xml_ledger.check_status を生成
  xml_export_status へ反映
  ↓
04_export_hia_xml.py
  xml_ledger.xml_export_status とチェック結果を参照
  HIAアップロード用XMLを生成
  医療機関フォルダ配下の 03_健診結果（アップロード）へRun単位で出力
```

### 3.1 ステータス責務

| カラム | 担当スクリプト | 意味 | 値 |
| --- | --- | --- | --- |
| `xml_status` | `02_import_xml.py` | XML状態。XMLそのものの状態のみを表す。 | `READY` / `PARSE_ERROR` |
| `subscriber_match_status` | `02_import_xml.py` | 加入者照合状態。identity生成・加入者照合結果を表す。 | `MATCHED` / `NOT_FOUND` / `IDENTITY_ERROR` / `NOT_EXECUTED` |
| `exam_item_status` | 後続正規化Phase | 検査値抽出・バリデーション状態。Phase4では更新しない。 | `OK` / `WARNING` / `ERROR` / `NOT_EXECUTED` |
| `check_status` | `03_check_exam_results.py` | 制度チェック状態 | `PENDING` / `OK` / `WARNING` / `NG` |
| `xml_export_status` | `04_export_hia_xml.py` | HIA出力状態 | `PENDING` / `READY` / `EXPORTED` / `ERROR` / `SKIPPED` |

reason code の詳細は未決とし、機械的ステータスと人間の業務確認ステータスは混在させない。

`xml_status` に加入者照合結果や検査値バリデーション結果を混在させない。加入者照合NG時に `xml_status` は変更しない。`subscriber_match_status` と `exam_item_status` はそれぞれ独立した状態として扱う。ただしPhase4では `exam_item_status` を更新しない。

`xml_ledger.exam_item_status` はDDL追加が必要である。必要に応じて `xml_ledger.exam_item_reason` も追加し、DDL変更と既存DB向けMigrationを同時に作成する。health_exam_result のMigrationファイル名は `YYYYMMDD_NNN_health_exam_result_<description>.sql` とし、例は `20260707_001_health_exam_result_add_exam_item_status.sql` とする。DDLのみ更新してMigrationを後回しにしない。

---

## 4. オーケストラスクリプト

## 4.1 `01_scan_files.py`

### 目的

対象イベントの `02_健診結果（編集）` 配下を毎回フルスキャンし、初期実装では ZIP / XML の未登録ファイルのみ `file_receipts.status = DISCOVERED` で登録する。

Phase3はファイル検出と `file_receipts` 登録に責務を限定し、ZIP展開、XML読込、健診値抽出は `02_import_xml.py` へ送る。CSVは初期実装では登録せず、将来CSV対応時にスキャン対象へ追加する。

### 入力

- `scripts/from_medical/config/scan_files.yml`
- CLI引数（指定時のみconfig値を一時的に上書きする）
- `dev_phr.event.result_root_path`
- `health_exam_result.medical_folder_aliases`
- `event.result_root_path` 配下の `02_健診結果（編集）`

### 出力

- 新規登録された `file_receipts`
- スキャン実行を表す `etl_runs`
- スキップ・エラー内容を表す `etl_errors`

### 更新テーブル

- `health_exam_result.file_receipts`
- `etl_runs`
- `etl_errors`

### 参照テーブル

- `dev_phr.event`
- `health_exam_result.medical_folder_aliases`
- `health_exam_result.file_receipts`

### 主な処理順

1. `etl_runs` にスキャンRunを開始登録する。
2. `scripts/from_medical/config/scan_files.yml` を正本として読み込み、指定されたCLI引数のみ上書きする。
3. `dev_phr.event` から `result_root_path` を取得する。
4. 対象 `event_id` の `result_root_path` が未設定の場合はエラーとする。
5. `medical_folder_aliases` を参照し、イベント配下の医療機関フォルダを解決する。
6. 各医療機関フォルダの `02_健診結果（編集）` 配下をフルスキャンする。
7. ファイル種別、`event.result_root_path` からの相対パス、ファイルサイズ、SHA256、医療機関フォルダ情報を取得する。
8. 初期登録対象は ZIP と健診結果本体XMLのファイル名規定に合う単体XMLとし、CSV、隠しファイル、一時ファイル、対象外拡張子は `file_receipts` に登録しない。
9. 未知フォルダ、`is_active = 0` alias、`manual_judgement = 1` alias はスキップし、必要に応じて `etl_errors` に記録する。
10. 登録済みファイルはスキップし、必要に応じて `etl_errors` またはRunサマリーに記録する。
11. 未登録ファイルのみ `file_receipts` に追加し、登録時の `etl_run_id` を保持する。
12. `etl_runs` に件数サマリーと終了状態を記録する。

### 再実行方針

- 毎回フルスキャンしてよい。
- 既存 `file_receipts` と同一ファイルは登録せず、未登録ファイルだけを追加する。
- `file_receipts` の論理一意キーは `event_id` / `relative_path` / `file_sha256` のままとする。
- `relative_path` は `event.result_root_path` からの相対パスとする。
- `file_sha256` はPhase3スキャン時に計算する。
- `processable_count` はPhase3では設定せず `NULL` とする。
- DDL実装ではMySQLのキー長などの制約回避のため、長尺文字列部分をSHA256生成列へ変換してUNIQUE制約へ含めるが、スクリプト上の重複判定の考え方は論理一意キーを基準とする。
- `work` へのコピーは行わないため、再実行しても一時ファイルの後始末は不要。

### `file_receipts` 登録値

Phase3登録時の固定値は以下とする。

- `file_role = FROM_MEDICAL`
- `file_type = ZIP / XML`
- `storage_folder_type = MEDICAL_RESULT_ROOT`
- `status = DISCOVERED`
- `processable_count = NULL`

`file_type = OTHER` は初期実装では登録対象としない。`file_type = CSV` は将来CSV対応時に追加する。

単体XMLは `h*.xml` のみ登録対象とし、`ix08*.xml` / `su08*.xml` / schema関連 / XSD関連のXMLは登録しない。対象外XMLは `etl_errors` にも記録しない。ZIPはPhase3では中身を確認せず、ZIPファイル自体を登録する。

### ETL状態値

- ETL記帳は `scripts/lib/etl` の共通APIを利用する。
- `etl_runs.phase = SCAN_FILES`、`etl_runs.source = FROM_MEDICAL` とする。
- `etl_runs.status` は共通ETL仕様の `running / success / partial / failed` を利用する。
- scan結果サマリーは標準出力に表示し、可能な範囲で `etl_runs.notes` に記録する。
- `notes` は人間が読みやすい短いテキストとし、JSON等の構造化データは採用しない。

`etl_errors` は運用上対応が必要な事象のみ記録する。対象外ファイル（CSV、隠しファイル、一時ファイル等）は原則スキップし、`etl_errors` にも記録しない。Phase3固有の分類は共通ETL構造の `field` / `error_code` に寄せ、将来必要に応じて拡張する。

### エラー記録方針

- フォルダ参照不可、ファイル属性取得不可、SHA256算出不可などは `etl_errors` に記録する。
- 対象 `event_id` の `result_root_path` 未設定はエラーとして記録する。
- 登録済みスキップは原則エラーではなく、Runサマリーまたはスキップ理由として記録する。

---

## 4.2 `02_import_xml.py`

### 目的

通常実行は `event_id + file_receipts.status = DISCOVERED` を対象に、CLI `etl_run_id` 指定時のみ対象Runへ限定して、XML内容の取込、物理ファイルとのリンク、基本情報抽出、加入者照合、健診項目raw値抽出を行う。

### 入力

- `etl_run_id`
- 未処理の `health_exam_result.file_receipts`
- 元ファイルの `source_path`
- `scripts/from_medical/config/import_xml.yml`
- `--keep-work` などのデバッグ用オプション

### 出力

- `xml_file_links`
- `xml_ledger`
- `exam_item_values`
- `file_receipts` の処理サマリー
- `etl_runs` / `etl_errors`

### 更新テーブル

- `health_exam_result.file_receipts`
- `health_exam_result.xml_file_links`
- `health_exam_result.xml_ledger`
- `health_exam_result.exam_item_values`
- `etl_runs`
- `etl_errors`

### 参照テーブル

- `health_exam_result.file_receipts`
- `health_exam_result.xml_ledger`
- `health_exam_result.xml_file_links`
- `dev_phr.subscribers`

### 主な処理順

1. 既存 `scripts/from_medical/config/import_xml.yml` を正本として読み込み、指定されたCLI引数のみ上書きする。Phase4では設定項目の追加検討は行わない。
2. `etl_runs` にXML取込Runを開始登録する。
3. 通常実行は `event_id + file_receipts.status = DISCOVERED`、CLI `etl_run_id` 指定時のみ対象Runへ限定して未処理 `file_receipts` を取得する。
4. 対象 `file_receipt` ごとにDBトランザクションを開始する。
5. 対象ファイルを処理直前に `work` へ一時コピーする。
6. ZIPの場合はこのスクリプト内でのみ展開する。
7. ZIP内の取込対象XML件数を数え、`file_receipts.processable_count` に更新する。
   - Phase3ではZIP内件数を算出しない。
   - ZIP内対象XMLが0件の場合は `file_receipts.status = ERROR` とし、`etl_errors` に `field = ZIP`、`error_code = ZIP_NO_TARGET_XML` を基本として記録する。
8. XML単体ファイルの場合はそのままXMLとして扱う。
9. XMLごとに `xml_sha256` を算出する。parse不能XMLでもXMLファイル自体のSHA256から `xml_sha256` を算出する。
10. `xml_sha256` が既存の場合は、`xml_ledger` を重複作成せず `xml_file_links` のみ追加する。
11. `xml_sha256` が未登録の場合は、XML基本情報を抽出して `xml_ledger` を登録し、取込成功時は `xml_status = READY` とする。
    - parse不能XMLでも最小情報で `xml_ledger` を作成し、`xml_status = PARSE_ERROR` とする。
    - parse不能XMLではidentity系項目を設定せず、`exam_item_values` も登録しない。
    - parse不能XMLの詳細は `etl_errors` に `field = XML`、`error_code = XML_PARSE_FAILED` を基本として記録する。
12. XML基本情報のraw値からdictを作成し、`scripts.lib.identity.generator.generate_identity_bundle(**raw)` で `person_id_custom` / `identity_hash` を生成し、`dev_phr.subscribers` と照合する。
13. 照合結果を `xml_ledger` に保持する。
14. XML内の健診項目raw値を抽出し、`exam_item_values` に登録する。
    - `exam_item_values` は `xml_ledger` 作成後に登録する。
    - XML解析が成功した場合は、identity生成に失敗しても登録する。
    - Phase4では検査値の正規化、バリデーション、`exam_item_status` 更新、`normalize_status` 更新、`validation_status` 更新を実施しない。
    - XML内に項目entryとして存在したものは、可能な限りraw値の行を作る。
    - 一部検査値raw値の抽出に失敗した場合は、取得可能なraw値を登録し、不足・異常は `etl_errors` に記録して処理を継続する。
    - `exam_item_master`、`norm_variants`、`normalize_exam_item_value()` を用いた正規化・バリデーションは後続Phaseで実施する。
15. `file_receipts` に処理件数・処理状態を集約し、そのファイル分のトランザクションを確定する。
16. 処理完了後、通常は `work` のコピー・展開済みファイルを削除する。
17. `--keep-work` 指定時のみ、デバッグ目的で一時ファイルを保持する。
18. 失敗した `file_receipt` はロールバックして `etl_errors` に記録し、次のファイルへ進む。

### 再実行方針

- 同一 `xml_sha256` のXMLは `xml_ledger` に重複作成しない。
- 同一XMLを別ZIP等で再受領した場合は `xml_file_links` のみ追加し、基本情報抽出・加入者照合・`exam_item_values` 登録は再実行しない。
- 同一 `xml_sha256` の再受領時は `exam_item_values` を再登録しない。
- Run単位で対象 `file_receipts` を処理するが、成功した `file_receipt` の処理結果は確定する。
- 1ファイル失敗してもRun全体は止めず、失敗分は `etl_errors` に記録して次のファイルへ進む。
- 途中失敗したファイルは、状態とエラーを確認して同じ `etl_run_id` または新しいRunで再実行する。

### エラー記録方針

- コピー失敗、ZIP展開失敗、XML parse失敗、基本情報不足、加入者照合NG、項目抽出失敗は `etl_errors` に記録する。
- `etl_errors` は共通ETL構造を利用し、ファイル・XML・項目の補足情報は `src_file`、`field`、`field_value`、`error_code`、`message`、`notes` 相当の既存表現へ寄せる。
- Phase4の `etl_errors.field` は `CONFIG` / `FILE` / `ZIP` / `XML` / `IDENTITY` / `SUBSCRIBER` / `DB` を基本とする。
- Phase4の `etl_errors.error_code` は `CONFIG_INVALID`、`FILE_NOT_FOUND`、`FILE_READ_FAILED`、`ZIP_OPEN_FAILED`、`ZIP_NO_TARGET_XML`、`XML_READ_FAILED`、`XML_PARSE_FAILED`、`XML_RAW_EXTRACT_FAILED`、`IDENTITY_GENERATION_FAILED`、`SUBSCRIBER_NOT_FOUND`、`SUBSCRIBER_LOOKUP_FAILED`、`DB_XML_LEDGER_SAVE_FAILED`、`DB_XML_FILE_LINK_SAVE_FAILED`、`DB_EXAM_ITEM_VALUES_SAVE_FAILED`、`DB_FILE_RECEIPT_STATUS_UPDATE_FAILED` を基本とする。
- 検査値raw抽出エラーは `field = XML`、`error_code = XML_RAW_EXTRACT_FAILED` に寄せ、検査値単位の詳細エラーコードと normalize / validation 専用エラーコードはPhase4では作成しない。
- `etl_errors.message` は対象ファイル、対象XML、対象フィールド、理由を含む人間確認用テキストとする。
- XML parse不能は `xml parse failed: path=<path>, inner_path=<inner_path>, reason=<parser_error>` を基本形式とする。
- ZIP内対象XML0件は `zip has no target xml: path=<path>, pattern=h*.xml, excludes=ix08,su08,schema,xsd` を基本形式とする。
- raw抽出失敗は `xml raw extract failed: path=<path>, inner_path=<inner_path>, field=<field>, reason=<reason>`、複数fieldの場合は `xml raw extract failed: path=<path>, inner_path=<inner_path>, fields=<field1>,<field2>, reason=<reason>` を基本形式とする。
- XML parse不能の場合は `xml_ledger.xml_status = PARSE_ERROR` / `xml_reason` に集約する。
- ファイル単位の件数・総合状態は `file_receipts` に集約する。

### ETL metrics方針

- `files`: 処理対象 `file_receipts` 件数。
- `rows_seen`: 対象XML件数。
- `rows_inserted`: 新規 `xml_ledger` 件数。
- `rows_updated`: `xml_file_links` 登録件数 + `file_receipts` 更新件数。
- `rows_skipped`: 既存 `xml_sha256` 再受領・対象外XML件数。
- `errors`: `etl_errors` 登録件数。
- `exam_item_values` 件数は `rows_inserted` に含めず、必要に応じて `etl_runs.notes` のサマリーへ記録する。

### identity生成方針

- `identity_hash` / `person_id_custom` 生成は `scripts/lib/identity/generator.py` を唯一の入口とする。
- Phase4では `generate_identity_bundle(**raw)` を利用する。
- 入力キーは `birthdate`、`insurer_number_raw`、`insurance_symbol_raw`、`insurance_number_raw`、`name_kana_full_raw`、`gender_code` とする。
- Phase4が `generate_identity_bundle()` の戻り値として利用するのは、`ok`、`reason`、`person_id_custom`、`identity_hash`、`field_results` のみとする。
- `02_import_xml.py` から `scripts/lib/identity/builder/` や `scripts/lib/identity/field/` を直接呼ばない。
- XML parserはraw値抽出のみを担当し、identity用の独自正規化を実装しない。
- XMLとして正常に読み込み可能な場合は、identity生成に失敗しても `xml_ledger` を作成し、詳細を `etl_errors` に `field = IDENTITY` として記録する。
- identity生成失敗時は `generator.reason` を代表理由、`field_results` を詳細ソース、`etl_errors.message` を人間確認用として扱う。
- identity生成失敗時の `etl_errors.error_code` は `IDENTITY_GENERATION_FAILED` を基本とする。
- identity生成失敗時の `etl_errors.message` は、`identity generation failed: <field>=NG(<reason>), <field>=NG(<reason>)` の形式を基本とし、複数fieldが失敗した場合はカンマ区切りで列挙する。

### `xml_inner_path` 方針

- ZIP内XMLの場合、`xml_file_links.xml_inner_path` にはZIP内相対パスを保持する。
- XML単体ファイルの場合、`xml_file_links.xml_inner_path` は `NULL` とする。
- `xml_file_links` の論理一意キーは `file_receipt_id` / `xml_ledger_id` / `xml_inner_path` のままとする。
- DDL実装ではMySQLのキー長などの制約回避のため、長尺文字列部分をSHA256生成列へ変換してUNIQUE制約へ含める。

---

## 4.3 `03_check_exam_results.py`

### 目的

DB上の `xml_ledger` / `exam_item_values` を入力に、統合された制度チェック対象72項目の項目別 `status` / `reason` を生成し、`exam_check_results` に横持ちで保持する。

制度チェックは「項目単位の判定」と「制度単位の総合判定」を分離する。制度単位の `check_result` は `exam_check_results` の項目別 `status` を制度グループ単位で集計する。

### 入力

- チェック対象の `event_id` または対象Run条件
- `health_exam_result.xml_ledger`
- `health_exam_result.exam_item_values`
- `dev_phr.exam_item_master`
- `dev_phr.exam_item_group_*` 系マスタ

### 出力

- `exam_check_results`
- `xml_ledger.check_status`
- `xml_ledger.xml_export_status`
- `etl_runs` / `etl_errors`

### 更新テーブル

- `health_exam_result.exam_check_results`
- `health_exam_result.xml_ledger`
- `etl_runs`
- `etl_errors`

### 参照テーブル

- `health_exam_result.xml_ledger`
- `health_exam_result.exam_item_values`
- `dev_phr.exam_item_master`
- `dev_phr.exam_item_group_*` 系マスタ

### 主な処理順

1. `etl_runs` にチェックRunを開始登録する。
2. チェック対象の `xml_ledger` を取得する。
3. 対象XMLに紐づく `exam_item_values` を取得する。
4. `dev_phr.exam_item_group_*` 系マスタから制度チェックルールを取得する。
5. `exam_item_master` で `namecode` と `identity_item_code` の対応を解決する。
6. 共通72項目用グループに従って、項目別 `status` / `reason` を生成する。
7. `exam_check_results` を登録・更新する。
8. 制度単位の総合判定を行う場合は、法定健診判定用グループ・特定健診判定用グループに従って、`exam_check_results` の項目別 `status` を集計し、`xml_ledger.check_status` を生成する。
9. チェック結果をもとに、出力候補は `xml_ledger.xml_export_status = READY`、出力対象外は必要に応じて `SKIPPED` へ更新する。
10. `etl_runs` に件数サマリーと終了状態を記録する。

### 制度チェック総合判定

- 法定OK・特定OKの場合は、`xml_ledger.check_status = OK` とする。
- 法定OK・特定WARNINGの場合は、`xml_ledger.check_status = WARNING` とする。
- 法定NGの場合は、特定健診の結果にかかわらず `xml_ledger.check_status = NG` とする。
- 特定健診不足は `WARNING`、法定健診不足は `NG` とする。

### 制度チェックルール方針

- `ANY_NONEMPTY` は presence 判定のみを担当する。
- 対象 `namecode` 群のうち1つ以上に有効値が存在すれば充足とする。
- `ANY_NONEMPTY` は行が存在するだけでは充足とせず、`NULL`・空値・無効値は充足扱いしない。
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
- 制度チェックはルール種別をキーとして、対応する判定関数へディスパッチする。
- DBはどのルールを使うかを管理し、スクリプトはそのルールをどう判定するかを実装する。
- v2初期では `exam_item_group_identity_members` への追加カラムは作成しない。
- 特定健診用グループは初期マスタ未投入でも動作可能とし、後からマスタ投入すれば判定可能にする。

### 再実行方針

- XMLファイルは再読込しない。
- `exam_item_values` とマスタの現在状態から再計算する。
- 同一 `ledger_type` / `ledger_id` の `exam_check_results` は上書き更新または再作成方針を実装時に統一する。

### エラー記録方針

- マスタ不足、想定外値、チェック処理例外は `etl_errors` に記録する。
- チェック不能なXMLは `xml_ledger.check_status` / `check_reason` または `xml_export_status` に反映し、詳細は `etl_errors` に記録する。

---

## 4.4 `04_export_hia_xml.py`

### 目的

DB上のチェック結果・出力状態を参照し、HIAアップロード用XMLを生成する。

### 入力

- 出力対象の `event_id` または対象条件
- `xml_ledger.xml_export_status`
- `exam_item_values`
- `exam_check_results`
- 必要に応じて `file_receipts` / `xml_file_links` から辿れる元ファイル

### 出力

- HIAアップロード用XML
- 出力Runの `etl_runs`
- 出力エラーの `etl_errors`
- `xml_ledger.xml_export_status` の出力済み状態

### 更新テーブル

- `health_exam_result.xml_ledger`
- `etl_runs`
- `etl_errors`

### 参照テーブル

- `health_exam_result.xml_ledger`
- `health_exam_result.exam_item_values`
- `health_exam_result.exam_check_results`
- `health_exam_result.file_receipts`
- `health_exam_result.xml_file_links`

### 主な処理順

1. `etl_runs` に出力Runを開始登録する。
2. `xml_export_status = READY` の対象を取得する。
3. DB上の正規化済み基本情報・健診値・チェック結果を取得する。
4. HIAアップロード用XMLを生成する。
5. 医療機関フォルダ配下の `03_健診結果（アップロード）` にRun単位ディレクトリを作成する。
6. `<event.result_root_path>/<医療機関フォルダ>/03_健診結果（アップロード）/yyyymmdd_hhmmss_<run_id>/<xxx.zip>` へ書き出す。
7. 出力成功時は `xml_ledger.xml_export_status = EXPORTED`、失敗時は `ERROR`、出力対象外は `SKIPPED` として記録する。
8. `etl_runs` に件数サマリーと終了状態を記録する。

### 再実行方針

- 基本方針はDBの正規化済みデータから再生成する。
- XML原本を再読込する必要がある場合は、`file_receipts` / `xml_file_links` から元ファイルを辿る。
- 既存出力ファイルは上書きしない。
- 出力済みファイルの削除・整理は運用側の責務とする。
- v2初期では `xml_ledger.xml_export_status` がXML単位の最新出力状態を保持し、出力履歴はRun単位の出力フォルダを証跡とする。
- 将来、出力履歴の検索・監査・再出力履歴管理が必要になった場合のみ `xml_export_logs` 等の出力台帳を追加する。

### エラー記録方針

- 出力対象不整合、XML生成失敗、ファイル書込失敗は `etl_errors` に記録する。
- 出力不能な理由は、`xml_ledger.xml_export_status = ERROR` と reason系カラム、または `etl_errors` へ集約する。

---

## 5. 共通基盤へ寄せる候補

既存の共通基盤側へ寄せる候補。

| 候補 | 主な責務 |
| --- | --- |
| DB接続 / transaction | DB接続、トランザクション、リトライ、接続設定 |
| ETL run/error記録 | `etl_runs` / `etl_errors` の登録・終了更新・標準エラー形式 |
| ZIP展開 | ZIP安全展開、文字コード対応、展開先管理 |
| XML parse基礎 | XML parser、名前空間処理、共通XPath補助 |
| 設定読込 | YAML読込、環境別設定、必須項目検証 |
| 正規化共通処理 | 氏名カナ、保険証記号・番号、生年月日、性別などの共通正規化 |
| identity_hash | 共通仕様に基づく人物識別キー生成 |

---

## 6. from_medical固有libへ置く候補

`scripts/from_medical/script_lib/` に置く候補。

| 候補 | 主な責務 |
| --- | --- |
| medical_folder_aliases解決 | event別フォルダ名解決、`02_健診結果（編集）` 探索補助 |
| file_receipts登録 | ファイル種別判定、登録済み判定、受領台帳登録、サマリー更新 |
| xml_file_links登録 | 物理ファイルとXML内容の対応登録、`xml_inner_path` 管理 |
| xml_ledger登録 | XML内容一意判定、基本情報登録、照合結果・状態更新 |
| exam_item_values抽出 | XML entry/observation 解析、健診値抽出、項目辞書解決 |
| subscriber照合 | XML基本情報から加入者を照合し、台帳へ反映 |
| 制度チェック | 72項目の項目別 `status` / `reason` 生成、制度単位の `check_result` 集計 |
| exam_check_results登録 | 横持ちチェック結果のupsert、reason生成 |
| HIA出力 | DB上の正規化済みデータからHIAアップロード用XMLを生成 |

---

## 7. 初期実装でやらないこと

- 本格的な event_subject / person_event 更新。
- 人＋イベント単位の状態管理台帳の本格実装。
- 結果未着管理。
- 再提出管理の本格運用。
- CSV取込の本格対応。
- 保険者変換の完全実装。
- HIAアップロード後の自動結果反映。
- `work` 領域を恒久保存・アーカイブ用途に使うこと。

---

## 8. 現時点の結論

v2のオーケストラスクリプトは、`01_scan_files.py`、`02_import_xml.py`、`03_check_exam_results.py`、`04_export_hia_xml.py` の4本構成とする。

`01_scan_files.py` はファイル検出と `file_receipts` 登録のみを担当し、`work` へコピーしない。

`02_import_xml.py` は処理直前に `work` へ一時コピーし、ZIP展開・XML読込・XML基本情報抽出・加入者照合・健診項目値抽出を一括で行い、通常は処理完了後に `work` を削除する。

`03_check_exam_results.py` はXMLファイルを読まず、DB上の `xml_ledger` / `exam_item_values` と `dev_phr.exam_item_group_*` 系マスタから `exam_check_results` を生成する。

`04_export_hia_xml.py` は `xml_export_status` とDB上の正規化済みデータをもとに、HIAアップロード用XMLを生成する。

将来的には、人＋イベント単位の状態管理台帳を追加する方向とする。v2初期で本格実装するかは別途判断するが、`file_receipts` は物理ファイル単位、`xml_ledger` はXML内容単位の機械的な状態に限定し、人＋イベント単位の最終完了状態や人間の業務確認状態は背負わせない。

---

## 9. スクリプト・テーブル責務マトリクス

### 更新責務

| テーブル | 作成 | 更新 | 主な参照 |
| --- | --- | --- | --- |
| `health_exam_result.file_receipts` | `01_scan_files.py` | `02_import_xml.py` | 全オーケストラスクリプト |
| `health_exam_result.xml_file_links` | `02_import_xml.py` | 原則なし | `04_export_hia_xml.py` |
| `health_exam_result.xml_ledger` | `02_import_xml.py` | `02_import_xml.py`<br>`03_check_exam_results.py`<br>`04_export_hia_xml.py` | `03_check_exam_results.py`<br>`04_export_hia_xml.py` |
| `health_exam_result.exam_item_values` | `02_import_xml.py` | 原則なし | `03_check_exam_results.py`<br>`04_export_hia_xml.py` |
| `health_exam_result.exam_check_results` | `03_check_exam_results.py` | `03_check_exam_results.py` | `04_export_hia_xml.py` |
| `etl_runs` | 各オーケストラスクリプト | 各オーケストラスクリプト | 運用・調査 |
| `etl_errors` | 各オーケストラスクリプト | 必要に応じ追記 | 運用・調査 |

### dev_phr 参照責務

| テーブル | 参照スクリプト | 用途 |
| --- | --- | --- |
| `event` | `01_scan_files.py` | `result_root_path` 取得 |
| `subscribers` | `02_import_xml.py` | 加入者照合 |
| `exam_item_master` | 後続正規化Phase<br>`03_check_exam_results.py` | 項目定義・バリデーション基準 |
| `norm_variants` | 後続正規化Phase | CD/CO系検査値の表記ゆれ辞書。初期は `result_code_oid + raw_value_utf8` 完全一致のみ。 |
| `exam_item_group_*` 系 | `03_check_exam_results.py` | 法定・特定健診判定 |

### 更新責務の原則

- 作成(Create)を担当するスクリプトは原則1つとする。
- 更新(Update)は必要最小限のスクリプトのみが担当する。
- `dev_phr` は原則参照専用とし、共通マスタ・加入者情報を提供する。
- 処理系・台帳系の更新は `health_exam_result` に集約する。
