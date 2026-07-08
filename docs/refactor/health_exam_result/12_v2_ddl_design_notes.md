# health_exam_result v2 DDL設計メモ

このドキュメントは、`health_exam_result v2` のDB・テーブル設計を整理するためのメモである。

詳細な現行DDL調査は `08_table_ddl_summary_codex.md`、v2テーブル責務の初期整理は `09_v2_table_design_notes.md` を参照する。

---

## 1. 目的

v2で必要になるDB・テーブル・既存dev_phrテーブルの利用方針を整理し、DDL作成前の骨子を定義する。

---

## 2. DB方針

### 2.1 新規DB

v2の処理系・台帳系は、既存 `dev_phr` へ混在させず、新しいDB `health_exam_result` に配置する。

DB名:

```text
health_exam_result
```

初期DDLは `sql/ddl/health_exam_result/` 配下にテーブル単位で作成する。DDLファイル名は `NNNN_health_exam_result__<table_name>.sql` を基本とする。

DDLは固定せず、設計変更に合わせて更新する。新規環境は最新DDLから構築し、既存環境はMigrationで追従する。DDLを変更した場合、既存DBが対象となる変更についてはMigrationを同時に作成する。設計変更は、設計書更新後にDDL・Migration・スクリプトへ反映する。

### 2.2 DB責務分離

```text
dev_phr
  = 既存マスタ・加入者・健保・イベント定義

health_exam_result
  = v2の取込・XML Ledger・健診値・チェック結果・処理ログ
```

### 2.3 DDL制約方針

- status系カラムはDB enumではなく `varchar` で定義する。
- `health_exam_result` 内の業務データ同士のFKは維持する。
- `etl_runs` は監査・実行履歴であり、業務データの親として扱わない。
- 業務テーブルおよび `etl_errors` から `etl_runs` へのFK制約は原則張らない。
- run_id系カラムには参照・検索用INDEXを付与する。
- `dev_phr` など外部DB・外部スキーマへのcross schema FKは張らない。
- `event_id`、`subscriber_id`、`hia_subscriber_id` など外部参照・検索用カラムは必要に応じてINDEXを付与する。

---

## 3. dev_phr側で利用する既存テーブル

### 3.1 subscribers

### 扱い

参照のみ。

### 用途

- XML基本情報から `subscribers.id` を解決する。
- 解決したIDは `health_exam_result.xml_ledger.subscriber_id` に保持する。

---

### 3.2 funds / fund_insurer_numbers

### 扱い

参照のみ。

### 用途

- 保険者番号から健保を解決する。
- 取り込み設定や event 設定の検証に利用する。

---

### 3.3 event

### 扱い

参照。一部Migrationで拡張する。

v2初期実装では、設定YAMLから固定 `event_id` を指定して処理を実行する。

2026年度 v2.0.0 では `event_id = 2` を対象イベントとして扱う。

### 用途

- `event_id` によって、対象年度・健診イベント・保険者を識別する。
- `event.result_root_path` によって、対象イベントの健診結果ルートフォルダを識別する。
- `file_receipts.event_id`、`xml_file_links.event_id`、`xml_ledger.event_id`、`exam_item_values.event_id`、`exam_check_results.event_id` に冗長保持する。

### 追加カラム

```text
result_root_path
```

### メモ

- `result_root_path` は医療機関フォルダの親ディレクトリまでを保持する。
- `result_root_path` の型は `text` とする。
- `result_root_path` は既存イベントへの影響を避けるため `NULL` 許可とする。
- v2処理では、対象 `event_id` の `result_root_path` が未設定の場合はエラーとする。
- 医療機関ごとのフォルダ名は `health_exam_result` 側の event対応フォルダエイリアステーブルで管理する。
- 旧 `work_other.medi_shared_folder_aliases` は event 非対応のため、v2では利用しない。

---

### 3.4 person_event

### 扱い

後続フェーズで利用。

### 用途

- 人 × event 単位の状態管理。
- v2初期では必須実装に含めず、`xml_ledger` に `event_id` と `subscriber_id` を持たせて後続接続できる状態にする。

---

### 3.5 exam_item_master

### 扱い

参照。

### 用途

- `namecode` から項目名・値型・単位・代表項目コードを解決する。
- 後続正規化Phaseや制度チェック時の項目辞書として利用する。
- 異常値チェックの min/max を追加する候補。

---

### 3.6 exam_item_groups / exam_item_group_members / exam_item_group_method_members / exam_item_group_identity_members

### 扱い

参照。一部Migrationで追加・差分更新する。

旧法定健診チェックで利用していた `LSIO_Legal_Item` は、完成済みの制度チェック機能ではなく、実データから法定健診項目の存在を逆引き確認するための簡易presenceチェック・エビデンス用途として扱う。

v2では、旧マスタをそのまま正とせず、既存構造を参考にしながら制度チェックに必要なマスタデータを追加・修正する。

### 用途

- 共通72項目用グループ、法定健診判定用グループ、特定健診判定用グループを分けて扱う。
- 共通72項目用グループは、`exam_check_results` の項目別 `status` / `reason` を生成するために利用する。
- 法定健診判定用グループと特定健診判定用グループは、制度単位の `check_result` を集計するために利用する。
- 現行では `LSIO_Legal_Item` が法定健診項目グループとして使われている。
- 法定健診ルールは、既存 `LSIO_Legal_Item` を `02_exam_check_item_spec_v2_0_0.md` と突き合わせて差分確認し、必要な差分のみMigrationで追加・修正する。
- 特定健診用グループは、初期実装ではマスタ未投入でも動作可能な構成とし、後でマスタを投入すれば判定できるようにする。
- `exam_item_group_identity_members` は、同一性項目コード単位の必須区分・presence判定ルール管理として利用する。
- `exam_item_group_members` は nameCode ベースのグループ所属を持つ。
- `exam_item_group_method_members` は methodCode ベースの presence 判定補助を持つ。
- `exam_check_results` の `status_<item_code>` / `reason_<item_code>` は、`exam_item_values` と `dev_phr.exam_item_group_*` 系マスタから生成する。

### v2初期で利用する既存カラム

v2初期では `exam_item_group_identity_members` への追加カラムは作成せず、既存カラムを利用する。

```text
required_flag
condition_expr
required_presence_namecodes
presence_value_mode
```

### 将来拡張候補

既存カラムで表現しきれない詳細判定が必要になった場合は、制度チェックルールの拡張を別途検討する。

### メモ

- 旧法定チェックは主に「対象namecode行が存在するか」を見ており、値の意味・nullFlavor・代替・算出・条件付き必須までは十分に扱っていない。
- `ANY_NONEMPTY` は presence 判定のみを担当する。
- `ANY_NONEMPTY` は、対象 `namecode` 群のうち1つ以上に有効値が存在すれば充足とする。
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
- 法定健診チェックは、健診機関確認・再提出フローに耐える制度チェック機能として設計する。
- 特定健診チェックは、同じ仕組みに載せるが、運用上は warning / 参考判定を基本とする。
- v2では `dev_phr.exam_item_group_*` 系マスタをMigration対象とし、72項目対応に必要な初期データを追加・修正する。これはDDL追加ではなくデータ不足として扱う。

---

## 4. health_exam_result側で作成するテーブル候補

core DDLの初期作成対象は以下の7テーブルとする。

```text
etl_runs
etl_errors
medical_folder_aliases
file_receipts
xml_ledger
xml_file_links
exam_item_values
```

`exam_check_results` はcore DDLから一旦外し、制度チェックDDLとして後続で作成する。

## 4.1 file_receipts

### 役割

ファイル資産台帳。

受領ファイル・投入用ファイル・将来的な出力ファイルを1ファイル単位で管理する。

### 責務

- ファイル種別管理。
- source/output パス管理。
- ファイルSHA256管理。
- 格納フォルダやファイル名から取得できる保険者番号・健診機関番号・健診機関名の保持。
- 処理対象件数と中身確認日時の保持。
- 処理結果サマリーの保持。
- 物理ファイル単位の機械的な処理状態の保持。
- `file_receipts.status` は `DISCOVERED / IMPORTING / IMPORTED / ERROR` の4状態で管理する。
- 既に取り込み済みの重複ファイルは `file_receipts` に新規登録せず、重複件数は `etl_runs` のスキップ件数・実行サマリーで管理する。
- `01_scan_files.py` が発見・登録、`02_import_xml.py` が状態遷移を更新する。
- Phase3 `01_scan_files.py` はファイル検出と `file_receipts.status = DISCOVERED` 登録に責務を限定する。
- Phase3の初期登録対象は ZIP / XML とし、CSVは初期実装では `file_receipts` に登録しない。
- CSVは将来対応時にスキャン対象へ追加し、その時点で `file_type = CSV` を追加する。
- `file_type = OTHER` は初期実装では登録対象としない。

### 主なカラム候補

```text
id
event_id
file_role
file_type
file_name
file_ext
source_path
relative_path
output_path
file_sha256
file_size
processable_count
insurer_number
submitter_facility_code
facility_code
facility_name
storage_folder_type
status
summary_message
etl_run_id
first_seen_at
last_seen_at
content_checked_at
received_at
processed_at
created_at
updated_at
```

### メモ

- `zip_receipts` は初期実装では独立テーブルにしない。
- ZIPは `file_type = ZIP` の処理分岐として扱う。
- Phase3登録時の `file_role` は `FROM_MEDICAL` とする。
- Phase3初期実装で登録対象とする `file_type` は `ZIP / XML` とする。
- `file_type = OTHER` は初期実装では登録対象としない。
- Phase3登録時の `storage_folder_type` は `MEDICAL_RESULT_ROOT` とする。
- Phase3では `file_sha256` をスキャン時に計算する。
- Phase3では `processable_count` を設定せず `NULL` とする。
- `facility_code` / `facility_name` は健診機関コード・名称として保持する。
- `processable_count` はZIPならXML件数、XML単体なら通常1、CSVなら設定に従って算出したデータ行数を保持する。
- `content_checked_at` はファイル種別に依存しない中身確認日時として保持する。
- `file_receipts.file_sha256` 単独UNIQUEは採用しない。
- `file_receipts` の重複防止は `event_id`、`relative_path`、`file_sha256` の組み合わせを基本とする。
- Phase3の `relative_path` は `event.result_root_path` からの相対パスとする。
- 上記は論理一意キーとして維持し、DDL実装ではMySQLのキー長などの制約回避のため、長尺文字列部分をSHA256生成列へ変換してUNIQUE制約へ含める。
- `work` は `02_import_xml.py` が処理中だけ利用する一時領域であり、`file_receipts` では恒久的な `work_path` を保持しない。
- `file_receipts` は人＋イベント単位の最終完了状態を管理しない。
- 人間の業務確認状態を持つ場合は、機械的な `status` と混ぜず、将来の `operation_status` として分離する。
- `person_event` / `operation_status` 系は、03 の保留事項のため、初期DDLでは未定のままとする。

---

## 4.2 xml_ledger

### 役割

XML内容の品質・突合・処理状態を管理する一意台帳。

### 責務

- XML内容由来の基本情報Ledger（受診者/文書単位）。
- `event_id` と `subscriber_id` の保持。
- `hia_subscriber_id` の検索用冗長保持。
- XML基本情報の raw値 / match値の保持。
- `xml_sha256` によるXML内容の一意管理。
- 加入者突合結果の保持。
- XMLとしての処理状態と理由の保持。
- 検査値抽出・妥当性の総合状態と理由・サマリーの保持。
- XML単位の制度チェック状態の保持。
- XML単位のHIA出力状態の保持。
- チェックNG後に業務確認で出力OKとした手動承認情報の保持。

### 主なカラム候補

```text
id
event_id
subscriber_id
hia_subscriber_id
xml_sha256
document_id
insurer_number
facility_code
facility_name
exam_date
name_kana_raw
name_kana_match
insurance_symbol_raw
insurance_symbol_match
insurance_number_raw
insurance_number_match
birthdate
gender_code
identity_hash
person_id_custom
subscriber_match_status
subscriber_match_method
subscriber_match_reason
exam_item_status
exam_item_reason
xml_status
xml_reason
check_status
xml_export_status
manual_export_approved
manual_export_reason
created_at
updated_at
```

### メモ

- `xml_status` は `02_import_xml.py` のXML取込状態を保持する。
- `exam_item_status` は `02_import_xml.py` の検査値抽出・妥当性の総合状態を保持する。
- Phase4で使用する `exam_item_status` は `OK` / `WARNING` / `ERROR` / `NOT_EXECUTED` とする。
- `exam_item_reason` は必要に応じて検査値総合状態の理由・サマリーを保持する。
- `check_status` は `03_check_exam_results.py` の制度チェック状態を保持する。
- `xml_export_status` は `04_export_hia_xml.py` のHIA出力状態をXML単位で保持する。
- `xml_reason` は固定enumではなく、スクリプト実装に応じて理由コードを追加できる文字列カラムとする。
- `xml_status` / `xml_reason` は、XML読込エラー、Namespaceエラー、XMLフォーマットエラーなどXMLそのものの状態のみを扱う。
- 加入者照合結果は `subscriber_match_status`、検査値抽出・妥当性の総合状態は `exam_item_status` で管理し、`xml_status` には混在させない。
- `xml_status` / `check_status` / `xml_export_status` の値定義は確定済みとし、reason code詳細は未決として残す。
- 制度単位の判定が `NG` でも、医療機関確認等により正当理由が確認できた場合は、`manual_export_approved = true`、`manual_export_reason` を設定し、`xml_export_status = READY` とできる。
- `check_status` はシステム判定結果として保持し、手動承認によって変更しない。
- `item_extract_status`、旧HIA ready系ステータス、`xsd_valid`、`is_exam_result`、`error_count`、`warning_count` は独立カラムとしては持たない。
- `xml_sha256` はXML内容の一意キーとして保持する。v2内の主参照は `xml_ledger.id` を基本とする。
- `file_receipt_id` は `xml_ledger` に持たず、物理ファイルとの関係は `xml_file_links` で管理する。
- `first_seen_run_id`、`last_seen_run_id`、`first_seen_at`、`last_seen_at` は `xml_ledger` には持たず、物理受領履歴は `file_receipts` と `xml_file_links` 側で管理する。
- 別ZIP等で同一 `xml_sha256` のXMLを受領した場合は、`xml_ledger` を重複作成せず、`xml_file_links` のみ追加する。
- `hia_subscriber_id` は正ではなく、HIA加入者IDで調査・検索するための運用補助キーとして冗長保持する。
- v2初期では `xml_export_status` を `xml_ledger` に保持し、XML単位の最新出力状態を管理する。
- 出力履歴はRun単位の出力フォルダを証跡とする。
- 将来、出力履歴の検索・監査・再出力履歴管理が必要になった場合のみ `xml_export_logs` 等の出力台帳を追加する。
- `xml_ledger` は人＋イベント単位の最終完了状態を管理しない。
- 人間の業務確認状態を持つ場合は、機械的な `xml_status` / `check_status` / `xml_export_status` と混ぜず、将来の `operation_status` として分離する。

---

## 4.3 xml_file_links

### 役割

物理ファイル受領台帳 `file_receipts` と、XML内容の一意台帳 `xml_ledger` の対応台帳。

### 責務

- 物理ファイルとXML内容の対応を保持する。
- ZIP内XMLパスなど、物理ファイル内でのXML位置を保持する。
- 別ZIP等で同一XMLを受領した場合に、同じ `xml_ledger` へ複数の受領リンクを保持する。
- `xml_ledger` を物理受領履歴で重複させない。

### 主なカラム候補

```text
id
event_id
file_receipt_id
xml_ledger_id
xml_inner_path
created_at
```

### メモ

- `file_receipts` は物理ファイルの正台帳、`xml_ledger` はXML内容の正台帳とする。
- `xml_file_links` は物理ファイルとXML内容の対応台帳とする。
- ZIP内XMLの場合、`xml_inner_path` はZIP内相対パスを保持する。
- 単体XMLの場合、`xml_inner_path` は `NULL` とする。
- 同一 `xml_sha256` のXMLを別ZIP等で受領した場合は、既存 `xml_ledger` を参照する `xml_file_links` を追加する。
- `xml_file_links` は `file_receipt_id`、`xml_ledger_id`、`xml_inner_path` の組み合わせをUNIQUEとする。
- 上記は論理一意キーとして維持し、DDL実装ではMySQLのキー長などの制約回避のため、長尺文字列部分をSHA256生成列へ変換してUNIQUE制約へ含める。

---

## 4.4 exam_item_values

### 役割

健診結果値の共通基盤。初期実装ではXML由来を対象とし、将来的にCSV由来も受け入れる。正しい検査値だけではなく、XMLから健診値として取得できた事実を保持する。

### 責務

- XML / CSV 由来の健診項目値を共通形式で保持する。
- 由来Ledgerを `ledger_type` / `ledger_id` で表現する。
- 検索性向上のため `event_id` / `subscriber_id` / `hia_subscriber_id` を冗長保持する。
- 実際に存在した健診値のみを保持する。不足項目を補完行として作ることはしない。
- namecodeが判定できない、または未対応の検査値entryでも、raw値を取得できる場合は捨てずに保持する。
- unsupported namecode は `namecode = NULL` とし、`code_system` / `code_value` / `code_display` / `namecode_display_name` / raw系カラムへ取得できた情報を保持する。
- `namecode = NULL` は「検査値候補として届いたが、検査項目コードとして未対応・未判定」を表す。
- unsupported namecode はETL Errorにも記録し、調査・マスタ追加・再処理の導線を残す。
- `observation/code/displayName` は検査項目名として `namecode_display_name` に保持する。
- `value/@displayName` はCD/CO等の結果値コード名称として `code_display` に保持し、PQ/ST等では `code_display` を設定しない。
- `observation/@negationInd` は `negation_ind` にraw属性として保持する。
- raw値と正規化値を保持する。
- 正規化状態・正規化理由を保持する。
- 項目値としての妥当性（範囲外・形式不正等）を保持する。

### 主なカラム候補

```text
id
event_id
ledger_type
ledger_id
subscriber_id
hia_subscriber_id
namecode (NULL許可。unsupported namecodeの場合はNULL)
occurrence_no
raw_value
raw_value_type
raw_unit
normalized_value
normalized_unit
nullflavor
code_system
code_value
code_display (結果値コード名称)
namecode_display_name (検査項目名)
negation_ind
identity_item_code
jun_no
normalize_status
normalize_reason
validation_status
validation_reason
extracted_run_id
extracted_at
normalized_at
created_at
updated_at
```

### メモ

- `exam_item_values` はXML専用ではなく、XML / CSV 共通の健診値テーブルとする。
- `ledger_type` は現時点では `XML` / `CSV` を採用し、それ以外の入力元は現時点では決定しない。
- `ledger_id` は `ledger_type` と組み合わせて由来Ledgerを表現する。
- `exam_item_values` は実際に存在した健診値のみを保持する。不足項目の補完行は作らないが、XML上に検査値らしきentryとして存在するものは、namecodeが判定できない場合も可能な限りraw行を保持する。
- `namecode` はNULL許可とし、unsupported namecodeのraw行ではNULLを設定する。検査項目コード体系や形式が未対応の場合でも、`code_system` / `code_value` / `code_display` に取得できたコード情報を残す。
- `namecode = NULL` の行も受領事実として保持し、後続でマスタ追加・再正規化・再判定できるようにする。
- 制度チェックは、`exam_item_values` に存在する値だけでなく「存在しない項目」も判定材料とするため、`exam_check_results` 側の責務とする。
- 項目値としての妥当性（範囲外・形式不正等）は `validation_status` / `validation_reason` で保持する。
- `validation_status` は制度チェックではなく、値そのものの妥当性を表す。
- CSVからHIAアップロード用XMLを生成する場合も、`exam_item_values` の正規化済み値を利用する。
- `event_id`、`subscriber_id`、`hia_subscriber_id` は、正規化のためではなく、SQLによる運用調査・障害解析・検索性向上のために冗長保持する。
- `hia_subscriber_id` は正ではなく、HIA加入者IDで調査・検索するための運用補助キーとして扱う。
- `normalized_value` は、数値だけでなく ST 型など文字列値も入るため `text` とする。

---

## 4.5 exam_check_results

### 役割

制度チェック対象72項目の項目別チェック結果台帳。

人が確認・エクスポート・集計しやすいよう、横持ちを基本とする。

### 責務

- 1受診者・1Ledger単位の制度チェック結果を保持する。
- `exam_item_values` に存在する値、および存在しない項目を判定材料として保持する。
- 統合された制度チェック対象72項目を同一性項目コード単位で横持ちする。
- 各チェック対象項目の状態を `status` / `reason` で保持する。
- 法定健診・特定健診で項目別 `status` / `reason` を二重に持たない。
- 検索性向上のため `event_id` / `subscriber_id` / `hia_subscriber_id` を冗長保持する。

### 主なカラム候補

```text
id
event_id
ledger_type
ledger_id
subscriber_id
hia_subscriber_id
check_run_id
checked_at
created_at
updated_at
status_<item_code>
reason_<item_code>
```

### 横持ち項目の生成元

横持ち対象項目は、以下の仕様書を正とする。

- `docs/spec/health_examinations/02_exam_check_item_spec_v2_0_0.md`

仕様書には、`付属2_制度整理` シート由来の制度チェック対象72項目を保持している。

並び順は以下とする。

1. 区分番号 昇順
2. 同一性項目コード 昇順

### 項目別カラム形式

項目別カラムは、同一性項目コード単位で以下の2カラムを持つ。

```text
status_<item_code>
reason_<item_code>
```

例:

```text
status_9N001
reason_9N001
status_9N006
reason_9N006
```

### reason形式

`reason_<item_code>` は特記事項のみ保持し、`OK` の場合は `NULL` とする。

例:

```text
算出元:9N206
```

XML処理結果ログおよび医療機関向けメッセージは、`reason` が `NULL` ではない項目を集約して生成する。

### check_result

`check_result` は制度単位の最終判定を表す。

- `check_result` は `exam_check_results` の項目別 `status` を制度グループ単位で集計して算出する。
- 法定健診・特定健診の総合判定は `exam_check_results` を唯一の入力として算出し、XMLや `exam_item_values` を直接参照しない。
- 制度チェック総合判定は `xml_ledger.check_status` に保持する。
- 法定OK・特定OKの場合は `OK`、法定OK・特定WARNINGの場合は `WARNING`、法定NGの場合は `NG` とする。
- 特定健診不足は `WARNING`、法定健診不足は `NG` とする。

### メモ

- `exam_check_results` は制度チェック結果台帳であり、実値テーブルではない。
- 実値は `exam_item_values` に縦持ちで保持する。
- 制度チェックでは、`exam_item_values` に存在する値だけでなく「存在しない項目」も判定材料とする。
- 横持ち項目は同一性項目コード単位で作成する。
- 検査方法、左右、裸眼/矯正などではカラムを分けない。
- 項目別の値あり/なしや値有効/不正は、`present` / `valid` ではなく `status` / `reason` に集約する。
- 法定健診・特定健診で値の事実や項目別 `status` / `reason` を二重管理しない。
- 項目別 `status` は `OK` / `CALCULATED` / `ALTERNATIVE` / `MISSING` / `INVALID` とする。カラム命名規則は `status_<item_code>` / `reason_<item_code>` とする。
- `CALCULATE` による算出結果は項目別 `status = CALCULATED` として表現し、`CALCULATE` と `ALTERNATIVE` のいずれでも値を確定できない場合は項目別 `status = MISSING` として表現する。
- 判定ルール自体は `exam_check_results` に保持しない。既存 `dev_phr.exam_item_group_*` 系マスタを利用する。
- 法定健診ルールマスタは現行内容を棚卸しし、`02_exam_check_item_spec_v2_0_0.md` との差分確認を行う。
- 特定健診用グループは、初期実装ではマスタ未投入でも動作可能な構成とし、後でマスタを投入すれば判定できるようにする。

---

## 4.6 etl_errors

### 役割

処理中に発生したエラーを、ファイル・XML・項目へ紐付けて保持する。

### 主なカラム候補

```text
error_id
run_id
phase
source
insurer_number
src_file
src_row_no
src_line_no
staging_rowid
person_id_custom
field
field_value
error_code
message
created_at
```

### メモ

- `etl_errors` は既存 `scripts/lib/etl` の共通構造に合わせる。
- `file_receipt_id`、`xml_ledger_id`、`item_value_id`、`error_type`、`status`、`resolved_by_xml_ledger_id` はhealth_exam_result独自ETL構造になるため採用しない。
- Phase3の `etl_errors` は運用上対応が必要な事象のみ記録する。
- Phase3固有の分類は共通ETL構造の `field` / `error_code` に寄せ、将来必要に応じて拡張する。

---

## 4.7 etl_runs

### 役割

取込・チェック処理の実行単位を管理する。実行サマリーとして、件数・スキップ件数・エラー件数などを保持する。

### 主なカラム候補

```text
run_id
phase
source
db_schema
started_at
finished_at
status
input_base
input_file
total_rows
processed_rows
ok_rows
warning_rows
error_rows
skipped_rows
notes
admin_note
created_at
```

### メモ

- `etl_runs` は既存 `scripts/lib/etl` の共通構造に合わせる。
- Phase3 `01_scan_files.py` は `phase = SCAN_FILES`、`source = FROM_MEDICAL` として記録する。
- Phase3 `01_scan_files.py` の `status` は共通ETL仕様の `running / success / partial / failed` を利用する。
- scan結果サマリーは標準出力に表示し、可能な範囲で `etl_runs.notes` に記録する。
- `notes` は人間が読みやすい短いテキストとし、JSON等の構造化データは採用しない。

---

## 4.8 medical_folder_aliases

### 役割

イベント単位の医療機関フォルダ名変換台帳。

`event.result_root_path` 配下に存在する実フォルダ名と、システム内部で扱う正規フォルダ名の対応を保持する。

### 責務

- event単位の共有フォルダ名対応を保持する。
- ネットワーク共有上の実フォルダ名を `src_folder_raw` として保持する。
- システム内部で利用する正規フォルダ名を `dst_folder_norm` として保持する。
- フォルダ名変更や仮名称の確定名称化を吸収する。
- 初期投入では原則 `src_folder_raw = dst_folder_norm` とする。

### 主なカラム候補

```text
alias_id
event_id
src_folder_raw
dst_folder_norm
manual_judgement
note
is_active
created_at
updated_at
```

### メモ

- 旧 `work_other.medi_shared_folder_aliases` は event 非対応のため、v2では利用しない。
- v2では `health_exam_result` 側に event対応版として新規作成する。
- このテーブルは医療機関マスタではなく、イベント単位のフォルダ名変換台帳である。
- `file_receipts` には、実際に読み込んだ共有フォルダ名を保持する。
- 初期投入データは `docs/spec/health_examinations/03_medical_folder_aliases_initial_data_v2_0_0.md` を正とする。
- 一意制約は `UNIQUE(event_id, src_folder_raw)` とする。
- `dst_folder_norm` には一意制約を設けず、複数の実フォルダ名から同一名称への集約を許可する。
- 初期実装のインデックスは `event_id` および `UNIQUE(event_id, src_folder_raw)` によるものを基本とする。
- 初期データは `event_id = 2` の188件を投入対象とする。
- 初期データでは原則 `src_folder_raw = dst_folder_norm` とする。
- 初期データSQLの配置先は `sql/seed/health_exam_result/` とする。
- 初期データSQLのファイル名は `0010_health_exam_result__medical_folder_aliases_event2.sql` とする。
- 初期データSQLは `INSERT ... ON DUPLICATE KEY UPDATE` で再実行可能にする。
- 初期データSQL再実行時の更新対象は `dst_folder_norm`、`note`、`is_active`、`manual_judgement`、`updated_at` とする。
- `created_at` は初回INSERT時のみ設定する。
- `alias_id` は自動採番に任せ、seed SQLでは明示投入しない。
- `is_active` の初期値は `1` とする。
- `manual_judgement` の初期値は `0` とする。
- 補足がある行のみ `note` に値を入れ、補足なしは `NULL` とする。
- 仮名称等の補足情報は `note` に保持し、`manual_judgement` の判定条件とはしない。

---

## 4.9 HIA出力状態・出力先

### 扱い

v2初期では、XML単位の最新出力状態を `xml_ledger.xml_export_status` に保持する。

`04_export_hia_xml.py` は、医療機関フォルダ配下の `03_健診結果（アップロード）` にRun単位ディレクトリを作成して出力する。

出力先形式:

```text
<event.result_root_path>/<医療機関フォルダ>/03_健診結果（アップロード）/yyyymmdd_hhmmss_<run_id>/<xxx.zip>
```

### メモ

- 既存出力ファイルは上書きしない。
- 出力済みファイルの削除・整理は運用側の責務とする。
- 出力履歴はRun単位の出力フォルダを証跡とする。
- 将来、出力履歴の検索・監査・再出力履歴管理が必要になった場合のみ `xml_export_logs` 等の出力台帳を追加する。

---

## 5. 初期実装で作らない候補

### 5.1 zip_receipts

初期実装では作らない。

ZIPは `file_receipts.file_type` による処理分岐として扱う。

### 5.2 legal presence / missing 中間テーブル

初期実装では作らない候補。

現行では以下のような縦持ちテーブルがあった。

```text
medi_lsio_identity_presence
medi_lsio_missing_items
```

v2では、まず `exam_item_values` とルールマスタから直接 `exam_check_results` を生成し、制度単位の `check_result` は `exam_check_results` から集計する。

必要になった場合のみ、不足項目明細テーブルを追加する。

### 5.3 csv_row_ledger

初期実装では作成しない。

CSV直取込を行う場合は、CSV1行=1受診者単位の基本情報Ledgerとして追加する。

CSVや紙データは、初期実装では別プロジェクト・別スクリプトでXMLへ変換し、`02_健診結果（編集）` へ投入する運用とする。

---

### 5.4 人＋イベント単位の状態管理台帳

最終的には、人＋イベント単位の状態管理台帳を追加する方向とする。

v2初期で本格実装するかは別途判断する。

この台帳では、その人の健診イベントが最終的にOKか、確認中か、再提出依頼中か、完了かを管理する。

`file_receipts` は物理ファイル単位、`xml_ledger` はXML内容単位の機械的状態を管理し、人＋イベント単位の最終完了状態は持たない。

人間の業務確認状態は、将来的には `operation_status` として機械的な `xml_status` / `check_status` / `xml_export_status` から分離する。

---

## 6. DDL設計で決めること

### 決定済み

1. 新規DB名は `health_exam_result` とする。
2. 初期DDLは `sql/ddl/health_exam_result/` 配下にテーブル単位で作成する。
3. DDLファイル名は `NNNN_health_exam_result__<table_name>.sql` とする。
4. core DDL対象は `etl_runs`、`etl_errors`、`medical_folder_aliases`、`file_receipts`、`xml_ledger`、`xml_file_links`、`exam_item_values` の7テーブルとする。
5. `exam_check_results` はcore DDLから一旦外し、制度チェックDDLとして後続で作成する。
6. `exam_check_results` の横持ち対象は `02_exam_check_item_spec_v2_0_0.md` の72項目を正とする。
7. `exam_check_results` の項目別 `status` は `OK` / `CALCULATED` / `ALTERNATIVE` / `MISSING` / `INVALID` とする。
8. `reason_<item_code>` は特記事項のみ保持し、`OK` 時は `NULL` とする。
9. `ANY_NONEMPTY` は presence 判定のみを担当し、対象 `namecode` 群のうち1つ以上に有効値が存在すれば充足とする。
10. v2初期では `exam_item_group_identity_members` への追加カラムは作成しない。
11. `dev_phr.exam_item_group_*` 系マスタは migration / 初期データ追加で72項目対応する。DDL追加ではなくデータ不足として扱う。
12. `medical_folder_aliases` の初期投入データは `docs/spec/health_examinations/03_medical_folder_aliases_initial_data_v2_0_0.md` を正とする。
13. `xml_file_links` は `file_receipt_id`、`xml_ledger_id`、`xml_inner_path` の組み合わせをUNIQUEとする。
14. 長尺文字列を含む複合UNIQUE制約は、DDL実装ではMySQLのキー長などの制約回避のため、長尺文字列部分をSHA256生成列へ変換してUNIQUE制約へ含める。
15. SHA256生成列は物理実装上の制約回避であり、論理設計上の一意キーは変更しない。
16. `medical_folder_aliases` の一意制約は `UNIQUE(event_id, src_folder_raw)` とする。
17. `medical_folder_aliases.dst_folder_norm` には一意制約を設けず、複数の実フォルダ名から同一名称への集約を許可する。
18. `medical_folder_aliases` のインデックスは、初期実装では `event_id` および `UNIQUE(event_id, src_folder_raw)` によるものを基本とする。
19. `medical_folder_aliases.is_active` の初期値は `1` とする。
20. `medical_folder_aliases.manual_judgement` の初期値は `0` とする。
21. 仮名称等の補足情報は `medical_folder_aliases.note` に保持し、`manual_judgement` の判定条件とはしない。
22. status系カラムはDB enumではなく `varchar` で定義する。
23. `health_exam_result` 内の業務データ同士のFKは維持する。
24. `xml_ledger.exam_item_status` は検査値抽出・妥当性の総合状態を表し、Phase4では `OK` / `WARNING` / `ERROR` / `NOT_EXECUTED` を使用する。
25. `xml_ledger.exam_item_reason` は必要に応じて検査値総合状態の理由・サマリーを保持する。
26. `etl_runs` へのFK制約は原則張らず、run_id系カラムとINDEXで参照・検索性を確保する。
27. `dev_phr` など外部DB・外部スキーマへのcross schema FKは張らない。
28. `file_receipts.file_sha256` 単独UNIQUEは採用しない。
29. `file_receipts` の重複防止は `event_id`、`relative_path`、`file_sha256` の組み合わせを基本とする。
30. `exam_item_values.normalized_value` は `text` とする。
31. `dev_phr.event.result_root_path` は migration で追加する。
32. `dev_phr.event.result_root_path` の型は `text` とし、`NULL` 許可とする。
33. v2処理では、対象 `event_id` の `result_root_path` が未設定の場合はエラーとする。
34. `medical_folder_aliases` 初期データは `event_id = 2` の188件を投入対象とする。
35. `medical_folder_aliases` 初期データSQLの配置先は `sql/seed/health_exam_result/` とする。
36. `medical_folder_aliases` 初期データSQLのファイル名は `0010_health_exam_result__medical_folder_aliases_event2.sql` とする。
37. `medical_folder_aliases` 初期データSQLは `INSERT ... ON DUPLICATE KEY UPDATE` で再実行可能にする。
38. 初期データSQL再実行時の更新対象は `dst_folder_norm`、`note`、`is_active`、`manual_judgement`、`updated_at` とする。
39. `medical_folder_aliases.created_at` は初回INSERT時のみ設定する。
40. `medical_folder_aliases.alias_id` は自動採番に任せ、seed SQLでは明示投入しない。
41. `medical_folder_aliases.note` は、補足がある行のみ値を入れ、補足なしは `NULL` とする。

### 未決として残す

1. `dev_phr.event.result_root_path` migration の正式ファイル名。
2. `result_root_path` の初期値を既存 `event_id = 2` へ設定するか、別途手動更新とするか。
3. seed SQL 内の188件データの最終確認。
4. `file_receipts` の file_role / file_type / storage_folder_type の値定義。
5. `xml_status` / `check_status` / `xml_export_status` のreason code詳細。
6. `exam_item_values` の raw値・正規化値の保持範囲。
7. `exam_item_values.validation_status` の値定義。
8. XML単位の詳細ステータス（項目別・工程別）の追加要否。
9. reason集約の詳細。
10. `dev_phr.exam_item_master` に異常値 min/max を追加するか。
11. 既存 `LSIO_Legal_Item` と `02_exam_check_item_spec_v2_0_0.md` の法定項目差分をどう反映するか。
12. 人＋イベント台帳の正式名称。
13. v2初期スコープに人＋イベント台帳を含めるか。
14. 人＋イベント台帳に保持する `operation_status` の正式値。
15. 再提出XMLをどの旧XMLの解決として扱うかの紐付け方法。
16. HIA出力履歴台帳を将来追加するか。

---

## 7. 現時点の結論

v2では、`dev_phr` の既存マスタ・加入者・event系テーブルを活かしつつ、処理系・台帳系は新規DB `health_exam_result` に分離する。

core DDLの初期作成対象は以下の7テーブルとする。

```text
etl_runs
etl_errors
medical_folder_aliases
file_receipts
xml_ledger
xml_file_links
exam_item_values
```

`exam_check_results` はcore DDLから一旦外し、制度チェックDDLとして後続で作成する。

XML取込から制度チェックまでの基本フローは、`file_receipts → xml_file_links → xml_ledger → exam_item_values → exam_check_results → xml_ledger.check_status` とし、物理受領台帳・XML内容台帳・健診値・項目別チェック結果・制度単位総合判定を分離する。

将来的にはCSV直取込に対応する場合、`csv_row_ledger` を追加し、`file_receipts → xml_file_links / csv_row_ledger → xml_ledger / exam_item_values / exam_check_results` の構造で基本情報Ledgerと健診値を分離する。

将来的には人＋イベント単位の状態管理台帳を追加する方向とし、`file_receipts` / `xml_ledger` には人単位の最終完了状態を背負わせない。
