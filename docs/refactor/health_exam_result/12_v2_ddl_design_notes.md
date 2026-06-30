# health_exam_result v2 DDL設計メモ

このドキュメントは、`health_exam_result v2` のDB・テーブル設計を整理するためのメモである。

詳細な現行DDL調査は `08_table_ddl_summary_codex.md`、v2テーブル責務の初期整理は `09_v2_table_design_notes.md` を参照する。

---

## 1. 目的

v2で必要になるDB・テーブル・既存dev_phrテーブルの利用方針を整理し、DDL作成前の骨子を定義する。

---

## 2. DB方針

### 2.1 新規DB候補

v2の処理系・台帳系は、既存 `dev_phr` へ混在させず、新しいDBを作成する方向で検討する。

DB名候補:

```text
health_exam_result
```

### 2.2 DB責務分離

```text
dev_phr
  = 既存マスタ・加入者・健保・イベント定義

health_exam_result
  = v2の取込・XML Ledger・健診値・チェック結果・処理ログ
```

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

### 追加カラム候補

```text
result_root_path
```

### メモ

- `result_root_path` は医療機関フォルダの親ディレクトリまでを保持する。
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
- `item_values` 登録時の項目辞書として利用する。
- 異常値チェックの min/max を追加する候補。

---

### 3.6 exam_item_groups / exam_item_group_members / exam_item_group_method_members / exam_item_group_identity_members

### 扱い

参照。

### 用途

- 法定健診・特定健診などの項目グループ定義。
- 現行では `LSIO_Legal_Item` が法定健診項目グループとして使われている。
- `exam_item_group_identity_members` は代表項目・必須フラグ・presence判定ルールを持つ。
- `exam_item_group_members` は nameCode ベースのグループ所属を持つ。
- `exam_item_group_method_members` は methodCode ベースの presence 判定補助を持つ。

---

## 4. health_exam_result側で作成するテーブル候補

## 4.1 file_receipts

### 役割

ファイル資産台帳。

受領ファイル・投入用ファイル・将来的な出力ファイルを1ファイル単位で管理する。

### 責務

- ファイル種別管理。
- source/work/output パス管理。
- ファイルSHA256管理。
- 格納フォルダやファイル名から取得できる保険者番号・健診機関番号・健診機関名の保持。
- 処理対象件数と中身確認日時の保持。
- 処理結果サマリーの保持。

### 主なカラム候補

```text
id
event_id
file_role
file_type
file_name
file_ext
source_path
work_path
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
copied_at
processed_at
created_at
updated_at
```

### メモ

- `zip_receipts` は初期実装では独立テーブルにしない。
- ZIPは `file_type = ZIP` の処理分岐として扱う。
- `facility_code` / `facility_name` は健診機関コード・名称として保持する。
- `processable_count` はZIPならXML件数、XML単体なら通常1、CSVなら設定に従って算出したデータ行数を保持する。
- `content_checked_at` はファイル種別に依存しない中身確認日時として保持する。

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
- 健診内容チェックの総合判定サマリー保持。
- XML単位のHIAアップロード用出力可否の保持。
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
xml_status
xml_reason
check_status
check_reason
xml_export_status
manual_export_approved
manual_export_reason
created_at
updated_at
```

### メモ

- `xml_status` はXMLとしての処理状態を保持する。
- `xml_reason` は固定enumではなく、スクリプト実装・チェック追加に応じて理由コードを追加できる文字列カラムとする。
- `check_status` / `check_reason` は健診内容チェックの総合判定サマリーとして保持する。
- `xml_export_status` はHIAアップロード用XMLとして出力してよいかをXML単位で保持する。基本は `OK` / `NG`。
- `check_status = NG` でも、医療機関確認等により正当理由が確認できた場合は、`manual_export_approved = true`、`manual_export_reason` を設定し、`xml_export_status = OK` とできる。
- `check_status` はシステム判定結果として保持し、手動承認によって変更しない。
- `item_extract_status`、`hia_ready_status`、`xsd_valid`、`is_exam_result`、`error_count`、`warning_count` は独立カラムとしては持たない。
- `xml_sha256` はXML内容の一意キーとして保持する。v2内の主参照は `xml_ledger.id` を基本とする。
- `file_receipt_id` は `xml_ledger` に持たず、物理ファイルとの関係は `xml_file_links` で管理する。
- `first_seen_run_id`、`last_seen_run_id`、`first_seen_at`、`last_seen_at` は `xml_ledger` には持たず、物理受領履歴は `file_receipts` と `xml_file_links` 側で管理する。
- 別ZIP等で同一 `xml_sha256` のXMLを受領した場合は、`xml_ledger` を重複作成せず、`xml_file_links` のみ追加する。
- `hia_subscriber_id` は正ではなく、HIA加入者IDで調査・検索するための運用補助キーとして冗長保持する。
- 出力済み状態（`exported_at` / `export_run_id` 等）を `xml_ledger` に持つか、出力台帳側に持つかは、exportスクリプト設計時に決定する。

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
- 単体XMLの場合、`xml_inner_path` はNULL可とする。
- 同一 `xml_sha256` のXMLを別ZIP等で受領した場合は、既存 `xml_ledger` を参照する `xml_file_links` を追加する。

---

## 4.4 exam_item_values

### 役割

健診結果値の共通基盤。初期実装ではXML由来を対象とし、将来的にCSV由来も受け入れる。

### 責務

- XML / CSV 由来の健診項目値を共通形式で保持する。
- 由来Ledgerを `ledger_type` / `ledger_id` で表現する。
- 検索性向上のため `event_id` / `subscriber_id` / `hia_subscriber_id` を冗長保持する。
- 実際に存在した健診値のみを保持する。
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
namecode
occurrence_no
raw_value
raw_value_type
raw_unit
normalized_value
normalized_unit
nullflavor
code_system
code_value
code_display
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
- `exam_item_values` は実際に存在した健診値のみを保持する。
- 制度チェックは、`exam_item_values` に存在する値だけでなく「存在しない項目」も判定材料とするため、`exam_check_results` 側の責務とする。
- 項目値としての妥当性（範囲外・形式不正等）は `validation_status` / `validation_reason` で保持する。
- `validation_status` は制度チェックではなく、値そのものの妥当性を表す。
- CSVからHIAアップロード用XMLを生成する場合も、`exam_item_values` の正規化済み値を利用する。
- `event_id`、`subscriber_id`、`hia_subscriber_id` は、正規化のためではなく、SQLによる運用調査・障害解析・検索性向上のために冗長保持する。
- `hia_subscriber_id` は正ではなく、HIA加入者IDで調査・検索するための運用補助キーとして扱う。

---

## 4.5 exam_check_results

### 役割

法定健診・特定健診のチェック結果台帳。

人が確認・エクスポート・集計しやすいよう、横持ちを基本とする。

### 責務

- 1受診者・1Ledger単位の制度チェック結果を保持する。
- `exam_item_values` に存在する値、および存在しない項目を判定材料として保持する。
- 制度チェック対象項目を同一性項目コード単位で横持ちする。
- 各チェック対象項目の状態を `status` / `reason` で保持する。
- 法定健診・特定健診の総合判定を保持する。
- 法定健診・特定健診の reason summary を保持する。
- 検索性向上のため `event_id` / `subscriber_id` / `hia_subscriber_id` を冗長保持する。

### 主なカラム候補

```text
id
event_id
ledger_type
ledger_id
subscriber_id
hia_subscriber_id
legal_status
legal_reason_summary
specific_status
specific_reason_summary
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

法定健診・特定健診の reason summary は、項目別 `status` / `reason` から集約して保持する。

例:

```text
9N206:項目なし|9D100:値なし|9E160:値なし
```

### メモ

- `exam_check_results` は制度チェック結果台帳であり、実値テーブルではない。
- 実値は `exam_item_values` に縦持ちで保持する。
- 制度チェックでは、`exam_item_values` に存在する値だけでなく「存在しない項目」も判定材料とする。
- 横持ち項目は同一性項目コード単位で作成する。
- 検査方法、左右、裸眼/矯正などではカラムを分けない。
- 項目別の値あり/なしや値有効/不正は、`present` / `valid` ではなく `status` / `reason` に集約する。
- 法定健診・特定健診で値の事実を二重管理しない。
- 法定健診・特定健診で分けるのは総合評価と reason summary のみとする。
- 項目別 `status` の正式コード一覧はDDL作成前に確定する。カラム命名規則は `status_<item_code>` / `reason_<item_code>` とする。
- 判定ルール自体は `exam_check_results` に保持しない。既存 `dev_phr.exam_item_group_*` 系マスタを利用する。
- 法定健診ルールマスタは現行内容を棚卸しし、`02_exam_check_item_spec_v2_0_0.md` との差分確認を行う。
- 特定健診ルールマスタは、スクリプト方針が固まった後に `02_exam_check_item_spec_v2_0_0.md` を元に新規作成する。

---

## 4.6 etl_errors

### 役割

処理中に発生したエラーを、ファイル・XML・項目へ紐付けて保持する。

### 主なカラム候補

```text
id
run_id
file_receipt_id
xml_ledger_id
item_value_id
error_type
error_code
error_message
status
resolved_by_xml_ledger_id
created_at
resolved_at
```

---

## 4.7 etl_runs

### 役割

取込・チェック処理の実行単位を管理する。

### 主なカラム候補

```text
id
run_type
event_id
started_at
finished_at
status
summary_message
created_at
updated_at
```

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

v2では、まず `exam_item_values` とルールマスタから直接 `xml_ledger`・`exam_check_results` へ集約する。

必要になった場合のみ、不足項目明細テーブルを追加する。

### 5.3 csv_row_ledger

初期実装では作成しない。

CSV直取込を行う場合は、CSV1行=1受診者単位の基本情報Ledgerとして追加する。

CSVや紙データは、初期実装では別プロジェクト・別スクリプトでXMLへ変換し、`02_健診結果（編集）` へ投入する運用とする。

---

## 6. DDL設計で決めること

1. 新規DB名を `health_exam_result` とするか。
2. `dev_phr.event` へ `result_root_path` を追加するMigration。
3. event対応版 `medical_folder_aliases` の正式テーブル名とDDL。
4. `file_receipts` の file_role / file_type / storage_folder_type の値定義。
5. `xml_file_links` の一意制約と再取込時のリンク追加ルール。
6. `xml_ledger` の `xml_status` / `check_status` / `xml_export_status` の値定義。
7. `exam_item_values` の raw値・正規化値の保持範囲。
8. `exam_item_values.validation_status` の値定義。
9. `exam_check_results` の横持ち72項目の正式カラム名。
10. `exam_check_results` の項目別 `status` の正式コード一覧。
11. 法定健診・特定健診 reason summary の区切り文字と出力形式。
12. 共通ETL仕様に従い、`etl_runs` / `etl_errors` を利用する。
13. `dev_phr.exam_item_master` に異常値 min/max を追加するか。
14. 既存法定健診ルールマスタが `02_exam_check_item_spec_v2_0_0.md` に耐えられるか。

---

## 7. 現時点の結論

v2では、`dev_phr` の既存マスタ・加入者・event系テーブルを活かしつつ、処理系・台帳系は新規DB `health_exam_result` に分離する方向とする。

初期実装の中心は以下の6テーブルとする。

```text
file_receipts
xml_file_links
xml_ledger
exam_item_values
exam_check_results
medical_folder_aliases
```

XML取込の基本フローは、`file_receipts → xml_file_links → xml_ledger / exam_item_values / exam_check_results` とし、物理受領台帳・XML内容台帳・健診値/チェック結果を分離する。

将来的にはCSV直取込に対応する場合、`csv_row_ledger` を追加し、`file_receipts → xml_file_links / csv_row_ledger → xml_ledger / exam_item_values / exam_check_results` の構造で基本情報Ledgerと健診値を分離する。
