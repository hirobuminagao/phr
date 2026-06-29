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

参照。

v2初期実装では、設定YAMLから固定 `event_id` を指定して処理を実行する。

### 用途

- `event_id` によって、対象年度・健診イベント・保険者を識別する。
- `xml_ledger.event_id` に保持する。

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
insurer_number
submitter_facility_code
medical_institution_code
medical_institution_name
storage_folder_type
status
summary_message
received_at
copied_at
processed_at
created_at
updated_at
```

### メモ

- `zip_receipts` は初期実装では独立テーブルにしない。
- ZIPは `file_type = ZIP` の処理分岐として扱う。

---

## 4.2 xml_ledger

### 役割

XML単位の品質・突合・処理状態を管理する中心台帳。

### 責務

- XML由来の基本情報Ledger（受診者/文書単位）。
- `event_id` と `subscriber_id` の保持。
- XML基本情報の保持。
- 加入者突合結果の保持。
- item_values抽出状態の保持。
- 法定健診・特定健診チェックのサマリー保持。
- HIAアップロード可否の判断材料保持。

### 主なカラム候補

```text
id
event_id
file_receipt_id
subscriber_id
hia_subscriber_id
xml_filename
xml_path
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
subscriber_match_status
xml_parse_status
item_extract_status
legal_required_count
legal_present_count
legal_is_complete
specific_required_count
specific_present_count
specific_is_complete
check_status
hia_ready_status
status
error_count
created_at
updated_at
```

### メモ

- 現行 `medi_xml_ledger` の `lsio_legal_*` は、v2では命名を再整理する。
- `xml_sha256` は証跡として保持するが、v2内の主参照は `xml_ledger.id` を基本とする。
- `hia_subscriber_id` は正ではなく、HIA加入者IDで調査・検索するための運用補助キーとして冗長保持する。

---

## 4.3 exam_item_values

### 役割

健診結果値の共通基盤。初期実装ではXML由来を対象とし、将来的にCSV由来も受け入れる。

### 責務

- XMLから抽出した健診値の事実データを保持する。
- 初期実装ではXML由来を対象とする。
- 将来的にはCSV由来も同じ構造で保持する。
- 不足項目は保持しない。
- raw値と正規化値を必要な重複として保持する。

### 主なカラム候補

```text
id
source_type
source_id
event_id
subscriber_id
hia_subscriber_id
namecode
item_code
item_name
identity_item_code
value_type
value_raw
value_text
value_numeric
unit_raw
unit_normalized
code_value
code_system
method_code
null_flavor
effective_time
source_xpath
normalize_status
normalize_error
abnormal_check_status
created_at
updated_at
```

### メモ

- `item_values` は「存在した値」だけを持つ。
- 法定健診・特定健診の不足判定結果は `item_values` に持たない。
- source_type/source_id により由来Ledgerへ接続する。
- XML由来では source_id は xml_ledger.id を指す。
- CSV由来を追加する場合は csv_row_ledger.id を指す。
- xml_sha256 や row_hash は由来Ledger側で保持する。
- `event_id`、`subscriber_id`、`hia_subscriber_id` は、正規化のためではなく、SQLによる運用調査・障害解析・検索性向上のために冗長保持する。
- `hia_subscriber_id` は正ではなく、HIA加入者IDで調査・検索するための運用補助キーとして扱う。
- 正は `subscriber_id` とし、`hia_subscriber_id` は加入者照合後に検索用キャッシュとして設定する。

---

## 4.4 exam_check_results

### 役割

法定健診・特定健診のチェック結果台帳。

人が確認・エクスポート・集計しやすいよう、横持ちを基本とする。

### 責務

- 法定健診項目のOK/NGステータス保持。
- 特定健診項目のOK/NGステータス保持。
- 法定健診・特定健診の reason を日本語TEXTで保持。
- 由来Ledger（初期は `xml_ledger`、将来は `csv_row_ledger`）と接続してエクスポート・集計に利用する。

### 主なカラム候補

```text
id
source_type
source_id
event_id
subscriber_id
hia_subscriber_id
legal_status
specific_status
legal_reason
specific_reason
legal_required_count
legal_present_count
specific_required_count
specific_present_count
created_at
updated_at
```

### 法定健診項目ステータス例

```text
legal_9N001_status
legal_9N006_status
legal_9N011_status
legal_9N016_status
legal_9A750_status
legal_9A760_status
legal_1A010_status
legal_1A020_status
legal_2A020_status
legal_2A030_status
legal_3B035_status
legal_3B045_status
legal_3B090_status
legal_3B339_status
legal_3F015_status
legal_3F070_status
legal_3F077_status
legal_3D010_status
legal_3D046_status
legal_9A110_status
legal_9N206_status
legal_9D100_status
legal_9E160_status
```

### reason形式

reason は項目別カラムではなく、法定健診で1カラム、特定健診で1カラムとする。

例:

```text
胸部X線：項目なし｜聴力：値なし｜視力：値なし
```

### メモ

- 現行の `medi_lsio_identity_presence` や `medi_lsio_missing_items` のような縦持ち中間テーブルは、初期実装では必須としない。
- 必要であれば、不足項目だけを保持する明細テーブルを後続で追加する。
- 従来の `xml_ledger_id` の役割は、`source_type` + `source_id` に一般化する。
- 初期実装では `source_type = XML`、`source_id = xml_ledger.id` として扱う。
- 将来的にCSV直取込へ対応する場合は、`source_type = CSV`、`source_id = csv_row_ledger.id` として同じ構造を利用する。
- `event_id`、`subscriber_id`、`hia_subscriber_id` は、SQLによる運用調査・障害解析・検索性向上のために冗長保持する。

---

## 4.5 process_errors

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

## 4.6 runs

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

v2では、まず `item_values` とルールマスタから直接 `xml_ledger`・`exam_check_results` へ集約する。

必要になった場合のみ、不足項目明細テーブルを追加する。

### 5.3 csv_row_ledger

初期実装では作成しない。

CSV直取込を行う場合は、CSV1行=1受診者単位の基本情報Ledgerとして追加する。

CSVや紙データは、初期実装では別プロジェクト・別スクリプトでXMLへ変換し、`02_健診結果（編集）` へ投入する運用とする。

---

## 6. DDL設計で決めること

1. 新規DB名を `health_exam_result` とするか。
2. `file_receipts` の file_role / file_type / storage_folder_type の値定義。
3. `xml_ledger` のステータス体系。
4. `item_values` の raw値・正規化値の保持範囲。
5. `exam_check_results` の横持ちカラム一覧。
6. 法定健診・特定健診 reason の区切り文字と出力形式。
7. `process_errors` と `runs` を独自に持つか、既存ETL系に寄せるか。
8. `dev_phr.exam_item_master` に異常値 min/max を追加するか。

---

## 7. 現時点の結論

v2では、`dev_phr` の既存マスタ・加入者・event系テーブルを活かしつつ、処理系・台帳系は新規DB `health_exam_result` に分離する方向とする。

初期実装の中心は以下の4テーブルとする。

```text
file_receipts
xml_ledger
exam_item_values
exam_check_results
```

将来的にはCSV直取込に対応する場合、`csv_row_ledger` を追加し、`file_receipts → xml_ledger / csv_row_ledger → exam_item_values` の菱形構造で基本情報Ledgerと健診値を分離する。
