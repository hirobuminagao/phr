# health_exam_result v2 テーブル設計メモ

このドキュメントは `08_table_ddl_summary_codex.md` をもとに、現行参照実装のDDL調査結果を v2 設計へ落とし込むための設計メモである。

`08_table_ddl_summary_codex.md` は現行調査の一次資料として扱い、このドキュメントでは v2 で採用する責務・統合方針・保留事項を整理する。

---

## 1. v2 設計方針

### 1.1 現行参照実装との関係

- 現行参照実装は `kenshin_list_pydir` として扱う。
- 今回の移設・リファクタリング後の新システムを `health_exam_result v2` と呼ぶ。
- v2 では現行構造をそのまま踏襲せず、責務を明確化して再設計する。
- 現行実装のうち、ZIP展開・XML解析・item抽出・正規化・照合などの処理資産は必要に応じて再利用する。

### 1.2 初期スコープ

v2 初期スコープは、イベント・対象者台帳まで広げず、まず XML 品質保証基盤を完成させる。

初期スコープに含めるもの:

- ファイル台帳
- ZIP Receipt
- XML Ledger
- subscribers.id との紐付け
- item_values 登録
- XMLチェック
- 健診項目不足チェック
- XML Ledger へのチェック結果反映

初期スコープ外:

- event_subject（対象者台帳）
- 結果未着管理
- 再提出管理の本格運用
- 特定健診・法定健診の集計機能
- 請求・金額処理

### 1.3 event の扱い

- v2 初期段階から `event` の概念は持つ。
- 初期実装では、`event_id` は設定YAMLから固定値として渡す。
- XML Ledger には `event_id` を保持する。
- event の本格管理、対象者台帳、集計は次期実装で整備する。

---

## 2. v2 の基本階層

v2 初期実装の基本階層は以下とする。

```text
file_receipts
  ↓
zip_receipts
  ↓
xml_ledger
  ↓
item_values
```

各階層の責務は次の通り。

| 階層 | 責務 |
| --- | --- |
| file_receipts | 受領・投入対象ファイルの管理 |
| zip_receipts | ZIP単位の展開・取込結果管理 |
| xml_ledger | XML単位の業務・品質状態管理 |
| item_values | XMLから実際に存在した健診値の管理 |

---

## 3. v2 テーブル候補

## 3.1 file_receipts

### 役割

ファイル単位の物流・取込管理を行う。

受領台帳とは別物とし、v2システム側が自動生成・更新するファイル台帳とする。

### 現行参照

- `work_other.medi_shared_files`
- `work_other.medi_zip_receipts`

### v2 方針

- ネットワーク共有フォルダ上の原本ファイルや、`02_健診結果（編集）` から取り込むファイルを管理する。
- ファイルのSHA256、元パス、workコピー後パス、形式判定、処理状態を保持する。
- 受領台帳へのサマリー書き戻しは、このテーブルまたは派生ビューをもとに行う。

### 主なカラム候補

- `file_receipt_id`
- `event_id`
- `insurer_number`
- `facility_code`
- `source_path`
- `work_path`
- `file_name`
- `file_ext`
- `file_type`
- `file_sha256`
- `file_size`
- `received_at`
- `copied_at`
- `status`
- `summary_message`
- `created_at`
- `updated_at`

### 保留

- `source_path` と `work_path` のどちらを正とするか。
- 処理後の `data/medical/work` のファイルを削除・保持・アーカイブのどれにするか。

---

## 3.2 zip_receipts

### 役割

ZIP単位の取込結果を管理する。

### 現行参照

- `work_other.medi_zip_receipts`
- `work_other.medi_zip_receipt_runs`

### v2 方針

- 1 ZIP につき 1 レコードを基本とする。
- ZIP内のXML件数、展開結果、パスワード有無、エラー件数を管理する。
- `file_receipts` がZIPファイルそのものの管理、`zip_receipts` はZIP展開結果の管理とする。

### 主なカラム候補

- `zip_receipt_id`
- `file_receipt_id`
- `event_id`
- `zip_name`
- `zip_sha256`
- `extract_status`
- `xml_count_total`
- `xml_count_success`
- `xml_count_error`
- `password_used`
- `extracted_dir`
- `error_message`
- `created_at`
- `updated_at`

### 保留

- ZIP単位の台帳を `file_receipts` に統合できるか。
- ZIP以外のXML単体・CSV等を将来扱う場合の抽象化。

---

## 3.3 xml_ledger

### 役割

XML単位の業務・品質状態を管理する。

v2 初期実装の中心テーブルとする。

### 現行参照

- `work_other.medi_xml_ledger`
- `work_other.medi_xml_receipts`

### v2 方針

- XML原本の証跡と、加入者紐付け・品質チェック結果を保持する。
- `subscribers.id` を保持する。
- `event_id` を保持する。
- XML単位で、HIA投入可能かどうかを判断できる状態にする。
- 制度チェックの詳細は将来的に専用台帳へ分離するが、初期実装ではXML単位のチェックサマリーを持つ。

### 主なカラム候補

- `xml_ledger_id`
- `event_id`
- `file_receipt_id`
- `zip_receipt_id`
- `subscriber_id`
- `xml_filename`
- `xml_path`
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
- `subscriber_match_status`
- `xml_parse_status`
- `xml_schema_status`
- `item_extract_status`
- `item_check_status`
- `abnormal_value_status`
- `hia_ready_status`
- `status`
- `error_count`
- `warning_count`
- `created_at`
- `updated_at`

### 責務上の注意

- `xml_ledger` は「XMLの証跡」と「XML単位の品質状態」を持つ。
- 対象者台帳やイベント対象者管理の責務は持たない。
- 不足項目そのものを `item_values` に追加しない。
- 健診値として存在しない項目は、別のチェック結果・エラー・サマリーで表現する。

### 保留

- `medi_xml_receipts` を独立テーブルとして残すか、`xml_ledger` に統合するか。
- `judge_*` や `lsio_legal_*` のような制度チェック列を初期実装で持つか、将来の制度チェック台帳へ分離するか。

---

## 3.4 item_values

### 役割

XMLから実際に存在した健診値を保持する。

### 現行参照

- `work_other.medi_xml_item_values`
- `work_other.medi_exam_result_item_values`

### v2 方針

- v2では item_values を1系統に寄せる。
- `item_values` は「存在した健診値」のみ保持する。
- 不足項目は item_values に入れない。
- raw値と正規化値は必要な重複として同一テーブルに保持する方向で検討する。

### 主なカラム候補

- `item_value_id`
- `xml_ledger_id`
- `event_id`
- `subscriber_id`
- `item_code`
- `item_name`
- `value_type`
- `value_raw`
- `value_text`
- `value_numeric`
- `unit_raw`
- `unit_normalized`
- `code_value`
- `code_system`
- `null_flavor`
- `effective_time`
- `source_xpath`
- `normalize_status`
- `normalize_error`
- `abnormal_check_status`
- `created_at`
- `updated_at`

### 保留

- raw値と正規化値を同一テーブルに持つか、別テーブルに分けるか。
- 異常値チェック結果を item_values に持つか、別の value_check テーブルに分けるか。

---

## 3.5 process_logs / errors

### 役割

処理ログとエラーを管理する。

### 現行参照

- `work_other.medi_xml_process_logs`
- `work_other.etl_errors`
- `work_other.medi_import_runs`
- `work_other.etl_runs`

### v2 方針

- run管理は既存の共通ETL思想に合わせる。
- エラーは単なるログではなく、対象XMLやファイルへ戻れるリレーションを持たせる。
- 初期実装では広げすぎず、XML Ledgerやfile_receiptsに紐付くエラー記録を優先する。

### 主なカラム候補

- `error_id`
- `run_id`
- `file_receipt_id`
- `zip_receipt_id`
- `xml_ledger_id`
- `item_value_id`
- `error_type`
- `error_code`
- `error_message`
- `status`
- `resolved_by_xml_ledger_id`
- `created_at`
- `resolved_at`

### 保留

- process log と error ledger を分けるか統合するか。
- エラーのステータス管理を初期実装に含めるか。

---

## 4. v2で参照する既存DB

## 4.1 dev_phr.subscribers

### 扱い

参照のみ。

v2側へ複製しない。

### 用途

- XML基本情報から subscriber を照合する。
- 照合結果として `xml_ledger.subscriber_id` に `subscribers.id` を保持する。

---

## 4.2 dev_phr.funds / fund_insurer_numbers

### 扱い

参照のみ。

### 用途

- 保険者番号から健保情報を解決する。
- event設定や取り込み設定の検証に利用する。

---

## 4.3 dev_phr.exam_item_master

### 扱い

参照のみ。

### 用途

- item_code の名称・型・単位を解決する。
- 異常値チェックの min/max を持たせる候補。

---

## 5. 統合候補

### 5.1 medi_xml_item_values + medi_exam_result_item_values

v2では `item_values` へ統合する方向。

理由:

- どちらも健診項目値を保持している。
- raw値・正規化値・normalize_status を整理すれば1系統で管理可能。
- 制度チェック結果は別テーブルへ分離するため、item_valuesは事実値に専念できる。

---

### 5.2 medi_xml_receipts + medi_xml_ledger

完全統合するかは保留。

ただし、v2では以下の責務に分ける。

- receipt: 原本・受領・展開状態
- ledger: XML単位の業務・品質状態

現行のように近い情報を複数箇所に持つ場合は、どちらが正かを明確にする。

---

### 5.3 medi_import_runs + etl_runs

v2では共通 `etl_runs` 系へ寄せる方向。

理由:

- run管理の仕組みは共通化した方がよい。
- DBごとに `etl_runs` / `etl_errors` を持つ運用は既存のHIA subscriber syncとも揃う。

---

## 6. 廃止・参照のみ候補

### 6.1 medi_exam_result_ledger

v2初期スコープでは採用しない候補。

理由:

- 初期スコープはXML Ledgerまで。
- 成果物生成・XML出力向けの責務は後段へ分離する。

---

### 6.2 medi_exam_result_item_values

v2では `item_values` へ統合候補。

---

### 6.3 csv_header_map_submit

v2初期スコープでは対象外。

CSV取込を本格対応する段階で再検討する。

---

## 7. v2設計で先に決めること

1. `file_receipts` と `zip_receipts` の境界。
2. `xml_receipts` を残すか、`xml_ledger` に統合するか。
3. `xml_ledger` に持たせるチェックサマリーの範囲。
4. `item_values` の raw値・正規化値の保持方法。
5. 異常値チェック結果の保持先。
6. エラー台帳とprocess logの境界。
7. `event_id` の設定YAMLとDBイベント定義の関係。
8. `dev_phr.exam_item_master` に異常値min/maxを追加するか。
9. 受領台帳へのサマリー書き戻し方法。

---

## 8. 現時点の結論

v2 初期実装では、現行の receipt / ledger / item_values の構造をそのまま移設しない。

以下の責務へ再編する。

```text
file_receipts
  = ファイル単位の取込・物流管理

zip_receipts
  = ZIP単位の展開結果管理

xml_ledger
  = XML単位の業務・品質状態管理

item_values
  = 実際に存在した健診値の管理
```

v2の中心は `xml_ledger` とし、`subscribers.id`、`event_id`、XMLチェック結果、健診項目チェック結果をここに集約する。

対象者台帳・制度チェック台帳・イベント管理は、v2初期基盤の上に次期機能として追加する。
