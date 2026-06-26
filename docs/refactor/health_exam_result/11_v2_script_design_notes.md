

# health_exam_result v2 スクリプト設計メモ

このドキュメントは、`health_exam_result v2` のスクリプト構成・責務・実行順序を整理するためのメモである。

DDL側の骨子は `12_v2_ddl_design_notes.md` を参照する。

---

## 1. 目的

v2 初期実装で必要になるスクリプト群を整理し、どの処理をエントリースクリプトに置き、どの処理を `script_lib` / `scripts/lib` に寄せるかを明確にする。

---

## 2. 基本方針

### 2.1 スクリプト配置

```text
scripts/medical/
  人が実行するエントリースクリプトのみ配置する。

scripts/medical/script_lib/
  medical専用の業務ロジックを配置する。

scripts/lib/
  全システム共通ライブラリを配置する。
```

### 2.2 責務分離

- `scripts/medical/` 直下は処理順序を表す薄いオーケストレーターとする。
- ファイル台帳、XML Ledger、加入者照合、item_values登録、チェック処理などの実処理は `scripts/medical/script_lib/` に実装する。
- 正規化、identity_hash、DB接続、XMLユーティリティなどの汎用処理は `scripts/lib/` を利用する。
- 既存 `kenshin_list_pydir` は参照実装とし、必要な処理のみ v2 へ移設・再設計する。

---

## 3. v2 初期実装の処理フロー

```text
02_健診結果（編集）
  ↓
data/medical/work へコピー
  ↓
file_receipts 登録
  ↓
ファイル種別で処理分岐
  ├─ ZIP → 展開 → XML検出
  ├─ XML → XML検出
  └─ CSV → 将来対応
  ↓
xml_ledger 登録
  ↓
XML基本情報抽出
  ↓
subscriber lookup
  ↓
item_values 登録
  ↓
法定健診チェック / 特定健診チェック / 異常値チェック
  ↓
xml_ledger へチェック結果集約
  ↓
exam_check_results 登録
  ↓
file_receipts へ処理結果サマリー集約
  ↓
HIAアップロード待ち / エラー
```

---

## 4. エントリースクリプト候補

## 4.1 `01_register_files.py`

### 役割

`02_健診結果（編集）` 等の投入対象フォルダからファイルを検出し、`file_receipts` に登録する。

### 主な処理

- 設定YAML読み込み。
- `event_id` 取得。
- input/sourceフォルダ走査。
- ファイル種別判定。
- SHA256算出。
- 格納フォルダやファイル名から保険者番号・健診機関番号・健診機関名を抽出。
- workフォルダへコピー。
- `file_receipts` 登録または更新。

### 呼び出す想定の `script_lib`

- `config_loader`
- `file_receipt_service`
- `file_metadata_parser`
- `work_file_manager`

---

## 4.2 `02_import_xml_files.py`

### 役割

`file_receipts` に登録されたファイルを処理し、XMLを検出して `xml_ledger` に登録する。

### 主な処理

- `file_receipts` から未処理ファイルを取得。
- ZIPの場合は展開。
- XML単体の場合はそのまま処理。
- XMLファイル検出。
- XML SHA256算出。
- XML基本情報抽出。
- `xml_ledger` 登録。
- `file_receipts` の処理サマリー更新。

### 呼び出す想定の `script_lib`

- `file_receipt_service`
- `zip_extract_service`
- `xml_discovery_service`
- `xml_ledger_service`
- `xml_basic_info_extractor`

---

## 4.3 `03_match_subscribers.py`

### 役割

XML基本情報から加入者を照合し、`xml_ledger.subscriber_id` を設定する。

### 主な処理

- `xml_ledger` から未照合XMLを取得。
- 保険者番号・記号・番号・生年月日・氏名カナ・性別を正規化。
- identity_hash を生成。
- `dev_phr.subscribers` を lookup。
- `xml_ledger.subscriber_id`、`identity_hash`、match系カラム、match_status を更新。

### 呼び出す想定の `script_lib`

- `subscriber_matcher`
- `xml_ledger_service`

### 呼び出す想定の `scripts/lib`

- normalize系
- identity_hash系

---

## 4.4 `04_extract_item_values.py`

### 役割

`xml_ledger` に登録されたXMLから健診値を抽出し、`item_values` に登録する。

### 主な処理

- `xml_ledger` から item未抽出XMLを取得。
- XMLを読み込む。
- observation / entry を解析。
- `dev_phr.exam_item_master` を参照して項目情報を解決。
- `item_values` 登録。
- `xml_ledger.item_extract_status` 更新。

### 呼び出す想定の `script_lib`

- `xml_ledger_service`
- `item_value_extractor`
- `exam_item_master_repository`
- `item_value_writer`

---

## 4.5 `05_check_exam_results.py`

### 役割

`item_values` とルールマスタをもとに、法定健診・特定健診・異常値のチェックを行う。

### 主な処理

- `xml_ledger` からチェック未実施XMLを取得。
- `item_values` を取得。
- `dev_phr.exam_item_groups` 系マスタを参照。
- 法定健診チェックを実行。
- 特定健診チェックを実行。
- 異常値チェックを実行。
- `xml_ledger` にチェックサマリーを更新。
- `exam_check_results` に横持ちチェック結果を登録。
- エラーがあれば `process_errors` に登録。

### 呼び出す想定の `script_lib`

- `legal_check_service`
- `specific_check_service`
- `abnormal_value_check_service`
- `exam_check_result_writer`
- `xml_ledger_service`
- `process_error_writer`

---

## 4.6 `06_export_hia_ready.py`

### 役割

HIAアップロード可能なXMLまたは成果物を抽出・出力する。

### 主な処理

- `xml_ledger` からHIAアップロード可能対象を取得。
- 必要に応じて出力フォルダ `03_健診結果（アップロードデータ）` にコピーまたは生成。
- `file_receipts` に出力ファイルを登録するかは後続検討。
- `xml_ledger.hia_ready_status` / `status` を更新。

### 呼び出す想定の `script_lib`

- `hia_export_service`
- `xml_ledger_service`
- `file_receipt_service`

---

## 5. script_lib 候補

## 5.1 file_receipt_service

### 役割

`file_receipts` の登録・更新・サマリー更新を担当する。

### 主な責務

- ファイル登録。
- 重複SHA判定。
- status更新。
- summary_message生成。

---

## 5.2 file_metadata_parser

### 役割

ファイル名・格納フォルダからメタ情報を抽出する。

### 主な責務

- 保険者番号抽出。
- 健診医療機関番号抽出。
- 健診医療機関名抽出。
- storage_folder_type 判定。

---

## 5.3 work_file_manager

### 役割

ネットワークフォルダ上のファイルを `data/medical/work` へコピーする。

### 主な責務

- workディレクトリ作成。
- コピー。
- SHA256確認。
- 処理後の保持・削除・アーカイブ方針は後続検討。

---

## 5.4 xml_discovery_service

### 役割

ZIP展開後またはXML単体ファイルから処理対象XMLを検出する。

### 主な責務

- ZIP内XML検出。
- 二重フォルダ対応。
- XML拡張子判定。
- 対象外ファイル除外。

---

## 5.5 xml_ledger_service

### 役割

`xml_ledger` の登録・更新を担当する。

### 主な責務

- XML証跡登録。
- XML基本情報更新。
- subscriber_id更新。
- チェックサマリー更新。
- HIA ready status更新。

---

## 5.6 xml_basic_info_extractor

### 役割

XMLから加入者照合・台帳登録に必要な基本情報を抽出する。

### 主な責務

- 保険者番号。
- 健診機関番号。
- 健診機関名。
- 健診日。
- 氏名カナ。
- 記号。
- 番号。
- 生年月日。
- 性別。

---

## 5.7 subscriber_matcher

### 役割

XML基本情報から `dev_phr.subscribers.id` を解決する。

### 主な責務

- 照合用正規化。
- identity_hash生成。
- subscribers lookup。
- 照合結果オブジェクト返却。

---

## 5.8 item_value_extractor

### 役割

XMLから健診項目値を抽出する。

### 主な責務

- nameCode抽出。
- methodCode抽出。
- PQ / CD / ST 等の値型抽出。
- nullFlavor抽出。
- source_xpath保持。

---

## 5.9 legal_check_service

### 役割

法定健診項目の不足判定を行う。

### 主な責務

- `LSIO_Legal_Item` グループルール取得。
- item_values から代表項目のpresence判定。
- required_count / present_count / is_complete 算出。
- 日本語 reason 生成。
- 横持ちステータス生成。

---

## 5.10 specific_check_service

### 役割

特定健診項目の不足判定を行う。

### 主な責務

- 特定健診ルール取得。
- item_values から必須項目判定。
- required_count / present_count / is_complete 算出。
- 日本語 reason 生成。
- 横持ちステータス生成。

---

## 5.11 abnormal_value_check_service

### 役割

健診値の異常値チェックを行う。

### 主な責務

- `exam_item_master` の min/max 候補を参照。
- 数値化可能な値をチェック。
- 明らかな異常値のみ error とする。
- warning は初期実装では扱わない方向。

---

## 5.12 exam_check_result_writer

### 役割

`exam_check_results` にチェック結果を登録する。

### 主な責務

- 法定健診項目ステータス横持ち登録。
- 特定健診項目ステータス横持ち登録。
- legal_reason / specific_reason 登録。

---

## 5.13 process_error_writer

### 役割

処理エラーを `process_errors` に登録する。

### 主な責務

- ファイル単位エラー登録。
- XML単位エラー登録。
- item単位エラー登録。
- error_type / error_code / error_message の標準化。

---

## 6. scripts/lib で利用する共通処理

既存 `scripts/lib/` から利用する候補。

```text
normalize
identity_hash
file hash
DB connection / transaction
XML parser utility
logging
config loader
```

※ 実際の既存lib名は実装時に確認する。

---

## 7. 初期実装でやらないこと

- 本格的な event_subject / person_event 更新。
- 結果未着管理。
- 再提出管理の本格運用。
- CSV取込の本格対応。
- 保険者変換の完全実装。
- HIAアップロード後の自動結果反映。

---

## 8. 今後決めること

1. エントリースクリプトを何本に分けるか。
2. `03_match_subscribers.py` を独立させるか、XML import内で実施するか。
3. `04_extract_item_values.py` と `05_check_exam_results.py` を分けるか、1人/1XML単位で連続実行するか。
4. `runs` の作成タイミング。
5. `process_errors` のエラーコード体系。
6. HIAアップロード待ちステータスをどのテーブルで最終管理するか。

---

## 9. 現時点の結論

v2のエントリースクリプトは薄く保ち、実処理は `scripts/medical/script_lib/` に集約する。

初期実装では、まず以下の流れを成立させる。

```text
file_receipts 登録
  ↓
xml_ledger 登録
  ↓
subscriber_id 解決
  ↓
item_values 登録
  ↓
法定健診・特定健診チェック
  ↓
exam_check_results 登録
  ↓
xml_ledger / file_receipts へサマリー集約
```
