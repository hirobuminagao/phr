# HIA to Fund XML Delivery Rebuild Design

## Status

Draft as of 2026-08-10.

本ドキュメントは、HIAからダウンロードしたXML/ZIPを健保へ納品する後工程を、既存 `scripts/work_folder` から切り出し、`scripts/hia` と正式DBへ再構築するための設計メモである。

健診機関から受領してHIAへアップロードする処理とは別レイヤーとして扱う。

## 背景

既存実装は以下にある。

- `scripts/work_folder/scripts/hia_import_zip.py`
- `scripts/work_folder/scripts/hia_build_delivery_zip.py`
- `work_other.hia_import_zips`
- `work_other.hia_xml_events`
- `work_other.hia_person_years`
- `work_other.hia_delivery_exclusion_rules`

既存実装は2025年度後半の運用に合わせ、HIAからダウンロードしたZIPを読み、同一人物が過去に登場していれば新しいZIP側から除外する考えで作られている。

今回の運用では、HIAへ先にアップロード済みのXMLがあり、その後に修正版XMLを健保へ納品したいケースがある。この場合、従来の「古い方を残し、新しい方から除外する」固定ロジックでは対応しにくい。

また、現行 `hia_xml_events` は、XML原本台帳と人への紐付けイベントを兼ねている。今後の再納品、修正版採用、累計サマリーを考えると、XMLそのものの台帳と、人に紐づく履歴管理を分けた方がよい。

## 目的

- HIAからダウンロードしたZIP/XMLを、HIA後工程の正式台帳として管理する。
- XML原本台帳と、人単位の紐付け履歴を分ける。
- 健保納品時に、既存優先または修正版優先を設定で切り替えられるようにする。
- 健診機関毎、受診月毎の最終ZIPサマリーを出せるようにする。
- 納品回数が増えた場合に、累計サマリーを出せるようにする。
- 旧 `work_other` / `scripts/work_folder` は参考実装とし、新規実装は `scripts/hia` に置く。

## 対象外

- 健診機関からHIAへアップロードするXML出力処理。
- HIA画面へのアップロード作業管理。
- 健診機関別CSVマッピング。
- HIA API連携。
- 健保納品後の事業所納品管理。

## DB方針

HIA後工程の正式台帳は `health_exam_result` に置く。

採用DB:

- `health_exam_result`

理由:

- `work_other` は作業用DBであり、正式な納品履歴を置くには責務が曖昧である。
- HIAから戻ったXMLも健診結果のライフサイクル上にあるため、`health_exam_result` の中で一連の流れとして扱える方が実務上追いやすい。
- 既存の健診機関からHIAへの出力履歴も `health_exam_result` にあるため、HIA後工程だけ別DBに切り出すと参照・運用が分散する。
- ただし、健診機関からHIAへ出す前段と混ざらないよう、テーブル名の prefix で責務を分ける。

prefix 方針:

- `hia_download_*`: HIAからダウンロードしたZIP/XMLの原本台帳。
- `hia_person_*`: HIAダウンロードXMLを人・年度へ寄せた履歴。
- `fund_delivery_*`: 健保へ納品するZIP/メンバー/除外ルールの履歴。

旧 `work_other` は参考元とし、初期実装ではHIA ZIPの再取込を基本方針にする。必要になった場合だけ、旧 `work_other` からの移行スクリプトを別途作る。

## テーブル構成案

### hia_download_zips

HIAからダウンロードしたZIP単位の台帳。

主な項目:

- `download_zip_id`
- `insurer_number`
- `facility_code`
- `folder_name`
- `zip_name`
- `dl_date`
- `send_seq`
- `zip_sha256`
- `source_zip_path`
- `archive_zip_path`
- `import_status`
- `xml_count_total`
- `xml_count_success`
- `xml_count_error`
- `created_at`
- `updated_at`

旧 `work_other.hia_import_zips` 相当。

### hia_download_xmls

ZIP内のXML 1ファイルごとの原本台帳。

人に紐づく前のXMLそのものの事実を保持する。

主な項目:

- `hia_download_xml_id`
- `download_zip_id`
- `xml_filename`
- `xml_inner_path`
- `xml_sha256`
- `exam_date`
- `exam_year`
- `facility_code`
- `facility_name`
- `report_category`
- `health_program_code`
- `insurer_number`
- `insurance_symbol_raw`
- `insurance_number_raw`
- `birthdate`
- `name_kana_raw`
- `gender_code`
- `parse_status`
- `parse_reason`
- `is_active_in_zip`
- `created_at`
- `updated_at`

現行 `hia_xml_events` のXML原本部分を切り出す。

### hia_person_years

人×年度の台帳。

主な項目:

- `person_year_id`
- `person_id_custom`
- `identity_hash`
- `name_kana_norm`
- `gender_code`
- `exam_year`
- `insurer_number`
- `insurance_symbol_match`
- `insurance_number_match`
- `birthdate`
- `first_seen_dl_date`
- `last_seen_dl_date`
- `dl_count`
- `created_at`
- `updated_at`

旧 `work_other.hia_person_years` 相当。

### hia_person_xml_events

XMLを人×年度へ紐付けた履歴。

主な項目:

- `person_xml_event_id`
- `person_year_id`
- `hia_download_xml_id`
- `event_type`
- `event_status`
- `is_current`
- `link_reason`
- `superseded_by_event_id`
- `created_at`
- `updated_at`

想定する `event_type`:

- `LINKED`
- `UNLINKED`
- `SUPERSEDED`
- `DELIVERY_SELECTED`
- `DELIVERED`
- `DELIVERY_EXCLUDED`

このテーブルを、人に対するXML履歴管理の中心とする。

### fund_delivery_runs

健保納品ZIP作成単位の履歴。

主な項目:

- `delivery_run_id`
- `insurer_number`
- `delivery_policy`
- `source_dl_date`
- `source_zip_name`
- `output_zip_name`
- `output_zip_path`
- `delivery_status`
- `delivery_xml_count`
- `delivery_person_count`
- `created_at`
- `created_by`
- `note`

### fund_delivery_members

納品ZIPに入れた個人XML単位の履歴。

主な項目:

- `delivery_member_id`
- `delivery_run_id`
- `person_year_id`
- `hia_download_xml_id`
- `person_xml_event_id`
- `xml_filename`
- `xml_sha256`
- `facility_code`
- `facility_name`
- `exam_date`
- `exam_month`
- `member_status`
- `created_at`

累計サマリーはこのテーブルから集計する。

### fund_delivery_exclusion_rules

健保納品から除外するルール。

旧 `work_other.hia_delivery_exclusion_rules` 相当。

主な項目:

- `exclusion_rule_id`
- `insurer_number`
- `facility_code`
- `facility_name`
- `rule_type`
- `rule_value`
- `reason`
- `valid_from`
- `valid_to`
- `is_active`
- `created_at`
- `updated_at`

初期想定:

- 契約外または納品対象外の健診機関を除外する。
- ルールは健保納品ZIP作成時に適用する。
- 除外されたXMLは削除せず、納品メンバーには入れない。

## スクリプト配置

番号付き入口は `scripts/hia` 直下に置く。

```text
scripts/hia/
  01_import_downloaded_xml_zip.py
  02_link_downloaded_xml_to_person_year.py
  03_build_fund_delivery_zip.py
  04_summarize_fund_delivery.py
```

処理本体は `scripts/hia/script_lib` に置く。

```text
scripts/hia/script_lib/
  hia_xml_zip_io.py
  hia_xml_parser.py
  hia_download_repository.py
  hia_person_linker.py
  fund_delivery_selector.py
  fund_delivery_zip_builder.py
  fund_delivery_summary.py
```

設定は `scripts/hia/config` に置く。

```text
scripts/hia/config/fund_delivery.yml
```

## 実行順

### 01_import_downloaded_xml_zip.py

HIAからダウンロードしたZIPを取り込む。

入力:

- `data/hia_export/input_zip`

出力:

- `hia_download_zips`
- `hia_download_xmls`
- archive ZIP

### 02_link_downloaded_xml_to_person_year.py

XML台帳を人×年度へ紐付ける。

出力:

- `hia_person_years`
- `hia_person_xml_events`

### 03_build_fund_delivery_zip.py

健保納品ZIPを作る。

入力:

- `hia_download_xmls`
- `hia_person_years`
- `hia_person_xml_events`
- 設定 `delivery_policy`

出力:

- `data/hia_export/output_to_fund`
- `fund_delivery_runs`
- `fund_delivery_members`

### 04_summarize_fund_delivery.py

納品履歴からサマリーを出す。

サマリー単位:

- 健診機関毎
- 受診月毎
- ZIP単位
- 累計

## delivery_policy

健保納品時の同一人物重複制御は設定で切り替える。

### EXCLUDE_PRIOR

既存互換。

過去に同一人物×年度が納品対象または納品済みの場合、今回ZIP側から除外する。

### PREFER_CURRENT

修正版納品用。

過去に同一人物×年度が存在しても、今回選択したXMLを納品対象にする。

この場合、過去XMLは削除せず、`hia_person_xml_events` 上で `SUPERSEDED` または納品履歴上の旧版として追跡する。

## サマリー方針

健保側には重複除外の内部計算は不要。

正式サマリーは、最終ZIPに含まれるXMLだけを対象にする。

ZIP単位サマリー:

- `delivery_run_id`
- `output_zip_name`
- `insurer_number`
- `facility_code`
- `facility_name`
- `exam_month`
- `delivery_xml_count`
- `delivery_person_count`
- `report_category_10_count`

累計サマリー:

- `insurer_number`
- `facility_code`
- `facility_name`
- `exam_month`
- `cumulative_delivery_xml_count`
- `cumulative_delivery_person_count`
- `last_delivery_run_id`
- `last_output_zip_name`
- `last_delivered_at`

内部確認用には以下をログまたは内部CSVに残す。

- `delivery_policy`
- `excluded_prior_count`
- `excluded_rule_count`
- `deduped_xml_count`

## 旧実装との関係

旧実装は参考実装として残す。

- ZIP名解析
- ZIP展開
- DATA/h*.xml 収集
- ix08 / su08 最小書換え
- XSDコピー
- 既存除外ルール

ただし、新実装では旧 `hia_xml_events` のようにXML台帳と人イベントを兼ねる構造にはしない。

## 未決事項

- `delivery_policy` の初期値。
- `EXCLUDE_PRIOR` で見る「過去」の基準を、納品済みだけにするか、納品対象作成済みも含めるか。
- `PREFER_CURRENT` 時に旧版をどの状態名で残すか。
- 健保へ渡すサマリーCSVの正式列順。
