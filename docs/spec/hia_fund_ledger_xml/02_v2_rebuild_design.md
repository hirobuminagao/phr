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
- `etl_run_id`
- `event_id`
- `insurer_number`
- `facility_code`
- `facility_name`
- `folder_name`
- `zip_name`
- `dl_date`
- `send_seq`
- `zip_sha256`
- `source_zip_path`
- `archive_zip_path`
- `import_status`
- `import_reason`
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
- `etl_run_id`
- `event_id`
- `xml_filename`
- `xml_inner_path`
- `xml_sha256`
- `exam_date`
- `exam_year`
- `exam_month`
- `facility_code`
- `facility_name`
- `report_category_code`
- `program_type_code`
- `insurer_number`
- `insurance_symbol_raw`
- `insurance_number_raw`
- `insurance_symbol_match`
- `insurance_number_match`
- `birthdate`
- `name_kana_raw`
- `name_kana_norm`
- `gender_code`
- `person_id_custom`
- `identity_hash`
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
- `event_id`
- `person_id_custom`
- `identity_hash`
- `name_kana_raw`
- `name_kana_norm`
- `gender_code`
- `exam_year`
- `insurer_number`
- `insurance_symbol_raw`
- `insurance_number_raw`
- `insurance_symbol_match`
- `insurance_number_match`
- `birthdate`
- `report_category_code`
- `program_type_code`
- `first_seen_dl_date`
- `first_seen_download_zip_id`
- `first_seen_hia_download_xml_id`
- `last_seen_dl_date`
- `last_seen_download_zip_id`
- `last_seen_hia_download_xml_id`
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
- `download_zip_id`
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

出力runは出力リストを元に作成する。HIAから受け取ったZIP単位ではなく、原則として受診月単位で納品ZIPを作る。

初期実装では健診機関単位に分割せず、健保向けにまとめて出力する。

- `grouping_mode = ALL`
- 送信元コードは `1322100106`
- 後続で健診機関単位が必要になった場合は `grouping_mode = BY_FACILITY` を追加実装する。

主な項目:

- `delivery_run_id`
- `etl_run_id`
- `delivery_list_id`
- `event_id`
- `insurer_number`
- `output_mode`
- `exam_month`
- `grouping_mode`
- `sender_code`
- `sender_name`
- `delivery_policy`
- `same_exam_date_policy`
- `include_delivery_status`
- `source_dl_date`
- `source_download_zip_id`
- `source_zip_name`
- `output_zip_name`
- `output_zip_path`
- `output_zip_sha256`
- `delivery_status`
- `delivery_xml_count`
- `delivery_person_count`
- `excluded_prior_count`
- `excluded_rule_count`
- `deduped_xml_count`
- `created_at`
- `created_by`
- `note`

### fund_delivery_xml_candidates

健保納品候補になるXML単位の状態。

同一人物・同一年度で未提出候補が複数ある場合でも、原本XMLは消さず、採用・非採用だけをこのテーブルで管理する。

主な項目:

- `delivery_candidate_id`
- `event_id`
- `person_year_id`
- `hia_download_xml_id`
- `person_xml_event_id`
- `exam_date`
- `exam_month`
- `dl_date`
- `send_seq`
- `candidate_status`
- `selection_policy`
- `selection_reason`
- `not_selected_reason`
- `created_at`
- `updated_at`

想定する `candidate_status`:

- `SELECTED`: 出力候補として採用。
- `NOT_SELECTED`: 候補ではあるが、同一人物内の別XMLを採用したため非採用。
- `REVIEW_REQUIRED`: 自動採用できず、人の確認待ち。
- `EXCLUDED`: 納品対象外。

### fund_delivery_person_status

人×年度単位の現在の健保納品状態。

履歴は `fund_delivery_members` に残し、今どう扱うかはこのテーブルで見る。

主な項目:

- `delivery_person_status_id`
- `event_id`
- `person_year_id`
- `current_hia_download_xml_id`
- `current_delivery_candidate_id`
- `delivery_tracking_status`
- `tracking_reason`
- `last_delivery_run_id`
- `last_delivery_member_id`
- `last_delivered_at`
- `last_delivered_by`
- `redelivery_reason`
- `created_at`
- `updated_at`

想定する `delivery_tracking_status`:

- `NOT_DELIVERED`: 未提出。
- `DELIVERED`: 提出済み。
- `REDELIVERY_NEEDED`: 修正版などで再提出したい。
- `EXCLUDED`: 提出対象外。
- `REVIEW_REQUIRED`: 人の確認待ち。

### fund_delivery_lists

健保納品ZIPを作るための出力リスト。

画面では、候補を検索し、対象者をこのリストへ追加してから出力する。

リスト作成時点で出力単位と送信元を固定し、出力runへ写す。

初期値:

- `grouping_mode = ALL`
- `sender_code = 1322100106`

主な項目:

- `delivery_list_id`
- `event_id`
- `insurer_number`
- `list_name`
- `list_status`
- `output_mode`
- `exam_month`
- `grouping_mode`
- `sender_code`
- `sender_name`
- `delivery_policy`
- `same_exam_date_policy`
- `include_delivery_status`
- `search_condition_note`
- `submitted_at`
- `submitted_by`
- `submission_note`
- `created_by`
- `created_at`
- `updated_at`

想定する `output_mode`:

- `EXAM_MONTH`: 受診月単位で出力する通常モード。
- `ALL`: 条件対象をまとめて出力する確認・再作成モード。

### fund_delivery_list_members

出力リストに追加された人単位の明細。

主な項目:

- `delivery_list_member_id`
- `delivery_list_id`
- `person_year_id`
- `delivery_candidate_id`
- `hia_download_xml_id`
- `list_member_status`
- `list_member_reason`
- `added_by`
- `created_at`
- `updated_at`

### fund_delivery_members

納品ZIPに入れた個人XML単位の履歴。

主な項目:

- `delivery_member_id`
- `delivery_run_id`
- `person_year_id`
- `hia_download_xml_id`
- `delivery_candidate_id`
- `person_xml_event_id`
- `xml_filename`
- `xml_sha256`
- `facility_code`
- `facility_name`
- `exam_date`
- `exam_month`
- `report_category_code`
- `program_type_code`
- `member_status`
- `member_reason`
- `submitted_at`
- `submitted_by`
- `submission_note`
- `created_at`

累計サマリーはこのテーブルから集計する。

### 提出済み記帳

健保への提出完了は、人が出力リスト単位で記帳できるようにする。

画面では、出力リスト詳細から「このリストを提出済みにする」を実行する。

一括提出済み時の反映先:

- `fund_delivery_lists`
  - `list_status = SUBMITTED`
  - `submitted_at`
  - `submitted_by`
  - `submission_note`
- `fund_delivery_members`
  - `member_status = SUBMITTED`
  - `submitted_at`
  - `submitted_by`
  - `submission_note`
- `fund_delivery_person_status`
  - `delivery_tracking_status = DELIVERED`
  - `last_delivery_run_id`
  - `last_delivery_member_id`
  - `last_delivered_at`
  - `last_delivered_by`

提出済み記帳は履歴を消さずに状態を進める操作とする。

誤操作時の戻しは別操作として用意し、直接UPDATE前提にはしない。

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
  02_create_fund_delivery_list.py
  03_export_fund_delivery_zip.py
  04_mark_fund_delivery_submitted.py
```

処理本体は `scripts/hia/script_lib` に置く。

```text
scripts/hia/script_lib/
  hia_download_importer.py
  hia_download_xml_parser.py
  fund_delivery_list_builder.py
  fund_delivery_zip_exporter.py
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

初期実装方針:

- `scripts/hia/01_import_downloaded_xml_zip.py` を入口にする。
- 既定入力は `data/hia_export/input_zip/{insurer_number}/*.zip`。
- 既定アーカイブは `copy` とし、入力ZIPを即時移動しない。
- `--archive-mode move` 指定時のみ旧実装に近い移動運用にする。
- `--dry-run` ではDB更新もアーカイブ作成も残さない。
- ZIPの同一性は `insurer_number + zip_name` を基本にする。
- `zip_sha256` は検索・確認用であり、履歴台帳上の一意制約にはしない。

### 02_create_fund_delivery_list.py

健保納品ZIPの出力リストを作る。

入力:

- `hia_download_xmls`
- `hia_person_years`
- `hia_person_xml_events`
- `fund_delivery_xml_candidates`
- `fund_delivery_person_status`

出力:

- `fund_delivery_xml_candidates`
- `fund_delivery_person_status`
- `fund_delivery_lists`
- `fund_delivery_list_members`

画面化後は、検索条件入力、候補確認、個別追加・削除をこの手順で扱う。

CLI初期実装では、受診月・健保・提出状態を条件にリストを作成できるようにする。

### 03_export_fund_delivery_zip.py

健保納品ZIPを作る。

入力:

- `fund_delivery_lists`
- `fund_delivery_list_members`
- `fund_delivery_xml_candidates`
- `fund_delivery_person_status`
- `hia_download_xmls`

出力:

- `data/fund_delivery/output/{yyyymmdd_hhmmss}/{exam_month}`
- `fund_delivery_runs`
- `fund_delivery_members`

方針:

- `fund_delivery_lists` / `fund_delivery_list_members` に固定された対象だけを出す。
- 出力時に候補の再選定はしない。
- 初期実装は `grouping_mode = ALL` のみ対応する。
- 元ZIP内の個人XMLを読み、出力ZIP内では厚労省の送付用ファイルアーカイブ仕様に合わせて `DATA/h{送信元コード}{提出日}{同日作成回数}{実施区分コード}{連番6桁}.xml` へ連番化する。
- 実施区分コードは健診結果のため `1` とする。
- `ix08_V08.xml` / `su08_V08.xml` は、元ZIPの原文が使える場合は件数のみ最小置換する。
- 元ZIP側に件数タグがない場合は、XSDに合う最小XMLを生成する。
- `XSD` は `scripts/from_medical/source/XSD/mhlw_v4_20230331_v08` を同梱する。
- `fund_delivery_runs.delivery_status` と `fund_delivery_members.member_status` は `CREATED` とし、健保提出済みへの更新は `04` で行う。

### 04_mark_fund_delivery_submitted.py

健保への提出完了を反映する。

入力:

- `fund_delivery_lists`
- `fund_delivery_runs`
- `fund_delivery_members`
- `fund_delivery_person_status`

出力:

- `fund_delivery_lists.submitted_*`
- `fund_delivery_members.submitted_*`
- `fund_delivery_person_status.last_delivered_*`

## サマリー

納品履歴からサマリーを出す。

サマリー単位:

- 健診機関毎
- 受診月毎
- ZIP単位
- 累計

## output_mode

出力リスト作成時に選ぶ。

### EXAM_MONTH

受診月単位で出力する通常モード。

### ALL

条件対象をまとめて出力する確認・再作成モード。

## delivery_policy

出力リスト作成時に選ぶ。

### NOT_DELIVERED_ONLY

未提出の人だけを出力対象にする。

### REDELIVERY_ONLY

再提出対象だけを出力対象にする。

### NOT_DELIVERED_AND_REDELIVERY

未提出と再提出対象を出力対象にする。

### ALL

提出済みも含めて条件対象を出力する。

確認用・再作成用。

## same_exam_date_policy

同一人物・同一年度・同一受診日のXML候補が複数ある場合の採用方針。

画面化後は出力リスト作成条件として選択できるようにする。

### LATEST_DOWNLOAD

`dl_date + send_seq` が新しい方を採用する。

### EARLIEST_DOWNLOAD

`dl_date + send_seq` が古い方を採用する。

### MANUAL_REVIEW

自動採用せず、確認待ちにする。

## 旧 delivery_policy との関係

旧実装の `EXCLUDE_PRIOR` / `PREFER_CURRENT` は、ZIP単位の塊制御としては採用しない。

v2では、受診月ベースの出力リスト、人単位の `delivery_tracking_status`、XML候補単位の `candidate_status` で表現する。

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
