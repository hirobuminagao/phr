# CSV Exam Result Import Pre-Implementation Review

## Status

Historical pre-implementation baseline.

この文書は実装着手時点の判断材料を保存するものであり、2026-07-29時点の現在正ではない。
決定事項は `03_decisions.md`、実装到達点・残課題・CSVからXML作成への引継ぎは `33_implementation_status_and_xml_handoff.md` を参照する。

このドキュメントは、`02_02_exam_result_csv_import` 実装前に、当時の決定事項、実装対象、未決事項、レビュー観点を1枚に集約するために作成したものである。
詳細な履歴は `05_design_history.md`、採用済み決定は `03_decisions.md`、DDL案は `10_phr_master_initial_ddl_draft.md`、処理詳細は `11_csv_import_processing_design_draft.md` を参照する。

## Implementation Goal

初期実装の目的は、CSV健診結果をまず取り込み、エラー・不足・warningを明確に記録し、後続確認へ渡せる状態を作ることである。
事前停止は最小限にし、停止候補は誤登録リスクが高いケースへ絞る。

止める候補:

- 必要なmapping列を安全に解決できない。
- 列番号指定ruleがあり、ヘッダー不一致や列ズレにより誤った値を読む可能性が高い。
- CSVとして読めない、文字コード・delimiterが解決できない、ファイル破損などで行単位処理へ進めない。

止めない候補:

- 健診日など基本情報が不足している。
- 加入者突合が `NOT_FOUND` / `CANDIDATE` になる。
- 一部検査値のnormalizeや変換が失敗する。
- `namecode` と実値の意味・型が合わず、健診機関へのフォーマット確認が必要になる。

止めない場合も、`file_receipts`, `csv_row_ledger`, `exam_item_values`, `etl_errors` へ状態と理由を残す。

## Confirmed Scope

初期実装で対象にする。

- `phr_master` 初期DDL案の実装準備
  - `exam_facilities`
  - `medical_folder_aliases`
  - `exam_item_concept_groups`
  - `exam_item_concept_group_members`
  - `csv_format_versions`
  - `csv_exam_result_mapping_rules`
  - `csv_exam_result_mapping_conditions`
  - `norm_variants`
- `health_exam_result` 側の追加準備
  - `file_receipts` CSV関連カラム追加
  - `csv_row_ledger`
  - `exam_item_values.source_reference_lower`
  - `exam_item_values.source_reference_upper`
- 共通lib追加・拡張
  - 健診機関lookup
  - `norm_variants` lookup
  - 健診結果値normalize共通lib
  - CSV構造化読込API追加
- CSV取込処理
  - `file_receipts` 起点
  - format選択
  - header fingerprint計算
  - mapping rule評価
  - CSV行単位ledger作成
  - XML import と同じ加入者突合手順
  - `exam_item_values` 登録
  - normalize実行

## Out Of Initial Scope

初期実装では実装しない、または後続タスクに送る。

- FastAPIによるテンプレート登録管理API。
- 本番用テンプレート登録画面。
- 全検査項目の医学分類を網羅する手作業グループ設計。
- 旧 `dev_phr.norm_variants` の廃止判断。廃止タイミングは今回決めず、CSV取込実装後に別途判断する。
- `exam_item_reference_ranges` など、マスタ基準範囲・判定ルールの正式設計。
- `check_result` 側の不足情報追加改修。
- ヒロオカクリニック実CSVの機微情報を含む中身確認。

## Table Decisions

### phr_master.exam_facilities

健診機関そのものを表す親マスタ。
医療機関とは分け、CSV format/mapping選択の起点にする。

主な項目:

- `exam_facility_id`
- `exam_facility_code`
- `exam_facility_name`
- `exam_facility_display_name`
- `exam_facility_type`
- `medical_institution_code`
- `reservation_system_medical_institution_code`
- `postal_code`
- `address`
- `phone_number`
- `website_url`
- `management_entity`
- `data_source_name`
- `data_source_file_name`
- `data_source_file_sha256`
- `data_source_note`
- `note`
- `is_active`

支払基金CSVとの突合結果:

- ヒロオカクリニック、ハートクロス健診プラザ赤坂駅前は支払基金CSVに存在する。
- 既存 `medical_folder_aliases` seed 188件中、179件はalias先頭10桁と支払基金CSV `機関コード` が一致する。
- 8件はalias先頭10桁では支払基金CSVの `機関コード` と一致しないが、7件は過去CSV/XML確認により採用コードを確定済み。
- 浦和医師会 健診センターのみ保留し、データ受領時にCSV/XML内の健診機関番号で紐付ける。
- `202604開院_...` は旧仮フォルダ名であり、正式採番済みフォルダがあるため `exam_facilities` 対象外とする。
- 詳細は `32_exam_facility_master_data_check.md` を参照する。

番号ギャップの扱い:

- 受領フォルダ先頭10桁は医療機関番号候補として扱う。
- 全国CSVで見つからない番号は、地方厚生局・都道府県単位のオープンデータや別年度/別区分の公開データに存在する可能性がある。
- 初期実装では地方局データ探索まで広げず、支払基金CSV、過去CSV/XML実績、受領データ内番号で確認できる範囲だけを採用する。
- 契約・請求側と接続する段階で、医療機関番号を正規管理する `medical_institutions` 相当のマスタを後続追加し、`exam_facilities` と紐づける。
- この後続論点は事業所単位ではなく、健保、代行機関、医療施設の連携関係として扱う。
- 後続設計では、一階層上の医療施設/医療機関マスタを追加する案と、`exam_facilities` に連携カラムを追加する案を比較する。

決定済み:

- `exam_facility_name` は支払基金CSVの正式名とする。
- `exam_facility_display_name` はalias側の短い名前とする。
- 支払基金CSVから初期投入した行には、公開CSV由来であることを示すsource情報を保持し、社内作業データや機微情報ではないことをDB上で説明できるようにする。
- 浦和医師会 健診センターは `exam_facility_id = NULL` の確認対象として扱う。

### phr_master.medical_folder_aliases

既存テーブル名を維持し、`phr_master` 側へ移す。
`event_id + src_folder_raw` の一意性を維持し、`exam_facility_id` を追加する。

`01_scan_files.py` はこのaliasから健診機関を解決し、`file_receipts.exam_facility_id` へ渡す。

### phr_master.csv_format_versions

CSV構造の親設定。
健診機関単位ではなく、健診機関内のmapping version単位でCSV構造を持つ。
同一施設・同一header shaに複数の有効formatが存在する場合に備え、施設内default指定を持つ。

主な項目:

- `exam_facility_id`
- `mapping_version`
- `file_type`
- `format_name`
- `header_mode`
- `header_structure_type`
- `header_context_rule`
- `data_start_row_no`
- `header_sha256`
- `header_snapshot_json`
- `header_hash_status`
- `header_mismatch_policy`
- `allow_column_no_rules`
- `duplicate_row_policy`
- `missing_basic_info_policy`
- `character_encoding`
- `encoding_fallback_policy`
- `delimiter`
- `quote_char`

文字コードは `character_encoding` を第一候補とし、`encoding_fallback_policy = ALLOW_COMMON_ENCODINGS` の場合だけ UTF-8 BOM / UTF-8 / CP932を追加候補にする。候補で読めても `header_sha256` が登録値と一致しなければ停止する。採用文字コードは `file_receipts.actual_character_encoding` に記録する。delimiterは登録値固定、quoteはparser設定として利用し、引用符の有無だけでは停止しない。
- `valid_from`
- `valid_to`
- `is_active`

初期値方針:

- `header_mismatch_policy = ALLOW_AFTER_CONFIRM`
- `allow_column_no_rules = 0`
- `duplicate_row_policy = SKIP_CHECKED_OK`
- `missing_basic_info_policy = IMPORT_AND_CHECK_LATER`
- `mapping_version` は人が識別しやすい名称とし、施設略称、対象年月、パターン、版を含める。
  - 例: `HIROOKA_2026_05_PATTERN_A_V1`
- 健診機関番号は `exam_facilities.medical_institution_code` / `exam_facility_code` に持つため、`mapping_version` へ必須では入れない。
- `valid_from` は明示指定を基本とし、未指定なら登録日を適用開始日として扱う。
- `valid_to` はNULLを無期限として扱う。

血糖の `区分列で分岐` / `空腹時・随時別列` などはformat本体の永続カラムにはしない。
seed/FastAPI入力支援の補助設定として扱い、最終的にはrule/conditionに展開する。

### phr_master.csv_exam_result_mapping_rules

CSV format内の1つの抽出・登録ruleを表す親テーブル。
基本情報も検査結果値も同じrule形式で扱う。

主な項目:

- `csv_format_version_id`
- `target_kind`
- `target_resolution_type`
- `selection_mode`
- `selection_group_code`
- `target_namecode`
- `target_identity_item_code`
- `target_field`
- `method_structure_type`
- `raw_value_type`
- `raw_unit`
- `is_required`
- `priority`
- `is_active`

`selection_group_code` は `EXCLUSIVE_ONE` の排他対象範囲を表す。
例: `GLUCOSE_TIMING`, `BP_MEASURE_SITE`。

`target_field` は `LEDGER_FIELD` の場合だけ登録先 `csv_row_ledger` カラムを表す。
`EXAM_ITEM_VALUE` の場合は原則NULLとし、値・下限・上限・判定の区別は `csv_exam_result_mapping_conditions.source_role` で表す。

### phr_master.csv_exam_result_mapping_conditions

親ruleにぶら下がるCSV上の値取得・条件判定を表す。

主な項目:

- `csv_exam_result_mapping_rule_id`
- `condition_group_no`
- `condition_type`
- `locator_type`
- `header_context`
- `header_name`
- `header_occurrence`
- `column_no`
- `operator`
- `expected_value`
- `expected_value_normalized`
- `source_role`
- `priority`
- `is_active`

`condition_group_no` はOR条件の単位。
同じgroup内の複数conditionはAND評価する。

`source_role` の初期候補:

- `VALUE`
- `LOWER_LIMIT`
- `UPPER_LIMIT`
- `JUDGEMENT`
- `METHOD`
- `QUALIFIER`

### phr_master.exam_item_concept_groups / members

CSVテンプレート登録で候補 `namecode` を探しやすくするための入力支援レイヤー。

採用方針:

- `ANNEX2_IDENTITY` 197件を物理seedとして保持する。
- `INPUT_BUNDLE` で血糖、脂質、腎機能などの入力支援bundleを保持する。
- 親bundleと子bundleの階層を許容する。
- bundleは保存先や排他性を決めない。
- 実際の保存先はmapping ruleの `target_namecode` で明示する。

採用済みbundle例:

- `GLUCOSE_RELATED`
  - `GLUCOSE`
  - `HBA1C`
- `LIPID_RELATED`
  - `TRIGLYCERIDE`
  - `HDL_CHOLESTEROL`
  - `LDL_CHOLESTEROL`
  - `NON_HDL_CHOLESTEROL`
  - `TOTAL_CHOLESTEROL`
- `RENAL_RELATED`
  - `CREATININE`
  - `EGFR`
  - `URIC_ACID`
  - `URINE_ALBUMIN`

`CREATININE` は `3C015`、`EGFR` は `8A065` として分ける。

### health_exam_result.file_receipts

CSVファイル単位の受領・処理状態を持つ。
人/行単位の基本情報台帳としては使わない。

追加候補:

- `exam_facility_id`
- `actual_header_sha256`
- `actual_character_encoding`
- `matched_csv_format_version_id`
  - scan時または `01_01_match_csv_format.py` の再照合で設定する。
- `import_resume_approved`
- `import_resume_approved_at`
- `import_resume_approved_by`
- `import_resume_approved_reason`
- `import_resume_scope`

既存XML側と同じく、`file_receipts.status` はファイル単位の現在状態として使う。
`summary_message` にはCSV取込結果の要約、停止理由、確認待ち理由を入れる。
加入者突合と健診結果値処理の状態は、XML側の `xml_ledger` に相当する `csv_row_ledger` 側へ持たせる。

### health_exam_result.csv_row_ledger

CSVデータ行単位、つまり1人分のCSV由来台帳。
`xml_ledger` と対になるCSV行台帳として扱う。

主な責務:

- CSV行証跡を保持する。
- 基本情報抽出結果を保持する。
- XML import と同じ加入者突合結果を保持する。
- `exam_item_values.ledger_type = CSV`, `ledger_id = csv_row_ledger.csv_row_ledger_id` の参照先になる。
- check/export状態を将来連携できるように持つ。

`raw_row_json` はCSVパース後の1行分snapshotであり、元ファイル全体の保存ではない。

### health_exam_result.exam_item_values

CSV由来もXML由来と同じ縦持ち結果値テーブルへ登録する。

CSV固有追加候補:

- `source_reference_lower`
- `source_reference_upper`

ここで扱うCSV由来の判定は、法定項目の必須/不足チェックや `check_result` の評価ではなく、健診機関がCSVに出してきた検査別判定・カテゴリ総合判定を指す。
`未実施` / `測定不能` / `判定不能` など、entry内の項目結果値として出てくる実施状態・測定可否は、この健診機関由来の健診判定とは別に扱う。
健診機関由来の健診判定は、健診機関ごとの基準、契約、事業所向け要件で意味が変わる可能性が高い。
初期実装ではPHR側の判定ロジックや納品判定には利用せず、XML由来の `interpretationCode` と同じカラムへ寄せない。
原本証跡として、最低限 `csv_row_ledger.raw_row_json` から復元できる状態にする。
必要になった場合は、`exam_item_values.source_judgement_raw` などの専用カラム、または健診機関別判定マスタを後続バージョンで検討する。

## Processing Decisions

### Run Selection

CSV取込Runは毎回同じ入口にする。
専用の再取込モードは作らない。

通常Runで拾う対象:

1. scanまたはformat再照合で `READY` になったCSV。
2. 過去に停止したが `file_receipts.import_resume_approved` が立っているCSV。
3. `csv_row_ledger` までは作成済みだが、加入者突合や検査値登録などの後続処理が未完了のCSV。

`DISCOVERED` はformat照合前または取込準備未完了の状態として扱い、CSV取込Runの通常対象にはしない。

### Row-Oriented Processing

初期実装は1行ずつ処理する。

1. CSV行を読む。
2. `row_sha256` を算出する。
3. 完全空行はskipする。
4. 基本情報ruleを評価して `csv_row_ledger` payloadを作る。
5. XML import と同じ手順で加入者突合を行う。
6. 検査値ruleを評価して `exam_item_values` payloadを作る。
7. raw値をnormalize共通libへ渡す。
8. 再処理対象行の場合は、既存のCSV由来 `exam_item_values` をdeleteする。
9. `exam_item_values` をinsertする。
10. `csv_row_ledger` と `exam_item_values` を1行単位でcommitする。

行処理中に失敗した場合は、その行の変更をrollbackし、行単位errorとして記録する。
`exam_item_values` は既存一意制約を追加せず、`ledger_type = 'CSV'` かつ `ledger_id = csv_row_ledger_id` 単位のdelete+insertで再処理する。

### Subscriber Matching

CSV行単位の加入者突合はXML importと同じ流れに揃える。

1. CSVから基本情報を抽出する。
2. `generate_identity_bundle()` を呼ぶ。
3. `resolve_subscriber_identity()` を呼ぶ。
4. 結果を `csv_row_ledger` に保存する。

保存項目:

- `subscriber_id`
- `hia_subscriber_id`
- `identity_hash`
- `person_id_custom`
- `subscriber_match_status`
- `subscriber_match_method`
- `subscriber_match_reason`

加入者突合結果は `csv_row_ledger.subscriber_match_*` に保持する。
`file_receipts` には加入者突合の集約カラムを追加しない。

### Exam Item Value Creation

`exam_item_values` を作る条件:

- `source_role = VALUE` のraw値が完全空セルではない。

作らない条件:

- `VALUE` が完全空セル。
- 下限・上限・判定だけが存在する。
- 完全空行。

下限・上限・判定は、同じruleから作成される `exam_item_values` に付随するCSV由来項目として扱う。
`未実施` / `キャンセル` / `測定不能` などの非測定値語は完全空ではないため、`exam_item_values.raw_value` に原文を残してnormalize結果のreasonで分類する。
これらはentry内の項目結果値として出てくる実施状態・測定可否であり、健診機関由来のABC等の健診判定とは別に扱う。

### Normalize

CSV取込時にnormalizeまで行う。

基本方針:

- `namecode` とraw値を入力する。
- `exam_item_master` lookupから型・単位を取得する。
- CD/CO系は `phr_master.norm_variants` を参照する。
- `未実施`, `未受診`, `キャンセル`, `測定不能` などの非測定値語は、CD/CO名寄せとは別の共通前処理で扱う。
- 非測定値語辞書は初期実装ではYAMLファイルとして管理する。
- 実施されていない語は `RAW_VALUE_NO_RESULT`、測定できなかった語は `RAW_VALUE_UNMEASURABLE`、型に合わない未知文字列は `INVALID_VALUE_TYPE` として分類する。
- CD/CO系で辞書一致OKになった場合は、`RAW_VALUE_EXACT_MATCH` / `RAW_VALUE_NORMALIZED_MATCH` により、原文一致か前処理後一致かを区別する。
- `あり` / `なし` は初期の共通ノイズ辞書に含めない。
- `異常なし` / `所見なし` も項目によって結果値として意味を持つため、共通ノイズ辞書には含めない。
- 数値系 `data_type` は `PQ`, `INT`, `REAL` とする。
- `raw_unit` と `item_master.unit` が違う場合、初期実装では単位変換せずエラーにする。
- CSV値の機械的な前処理は共通lib側へ寄せ、項目別明示変換は初期実装では扱わない。
- `norm_variant` lookupは単品APIと一括APIの両方を持ち、CSV取込では一括APIを使う。

### CSV Format Confirmation Items

実装とは別に健診機関へ確認する事項。

- CSVに `namecode` が入っていても、その `namecode` の型・コード体系と実値が一致しない場合は、システム側で別項目へ推測振替しない。
- ハートクロスサンプルでは、`内科診察所見1` / `9N066000000000011` に `心雑音 要受診` が入っている。これはCD値として辞書追加する対象ではなく、自由記載または医師判断相当の値がCD項目へ入っている可能性として健診機関へ確認する。
- この確認が残っていてもCSV取込実装は進める。取込側はnormalizeエラーと原本証跡を残し、業務確認へ渡す。

### Header Fingerprint

実CSVからheader fingerprintを算出し、`csv_format_versions.header_sha256` と照合する。
scan時点で照合できる場合は `file_receipts.matched_csv_format_version_id` に保存し、import時はそのIDを優先する。

基本方針:

- 初期は `1 mapping_version = 1 header fingerprint = 1 mapping set` とする。
- ヘッダー名の表記ゆれは自動吸収しない。
- ヘッダー名や列構造が違うCSVは、同じ施設でも別 `mapping_version` として明示登録する。
- ルールやマッピングは完全自動生成しない。
- 健診機関・mapping versionごとの初回テンプレートは、人がCSV実物を確認して手動登録する。
- `CARRY_FORWARD_ITEM` は自動推測ではなく、手動登録済みの `header_snapshot_json.normalized_columns` に従って持ち回りcontext形式を再現する方式として扱う。
- 将来複数ヘッダーを扱う場合も、取り込み時の自動推測ではなく、人が確認済みのheader variantとして扱う。
- 一致した場合、登録済みヘッダー内の未マッピング列は意図的な非取込列として扱う。
- 不一致の場合、初期実装では人の確認なしに自動続行しない。
- 続行する場合は、format側が確認後Goを許可し、かつ `file_receipts` 側に人が確認済みでGoした証跡がある場合に限る。
- 必要列が解決できない場合は停止候補。
- 列番号指定ruleがある場合は停止候補。

## Seed Decisions

初期seed対象:

- `exam_facilities`
  - 支払基金CSVを元に作成する。
  - 初期検証では既存aliasに紐づく施設を優先する。
  - ヒロオカクリニック、ハートクロス健診プラザ赤坂駅前は検証対象施設として含める。
- 既存alias seed
  - `event_id = 2`
  - `src_folder_raw = 1310438796_ヒロオカクリニック`
  - `dst_folder_norm = 1310438796_ヒロオカクリニック`
- `ANNEX2_IDENTITY` 197件
- 入力支援bundle
- ヒロオカCSV用 `csv_format_versions`
- ヒロオカCSV用 `csv_exam_result_mapping_rules`
- ヒロオカCSV用 `csv_exam_result_mapping_conditions`
- ハートクロスCSV用 `csv_format_versions`
- ハートクロスCSV用 `csv_exam_result_mapping_rules`
- ハートクロスCSV用 `csv_exam_result_mapping_conditions`
- `norm_variants` 初期追加候補

CSV format / mapping seed のレビュー用SQLは `sql/seed/phr_master/0001_draft_csv_exam_result_format_mappings_samples.sql` に作成する。
このSQLはドラフトであり、実投入前に健診機関seedを先に作成し、`medical_institution_code` から `exam_facility_id` を解決できる状態にする。
`header_snapshot_json` は実CSVから生成した完全な列配列で確定する。

## Implementation Order

推奨順序:

1. DDL/migration案の最終レビュー。
2. 共通lookup lib追加。
3. CSV loader追加API。
4. normalize共通lib。
5. seed生成・投入手順。
6. `02_02_exam_result_csv_import` のdry-run骨格。
7. `csv_row_ledger` 作成。
8. 加入者突合。
9. 検査値rule評価。
10. `exam_item_values` 登録とnormalize。
11. file/row/runの状態集約。
12. ヒロオカCSVで限定検証。
13. FastAPIテンプレート登録API検討。

## Remaining Before Implementation

実装前に残っている作業は以下である。

### 1. Mock To Table Mapping

画面モックの入力要素をDBへ写す対応は以下で固定する。

| モック上の要素 | 保存先 | 備考 |
|---|---|---|
| 健診機関 | `csv_format_versions.exam_facility_id` | `file_receipts.exam_facility_id` から選択 |
| mapping version | `csv_format_versions.mapping_version` | 施設内で一意 |
| ヘッダー設定 | `csv_format_versions.header_mode` | `NONE` / `SINGLE` / `WITH_CONTEXT` |
| contextあり設定 | `csv_format_versions.header_structure_type`, `header_context_rule` | 2行ヘッダー、持ち回りcontext |
| データ開始行 | `csv_format_versions.data_start_row_no` | 1始まり |
| 投入先 | `csv_exam_result_mapping_rules.target_kind` | `LEDGER_FIELD` / `EXAM_ITEM_VALUE` |
| 投入先namecode | `csv_exam_result_mapping_rules.target_namecode` | 検査値の場合 |
| 同一性項目 | `csv_exam_result_mapping_rules.target_identity_item_code` | 候補探索・bundle表示に使う |
| 単一/複数/どれか | `selection_mode`, `selection_group_code` | `DIRECT` / `EXCLUSIVE_ONE` / `MULTI_ENTRY` |
| 値/下限/上限/判定 | `csv_exam_result_mapping_conditions.source_role` | `VALUE` / `LOWER_LIMIT` / `UPPER_LIMIT` / `JUDGEMENT` |
| ヘッダー名/列番号/両方 | `locator_type`, `header_*`, `column_no` | 原則ヘッダー名優先 |
| 条件 | `condition_type`, `operator`, `expected_value` | 方式列=1など |
| 複数条件 | `condition_group_no` | 同一group内AND、複数groupはOR |

rule/conditionの重複validateはDB制約ではなく、seed生成時および将来のFastAPI登録時に行う。
同一rule key、同一rule内の同一 `source_role + locator`、同一rule内の複数 `VALUE`、`EXCLUSIVE_ONE` の同priority衝突などはテンプレート登録エラーとして扱う。

### 2. etl_runs Based State Model

`csv_import_batches` は採用しない。
以下の役割で実装する。

| 単位 | テーブル | 役割 |
|---|---|---|
| 実行 | `etl_runs` | dry-run、通常取込、再実行の履歴 |
| エラー | `etl_errors` | run/file/row/item単位の証跡 |
| ファイル | `file_receipts` | CSVファイルの現在状態、format照合、確認Go |
| 行/1人分 | `csv_row_ledger` | 基本情報、加入者突合、検査値処理、check/export状態 |
| 結果値 | `exam_item_values` | `ledger_type = CSV`, `ledger_id = csv_row_ledger_id` |

決定済み:

- `etl_runs.phase` は既存の実行種別文字列であり、XML側では `IMPORT_XML`、scanでは `SCAN_FILES`、checkでは `CHECK_EXAM_RESULTS` を使っている。
- CSV取込では `IMPORT_CSV_EXAM_RESULTS` を使用する。
- `file_receipts.etl_run_id` は既存XML側と同じくscan時runの参照として扱い、CSV取込runで上書きしない。
- CSV取込runは `csv_row_ledger.etl_run_id` と `etl_errors.run_id` に残す。

後続検討:

- `file_receipts` からCSV取込runを直接引きたい要件が出る場合のみ、`csv_import_etl_run_id` 追加を検討する。

### XML/ZIP Password Impact

既存XML取込への影響は限定的である。

- XML基本情報の施設コード・施設名はXML本文から抽出し、`xml_ledger.facility_code` / `facility_name` へ保存する。
- `exam_item_values` のXML登録は明示カラム指定であり、CSV用の `source_reference_lower` / `source_reference_upper` nullable追加の影響を受けない。
- ただし暗号ZIPのパスワード解決では、`file_receipts.facility_code` / `submitter_facility_code` / 受領フォルダ名を `work_other.medi_zip_passwords` の候補として使う。

このため、scan時に `file_receipts.facility_code` へ健診機関マスタ由来コードを入れる変更は、ZIPパスワード解決の影響範囲に含める。

初期方針:

- `facility_folder_name` 一致を既存互換の主要な逃げ道として維持する。
- `facility_code` 一致は既存通り候補に残す。
- 旧フォルダコードと支払基金由来コードが異なる施設では、必要に応じて `medi_zip_passwords` 側に新コードまたはフォルダ名を登録して解決する。
- `exam_facility_id` によるパスワード解決は初期実装では追加しない。必要になった場合に、`medi_zip_passwords` のmaster移設または拡張と合わせて検討する。

### 3. Status Codes

初期実装では既存XML側の状態表現に寄せる。

- `file_receipts.status`
  - `DISCOVERED`
  - `IMPORTING`
  - `IMPORTED`
  - `WARNING`
  - `ERROR`
  - `WAITING_CONFIRM`
  - `WAITING_PASSWORD` はXML/ZIP用で、CSVでは通常使わない。
- `file_receipts.summary_message`
  - header不一致、format未設定、読込不能などの理由。
- `csv_row_ledger.row_status`
  - 1行の取込状態。
- `csv_row_ledger.exam_item_status`
  - 検査値作成・normalizeの状態。
- `subscriber_match_status`
  - XML import と同じ `MATCHED` / `CANDIDATE` / `NOT_FOUND` / `IDENTITY_ERROR` / `NOT_EXECUTED`。

CSV固有で既存XML側だけでは表現できないもの:

- 実CSVヘッダーhash: `actual_header_sha256`
- 採用されたCSV format: `matched_csv_format_version_id`
- header不一致など確認後Goの証跡: `import_resume_approved` / `import_resume_*`
- CSV行の証跡: `csv_row_ledger.raw_row_json`
- CSV行hash: `csv_row_ledger.row_sha256`

決定済み:

- `WAITING_CONFIRM` はCSVの確認待ち状態として `file_receipts.status` に追加する。
- 停止後Go項目は `import_resume_approved` / `import_resume_approved_at` / `import_resume_approved_by` / `import_resume_approved_reason` / `import_resume_scope` とする。
- `row_sha256` は、CSV loaderで文字コード変換後のセル配列を列順込みでJSON正規化し、SHA-256化する。
- 同一 `row_sha256` でも、列順が変わったCSVは別hashになる。
- check済みOKの既存行はskipし、未完了、WARNING、ERROR、check未実行は再処理対象とする。
- `etl_errors` に rule_id / condition_id / namecode 専用カラムは初期追加しない。

### 4. First Seed Shape

初期CSV format seedは、実CSV確認後に以下を表現できる範囲で作成する。

- 基本情報rule。
- 固定namecodeの単純値rule。
- 値/下限/上限/判定を同一namecodeへ付けるrule。
- `EXCLUSIVE_ONE` による空腹時/随時分岐。
- `MULTI_ENTRY` による空腹時列・随時列の別列パターン。

現時点のドラフトseedでは、ヒロオカ Pattern A とハートクロス Pattern B の基本情報rule、固定namecodeの単純値ruleを先に作成する。
空腹時/随時の補助列分岐や下限/上限/判定付きruleは、該当サンプルまたは実値確認後に追加する。

## Rule Codes

初期実装では、モックを再現できる最小セットとして以下を採用する。

### csv_exam_result_mapping_rules.target_kind

- `LEDGER_FIELD`
  - `csv_row_ledger` の基本情報へ入れる。
- `EXAM_ITEM_VALUE`
  - `exam_item_values` へ入れる。

### target_resolution_type

- `SINGLE_NAMECODE`
  - 投入先 `target_namecode` を固定する。
- `IDENTITY_ITEM_CANDIDATES`
  - `target_identity_item_code` を候補表示・絞り込みの起点にする。

### selection_mode

- `DIRECT`
  - 固定投入。単純な基本情報、固定namecode。
- `EXCLUSIVE_ONE`
  - 同じ `selection_group_code` 内で条件成立した1つだけを採用する。
  - 例: 同じ血糖値列を、区分列により空腹時/随時のどれか1つへ振り分ける。
- `MULTI_ENTRY`
  - 成立したruleを複数entryとして作る。
  - 例: 空腹時血糖列と随時血糖列が別々に存在する。

### method_structure_type

- `SINGLE_COLUMN`
  - 方式/補助条件が1列で表現される。
- `MULTI_COLUMN`
  - 方式/補助条件が複数列で表現される。

### csv_exam_result_mapping_conditions.source_role

- `VALUE`
  - `exam_item_values.raw_value` の取得元。
- `LOWER_LIMIT`
  - CSV由来の基準下限。
- `UPPER_LIMIT`
  - CSV由来の基準上限。
- `JUDGEMENT`
  - CSV由来の判定。
- `METHOD`
  - 方式判定に使う列。
- `QUALIFIER`
  - 補助条件に使う列。

### condition_type

- `HEADER_MATCH`
  - 指定列から値を取得する。
- `METHOD_MATCH`
  - 方式列や補助列が期待値に一致する場合に採用する。
- `VALUE_PRESENT`
  - 指定列が空でない場合に採用する。
- `VALUE_MATCH`
  - 指定列が期待値に一致する場合に採用する。

### locator_type

- `HEADER_NAME`
  - ヘッダー名で列を特定する。
- `COLUMN_NO`
  - 列番号で列を特定する。初期は例外扱い。
- `HEADER_AND_COLUMN`
  - ヘッダー名で特定し、期待列番号と一致するか検証する。

### operator

- `EQUALS`
- `NOT_EQUALS`
- `IN`
- `NOT_IN`
- `EXISTS`
- `NOT_EXISTS`
- `NOT_EMPTY`
- `EMPTY`
