# 15 medi → v2 カラム棚卸し

## 目的

旧 `medi_*` 系テーブルのカラムを棚卸しし、health_exam_result v2 での扱いを決定する。

本ドキュメントはDDLではなく、旧設計からv2への移行判断を記録するための設計メモとする。

関連資料

- 03_decisions.md
- 05_design_history.md
- 12_v2_ddl_design_notes.md
- 08_table_ddl_summary_codex.md

---

# 判定区分

| 区分 | 内容 |
|------|------|
| 移行 | 同じ責務でv2へ移す |
| 名称変更 | 名前を変更してv2へ移す |
| 再配置 | 別テーブルへ責務を移して保持する |
| 廃止 | v2では保持しない |
| 参照化 | v2で保持せず参照テーブルから取得する |
| 追加 | v2で新規追加したカラム |

---

# 棚卸し順序

1. medi_shared_files / medi_zip_receipts → file_receipts
2. medi_xml_receipts / medi_xml_ledger → xml_ledger
3. medi_xml_receipts / medi_xml_ledger → xml_file_links
4. medi_xml_item_values / medi_exam_result_item_values → exam_item_values
5. medi_lsio_* / judge_* → exam_check_results
6. medi_import_runs 等 → etl_runs / etl_errors

---

# 1. file_receipts

対象旧テーブル

- medi_shared_files
- medi_zip_receipts

| 旧テーブル | 旧カラム | v2テーブル | v2カラム | 判定 | 理由・備考 |
|------------|-----------|------------|-----------|------|------------|
| medi_shared_files | path | file_receipts | source_path / relative_path | 名称変更 | eventルートからの相対パスを基本とし、元ファイル位置を管理する。 |
| medi_shared_files | file_name | file_receipts | file_name | 移行 | 物理ファイル名として保持する。 |
| medi_shared_files | ext | file_receipts | file_type | 名称変更 | 拡張子ではなくファイル種別(ZIP/XML/CSV等)として管理する。 |
| medi_shared_files | file_size | file_receipts | file_size | 移行 | 物理ファイル属性として保持する。 |
| medi_shared_files | sha256 | file_receipts | file_sha256 | 名称変更 | ファイル識別子として保持する。 |
| medi_shared_files | first_seen_at | file_receipts | first_seen_at | 移行 | 初回検出日時。 |
| medi_shared_files | last_seen_at | file_receipts | last_seen_at | 移行 | 最終検出日時。 |
| medi_shared_files | stage_status | file_receipts | status | 名称変更 | ファイル単位の総合処理ステータスとして再設計する。 |
| medi_shared_files | created_at / updated_at | file_receipts | created_at / updated_at | 移行 | 台帳管理用監査項目。 |
| medi_zip_receipts | facility_code | file_receipts | facility_code | 移行 | 医療機関コード。 |
| medi_zip_receipts | facility_name | file_receipts | facility_name | 移行 | 医療機関名称。 |
| medi_zip_receipts | run_id | file_receipts | etl_run_id | 名称変更 | ADR-0023準拠のETL実行IDへ整理する。 |
| medi_zip_receipts | first_seen_run_id / last_seen_run_id | file_receipts | first_seen_run_id / last_seen_run_id | 名称変更 | ETL実行履歴として保持する。 |
| medi_zip_receipts | zip_xml_count | file_receipts | processable_count | 名称変更 | ZIP/XML/CSV共通の「処理対象件数」へ一般化する。 |
| medi_zip_receipts | zip_xml_checked_at | file_receipts | content_checked_at | 名称変更 | 中身確認日時をファイル種別共通化する。 |
| medi_zip_receipts | zip_has_xml | file_receipts | processable_count | 再配置 | processable_count=0 を処理対象なし(エラー)として扱うため専用フラグは不要。 |
| medi_zip_receipts | is_zip | file_receipts | file_type | 再配置 | file_typeで表現するため独立カラムは不要。 |


### 現時点の設計メモ

- `file_receipts` は物理ファイルの正台帳とする。
- `work` 配下は一時処理領域であり、正台帳とはしない。
- CSV・XML・ZIP を共通設計で扱うため、ZIP専用カラムは一般化する。
- `processable_count` は処理対象件数とし、0件はエラー扱いとする。
- CSVのヘッダー有無など件数算出方法は設定・スクリプト側の責務とする。


> この表を埋めながら file_receipts のカラム構成を確定する。

---

# 2. xml_ledger

対象旧テーブル

- medi_xml_receipts
- medi_xml_ledger

## 責務整理

v2 の `xml_ledger` は XML内容の一意台帳とする。

保持する責務

- 1 XML内容 = 1レコード
- XMLから抽出した基本情報
- 加入者照合に必要な raw 値・match 値
- `person_id_custom` / `identity_hash` の生成結果
- `identity_hash` による `subscribers.id` との照合結果
- `subscriber_id` / `hia_subscriber_id` の保持
- `xml_sha256` によるXML内容の一意管理
- XML処理の総合判定（`xml_status` / `xml_reason`）
- 健診内容チェックの総合判定（`check_status` / `check_reason`）
- XML単位の出力可否（`xml_export_status`）
- チェックNG後に業務確認で出力OKとした手動承認情報

保持しない責務

- 健診項目値（→ exam_item_values）
- 法定健診・特定健診等のチェック詳細（→ exam_check_results）
- ETLエラー詳細（→ etl_errors）
- 抽出済み件数・警告件数などの詳細集計
- entry配下の個別検査値
- 物理ファイルとの紐付け（→ xml_file_links）
- 物理ファイルの初回・最終受領履歴（→ file_receipts / xml_file_links）

## カラム棚卸し

| 旧テーブル | 旧カラム | v2テーブル | v2カラム | 判定 | 理由・備考 |
|------------|-----------|------------|-----------|------|------------|
| medi_xml_receipts | xml_receipt_id | xml_ledger | id | 名称変更 | v2では XML内容の一意台帳の主キーとして整理する。 |
| medi_xml_receipts | xml_sha256 | xml_ledger | xml_sha256 | 移行 | XML原本識別子として保持する。 |
| medi_xml_receipts | document_id | xml_ledger | document_id | 移行 | XML追跡用として保持する。 |
| medi_xml_receipts | zip_inner_path | xml_file_links | xml_inner_path | 再配置 | ZIP内XMLパスは物理ファイル内の位置情報として `xml_file_links` に保持する。単体XMLではNULL可。 |
| medi_xml_receipts | file_size | xml_ledger | xml_file_size | 名称変更 | XML単位のファイルサイズとして保持する。 |
| medi_xml_receipts | file_mtime | xml_ledger | xml_file_mtime | 名称変更 | XML単位のmtimeとして保持する。 |
| medi_xml_receipts | facility_code | xml_ledger | facility_code | 移行 | 医療機関コード。 |
| medi_xml_receipts | facility_name | xml_ledger | facility_name | 移行 | 医療機関名称。 |
| medi_xml_receipts | insurer_number | xml_ledger | insurer_number | 移行 | 加入者照合・検索用。 |
| medi_xml_receipts | patient_name_kana | xml_ledger | name_kana_raw | 名称変更 | XML由来の氏名カナ原本値として保持する。 |
| medi_xml_receipts | birthdate | xml_ledger | birthdate | 移行 | XML由来の生年月日として保持する。 |
| medi_xml_receipts | exam_date | xml_ledger | exam_date | 名称変更 | 健診実施日として保持する。 |
| medi_xml_receipts | status | xml_ledger | xml_status | 名称変更 | XML処理の総合判定として `OK` / `WARNING` / `NG` で保持する。 |
| medi_xml_receipts | error_code | xml_ledger | xml_reason | 名称変更 | XML処理が `WARNING` / `NG` となった理由コードとして保持する。 |
| medi_xml_receipts | error_message | etl_errors | error_message | 再配置 | 詳細メッセージは詳細エラー台帳へ分離する。台帳側にはreasonのみ保持する。 |
| medi_xml_receipts | items_extract_status | exam_item_values / etl_runs / etl_errors | 廃止 | 廃止 | item抽出の詳細状態は `exam_item_values` の有無、`xml_status` / `xml_reason`、および `etl_runs` / `etl_errors` で確認する。 |
| medi_xml_receipts | extracted_run_id | etl_runs / etl_errors | run_id | 再配置 | XML基本情報抽出runは `xml_ledger` には持たず、実行単位は `etl_runs`、XML別の異常・詳細は `etl_errors` で確認する。 |
| medi_xml_receipts | items_extracted_run_id | etl_runs / etl_errors | run_id | 再配置 | item抽出runは `xml_ledger` には持たず、実行単位は `etl_runs`、XML別の異常・詳細は `etl_errors` で確認する。 |
| medi_xml_receipts | extracted_at | etl_runs / etl_errors | started_at / finished_at / created_at | 再配置 | XML基本情報抽出日時は台帳カラムにせず、run日時とエラー発生日時で追跡する。 |
| medi_xml_receipts | items_extracted_at | etl_runs / etl_errors | started_at / finished_at / created_at | 再配置 | item抽出日時は台帳カラムにせず、run日時とエラー発生日時で追跡する。 |
| medi_xml_receipts | first_seen_run_id / last_seen_run_id / first_seen_at / last_seen_at | xml_ledger | 廃止 | 廃止 | XML単位では初回・最終検出履歴は保持しない。物理受領履歴は file_receipts 側で管理する。 |
| medi_xml_receipts | admin_note | xml_ledger | note | 名称変更 | 運用メモとして保持する。 |
| medi_xml_receipts | created_at / updated_at | xml_ledger | created_at / updated_at | 移行 | 台帳管理用監査項目。 |
| medi_xml_ledger | xml_ledger_id | xml_ledger | id | 名称変更 | v2の主キーへ統合する。 |
| medi_xml_ledger | run_id | xml_ledger | etl_run_id | 名称変更 | ADR-0023準拠のETL実行IDへ整理する。 |
| medi_xml_ledger | zip_receipt_id | xml_file_links | file_receipt_id | 再配置 | `xml_ledger` には直接持たず、物理ファイルとXML内容の対応を `xml_file_links` で管理する。 |
| medi_xml_ledger | zip_sha256 | file_receipts | file_sha256 | 再配置 | 由来ファイルの正は `file_receipts.file_sha256` とする。 |
| medi_xml_ledger | zip_inner_path_sha256 | xml_file_links | 廃止 | 廃止 | ZIP内XMLパスは `xml_file_links.xml_inner_path` で保持し、パスSHAは独立カラムとしては持たない。 |
| medi_xml_ledger | xml_sha256 | xml_ledger | xml_sha256 | 移行 | XML原本の証跡として保持する。 |
| medi_xml_ledger | facility_folder_name | xml_ledger | facility_folder_name | 移行 | 医療機関フォルダ由来の名称として保持する。 |
| medi_xml_ledger | facility_code | xml_ledger | facility_code | 移行 | 医療機関コードとして保持する。 |
| medi_xml_ledger | facility_name | xml_ledger | facility_name | 移行 | 医療機関名として保持する。 |
| medi_xml_ledger | zip_name | file_receipts | file_name | 再配置 | 由来ファイル名は物理ファイル台帳 `file_receipts` 側を正とする。 |
| medi_xml_ledger | xml_filename | xml_file_links / file_receipts | 廃止 | 廃止 | XMLファイル名は独立カラムとして持たず、ZIP内XMLでは `xml_file_links.xml_inner_path`、単体XMLでは `file_receipts.file_name` から確認する。 |
| medi_xml_ledger | zip_inner_path | xml_file_links | xml_inner_path | 再配置 | ZIP内XMLパスは物理ファイル内の位置情報として `xml_file_links` に保持する。 |
| medi_xml_ledger | insurer_number | xml_ledger | insurer_number | 移行 | 加入者照合・検索用として保持する。 |
| medi_xml_ledger | insurance_symbol | xml_ledger | insurance_symbol_raw | 名称変更 | XML由来の保険証記号原本値として保持する。 |
| medi_xml_ledger | insurance_number | xml_ledger | insurance_number_raw | 名称変更 | XML由来の保険証番号原本値として保持する。 |
| medi_xml_ledger | insurance_branch_number | xml_ledger | insurance_branch_number_raw | 名称変更 | XML由来の枝番原本値として保持する。 |
| medi_xml_ledger | birth_date | xml_ledger | birthdate | 名称変更 | identity生成・加入者照合・検索に使用する。 |
| medi_xml_ledger | kenshin_date | xml_ledger | exam_date | 名称変更 | 健診実施日として保持する。 |
| medi_xml_ledger | gender_code | xml_ledger | gender_code | 移行 | identity生成・加入者照合・検索に使用する。 |
| medi_xml_ledger | name_kana_full | xml_ledger | name_kana_raw | 名称変更 | XML由来の氏名カナ原本値として保持する。 |
| medi_xml_ledger | postal_code | xml_ledger | postal_code | 移行 | XML全体に関わる対象者基本情報として保持する。 |
| medi_xml_ledger | address | xml_ledger | address | 移行 | XML全体に関わる対象者基本情報として保持する。 |
| medi_xml_ledger | org_name_in_xml | xml_ledger | org_name_in_xml | 移行 | XML内の健診機関名として保持する。 |
| medi_xml_ledger | org_code_in_xml | xml_ledger | org_code_in_xml | 移行 | XML内の健診機関コードとして保持する。 |
| medi_xml_ledger | report_category_code | xml_ledger | report_category_code | 移行 | 報告区分コードとして保持する。 |
| medi_xml_ledger | program_type_code | xml_ledger | program_type_code | 移行 | 健診プログラム種別として保持する。 |
| medi_xml_ledger | guidance_level_code | xml_ledger | guidance_level_code | 移行 | 保健指導レベルとして保持する。 |
| medi_xml_ledger | metabo_code | xml_ledger | metabo_code | 移行 | メタボ判定として保持する。 |
| medi_xml_ledger | insurance_symbol_match | xml_ledger | insurance_symbol_match | 移行 | 加入者照合用の正規化済み記号として保持する。 |
| medi_xml_ledger | insurance_number_match | xml_ledger | insurance_number_match | 移行 | 加入者照合用の正規化済み番号として保持する。 |
| medi_xml_ledger | name_kana_match | xml_ledger | name_kana_match | 移行 | 加入者照合用の正規化済みカナとして保持する。 |
| medi_xml_ledger | person_id_custom | xml_ledger | person_id_custom | 移行 | identity共通仕様に従って生成した人物識別補助キーとして保持する。 |
| medi_xml_ledger | identity_hash | xml_ledger | identity_hash | 移行 | identity共通仕様に従って生成し、`subscribers.id` を引く唯一の加入者照合キーとして保持する。 |
| medi_xml_ledger | xsd_valid | xml_ledger | xml_status / xml_reason | 再配置 | XSD判定は `xml_status` / `xml_reason` に吸収する。XSD詳細は `etl_errors` 側で確認する。 |
| medi_xml_ledger | error_content | xml_ledger | xml_reason | 名称変更 | XML処理が `WARNING` / `NG` となった理由コードへ整理する。詳細は `etl_errors`。 |
| medi_xml_ledger | judge_status | xml_ledger | check_status | 再配置 | 健診内容チェックの総合判定として `OK` / `WARNING` / `NG` で保持する。 |
| medi_xml_ledger | judge_score | exam_check_results | 廃止 | 廃止 | 旧判定スコアは独立カラムとして持たない。必要な集計は項目別 `status` / `reason` と reason summary から確認する。 |
| medi_xml_ledger | judge_note | exam_check_results | check_reason / reason | 再配置 | チェック詳細理由は `exam_check_results` 側へ寄せる。台帳側には `check_reason` のみ保持する。 |
| medi_xml_ledger | judged_run_id | exam_check_results / etl_runs | check_run_id | 再配置 | 総合チェックrunは `xml_ledger` には持たず、チェック結果側の共通 `check_run_id` と `etl_runs` で追跡する。 |
| medi_xml_ledger | judged_at | exam_check_results / etl_runs | checked_at | 再配置 | 総合チェック実施日時は `xml_ledger` には持たず、チェック結果側の共通 `checked_at` と `etl_runs` で追跡する。 |
| medi_xml_ledger | is_exam_result | xml_ledger | xml_status / xml_reason | 再配置 | 健診結果XMLとして扱えるかは `xml_status` / `xml_reason` に吸収する。独立フラグは持たない。 |
| medi_xml_ledger | is_legal_exam | exam_check_results | legal_status | 再配置 | 法定健診判定は `exam_check_results` へ寄せる。 |
| medi_xml_ledger | lsio_legal_is_complete | exam_check_results | legal_status | 再配置 | 法定健診のOK/NGは `exam_check_results` の詳細根拠とする。台帳側は `check_status` で結論を持つ。 |
| medi_xml_ledger | lsio_legal_required_count | exam_check_results | 廃止 | 廃止 | 必要項目数は独立カラムとして持たず、横持ち項目・マスタ定義から確認する。 |
| medi_xml_ledger | lsio_legal_present_count | exam_check_results | 廃止 | 廃止 | 実施項目数は独立カラムとして持たず、項目別 `status` から確認する。 |
| medi_xml_ledger | lsio_legal_missing_methods | exam_check_results | legal_reason / missing_summary | 再配置 | 不足項目理由として `exam_check_results` へ寄せる。 |
| medi_xml_ledger | lsio_legal_judged_run_id | exam_check_results | check_run_id | 再配置 | 制度別runカラムは持たず、法定健診・特定健診で共通の `check_run_id` に統一する。 |
| medi_xml_ledger | lsio_legal_judged_at | exam_check_results | checked_at | 再配置 | 制度別日時カラムは持たず、法定健診・特定健診で共通の `checked_at` に統一する。 |
| medi_xml_ledger | created_at | xml_ledger | created_at | 移行 | 台帳管理用監査項目。 |

## v2で新規追加するカラム候補

| v2カラム | 理由 |
|-----------|------|
| event_id | イベント単位で検索するため。 |
| subscriber_id | `identity_hash` で一致した `subscribers.id` を保持する。 |
| hia_subscriber_id | `subscriber_id` から取得し、運用検索性向上のため冗長保持する。 |
| xml_status | XML処理の総合判定を `OK` / `WARNING` / `NG` で保持する。 |
| xml_reason | `xml_status` が `WARNING` / `NG` となった理由コードを保持する。 |
| check_status | 健診内容チェックの総合判定を `OK` / `WARNING` / `NG` で保持する。 |
| check_reason | `check_status` が `WARNING` / `NG` となった理由コードを保持する。 |
| xml_export_status | HIAアップロード用XMLとして出力してよいかをXML単位で保持する。基本は `OK` / `NG`。 |
| manual_export_approved | `check_status = NG` でも業務確認により出力OKとしたことを示す手動承認フラグ。 |
| manual_export_reason | 手動承認により出力OKとした理由を保持する。 |

## 現時点の設計メモ

- `xml_ledger` は XML内容の一意台帳とする。
- 同一 `xml_sha256` のXMLは `xml_ledger` に重複作成しない。
- 物理ファイルとの関係は `xml_file_links` で管理し、`xml_ledger` に `file_receipt_id` は持たない。
- `status` は `OK` / `WARNING` / `NG` の3状態を基本とする。
- `reason` は `status` が `WARNING` / `NG` となった理由コードとして保持する。
- `reason` は固定enumではなく、スクリプト実装・チェック追加に応じて理由コードを追加できる文字列カラムとする。
- `person_id_custom` / `identity_hash` は identity 共通仕様の説明書に従い、既存の共通生成処理を利用して生成する。
- raw値を直接独自ロジックで組み立てず、identity共通仕様に従う。
- `identity_hash` を唯一の加入者照合キーとし、一致した `subscribers.id` を `subscriber_id` として保持する。
- `hia_subscriber_id` は `subscriber_id` から取得し、検索性向上のための運用補助キーとして冗長保持する。
- identity共通生成処理で `ok=false` となる場合は、必要項目不足または正規化NGとして加入者照合NGにする。
- `insurance_symbol_raw` / `insurance_number_raw` / `name_kana_raw` はXML由来の原本値として保持する。
- `insurance_symbol_match` / `insurance_number_match` / `name_kana_match` は照合用の正規化済み値として保持する。
- XML全体・受診者・健診イベントに関わる情報は `xml_ledger` に保持する。
- entry単位の個別検査値は `exam_item_values` に保持する。
- 人が最初に確認する入口として、総合判定・検索キー・現在状態を保持する。
- 詳細な根拠は `exam_check_results`・`exam_item_values`・`etl_errors` に分離する。
- 台帳側には結論のみを冗長保持し、人がJOINせず状況確認できることを優先する。
- `check_status` は制度・内容チェックのシステム判定結果として保持し、手動承認によって変更しない。
- `xml_export_status` は最終的にHIAアップロード用XMLとして出力してよいかを表すXML単位の出力可否とする。
- `check_status = NG` でも、医療機関確認等により正当理由が確認できた場合は、`manual_export_approved = true`、`manual_export_reason` を設定し、`xml_export_status = OK` とできる。
- 出力済み状態（`exported_at` / `export_run_id` 等）を `xml_ledger` に持つか、出力台帳側に持つかは、exportスクリプト設計時に決定する。
- `error_count` / `warning_count`、`xsd_valid`、`item_extract_status`、`is_exam_result` は独立カラムとしては持たず、詳細テーブル・実データ有無・`xml_status` / `xml_reason` へ集約する。
- `extracted_run_id` / `items_extracted_run_id` / `extracted_at` / `items_extracted_at` は `xml_ledger` に持たず、`etl_runs` / `etl_errors` 側で追跡する。
- `check_run_id` / `checked_at` は `xml_ledger` に持たず、`exam_check_results` / `etl_runs` 側で追跡する。
- `first_seen_run_id` / `last_seen_run_id` / `first_seen_at` / `last_seen_at` は `xml_ledger` には持たず、物理受領履歴は `file_receipts` / `xml_file_links` 側で管理する。
- 別ZIP等で内容が同一のXMLを受領した場合は、`xml_ledger` を新規作成せず `xml_file_links` のみ追加する。
- 同一内容XMLであっても受領事実は `xml_file_links` で保持する。

---

# 3. xml_file_links

対象旧テーブル

- medi_xml_receipts
- medi_xml_ledger

## 責務整理

v2 の `xml_file_links` は、物理ファイルとXML内容の対応台帳とする。

保持する責務

- `file_receipts` と `xml_ledger` の対応を保持する
- ZIP内XMLパスなど、物理ファイル内でのXML位置を保持する
- 別ZIP等で同一XMLを受領した場合に、同じ `xml_ledger` へ複数の受領リンクを保持する

保持しない責務

- 物理ファイルそのものの正情報（→ file_receipts）
- XML内容の正情報（→ xml_ledger）
- 健診項目値（→ exam_item_values）
- ETLエラー詳細（→ etl_errors）

## カラム棚卸し

| 旧テーブル | 旧カラム | v2テーブル | v2カラム | 判定 | 理由・備考 |
|------------|-----------|------------|-----------|------|------------|
| medi_xml_receipts | xml_receipt_id | xml_file_links | id | 追加 | 旧主キーをそのまま移行するのではなく、物理ファイルとXML内容の対応行として新規採番する。 |
| medi_xml_receipts | zip_inner_path | xml_file_links | xml_inner_path | 再配置 | ZIP内XMLパスを物理ファイル内の位置情報として保持する。単体XMLではNULL可。 |
| medi_xml_ledger | zip_receipt_id | xml_file_links | file_receipt_id | 再配置 | 物理ファイル台帳 `file_receipts` への参照として保持する。 |
| medi_xml_ledger | xml_ledger_id | xml_file_links | xml_ledger_id | 再配置 | XML内容の一意台帳 `xml_ledger` への参照として保持する。 |
| medi_xml_ledger | zip_inner_path | xml_file_links | xml_inner_path | 再配置 | ZIP内XMLパスを物理ファイル内の位置情報として保持する。 |

## v2で新規追加するカラム候補

| v2カラム | 理由 |
|-----------|------|
| id | `xml_file_links` の主キー。 |
| event_id | イベント単位で受領ファイルとXML内容の対応を検索するため。 |
| file_receipt_id | 物理ファイル台帳 `file_receipts` を参照する。 |
| xml_ledger_id | XML内容の一意台帳 `xml_ledger` を参照する。 |
| xml_inner_path | ZIP内XMLパス。単体XMLではNULL可。 |
| created_at | 対応行の作成日時。 |

## 現時点の設計メモ

- `file_receipts` は物理ファイルの正台帳、`xml_ledger` はXML内容の正台帳とする。
- `xml_file_links` は物理ファイルとXML内容の対応台帳とする。
- 同一 `xml_sha256` のXMLを別ZIP等で受領した場合は、既存 `xml_ledger` を参照する `xml_file_links` を追加する。
- `xml_ledger` は物理受領履歴を持たない。

---

# 4. exam_item_values

対象旧テーブル

- medi_xml_item_values
- medi_exam_result_item_values

## 責務整理

v2 の `exam_item_values` は、入力元に依存しない健診値共通基盤とする。

保持する責務

- XML / CSV 由来の健診項目値を共通形式で保持する
- 由来Ledgerを `ledger_type` / `ledger_id` で表現する
- 検索性向上のため `event_id` / `subscriber_id` / `hia_subscriber_id` を冗長保持する
- 実際に存在した健診値のみを保持する
- raw値と正規化値を保持する
- 正規化状態・正規化理由を保持する
- 項目値としての妥当性（範囲外・形式不正等）を保持する

保持しない責務

- XML全体・受診者・健診イベントの状態（→ xml_ledger）
- 法定健診・特定健診など制度チェック結果（→ exam_check_results）
- 制度チェック上の不足項目そのもの（→ exam_check_results）
- ETLエラー詳細（→ etl_errors）

## カラム棚卸し

| 旧テーブル | 旧カラム | v2テーブル | v2カラム | 判定 | 理由・備考 |
|------------|-----------|------------|-----------|------|------------|
| medi_xml_item_values | xml_item_value_id | exam_item_values | id | 名称変更 | v2の健診項目値主キーとして整理する。 |
| medi_xml_item_values | xml_sha256 | xml_ledger | xml_sha256 | 再配置 | XML原本識別子の正は `xml_ledger.xml_sha256` とする。`exam_item_values` は `ledger_type = XML` / `ledger_id = xml_ledger.id` で由来を表現する。 |
| medi_xml_item_values | zip_sha256 | file_receipts | file_sha256 | 再配置 | 由来ファイル情報は `file_receipts` 側を正とする。 |
| medi_xml_item_values | zip_inner_path_sha256 | xml_file_links | 廃止 | 廃止 | ZIP内XMLパスは `xml_file_links.xml_inner_path` で保持し、パスSHAは独立カラムとしては持たない。 |
| medi_xml_item_values | zip_inner_path | xml_file_links | xml_inner_path | 再配置 | ZIP内XMLパスは物理ファイル内の位置情報として `xml_file_links` に保持する。 |
| medi_xml_item_values | namecode | exam_item_values | namecode | 移行 | 健診項目コードとして保持する。 |
| medi_xml_item_values | occurrence_no | exam_item_values | occurrence_no | 移行 | 同一項目が複数出現した場合の出現順として保持する。 |
| medi_xml_item_values | value_raw | exam_item_values | raw_value | 名称変更 | 入力元由来の未加工値として保持する。 |
| medi_xml_item_values | value_type | exam_item_values | raw_value_type | 名称変更 | 入力元由来の値型として保持する。 |
| medi_xml_item_values | unit | exam_item_values | raw_unit | 名称変更 | 入力元由来の単位として保持する。 |
| medi_xml_item_values | code_system | exam_item_values | code_system | 移行 | コード値のOID等として保持する。 |
| medi_xml_item_values | code_value | exam_item_values | code_value | 移行 | コード値として保持する。 |
| medi_xml_item_values | code_display | exam_item_values | code_display | 移行 | コード表示名として保持する。 |
| medi_xml_item_values | extracted_run_id | exam_item_values | extracted_run_id | 移行 | 項目抽出を実行した `etl_runs` を参照する。 |
| medi_xml_item_values | extracted_at | exam_item_values | extracted_at | 移行 | 項目抽出日時として保持する。 |
| medi_exam_result_item_values | item_value_id | exam_item_values | id | 名称変更 | v2の健診項目値主キーへ統合する。 |
| medi_exam_result_item_values | ledger_id | exam_item_values | ledger_id | 名称変更 | v2では `ledger_type` と組み合わせて由来Ledgerを表現する。 |
| medi_exam_result_item_values | namecode | exam_item_values | namecode | 移行 | 健診項目コードとして保持する。 |
| medi_exam_result_item_values | value_seq | exam_item_values | occurrence_no | 名称変更 | 同一項目内の値順として `occurrence_no` へ寄せる。 |
| medi_exam_result_item_values | raw_value | exam_item_values | raw_value | 移行 | 入力元由来の未加工値として保持する。 |
| medi_exam_result_item_values | value | exam_item_values | normalized_value | 名称変更 | 正規化後の値として保持する。 |
| medi_exam_result_item_values | nullflavor | exam_item_values | nullflavor | 移行 | XML上のnullFlavor、または未実施等の表現として保持する。 |
| medi_exam_result_item_values | identity_item_code | exam_item_values | identity_item_code | 移行 | 同一性項目コードとして保持する。項目マスタ参照に使う。 |
| medi_exam_result_item_values | jun_no | exam_item_values | jun_no | 移行 | 制度資料上の順序・項目並びの補助として保持する。 |
| medi_exam_result_item_values | normalize_status | exam_item_values | normalize_status | 移行 | 正規化結果の状態として保持する。 |
| medi_exam_result_item_values | normalize_error | exam_item_values | normalize_reason | 名称変更 | 正規化NG/WARNINGの理由コードとして保持する。詳細は `etl_errors`。 |
| medi_exam_result_item_values | normalized_at | exam_item_values | normalized_at | 移行 | 正規化日時として保持する。 |
| medi_exam_result_item_values | created_at / updated_at | exam_item_values | created_at / updated_at | 移行 | 台帳管理用監査項目。 |

## v2で新規追加するカラム候補

| v2カラム | 理由 |
|-----------|------|
| event_id | イベント単位で健診値を検索・集計するため。 |
| ledger_type | 由来Ledger種別を表す。現時点では `XML` / `CSV` を採用する。 |
| ledger_id | 由来LedgerのIDを保持する。`ledger_type = XML` の場合は `xml_ledger.id`。 |
| subscriber_id | `dev_phr.subscribers.id` で健診値を直接検索するため冗長保持する。 |
| hia_subscriber_id | HIA加入者IDで健診値を直接検索するため冗長保持する。 |
| normalized_unit | 正規化後の単位を保持する。 |
| validation_status | 項目値としての妥当性を `OK` / `WARNING` / `NG` で保持する。 |
| validation_reason | `validation_status` が `WARNING` / `NG` となった理由コードを保持する。 |

## 現時点の設計メモ

- `exam_item_values` はXML専用ではなく、XML / CSV 共通の健診値テーブルとする。
- `ledger_type` は現時点では `XML` / `CSV` を採用し、それ以外の入力元は現時点では決定しない。
- 将来入力元が追加された場合の拡張方法は、その時点で検討する。
- `ledger_id` は `ledger_type` と組み合わせて由来Ledgerを表現する。
- XML由来の場合、`xml_sha256` は `exam_item_values.ledger_id` へ直接移すのではなく、`xml_ledger.xml_sha256` を正とし、`exam_item_values.ledger_type = XML` / `exam_item_values.ledger_id = xml_ledger.id` で由来を表現する。
- `event_id` / `subscriber_id` / `hia_subscriber_id` は検索性向上のため冗長保持する。
- `exam_item_values` は実際に存在した健診値のみを保持する。
- 制度チェックは、`exam_item_values` に存在する値だけでなく「存在しない項目」も判定材料とするため、`exam_check_results` 側の責務とする。
- 項目値としての妥当性（範囲外・形式不正等）は `validation_status` / `validation_reason` で保持する。
- `validation_status` は制度チェックではなく、値そのものの妥当性を表す。
- 正規化値・正規化状態は `exam_item_values` の責務とする。
- CSVからHIAアップロード用XMLを生成する場合も、`exam_item_values` の正規化済み値を利用する。

---

# 5. exam_check_results

対象旧カラム・旧責務

- `medi_xml_ledger.judge_*`
- `medi_xml_ledger.lsio_legal_*`
- 旧スクリプト上の法定健診・特定健診チェック結果
- `docs/spec/health_examinations/02_exam_check_item_spec_v2_0_0.md` の制度チェック対象72項目

## 責務整理

v2 の `exam_check_results` は、制度チェック結果を人が検索・集計しやすい形で保持する業務台帳とする。

保持する責務

- 1受診者・1Ledger単位の制度チェック結果を保持する
- `exam_item_values` に存在する値、および存在しない項目を判定材料として保持する
- 制度チェック対象項目を同一性項目コード単位で横持ちする
- 各チェック対象項目の状態を `status` / `reason` で保持する
- 法定健診・特定健診の総合判定を保持する
- 法定健診・特定健診の reason summary を保持する
- 検索性向上のため `event_id` / `subscriber_id` / `hia_subscriber_id` を冗長保持する

保持しない責務

- 実際に存在した健診値そのもの（→ exam_item_values）
- XML全体の処理状態（→ xml_ledger）
- XML単位のHIAアップロード用出力可否（→ xml_ledger.xml_export_status）
- XML解析・ETLエラー詳細（→ etl_errors）
- 検査方法、左右、裸眼/矯正などの粒度別カラム

## 設計方針

- `exam_item_values` は実値を縦持ちで保持する。
- `exam_check_results` は制度チェック対象項目を横持ちで保持する。
- 横持ち対象は `docs/spec/health_examinations/02_exam_check_item_spec_v2_0_0.md` の同一性項目コード一覧を正とする。
- 横持ち項目は同一性項目コード単位で作成する。
- 検査方法、左右、裸眼/矯正などではカラムを分けない。
- 項目ごとに `present` / `valid` を分けず、`status` / `reason` で表現する。
- `status` は、値あり・算出・代替・不足・不正などの状態を表す。
- `reason` は、算出元、代替元、不足理由、不正理由などを保持する。
- 法定健診・特定健診の総合判定は、項目別 `status` / `reason` とは別に保持する。
- 法定健診・特定健診の reason summary は、項目別 `status` / `reason` から集約して保持する。
- `exam_check_results` は正規化を優先するテーブルではなく、人がSQLや一覧で確認しやすいことを優先する。

## カラム棚卸し

| 旧テーブル | 旧カラム | v2テーブル | v2カラム | 判定 | 理由・備考 |
|------------|-----------|------------|-----------|------|------------|
| medi_xml_ledger | judge_status | exam_check_results / xml_ledger | legal_status / specific_status / check_status | 再配置 | 制度別の総合判定は `exam_check_results` に保持し、XML単位の総合判定サマリーは `xml_ledger.check_status` に保持する。 |
| medi_xml_ledger | judge_score | exam_check_results | 廃止 | 廃止 | 旧判定スコア・集計値は独立カラムとして持たない。必要な確認は項目別 `status` / `reason` と reason summary で行う。 |
| medi_xml_ledger | judge_note | exam_check_results | legal_reason_summary / specific_reason_summary | 再配置 | 人が確認する制度別reason summaryとして保持する。詳細理由は項目別reasonにも分解する。 |
| medi_xml_ledger | judged_run_id | exam_check_results / etl_runs | check_run_id | 再配置 | 制度チェックを実行した `etl_runs` を、チェック結果側の共通 `check_run_id` で参照する。 |
| medi_xml_ledger | judged_at | exam_check_results / etl_runs | checked_at | 再配置 | 制度チェック実施日時はチェック結果側の共通 `checked_at` と `etl_runs` で追跡する。 |
| medi_xml_ledger | is_legal_exam | exam_check_results | legal_status | 再配置 | 法定健診としての判定は `legal_status` に統合する。独立フラグは持たない。 |
| medi_xml_ledger | lsio_legal_is_complete | exam_check_results | legal_status | 再配置 | 法定健診のOK/NG総合判定として保持する。 |
| medi_xml_ledger | lsio_legal_required_count | exam_check_results | 廃止 | 廃止 | 法定健診の必要項目数は独立カラムとして持たず、横持ち項目・マスタ定義から確認する。 |
| medi_xml_ledger | lsio_legal_present_count | exam_check_results | 廃止 | 廃止 | 法定健診の実施項目数は独立カラムとして持たず、項目別 `status` から確認する。 |
| medi_xml_ledger | lsio_legal_missing_methods | exam_check_results | legal_reason_summary | 再配置 | 法定健診の不足理由summaryとして保持する。 |
| medi_xml_ledger | lsio_legal_judged_run_id | exam_check_results | check_run_id | 再配置 | 制度別runカラムは持たず、法定健診・特定健診で共通の `check_run_id` に統一する。 |
| medi_xml_ledger | lsio_legal_judged_at | exam_check_results | checked_at | 再配置 | 制度別日時カラムは持たず、法定健診・特定健診で共通の `checked_at` に統一する。 |
| docs/spec/health_examinations/02_exam_check_item_spec_v2_0_0.md | 同一性項目コード一覧72項目 | exam_check_results | `status_<item_code>` / `reason_<item_code>` | 追加 | 制度チェック対象項目を同一性項目コード単位で横持ちする。 |

## v2で新規追加するカラム候補

| v2カラム | 理由 |
|-----------|------|
| id | `exam_check_results` の主キー。 |
| event_id | イベント単位で制度チェック結果を検索・集計するため。 |
| ledger_type | 由来Ledger種別を表す。現時点では `XML` / `CSV` を採用する。 |
| ledger_id | 由来LedgerのIDを保持する。`ledger_type = XML` の場合は `xml_ledger.id`。 |
| subscriber_id | `dev_phr.subscribers.id` で制度チェック結果を直接検索するため冗長保持する。 |
| hia_subscriber_id | HIA加入者IDで制度チェック結果を直接検索するため冗長保持する。 |
| legal_status | 法定健診としての総合判定を `OK` / `WARNING` / `NG` で保持する。 |
| legal_reason_summary | 法定健診のNG/WARNING理由を `項目コード:理由|項目コード:理由` のsummary形式で保持する。 |
| specific_status | 特定健診としての総合判定を `OK` / `WARNING` / `NG` で保持する。 |
| specific_reason_summary | 特定健診のNG/WARNING理由を `項目コード:理由|項目コード:理由` のsummary形式で保持する。 |
| check_run_id | 制度チェックを実行した `etl_runs` を参照する。 |
| checked_at | 制度チェック実施日時。 |
| created_at / updated_at | 台帳管理用監査項目。 |
| `status_<item_code>` | 同一性項目コード単位の項目状態を保持する。例: `status_9N001`。 |
| `reason_<item_code>` | 同一性項目コード単位の理由を保持する。例: `reason_9N001`。 |

## 横持ち項目の生成元

横持ち対象項目は、以下の仕様書を正とする。

- `docs/spec/health_examinations/02_exam_check_item_spec_v2_0_0.md`

仕様書には、`付属2_制度整理` シート由来の制度チェック対象72項目を保持している。

並び順は以下とする。

1. 区分番号 昇順
2. 同一性項目コード 昇順

横持ちカラム名は、項目状態を `status_<item_code>`、理由を `reason_<item_code>` とする。

## 現時点の設計メモ

- `exam_check_results` は制度チェック結果台帳であり、実値テーブルではない。
- 実値は `exam_item_values` に縦持ちで保持する。
- 制度チェックでは、`exam_item_values` に存在する値だけでなく「存在しない項目」も判定材料とする。
- 横持ち項目は同一性項目コード単位で作成する。
- 検査方法、左右、裸眼/矯正などではカラムを分けない。
- 項目別の値あり/なしや値有効/不正は、`present` / `valid` ではなく `status` / `reason` に集約する。
- 法定健診・特定健診で値の事実を二重管理しない。
- 法定健診・特定健診で分けるのは総合評価と reason summary のみとする。
- `reason` は `項目コード:理由|項目コード:理由` のsummary形式を想定する。
- 項目別 `status` の正式コード一覧はDDL作成前に確定する。
- 項目別カラム命名規則は `status_<item_code>` / `reason_<item_code>` とする。
- `judge_score` / `legal_required_count` / `legal_present_count` は独立カラムとして持たない。
- `legal_check_run_id` / `legal_checked_at` のような制度別run日時は持たず、共通の `check_run_id` / `checked_at` に統一する。
- 判定ルール自体は `exam_check_results` に保持しない。既存 `dev_phr.exam_item_group_*` 系マスタを利用する。
- 法定健診ルールマスタは現行内容を棚卸しし、`02_exam_check_item_spec_v2_0_0.md` との差分確認を行う。
- 特定健診ルールマスタは、スクリプト方針が固まった後に `02_exam_check_item_spec_v2_0_0.md` を元に新規作成する。
