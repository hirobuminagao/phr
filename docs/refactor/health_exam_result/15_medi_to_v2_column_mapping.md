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
3. medi_xml_item_values / medi_exam_result_item_values → exam_item_values
4. medi_lsio_* / judge_* → exam_check_results
5. medi_import_runs 等 → etl_runs / etl_errors

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

v2 の `xml_ledger` は XML 単位の業務台帳とする。

保持する責務

- 1 XML = 1レコード
- XMLの由来情報（file_receiptsとの紐付け）
- XMLから抽出した基本情報
- 加入者照合に必要な raw 値・match 値
- `person_id_custom` / `identity_hash` の生成結果
- `identity_hash` による `subscribers.id` との照合結果
- `subscriber_id` / `hia_subscriber_id` の保持
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

## カラム棚卸し

| 旧テーブル | 旧カラム | v2テーブル | v2カラム | 判定 | 理由・備考 |
|------------|-----------|------------|-----------|------|------------|
| medi_xml_receipts | xml_receipt_id | xml_ledger | id | 名称変更 | v2では XML 単位Ledgerの主キーとして整理する。 |
| medi_xml_receipts | xml_sha256 | xml_ledger | xml_sha256 | 移行 | XML原本識別子として保持する。 |
| medi_xml_receipts | document_id | xml_ledger | document_id | 移行 | XML追跡用として保持する。 |
| medi_xml_receipts | zip_inner_path | xml_ledger | xml_inner_path | 名称変更 | ZIP内XMLパスとして保持する。単体XMLではNULL可。 |
| medi_xml_receipts | file_size | xml_ledger | xml_file_size | 名称変更 | XML単位のファイルサイズとして保持する。 |
| medi_xml_receipts | file_mtime | xml_ledger | xml_file_mtime | 名称変更 | XML単位のmtimeとして保持する。 |
| medi_xml_receipts | facility_code | xml_ledger | facility_code | 移行 | 医療機関コード。 |
| medi_xml_receipts | facility_name | xml_ledger | facility_name | 移行 | 医療機関名称。 |
| medi_xml_receipts | insurer_number | xml_ledger | insurer_number | 移行 | 加入者照合・検索用。 |
| medi_xml_receipts | patient_name_kana | xml_ledger | name_kana_full_raw | 名称変更 | XML由来の氏名カナ原本値として保持する。 |
| medi_xml_receipts | birthdate | xml_ledger | birth_date | 名称変更 | XML由来の生年月日として保持する。 |
| medi_xml_receipts | exam_date | xml_ledger | exam_date | 名称変更 | 健診実施日として保持する。 |
| medi_xml_receipts | status | xml_ledger | xml_status | 名称変更 | XML処理の総合判定として `OK` / `WARNING` / `NG` で保持する。 |
| medi_xml_receipts | error_code | xml_ledger | xml_reason | 名称変更 | XML処理が `WARNING` / `NG` となった理由コードとして保持する。 |
| medi_xml_receipts | error_message | etl_errors | message | 再配置 | 詳細メッセージは詳細エラー台帳へ分離する。台帳側にはreasonのみ保持する。 |
| medi_xml_receipts | items_extract_status | xml_ledger / etl_runs | 廃止候補 | 廃止 | item抽出の詳細状態は `exam_item_values` の有無、`xml_status` / `xml_reason`、および `etl_runs` / `etl_errors` で確認する。 |
| medi_xml_receipts | extracted_run_id | xml_ledger | extracted_run_id | 移行 | XML基本情報抽出runとして `etl_runs` を参照する。 |
| medi_xml_receipts | items_extracted_run_id | xml_ledger | item_extract_run_id | 名称変更 | item抽出runとして `etl_runs` を参照する。 |
| medi_xml_receipts | extracted_at | xml_ledger | extracted_at | 移行 | XML基本情報抽出日時。 |
| medi_xml_receipts | items_extracted_at | xml_ledger | item_extracted_at | 名称変更 | item抽出日時。 |
| medi_xml_receipts | first_seen_run_id / last_seen_run_id | xml_ledger | first_seen_run_id / last_seen_run_id | 移行 | XML単位の検出run履歴として保持する。 |
| medi_xml_receipts | first_seen_at / last_seen_at | xml_ledger | first_seen_at / last_seen_at | 移行 | XML単位の初回/最終検出日時として保持する。 |
| medi_xml_receipts | admin_note | xml_ledger | note | 名称変更 | 運用メモとして保持する。 |
| medi_xml_receipts | created_at / updated_at | xml_ledger | created_at / updated_at | 移行 | 台帳管理用監査項目。 |
| medi_xml_ledger | xml_ledger_id | xml_ledger | id | 名称変更 | v2の主キーへ統合する。 |
| medi_xml_ledger | run_id | xml_ledger | etl_run_id | 名称変更 | ADR-0023準拠のETL実行IDへ整理する。 |
| medi_xml_ledger | zip_receipt_id | xml_ledger | file_receipt_id | 名称変更 | v2では由来ファイル台帳 `file_receipts` へ接続する。 |
| medi_xml_ledger | zip_sha256 | file_receipts | file_sha256 | 再配置 | 由来ファイルの正は `file_receipts.file_sha256` とする。 |
| medi_xml_ledger | zip_inner_path_sha256 | xml_ledger | xml_inner_path_sha256 | 名称変更 | ZIP内XMLパスの識別補助として保持する。 |
| medi_xml_ledger | xml_sha256 | xml_ledger | xml_sha256 | 移行 | XML原本の証跡として保持する。 |
| medi_xml_ledger | facility_folder_name | xml_ledger | facility_folder_name | 移行 | 医療機関フォルダ由来の名称として保持する。 |
| medi_xml_ledger | facility_code | xml_ledger | facility_code | 移行 | 医療機関コードとして保持する。 |
| medi_xml_ledger | facility_name | xml_ledger | facility_name | 移行 | 医療機関名として保持する。 |
| medi_xml_ledger | zip_name | xml_ledger | source_file_name | 名称変更 | 由来ファイル名として保持する。単体XML時も同じ概念に寄せる。 |
| medi_xml_ledger | xml_filename | xml_ledger | xml_filename | 移行 | XMLファイル名として保持する。 |
| medi_xml_ledger | zip_inner_path | xml_ledger | xml_inner_path | 名称変更 | ZIP内XMLパスとして保持する。 |
| medi_xml_ledger | insurer_number | xml_ledger | insurer_number | 移行 | 加入者照合・検索用として保持する。 |
| medi_xml_ledger | insurance_symbol | xml_ledger | insurance_symbol_raw | 名称変更 | XML由来の保険証記号原本値として保持する。 |
| medi_xml_ledger | insurance_number | xml_ledger | insurance_number_raw | 名称変更 | XML由来の保険証番号原本値として保持する。 |
| medi_xml_ledger | insurance_branch_number | xml_ledger | insurance_branch_number_raw | 名称変更 | XML由来の枝番原本値として保持する。 |
| medi_xml_ledger | birth_date | xml_ledger | birth_date | 移行 | identity生成・加入者照合・検索に使用する。 |
| medi_xml_ledger | kenshin_date | xml_ledger | exam_date | 名称変更 | 健診実施日として保持する。 |
| medi_xml_ledger | gender_code | xml_ledger | gender_code | 移行 | identity生成・加入者照合・検索に使用する。 |
| medi_xml_ledger | name_kana_full | xml_ledger | name_kana_full_raw | 名称変更 | XML由来の氏名カナ原本値として保持する。 |
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
| medi_xml_ledger | judge_score | exam_check_results | check_score / summary | 再配置 | チェック詳細・集計値は `exam_check_results` 側へ寄せる。 |
| medi_xml_ledger | judge_note | exam_check_results | check_reason / reason | 再配置 | チェック詳細理由は `exam_check_results` 側へ寄せる。台帳側には `check_reason` のみ保持する。 |
| medi_xml_ledger | judged_run_id | xml_ledger | check_run_id | 名称変更 | 総合チェックを実施した `etl_runs` を参照する。 |
| medi_xml_ledger | judged_at | xml_ledger | checked_at | 名称変更 | 総合チェック実施日時として保持する。 |
| medi_xml_ledger | is_exam_result | xml_ledger | xml_status / xml_reason | 再配置 | 健診結果XMLとして扱えるかは `xml_status` / `xml_reason` に吸収する。独立フラグは持たない。 |
| medi_xml_ledger | is_legal_exam | exam_check_results | legal_status | 再配置 | 法定健診判定は `exam_check_results` へ寄せる。 |
| medi_xml_ledger | lsio_legal_is_complete | exam_check_results | legal_status | 再配置 | 法定健診のOK/NGは `exam_check_results` の詳細根拠とする。台帳側は `check_status` で結論を持つ。 |
| medi_xml_ledger | lsio_legal_required_count | exam_check_results | legal_required_count | 再配置 | 法定健診チェック件数として `exam_check_results` へ寄せる。 |
| medi_xml_ledger | lsio_legal_present_count | exam_check_results | legal_present_count | 再配置 | 法定健診実施件数として `exam_check_results` へ寄せる。 |
| medi_xml_ledger | lsio_legal_missing_methods | exam_check_results | legal_reason / missing_summary | 再配置 | 不足項目理由として `exam_check_results` へ寄せる。 |
| medi_xml_ledger | lsio_legal_judged_run_id | exam_check_results | legal_check_run_id | 再配置 | 法定健診チェックrunとして `exam_check_results` へ寄せる。 |
| medi_xml_ledger | lsio_legal_judged_at | exam_check_results | legal_checked_at | 再配置 | 法定健診チェック日時として `exam_check_results` へ寄せる。 |
| medi_xml_ledger | created_at | xml_ledger | created_at | 移行 | 台帳管理用監査項目。 |

## v2で新規追加するカラム候補

| v2カラム | 理由 |
|-----------|------|
| event_id | イベント単位で検索するため。 |
| file_receipt_id | 元ファイルへ辿るため。 |
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

- `xml_ledger` は XML 単位の業務台帳とする。
- `status` は `OK` / `WARNING` / `NG` の3状態を基本とする。
- `reason` は `status` が `WARNING` / `NG` となった理由コードとして保持する。
- `reason` は固定enumではなく、スクリプト実装・チェック追加に応じて理由コードを追加できる文字列カラムとする。
- `person_id_custom` / `identity_hash` は identity 共通仕様の説明書に従い、既存の共通生成処理を利用して生成する。
- raw値を直接独自ロジックで組み立てず、identity共通仕様に従う。
- `identity_hash` を唯一の加入者照合キーとし、一致した `subscribers.id` を `subscriber_id` として保持する。
- `hia_subscriber_id` は `subscriber_id` から取得し、検索性向上のための運用補助キーとして冗長保持する。
- identity共通生成処理で `ok=false` となる場合は、必要項目不足または正規化NGとして加入者照合NGにする。
- `insurance_symbol_raw` / `insurance_number_raw` / `name_kana_full_raw` はXML由来の原本値として保持する。
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
# 3. exam_item_values

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
| medi_xml_item_values | xml_sha256 | exam_item_values | ledger_id | 再配置 | v2ではXMLそのものではなく、`ledger_type = XML` / `ledger_id = xml_ledger.id` で由来を表現する。 |
| medi_xml_item_values | zip_sha256 | file_receipts | file_sha256 | 再配置 | 由来ファイル情報は `file_receipts` 側を正とする。 |
| medi_xml_item_values | zip_inner_path_sha256 | xml_ledger | xml_inner_path_sha256 | 再配置 | ZIP内XML識別補助は `xml_ledger` 側へ寄せる。 |
| medi_xml_item_values | zip_inner_path | xml_ledger | xml_inner_path | 再配置 | ZIP内XMLパスは `xml_ledger` 側へ寄せる。 |
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
- `event_id` / `subscriber_id` / `hia_subscriber_id` は検索性向上のため冗長保持する。
- `exam_item_values` は実際に存在した健診値のみを保持する。
- 制度チェックは、`exam_item_values` に存在する値だけでなく「存在しない項目」も判定材料とするため、`exam_check_results` 側の責務とする。
- 項目値としての妥当性（範囲外・形式不正等）は `validation_status` / `validation_reason` で保持する。
- `validation_status` は制度チェックではなく、値そのものの妥当性を表す。
- 正規化値・正規化状態は `exam_item_values` の責務とする。
- CSVからHIAアップロード用XMLを生成する場合も、`exam_item_values` の正規化済み値を利用する。