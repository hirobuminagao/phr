# health_exam_result リファクタリング 決定事項

## 決定

### 全体方針

- 現行実装（work_folder / kenshin_list_pydir）は参照実装とする。
- 既存構成は踏襲せず、業務責務に基づいて新システムを設計する。
- 既存ロジックは必要に応じて再利用するが、構造は新規設計とする。

---

### ディレクトリ構成

- `scripts/` は新システムのみ配置する。
- `data/` は新システムの実データのみ配置する。
- `data/` は健診結果処理に限らず、他システム・他スクリプトからも共通利用するデータ領域とする。
- `data/` は Git 管理対象外とし、環境固有の実データ・入出力データのみを配置する。
- `data/` にプログラム・ライブラリ・設定ファイルは配置しない。
- `work_folder`、`kenshin_list_pydir` は当面移動しない。
- 将来的に `legacy/` へ移動する可能性はあるが、今回は対象外とする。

---

### scripts

- 医療機関から受領した健診結果処理のスクリプトは `scripts/from_medical/` 配下に配置する。
- `scripts/lib/` は全システム共通ライブラリとする。
- `scripts/from_medical/script_lib/` は医療機関取込処理内の業務固有共通処理の配置先とする。
- `scripts/from_medical/config/` は医療機関取込処理用の設定ファイル配置先とする。
- `scripts/from_medical/` 直下には人が実行するオーケストラスクリプトのみ配置する。
- 人が実行するオーケストラスクリプトは `01_scan_files.py`、`02_import_xml.py`、`03_check_exam_results.py`、`04_export_hia_xml.py` の4本構成とする。
- 実処理は `script_lib` に実装する。
- `script_lib` 配下は直接実行しない。
- `script_lib` は全プロジェクト共通ライブラリではなく、`from_medical` 内で再利用する処理を置く場所とする。
- 全プロジェクト共通処理は、既存の共通基盤側へ寄せる。
- 業務固有処理は `health_exam_result` 側へ置く。
- 共通ライブラリは汎用的な技術処理のみを対象とする。
- 標準ライブラリの単純なラッパーは作成しない。
- 必要性が明確になった時点で共通ライブラリ化を判断する。

---

### DDL / migration

- 新規DBの初期DDLは `sql/ddl/<db_name>/` 配下へ配置する。
- 既存DBへの変更SQLは `sql/migrations/<target_db_name>/` 配下へ配置する。
- 初期データSQLは `sql/seed/<db_name>/` 配下へ配置する。
- migration の配置先は機能名ではなく、変更対象DB名を基準とする。
- migration ファイル名は `YYYYMMDD_NNN_<target_db_name>_<action>_<summary>.sql` を基本とする。
- `health_exam_result` は新規DBのため、初期テーブル作成SQLは migration ではなく `sql/ddl/health_exam_result/` 配下へ配置する。
- `health_exam_result` の初期DDLはテーブル単位ファイルとする。
- `health_exam_result` の初期DDLファイル名は `NNNN_health_exam_result__<table_name>.sql` を基本とする。
- 初期DDLの連番は既存 `sql/ddl/dev_phr/` の形式に合わせ、作成順・依存順が分かるように付与する。
- core DDLの初期作成対象は `etl_runs`、`etl_errors`、`medical_folder_aliases`、`file_receipts`、`xml_ledger`、`xml_file_links`、`exam_item_values` とする。
- `exam_check_results` は制度チェック方針を再確認した後にDDL化する。
- status系カラムはDB enumではなく `varchar` で定義する。
- `health_exam_result` 内のテーブル間FKは張る。
- `dev_phr` など外部DB・外部スキーマへのcross schema FKは張らない。
- `event_id`、`subscriber_id`、`hia_subscriber_id` など外部参照・検索用カラムは必要に応じてINDEXを付与する。
- `file_receipts.file_sha256` 単独UNIQUEは採用しない。
- `file_receipts` の重複防止は `event_id`、`relative_path`、`file_sha256` の組み合わせを基本とする。
- `xml_file_links` は `file_receipt_id`、`xml_ledger_id`、`xml_inner_path` の組み合わせをUNIQUEとする。
- 長尺文字列を含む複合UNIQUE制約は、DDL実装ではSHA256生成列を利用して実現する。
- SHA256生成列の採用はMySQL実装上の制約回避を目的とした物理実装であり、論理設計で定義した一意キーは変更しない。
- `exam_item_values.normalized_value` は `text` とする。
- `medical_folder_aliases` の一意制約は `UNIQUE(event_id, src_folder_raw)` とする。
- `medical_folder_aliases.dst_folder_norm` には一意制約を設けず、複数の実フォルダ名から同一名称への集約を許可する。
- `medical_folder_aliases` のインデックスは、初期実装では `event_id` および `UNIQUE(event_id, src_folder_raw)` によるものを基本とする。
- `medical_folder_aliases.is_active` の初期値は `1` とする。
- `medical_folder_aliases.manual_judgement` の初期値は `0` とする。
- 仮名称等の補足情報は `medical_folder_aliases.note` に保持し、`manual_judgement` の判定条件とはしない。
- `medical_folder_aliases` 初期データSQLを作成する。
- `medical_folder_aliases` 初期データの元資料は `docs/spec/health_examinations/03_medical_folder_aliases_initial_data_v2_0_0.md` とする。
- `medical_folder_aliases` 初期データは `event_id = 2` の188件を投入対象とする。
- `medical_folder_aliases` 初期データでは原則 `src_folder_raw = dst_folder_norm` とする。
- `medical_folder_aliases` 初期データでは、補足がある行のみ `note` に値を入れ、補足なしは `NULL` とする。
- `medical_folder_aliases` 初期データSQLの配置先は `sql/seed/health_exam_result/` とする。
- `medical_folder_aliases` 初期データSQLのファイル名は `0010_health_exam_result__medical_folder_aliases_event2.sql` とする。
- `medical_folder_aliases` 初期データSQLは `INSERT ... ON DUPLICATE KEY UPDATE` で再実行可能にする。
- 初期データSQL再実行時の更新対象は `dst_folder_norm`、`note`、`is_active`、`manual_judgement`、`updated_at` とする。
- `medical_folder_aliases.created_at` は初回INSERT時のみ設定する。
- `medical_folder_aliases.alias_id` は自動採番に任せ、seed SQLでは明示投入しない。
- `dev_phr.event.result_root_path` は migration で追加する。
- `dev_phr.event.result_root_path` の型は `text` とする。
- `dev_phr.event.result_root_path` は `NULL` 許可とする。
- `dev_phr` のマスタ拡張や event カラム追加は `sql/migrations/dev_phr/` 配下へ配置する。
- `work_other` を変更する場合は `sql/migrations/work_other/` 配下へ配置する。
- migration の日付は厳密な作成日ではなく、適用順を把握するための管理日として扱う。

---

### システム責務

健診結果処理は以下の責務で設計する。

1. 医療機関
2. 受領
3. 必要に応じた健診結果作成
4. 結果管理
5. 保険者変換
6. 納品

---

### health_exam_result v2 基本方針

- 今回の移設・リファクタリング後の新システムを `health_exam_result v2` とする。
- v2 は現行システムの構成をそのまま踏襲せず、業務責務を基準として再設計する。
- v2 初期実装は XML 品質保証基盤を中心とする。
- 初期実装では `file_receipts`、`xml_ledger`、`exam_item_values`、`exam_check_results` を中心に構成する。
- `zip_receipts` は初期実装では独立テーブルとせず、`file_receipts.file_type` による種別管理で対応する。
- `file_receipts` は入力ファイルだけでなく、将来的な出力ファイルも含めた物理ファイル資産台帳とする。
- `file_receipts` は物理ファイル単位の台帳とする。
- `file_type` は `02_健診結果（編集）` に配置された時点の投入ファイル種別とする。
- `is_zip` は独立カラムとして採用しない。
- `zip_has_xml` は採用せず、処理対象件数は `processable_count` に一般化する。
- `zip_xml_count` は `processable_count` に置き換える。
- `zip_xml_checked_at` は `content_checked_at` に置き換える。
- `processable_count` は処理対象として認識したデータ件数とし、0件はエラー扱いとする。
- ZIPの場合、`processable_count` はXML件数とする。
- XML単体の場合、`processable_count` は通常1とする。
- CSVの場合、`processable_count` は設定に従って算出したデータ行数とする。
- CSV直取込は初期実装の対象外とし、紙・CSVは別処理でXML化して投入する。
- `xml_ledger` はXML内容の一意台帳とし、`xml_sha256` を一意性判定の基準とする。
- 同一 `xml_sha256` のXMLは `xml_ledger` に重複作成しない。
- `xml_ledger.file_receipt_id` は持たない。
- `duplicate_of_xml_ledger_id` は採用しない。
- 物理ファイルとXML内容の対応は `xml_file_links` で管理する。
- 別ZIP等で同一XMLを再受領した場合は、`xml_file_links` のみ追加し、item抽出・制度チェックは再実行しない。
- ZIP全体の重複は `zip_sha256` で判定し、同一ZIPは処理対象外とする。
- `xml_file_links.xml_inner_path` はZIP内相対パスを保持する。
- XML単体ファイルの場合、`xml_file_links.xml_inner_path` は `NULL` とする。
- `exam_item_values` は健診結果値の共通基盤とする。初期実装では XML 由来を対象とし、将来的に CSV 由来も同一構造で扱う。
- `exam_item_values` は実際に存在した健診値のみを保持する。
- `exam_item_values` は縦持ちとする。
- `exam_item_values` の由来は `ledger_type` / `ledger_id` で表現する。
- `exam_item_values` には `event_id`、`subscriber_id`、`hia_subscriber_id` を検索性向上のため冗長保持する。
- `exam_item_values` の正規化値・正規化状態は `exam_item_values` の責務とする。
- 項目値としての妥当性チェックは `exam_item_values` で管理する。
- 法定健診・特定健診の不足判定結果は `exam_item_values` に保持せず、`exam_check_results` で管理する。
- `exam_check_results` は同一性項目コード単位の横持ちとする。
- `exam_check_results` は項目ごとに `status` / `reason` を持つ。
- 制度チェックは、項目単位の判定と制度単位の総合判定を分離する。
- `exam_check_results` は結果のみ保持し、判定ルールはマスタで管理する。
- `exam_check_results` の横持ち対象は `docs/spec/health_examinations/02_exam_check_item_spec_v2_0_0.md` の72項目を正とする。
- まず統合された制度チェック対象72項目について、同一性項目コード単位で `exam_check_results` に横持ちの `status` / `reason` を記録する。
- 72項目の項目別 `status` / `reason` は、法定健診・特定健診で二重に持たない。
- `exam_check_results` の項目別 `status` は `OK`、`CALCULATED`、`ALTERNATIVE`、`MISSING`、`INVALID` の5種類とする。
- `exam_check_results` の項目別 `reason` は特記事項のみ保持し、`OK` の場合は `NULL` とする。
- XML処理結果ログおよび医療機関向けメッセージは、`reason` が `NULL` ではない項目を集約して生成する。
- 法定健診・特定健診の総合判定は、72項目の `status` をもとに、それぞれの制度グループ定義に従って `OK` / `WARNING` / `NG` を集計する。
- 法定健診・特定健診の総合判定は `exam_check_results` を唯一の入力として算出し、XML や `exam_item_values` を直接参照しない。
- 項目ごとの判定結果は `exam_check_results` の項目別 `status` / `reason` が保持する。
- `check_result` は `exam_check_results` の項目別 `status` を制度グループ単位で集計した最終判定とする。
- 制度チェック総合判定は、`exam_check_results` の制度判定結果から `xml_ledger.check_status` を生成する。
- 法定健診・特定健診の制度チェック総合判定は以下とする。
  - 法定OK・特定OK → `OK`
  - 法定OK・特定WARNING → `WARNING`
  - 法定NG → `NG`
- 特定健診不足は `WARNING`、法定健診不足は `NG` とする。
- `ANY_NONEMPTY` は presence 判定ルールとして扱い、対象 `namecode` 群のうち1つ以上に有効値が存在すれば充足とする。
- `ANY_NONEMPTY` は行が存在するだけでは充足とせず、`NULL`・空値・無効値は充足扱いしない。
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
- 採用されなかった案・比較案としての Alternative は `05_design_history.md` で管理し、`03_decisions.md` には採用された最終決定のみを記載する。
- 旧 `LSIO_Legal_Item` は v2 の正ではなく、差分確認・参考資料として扱う。
- `dev_phr.exam_item_group_*` は migration 対象とし、必要差分のみ追加・修正する。
- マスタ構成としては、共通72項目用グループ、法定健診判定用グループ、特定健診判定用グループを分けて扱う方針とする。
- 共通72項目用グループは、`exam_check_results` の項目別 `status` / `reason` を生成するために利用する。
- 法定健診判定用グループと特定健診判定用グループは、制度単位の `check_result` を集計するために利用する。
- 特定健診用グループは、初期実装ではマスタ未投入でも動作可能な構成とし、後でマスタを投入すれば判定できるようにする。
- v2初期では `exam_item_group_identity_members` への追加カラムは行わず、既存カラムを利用する。
- 制度チェックの判定ロジックは `03_check_exam_results.py` に集約する。
- DBはどのルールを使うかを管理し、スクリプトはそのルールをどう判定するかを実装する。
- 法定健診チェックは主判定とする。
- 特定健診チェックは原則 warning / 参考判定とする。
- 特定健診チェック結果だけを理由に、健診機関へ再提出要求する運用にはしない。
- `check_status` と `xml_export_status` は分離する。
- 手動承認で出力可にしても `check_status` は変更しない。
- `event` の概念は v2 初期から利用する。
- 初期実装では設定ファイルから `event_id` を指定して処理を実行する。
- v2処理では、対象 `event_id` の `result_root_path` が未設定の場合はエラーとする。
- 新規DB名は `health_exam_result` とする。
- `dev_phr` は原則参照とする。ただし、人・イベント・共通マスタなど `dev_phr` が正となるテーブルは、必要に応じてテーブル単位で書き込みを許容する。
- 処理系・台帳系テーブルは `health_exam_result` に配置する。
- v2初期処理は XML/受診者単位で一気通貫に実行する。
- 処理順序は「基本情報抽出 → 加入者照合 → 健診項目抽出 → 項目単位チェック → XML/受診者単位チェック集約」を基本とする。
- ファイル単位の件数・エラー数・サマリー更新は、XML/受診者単位処理の完了後に集約する。
- 将来的にCSV直取込へ対応する場合は、`csv_row_ledger` を追加し、基本情報Ledgerと健診結果値を分離した構造とする。
- status はシステム処理ステータスと業務フローステータスを分けて設計する。
- `file_receipts.status` は `DISCOVERED / IMPORTING / IMPORTED / ERROR` の4状態で管理する。
- `xml_status` は `02_import_xml.py` のXML取込状態を表し、`PENDING / IMPORTED / ERROR / SKIPPED` の4状態で管理する。
- `xml_ledger` に `xml_status` / `xml_reason` を保持する。
- `xml_status` / `xml_reason` は、XML読込エラー、Namespaceエラー、XMLフォーマットエラー、基本情報不足、加入者照合不可、その他XML単位で出力対象外となる理由をまとめて扱う。
- XML単位の詳細ステータスは初期実装では持たない。
- `check_status` は `03_check_exam_results.py` の制度チェック状態を表し、`PENDING / OK / WARNING / NG` の4状態で管理する。
- `xml_export_status` は `04_export_hia_xml.py` のHIA出力状態を表し、`PENDING / READY / EXPORTED / ERROR / SKIPPED` の5状態で管理する。
- v2初期では `xml_export_status` を `xml_ledger` に保持し、XML単位の最新出力状態を管理する。
- 重複ファイルは、同一物理ファイルを再検出した場合を指す。
- 重複ファイルは `file_receipts` に新規登録しない。
- 重複件数は `etl_runs` のスキップ件数・実行サマリーで管理する。
- 医療機関から再提出された修正版ファイルは、同一人物・同一健診結果に関係する場合でも、別の受領ファイルとして `file_receipts` に新規登録する。
- 元ファイルと再提出ファイルの親子関係・世代管理は将来フェーズで検討する。
- `work` 配下は処理中だけ利用する一時作業領域とする。
- 処理完了後、`work` 配下にはコピー・展開済みファイルを残さない。
- デバッグ時のみ `--keep-work` のような明示オプションで `work` を一時保持できる。
- `01_scan_files.py` は対象フォルダを毎回フルスキャンし、未登録ファイルのみ `file_receipts` へ登録する。
- `01_scan_files.py` はファイル検出と `file_receipts.status = DISCOVERED` 登録に責務を限定する。
- Phase3 `01_scan_files.py` の初期登録対象ファイルは ZIP / XML とする。
- CSVは初期実装では `file_receipts` に登録しない。
- CSVは将来対応時にスキャン対象へ追加し、その時点から `file_receipts` へ登録する。
- `file_sha256` はPhase3スキャン時に計算する。
- `processable_count` はPhase3では設定せず `NULL` とする。
- Phase3登録時の `file_role` は `FROM_MEDICAL` とする。
- Phase3初期実装で登録対象とする `file_type` は `ZIP / XML` とする。
- `file_type = OTHER` は初期実装では登録対象としない。
- CSV対応時に `file_type = CSV` を追加する。
- Phase3登録時の `storage_folder_type` は `MEDICAL_RESULT_ROOT` とする。
- `relative_path` は `event.result_root_path` からの相対パスとする。
- Phase3の重複判定は `event_id`、`relative_path`、`file_sha256` を基準とする。
- 未知フォルダ、`is_active = 0` alias、`manual_judgement = 1` alias はPhase3ではスキップし、必要に応じて `etl_errors` に記録する。
- 隠しファイル、一時ファイル、対象外拡張子は `file_receipts` に登録しない。
- 対象外ファイル（CSV、隠しファイル、一時ファイル等）は原則スキップし、`etl_errors` にも記録しない。
- Phase3の `etl_errors` は運用上対応が必要な事象のみ記録する。
- Phase3の `etl_errors` 記録対象は、未知フォルダ、無効alias、`manual_judgement = 1` alias などを基本とする。
- Phase3の `etl_errors.error_type` / `error_code` は必要最小限のみ定義し、将来必要に応じて拡張する。
- Phase3の `etl_runs.run_type` は `SCAN_FILES` とする。
- Phase3の `etl_runs.status` は `RUNNING / SUCCESS / WARNING / ERROR` とする。
- `etl_errors.status` は `OPEN / RESOLVED` とする。
- Phase3のscan結果サマリーは標準出力に表示し、可能な範囲で `etl_runs.summary_message` に記録する。
- `summary_message` は人間が読みやすい短いテキストとし、JSON等の構造化データは採用しない。
- `01_scan_files.py` は未登録ファイルに `etl_run_id` を付与し、そのRunを `02_import_xml.py` の入力とする。
- `02_import_xml.py` は指定 `etl_run_id` の未処理 `file_receipts` を対象に、Run単位で処理する。
- `02_import_xml.py` のDBトランザクションは `file_receipt` 単位とする。
- 1ファイル失敗してもRun全体は止めず、失敗分を `etl_errors` に記録して次のファイルへ進む。
- ZIP展開、XML読込、XML基本情報抽出、加入者照合、健診項目値抽出、`xml_ledger`・`xml_file_links`・`exam_item_values` 登録は `02_import_xml.py` で一括実施する。
- `03_check_exam_results.py` はXMLファイルを再読込せず、`xml_ledger`・`exam_item_values` を入力として制度チェックを実施する。
- `03_check_exam_results.py` は `exam_check_results` を更新し、その結果を `xml_ledger` へ集約する。
- `04_export_hia_xml.py` はDB上のチェック結果・出力可否を参照してHIAアップロード用XMLを生成する。
- HIAアップロード用XMLは `<event.result_root_path>/<医療機関フォルダ>/03_健診結果（アップロード）/yyyymmdd_hhmmss_<run_id>/<xxx.zip>` へ出力する。
- 既存出力ファイルは上書きしない。
- 出力済みファイルの削除・整理は運用側の責務とする。
- 出力履歴はRun単位の出力フォルダを証跡とする。
- `exam_item_values.normalized_value` / `normalized_unit` は `02_import_xml.py` の登録処理内で生成する。
- SHA256計算は共通ライブラリ化せず、各処理内で実装する。
- 最終的には、人＋イベント単位の状態管理台帳を追加する方向とする。
- 人＋イベント単位の台帳では、その人の健診イベントが最終的にOKか、確認中か、再提出依頼中か、完了かを管理する。
- 欠損XMLと再提出XMLは別々の `xml_ledger` として保持する。
- 欠損XMLが存在しても、再提出XMLにより必要情報が揃った場合は、人＋イベント単位の状態をOKまたは完了にできる。
- `xml_ledger` と `file_receipts` は人＋イベント単位の最終完了状態を背負わない。
- ETL実行管理は共通仕様とし、`etl_runs`・`etl_errors` は ADR-0023 に従う。
- ETLテーブルは実行管理のみを担当し、業務状態やシステム状態は保持しない。
- 設計の正式決定事項は `03_decisions.md` に集約し、`05_design_history.md` は議論・経緯の履歴として扱う。
- 実装・設計資料は `03_decisions.md` を基準に更新し、チェックリスト専用ファイルは作成しない。

---

### 保険者変換

- 保険者固有処理は共通処理と分離する。
- `medi_trans_06139463.py` は参照実装とする。
- namespace補正、match等の共通処理は共通ライブラリへ切り出す。
- 保険者番号固定、記号半角化等は保険者固有処理として実装する。

---

## 保留

- `xml_status` / `check_status` / `xml_export_status` の reason code 詳細
- XML単位の詳細ステータス（項目別・工程別）の追加要否
- `INVALID` に入れる不正理由の詳細表現
- `dev_phr.event.result_root_path` migration の正式ファイル名
- `result_root_path` の初期値を既存 `event_id = 2` へ設定するか、別途手動更新とするか
- seed SQL 内の188件データの最終確認
- `person_event` を初期実装に含めるかの最終判断
- 人＋イベント単位の状態管理台帳の正式名称・初期実装範囲・状態値
- 欠損XML・再提出XML・最終採用XMLの関係の表現方法
- Exportプラグイン構成
- 将来の出力履歴台帳の要否
