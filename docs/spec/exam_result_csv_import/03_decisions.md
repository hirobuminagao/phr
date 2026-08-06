# phr_master Decisions

## Status

Current as of 2026-08-05.

このドキュメントは `02_02_exam_result_csv_import` の前提となるマスタ整備、および `phr_master` 新設に関する採用済み決定事項を管理する。
協議経緯は `05_design_history.md` に記録し、本ファイルには実装・DDL・seed・migration の前提にできる内容だけを反映する。
実装到達点と採用済みだが未実装の範囲は `33_implementation_status_and_xml_handoff.md` を参照する。

## Decisions

### Primary Objective

- 本 spec の主目的は `02_02_exam_result_csv_import` の設計前提を固めることである。
- `phr_master` 新設は、CSV取込に必要な健診機関マスタ、CSVフォーマット、CSV列マッピング、結果値normalize辞書を整理するための前提整備として扱う。
- `phr_master` 作成そのものを本丸とはしない。

### DB Name

- マスタ用DB名は `phr_master` とする。

### Role

- `phr_master` は、PHR全体で参照する共通マスタを管理するDBとする。
- `phr_master` は、個人情報・健診結果値・受領ファイル・ETL実行結果などの業務トランザクションを保持しない。
- `phr_master` は、将来的な複数人運用における権限分離の境界として利用する。

### Initial Boundary

- `dev_phr` は、加入者、個人、イベント、実体データを中心に保持する。
- `health_exam_result` は、健診結果受領、取込、チェック、状態管理などの業務データを保持する。
- `work_other` は、既存運用・一時処理・移行前資産の作業用DBとして扱う。
- `phr_master` は、共通マスタ、コード、辞書、機関情報を保持する。

### Exam Facility Master

- 医療機関と健診機関は明確に分ける。
- 今回扱うのは健診結果CSV取込に必要な「健診機関」であるため、マスタ名・ドキュメント上の呼称は健診機関寄りにする。
- 健診機関そのものを表す親マスタを `phr_master` 側に作成する方針とする。
- 健診機関マスタの正式テーブル名は `exam_facilities` とする。
- 現行の `medical_folder_aliases` は、健診機関そのものではなく、受領フォルダ名の揺れ・イベント別配置を扱う子情報として整理する。
- `medical_folder_aliases` は、このCSV取込前提整備のタイミングで `phr_master` 側へ移す方向とする。
- aliasテーブル名は既存の `medical_folder_aliases` をそのまま使う。
- 現行の `health_exam_result.medical_folder_aliases` 登録内容は、原則そのまま新しいaliasテーブルへ移す。
- aliasテーブルには新しい健診機関マスタのIDを保持する。
- `01_scan_files.py` は、フォルダaliasから健診機関IDを確定し、後続処理へ引き継げる状態で `file_receipts` へ登録する。
- `01_scan_files.py` は、CSVファイルについてはscan時に登録済みCSV formatとの照合も行い、`actual_header_sha256` / `actual_character_encoding` / `matched_csv_format_version_id` / `status` へ反映する。
- `file_receipts` には、CSV取込の後続処理が参照できるように `exam_facility_id` を追加する。
- 健診機関コード・名称のスナップショットは、既存 `file_receipts.facility_code` / `facility_name` を利用する。
- フォルダaliasから健診機関を解決する処理は、個別スクリプトへSQLを直書きせず、`scripts/lib/db/lookup/exam_facility.py` の共通lookup libとして追加する案を基本とする。
- lookup libはフォルダ名、イベントID、マスタDB名を入力し、`exam_facility_id`, `exam_facility_code`, `exam_facility_name` を返す案を基本とする。
- 同じlookup libに、`exam_facility_id` から健診機関handleを返す関数と、`exam_facility_code` から健診機関handleを返す関数も追加する案を基本とする。
- lookup libの各入口は、返却キーとして `exam_facility_id`, `exam_facility_code`, `exam_facility_name`, `exam_facility_display_name`, `medical_institution_code` を揃える案を基本とする。

### CSV Import Impact

- `02_02_exam_result_csv_import` では、受領ファイルから健診機関を特定し、健診機関に応じたCSVフォーマット・マッピングを選択する必要がある。
- CSVフォーマット定義、CSV列マッピング、結果値変換辞書は `phr_master` 側に置く。
- CSVマッピングは、健診機関ID、mapping version、CSV項目、変換ルールを持つ構成を基本とする。
- `01_scan_files.py` から `file_receipts` へCSV取込に必要な健診機関情報を引き渡す設計は、`02_02_exam_result_csv_import` の前提として別途整理する。
- CSV取込では、`exam_item_values` 登録時にnormalize処理まで組み込む。
- CSV取込時のnormalizeは、CSV由来のraw値を入力とし、その結果を `exam_item_values` のnormalize系カラムへ反映する。
- 健診結果値normalizeはCSV取込専用実装にせず、`scripts/lib/examination/value_normalizer.py` の共通libとして作成する案を基本とする。
- normalize共通libの主APIは、`namecode` とraw値を入力し、`normalized_value`, `normalized_unit`, `normalize_status`, `normalize_reason`, `validation_status`, `validation_reason` を返す `normalize_exam_item_value()` とする案を基本とする。
- `namecode` から型・単位を返すlookupは、既存 `scripts/lib/db/lookup/exam_item_master.py` の `get_exam_item()` / `get_exam_items()` を利用する案を基本とする。
- CD/CO系の結果値名寄せは `phr_master.norm_variants` を利用する。
- `norm_variants` 参照は個別スクリプトへSQLを直書きせず、`scripts/lib/db/lookup/norm_variant.py` の共通lookup libとして追加する案を基本とする。
- 初期案では、CD/CO系かつ `exam_item_master.result_code_oid` が存在する場合のみ、`result_code_oid + raw_value_utf8` の完全一致で `norm_variants` を引く。この完全一致はbinary比較とし、`-` / `－` / `ー` などをSQL照合順序で同一扱いしない。
- CD/CO系で `norm_variants` に一致してOKになった場合、`normalize_reason` に `RAW_VALUE_EXACT_MATCH` または `RAW_VALUE_NORMALIZED_MATCH` を入れ、原文一致でOKになったのか、前処理後トークンでOKになったのかを区別する。
- CD/CO系で `norm_variants` に一致しない場合は、`normalize_status = ERROR`, `validation_status = INVALID`, `validation_reason = NORMALIZE_VARIANT_NOT_FOUND` とする案を基本とする。
- `norm_variants` は旧「紙→Excel→DB 2テーブル直接投入→normalize→export」フローで実利用されている資産であり、今後はnormalize共通libから参照する。
- `norm_variants` はCSV健診結果取込で必要な共通マスタとして `phr_master` 初期DDLに含める。テーブル責務はCD/CO系結果値名寄せ辞書とする。
- 旧 `dev_phr.norm_variants` の廃止タイミングは今回決めない。CSV取込実装後、参照切替と運用影響を確認してから別途判断する。
- `norm_variants` は、既存辞書をそのまま移すだけでなく、実CSVで必要な表記揺れを確認して追加する。
- ヒロオカCSVで確認した追加候補は `23_hirooka_clinic_pattern_a_review.md` の `norm_variants Coverage` に整理する。
- 辞書追加は「取り込むと決めた実値」のみ行う。ヘッダー表記揺れと同じく、CD/CO結果値もシステム側で憶測吸収しない。
- 検証目的で、意図的に未登録のCD/CO値を残したテストケースを用意し、`NORMALIZE_VARIANT_NOT_FOUND`、辞書追加、同一CSV再取込で正常化される流れを確認する。
- CSV由来raw値に含まれうる `未実施`, `測定不能`, `未受診` などの非測定値語は、CD/CO系 `norm_variants` とは別に、型に依存しない共通前処理として `value_normalizer` 側で扱う。
- 非測定値語辞書は初期実装ではYAMLファイルとして管理し、DBテーブル化は後続課題とする。
- `異常なし`, `所見なし`, `あり`, `なし`, `陽性`, `陰性`, `+`, `-` は項目によって結果値として意味を持つため、非測定値語辞書には入れず、CD/COなら `norm_variants`、STなら文字列正規化で扱う。
- 完全空セルは `exam_item_values` 行を作らない。
- `未実施` / `測定不能` / `判定不能` などは、健診機関由来のABC等の健診判定ではなく、entry内の項目結果値そのものが示す実施状態・測定可否として扱う。
- そのため、健診機関由来の健診判定を初期実装で保持・利用しない方針の影響を受けず、`exam_item_values.raw_value` とnormalize状態に残す。
- `未実施`, `未受診`, `実施せず`, `キャンセル`, `ｷﾔﾝｾﾙ`, `中止`, `拒否`, `対象外` など、実施されていないことを示す語は、元値を `exam_item_values.raw_value` に残し、`normalize_status = SKIPPED`, `normalize_reason = RAW_VALUE_NO_RESULT`, `validation_status = WARNING` として扱う。
- `測定不能`, `判定不能`, `検体不良`, `採血不可`, `測定不可` など、測定できなかったことを示す語は、元値を `exam_item_values.raw_value` に残し、`normalize_status = SKIPPED`, `normalize_reason = RAW_VALUE_UNMEASURABLE`, `validation_status = WARNING` として扱う。
- 型に合わない未知文字列は、元値を `exam_item_values.raw_value` に残し、`normalize_status = ERROR`, `normalize_reason = INVALID_VALUE_TYPE`, `validation_status = INVALID` として扱う。
- 数値系項目の `<0.1` / `0.1未満` / `1.005以下` のような比較付き表現は、XMLのPQで比較演算子を表現できないため、元値を `exam_item_values.raw_value` に残し、数値部を `exam_item_values.normalized_value` に保持する。
- 下限未満表現を数値部へ寄せた場合は、`normalize_status = OK`, `normalize_reason = RAW_VALUE_NUMERIC_COMPARATOR_NORMALIZED`, `validation_status = VALID` とする。
- `あり` / `なし` は結果値として意味を持つ可能性があるため、初期の共通ノイズ辞書には含めず、項目別ルール、CD/CO辞書、または変換ルールで扱う。
- 数値系 `data_type` は初期実装では `PQ`, `INT`, `REAL` とする。現行exportの数値型は `PQ` だが、旧 `norm_rules` との互換として `INT` / `REAL` も受ける。
- `raw_unit` と `item_master.unit` が異なる場合、初期実装では単位変換せず、`normalize_status = ERROR`, `normalize_reason = UNIT_MISMATCH`, `validation_status = INVALID` とする。
- CSV値の機械的な前処理は、`identity_hash` と同じくDBルールではなく共通lib側へ寄せる。
- 処理順は、完全空値判定、共通base normalize、非測定値語判定、型別normalize、単位チェックとする。
- `transform_rule_code` は初期DDLに含めない。将来、項目別明示変換が必要になった時点で用途名と仕様を決めてmigration追加する。
- 所見有無CDと所見本文STの組合せを施設別テンプレートで明示できるよう、mapping ruleは `value_source_type = SOURCE / FIXED`、`fixed_value`、`value_join_separator`、`value_exclude_values` を持つ。
- `FIXED` は行条件が成立した場合だけ明示した固定値を生成する。医学的意味を文言から推測して固定値を決めない。
- `SOURCE` で複数の `VALUE` 列を指定する場合は `value_join_separator` を必須とし、空欄列と `value_exclude_values` に一致する値を除外してCSV列順に結合する。
- `value_exclude_values` は改行区切りで保持する。元行証跡は `exam_ledgers.raw_row_json` に残るため、`exam_item_values.raw_value` には取込結果として有用な値だけを残す。
- ハートクロスへこの所見有無CD/STルールを適用するかは健診機関回答後に決定し、回答前のseedには反映しない。
- XML取込で所見本文STが存在し、対応する所見有無CDが存在しない場合は、XML原本の不備として `exam_item_values` に補完CDを追加する。補完値は `raw_value = 所見あり`, `raw_value_type = CD`, `code_value = 1` とし、元ST行はそのまま保持する。
- 補完対象は、既往歴、自覚症状、他覚症状、心電図、胸部X線、眼底、胃部X線、胃内視鏡、腹部超音波、乳房視触診、マンモグラフィ、子宮頸部細胞診、婦人科診察の所見本文STと対応する所見有無CDの組み合わせとする。
- `norm_variant` lookupは単品APIと一括APIの両方を持つ。CSV取込では一括APIで事前取得し、単品APIは少量処理・テスト・再normalize用に使う。
- CSV直取込では、CSVデータ行単位の台帳として `health_exam_result.exam_ledgers` を使う。
- CSV由来の `exam_item_values` は `ledger_type = 'EXAM'`, `ledger_id = exam_ledgers.exam_ledger_id` で由来を表す。
- CSVに保険者番号を持たない施設フォーマットでは、`file_receipts.insurer_number` を `exam_ledgers.insurer_number` と加入者identity生成の入力に利用する。
- `03_00_check_imported_exam_ledgers.py` はXML由来とCSV由来の `exam_ledgers` を対象にする。
- check結果は source単位では `exam_check_results.ledger_type = 'EXAM'` とし、`exam_ledger_id` に紐づける。
- CSV/XML由来のcheck状態は `exam_ledgers.check_status` / `check_reason` に反映する。
- 保険記号・保険番号は加入者identity生成に必要なため、施設フォーマット側にない場合は本番取込前の整形でCSV末尾へ追加する方針とする。
- `csv_loader` はCSV読込の共通部品として利用し、mapping適用、rule実行、normalize、identity生成、加入者照合は `csv_loader` の責務外とする。
- CSVヘッダー読取は既存 `scripts/lib/csv/csv_loader.py` の `load_csv()` / `CSVLoader.get_headers()` / `CSVLoader.get_header_dict()` を利用する案を基本とする。
- 既存 `csv_loader` 実装と `docs/spec/common_lib/csv_loader.md` の想定APIには差分があるため、既存利用スクリプトに影響を与えない追加APIとして `CsvLoadResult` / `CsvHeaderSet` 形式へ拡張する案を基本とする。
- 既存 `load_csv()` の戻り値は `CSVLoader` のまま維持し、構造化結果が必要な処理向けに `load_csv_result()` などの追加APIを作る案を基本とする。

### Unified Exam Ledger, Export Case, and Person Event

- CSV→XML出力まで一通り動作確認できたため、今後の取込、補正、再突合、法定check、XML出力、出力画面、HIAアップロード状態管理は、`exam_ledgers`、結合出力用case、`person_event` の3層に分けて扱う。
- 既存 `xml_ledger` / `csv_row_ledger` は直ちに廃止しない。移行完了までは取込済みデータの移行元、既存スクリプトの後方互換、再scan/再import時の由来保持として扱う。
- 最終的な通常取込では、XML由来もCSV由来も `exam_ledgers` へ登録する。XMLならXML内の1人分、CSVならCSV 1行、紙入力なら紙入力1人分を `exam_ledgers` 1件として扱う。
- `exam_ledgers` は `xml_ledger` / `csv_row_ledger` の統合版であり、ファイルまたは行由来の取込結果1件単位のledgerとする。
- `exam_ledgers` は `source_type = XML / CSV / PAPER`、source file/row情報、`file_receipt_id`、`event_id`、加入者突合情報、基本情報、検査値処理状態、source単位の法定check状態を持つ。
- `exam_item_values` は `exam_ledgers.exam_ledger_id` に紐づく検査値source値として扱い、raw、normalize、validation、由来、エラーを保持する。
- 既存個別ledgerから `exam_ledgers` へのbackfillは `sync_exam_ledgers.py` で行う。通常運用ではimport完了時に該当 `exam_ledgers` が更新され、`sync_exam_ledgers.py` は初回移行、復旧、再構築用に下げる。
- XML/CSV固有の原本証跡は、移行完了までは既存個別ledgerまたはsource detailへ残す。新規の業務処理・画面・check・補正・出力制御は `exam_ledgers` を参照する。
- 個別ledgerの廃止タイミングは、`exam_ledgers` ベースの取込、check、補正、XML出力が実行環境で安定してから決める。
- `exam_ledgers` の上位に、eventに対する人単位の現在状態を管理する既存 `dev_phr.person_event` を使う。
- 増減しやすい状態項目、check集計、出力状態、HIAアップロード状態、要対応理由は `dev_phr.person_event_status_items` に縦持ちする。
- `person_event` は汎用イベント管理ではなく、健診イベントに対する人単位の進捗・確認状態に限定して使う。
- `person_event` の母集団は結果受領者ではなく、`event_id` から解決した保険者番号に一致する `dev_phr.subscribers` 全員とする。
- 資格喪失者も `person_event` 母集団から除外しない。資格喪失日は除外条件ではなく、状況確認・資格状態判定のための状態項目として扱う。
- 予約申込、受診、結果ファイル受領、健診結果check、HIA状態、健保・事業所納品状態は、eventごとの人チェック項目として `person_event_status_items` に集約する。
- 年度内複数受診や特殊健診を想定し、`person_event` は人×eventの親、個別の受診・結果は `exam_ledgers` または結合出力用caseの複数件として扱う。
- 未突合ledgerはまだ人として確定していないため `person_event` を作らず、`exam_ledgers` 側の未突合状態として扱う。
- HIAダッシュボードCSV由来の最新状態は `work_other.hia_dashboard_status`、年度最終状態は `work_other.hia_dashboard_year_end_status`、健診eventに対する人チェック状態は `person_event_status_items` として分ける。
- 2025年度のHIA年度最終状態は `hia_dashboard_year_end_status` へスナップショット保管済みである。
- 進行中年度のHIA状態は `hia_dashboard_status` を入力にできるが、過年度eventの状態判定に最新テーブルを直接使わない。
- HIAダッシュボードCSVの新フォーマットでは先頭にHIA加入者IDが追加されているため、HIA加入者IDが存在する場合は加入者照合の第一候補として扱う。
- source単位の法定check結果は `exam_check_results` に残し、集計結果を `exam_ledgers.check_status` / `check_reason` へ戻す。
- source単位の法定checkは、人が `--ledger-type` を指定しなくてよいように `03_00_check_imported_exam_ledgers.py` を通常入口とする。
- 結合出力用caseの法定checkは `03_04_check_exam_export_cases.py` を通常入口とする。
- `03_check_exam_results.py` は廃止し、source単位と結合出力用case単位の入口を分ける。
- 複数の `exam_ledgers` を組み合わせてXML出力する場合は、結合出力用caseを作る。結合出力用caseは、人単位・1回分健診・XML出力候補を表し、出力OK/NG、結合状態、手動許可、XML出力状態を持つ。
- 結合出力用caseの構成元は `exam_export_case_sources` 相当のテーブルで `exam_ledgers` を複数保持する。
- 結合出力用caseの採用済み整値は `exam_export_case_values` 相当のテーブルで保持する。raw値は持たず、XML出力に必要な最小限の採用済み値、採用元 `exam_item_values.id`、採用理由、採用状態を持つ。
- 結合出力用caseの法定checkは、採用済み整値 + `exam_item_master` に対して行い、結果を結合出力用caseへ戻す。
- XML出力候補判定は結合出力用case、出力後の業務状態管理とHIAアップロード状態は `person_event` / `person_event_status_items` の責務とする。
- 結合出力用caseの人が見る総合状態は `exam_export_cases.export_readiness_status` / `export_readiness_reason` に持つ。
- `export_readiness_status` は少なくとも `EXPORT_READY`, `APPROVED_WITH_REASON`, `BLOCKED`, `WAITING_VALUES`, `WAITING_CHECK`, `EXPORTED`, `EXPORT_ERROR` を扱う。
- `export_readiness_status` は `03_01_build_exam_export_cases.py`, `03_02_build_exam_export_case_values.py`, `03_04_check_exam_export_cases.py` の後で再計算し、caseの `subscriber_match_status`, `merge_status`, `case_status`, `value_build_status`, `check_status`, `manual_export_approved`, `xml_export_status` から導く。
- XML出力済みのcaseには、`output_zip_path`, `output_zip_file_name`, `output_xml_file_name`, `xml_exported_at`, `xml_export_etl_run_id` を保持する。これは後続の出力画面、HIAアップロード依頼、再出力判断のための証跡である。
- CSV→XML出力済みの正本は `xml_export_zips` / `xml_export_members` とする。再scan/importや `sync_exam_ledgers` で `xml_export_status` を未出力へ戻してはならない。
- 結合出力用caseの `xml_export_status` は、構成元 `exam_ledgers` の技術状態だけでなく `xml_export_members` の出力事実を参照して `EXPORTED` を復元する。
- HIAアップロード作業リストは `xml_export_zips` / `xml_export_members` を拡張して保持する。ZIP単位のアップロード状態は `xml_export_zips.hia_upload_status`、個人XML単位のエラーや記帳は `xml_export_members.hia_upload_status` とエラー列に持つ。
- 画面・確認SQL向けには `v_xml_export_hia_upload_worklist` を使い、病院毎、出力リスト毎、ZIP毎、個人毎に出力履歴とHIAアップロード状態を確認できるようにする。
- ledgerが増える、再取込される、checkが更新される、結合出力用caseが更新されるたびに、該当者の `person_event_status_items` は再同期される。
- 検査値は、ファイル由来のsource値と、納品・XML出力用の清書値を分けて扱う。
- source値はraw、normalize、validation、由来、エラーを持つ処理・証跡層とする。
- XML出力時は採用済み整値 + `exam_item_master` でentryを構成する。型、単位、OID、section、methodCode、順番、一連検査グループは原則として `exam_item_master` から参照し、整値テーブルへ複製しない。
- 採用済み整値には採用元 `exam_item_values.id` を必須候補として持たせ、raw証跡へ戻れるようにする。手修正値の場合は補正履歴IDを持たせる。
- XML/CSVの両方に同じ `namecode + occurrence_no` が存在する場合、原則はXML優位とする。
- ただし健診機関XMLの `9N511 医師の診断(判定)` に「メタボリックシンドローム判定にて非該当です。」のような制度判定の口語説明だけが入るケースがあるため、全項目フラグではなく `exam_item_value_precedence_rules` によるnamecode単位の例外ルールで制御する。
- 採用例外ルールは、`CSV_FIRST` / `CSV_IF_XML_MATCHES_PATTERN` / `JOIN_XML_CSV` / `MANUAL_REVIEW` を表現できるようにする。取り込み時のsource値は改変せず、結合出力用caseの採用済み整値を作る時だけ適用する。
- 初期ルールとして、XML側の `9N511` がメタボリックシンドローム判定の口語説明のみで、CSV側の `9N511` が存在する場合はCSV側を採用する。
- 通常の実行順は、`01_scan_files.py`、必要に応じて `01_01_match_csv_format.py`、`02_import_xml.py`、`02_02_exam_result_csv_import.py`、`03_00_check_imported_exam_ledgers.py`、`03_01_build_exam_export_cases.py`、`03_02_build_exam_export_case_values.py`、`03_04_check_exam_export_cases.py` とする。
- `03_00_check_imported_exam_ledgers.py` はfile/row source単位の法定check、`03_04_check_exam_export_cases.py` は結合後case単位の法定checkであり、役割が違うため両方実行する。
- `sync_exam_ledgers.py` は通常運用の必須手順ではない。旧個別ledgerからの初回移行、復旧、再構築用に限定する。
- `04_export_hia_xml.py` は次段階で `exam_export_cases` / `exam_export_case_values` 起点へ切り替える。旧CSV行台帳起点の出力経路は移行中の互換経路であり、今後の正にはしない。

### XML Import Current Rules

- XML取込も通常取込の保存先は `exam_ledgers` とする。
- XML由来の `exam_item_values` も `ledger_type = 'EXAM'`, `ledger_id = exam_ledgers.exam_ledger_id` で登録する。
- XMLの健診機関IDは `file_receipts.exam_facility_id` を正とし、XML本文に施設コード・名称があっても `exam_facility_id` の決定には使わない。
- XML本文から健診機関コード・名称を抽出できない場合は、`file_receipts.facility_code` / `facility_name` のscan時スナップショットを補完表示値として使う。
- 受診者住所は `recordTarget/patientRole/addr` だけから抽出する。`representedOrganization/addr` など医療機関住所を受診者住所へ流用しない。
- 住所抽出は `state + city + streetAddressLine` を優先し、タグ外mixed contentが住所として入っているXMLでは `postalCode` を除いた本文をfallbackとして扱う。
- `02_import_xml.py --include-imported` は、取込済みreceiptの再読込、`WARNING` receipt、既存XML ledgerが `READY/PENDING` のもののbackfillに使う。

### Initial `exam_facilities` Shape

- `exam_facilities` は健診機関そのものを表す親マスタとする。
- 主キーは `exam_facility_id` とする。
- 健診機関の業務コードは `exam_facility_code` として保持する。
- 正式名は `exam_facility_name`、表示用の短い名称は `exam_facility_display_name` とする。
- 受領フォルダ名や既存XML由来の `facility_code` / `facility_name` は、親マスタのIDとは別の由来値として扱う。
- 初期カラムは、識別・表示・有効/無効・監査日時を中心に最小構成とする。
- 具体的な初期候補は `exam_facility_id`, `exam_facility_code`, `exam_facility_name`, `exam_facility_display_name`, `exam_facility_type`, `medical_institution_code`, `reservation_system_medical_institution_code`, `postal_code`, `address`, `phone_number`, `website_url`, `management_entity`, `data_source_name`, `data_source_file_name`, `data_source_file_sha256`, `data_source_note`, `note`, `is_active`, `created_at`, `updated_at` とする。
- 支払基金CSVの `機関種別`, `ホームページ`, `経営主体` は、それぞれ `exam_facility_type`, `website_url`, `management_entity` へ保持する案を基本とする。
- 支払基金CSVの `機関コード` は `medical_institution_code` へ保持する案を基本とする。
- 初期投入した健診機関データが社内作業データではなく公開CSV由来であることを明示するため、全行に `data_source_name`, `data_source_file_name`, `data_source_file_sha256`, `data_source_note` を保持する。
- 支払基金CSV由来行の `data_source_name` は `社会保険診療報酬支払基金 全国特定健診・特定保健指導機関CSV` とする。
- 既存受領フォルダは医療機関番号を先頭10桁にして作成されている運用と考え、alias先頭10桁は `medical_institution_code` 候補として扱う。
- 全国CSVに見つからない番号は、地方厚生局・都道府県単位のオープンデータや別年度/別区分の公開データに存在する可能性があるが、初期実装では支払基金CSV、過去CSV/XML実績、受領データ内の番号で確認できた範囲のみ採用する。
- 契約・請求側との接続が必要になった段階で、医療機関番号を正規管理する `medical_institutions` 相当のマスタを後続追加し、`exam_facilities` と紐づける方針を検討する。
- この後続論点は事業所単位ではなく、健保、代行機関、医療施設の連携関係として扱う。
- 後続設計では、`exam_facilities` の一階層上に医療施設/医療機関マスタを置く案、または `exam_facilities` に連携カラムを追加する案を比較する。

### `medical_folder_aliases` Connection

- `medical_folder_aliases` は `phr_master` 側へ移す方向とし、`exam_facility_id` を持つ。
- `event_id` と `src_folder_raw` による既存の一意性は維持する。
- cross schema FK は原則張らず、アプリケーション・移行SQL・検査SQLで整合性を確認する。
- `dst_folder_norm` は既存運用どおり、フォルダ配置・出力先の正規化名として残す。
- `medical_folder_aliases` の参照は共通lookup lib経由を基本とし、lookupでは `exam_facilities` とJOINして有効な健診機関だけを返す。

### `file_receipts` Facility Handoff

- `file_receipts` には `exam_facility_id` を追加する。
- `exam_facility_id` はCSV取込が `phr_master` のCSVフォーマット・マッピングを選択するためのキーとする。
- 既存の `facility_code` / `facility_name` は、scan時にlookupした健診機関コード・名称のスナップショットとして利用する案を基本とする。
- `exam_facility_code` / `exam_facility_name` を `file_receipts` に別カラムとして追加する案は採用しない方向とする。
- 既存XML/ZIP取込では、暗号ZIPのパスワード解決時に `file_receipts.facility_code` / `submitter_facility_code` / 受領フォルダ名を参照するため、この変更はZIPパスワード解決の影響範囲に含める。
- 初期実装では `facility_folder_name` 一致を既存互換として維持し、`exam_facility_id` によるパスワード解決は追加しない。
- `file_receipts` はXML側の実装に寄せ、ファイル単位の現在状態は既存 `status` / `summary_message` / `processable_count` / `content_checked_at` / `processed_at` で表現する。
- `file_receipts` にはCSV実ファイルの照合情報として `actual_header_sha256` / `actual_character_encoding` / `matched_csv_format_version_id` を保持する。
- scan時点でCSV formatが1件に確定できた場合は `matched_csv_format_version_id` を入れて `READY` とし、0件または複数件の場合は `WAITING_CONFIRM` とする。
- 初回scan時にmapping未登録で `WAITING_CONFIRM` になったCSVは、mapping登録後に `01_01_match_csv_format.py` を再実行してformat照合だけを再適用する。
- activeなformatがない場合は「マッピング未登録」、activeなformatはあるがheaderが一致しない場合は「CSV構造不一致」として扱い、どちらも `WAITING_CONFIRM` で止めるが `summary_message` / `etl_errors` の理由は区別する。
- CSV行単位の加入者突合は、XML import と同じく基本情報抽出、`generate_identity_bundle()`、`resolve_subscriber_identity()` の流れに揃える。
- 加入者突合、健診結果値処理、check/export状態は、CSV/XML共通で `exam_ledgers` へ持たせる。
- `file_receipts` に `subscriber_match_*` / `exam_item_*` / `csv_status` / `csv_reason` を追加する案は採用しない。
- ヘッダー関連など、ファイル単位で停止または確認Goが必要な内容は既存 `status` / `summary_message` と停止後Go項目で扱う。
- `file_receipts` には停止後Goの証跡として `import_resume_approved` / `import_resume_approved_at` / `import_resume_approved_by` / `import_resume_approved_reason` / `import_resume_scope` を追加する案を基本とする。
- `WAITING_CONFIRM` はCSVの確認待ち状態として `file_receipts.status` に追加する。
- CSV取込Runの通常対象は `file_receipts.status = READY` のCSVと、`WAITING_CONFIRM` だが `import_resume_approved = 1` のCSVに限定する。
- `DISCOVERED` はformat照合前または取込準備未完了の状態として扱い、CSV取込Runの対象にはしない。
- 同一event・同一相対パスへ別shaのファイルがscanされた場合は別 `file_receipts` として登録し、旧 `DISCOVERED` / `READY` / `WAITING_CONFIRM` を `SUPERSEDED` にする。旧 `IMPORTED` は受領・処理履歴として変更しない。
- eventのactive aliasに対応する施設フォルダがまだ存在しないことは、未受領の正常状態として扱いscan errorにしない。
- alias対応施設フォルダが存在するのに `02_健診結果（編集）` がない場合、実フォルダ名がalias未登録の場合、または実フォルダに対応するaliasが無効・健診機関未解決の場合だけフォルダ・aliasエラーとする。
- CSV取込の `etl_runs.phase` は `IMPORT_CSV_EXAM_RESULTS` とする。
- `file_receipts.etl_run_id` は既存XML側と同じくscan時runの参照として扱い、CSV取込runでは上書きしない。
- CSV取込runは `exam_ledgers.source_etl_run_id` と `etl_errors.run_id` に残す。

### CSV Mapping Tables

- CSVフォーマット定義の親テーブルを `csv_format_versions` とする。
- CSVテンプレートのマッピングは、`csv_exam_result_mapping_rules` / `csv_exam_result_mapping_conditions` を主案とする。
- `csv_column_mappings` は初期検討時の旧暫定案として扱い、初期DDLの主対象にはしない。
- CSV結果値変換ルールテーブル `csv_value_transform_rules` は初期DDL対象から外す。
- `csv_format_versions` は、健診機関ID、mapping version、対象ファイル種別、ヘッダー有無、文字コード、区切り文字、有効期間、有効/無効、施設内default指定を持つ構成を基本とする。
- `csv_exam_result_mapping_rules` は、format version、登録先種別、登録先namecodeまたは基本情報field、排他/複数entry制御、必須/任意を持つ構成を基本とする。
- `csv_exam_result_mapping_conditions` は、CSV列識別、値/下限/上限/判定/方式などのsource role、条件値、OR/AND groupを持つ構成を基本とする。
- CSV列識別は、ヘッダー名と列番号の両方を保持できるようにする。
- 変換ルールのDB管理は初期対象外とし、全角半角、空白、制御文字、非測定値語などの共通処理は `value_normalizer` 側で行う。
- `target_kind` は `LEDGER_FIELD` / `EXAM_ITEM_VALUE` とする。
- `target_resolution_type` は `SINGLE_NAMECODE` / `IDENTITY_ITEM_CANDIDATES` とする。
- `selection_mode` は `DIRECT` / `EXCLUSIVE_ONE` / `MULTI_ENTRY` とする。
- `method_structure_type` は `SINGLE_COLUMN` / `MULTI_COLUMN` とする。
- `source_role` は `VALUE` / `LOWER_LIMIT` / `UPPER_LIMIT` / `JUDGEMENT` / `METHOD` / `QUALIFIER` とする。
- `condition_type` は `HEADER_MATCH` / `METHOD_MATCH` / `VALUE_PRESENT` / `VALUE_MATCH` とする。
- `locator_type` は `HEADER_NAME` / `COLUMN_NO` / `HEADER_AND_COLUMN` とする。
- `operator` は `EQUALS` / `NOT_EQUALS` / `IN` / `NOT_IN` / `EXISTS` / `NOT_EXISTS` / `NOT_EMPTY` / `EMPTY` とする。
- `csv_exam_result_mapping_rules.target_field` は `LEDGER_FIELD` の登録先カラムを表すための項目とし、`EXAM_ITEM_VALUE` では原則NULLとする。
- 検査値ruleの値・下限・上限・判定の区別は、`target_field` ではなく `csv_exam_result_mapping_conditions.source_role` で表す。
- CSVテンプレート登録という入口は1つにする。
- 基本情報マッピングと検査結果値マッピングは登録先としては分けるが、CSVから値を抽出するマッピング形式は共通化する案を基本とする。
- 基本情報マッピングは `exam_ledgers` の基本情報カラムへ反映する。
- 検査結果値マッピングは `exam_item_values` の `namecode` ベース登録へ反映する。
- 基本情報は、条件なしの `target_kind = LEDGER_FIELD`, `source_role = VALUE` のruleとして表現する。
- 検査結果値は、`target_kind = EXAM_ITEM_VALUE`、`target_namecode` または `target_identity_item_code`、`source_role`、必要に応じた条件を持つruleとして表現する。
- `csv_ledger_field_mappings` のような基本情報専用テーブルは代替案として残す。
- 加入者CSV取込の `template_mappings` に合わせ、健診結果CSVマッピングも `csv_header_name` を主たる照合キーとし、列順は `csv_column_order` として1始まりの定義順・検査補助に使う案を基本とする。
- 健診結果CSVは任意項目の増減、施設別・健保別テンプレート差分、健診基幹システムの出力設定差分により列位置が変わりやすいため、`csv_column_order` を処理上の値取得キーとして使わない案を基本とする。
- CSVヘッダー構造の解釈方式は、健診機関単位ではなく `csv_format_versions.header_structure_type` としてformat version単位に保持する案を基本とする。
- CSVヘッダーcontextの作り方も、`csv_format_versions.header_context_rule` としてformat version単位に保持する案を基本とする。
- CSVベース設定として、`csv_format_versions.header_mode` と `data_start_row_no` を持たせる案を基本とする。
- CSVテンプレート変更による静かな欠落を防ぐため、`csv_format_versions` に `header_sha256` / `header_snapshot_json` / `header_hash_status` を持たせる案を基本とする。
- `header_sha256` は、context/occurrence解決後の正規化済みヘッダー構造を列順込みでhash化する。
- 実取込時は、CSV実ファイルから算出した `header_sha256` とformat versionの `header_sha256` を照合する。
- CSV取込の基本方針は、可能な限り取り込み、エラー・不足・警告を明確に記録することとする。
- ヘッダー不一致の場合は、初期実装では自動続行しない。
- ヘッダー不一致時に続行する場合は、format側で確認後Goを許可し、かつ `file_receipts` 側に人が内容確認済みでGoした証跡がある場合に限る。
- ヘッダー一致の場合、登録済みヘッダー内の未マッピング列はテンプレート登録時に不要と判断した意図的な非取込列として扱い、coverage不足エラーにはしない。
- ヘッダー不一致時に列番号指定ruleがある場合、または必要なmapping列を解決できない場合は、誤登録リスクが高いため停止候補とする。
- `csv_format_versions` に `header_mismatch_policy`, `allow_column_no_rules`, `duplicate_row_policy`, `missing_basic_info_policy` を持たせる案を基本とする。
- `header_mismatch_policy` の初期値は `ALLOW_AFTER_CONFIRM` とする。
- ヘッダー不一致時の続行は、format側の `header_mismatch_policy`、実CSVで必要列を解決できるか、`file_receipts` 側の確認済みGo証跡を組み合わせて制御するが、人の確認なしに自動続行はしない。
- 同一施設・同一header shaに複数の有効formatが存在する場合、`csv_format_versions.is_default_for_facility = 1` が1件だけならそれを採用し、defaultも一意でない場合は `WAITING_CONFIRM` とする。
- `allow_column_no_rules` は列番号指定ruleを許すかを表し、初期値は許可しない方針とする。
- `duplicate_row_policy` は同一 `row_sha256` の扱いを表し、初期値はcheck済みOK行をskipする方針とする。
- `missing_basic_info_policy` は健診日など基本情報不足時の扱いを表し、初期値は取込を進めて後続checkで扱う方針とする。
- `header_snapshot_json` は確認用snapshotとしてJSONで保持する案を基本とし、取込制御に使う値は通常カラムに出す。
- 初期の `header_mode` は `NONE`, `SINGLE`, `WITH_CONTEXT` とする。
- 初期の `header_structure_type` は `SIMPLE_HEADER` と `GROUPED_VALUE_METHOD` とする。
- 初期の `header_context_rule` は `NONE`, `UPPER_HEADER`, `CARRY_FORWARD_ITEM` とする。
- ハートクロスのように2行目にfield code/namecodeがあるCSVも、専用の `NAMECODE_ROW` 方式は増やさず、既存の複数行ヘッダーとして扱う。
- 2行目コード/namecodeは、通常のヘッダー名指定で使う実ヘッダーとして扱う。専用の `header_code` / `header_namecode` カラムは初期実装では追加しない。
- 複数行ヘッダーでは、どの行を実ヘッダーとして列指定に使うかを表す `active_header_row` 相当の設定を持つ案を基本とする。
- 初期実装では、1つの `csv_format_versions` / `mapping_version` に対して登録できるヘッダーは1種類とする。
- ヘッダー名の表記ゆれはシステム側で自動吸収しない。
- ヘッダー名が違うCSV、列構造が違うCSV、施設・健保・出力テンプレート差分があるCSVは、同じ施設でも別 `mapping_version` として明示登録する。
- ヘッダー表記ゆれのN対N自動マッチングは、違う列を憶測で同一視するリスクがあるため初期実装では扱わない。
- ルールやマッピングは完全自動生成しない。健診機関・mapping versionごとの初回テンプレートは、人がCSV実物を確認して手動登録する。
- `CARRY_FORWARD_ITEM` は自動推測エンジンではなく、手動登録済みの `header_snapshot_json.normalized_columns` に従って、持ち回りcontext形式のCSVヘッダー構造を再現する方式として扱う。
- `CARRY_FORWARD_ITEM` でも、取込時にシステムが検査項目名を推測してmappingを作ることはしない。実取込では登録済みheader fingerprintとmapping conditionを照合する。
- 将来複数ヘッダーを扱う場合も、取り込み時の自動推測ではなく、人が確認済みのheader variantとして登録する方向を検討する。
- 血糖の `区分列で分岐` / `空腹時・随時別列` などの選択は、format本体の永続カラムではなく、seed/FastAPI入力支援側の補助設定として扱う。
- 同一ヘッダー名が繰り返されるCSVに備え、`csv_exam_result_mapping_conditions` は `header_context`, `header_name`, `header_occurrence` を持つ案を基本とする。
- ヘッダー指定による列解決は、指定条件で1列に決まることを必須とする。
- 指定条件で0件の場合は列未検出、2件以上の場合は曖昧指定としてエラーにし、推測して続行しない。
- 同値ヘッダーが複数存在するCSVでは、人が `header_context`, `header_occurrence`, `COLUMN_NO`, `HEADER_AND_COLUMN` などで一意化条件を登録する。
- 健診結果値のマッピングは、単純な「CSV列 -> namecode」ではなく、`namecode` を親にしてCSV上の値取得条件を追加していくルールモデルを採用する。
- `csv_exam_result_mapping_rules` は登録先種別を持つ親ルール、`csv_exam_result_mapping_conditions` はヘッダー名、context、方式列、値有無などの評価条件を表す子ルールとする案を検討する。
- 同一rule内の `condition_group_no` はOR条件の単位、同一group内の複数条件はAND条件として評価する案を基本とする。
- rule/conditionの重複validateはDB制約ではなく、seed生成時および将来のFastAPI登録時に行う。
- 同一rule key、同一rule内の同一 `source_role + locator`、同一rule内の複数 `VALUE`、`EXCLUSIVE_ONE` の同priority衝突などはテンプレート登録エラーとして扱う。
- 投入先の決め方は `SINGLE_NAMECODE` と `IDENTITY_ITEM_CANDIDATES` を候補とする。
- `SINGLE_NAMECODE` は投入先 `namecode` を固定指定する。
- `IDENTITY_ITEM_CANDIDATES` は同一性項目を起点に候補 `namecode` を表示・絞り込む。
- `identity_item_code` は候補探索や画面表示の括りとして使い、それだけで候補同士が排他であるとは判断しない。
- CSVマッピングでは `selection_mode` を持たせ、`DIRECT` / `EXCLUSIVE_ONE` / `MULTI_ENTRY` を候補とする。
- `EXCLUSIVE_ONE` は同じ候補グループ内で条件に一致した1つの `namecode` のみ採用する。
- `EXCLUSIVE_ONE` の排他範囲を明示するため、親ruleに `selection_group_code` を持たせる案を基本とする。
- `MULTI_ENTRY` は同じ `identity_item_code` 配下で複数ruleが成立しても、それぞれ別 `exam_item_values` entryとして登録する。
- 値列と検査方法列の指定方法は、ヘッダー名、列番号、ヘッダー名+列番号を候補とする。
- 列番号のみ指定は例外対応とし、原則はヘッダー名を優先する。
- ヘッダー名+列番号指定は、値取得に加えて列ズレ検知に使う案を基本とする。
- テンプレート登録画面は、上位に健診機関とmapping versionを持ち、CSVベース設定を行ったうえで、同一性項目を選択し、候補 `namecode` をチェックして使用条件を設定する流れを候補とする。
- CSVテンプレート登録では、候補 `namecode` ごとにCSV内の `値` / `下限` / `上限` / `判定` 列が存在するか、存在する場合に取り込むかを設定できるようにする案を基本とする。
- ここで扱う `判定` は、法定項目の必須/不足チェックや `check_result` の評価ではなく、健診機関がCSVに出してきた検査別判定・カテゴリ総合判定を指す。
- `未実施` / `測定不能` / `判定不能` など、entry内の項目結果値として出てくる実施状態・測定可否は、この健診機関由来の健診判定とは別に扱う。
- 健診機関由来の健診判定は、健診機関ごとの基準、契約、事業所向け帳票要件で意味が変わる可能性が高いため、初期実装ではPHR側の判定ロジックや納品判定には利用しない。
- 健診機関由来の健診判定は原本証跡として扱う。正規化、評価、納品利用、健診機関別判定範囲マスタの作成は後続バージョンで別途検討する。
- 初期実装では、`source_role = JUDGEMENT` は「CSV内に健診機関由来の判定列が存在する/取り込む対象にできる」ことを表すだけで、`exam_item_values.interpretation_code` へ必ず反映する意味ではない。
- ヒロオカCSVの `Ａ` / `Ｂ` / `Ｃ６` / `Ｃ１２` / `Ｄ` のような施設判定は、`norm_variants` へ追加しない。
- CSVテンプレート登録支援として、付属2由来の `identity_item_code` を網羅する上位グループ初期セットを用意する案を基本とする。
- `exam_item_master.identity_item_code` ごとの `ANNEX2_IDENTITY` グループを最小網羅単位とし、少なくとも付属2項目の候補表示漏れを防ぐ。
- `ANNEX2_IDENTITY` 197件は `phr_master` に物理seedとして保存する。
- `exam_item_concept_groups` / `exam_item_concept_group_members` は正式テーブル化する。
- 血糖・脂質・血圧などは、複数の `ANNEX2_IDENTITY` を束ねる入力支援bundleとして扱う。
- 入力支援bundleも初期セットとして決める方針とする。血糖/HbA1c、脂質、腎機能関連は階層型で採用する。
- 入力支援bundleは、画面では大きい親bundleで探し、内部では小さい意味単位の子bundleに分ける階層型を採用する。
- `exam_item_concept_groups` は `parent_concept_group_id` / `parent_concept_group_code` / `concept_group_depth` を持つ案を基本とする。
- `GLUCOSE_RELATED` 配下に `GLUCOSE` / `HBA1C` を置く。
- `LIPID_RELATED` 配下に `TRIGLYCERIDE` / `HDL_CHOLESTEROL` / `LDL_CHOLESTEROL` / `NON_HDL_CHOLESTEROL` / `TOTAL_CHOLESTEROL` を置く。
- `RENAL_RELATED` 配下に `CREATININE` / `EGFR` / `URIC_ACID` / `URINE_ALBUMIN` を置く。
- `CREATININE` は `3C015`、`EGFR` は `8A065` として分ける。
- 入力支援bundleは検体別分類より意味別分類を優先し、腎機能関連では血清尿酸・尿中アルブミンも近くに置く。
- 上位グループは全検査項目の医学分類を手作業で設計するものではなく、CSV登録時に候補 `namecode` を探しやすくするための支援レイヤーとして扱う。
- 上位グループは既存 `exam_item_master` / `exam_item_group_*` をseed材料にできるが、労安法チェック用グループとは責務を分ける。
- 上位グループ配下でも、実際の保存先は `target_namecode` 単位で明示する。
- `LOWER_LIMIT` / `UPPER_LIMIT` / `JUDGEMENT` は、マスタ基準範囲ではなくCSV由来項目列を表す。
- `20_mapping_rule_screen_mock.html` は画面実装ではなく、テンプレート登録構造を把握するためのサンプルモックとして扱う。
- 今回スコープでは、CSV取込を成立させるための初期テンプレート登録はseed前提とする。
- テンプレート登録は、今回スコープ完了後の次タスクでFastAPIベースの管理API化を検討する。
- 現時点ではFastAPI実装は行わず仕様整理に留める。
- CSV取込の初期処理方式は、1行ごとに基本情報と検査結果値を抽出し、`exam_ledgers` と `exam_item_values` を順に登録する行単位処理を採用する。
- ただし、抽出処理の関数境界は基本情報と検査結果値で分け、将来のバッチ処理へ転用できる形にする。
- CSV取込ではリアルタイム性を求めず、初期実装は1人/1行ずつ処理する方針を基本とする。
- CSVテンプレートは候補ruleを定義するだけで、実際に作成する `exam_item_values.namecode` はCSVデータ行ごとの補助列・方式条件を評価して決定する。
- 1行処理では、抽出対象ruleを取得し、各項目ruleに従って値を1個ずつ作成し、1行分の `exam_ledgers` / `exam_item_values` 登録を組み立ててcommitする。
- CSV行を再処理する場合、`exam_item_values` は `ledger_type = 'EXAM'` かつ `ledger_id = exam_ledger_id` の既存行をdeleteしてからinsertする。
- 現状の `exam_item_values` に新しい一意制約を追加せず、CSV由来結果値は行台帳単位のdelete+insertで整合させる。
- 行処理中に失敗した場合は、その行の変更をrollbackし、行単位errorとして記録する案を基本とする。
- 抽出マッピングは基本情報も検査結果値も同じrule/condition形式で扱う。
- 基本情報は条件なしのruleとして扱う。
- 既存 `csv_row_ledger` は移行元・後方互換用として残すが、新規通常取込の正は `exam_ledgers` とする。
- CSVファイル全体の重複抑制には `file_receipts.file_sha256` を使う。
- CSV行単位の重複抑制には `exam_ledgers.row_sha256` を使う。
- `row_sha256` は列順込みのセル配列から算出し、ヘッダー名sort済みkey-value hashにはしない。
- `row_sha256` は、CSV loaderで文字コード変換後のセル配列を列順込みでJSON正規化し、SHA-256化する。
- check済みでOK扱いの同一 `row_sha256` は再取込時にskipする。
- check済みOKの既存行はskipし、未完了、WARNING、ERROR、check未実行は再処理対象とする。
- CSV取込Runは、新規CSVだけでなく、過去にヘッダー不一致等で停止したが `file_receipts` 側で確認Goが出ているCSVも同じ入口で処理対象にする。
- CSV format照合の共通処理は `scripts/lib/csv/exam_result_format_matcher.py` に置き、`01_scan_files.py` と `01_01_match_csv_format.py` の両方から利用する。
- 停止済みCSVの再投入Goは、別モードを作らず通常Run内で拾い、未完了の後続処理を進める案を基本とする。
- CSV由来の `VALUE` が完全空セルの場合は `exam_item_values` 行を作らない。
- CSV由来の下限・上限・判定だけが存在し、`VALUE` が完全空セルの場合も `exam_item_values` 行を作らない。
- `未実施` / `キャンセル` / `測定不能` などの非測定値語は完全空ではないため、`exam_item_values.raw_value` に原文を残す。
- 健診日など基本情報が不足していてもCSV取込段階ではskipしない。足りない基本情報の判定は将来 `check_result` 側のスコープで扱う。
- ハートクロスCSVのようにCSV単体に健診日がないサンプルでも、別データで健診日を特定できる見込みがある場合は、実装検証を止めない。
- その場合、テンプレートは暫定設定として作成し、健診日取得元は健診機関回答待ちまたは別データ連携待ちとして明示する。
- 健診日未解決の行は `exam_ledgers.exam_date = NULL` で保持し、後続checkで不足として検知できる状態にする。
- 完全空行はCSV取込段階でskipする。
- 既存 `work_other.medi_exam_result_ledger` は旧紙/Excel系の1人=1件の基本情報台帳であり、CSV行台帳設計の参照元とする。
- `health_exam_result.file_receipts` はファイル単位台帳であり、人/行単位の基本情報台帳としては共用しない。
- `exam_item_values` にはCSV由来の下限/上限専用カラムが現状ないため、原本由来情報として `source_reference_lower` / `source_reference_upper` を追加するmigration候補を作成する。
- CSV由来の下限/上限の単位は、結果値の `raw_unit` と同じ前提で扱う。
- 下限/上限だけ別単位で提出されるケースはかなり特殊であり、初期設計では専用単位カラムを持たない。
- 健診機関由来の健診判定を `exam_item_values.interpretation_code` / `interpretation_code_system` / `interpretation_name` に寄せる案は、初期実装では採用しない。
- XML由来の `interpretationCode` は標準コードとして扱えるが、CSV由来判定は施設固有判定である可能性が高いため、同一扱いしない。
- 健診機関由来の健診判定の保存先は、初期実装では原本証跡として `raw_row_json` から復元できる状態を最低条件とし、必要に応じて `exam_item_values.source_judgement_raw` などの専用カラム追加を別途検討する。
- CSVに `namecode` が付与されていても、その列の実値が `exam_item_master.xml_value_type` や `result_code_oid` と整合しない場合は、システム側で別項目へ推測振替しない。
- 例: CD項目 `9N066000000000011` に `心雑音 要受診` のような自由記載/医師判断相当の文字列が入る場合は、`norm_variants` を追加して救済する対象ではなく、健診機関へのフォーマット確認事項として扱う。
- この種の不整合は、CSV取込マッピング仕様で吸収する課題ではなく、健診機関とのCSV仕様すり合わせ・確認依頼の業務フローで扱う。
- ヘッダー不一致時の続行可否は、rule/template側に「確認後Goを許す設定」を持ち、`file_receipts` 側に「このファイル内容を確認済みでGo」の証跡を持つ二段構えを基本とする。
- `etl_errors` には初期実装で rule_id / condition_id / namecode 専用カラムを追加しない。既存 `field` / `field_value` / `message` に寄せ、必要になったら後続で補助カラム追加を検討する。
- CSV取込の事前停止は最小限とし、目的は「まず取り込む」「エラー・不足・警告を明確にする」こととする。
- 停止候補は、必要なmapping列を安全に解決できない場合、列番号指定ruleにより誤登録リスクが高い場合などに限定する。
- `csv_format_versions.character_encoding` は想定文字コードとして保持し、`encoding_fallback_policy` で文字コードfallback可否を制御する。
- 初期値は `ALLOW_COMMON_ENCODINGS` とし、登録文字コード、UTF-8 BOM、UTF-8、CP932の順で重複を除いて読込候補にする。`STRICT` の場合は登録文字コードだけを使う。
- fallbackで別文字コードを試しても、登録済み `header_sha256` と一致した場合だけformat一致とする。文字化けした読込結果やヘッダー表記揺れを推測採用しない。
- 実際に採用した文字コードは `file_receipts.actual_character_encoding` に保存する。
- `quote_char` はCSV parserへ実際に渡す。引用符が存在しないCSVも同じ設定で読めるため、引用符の有無だけではformat不一致にしない。
- delimiterは初期実装では登録値を固定使用し、自動fallbackの対象にしない。
- `exam_ledgers.health_exam_report_category` と `program_code` は、施設・format version別のmapping ruleで正しい厚生労働省コードを明示的に得られた場合はその値を保存する。
- 小禄病院CSVの `医療機関コード` は施設内コードであり、健診機関識別には使用しない。小禄の `facility_code` / `facility_name` は、scan時に `exam_facilities` から `file_receipts` へ保存したスナップショットを使用する。
- CSVに対応項目がない、または対応項目の値がNULLの場合は、`event.age_rule_type` と `event.age_reference_date` による満年齢判定で不足コードを補完する。
- 満年齢40～74歳は `health_exam_report_category = 10`, `program_code = 010`、それ以外は `health_exam_report_category = 40`, `program_code = 990` とする。
- `event.age_rule_type = EXAM_DATE` のeventでは健診日を年齢基準日とし、`FIXED_DATE` のeventでは `age_reference_date` を使う。`event_id = 2` は2026年度の `2026-11-30` を年齢基準日とする。
- コース名称、検査項目構成、特定健診判定からは報告区分を推測しない。今回の自動補完はeventに明示された年齢判定規則だけを根拠とする。
- 施設側コースコードは厚生労働省プログラムコードとして使用しない。小禄病院の `健診コースコード` mappingも無効化する。

### CSV to HIA XML Export

- `health_exam_report_category` と厚生労働省プログラムコードは、CSV mappingによって正しい値が登録されている場合は、その値をXML出力に使用する。
- mapping値がない場合はCSV取込時にevent年齢規則で補完されたledger値をXML出力に使用する。
- XML取込では、元XMLの `ClinicalDocument/code` を `exam_ledgers.report_category_code`、`documentationOf/serviceEvent/code` を `exam_ledgers.program_type_code` に保存する。
- XML由来コードは元XMLの明示値を正とし、event年齢規則による上書きや補完は行わない。
- 既存XMLは `02_import_xml.py --include-imported` で再取込し、新カラムをbackfillできるようにする。
- XML検査値は `VALID` のみ出力する。`WARNING/SKIPPED` は初期版ではentryを省略し、`INVALID` も該当entryを出力しない。
- 妊娠中等の確認済み理由により法定項目が `MISSING` の場合は、結合出力用case側の `manual_export_approved` と必須の理由、承認者、承認日時を記録してXML出力を許可できる。
- 手動出力許可は法定チェックNGの原因が `MISSING` のみの場合に限定し、`INVALID`、`PARSE_ERROR`、加入者不一致、報告区分・プログラムコード不足、健診機関不一致、XML生成・XSD検証エラーは通過させない。
- XML不足をCSVで実値補完して法定OKにする処理と、妊娠中等の業務確認によりMISSINGのまま条件付き出力OKにする処理は別レーンとして扱う。
- 条件付き出力OKでは架空の検査値を作らず、`check_status = NG` / `check_reason` を維持したまま `manual_export_approved` / `manual_export_reason` / `manual_export_approved_by` / `manual_export_approved_at` を記録する。
- 初期のCSV補完診断対象は、視力 `4403004001`、聴力 `4403005001`、胸部X線 `4404001001`、心電図 `4411001001`、既往歴 `4401001001`、自覚症状 `4402001001`、他覚症状 `4402001002` の7分類とする。
- ただしCSV取込自体は7分類へ絞らない。健診機関ごとの通常マッピングを作り、取込可能な検査値・基本情報は従来どおり全てsource値として取り込む。7分類は補完診断で重点的に見る分類であり、マッピング対象や取込対象を制限するものではない。
- 手動出力許可後も `exam_check_results` と結合出力用caseの `check_status` は書き換えず、架空の検査値を作らない。該当entryはXMLへ出力しない。
- 同日分割送信回数は既存ZIPからの自動採番を既定とし、`0`から`9`の明示指定も可能にする。既存ZIPと衝突する番号では上書きしない。
- 個人XMLファイル名21桁目の種別は、特定健診情報を表す実施区分コード `1` 固定とする。
- XML出力の原子単位はZIPとする。同じ健診機関・保険者・作成日・同日分割送信回数の対象者に1人でも生成またはXSD検証失敗があれば、そのZIP全体を出力しない。
- 失敗したZIPとは別のZIP単位は処理を継続する。
- 健診機関情報は `phr_master.exam_facilities` を正とし、ledgerの健診機関コードと不一致の場合は該当ZIPを停止する。
- XML出力履歴は `etl_runs` を処理の親とし、正常完成したZIPと収録した個人XMLを専用履歴テーブルへ追記する。
- 画面運用では、即出力ではなく「出力リスト」を作成し、検索した `exam_export_cases` を追加・確認してから出力する方式を正とする。これは人が操作する作業箱であり、`etl_runs` とは別責務である。
- 出力リストの初期DDL候補名は `xml_export_lists` / `xml_export_list_cases` とする。`run_id` という名前はETL実行履歴と混同するため避ける。
- `etl_runs` は出力処理を実際に走らせた実行ログとして使い続ける。出力リストは「誰を今回出す候補にしたか」、`xml_export_zips` / `xml_export_members` は「実際にどのZIP・個人XMLを出したか」を表す。
- 個人XML履歴には、出力時点の手動許可有無、理由、承認者、承認日時をsnapshotとして残す。
- 再出力は通常運用として想定し、過去の出力履歴を更新・削除せず、新しいZIP・個人XML履歴として追加する。
- 出力履歴は「誰をどのZIPへ出力したか」という事実を保存する責務に限定する。個人単位の業務状態、修正版の正本判定、後続業務データへの反映時点は後続版で決める。
- XML基本情報の値生成・妥当性確認は、同じ処理を持つ既存identity共通libを必ず使用する。`export_fields.py` は既存関数を組み合わせる薄いprojectionとし、同じ正規化ロジックを再実装しない。
- HIA受付では受診者住所・郵便番号が必須扱いとなるため、CSV/XMLに住所がない場合でも補正・代替値でXML出力できる運用を用意する。
- 住所補完は、まずCSV/XML原本値、次に加入者住所等の業務的に利用許可された値、次に日本郵便の郵便番号データ由来の住所マスタを使用する。
- 郵便番号マスタは日本郵便の公式「住所の郵便番号（1レコード1行、UTF-8形式）」を入力候補とし、郵便番号から都道府県・市区町村・町域までの住所を補完する。丁目、番地、建物名など個人住所の詳細は推測しない。
- 日本郵便APIは初期実装では採用しない。ビジネスアカウント登録、API権限、通信可否、障害時運用が必要になるため、リアルタイム性を求めない今回の住所補完では公式CSVをmaster DBへ取り込む方式を採用する。
- 従来形式CSVや事業所の個別郵便番号CSVは初期対象外とする。事業所個別郵便番号が必要になった場合は、通常住所masterとは別masterとして後続で追加する。
- 日本郵便データに含まれる「以下に掲載がない場合」等の表現は、XML出力用にそのまま使わず、住所文字列として扱える表記へ整備する。整備規則と元表記は記録する。
- 郵便番号からも住所を補完できない場合は、HIA提出用の代替値として郵便番号 `000-0000`、住所 `－`（全角ハイフン）を使用できる。
- 住所補完または代替値使用を行った場合は、XMLに出した値、元値、補完元、補完理由、処理日時、処理者または処理Runを必ず記帳する。原本CSV/XML値そのものは上書きしない。
- 基本情報の補正はCSVだけでなくXML取込にも必要である。現在XML出力で使う補正値と項目別の最新変更履歴IDは `exam_ledgers` 側に持たせ、変更履歴は `exam_ledger_id` を起点に項目ごとの変更チェーンとして残す。
- 初期の補正対象は `insurer_number`, `insurance_symbol`, `insurance_number`, `insurance_branch_number`, `exam_ticket_number`, `exam_ticket_expires_on`, `name_kana`, `postal_code`, `address` とする。
- 補正履歴は `field_name`, `before_value`, `after_value`, `correction_source`, `correction_reason`, `previous_correction_history_id`, `etl_run_id`, `corrected_by`, `corrected_at` を持つ。`active` flagではなく、ledger側の最新履歴IDで現在値を示す。
- 基本情報修正画面では、加入者突合済みの行に対して `subscribers` の保険証記号、保険証番号、枝番、氏名カナ、郵便番号、住所を補正候補として表示する。採用時は `correction_source = 'SUBSCRIBER'` の補正履歴として記録し、原本値を上書きしない。
- 人向けの不足情報CSVを追加するかは後続で決め、初期XML実装を止めない。`manual_export_approved` / `manual_export_reason` は不足情報CSVとは別概念とする。
- `*_match` は照合・検索専用であり、HIA/XML出力値としては使用しない。
- 保険証記号、保険証番号、氏名カナは、ledgerに `*_export_value` / `*_export_source` / `*_export_reason` を持たせる。
- CSV/XML原本から作る場合は `*_export_source = SOURCE`、加入者突合済みの `subscribers` 登録値から作る場合は `*_export_source = SUBSCRIBER` とする。
- `subscribers` からHIA/XML出力用基本情報を読む処理は、`scripts/lib/db/lookup/subscriber_export_projection.py` に閉じ込める。`subscribers` 本体の列名やraw/norm/match/exportの混在は、出力処理側へ直接漏らさない。
- `subscribers.insurance_symbol_export` は保険証記号の出力候補として使用できる。保険証番号は `subscribers.insurance_number`、氏名カナは `subscribers.name_kana_full` を元値として既存identity共通libへ通し、出力値を作る。

### CSV Mapping and Normalization Assets

- m4側に作成したCSVサンプル、CSVフォーマット定義、マッピングルール、`norm_variants` 追加seed、寄せた/寄せない判断メモは、納品処理の一時データではなく健診結果取込の経験値資産として保持する。
- 機微情報を含む実データは保持しないが、機微情報を除去・加工したサンプルと、`raw値 -> namecode / OID / code` の対応、normalizeエラーとして残す判断は保持する。
- 施設判定、独自コード、CSVヘッダーのクセ、型に合わない値、健診機関確認待ちの内容は、後続の標準化・解析レイヤーで参照できるように記録を残す。
- これらの資産は、将来の自動推測に使うためではなく、人が標準化、マッピング、健診機関とのすり合わせを行う際の判断材料として使う。
- 健診機関へ返す是正項目まとめを後続機能として作る。これは内部エラー一覧ではなく、健診機関に確認・修正依頼できる粒度で、標準コード不一致、項目内容不一致、列名と値の意味不一致、施設独自コード、必須基本情報不足、提出データ品質の問題を整理する。
- 健診機関からCSV抽出費用等が発生する場合でも、受領データが標準仕様や契約上の用途を満たさない場合は、名寄せ・変換・補正作業をPHR側の無償吸収として扱わず、是正依頼または別途作業として説明できる証跡を残す。
- 健診結果項目の標準化を検討する後続版では、省庁、標準化団体、医療情報標準、特定健診・労安法健診の法体系、関連する政策動向を調査対象に含める。

### Production Folder Aliases

- event 2の実機ルート直下フォルダは2026-07-30時点で198件であり、初期alias seed 187件との完全一致は185件だった。
- 実機だけに存在した13フォルダはすべてコードで既存 `exam_facilities` へ確定し、追加aliasとして登録する。
- 同一施設の旧名称aliasは削除せず履歴互換として残す。物理フォルダがないaliasは未受領として正常skipする。
- 実機フォルダ一覧そのものはrepositoryへ保存せず、差分seedと件数・判断結果だけを設計資料へ残す。

### Source CSV Check

- 支払基金の全国CSV `Pref_00.csv` は `docs/spec/exam_result_csv_import/downloads/Pref_00.csv` に配置済みである。
- `/Users/hiro/Downloads/Pref_00.csv` と project 配下の `Pref_00.csv` は `cksum` が一致している。
- `Pref_00.csv` はCP932として読込可能であり、UTF-8としては読込不可である。
- `Pref_00.csv` のヘッダーは `機関コード`, `機関種別`, `機関名`, `郵便番号`, `電話番号`, `機関所在地`, `ホームページ`, `経営主体` の8列である。
- `Pref_00.csv` は54,713行、データ行54,712行であり、全データ行が8列である。

## Not Decided Yet

- `phr_master` の初期DDL詳細。
- `exam_item_master`、`exam_item_groups`、`norm_rules` の移動時期。
- `funds`、`fund_insurer_numbers` など保険者系マスタの移動時期。
- 将来、`exam_facilities.exam_facility_code` を `medical_institution_code` と別採番にする必要が出た場合の移行方針。
- `exam_facilities.exam_facility_type` の正式コード体系。
- `dev_phr` 既存スクリプト・SQLの参照先変更範囲。
- マスタ管理者、業務処理者、参照者などの具体的なDBロールDDL。
- 既存データ移行SQL。
- ADR化する単位。
- `csv_format_versions` / `csv_exam_result_mapping_rules` / `csv_exam_result_mapping_conditions` / `norm_variants` の最終DDL SQL化。
- mapping version の命名規則と切替ルール。
- 健診結果値normalize共通libの正式API。
- 非測定値語を `nullflavor` へ写像するかどうか。
- `あり` / `なし` の項目別扱いの具体seed。
- テンプレート登録FastAPIの正式スコープ。
- 既存 `csv_row_ledger` 廃止タイミングと、必要になった場合のデータ退避・参照停止手順。
- ヒロオカクリニック実CSVを元にした初期seed内容。
- `csv_loader` 追加APIの正式名。
- alias lookupで `folder_name` が空、未登録、無効施設の場合の正式な状態遷移。
