# 25 Phase3 scan_files Design Review

## 1. 目的

Phase3 `01_scan_files.py` 実装前に、入力、参照テーブル、処理フロー、`file_receipts` 登録ルール、重複判定、エラー処理、ログ記録、未決事項を整理する。

本資料は実装前レビュー資料であり、スクリプト、DDL、migration、seed SQL は作成しない。未決事項は勝手に決定せず、旧スクリプト要約と現行設計資料から判断材料を提示する。

## 2. 参照資料

- `docs/refactor/health_exam_result/03_decisions.md`
- `docs/refactor/health_exam_result/11_v2_script_design_notes.md`
- `docs/refactor/health_exam_result/20_implementation_plan.md`
- `docs/refactor/health_exam_result/23_phase1_core_ddl_detail.md`
- `docs/refactor/health_exam_result/24_phase2_design_review.md`
- `docs/refactor/health_exam_result/01_codex_findings.md`
- `docs/refactor/health_exam_result/07_current_script_specs_codex.md`
- `sql/ddl/health_exam_result/0010_health_exam_result__etl_runs.sql`
- `sql/ddl/health_exam_result/0020_health_exam_result__etl_errors.sql`
- `sql/ddl/health_exam_result/0030_health_exam_result__medical_folder_aliases.sql`
- `sql/ddl/health_exam_result/0040_health_exam_result__file_receipts.sql`
- `sql/migrations/dev_phr/20260703_001_dev_phr_add_result_root_path_to_event.sql`
- `sql/seed/health_exam_result/0010_health_exam_result__medical_folder_aliases_event2.sql`

## 3. 旧スクリプト要約から見た既存挙動

旧スクリプトでは、共有フォルダ観測からローカル input へのコピーまでが複数段階に分かれていた。

| 旧スクリプト | 主な責務 | v2 Phase3で流用できる考え方 | v2で見直す点 |
|---|---|---|---|
| `medi_shared_files_scan.py` | 共有フォルダを拡張子指定で走査し、`medi_shared_files` にUPSERTする。既定拡張子は `zip`。 | フルスキャン、拡張子別探索、ファイル属性取得、初回/最終検出時刻の考え方。 | v2では `stage_status` を毎回 `NEW` に戻さず、未登録ファイルだけ `file_receipts.status = DISCOVERED` で登録する。 |
| `medi_shared_files_hash_zip.py` | ZIPのSHA-256を後段で計算し、`medi_shared_files.sha256` に保存する。 | ファイル内容SHA-256を重複判定に使う考え方。チャンク読み込み。 | v2では `file_receipts.file_sha256` が NOT NULL かつ一意判定要素のため、登録前に計算が必要。 |
| `medi_shared_files_auto_judge.py` | ZIP内XML有無をprobeし、`auto_judgement` とXML件数を更新する。 | ZIP内XML件数を `processable_count` として扱う考え方。 | v2ではZIP展開・XML読込は `02_import_xml.py` に寄せる方針。Phase3では `processable_count` を設定しない。 |
| `medi_shared_files_copy_to_input.py` | 健診対象ZIPをalias解決後にinputへコピーする。manual judgementを優先する。 | aliasによる医療機関フォルダ解決、同一ZIP SHA除外、手動判断優先の考え方。 | v2の `01_scan_files.py` は `work` やinputへのコピーを行わない。 |
| `medi_zip_import.py` | input配下ZIPのSHA計算、ZIP構造確認、XML棚卸し、Run記録を行う。 | run単位管理、ZIP/XML件数、個別失敗を記録して次へ進む方針。 | v2ではファイル検出はPhase3、ZIP展開/XML取込はPhase4に分離する。 |

旧実装は「観測台帳にまず登録し、後段でハッシュ・判定・コピーする」流れだった。v2 Phase3では `file_receipts` が後続 `02_import_xml.py` の入力そのものになるため、登録時点で `file_sha256`、`relative_path`、`file_type`、`etl_run_id` を揃える必要がある。

## 4. Phase3の入力・出力

### 入力

- 設定ファイルまたは実行引数の `event_id`
- `dev_phr.event.result_root_path`
- `health_exam_result.medical_folder_aliases`
- `<event.result_root_path>/<医療機関フォルダ>/02_健診結果（編集）/` 配下の ZIP / XML ファイル
- 既存の `health_exam_result.file_receipts`

### 出力

- `health_exam_result.etl_runs` のscan Run
- 新規検出ファイルの `health_exam_result.file_receipts`
- 異常・スキップ調査用の `health_exam_result.etl_errors`
- Run終了時の `etl_runs.summary_message`

### Phase3では行わないこと

- ZIP展開
- XML parse
- XML基本情報抽出
- `xml_ledger` / `xml_file_links` / `exam_item_values` 登録
- `work` へのコピー
- DDL、migration、seed SQL の作成・更新

## 5. 参照テーブル

| テーブル | Phase3での扱い | 主な参照・更新カラム |
|---|---|---|
| `dev_phr.event` | 参照のみ。対象 `event_id` の `result_root_path` を取得する。 | `event_id`, `result_root_path` |
| `health_exam_result.medical_folder_aliases` | 参照のみ。event内の医療機関フォルダを解決する。 | `event_id`, `src_folder_raw`, `dst_folder_norm`, `manual_judgement`, `is_active`, `note` |
| `health_exam_result.file_receipts` | 既存行参照、新規行INSERT。重複ファイルはINSERTしない。 | `event_id`, `relative_path`, `file_sha256`, `status`, `etl_run_id` など |
| `health_exam_result.etl_runs` | scan Run開始・終了を記録する。 | `run_type`, `event_id`, `status`, `started_at`, `finished_at`, `summary_message` |
| `health_exam_result.etl_errors` | scan中の異常を記録する。 | `run_id`, `file_receipt_id`, `error_type`, `error_code`, `error_message`, `status` |

## 6. 処理フロー案

1. 設定ファイルまたは実行引数から `event_id` を取得する。
2. `etl_runs` に `01_scan_files.py` の開始Runを作成する。
3. `dev_phr.event` から `result_root_path` を取得する。
4. `result_root_path` が `NULL`、空文字、存在しない、または参照不可の場合はRunを失敗終了し、`etl_errors` に記録する。
5. `medical_folder_aliases` から対象eventのaliasを取得する。
6. 原則 `is_active = 1` のaliasを対象候補とし、`src_folder_raw` を実フォルダ名、`dst_folder_norm` を正規フォルダ名として扱う。
7. 各aliasについて `<result_root_path>/<src_folder_raw>/02_健診結果（編集）/` を探索対象にする。
8. 探索対象フォルダが存在しない場合は、ファイル単位ではなくalias/フォルダ単位で `etl_errors` またはRunサマリーに記録し、次のaliasへ進む。
9. ZIP / XML の通常ファイルを列挙する。CSV、隠しファイル、一時ファイル、対象外拡張子、ディレクトリは登録しない。
10. ファイル属性を取得し、`file_sha256` を計算する。
11. `event_id`、`relative_path`、`file_sha256` で既存 `file_receipts` を確認する。
12. 未登録の場合のみ `file_receipts` に `status = DISCOVERED` でINSERTする。
13. 登録済みの場合は新規登録せず、重複件数としてRunサマリーへ集約する。
14. 個別ファイルの属性取得・SHA計算・INSERT失敗は `etl_errors` に記録し、可能な範囲で次ファイルへ進む。
15. `etl_runs.finished_at`、`status`、`summary_message` を更新して終了する。

## 7. file_receipts 登録ルール

登録対象は、Phase3の探索対象フォルダ配下に存在する ZIP / XML の通常ファイルのうち、`event_id` / `relative_path` / `file_sha256` の組み合わせが未登録のものとする。CSVは初期実装では登録せず、将来対応時にスキャン対象へ追加する。

| カラム | Phase3登録値の推奨 | 判断材料・補足 |
|---|---|---|
| `event_id` | 実行対象event ID。 | 後続処理の検索条件。 |
| `file_role` | `FROM_MEDICAL`。 | Phase3で検出する医療機関由来ファイルを表す決定済み値。 |
| `file_type` | 初期実装では `ZIP` / `XML`。CSV対応時に `CSV` を追加する。 | `zip_receipts` は作らず `file_type` で管理する決定済み。`OTHER` は初期実装では登録対象としない。 |
| `file_name` | パス末尾のファイル名。 | DDLでNOT NULL。 |
| `file_ext` | 小文字化した拡張子から先頭ドットを除いた値。 | 旧scanも拡張子条件で探索。 |
| `source_path` | 実ファイルの絶対パスまたは `result_root_path` から解決したフルパス。 | 後続 `02_import_xml.py` が元ファイルを辿るため必要。 |
| `relative_path` | `event.result_root_path` からの相対パス。 | 論理重複キーの要素。医療機関フォルダと `02_健診結果（編集）` を含める。 |
| `output_path` | `NULL`。 | Phase3は入力検出のみ。 |
| `file_sha256` | 登録前に計算した物理ファイルSHA-256。 | DDLでNOT NULL、重複判定要素。 |
| `file_size` | `stat()` で取得したバイト数。 | 旧scanでも保持。 |
| `processable_count` | `NULL`。 | Phase3では設定しない。ZIP展開/XML読込はPhase4責務。 |
| `insurer_number` | `NULL`。 | Phase3ではファイル名・XML中身から確定しない。 |
| `submitter_facility_code` | フォルダ名先頭コードを安全に抽出できる場合のみ候補。初期は `NULL` 推奨。 | 旧実装は施設フォルダ名を分解したが、v2の正は未定。 |
| `facility_code` | 初期は `NULL` 推奨。 | XML基本情報抽出はPhase4。 |
| `facility_name` | 初期は `dst_folder_norm` または `NULL` 候補。 | 医療機関名としてDBに入れるか、フォルダ表示名として扱うか要判断。 |
| `storage_folder_type` | `MEDICAL_RESULT_ROOT`。 | Phase3登録時の決定済み値。 |
| `status` | `DISCOVERED`。 | 決定済み。 |
| `summary_message` | 原則 `NULL`。警告をファイル単位に残す場合のみ短い要約。 | 詳細は `etl_errors` へ寄せる。 |
| `etl_run_id` | 当該scan Runの `etl_runs.id`。 | `02_import_xml.py` の入力Runになる。 |
| `first_seen_at` | INSERT時刻。DBデフォルト利用候補。 | 旧scanの `first_seen_at` 相当。 |
| `last_seen_at` | INSERT時は同時刻を入れる候補、または `NULL`。 | 再スキャン時に既存行更新しないなら `NULL` のままになる。運用判断が必要。 |
| `content_checked_at` | `NULL`。 | 中身確認をPhase4へ寄せるなら未設定。 |
| `received_at` | INSERT時刻候補。 | 「受領扱い時刻」をscan検出時にするか人間判断。 |
| `processed_at` | `NULL`。 | Phase4以降で更新。 |

## 8. 重複判定ルール

決定済みの論理重複キーは `event_id` / `relative_path` / `file_sha256` である。DDL上は `relative_path_sha256` 生成列を使い、`UNIQUE(event_id, relative_path_sha256, file_sha256)` で実装されている。

推奨する判定順は以下。

1. `relative_path` を決定する。
2. `file_sha256` を計算する。
3. `file_receipts` を `event_id` / `relative_path` / `file_sha256` で検索する。
4. 存在する場合は重複としてINSERTしない。
5. 存在しない場合はINSERTする。
6. 並行実行などでUNIQUE制約に衝突した場合は、重複スキップとして扱いRunサマリーに加算する。

注意点:

- 同一SHAでもパスが異なるファイルは、現行設計では別物理ファイルとして登録される。
- 同一パスでも内容SHAが変わったファイルは、再提出・差替えファイルとして新規登録される。
- `file_sha256` 単独UNIQUEは採用しない。
- 「同一ZIP SHAは処理対象外」とする旧実装の考え方は有用だが、Phase3の正式キーは `event_id` / `relative_path` / `file_sha256` である。

## 9. medical_folder_aliases 参照ルール

Phase2実装済みの `medical_folder_aliases` は `event_id = 2` の188件を初期データとして持つ。`UNIQUE(event_id, src_folder_raw)`、`is_active` 初期値 `1`、`manual_judgement` 初期値 `0` は決定済みである。

推奨:

- `src_folder_raw` を実フォルダ名として `<result_root_path>/<src_folder_raw>/02_健診結果（編集）/` を探索する。
- `dst_folder_norm` は内部表示名・出力先名・将来の正規化名として保持し、Phase3登録時に利用する場合は `facility_name` または `summary_message` ではなく、まず `relative_path` に含めずにalias情報として扱う。
- `is_active = 1` のaliasのみ探索対象にする。
- 未知フォルダ、`is_active = 0` alias、`manual_judgement = 1` alias はスキップし、必要に応じて `etl_errors` に記録する。

## 10. event.result_root_path 参照ルール

`dev_phr.event.result_root_path` はPhase2 migrationで追加済みで、型は `text NULL`。`03_decisions.md` では、対象 `event_id` の `result_root_path` が未設定の場合はv2処理でエラーとすることが決定済みである。

推奨:

- `NULL`、空文字、空白のみは設定なしとしてRun失敗。
- パスが存在しない、ディレクトリではない、権限等で参照できない場合もRun失敗。
- `result_root_path` が存在するが、個別医療機関フォルダや `02_健診結果（編集）` が存在しない場合は、Run全体失敗ではなくalias/フォルダ単位のエラーまたは警告として継続する。
- `source_path` は実ファイルのフルパス、`relative_path` は `result_root_path` からの相対パスにする。

## 11. エラー・スキップ方針

| 種別 | 推奨扱い | 記録先 |
|---|---|---|
| `event_id` 不正・存在なし | Run失敗 | `etl_runs.status`, `etl_errors` |
| `result_root_path` 未設定 | Run失敗 | `etl_runs.status`, `etl_errors` |
| `result_root_path` 参照不可 | Run失敗 | `etl_runs.status`, `etl_errors` |
| alias未登録の実フォルダ | 登録しない。未知フォルダとして扱う。 | Runサマリー、必要に応じて `etl_errors` |
| `is_active = 0` alias | スキップする。 | Runサマリー、必要に応じて `etl_errors` |
| `manual_judgement = 1` alias | スキップする。 | Runサマリー、必要に応じて `etl_errors` |
| 医療機関フォルダなし | 継続。 | Runサマリー、必要に応じて `etl_errors` |
| `02_健診結果（編集）` なし | 継続。 | Runサマリー、必要に応じて `etl_errors` |
| CSV | 初期実装では登録しない。 | Runサマリー |
| 対象外拡張子 | 登録しない。 | Runサマリー |
| 隠しファイル | 登録しない。 | Runサマリー |
| 一時ファイル | 登録しない。 | Runサマリー |
| `stat()` 失敗 | 当該ファイルを登録しない。継続。 | `etl_errors` |
| SHA-256計算失敗 | 当該ファイルを登録しない。継続。 | `etl_errors` |
| INSERT失敗 | 当該ファイルを登録しない。継続可能なら継続。 | `etl_errors` |
| 重複ファイル | エラーではない。INSERTしない。 | Runサマリー |

## 12. etl_runs / etl_errors 方針

### etl_runs

DDL上、`run_type` と `status` は `varchar` で値定義はDB制約化されていない。

採用値:

- `run_type`: `SCAN_FILES`
- 開始時 `status`: `RUNNING`
- 正常終了 `status`: `SUCCESS`
- 一部スキップやエラー記録ありで処理継続した場合の `status`: `WARNING`
- Run前提不備で終了する場合の `status`: `ERROR`

旧実装では `medi_import_runs` や共通ETLでrun単位の開始・終了・summary noteを持っていた。Phase3でも、件数サマリーを標準出力に表示し、可能な範囲で `etl_runs.summary_message` に記録する。`summary_message` は人間が読みやすい短いテキストとし、JSON等の構造化データは採用しない。

### etl_errors

Phase3の `etl_errors` は、運用上対応が必要な事象のみ記録する。ファイル登録前のエラーが多いため `file_receipt_id` が `NULL` になるケースを許容する。

推奨する記録単位:

- Run前提エラー: `run_id` 単位、`file_receipt_id = NULL`
- alias/フォルダエラー: `run_id` 単位、`file_receipt_id = NULL`、`error_message` に対象パス
- ファイル属性取得エラー: `run_id` 単位、`file_receipt_id = NULL`、`error_message` に対象パス
- SHA計算エラー: `run_id` 単位、`file_receipt_id = NULL`、`error_message` に対象パス
- INSERT後に発覚したファイル単位エラー: `file_receipt_id` を設定可能

Phase3で必要最小限の候補:

- `error_type`: `SCAN_PRECONDITION` / `FOLDER_SCAN` / `FILE_STAT` / `FILE_HASH` / `DB_INSERT`
- `error_code`: `RESULT_ROOT_PATH_MISSING` / `RESULT_ROOT_PATH_NOT_FOUND` / `EDIT_FOLDER_NOT_FOUND` / `UNKNOWN_MEDICAL_FOLDER` / `STAT_FAILED` / `SHA256_FAILED` / `INSERT_FAILED`
- `status`: `OPEN` / `RESOLVED`

`etl_errors.status` は `OPEN / RESOLVED` とする。`etl_errors.error_type` / `error_code` はPhase3で必要最小限のみ定義し、将来必要に応じて拡張する。

### scan結果サマリー

scan結果サマリーは標準出力に表示し、可能な範囲で `etl_runs.summary_message` にも記録する。`summary_message` は人間が読みやすい短いテキストとし、JSON等の構造化データは採用しない。

サマリーには、最低限以下を出すことを推奨する。

- `event_id`
- `result_root_path`
- alias件数
- 探索対象alias件数
- `is_active = 0` alias件数
- `manual_judgement = 1` alias件数
- 探索対象フォルダ件数
- 存在しない編集フォルダ件数
- 検出ファイル件数
- 新規登録件数
- 重複スキップ件数
- 対象外拡張子スキップ件数
- 隠し/一時ファイルスキップ件数
- エラー件数
- 拡張子別件数
- `file_type` 別件数

JSON等の構造化データは採用しない。将来、機械集計が必要になった場合は、`summary_message` ではなく専用カラムまたは別テーブルで検討する。

## 13. 未決事項の判断材料一覧

| 論点 | 旧スクリプトの挙動 | 今回の推奨 | 理由 | 人間判断 |
|---|---|---|---|---|
| 対象ファイル拡張子 | `medi_shared_files_scan.py` は `MEDI_SHARED_SCAN_EXTS` / `MEDI_SHARED_EXTS` を使い、既定は `zip`。`import_submit_csv.py` は別フローでCSVを扱う。 | 初期登録対象はZIP/XMLとする。CSVは初期実装では登録せず、将来対応時にスキャン対象へ追加する。 | v2はXML品質保証基盤中心。単体XMLは設計上処理対象。CSVを現時点で登録すると後続処理できない未処理データが蓄積する。 | 拡張子リストを設定化するか。 |
| ZIP / XML / CSV の扱い | ZIPは共有観測、hash、XML有無probe、inputコピー、ZIP取込へ進む。XML単体は主フローでは弱い。CSVは別スクリプト。 | ZIP/XMLは `file_receipts` に登録。ZIP展開/XML読込はPhase4。CSVは初期実装では登録しない。 | `01_scan_files.py` は検出と登録のみ、`02_import_xml.py` がZIP展開/XML読込を担当する決定済み。 | CSV対応時の追加仕様。 |
| `file_sha256` の計算タイミング | 旧scan後、`medi_shared_files_hash_zip.py` がZIPのみ後段計算。 | Phase3登録前に対象ファイルすべてで計算する。 | `file_receipts.file_sha256` はNOT NULLで、重複キーの一部。未計算ではINSERTできない。 | チャンクサイズ、巨大ファイル時の上限・タイムアウトを設定化するか。 |
| `file_receipts` へ登録するカラム | 旧 `medi_shared_files` はpath、file_name、ext、file_size、mtime、src_folder_raw、facility_hint、status等。後段でsha256等を追加。 | `event_id`, `file_role = FROM_MEDICAL`, `file_type = ZIP / XML`, `file_name`, `file_ext`, `source_path`, `relative_path`, `file_sha256`, `file_size`, `storage_folder_type = MEDICAL_RESULT_ROOT`, `status`, `etl_run_id`, 時刻系を登録。`processable_count` は `NULL`。 | DDLのNOT NULLと後続 `02_import_xml.py` 入力に必要な項目を満たす。中身由来の施設・保険者情報はPhase4責務。`OTHER` は初期実装では登録対象外。 | `received_at` をscan時刻にするか。`facility_name` に `dst_folder_norm` を入れるか。 |
| `file_receipts.status = DISCOVERED` の登録条件 | 旧scanは `stage_status='NEW'` でUPSERTし、後段で判定・コピー。 | 未登録かつZIP/XMLの対象ファイルとして扱える場合のみ `DISCOVERED` でINSERT。既存行のstatusは更新しない。対象外ファイルは登録しない。 | v2では状態遷移を `DISCOVERED / IMPORTING / IMPORTED / ERROR` に限定し、再スキャンで処理済み状態を戻さないため。対象外ファイルまで台帳化するとノイズになるため。 | なし。 |
| 再スキャン時の挙動 | 旧scanは `ON DUPLICATE KEY UPDATE` で `last_seen_at` を毎回更新し、`stage_status='NEW'` に戻す懸念がある。 | 毎回フルスキャンし、重複はINSERTしない。既存 `file_receipts` のstatusは変えない。`last_seen_at` 更新は行うなら慎重に別方針化。 | 再スキャンで後続処理状態を破壊しない。決定事項でも重複ファイルは新規登録しない。 | 既存行の `last_seen_at` だけ更新するか。更新する場合、処理済み行も更新するか。 |
| 重複判定 `event_id / relative_path / file_sha256` | 旧共有観測は `path_hash=SHA1(path)`、ZIP取込は `zip_sha256` 単独や `(zip_sha256, zip_inner_path)` を利用。 | 論理キー `event_id / relative_path / file_sha256` を基準にする。`relative_path` は `event.result_root_path` からの相対パスとする。DDLの生成列UNIQUE衝突も重複として扱う。 | 決定事項とDDLに整合。パス違い同一内容、同一パス内容差替えを区別できる。 | なし。 |
| `medical_folder_aliases` に存在しないフォルダの扱い | 旧copyはaliasありの行だけinputコピー対象。aliasなしはコピーされない。 | 未知フォルダ配下のファイルは登録せず、必要に応じて `etl_errors` に記録する。 | 誤フォルダ・未整備フォルダを自動登録すると医療機関対応が曖昧になる。 | `etl_errors` に記録する具体粒度。 |
| `is_active = 0` のaliasの扱い | 旧aliasの有効/無効運用はv2資料上の直接対応が薄い。 | スキップし、必要に応じて `etl_errors` に記録する。 | Phase2で `is_active` は有効/無効管理として定義済み。無効aliasからの自動登録は避けるべき。 | `etl_errors` に記録する具体粒度。 |
| `manual_judgement = 1` のaliasの扱い | 旧copyでは `COALESCE(manual_judgement, auto_judgement)='KENSHIN'` とし、手動判断が自動判定より優先。 | スキップし、手動確認が必要なものとして必要に応じて `etl_errors` に記録する。 | Phase3で自動登録すると、人間判断が必要なフォルダを後続処理へ送る可能性がある。 | `etl_errors` に記録する具体粒度。 |
| `event.result_root_path` 未設定時の扱い | 旧実装は `.env` の `MEDI_SHARED_ROOT` / `MEDI_IMPORT_INPUT_ROOT` に依存。 | Run失敗。`etl_errors` に `RESULT_ROOT_PATH_MISSING` 相当を記録。 | `03_decisions.md` で未設定時はエラーと決定済み。 | なし。エラーコード・メッセージ形式のみ要判断。 |
| 対象外ファイル・隠しファイル・一時ファイルの扱い | 旧scanは拡張子別 `rglob("*.ext")` で対象外を自然に拾わない。 | 登録しない。原則 `etl_errors` にも記録しない。 | 共有フォルダにはOS生成物やアップロード途中ファイルが混ざり得る。対象外ファイルまで記録するとノイズが増え、運用負荷が高くなる。 | 一時ファイル判定パターンを決める必要がある。例: `.~`, `~$`, `.tmp`, `.part`, `.crdownload` など。 |
| エラー時の `etl_errors` 記録単位 | 旧scanの `stat()` 失敗はwarningで継続。hash/probe失敗はnoteに保存。ZIP取込は可能な限りreceiptやprocess logに記録。 | 運用上対応が必要な事象のみ記録する。未知フォルダ、無効alias、`manual_judgement = 1` aliasなどを基本とする。`etl_errors.status` は `OPEN / RESOLVED`。 | 人が対応すべき事象のみをETLエラーとして管理することで、障害対応の優先度を明確にできる。 | Phase3実装時に必要最小限の `error_type` / `error_code` 具体値を定義する。 |
| `etl_runs` の run_type / status | 旧 `medi_import_runs` と共通ETLでrunを作り、summary noteを更新。status値は既存基盤に揺れがある。 | `run_type = SCAN_FILES`。`status = RUNNING / SUCCESS / WARNING / ERROR`。 | DDLはvarcharで制約なし。scan処理の意味を明確にし、一部スキップやエラー記録ありで継続した場合を `WARNING` として表現する。 | なし。 |
| scan結果サマリーとして何を出すか | 旧実装は標準ログ、note、run summaryに件数を出す。 | scan結果サマリーは標準出力に表示し、可能な範囲で `etl_runs.summary_message` に記録する。`summary_message` は人間が読みやすい短いテキストとし、JSON等の構造化データは採用しない。 | 重複件数は `file_receipts` に残さない決定のため、Runサマリーが主な証跡になる。 | なし。 |

## 14. Phase3 実装GO判定

判定: GO。

Phase3の主責務である「`event.result_root_path` と `medical_folder_aliases` を参照し、対象フォルダをフルスキャンし、未登録ファイルのみ `file_receipts.status = DISCOVERED` で登録する」ための骨子は決定済みである。Phase1 DDLとPhase2 seed/migrationも、実装に必要なテーブル・カラムとしては揃っている。

Phase3実装前の主要な判断事項は整理済みであり、実装へ進める。

実装時には、必要最小限の `etl_errors.error_type` / `error_code` 具体値と一時ファイル判定パターンを、ここで確定した方針の範囲内で定義する。
