# Current Script Specs by Codex

## 調査日時
2026-06-25 15:57 JST

## 調査対象
- `scripts/kenshin_list_pydir/scripts/`
  - `medi_shared_files_scan.py`
  - `medi_shared_files_hash_zip.py`
  - `medi_shared_files_auto_judge.py`
  - `medi_shared_files_copy_to_input.py`
  - `medi_zip_import.py`
  - `medi_xml_item_extract.py`
  - `normalize_db_update.py`
  - `normalize_item_values.py`
  - `import_submit_csv.py`
  - `medi_export_xml.py`
  - `medi_trans_06139463.py`
- `scripts/kenshin_list_pydir/kenshin_lib/`
  - `kenshin_lib/medi/db_shared_files.py`
  - `kenshin_lib/medi/db_medi.py`
  - `kenshin_lib/medi/xml_extract.py`
  - `kenshin_lib/medi/zip_extract.py`
  - `kenshin_lib/medi/zip_inspect.py`
  - `kenshin_lib/medi/zip_passwords.py`
  - `kenshin_lib/db_value_update.py`
  - `kenshin_lib/phr/db_phr.py`
- `scripts/kenshin_list_pydir/lib/`
  - `lib/normalize/common.py`
  - `lib/errors/normalize.py`
  - `lib/custom_id_gen.py`

## 全体概要
現行参照実装は、医療機関等から受領した健診結果 ZIP/XML を、共有フォルダ観測、健診対象判定、ローカル input へのコピー、ZIP 受領台帳化、XML 受領台帳化、XML 索引抽出、健診項目値の生抽出、正規化、XML 再出力、保険者固有変換へ分けて処理するスクリプト群である。

事実として、共有フォルダ観測系は `medi_shared_files` を中心にファイル単位の台帳を作る。ZIP 取込系は `medi_import_runs`、`medi_zip_receipts`、`medi_xml_receipts`、`medi_xml_ledger`、`medi_xml_process_logs` を使い、ZIP/XML の受領・抽出・処理ログを管理する。健診項目値は `medi_xml_item_values` と `medi_exam_result_item_values` の2系統が存在し、前者は XML observation の生抽出、後者は正規化・XML出力で参照される現行成果物寄りの item values と見られる。

## 実行順序の推定
以下はスクリプト名・テーブル依存・docstring からの推測である。

1. shared files scan: `medi_shared_files_scan.py`
2. hash / zip inspect: `medi_shared_files_hash_zip.py`
3. auto judge: `medi_shared_files_auto_judge.py`
4. copy to input: `medi_shared_files_copy_to_input.py`
5. zip import: `medi_zip_import.py` の `ZIP_IMPORT` または `FULL`
6. xml index extract: `medi_zip_import.py` の `XML_EXTRACT` または `FULL`
7. xml item extract: `medi_xml_item_extract.py`
8. normalize: `normalize_db_update.py`、`normalize_item_values.py`
9. export: `medi_export_xml.py`
10. insurer-specific transform: `medi_trans_06139463.py`

補足: `import_submit_csv.py` は医療機関 ZIP/XML 受領フローとは別に、企業個別提出CSVを生取込テーブルへ投入するための補助系スクリプトと推測する。

---

# スクリプト仕様

## medi_shared_files_scan.py

### 役割
共有フォルダを拡張子指定で走査し、観測したファイルを `medi_shared_files` に UPSERT する。`path_hash=SHA1(path)` を一意キーとし、共有フォルダ観測フェーズの正本実装として書かれている。

### 入力
- フォルダ: `MEDI_SHARED_ROOT`
- 設定ファイル: `scripts/kenshin_list_pydir/.env`
- 環境変数: `MEDI_SHARED_SCAN_EXTS`、`MEDI_SHARED_EXTS`、`MEDI_SHARED_SCAN_LIMIT`、`MEDI_SHARED_FACILITY_HINT_DEPTH`
- DB: `MEDI_IMPORT_DB_HOST`、`MEDI_IMPORT_DB_PORT`、`MEDI_IMPORT_DB_NAME`、`MEDI_IMPORT_DB_USER`、`MEDI_IMPORT_DB_PASSWORD`

### 出力
- DB: `medi_shared_files`
- ログ: 標準出力/標準エラーの logger

### 参照テーブル
- 直接の SELECT はなし。
- `ON DUPLICATE KEY UPDATE` のため `medi_shared_files` の既存行を暗黙に参照する。

### 更新テーブル
- `medi_shared_files`

### 主な処理
- `.env` を読み込む。
- `MEDI_SHARED_ROOT` の存在を確認する。
- 許可拡張子を `MEDI_SHARED_SCAN_EXTS`、`MEDI_SHARED_EXTS`、既定 `zip` の順で決める。
- `rglob("*")` ではなく `rglob("*.ext")` で対象ファイルを列挙する。
- `path`、`file_name`、`ext`、`file_size`、`mtime`、`src_folder_raw`、`facility_hint` を取得する。
- `SharedFileRow` を作成し、`db_upsert_shared_file()` で `medi_shared_files` に UPSERT する。
- 2000件ごとに commit する。

### status更新
- `medi_shared_files.stage_status='NEW'` を設定する。
- `auto_judgement='UNKNOWN'` を設定する。
- `first_seen_at` は初回のみ、`last_seen_at` は毎回更新される。

### エラー時の挙動
- `stat()` 失敗は warning を出し、`file_size=0`、`mtime=None` で継続する。
- 全体例外では rollback して再送出する。

### 後続スクリプト
- `medi_shared_files_hash_zip.py`

### health_exam_result v2で再利用できそうな処理
- ファイル台帳の走査・UPSERT 方針。
- `path_hash` による冪等化。
- `first_seen_at` / `last_seen_at` の考え方。
- UNC/共有フォルダ負荷を考慮した拡張子別探索。

### health_exam_result v2で見直すべき処理
- `stage_status` を scan 時に常に `NEW` へ戻す挙動は、v2 の状態遷移と衝突しないか要確認。
- `src_folder_raw` / `facility_hint` は運用依存が強いため、施設マスタ・受領フォルダ設計と合わせて再定義する。

## medi_shared_files_hash_zip.py

### 役割
`medi_shared_files` の ZIP 行のうち、`sha256` 未計算のものに対してファイル内容の SHA-256 を計算し DB に保存する。

### 入力
- ファイル: `medi_shared_files.path` が指す ZIP ファイル
- DB: `medi_shared_files`
- 設定ファイル: `.env`
- 環境変数: `MEDI_SHARED_HASH_LIMIT`、`MEDI_SHARED_HASH_ONLY_STAGE`、`MEDI_SHARED_HASH_CHUNK_MB`、`MEDI_IMPORT_DB_*`

### 出力
- DB: `medi_shared_files.sha256`、`medi_shared_files.note`
- ログ: logger

### 参照テーブル
- `medi_shared_files`

### 更新テーブル
- `medi_shared_files`

### 主な処理
- `ext='zip'`、`sha256 IS NULL OR sha256=''`、必要に応じて `stage_status` 条件で対象を取得する。
- `path` の存在を確認する。
- ファイルをチャンク読み込みして SHA-256 を計算する。
- 成功時は `sha256` と `updated_at` を更新する。
- 欠損・失敗時は `note` と `updated_at` を更新する。
- 50件ごとに commit する。

### status更新
- `stage_status` は更新しない。

### エラー時の挙動
- ソースファイル欠損時は `note='source missing when hashing'` として継続する。
- ハッシュ計算例外時は `note='hash failed: ...'` として継続する。
- 全体例外では rollback して再送出する。

### 後続スクリプト
- `medi_shared_files_auto_judge.py`

### health_exam_result v2で再利用できそうな処理
- ZIP重複判定用の `sha256` 計算。
- LIMIT・チャンクサイズ指定。

### health_exam_result v2で見直すべき処理
- 失敗を `note` のみで表現しているため、v2 ではエラーコード・エラー台帳へ分離したい。

## medi_shared_files_auto_judge.py

### 役割
`medi_shared_files` の NEW ZIP を対象に ZIP 内 XML 有無を確認し、`auto_judgement` を `KENSHIN` または `UNKNOWN` に更新する。

### 入力
- ファイル: `medi_shared_files.path` の ZIP
- DB: `medi_shared_files`
- 設定ファイル: `.env`
- 環境変数: `MEDI_SHARED_AUTO_LIMIT`、`MEDI_SHARED_AUTO_ONLY_STAGE`、`MEDI_SHARED_AUTO_PROBE_ALWAYS`、`MEDI_SHARED_AUTO_COMMIT_EVERY`、`MEDI_IMPORT_DB_*`

### 出力
- DB: `medi_shared_files.zip_has_xml`、`zip_xml_count`、`zip_xml_checked_at`、`auto_judgement`、`note`
- ログ: logger

### 参照テーブル
- `medi_shared_files`

### 更新テーブル
- `medi_shared_files`

### 主な処理
- `db_select_new_zip_files_for_judge()` で `ext='zip'`、`stage_status=only_stage`、`sha256` あり、`manual_judgement IS NULL` の行を取得する。
- `zip_has_xml` 未設定または `MEDI_SHARED_AUTO_PROBE_ALWAYS=true` の場合、`probe_zip_has_xml()` で ZIP 内 XML 数を確認する。
- probe 成功時は `zip_has_xml`、`zip_xml_count`、`zip_xml_checked_at` を更新する。
- `zip_has_xml==1` なら `auto_judgement='KENSHIN'`、それ以外は `UNKNOWN` とする。
- 指定件数ごとに commit する。

### status更新
- `stage_status` は更新しない。
- `auto_judgement` を更新する。

### エラー時の挙動
- probe 失敗時は `zip_has_xml=NULL`、`zip_xml_count=NULL`、`note` に失敗理由を保存し、`auto_judgement='UNKNOWN'` として継続する。
- 全体例外では rollback して再送出する。

### 後続スクリプト
- `medi_shared_files_copy_to_input.py`

### health_exam_result v2で再利用できそうな処理
- ZIP 内 XML 有無の軽量 probe。
- 自動判定と手動判定を分ける考え方。

### health_exam_result v2で見直すべき処理
- `UNKNOWN` は非健診確定ではないため、v2 では判定理由と次アクションを明示するステータスが必要。

## medi_shared_files_copy_to_input.py

### 役割
共有フォルダ上で健診対象と判定された ZIP を、`MEDI_IMPORT_INPUT_ROOT/<dst_folder_norm>/<file_name>` へコピーする。

### 入力
- ファイル: `medi_shared_files.path` の ZIP
- フォルダ: `MEDI_IMPORT_INPUT_ROOT`
- DB: `medi_shared_files`、`medi_shared_folder_aliases`、`medi_zip_receipts`
- 設定ファイル: `.env`
- 環境変数: `MEDI_SHARED_COPY_LIMIT`、`MEDI_SHARED_COPY_OVERWRITE`、`MEDI_IMPORT_DB_*`

### 出力
- ファイル: input フォルダ配下の ZIP コピー
- DB: `medi_shared_files.stage_status`、`note`
- ログ: logger

### 参照テーブル
- `medi_shared_files`
- `medi_shared_folder_aliases`
- `medi_zip_receipts`

### 更新テーブル
- `medi_shared_files`

### 主な処理
- `stage_status='NEW'`、`ext='zip'`、`sha256` あり、`COALESCE(manual_judgement, auto_judgement)='KENSHIN'`、`zip_has_xml=1`、alias あり、同一 `zip_sha256` が `medi_zip_receipts` にない行を取得する。
- コピー先ディレクトリを作成する。
- コピー先同名ファイルがあり overwrite false の場合はコピー済み扱いにする。
- `shutil.copy2()` でコピーする。
- 結果に応じて `db_mark_stage_status()` を呼ぶ。

### status更新
- コピー成功: `stage_status='INPUT_COPIED'`
- コピー先既存かつ overwrite false: `stage_status='INPUT_COPIED'`
- ソース欠損: `stage_status='SKIPPED'`
- その他失敗: 原則 `stage_status='NEW'`

### エラー時の挙動
- ソース欠損は SKIPPED として継続する。
- mkdir/copy 失敗は note に理由を残して NEW のまま継続する。
- 全体例外では rollback して再送出する。

### 後続スクリプト
- `medi_zip_import.py`

### health_exam_result v2で再利用できそうな処理
- ネットワーク共有フォルダからローカル input/work へコピーして処理する境界。
- `manual_judgement` 優先の考え方。
- 既に取り込んだ ZIP SHA を除外する冪等性。

### health_exam_result v2で見直すべき処理
- alias テーブルで施設フォルダ名を決める設計は v2 の受領フォルダ仕様と整合させる。
- ファイルコピーと DB ステータス更新の非トランザクション性を明示的なリカバリ設計にする。

## medi_zip_import.py

### 役割
input フォルダ配下の施設フォルダを走査し、ZIP 受領台帳 `medi_zip_receipts` と XML 受領台帳 `medi_xml_receipts` を UPSERT する。`MEDI_IMPORT_MODE` により ZIP_IMPORT、XML_EXTRACT、FULL を切り替える。

### 入力
- フォルダ: `MEDI_IMPORT_INPUT_ROOT/<facility_code>_<facility_name>/*.zip`
- 作業フォルダ: `MEDI_IMPORT_TEMP_ROOT`
- DB: `medi_import_runs`、`medi_zip_receipts`、`medi_xml_receipts`、`medi_zip_passwords`
- 設定ファイル: `.env`
- 環境変数: `MEDI_IMPORT_MODE`、`MEDI_IMPORT_XML_ENABLED`、`MEDI_IMPORT_XML_PARSE_WELLFORMED`、`MEDI_IMPORT_XML_EXTRACT_LIMIT`、`MEDI_IMPORT_XML_TARGET_STATUS`、`MEDI_IMPORT_DB_*`

### 出力
- DB: `medi_import_runs`、`medi_zip_receipts`、`medi_zip_receipt_runs`、`medi_xml_receipts`、`medi_xml_receipt_runs`
- 一時ファイル: `MEDI_IMPORT_TEMP_ROOT/run_<run_id>/...` 配下に展開し、原則削除する。
- ログ: logger

### 参照テーブル
- `medi_zip_receipts`
- `medi_xml_receipts`
- `medi_zip_passwords`
- XML_EXTRACT モードでは `medi_xml_receipts`、`medi_zip_receipts`

### 更新テーブル
- `medi_import_runs`
- `medi_zip_receipts`
- `medi_zip_receipt_runs`
- `medi_xml_receipts`
- `medi_xml_receipt_runs`
- XML_EXTRACT モードでは `medi_xml_process_logs`、`medi_xml_ledger`、`medi_xml_receipts`

### 主な処理
- run を `medi_import_runs` に作成する。
- 施設フォルダ名を `facility_code` と `facility_name` に分解する。
- ZIP SHA-256 を計算し、既存 ZIP なら `SEEN`、新規なら `NEW` とする。
- `medi_zip_passwords` の候補を使い ZIP 展開を試行する。
- DATA フォルダ数と XML 件数を確認する。
- DATA が複数でも XML が拾えれば `structure_status='OK'` とし、異常は `error_code` / message に残す。
- `medi_zip_receipts` と `medi_zip_receipt_runs` に記帳する。
- `MEDI_IMPORT_XML_ENABLED=true` かつ ZIP 構造 OK の場合、XML を棚卸しして `medi_xml_receipts` と `medi_xml_receipt_runs` に記帳する。
- `MEDI_IMPORT_MODE=XML_EXTRACT` または `FULL` の場合、`xml_extract_phase()` を実行する。
- run 終了時に `medi_import_runs.finished_at` と summary note を更新する。

### status更新
- `medi_zip_receipts.structure_status`: `OK` / `ERROR`
- `medi_xml_receipts.status`: XML 棚卸し時は原則 `PENDING`、well-formed 失敗時は `ERROR`
- XML_EXTRACT 成功時は `medi_xml_receipts.status='OK'`
- XML_EXTRACT 失敗時は `medi_xml_receipts.status='ERROR'`

### エラー時の挙動
- ZIP SHA 計算失敗はログ出力して当該 ZIP をスキップする。
- ZIP 展開・構造判定失敗は `medi_zip_receipts` に ERROR として可能な限り記帳する。
- XML 棚卸し中の個別 XML 例外は `medi_xml_receipts.status='ERROR'` として記帳する。
- DB upsert 失敗時は rollback し、当該 ZIP の一時展開を削除して次へ進む。

### 後続スクリプト
- `medi_xml_item_extract.py`
- 推測: `normalize_db_update.py`、`normalize_item_values.py`

### health_exam_result v2で再利用できそうな処理
- `medi_import_runs` による run 単位管理。
- ZIP Receipt と XML Receipt の分離。
- `zip_sha256`、`xml_sha256`、`zip_inner_path_sha256` による冪等化。
- DATA フォルダが非標準でも XML 検出を優先して棚卸しする方針。
- 暗号 ZIP パスワード候補処理。

### health_exam_result v2で見直すべき処理
- XML_EXTRACT が同じ runner に含まれるため、v2 では ZIP 受領と XML 抽出を明確に分離した方がよい。
- `PENDING`、`OK`、`ERROR` の粒度は XML Ledger のチェック結果と状態遷移に合わせて再設計する。

## medi_xml_item_extract.py

### 役割
`medi_xml_receipts.status='OK'` の XML を ZIP から読み出し、CDA `observation` を走査して item の生値を `medi_xml_item_values` に UPSERT する。

### 入力
- ファイル: `medi_zip_receipts.zip_path` が指す ZIP 内の `medi_xml_receipts.zip_inner_path`
- DB: `medi_xml_receipts`、`medi_zip_receipts`、`medi_zip_passwords`、`medi_import_runs`、`exam_item_master`
- 設定ファイル: `.env`
- 環境変数: `ITEM_EXTRACT_LIMIT`、`ITEM_EXTRACT_RUN_ID`、`ITEM_EXTRACT_NOTE`、`ITEM_EXTRACT_ZIP_PASSWORD_ENABLED`、`MEDI_IMPORT_DB_*`、`PHR_MYSQL_*`

### 出力
- DB: `medi_xml_item_values`
- DB: `medi_xml_receipts.items_extract_status`、`items_extracted_run_id`、`items_extracted_at`
- DB: `medi_xml_process_logs`
- DB: `medi_import_runs`

### 参照テーブル
- `medi_xml_receipts`
- `medi_zip_receipts`
- `medi_zip_passwords`
- `medi_import_runs`
- `dev_phr.exam_item_master`

### 更新テーブル
- `medi_import_runs`
- `medi_xml_item_values`
- `medi_xml_receipts`
- `medi_xml_process_logs`

### 主な処理
- run を新規作成、または `ITEM_EXTRACT_RUN_ID` の既存 run を使用する。
- `exam_item_master` を読み、`namecode` map を作る。
- `medi_xml_receipts` から `status='OK'` かつ `items_extract_status <> 'OK'` の行を取得する。
- ZIP 内 XML member を読み出す。暗号 ZIP は `medi_zip_passwords` 候補を使う。
- lxml で XML を parse する。
- ルートが CDA `ClinicalDocument` でない場合は SKIP とする。
- `//cda:observation` を走査し、`code/@code` を `namecode` とする。
- `observation/value` を優先し、なければ `observation/text` をフォールバックにする。
- `exam_item_master.xml_value_type`、`value_method` はヒントとして使うが、master 未登録でも抽出は行う。
- `medi_xml_item_values` に `UNIQUE(xml_sha256, namecode, occurrence_no)` で UPSERT する。

### status更新
- `written>0`: `items_extract_status='OK'`
- CDA でない: `items_extract_status='SKIP'`
- parse 失敗、ZIP member 不明、0件抽出: `items_extract_status='ERROR'`

### エラー時の挙動
- 個別 XML の失敗は `medi_xml_process_logs` と `medi_xml_receipts.items_extract_status` に記録し、次の XML へ進む。
- 50件ごとに commit する。
- 終了コードは `err>0` または `zero_hit>0` で 2。

### 後続スクリプト
- 推測: `normalize_item_values.py` または v2 の item_values 登録/正規化処理。

### health_exam_result v2で再利用できそうな処理
- XML observation から item 値を広く抽出するロジック。
- `xml_sha256 + namecode + occurrence_no` による生値の冪等化。
- CDA 判定、ZIP member 読み出し、暗号 ZIP 対応。

### health_exam_result v2で見直すべき処理
- v2 の item_values は `subscribers.id` や受診者・年度と結びつく必要があるため、`medi_xml_item_values` のままでは不足。
- master 未登録でも抽出する方針は台帳としては有用だが、制度チェック対象項目との区別が必要。

## normalize_db_update.py

### 役割
既存 DB テーブルの照合用派生列を正規化して UPDATE する汎用オーケストレーター。カナ、保険証番号、保険証記号などを `*_match` 列へ書き戻す。

### 入力
- DB: `work_other.find_xml_subscribers_list_20260128`、`work_other.medi_xml_ledger`、`work_other.medi_exam_result_ledger`
- 設定ファイル: `.env`
- 環境変数: `NORMALIZE_DRY_RUN`、`NORMALIZE_JOB_*`

### 出力
- DB: 対象テーブルの `*_match` 派生列
- ログ: print

### 参照テーブル
- `find_xml_subscribers_list_20260128`
- `medi_xml_ledger`
- `medi_exam_result_ledger`

### 更新テーブル
- `find_xml_subscribers_list_20260128`
- `medi_xml_ledger`
- `medi_exam_result_ledger`

### 主な処理
- `build_job_specs()` で Job 一覧を定義する。
- `.env` の `NORMALIZE_JOB_*` が true の Job だけを実行対象にする。
- `run_update_job()` が対象行を SELECT し、transform 関数で正規化し、値が変わる場合だけ UPDATE する。
- `--dry-run` または `NORMALIZE_DRY_RUN` で更新せず件数確認できる。

### status更新
- status 系カラムは更新しない。

### エラー時の挙動
- Job 実行中の例外は上位へ伝播する。
- dry-run は rollback する。

### 後続スクリプト
- 推測: 名寄せ、subscriber 紐付け、`medi_export_xml.py`

### health_exam_result v2で再利用できそうな処理
- カナ・保険証記号・番号の照合用正規化関数。
- dry-run 前提の大量 UPDATE ユーティリティ。

### health_exam_result v2で見直すべき処理
- テーブル名が固定・暫定名を含むため、v2 の正式テーブルに合わせた Job 定義が必要。
- `MYSQL_*` と `MEDI_IMPORT_DB_*` 系 ENV が混在しているため整理したい。

## normalize_item_values.py

### 役割
`medi_exam_result_item_values.normalize_status='RAW'` の項目値を、`exam_item_master` と `norm_variants` を参照して最小限正規化し、`value` と `normalize_status` を更新する。

### 入力
- DB: `work_other.medi_exam_result_item_values`
- DB: `dev_phr.exam_item_master`、`dev_phr.norm_variants`
- 設定ファイル: `.env`
- 環境変数: `NORMALIZE_LIMIT`、`MEDI_IMPORT_DB_*`、`PHR_MYSQL_*`

### 出力
- DB: `medi_exam_result_item_values.value`、`normalize_status`、`normalized_at`、`normalize_error`
- ログ: print

### 参照テーブル
- `medi_exam_result_item_values`
- `exam_item_master`
- `norm_variants`

### 更新テーブル
- `medi_exam_result_item_values`

### 主な処理
- `normalize_status='RAW'` かつ `value IS NULL OR value=''` の行を取得する。
- `exam_item_master` から `xml_value_type` と `result_code_oid` を取得する。
- ST は raw をそのまま、PQ は trim 後に float 変換可能性を確認、CD/CO は `norm_variants` 完全一致で `normalized_code` を取得する。
- OK の場合は `value` を設定し `normalize_status='OK'`。
- ERROR の場合は `normalize_status='ERROR'` と `normalize_error` を設定する。
- ループ完了後に1回 commit する。

### status更新
- `normalize_status='OK'`
- `normalize_status='ERROR'`

### エラー時の挙動
- 個別行の正規化失敗は `normalize_error` に記録して継続する。
- ERROR が1件以上あれば終了コード 2。

### 後続スクリプト
- `medi_export_xml.py`

### health_exam_result v2で再利用できそうな処理
- `ST`、`PQ`、`CD`、`CO` の最小正規化ルール。
- `norm_variants` 完全一致を使うコード正規化。

### health_exam_result v2で見直すべき処理
- 現行対象は `medi_exam_result_item_values` であり、`medi_xml_item_values` とは別テーブル。v2 では raw value、normalized value、status の保持先を一本化または責務分離する必要がある。
- エラー・ログを item_values 内に持つか、別台帳に持つか検討が必要。

## import_submit_csv.py

### 役割
企業個別の提出用 CSV を、指定された DB 生取込テーブルへそのまま INSERT する。項目解釈・正規化・名寄せは行わない。

### 入力
- ファイル: `SUBMIT_INBOX_ROOT` 配下の CSV 1本、または `SUBMIT_CSV_FILENAME`
- DB: `csv_header_map_submit`、取込先テーブル、`information_schema.COLUMNS`
- 設定ファイル: `.env`
- 環境変数: `SUBMIT_INBOX_ROOT`、`SUBMIT_TARGET_TABLE`、`SUBMIT_TRUNCATE`、`SUBMIT_CSV_FILENAME`、`SUBMIT_INSERT_BATCH`、`SUBMIT_DB_*`

### 出力
- DB: `SUBMIT_TARGET_TABLE`
- ログ: logger

### 参照テーブル
- `csv_header_map_submit`
- `information_schema.COLUMNS`

### 更新テーブル
- `SUBMIT_TARGET_TABLE`

### 主な処理
- inbox 内 CSV が1本であることを確認する。
- `csv_header_map_submit` を `display_order` 順に読み、期待ヘッダーと INSERT 対象カラムを作る。
- CSV ヘッダーを `csv_header` または `original_header` と照合する。
- `information_schema.COLUMNS` から取込先の数値カラムを判定する。
- `SUBMIT_TRUNCATE=true` の場合は対象テーブルを TRUNCATE する。
- CSV 行を読み、空文字は NULL、数値カラムの非数値は NULL に変換して batch INSERT する。

### status更新
- status 系カラムは更新しない。

### エラー時の挙動
- CSV 複数、ヘッダー不一致、列数不一致などは例外で停止する。
- INSERT は batch ごとに commit する。途中失敗時の全体 rollback はない。

### 後続スクリプト
- 推測: CSV 由来の健診結果 ledger/item_values 作成処理。

### health_exam_result v2で再利用できそうな処理
- CSV ヘッダー map による固定レイアウト取込。
- 日本語 CSV の encoding fallback。
- information_schema による数値カラム判定。

### health_exam_result v2で見直すべき処理
- v2 初期ゴールが ZIP/XML 中心なら直接対象外。
- TRUNCATE して再取込する運用は、受領台帳・差分管理とは相性が悪いため分離が必要。

## medi_export_xml.py

### 役割
`medi_exam_result_ledger` と `medi_exam_result_item_values` から、厚労省標準様式 CDA V08 個人 XML と IX08 を生成し、「健診実施機関 × 提出先保険者」単位で ZIP 出力する。

### 入力
- DB: `work_other.medi_exam_result_ledger`
- DB: `work_other.medi_exam_result_item_values`
- DB: `dev_phr.exam_item_master`
- フォルダ: `xsd/`
- 設定ファイル: `.env`
- 環境変数: `EXPORT_ROOT`、`EXPORT_FILE_DATE`、`EXPORT_LIMIT`、`EXPORT_IX08_NAME`、`EXPORT_XML_ENCODING`、`EXPORT_SPLIT_NO`、`EXPORT_IMPL_CODE`、`EXPORT_FILE_KIND`、`EXPORT_GROUP_DIR_TEMPLATE`、`EXPORT_ZIP_NAME_TEMPLATE`、`MEDI_IMPORT_DB_*`

### 出力
- ファイル: `<EXPORT_ROOT>/<root_dir>/ix08.xml`
- ファイル: `<EXPORT_ROOT>/<root_dir>/DATA/*.xml`
- フォルダ: `<EXPORT_ROOT>/<root_dir>/XSD/`
- ファイル: `<EXPORT_ROOT>/<root_dir>.zip`
- DB: なし

### 参照テーブル
- `work_other.medi_exam_result_ledger`
- `work_other.medi_exam_result_item_values`
- `dev_phr.exam_item_master`

### 更新テーブル
- なし

### 主な処理
- ledger を `health_examination_organization_no`、`insurer_number`、`ledger_id` 順に取得する。
- `EXPORT_LIMIT` があれば件数を制限する。
- `sender_org_no(10桁)` と `receiver_insurer_no(8桁)` でグループ化する。
- グループごとに出力 root、`DATA`、`XSD` を作成し、XSD をコピーする。
- ledger ごとに item values と `exam_item_master` を JOIN して項目を取得する。
- CDA `ClinicalDocument` を生成する。
- `annex2_legal_report_flag` により法定セクション `01010` と任意セクション `01990` に振り分ける。
- IX08 を生成する。
- root フォルダごと ZIP 化する。

### status更新
- status 系カラムは更新しない。

### エラー時の挙動
- XSD フォルダ欠損、出力失敗、DB 接続失敗等は例外で停止する。
- DB 更新はない。

### 後続スクリプト
- 推測: HIA アップロードまたは `medi_trans_06139463.py` のような保険者固有変換。

### health_exam_result v2で再利用できそうな処理
- CDA/IX08 生成の具体ロジック。
- 法定/任意セクション振り分け。
- XML 出力前の住所・郵便番号・電話番号正規化。

### health_exam_result v2で見直すべき処理
- v2 初期ゴールが受領・台帳・チェック中心なら export は後段として分離する。
- `medi_exam_result_ledger` 前提のため、v2 の XML Ledger / item_values から出力するなら adapter が必要。

## medi_trans_06139463.py

### 役割
保険者番号 `06139463` 向けに、受領 ZIP 内の `DATA/*.xml` を HIA 取込前に一括変換し、再 ZIP 化する。DB 更新は行わない。

### 入力
- ファイル: `<TRANS_ROOT_DIR>/in/*.zip`
- フォルダ: 既定 `<kenshin_list_pydir>/medi_trans_06139463/in`
- 環境変数: `TRANS_ROOT_DIR`、`TRANS_INSURER_NO`、`TRANS_DRY_RUN`、`TRANS_KEEP_TEMP`

### 出力
- ファイル: `<TRANS_ROOT_DIR>/out/out_YYYYMMDD_HHMMSS/*.zip`
- ログファイル: `OK_<zip_name>.log`、`ERROR_<zip_name>.log`
- ファイル移動: 成功した元 ZIP を `<TRANS_ROOT_DIR>/done/done_YYYYMMDD_HHMMSS/` へ移動
- DB: なし

### 参照テーブル
- なし

### 更新テーブル
- なし

### 主な処理
- 入力 ZIP を名前順に処理する。
- ZIP を一時ディレクトリへ展開する。
- `DATA/*.xml` を探す。
- XML を parse し namespace を再登録して `ns0` prefix が出ない形で再シリアライズする。
- 保険者番号 root `1.2.392.200119.6.101` を `06139463` に上書きする。
- 記号 root `1.2.392.200119.6.204` と番号 root `1.2.392.200119.6.205` は数字化し先頭0を削除する。
- `telecom/@value` の `tel:` 以降を数字のみにする。
- 住所テキストを空白なし・全角・cp932 80バイト以内へ寄せる。
- 不正な HLD participant を削除する。
- 同じ構成で再 ZIP 化する。

### status更新
- status 系カラムはなし。

### エラー時の挙動
- 1つでも ZIP 処理に失敗したらバッチ全体を停止する。
- 失敗 ZIP は `in/` に残る。
- それ以前に成功した ZIP は out/done へ移動済みでロールバックしない。
- スタックトレース付き `ERROR_*.log` を出力する。

### 後続スクリプト
- HIA アップロード。

### health_exam_result v2で再利用できそうな処理
- 保険者固有変換を共通処理から分離する考え方。
- XML namespace 再シリアライズ、電話番号、住所、HLD participant の補正ロジック。

### health_exam_result v2で見直すべき処理
- 保険者番号固定・フォルダ固定であり、v2 では保険者別 plugin/adapter 的に分離したい。
- 変換前後の差分・監査ログがファイルログのみなので、台帳に残すか検討する。

---

# DB・台帳との対応

## ファイル台帳候補
- 事実: `medi_shared_files` が共有フォルダ上のファイル台帳に相当する。
- 主な処理: `medi_shared_files_scan.py`、`medi_shared_files_hash_zip.py`、`medi_shared_files_auto_judge.py`、`medi_shared_files_copy_to_input.py`
- 主なカラム: `path_hash`、`path`、`src_folder_raw`、`facility_hint`、`file_name`、`ext`、`file_size`、`mtime`、`sha256`、`auto_judgement`、`manual_judgement`、`stage_status`、`zip_has_xml`、`zip_xml_count`、`first_seen_at`、`last_seen_at`、`note`

## ZIP Receipt候補
- 事実: `medi_zip_receipts` が ZIP Receipt に相当する。
- 補助: `medi_zip_receipt_runs` が run×zip の実績に相当する。
- 主な処理: `medi_zip_import.py`、`kenshin_lib/medi/db_medi.py`
- 主なカラム: `zip_receipt_id`、`run_id`、`facility_folder_name`、`facility_code`、`facility_name`、`zip_name`、`zip_path`、`zip_sha256`、`structure_status`、`error_code`、`error_message`、`structure_message`、`data_dir_count`、`data_xml_count`、`first_seen_run_id`、`last_seen_run_id`

## XML Ledger候補
- 事実: XML 受領台帳は `medi_xml_receipts`、XML 索引・業務台帳は `medi_xml_ledger` に分かれている。
- `medi_xml_receipts`: ZIP 内 XML の受領・抽出状態。
- `medi_xml_ledger`: XML から抽出した受診者識別・保険証情報・健診日・施設情報等の横持ち索引。
- 主な処理: `medi_zip_import.py`、`kenshin_lib/medi/xml_extract.py`、`kenshin_lib/medi/db_medi.py`

## item_values候補
- 事実: `medi_xml_item_values` は XML observation からの生抽出 item values に相当する。
- 事実: `medi_exam_result_item_values` は正規化・XML 出力で使用される item values に相当する。
- 主な処理:
  - `medi_xml_item_extract.py`: `medi_xml_item_values` へ UPSERT。
  - `normalize_item_values.py`: `medi_exam_result_item_values` の `value` / `normalize_status` を更新。
  - `medi_export_xml.py`: `medi_exam_result_item_values` を参照して CDA を出力。
- 推測: 現行実装では受領XML由来の生抽出層と、出力用に整えた item values 層が分かれている。

## エラー・ログ候補
- `medi_import_runs`: run 単位の開始・終了・summary。
- `medi_zip_receipt_runs`: run×zip の NEW/SEEN 実績。
- `medi_xml_receipt_runs`: run×xml の NEW/SEEN 実績。
- `medi_xml_process_logs`: XML 処理ステップ別ログ。`WELLFORMED`、`CDA_INDEX`、`XSD_VALIDATE`、`EXTRACT_ITEMS`、`LEDGER` など。
- `medi_shared_files.note`: 共有ファイル観測・判定・コピーの短い理由。
- `medi_zip_receipts.error_code` / `error_message` / `structure_message`: ZIP 構造エラー。
- `medi_xml_receipts.error_code` / `error_message`: XML 受領・抽出エラー。
- `medi_exam_result_item_values.normalize_error`: item 値正規化エラー。
- ファイルログ: `medi_trans_06139463.py` の `OK_*.log`、`ERROR_*.log`

---

# v2設計への示唆

## 流用できるもの
- ファイル台帳:
  - `medi_shared_files` の観測、hash、auto/manual 判定、copy の分段設計。
  - `path_hash`、`sha256`、`first_seen_at`、`last_seen_at` による冪等化。
- ZIP Receipt:
  - `medi_zip_receipts` / `medi_zip_receipt_runs` の設計。
  - `zip_sha256` 一意、構造判定、DATA 複数/なしでも XML 検出優先で棚卸しする方針。
- XML Ledger:
  - `medi_xml_receipts` と `medi_xml_ledger` の分離。
  - `zip_sha256 + zip_inner_path_sha256` による ZIP 内 XML の識別。
  - `medi_xml_process_logs` の step/result/message 方式。
- subscribers.id 紐付け:
  - `medi_xml_ledger` の `name_kana_match`、`insurance_symbol_match`、`insurance_number_match`、`birth_date`、`identity_hash` は照合材料として流用候補。
  - `normalize_db_update.py` の正規化関数群は subscriber 突合前処理として流用候補。
- item_values 登録:
  - `medi_xml_item_extract.py` の observation 走査と `namecode` / `occurrence_no` 採番。
  - `normalize_item_values.py` の `ST` / `PQ` / `CD` / `CO` 正規化ルール。
- XMLチェック:
  - `xml_extract.py` の `WELLFORMED`、`CDA_INDEX`、`XSD_VALIDATE`、warning 付き ledger upsert。
  - 暗号 ZIP member 対応。
- XML Ledger への結果反映:
  - `db_update_xml_index_fields()`、`db_upsert_xml_ledger()` の更新責務。

## 捨てる、または初期 v2 から外すもの
- `medi_export_xml.py` は HIA 納品 XML 生成であり、v2 初期ゴールの受領・台帳・チェックからは後段として切り離せる。
- `medi_trans_06139463.py` は保険者固有変換であり、共通 v2 コアには入れず adapter として扱うべき。
- `import_submit_csv.py` は CSV 生取込であり、ZIP/XML 受領 v2 の初期スコープからは外せる。
- `normalize_db_update.py` の暫定テーブル名 `find_xml_subscribers_list_20260128` は v2 へ直接持ち込まない。

## 設計を変えるべきもの
- 共有フォルダとローカル work の責務:
  - 現行は `medi_shared_files_copy_to_input.py` が input へコピーする。v2 ではネットワーク共有は業務運用領域、`data/medical/work` はシステム作業領域として明示する。
- status 設計:
  - 現行は `stage_status`、`structure_status`、`status`、`items_extract_status`、`normalize_status` が分散している。v2 では File、ZIP Receipt、XML Ledger、item_values、subject_status の状態を分離して定義する。
- エラー管理:
  - `note` に短文を上書きする箇所がある。v2 ではエラーコード、詳細、発生ステップ、再実行可否をログ台帳に残す。
- item_values:
  - 現行は `medi_xml_item_values` と `medi_exam_result_item_values` の2系統がある。v2 では「XML原本から抽出した事実」と「正規化後の値」と「制度チェック結果」を混同しない。
  - 制度チェック結果や不足項目は item_values に入れず、別のチェック台帳へ保持する方針と整合させる。
- subscribers.id 紐付け:
  - 現行 `medi_xml_ledger` は照合材料を持つが、`subscribers.id` との正式な FK/紐付けは見えない。v2 では XML Ledger または subject_status に subscriber linkage を明示する。
- 健診項目不足チェック:
  - 現行に LSIO/法定健診成立判定の完成実装は見当たらない。v2 では item_values 登録後に、年度・制度ルールに基づく専用チェック処理と台帳を追加する必要がある。
- XML Ledger への結果反映:
  - 現行は XML抽出結果と process log はあるが、制度チェック結果の反映先は未整理。v2 では XML Ledger に XML parse/XSD/item extract/subscriber match/check summary をどう持つかを定義する。
