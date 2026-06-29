# ETL共通設計 調査メモ

## 調査概要

health_exam_result v2 で `etl_runs` / `etl_errors` をどう扱うべきか判断するため、spec / ADR / README / scripts/lib / scripts / DDL / migration / health_exam_result refactor docs を横断調査した。

確認済み事実として、ETL実行管理・エラー管理は `scripts/lib/etl` を共通基盤とし、`etl_runs` / `etl_errors` に記録する方針が既にある。物理配置は共通DB集約ではなく、対象データと同じスキーマに同名テーブルを置く「スキーマ並行運用」が現行ルールである。

一方で、health_exam_result v2 の一部メモにはまだ `runs` / `process_errors` という独自名が残っており、共通ETL方針との整合が必要である。

## 発見した資料・ファイル一覧

- `docs/spec/common/etl_common_lib.md`: ETL共通libの責務、対象モジュール、スキーマ分離方針を定義。`etl_runs` / `etl_errors` / `RunMetrics` の責務が明記されている。該当: `docs/spec/common/etl_common_lib.md:3`, `docs/spec/common/etl_common_lib.md:13`, `docs/spec/common/etl_common_lib.md:25`, `docs/spec/common/etl_common_lib.md:151`
- `docs/spec/common/etl_run_lifecycle.md`: ETL run のライフサイクル、粒度、ステータス判定、DBテーブル、呼び出し側責務を定義。該当: `docs/spec/common/etl_run_lifecycle.md:1`, `docs/spec/common/etl_run_lifecycle.md:32`, `docs/spec/common/etl_run_lifecycle.md:61`, `docs/spec/common/etl_run_lifecycle.md:93`, `docs/spec/common/etl_run_lifecycle.md:135`
- `docs/adr/0020-etl-common-lib-boundary.md`: ETL共通libを基盤レイヤに限定し、スキーマ並行運用を採用するADR。該当: `docs/adr/0020-etl-common-lib-boundary.md:1`, `docs/adr/0020-etl-common-lib-boundary.md:33`, `docs/adr/0020-etl-common-lib-boundary.md:76`
- `docs/adr/0003-script-actions-and-wiring-map.md`: Hub側スクリプトの as-is 事実固定。`etl_runs` INSERT、`etl_errors` INSERT、開始直後commitが記録されている。該当: `docs/adr/0003-script-actions-and-wiring-map.md:50`
- `docs/adr/0007-hia-dashboard-csv-ingestion.md`: HIA dashboard CSV は `work_other.etl_runs` / `work_other.etl_errors` を使うと決定。該当: `docs/adr/0007-hia-dashboard-csv-ingestion.md:47`
- `docs/adr/0021-hia-subscriber-import-apply-refactor.md`: HIA加入者 import/apply 再設計。apply failure は `etl_errors` に記帳し、`etl_runs` 全体の最新run_idは参照しない方針。該当: `docs/adr/0021-hia-subscriber-import-apply-refactor.md:40`, `docs/adr/0021-hia-subscriber-import-apply-refactor.md:127`
- `docs/spec/hia_export_subscribers_csv/*`: `import_run_id` / `processed_run_id` / apply `run_id` の使い分けが詳細化されている。該当: `docs/spec/hia_export_subscribers_csv/subscriber_apply.md:90`, `docs/spec/hia_export_subscribers_csv/staging_schema.md:595`, `docs/spec/hia_export_subscribers_csv/target_tables_schema.md:655`
- `docs/spec/hia_fund_dashboard_csv/*`: `work_other.etl_runs` / `work_other.etl_errors` を用いた run 記帳、`run_id` のスナップショット証跡用途を定義。該当: `docs/spec/hia_fund_dashboard_csv/README.md:33`, `docs/spec/hia_fund_dashboard_csv/snapshot_policy.md:473`
- `scripts/lib/etl/*`: 現行の共通ETL実装。該当: `scripts/lib/etl/README.md:3`, `scripts/lib/etl/runs.py:52`, `scripts/lib/etl/errors.py:54`, `scripts/lib/etl/metrics.py:43`
- `sql/ddl/dev_phr/0010_dev_phr__etl_runs.sql`, `sql/ddl/dev_phr/0020_dev_phr__etl_errors.sql`, `sql/ddl/work_other/0055_work_other__etl_runs.sql`, `sql/ddl/work_other/0056_work_other__etl_errors.sql`: 実DDL。
- `docs/refactor/health_exam_result/05_design_history.md`: health_exam_result v2 で `runs` / `process_errors` を `etl_runs` / `etl_errors` に寄せる議論と決定。該当: `docs/refactor/health_exam_result/05_design_history.md:365`
- `docs/refactor/health_exam_result/11_v2_script_design_notes.md`, `docs/refactor/health_exam_result/12_v2_ddl_design_notes.md`: まだ `runs` / `process_errors` が残る箇所あり。該当: `docs/refactor/health_exam_result/11_v2_script_design_notes.md:197`, `docs/refactor/health_exam_result/12_v2_ddl_design_notes.md:372`

## 仕様の起点

確認済み事実:

- 共通仕様として最も直接的な起点は `docs/spec/common/etl_run_lifecycle.md` と `docs/spec/common/etl_common_lib.md` である。前者は ETL Run を「1回のETL処理の実行単位」と定義し、`dev_phr` / `work_other` の並行運用を正式前提としている。該当: `docs/spec/common/etl_run_lifecycle.md:5`, `docs/spec/common/etl_run_lifecycle.md:9`, `docs/spec/common/etl_run_lifecycle.md:27`
- `docs/spec/common/etl_common_lib.md` は共通libの対象モジュールを `runs.py`, `metrics.py`, `errors.py`, `ddl.py`, `progress.py` とし、`etl_runs` 永続化と `etl_errors` 記録を責務としている。該当: `docs/spec/common/etl_common_lib.md:13`, `docs/spec/common/etl_common_lib.md:27`, `docs/spec/common/etl_common_lib.md:67`
- より古い as-is 記録としては ADR-0003 があり、Hub側の `import_subscribers_to_staging_hub.py` で `etl_runs` INSERT、`etl_errors` INSERT、start_run直後commitを事実固定している。該当: `docs/adr/0003-script-actions-and-wiring-map.md:52`

推測:

- 「仕様の起点」という意味では、実装・運用の起点は ADR-0003、共通化後の正式仕様の起点は `docs/spec/common/*` と見るのが自然である。

## ADR/README/specでの方針

確認済み事実:

- ADR-0020 は Accepted で、ETL共通libは基盤レイヤに限定する。MUST は `start_run`, `finish_run`, `etl_runs` 記録、`etl_errors` 記録、`RunMetrics` 提供、進捗ログであり、業務ロジック・正規化・identity照合は MUST NOT。該当: `docs/adr/0020-etl-common-lib-boundary.md:33`, `docs/adr/0020-etl-common-lib-boundary.md:39`, `docs/adr/0020-etl-common-lib-boundary.md:47`
- ADR-0020 は `dev_phr.etl_runs` / `dev_phr.etl_errors` と `work_other.etl_runs` / `work_other.etl_errors` の並行運用を明記し、`run_id` はスキーマ内のみ一意、対象データと同一スキーマのETLテーブルを使うとしている。該当: `docs/adr/0020-etl-common-lib-boundary.md:76`
- ADR-0007 は HIA dashboard CSV の取込先を `work_other` とし、ETL run 管理は `work_other.etl_runs` / `work_other.etl_errors` を使い、ETLテーブル構造は拡張しないとする。該当: `docs/adr/0007-hia-dashboard-csv-ingestion.md:47`
- ADR-0021 は apply failure を `etl_errors` に流す設計で、`import_run_id: auto` は未処理 staging の最新 `import_run_id` を見る。`etl_runs` 全体の最新 `run_id` は参照しない。該当: `docs/adr/0021-hia-subscriber-import-apply-refactor.md:42`, `docs/adr/0021-hia-subscriber-import-apply-refactor.md:127`
- `docs/spec/hia_export_subscribers_csv/subscriber_apply.md` も同じく、`etl_runs` 全体の最新run_idを参照しない理由を「他処理でも採番されるため」と明記している。該当: `docs/spec/hia_export_subscribers_csv/subscriber_apply.md:101`
- `docs/spec/hia_export_subscribers_csv/staging_schema.md` は `import_run_id` を取込実行ID、`processed_run_id` をapply実行IDとして意味分離している。該当: `docs/spec/hia_export_subscribers_csv/staging_schema.md:595`
- `docs/spec/hia_export_subscribers_csv/target_tables_schema.md` は本番更新の `run_id` には import run ではなく実際に本番更新を行った apply run_id を入れるとしている。該当: `docs/spec/hia_export_subscribers_csv/target_tables_schema.md:655`
- `docs/spec/hia_fund_dashboard_csv/README.md` は `work_other.etl_runs` / `work_other.etl_errors` を用いた run 記帳を第一段階スコープに含める。該当: `docs/spec/hia_fund_dashboard_csv/README.md:33`
- root `README.md` は関連ドキュメントの一覧として `error_policy.md` 等を示すが、共通ETL管理の運用ルール自体は今回確認範囲の `README.md` では見つからなかった。該当: `README.md:236`

## DDL実態

確認済み事実:

- ETL DDL は `dev_phr` と `work_other` に存在する。`find sql/ddl -name '*etl*'` の結果は以下のみ。
  - `sql/ddl/dev_phr/0010_dev_phr__etl_runs.sql`
  - `sql/ddl/dev_phr/0020_dev_phr__etl_errors.sql`
  - `sql/ddl/work_other/0055_work_other__etl_runs.sql`
  - `sql/ddl/work_other/0056_work_other__etl_errors.sql`
- `dev_phr.etl_runs` と `work_other.etl_runs` は、スキーマ名以外は同一構造だった。主なカラムは `run_id`, `phase`, `source`, `db_schema`, `status`, `started_at`, `finished_at`, `db_path`, `input_base`, `input_file`, `insurer_number`, `dry_run`, `limit_rows`, metrics系, `notes`, `admin_note`。該当: `sql/ddl/dev_phr/0010_dev_phr__etl_runs.sql:1`, `sql/ddl/work_other/0055_work_other__etl_runs.sql:1`
- `dev_phr.etl_errors` と `work_other.etl_errors` は、スキーマ名・FK参照先以外は同一構造だった。主なカラムは `error_id`, `run_id`, `phase`, `source`, `insurer_number`, `src_file`, `src_row_no`, `src_line_no`, `staging_rowid`, `person_id_custom`, `field`, `field_value`, `error_code`, `message`, `created_at`。該当: `sql/ddl/dev_phr/0020_dev_phr__etl_errors.sql:1`, `sql/ddl/work_other/0056_work_other__etl_errors.sql:1`
- 静的DDLでは `etl_errors.run_id` は `NOT NULL` で、同一スキーマの `etl_runs.run_id` へFKを持つ。該当: `sql/ddl/dev_phr/0020_dev_phr__etl_errors.sql:3`, `sql/ddl/dev_phr/0020_dev_phr__etl_errors.sql:23`, `sql/ddl/work_other/0056_work_other__etl_errors.sql:3`, `sql/ddl/work_other/0056_work_other__etl_errors.sql:23`
- `sql/migrations/` 配下には `etl_runs` / `etl_errors` 自体の作成・変更 migration は見つからなかった。
- 旧健診系には独自の `work_other.medi_import_runs` と `medi_xml_process_logs` がある。`medi_import_runs` は `run_id`, `started_at`, `finished_at`, `input_root`, `note` のシンプルな実行ログ。`medi_xml_process_logs` は `run_id` で `medi_import_runs` にFKを持つXML処理ログ。該当: `sql/ddl/work_other/0005_work_other__medi_import_runs.sql:1`, `sql/ddl/work_other/0017_work_other_medi_xml_process_logs.sql:1`

設計ズレ:

- `scripts/lib/etl/ddl.py` の自動作成DDLは、静的DDLと差分がある。特に `etl_errors.run_id` が `NULL` 許容で、FK定義がなく、indexも `idx_etl_errors_run_created` / `idx_etl_errors_src` である。該当: `scripts/lib/etl/ddl.py:91`
- 静的DDLは `utf8mb4_ja_0900_as_cs`、`scripts/lib/etl/ddl.py` は `utf8mb4_0900_ai_ci` で照合順序も異なる。該当: `sql/ddl/dev_phr/0010_dev_phr__etl_runs.sql:29`, `scripts/lib/etl/ddl.py:82`

## scripts/lib 実装実態

確認済み事実:

- `scripts/lib/etl/README.md` は `scripts/lib/etl/` をPHRのETL共通基盤とし、`etl_runs` 記録、`etl_errors` 行エラー記録、metrics、進捗、DDL存在保証を扱うと明記している。該当: `scripts/lib/etl/README.md:3`
- `scripts/lib/etl/runs.py` は `start_run()` で `etl_runs` に `status='running'` をINSERTし、`finish_run()` で status / metrics / notes を更新する。commit / rollback は呼び出し側責務。該当: `scripts/lib/etl/runs.py:52`, `scripts/lib/etl/runs.py:108`
- status判定は `_decide_status()` に集約され、`errors > 0 && changed > 0` は `partial`、`errors == 0 && changed > 0` は `success`、それ以外は `failed`。該当: `scripts/lib/etl/runs.py:93`
- `scripts/lib/etl/errors.py` は `log_error()` で `etl_errors` に1行INSERTし、その後 `etl_runs.errors` を +1 する。該当: `scripts/lib/etl/errors.py:45`, `scripts/lib/etl/errors.py:54`
- `scripts/lib/etl/metrics.py` は `RunMetrics` を純粋なデータコンテナとして定義する。該当: `scripts/lib/etl/metrics.py:43`
- `scripts/lib/etl/__init__.py` は `RunMetrics`, `ProgressLogger`, `ensure_tables`, `start_run`, `finish_run`, `log_error` を公開しているが、`log_normalize_error` は公開していない。該当: `scripts/lib/etl/__init__.py:15`
- `scripts/lib/etl/README.md` は、`scripts/work_folder/lib/etl/` からコピーして共通化した版で、今後は `scripts.lib.etl.*` を正とするとしている。該当: `scripts/lib/etl/README.md:124`

## 既存スクリプトでの利用実態

確認済み事実:

- 新しめの `scripts/from_fund/import_staging_subscribers_fund.py` は `scripts.lib.etl` の `RunMetrics`, `start_run`, `finish_run`, `log_error` を使う。CSVファイルごとに `start_run()` し、その `run_id` を `import_run_id` として staging に渡す。該当: `scripts/from_fund/import_staging_subscribers_fund.py:79`, `scripts/from_fund/import_staging_subscribers_fund.py:862`
- 同スクリプトは import 成功後、同じ import `run_id` を使って company mapping enrichment と apply を起動している。該当: `scripts/from_fund/import_staging_subscribers_fund.py:941`
- `scripts/from_fund/apply_staging_subscribers_fund_to_subscribers.py` は import側 `--run-id` を受け取り、別途 apply用 `apply_run_id` を `start_run()` で発行し、`change_run_id=apply_run_id` として監査に渡す。該当: `scripts/from_fund/apply_staging_subscribers_fund_to_subscribers.py:59`, `scripts/from_fund/apply_staging_subscribers_fund_to_subscribers.py:72`, `scripts/from_fund/apply_staging_subscribers_fund_to_subscribers.py:102`
- `scripts/hia/import_subscribers_to_staging_hub.py` は `scripts.lib.etl` を使い、import run を開始直後にcommitし、その後 current snapshot 用に別の `current_snapshot_run_id` を発行している。該当: `scripts/hia/import_subscribers_to_staging_hub.py:147`, `scripts/hia/import_subscribers_to_staging_hub.py:266`, `scripts/hia/import_subscribers_to_staging_hub.py:345`
- `scripts/hia/script_lib/hub_subscriber_import.py` は行単位エラーで `log_error()` を呼び、`etl_errors` に記録する。該当: `scripts/hia/script_lib/hub_subscriber_import.py:40`, `scripts/hia/script_lib/hub_subscriber_import.py:270`, `scripts/hia/script_lib/hub_subscriber_import.py:551`
- `scripts/hia/script_lib/hub_subscriber_current_snapshot.py` は snapshot中のエラーを `log_error()` で記録するが、`run_id=import_run_id` として渡している箇所がある。該当: `scripts/hia/script_lib/hub_subscriber_current_snapshot.py:32`, `scripts/hia/script_lib/hub_subscriber_current_snapshot.py:350`
- `scripts/hia/apply_hia_subscriber_sync.py` は `import_run_id: auto` のとき未処理 staging の最新 `import_run_id` を解決し、別途 `apply_run_id` を発行する。該当: `scripts/hia/apply_hia_subscriber_sync.py:87`, `scripts/hia/apply_hia_subscriber_sync.py:200`
- 古い `scripts/work_folder/scripts/hia_import_dashboard_csv.py` は `scripts.work_folder.lib.etl` を使い、CSVごとに `work_other` の run を発行している。該当: `scripts/work_folder/scripts/hia_import_dashboard_csv.py:54`, `scripts/work_folder/scripts/hia_import_dashboard_csv.py:927`
- 旧健診系 `scripts/kenshin_list_pydir/scripts/medi_zip_import.py` は共通 `etl_runs` ではなく `medi_import_runs` に `db_insert_run()` / `db_finish_run()` で記録する。該当: `scripts/kenshin_list_pydir/scripts/medi_zip_import.py:690`
- 旧健診系 `scripts/kenshin_list_pydir/scripts/medi_xml_item_extract.py` も `medi_import_runs` の `run_id` を使う。環境変数指定run_idがあれば存在確認し、なければ `db_insert_run()` する。該当: `scripts/kenshin_list_pydir/scripts/medi_xml_item_extract.py:570`
- `scripts/kenshin_list_pydir/scripts/normalize_item_values.py` は明示的に `etl_runs` / `etl_errors` / `medi_import_runs` 等への記帳を行わないと固定している。該当: `scripts/kenshin_list_pydir/scripts/normalize_item_values.py:74`

## 命名ゆれ・設計ズレ・弊害

確認済み事実:

- `etl_run` と `etl_runs` の表記ゆれがある。`docs/spec/common/etl_run_lifecycle.md` はタイトル・本文で `etl_run` と書くが、物理テーブルは `etl_runs`。該当: `docs/spec/common/etl_run_lifecycle.md:5`, `docs/spec/common/etl_run_lifecycle.md:142`
- `run_id`, `import_run_id`, `processed_run_id`, `apply_run_id`, `last_change_run_id`, `first_seen_run_id`, `last_seen_run_id` が用途別に存在する。HIA spec では意味分離されているが、health_exam_result v2 ではまだ整理が必要。該当: `docs/spec/hia_export_subscribers_csv/staging_schema.md:595`, `docs/spec/hia_export_subscribers_csv/target_tables_schema.md:655`
- `etl_runs` は複数処理で採番されるため、「最新run_id」を業務対象選択に使ってはいけないことが明記されている。該当: `docs/spec/hia_export_subscribers_csv/subscriber_apply.md:113`
- `scripts/lib/etl/ddl.py` と `sql/ddl/*__etl_*.sql` のDDL差分は実害候補である。空DBで `ensure_tables()` が先に走ると、静的DDLと異なる `etl_errors` が作られる可能性がある。
- `scripts/hia/script_lib/hub_subscriber_current_snapshot.py` では、entrypoint側で `current_snapshot_run_id` を発行している一方、lib内エラー記録は `run_id=import_run_id` を渡している箇所がある。snapshot用runとエラー紐付けの意図確認が必要。該当: `scripts/hia/import_subscribers_to_staging_hub.py:345`, `scripts/hia/script_lib/hub_subscriber_current_snapshot.py:350`
- health_exam_result refactor docs では、`05_design_history.md` で `etl_runs` / `etl_errors` に寄せる決定がある一方、`11_v2_script_design_notes.md` と `12_v2_ddl_design_notes.md` に `runs` / `process_errors` が残っている。該当: `docs/refactor/health_exam_result/05_design_history.md:385`, `docs/refactor/health_exam_result/11_v2_script_design_notes.md:197`, `docs/refactor/health_exam_result/12_v2_ddl_design_notes.md:372`

推測:

- 共通ETL管理の最大の弊害は `run_id` の意味がテーブル・スキーマ・phaseで閉じているにもかかわらず、単独の数値として扱うと誤参照しやすい点である。HIA側は `import_run_id` / `apply_run_id` を分けることで回避している。
- `etl_errors` は行単位エラー中心の設計で、health_exam_result のファイル・XML・item単位エラーを十分表現するには、既存カラムへの写像ルールか、共通DDLの拡張判断が必要になる。

## health_exam_result v2 への示唆

確認済み事実に基づく示唆:

- health_exam_result v2 では、独自の `runs` / `process_errors` という名前は避け、`health_exam_result.etl_runs` / `health_exam_result.etl_errors` に寄せるのが既存方針と整合する。根拠は `05_design_history.md` の決定事項と、ADR-0020 のスキーマ並行運用方針。該当: `docs/refactor/health_exam_result/05_design_history.md:385`, `docs/adr/0020-etl-common-lib-boundary.md:76`
- 物理配置は共通DB集約ではなく、`health_exam_result` DB内に同名・同構造の `etl_runs` / `etl_errors` を置く方針が現行ルールと合う。該当: `docs/spec/common/etl_common_lib.md:153`, `docs/spec/common/etl_run_lifecycle.md:9`
- v2の各エントリースクリプトは `scripts.lib.etl` の `start_run()` / `finish_run()` / `RunMetrics` / `log_error()` を使うのが自然である。`scripts/work_folder.lib.etl` ではなく `scripts.lib.etl` を正とする。該当: `scripts/lib/etl/README.md:124`
- `run_id` の粒度は、標準は「1 run = 1スクリプト実行」、必要ならファイル単位runも許容されている。health_exam_result v2 では、`file_receipts` との関係が重要なため、最初に「スクリプト実行単位run」と「ファイル単位状態」を分け、ファイル単位の成功/失敗は `file_receipts` / `xml_ledger` 側に持たせるのが過剰分割を避けやすい。該当: `docs/spec/common/etl_run_lifecycle.md:32`
- `etl_runs` の `phase` は現行DDLで `import` / `apply` の2値のみである。health_exam_result v2 の `match`, `extract`, `check`, `export` をそのまま phase に入れることはできないため、初期実装では `phase='import'` に寄せて `source='03_match_subscribers.py'` などで識別するか、共通DDL拡張を検討する必要がある。該当: `sql/ddl/dev_phr/0010_dev_phr__etl_runs.sql:3`
- `etl_errors` は既存DDLに `file_receipt_id`, `xml_ledger_id`, `item_value_id`, `error_type`, `status`, `resolved_at` を持たない。v2でXML・項目単位の調査性を重視するなら、既存カラムへ `src_file`, `staging_rowid`, `field`, `field_value`, `message` で写像するだけで足りるか、共通DDLの拡張が必要かを先に決めるべきである。該当: `sql/ddl/dev_phr/0020_dev_phr__etl_errors.sql:1`, `docs/refactor/health_exam_result/09_v2_table_design_notes.md:305`
- `scripts/lib/etl/ddl.py` と静的DDLの差分は、health_exam_result用DDLを作る前に整理した方がよい。少なくとも v2 DDL設計では静的DDLを正とするのか、`ensure_tables()` を正とするのかを明記する必要がある。

推測:

- health_exam_result v2 の初期実装では、`etl_runs` は実行サマリー、`file_receipts` / `xml_ledger` は業務状態、`etl_errors` は再調査用の構造化エラー、という三層に分けるのが既存設計と衝突しにくい。
- `process_error_writer` は名前を `etl_error_writer` または `etl_error_service` に寄せ、内部で `scripts.lib.etl.errors.log_error()` を使う薄い業務アダプタにするのがよい。

## 未確認事項

- `etl_runs` / `etl_errors` の正式DDLを、静的DDLと `scripts/lib/etl/ddl.py` のどちらに寄せるか。
- `etl_errors.run_id` を `NOT NULL + FK` とするか、`start_run` 前の致命エラー用に `NULL` 許容を正式化するか。
- `phase` enum を `import` / `apply` のまま維持するか、health_exam_result v2 の `match` / `extract` / `check` / `export` を表せるよう拡張するか。
- health_exam_result v2 の `etl_errors` に `file_receipt_id`, `xml_ledger_id`, `item_value_id`, `resolved_*` 系を追加するか、既存共通DDLに合わせて `src_file` / `staging_rowid` / `field` / `message` へ写像するか。
- `file_receipts.id` と `etl_runs.run_id` の関係。1 run で複数ファイルを処理するのか、ファイルごとにrunを切るのか。
- `scripts/hia/script_lib/hub_subscriber_current_snapshot.py` のエラー記録が `current_snapshot_run_id` ではなく `import_run_id` に見える点が意図通りか。
- `sql/migrations/` に ETL DDL の migration が無い運用でよいか。DDLファイルだけで各DBへ適用する運用かどうか。
