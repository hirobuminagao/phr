# Phase7 exam_check_results implementation review

## Review metadata
- Review No.: Phase7-IMPL-001
- Review Date: 2026-07-09
- Status: GO
- Reviewer: Codex

## 1. Review target
- `docs/refactor/health_exam_result/03_decisions.md`
- `docs/spec/health_examinations/02_exam_check_item_spec_v2_0_0.md`
- `sql/ddl/health_exam_result/0080_health_exam_result__exam_check_results.sql`
- `sql/ddl/health_exam_result/0050_health_exam_result__xml_ledger.sql`
- `sql/ddl/dev_phr/0060_dev_phr__exam_item_group_method_members.sql`
- `sql/seed/dev_phr/0010_dev_phr__exam_item_groups_v2_2026.sql`
- `scripts/from_medical/03_check_exam_results.py`
- `scripts/lib/examination/`

## 2. Summary
- GO / No-Go: GO
- Phase7初期の制度チェック基盤として、`--event-id` 単位の削除後再生成、`exam_check_results` 生成、`xml_ledger.check_status` / `check_reason` 集約まで実装した。

## 3. Implementation scope
- `03_check_exam_results.py` を新規作成した。
- `scripts/lib/examination/` に Lookup / Rule / Calculate / Alternative の共通処理を追加した。
- `ANY_VALID_VALUE`、`ANY_RECORD`、`ANY_OF_NAMECODES`、`CALCULATED`、`ALTERNATIVE` を実装した。
- `BMI`、`OBESITY_INDEX`、`NON_HDL_CHOLESTEROL` を計算ルールとして実装した。
- `METABOLIC_SYNDROME`、`HEALTH_GUIDANCE_LEVEL` は `status = INVALID`、`reason = NOT_IMPLEMENTED` とする。
- `--xml-ledger-id` / `--subscriber-id` は実装していない。

## 4. Decision consistency
- `03_check_exam_results.py` はオーケストレーターとし、ルール処理は共通libへ委譲している。
- Phase7 CLIは `--event-id` 必須、`--dry-run`、`--limit`、`--db-prefix`、`--health-db`、`--dev-db` に限定している。
- 制度チェック結果は `exam_check_results.status_*` / `reason_*` に保持している。
- 制度別summaryは `legal_reason_summary` / `specific_reason_summary` に保持している。
- `xml_ledger.check_status` / `check_reason` を `exam_check_results` 生成結果から集約更新する。
- `etl_errors` はスクリプト異常時のみ `CHECK_EXAM_RESULTS_FAILED` として記録する。

## 5. DDL / seed connection
- `exam_check_results` の72項目横持ちカラムへ、同一性項目コード小文字の `status_*` / `reason_*` を投入する実装である。
- `dev_phr.exam_item_group_identity_members` は制度上の同一性項目管理として参照する。
- `dev_phr.exam_item_group_method_members` は `presence_value_mode` / `rule_code` / `rule_source_*` のルール定義として参照する。
- `dev_phr.exam_item_group_members` は namecode 単位の取得候補として参照する。
- `METABOLIC_SYNDROME` / `HEALTH_GUIDANCE_LEVEL` はseedに残るが、Phase7では未実装ルールとして扱う。

## 6. CLI / ETL review
- 既存 `01_scan_files.py` / `02_import_xml.py` と同様に `argparse`、`load_mysql_base_params()`、`connect_ctx()`、`dict_cursor()`、`etl_start_run()`、`etl_finish_run()` を利用する。
- `--dry-run` の場合はDB書き込みとETL run作成を行わない。
- 通常実行ではETL runを作成し、終了時に `RunMetrics` を記録する。

## 7. Rule review
- 直接有効値がある場合は `OK` とする。
- `CALCULATED` は直接値がない場合のみ評価する。
- `ALTERNATIVE` は直接値や計算結果で確定できない場合の代替充足として評価する。
- `rule_source_*` はカンマ区切りCSVとしてtrim・空要素除外・重複除去して解釈する。
- LDLがnon-HDLを代替参照するような依存関係に対応するため、必要なsource identityは先に評価する。

## 8. Remaining items
- メタボ判定はvNext対象。
- 保健指導レベル判定はvNext対象。
- `--xml-ledger-id` / `--subscriber-id` の個別再実行CLIはvNext対象。
- 正規化 / validation 本格統合はvNext対象。
- CSV直取込由来データの制度チェックはvNext対象。

## 9. Check results
- `python -m py_compile scripts/from_medical/03_check_exam_results.py scripts/lib/examination/*.py`: OK
- `python scripts/from_medical/03_check_exam_results.py --help`: OK
- DB接続を伴う実行確認は未実施。
- `git diff --check`: OK
- 実行時に `pyenv: cannot rehash: /Users/hiro/.pyenv/shims isn't writable` の警告が表示されたが、確認コマンド自体は成功している。
