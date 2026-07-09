# Phase6 exam_check_results DDL review

## Review metadata
- Review No.: Phase6-DDL-001
- Review Date: 2026-07-09
- Status: GO
- Reviewer: Codex

## 1. Review target
- `docs/refactor/health_exam_result/03_decisions.md`
- `docs/spec/health_examinations/02_exam_check_item_spec_v2_0_0.md`
- `sql/ddl/health_exam_result/0050_health_exam_result__xml_ledger.sql`
- `sql/ddl/health_exam_result/0080_health_exam_result__exam_check_results.sql`
- `sql/migrations/health_exam_result/20260709_001_health_exam_result_add_check_reason_to_xml_ledger.sql`
- `sql/migrations/health_exam_result/20260709_002_health_exam_result_create_exam_check_results.sql`

## 2. Summary
- GO / No-Go: GO
- Phase6 DDL/migration は `03_decisions.md` の決定事項と整合しており、`exam_check_results` 作成および `xml_ledger.check_reason` 追加へ進める。

## 3. Findings
### High
- なし。

### Medium
- なし。

### Low
- 本レビュー作業では scripts / seed / `03_decisions.md` / `05_design_history.md` は変更していない。
- 作業時点の git status 上では `03_decisions.md` / `05_design_history.md` に既存差分が残っているため、今回レビュー作業による変更とは切り分けて扱う。

## 4. Decision consistency
- `exam_check_results` は `id` 単独PKで作成されている。
- 業務キーuniqueは定義されていない。
- FK制約は定義されていない。
- 参照キーとして `xml_ledger_id` / `event_id` / `subscriber_id` / `hia_subscriber_id` を保持している。
- 法定健診の総合判定カラムとして `legal_check_result` を保持している。
- 特定健診の総合判定カラムとして `specific_check_result` を保持している。
- 法定健診のreason summaryカラムとして `legal_reason_summary` を保持している。
- 特定健診のreason summaryカラムとして `specific_reason_summary` を保持している。
- 72項目分の `status_<同一性項目コード小文字>` / `reason_<同一性項目コード小文字>` を保持している。
- `xml_ledger` にはJOIN削減用の `check_reason` が追加されている。

## 5. DDL / migration consistency
- `0080_health_exam_result__exam_check_results.sql` と `20260709_002_health_exam_result_create_exam_check_results.sql` の `CREATE TABLE` 内容は一致している。
- `exam_check_results` のDDL/migrationには、業務キーunique、FK制約、外部参照制約は含まれていない。
- `0050_health_exam_result__xml_ledger.sql` と `20260709_001_health_exam_result_add_check_reason_to_xml_ledger.sql` は、`check_status` の直後に `check_reason text` を追加する内容で一致している。

## 6. 72 item column check
- `docs/spec/health_examinations/02_exam_check_item_spec_v2_0_0.md` の同一性項目コードは72件。
- DDL上の `status_<同一性項目コード小文字>` は72件。
- DDL上の `reason_<同一性項目コード小文字>` は72件。
- specに存在してDDLにない項目はなし。
- DDLに存在してspecにない項目はなし。
- status/reason合計は144カラム。

## 7. xml_ledger check_reason
- DDL: `check_status` の直後に `check_reason text` を追加している。
- migration: `ALTER TABLE health_exam_result.xml_ledger ADD COLUMN check_reason text AFTER check_status` で追加している。
- 型はDDL/migrationともに `text`。
- 追加位置はDDL/migrationともに `check_status` の直後。

## 8. Index / constraint review
- PK: `PRIMARY KEY (id)` のみ。
- UNIQUE: `exam_check_results` にunique制約はない。
- FK: `exam_check_results` にFK制約はない。
- INDEX:
  - `idx_exam_check_results_xml_ledger` (`xml_ledger_id`)
  - `idx_exam_check_results_event` (`event_id`)
  - `idx_exam_check_results_subscriber` (`subscriber_id`)
  - `idx_exam_check_results_hia_subscriber` (`hia_subscriber_id`)
  - `idx_exam_check_results_legal_result` (`legal_check_result`)
  - `idx_exam_check_results_specific_result` (`specific_check_result`)
  - `idx_exam_check_results_created_at` (`created_at`)
- 参照キーはINDEXのみ作成するという03決定事項と矛盾しない。

## 9. Non-target confirmation
- 本レビュー作業では scripts を変更していない。
- 本レビュー作業では seed を変更していない。
- 本レビュー作業では `03_decisions.md` を変更していない。
- 本レビュー作業では `05_design_history.md` を変更していない。
- 新規作成したファイルは本レビュー資料 `docs/refactor/health_exam_result/28_phase6_exam_check_results_ddl_review.md` のみ。

## 10. Check results
- `exam_check_results` DDL/migration一致確認: OK
- `xml_ledger.check_reason` DDL/migration一致確認: OK
- 72項目spec突合: OK
- `git diff --check`: OK
- 実行時に `pyenv: cannot rehash: /Users/hiro/.pyenv/shims isn't writable` の警告が表示されたが、確認コマンド自体は成功している。
