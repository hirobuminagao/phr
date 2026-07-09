# Phase7 implementation review

## Review metadata
- Review No.: 30
- Review Date: 2026-07-09
- Status: No-Go
- Reviewer: Codex

## 1. Review target
- `scripts/from_medical/03_check_exam_results.py`
- `scripts/lib/examination/`
- `docs/refactor/health_exam_result/03_decisions.md`
- `docs/spec/health_examinations/`
- `sql/ddl/`
- `sql/migrations/`
- `sql/seed/`

## 2. Summary
- 判定: No-Go
- Phase7の骨格、CLI、削除後再生成、`exam_check_results` / `xml_ledger` 更新、presence/rule/calculate/alternativeの分離は実装されている。
- ただし、制度マスタseedとの接続で `method_code` / `namecode` から `identity_item_code` を5桁prefix推定しており、72項目横持ち結果へ誤接続するリスクが高い。
- ETL run異常終了時に `etl_runs.status='running'` が残る可能性がある。
- `METABOLIC_SYNDROME` / `HEALTH_GUIDANCE_LEVEL` は、直接値が存在すると `OK` になり得るため、03の `INVALID / NOT_IMPLEMENTED` 方針と差分がある。

## 3. GO / Conditional GO / No-Go
- No-Go
- High findings を解消するまで、本番データに対する実行は避ける。
- `--dry-run` と限定データでの検証は可能だが、現状結果は正式判定として扱わない。

## 4. High findings

### H-1. `method_code` / `namecode` からのidentity推定がseedと一致しない
- 対象:
  - `scripts/lib/examination/models.py`
  - `scripts/lib/examination/lookup.py`
- 内容:
  - `MethodRule.from_row()` が `xml_method_code[:5]` を `identity_code` として扱っている。
  - `fetch_group_namecodes()` も `namecode[:5]` をidentityとして扱っている。
  - しかしPhase5 seedでは、例として `9A750` 収縮期血圧の method/namecode が `9A755...`、`9A752...`、`9A751...` であり、prefix 5桁は `9A750` にならない。
- 影響:
  - `9A750` / `9A760` など、method/namecodeのprefixと同一性項目コードが一致しない項目で、method ruleやnamecode候補が72項目のidentityへ紐づかない。
  - `exam_item_values.identity_item_code` が未設定、または補完に依存するケースで `MISSING` / `OK` / `CALCULATED` / `ALTERNATIVE` が誤判定される。
  - 03の「72項目specを正とし、identity_item_codeと横持ちカラムを対応させる」方針と接続が不安定。
- 修正方針:
  - prefix推定をやめ、`exam_item_master` 由来の `identity_item_code`、またはseed生成時と同じ method/namecode -> identity の明示マッピングを使う。
  - 少なくとも `exam_item_group_method_members` / `exam_item_group_members` を読む際に、group内identity_membersやmasterとJOINしてidentityを確定する。

## 5. Medium findings

### M-1. 異常終了時にETL runが完了状態へ更新されない
- 対象:
  - `scripts/from_medical/03_check_exam_results.py`
  - `scripts/lib/etl/runs.py`
- 内容:
  - `start_check_run()` 後に例外が発生した場合、`record_script_error()` は呼ばれるが、`finish_check_run()` は呼ばれず、例外が再送出される。
  - `start_run()` は `status='running'` でINSERTするため、失敗時に `etl_runs` が running のまま残る可能性がある。
- 影響:
  - 03の「ETL run管理パターンに寄せる」「etl_errorsはスクリプト異常のみ記録」とは方向性が合うが、運用上は失敗runを完了状態で追跡できない。
- 修正方針:
  - 例外時も `summary.errors` を反映して `finish_run(..., status_override='failed')` 相当を呼ぶ。

### M-2. v2初版対象外の判定項目が直接値で `OK` になり得る
- 対象:
  - `scripts/lib/examination/rules.py`
- 内容:
  - `evaluate_identity()` は direct value を先に見て、値があれば `STATUS_OK` を返す。
  - そのため `9N501` メタボリックシンドローム判定、`9N506` 保健指導レベルにXML上の直接値が存在すると、`CALCULATED` ruleの `NOT_IMPLEMENTED` 判定に到達しない。
- 影響:
  - 03では `METABOLIC_SYNDROME` / `HEALTH_GUIDANCE_LEVEL` はPhase7対象外、未実装ルールは `status=INVALID` / `reason=NOT_IMPLEMENTED` と決定済み。
- 修正方針:
  - 対象identityまたは対象rule_codeが未実装の場合、direct valueよりも `INVALID / NOT_IMPLEMENTED` を優先する。

## 6. Low findings

### L-1. 大量eventでの一括IN句が大きくなる
- 対象:
  - `scripts/lib/examination/lookup.py`
- 内容:
  - 対象ledgerを一括取得し、`exam_item_values` も `ledger_id IN (...)` で一括取得している。
- 影響:
  - 大規模eventではSQLサイズやメモリ使用量が増える。
- 対応:
  - Phase7初期は `--limit` で抑制可能。将来、ledger単位またはchunk単位処理へ分割する。

### L-2. 未使用定数・importが残っている
- 対象:
  - `scripts/from_medical/03_check_exam_results.py`
- 内容:
  - `CHECK_STATUS_PENDING`、`STATUS_MISSING` など未使用要素がある。
- 影響:
  - 動作影響は小さいが、保守性のため整理候補。

## 7. 良い点
- Phase7初期CLIとして `--event-id` 必須、`--dry-run`、`--limit`、`--db-prefix`、`--health-db`、`--dev-db` が実装されている。
- `--xml-ledger-id` / `--subscriber-id` を実装しておらず、Phase7スコープに収まっている。
- `exam_check_results` は対象ledgerの既存結果を削除してから再生成しており、03の削除後再生成方式と一致している。
- `xml_ledger.check_status` / `xml_ledger.check_reason` の集約更新が実装されている。
- Lookup / Rule / Calculate / Alternative が分離され、今後のルール追加に備えた構成になっている。
- `NON_HDL_CHOLESTEROL`、`BMI`、`OBESITY_INDEX` の計算関数が実装されている。
- `3F077`、`3D010`、`3D046` の代替判定は `ALTERNATIVE_*` として分離されている。
- `dry-run` 時は削除・INSERT・ledger更新・ETL run作成を行わない。
- `condition_code` / `condition_expr` / `rule_params JSON` を使っていない。

## 8. 実装漏れ
- H-1により、seedの method/namecode と 72項目identityの明示接続が不足している。
- M-1により、異常終了時のETL run完了更新が不足している。
- M-2により、未実装判定項目の direct value 優先抑止が不足している。
- DB接続ありの実データ実行確認は未実施。

## 9. 03との差分
- `METABOLIC_SYNDROME` / `HEALTH_GUIDANCE_LEVEL` はPhase7対象外で `INVALID / NOT_IMPLEMENTED` とする決定だが、直接値がある場合は `OK` になり得る。
- `identity_item_code` を横持ちカラムへ対応させる前提に対し、method/namecode側のidentity決定がprefix推定になっている。
- ETL run管理は共通libを使っているが、失敗時のrun完了状態反映が不足している。

## 10. DDLとの差分
- `exam_check_results` へのINSERT対象カラム名はDDLの `status_<identity小文字>` / `reason_<identity小文字>` 方針に沿っている。
- `xml_ledger.check_status` / `xml_ledger.check_reason` の更新対象はDDLと一致している。
- FK制約や業務キーuniqueを前提にした実装はない。
- DDL自体との明確なカラム差分は見つからない。

## 11. seedとの差分
- seedは `exam_item_group_method_members` を method_code 単位で保持し、コメント上はidentityを示しているが、実装はコメントを使わず `xml_method_code[:5]` でidentityを推定している。
- seed例:
  - `9A750` 収縮期血圧に対して `9A75500009` / `9A75200000` / `9A75100000`
  - `9A760` 拡張期血圧に対して `9A76500009` / `9A76200000` / `9A76100000`
- このため、seedの責務分担と実装側のidentity解決が一致していない。
- `3F069` は `CALCULATED` のみ、LDL側で `ALTERNATIVE_3F077` の参照元として扱うseed方針に沿って実装されている。

## 12. vNext送り事項
- メタボリックシンドローム判定の本実装。
- 保健指導レベル判定の本実装。
- 年齢・性別・喫煙など複合条件を含む制度判定。
- 正規化/validation結果との本格統合。
- CSV直取込由来データの制度チェック。
- 大規模event向けのchunk処理と進捗管理。
- 計算結果の数値保持や後続計算への再利用。

## 13. 実行前に直した方が良い点
- `method_code` / `namecode` からidentityをprefix推定する処理を廃止し、明示的なidentityマッピングへ変更する。
- 異常終了時も `etl_runs` を `failed` または `partial` でfinishする。
- `METABOLIC_SYNDROME` / `HEALTH_GUIDANCE_LEVEL` は直接値があっても `INVALID / NOT_IMPLEMENTED` とする。
- 上記修正後、少なくとも次のケースでDB実行テストを行う。
  - 収縮期/拡張期血圧のmethod違い。
  - BMI / 肥満度 / non-HDL計算。
  - LDL missing + non-HDL calculated による `ALTERNATIVE_3F077`。
  - 血糖/HbA1c相互代替。
  - `9N501` / `9N506` の `INVALID / NOT_IMPLEMENTED`。

## 14. 実行後に確認すべき点
- `exam_check_results` が対象ledgerごとに1行生成されること。
- 72項目の `status_` / `reason_` カラムに想定どおり値が入ること。
- `legal_check_result` / `specific_check_result` と reason summary が03の集約ルールどおりになること。
- `xml_ledger.check_status` / `check_reason` が `exam_check_results` と整合すること。
- 再実行時に旧結果が削除され、重複行が残らないこと。
- script異常時に `etl_errors` と `etl_runs` が追跡可能な状態になること。

## 15. Check results
- `git diff --check`: OK

---

# Phase7 implementation review - Review No.2

## Review metadata
- Review No.: 30-2
- Review Date: 2026-07-09
- Status: GO
- Reviewer: Codex

## 1. Review target
- `scripts/from_medical/03_check_exam_results.py`
- `scripts/lib/examination/`
- `docs/refactor/health_exam_result/30_phase7_implementation_review.md`

## 2. Summary
- 判定: GO
- Review No.1 の No-Go 指摘3件は修正済み。
- `xml_method_code[:5]` / `namecode[:5]` によるidentity推定は廃止し、`dev_phr.exam_item_master.identity_item_code` を正として参照する形に変更した。
- 例外発生時も `etl_runs` を `failed` で終了し、`etl_errors` にはスクリプト異常のみ記録する。
- `METABOLIC_SYNDROME` / `HEALTH_GUIDANCE_LEVEL` は直接値があっても `INVALID / NOT_IMPLEMENTED:<identity_code>` とする。

## 3. Findings

### High
- なし。

### Medium
- なし。

### Low
- DB接続ありの実データ実行確認は未実施。初回実行時に限定eventでの照合を行う。

## 4. Fixed items

### H-1. identity推定の修正
- `ExamValue.from_row()` から `namecode[:5]` fallbackを削除した。
- `MethodRule.from_row()` から `xml_method_code[:5]` fallbackを削除した。
- `fetch_exam_values()` は `dev_phr.exam_item_master` をJOINし、`COALESCE(exam_item_values.identity_item_code, exam_item_master.identity_item_code)` を使用する。
- `fetch_method_rules()` は `exam_item_group_method_members.xml_method_code` と `exam_item_master.xml_method_code` をJOINし、`exam_item_master.identity_item_code` で rule をidentityへ接続する。
- `fetch_group_namecodes()` は `exam_item_group_members.namecode` と `exam_item_master.namecode` をJOINし、`exam_item_master.identity_item_code` で namecode候補をidentityへ接続する。
- これにより `9A750` / `9A760` のように method/namecode prefix と同一性項目コードが一致しない項目でも、master上のidentityへ接続される。

### M-1. 例外時ETL run終了
- `process_ledgers()` 例外時に `record_script_error()` で `etl_errors` へスクリプト異常を記録する。
- その後 `etl_finish_run(..., status_override="failed")` を呼び、`etl_runs.status='running'` が残らないようにした。

### M-2. vNext対象ruleの扱い
- `9N501` / `9N506` は direct value より前に判定し、常に `INVALID / NOT_IMPLEMENTED:<identity_code>` を返す。
- `METABOLIC_SYNDROME` / `HEALTH_GUIDANCE_LEVEL` の計算rule経由でも同じreason形式を返す。
- Phase7対象外ruleの本実装は行っていない。

## 5. 03 / DDL / seed consistency
- 03のPhase7スコープ、削除後再生成、status/reason方針、vNext対象rule方針と整合する。
- DDL / migration / seed は変更していない。
- `condition_code` / `condition_expr` / `rule_params JSON` は使用していない。
- Phase5 seedの `identity_item_code` / `xml_method_code` / `namecode` の責務に合わせ、identityは `exam_item_master.identity_item_code` を正として扱う。

## 6. Check results
- `python -m py_compile scripts/from_medical/03_check_exam_results.py scripts/lib/examination/*.py`: OK
- `python scripts/from_medical/03_check_exam_results.py --help`: OK
- `git diff --check`: OK
