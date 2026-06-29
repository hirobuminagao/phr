

# 0023: ETL共通実行管理・エラー管理ポリシー

## Status

Accepted

## Date

2026-06-26

## Context

PHR関連の複数システムでは、取込・変換・照合・チェック・apply などのETL処理が複数存在する。

これまでの実装では、ETL実行単位やエラー記録について、`etl_runs`、`etl_errors`、`run_metrics`、`import_run_id` などの概念が利用されてきた。一方で、システムやDBごとに独自の `runs`、`process_errors`、`job_logs` 等を作り始めると、同じ責務のテーブルが乱立し、運用・調査・保守の負荷が高くなる。

`health_exam_result v2` の設計でも、当初は `runs` や `process_errors` という名称で処理実行管理・エラー管理テーブルを検討していた。しかし、これらは業務固有の台帳ではなく、ETL共通基盤として扱うべき責務である。

ETL実行管理・エラー管理は、特定業務システムのローカル設計ではなく、PHR全体で統一された基盤として扱う必要がある。

## Decision

ETL実行管理・エラー管理は、全システム共通仕様として扱う。

標準テーブル名は以下とする。

- `etl_runs`
- `etl_errors`

各業務DBは、必要に応じて同じテーブル名・同じテーブル構造・同じ運用方法で `etl_runs` / `etl_errors` を持つ。

現時点では、ETL管理専用の共通DBは作成しない。物理配置は各業務DB内とし、仕様だけを共通化する。

例:

```text
health_exam_result.etl_runs
health_exam_result.etl_errors

dev_phr.etl_runs
dev_phr.etl_errors

other_db.etl_runs
other_db.etl_errors
```

各システムで以下のような独自名称のETL管理テーブルを新規作成しない。

```text
runs
process_errors
job_runs
job_errors
import_logs
batch_logs
```

ETL実行管理・エラー管理が必要な場合は、原則として `etl_runs` / `etl_errors` を利用する。

## Scope

このADRの対象は以下である。

- ETL実行単位の記録
- ETL処理中に発生したエラーの記録
- ETL共通ライブラリからの記帳
- 各業務DBに配置するETL管理テーブルの命名・構造・運用方針

このADRの対象外は以下である。

- 業務固有の台帳
- 業務上のステータス管理
- ファイル台帳
- XML台帳
- 健診値台帳
- チェック結果台帳
- 監査ログ全般

## Standard tables

### etl_runs

`etl_runs` は、ETL処理の1実行単位を記録する。

主な用途は以下である。

- 処理開始・終了の記録
- 処理種別の記録
- 実行ステータスの記録
- 処理件数やサマリーの記録
- エラー記録との紐付け

### etl_errors

`etl_errors` は、ETL処理中に発生したエラーを記録する。

主な用途は以下である。

- `etl_run_id` との紐付け
- エラー種別の記録
- エラーコードの記録
- エラーメッセージの記録
- 対象テーブル・対象キー・対象ファイル等の記録
- 調査・再実行・運用判断の材料

## Placement policy

ETL管理テーブルは共通DBに集約しない。

各業務DBに同じ名称・同じ構造で配置する。

理由は以下である。

- 業務DB単位でバックアップ・リストア・調査を完結しやすい。
- 業務データとETL実行証跡の対応を同じDB内で追いやすい。
- 共通DB障害による全システム影響を避けられる。
- 一方で、名称・構造・運用を統一することで、実装と調査の共通化を維持できる。

## Library policy

ETL実行管理・エラー管理の記帳は、共通ライブラリに寄せる。

原則として、各スクリプトが独自SQLで `etl_runs` / `etl_errors` を自由に操作しない。

共通実装は以下を基準とする。

```text
scripts/lib/etl/
```

既存の共通ETLライブラリが提供する関数・クラスを利用し、各業務スクリプトは以下の責務に集中する。

- 業務データの読込
- 業務ロジックの実行
- 業務テーブルの更新
- ETL共通ライブラリへの処理開始・終了・エラー記録依頼

## Operation policy

### run_id の扱い

`run_id` はETL実行単位の識別子である。

業務対象を選択するための正としない。

例えば、「最新 `run_id` のデータだけを業務対象とする」といった設計は避ける。業務対象は、業務テーブル側のキー・日付・event・ファイル・ステータス等で判定する。

`run_id` は以下の用途に限定する。

- 処理実行の証跡
- エラーとの紐付け
- 実行単位の調査
- 再実行時の比較材料
- 件数や処理時間などのメトリクス管理

### phase / process type

ETL処理の段階を表す値は、共通仕様に従う。

`health_exam_result v2` のように、`match`、`extract`、`check`、`export` などの処理段階が必要な場合でも、システムごとに独自カラムや独自テーブルを増やさず、共通ETL仕様で表現できるようにする。

共通仕様で表現できない処理段階が出た場合は、個別システム内で勝手に拡張せず、共通ETL仕様の変更として扱う。

## Change policy

`etl_runs` / `etl_errors` は共通基盤である。

そのため、仕様変更が必要になった場合は、対象システムだけを個別に変更しない。

変更時は以下を同時に確認・更新する。

- ADR
- spec
- README
- 共通DDL
- 各DBのDDL
- `scripts/lib/etl/`
- 既存スクリプトの利用箇所
- 移行SQLまたはマイグレーション

共通仕様を変更する場合は、影響DBと影響スクリプトを明示する。

## Consequences

### Positive

- ETL実行管理・エラー管理の責務が統一される。
- `runs` / `process_errors` / `job_logs` などの独自テーブル乱立を防げる。
- 各システムの調査方法が揃う。
- 共通ライブラリにより、ETL記帳の実装重複を減らせる。
- `health_exam_result v2` でも、既存ETL基盤と同じ考え方で実装できる。

### Negative

- 各DBに同じ構造のテーブルを持つため、DDL変更時は複数DBへ反映が必要になる。
- 共通仕様を変更する場合、影響範囲の確認が必要になる。
- 個別システムだけで素早く独自拡張することはできない。

### Neutral / trade-off

- 共通DBを作らないため、物理的な一元管理はしない。
- その代わり、業務DBごとの独立性とバックアップ・調査のしやすさを優先する。

## Application to health_exam_result v2

`health_exam_result v2` では、独自の `runs` テーブルを作成しない。

また、独自名称の `process_errors` テーブルも作成しない。

以下の共通ETL管理テーブルを `health_exam_result` DB内に配置する。

```text
health_exam_result.etl_runs
health_exam_result.etl_errors
```

`file_receipts`、`xml_ledger`、`item_values`、`exam_check_results` は業務固有の台帳であり、`health_exam_result` 固有テーブルとして設計する。

一方、実行履歴とETLエラーは共通基盤として `etl_runs` / `etl_errors` に寄せる。

## Related documents

- `docs/adr/0020-etl-common-lib-boundary.md`
- `docs/refactor/health_exam_result/05_design_history.md`
- `docs/refactor/health_exam_result/12_v2_ddl_design_notes.md`
- `docs/refactor/health_exam_result/14_etl_common_design_investigation_codex.md`