# ETL実行ライフサイクル仕様 (v1.0)

## 1. 概要

本ドキュメントは、PHRシステムにおけるETL実行管理（`etl_run`）のライフサイクル、責務、およびデータ構造を定義する。

**ETL Run** とは、1回のETL処理（import / apply / backfill など）の実行単位を指す。

### 現在の運用上の注意（スキーマ並行運用）

現在、ETLのトラッキングテーブルは以下の2つのスキーマで**並行運用**されている：

- `dev_phr.etl_runs` / `dev_phr.etl_errors`
- `work_other.etl_runs` / `work_other.etl_errors`

この状態は以下を意味する：

- `run_id` は **スキーマごとに独立して管理される**
- `run_id` は **スキーマをまたいで一意ではない**
- ETLスクリプトは、処理対象テーブルと**同一スキーマのETLテーブル**を使用する必要がある

今回の `staging_subscribers_fund` 取り込みにおいては：

- 対象テーブル：`dev_phr.staging_subscribers_fund`
- 使用するETLテーブル：`dev_phr.etl_runs` / `dev_phr.etl_errors`

将来的にスキーマ統合またはETL専用スキーマへ移行する可能性はあるが、現時点では未確定である。
本仕様では、**スキーマ単位での並行運用を正式な前提**とする。

---

## 2. 実行単位（粒度）

- 標準は `1 run = 1スクリプト実行` とする
- 1 run で複数ファイルを処理可能
- トランザクション境界は run 単位と揃えること

### 例外運用（ファイル単位run）

ファイル単位で意味を持つ ETL では、`1 file = 1 run` を許容する。

適用条件の例：

- ファイルごとに成功 / 失敗 / partial を判定したい
- 成功ファイルのみ archive へ移動したい
- エラーファイルのみ差し戻したい
- ファイル単位で再実行したい

今回の `staging_subscribers_fund` 取り込みはこの例外に該当する。

理由：

- CSV ごとに運用上の意味がある
- archive / 差し戻し判断をファイル単位で行いたい
- ETL実行結果（success / partial / failed）をファイル単位で管理したい

したがって、本ETLでは **1ファイル = 1 etl_run** を採用してよい。

---

## 3. ライフサイクル

### 3.1 開始（Start）

- 関数：`start_run(...)`
- 処理内容：
  - `etl_runs` にレコードをINSERT
  - `status = 'running'` を設定
  - `started_at` を設定

---

### 3.2 実行中（Processing）

- 行・ファイルの処理は呼び出し元が担当
- Metricsは処理中に更新される
- エラーは `etl_errors` に記録される

---

### 3.3 終了（Finish）

- 関数：`finish_run(...)`
- 処理内容：
  - `_decide_status(...)` により最終ステータスを決定
  - 以下を更新：
    - `status`
    - `finished_at`
    - metrics系カラム

---

## 4. ステータス定義

ステータスは metrics と変更件数により決定される。

定義：
- `changed = rows_inserted + rows_updated`

| ステータス | 条件 |
|-----------|------|
| success   | errors == 0 AND changed > 0 |
| partial   | errors > 0 AND changed > 0 |
| failed    | changed == 0 OR (errors > 0 AND changed == 0) |

補足：
- `_decide_status` が唯一の判定ロジック
- 変更が1件もない実行（changed == 0）は異常として `failed` 扱い
- 意図的に「no-op」を検知する設計とする

---

## 5. Metrics（RunMetrics）

RunMetrics は**純粋なデータコンテナ**である。

### フィールド

- files
- rows_seen
- rows_inserted
- rows_updated
- rows_unchanged
- rows_skipped
- errors

### 原則

- DB操作を持たない
- ステータス判定ロジックを持たない
- 事実（カウント）のみを保持する

---

## 6. データベーステーブル

**スキーマに関する注意：**
- ETLテーブルは `dev_phr` と `work_other` の両方に存在する可能性がある
- 現在は並行運用されている
- 各ETL実行は、対象テーブルと同一スキーマのETLテーブルに記録する

### 6.1 etl_runs

実行単位のサマリを保持するテーブル：

- id (PK)
- job_name
- status
- started_at
- finished_at
- metrics系カラム

目的：
- 監査ログ
- 実行トレース

---

### 6.2 etl_errors

行単位のエラーを保持するテーブル：

- run_id (FK)
- phase
- source
- insurer_number
- src_file
- src_row_no
- field
- error_code
- message

目的：
- エラー管理
- リトライ・分析支援

---

## 7. 責務

### 7.1 呼び出し側（ETLスクリプト）

必須事項：

- `start_run()` を呼ぶ
- `RunMetrics` を管理する
- 行エラーを `etl_errors` に記録する
- 必ず `finish_run()` を呼ぶ

---

### 7.2 etl_run モジュール

責務：

- ライフサイクル制御
- ステータス判定
- 実行サマリの永続化

---

### 7.3 metrics モジュール

責務：

- 実行カウントの保持

---

### 7.4 errors モジュール

責務：

- 行単位エラーの永続化

---

## 8. 設計原則

### 8.1 関心の分離

| コンポーネント | 責務 |
|---------------|------|
| metrics       | 事実（カウント） |
| runs          | ライフサイクルと意味付け |
| ddl           | 永続化 |
| errors        | エラー記録 |

---

### 8.2 単一責任の原則（Single Source of Truth）

- ステータス判定は `_decide_status` のみが担う
- カウントは metrics のみを信頼する

---

### 8.3 拡張性

将来的な拡張例：

- 進捗の永続化
- リトライ機能
- 増分処理

---

## 9. 統合ガイドライン

新規ETLスクリプトに組み込む際の手順：

1. 冒頭で `start_run()` を呼ぶ
2. `RunMetrics` を初期化
3. 処理中にmetricsを更新
4. エラーを `etl_errors` に記録
5. 最後に `finish_run()` を呼ぶ

---

## 10. 補足

- `id` は業務キーではない
- `run_id` はトレース用途
- ETLの正確性はmetricsの正確性に依存する
- `run_id` はスキーマ内でのみ一意
- 現在は `dev_phr` / `work_other` の並行運用
- `staging_subscribers_fund` は `dev_phr` 側のETLテーブルを使用する