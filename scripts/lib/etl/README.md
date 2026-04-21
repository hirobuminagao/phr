

# ETL Common Library

`scripts/lib/etl/` は、PHR における ETL 共通基盤ライブラリである。

本ディレクトリは、ETL 実行のライフサイクル管理、metrics 管理、行エラー記録、DDL存在保証、進捗表示を共通化することを目的とする。

---

## 役割

このライブラリは **基盤レイヤ** であり、業務ロジックは持たない。

扱うもの:
- ETL 実行開始 / 実行終了
- `etl_runs` への実行記録
- `etl_errors` への行単位エラー記録
- metrics（件数カウンタ）の保持
- 進捗ログ表示
- ETLテーブルの存在保証

扱わないもの:
- 業務判定
- 正規化ルール
- mapping 解釈
- identity 照合
- ドメイン固有バリデーション

---

## ディレクトリ構成

- `runs.py`
  - ETL 実行の開始 / 終了
  - run status 判定
  - `etl_runs` の更新
- `metrics.py`
  - `RunMetrics` 定義
  - 実行カウントの保持（事実のみ）
- `errors.py`
  - `etl_errors` への行エラー記録
- `ddl.py`
  - `etl_runs` / `etl_errors` テーブルの存在保証
- `progress.py`
  - 進捗ログ表示
- `__init__.py`
  - パッケージ定義

---

## 責務分離

### runs.py
- ライフサイクル管理を担当する
- `start_run()` / `finish_run()` / `_decide_status()` を持つ
- status の意味付けを行う

### metrics.py
- `RunMetrics` を定義する
- 純粋なデータコンテナであり、DBアクセスや業務ロジックを持たない

### errors.py
- 行単位エラーを `etl_errors` に保存する
- ライフサイクル管理は行わない

### ddl.py
- 必要テーブルの存在を保証する
- DB操作前の前提条件を整える

### progress.py
- `RunMetrics` を参照して進捗を表示する
- 表示専用であり、内部カウントや永続化は持たない

---

## 基本的な使い方

標準的な ETL スクリプトは以下の順でこのライブラリを使う。

1. `start_run()` を呼ぶ
2. `RunMetrics` を初期化する
3. 行処理中に metrics を更新する
4. 行エラーを `errors.py` 経由で記録する
5. 最後に `finish_run()` を呼ぶ

---

## ステータス判定

`runs.py` の `_decide_status()` が唯一のステータス判定ロジックを持つ。

定義:
- `changed = rows_inserted + rows_updated`

判定:
- `errors == 0` かつ `changed > 0` → `success`
- `errors > 0` かつ `changed > 0` → `partial`
- `changed == 0` → `failed`
- `errors > 0` かつ `changed == 0` → `failed`

補足:
- 変更が1件もない run は **no-op 異常** として `failed` 扱いにする

---

## スキーマ運用上の注意

現在、ETL テーブルは以下のスキーマで並行運用されている。

- `dev_phr.etl_runs` / `dev_phr.etl_errors`
- `work_other.etl_runs` / `work_other.etl_errors`

このため:
- `run_id` はスキーマごとに独立して採番される
- `run_id` はスキーマをまたいで一意ではない
- ETL スクリプトは、対象テーブルと同一スキーマの ETL テーブルを使用する

例:
- `dev_phr.staging_subscribers_fund` を扱う ETL は `dev_phr.etl_runs` / `dev_phr.etl_errors` を使う

---

## 設計上の前提

このディレクトリは `scripts/work_folder/lib/etl/` から `scripts/lib/etl/` へコピーして共通化した版である。

今後の方針:
- 共通 ETL 基盤として `scripts.lib.etl.*` を正とする
- `work_folder` 側実装との差異は、必要時に整理・統合する

---

## 関連ドキュメント

- `docs/spec/common/etl_run_lifecycle.md`
  - ETL 実行ライフサイクル仕様
- `docs/spec/common/etl_common_lib.md`
  - ETL 共通ライブラリ仕様
- `docs/adr/0020-etl-common-lib-boundary`
  - ETL 共通libの責務境界とスキーマ並行運用方針

ADR-0020 により、本ライブラリは **基盤レイヤのみを担当し、業務ロジックを持たない** ことが正式決定されている。

---

## 注意

- commit / rollback は呼び出し側の責務である
- ETL エラー件数は metrics と `etl_errors` 記録の両面で管理される
- 業務固有の変換や判定は本ディレクトリに追加しない