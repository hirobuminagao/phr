

# ETL Common Library Specification (v1.0)

## 1. Purpose

This document defines the responsibilities, boundaries, and design principles of the ETL common library used in the PHR system.

The ETL common library provides a **shared foundation layer** for all ETL scripts, ensuring consistent execution tracking, error handling, and metrics management.

---

## 2. Target Modules

The ETL common library consists of the following modules:

- `runs.py`
- `metrics.py`
- `errors.py`
- `ddl.py`
- `progress.py`

---

## 3. モジュール責務

### 3.1 runs.py

**責務：**
- ETL実行のライフサイクル管理
- 実行サマリ（`etl_runs`）の永続化
- 最終ステータスの決定

**主な関数：**
- `start_run()`
- `finish_run()`
- `_decide_status()`

**補足：**
- ライフサイクル状態（`running → success / partial / failed`）を管理する
- 行単位の処理は扱わない

---

### 3.2 metrics.py

**責務：**
- 実行カウント（事実）の保持

**フィールド：**
- files
- rows_seen
- rows_inserted
- rows_updated
- rows_unchanged
- rows_skipped
- errors

**設計ルール：**
- 純粋なデータコンテナ
- DBアクセスを持たない
- 業務ロジックを持たない
- ステータス判定ロジックを持たない

---

### 3.3 errors.py

**責務：**
- 行単位エラーの `etl_errors` への記録

**主な関数：**
- `log_error()`
- `log_normalize_error()`

**補足：**
- エラー記録のみを責務とする
- ヘルパー経由でエラーカウントを加算する
- ライフサイクル制御は行わない

---

### 3.4 ddl.py

**責務：**
- 必要なETLテーブルの存在保証

**主な関数：**
- `ensure_tables()`

**補足：**
- DB操作前に他モジュールから呼ばれる
- スキーマの準備を保証する

---

### 3.5 progress.py

**責務：**
- 実行進捗の表示（ログ出力のみ）

**主なクラス：**
- `ProgressLogger`

**設計ルール：**
- `RunMetrics` を参照するだけ（読み取り専用）
- 内部カウントを持たない
- 永続化は行わない

---

## 4. Caller Responsibilities (ETL Script)

The ETL script (caller) must:

- Call `start_run()` at the beginning
- Initialize and update `RunMetrics`
- Perform row-level processing
- Record row errors using `errors.py`
- Call `finish_run()` at the end
- Control transaction (`commit / rollback`)

---

## 5. Non-Responsibilities of Common Library

The ETL common library must NOT handle:

- Business logic
- Data normalization rules
- Mapping interpretation
- Identity matching
- Domain-specific validation

These responsibilities belong to the ETL script layer.

---

## 6. Dependency Structure

```
runs.py     → ddl.py, metrics.py
errors.py   → ddl.py
progress.py → metrics.py
metrics.py  → (no dependencies)
ddl.py      → (no dependencies)
```

---

## 7. Operational Assumptions

### 7.1 Schema Separation

ETL tables may exist in multiple schemas:

- `dev_phr`
- `work_other`

Rules:

- Each schema maintains its own `etl_runs` and `etl_errors`
- `run_id` is unique only within a schema
- ETL scripts must use the ETL tables in the same schema as their target data

### 7.2 Current State

- Parallel schema operation is active
- Future consolidation or dedicated ETL schema is undecided

---

## 8. Design Principles

### 8.1 Separation of Concerns

| Component | Responsibility |
|----------|---------------|
| metrics  | Facts (counts) |
| runs     | Lifecycle & meaning |
| errors   | Error persistence |
| ddl      | Schema guarantee |
| progress | Logging only |

---

### 8.2 Single Source of Truth

- Status is determined only by `_decide_status()`
- Counts are trusted only from `RunMetrics`

---

### 8.3 Extensibility

Possible future extensions:

- Retry mechanism
- Persistent progress tracking
- Incremental execution support
- Dedicated ETL schema

---

## 9. Integration Guideline

Standard ETL script flow:

1. Call `start_run()`
2. Initialize `RunMetrics`
3. Process data and update metrics
4. Log errors via `errors.py`
5. Call `finish_run()`

---

## 10. Summary

The ETL common library is a **foundation layer**, not a business layer.

It ensures:

- Consistent execution tracking
- Unified error handling
- Reliable metrics collection

while keeping all domain-specific logic outside the library.
