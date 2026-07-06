# 24 Phase2 Design Review

## 1. 目的

Phase2 medical_folder_aliases 初期データ / event migration について、実装前に設計上の確認点を整理する。

本資料では決定事項を新規に確定せず、`03_decisions.md` を正式仕様、`05_design_history.md` を設計理由・協議履歴として扱い、Phase1との整合性、テーブル構成、カラム定義、PK / FK、UNIQUE、INDEX、制約、命名、運用上の懸念、将来拡張性をレビューする。

## 2. 参照資料

- `docs/refactor/health_exam_result/03_decisions.md`
- `docs/refactor/health_exam_result/05_design_history.md`
- `docs/refactor/health_exam_result/20_implementation_plan.md`
- `docs/refactor/health_exam_result/21_dry_run_review.md`
- `docs/refactor/health_exam_result/23_phase1_core_ddl_detail.md`
- `docs/spec/health_examinations/03_medical_folder_aliases_initial_data_v2_0_0.md`

## 3. 現状確認

- 既存の `24_phase2_*.md` は確認時点では存在しない。
- Phase2の対象は、`medical_folder_aliases` 初期データと `dev_phr.event.result_root_path` の準備である。
- `medical_folder_aliases` テーブル自体はPhase1 Core DDL対象であり、Phase2では初期データ投入SQLが主対象となる。
- `dev_phr.event.result_root_path` は migration で追加する。
- `03_medical_folder_aliases_initial_data_v2_0_0.md` には `event_id = 2` の初期データ188件が定義されている。
- 初期投入時点では原則 `src_folder_raw = dst_folder_norm` とする。
- `202604開院_福岡労働衛生研究所　健診スクエア博多` は仮フォルダ名の可能性がある注意事項として扱う。

## 4. Phase1との整合性確認

| 観点 | 確認結果 | 分類 |
|---|---|---|
| `medical_folder_aliases` の配置 | Phase1 Core DDLで `health_exam_result.medical_folder_aliases` を作成対象としている。Phase2初期データ投入と矛盾しない。 | 問題なし |
| cross schema FK | Phase1方針では `dev_phr` へのcross schema FKは張らない。Phase2の `event_id` は外部参照・検索用として扱う必要がある。 | 問題なし |
| `event_id` INDEX | 初期実装では `event_id` および `UNIQUE(event_id, src_folder_raw)` によるものを基本とする。 | 問題なし |
| UNIQUE | `medical_folder_aliases` の一意制約は `UNIQUE(event_id, src_folder_raw)` とする。 | 問題なし |
| `src_folder_raw` / `dst_folder_norm` | 03 / 05 / 初期データ仕様で、実フォルダ名と正規フォルダ名の責務は一致している。 | 問題なし |
| `is_active` | 初期値は `1` とする。 | 問題なし |
| `manual_judgement` | 初期値は `0` とし、仮名称等の補足情報は `note` に保持する。 | 問題なし |
| `note` | 初期データ仕様に `note` があり、仮フォルダ名注意を保持できる。DDL上の型・投入方針はPhase1側に依存する。 | 問題なし |

## 5. Phase2設計レビュー

### 5.1 テーブル構成

| 項目 | 確認結果 | 分類 |
|---|---|---|
| `medical_folder_aliases` | event単位の医療機関フォルダ名変換台帳として利用する方針は03/05/12/19/23で整合している。 | 問題なし |
| `dev_phr.event` | `result_root_path` を migration で追加する。型は `text`、`NULL` 許可とする。 | 問題なし |
| 初期データ投入SQL | `sql/seed/health_exam_result/0010_health_exam_result__medical_folder_aliases_event2.sql` として作成する。 | 問題なし |
| event migration | `dev_phr.event.result_root_path` を追加するmigrationを作成する。 | 問題なし |

### 5.2 カラム定義

| カラム | 確認結果 | 分類 |
|---|---|---|
| `alias_id` | 自動採番に任せ、seed SQLでは明示投入しない。 | 問題なし |
| `event_id` | 初期データは `event_id = 2`。cross schema FKは張らない方針と整合。 | 問題なし |
| `src_folder_raw` | 共有フォルダ上の実フォルダ名。初期データ仕様と整合。 | 問題なし |
| `dst_folder_norm` | システム内部で利用する正規フォルダ名。初期投入では原則 `src_folder_raw = dst_folder_norm`。 | 問題なし |
| `manual_judgement` | 初期値は `0`。仮名称等の補足情報は `note` に保持し、`manual_judgement` の判定条件とはしない。 | 問題なし |
| `note` | 初期データ仕様のnoteを保持する用途と整合。 | 問題なし |
| `is_active` | 初期値は `1`。 | 問題なし |
| `created_at` / `updated_at` | seed SQLでは `created_at` は初回INSERT時のみ設定し、再実行時は `updated_at` を更新対象とする。 | 問題なし |
| `result_root_path` | 型は `text`、`NULL` 許可とする。既存 `event_id = 2` への初期値設定は保留。 | 保留 |

### 5.3 PK / FK

| 観点 | 確認結果 | 分類 |
|---|---|---|
| `medical_folder_aliases.alias_id` PK | Phase1方針と整合。 | 問題なし |
| `medical_folder_aliases.event_id` FK | cross schema FKは張らない方針のため、`dev_phr.event` へのFKは不要。 | 問題なし |
| `event.result_root_path` | `dev_phr` migrationで追加する。 | 問題なし |

### 5.4 UNIQUE

| 観点 | 確認結果 | 分類 |
|---|---|---|
| `event_id` + `src_folder_raw` | `UNIQUE(event_id, src_folder_raw)` とする。 | 問題なし |
| `event_id` + `dst_folder_norm` | 一意制約を設けず、複数の実フォルダ名から同一名称への集約を許可する。 | 問題なし |
| 初期データ188件の重複 | 仕様書上の行は188件。`UNIQUE(event_id, src_folder_raw)` により重複登録を防止する。 | 問題なし |

### 5.5 INDEX

| 観点 | 確認結果 | 分類 |
|---|---|---|
| `event_id` | 初期実装のインデックス対象とする。 | 問題なし |
| `event_id` + `src_folder_raw` | `UNIQUE(event_id, src_folder_raw)` によるものを基本とする。 | 問題なし |
| `event_id` + `is_active` | 初期実装では追加INDEX対象外とし、運用実績を見て追加する。 | 保留 |
| `dst_folder_norm` | 正規フォルダ名で逆引き・調査するかに依存する。 | 保留 |

### 5.6 制約

| 観点 | 確認結果 | 分類 |
|---|---|---|
| `src_folder_raw` NOT NULL | 共有上の実フォルダ名であり、NOT NULLで矛盾しない。 | 問題なし |
| `dst_folder_norm` NOT NULL | システム内部で使う正規名であり、NOT NULLで矛盾しない。 | 問題なし |
| `is_active` デフォルト | 初期値は `1` とする。 | 問題なし |
| `manual_judgement` デフォルト | 初期値は `0` とする。 | 問題なし |
| `note` 空文字 / NULL | 補足がある行のみ `note` に値を入れ、補足なしは `NULL` とする。 | 問題なし |

### 5.7 命名

| 観点 | 確認結果 | 分類 |
|---|---|---|
| テーブル名 | `medical_folder_aliases` は03/12/19/20/23で整合。 | 問題なし |
| `src_folder_raw` / `dst_folder_norm` | 05/12/19/23/初期データ仕様で整合。 | 問題なし |
| `result_root_path` | `dev_phr.event.result_root_path` として migration で追加する。 | 問題なし |
| 初期データSQL名 | `sql/seed/health_exam_result/0010_health_exam_result__medical_folder_aliases_event2.sql` とする。 | 問題なし |
| `dev_phr.event` migration名 | 正式ファイル名は未決。 | 要協議 |

### 5.8 運用上の懸念

| 懸念 | 内容 | 分類 |
|---|---|---|
| 仮フォルダ名 | `202604開院_福岡労働衛生研究所　健診スクエア博多` は仮フォルダ名の可能性がある補足情報として `note` に保持する。 | 問題なし |
| フォルダ名変更 | 将来 `src_folder_raw` と `dst_folder_norm` が異なるケースを想定する。更新運用の詳細は将来扱う。 | 保留 |
| 初期データ再実行 | `INSERT ... ON DUPLICATE KEY UPDATE` で再実行可能にする。 | 問題なし |
| `event.result_root_path` 環境差 | 共有フォルダパスとローカルDocker向けパスを同一カラムで扱う。具体的な初期値設定方法は別途確認する。 | 要協議 |
| 初期データの正 | `03_medical_folder_aliases_initial_data_v2_0_0.md` を正とする方針は明確。 | 問題なし |

### 5.9 将来拡張性

| 観点 | 確認結果 | 分類 |
|---|---|---|
| event単位対応 | `event_id` を持つため年度・イベント単位のフォルダ差分に対応可能。 | 問題なし |
| フォルダ名変更吸収 | `src_folder_raw` と `dst_folder_norm` の分離により対応可能。 | 問題なし |
| 医療機関マスタ化 | 本テーブルは医療機関マスタではないと明記されており、責務分離できている。 | 問題なし |
| 複数rawから同一norm | `dst_folder_norm` には一意制約を設けず、複数の実フォルダ名から同一名称への集約を許可する。 | 問題なし |
| 無効化運用 | `is_active` の初期値は `1` とする。更新ルールは将来運用で扱う。 | 保留 |

## 6. 協議事項

以下は決定せず、協議事項として扱う。

1. `dev_phr.event.result_root_path` migration の正式ファイル名をどうするか。
2. `result_root_path` の初期値を既存 `event_id = 2` へ設定するか、別途手動更新とするか。
3. seed SQL 内の188件データの最終確認をどう行うか。

## 7. 分類一覧

### 問題なし

- `medical_folder_aliases` をevent単位のフォルダ名変換台帳として扱うこと。
- `src_folder_raw` を共有フォルダ上の実フォルダ名、`dst_folder_norm` を正規フォルダ名として扱うこと。
- 初期投入時点では原則 `src_folder_raw = dst_folder_norm` とすること。
- `03_medical_folder_aliases_initial_data_v2_0_0.md` を初期データの正とすること。
- `event_id = 2` の188件を初期データ候補として扱うこと。
- `dev_phr` へのcross schema FKを張らないこと。
- Phase1 Core DDL対象の `medical_folder_aliases` に対して、Phase2で初期データ投入を行う流れ。
- `medical_folder_aliases` の一意制約を `UNIQUE(event_id, src_folder_raw)` とすること。
- `dst_folder_norm` に一意制約を設けず、複数の実フォルダ名から同一名称への集約を許可すること。
- 初期実装のインデックスは `event_id` および `UNIQUE(event_id, src_folder_raw)` によるものを基本とすること。
- `is_active` の初期値を `1` とすること。
- `manual_judgement` の初期値を `0` とすること。
- 仮名称等の補足情報は `note` に保持し、`manual_judgement` の判定条件とはしないこと。
- `dev_phr.event.result_root_path` を migration で追加すること。
- `result_root_path` の型を `text` とし、`NULL` 許可とすること。
- 対象 `event_id` の `result_root_path` が未設定の場合、v2処理ではエラーとすること。
- 初期データSQLの配置先を `sql/seed/health_exam_result/` とすること。
- 初期データSQLのファイル名を `0010_health_exam_result__medical_folder_aliases_event2.sql` とすること。
- 初期データSQLは `INSERT ... ON DUPLICATE KEY UPDATE` で再実行可能にすること。
- 再実行時の更新対象を `dst_folder_norm`、`note`、`is_active`、`manual_judgement`、`updated_at` とすること。
- `created_at` は初回INSERT時のみ設定すること。
- `alias_id` は自動採番に任せ、seed SQLでは明示投入しないこと。
- 補足がある行のみ `note` に値を入れ、補足なしは `NULL` とすること。

### 要協議

- `dev_phr.event.result_root_path` migration の正式ファイル名。
- `result_root_path` の初期値を既存 `event_id = 2` へ設定するか、別途手動更新とするか。
- seed SQL 内の188件データの最終確認。

### 保留

- `dst_folder_norm` の逆引きINDEX要否。
- フォルダ名変更後の履歴管理を持つかどうか。
- フォルダ名変更時の更新運用。
- `is_active` を使った無効化運用の更新ルール。

## 8. 判定

Phase2はGO。

`medical_folder_aliases` のUNIQUE / INDEX / 初期値方針、seed SQLの配置・命名・再実行方針、`dev_phr.event.result_root_path` の追加方針は決定済みであり、Phase2の実装へ進める状態である。残る確認事項は、migrationの正式ファイル名、`event_id = 2` への初期値設定方法、seed SQL内188件データの最終確認である。


## 10. 実装結果

### 実装日
2026-07-04

### 実装ファイル
- `sql/migrations/dev_phr/20260703_001_dev_phr_add_result_root_path_to_event.sql`
- `sql/seed/health_exam_result/0010_health_exam_result__medical_folder_aliases_event2.sql`

### 実装内容
- `dev_phr.event.result_root_path` migration 作成済み。
- `medical_folder_aliases` seed SQL 作成済み。
- `event_id = 2` の188件を投入対象として生成済み。
- seed は `ON DUPLICATE KEY UPDATE` で再実行可能。

### 現在の判定
Phase2 実装完了。
DB実機適用と188件目視確認は未実施。