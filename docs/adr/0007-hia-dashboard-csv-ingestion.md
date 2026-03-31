# ADR 0007: HIA Dashboard CSV ingestion

- Status: Accepted
- Date: 2026-03-31

## Context

HIA の管理画面から、健保や事業所が現在の健診受診状況を確認するための CSV をダウンロードできる。

この CSV は、加入者ごとの現在状態を表す業務CSVであり、状態が更新されれば CSV の内容も更新される。

本 CSV をローカル環境（Ubuntu 上の MySQL）へ取り込み、既存の HIA 関連データや他データと付け合わせ・検索できるようにしたい。

現時点で分かっている前提は以下の通り。

- 入手元は HIA 管理画面
- CSV は状態確認用の業務データである
- 状態は更新されうる
- ローカル MySQL に取り込んで検索・突合に使う
- CSV には氏名カナが含まれていない
- CSV ヘッダー詳細は後続で確定する
- CSV は画面フィルタ付きでダウンロードでき、必ずしも全件を含まない
- 保険者番号は CSV 自体に含まれず、input 配置フォルダ名で補完する

既存の関連台帳として、少なくとも以下が存在する。

- `hia_person_years`
- `hia_xml_events`
- `hia_import_zips`

今回の CSV 取込は、これら既存 HIA 台帳と連携しうるが、まずは CSV 取込と状態スナップショット保持を第一段階として整理する必要がある。

## Decision

### 1. 第一段階では「最新状態テーブル + 変更履歴テーブル」で管理する

本 CSV は更新される業務CSVであるため、第一段階では次の構成で管理する。

- `hia_dashboard_status`  
  人物ごとの最新状態を保持するテーブル
- `hia_dashboard_status_history`  
  変更があった項目のみを記録する履歴テーブル

新規レコードは `hia_dashboard_status` に追加するが、履歴テーブルには記録しない。  
変更があった場合のみ、列単位 diff を `hia_dashboard_status_history` に記録する。

### 2. 取込先はローカル MySQL とする

HIA ダッシュボード CSV は Ubuntu 上の MySQL に取り込む。  
第一段階では `work_other` スキーマを使用する。

ETL run 管理は `work_other.etl_runs` / `work_other.etl_errors` を使用し、  
現段階では ETL テーブル構造の拡張は行わない。

### 3. 第一段階の目的は「取込 + 既存台帳との突合可能化」とする

第一段階では以下を目的とする。

- CSV の取込
- `work_other.etl_runs` / `work_other.etl_errors` による run 記帳
- `hia_dashboard_status` への最新状態反映
- `hia_dashboard_status_history` への変更履歴記帳
- `hia_dashboard_reminder_events` への受診勧奨送信日時保存
- SQL による一覧参照

### 4. 人物識別はダッシュボードCSV専用の論理キーで行う

この CSV は加入者マスタそのものではなく、HIA ダッシュボード画面の表示用CSVである。  
そのため、氏名の生値（`name`）は識別キーとして信用しない。

第一段階では、次の組み合わせを人物識別の論理キーとする。

- `insurer_number`
- `insurance_symbol_match`
- `insurance_number_match`
- `relationship_match`
- `name_match`

この組み合わせから `snapshot_identity_key` を構築する。

補足:

- `insurer_number` は input 配置フォルダ名で補完する
- 枝番は保持するが識別キーには使わない
- 氏名の生値は表示用として保持する
- 識別には `name_match` を使用する

理由:

- ダッシュボードCSVでは同一保険者・同一記号番号・同一続柄でも氏名表記差異や別人混在を避けたい
- 氏名違いを同一人物として束ねないため、識別キーには `name_match` を含める

`hia_person_years` との完全一致 join は第一段階では前提にしない。  
まずは HIA ダッシュボードCSV単体で最新状態管理と変更履歴管理を成立させることを優先する。

### 5. 状態定義は CSV 原文保持を基本とする

状態値はまず CSV 原文を保持する方針とし、内部コード化が必要な場合は後続で追加する。

想定状態例:

- 未予約
- 予約済み
- 受診済み
- 結果登録済み

受診勧奨送信回数も CSV 値をそのまま保持する。  
送信回数は `hia_dashboard_reminder_events` から再計算して強制一致させる対象とはしない。  
必要な場合のみ、後から比較確認できればよいものとする。

### 6. 変更判定は `snapshot_identity_key` と `row_sha256` を中心に行う

各 CSV 行は正規化後に `row_sha256` を計算する。

`row_sha256` は、人物単位の「現在状態」を高速判定するための要約値として使う。  
現行の構成項目は以下の通り。

- `status`
- `name_match`
- `insurance_symbol_match`
- `insurance_number_match`
- `relationship_match`
- `insured_type`
- `company_name`
- `department_name`
- `medical_institution`
- `course_name`
- `reservation_date`
- `exam_date`
- `employee_number`
- `email`
- `reminder_send_count`
- `exclusion_reason`

判定手順は次の通り。

- 同一 `snapshot_identity_key` が存在しない → `INSERT`
- 同一 `snapshot_identity_key` が存在する場合、まず `row_sha256` を比較する
- `row_sha256` が同じ → `UNCHANGED`
- `row_sha256` が異なる場合のみ列単位 diff を実行する
- 列単位 diff がある → `UPDATE`
- `row_sha256` が異なるが列単位 diff がない → `UNCHANGED`

補足:

- 履歴テーブルには `UPDATE` のみを記録する
- 新規 `INSERT` は履歴には記録しない
- `row_sha256` 自体は履歴テーブルの changed_column としては記録しない
- 日付項目（例: `reservation_date`, `exam_date`, `subscriber_birth`）は diff 比較時に表現差を吸収する
- `insured_type` は status 管理対象および row hash 構成項目に含める

移行対応:

- `insured_type` を row hash に追加したため、既存 `hia_dashboard_status` の `row_sha256` は backfill で再計算する

### 7. 自動DELETE判定は禁止する

この CSV は画面フィルタ付きで出力できるため、ある run の CSV に含まれないことをもって  
レコード削除・消失と判断してはならない。

そのため、DELETE は自動判定しない。  
必要な場合は `last_seen_run_id` を使った手動分析対象とする。

### 8. 受診勧奨送信日時は別テーブルに正規化保存する

`受診勧奨送信日時` は `|` 区切りの複数値を取りうるため、  
`hia_dashboard_reminder_events` に 1送信 = 1レコード で保存する。

推奨ユニーク条件:

- `(hia_dashboard_person_id, sent_at)`

## Consequences

### メリット

- 最新状態を `hia_dashboard_status` で即参照できる
- 変更点のみを `hia_dashboard_status_history` で追跡できる
- CSV が部分データでも安全に取り込める
- ファイル単位の変更は `run_id` を通じて追跡できる
- 受診勧奨送信日時を正規化して後続分析に使える

### デメリット / 留意点

- `hia_person_years` との完全一致突合は第一段階では行わない
- 氏名カナ、生年月日、性別が無いため、人物識別はダッシュボードCSV専用キーに依存する
- 自動DELETEを行わないため、消失分析は別途必要になる
- 送信回数はCSV値を保持するが、イベント件数との厳密同期は保証しない
- row hash 定義変更時は、既存データに対する backfill が必要になる

## Next step

1. `docs/spec/hia_fund_dashboard_csv/README.md` と `snapshot_policy.md` を現行実装と同期維持する  
2. `work_other` 向け DDL / migration / backfill の整合を保つ  
3. 会社環境を含む実行環境差分（import path / Pylance / DB接続）の吸収方針を v1.1 で整理する  
4. 必要に応じて `hia_person_years` など既存 HIA 台帳との分析・突合仕様を拡張する