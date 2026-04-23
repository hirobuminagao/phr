# HIA_fund_dashboard_csv

## 概要

HIA システムの管理画面からダウンロードできる「加入者の健診受診状況確認用 CSV」を取り込み、
ローカル環境（Ubuntu 上の MySQL）で検索・付け合わせ可能にするための仕様を整理する。

本 CSV は、健保や事業所が現在の受診状況を確認するための業務CSVであり、
状況が更新されれば CSV の内容も更新される。

想定状態例:

- 未予約
- 予約済み
- 受診済み
- 結果登録済み

---

## 目的

第一段階の目的は以下とする。

- HIA ダッシュボード CSV をローカル MySQL に取り込む
- 取込時点の状態スナップショットを保持する
- 既存の HIA 関連データと付け合わせ可能な状態を作る
- Navicat 等で SQL 一覧を参照できるようにする
- `hia_person_years` と join して、HIA export ZIP / XML 由来の person-year 情報を一覧で確認できるようにする
- 将来の状態変化履歴管理の基礎データとする

---

## 第一段階のスコープ

- CSV 取込
- `work_other.etl_runs` / `work_other.etl_errors` を用いた run 記帳
- 最新状態テーブルへの反映
- 変更履歴テーブルへの記帳
- 保険者番号の補完
- 受診勧奨送信日時の別テーブル保存
- 一覧 SQL 作成
- `identity_hash` を用いて `subscribers` から補助ID（`subscribers_id`, `hia_subscriber_id`）を解決し、`hia_dashboard_status` に保持する

---

## 第一段階の非スコープ

- 通知やアラート
- HIA 側への返却更新
- 完全一致の人物突合ロジック確定
- 自動DELETE判定

---

## CSV の位置づけ

この CSV は、HIA システム上のダッシュボード状態を表す業務CSVである。

前提:

- HIA 管理画面からダウンロードできる
- 日次または状態更新のたびに内容が変わりうる
- 現在状態を確認する用途のデータである
- 健保ログインで取得するため、CSV 自体には保険者番号が含まれない

そのため、CSV 単体で完全な人物識別を行うのではなく、
まずは「その時点の状態スナップショット」として保持することを優先する。

---

## input 構成

CSV 自体に保険者番号が含まれないため、input 配置フォルダで保険者番号を補完する。

```text
/data/hia_export/input_dashboard_csv/{insurer_number}/
```

例:

```text
/data/hia_export/input_dashboard_csv/06139463/
```

この `insurer_number` を CSV 取込時に補完値として使用する。

---

## 現時点で確認できている CSV ヘッダー

現時点で確認できている列は以下。

- ステータス
- 氏名
- 被保険者記号
- 被保険者番号
- 枝番
- 被保険者分類
- 続柄
- 企業名
- 部署名
- 医療機関
- 対象コース名
- 予約日
- 受診日
- 社員番号
- メールアドレス
- 受診勧奨送信回数
- 受診勧奨送信日時
- 除外理由

---

## 識別と突合の考え方

この CSV には、現時点で以下が含まれていない。

- 保険者番号（フォルダ名で補完）
- 氏名カナ
- 生年月日
- 性別

また、この CSV は加入者マスタそのものではなく、HIA ダッシュボード画面の表示用CSVである。
そのため、氏名（漢字）は識別キーとして信用しない。

第一段階では、次の組み合わせを人物識別の論理キーとする。

- insurer_number（フォルダ名補完）
- insurance_symbol
- insurance_number
- relationship

この組み合わせから次を構築する。

```text
snapshot_identity_key
```

補足:

- 枝番はキーに使用しない
- 氏名は参照用フィールドとして保持する
- 社員番号などの表示項目は補助確認用とする

`hia_person_years` との完全一致 join は第一段階では前提にしない。
まずは HIA ダッシュボードCSV単体で最新状態管理と変更履歴管理を成立させることを優先する。

補足（v1.1.0 以降の方針）:

- 第一段階ではダッシュボードCSV単体での最新状態管理を優先する
- ただし年度末固定および横断比較に備え、`identity_hash` を用いた `subscribers` 参照により補助IDを `hia_dashboard_status` に保持する
- これらの補助IDは import 時に解決し、snapshot 時には追加の補完処理を行わない

---

## 状態定義

第一段階では状態値を CSV 原文のまま保持する。

想定状態例:

- 未予約
- 予約済み
- 受診済み
- 結果登録済み

必要に応じて後続段階で内部コード列を追加する。

---

## 特記事項

### 受診勧奨送信日時

`受診勧奨送信日時` は `|` 区切りで複数日時を保持している可能性がある。

第一段階から `|` 区切りを分解し、別テーブルへ保存する。

保存先:

```text
hia_dashboard_reminder_events
```

### 枝番

`枝番` は空値の可能性があるため、NULL 許容で保持する。
ただし、人物識別キーには使用しない。

### DELETE 判定

この CSV は画面フィルタ付きでダウンロードできるため、必ずしも全件を含まない。

そのため、あるCSVに存在しないレコードを自動的に削除・消失と判定してはならない。

第一段階では:

- INSERT
- UPDATE
- UNCHANGED

のみを扱い、DELETE は手動分析対象とする。

---

## 想定テーブル

- `work_other.etl_runs`
- `work_other.etl_errors`
- `hia_dashboard_status`
- `hia_dashboard_status_history`
- `hia_dashboard_reminder_events`

---

## subscribers との連携方針（補助ID保持）

`hia_dashboard_status` は、ダッシュボードCSVの現状態に加えて、後続の年度末固定および比較処理で利用する補助IDを保持する。

方針:

- `identity_hash` をキーに `subscribers` テーブルを参照する
- 次の補助IDを `hia_dashboard_status` に保持する
  - `subscribers_id`（`subscribers.id`）
  - `hia_subscriber_id`（HIA由来の加入者ID）
- `subscriber_person_id_custom` は従来どおり保持する

責務分離:

- 補助IDの解決は **dashboard import 時点で行う**
- 年度末スナップショット（`hia_dashboard_year_end_status`）では、`subscribers` 等への追加 join による補完は行わない
- snapshot は `hia_dashboard_status` に保持された値をそのまま記帳する

---

## 想定スクリプト

- `hia_import_dashboard_csv.py`

---

## ETL run 記帳方針

run 管理には次を使用する。

```text
work_other.etl_runs
work_other.etl_errors
```

現段階では ETL テーブル構造の拡張は行わない。

ファイル名やファイル更新日時などの版情報は `etl_runs.notes` に記録する。

例:

```text
filename=dashboard_06139463_20260312.csv file_mtime=2026-03-12 10:22:31 filter: status=未予約
```

---

## 今後このディレクトリに追加するドキュメント

```text
README.md
flow_overview.md
identity_and_matching.md
snapshot_policy.md
status_definition.md

dashboard_person_year_join.md
```

---

## ステータス

現在は **基礎設計完了直前の整理フェーズ**。

次のステップ:

1. README / ADR を現在合意に合わせて更新する
2. 一旦コミットして基礎設計完了とする
3. DDL を設計する
4. 必要に応じて ER を追加する
5. 実装後に spec と ADR を更新して v1 を freeze する


---

## 開発部提供データ（HIA加入者ID）の取込と補完

### 概要

開発部から提供されるExcel（HIA加入者IDおよび識別情報）を用いて、
`subscribers.hia_subscriber_id` を補完する。

この処理は dashboard import の前提データ整備として位置付ける。

---

### Excel取込時の前処理（NULL表現）

開発部提供のExcelには、NULL値が文字列として「« NULL »」で表現されている場合がある。

本処理ではこの値をデータとして扱わず、以下の前処理を必須とする。

- Excel上で全置換を実施する
  - 置換前: `« NULL »`
  - 置換後: 空文字（""）

この前処理を行った上で、stagingテーブルへ投入する。

理由:

- identity生成および突合処理において、文字列としての「NULL」が混入すると正規化が破綻する可能性がある
- DB上では NULL または空値として扱うことを前提とする

---

### 処理フロー

1. Excel を staging テーブルへ投入する
   - テーブル: `work_other.staging_hia_subscribers_master_export_ids`

2. staging テーブル上で以下を実施
   - identity 算出元データの正規化
   - `identity_hash` の生成
   - `subscribers` との突合による `subscribers_id` 解決

3. 条件を満たすレコードのみ `subscribers` を更新
   - 条件:
     - `identity_hash` が生成されている
     - `subscribers_id` が解決されている
     - `hia_subscriber_id` が存在する

4. `subscribers.hia_subscriber_id` を更新する

---

### 責務分離

- Excel 由来データの取込・正規化・突合は staging + backfill スクリプトで行う
- `hia_dashboard_status` はこの結果を参照するのみとする
- snapshot では補完処理を行わない

---

### 想定スクリプト

- `from_dev_team_to_subscribers_hia_ids.py`（オーケストレーション）
- `backfill_staging_hia_subscribers_master_export_ids_identity.py`
- `backfill_subscribers_hia_subscriber_id_from_staging.py`

---

### 運用

- staging テーブルは都度 truncate して使用する
- Navicat 等で Excel を投入する
- backfill スクリプトを実行する
- Excel投入前に「« NULL »」の全置換を必ず実施する