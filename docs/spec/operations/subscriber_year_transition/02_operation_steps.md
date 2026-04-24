# 02_operation_steps

## 目的

本ファイルは、加入者年度更新運用における各ステップの実施内容、入出力、主に参照する system spec、および次ステップへの受け渡しを整理するための運用手順整理ファイルである。

本ファイルは個別スクリプトの詳細手順書ではなく、年度更新運用全体の中で

- 各ステップで何を行うか
- 何を入力として使うか
- 何を出力として次へ渡すか
- どの既存 spec / ADR を参照するか

を明確にすることを目的とする。

## 基本方針

- 各ステップは、業務上の目的と system spec の利用関係が分かる粒度で整理する
- 個別ロジックの詳細は各詳細 spec に委ねる
- 本ファイルでは、年度更新運用の配線図としての役割を持たせる
- 各ステップの結果は、次ステップの入力として何が受け渡されるかを明示する

## ステップ一覧

| Step | 名称 | 主目的 |
|---|---|---|
| 0 | 2025年度最終状態の確定 | 比較基準面と年度末状態を固定する |
| 1 | 2025年度基準面の補完準備 | HIA加入者情報・既存 subscribers を比較可能な形へ整える |
| 2 | 2025年度基準面の補完 | `subscribers` を比較基準面として補完する |
| 3 | 2026受領データの staging 取り込み | 健保受領データを比較基盤へ整える |
| 4 | 一次比較 | 基準面と新受領データの差分を一次分類する |
| 5 | 二次判定 | 転籍候補・キー変更・例外を再判定する |
| 6 | 更新反映 | 判定結果を `subscribers` 等へ反映する |
| 7 | 次年度運用準備 | 必要に応じて運用状態を次年度向けに整える |

## Step 0. 2025年度最終状態の確定

### 目的

2025年度運用後の状態を、2026年度比較の基準として扱えるよう固定する。

### 主な実施内容

- HIA加入者最新状態を確認し、必要に応じて `subscribers` へ反映する（当時点の事実として確定）
- HIAダッシュボード運用テーブルの現状態を年度末状態として記帳する
- 記帳時に以下を保持する
  - dashboard状態（status / reservation / exam）
  - subscribers由来の補助ID（subscribers_id / hia_subscriber_id / identity_hash）
  - subscribers由来の資格情報（qualification_lost_date）
- 資格喪失日は dashboard CSV から推定せず、`subscribers` の値を参照する
- 翌年度比較で不要な運用中状態を初期化する

### 主な入力

- HIA加入者最新情報
- HIAダッシュボード運用テーブル
- 既存 `subscribers`

### 主な出力

- 2025年度最終状態として扱う `subscribers`
- `hia_dashboard_year_end_status`

### 主に参照する spec

- `01_overview.md`
- `04_dashboard_year_end_status.md`
- `docs/spec/hia_export_subscribers_csv/subscriber_apply.md`
- `docs/spec/hia_fund_dashboard_csv/snapshot_policy.md`

## Step 1. 2025年度基準面の補完準備

### 目的

2025年度比較基準面として使う前に、既存 `subscribers` の不足情報や比較上の弱点を洗い出す。

### 主な実施内容

- 既存 `subscribers` の不足項目を確認する
- HIA由来の加入者情報を比較し、補完対象を整理する
- 氏名分解や match 項目の補完方針を適用可能な状態にする

### 主な入力

- 既存 `subscribers`
- HIA加入者情報
- HIA由来CSV / 既存補完材料

### 主な出力

- 補完対象一覧
- 補完処理へ渡す入力面

### 主に参照する spec

- `05_staging_subscribers_fund.md`
- `06_subscriber_enrichment.md`
- `docs/spec/hia_export_subscribers_csv/identity_policy.md`

## Step 2. 2025年度基準面の補完

### 目的

`subscribers` を比較基準面として利用できるよう、必要な項目を補完する。

### 主な実施内容

- 氏名分解カラムを補完する
- 必要な match 項目を補完する
- 比較に必要な基準面を確定する

### 主な入力

- Step1 の補完対象
- 補完用 staging / HIA加入者情報 / 既存 `subscribers`

### 主な出力

- 補完後 `subscribers`
- 2025年度最終比較基準面

### 主に参照する spec

- `06_subscriber_enrichment.md`
- `docs/spec/hia_export_subscribers_csv/subscriber_apply.md`

## Step 3. 2026受領データの staging 取り込み

### 目的

健保から受領した2026年度加入者データを、比較可能な staging 基盤へ取り込む。

### 主な実施内容

- CSVをテンプレートに従って `staging_subscribers_fund` へ取り込む
- norm / match / identity を生成する
- 必要に応じて `subscribers` 照合結果を保持する
- 差分判定用カラムを付与する
  - `diff_status`
  - `diff_status_method`
- 本テーブルは年度比較の作業基盤として扱い、投入前に状態（年度）を明確化する

### 主な入力

- 健保受領CSV
- `templates`
- `template_mappings`

### 主な出力

- `staging_subscribers_fund`

### 主に参照する spec

- `05_staging_subscribers_fund.md`
- `docs/spec/hia_export_subscribers_csv/staging_schema.md`
- `docs/spec/hia_export_subscribers_csv/identity_policy.md`

## Step 4. 一次比較

### 目的

2025年度基準面と2026受領データの差分を一次分類する。

### 主な実施内容

- 基準面と新受領データを比較する
- `no_change` / `update` / `missing_from_new` / `new_in_file` に分類する
- 補助判定情報を付与する
  - 最新資格取得日を基準とした新規候補判定
  - identity_hash による存在確認

### 主な入力

- 補完後 `subscribers`
- `staging_subscribers_fund`

### 主な出力

- 一次分類結果

### 主に参照する spec

- `03_comparison_policy.md`

## Step 5. 二次判定

### 目的

一次比較だけでは確定できないケースを再判定する。

### 主な実施内容

- 転籍候補を判定する
- キー変更、表記揺れ、年度跨ぎ例外を再確認する
- 単純比較では誤判定となる対象を切り分ける

### 主な入力

- 一次分類結果
- `subscribers`
- `staging_subscribers_fund`
- `hia_dashboard_year_end_status`

### 主な出力

- 確定判定結果
- 要確認対象一覧

### 主に参照する spec

- `03_comparison_policy.md`
- `04_dashboard_year_end_status.md`

## Step 6. 更新反映

### 目的

確定した判定結果をもとに必要な更新を反映する。

### 主な実施内容

- `subscribers` を更新する
- 必要に応じて新規追加・既存更新・失効扱いを反映する
- 反映結果を次運用へ引き継げる状態にする

### 主な入力

- 確定判定結果
- `subscribers`

### 主な出力

- 更新後 `subscribers`

### 主に参照する spec

- `03_comparison_policy.md`
- `06_subscriber_enrichment.md`
- `docs/spec/hia_export_subscribers_csv/subscriber_apply.md`

## Step 7. 次年度運用準備

### 目的

必要に応じて年度更新後の状態を次年度へ引き継げるよう整理する。

### 主な実施内容

- 次年度比較で必要な基準状態を確認する
- 運用テーブル・年度履歴テーブルの整合を確認する

### 主な入力

- 更新後 `subscribers`
- ダッシュボード運用状態
- 年度末状態管理テーブル

### 主な出力

- 次年度運用準備完了状態

### 主に参照する spec

- `04_dashboard_year_end_status.md`

## ステップと主要データの関係

| データ | 主に使うステップ | 役割 |
|---|---|---|
| `subscribers` | Step0, 1, 2, 4, 5, 6 | 比較基準面 / 最終保持面 |
| `staging_subscribers_fund` | Step3, 4, 5 | 新受領データの比較基盤 |
| `hia_dashboard_year_end_status` | Step0, 5, 7 | 年度末固定状態 |
| HIAダッシュボード運用テーブル | Step0, 7 | 運用中状態 |
| 健保受領CSV | Step3 | 新年度入力原本 |

## 関連 spec

- `README.md`
- `01_overview.md`
- `03_comparison_policy.md`
- `04_dashboard_year_end_status.md`
- `05_staging_subscribers_fund.md`
- `06_subscriber_enrichment.md`