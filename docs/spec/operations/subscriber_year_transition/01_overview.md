# 01_overview

## 目的

本ファイルは、加入者年度更新運用の全体フローを俯瞰的に整理するための概要ドキュメントである。

本内容は確定版とし、各詳細 spec（staging / enrichment / dashboard / comparison）との整合を維持する。

## 全体フロー

本運用は、以下のステップで構成される。

### Step 0. 2025年度最終状態の確定

- HIAの加入者最新（2026/03/31時点）を `subscribers` に反映する
- ダッシュボードの年度履歴テーブル（`hia_dashboard_year_end_status`）を作成する
- 現在のダッシュボード状態を年度履歴テーブルへ記帳する
- ダッシュボードの運用テーブルのステータスを未予約等の初期状態へクリアする
- 上記の完了をもって2025年度の終了状態とする

参照:
- `04_dashboard_year_end_status.md`

---

### Step 1. staging_subscribers_fund の整理

- 健保受領データを `staging_subscribers_fund` に取り込む
- norm / match の整理方針に基づきカラムを整備する
- `person_id_custom` / `identity_hash` を生成する

参照:
- `05_staging_subscribers_fund.md`

---

### Step 2. subscribers 補完（enrichment）

- `staging_subscribers_fund` を入力として `subscribers` の不足カラムを補完する
- 主に氏名分解カラム（family / given）の補完を行う
- 比較可能な基準面として `subscribers` を整備する

参照:
- `06_subscriber_enrichment.md`

---

### Step 3. 2025年度最終状態の確定（補完後基準面の確定）

- Step1, Step2 を経て整備された `subscribers` を2025年度最終状態の比較基準面として確定する
- 以降の比較処理はこの状態を基準として行う

参照:
- `04_dashboard_year_end_status.md`

---

### Step 4. 2026受領データの取り込み

- 2026年度の健保受領データを staging に取り込む
- 同様に raw / norm / match / identity を生成する

参照:
- `05_staging_subscribers_fund.md`

---

### Step 5. 一次比較

- 2025年度最終状態（subscribers）と 2026受領データを比較する
- 以下の分類を行う
  - no_change
  - update
  - missing_from_new
  - new_in_file

参照:
- `03_comparison_policy.md`

---

### Step 6. 二次判定（転籍・例外対応）

- missing / new のうち単純比較では確定できないケースを再判定する
- 転籍、キー変更、表記揺れ等を考慮する

参照:
- `03_comparison_policy.md`

---

### Step 7. 更新反映（次フェーズ）

- 判定結果に基づく `subscribers` 更新は次フェーズで扱う
- 本specの主目的は差分抽出までとする

---

## 使用データの関係（概念）

本運用では以下のデータを主に利用する。

- `staging_subscribers_fund`
  - 受領データの norm / match / identity を保持する比較基盤

- `subscribers`
  - 補完後の加入者マスタ（比較基準面）

- `hia_dashboard_year_end_status`
  - 年度末の状態スナップショット（軽量台帳）

## 設計方針（重要）

- `staging_subscribers_fund` は比較基盤であり、加入者マスタではない
- raw は保持せず、norm を主値、match を照合用として扱う
- `subscribers` は最新状態かつ比較基準面として扱う
- identity_hash を年度更新運用における主たる比較・接続キー（論理キー）とする
- `subscribers.id` は自システム内の物理参照IDとして扱う
- HIA加入者IDは外部システム側の物理参照IDとして扱い、取得可能な場合は identity_hash および subscribers.id と対応付ける
- 運用判定（転籍等）は staging ではなく比較・判定フェーズで行う

## 今後の更新方針

本ファイルは確定フローとして扱い、変更が必要な場合のみ更新する。

- staging のDDL整理
- enrichment の詳細仕様
- comparison の判定ロジック
- dashboard 年度末記帳ルール

本ファイルを年度更新運用の公式フローとして固定する。