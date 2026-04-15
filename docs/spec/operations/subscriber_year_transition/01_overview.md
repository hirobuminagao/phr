

# 01_overview

## 目的

本ファイルは、加入者年度更新運用の全体フローを俯瞰的に整理するための概要ドキュメントである。

本内容は暫定（ドラフト）とし、各詳細 spec（staging / enrichment / dashboard / comparison）の確定に応じて更新・確定する。

## 全体フロー（暫定）

本運用は、以下のステップで構成される。

### Step 0. staging_subscribers_fund の整理

- 健保受領データを `staging_subscribers_fund` に取り込む
- raw / norm / match の整理方針に基づきカラムを整備する
- `person_id_custom` / `identity_hash` を生成する

参照:
- `05_staging_subscribers_fund.md`

---

### Step 1. subscribers 補完（enrichment）

- `staging_subscribers_fund` を入力として `subscribers` の不足カラムを補完する
- 主に氏名分解カラム（family / given）の補完を行う
- 比較可能な基準面として `subscribers` を整備する

参照:
- `06_subscriber_enrichment.md`

---

### Step 2. 2025年度最終状態の確定

- 補完後の `subscribers` を 2025年度最終状態として扱う
- 必要に応じてダッシュボード情報を年度末状態として記帳する

参照:
- `04_dashboard_year_end_status.md`

---

### Step 3. 2026受領データの取り込み

- 2026年度の健保受領データを staging に取り込む
- 同様に raw / norm / match / identity を生成する

参照:
- `05_staging_subscribers_fund.md`

---

### Step 4. 一次比較

- 2025年度最終状態（subscribers）と 2026受領データを比較する
- 以下の分類を行う
  - no_change
  - update
  - missing_from_new
  - new_in_file

参照:
- `03_comparison_policy.md`

---

### Step 5. 二次判定（転籍・例外対応）

- missing / new のうち単純比較では確定できないケースを再判定する
- 転籍、キー変更、表記揺れ等を考慮する

参照:
- `03_comparison_policy.md`

---

### Step 6. 更新反映

- 判定結果に基づき `subscribers` を更新する
- 必要に応じて履歴・差分を保持する

---

## 使用データの関係（概念）

本運用では以下のデータを主に利用する。

- `staging_subscribers_fund`
  - 受領データの raw / norm / match / identity を保持する比較基盤

- `subscribers`
  - 補完後の加入者マスタ（比較基準面）

- `hia_dashboard_year_end_status`
  - 年度末の状態スナップショット（軽量台帳）

## 設計方針（重要）

- `staging_subscribers_fund` は比較基盤であり、加入者マスタではない
- `subscribers` は最新状態かつ比較基準面として扱う
- identity（`person_id_custom` / `identity_hash`）を全体の接続キーとする
- 運用判定（転籍等）は staging ではなく比較・判定フェーズで行う

## 今後の更新方針

本ファイルは暫定フローであり、以下の確定に応じて更新する。

- staging のDDL整理
- enrichment の詳細仕様
- comparison の判定ロジック
- dashboard 年度末記帳ルール

最終的に、本ファイルを年度更新運用の公式フローとして固定する。