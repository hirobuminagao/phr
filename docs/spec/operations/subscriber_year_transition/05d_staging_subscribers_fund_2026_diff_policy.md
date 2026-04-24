

# 05d_staging_subscribers_fund_2026_diff_policy

## 目的

本ファイルは、2026年度受領データと2025年度固定済み基準との**差分判定方針**を整理するための spec である。

親 spec:
- `05_staging_subscribers_fund.md`

関連:
- `01_overview.md`（基準面の考え方）
- `02_operation_steps.md`（実行フロー）
- `05b_staging_subscribers_fund_column_policy.md`（diffカラム仕様）

---

## 用語

- **2025基準面**: `hia_dashboard_year_end_status` を起点に、当時点の `subscribers` 補完情報を含めた固定状態
- **2026受領データ**: 健保から受領した加入者CSV（記号100本人は整形後に含める）
- **staging**: `staging_subscribers_fund`

---

## 比較の前提

- 比較は「2025固定」 vs 「2026受領」の**時点差分**で行う
- `subscribers` は比較時点の参照として使用するが、基準面は**更新されない**
- identity は `identity_hash` を主キーとする

---

## 判定で使う主な材料

- `identity_hash`（存在確認の第一キー）
- `person_id_custom`（補助）
- `insurance_symbol_match` + `insurance_number_match`（補助）
- `name_kana_full_match` / `gender_code_norm`（補助）
- `qualification_acquired_date_norm`（新規候補判定）

---

## 一次分類（初期ラベリング）

staging 取り込み後、以下の4分類を付与する。

- `no_change`
- `update`
- `missing_from_new`
- `new_in_file`

### 判定ロジック（概念）

- 2025基準面に存在し、2026にも同一 `identity_hash` が存在
  - 主要項目が同一 → `no_change`
  - 主要項目に差分あり → `update`
- 2025基準面に存在し、2026に存在しない → `missing_from_new`
- 2025基準面に存在せず、2026に存在 → `new_in_file`

---

## 補助判定（差分の意味付け）

一次分類に加えて、以下の補助判定を行い、`diff_status` へ反映する。

### 新規候補（new）

以下を満たす場合に `new` 候補とする。

- `identity_hash` が基準面に存在しない
- かつ `qualification_acquired_date_norm` が、
  - 現在の `subscribers` における当該保険者の**最新資格取得日以上**

※ 最終確定ではなく候補判定

---

### 転籍候補（transfer）

以下のいずれかで候補とする。

- `person_id_custom` が一致するが `identity_hash` が変化
- 会社情報（`received_company_*`）の変化が検出される

---

### 既存（existing）

- `identity_hash` が一致し、主要項目差分が軽微
- もしくは差分があっても同一人物継続と判断できる

---

### 不明（unknown）

- 上記ルールで自動判定できない
- identity 欠損
- キー不整合

---

## diff カラムへの反映

staging に以下を記録する。

- `diff_status`
  - `new` / `transfer` / `existing` / `unknown`
- `diff_status_method`
  - `script`（自動判定）
  - `manual`（手動補正）
- `diff_status_reason`
  - 判定根拠（文字列）

例:

- `identity_hash not found`
- `acquired_date >= current_max`
- `person_id_custom matched but identity changed`
- `manual override`

---

## 記号100本人の扱い

- データは受領済み（Excel等）
- staging 投入前に**既存テンプレート形式へ整形**
- 本比較では他レコードと同等に扱う

制約:

- 元フォーマット差による正規化揺れに注意

---

## 注意点

- 本判定は**最終確定ではない**（staging上の一時判定）
- `subscribers` への反映ロジックとは分離する
- 誤判定は `diff_status_method=manual` で上書き可能にする

---

## 一文まとめ

> 2026差分判定は、2025固定基準に対する identity 主体の比較により初期分類を行い、取得日・補助キーで意味付けを行う二段階判定とする