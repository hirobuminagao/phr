# HIA_fund_ledger_xml Year Rule

このドキュメントは `HIA_fund_ledger_xml` における **健診年度（exam_year）** の決め方を整理するためのメモである。

本タスクでは、人物照合キーと人台帳更新の両方で `exam_year` を使用するため、年度判定ルールを明示的に固定する必要がある。

---

# 目的

- `exam_year` の決定ルールを固定する
- カレンダー年と健診年度を分けて扱う
- `.env` 等で年度開始日を切り替えられる前提を明確にする
- 将来の年度設定テーブル化に備える

---

# 基本方針

健診年度は **カレンダー年ではなく、設定値で判定する業務年度** とする。

例:

```text
2025年度 = 2025-04-01 〜 2026-03-31
2026年度 = 2026-04-01 〜 2027-03-31
```

したがって `YEAR(exam_date)` をそのまま `exam_year` として使わない。

---

# exam_year の決定元

`exam_year` は原則として **exam_date から算出**する。

## 理由

- 健診の実施年度を表したい
- DL 日やスクリプト実行日では年度がズレる可能性がある
- 人台帳の粒度 `person + exam_year` を安定させたい

---

# 必須項目

`exam_date` は必須項目とする。

扱い:

- `exam_date` が存在する → `exam_year` を算出する
- `exam_date` が無い → `EXAM_DATE_MISSING` として ZIP 単位エラー

つまり、`dl_date` を `exam_date` の代替として使わない。

---

# 初期実装での設定方法

年度境界は初期実装では `.env` 等の設定で管理する。

想定例:

```text
HIA_EXAM_YEAR_START_MONTH=4
HIA_EXAM_YEAR_START_DAY=1
```

この設定に基づいて、`exam_date` から `exam_year` を算出する。

---

# 判定イメージ

例として年度開始が 4 月 1 日の場合。

|exam_date|exam_year|
|---|---|
|2025-04-01|2025|
|2025-10-10|2025|
|2026-03-31|2025|
|2026-04-01|2026|

---

# 使いどころ

`exam_year` は少なくとも以下で使用する。

## 1 同一人物判定キー

```text
person_id_custom
+ name_kana_norm
+ gender_code
+ exam_year
```

## 2 人台帳の粒度

```text
person + exam_year
```

## 3 納品対象抽出

- 指定年度の対象抽出
- 年度内初回登場の管理

---

# dl_date との違い

`dl_date` はフォルダ名 / ZIP 名から取得する **業務上の到着日** である。

一方 `exam_year` は **健診実施日ベースの年度** である。

この 2 つは役割が異なるため混同しない。

|項目|意味|
|---|---|
|exam_date|健診実施日|
|exam_year|健診年度|
|dl_date|HIA 伝送上の業務日付|
|created_at|DB 記帳日時|

---

# なぜ dl_date を代替にしないか

`dl_date` を年度判定に使うと、以下の問題が起こる。

- 年跨ぎ取込で年度がズレる
- 過去データの後追い取込で本来年度を失う
- `person + exam_year` の粒度が壊れる

そのため、`exam_date` が無い XML は正常系に含めない。

---

# 将来拡張

※ v1 では `.env` による年度開始日設定を使用している。

将来は年度設定を DB テーブル化する可能性がある。

例:

```text
exam_year_config
  exam_year
  start_date
  end_date
```

ただし現時点では `.env` 管理で十分とする。

---

# 注意点

## YEAR(exam_date) を直接使わない

必ず年度開始日の設定を経由して `exam_year` を決定する。

## exam_year は取込時に確定値として保存する

後計算に頼らず、取込時点で算出した値を台帳に保持する前提とする。

---

# ステータス

v1 実装完了（2026-03）。

本ドキュメントは現在の `exam_year` 判定ルールを freeze した状態を示す。

対応実装

- exam_date 必須チェック
- exam_date から exam_year 算出
- 年度境界は `.env` 設定を使用
- person_year 識別キーに exam_year を使用