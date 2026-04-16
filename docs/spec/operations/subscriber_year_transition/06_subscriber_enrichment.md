# 06_subscriber_enrichment

## 目的

本specは、`subscribers` を比較基準面として利用するための補完（enrichment）処理の方針を定義する。

本処理の目的は以下とする。

- HIA受領データを基に、比較に必要な正規化・照合用カラムを整備する
- `identity_hash` を安定的に生成可能な状態にする
- comparison（03）で利用可能な基準面を確立する

---

## 前提

- `subscribers` は年度更新における比較基準面として扱う
- 本処理は「更新」ではなく「補完」である
- 補完は既存データを破壊せず、追加・整形のみを行う

---

## 補完対象

以下のカラム群を対象に補完を行う。

### 1. 正規化系（norm）

- `insurance_symbol_norm`
- `insurance_number_norm`
- `name_kana_full_norm`
- `birth_norm`
- `gender_code_norm`

### 2. 照合系（match）

- `insurance_symbol_match`
- `insurance_number_match`
- `name_kana_full_match`
- `name_kanji_full_match`
- `name_kanji_family_match`
- `name_kanji_given_match`

### 3. ID系

- `person_id_custom`
- `identity_hash`

---

## 処理内容

### 1. 正規化処理

- raw値から norm を生成する
- 正規化ルールは既存の共通ロジック（normalize系）に従う

---

### 2. match生成

- norm または raw を基に match を生成する
- match は照合用途に最適化された形式とする

---

### 3. identity生成

- `person_id_custom` を生成する
- `identity_hash` を生成する

生成条件：

- 必須項目が揃っている場合のみ生成
- 欠損がある場合は生成しない

---

## 注意事項

- enrichmentは比較の前処理であり、業務的な意味付けは行わない
- 本処理では喪失・転籍・更新の判断は行わない
- identity生成ロジックは一貫性を最優先とする

---

## 本specの責務範囲

本specは以下までを責務とする。

- 正規化（norm）の生成
- 照合用データ（match）の生成
- identity関連カラムの生成

以下は本specの範囲外とする。

- 差分判定（03で実施）
- 更新処理（別フェーズ）
- 業務判断
