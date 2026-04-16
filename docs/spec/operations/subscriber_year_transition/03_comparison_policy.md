# 03_comparison_policy

## 目的

本specは、年度更新における加入者差分の判定ルールを定義する。

本比較は、以下を目的とする。

- 現在の加入者基準面（subscribers）と受領データの差分を明確にする
- 差分を分類し、後続処理（更新・確認・保留）に接続する
- 年度更新処理における一貫した判定基準を提供する

---

## 前提条件（重要）

本比較は以下の制約条件を前提とする。

- 受領する加入者データは、記号100本人以外の最新に限られる場合がある
- 記号100本人は別タイミングで受領される
- このため、本比較は健保全体の完全最新比較ではなく、部分最新比較として扱う

### 制約による影響

- `missing_from_new` は即時に喪失を意味しない
- 特に以下は本比較のみでは確定できない
  - 喪失
  - 記号100への転籍
  - 単なる未受領

- 記号100以外 → 記号100 の遷移は本比較では確定不能とする

---

## 比較キー

本比較は以下のキーで実施する。

- `identity_hash` を主たる比較キーとする

補助情報として以下を利用する。

- `person_id_custom`
- `insurance_symbol_match`
- `insurance_number_match`

---

## 比較対象

### 1. 基準面

- `subscribers`
- 最新状態（ただし部分最新の可能性あり）

### 2. 受領面

- staging_subscribers_fund
- 受領データ（記号100本人以外の可能性あり）

---

## 差分分類

比較結果は以下の区分に分類する。

### 1. no_change

- identity_hash が一致
- 比較対象項目に差分なし

---

### 2. update

- identity_hash が一致
- 比較対象項目に差分あり
- 比較対象項目は以下を基本とする
  - `insurance_symbol_match`
  - `insurance_number_match`
  - `name_kanji_full_match`
  - `name_kana_full_match`
  - `birth_norm`
  - `gender_code_norm`

例：
- 記号変更
- 番号変更
- 氏名変更（氏変更を含む。表記揺れ除く）

---

### 3. new

- 受領データに存在
- 基準面に存在しない

---

### 4. missing_from_new

- 基準面に存在
- 受領データに存在しない

※注意：
本区分は以下を含む可能性がある。

- 喪失
- 記号100への転籍
- 受領タイミング差

よって、本区分単独では状態を確定しない。

#### missing_from_new からの推察

`missing_from_new` は確定判定ではなく、追加の推察対象として扱う。

##### 1. 転籍推察

以下に該当する場合は、転籍候補として推察対象に含める。

- `name_kana_full_match` が一致する
- `birth_norm` が一致する
- `gender_code_norm` が一致する
- `insurance_symbol_match` が不一致である
- `insurance_number_match` が不一致である
- 補助情報として `name_kanji_full_match` の一致を確認できる場合は、より強い推察とする

##### 2. 氏名変更推察

以下に該当する場合は、氏名変更候補として推察対象に含める。

- `birth_norm` が一致する
- `gender_code_norm` が一致する
- `insurance_symbol_match` が一致する
- `insurance_number_match` が一致する
- `name_kanji_family_match` が不一致である
- `name_kanji_given_match` が一致する

この場合、確定区分は `missing_from_new` または `needs_review` としつつ、補助判定として「氏名変更候補」を付与する。

##### 3. 扱い

- 上記はいずれも確定判定ではなく、推察情報として扱う
- 推察情報のみを根拠に `subscribers` を更新しない
- 最終的な確定は後続の業務判断または追加データに委ねる

---

### 5. needs_review（要確認）

以下に該当する場合は要確認とする。

- 記号100関連の可能性がある missing
- 転籍候補と推察されるケース
- 氏名変更候補と推察されるケース
- identity_hash が一致しないが近似するケース
- 判定が一意に定まらないケース

---

## 判定ルール

### 基本フロー

1. identity_hash による突合
2. 一致有無で分岐
   - 一致 → no_change / update
   - 不一致 → new / missing_from_new
identity_hash が不一致の場合でも、補助条件により同一人物候補を検出することがある。

### 候補有無の確認条件

`missing_from_new` や `new` をそのまま確定扱いせず、補助的な推察を行う場合は、比較対象の中に「候補が存在するか」を先に確認する。

#### 1. 転籍候補の存在確認

以下をすべて満たす受領レコードが存在する場合、当該 `missing_from_new` レコードに対して転籍候補ありとみなす。

- `name_kana_full_match` が一致する
- `birth_norm` が一致する
- `gender_code_norm` が一致する
- `identity_hash` は不一致である
- `insurance_symbol_match` が不一致である
- `insurance_number_match` が不一致である

#### 2. 氏名変更候補の存在確認

以下をすべて満たす受領レコードが存在する場合、当該 `missing_from_new` または `new` レコードに対して氏名変更候補ありとみなす。

- `birth_norm` が一致する
- `gender_code_norm` が一致する
- `insurance_symbol_match` が一致する
- `insurance_number_match` が一致する
- `name_kanji_family_match` が不一致である
- `name_kanji_given_match` が一致する

#### 3. 候補なしの場合の扱い

- 上記条件に合致する候補が存在しない場合は、転籍候補・氏名変更候補の推察ラベルを付与しない
- `missing_from_new` は `missing_from_new` のまま扱う
- `new` は `new` のまま扱う

#### 4. 候補が複数存在する場合の扱い

- 候補が複数存在する場合は、自動確定しない
- `needs_review` を付与し、後続の業務判断へ回す

---

### 補助判定

以下の情報を用いて補助的に判定する。

- insurance_symbol_match
- insurance_number_match
- name_kana_full_match
- name_kanji_full_match
- name_kanji_family_match
- name_kanji_given_match
- birth_norm
- gender_code_norm

候補有無の確認条件を満たす場合に限り、`転籍候補` および `氏名変更候補` の推察ラベルを付与する。

---

## 注意事項

- 本比較は「確定判定」ではなく「分類処理」である
- 特に missing_from_new は確定的な意味を持たない
- `転籍候補` や `氏名変更候補` は推察情報であり、確定判定ではない
- 推察ラベルは、比較対象内に候補が存在することを確認できた場合にのみ付与する
- 判定結果は後続の業務判断または追加データにより確定する

---

## 本specの責務範囲

本specは以下までを責務とする。

- 差分の検出
- 差分の分類

以下は本specの範囲外とする。

- subscribers の更新処理
- 喪失／転籍の確定判定
- 業務ロジックによる最終判断
