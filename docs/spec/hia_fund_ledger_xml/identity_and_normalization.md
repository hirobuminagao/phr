

# HIA_fund_ledger_xml Identity and Normalization

このドキュメントは `HIA_fund_ledger_xml` における **同一人物判定** と **正規化ルール** の考え方を整理するためのメモである。

本タスクでは、台帳更新・XML 照合・将来の再処理のすべてで、**同じ正規化ロジック**を使用することを前提とする。

---

# 目的

- 同一人物判定キーを固定する
- 元値と正規化値の両方を保持する理由を明確化する
- XML 読込時の照合手順を統一する
- 将来の人台帳 / 健診イベント台帳追加時にも再利用できる形にする

---

# 同一人物判定キー

現時点の freeze 方針は以下。

```
person_id_custom
+ name_kana_norm
+ gender_code
+ exam_year
```

## 各要素の意味

|要素|意味|
|---|---|
|person_id_custom|保険者番号・記号・番号・生年月日から生成する識別キー|
|name_kana_norm|氏名カナの正規化値|
|gender_code|XML 上の性別コード|
|exam_year|年度設定に基づく健診年度|

---

# person_id_custom

## 元データ

`person_id_custom` は以下を元に生成する。

- insurer_number（保険者番号）
- insurance_symbol（記号）
- insurance_number（番号）
- birthdate（生年月日）

## 位置づけ

`person_id_custom` は、保険者番号・記号・番号・生年月日をまとめた **人物識別の中核キー** として扱う。

ただし最終照合は `person_id_custom` 単独ではなく、以下を含めた複合条件で行う。

- `person_id_custom`
- `name_kana_norm`
- `gender_code`
- `exam_year`

---

# 元値と正規化値を両方保持する方針

人台帳には、**元値** と **正規化値** の両方を保持する。

## 理由

1. 何を元に人照合したかを後で説明できる
2. 正規化前の元データ確認ができる
3. 将来ルール変更時に再判定しやすい
4. XML 読込時と台帳保存時の処理を一致させやすい

## 例

|項目|元値|正規化値|
|---|---|---|
|氏名カナ|name_kana|name_kana_norm|
|記号|insurance_symbol|insurance_symbol_norm|
|番号|insurance_number|insurance_number_norm|

---

# 正規化方針

## 1 氏名カナ

`name_kana_norm` は、人物照合用の正規化値として扱う。

現時点での方針。

- 全角カタカナへ寄せる
- 半角カナを吸収する
- ひらがなをカタカナへ寄せる
- 全角/半角スペースを除去する
- 中点を除去する
- 長音・ダッシュ類の揺れは今後の共通正規化ルールに従う

## 2 記号

`insurance_symbol_norm` は照合用の正規化値として扱う。

現時点での方針。

- 前後空白除去
- 全角数字は半角へ寄せる
- 必要に応じて照合不要文字を除去する

## 3 番号

`insurance_number_norm` は照合用の正規化値として扱う。

現時点での方針。

- 前後空白除去
- 全角数字は半角へ寄せる
- 必要に応じて照合不要文字を除去する

## 4 gender_code

`gender_code` は XML の値を用いる。

- 空はエラー
- 正常値の取り扱いは XML 仕様に従う
- 人照合の複合キー要素として使用する

---

# XML 読込時の照合ルール

XML 読込時は、必ず以下の順序で処理する。

1. XML から元値を取得
2. 共通関数で正規化
3. `person_id_custom` を生成
4. `exam_year` を算出
5. 人台帳を以下で照合

```
person_id_custom
+ name_kana_norm
+ gender_code
+ exam_year
```

6. 既存レコードがあれば更新対象とする
7. 無ければ新規人物年度レコードとして追加する

---

# 人台帳に保持する主な項目（想定）

## 識別元データ

- insurer_number
- insurance_symbol
- insurance_number
- birthdate
- gender_code
- name
- name_kana

## 正規化後データ

- insurance_symbol_norm
- insurance_number_norm
- name_kana_norm

## 生成キー

- person_id_custom
- exam_year

## 管理項目

- first_seen_dl_date
- first_seen_send_seq
- last_seen_dl_date
- last_seen_send_seq
- dl_count
- created_at
- updated_at

---

# 注意点

## person_id_custom 単独では照合しない

保険者番号・記号・番号・生年月日だけでは、静かな誤突合の可能性がある。
そのため最終照合は以下の複合条件で行う。

```
person_id_custom
+ name_kana_norm
+ gender_code
+ exam_year
```

## 正規化処理は 1 箇所に固定する

同じ見た目の処理を別スクリプトに複製しない。

今後は、共通関数として 1 箇所に集約する前提とする。

---

# 今後ここで詰めること

- `person_id_custom` 生成仕様の明文化
- 氏名カナのダッシュ / 長音ゆれ吸収ルール
- 記号 / 番号の照合不要文字ルール
- 将来の健診イベント台帳追加時の識別粒度

---

# ステータス

現在は **freeze 前の仕様整理フェーズ**。

この文書は、DDL 作成前の識別ルール整理メモとして扱う。