# HIA_fund_ledger_xml Identity and Normalization

このドキュメントは `HIA_fund_ledger_xml` における **同一人物判定** と **正規化ルール** の考え方を整理するためのメモである。

本タスクでは、台帳更新・XML 照合・将来の再処理のすべてで、**同じ正規化ロジック**を使用することを前提とする。

v1.1.0 では、本パイプライン固有の identity 生成処理を追加するのではなく、既存の identity 共通lib を前提に実装を整理する。

したがって、本ドキュメントは HIA_fund_ledger_xml 固有の識別ルールを定義するだけでなく、identity 共通lib をどのような前提で利用するかを明示するものとして扱う。

関連する上位 / 共通 spec は以下とする。

- docs/spec/identity_canonicalization/README.md
- docs/spec/identity_canonicalization/identity_layer_structure.md
- docs/spec/identity_canonicalization/identity_layers_norm_and_purpose.md
- docs/spec/identity_canonicalization/v1.1.0_identity_layer_commonization.md

---

# 目的

- 同一人物判定キーを固定する
- 元値と正規化値の両方を保持する理由を明確化する
- XML 読込時の照合手順を統一する
- 将来の人台帳 / 健診イベント台帳追加時にも再利用できる形にする
- identity 共通lib 前提で HIA XML import 実装を整理するための確認基準にする
- event / event_instance へ接続する前段の識別仕様として位置づける

---

# v1.1.0 における位置づけ

本ドキュメントは、HIA_fund_ledger_xml の人物識別仕様を定義する。

ただし、v1.1.0 では以下を前提とする。

- person_id_custom / identity_hash / match 値は個別スクリプト内で直接生成しない
- identity 共通lib を利用して生成する
- 本ドキュメントは、HIA XML import 側で必要な入力項目・照合粒度・運用上の前提を定義する

つまり、本ドキュメントは「HIA XML import 側の識別要件 spec」であり、実際の生成ロジックの責務は identity 共通lib 側に置く。

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

`person_id_custom` は、保険者番号・記号・番号・生年月日をまとめた **主要な識別補助キー** として扱う。

ただし最終照合は `person_id_custom` 単独ではなく、以下を含めた複合条件で行う。

- `person_id_custom`
- `name_kana_norm`
- `gender_code`
- `exam_year`

なお、v1.1.0 のイベント台帳系では subscriber_id を実体参照キーとして扱う。

そのため、本パイプラインにおける person_id_custom は、HIA XML import 時点の人物識別および照合補助のためのキーとして位置づける。

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
|記号|insurance_symbol|insurance_symbol_match|
|番号|insurance_number|insurance_number_match|

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

なお、`name_kana_norm` は HIA_fund_ledger_xml における人物年度照合用の正規化値として扱う。

一方で、identity 共通lib では `name_kana_full_match` という canonical 値を別途生成し、identity_hash 生成および全体横断の照合用途に利用する。

## 2 記号

`insurance_symbol_match` は照合用の正規化値として扱う。

現時点での方針。

- 前後空白除去
- NFKC により英数を半角へ寄せる
- 区切り記号（例: `-`, `ー`, `−`, `―`）を除去する
- 数字連続部分は先頭 0 を削除する
- 照合結果は、たとえば `埼ー０１` / `埼−１` / `埼1` をすべて `埼1` に寄せる

|元値|照合用正規化値|
|---|---|
|埼ー０１|埼1|
|埼−１|埼1|
|AB-01|AB1|
|ＡＢ０１|AB1|

## 3 番号

`insurance_number_match` は照合用の正規化値として扱う。

現時点での方針。

- 前後空白除去
- NFKC により数字を半角へ寄せる
- 数字以外を除去する
- 先頭 0 を削除する

## 4 gender_code

`gender_code` は XML の値を用いる。

- 空はエラー
- 正常値の取り扱いは XML 仕様に従う
- 人照合の複合キー要素として使用する

---

# XML 読込時の照合ルール

XML 読込時は、必ず以下の順序で処理する。

1. XML から元値を取得する
2. identity 共通lib で項目別正規化を行う
3. match 値を生成する
4. person_id_custom を builder 経由で生成する
5. identity_hash を builder 経由で生成する
6. exam_year を算出する
7. 人台帳を以下で照合する

```
person_id_custom
+ name_kana_norm
+ gender_code
+ exam_year
```

8. 既存レコードがあれば更新対象とする
9. 無ければ新規人物年度レコードとして追加する

---

# 人台帳に保持する主な項目（v1 実装）

## 識別元データ

- insurer_number
- insurance_symbol
- insurance_number
- birthdate
- gender_code
- name
- name_kana

## 正規化後データ

- insurance_symbol_match
- insurance_number_match
- name_kana_norm

## 生成キー

- person_id_custom
- exam_year
- identity_hash

identity_hash は、identity 共通lib により以下を材料として生成する。

```
person_id_custom
+ name_kana_full_match
+ gender_code
```

## 管理項目

- first_seen_dl_date
- first_seen_send_seq
- last_seen_dl_date
- last_seen_send_seq
- dl_count
- created_at
- updated_at

---

# event / event_instance との接続方針（v1.1.0）

本パイプラインは、event 生成そのものを行う層ではなく、event / event_instance へ接続する前段の識別層として扱う。

ここで算出される exam_year は、HIA XML import における人物年度粒度を定義するための値である。

v1.1.0 のイベント台帳系では、この exam_year を event_year / event_id への所属判定に利用する。

また、HIA XML import 側で生成された person_id_custom / identity_hash / match 値は、event_instance 記帳時の識別補助値として利用する。

ただし、イベント台帳系における最終的な実体参照キーは subscriber_id とする。

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

## 共通lib前提で実装する

HIA_fund_ledger_xml では、個別スクリプト内で person_id_custom / identity_hash / match 値の生成ロジックを複製しない。

identity 共通lib を前提に実装し、本ドキュメントはその利用条件と照合粒度を定義するものとする。

---

# 今後ここで詰めること

- 氏名カナのダッシュ / 長音ゆれ吸収ルール
- 記号 / 番号の照合不要文字ルール
- 将来の健診イベント台帳追加時の識別粒度
- identity 共通lib適用後の実装差分反映と freeze

---

# ステータス

v1 実装完了（2026-03）。

本ドキュメントは、HIA XML import における現行識別ルールをベースにしつつ、v1.1.0 の identity 共通lib適用および event 接続前提へ更新中の状態を示す。

対応実装

- person_id_custom 生成
- 氏名カナ正規化
- 記号 / 番号照合用正規化
- person_year 識別キー
- identity 共通lib への移行（予定）
- event / event_instance 接続前提への更新（予定）