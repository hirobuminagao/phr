

# 10_staging_subscribers_fund_apply

## ■ 目的
`staging_subscribers_fund` に取り込まれた加入者データのうち、
`matched_subscriber_id` により既存 `subscribers` と同一人物であることが確認できた行について、
`subscribers` 側の不足情報を安全に補完する。

本処理は **staging → subscribers の補完処理** であり、
新規加入者作成ではなく、既存加入者への空欄補完を目的とする。

---

## ■ 位置づけ
本処理は以下の後続処理として実施する。

1. `import_staging_subscribers_fund.py`
   - CSV を `staging_subscribers_fund` へ取り込む
   - norm / match / identity / `matched_subscriber_id` を生成する
2. 本 apply スクリプト
   - `matched_subscriber_id` が存在する staging 行を使って
   - `subscribers` 側の name parts 系 norm 列を補完する

---

## ■ 実装方針
- apply 専用の別スクリプトを作成する
- import 本体スクリプトが条件を満たした場合のみ、その apply スクリプトを呼び出す
- import と apply は責務を分離する

### 分離理由
- `import_staging_subscribers_fund.py` が肥大化しすぎるのを防ぐ
- staging 作成と subscribers 補完は別責務である
- apply 失敗時の切り分けを容易にする
- 後で apply 単体再実行しやすくする

---

## ■ run 粒度
本処理は `import_staging_subscribers_fund.py` の **1ファイル = 1 etl_run** 方針に従う。

したがって、apply の呼び出し判定も **CSV単位 / run_id単位** で行う。

---

## ■ apply 呼び出し条件
apply は、各 CSV の import 結果に対して以下の条件を満たす場合に実行する。

- 対象 run の status が `success` または `partial`
- `rows_inserted > 0`
- `failed` ではない

### 補足
- `all ok` のときだけに限定しない
- 一部行エラーがあっても、有効な staging 行が存在するなら apply 対象とする
- CSV 構造エラー等で import 自体が成立していない場合は apply しない

---

## ■ 対象データ
apply 対象は以下の条件を満たす staging 行のみとする。

- `import_run_id = 対象 run_id`
- `matched_subscriber_id IS NOT NULL`

つまり、
**同一人物が既存 subscribers に存在すると判定済みの行だけ** を対象とする。

---

## ■ 補完対象カラム
今回補完対象とするのは、`subscribers` 側の以下の norm 列である。

### カナ
- `name_kana_family_norm`
- `name_kana_middle_norm`
- `name_kana_given_norm`

### 漢字
- `name_kanji_family_norm`
- `name_kanji_middle_norm`
- `name_kanji_given_norm`

---

## ■ 補完ルール
### 共通原則
- `subscribers` 側が空欄のときのみ更新する
- 既存値が入っている場合は上書きしない
- staging 側の norm 値をそのまま使用する
- match 列ではなく norm 列を補完対象とする

### カナ parts
以下の条件で補完する。

- `matched_subscriber_id` がある
- `subscribers` 側の `name_kana_*_norm` が空欄
- `staging_subscribers_fund` 側の `name_kana_*_norm` に値がある

### 漢字 parts
以下の条件で補完する。

- `matched_subscriber_id` がある
- `subscribers` 側の `name_kanji_*_norm` が空欄
- `staging_subscribers_fund` 側の `name_kanji_*_norm` に値がある

### 漢字補完の前提
漢字 parts の扱いは、既存の `staging_hia -> apply` 系で採用している full → parts の考え方と整合するようにする。

---

## ■ identity 前提
`matched_subscriber_id` が入っていることは、少なくとも以下が一致していることを意味する。

- `identity_hash`
- その構成要素である `person_id_custom`
- `name_kana_full_match`
- `gender_code`

したがって、本 apply では
**同一人物判定は import 側で完了済み** とみなし、
apply 側では追加の identity 判定は行わない。

---

## ■ 更新禁止ルール
本処理では以下を行わない。

- 新規 `subscribers` レコード作成
- 既存非空欄値の上書き
- identity 再判定
- 業務判断による merge / split
- match 列の補完

---

## ■ ETL / エラー方針
apply は import と分離した別スクリプトであり、必要に応じて独立した ETL run として管理可能とする。

最低限の方針：
- import 側 run_id を単位に apply 対象を決める
- apply 内の更新件数・エラー件数は別管理できる構造とする
- apply 失敗は import 成功を否定しない

---

## ■ 期待する効果
- 既存 `subscribers` の name parts 欠損を補完できる
- HIA / fund / subscribers 間で name parts の整合性を高められる
- 後続の比較・判定・出力処理で parts 列を利用しやすくなる

---

## ■ 今後の拡張候補
- `relationship_name` 系の補完
- 住所系の補完
- apply 単体再実行スクリプトの整備
- apply 用 ETL run / ETL errors の独立管理