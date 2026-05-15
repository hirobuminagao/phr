

# 10_staging_subscribers_fund_apply

## ■ 目的
`staging_subscribers_fund` に取り込まれた加入者データのうち、
`matched_subscriber_id` により既存 `subscribers` と同一人物であることが確認できた行について、
`subscribers` 側の不足情報を安全に補完する。

本処理は **staging → subscribers の補完処理** であり、
新規加入者作成ではなく、既存加入者への空欄補完を目的とする。

本specでは、`staging_subscribers_fund` を利用する処理のうち、
**後追いの parts 補完** を主対象とする。

登録・変更候補としての判定は import 時点の `matched_subscriber_id` を利用するが、
後追い parts 補完では、現在の `subscribers` 状態を再確認するため、
parts 補完専用の適用先IDを別カラムで管理する。

---

## ■ 位置づけ
本処理は以下の後続処理として実施する。

1. `import_staging_subscribers_fund.py`
   - CSV を `staging_subscribers_fund` へ取り込む
   - norm / match / identity / `matched_subscriber_id` を生成する
2. 本 apply スクリプト
   - `matched_subscriber_id` は import 時点の登録・変更候補参照として扱う
   - 後追い parts 補完時は、現在の `subscribers` 状態を確認したうえで `parts_apply_subscriber_id` を設定する
   - `parts_apply_subscriber_id` が存在する staging 行を使って、`subscribers` 側の name parts 系 norm 列を補完する

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

## ■ 用途の分離

`staging_subscribers_fund` は、以下の2用途で利用される。

### 1. 登録・変更候補としての利用

CSV import 時点で、既存 `subscribers` との突合結果を `matched_subscriber_id` に保持する。

この値は、取り込み時点における登録・変更候補の参照先として扱う。

- `matched_subscriber_id` は import 時点の突合結果である
- HIA本番や管理画面側で氏名等が変更された場合、後続時点の `identity_hash` と一致しない可能性がある
- そのため、後追い parts 補完の更新先としてはそのまま使用しない

### 2. 後追い parts 補完としての利用

HIA本番から `subscribers` への登録・変更反映が完了した後、
`staging_subscribers_fund` に残っている氏名 parts 情報を使って、
既存 `subscribers` の不足 parts を補完する。

この用途では、現在時点の `subscribers` と staging 側の `identity_hash` が一致することを確認してから更新する。

- 後追い parts 補完用の適用先IDとして `parts_apply_subscriber_id` を使用する
- `parts_apply_subscriber_id` は parts補完用の再確認済み subscribers.id である
- `matched_subscriber_id` の意味は変更しない

---

## ■ 対象データ
apply 対象は以下の条件を満たす staging 行のみとする。

- `import_run_id = 対象 run_id`
- `parts_apply_subscriber_id IS NOT NULL`
- `parts_apply_status = 'IDENTITY_MATCHED'`

つまり、
**後追い parts 補完時点で、現在の subscribers と identity_hash の一致を確認できた行だけ** を対象とする。

`matched_subscriber_id` は import 時点の登録・変更候補参照であり、
後追い parts 補完の更新先としては直接使用しない。

---

## ■ parts 補完用の再確認カラム

後追い parts 補完では、import 時点の `matched_subscriber_id` とは別に、
parts 補完専用の適用先IDを staging 側に保持する。

想定カラム:

| カラム | 用途 |
|--------|------|
| `parts_apply_subscriber_id` | parts 補完時点で更新先として確認済みの `subscribers.id` |
| `parts_apply_status` | parts 補完用の再確認結果 |
| `parts_apply_reason` | 補足理由・スキップ理由 |
| `parts_apply_checked_at` | 再確認日時 |

### 再確認ルール

parts 補完前に、対象 run の `parts_apply_*` 系カラムを一度クリアする。

そのうえで、以下を確認する。

- `matched_subscriber_id IS NOT NULL`
- `subscribers.id = staging_subscribers_fund.matched_subscriber_id`
- `subscribers.identity_hash = staging_subscribers_fund.identity_hash`

上記を満たす場合のみ、`parts_apply_subscriber_id` に `subscribers.id` を設定し、
`parts_apply_status = 'IDENTITY_MATCHED'` とする。

### identity_hash 不一致時の扱い

`matched_subscriber_id` が存在していても、現在の `subscribers.identity_hash` と staging 側の `identity_hash` が一致しない場合は、parts 補完を行わない。

このケースは、HIA本番・管理画面側で氏名等が変更され、
staging 側の情報が古くなっている可能性があるためである。

想定ステータス:

| status | 意味 | apply対象 |
|--------|------|-----------|
| `IDENTITY_MATCHED` | 現在の subscribers と identity_hash が一致 | 対象 |
| `IDENTITY_CHANGED` | matched_subscriber_id はあるが identity_hash が不一致 | 対象外 |
| `SUBSCRIBER_NOT_FOUND` | matched_subscriber_id の subscribers が存在しない | 対象外 |
| `MISSING_IDENTITY_HASH` | staging 側 identity_hash がない | 対象外 |

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
- `parts_apply_subscriber_id` で確認できた `subscribers` 側が空欄のときのみ更新する
- 既存値が入っている場合は上書きしない
- staging 側の norm 値をそのまま使用する
- match 列ではなく norm 列を補完対象とする

### カナ parts
以下の条件で補完する。

- `parts_apply_subscriber_id` がある
- `subscribers` 側の `name_kana_*_norm` が空欄
- `staging_subscribers_fund` 側の `name_kana_*_norm` に値がある

### 漢字 parts
以下の条件で補完する。

- `parts_apply_subscriber_id` がある
- `subscribers` 側の `name_kanji_*_norm` が空欄
- `staging_subscribers_fund` 側の `name_kanji_*_norm` に値がある

### 漢字補完の前提
漢字 parts の扱いは、既存の `staging_hia -> apply` 系で採用している full → parts の考え方と整合するようにする。

---

## ■ identity 前提

### import 時点

`matched_subscriber_id` は、CSV import 時点における既存 `subscribers` との突合結果である。

この値は登録・変更候補処理では有効な参照値として扱うが、
後追い parts 補完の更新先としては直接使用しない。

### parts 補完時点

後追い parts 補完では、`matched_subscriber_id` の先にある現在の `subscribers` 行を確認し、
現在の `subscribers.identity_hash` と staging 側 `identity_hash` が一致する場合のみ更新対象とする。

同じ `matched_subscriber_id` を持っていても、identity_hash が不一致の場合は、
staging 側の情報が古い可能性があるため parts 補完を行わない。

---

## ■ 更新禁止ルール
本処理では以下を行わない。

- 新規 `subscribers` レコード作成
- 既存非空欄値の上書き
- 登録・変更候補としての `matched_subscriber_id` の意味変更
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
- `parts_apply_*` カラムのDDL追加とbackfillスクリプト整備