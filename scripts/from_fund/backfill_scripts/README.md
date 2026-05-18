

# from_fund backfill scripts

`staging_subscribers_fund` / `subscribers` 系の補完・再計算・再突合用スクリプト一覧。

原則:
- import 本体とは分離して後追い実行する
- VSCode Run ボタンから単体実行可能にする
- run_id 単位で再実行可能にする
- 「既存値を壊さない」方針を優先する

---

## backfill_staging_subscribers_fund_address_match.py

`staging_subscribers_fund` の住所系 match カラムを再生成する backfill。

主用途:
- address raw/norm/match の再計算
- 住所正規化ロジック変更後の再反映
- import 済みデータへの後追い補完

対象:
- `staging_subscribers_fund`

主な更新対象:
- `address_*_match`

---

## backfill_staging_subscribers_fund_name_parts_match.py

`staging_subscribers_fund` の氏名 parts / match カラムを再生成する backfill。

主用途:
- 漢字・カナ parts 分割ロジック変更後の再計算
- family / given / middle 系 match の再生成
- import 済み staging データへの後追い補完

対象:
- `staging_subscribers_fund`

主な更新対象:
- `name_kana_*`
- `name_kanji_*`
- `*_parts`
- `*_match`

---

## backfill_subscribers_name_parts_match.py

`subscribers` 側の氏名 parts / match カラムを再生成する backfill。

主用途:
- subscribers の氏名 split ロジック変更後の再反映
- 既存 subscribers の parts / match 再計算
- NULL / 不正 parts の補完

対象:
- `subscribers`

主な更新対象:
- `name_kana_*`
- `name_kanji_*`
- `*_parts`
- `*_match`

---

## v1_1_0_refresh_parts_apply_subscriber_id.py

後追い parts 補完用の `parts_apply_*` を再確認・更新する backfill。

目的:
- import 時点の `matched_subscriber_id` をそのまま信用しない
- 現在の `subscribers.identity_hash` と staging 側 `identity_hash` を再確認する
- 安全に parts 補完可能な subscribers.id を `parts_apply_subscriber_id` に保持する

主用途:
- HIA本番反映後の後追い parts 補完
- identity_hash 変更検知
- 安全な parts apply 対象絞り込み

対象:
- `staging_subscribers_fund`
- `subscribers`

主な更新対象:
- `parts_apply_subscriber_id`
- `parts_apply_status`
- `parts_apply_reason`
- `parts_apply_checked_at`

補足:
- dry_run=true の場合は DB更新せず metrics のみ返す
- config の `import_run_ids` は複数指定可能
- run_id 単位で再実行可能

---

## v1_1_0_reset_subscriber_name_parts_to_null_if_invalid.py

不正な subscribers.name parts を NULL へ戻す backfill。

主用途:
- v1.1.0 移行時の暫定 parts リセット
- split 不可信頼データの除去
- parts 再生成前の初期化

対象:
- `subscribers`

主な更新対象:
- `name_kana_*`
- `name_kanji_*`
- `*_parts`

補足:
- 「不正 parts を一旦捨てる」用途
- 後続 backfill / apply により再補完する前提