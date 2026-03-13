

# ADR-0008: subscribers に identity match columns を追加

Status  
Accepted

Version  
PHR v1.0.1

## Context

PHR v1.0 では、加入者の識別は主に以下の情報に依存している。

- insurance_symbol
- insurance_number
- relationship
- name

しかし、以下のデータソースとの突合を行う際に、
表記ゆれの影響を受ける問題が確認された。

- HIA dashboard CSV
- 健診 XML (CDA)

これらのデータでは次のような差異が発生する。

- 全角 / 半角
- スペースの有無
- 記号の混在
- カナ表記ゆれ

そのため、直接比較では人物突合が安定しない。

PHR の人物突合ロジックを安定させるため、
正規化済みの識別列（match columns）を
subscribers テーブルに保持する方針とする。

## Decision

`dev_phr.subscribers` テーブルに以下の match 列を追加する。

- `name_kana_full_match`
- `name_full_match`
- `insurance_symbol_match`
- `insurance_number_match`

これらの列は Hub CSV の apply 処理時に生成し保存する。

dashboard CSV 取り込み処理および XML 突合では、
これらの match 列を利用して subscriber lookup を行う。

`birth` および `gender_code` は表記ゆれが発生しないため、
既存列をそのまま使用する。

## Consequences

利点

- dashboard / XML の人物突合が安定する
- match ロジックを subscribers に集約できる
- ETL ごとに重複した正規化処理を持つ必要がなくなる

影響

- `subscribers` スキーマ拡張
- Hub apply 処理の更新
- dashboard import の lookup 処理追加