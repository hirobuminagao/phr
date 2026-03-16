

# ADR-0010: subscriber_audit implementation strategy

## Status
Accepted

## Context

PHR v1.0.1 では  
staging_subscribers_hub から subscribers へ加入者データを apply する
パイプラインを構築している。

対象スクリプト

scripts/work_folder/scripts/apply_subscribers_from_staging_hub.py

SQLite 版では  
subscribers テーブルの変更監査は trigger により
subscriber_audit テーブルへ記録されていた。

主な trigger

- trg_subscribers_audit_insert
- trg_subscribers_audit_update
- trg_subscribers_audit_delete

update 監査は列単位差分として複数行 INSERT される。

また source は audit_context テーブルから取得される構造であった。

```
(SELECT source FROM audit_context WHERE id = 1)
```

という仕組みで、DB trigger 内から実行コンテキストを取得していた。

## Problem

MySQL 移行において同じ仕組みを trigger で再現すると次の問題がある。

- デバッグが困難
- 変更処理の実行経路が見えにくい
- run context の注入が複雑になる
- dev / stg / prod で挙動差異が発生しやすい
- ETL / apply スクリプトと DB の責務が混ざる

PHR システムでは、データ更新は基本的に
ETL / apply パイプライン経由で実行される設計となっている。

## Decision

subscriber_audit の生成は

DB trigger ではなく  
apply スクリプト側で実装する。

具体的には

apply_subscribers_from_staging_hub.py において

- insert
- update
- delete

のイベント発生時に

subscriber_audit テーブルへ監査行を INSERT する。

update の場合は SQLite 版と同様に

**変更列単位で監査行を生成する。**

監査対象列は SQLite 版 trigger を基準として定義する。

## Audit Scope

監査対象列

- insurer_number
- insurance_symbol
- insurance_symbol_digits
- insurance_number
- insurance_branchnumber
- birth
- gender_code
- name_kana_full
- name_kanji_full
- relationship_name
- qualification_acquired_date
- qualification_lost_date
- employer_code
- department_code
- distribution_code
- employee_code
- connect_id

update 時は、値が変更された列ごとに  
subscriber_audit に1行 INSERT する。

## Insert Audit

insert 時

```
field = "__insert__"
old_value = NULL
new_value = "inserted"
```

## Delete Audit

delete 時

```
field = "__delete__"
old_value = "deleted"
new_value = NULL
```

## Metadata

source は apply スクリプトの引数から設定する。

例

```
apply_staging
```

change_run_id は

```
subscribers.last_change_run_id
```

をそのまま使用する。

## Consequences

メリット

- 監査ロジックを Git で管理できる
- run_id / source の注入が簡単
- ETL / apply pipeline と整合する
- デバッグが容易
- DB trigger 依存を減らせる

デメリット

- DB単体更新では audit が生成されない

ただし PHR システムでは  
subscribers 更新は apply pipeline を経由する設計のため問題ない。

## Notes

subscriber_addresses  
subscriber_contacts

は履歴テーブルとして設計されているため

**subscriber_audit の対象には含めない。**

住所・連絡先の履歴は

```
valid_from
valid_to
is_current
```

によって管理する。