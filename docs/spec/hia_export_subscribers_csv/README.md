# HIA Export Subscribers CSV

## Purpose
このディレクトリは **HIA 側から提供される加入者CSV（export）を PHR に取り込む処理仕様**をまとめたものです。

対象範囲は以下の処理フローです。

```
HIA export subscribers CSV
        ↓
import_subscribers_to_staging_hub.py
        ↓
staging_subscribers_hub
        ↓
apply_subscribers_from_staging_hub.py
        ↓
subscribers
subscriber_addresses
subscriber_contacts
subscriber_audit
```


## Reading Map

この spec 群は、以下の順に読むと処理全体が追いやすい。

```
HIA Export CSV
   ↓
import_phase.md
   ↓
staging_subscribers_hub
   ↓
staging_schema.md
   ↓
identity_policy.md
   ↓
subscriber_apply.md
   ↓
subscribers
subscriber_addresses
subscriber_contacts
subscriber_audit
```

読み始めは `flow_overview.md`、詳細確認は各個別 spec を参照する。

---

## Scope

対象システム: **PHR**

対象データ:  
HIA 側から提供される加入者 CSV（export）

処理フェーズ:

1. CSV Import
2. Staging 保存
3. Subscriber Apply
4. Address / Contact 履歴管理
5. Subscriber Audit 生成

---

## Related ADR

この仕様は以下の ADR に基づきます。

- `ADR-0008` subscribers identity matching
- `ADR-0009` DB connection policy
- `ADR-0010` subscriber audit implementation

---

## Directory Structure

```
hia_export_subscribers_csv/

README.md
    この仕様の入口ドキュメント

flow_overview.md
    全体処理フロー

import_phase.md
    CSV → staging_subscribers_hub の取込仕様

staging_schema.md
    staging_subscribers_hub テーブル構造と役割

identity_policy.md
    subscriber 同一人物判定ポリシー

subscriber_apply.md
    staging → subscribers 反映仕様
```

---

## Design Principles

### 1. DBロジックは最小化
履歴管理や audit 生成は **DB trigger ではなく Python apply script で制御する**

理由:
- デバッグ容易性
- run context を扱いやすい
- ETL責務をスクリプト側へ集約

---

### 2. 履歴テーブルは audit 対象外

以下は **履歴管理テーブルのため audit しない**

```
subscriber_addresses
subscriber_contacts
```

変更履歴は

```
valid_from
valid_to
is_current
```

で管理する。

---

### 3. Subscriber Identity

subscriber の同一人物判定は以下の組み合わせを使用する。

```
person_id_custom
name_kana_full
gender_code
```

詳細は `ADR-0008` を参照。

---

## Implementation Entry Points

主な実装スクリプト:

```
scripts/work_folder/scripts/import_subscribers_to_staging_hub.py
scripts/work_folder/scripts/apply_subscribers_from_staging_hub.py
```

---

## Version

PHR v1.1.0

### v1.1.0 Changes (Subscriber Apply / Identity Handling)

背景:
- HIA 由来データで `name_kana_given` に `name_kana_full` をそのまま格納していた
- その結果、parts 列が「空欄ではない」と判定され、fund 由来の正しい分割値が上書きされない問題が発生
- `NULL` 判定のみだったため、空文字（''）が未設定として扱われなかった

対応:
- parts 列は「分割できた場合のみ格納」、それ以外は NULL を保持
- 未設定判定を「NULL または空文字」に統一
- identity 生成は parts に依存せず `name_kana_full` ベースで実施

効果:
- fund 側の高精度な name split が正しく反映される
- HIA / fund のデータ整合性が安定
- 再処理・バックフィル時の安全性向上