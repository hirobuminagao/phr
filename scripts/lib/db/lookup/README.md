# scripts/lib/db/lookup

DB lookup 共通lib。

目的:

```text
「どう検索するか」
「どのキーで候補を引くか」
「どう段階的に identity resolve するか」
```

を業務ロジックから分離すること。

本ディレクトリは:

```text
business rule
apply_action decision
ETL orchestration
subscriber update
prepare/apply flow
```

を責務に持たない。

---

# Current Design Policy

lookup layer は:

```text
軽量な identity-ish values を返す
```

ことを基本方針とする。

つまり:

```text
subscriber_id のみ
```

ではなく、後続 lookup / hydrate / compare に再利用しやすい
lightweight identity handle を返す。

例:

```python
{
    "subscriber_id": 123,
    "identity_hash": "...",
    "person_id_custom": "...",
    "hia_subscriber_id": "...",
}
```

lightweight identity handle は:

```text
検索
compare
hydrate
resolver chaining
```

などの後続処理で再利用できる lightweight key set を意図する。

lookup layer は:

```text
住所
電話
メール
業務詳細データ
```

などの重い hydrate data は返さない。

---

# lookup / resolver / hydrate の分離

本ディレクトリでは:

```text
lookup
resolver
hydrate
```

を分離する。

---

## lookup

責務:

```text
exact search
candidate retrieval
lightweight identity handle retrieval
```

返却:

```text
lightweight identity handle
```

例:

```python
{
    "subscriber_id": 123,
    "identity_hash": "...",
    "person_id_custom": "...",
    "hia_subscriber_id": "...",
}
```

lookup は:

```text
単純検索
候補抽出
軽量 identity handle 取得
```

を担当する。

---

## resolver

責務:

```text
identity resolve
staged fallback
ambiguity handling
lookup orchestration
```

返却:

```python
SubscriberIdentityLookupResult(
    status="matched",
    matched_by="identity_hash",
    rows=[...],
)
```

resolver は:

```text
複数 lookup を順番に試行し、
最終的な candidate 状態を整理する。
```

---

## hydrate

責務:

```text
subscriber_id
↓
current snapshot data load
```

hydrate は:

```text
address
contact
current snapshot
```

などの実データ取得を担当する。

---

# Current Files

## subscriber.py

subscriber lookup 基盤。

現在の役割:

```text
- identity_hash lookup
- lightweight subscriber row retrieval
- ambiguity handling
```

特徴:

```text
- lightweight columns を返す
- hydrate data は返さない
- SubscriberAmbiguousError を持つ
```

現在の設計は:

```text
lookup layer の土台
```

として扱う。

既存挙動は崩さない。

---

## fund.py

fund 系 lookup helper。

---

## hia_company.py

HIA company 系 lookup helper。

---

## subscriber_projection.py

subscriber projection helper。

現在の役割:

```text
- subscribers.id リストを入力にする
- 用途別に必要な subscribers カラムだけ SELECT する
- current snapshot / hydrate / compare に渡しやすい軽量 row を返す
```

特徴:

```text
- 検索は行わない
- identity resolve は行わない
- ambiguity handling は行わない
- 渡された subscribers.id のみを対象にする
- 表示・snapshot・compare 用の projection として扱う
```

現在実装済み:

```text
- load_subscriber_rows_for_hia_current_snapshot
- load_current_address_rows_for_hia_current_snapshot
- load_current_contact_rows_for_hia_current_snapshot
```

現在の返却イメージ:

subscriber rows:

```python
[
    {
        "subscriber_id": 123,
        "hia_subscriber_id": "...",
        "identity_hash": "...",
        "person_id_custom": "...",
        "name_kana_full_match": "...",
    }
]
```

current address rows:

```python
[
    {
        "subscriber_id": 123,
        "current_address_id": 456,
    }
]
```

current contact rows:

```python
[
    {
        "subscriber_id": 123,
        "current_contact_id": 789,
    }
]
```

current snapshot では:

```text
resolver
  ↓
subscribers.id list
  ↓
subscriber_projection
  ├─ subscribers current lightweight row
  ├─ current address row
  └─ current contact row
```

の流れで staging_subscribers_hub.current_* 更新用データを取得する。

address / contact の current 判定は:

```text
subscriber_addresses.is_current = 1
subscriber_contacts.is_current = 1
```

を使用する。

この current 制御は:

```text
apply_subscribers_from_staging_hub.py
```

を一次情報として扱う。

---

# Planned Files

## subscriber_identity.py

subscriber identity resolver。

責務:

```text
- HIA subscriber ID lookup
- identity_hash lookup
- person_id_custom lookup
- lightweight identity handle retrieval
- candidate handling
- multiple match handling
- staged identity resolve
```

方針:

```text
既存 subscriber.py の挙動は崩さず、
resolver layer を追加する。
```

lookup priority:

```text
1. hia_subscriber_id
2. identity_hash
3. person_id_custom
```

現在の返却形式:

```python
SubscriberIdentityLookupResult(
    status="matched",
    matched_by="identity_hash",
    rows=[...],
)
```

lightweight identity handle:

```python
{
    "subscriber_id": 123,
    "hia_subscriber_id": "...",
    "identity_hash": "...",
    "person_id_custom": "...",
    "name_kana_full_match": "...",
}
```

特徴:

```text
- 重い hydrate data を返さない
- lookup と hydrate を分離する
- candidate rows を保持する
- multiple_match を表現可能
- 後続 compare / hydrate に使いやすい
```

現在実装済み:

```text
- list_identity_handles_by_hia_subscriber_id
- list_identity_handles_by_identity_hash
- list_identity_handles_by_person_id_custom
- resolve_subscriber_identity
```

今後追加予定:

```text
- insurance lookup
- kana lookup
- staged fallback lookup
- ambiguity review helper
```

---

# Design Constraints

重要:

```text
現在の lookup 実装の既存挙動は崩さない
```

特に:

```text
subscriber.py
```

は既存利用箇所がある前提で、
新しい resolver / hydrate layer は追加方式で拡張する。

既存関数の:

```text
返却形式
例外仕様
検索条件
```

を破壊的変更しない。
