

# scripts/lib/db/lookup

DB lookup 共通lib。

目的:

```text
「どう検索するか」
「どのキーで候補を引くか」
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

ではなく、後続 lookup / hydrate / compare に使いやすい
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

lookup layer は:

```text
住所
電話
メール
業務詳細データ
```

などの重い hydrate data は返さない。

---

# lookup と hydrate の分離

本ディレクトリでは:

```text
lookup
```

と

```text
hydrate
```

を分離する。

---

## lookup

責務:

```text
candidate search
identity resolve
ambiguity handling
```

返却:

```text
lightweight identity handle
```

例:

```python
{
    "status": "identity_hash_matched",
    "matched_by": "identity_hash",
    "rows": [...],
}
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

# Planned Files

## subscriber_identity.py

subscriber identity resolver（予定）。

責務:

```text
- HIA subscriber ID lookup
- identity_hash lookup
- person_id_custom lookup
- insurance lookup
- kana lookup
- candidate handling
- multiple match handling
```

方針:

```text
既存 subscriber.py の挙動は崩さず、
resolver layer を追加する。
```

返却イメージ:

```python
{
    "status": "identity_hash_matched",
    "matched_by": "identity_hash",
    "rows": [
        {
            "subscriber_id": 123,
            "identity_hash": "...",
            "person_id_custom": "...",
            "hia_subscriber_id": "...",
        }
    ],
}
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