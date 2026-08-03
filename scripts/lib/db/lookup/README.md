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
DBから特定キーでよく使う値を取得する
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

lookup layer は、用途に応じて以下を返す。

```text
- lightweight identity handle
- current snapshot values
- compare / export / apply でよく使うDB値
```

ただし、業務判断・apply action decision・ETL orchestration は行わない。

つまり本ディレクトリの責務は:

```text
DBから値を取得する
取得単位を共通化する
呼び出し側が扱いやすい形に整える
```

までとする。

## Postal Code Address Lookup

`postal_code_address.py` は `phr_master.postal_code_addresses` から郵便番号に対応する住所候補を返す。
入力はハイフンあり・なしを受け、内部では7桁数字に正規化する。

主API:

```python
lookup_postal_code_address(cur, "100-0001")
lookup_postal_code_address_for_xml(cur, "1000001")
```

返却する `PostalAddressLookupResult` は、候補一覧、候補数、XML補完用の代表住所、理由を持つ。
複数候補時に代表住所が返る場合でも、lookup層は業務採用可否を判断しない。
呼び出し側は `reason` と `candidate_count` を見て、画面確認に回すか、HIA提出優先で市区町村までを採用するかを決める。

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
current snapshot values
well-known DB values
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

補足:

```text
hydrate は、IDやキーだけでは足りない後続処理のために、
関連する実データを読み込んで usable な形にすることを指す。
```

hydrate は:

```text
address
contact
current snapshot
```

現在実装:

- subscriber_addresses.py
- subscriber_contact_points.py

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

## subscriber_addresses.py

current address lookup helper。

役割:

- subscribers.id list を入力にする
- current address を取得する
- compare / current snapshot 用に利用する

返却例:

```python
{
    "subscriber_id": 123,
    "postal_code": "1000001",
    "address_line": "東京都...",
    "building": "サンプルビル101",
}
```

特徴:

```text
- identity resolve は行わない
- 検索は行わない
- current address hydrate のみ担当する
```

## subscriber_contact_points.py

current contact point lookup helper。

役割:

- subscribers.id list を入力にする
- current contact point を取得する
- compare / current snapshot 用に利用する

返却例:

```python
{
    "subscriber_id": 123,
    "phone": "090xxxx",
    "email": "sample@example.com",
}
```

特徴:

```text
- identity resolve は行わない
- 検索は行わない
- current contact hydrate のみ担当する
- 将来 contact_type 追加に対応可能
```

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
