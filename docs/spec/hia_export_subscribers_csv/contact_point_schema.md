

# contact_point_schema.md

## Purpose

このドキュメントは、Hub apply orchestration における
`subscriber_contact_points` の設計方針を定義する。

現行 `subscriber_contacts` は:

```text
phone + email 同居型
```

であり、以下を安全に扱いにくい。

```text
- phoneのみ変更
- emailのみ変更
- null時の current解除
- 複数連絡先保持
- 過去連絡先への戻り
```

そのため Hub apply では:

```text
subscriber_contact_points
```

を連絡先の正本構造として扱う。

---

## Scope

対象:

```text
Hub apply orchestration
```

対象処理:

```text
scripts/hia/apply_hia_subscriber_sync.py
scripts/hia/script_lib/hub_subscriber_prepare.py
scripts/hia/script_lib/hub_subscriber_compare.py
scripts/hia/script_lib/hub_subscriber_apply.py
scripts/hia/script_lib/hub_subscriber_audit.py
```

対象外:

```text
staging_subscribers_fund
fund側 diff / apply
```

fund側は Hub apply 完走後に見直す。

---

# 1. Table Overview

新テーブル:

```text
subscriber_contact_points
```

役割:

```text
1レコード = 1連絡先
```

として保持する。

現行 `subscriber_contacts` は:

```text
subscriber_id
phone
email
is_current
```

であり、1行に phone / email が混在する。

新構造では:

```text
subscriber_id
contact_type
contact_value
```

へ分離する。

---

# 2. Expected Columns

| column | type | meaning |
|---|---|---|
| `contact_point_id` | BIGINT / INTEGER | contact point ID |
| `subscriber_id` | BIGINT / INTEGER | 対象 subscribers.id |
| `contact_type` | TEXT / VARCHAR | `phone` / `email` |
| `contact_value` | TEXT / VARCHAR | 連絡先値 |
| `is_current` | INTEGER / BOOLEAN | current flag |
| `valid_from` | DATETIME / TEXT | current開始日時 |
| `valid_to` | DATETIME / TEXT | current終了日時 |
| `source` | TEXT / VARCHAR | データ由来 |
| `created_at` | DATETIME / TEXT | 作成日時 |
| `updated_at` | DATETIME / TEXT | 更新日時 |

---

# 3. contact_type

初期対応:

```text
phone
email
```

将来的には:

```text
mobile
home_phone
work_phone
sub_email
emergency
LINE
```

などの拡張余地を残す。

---

# 4. current Semantics

contact point は履歴型テーブルとして扱う。

基本方針:

```text
current row は複数可
history row は複数保持
```

例:

```text
phone current
email current
```

は同時存在可能。

ただし:

```text
同一 contact_type の current は原則1件
```

を期待する。

つまり:

```text
phone current は原則1件
email current は原則1件
```

運用とする。

---

# 5. Null Semantics

HIA CSV における:

```text
phone = null
email = null
```

は:

```text
何もしない
```

ではない。

意味:

```text
HIA正本上、現在値なし
```

と扱う。

## phone null

```text
subscriber_id に紐づく
contact_type='phone'
AND is_current=1
```

を全て current 解除する。

## email null

```text
subscriber_id に紐づく
contact_type='email'
AND is_current=1
```

を全て current 解除する。

---

# 6. Compare Policy

現時点では:

```text
contact compare hash
```

は導入しない。

compare は:

```text
subscriber_id
contact_type
contact_value
is_current
```

を利用する。

## phone compare

```text
subscriber_id
+ contact_type='phone'
+ contact_value
```

で既存確認する。

判定:

| status | 条件 |
|---|---|
| `noop` | 同一値 current 存在 |
| `switch_current` | 同一値 history 存在 |
| `insert` | 同一値なし |
| `review` | current複数等 |

## email compare

email も同様。

---

# 7. Apply Policy

## same value current exists

```text
noop
```

何もしない。

## same value exists but history

```text
switch_current
```

```text
旧current
  is_current = 0

existing history row
  is_current = 1
```

## same value not exists

```text
insert
```

```text
旧current
  is_current = 0

新row insert
  is_current = 1
```

---

# 8. Legacy Backfill

旧テーブル:

```text
subscriber_contacts
```

から backfill を行う。

## backfill flow

```text
subscriber_contacts
  ↓
phone が空でなければ
subscriber_contact_points(contact_type='phone')
  ↓
email が空でなければ
subscriber_contact_points(contact_type='email')
```

旧1行は最大2行へ分解される。

```text
旧:
subscriber_id + phone + email + is_current

新:
subscriber_id + contact_type='phone' + contact_value + is_current
subscriber_id + contact_type='email' + contact_value + is_current
```

`subscriber_contacts` は:

```text
legacy / backfill source / temporary reference
```

として扱う。

---

# 9. Migration Order

実装順:

```text
1. subscriber_contact_points DDL
2. migration適用
3. backfill script 作成
4. subscriber_contacts → contact_points backfill
5. Hub current projection 差し替え
6. Hub compare/apply を contact point 前提へ変更
7. Hub apply orchestration 完走
8. 最後に fund側見直し
```

理由:

```text
Hub apply を先に完走させるため。
fund側を同時に触ると、contact再設計とdiff設計が並行して中途半端になるため。
```

---

# 10. Audit / History Policy

contact point は履歴型テーブルとして保持する。

履歴管理は:

```text
is_current
valid_from
valid_to
```

により行う。

例:

```text
旧電話番号
  is_current = 0
  valid_to = 更新日時

新電話番号
  is_current = 1
  valid_from = 更新日時
```

現時点では:

```text
subscriber_contact_point_audit
```

のような専用 audit テーブルは持たない。

また contact point の変更は:

```text
subscriber_audit
```

へ必須記帳とはしていない。

したがって現在の実装では:

```text
contact point
  → 履歴あり
  → auditなし
```

として扱う。

将来的に:

```text
subscriber_contact_point_audit
```

または

```text
subscriber_audit
```

への記帳を追加する場合は、本ドキュメントを更新する。

---

# Summary

Hub apply では:

```text
subscriber_contact_points
```

を連絡先の正本構造として扱う。

```text
1レコード = 1連絡先
```

とすることで:

```text
- phoneのみ変更
- emailのみ変更
- null current解除
- 複数連絡先保持
- 過去連絡先への戻り
```

を安全に扱えるようにする。

現行 `subscriber_contacts` は:

```text
legacy / backfill source / temporary reference
```

として扱う。