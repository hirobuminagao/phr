# HIA Export Subscribers CSV – Compare / Prepare Phase

このドキュメントは、
`staging_subscribers_hub` に保持された import 値と current snapshot を比較し、
apply_action を生成する compare / prepare phase の仕様を定義する。

関連ADR:

- ADR-0021 HIA加入者 import / compare / apply フロー再設計

---

# 1. Purpose

compare / prepare phase の目的は:

```text
import 済み staging データと current snapshot を比較し、
apply_action を確定すること
```

である。

旧実装では apply phase 内で compare と apply を同時に実施していた。

ADR-0021 以降は:

```text
import
  ↓
current snapshot update
  ↓
prepare / compare
  ↓
apply
```

へ責務分離する。

---

# 2. Input Tables

compare / prepare phase は、import phase により staging 側へ保持された current snapshot を主な入力として使用する。

## staging

```text
staging_subscribers_hub
```

staging には以下が保持されている前提とする。

```text
import値
current_subscriber_id
current_identity_hash
current_name_kana_full_match
current_address_id
current_contact_id
current_lookup_status
current_lookup_checked_at
```

## current snapshot

```text
current_subscriber_id
current_identity_hash
current_name_kana_full_match
current_address_id
current_contact_id
current_lookup_status
```

current snapshot は import phase の後続処理で取得済みとする。

prepare / compare phase は、原則として current snapshot と staging import 値を比較する。

---

# 3. Compare Flow

```text
staging_subscribers_hub
  ↓
current snapshot確認
  ↓
identity_hash compare
  ↓
address compare
  ↓
contact compare
  ↓
apply_action decision
```

---

# 4. Current Snapshot / HIA Subscriber ID

HIA subscriber ID は、import phase の current snapshot update において、同一 subscriber を追跡する最優先外部IDとして利用する。

prepare / compare phase では、その結果として staging に保持された:

```text
current_subscriber_id
current_lookup_status
```

を利用する。

## current subscriber found

```text
current_subscriber_id IS NOT NULL
```

の場合:

```text
既存 subscriber あり
```

として compare を継続する。

---

## current subscriber not found

current snapshot update の結果、既存 subscriber が存在しない場合:

```text
current_subscriber_id IS NULL
current_lookup_status = not_found
```

として扱う。

この場合、compare phase では insert 候補として扱う。

---

# 5. identity_hash Compare

compare phase では、staging import値の:

```text
identity_hash
```

と current snapshot の:

```text
current_identity_hash
```

を比較する。

---

## identity_hash same / current match

```text
identity_hash = current_identity_hash
```

の場合:

```text
同一 subscriber
```

として扱う。

この場合は:

- address compare
- contact compare
- qualification compare
- employer / department compare

へ進む。

---

## identity_hash changed against current snapshot

```text
current_subscriber_id IS NOT NULL
AND identity_hash <> current_identity_hash
```

の場合:

```text
HIA 最新状態への更新候補
```

として扱う。

ただし差分内容を確認する。

### name_kana_full_match changed

```text
name_kana_full_match changed
```

の場合:

```text
既存 name parts を clear
```

する。

理由:

```text
旧 parts が新 full name と不整合になる可能性が高いため
```

parts clear 後、後続 normalize / split により再生成する。

---

## insurance_symbol / insurance_number changed only

記号・番号のみ変更の場合:

```text
parts は維持
```

する。

---

# 6. Address Compare

比較対象:

```text
subscriber_addresses current row
```

compare 対象:

- postal_code
- address_line
- building
- prefecture
- city
- prefecture_code

注意:

旧spec上で `address1` / `address2` / `address3` と表現している箇所がある場合、
本DDLでは `address_line` / `building` / `prefecture` / `city` 等へ読み替える。

address compare の結果は `address_diff_status` に保持する。

想定値:

```text
noop
changed
insert
```

意味:

| status | 意味 |
|---|---|
| `noop` | current address が存在し、差分なし |
| `changed` | current address が存在し、差分あり |
| `insert` | current address が存在しない |

---

# 7. Contact Compare

比較対象:

```text
subscriber_contacts current row
```

compare 対象:

- phone
- email

注意:

現DDLの `subscriber_contacts` には `mobile` 列は存在しないため、
連絡先比較では `phone` / `email` のみを対象とする。

contact compare の結果は `contact_diff_status` に保持する。

想定値:

```text
noop
changed
insert
```

意味:

| status | 意味 |
|---|---|
| `noop` | current contact が存在し、差分なし |
| `changed` | current contact が存在し、差分あり |
| `insert` | current contact が存在しない |

---

# 8. Apply Action Decision

compare 結果から apply_action を決定する。

## insert

条件:

```text
current_subscriber_id IS NULL
AND
current_lookup_status = 'not_found'
```

結果:

```text
apply_action = insert
```

---

## update

条件:

```text
existing subscriber found
AND
いずれかの差分あり
```

差分対象:

- subscribers 本体差分
- identity_hash <> current_identity_hash
- address_diff_status IN ('changed', 'insert')
- contact_diff_status IN ('changed', 'insert')
```

結果:

```text
apply_action = update
```

---

## noop

条件:

```text
current_subscriber_id IS NOT NULL
AND
subscribers 本体差分なし
AND
identity_hash = current_identity_hash
AND
address_diff_status = 'noop'
AND
contact_diff_status = 'noop'
```

結果:

```text
apply_action = noop
```

---

## review

条件例:

- current_lookup_status = 'multiple_match'
- compare ambiguity
- invalid normalize

結果:

```text
apply_action = review
```

review 行は apply phase で自動更新しない。

---

# 9. Compare Result Storage

compare / prepare phase の結果は staging 側へ保持する。

想定カラム:

```text
current_subscriber_id
current_identity_hash
current_name_kana_full_match
current_address_id
current_contact_id
current_lookup_status
apply_action
apply_diff_columns
identity_match_status
address_diff_status
contact_diff_status
apply_checked_at
```

---

# 10. Apply Phase Relationship

apply phase は:

```text
判定済み apply_action を実行するだけ
```

とする。

apply phase 自身は compare 判定を行わない。

例:

```text
apply_action = insert
  → insert

apply_action = update
  → update

apply_action = noop
  → skip

apply_action = review
  → skip
```

---

# 11. Audit Policy

compare / prepare phase では、staging import値と current snapshot の差分から audit 用差分情報を生成する。

対象:

- identity_hash change
- address change
- contact change
- qualification change
- employer / department change

audit は apply phase 側で永続保存する。

---

# Summary

compare / prepare phase は:

```text
staging_subscribers_hub
```

に保持された:

```text
current snapshot
```

と import値を比較し、

```text
apply_action
compare status
diff columns
```

を staging 側へ保持する。

compare の中心には:

```text
HIA subscriber ID
identity_hash
```

を利用する。

identity_hash changed の場合でも、
HIA 側を最新正本として subscribers へ反映する。

ただし:

```text
name_kana_full_match changed
```

の場合は、既存 name parts を clear し、
後続 normalize / split により再生成する。