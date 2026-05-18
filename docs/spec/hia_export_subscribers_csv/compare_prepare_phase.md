# HIA Export Subscribers CSV – Compare / Prepare Phase

このドキュメントは、
`staging_subscribers_hub` と本番 subscriber 系テーブルを比較し、
apply_action を生成する compare / prepare phase の仕様を定義する。

関連ADR:

- ADR-0021 HIA加入者 import / compare / apply フロー再設計

---

# 1. Purpose

compare / prepare phase の目的は:

```text
import 済み staging データを、本番 subscribers 系と比較し、
apply_action を確定すること
```

である。

旧実装では apply phase 内で compare と apply を同時に実施していた。

ADR-0021 以降は:

```text
import
  ↓
prepare / compare
  ↓
apply
```

へ責務分離する。

---

# 2. Input Tables

compare / prepare phase は以下を入力として使用する。

## staging

```text
staging_subscribers_hub
```

## current subscriber

```text
subscribers
```

## current address

```text
subscriber_addresses
```

current 行のみ比較対象とする。

## current contact

```text
subscriber_contacts
```

current 行のみ比較対象とする。

---

# 3. Compare Flow

```text
staging_subscribers_hub
  ↓
HIA subscriber ID compare
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

# 4. HIA Subscriber ID Compare

HIA subscriber ID は:

```text
同一 subscriber を追跡する最優先外部ID
```

として扱う。

## HIA subscriber ID match

```text
staging.hia_subscriber_id
=
subscribers.hia_subscriber_id
```

の場合:

```text
同一 HIA subscriber
```

として compare を継続する。

identity_hash が変更されていても、
原則として HIA 最新状態への更新候補とする。

---

## HIA subscriber ID not found

HIA subscriber ID に一致する subscriber が存在しない場合:

```text
identity_hash compare
```

へ進む。

---

# 5. identity_hash Compare

compare phase では:

```text
identity_hash
```

を subscriber identity compare の中心に利用する。

identity_hash 入力:

```text
person_id_custom
name_kana_full_match
gender_code
```

---

## identity_hash same

```text
identity_hash same
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

## identity_hash changed

```text
identity_hash changed
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

## identity_hash not found

identity_hash 一致 subscriber が存在しない場合:

```text
insert candidate
```

として扱う。

---

## identity_hash multiple match

同じ identity_hash を持つ subscriber が複数存在する場合:

```text
review
```

として扱う。

自動 apply は行わない。

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

差分がある場合:

```text
address_diff_status = changed
```

とする。

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

差分がある場合:

```text
contact_diff_status = changed
```

とする。

---

# 8. Apply Action Decision

compare 結果から apply_action を決定する。

## insert

条件:

```text
HIA subscriber ID not found
AND
identity_hash not found
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
差分あり
```

結果:

```text
apply_action = update
```

---

## noop

条件:

```text
existing subscriber found
AND
差分なし
```

結果:

```text
apply_action = noop
```

---

## review

条件例:

- identity_hash multiple match
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
matched_subscriber_id
apply_subscriber_id
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

compare / prepare phase では audit 用差分情報を生成する。

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

と:

```text
subscribers
subscriber_addresses
subscriber_contacts
```

を比較し、

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