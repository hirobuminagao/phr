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
current_lookup_status確認
  ↓
compare hash確認
  ↓
必要な行のみ詳細compare
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

# 5. identity_hash / compare hash Policy

## identity_hash の位置づけ

`identity_hash` は:

```text
同一 subscriber を resolve / join するための検索用キー
```

として扱う。

つまり `identity_hash` は:

```text
人を見つけるための値
```

であり、登録値差分を直接判定するための compare hash ではない。

`identity_hash` の主な利用箇所:

```text
- import phase の current snapshot update
- subscriber identity resolver
- compare / apply phase における current_subscriber_id の前提確認
```

---

## compare hash の位置づけ

compare / prepare phase では、詳細比較対象を絞るために compare hash を利用する。

compare hash の目的は:

```text
full compare を完全に無くすこと
```

ではなく:

```text
full compare が必要な候補を高速に絞ること
```

である。

compare hash が一致する場合:

```text
該当ブロックは詳細compare不要候補
```

compare hash が不一致の場合:

```text
該当ブロックは詳細compare対象
```

として扱う。

compare hash は `scripts/lib/hash/compare_hash.py` の `build_compare_hash()` で生成する。

固定手順:

```text
1. values list を受け取る
2. 各値を base_norm に通す
3. delimiter で連結する
4. sha256 を生成する
5. hex digest を返す
```

重要:

```text
compare hash は match 値を前提にしない
```

標準用途では、DB格納用の norm 値を渡す。
match 値を hash 化したい場合は、呼び出し側で match 値を生成してから渡す。

---

## compare_identity_norm_hash

`compare_identity_norm_hash` は:

```text
identity登録値の差分検知用hash
```

として扱う。

対象値:

```text
insurance_symbol
insurance_number
name_kana_full
name_kanji_full
birth
gender_code
```

対象外:

```text
insurance_branchnumber
```

枝番を対象外にする理由:

```text
insurance_branchnumber は健保・運用側が独自に採番する補助番号であり、
本人/扶養/続柄/任意継続等の管理ルールが健保ごとに揺れるため、
identity登録値差分の主軸として管理しない。
```

本人/扶養/任意継続などの属性は、枝番ではなく:

```text
relationship_name
insured_attribute_name
```

などの明示項目で管理する。

保存先:

```text
staging_subscribers_hub.compare_identity_norm_hash
subscribers.compare_identity_norm_hash
```

---

## compare_other_hash

`compare_other_hash` は:

```text
identity以外の subscribers 本体属性差分検知用hash
```

として扱う。

対象候補:

```text
insured_attribute_name
relationship_name
qualification_acquired_date
qualification_lost_date
employer_code
department_code
distribution_code
employee_code
connect_id
```

保存先:

```text
staging_subscribers_hub.compare_other_hash
subscribers.compare_other_hash
```

---

## address_hash

`address_hash` は:

```text
住所値の存在確認・差分検知用hash
```

として扱う。

対象値:

```text
postal_code
address_line
building
```

保存先:

```text
staging_subscribers_hub.address_hash
subscriber_addresses.address_hash
```

注意:

```text
address_hash 一致 = current address 一致
```

ではない。

`subscriber_addresses` は subscriber に対して 1:n の履歴型テーブルであり、
同一住所値が過去行として存在する可能性がある。

そのため address compare では:

```text
same address_hash exists?
  yes:
    is_current = 1?
      yes -> noop
      no  -> current切替候補
  no:
    新住所insert + current切替候補
```

として扱う。

---

# 6. Address Compare

住所は単純な current row 比較だけでなく、subscriber に紐づく住所履歴全体を対象に確認する。

理由:

```text
subscriber_addresses は 1:n の履歴型テーブルであり、
is_current = 1 は「現在採用している住所行」を示す。
```

`is_current = 1` は:

```text
current active address row
```

`is_current = 0` は:

```text
historical address row
```

として扱う。

compare 対象値:

```text
postal_code
address_line
building
```

compare では staging の `address_hash` を使い、既存 `subscriber_addresses.address_hash` と照合する。

判定:

| status | 条件 | 意味 |
|---|---|---|
| `noop` | 同一 address_hash の行が存在し、かつ `is_current = 1` | 現在住所と一致 |
| `switch_current` | 同一 address_hash の行が存在するが、`is_current = 0` | 既存住所へ current 切替候補 |
| `insert` | 同一 address_hash の行が存在しない | 新住所 insert 候補 |
| `review` | 同一 hash が複数 current 等、判定不能 | 自動apply不可 |

注意:

旧spec上で `address1` / `address2` / `address3` と表現している箇所がある場合、
本DDLでは `address_line` / `building` 等へ読み替える。

---

# 7. Contact Compare

連絡先は現行 `subscriber_contacts` の phone + email セット構造では、
compare hash による差分判定を一旦行わない。

理由:

```text
phone と email が同一レコードに同居しているため、
phoneのみ変更 / emailのみ変更 / 複数連絡先 / null時の current解除
を安全に表現しづらい。
```

今後、連絡先は以下のような contact point 型へ再設計する方針とする。

```text
subscriber_id
contact_type
contact_value
is_current
valid_from
valid_to
source
```

`contact_type` の初期候補:

```text
phone
email
```

staging_subscribers_hub からの apply は、新 contact 形式を前提に実装する。

null の扱い:

```text
HIA CSV の phone が null
  -> subscriber_id に紐づく phone current を全て current から外す

HIA CSV の email が null
  -> subscriber_id に紐づく email current を全て current から外す
```

つまり null は:

```text
何もしない
```

ではなく:

```text
HIA正本上、現在値なし
```

として扱う。

現行 `subscriber_contacts` は current snapshot 取得の暫定用途に留め、
compare / apply の本実装では新 contact 形式へ移行する。

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

```text
- compare_identity_norm_hash mismatch
- compare_other_hash mismatch
- address_diff_status IN ('switch_current', 'insert')
- contact新形式での差分あり
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
compare_identity_norm_hash一致
AND
compare_other_hash一致
AND
address_diff_status = 'noop'
AND
contact新形式での差分なし
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
compare_identity_norm_hash
compare_other_hash
address_hash
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

```md
- compare_identity_norm_hash change
- compare_other_hash change
- address current switch / insert
- contact新形式での current change
- qualification change
- employer / department change
```

audit は apply phase 側で永続保存する。

---

# Summary

compare の前提には:

```text
current_subscriber_id
current_lookup_status
identity_hash
```

を利用する。

ただし `identity_hash` は人を resolve / join するための検索用キーであり、
登録値差分検知の中心にはしない。

差分候補の絞り込みには:

```text
compare_identity_norm_hash
compare_other_hash
address_hash
```

を利用する。

`compare_identity_norm_hash` は norm値ベースで生成し、
小書き文字や漢字表記など、match値では吸収される登録値差分も検知できるようにする。

住所は `address_hash` により同一住所値の存在を確認した上で、
`is_current` を見て noop / current切替 / insert を判断する。

連絡先は現行 `subscriber_contacts` の hash比較を行わず、
新 contact 形式を前提に compare / apply を設計する。