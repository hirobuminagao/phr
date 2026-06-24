# HIA Export Subscribers CSV – Compare / Prepare Phase

このドキュメントは、
`staging_subscribers_hub` に保持された import 値と current snapshot を比較し、
apply_action を生成する apply orchestration 内の prepare / compare 処理仕様を定義する。

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
import orchestration
  ↓
current snapshot update
  ↓
apply orchestration
  ├ prepare / compare
  ├ apply
  └ audit
```

へ責務分離する。

---

# 2. Input Tables

prepare / compare は、import orchestration により staging 側へ保持された current snapshot を主な入力として使用する。

## staging

```text
staging_subscribers_hub
```

staging には以下が保持されている前提とする。

```text
import値
current_subscriber_id
current_hia_subscriber_id
current_identity_hash
current_compare_identity_norm_hash
current_compare_other_hash
current_name_kana_full_match
current_address_id
current_address_hash
current_phone_contact_point_id
current_email_contact_point_id
current_lookup_status
current_lookup_checked_at
```

## current snapshot

```text
current_subscriber_id
current_hia_subscriber_id
current_identity_hash
current_compare_identity_norm_hash
current_compare_other_hash
current_name_kana_full_match
current_address_id
current_address_hash
current_phone_contact_point_id
current_email_contact_point_id
current_lookup_status
```

current snapshot は import orchestration の current snapshot update で取得済みとする。

prepare / compare は、原則として current snapshot と staging import 値を比較する。

---

# 3. Compare Flow

```text
staging_subscribers_hub
  ↓
current snapshot確認
  ↓
current_lookup_status確認
  ↓
compare hash candidate filtering
  ↓
必要な行のみ detailed compare
  ↓
apply_action decision
```

---

# 4. Current Snapshot / HIA Subscriber ID

HIA subscriber ID は、import orchestration の current snapshot update において、同一 subscriber を追跡する最優先外部IDとして利用する。

prepare / compare phase では、その結果として staging に保持された:

```text
current_subscriber_id
current_lookup_status
```

加えて:

```text
current_hia_subscriber_id
```

も review 時の重要な確認材料として保持する。

例:

```text
hia_subscriber_id != current_hia_subscriber_id
```

の場合:

```text
- HIA側ID変更
- 上流ID差し替え
- 別人候補
```

などを review 対象として確認する。

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

この場合、prepare / compare では insert 候補として扱う。

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

実装上の補足:

```text
prepare phase では identity_hash mismatch も apply_diff_columns に記録する。

ただし identity_hash は詳細な登録値差分の説明用ではなく、
同一 subscriber の前提確認・追跡用の差分として扱う。
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
該当ブロックは detailed compare 不要候補
```

compare hash が不一致の場合:

```text
該当ブロックは detailed compare 対象
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
address_hash 一致 = 同一住所値の存在
```

であり、current address 一致とは限らない。

`subscriber_addresses` は subscriber に対して 1:n の履歴型テーブルであり、
同一住所値が過去行として存在する可能性がある。

そのため address compare では:

```text
same address_hash exists?
  yes:
    is_current = 1?
      yes -> noop
      no  -> switch_current candidate
  no:
    insert candidate
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

## Address Null / Empty Policy

`postal_code` / `address_line` / `building` が全て空の場合は、住所削除ではなく住所未提供として扱う。

- current address なし + staging address なし → noop
- current address なし + staging address あり → insert
- current address あり + staging address なし → noop
- current address あり + current一致 → noop
- current address あり + history一致 → switch_current
- current address あり + 未登録住所 → insert
- 判定不能 → review

current address あり + staging address なしの場合、current解除しない。

判定:

| status | 条件 | 意味 |
|---|---|---|
| `noop` | 同一 address_hash の行が存在し、かつ `is_current = 1` | 現在住所と一致 |
| `switch_current` | 同一 address_hash の行が存在するが、`is_current = 0` | 既存住所へ current 切替候補 |
| `insert` | staging address_hash が存在し、同一 address_hash の行が存在しない | 新住所 insert 候補 |
| `review` | 同一 hash が複数 current 等、判定不能 | 自動apply不可 |

## Compare Implementation Policy

compare は current row のみを比較対象にしてはならない。

判定順序:

1. staging address が存在するか
2. current address と一致するか
3. `subscriber_addresses` 履歴に同一 `address_hash` が存在するか
4. 存在する場合 switch_current
5. 存在しない場合 insert

`current_address_id IS NULL` のみを理由に insert 判定してはならない。

注意:

旧spec上で `address1` / `address2` / `address3` と表現している箇所がある場合、
本DDLでは `address_line` / `building` 等へ読み替える。

---

# 7. Contact Compare

連絡先は Hub apply では `subscriber_contact_points` を正本構造として扱う。

contact point は compare hash による差分判定を行わない。

理由:

- staging に current連絡先値や履歴情報を持ちすぎない
- contact point は履歴型テーブルであり switch_current 判定が必要
- current値比較と履歴検索を分離したい
- current snapshot 時点の状態を比較基準として固定したい

そのため contact point は compare hash ではなく、
current_phone_contact_point_id / current_email_contact_point_id を起点に current値を取得して比較する。

current snapshot update で staging に保持した current contact point id を比較起点とする。


Hub apply では以下の contact point 型を前提に compare / apply を行う。

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

`subscriber_contacts` は legacy / backfill source / temporary reference として扱う。

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

現行 `subscriber_contacts` は legacy / backfill source / temporary reference として扱う。

compare / apply の本実装では `subscriber_contact_points` を利用する。

## Contact Compare Procedure

contact point compare は以下の順で行う。

```text
1. current snapshot の current_phone_contact_point_id / current_email_contact_point_id を起点に current 値を取得する
2. staging の phone / email と current 値を contact_type ごとに比較する
3. current 値と同じ場合は noop
4. staging 値が NULL で current が存在する場合は clear_current
5. staging 値が current と異なる場合のみ、同じ contact_value の history 行を検索する
6. history 行が存在する場合は switch_current
7. history 行が存在しない場合は insert
8. current 複数等で判定不能な場合は review
```

補足:

```text
current比較の起点は current snapshot の contact point id とする。

history検索は、current 値との差分がある場合にのみ行う。

これにより、compare / apply の基準となる current 値を snapshot 時点に固定しつつ、過去連絡先への switch_current も可能にする。

## Contact Compare Result Columns

contact point は集約ステータスと contact_type 別ステータスを分けて保持する。

```text
contact_point_diff_status
  contact point 全体の集約ステータス

phone_diff_status
phone_target_contact_point_id
  phone の apply 処理種別と対象 contact_point_id

email_diff_status
email_target_contact_point_id
  email の apply 処理種別と対象 contact_point_id
```

`contact_point_diff_status` は、contact point 系の処理が必要かを判断するための集約フラグとして扱う。

```text
noop
  phone / email ともに変更なし

changed
  phone / email のいずれかに apply 対象あり

review
  phone / email のいずれかが自動判定不能
```

実際の apply 内容は `phone_diff_status` / `email_diff_status` を参照する。

```text
noop
  更新なし
  target_contact_point_id は current contact point id または NULL

insert
  新規 contact point を current として追加
  target_contact_point_id は NULL

switch_current
  既存 history row を current に戻す
  target_contact_point_id は history contact_point_id

clear_current
  current row を history 化する
  target_contact_point_id は current contact_point_id

review
  自動更新しない
  target_contact_point_id は NULL
```

compare phase は phone / email それぞれについて、上記 status と target id を staging に記録する。

apply phase は `phone_diff_status` / `email_diff_status` と target id に従って処理し、contact point の current 判定を再実行しない。
```

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
- contact point での差分あり
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
contact point での差分なし
```

結果:

```text
apply_action = noop
```

---

## review

条件例:

- current_lookup_status = 'multiple_match'
- current_lookup_status = 'review'
- current_lookup_status = 'projection_error'
- hia_subscriber_id != current_hia_subscriber_id
- compare ambiguity
- invalid normalize

結果:

```text
apply_action = review
```

review 行は apply phase で自動更新しない。

---

# 8.5. identity_match_status

prepare phase では、current lookup / identity確認結果を `identity_match_status` として staging に保持する。

実装上の主な値:

| value | 意味 |
|---|---|
| `multiple_match` | current lookup が複数候補になった |
| `review` | current lookup が review 扱いになった |
| `projection_error` | current projection 取得・構築に失敗した |
| `not_found` | current subscriber が存在しない |
| `hia_subscriber_id_mismatch` | HIA加入者ID が current と一致しない |
| `identity_hash_matched` | identity_hash が一致した |
| `identity_hash_mismatch` | identity_hash が一致しない |

補足:

```text
identity_match_status は apply_action そのものではない。

apply_action は current_lookup_status、HIA加入者ID一致、compare hash、address/contact compare の結果から決定する。
```

---

# 9. Compare Result Storage

compare / prepare phase の結果は staging 側へ保持する。

想定カラム:

```text
current_subscriber_id
current_hia_subscriber_id
current_identity_hash
current_compare_identity_norm_hash
current_compare_other_hash
current_name_kana_full_match
current_address_id
current_address_hash
current_phone_contact_point_id
current_email_contact_point_id
current_lookup_status
compare_identity_norm_hash
compare_other_hash
address_hash
apply_action
apply_diff_columns
identity_match_status
address_diff_status
contact_point_diff_status
phone_diff_status
phone_target_contact_point_id
email_diff_status
email_target_contact_point_id
apply_checked_at
```

contact point compare では、`current_phone_contact_point_id` / `current_email_contact_point_id` を current 値取得の起点として使用する。

contact point compare 結果として、以下も staging に保持する。

```text
phone_diff_status
phone_target_contact_point_id
email_diff_status
email_target_contact_point_id
```

`contact_point_diff_status` は集約ステータスとして残し、実際の phone / email apply は contact_type 別の status / target id を利用する。

---

# 10. Apply Phase Relationship

apply orchestration 内の apply は:

```text
判定済み apply_action を実行するだけ
```

とする。

apply 本体自身は compare 判定を行わない。

例:

```text
apply_action = insert
  → subscriber root / address / contact point を insert
  → processed mark

apply_action = update
  → subscriber root / address / contact point を必要に応じて update
  → processed mark

apply_action = noop
  → subscriber root / address / contact point は更新しない
  → processed mark

apply_action = review
  → 自動更新しない
  → processed mark しない
```

---

# 11. Audit Policy

compare / prepare phase では、audit 対象候補となる差分情報を staging に保持する。

実際の audit row 生成・永続化は apply phase 側で行う。

対象:

```md
- compare_identity_norm_hash change
- compare_other_hash change
- qualification change
- employer / department change
```

address / contact point は履歴型テーブルで current / history を保持する。

現時点では address / contact point 専用 audit テーブルは持たない。

ただし address current switch / insert、および contact point current change / insert / clear_current / switch_current は `subscribers_audit` に記帳する。

audit は apply phase 側で永続保存する。

apply phase 側では、更新対象 field の old_value / new_value を比較し、同値の場合は audit row を作成しない。

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

contact point は compare hash を持たず、current snapshot の contact point id を起点に contact_type ごとに比較する。

住所は `address_hash` により同一住所値の存在を確認した上で、
`is_current` を見て noop / current切替 / insert を判断する。

連絡先は `subscriber_contact_points` を正本構造として扱い、
current snapshot の contact point id から current 値を取得して比較する。
現行 `subscriber_contacts` は legacy / backfill source / temporary reference として扱う。