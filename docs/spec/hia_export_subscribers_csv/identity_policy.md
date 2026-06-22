# Subscriber Identity Policy

このドキュメントは **HIA export 加入者 CSV を PHR subscriber master に反映する際の同一人物判定ポリシー**を定義する。

対象処理:

```
import_subscribers_to_staging_hub.py
apply_hia_subscriber_sync.py
```

このポリシーは主に apply orchestration の prepare / compare で利用される。

旧実装では apply 内で subscriber identity 判定を実施していたが、
ADR-0021 以降は import orchestration が current snapshot を staging 側へ保持し、
prepare / compare が compare 状態を staging 側へ保持する。

このポリシーは、
`staging_subscribers_hub` の1行を既存 `subscribers` に結びつけ、
HIA 最新状態との比較・同期を行うための基準となる。

関連ADR:

- `0008-subscribers-match-columns-for-identity.md`
- `0010-subscriber-audit-implementation.md`

---

# 1. Purpose

subscriber identity は以下を目的とする。

- 同一加入者を安定して再識別する
- staging → subscribers apply 時の resolve / compare の基準とする
- HIA export CSV の表記ゆれに対して過度に脆くならない
- 住所や連絡先などの付随情報と、加入者本体の識別を分離する

---

# 2. Identity Core

subscriber identity の中核には以下を使用する。

```text
identity_hash
```

identity_hash は:

```text
person_id_custom
name_kana_full_match
gender_code
```

を元に生成される subscriber resolve / join 用 hash である。

旧実装では3列を直接比較していたが、
ADR-0021 以降は prepare / compare において `identity_hash` を subscriber resolve / join 用に利用する。

---

# 3. identity_hash Components

identity_hash は以下の3要素から構成される。

| column | role | reason |
|---|---|---|
| `person_id_custom` | 主識別子 | 保険者番号・記号・番号・生年月日を元に生成される加入者識別キー |
| `name_kana_full_match` | 氏名照合補助 | 正規化済みカナ照合キー |
| `gender_code` | 補助識別子 | 同名・近似データの誤結合抑止 |

identity_hash は:

- subscriber resolve
- subscriber join
- current snapshot lookup

に利用する。

identity_hash 自体を登録値差分検知の中心にはしない。

---

# 4. person_id_custom

`person_id_custom` は加入者識別用のカスタムIDである。

生成入力:

```
insurer_number
insurance_symbol
insurance_number
birth
```

生成処理:

```
generate_person_id_custom()
```

実装:

```
phr/lib/normalize/subscriber.py
```

注意:

- これは apply 時の主キーそのものではない
- subscriber master 内での「同一人物候補検索キー」として使用する
- person_id_custom 単独ではなく、`name_kana_full_match` と `gender_code` を併用する

ADR-0021 以降は、これらを combine した `identity_hash` を subscriber resolve / join 用 hash として扱う。

---

# 5. name_kana_full_match

`name_kana_full_match` は **氏名カナの正規化・空白吸収後の照合用文字列**を使用する。

正規化ルール:

- NFKC 正規化
- ひらがな → カタカナ
- 全角/半角空白の除去
- 連続空白の吸収
- match 用の full key を生成

生成関数:

```
normalize_name_fields()
```

実装:

```
phr/lib/normalize/subscriber.py
```

例:

| raw | normalized match key |
|---|---|
| `ﾅｶﾞｵ ﾋﾛﾌﾐ` | `ナガオヒロフミ` |
| `ながお　ひろふみ` | `ナガオヒロフミ` |
| `ナガオ ヒロフミ` | `ナガオヒロフミ` |

この列は表記ゆれ吸収後の照合キーであり、
見た目用の氏名表示列ではない。

---

# 6. gender_code

`gender_code` は補助識別子として使用する。

想定値:

```
1
2
9
NULL
```

基本方針:

- `1` と `2` は明確に区別する
- `9` は不明・未設定系として扱う
- SQL 上は `IS ?` を使い、`NULL` 同士も一致判定できるようにする

旧検索イメージ:

```sql
SELECT *
FROM subscribers
WHERE person_id_custom = ?
  AND name_kana_full_match = ?
  AND gender_code IS ?
LIMIT 1;
```

---

# 7. Compare Policy

旧実装では:

```text
person_id_custom
name_kana_full_match
gender_code
```

の3列一致を subscriber match としていた。

ADR-0021 以降は:

```text
HIA subscriber ID
↓
identity_hash
↓
current snapshot lookup
↓
compare hash candidate filtering
↓
detailed compare
↓
compare status
```

HIA subscriber ID は、同一 subscriber を追跡する最優先の外部IDとして扱う。

import orchestration は:

```text
current_hia_subscriber_id
```

を staging に保持し、review 時の重要な確認材料として利用する。

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

---

## compare hash policy

登録値差分検知には:

```text
compare_identity_norm_hash
compare_other_hash
```

を使用する。

identity_hash は resolve / join 用であり、登録値差分検知の中心にはしない。

### compare_identity_norm_hash

対象値:

```text
insurance_symbol
insurance_number
name_kana_full
name_kanji_full
birth
gender_code
```

目的:

```text
identity登録値差分検知
```

### insurance_branchnumber

`insurance_branchnumber` は compare_identity_norm_hash 対象外とする。

理由:

```text
枝番は健保・運用側が独自に採番する補助番号であり、
本人/扶養/続柄/任意継続等の管理ルールが健保ごとに揺れるため。
```

identity登録値差分の主軸として管理しない。

name parts 管理にも利用しない。

枝番変更のみでは:

- name parts をクリアしない
- name parts match をクリアしない

氏名変更判定の条件には使用しない。

### compare_other_hash

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

目的:

```text
subscriber属性差分検知
```

compare hash は:

```text
full compare を完全に無くすためではなく、
詳細compare候補を高速に絞るために利用する。
```

---

# 8. Non-Identity Fields

以下の項目は identity 判定には使用しない。

- `postal_code`
- `address_line`
- `building`
- `phone`
- `email`
- `qualification_acquired_date`
- `qualification_lost_date`
- `relationship_name`
- `employer_code`
- `department_code`
- `distribution_code`
- `employee_code`
- `connect_id`

理由:

- 履歴として変わり得る
- 事後補正・更新があり得る
- 本人識別ではなく属性情報だから

これらは identity resolve 判定ではなく、compare_other_hash / address compare / contact point compare の対象として扱う。

---

# 9. Apply Decision Relationship

旧実装では apply phase 内で compare と apply を同時に実施していた。

ADR-0021 以降は:

```text
import orchestration
  ↓
current snapshot update
  ↓
apply orchestration
  ├ prepare / compare
  ├ apply_action 作成
  ├ apply
  └ audit
```

へ分離する。

prepare / compare では:

```text
compare_identity_norm_hash
compare_other_hash
address_hash + is_current
contact point
qualification
employer/dept
```

などを比較し、staging 側へ compare 状態を保持する。

例:

```text
apply_action:
- insert
- update
- noop
- review
```

noop は subscriber 更新を行わないが processed mark 対象とする。

review は自動更新を行わず、processed mark もしない。
```

apply orchestration 内の apply は compare 結果をもとに insert / update / noop を実行する。

---

# 10. Error / Ambiguity Policy

本ポリシーでは、同一 key に対して複数 subscriber が存在する状態は想定しない。

想定外状態:

- 同じ `identity_hash` を持つ subscriber が複数存在
- `hia_subscriber_id != current_hia_subscriber_id`
- 正規化前入力の異常により person_id_custom が生成不能
- name_kana_full_match が空

これらは正常 apply 対象ではなく、
これらは正常 apply 対象ではない。

実装上は staging 側へ review / multiple_match / projection_error 等の状態を保持し、
自動 apply 対象から除外する。

即時エラー終了を意味するものではなく、運用確認対象として残す場合がある。

---

# 11. Design Notes

この identity policy は、SQLite 版で運用していた以下の照合思想を MySQL / PHR v1.0.1 に引き継ぐものである。

```
identity_hash
```

identity_hash を resolve / join 用へ限定し、
登録値差分検知を compare hash へ分離することで:

- 住所変更
- contact point 変更
- 続柄更新
- 企業属性変更

などの通常更新を安全に扱えるようにしている。

---

# Summary

PHR subscriber identity は:

```text
person_id_custom
name_kana_full_match
gender_code
```

を元に生成される:

```text
identity_hash
```

を subscriber resolve / join 用として利用する。

ADR-0021 以降は:

```text
import orchestration
  ↓
current snapshot update
  ↓
apply orchestration
```

へ責務分離し、compare 状態と diff 情報を staging 側へ保持する。

登録値差分検知には:

```text
compare_identity_norm_hash
compare_other_hash
```

を使用する。

住所は `address_hash + is_current`、連絡先は `subscriber_contact_points` を利用して compare / apply を行う。

`subscriber_contacts` は legacy / backfill source / temporary reference として扱う。