# Subscriber Identity Policy

このドキュメントは **HIA export 加入者 CSV を PHR subscriber master に反映する際の同一人物判定ポリシー**を定義する。

対象処理:

```
import_subscribers_to_staging_hub.py
apply_subscribers_from_staging_hub.py
```

このポリシーは主に apply phase で利用され、
旧実装では apply phase 内で subscriber identity 判定を実施していたが、
ADR-0021 以降は prepare / compare phase により compare 状態を staging 側へ保持する。

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
- staging → subscribers apply 時の insert / update / noop 判定を行う
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

を元に生成される compare / join 用 hash である。

旧実装では3列を直接比較していたが、
ADR-0021 以降は compare phase において `identity_hash` を中心に利用する。

---

# 3. identity_hash Components

identity_hash は以下の3要素から構成される。

| column | role | reason |
|---|---|---|
| `person_id_custom` | 主識別子 | 保険者番号・記号・番号・生年月日を元に生成される加入者識別キー |
| `name_kana_full_match` | 氏名照合補助 | 正規化済みカナ照合キー |
| `gender_code` | 補助識別子 | 同名・近似データの誤結合抑止 |

identity_hash は compare phase における:

- subscriber 同一性比較
- diff 判定
- identity change 検知
- parts clear 判定

に利用する。

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

ADR-0021 以降は、これらを combine した `identity_hash` を compare phase の主比較キーとして扱う。

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

apply script の検索イメージ:

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
compare status
```

HIA subscriber ID は、同一 subscriber を追跡する最優先の外部IDとして扱う。

HIA subscriber ID が一致する場合、
identity_hash が変更されていても、原則として:

```text
同一 HIA subscriber の情報更新
```

として扱う。

ただし、identity_hash 変更内容は compare phase で確認する。

特に:

```text
name_kana_full_match changed
```

の場合は、既存 name parts をクリアし、
後続 normalize / split により再生成する。

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

```text
address
contact
qualification
employer/dept
```

などの差分比較へ進む。

## identity_hash changed

```text
identity_hash changed
```

の場合:

```text
HIA 最新状態へ更新候補
```

として扱う。

ただし差分内容を確認する。

### name_kana_match changed

```text
name_kana_full_match changed
```

の場合:

```text
既存 name parts をクリア
```

し、後続 normalize / split により再生成する。

### insurance_symbol / insurance_number changed only

記号・番号のみ変更の場合:

```text
parts は維持
```

する。

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

これらは一致判定ではなく、apply 後の update / 履歴管理対象とする。

---

# 9. Apply Decision Relationship

旧実装では apply phase 内で compare と apply を同時に実施していた。

ADR-0021 以降は:

```text
import
  ↓
prepare / compare
  ↓
apply_action 作成
  ↓
apply
```

へ分離する。

compare phase では:

```text
identity_hash
address
contact
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
- identity_changed
- review
```

apply phase は compare 結果をもとに insert / update / noop を実行する。

---

# 10. Error / Ambiguity Policy

本ポリシーでは、同一 key に対して複数 subscriber が存在する状態は想定しない。

想定外状態:

- 同じ `identity_hash` を持つ subscriber が複数存在
- 正規化前入力の異常により person_id_custom が生成不能
- name_kana_full_match が空

これらは正常 apply 対象ではなく、
インポートまたは apply のエラーとして扱う。

---

# 11. Design Notes

この identity policy は、SQLite 版で運用していた以下の照合思想を MySQL / PHR v1.0.1 に引き継ぐものである。

```
(person_id_custom, name_kana_full_match, gender_code)
```

過度に多くの列を identity に含めないことで、

- 住所変更
- 連絡先変更
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

を中心に compare / apply を行う。

ADR-0021 以降は:

```text
import
  ↓
prepare / compare
  ↓
apply
```

へ責務分離し、compare 状態と diff 情報を staging 側へ保持する。

identity_hash changed の場合でも、
HIA 側を最新正本として subscribers へ反映する。

ただし:

```text
name_kana_full_match changed
```

の場合は、既存 name parts をクリアし、後続 normalize / split により再生成する。