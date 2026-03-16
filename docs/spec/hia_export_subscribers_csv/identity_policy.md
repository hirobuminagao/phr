

# Subscriber Identity Policy

このドキュメントは **HIA export 加入者 CSV を PHR subscriber master に反映する際の同一人物判定ポリシー**を定義する。

対象処理:

```
import_subscribers_to_staging_hub.py
apply_subscribers_from_staging_hub.py
```

このポリシーは主に apply phase で利用され、
`staging_subscribers_hub` の1行を既存 `subscribers` に結びつけるための基準となる。

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

# 2. Identity Key

同一人物判定キーは以下の3項目を使用する。

```
person_id_custom
name_kana_full
gender_code
```

この3項目がすべて一致した場合に、同一 subscriber とみなす。

---

# 3. Why These Columns

| column | role | reason |
|---|---|---|
| `person_id_custom` | 主識別子 | 保険者番号・記号・番号・生年月日を元に生成されるため、加入者識別の中核となる |
| `name_kana_full` | 氏名照合補助 | person_id_custom が偶発的に衝突するケースや入力異常の検出補助 |
| `gender_code` | 補助識別子 | 同名・近似データの誤結合抑制 |

この設計により、

- 保険証系情報ベースの識別
- 氏名カナによる本人性の補助確認
- 性別による誤結合抑止

をバランス良く実現する。

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
- person_id_custom 単独ではなく、`name_kana_full` と `gender_code` を併用する

---

# 5. name_kana_full

`name_kana_full` は **氏名カナの正規化済み全文字列**を使用する。

正規化ルール:

- NFKC 正規化
- ひらがな → カタカナ
- 全角/半角空白の除去
- 連続空白の吸収

生成関数:

```
normalize_name_fields()
```

実装:

```
phr/lib/normalize/subscriber.py
```

例:

| raw | normalized |
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
  AND name_kana_full = ?
  AND gender_code IS ?
LIMIT 1;
```

---

# 7. Matching Rule

matching rule は次の通り。

## match

以下の3項目がすべて一致:

```
person_id_custom
name_kana_full
gender_code
```

結果:

```
existing subscriber を採用
```

## no match

3項目のいずれかが一致しない場合:

```
新規 subscriber 候補
```

結果:

```
INSERT subscribers
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

これらは一致判定ではなく、apply 後の update / 履歴管理対象とする。

---

# 9. Apply Decision Relationship

identity 判定後の分岐:

```
identity match
   ↓
subscriber exists?
   ├ yes → diff compare → update / noop
   └ no  → insert
```

つまり identity は

```
insert / update / noop
```

の入口判定である。

---

# 10. Error / Ambiguity Policy

本ポリシーでは、同一 key に対して複数 subscriber が存在する状態は想定しない。

想定外状態:

- 同じ `(person_id_custom, name_kana_full, gender_code)` を持つ subscriber が複数存在
- 正規化前入力の異常により person_id_custom が生成不能
- name_kana_full が空

これらは正常 apply 対象ではなく、
インポートまたは apply のエラーとして扱う。

---

# 11. Design Notes

この identity policy は、SQLite 版で運用していた以下の照合思想を MySQL / PHR v1.0.1 に引き継ぐものである。

```
(person_id_custom, name_kana_full, gender_code)
```

過度に多くの列を identity に含めないことで、

- 住所変更
- 連絡先変更
- 続柄更新
- 企業属性変更

などの通常更新を安全に扱えるようにしている。

---

# Summary

PHR subscriber identity は以下の3列で定義する。

```
person_id_custom
name_kana_full
gender_code
```

この3列は

- 加入者識別の安定性
- 表記ゆれ耐性
- 誤結合抑制

を両立するための最小コアである。