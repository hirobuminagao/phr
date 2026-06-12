# from_fund_subscribers Compare Policy

## Purpose

本ドキュメントは、健保から受領した加入者情報を HIA へ反映するための比較方針を定義する。

本比較は、加入者正本を直接更新するためではなく、HIA反映CSV生成のために差分を検出・分類することを目的とする。

---

## Position

```text
健保受領CSV
  ↓
staging_subscribers_fund
  ↓
subscribers から現在状態を取得
  ↓
差分判定
  ↓
HIA反映CSV生成
```

---

## Responsibilities

本比較処理の責務:

```text
- 健保受領データと現在の subscribers 関連情報を比較する
- HIA反映CSVへ出力すべき差分候補を分類する
- 比較結果を staging_subscribers_fund へ記録する
```

責務外:

```text
- HIAそのものの更新
- subscribers 正本更新
- subscriber_addresses 更新
- subscriber_contact_points 更新
- dashboard 更新
- 業務判断による最終確定
```

例外として、比較品質向上および加入者管理情報維持のため、限定的な補完処理を実施する場合がある。

現行実装では name parts 補完を行う。

---

## Comparison Source

比較は以下を入力とする。

```text
受領側:
  staging_subscribers_fund

現在状態側:
  subscribers
  subscriber_addresses
  subscriber_contact_points
```

比較の正本は `subscriber_contact_points` とする。

現行実装には旧連絡先構造を参照する箇所が存在する。

---

## Difference Categories

比較対象は以下のカテゴリに分ける。

```text
identity
organization / affiliation
address
contact
```

---

## identity

identity は同一人物判定に利用する。

実装では、`person_id_custom` と氏名カナ・性別を基に `identity_hash` を生成する。

代表的な入力:

```text
person_id_custom
name_kana_full
name_kana_full_match / name_kana_full_raw
gender_code
```

本specでは identity 判定の思想のみを定義する。

identity の詳細な構成は実装に合わせて管理する。
古い年度更新specに記載された構成要素と異なる可能性があるため、実装を正とする。

---

## organization / affiliation

会社・部署・資格・続柄などの属性差分を比較する。

代表的な比較対象:

```text
relationship_name
qualification_acquired_date
qualification_lost_date
employer_code
department_code
distribution_code
employee_code
connect_id
```

健保受領値と HIA / subscribers 側の値が直接一致しない場合は、mapping 後の値を比較対象とする。

---

## address

住所は比較対象である。

ただし、from_fund_subscribers は `subscriber_addresses` を直接更新しない。
住所差分は HIA反映CSV生成のための差分候補として扱う。

代表的な比較対象:

```text
postal_code_match
address_match
```

住所の正規化・match ルールは identity 系とは分けて扱う。

```text
identity系:
  同一人物判定のための照合寄せ

住所系:
  住所値の差分検出のための比較寄せ
```

住所は日本語住所として比較しやすい形へ寄せる。
英数字・記号の扱いは住所用の正規化方針に従う。

---

## contact

連絡先は比較対象である。

ただし、from_fund_subscribers は `subscriber_contact_points` を直接更新しない。
連絡先差分は HIA反映CSV生成のための差分候補として扱う。

代表的な比較対象:

```text
phone
email
```

電話番号は比較用に正規化した値を利用する。
メールアドレスは表記ゆれを考慮して比較する。

---

## diff_status

比較結果は `staging_subscribers_fund` 側へ記録する。

代表的な分類:

```text
通常判定:

add
update
no_change

確認対象:

major_candidate

運用確認対象:

missing_from_new
```

`update` は subscribers root 更新を意味しない。

from_fund_subscribers における `update` は:

```text
HIA反映CSVへ出力すべき差分候補がある
```

ことを意味する。

したがって、住所・連絡先差分も `update` 相当の差分候補になり得る。

---

## diff details

差分がある場合は、可能な範囲で差分項目を記録する。

例:

```text
identity
organization / affiliation
address
contact
```

将来的には `diff_columns` または同等の詳細列で、どの項目に差分があるかを明示できるようにする。

---

## name parts enrichment

from_fund_subscribers では、比較品質向上および加入者管理情報維持のため、限定的に subscribers を補完する場合がある。

現行実装では、name parts 補完を行う。

現行実装条件:

```text
parts_apply_subscriber_id IS NOT NULL
parts_apply_status = 'IDENTITY_MATCHED'
subscribers 側が空欄
staging 側が非空欄
```

方針:

```text
- 新規 subscribers は作成しない
- 既存非空欄値は上書きしない
- 補完内容は subscriber_audit に記録する
```

---

## Relation to HIA export apply

HIA反映CSVを HIA へ投入した後、HIA export を取得し、Hub apply 側で subscribers 関連テーブルへ反映する。

```text
HIA反映CSV
  ↓
HIA
  ↓
HIA export
  ↓
staging_subscribers_hub
  ↓
Hub apply
  ↓
subscribers / subscriber_addresses / subscriber_contact_points
```

このため、from_fund_subscribers 側では住所・連絡先の差分を検出するが、住所・連絡先テーブルへの直接反映は行わない。

---

## Related Documents

```text
docs/spec/from_fund_subscribers/01_overview.md
docs/spec/from_fund_subscribers/03_staging_subscribers_fund.md
docs/spec/from_fund_subscribers/05_output_to_hia.md
docs/spec/hia_export_subscribers_csv
docs/spec/subscriber
```
