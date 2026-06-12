# HIA Output

## Purpose

健保から受領した加入者情報と現在状態との差分を基に、HIAへ反映するための更新データを生成する。

本処理の責務は HIA反映CSV生成までとし、HIAへのアップロードおよび HIA export 後の反映は責務外とする。

## Position

健保受領CSV
  ↓
staging_subscribers_fund
  ↓
差分判定
  ↓
HIA反映CSV生成
  ↓
HIA

## Input

入力元:

```text
差分判定結果

add
update
```

## Output

出力先:

```text
HIA反映CSV
```

本出力は HIA 側加入者情報の更新候補を表現する。

## Output Selection Policy

HIA反映CSVは、差分判定結果から HIA更新対象として採用されたレコードを出力する。

現行実装:

```text
add_export_rows
update_export_rows
```

出力対象外:

```text
no_change
major_candidate
missing_from_new
unknown
```

`update` は subscribers 更新を意味しない。

HIA反映対象であることを意味する。

```text
住所差分
連絡先差分
会社差分
部署差分
資格差分
identity差分
```

など、HIAへ反映すべき差分が存在することを意味する。

## Output Categories

### identity

代表的な出力項目:

```text
insurance_symbol_norm
insurance_number_norm
insurance_branchnumber_norm
name_kanji_full_norm
name_kana_full_norm
gender_code_norm
birth_norm
```

### organization / affiliation

代表的な出力項目:

```text
relationship_name_norm
qualification_acquired_date_norm
qualification_lost_date_norm
mapped_employer_code
mapped_department_code
received_distribution_code_norm
received_employee_code_norm
connect_id_norm
```

### address

代表的な出力項目:

```text
postal_code_norm
address_match
```

住所は比較対象であり、HIA反映CSV出力対象でもある。

### contact

代表的な出力項目:

```text
phone_norm
email_norm
```

連絡先は比較対象であり、HIA反映CSV出力対象でもある。

## Output Record Rules

現行実装では1加入者につき1レコードを出力する。

将来の出力単位は HIA反映CSV仕様に従う。

## CSV Layout

出力値には受領値そのものではなく、正規化後・比較用に整形した値を利用する場合がある。

代表例:

```text
郵便番号             ← postal_code_norm
住所                 ← address_match
電話番号             ← phone_norm
メールアドレス       ← email_norm
事業所コード         ← mapped_employer_code
所属コード           ← mapped_department_code
```

詳細な列定義は HIA反映CSV仕様に従う。

## HIA Upload Boundary

from_fund_subscribers の責務は HIA反映CSV生成までとする。

```text
CSV生成
  ○

HIAアップロード
  ×

HIA export取得
  ×
```

## Non Responsibilities

HIAアップロード
subscribers 正本更新
subscriber_addresses 更新
subscriber_contact_points 更新
staging_subscribers_hub 取込
Hub apply

## Relation to Hub Apply

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
subscribers
subscriber_addresses
subscriber_contact_points
  ↓
dashboard

## Related Documents

docs/spec/from_fund_subscribers/01_overview.md
docs/spec/from_fund_subscribers/03_staging_subscribers_fund.md
docs/spec/from_fund_subscribers/04_compare_policy.md
docs/spec/hia_export_subscribers_csv
