

# Subscriber Business Flow

## Purpose

本ドキュメントは、加入者更新業務の全体フローを定義する。

詳細な実装仕様ではなく、業務上どのデータがどこへ流れるかを整理することを目的とする。

---

## Overall Flow

```text
健保
  ↓
加入者情報受領
  ↓
from_fund_subscribers
  ↓
staging_subscribers_fund
  ↓
差分判定
  ↓
限定的な補完
  ↓
HIA反映CSV生成
  ↓
HIA
  ↓
HIA export
  ↓
hia_export_subscribers_csv
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
```

---

## Step 1. 健保受領

```text
健保
  ↓
加入者情報受領
```

健保から加入者情報を受領する。

受領データは加入者更新業務の起点となる。

---

## Step 2. from_fund_subscribers

```text
加入者情報受領
  ↓
from_fund_subscribers
```

受領データを取り込み、比較可能な形式へ整備する。

関連仕様:

```text
docs/spec/from_fund_subscribers
```

---

## Step 3. staging_subscribers_fund

```text
from_fund_subscribers
  ↓
staging_subscribers_fund
```

受領データを staging テーブルへ格納する。

この段階では加入者正本は更新しない。

---

## Step 4. 差分判定

```text
staging_subscribers_fund
  ↓
差分判定
```

受領データと現在状態との差分判定を実施する。

主な目的:

```text
- 新規加入候補
- 継続加入者
- 転籍候補
- 資格喪失候補
- 更新対象候補
```

の判定である。

年度更新は、この工程の特殊ケースとして扱う。

関連仕様:

```text
docs/spec/operations/subscriber_year_transition
```

---

## Step 5. HIA反映CSV生成

```text
差分判定
  ↓
HIA反映CSV生成
```

差分判定結果を基に、HIAへ投入するためのデータを生成する。

---

## Step 6. HIA

```text
HIA反映CSV
  ↓
HIA
```

加入者情報を HIA 側で管理する。

HIA は加入者運用上の正本として扱う。

---

## Step 7. HIA export

```text
HIA
  ↓
HIA export
```

HIA から最新の加入者情報を export する。

---

## Step 8. hia_export_subscribers_csv

```text
HIA export
  ↓
hia_export_subscribers_csv
```

HIA export を取り込み、PHR反映用の staging を生成する。

関連仕様:

```text
docs/spec/hia_export_subscribers_csv
```

---

## Step 9. staging_subscribers_hub

```text
hia_export_subscribers_csv
  ↓
staging_subscribers_hub
```

HIA export データを staging テーブルへ格納する。

prepare / compare / apply の対象となる。

---

## Step 10. Hub apply

```text
staging_subscribers_hub
  ↓
Hub apply
  ↓
subscribers
subscriber_addresses
subscriber_contact_points
```

Hub apply により加入者正本および関連テーブルへ反映する。

住所・連絡先は subscriber_addresses および subscriber_contact_points を通じて管理する。

---

## Step 11. dashboard

```text
subscribers
subscriber_addresses
subscriber_contact_points
  ↓
dashboard
```

加入者状態をダッシュボードへ反映する。

---

## Related Documents

```text
docs/spec/subscriber/README.md
docs/spec/subscriber/02_script_map.md
docs/spec/from_fund_subscribers
docs/spec/hia_export_subscribers_csv
docs/spec/operations/subscriber_year_transition
```