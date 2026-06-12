# from_fund_subscribers

## Purpose

この仕様群は、健保から受領した加入者情報を取り込み、HIAへ反映するための業務フローとデータ構造を定義する。

この仕様群は、加入者管理全体のうち:

```text
健保
  ↓
加入者情報受領
  ↓
staging_subscribers_fund
  ↓
差分判定
  ↓
限定的な補完
  ↓
HIA反映CSV生成
  ↓
HIAアップロード
```

までを主な責務範囲とする。

原則として、HIA更新そのもの、および `subscribers` / `subscriber_addresses` / `subscriber_contact_points` への正本反映は責務に持たない。

ただし、比較品質向上および加入者管理情報維持のため、限定的な補完処理を実施する場合がある。

現行実装では:

```text
subscribers の name parts 補完
```

加入者管理全体の親仕様は:

```text
docs/spec/subscriber
```

で管理する。

---

## Scope

本仕様で扱う内容:

```text
- 健保受領データ取込
- staging_subscribers_fund
- 差分判定
- HIA反映CSV生成
- HIAとの連携方針
- 比較品質向上および加入者管理情報維持のための限定的な補完処理
```

本仕様で扱わない内容:

```text
- HIAアップロード操作そのもの
- HIA export 取込
- Hub apply
- subscribers 正本反映
- subscriber_addresses 反映
- subscriber_contact_points 反映
- dashboard 反映
- 健診結果
- 保健指導結果
```

補足:

```text
HIA export
  ↓
staging_subscribers_hub
  ↓
Hub apply
  ↓
subscribers
subscriber_addresses
subscriber_contact_points
```

以降は以下で管理する。

```text
docs/spec/hia_export_subscribers_csv
```

---

## Business Flow

from_fund_subscribers の業務フロー:

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
HIAアップロード
```

加入者管理全体のフロー:

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
```

年度更新は、この加入者更新業務フローを利用した大規模更新案件の一例として扱う。

---

## Relationship to subscriber_year_transition

既存の:

```text
docs/spec/operations/subscriber_year_transition
```

は、2025→2026年度更新案件を整理した仕様群である。

今後は:

```text
docs/spec/subscriber
```

を加入者管理全体の親仕様とし、`from_fund_subscribers` はそのうち健保受領から HIA反映CSV生成までを扱う詳細仕様として位置付ける。

年度更新は `from_fund_subscribers` の差分判定・HIA反映CSV生成の仕組みを利用した一実施パターンとして扱う。