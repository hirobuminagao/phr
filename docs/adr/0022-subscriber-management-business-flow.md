# ADR-0022 Subscriber Management Business Flow

## Status

Accepted

---

## Context

加入者関連の仕様は、以下の観点ごとに個別に整理されていた。

```text
from_fund_subscribers
hia_export_subscribers_csv
subscriber_year_transition
```

しかし、これらは本来すべて:

```text
加入者更新業務
```

の一部であり、全体フローを説明する上位仕様が存在していなかった。

その結果、以下の課題が発生していた。

```text
- 健保受領から subscribers 反映までの流れが見えない
- 各specの責務境界が分かりにくい
- 年度更新仕様が加入者更新全体仕様のように見える
- 実装スクリプトの位置付けを追いにくい
```

---

## Decision

加入者管理の親仕様として:

```text
docs/spec/subscriber
```

を新設する。

subscriber spec は以下を管理する。

```text
- 加入者更新業務の全体像
- 業務フロー
- スクリプト対応表
- データフロー
- 各詳細specへの導線
```

---

## Subscriber Business Flow

加入者更新業務の標準フローを以下とする。

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

## Specification Boundaries

### from_fund_subscribers

責務:

```text
健保受領
↓
比較・確認
↓
必要に応じた補完
↓
HIA投入
```

管理場所:

```text
docs/spec/from_fund_subscribers
```

補足:

```text
運用上の加入者正本は HIA とする。

from_fund_subscribers は比較・確認および HIA反映データ生成を主責務とする。

ただし、比較品質向上および加入者管理情報維持のため、subscribers の name parts 補完などの限定的な補完処理を実施する場合がある。
```

---

### hia_export_subscribers_csv

責務:

```text
HIA export
↓
staging_subscribers_hub
↓
prepare
compare
apply
↓
subscribers
```

管理場所:

```text
docs/spec/hia_export_subscribers_csv
```

---

### subscriber_year_transition

責務:

```text
年度更新案件
```

比較・確認機能を利用した大規模更新運用として扱う。

加入者更新機能そのものを年度専用機能として定義しない。

管理場所:

```text
docs/spec/operations/subscriber_year_transition
```

---

## Dashboard Policy

加入者更新そのものは通常運用として扱う。

年度という概念は主に:

```text
dashboard
hia_dashboard_year_end_status
```

で利用する。

年度更新運用は、加入者更新機能ではなく年度状態管理のための業務イベントとして扱う。

---

## Consequences

今後は:

```text
subscriber
 ↓
業務
 ↓
実装
 ↓
データ
 ↓
詳細spec
```

の順に仕様を参照する。

これにより、加入者更新業務全体と各詳細仕様の責務境界を明確化する。