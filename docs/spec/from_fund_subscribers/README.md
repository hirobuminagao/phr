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


---

## Name Parts Enrichment / Backfill Flow

name parts 補完は、差分判定・HIA反映CSV生成とは独立した補助処理として扱う。

目的:

```text
HIA が保持しない name parts を、fund 由来の分割済み値で補完する。
```

処理は以下の2段階に分ける。

```text
1. parts_apply_subscriber_id 解決
2. 共通 apply による subscribers 更新
```

---

### Common Apply

実際の name parts 比較・更新は共通処理に委譲する。

```text
apply_subscribers_fund_name_parts.py
```

共通 apply の入力条件:

```text
parts_apply_subscriber_id IS NOT NULL
parts_apply_status が補完許可状態
```

共通 apply の責務:

```text
staging_subscribers_fund の name parts
↓
subscribers の name parts 空欄確認
↓
subscribers 更新
↓
subscribers_audit 記録
```

つまり、通常 import 後補完と Hub apply 後 backfill は、
補完先IDの解決方法だけが異なり、実際の更新処理は共通化する。

---

### Normal Import Path

通常 import 時点で既存 subscribers に一致した加入者は、`matched_subscriber_id` を起点に補完先を確認する。

```text
fund import
↓
staging_subscribers_fund
↓
matched_subscriber_id 解決
↓
parts_apply refresh
↓
parts_apply_subscriber_id 設定
↓
parts_apply_status = IDENTITY_MATCHED
↓
common apply
```

補足:

```text
matched_subscriber_id
  = import 時点の照合結果

parts_apply_subscriber_id
  = name parts 補完用の確定ID
```

---

### After Hub Apply Backfill Path

fund import 時点では subscribers に存在しない加入者が存在する。

そのため、通常 import 後の name parts 補完では対象外となる場合がある。

この場合は、HIA登録および Hub apply 後に、identity_hash から補完先 subscribers を再解決する。

```text
fund import
↓
staging_subscribers_fund

↓

HIA登録
↓
HIA export
↓
staging_subscribers_hub
↓
Hub apply
↓
subscribers 作成

↓

backfill_name_parts_after_hub_apply
↓
identity_hash 一致確認
↓
parts_apply_subscriber_id 解決
↓
parts_apply_status = IDENTITY_MATCHED
↓
common apply
```

補足:

```text
後追い補完では matched_subscriber_id を更新しない。

parts_apply_subscriber_id は、補完実行時点の subscribers 状態を基準として解決する。
```

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