# Subscriber Script Map

## Purpose

本ドキュメントは、加入者更新業務の各工程と実装スクリプトの対応関係を整理する。

業務フローは以下を参照する。

```text
docs/spec/subscriber/01_business_flow.md
```

---

## Overview

```text
業務工程
  ↓
スクリプト
  ↓
詳細spec
```

の対応を管理する。

---

## Step 1. 健保受領

```text
健保
  ↓
加入者情報受領
```

現時点では手作業受領。

---

## Step 2. from_fund_subscribers

```text
加入者情報受領
  ↓
from_fund_subscribers
```

主なスクリプト:

```text
import_staging_subscribers_fund.py
```

関連spec:

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

主な役割:

```text
受領データ保持
正規化値保持
比較用データ保持
```

関連spec:

```text
docs/spec/from_fund_subscribers
```

---

## Step 4. 差分判定

```text
staging_subscribers_fund
  ↓
差分判定
```

主なスクリプト:

```text
update_staging_subscriber_diff_status.py
diff_classifier.py
major_candidate 判定系
```

関連spec:

```text
docs/spec/from_fund_subscribers
（年度更新specは参考資料として参照）
docs/spec/operations/subscriber_year_transition
```

備考:

```text
差分判定機能は通常の加入者更新運用でも利用する。

年度更新は、この差分判定機能を利用した大規模更新案件の一例として扱う。
```

---

## Step 5. HIA反映CSV生成

```text
差分判定
  ↓
HIA反映CSV生成
```

主なスクリプト:

```text
update_staging_subscriber_diff_status.py
hia_subscribers_exporter.py
```

関連spec:

```text
docs/spec/from_fund_subscribers
```

---

## Step 6. HIA

```text
HIA反映CSV
  ↓
HIA
```

HIA側運用。

PHR管理対象外。

---

## Step 7. HIA export

```text
HIA
  ↓
HIA export
```

HIA側運用。

---

## Step 8. hia_export_subscribers_csv

```text
HIA export
  ↓
hia_export_subscribers_csv
```

主なスクリプト:

```text
scripts/hia/import_subscribers_to_staging_hub.py
```

役割:

```text
HIA export取込
current snapshot更新
```

関連spec:

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

役割:

```text
prepare対象
compare対象
apply対象
```

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

主なスクリプト:

```text
scripts/hia/apply_hia_subscriber_sync.py
scripts/hia/script_lib/hub_subscriber_prepare.py
scripts/hia/script_lib/hub_subscriber_compare.py
scripts/hia/script_lib/hub_subscriber_apply.py
scripts/hia/script_lib/hub_subscriber_audit.py
```

役割:

```text
prepare
compare
apply
subscriber audit
processed mark
Hub正本反映
```

関連spec:

```text
docs/spec/hia_export_subscribers_csv
```

---

## Step 11. dashboard

```text
subscribers
subscriber_addresses
subscriber_contact_points
  ↓
dashboard
```

主な役割:

```text
加入者状態表示
年度状態管理
```

備考:

```text
加入者更新そのものは通常運用として扱う。

年度という概念は主に dashboard 側の年度状態管理で利用する。

年度更新運用は、年度状態を切り替えるための業務イベントであり、加入者更新機能そのものを年度専用機能として扱うものではない。
```

---

## Maintenance Policy

本ドキュメントは、実装スクリプトの追加・変更時に更新する。

業務フロー変更時は:

```text
01_business_flow.md
```

を先に更新する。