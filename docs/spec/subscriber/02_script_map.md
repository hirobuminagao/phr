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

## Script Responsibility Types

本ドキュメントではスクリプトを以下の2種類に分けて整理する。

```text
オーケストレーター
  業務単位で実行する入口スクリプト。
  対象選定、設定読込、処理順序の制御、処理スクリプト呼び出しを行う。

処理スクリプト / script_lib
  呼び元から渡された対象・設定に対して処理を行う部品。
  原則として業務対象の選定責務を持たない。
```

特に name parts 補完では、対象選定と補完処理を分離する。

```text
対象選定
  オーケストレーター側の責務

parts_apply refresh / common apply
  処理スクリプト側の責務
```

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

実行入口:

```text
import_staging_subscribers_fund.py
  種別: オーケストレーター
  役割:
    - 健保CSVを staging_subscribers_fund に取込
    - CSV単位で import_run_id を発行
    - company mapping補完を起動
    - parts補完 apply を import_run_id 単位で起動
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

実行入口:

```text
update_staging_subscriber_diff_status.py
  種別: オーケストレーター
  役割:
    - 設定ファイルから対象 import_run_ids を読み込む
    - staging_subscribers_fund と subscribers を比較する
    - diff_status を staging_subscribers_fund に記録する
    - HIA反映CSV生成処理を呼び出す
```

主な処理部品:

```text
diff_classifier.py
  種別: script_lib
  役割:
    - staging行とsubscriber行の差分分類を行う

major_candidate_finder.py
  種別: script_lib
  役割:
    - identity系変更候補を探索する
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

## Step 5.5. subscribers name parts補完

```text
staging_subscribers_fund
  ↓
parts_apply refresh
  ↓
common name parts apply
  ↓
subscribers
```

通常 import 後補完:

```text
apply_staging_subscribers_fund_to_subscribers.py
  種別: オーケストレーター
  役割:
    - import_run_id を受け取る
    - matched_subscriber_id が解決済みの staging 行を選定する
    - parts_apply_refresh.py を matched mode で呼び出す
    - apply_subscribers_fund_name_parts.py を import_run_id 指定で呼び出す
```

Hub apply 後 backfill:

```text
backfill_name_parts_after_hub_apply.py
  種別: オーケストレーター
  役割:
    - 設定ファイルを読み込む
    - use_import_run_ids=false の場合は未解決 parts_apply 行を全件対象にする
    - use_import_run_ids=true の場合は指定 import_run_ids のみ対象にする
    - parts_apply_refresh.py を identity_hash mode で呼び出す
    - apply_subscribers_fund_name_parts.py を対象範囲に応じて呼び出す
```

共通処理部品:

```text
parts_apply_refresh.py
  種別: script_lib
  役割:
    - 呼び元から渡された staging 行を処理する
    - matched mode では matched_subscriber_id 起点で parts_apply_subscriber_id を解決する
    - identity_hash mode では identity_hash 起点で parts_apply_subscriber_id を解決する
    - 解決できた場合は parts_apply_status = IDENTITY_MATCHED を設定する
    - 対象 staging 行の選定は行わない

apply_subscribers_fund_name_parts.py
  種別: script_lib
  役割:
    - parts_apply_subscriber_id / parts_apply_status = IDENTITY_MATCHED の行を処理する
    - staging_subscribers_fund の name parts を subscribers の空欄 parts に補完する
    - 漢字parts / カナpartsをグループ単位で判定する
    - parts本体列と parts_match列をペアで更新する
    - subscribers_audit を記録する
    - 結果として PARTS_APPLIED または PARTS_FAILED を staging_subscribers_fund に記録する
```

補足:

```text
apply_staging_subscribers_fund_to_subscribers.py は、subscribers全体更新ではなく name parts補完専用の入口スクリプトとして扱う。
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

実行入口:

```text
scripts/hia/import_subscribers_to_staging_hub.py
  種別: オーケストレーター
  役割:
    - HIA export CSVを取込
    - staging_subscribers_hub を作成・更新
    - current snapshot を更新
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

実行入口:

```text
scripts/hia/apply_hia_subscriber_sync.py
  種別: オーケストレーター
  役割:
    - prepare / compare / apply / audit / processed mark を順に呼び出す
    - HIA側正本を subscribers 系テーブルへ反映する
```

主な処理部品:

```text
hub_subscriber_prepare.py
  種別: script_lib
  役割:
    - Hub apply 前の準備処理を行う

hub_subscriber_compare.py
  種別: script_lib
  役割:
    - staging_subscribers_hub と subscribers の比較を行う

hub_subscriber_apply.py
  種別: script_lib
  役割:
    - subscribers / subscriber_addresses / subscriber_contact_points へ反映する

hub_subscriber_audit.py
  種別: script_lib
  役割:
    - subscribers_audit 等へ変更履歴を記録する
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