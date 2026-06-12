# Operation Flow

## Purpose

本ドキュメントは、健保から受領した加入者情報を HIA へ反映するまでの業務フローおよびシステム処理フローを定義する。

from_fund_subscribers の責務は、健保受領データを差分判定し、HIA反映CSVを生成するところまでとする。

---

## Overview

```text
健保受領CSV
  ↓
from_fund_subscribers
  ↓
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
```

---

## Phase 1: Import

目的:

```text
健保受領データを staging_subscribers_fund へ取り込む
```

処理:

```text
健保CSV読込
↓
template_mappings 適用
↓
正規化値生成
↓
比較用値生成
↓
person_id_custom 生成
↓
identity_hash 生成
↓
matched_subscriber_id 解決
↓
staging_subscribers_fund 登録
```

関連spec:

```text
03_staging_subscribers_fund.md
```

---

## Phase 2: Enrichment

目的:

```text
比較および出力に必要な補完情報を付与する
```

代表例:

```text
会社コード補完
部署コード補完
name parts 補完
```

補足:

```text
本フェーズは subscribers 正本更新を目的としない。

比較品質向上および加入者管理情報維持のための限定的な補完を行う。
```

現行実装:

```text
Import
(import_staging_subscribers_fund.py)
↓ 自動実行
Apply
(apply_staging_subscribers_fund_to_subscribers.py)
```

Apply フェーズでは name parts 補完を実施する。

本フェーズは Import 実行時に自動起動される。

---

## Phase 3: Compare

目的:

```text
受領データと現在状態との差分を判定する
```

比較対象:

```text
identity
organization / affiliation
address
contact
```

判定結果例:

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

実行方法:

```text
update_staging_subscriber_diff_status.py
```

補足:

```text
Compare フェーズは Import / Apply から自動起動されない。

運用担当者が対象 import_run_id を確認後、
明示的に実行する。
```

関連spec:

```text
04_compare_policy.md
```

---

## Phase 4: HIA Output

目的:

```text
差分判定結果から HIA反映CSV を生成する
```

出力対象:

```text
add_export_rows
update_export_rows
```

出力対象外:

```text
no_change
major_candidate
missing_from_new
```

関連spec:

```text
05_output_to_hia.md
```

---

## Phase 5: HIA Operation

目的:

```text
HIAへ更新内容を反映する
```

処理:

```text
HIA反映CSVアップロード
↓
HIA更新
```

補足:

```text
本フェーズは from_fund_subscribers の責務外
本フェーズは加入者更新業務全体の説明のため記載している。

from_fund_subscribers の責務には含まれない。
```

---

## Phase 6: Hub Apply

目的:

```text
HIA export を基準として Hub 正本へ反映する
```

処理:

```text
HIA export
↓
staging_subscribers_hub
↓
Hub apply
```

反映先:

```text
subscribers
subscriber_addresses
subscriber_contact_points
```

補足:

```text
住所・連絡先は from_fund_subscribers で比較対象となるが、
直接更新は行わない。

最終反映は HIA export を経由する。
```

---

## Special Case: Name Parts Enrichment

from_fund_subscribers では例外的に subscribers を直接補完する場合がある。

現行実装:

```text
family_name_kanji
given_name_kanji
family_name_kana
given_name_kana
```

条件:

```text
IDENTITY_MATCHED
subscriber 側が空欄
staging 側に値あり
```

補完内容は audit に記録する。

---

## Responsibility Boundary

from_fund_subscribers の責務:

```text
Import
Enrichment
Compare
HIA Output
```

責務外:

```text
HIAアップロード
HIA export取得
Hub apply
subscribers 正本更新
subscriber_addresses 更新
subscriber_contact_points 更新
```

ただし、比較品質向上および加入者管理情報維持のための限定的な補完処理は例外とする。

現行実装では name parts 補完を行う。
