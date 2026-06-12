# staging_subscribers_fund

## Purpose

`staging_subscribers_fund` は、健保から受領した加入者情報を保持する作業テーブルである。

役割は:

```text
受領データ保持
↓
正規化
↓
差分判定
↓
HIA反映データ生成
```

であり、本テーブルの責務は差分判定および HIA反映データ生成までとする。

加入者情報の運用上の正本は HIA とし、
staging_subscribers_fund は差分判定および HIA投入データ生成のための作業領域として扱う。

---

## Position

```text
健保受領CSV
  ↓
staging_subscribers_fund
  ↓
差分判定
  ↓
HIA反映CSV
```

関連仕様:

```text
docs/spec/from_fund_subscribers/01_overview.md
```

---

## Responsibilities

### 1. 受領データ保持

健保から受領したデータを基に、差分判定および HIA反映データ生成に必要な値を保持する。

健保受領ファイルそのものを原本とし、staging_subscribers_fund は作業領域として扱う。

---

### 2. 正規化値保持

比較やキー生成に利用するため、正規化値を保持する。

例:

```text
氏名カナ正規化
記号正規化
番号正規化
郵便番号比較用値
住所比較用値
電話番号正規化値
```

---

### 3. 比較用データ保持

比較処理で利用する値を保持する。

例:

```text
person_id_custom
identity_hash
matched_subscriber_id
```

比較ロジックの詳細は別specで管理する。

---

### 4. HIA反映元データ

差分判定結果を基に、HIA投入データ生成の元データとして利用する。

---

## Non Responsibilities

以下は本テーブルの責務外とする。

```text
HIA更新
subscribers更新（補完処理を除く）
subscriber_addresses更新
subscriber_contact_points更新
dashboard更新
```

staging_subscribers_fund の責務は差分判定および HIA反映データ生成までとする。

HIA export 後の subscribers 反映処理は Hub apply 側で扱う。

例外として、比較品質向上および加入者管理情報維持のため、限定的な補完処理を実施する場合がある。

現行実装では:

subscribers の name parts 補完

---

## Current Design Status

現時点では実装を先行して整備している。

主要責務および主要比較項目は整理済みである。

列構成・比較用列・hash列などの詳細は、実装確認を継続しながら本specへ反映する。

そのため本ドキュメントは責務定義を優先し、詳細列定義は後続ドキュメントで管理する。

---

## Related Documents

```text
docs/spec/from_fund_subscribers/01_overview.md
docs/spec/from_fund_subscribers/04_compare_policy.md
docs/spec/from_fund_subscribers/05_output_to_hia.md
```