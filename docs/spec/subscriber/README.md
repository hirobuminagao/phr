

# subscriber

## Purpose

このディレクトリは、加入者管理に関する仕様の親ディレクトリとする。

加入者情報は以下の業務フローで管理される。

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
HIA反映CSV生成
  ↓
HIA

staging_subscribers_fund
  ↓
matched_subscriber_id
  ↓
name parts補完
  ↓
subscribers
subscriber_addresses
subscriber_contact_points
  ↓
dashboard
```

補足:

```text
健保受領データからは2系統の処理が発生する。

① HIA反映系
staging_subscribers_fund
 ↓
差分判定
 ↓
HIA反映CSV生成
 ↓
HIA

② subscribers補完系
staging_subscribers_fund
 ↓
matched_subscriber_id
 ↓
name parts補完
 ↓
subscribers
```

name parts補完は subscribers の氏名parts情報を限定的に補完する処理であり、加入者本体更新とは分離して管理する。

---

## Scope

本ディレクトリでは以下を管理する。

```text
- 加入者更新業務の全体像
- 業務フロー
- スクリプト対応表
- 各詳細specへの導線
```

各処理の詳細仕様は、それぞれのspecディレクトリで管理する。

---

## Related Specifications

### 健保受領〜HIA反映

```text
docs/spec/from_fund_subscribers
```

### HIA export〜Hub正本反映

```text
docs/spec/hia_export_subscribers_csv
```

### 年度更新運用

```text
docs/spec/operations/subscriber_year_transition
```

---

## Next Documents

```text
01_business_flow.md
02_script_map.md
03_data_flow.md
```

を加入者管理の上位ドキュメントとして管理する。

### Document Relationships

#### 01_business_flow.md

業務視点の全体フローを定義する。

```text
健保
 ↓
HIA
 ↓
subscribers
 ↓
dashboard
```

という業務上の流れを整理する。

#### 02_script_map.md

業務工程と実装スクリプトの対応関係を整理する。

```text
業務工程
 ↓
スクリプト
 ↓
詳細spec
```

の対応表として扱う。

#### 03_data_flow.md

データ視点の流れを整理する。

```text
staging_subscribers_fund
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

など、加入者関連データの格納先と関係を整理する。

### Reading Order

推奨読書順:

```text
README.md
 ↓
01_business_flow.md
 ↓
02_script_map.md
 ↓
03_data_flow.md
 ↓
各詳細spec
```

まず業務を理解し、その後に実装とデータ構造を確認する。