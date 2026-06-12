

# from_fund_subscribers Overview

## Purpose

本仕様は、健保から受領した加入者情報を HIA へ反映するまでの業務フローを整理する。

加入者管理全体の親仕様は以下を参照する。

```text
docs/spec/subscriber
```

---

## Position

from_fund_subscribers は加入者管理全体のうち、以下の範囲を担当する。

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

HIA export 以降は以下で扱う。

原則として加入者更新の責務は持たない。

ただし、比較品質向上および加入者管理情報維持のため、限定的な補完処理を実施する場合がある。

現行実装では:

```text
subscribers の name parts 補完
```

name parts は HIA が保持していない情報であり、加入者管理上必要な情報として PHR 側で補完・維持する。

```text
docs/spec/hia_export_subscribers_csv
```

---

## Business Flow

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
```

---

## Core Responsibilities

### 1. 健保受領データ管理

```text
受領CSV
↓
staging_subscribers_fund
```

受領データを保持し、比較可能な状態へ正規化する。

### 2. 加入者比較

```text
受領データ
↓
現在状態
↓
差分判定
```

比較機能は年度更新専用ではなく、通常の加入者更新でも利用する。

### 3. 限定的な補完

比較品質向上および加入者管理情報維持のため、限定的な補完処理を実施する。

例:

```text
subscribers の name parts 補完
```

補完処理は比較品質向上に加え、HIA が保持しない加入者管理情報を維持することを目的とする。

特に name parts は加入者管理上の重要情報であり、今後も PHR 側で補完・維持する前提とする。

### 4. HIA反映データ生成

```text
差分判定結果
↓
HIA反映CSV
```

HIAへ投入するためのデータを生成する。

---

## Year Transition Relationship

年度更新は from_fund_subscribers の特殊機能ではない。

```text
加入者更新
 ├ 通常更新
 └ 年度更新
```

という位置付けとする。

年度更新案件では大量件数の差分判定を行うが、利用している比較基盤は通常運用と共通とする。

関連仕様:

```text
docs/spec/operations/subscriber_year_transition
```

---

## Future Detailed Specifications

本ディレクトリでは以下を管理する。

```text
02_operation_flow.md
03_staging_subscribers_fund.md
04_compare_policy.md
05_output_to_hia.md
```

詳細仕様は実装と整合を取りながら更新する。