# ADR 0006: HIA_fund_ledger_xml v1 Policy

## Status

Accepted

---

## Context

HIA システムからダウンロードされる健診結果 ZIP/XML を元に、
Fund 向け納品用データを生成する補助パイプラインが必要となった。

既存の `medi_*` フローは XML 正規化・検査値抽出を目的としているため、
納品用の人物管理・年度管理・登場履歴管理を直接扱う用途には適していない。

そのため本リポジトリでは、以下の新しいフローを追加する。

```
HIA ZIP
 ↓
ZIP import
 ↓
XML検証 / 正規化
 ↓
hia_person_years ledger
 ↓
hia_xml_events ledger
 ↓
納品対象抽出
 ↓
Fund納品 ZIP 再構成
```

詳細仕様は以下の spec を参照する。

```
docs/spec/hia_fund_ledger_xml/
```

## Decision

以下を **HIA_fund_ledger_xml v1 の基本方針として固定する。**

### 1 フローは medi_* と分離する

HIA_fund_ledger_xml は `medi_*` フローとは独立した処理とする。

ただし以下は共通化する。

- 正規化ロジック
- XML項目名称
- 共通ユーティリティ

---

### 2 人照合キー

同一人物判定キーは以下。

```
person_id_custom
+ name_kana_norm
+ gender_code
+ exam_year
```

`person_id_custom` は以下から生成する。

- insurer_number
- insurance_symbol
- insurance_number
- birthdate

---

### 3 人台帳粒度

人台帳の粒度は以下とする。

```
person + exam_year
```

v1 では `hia_person_years` と `hia_xml_events` を分離して実装し、年2回以上の健診にも対応可能な構造とする。

---

### 4 年度判定

`exam_year` は以下で決定する。

- `exam_date` を使用する
- `YEAR(exam_date)` は使用しない
- `.env` の年度開始日設定を使用する

例:

```
2025年度 = 2025-04-01 〜 2026-03-31
```

`exam_date` が無い XML は `EXAM_DATE_MISSING` として ZIP 単位エラーとする。

---

### 5 dl_date の扱い

`dl_date` は以下とする。

```
フォルダ名 / ZIP 名から取得する業務日付
```

これは HIA 伝送日およびデータ到着日を意味し、`exam_year` 判定には使用しない。

---

### 6 エラーポリシー

エラー処理は **ZIP 単位 all-or-nothing** とする。

1 XML でも重大エラーがある場合は `import_status = ERROR` として扱い、`hia_import_zip_errors` に記録する。
その場合、以下は更新しない。

- `hia_person_years`
- `hia_xml_events`

---

### 7 必須項目

以下を必須とする。

人物照合:

- insurer_number
- insurance_symbol
- insurance_number
- birthdate
- name_kana
- genderCode

年度判定:

- exam_date

---

### 8 監査用時刻

以下を分離して扱う。

|列|意味|
|---|---|
|dl_date|HIA伝送日|
|created_at|DB記帳日時|
|updated_at|更新日時|

---

### 9 納品再構成時の除外ルール

取込と納品再構成は分離して扱う。

除外条件は取込時には適用せず、ledger には事実として記帳する。
除外条件は Fund 向け納品 ZIP の再構成時に適用する。

除外ルールは `hia_delivery_exclusion_rules` で管理し、少なくとも以下を持つ。

- insurer_number
- target_schema
- target_table
- target_column
- match_type
- match_value
- exclusion_reason
- source_note
- is_enabled

また、除外条件評価のため、ledger 側には医療機関を特定できる情報
（例: `facility_code`, `facility_name`, `insurer_number`）
を保持する前提とする。

v1 実装では、`hia_delivery_exclusion_rules` を delivery layer で適用し、主に `facility_code` による契約外医療機関除外を行う。

## Consequences

この設計により以下が保証される。

- ZIP 単位の再処理が安全
- 人物識別が安定
- 年度単位の履歴管理が可能
- `hia_person_years` と `hia_xml_events` の分離により event 粒度拡張が容易
- 納品再構成時の除外ルールを ledger から分離して管理できる
- ix08 / su08 を原文保持で最小書換えする方針を維持できる

## Related Documents

- `docs/spec/hia_fund_ledger_xml/README.md`
- `docs/spec/hia_fund_ledger_xml/flow_overview.md`
- `docs/spec/hia_fund_ledger_xml/identity_and_normalization.md`
- `docs/spec/hia_fund_ledger_xml/error_policy.md`
- `docs/spec/hia_fund_ledger_xml/year_rule.md`
- `docs/spec/hia_fund_ledger_xml/delivery_exclusion_rules.md`
- `docs/spec/hia_fund_ledger_xml/er_overview.md`
- `docs/spec/hia_fund_ledger_xml/delivery_exclusion_rules.md`