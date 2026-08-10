# HIA_fund_ledger_xml Error Policy

このドキュメントは `HIA_fund_ledger_xml` における **エラー判定** と **未記帳ポリシー** を整理するためのメモである。

本タスクでは、人物照合や年度判定に必要な項目が欠ける XML を中途半端に記帳しないことを最優先とする。
そのため、**ZIP 単位 all-or-nothing** を基本方針とする。

---

# 目的

- 何をエラーとみなすかを固定する
- どの粒度で未記帳にするかを固定する
- 修正後の再アップロード / 再取込の流れを明確にする
- 台帳を中途半端な状態にしない

---

# 基本方針

## ZIP 単位 all-or-nothing

1 つの ZIP に含まれる XML のうち、1 件でも重大エラーがあれば、その ZIP 全体を **未記帳** とする。

つまり以下を徹底する。

- 人台帳に途中まで記帳しない
- XML 台帳にも途中まで記帳しない
- エラー内容だけを残す
- 修正後に ZIP を再アップロード / 再取込する

---

# 重大エラー（現時点）

現時点で freeze 対象の重大エラーは以下。

|error_code|内容|扱い|
|---|---|---|
|GENDER_CODE_EMPTY|`genderCode` が空|ZIP 未記帳|
|EXAM_DATE_MISSING|`exam_date` が無い|ZIP 未記帳|
|XML_PARSE_ERROR|XML 構造を正しく読めない|ZIP 未記帳|
|PERSON_KEY_BUILD_FAILED|人物識別キー生成に必要な値が不足|ZIP 未記帳|

---

# 必須項目

人物照合・年度判定のため、現時点では最低限以下を必須とする。

## 人物照合系

- insurer_number
- insurance_symbol
- insurance_number
- birthdate
- name_kana
- genderCode

## 年度判定系

- exam_date

これらが欠ける場合、人物照合または年度判定が成立しないため、正常取込対象にしない。

---

# エラー時の処理

処理フローは以下。

```text
ZIP 読み込み
↓
XML 一覧取得
↓
XML 検証
↓
重大エラーあり
    ↓
    import_status = ERROR
    ↓
    hia_download_zips / hia_download_xmls にエラー状態を記録
    ↓
    hia_person_years 未更新
    ↓
    hia_person_xml_events 未登録
```

重要なのは、**ZIP/XML原本台帳にはエラー内容を記録するが、人年度台帳と人イベント台帳は更新しない** こと。

---

# 記録するエラー情報（v2）

エラーは、少なくとも以下を追跡できるようにする。

- download_zip_id
- zip_name
- xml_filename
- xml_inner_path
- error_code
- parse_reason
- created_at

必要に応じて、複数 XML のエラーを 1 ZIP に紐づけて保持する。

---

# 修正後の再取込

エラー ZIP は、元 XML を修正後に再アップロードし、再取込する前提とする。

```text
ZIP 取込
↓
エラー検出
↓
エラー内容確認
↓
元 XML 修正
↓
再アップロード
↓
再取込
↓
正常時のみ hia_person_years / hia_person_xml_events 反映
```

ここで重要なのは、**初回エラー時点では人年度台帳へ反映しない** こと。
ZIP/XML原本台帳にはエラー証跡を残し、再取込時の原因追跡に使う。

---

# なぜ ZIP 単位にするか

XML 単位で部分成功にすると、以下の事故が起こりやすい。

- 人台帳が途中まで更新される
- `dl_count` が過大になる
- `first_seen` / `last_seen` が壊れる
- 修正後再取込で XML 台帳が重複する
- 「この ZIP は未処理」と言えなくなる

そのため、本タスクでは ZIP 単位で結果をそろえる。

---

# ledger 更新の条件

以下の条件を **すべて満たした XML のみ** 人年度台帳への反映対象とする。

- XML 構造が読める
- `genderCode` が存在する
- `exam_date` が存在する
- 人物照合キーが生成できる

条件を 1 つでも満たさない場合は、`hia_download_xmls.parse_status = ERROR` とし、`hia_person_years` / `hia_person_xml_events` には反映しない。

---

# 今後追加検討するもの

今後、必要に応じて以下もエラー判定対象に追加する可能性がある。

- name_kana 不正値
- insurer_number 不正値
- 記号 / 番号のフォーマット異常

ただし現時点では、人物照合と年度判定に直結するものを優先して freeze 対象とする。

---

# ステータス

v2 再構築中（2026-08）。

本ドキュメントは HIA ZIP/XML 取込時のエラーポリシーを示す。

対応実装

- HIAダウンロードZIP/XML台帳へのエラー記録
- 成功XMLのみ `hia_person_years` / `hia_person_xml_events` へ反映
