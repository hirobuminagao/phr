# HIA_fund_ledger_xml Flow Overview

このドキュメントは **HIA_fund_ledger_xml の処理フロー全体**を俯瞰するための概要図を示す。

目的は次の通り。

- HIA から取得した ZIP/XML の処理順序を明確化
- 人照合と台帳更新の関係を整理
- Fund 納品データ生成までの流れを固定

---

# 全体フロー

```
HIA SYSTEM

  ↓

月締め ZIP ダウンロード

  ↓

ZIP import

  ↓

ZIP 展開

  ↓

DATA/XML 一覧取得

  ↓

XML 検証
  ├ genderCode
  ├ exam_date
  └ XML構造

  ↓

エラー判定

  ├ エラーあり
  │      ↓
  │   ZIP ERROR
  │      ↓
  │   台帳未記帳
  │
  └ エラーなし
         ↓

人物識別キー生成

person_id_custom
+ name_kana_norm
+ gender_code
+ exam_year

         ↓

hia_person_years 照合

  ├ 既存
  │     ↓
  │  last_seen 更新
  │
  └ 新規
        ↓
     person_year 登録

         ↓

hia_xml_events 登録

(xml_sha256
 exam_date
 facility_code
 zip_id)

         ↓

納品対象抽出

・対象 dl_date
・過去同一 person_year 除外
・exclusion_rules 適用
・xml_sha256 重複除外

         ↓

Fund 納品用 ZIP 再構成

・DATA XML コピー
・ix08 totalRecordCount 再計算
・su08 totalSubjectCount 再計算
```

---

# レイヤー構造

処理は次の 4 レイヤーで構成される。

## 1 Input Layer

HIA からの ZIP。

```
HIA
 ↓
ZIP
```

---

## 2 Validation Layer

XML の構造と必須項目を検証。

```
XML
 ↓
必須項目チェック
 ↓
ZIP単位エラー判定
```

---

## 3 Identity Layer

人物照合。

```
person_id_custom
name_kana_norm
gender_code
exam_year
```

---

## 4 Ledger Layer

台帳更新。

```
person_year ledger
xml ledger
```

---

# 台帳関係

```
hia_person_years

  │
  ├ first_seen_dl_date
  ├ last_seen_dl_date
  └ dl_count

        │
        │ 1:N
        ▼

hia_xml_events

  ├ xml_filename
  ├ xml_sha256
  ├ exam_date
  ├ facility_code
  └ zip_id
```

---

# 将来拡張ポイント

今後以下を追加予定。

- 健診イベント台帳
- 年2回以上健診対応
- 自動 Fund 納品 ZIP 生成

---

# ステータス

v1 実装完了（2026‑03）。

本フローは現在の実装に合わせて freeze されている。

主な実装スクリプト

- hia_import_zip.py
- hia_parse_xml.py
- hia_build_delivery_zip.py