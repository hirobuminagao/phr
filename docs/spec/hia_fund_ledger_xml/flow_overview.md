

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

ZIP 展開

  ↓

XML 一覧取得

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

person_year ledger 照合

  ├ 既存
  │     ↓
  │  last_seen 更新
  │
  └ 新規
        ↓
     person_year 登録

         ↓

xml ledger 登録

(person_year_id
 xml_filename
 zip_name
 dl_date)

         ↓

Fund 納品対象抽出

・対象年度
・過去登場無し

         ↓

納品用 ZIP 再構成
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
person_year ledger

  │
  ├ first_seen_dl_date
  ├ last_seen_dl_date
  └ dl_count

        │
        │ 1:N
        ▼

xml ledger

  ├ xml_filename
  ├ zip_name
  └ dl_date
```

---

# 将来拡張ポイント

今後以下を追加予定。

- XML SHA256 による重複検出
- 健診イベント台帳
- 年2回以上健診対応
- 自動 Fund 納品 ZIP 生成

---

# ステータス

現在は **設計整理フェーズ**。

実装順序。

1. 設計整理
2. ADR
3. DDL 作成
4. スクリプト実装