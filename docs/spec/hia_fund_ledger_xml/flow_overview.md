# HIA_fund_ledger_xml Flow Overview

このドキュメントは **HIA_fund_ledger_xml の処理フロー全体**を俯瞰するための概要図を示す。

目的は次の通り。

- HIA から取得した ZIP/XML の処理順序を明確化
- 人照合と台帳更新の関係を整理
- 月次最新スナップショット運用を固定
- Fund 納品データ生成までの流れを固定

---

# 全体フロー

```text
HIA SYSTEM

  ↓

月締め ZIP ダウンロード

  ↓

ZIP import

  ↓

ZIP名解析
  ├ facility_code
  ├ insurer_number
  ├ dl_date
  └ send_seq

  ↓

ZIP 展開

  ↓

DATA 配下の h*.xml 一覧取得

  ↓

XML 検証
  ├ genderCode
  ├ exam_date
  ├ 個人識別必須項目
  └ XML構造

  ↓

エラー判定（ZIP単位）

  ├ エラーあり
  │      ↓
  │   ZIP ERROR
  │      ↓
  │   error.txt 出力
  │      ↓
  │   DB未記帳
  │
  └ エラーなし
         ↓

人物識別キー生成

insurance_symbol_match
insurance_number_match
birth_yyyymmdd
name_kana_norm
name_kana_full_match
person_id_custom
identity_hash
exam_year

         ↓

hia_import_zips 更新

・zip_name 単位で UPSERT
・zip_sha256 一致なら skip
・同名ZIPで内容差分がある場合は再取込

         ↓

hia_xml_events 最新化

・対象 zip_id 配下を一旦 is_deleted=1
・今回存在する XML イベントを is_deleted=0 で更新/復帰
・今回初出のイベントを追加
・今回存在しなかったイベントは is_deleted=1 のまま残す

         ↓

hia_person_years 再集計（人物×年度スナップショット）

・is_deleted=0 の xml_event のみ集計
・dl_count = 有効イベント件数
・0件なら last_seen_* を NULL に戻す
・1件以上なら最新イベントで last_seen_* を更新
・人物×年度単位の最新状態を保持する集約テーブル

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

```text
HIA
 ↓
ZIP
```

---

## 2 Validation Layer

XML の構造と必須項目を検証。

```text
XML
 ↓
必須項目チェック
 ↓
ZIP単位 all-or-nothing エラー判定
 ↓
成功時のみ DB 記帳
```

---

## 3 Identity Layer

人物照合。

```text
insurance_symbol_match
insurance_number_match
birth_yyyymmdd
name_kana_norm
name_kana_full_match
person_id_custom
gender_code
exam_year
identity_hash
```

---

## 4 Ledger Layer

台帳更新。

```text
import zip ledger
xml event ledger
person year ledger
```

---

# 月次最新スナップショット方針

- 当月 ZIP は同一 zip_name でも中身が可変
- 前月以前の ZIP は固定
- ZIP は zip_name 単位で最新状態を保持する
- zip_sha256 一致時は再処理を skip する
- XMLファイル名は識別子に使わない
- 同名 XML でも別人物へ差し替わる可能性がある
- XMLイベントは物理削除せず `is_deleted` で状態管理する
- ZIP単位で XML を全件検証し、成功時のみ DB 記帳する

---

# 台帳関係

```text
hia_import_zips

  ├ zip_name (UNIQUE)
  ├ zip_sha256
  ├ dl_date
  ├ send_seq
  └ import_status

        │ 1:N
        ▼

hia_xml_events

  ├ person_year_id
  ├ zip_id
  ├ exam_date
  ├ facility_code
  ├ xml_sha256
  ├ xml_filename
  ├ is_deleted
  └ dl_date（zip 経由）

        │ N:1
        ▼

hia_person_years

  ├ first_seen_dl_date
  ├ last_seen_dl_date
  ├ last_seen_zip_name
  ├ last_seen_xml_filename
  └ dl_count
```

---

# hia_xml_events 更新ルール

一意イベントの暫定キーは以下とする。

```text
(person_year_id, zip_id, exam_date, facility_code)
```

補足:

- `facility_code` の NULL は空文字へ正規化する
- `xml_filename` は識別には使わない
- ZIP再取込時は対象 `zip_id` 配下を一旦 `is_deleted=1` にする
- 今回存在したイベントのみ `is_deleted=0` に戻す
- これにより「前はいたが今回消えた人」を追跡可能にする

---

# hia_person_years 再集計ルール

`hia_person_years` は `hia_xml_events(is_deleted=0)` を集約して更新する。
本テーブルはログではなく、人物×年度単位の「最新スナップショット」を保持する集約テーブルである。

- `dl_count` は有効イベント件数
- 有効イベントが 0 件の場合:
  - `dl_count = 0`
  - `last_seen_dl_date = NULL`
  - `last_seen_zip_name = NULL`
  - `last_seen_xml_filename = NULL`
- 有効イベントが 1 件以上ある場合:
  - `last_seen_*` は最新イベントから再設定する
- XMLイベントの履歴は `hia_xml_events` に保持し、`hia_person_years` は常に再計算で再現可能とする

---

# 将来拡張ポイント

今後以下を追加予定。

- event / person_event / event_instance との接続
- 年2回以上健診対応
- 自動 Fund 納品 ZIP 生成
- 月次最新スナップショット再構築バッチ

---

# ステータス

v1 実装完了（2026‑03）。

本フローは実装上、zip_name 単位の最新 ZIP 管理と、hia_xml_events の is_deleted による最新有効集合管理を採用している。

主な実装スクリプト

- hia_import_zip.py
- hia_parse_xml.py
- hia_build_delivery_zip.py
```