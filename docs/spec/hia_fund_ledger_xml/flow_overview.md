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

01 HIAダウンロードZIP取込

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
  │   hia_download_zips / hia_download_xmls にエラーとして記帳
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

hia_download_zips 更新

・zip_name 単位で UPSERT
・zip_sha256 一致なら skip
・同名ZIPで内容差分がある場合は再取込

         ↓

hia_download_xmls 最新化

・ZIP内XMLごとの原本台帳を更新
・今回存在するXMLを is_active_in_zip=1 として保持
・同一ZIP再取込時はXML sha / parse状態を更新

         ↓

hia_person_years 再集計（人物×年度スナップショット）

・parse_status=OK かつ is_active_in_zip=1 の XML を集計
・dl_count = 有効イベント件数
・0件なら last_seen_* を NULL に戻す
・1件以上なら最新イベントで last_seen_* を更新
・人物×年度単位の最新状態を保持する集約テーブル

         ↓

02 健保納品リスト作成

・対象受診月
・未提出/再提出/全件などの出力ポリシー
・初期出力単位は grouping_mode=ALL
・送信元コードは 1322100106
・同一人物・同一受診日が複数ある場合の新旧選択
・exclusion_rules 適用
・fund_delivery_lists / fund_delivery_list_members 更新

         ↓

03 Fund 納品用 ZIP 再構成

・DATA XML コピー
・ix08 totalRecordCount 再計算
・su08 totalSubjectCount 再計算

         ↓

04 提出済み反映

・リスト単位で提出済みにする
・member単位、人年度単位の提出済み状態を更新
```

---

# 外部Run入口

番号付きで `scripts/hia/` 直下に置くのは、人がRunボタンを押す工程だけとする。

| script | 工程 | 備考 |
| --- | --- | --- |
| `01_import_downloaded_xml_zip.py` | HIA ZIP取込 | 実装済み |
| `02_create_fund_delivery_list.py` | 納品リスト作成 | 候補選定などは `script_lib` |
| `03_export_fund_delivery_zip.py` | 健保納品ZIP出力 | 受診月単位/全件モード対応 |
| `04_mark_fund_delivery_submitted.py` | 提出済み反映 | 画面化時は一括提出済みボタン |

内部処理は `scripts/hia/script_lib/` に逃がし、番号付きスクリプトを増やしすぎない。

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
hia_download_zips

  ├ zip_name
  ├ zip_sha256
  ├ dl_date
  ├ send_seq
  └ import_status

        │ 1:N
        ▼

hia_download_xmls

  ├ person_year_id
  ├ download_zip_id
  ├ exam_date
  ├ facility_code
  ├ xml_sha256
  ├ xml_filename
  ├ is_active_in_zip
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

# hia_download_xmls 更新ルール

XML原本台帳は、HIAからダウンロードしたZIP内のXML 1ファイルを1行として保持する。

主な考え方:

- `download_zip_id + xml_inner_path` をZIP内XMLの基本識別とする。
- XMLファイル名だけを人物識別には使わない。
- XML本文から抽出した個人識別項目で `person_id_custom` / `identity_hash` を生成する。
- 必須項目不足は `parse_status=ERROR` として台帳に残す。
- 正常XMLのみ `hia_person_years` / `hia_person_xml_events` へ反映する。

---

# hia_person_xml_events 更新ルール

XML原本を人×年度へ紐付けた履歴を保持する。

初期実装では `parse_status=OK` のXMLを `LINKED` として登録し、`hia_person_years` のスナップショットを更新する。

将来的に手動除外、差替え、納品採用などを人履歴へ残す場合は、以下の `event_type` を使う。

- `LINKED`
- `UNLINKED`
- `SUPERSEDED`
- `DELIVERY_SELECTED`
- `DELIVERED`
- `DELIVERY_EXCLUDED`

---

# hia_person_years 再集計ルール

`hia_person_years` は `hia_download_xmls(parse_status=OK, is_active_in_zip=1)` と `hia_person_xml_events` を元に更新する。
本テーブルはログではなく、人物×年度単位の「最新スナップショット」を保持する集約テーブルである。

- `dl_count` は有効イベント件数
- 有効イベントが 0 件の場合:
  - `dl_count = 0`
  - `last_seen_dl_date = NULL`
  - `last_seen_zip_name = NULL`
  - `last_seen_xml_filename = NULL`
- 有効イベントが 1 件以上ある場合:
  - `last_seen_*` は最新イベントから再設定する
- XMLイベントの履歴は `hia_person_xml_events` に保持し、`hia_person_years` は再計算で再現可能とする

---

# 将来拡張ポイント

今後以下を追加予定。

- event / person_event / event_instance との接続
- 年2回以上健診対応
- 自動 Fund 納品 ZIP 生成
- 月次最新スナップショット再構築バッチ

---

# ステータス

v2 再構築中（2026-08）。

本フローは、HIAダウンロードZIP/XML台帳を `hia_download_*`、人への紐付け履歴を `hia_person_*`、健保納品管理を `fund_delivery_*` に分ける。

主な実装スクリプト

- `scripts/hia/01_import_downloaded_xml_zip.py`
- `scripts/hia/script_lib/hia_download_importer.py`
- `scripts/hia/script_lib/hia_download_xml_parser.py`
```
