# HIA_fund_ledger_xml

## 概要
`HIA_fund_ledger_xml` は、HIA システムから月締めでダウンロードされる健診結果 ZIP / XML を対象に、
Fund 向け納品のための **人単位・年度単位の台帳（ledger）を構築する補助パイプライン** である。

本フローは既存の `medi_*` 系処理とは **別フローで管理**するが、XML 由来の項目・正規化ロジックなどは可能な限り共通化する。

v1.1.0 では、人物識別に関わる正規化・canonicalization・ID生成は、HIA_fund_ledger_xml 固有の実装として増やすのではなく、全パイプライン共通の identity 共通lib（`scripts/lib/identity/`）を前提に整理する。

そのため、本ディレクトリ配下の spec は HIA_fund_ledger_xml 固有の要件を定義しつつ、人物識別そのものの実装責務は identity 共通lib 側に置く方針とする。

---

# 位置づけ

この機能は PHR 本体の ETL とは独立した補助処理として扱う。

```
HIA
  ↓
ZIP (月締め)
  ↓
ZIP import
  ↓
XML検証 / 正規化
  ↓
hia_xml_events ledger
  ↓
hia_person_years ledger
  ↓
納品対象抽出
  ↓
Fund納品 ZIP 再構成
```

---

# 基本設計方針

## フロー分離

- `medi_*` フローとは別管理
- ただし以下は共通思想とする

|項目|方針|
|---|---|
|正規化|共通ロジック使用|
|XML項目|medi系と可能な限り同名|
|ETLログ|既存設計を参考|

## identity 共通lib 前提

人物識別に関わる以下の処理は、HIA_fund_ledger_xml 個別の実装ではなく、全世界観共通の identity 共通lib を前提とする。

- 項目別正規化
- match 値生成
- person_id_custom 生成
- identity_hash 生成

HIA_fund_ledger_xml 側 spec では、何を入力とし、どの粒度で照合し、どの値を保持するかを定義する。

実際の生成ロジックは `scripts/lib/identity/` 配下の共通libに従う。

---

# 人物識別

同一人物判定キー

```
person_id_custom
+ name_kana_norm
+ gender_code
+ exam_year
```

ただし v1.1.0 では、`person_id_custom` は人物識別の主要な補助キーとして扱い、最終的なイベント台帳系との接続では `subscriber_id` を実体参照キーとして扱う前提とする。

本 README における人物識別は、HIA XML import 時点で人物を年度粒度へ寄せるための識別方針を示す。

## person_id_custom の元データ

以下から生成

- 保険者番号
- 記号
- 番号
- 生年月日

---

# 正規化方針

人台帳は **元値と正規化値の両方を保持**する。

例

|列|説明|
|---|---|
|name_kana|元値|
|name_kana_norm|正規化後|
|insurance_symbol|元値|
|insurance_symbol_norm|正規化後|
|insurance_number|元値|
|insurance_number_norm|正規化後|

XML 読込時も **同一正規化関数**で変換して照合する。

ここでいう同一正規化関数とは、HIA_fund_ledger_xml 専用実装ではなく、identity 共通lib（`scripts/lib/identity/`）に定義された primitive / base_norm / field / builder の各レイヤを指す。

本パイプラインは、共通libで生成された match 値・person_id_custom・identity_hash を前提に人物年度台帳を構築する。

---

# 年度

健診年度はカレンダー年ではなく設定値で判定する。

例

```
2025年度
2025-04-01
〜
2026-03-31
```

初期実装では `.env` 設定で年度開始日を管理する。

---

# 日付と順序

## dl_date

`dl_date` は

**フォルダ名 / ZIP 名から取得する業務日付**。

これは

- HIA の伝送日
- データの業務到着日

を意味する。

スクリプト実行日時とは別物。

## created_at / updated_at

DB 監査用。

|列|意味|
|---|---|
|created_at|DB登録日時|
|updated_at|更新日時|

順序判定には使用しない。

---

# 初回登場判定

初回登場は以下で決定する。

```
dl_date
+ send_seq
```

send_seq は厚労省ファイル伝送仕様の送信回数部分。

これにより

- 後から古い ZIP を取り込んでも
- 初回登場順序が崩れない

---

# エラー方針

以下は必須項目。

|項目|扱い|
|---|---|
|genderCode|必須|
|exam_date|必須|

これらが欠落する XML が含まれる場合

**ZIP 単位で未記帳**。

処理方針

```
ZIP読み込み
↓
XML検証
↓
エラーあり
    ↓
    ZIP ERROR
    ↓
    台帳未記帳
```

修正後に再アップロード・再取込する。

---

# 台帳構造（v1 実装）

本台帳は HIA_fund_ledger_xml 固有の ledger であるが、人物識別に用いる canonical 値は全世界観共通の identity 共通lib に従って生成する。

## person_year ledger

```text
person_id_custom
name_kana_norm
gender_code
exam_year
insurer_number
insurance_symbol
insurance_number
insurance_symbol_match
insurance_number_match
birthdate
identity_hash
first_seen_dl_date
last_seen_dl_date
dl_count
```

## hia_xml_events ledger

```
xml_event_id
person_year_id
xml_filename
xml_sha256
exam_date
facility_code
facility_name
zip_id
```

---


# 納品対象抽出ルール（v1 実装）

Fund 向け納品 ZIP を再構成する際、対象 XML は以下の順序で抽出される。

1. **対象 dl_date の ZIP を基準にする**

   対象月は `hia_import_zips.dl_date` で指定する。

2. **過去同一人物（同一年度）を除外**

   以下キーが一致する人物が過去の dl_date に存在する場合は除外する。

   ```
   person_id_custom
   + name_kana_norm
   + gender_code
   + exam_year
   ```

3. **除外ルール適用**

   `hia_delivery_exclusion_rules` に登録された条件を適用する。

   v1 実装では主に以下を使用。

   - `facility_code` = 契約外医療機関

4. **XML 重複除外**

   `xml_sha256` を用いて同一 XML を除外する。

5. **報告区分による件数集計**

   XML 内の `report_category` を使用する。

   - `report_category = 10` → 特定健診

6. **ix08 / su08 再生成**

   納品 ZIP 生成時に index / summary を再構成する。

   |項目|算出方法|
   |---|---|
   |ix08 totalRecordCount|DATA フォルダ XML 件数|
   |su08 totalSubjectCount|report_category = 10 件数|

ix08 / su08 は **原文 XML を保持したまま必要な値のみ書き換える**。

# 将来拡張

将来的には以下を追加予定。

- event / person_event / event_instance との接続
- 年2回以上健診対応
- 自動納品 ZIP 生成

---

# 今後このディレクトリに追加するドキュメント

```
/docs/spec/hia_fund_ledger_xml/

README.md
flow_overview.md
identity_and_normalization.md
error_policy.md
year_rule.md
```

identity 関連の確認は、まず以下の共通 spec を参照する。

- docs/spec/identity_canonicalization/README.md
- docs/spec/identity_canonicalization/identity_layer_structure.md
- docs/spec/identity_canonicalization/identity_layers_norm_and_purpose.md
- docs/spec/identity_canonicalization/v1.1.0_identity_layer_commonization.md

---

# ステータス

本仕様は **v1 実装完了（2026‑03）時点の内容をベースにしつつ、v1.1.0 に向けて identity 共通lib前提・event 接続前提へ更新中** とする。

実装済み機能

- HIA ZIP 取込パイプライン
- XML 検証
- person_year ledger 構築
- xml event ledger 構築
- 過去登場者除外ロジック
- 納品対象抽出
- Fund 向け納品 ZIP 再構成
- ix08 / su08 自動再生成
- HIA_fund_ledger_xml 固有要件と identity 共通lib との接続整理（進行中）

対応スクリプト

- `hia_import_zip.py`
- `hia_parse_xml.py`
- `hia_build_delivery_zip.py`

今後のドキュメント更新方針

1. 実装により設計と差異が出た箇所を本ディレクトリ内ドキュメントへ記録
2. 設計変更が発生する場合は ADR に記録
3. v2 以降の拡張は本 README ではなく個別ドキュメントで管理
4. identity 関連は個別パイプライン spec ではなく、共通 spec を正として参照関係を保つ