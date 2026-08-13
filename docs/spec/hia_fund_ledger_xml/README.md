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
01 HIAダウンロードZIP取込
  ↓
内部処理: XML検証 / 正規化 / 人年度台帳反映
  ↓
hia_download_xmls
  ↓
hia_person_years ledger
  ↓
hia_person_xml_events
  ↓
02 健保納品リスト作成
  ↓
03 健保納品ZIP出力
  ↓
04 提出済み反映
```

---

# スクリプト配置方針

番号付きで `scripts/hia/` 直下に置くのは、人がRunボタンを押す業務工程だけとする。

細かい処理は `scripts/hia/script_lib/` へ置き、直下の番号付きスクリプトから呼び出す。

目的:

- 実行順を人が迷わないようにする。
- 画面化したときのボタン単位とCLI単位を揃える。
- 内部処理を細かく分けても、運用入口を増やしすぎない。

想定する外部Run入口:

| script | 人が押す工程 | 内部で行う主な処理 |
| --- | --- | --- |
| `01_import_downloaded_xml_zip.py` | HIAから落としたZIPを取り込む | ZIP名解析、XML抽出、XML検証、identity生成、`hia_download_*` / `hia_person_*` 更新 |
| `02_create_fund_delivery_list.py` | 納品対象リストを作る | 候補抽出、重複候補選定、未提出/再提出判定、`fund_delivery_*` リスト更新 |
| `03_export_fund_delivery_zip.py` | 納品ZIPを出力する | リストに含まれるXMLを集め、受診月単位でZIPを再構成、出力履歴更新 |
| `04_mark_fund_delivery_submitted.py` | 提出済みにする | リスト/メンバー/人年度の提出済み状態を更新 |

`02` 以降で必要になる候補選定、同日重複の新旧選択、除外ルール適用、サマリー集計は、個別スクリプトとして表へ出さず `script_lib` に分ける。

## 管理画面の扱い

`apps/health_exam_admin` に HIA → 健保納品の確認画面と実行ボタンを追加している。

ただし、2026-08-10時点では **正式運用投入前** とする。

理由:

- 画面からの納品リスト削除が未実装。
- 同一 `event_id` / `insurer_number` / `exam_month` の READY / CREATED リストが複数残った場合、`03` で複数リストが出力対象になり得る。
- 出力対象リストを画面上で最終確認してから実行する導線がまだ不足している。

そのため、画面は当面、疎通確認・状態確認・実装検証用として扱う。
正式運用では、リスト削除、同一月リスト重複防止、出力対象確認、提出済み記帳の操作が揃ってから使用開始する。

## 初期出力単位

健保向け納品では、初期実装は健診機関単位に分割せず、健保向けにまとめて出力する。

- `grouping_mode = ALL`
- 送信元コードは `1322100106`
- `fund_delivery_lists` にリスト作成時の `grouping_mode` / `sender_code` を保持する。
- `fund_delivery_runs` に出力時点の `grouping_mode` / `sender_code` を写す。

後続バージョンで健診機関単位出力が必要になった場合は、画面のリスト作成時に `ALL` / `BY_FACILITY` を切り替え、`03_export_fund_delivery_zip.py` が `grouping_mode` に従って出力グループを分ける。

## CLI実行例

HIA ZIP取込:

```bash
python scripts/hia/01_import_downloaded_xml_zip.py --event-id 2
```

健保納品リスト作成 dry-run:

```bash
python scripts/hia/02_create_fund_delivery_list.py --event-id 2 --exam-month 202605
```

健保納品リスト作成 apply:

```bash
python scripts/hia/02_create_fund_delivery_list.py --event-id 2 --exam-month 202605 --confirm
```

`02` は `--confirm` を付けない限りDBへリストを作らない。

`fund_delivery.yml` では、`list.exam_months` に複数月を指定できる。
この場合、`02` は月ごとに `fund_delivery_lists` を作成する。

```yaml
list:
  output_mode: EXAM_MONTH
  exam_months:
    - "202605"
    - "202606"
```

注意:

- `02` はリスト履歴を新規作成する処理であり、同一条件の既存リストを自動削除しない。
- 試行で作成した READY / CREATED リストを残したまま `03` を未指定実行すると、出力対象に含まれる可能性がある。
- 03未実行の試行リストは、`fund_delivery_list_members` → `fund_delivery_lists` の順で整理する。

健保納品ZIP出力 dry-run:

```bash
python scripts/hia/03_export_fund_delivery_zip.py --delivery-list-id 1
```

健保納品ZIP出力 apply:

```bash
python scripts/hia/03_export_fund_delivery_zip.py --delivery-list-id 1 --confirm
```

`03` は `fund_delivery_lists` / `fund_delivery_list_members` に固定された対象だけを出力する。
候補の再選定は行わない。

`delivery_list_id` 未指定の場合は、READY / CREATED の出力待ちリストを月順で出力する。
複数月指定で作成したリストを一括出力するための挙動であり、同一月の試行リストが複数残っている場合は重複出力の原因になる。

初期実装の出力先は `data/fund_delivery/output/{yyyymmdd_hhmmss}/{exam_month}/`。
ZIP名は `送信元コード_保険者番号_提出日同日作成回数_実施区分コード.zip` 形式とする。
提出日は任意指定可能で、未指定時は実行日を使用する。
同日作成回数と実施区分コードはどちらも `0-9` の一桁とする。
同日作成回数 `auto` の場合、同一提出日・送信元・保険者番号・実施区分コードの既存出力を見て、0始まりで採番する。
実施区分コードは健診結果のため `1` とする。
例: `1322100106_06139463_202608100_1.zip`

ZIPには、ZIP名から拡張子を除いたルートフォルダを含める。
解凍時は `1322100106_06139463_202608100_1/` の下に `DATA/`、`XSD/`、`ix08_V08.xml`、`su08_V08.xml` が展開される構造とする。

ZIP内の個人XMLファイル名は厚労省の送付用ファイルアーカイブ仕様に合わせ、`h送信元コード提出日同日作成回数実施区分コード連番6桁.xml` とする。
例: `h13221001062026081001000001.xml`

`03` はZIPと同じフォルダに、確認用CSVも出力する。

|ファイル|内容|用途|
|---|---|---|
|`{zip_stem}_summary.csv`|`delivery_run_id`, `delivery_list_id`, `output_zip_name`, `insurer_number`, `exam_month`, `facility_code`, `facility_name`, `person_count`|健診機関・受診月ごとの納品人数確認|
|`{zip_stem}_members.csv`|出力XMLファイル名、元XML/元ZIP、`person_year_id`, `hia_download_xml_id`, `exam_date`, `facility_code`, `report_category_code` 等|ZIP内XMLと原本XMLの追跡、再確認|
|`{zip_stem}_person_raw.csv`|`delivery_run_id`, `delivery_list_id`, `output_zip_name`, `output_xml_filename`, `person_year_id`, `identity_hash`, `facility_code`, `facility_name`, `exam_month`, `exam_date`, `birthdate`, `fiscal_year_end_age`, `report_category_code`, `program_type_code`, `source_zip_name`, `source_xml_filename` 等|健保向け累計集計の素材。毎回分を追記して、Excel等で累計・重複制御・月別/健診機関別集計を行う|

CSVはExcelで開きやすいように UTF-8 BOM付きで出力する。
個人氏名などの機微情報は初期実装ではCSVへ出さず、DB上の台帳IDから追跡する。
累計集計用のraw CSVは、氏名・記号番号を出さず、`identity_hash` と出力/元ZIP名、健診機関、受診月、受診日、生年月日、年度末年齢を中心に出す。
年度末年齢は `hia_person_years.exam_year` の翌年3月31日時点で計算する。

健保提出済み反映 dry-run:

```bash
python scripts/hia/04_mark_fund_delivery_submitted.py --delivery-list-id 1 --all --submitted-by nagao
```

健保提出済み反映 apply:

```bash
python scripts/hia/04_mark_fund_delivery_submitted.py --delivery-list-id 1 --all --submitted-by nagao --confirm
```

`04` は個人単位を基本にする。
`--all` はリスト内の全 `fund_delivery_members` を処理するショートカット。

個別に反映する場合:

```bash
python scripts/hia/04_mark_fund_delivery_submitted.py --delivery-list-id 1 --delivery-member-id 10 --submitted-by nagao --confirm
```

状態は以下を扱う。

|状態|意味|
|---|---|
|`SUBMITTED`|健保へ提出済み|
|`SUBMISSION_ERROR`|提出時エラー|
|`PENDING`|提出保留|

`SUBMITTED` の場合のみ、`fund_delivery_person_status.delivery_tracking_status` を `DELIVERED` に進める。
提出エラーや保留は、個人の最終提出状態を進めず、member/list/run の作業状態として残す。

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

# v2 再構築方針

2026-08時点で、HIAから健保へのXML納品工程は、既存 `scripts/work_folder` 実装を参考にしつつ、`scripts/hia` と `health_exam_result` の正式台帳へ再構築する方針とする。

v2では、現行 `hia_xml_events` が兼ねている以下の責務を分ける。

- HIAからダウンロードしたXML原本台帳
- 人×年度へのXML紐付け履歴
- 健保納品ZIP作成履歴
- 健保納品メンバー履歴

採用する主なテーブル名は以下とする。

- `health_exam_result.hia_download_zips`
- `health_exam_result.hia_download_xmls`
- `health_exam_result.hia_person_years`
- `health_exam_result.hia_person_xml_events`
- `health_exam_result.fund_delivery_runs`
- `health_exam_result.fund_delivery_members`
- `health_exam_result.fund_delivery_exclusion_rules`

詳細は以下を参照する。

- `02_v2_rebuild_design.md`

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

これらが欠落する XML が含まれる場合、v2ではZIP/XML台帳へエラーとして記帳する。

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
    hia_download_zips / hia_download_xmls にエラー状態を記帳
```

修正後に再アップロード・再取込する。

---

# 台帳構造（v2 正式）

v2では、旧 `hia_xml_events` のようにXML原本台帳と人イベントを兼ねる構造にしない。

XML原本台帳は `hia_download_xmls`、人への紐付け履歴は `hia_person_xml_events`、健保納品管理は `fund_delivery_*` に分ける。

## hia_person_years

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

## hia_download_xmls

```
hia_download_xml_id
download_zip_id
xml_filename
xml_inner_path
xml_sha256
exam_date
exam_month
facility_code
facility_name
parse_status
parse_reason
person_id_custom
identity_hash
```

## hia_person_xml_events

```
person_xml_event_id
person_year_id
hia_download_xml_id
download_zip_id
event_type
event_status
is_current
```

---

# 納品対象抽出ルール（v2）

Fund 向け納品 ZIP を再構成する際、対象 XML は以下の順序で抽出される。

1. **出力リストを基準にする**

   画面またはCLIで作成された `fund_delivery_lists` / `fund_delivery_list_members` を対象にする。

2. **受診月・提出状態で候補を絞る**

   基本は受診月単位。必要に応じて全件モードも使用する。

3. **同一人物・同一受診日の候補を選ぶ**

   以下キーが一致する候補が複数ある場合、新旧どちらを採用するかポリシーで決める。

   ```
   person_id_custom
   + exam_date
   ```

4. **除外ルール適用**

   `fund_delivery_exclusion_rules` に登録された条件を適用する。

5. **ix08 / su08 再生成**

   納品 ZIP 生成時に index / summary を再構成する。

   |項目|算出方法|
   |---|---|
   |ix08 totalRecordCount|DATA フォルダ XML 件数|
   |su08 totalSubjectCount|report_category_code = 10 件数|

ix08 / su08 は **原文 XML を保持したまま必要な値のみ書き換える**。

---

# v1 実装の扱い

旧 `work_other` / `scripts/work_folder` 実装は参考元とする。

v2の正式実装では、旧DB/旧スクリプトのテーブル名をそのまま採用しない。

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

本仕様は **v2 再構築中（2026-08）** とする。

実装済み機能

- HIA ZIP 取込入口: `scripts/hia/01_import_downloaded_xml_zip.py`
- XML検証 / identity生成 / `hia_download_*` / `hia_person_*` 更新
- HIA fund delivery サンプルデータ生成
- 納品リスト作成入口: `scripts/hia/02_create_fund_delivery_list.py`
- 健保納品ZIP出力入口: `scripts/hia/03_export_fund_delivery_zip.py`
- 提出済み反映入口: `scripts/hia/04_mark_fund_delivery_submitted.py`
- ix08 / su08 自動再生成
- FastAPI管理画面からの確認・実行入口
- FastAPI管理画面からのZIP単位/個人XML単位の提出済み・提出エラー・保留記帳

対応スクリプト

- `scripts/hia/01_import_downloaded_xml_zip.py`
- `scripts/hia/02_create_fund_delivery_list.py`
- `scripts/hia/03_export_fund_delivery_zip.py`
- `scripts/hia/04_mark_fund_delivery_submitted.py`
- `scripts/hia/script_lib/hia_download_importer.py`
- `scripts/hia/script_lib/hia_download_xml_parser.py`

正式運用前の残項目

- 管理画面からの納品リスト削除
- 同一月READY/CREATEDリスト重複時の停止または選択UI
- 出力対象リストの画面確認
- 提出済み/エラー記帳の運用詳細確認

画面運用の考え方

- `scripts/hia/01_import_downloaded_xml_zip.py`、`02_create_fund_delivery_list.py`、`03_export_fund_delivery_zip.py`、`04_mark_fund_delivery_submitted.py` の順繰り実行は引き続き維持する。
- FastAPI管理画面では、順繰り実行ボタンに加えて、出力履歴ごとの一括記帳と個人XML単位の個別記帳を行えるようにする。
- 個人XML単位の状態は `fund_delivery_members.member_status`、ZIP単位の状態は `fund_delivery_runs.delivery_status`、リスト全体の状態は `fund_delivery_lists.list_status` に集約する。
- HIAアップロードで全件正常なら、出力履歴単位で `SUBMITTED` に更新する。
- 一部だけHIAアップロードエラーの場合は、該当個人XMLを `SUBMISSION_ERROR` とし、再対応待ちは `PENDING` に戻せるようにする。
- `fund_delivery_person_status.delivery_tracking_status` は、`SUBMITTED` の時だけ `DELIVERED` に進める。提出エラーや保留は作業状態として記帳し、過去の提出済み事実を自動で壊さない。

今後のドキュメント更新方針

1. 実装により設計と差異が出た箇所を本ディレクトリ内ドキュメントへ記録
2. 設計変更が発生する場合は ADR に記録
3. v2 以降の拡張は本 README ではなく個別ドキュメントで管理
4. identity 関連は個別パイプライン spec ではなく、共通 spec を正として参照関係を保つ
