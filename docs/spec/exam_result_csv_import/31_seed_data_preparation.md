# Seed Data Preparation

## Status

Implemented and expanded.

2026-07-30時点で、健診機関・alias、CSV format/mapping、CSVサンプル用normalize辞書のseedを作成済みである。
本書の初期2施設だけを前提とした記載は作成時点の履歴として残し、現在の適用対象と未整備範囲は `33_implementation_status_and_xml_handoff.md` を正とする。

このドキュメントは、CSVフォーマット関連の初期seedを作るための整備メモとして作成したものである。

## Draft SQL

`phr_master` seedの適用順:

1. `0000_generated_exam_facilities_and_aliases_event2.sql`
2. `0001_draft_csv_exam_result_format_mappings_samples.sql`
3. `0002_draft_norm_variants_csv_sample_additions.sql`
4. `0003_disable_oroku_csv_facility_code_mapping.sql`
5. `0004_add_event2_actual_machine_folder_aliases.sql`
6. `0005_add_heartcross_appended_postal_address_mapping.sql`
7. `0006_add_murakami_iin_paper_csv_mapping.sql`
8. `0007_add_urine_qualitative_dash_norm_variants.sql`

既に `0000` から `0003` を適用済みの環境は、実機フォルダ一覧との差分である `0004` だけを追加適用する。

`dev_phr` のローカル検証用seed:

- `9999_local_sample_subscribers_for_csv_import.sql`

これは m4 Docker の `m4_dev_phr` を対象にしたローカル検証専用seedである。実行環境には適用しない。

レビュー用SQL:

- `sql/seed/phr_master/0001_draft_csv_exam_result_format_mappings_samples.sql`

このSQLは以下を作成するドラフトである。

- `phr_master.csv_format_versions`
- `phr_master.csv_exam_result_mapping_rules`
- `phr_master.csv_exam_result_mapping_conditions`

対象サンプル:

- ヒロオカクリニック Pattern A
- ハートクロス健診プラザ赤坂駅前 Pattern B

seedは再実行可能なupsert/delete+insert構成とし、SQL末尾で `COMMIT` する。

## Included Shape

### Hirooka Clinic Pattern A

- `mapping_version = HIROOKA_2026_05_PATTERN_A_V1`
- `header_mode = SINGLE`
- `header_structure_type = SIMPLE_HEADER`
- `header_context_rule = NONE`
- `active_header_row_no = 1`
- `data_start_row_no = 2`
- `character_encoding = CP932`
- `encoding_fallback_policy = ALLOW_COMMON_ENCODINGS`
- `header_sha256 = 5d03088d9aec595715455bdc35b66ee8fa8c7d9d023d61e14d51de52ce98dfd0`

初期seedに含める基本情報:

- 受診日付
- 健診機関番号
- 健診機関名称
- 氏名
- カナ氏名
- 性別
- 生年月日
- 郵便番号
- 住所
- 保険者番号
- 保険記号
- 保険番号
- 保険枝番
- 社員番号

初期seedに含める検査値:

- 身長
- 体重
- BMI
- 腹囲
- 平均 収縮期
- 平均 拡張期
- 尿蛋白
- 尿糖
- 赤血球数
- 血色素量
- AST
- ALT
- γ-GTP
- 空腹時中性脂肪
- 随時中性脂肪
- HDLコレステロール
- LDLコレステロール
- non-HDLコレステロール
- 空腹時血糖
- 随時血糖
- HbA1c
- クレアチニン
- eGFR
- 安静時心電図所見
- 胸部X線所見
- 既往歴
- 自覚症状
- 他覚症状
- 業務歴
- メタボリックシンドローム判定
- 保健指導区分

追加マッピング範囲:

- 血圧1回目・2回目、裸眼・矯正視力（左右）
- 肥満度、尿潜血、尿酸、白血球、ヘマトクリット、血小板
- 総蛋白、総コレステロール、心拍数
- 腹部超音波、胃部X線、胃部内視鏡、便潜血2回
- 第4期質問票22項目、健診前の食事状況
- 心電図、胸部X線、腹部超音波、胃部X線、胃部内視鏡の条件付き所見有無CD

ヒロオカの施設由来ABC判定、カテゴリ総合判定は初期seedでは `exam_item_values` へマッピングしない。
所見有無CDはABC判定からではなく、対応する所見本文が `異常所見なし` か、それ以外の非空値かで生成する。

### Heartcross Akasaka Pattern B

- `mapping_version = HEARTCROSS_2026_05_PATTERN_B_V1`
- `header_mode = WITH_CONTEXT`
- `header_structure_type = GROUPED_VALUE_METHOD`
- `header_context_rule = UPPER_HEADER`
- `active_header_row_no = 2`
- `data_start_row_no = 3`
- `character_encoding = CP932`
- `encoding_fallback_policy = ALLOW_COMMON_ENCODINGS`
- `header_sha256 = 6ce5a7d844a2351c6f1ef97743f023e3c135cac2d669048fe032f4acfcc25544`

初期seedに含める基本情報:

- INSURER_NUMBER
- INSURANCE_CARD_SYMBOL
- INSURANCE_CARD_NUMBER
- INSURANCE_CARD_BRANCH_NUMBER
- NAME_KANA
- BIRTHDAY
- POSTALCODE
- GENDER
- EXAM_DATE
- 住所

ハートクロスCSVには健診日が存在しない。
実装検証は止めず、`exam_date` は別データまたは健診機関回答で確定するまで暫定未設定とする。
実行環境検証では末尾追加列 `GENDER` / `EXAM_DATE` / `住所` を使用する。
郵便番号は既存 `POSTALCODE` を使用し、住所だけ末尾追加列から取り込む。

初期seedに含める検査値:

- 身長
- 体重
- BMI
- 腹囲
- 平均最高血圧
- 平均最低血圧
- 尿蛋白
- 尿糖
- 尿潜血
- 赤血球数
- ヘモグロビン
- 中性脂肪
- HDLコレステロール
- LDLコレステロール
- AST
- ALT
- γ-GTP
- 食後時間
- 空腹時血糖
- HbA1c
- 随時血糖
- クレアチニン
- 尿酸
- 心電図所見
- 胸部X線所見
- 他覚症状
- 既往歴
- 服薬
- 喫煙
- メタボリックシンドローム判定
- 保健指導レベル
- eGFR
- non-HDLコレステロール

ハートクロスの心電図判定と胸部X線判定は、判定等級そのものを保存せず、条件付きルールで標準の所見有無CDを生成する。異常時は対応する所見本文もSTへ格納する。総合判定は施設由来判定として初期seedでは `exam_item_values` へマッピングしない。

## Not Included in the Initial Draft

初期2施設のseedドラフトには以下を含めていなかった。

- `exam_facilities` の実ID確定後の本投入値
- `medical_folder_aliases` の移設seed
- `ANNEX2_IDENTITY` 197件seed
- 入力支援bundle seed
- `norm_variants` seed
- 非測定値語YAML
- `header_snapshot_json` の完全な列配列
- FastAPIテンプレート登録API

## Decisions Identified Before the Initial Insert

初回投入前のレビューでは以下を判断対象としていた。

1. `header_snapshot_json` をSQLに直接持たせるか、CSVから生成するseed補助スクリプトで作るか。
2. ハートクロスの健診日を、別データ補完、前処理、または健診機関回答後のmapping追加のどれで扱うか。
3. `norm_variants` に初期追加する値と、意図的に未登録にしてエラー検証に使う値を分ける。
4. 非測定値語を管理するYAMLの初期語彙を確定する。

ヒロオカ、ハートクロスの `exam_facility_id` は未決事項にしない。
支払基金CSVを `exam_facilities` へ先に取り込み、format/mapping seedでは `medical_institution_code` から `exam_facilities.exam_facility_id` を参照して解決する。
対象コードはヒロオカ `1310438796`、ハートクロス `4011028133` とする。

施設由来判定は初期実装では `raw_row_json` 証跡のみで扱い、将来の専用保持先は後続バージョンで検討する。

## Current Recommendation

当初推奨していた `exam_facilities` / `medical_folder_aliases` の先行投入と、`medical_institution_code` からformat対象施設を解決する構成は実装済みである。
現在は5施設のformat/mapping seedとサンプルで利用した `norm_variants` 追加seedまで整備済みである。
非測定値語YAML、`ANNEX2_IDENTITY` 197件、入力支援bundle、完全な `header_snapshot_json` は未整備であり、現行5施設のCSV取込を止める要件ではないが汎用化の残課題とする。
