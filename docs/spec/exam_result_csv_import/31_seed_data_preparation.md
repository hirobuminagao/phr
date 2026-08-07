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
9. `0008_fill_base_norm_variants_from_export_sql.sql`
10. `0009_fix_questionnaire_2003_norm_codes.sql`
11. `0010_add_questionnaire_variants_from_runtime_errors.sql`
12. `0011_add_sapporo_kitahachi_csv_mapping.sql`
13. `0012_add_kitahachi_runtime_variants.sql`
14. `0013_add_exam_result_source_values.sql`
15. `0014_add_murakami_runtime_variants.sql`
16. `0015_fix_result_code_variant_code_systems.sql`

既に `0000` から `0003` を適用済みの環境でも、旧export辞書を未投入の場合は `0008` を追加適用する。
`0004` 以降は実装・検証で増えた差分seedであり、実行環境の最終適用済み地点に応じて追加適用する。

実行環境へ追加反映するDB変更はDB別に管理する。

`dev_phr` migration:

1. `20260805_001_dev_phr_add_optional_exam_item_master_from_normalize_errors.sql`
2. `20260806_001_dev_phr_add_gastric_cancer_risk_exam_item_master.sql`
3. `20260806_002_dev_phr_add_clia_tumor_marker_exam_item_master.sql`

`phr_master` migration:

1. `20260806_004_phr_master_create_exam_item_output_policies.sql`

`phr_master` seed:

1. `0015_fix_result_code_variant_code_systems.sql`

上記を適用しただけでは既に取り込んだ `exam_item_values` のnormalize結果は自動更新されない。
既存データへ反映するには、対象CSV/XMLを `--include-imported` 等で再取込し、その後 `03_00`、`03_01`、`03_02`、`03_04`、必要に応じて `03_05` を再実行する。

`0008_fill_base_norm_variants_from_export_sql.sql` は、旧 `sql/export_sql/norm_variants.sql` の812件を `phr_master.norm_variants` へ補充するseedである。
`INSERT IGNORE` のため、既に投入済みの辞書やCSVサンプルで追加済みの揺れは上書きしない。
実行環境で旧export辞書を未投入のまま `0002` / `0007` だけを当てた場合、CD/CO項目の基本値まで `NORMALIZE_VARIANT_NOT_FOUND` になるため、追加seedとして適用する。

`0009_fix_questionnaire_2003_norm_codes.sql` は、付属1の `1.2.392.200119.6.2003`（問診結果コード: `1=はい`, `2=いいえ`）に合わせ、旧辞書由来の `Y` / `N` 正規化値を `1` / `2` へ補正するseedである。

`0010_add_questionnaire_variants_from_runtime_errors.sql` は、実行環境でCSV再取込後に残ったCD/COの表記揺れを追加する差分seedである。初期追加値は、喫煙の「以前は吸っていたが、最近 1 ヶ月間は吸っていない」、飲酒頻度の「飲酒（週5～6日）」、脂質服薬の「コレステロール　薬剤治療中」（`9N711` の結果コードOID `1.2.392.200119.6.2003`）、便潜血の「（＋）」である。
`Y` / `N` や `true` / `false` はraw aliasとして受け止めてもよいが、CDの `normalized_code` は付属1のコード値に寄せる。

`0015_fix_result_code_variant_code_systems.sql` は、実行環境へ適用してよい `phr_master.norm_variants` の安定補正seedである。対象は、陽性/陰性OID `1.2.392.200119.6.2100` と尿定性OID `1.2.392.200119.6.2102` の `code_system` 欠落補正、およびカッコ付き `+` / `-` など実運用で必要なaliasである。
匿名化済みnormalizeエラーfixtureを保存する `health_exam_result.exam_item_value_normalize_error_fixtures` と、そのimportスクリプトはm4検証・辞書整備用であり、実行環境には適用しない。

`dev_phr.exam_item_master` の任意項目追加は、正式に採用したものだけ `sql/migrations/dev_phr/20260805_001_dev_phr_add_optional_exam_item_master_from_normalize_errors.sql` のような実行環境向けmigrationで行う。m4でfixture CSVを検証して作った追加候補は `sql/dev_tools/candidates/add_optional_exam_item_master_from_fixtures_v2_candidate.sql` に置き、実行環境にはそのまま適用しない。
対象候補は、CSV/XML健診結果で実際に出現し、検査結果として受け止めるべき血清アミラーゼ、BUN、尿pH、尿定性、便潜血、標準体重、婦人科細胞診、腫瘍マーカー、BNP/NT-proBNP、骨密度、胃がんリスク検査などである。
施設独自の総合判定・指導区分、標準コードとして判断できない `Z...` / `ZG...` 系項目、既存標準項目との同一視に確認が必要な項目はこの差分では追加しない。これらはマッピング除外、施設確認、または別レイヤーの標準化分析対象とする。

`sql/migrations/dev_phr/20260806_001_dev_phr_add_gastric_cancer_risk_exam_item_master.sql` は、m4 fixture候補から胃がんリスク検査系を正式昇格したmigrationである。対象はヘリコバクターピロリ抗体IgG判定、ABCD分類、ヘリコバクターピロリ抗体、ペプシノゲン1、ペプシノゲン2、ペプシノゲン1/2比、ペプシノゲン判定である。これらは検査結果として受け止める項目であり、出力ポリシーseedでは止めない。
`sql/migrations/dev_phr/20260806_002_dev_phr_add_clia_tumor_marker_exam_item_master.sql` は、CA125とCA19-9のCLIA法variantを正式昇格したmigrationである。既存のその他/不明method版と同じく任意腫瘍マーカー値として受け止め、出力ポリシーseedでは止めない。

施設確認後に「XMLへそのまま出す」または「証跡のみ残してXMLへ出さない」を切り替える項目は、normalize辞書や `exam_item_master` だけで表現しない。
`phr_master.exam_item_output_policies` に `exam_facility_id` と `namecode` 単位で登録し、`INCLUDE` / `EXCLUDE` / `REVIEW_REQUIRED` を指定する。
全施設共通ルールは `exam_facility_id = 0`、施設別判断は実際の `exam_facility_id` を指定する。
policy未登録の項目は `INCLUDE` として扱うため、seed不足だけで既存出力を止めない。

単位不一致は、共通normalize libの単位aliasで「同一単位の表記揺れ」だけ吸収する。例として `/uL`, `/μL`, `/㎕` は血球数系で使われる `/mm3` と同義として扱う。eGFRの `ml/min/{1.73m2}`, `ml/min./1.73m2`, `ml/min/1.7`, `ml/分` は、namecodeがeGFRで値域も同等であるため、厚労省サンプルXMLおよび付属2系マスタのXML単位に合わせて `ml/min/1.73m2` へ寄せる。数値換算が必要な真の単位差は引き続き `UNIT_MISMATCH` のまま残す。

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
