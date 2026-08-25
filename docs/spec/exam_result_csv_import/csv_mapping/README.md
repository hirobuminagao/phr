# 健診結果CSVマッピング解析

## 目的

健診機関ごとの健診結果CSVサンプルを確認し、`phr_master.csv_format_versions`、`csv_exam_result_mapping_rules`、`csv_exam_result_mapping_conditions`、`norm_variants` へ投入する前の判断を残す。

このディレクトリは、CSVマッピング作業の入口として使う。既存の施設別レビュー資料は当面移動せず、ここから参照する。

## 作業単位

- 1つのseedは、原則として1健診機関または1CSVフォーマットの追加分だけにする。
- 既存マッピング全量を再投入しない。
- サンプルCSV、ヘッダー、値の例、法定/特定健診への影響、未マッピング理由をセットで確認する。
- 判断に迷う列は、無理にマッピングせず `要確認` として残す。

## 既存の施設別解析

| 健診機関/パターン | 解析資料 | サンプル |
|---|---|---|
| ヒロオカクリニック Pattern A | `../23_hirooka_clinic_pattern_a_review.md` | `../samples/hirooka_clinic/` |
| ハートクロス健診プラザ赤坂駅前 Pattern B | `../24_heartcross_akasaka_pattern_b_review.md` | `../samples/heartcross_akasaka/` |
| 小禄病院 Joined Pattern C | `../25_oroku_hospital_joined_pattern_c_review.md` | `../samples/oroku_hospital/` |
| 村上医院 紙/CSV | `../samples/murakami_iin/README.md` | `../samples/murakami_iin/` |
| ヘルスケアクリニック厚木 | `../samples/healthcare_clinic_atsugi/README.md` | `../samples/healthcare_clinic_atsugi/` |
| 渋谷ウエストヒルズクリニック | `../samples/shibuya_westhills_clinic/README.md` | `../samples/shibuya_westhills_clinic/` |
| 円山クリニック 2026-06 | `20260825_maruyama_clinic_mapping_review.md` | ローカル受領サンプルのみ。CSV本体はGit管理しない。 |

## 横断資料

- `../21_mapping_rule_structure_examples.md`
  - `csv_exam_result_mapping_rules` / `csv_exam_result_mapping_conditions` の表現例。
- `../31_seed_data_preparation.md`
  - 既存seed準備、含める範囲、意図的に含めない列の考え方。
- `../33_implementation_status_and_xml_handoff.md`
  - CSV取込からXML出力へ渡せる情報と、実装済み範囲。

## 今後の解析メモ雛形

新しい健診機関CSVを解析する場合は、以下の項目を残す。

```text
# <健診機関名> CSVマッピング解析

## 対象

- 健診機関:
- 施設コード:
- 元CSV:
- 想定mapping_version:

## CSV構造

- 文字コード:
- 区切り文字:
- ヘッダー行:
- データ開始行:
- 複数行ヘッダー:
- 同名ヘッダー:

## 基本情報

- 氏名/氏名カナ:
- 生年月日:
- 性別:
- 記号/番号/枝番:
- HIA加入者ID:
- 健診実施日:
- 健診機関側ドキュメントID:

## 検査値マッピング

- 直接マッピング:
- 条件付きマッピング:
- 複数列結合:
- 検査方法分岐:
- CD/CO/ST normalize:

## 法定/特定健診チェック影響

- 法定チェック不足候補:
- 特定健診チェック不足候補:
- 法定と重なるため特定側で二重管理しない項目:

## 未マッピング列

- 意図的に除外:
- 健診機関確認:
- 後続検討:

## seed方針

- 追加seed:
- norm variant seed:
- alias更新:
```
