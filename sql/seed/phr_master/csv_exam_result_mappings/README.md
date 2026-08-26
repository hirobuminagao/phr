# 健診結果CSVマッピングseed

## 目的

健診機関ごとの健診結果CSVマッピング追加分を管理する。

今後追加するCSVマッピングseedは、このディレクトリへ配置する。
既存のseedは互換性と適用履歴を優先し、当面 `sql/seed/phr_master/` 直下のまま残す。

## 命名

```text
YYYYMMDD_NNN_<facility_slug>.sql
```

例:

```text
20260825_001_hirooka_clinic.sql
20260825_002_oroku_hospital.sql
20260825_003_shibuya_westhills_clinic.sql
```

## 作成方針

- 1ファイルは原則として1健診機関または1CSVフォーマットの追加分だけにする。
- 既存マッピング全量を再投入しない。
- `INSERT ... ON DUPLICATE KEY UPDATE` を使い、再実行できるseedにする。
- 先頭コメントに対象施設、施設コード、元CSV、目的、関連ドキュメントを残す。
- 旧版を止める場合は削除ではなく `is_active = 0` を基本にする。
- `csv_format_versions`、`csv_exam_result_mapping_rules`、`csv_exam_result_mapping_conditions`、必要な `norm_variants` を影響範囲に入れる。
- `medical_folder_aliases.csv_format_version_id` を更新する場合は、alias更新を同じseedに含めるか、別seedとして明示する。

## 既存seed

既存のCSVマッピング関連seedは、現在 `sql/seed/phr_master/` 直下にある。

| seed | 内容 |
|---|---|
| `0001_draft_csv_exam_result_format_mappings_samples.sql` | ヒロオカ、厚木、渋谷、小禄、ハートクロスの初期CSVフォーマット/マッピング |
| `0003_disable_oroku_csv_facility_code_mapping.sql` | 小禄CSVの施設コードマッピング調整 |
| `0005_add_heartcross_appended_postal_address_mapping.sql` | ハートクロスの住所/郵便番号追加マッピング |
| `0006_add_murakami_iin_paper_csv_mapping.sql` | 村上医院の紙/CSVマッピング |
| `0011_add_sapporo_kitahachi_csv_mapping.sql` | 札幌北八条系CSVマッピング |
| `0016_add_oroku_questionnaire_mapping.sql` | 小禄病院の特定健診問診マッピング追加 |
| `0017_add_oroku_questionnaire_runtime_variants.sql` | 小禄病院問診の表記揺れ追加 |

`norm_variants` 追加seedもCSV取込に関係するが、値辞書の性質が強いため、必要に応じてこのREADMEから参照する。

## 関連ドキュメント

- `docs/spec/exam_result_csv_import/csv_mapping/README.md`
  - 健診結果CSVマッピング解析の入口。
- `docs/spec/exam_result_csv_import/31_seed_data_preparation.md`
  - 既存seed準備と判断メモ。
