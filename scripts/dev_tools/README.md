# Dev Tools

番号付き業務フローには入らない、開発・保守・master data整備用のツールを置く。

`scripts/from_medical/dev_tools/` は健診結果取込フロー周辺の報告・確認用とし、このディレクトリは `phr_master` など共通masterの整備やseed生成に使う。

## `import_postal_code_addresses.py`

日本郵便公式の `utf_ken_all.csv` を `phr_master.postal_code_addresses` へ取り込む。

事前にDDLまたはmigrationを適用する。

```powershell
Get-Content sql/migrations/phr_master/20260731_004_phr_master_create_postal_code_addresses.sql -Raw |
  mysql -u USER -p
```

dry-runでCSVの内容を確認する。この実行ではDBを変更しない。

```powershell
python scripts/dev_tools/import_postal_code_addresses.py
```

DBへ反映する。

```powershell
python scripts/dev_tools/import_postal_code_addresses.py --apply
```

既存行を全削除してから入れ直す。

```powershell
python scripts/dev_tools/import_postal_code_addresses.py --apply --replace
```

`--source` 省略時は `scripts/dev_tools/import_csv/utf_ken_all.csv` を読む。
日本郵便の最新版CSVを取得したら、この場所へ `utf_ken_all.csv` という名前で配置する。
CSV本体はgit管理しない。

## `import_exam_item_value_normalize_error_fixtures.py`

実行環境から持ち帰った、匿名化済み・集計済みの `exam_item_values` normalizeエラーCSVを、m4検証環境の `health_exam_result.exam_item_value_normalize_error_fixtures` へ取り込む。

このテーブルは正式seedではなく、`norm_variants` 追加候補の確認、意図的に残すエラーの整理、回帰確認用の観察テーブルである。
原則としてm4検証環境だけに作成し、実行環境には作成・投入しない。
個人ID、氏名、ファイルパス、CSV行全体は入れない。

実行環境からCSVを作るときは、以下の抽出SQLだけを使う。
単位不一致の調査に必要な `raw_unit`, `normalized_unit`, `master_display_unit`, `master_ucum_unit` も出力する。
実行環境側にfixtureテーブルやfixture用migrationは不要である。

```text
sql/dev_tools/extract_exam_item_value_normalize_error_fixtures.sql
```

m4検証環境では、事前にDDLまたはmigrationを適用する。

```powershell
Get-Content sql/migrations/health_exam_result/20260805_003_health_exam_result_create_exam_item_value_normalize_error_fixtures.sql -Raw |
  mysql -u USER -p
```

既にfixtureテーブルを作成済みのm4検証環境では、単位列追加migrationも適用する。

```powershell
Get-Content sql/migrations/health_exam_result/20260806_003_health_exam_result_add_units_to_normalize_error_fixtures.sql -Raw |
  mysql -u USER -p
```

CSVは以下へ配置する。CSV本体はgit管理しない。

```text
scripts/dev_tools/import_csv/exam_item_values_error_20260805.csv
```

dry-runでCSVの内容を確認する。この実行ではDBを変更しない。

```powershell
python scripts/dev_tools/import_exam_item_value_normalize_error_fixtures.py
```

DBへ反映する。

```powershell
python scripts/dev_tools/import_exam_item_value_normalize_error_fixtures.py --apply
```

同じ `source_label` の既存行を削除してから入れ直す。

```powershell
python scripts/dev_tools/import_exam_item_value_normalize_error_fixtures.py --apply --replace
```

別ファイル名や別ラベルで取り込む場合:

```powershell
python scripts/dev_tools/import_exam_item_value_normalize_error_fixtures.py `
  --source scripts/dev_tools/import_csv/exam_item_values_error_20260805.csv `
  --source-label exam_item_values_error_20260805 `
  --apply --replace
```
