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
python scripts/dev_tools/import_postal_code_addresses.py --source C:\path\to\utf_ken_all.csv
```

DBへ反映する。

```powershell
python scripts/dev_tools/import_postal_code_addresses.py --source C:\path\to\utf_ken_all.csv --apply
```

既存行を全削除してから入れ直す。

```powershell
python scripts/dev_tools/import_postal_code_addresses.py --source C:\path\to\utf_ken_all.csv --apply --replace
```

macOSのローカル検証では、`--source` 省略時に `/Users/hiro/Downloads/utf_ken_all.csv` を読む。
