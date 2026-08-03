# Import CSV

開発・保守用master data loaderの入力CSV置き場。

CSV本体はgit管理しない。日本郵便の郵便番号masterを更新する場合は、公式CSVを以下の名前で配置する。

```text
scripts/dev_tools/import_csv/utf_ken_all.csv
```

配置後、dry-runで確認する。

```powershell
python scripts/dev_tools/import_postal_code_addresses.py
```

DBへ反映する。

```powershell
python scripts/dev_tools/import_postal_code_addresses.py --apply --replace
```
