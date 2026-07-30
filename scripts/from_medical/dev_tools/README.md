# 健診結果ledger報告用ツール

番号付きの取込・チェック・出力フローから独立した、現状確認用のツールです。

## `refresh_exam_result_ledger_report.py`

指定した `event_id` の次のデータを、`health_exam_result.exam_result_ledger_report` に作り直します。

- `health_exam_result.xml_ledger` の全カラム
- `health_exam_result.csv_row_ledger` の全カラム
- `dev_phr.subscribers.relationship_name`
- `dev_phr.subscribers.qualification_lost_date`

XMLとCSVは `ledger_type` で区別します。元ledgerの主キーは `ledger_id`、元ledgerの作成・更新日時は `source_created_at` / `source_updated_at` に格納します。CSVだけに存在する項目はXML行では `NULL`、XMLだけに存在する項目はCSV行では `NULL` です。

`SELECT *` の列順は、報告管理列、`xml_ledger` 全項目の現行順、`relationship_name`、`qualification_lost_date`、CSV固有列の順です。XML項目と加入者2項目はExcel突合用に連続した一塊として保持します。

実行前にmigrationを適用します。

```powershell
Get-Content sql/migrations/health_exam_result/20260730_005_health_exam_result_create_exam_result_ledger_report.sql -Raw |
  mysql -u USER -p

Get-Content sql/migrations/health_exam_result/20260730_006_health_exam_result_reorder_exam_result_ledger_report.sql -Raw |
  mysql -u USER -p
```

`005` を適用済みの環境では、列順を変更する `006` だけを追加適用します。

件数だけ確認します。この実行ではDBを変更しません。

```powershell
python scripts/from_medical/dev_tools/refresh_exam_result_ledger_report.py --dry-run
```

報告テーブルを更新します。

```powershell
python scripts/from_medical/dev_tools/refresh_exam_result_ledger_report.py
```

`--event-id` を省略した場合は `event_id=2` を対象にします。別イベントを更新する場合は、例えば `--event-id 3` のように明示します。

更新は `event_id` 単位の `DELETE + INSERT` です。XML/CSVの元ledgerが両方とも0件の場合は、既存の報告行を消さずに停止します。実行履歴は `health_exam_result.etl_runs` に `phase=REPORT_EXAM_RESULT_LEDGER`、`source=FROM_MEDICAL` として記録します。

更新後は次のように確認できます。

```sql
SELECT *
FROM health_exam_result.exam_result_ledger_report
WHERE event_id = 2
ORDER BY ledger_type, ledger_id;
```
