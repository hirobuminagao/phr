# 健診結果ledger報告用ツール

番号付きの取込・チェック・出力フローから独立した、現状確認用のツールです。

## `sync_exam_ledgers.py`

指定した `event_id` の `xml_ledger` / `csv_row_ledger` を、統合台帳
`health_exam_result.exam_ledgers` と `health_exam_result.exam_ledger_sources`
へ同期します。

このスクリプトは取込本体を変更せず、既存ledgerを統合ledgerへ寄せるためのブリッジです。
既存sourceを再読込した場合も、source ledger ID単位でupsertします。
実行環境で既に取り込んだ `xml_ledger` / `csv_row_ledger` を統合ledgerへ反映する
backfillとしても使用します。

CSVは `csv_row_ledger.file_receipt_id`、XMLは `xml_file_links.file_receipt_id` から
`exam_ledgers.file_receipt_id` と `exam_ledger_sources.file_receipt_id` を復元します。
CSV→XML出力済み状態は `xml_export_members` を正本として参照し、`EXPORTED` を未出力へ戻しません。

実行前にmigrationを適用します。

```powershell
Get-Content sql/migrations/health_exam_result/20260802_001_health_exam_result_create_exam_ledgers.sql -Raw |
  mysql -u USER -p
```

件数だけ確認します。この実行ではDBを変更しません。

```powershell
python scripts/from_medical/dev_tools/sync_exam_ledgers.py --dry-run
```

統合ledgerを更新します。

```powershell
python scripts/from_medical/dev_tools/sync_exam_ledgers.py
```

`--event-id` を省略した場合は `event_id=2` を対象にします。

基本の実行順は以下です。

```text
03_00_check_imported_exam_ledgers.py
03_01_build_exam_export_cases.py
03_02_build_exam_export_case_values.py
03_04_check_exam_export_cases.py
03_05_create_xml_export_list.py
04_export_hia_xml.py
scripts/health_exam_event/sync_person_event_population.py
scripts/health_exam_event/sync_person_event_status_items.py
refresh_exam_result_ledger_report.py
```

人単位event状態の同期は `scripts/health_exam_event/` に移動しました。

## 結合出力用case

旧COMBINED ledger方式の試作スクリプトは削除済みです。
通常運用では `exam_ledgers` を清書ledger化せず、次のcase方式を使用します。

```powershell
python scripts/from_medical/03_01_build_exam_export_cases.py
python scripts/from_medical/03_02_build_exam_export_case_values.py
python scripts/from_medical/03_04_check_exam_export_cases.py
```

既存caseを1件だけ再構築する場合は、3段階すべてへ同じcase IDを指定します。
`03_02` は採用値とcheck状態を作り直すため、必ず続けて `03_04` まで実行します。

```powershell
python scripts/from_medical/03_01_build_exam_export_cases.py --event-id 2 --case-id 3446
python scripts/from_medical/03_02_build_exam_export_case_values.py --event-id 2 --case-id 3446
python scripts/from_medical/03_04_check_exam_export_cases.py --event-id 2 --case-id 3446
```

出力リストもそのcaseだけで新規作成する場合は続けて実行します。

```powershell
python scripts/from_medical/03_05_create_xml_export_list.py --event-id 2 --case-id 3446
```

## `refresh_exam_result_ledger_report.py`

指定した `event_id` の `health_exam_result.exam_ledgers` を、
`health_exam_result.exam_result_ledger_report` に作り直します。

- `health_exam_result.exam_ledgers`
- source XML/CSV ledger由来の主要カラム
- `dev_phr.subscribers.relationship_name`
- `dev_phr.subscribers.qualification_lost_date`

XMLとCSVは `ledger_type` で区別します。統合ledgerの主キーは `exam_ledger_id`、
元source ledgerの主キーは `ledger_id`、元ledgerの作成・更新日時は
`source_created_at` / `source_updated_at` に格納します。
CSVだけに存在する項目はXML行では `NULL`、XMLだけに存在する項目はCSV行では `NULL` です。

`SELECT *` の列順は、報告管理列、`xml_ledger` 全項目の現行順、CSV固有列、`relationship_name`、`qualification_lost_date`、`refreshed_at` の順です。加入者2項目は末尾の更新日時直前に一塊として保持します。

実行前にmigrationを適用します。

```powershell
Get-Content sql/migrations/health_exam_result/20260730_005_health_exam_result_create_exam_result_ledger_report.sql -Raw |
  mysql -u USER -p

Get-Content sql/migrations/health_exam_result/20260730_006_health_exam_result_reorder_exam_result_ledger_report.sql -Raw |
  mysql -u USER -p

Get-Content sql/migrations/health_exam_result/20260730_007_health_exam_result_move_subscriber_columns_to_report_end.sql -Raw |
  mysql -u USER -p

Get-Content sql/migrations/health_exam_result/20260802_002_health_exam_result_add_exam_ledger_id_to_report.sql -Raw |
  mysql -u USER -p
```

`006` を適用済みの環境では、加入者2列の位置だけを変更する `007` を追加適用します。
`20260802_002` は、統合ledger起点で報告行を追跡するための `exam_ledger_id` を追加します。

通常運用では、XML/CSV importが直接 `exam_ledgers` を作成します。
`sync_exam_ledgers.py` は旧個別ledgerからの初回移行、復旧、再構築用です。

source単位の法定チェックを実行します。

```powershell
python scripts/from_medical/03_00_check_imported_exam_ledgers.py
```

結合出力用caseを作成し、採用値を作成した後、case単位の法定チェックを実行します。

```powershell
python scripts/from_medical/03_01_build_exam_export_cases.py
python scripts/from_medical/03_02_build_exam_export_case_values.py
python scripts/from_medical/03_04_check_exam_export_cases.py
```

件数だけ確認します。この実行ではDBを変更しません。

```powershell
python scripts/from_medical/dev_tools/refresh_exam_result_ledger_report.py --dry-run
```

報告テーブルを更新します。

```powershell
python scripts/from_medical/dev_tools/refresh_exam_result_ledger_report.py
```

`--event-id` を省略した場合は `event_id=2` を対象にします。別イベントを更新する場合は、例えば `--event-id 3` のように明示します。

更新は `event_id` 単位の `DELETE + INSERT` です。統合ledgerが0件の場合は、
既存の報告行を消さずに停止します。実行履歴は `health_exam_result.etl_runs` に
`phase=REPORT_EXAM_RESULT_LEDGER`、`source=FROM_MEDICAL` として記録します。

更新後は次のように確認できます。

```sql
SELECT *
FROM health_exam_result.exam_result_ledger_report
WHERE event_id = 2
ORDER BY ledger_type, ledger_id;
```

## 健診機関別エラー率VIEW

`20260730_010_health_exam_result_create_facility_error_rate_view.sql` を適用すると、
`health_exam_result.exam_result_facility_error_rate` で健診機関別の人数と法定チェックエラー率を確認できます。

```sql
SELECT *
FROM health_exam_result.exam_result_facility_error_rate
WHERE event_id = 2
ORDER BY error_rate_percent DESC, total_person_count DESC;
```

- エラーは最終法定チェックの `check_status = 'NG'` とする。
- `error_rate_percent` は未チェックを含む全人数に対するエラー率とする。
- `checked_error_rate_percent` は `OK / NG / WARNING` の判定済み人数だけを分母にする。
- 人数は、同じ施設内で `subscriber_id`、次に `identity_hash` を使って名寄せする。どちらもない行だけledger単位で数える。
- `source_result_count` は名寄せ前のXML/CSV ledger行数であり、`total_person_count` との差から重複元データの有無を確認できる。
