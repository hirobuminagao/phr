# 健診event人単位状態ツール

健診機関からの受領処理そのものではなく、健診eventに対する人単位の状態を作るためのツールです。

## `sync_person_event_population.py`

指定した `event_id` の保険者番号を `dev_phr.event` から取得し、同じ保険者番号の
`dev_phr.subscribers` 全員を `dev_phr.person_event` へ同期します。

結果ファイル受領済みの人だけでなく、未受領者、資格喪失者も含めたevent母集団を作るための
スクリプトです。資格喪失者は除外せず、`person_event_status_items` に
`QUALIFICATION_STATUS` / `QUALIFICATION_LOST_DATE` として状態を保持します。
`subscribers` に加入者が追加された場合や、氏名・資格喪失日・identity系情報が更新された場合も、
このスクリプトを再実行すると `person_event` と母集団系status itemをupsertします。

件数だけ確認します。この実行ではDBを変更しません。

```powershell
python scripts/health_exam_event/sync_person_event_population.py --dry-run
```

人単位のevent母集団を更新します。

```powershell
python scripts/health_exam_event/sync_person_event_population.py
```

## `sync_person_event_status_items.py`

指定した `event_id` の `health_exam_result.exam_ledgers` を、作成済みの人単位親
`dev_phr.person_event` と可変状態項目 `dev_phr.person_event_status_items`
へ同期します。

`person_event` の母集団は `sync_person_event_population.py` で先に作ります。
未突合ledgerは `exam_ledgers` の未突合状態として残し、加入者確定後に人単位へ反映します。
`person_event_status_items` は `person_event_id + item_code` の縦持ちで、
代表 `exam_ledger_id`、check件数、出力可能件数、出力済み件数などを保持します。
このスクリプトは結果状態系のitemだけを更新し、母集団系の資格状態itemは削除しません。

実行前にmigrationを適用します。

```powershell
Get-Content sql/migrations/dev_phr/20260803_001_dev_phr_create_person_event.sql -Raw |
  mysql -u USER -p

Get-Content sql/migrations/dev_phr/20260803_002_dev_phr_create_person_event_status_items.sql -Raw |
  mysql -u USER -p
```

件数だけ確認します。この実行ではDBを変更しません。

```powershell
python scripts/health_exam_event/sync_person_event_status_items.py --dry-run
```

人単位状態を更新します。

```powershell
python scripts/health_exam_event/sync_person_event_status_items.py
```

## 基本の実行順

```text
scripts/from_medical/dev_tools/sync_exam_ledgers.py
scripts/from_medical/03_check_exam_results.py --ledger-type EXAM
scripts/health_exam_event/sync_person_event_population.py
scripts/health_exam_event/sync_person_event_status_items.py
scripts/from_medical/dev_tools/refresh_exam_result_ledger_report.py
```
