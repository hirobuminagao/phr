# System support snapshots

通常処理では扱えない不具合・実装漏れ・仕様漏れの対象を、修正前後で保存するための保守用スクリプトです。

## 初期設定

1. `sql/migrations/phr_system_support/20260901_001_phr_system_support_create_support_snapshots.sql`
2. `sql/seed/phr_system_support/0001_phr_system_support__xml_author_import_gap.sql`

## 事象1: XML author取込漏れ

XMLを再取り込みする前に、健診項目マスタへauthor親子定義を反映する。

Windows実行環境（`dev_phr`）:

```text
sql/migrations/dev_phr/20260901_001_dev_phr_add_annex2_author_item_code.sql
sql/seed/dev_phr/0007_dev_phr__exam_item_master_annex2_authors.sql
```

ローカルDocker（`m4_dev_phr`）:

```text
sql/migrations/m4_dev_phr/20260902_001_m4_dev_phr_add_annex2_author_item_code.sql
sql/seed/m4_dev_phr/0001_m4_dev_phr__exam_item_master_annex2_authors.sql
```

既に列が存在する環境ではmigrationを再実行せず、seedのみ適用する。適用後は
`annex2_author_item_code IS NOT NULL` が18件であることを確認してから再取り込みへ進む。

修正前の対象確認:

```bash
python scripts/support_scripts/capture_snapshot.py --incident-id 1 --phase BEFORE --dry-run
```

修正前スナップショットの確定:

```bash
python scripts/support_scripts/capture_snapshot.py --incident-id 1 --phase BEFORE
```

再取込とcase採用値再作成後の差分確認:

```bash
python scripts/support_scripts/capture_snapshot.py --incident-id 1 --phase AFTER --dry-run
```

差分を保存し、修正前に出力済みだった復旧caseを出力リストへ登録:

```bash
python scripts/support_scripts/capture_snapshot.py --incident-id 1 --phase AFTER --create-export-list
```

出力リストは事象・eventごとに作成され、同じコマンドを再実行した場合は既存リストを再利用します。

既定では`queries/001.sql`を実行します。事象を追加するときは`support_incidents`へ登録し、同じ事象IDを3桁にしたSQLファイルを`queries`へ追加します。

抽出SQLは`target_type`と`target_id`を必ず返します。`event_id`、`exam_export_case_id`、`source_exam_ledger_id`、`reprocess_required`、`reexport_required`を返すと検索用列にも保存され、SELECT結果全体は`JSON`として保持されます。

## 重複した健診caseの統合

保険者番号の補正前後などで同じ受診が複数caseへ分裂した場合は、通常のcase生成でsourceを移動せず、専用コマンドを使う。

事前に次のmigrationを適用する。

```text
sql/migrations/health_exam_result/20260903_001_health_exam_result_add_case_lifecycle.sql
```

最初は必ずdry-runする。

```bash
python scripts/support_scripts/merge_exam_export_cases.py \
  --target-case-id 51072 \
  --source-case-id 123073 \
  --reason "保険者番号補正前後に分裂した同一受診caseの統合"
```

表示されたidentity、source件数、レビュー・補正競合、出力履歴を確認後、同じ引数へ `--apply` を追加して実行する。
統合元caseは削除されず `MERGED` となり、過去の出力リスト・XML出力履歴も元case IDのまま保持される。
