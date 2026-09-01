# System support snapshots

通常処理では扱えない不具合・実装漏れ・仕様漏れの対象を、修正前後で保存するための保守用スクリプトです。

## 初期設定

1. `sql/migrations/phr_system_support/20260901_001_phr_system_support_create_support_snapshots.sql`
2. `sql/seed/phr_system_support/0001_phr_system_support__xml_author_import_gap.sql`

## 事象1: XML author取込漏れ

修正前の対象確認:

```bash
python scripts/support_scripts/capture_snapshot.py --incident-id 1 --phase BEFORE --dry-run
```

修正前スナップショットの確定:

```bash
python scripts/support_scripts/capture_snapshot.py --incident-id 1 --phase BEFORE
```

既定では`queries/001.sql`を実行します。事象を追加するときは`support_incidents`へ登録し、同じ事象IDを3桁にしたSQLファイルを`queries`へ追加します。

抽出SQLは`target_type`と`target_id`を必ず返します。`event_id`、`exam_export_case_id`、`source_exam_ledger_id`、`reprocess_required`、`reexport_required`を返すと検索用列にも保存され、SELECT結果全体は`JSON`として保持されます。
