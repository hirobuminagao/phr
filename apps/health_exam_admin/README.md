# Health Exam Admin

社内ローカルで健診結果の確認・出力作業を行うための管理画面。

## 初期スコープ

- `phr_app` のユーザーでログインする。
- DB設定ロール/権限を読み、ログイン後画面に表示する。
- 出力リスト画面への入口を持つ。
- 受領ファイル、統合ledger、HIA XML出力リスト、HIA→健保納品の状態確認入口を持つ。

## 実行環境DB反映メモ

画面系は実行環境へ未反映の可能性があるため、反映タイミングは別途確認してから行う。

画面ログイン/権限管理に必要:

- `sql/ddl/phr_app/0000_phr_app__database.sql`
- `sql/ddl/phr_app/0010_phr_app__user_management.sql`
- `sql/migrations/phr_app/20260807_001_phr_app_create_user_management.sql`
- `sql/migrations/phr_app/20260807_002_phr_app_add_user_approval.sql`
- `sql/migrations/phr_app/20260807_003_phr_app_add_user_permission_overrides.sql`
- `sql/migrations/phr_app/20260807_004_phr_app_add_security_settings.sql`

HIA XML出力リスト画面に必要:

- `sql/ddl/health_exam_result/0108_health_exam_result__ops_xml_export_lists.sql`
- `sql/ddl/health_exam_result/0121_health_exam_result__ops_xml_export_list_cases.sql`
- `sql/migrations/health_exam_result/20260806_002_health_exam_result_create_ops_xml_export_lists.sql`
- `sql/migrations/health_exam_result/20260810_001_health_exam_result_fix_ops_xml_export_list_table_names.sql`

健保納品画面は既存の HIA→健保納品テーブルを参照する。
新規環境は `sql/ddl/health_exam_result/0190_health_exam_result__hia_download_ledgers.sql`、`0200_health_exam_result__hia_person_years.sql`、`0210_health_exam_result__fund_delivery.sql` を含むDDL一式を使用する。
既存環境は適用済みmigrationを確認して、不足分だけ新規migrationで補う。

## Docker起動

```bash
docker build -f apps/health_exam_admin/Dockerfile -t phr-health-exam-admin .
docker run --rm -p 8011:8011 \
  -e PHR_DB_HOST=host.docker.internal \
  -e PHR_DB_PORT=3306 \
  -e PHR_DB_USER=root \
  -e PHR_DB_PASSWORD=rootpass \
  -e PHR_APP_DB=phr_app \
  --name phr-health-exam-admin \
  phr-health-exam-admin
```

ブラウザ:

```text
http://localhost:8011
```

## Windows 直起動

実行環境でDockerを使わない場合は、VSCodeのターミナルからFastAPIを直接起動する。

初回のみ依存ライブラリを入れる。

```powershell
cd C:\Users\1107858.KSMD\work\phr
python -m pip install -r apps\health_exam_admin\requirements.txt
```

DB接続は `scripts\.env` または環境変数で指定する。
`scripts\.env` を使う場合の例:

```text
PHR_DB_HOST=localhost
PHR_DB_PORT=3306
PHR_DB_USER=root
PHR_DB_PASSWORD=your_password
PHR_APP_DB=phr_app
PHR_HEALTH_DB=health_exam_result
PHR_MASTER_DB=phr_master
PHR_DEV_DB=dev_phr
```

起動:

```powershell
cd C:\Users\1107858.KSMD\work\phr
python -m uvicorn apps.health_exam_admin.main:app --host 127.0.0.1 --port 8011 --reload
```

ブラウザ:

```text
http://127.0.0.1:8011
```

`--reload` はローカル確認用。ソースを更新すると自動で反映される。
