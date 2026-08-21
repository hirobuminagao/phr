# Health Exam Admin

社内ローカルで健診結果の確認・出力作業を行うための管理画面。

## 初期スコープ

- `phr_app` のユーザーでログインする。
- DB設定ロール/権限を読み、ログイン後画面に表示する。
- 出力リスト画面への入口を持つ。
- 受領ファイル、統合ledger、HIA XML出力リスト、HIA→健保納品の状態確認入口を持つ。
- HOMEの作業カードは押下後に処理中表示を出し、同じ画面遷移の連打を抑止する。
- 個人case一覧は初期500件表示とし、健診機関は受領実績がある候補から複数選択する。
- 個人case一覧のsummaryタイルは、出力可能、理由ありOK、BLOCKED、出力済み、XML+CSVの絞り込み入口として使う。
- HIAアップロード作業画面は、ZIPの親行と個人XMLの子行を分け、出力フォルダパスをコピーしやすいテキストエリアで表示する。

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

## m4ローカル起動（Docker）

m4ローカルでは、管理画面はDockerで起動する。
Windows実行環境のように `python -m uvicorn ...` を直接実行しない。

Codexに「m4ローカルのサイトを起動して」と依頼された場合は、この節を見て起動する。

前提:

- Docker Desktopを起動しておく。
- MySQLコンテナ `tokuho_mysql` が起動している。
- DB接続情報は `scripts/.env` を `--env-file` でコンテナへ渡す。
- `scripts/.env` はgit管理しない。
- コンテナ内からホスト側MySQLへ接続するため、起動時に `PHR_DB_HOST=host.docker.internal` を上書きする。
- 管理画面は `http://127.0.0.1:8011/login` で開く。

起動手順:

```bash
cd /Users/hiro/work/phr
open -a Docker
docker ps
```

`tokuho_mysql` が出ていることを確認する。

最新ソースを反映して管理画面イメージを作り直す。

```bash
docker build -f apps/health_exam_admin/Dockerfile -t phr-health-exam-admin:latest .
```

既存の管理画面コンテナがあれば止める。

```bash
docker rm -f phr-health-exam-admin
```

管理画面コンテナを起動する。

```bash
docker run -d \
  --name phr-health-exam-admin \
  --env-file scripts/.env \
  -e PHR_DB_HOST=host.docker.internal \
  -e PHR_ADMIN_HOST=0.0.0.0 \
  -p 8011:8011 \
  phr-health-exam-admin:latest
```

起動確認:

```bash
docker logs --tail 80 phr-health-exam-admin
curl -I http://127.0.0.1:8011/login
```

`curl -I` は `HEAD` のため `405 Method Not Allowed` になることがある。
ログイン画面の確認はGETで行う。

```bash
curl -s -o /tmp/phr_admin_login.html -w '%{http_code}\n' http://127.0.0.1:8011/login
```

`200` が返れば起動OK。

ブラウザ:

```text
http://127.0.0.1:8011/login
```

よくある間違い:

- m4でローカルPythonへ `python-multipart` などを入れて起動しようとしない。m4はDockerイメージ内の `requirements.txt` で依存を持つ。
- `--reload` はm4 Docker起動では使わない。ソース変更後は再ビルドしてコンテナを起動し直す。
- `scripts/.env` は `.dockerignore` でイメージに入らないため、必ず `--env-file scripts/.env` を付ける。
- Docker内で `PHR_DB_HOST=localhost` にすると管理画面コンテナ自身を見に行く。m4では `host.docker.internal` を使う。

## Docker起動（一般例）

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

LAN内の特定端末だけ許可する場合:

```text
PHR_ADMIN_HOST=0.0.0.0
PHR_ADMIN_ALLOWED_CLIENT_IPS=192.168.1.25,192.168.1.26
PHR_ADMIN_TRUST_PROXY_HEADERS=0
```

`PHR_ADMIN_ALLOWED_CLIENT_IPS` を設定すると、記載したIP以外はログイン画面にも到達できない。
未設定の場合は、ユーザーごとのログイン時IP制限だけを使う。
接続元IPは既定でFastAPIが直接受けたIPを見る。
Caddy等の信頼済みプロキシ配下で `X-Forwarded-For` を使う場合だけ、`PHR_ADMIN_TRUST_PROXY_HEADERS=1` にする。

起動:

```powershell
cd C:\Users\1107858.KSMD\work\phr
python -m uvicorn apps.health_exam_admin.main:app --host 0.0.0.0 --port 8011 --reload
```

ブラウザ:

```text
http://127.0.0.1:8011
```

`0.0.0.0` で起動しても、起動PC自身のブラウザからは `http://127.0.0.1:8011` で開ける。

別PCのブラウザからは以下の形式で開く。

```text
http://<起動しているPCのIP>:8011
```

`--reload` はローカル確認用。ソースを更新すると自動で反映される。

VSCodeとは別ウィンドウで起動したい場合は、以下を使う。

```powershell
scripts\dev_tools\start_health_exam_admin.cmd
```

デスクトップに起動ショートカットを作る場合は、初回だけ以下を実行する。

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\dev_tools\create_health_exam_admin_desktop_shortcut.ps1
```

作成された `PHR Health Exam Admin` ショートカットをダブルクリックすると、管理画面用のPowerShellがVSCodeとは別に起動する。
このショートカット起動は既定で `0.0.0.0` 待ち受けのため、同じLAN内の別PCからもアクセスできる。
起動PCだけに閉じたい場合は、環境変数 `PHR_ADMIN_HOST=127.0.0.1` を設定して起動する。
