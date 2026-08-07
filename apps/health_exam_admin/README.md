# Health Exam Admin

社内ローカルで健診結果の確認・出力作業を行うための管理画面。

## 初期スコープ

- `phr_app` のユーザーでログインする。
- DB設定ロール/権限を読み、ログイン後画面に表示する。
- 出力リスト画面への入口を持つ。

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
