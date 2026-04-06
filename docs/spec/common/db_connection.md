

# DB接続方針（PHR共通）

## 1. 基本方針

DB接続設定は schema 単位ではなく、host 単位で管理する。

接続情報（host, port, user, password）は `.env` に集約し、
利用する schema（例: dev_phr, work_other）はスクリプト側で指定する。

---

## 2. .env 管理項目

```env
PHR_DB_HOST=
PHR_DB_PORT=3306
PHR_DB_USER=
PHR_DB_PASSWORD=
```

- host / port / user / password は機微情報として `.env` で管理する
- schema 名は `.env` には含めない

---

## 3. schema の扱い

schema は業務設計の一部としてコード側で管理する。

例:

- dev_phr
- work_other

接続時に database 引数として渡す。

```python
conn = get_mysql_connection(database="work_other")
```

---

## 4. 接続設計

接続は以下の2層構造とする。

### (1) 接続基盤（.env）
- host
- port
- user
- password

### (2) 業務接続先（コード）
- database（schema）

---

## 5. 将来拡張

host が増える場合は prefix を追加して並列定義する。

例:

```env
PHR_DB_HOST=...
PHR_DB_PORT=...
PHR_DB_USER=...
PHR_DB_PASSWORD=...

PHR_STG_DB_HOST=...
PHR_STG_DB_PORT=...
PHR_STG_DB_USER=...
PHR_STG_DB_PASSWORD=...
```

スクリプト側で prefix を切り替えて接続する。

---

## 6. 目的

- 接続機密情報と業務設計を分離する
- スクリプトの可読性を保つ
- 環境差分を `.env` に閉じ込める
- 将来の複数host対応を容易にする
