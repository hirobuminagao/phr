

# ADR-0009: MySQL接続情報とスキーマ指定の責務分離

## Status
Accepted

## Context
PHR プロジェクトでは複数の MySQL schema を扱うようになっている。

主な例:

- dev_phr
- work_other

従来の実装では `load_mysql_params()` が以下を同時に管理していた。

- 物理接続情報
- database(schema)

しかしこの構成では以下の問題が発生する。

- スクリプトが意図しない schema（例: work_other）に接続する
- dev_phr と work_other を跨ぐ処理が書きにくい
- schema の責務が接続設定と混ざる

PHR v1 系では、今後も以下のような処理が増えることが想定される。

- dev_phr.subscribers
- work_other.hia_dashboard_status
- dev_phr.etl_runs

このため **接続情報と論理スキーマの責務分離**を行う。

---

## Decision

### 1. `.env` は物理接続情報のみ保持する

`.env` には以下のみを定義する。

```
MYSQL_HOST
MYSQL_PORT
MYSQL_USER
MYSQL_PASSWORD
```

これらは **DB接続に必要な物理情報のみ**とする。

`.env` に database(schema) は定義しない。

---

### 2. schema / table / column はスクリプト側で管理する

各スクリプトは対象 schema を明示的に指定する。

例:

```python
TARGET_SCHEMA = "dev_phr"

sql = f"""
SELECT *
FROM {TARGET_SCHEMA}.subscribers
"""
```

また、必要に応じて以下のように schema を跨ぐ SQL を許可する。

```sql
SELECT *
FROM dev_phr.subscribers s
JOIN work_other.hia_dashboard_status d
  ON ...
```

---

## Consequences

### メリット

- dev_phr / work_other の混在処理が安全に書ける
- 接続 DB を誤る事故を防止
- 将来的なマルチ schema / マルチ DB 構成に対応しやすい

### デメリット

- SQL に schema 名を明示する必要がある

ただし PHR の設計では schema を明示する方が安全であるため、このコストは許容する。

---

## Notes

この ADR は PHR v1.0.1 以降のスクリプト設計に適用する。

特に以下の系統のスクリプトはこのポリシーに従う。

- HIA import
- subscriber apply
- dashboard import
- medi 系 ETL
