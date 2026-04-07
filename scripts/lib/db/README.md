

# db library

## Overview

このディレクトリは、DB接続およびDB操作の共通処理を扱うライブラリである。

主な責務:

- MySQL接続の確立
- cursorの生成
- schema定義の共通化

identity と対になる位置づけとして、以下の役割を持つ。

- identity: 値の作り方
- db: 値の扱い方（接続・取得・更新）

---

## Usage

基本的な利用方法は以下。

```python
from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import WORK_OTHER

params = load_mysql_base_params()

with connect_ctx(params, database=WORK_OTHER) as conn:
    cursor = dict_cursor(conn)

    cursor.execute("SELECT * FROM some_table")
    rows = cursor.fetchall()
```

---

## Rule

- 接続は `connect_ctx` を使用する
- cursor は `dict_cursor` を使用する（keyアクセス前提）
- schema は明示的に指定する（例: WORK_OTHER, DEV_PHR）
- commit は明示的に行う
- transaction は呼び出し側で管理する

---

## Schema Role

```text
dev_phr    → 正規データ（本体）
work_other → 取込 / 作業 / 中間
```

---

## Notes

- DB処理は副作用を持つため、identity 層とは分離する
- generator / field / builder から直接DB操作を行わない
- DBアクセスは script 層からのみ行う