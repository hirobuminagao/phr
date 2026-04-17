

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

---

## Lookup Layer (補足)

本ディレクトリ配下では、**参照専用の薄い取得ロジック（lookup）**を配置することがある。

責務:
- 単一責務での SELECT のみを提供する
- 1関数 = 1取得ロジック
- 副作用（INSERT/UPDATE/DELETE）は持たない

設計ルール:
- connect_ctx / dict_cursor を必ず使用する
- schema は `schemas.py` の定数を利用して明示する
- SQLは関数内に閉じる（外部化しない）
- 例外はドメインに応じて明示的に分ける（NotFound / Ambiguous など）

ディレクトリ例:
```
scripts/lib/db/lookup/
  └─ fund.py
```

---

## Example: fund lookup

```python
from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import DEV_PHR

params = load_mysql_base_params()

with connect_ctx(params, database=DEV_PHR) as conn:
    cursor = dict_cursor(conn)
    cursor.execute(
        """
        SELECT fund_id
        FROM dev_phr.fund_insurer_numbers
        WHERE insurer_number = %s
        """,
        ("06139463",),
    )
    rows = cursor.fetchall()
```

ポイント:
- `DEV_PHR` を明示
- dict_cursor により `row["fund_id"]` で取得
- lookup は「insurer_number → fund_id」のような単方向変換に限定

---

## Responsibility Split（README分割方針）

現READMEの責務:
- DB接続ルール（connect_ctx / dict_cursor）
- schemaの扱い
- トランザクション方針

lookup側にREADMEを作る場合の責務:
- lookupの命名規則（get_xxx_from_yyy）
- 例外設計（NotFound / Ambiguous）
- 候補展開ルール（今回のinsurer_numberのような正規化パターン）
- 「1関数=1責務」の徹底

分離判断の目安:
- DB接続ルールの説明 → **このREADMEに残す**
- 業務ロジック寄り（fund / subscriber など） → **lookup READMEへ分離**