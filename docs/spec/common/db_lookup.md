# DB参照方針（PHR共通）

## 1. 目的

本specは、DB接続基盤とは別に、参照系ロジック（lookup）をどのように配置・実装するかを定義する。

本方針の目的は以下とする。

- DB接続処理と参照処理の責務を分離する
- 業務スクリプトから SQL 直書きを減らす
- 小さな参照関数を共通ライブラリとして再利用できるようにする
- 将来 lookup が増えても整理しやすい構造を維持する

---

## 2. 本specの位置づけ

- `db_connection.md` は「どう接続するか」を扱う
- 本specは「接続した先で、どう参照ロジックを切り出すか」を扱う

そのため、接続情報（host / port / user / password / schema）の管理方針は `db_connection.md` に従い、本specでは lookup の責務と配置だけを定義する。

---

## 3. 配置方針

lookup は `scripts/lib/db/lookup/` 配下へ配置する。

```text
scripts/lib/db/
  ├── config.py
  ├── mysql.py
  ├── schemas.py
  └── lookup/
        └── fund.py
```

### 配置意図

- `mysql.py` は接続責務のみを持つ
- lookup は参照（SELECT）責務のみを持つ
- 業務スクリプトは lookup を経由して値を取得する

---

## 4. lookup の責務

lookup の責務は以下とする。

- 既存テーブルから必要な値を SELECT で取得する
- 呼び出し元が直接 SQL を書かなくてもよいようにする
- 小さく単機能な関数として定義する

### 非責務

以下は lookup の責務に含めない。

- DB接続設定の解決
- INSERT / UPDATE / DELETE
- 業務判定
- 値正規化
- CSV処理

---

## 5. 設計方針

### 基本方針

- 1関数 = 1責務とする
- 初期段階では過度に抽象化しない
- 小さく実装し、必要になったら分割する

例:

```python
get_fund_id_from_insurer_number(insurer_number: str) -> int
```

### 初期実装

初期段階では `fund.py` を作成し、以下のような単機能参照から開始する。

- `get_fund_id_from_insurer_number(...)`

---

## 6. 拡張方針

lookup が肥大化した場合は、ドメイン単位で分割する。

例:

```text
lookup/
  fund.py
  template.py
  template_mapping.py
```

### 分割の目安

- 関数数が増えて見通しが悪くなった場合
- 関連ドメインが増えた場合
- 1ファイルの責務が複数に分かれ始めた場合

---

## 7. 利用方針

業務スクリプトは、原則として lookup を経由して参照値を取得する。

```python
fund_id = get_fund_id_from_insurer_number(insurer_number)
```

これにより、スクリプト側では SQL 文そのものではなく、「何を取得したいか」を直接表現する。

---

## 8. 本specで次に詰めること

- `lookup` 配下の README を作るかどうか
- 共通例外クラスの扱い
- `template.py` / `template_mapping.py` への分割タイミング
