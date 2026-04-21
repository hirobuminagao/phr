# subscriber_audit insert 共通仕様

## 概要
`scripts/lib/db/insert/subscriber_audit.py` は、  
subscriber の変更履歴（audit）を `dev_phr.subscriber_audit` に記録するための共通ライブラリである。

本ライブラリは **INSERT処理のみを責務とし、業務判断は一切行わない**。

---

## 責務

本ライブラリの責務は以下のみとする：

- audit 行の必須項目チェック
- 値の整形（文字列化）
- `subscriber_audit` テーブルへの INSERT

---

## 非責務（重要）

以下は呼び出し側（業務スクリプト）の責務とする：

- どの項目を audit 対象とするか
- old_value / new_value の差分判定
- audit 行の生成（dict作成）
- source / change_run_id の決定

---

## 入力フォーマット（dict）

本ライブラリは以下形式の dict を受け取る：

```python
{
    "subscriber_id": int,           # 必須
    "field": str,                  # 必須
    "old_value": str | None,
    "new_value": str | None,
    "source": str | None,
    "note": str | None,
    "change_run_id": int | None,
}
```

---

## 必須項目

| 項目 | 説明 |
|------|------|
| `subscriber_id` | 対象 subscriber |
| `field` | 変更されたカラム名 |

※ 未設定の場合は `ValueError` を発生させる

---

## 値整形ルール

- `None` はそのまま `NULL` として保存する
- それ以外は `str()` により文字列化する

---

## INSERT対象テーブル

`dev_phr.subscriber_audit`

### カラム対応

| dictキー | DBカラム |
|---------|----------|
| `subscriber_id` | `subscriber_id` |
| `field` | `field` |
| `old_value` | `old_value` |
| `new_value` | `new_value` |
| `source` | `source` |
| `note` | `note` |
| `change_run_id` | `change_run_id` |

※ `changed_at` は DB 側で自動設定する

---

## 提供関数

### `audit_value`

保存用の値整形を行う。

```python
audit_value(value: Any) -> str | None
```

---

### `validate_subscriber_audit_row`

1 行分の audit dict を検証する。

```python
validate_subscriber_audit_row(row: Mapping[str, Any]) -> None
```

---

### `build_subscriber_audit_params`

1 行分の audit dict を INSERT 用パラメータへ変換する。

```python
build_subscriber_audit_params(row: Mapping[str, Any]) -> tuple[Any, ...]
```

---

### `insert_subscriber_audit_row`

単一行 INSERT を行う。

```python
insert_subscriber_audit_row(cur: Any, row: Mapping[str, Any]) -> None
```

---

### `insert_subscriber_audit_rows`

複数行 INSERT を行う。

```python
insert_subscriber_audit_rows(cur: Any, rows: Sequence[Mapping[str, Any]]) -> None
```

---

## エラーハンドリング

- 必須項目不足 → `ValueError`
- DBエラー → 呼び出し側で処理する

---

## 設計意図

- audit の仕様は subscriber 専用として固定する
- 汎用化は行わず、責務を最小化する
- ETL / apply 処理から再利用しやすい構造にする

---

## 今後の拡張方針

- カラム追加は dict にキー追加で対応する
- lib 側は極力変更しない
- audit の生成ロジックは各業務スクリプトで管理する
