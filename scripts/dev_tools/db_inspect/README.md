# DB Inspect Tool

## 目的
ローカル環境（Docker MySQL）に接続し、  
テーブル構造およびデータ内容を確認・共有するためのツール。

---

## ディレクトリ構成

```
scripts/dev_tools/db_inspect/
├── test_db.py
├── .env
└── README.md
```

---

## 前提

- Python 3.11.x（pyenv推奨）
- python-dotenv
- mysql-connector-python

---

## セットアップ

```bash
pip install python-dotenv mysql-connector-python
```

---

## .env 設定

.env に接続情報を記載すること（本ファイルには具体値は記載しない）。

例：

MYSQL_HOST=...
MYSQL_PORT=...
MYSQL_DATABASE=...
MYSQL_USER=...
MYSQL_PASSWORD=...

---

## 実行方法

```bash
python test_db.py
```

---

## 確認内容

- MySQL接続確認
- SHOW TABLES
- exam_item_master 件数確認
- サンプルデータ出力

---

## 補足

- `.env` はスクリプトと同一ディレクトリに配置する
- `load_dotenv()` はファイルパス指定推奨（相対パス事故防止）

---

## 今後拡張

- CSVエクスポート機能
- カラム一覧取得
- 任意条件でのデータ抽出

---

## 位置づけ

本ツールは開発補助用の確認ツールであり、  
業務ロジック（work_folder配下）とは分離して管理する。