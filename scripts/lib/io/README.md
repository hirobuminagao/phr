# scripts/lib/io

共通 I/O 系 utility を配置するディレクトリ。

目的:

```text
「ファイルをどう読むか」
「ディレクトリをどう探索するか」
```

を業務ロジックから分離すること。

本ディレクトリは:

```text
CSV mapping
normalize
identity
DB更新
業務ルール
```

を責務に持たない。

---

# Design Policy

io layer の責務:

```text
- file open
- encoding handling
- BOM handling
- header handling
- row iteration
- directory discovery
- file existence check
- path utility
```

非責務:

```text
- business logic
- normalize / match
- identity generation
- DB access
- ETL orchestration
- CSV header mapping
```

---

# File List

## csv_loader.py

CSV 読み込み共通 utility。

責務:

```text
- CSV open
- encoding handling
- BOM handling
- header handling
- dict row iteration
- row count
```

利用例:

```python
from scripts.lib.csv.csv_loader import load_csv

loader = load_csv(
    path=str(csv_path),
    header_count=1,
)

row_count = loader.count_rows()

for row in loader.iter_dict_rows():
    print(row)
```

---

# Planned Files

## directory_discovery.py

ディレクトリ探索共通 utility（予定）。

想定責務:

```text
- 8桁保険者ディレクトリ列挙
- suffix別ファイル列挙
- CSV存在チェック
- ディレクトリ存在チェック
- row estimate helper
```

想定利用例:

```python
from scripts.lib.io.directory_discovery import (
    list_8digit_directories,
    list_files_by_suffix,
)

for d in list_8digit_directories(base_dir):
    csv_files = list_files_by_suffix(d, ".csv")
```

---

# Current Usage

現在利用中:

```text
scripts/hia/import_subscribers_to_staging_hub.py
```

利用中 component:

```text
csv_loader.py
```

---

# Naming Policy

コード上は:

```text
directory
```

を基本用語として利用する。

理由:

```text
pathlib / Python 標準用語に合わせるため
```

例:

```text
Path.is_dir()
Path.iterdir()
NotADirectoryError
```

ユーザー向け文言や業務会話では:

```text
folder
```

表現も許容する。
