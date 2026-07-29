# Exam Facility Lookup Lib Draft

## Status

Implemented.

共通lookupは `scripts/lib/db/lookup/exam_facility.py` に実装済みで、`01_scan_files.py` から利用している。
本書はAPI設計の経緯として残し、実際の挙動は実装とテストを正とする。

このドキュメントは、受領フォルダ名から健診機関を解決する共通lookup libの設計案を整理したものである。

## Purpose

フォルダaliasから健診機関を解決するSQLを、個別スクリプトへ直書きしない。
`scripts/lib/db/lookup/` 配下の共通lookupとして切り出し、複数の処理から同じルールで参照できるようにする。

主な利用元は以下とする。

- `01_scan_files.py`
- `02_02_exam_result_csv_import`
- 移行後の検査SQLや補助スクリプト

## Placement

配置案:

```text
scripts/lib/db/lookup/exam_facility.py
```

既存の `docs/spec/common/db_lookup.md` の方針に従い、lookup libは参照、SELECT、取得単位の共通化だけを担当する。
DB接続、INSERT、UPDATE、業務状態更新、CSV処理、normalizeは担当しない。

## Main API Draft

共通lookup libは、呼び出し元が持っているキーに応じて以下の3つの入口を持つ案とする。

- フォルダaliasから引く
- `exam_facility_id` から引く
- `exam_facility_code` から引く

いずれの入口でも、返却する健診機関handleの形は揃える。

### Folder Alias Lookup

```python
resolve_exam_facility_by_folder_alias(
    cur,
    *,
    master_db: str,
    event_id: int,
    folder_name: str,
) -> dict | None
```

#### Input

| 引数 | 内容 |
|---|---|
| `cur` | 呼び出し元が管理するDB cursor |
| `master_db` | `phr_master` などのマスタDB名 |
| `event_id` | 対象イベントID |
| `folder_name` | 受領フォルダ名。`medical_folder_aliases.src_folder_raw` と照合する |

### ID Lookup

```python
get_exam_facility_by_id(
    cur,
    *,
    master_db: str,
    exam_facility_id: int,
) -> dict | None
```

#### Input

| 引数 | 内容 |
|---|---|
| `cur` | 呼び出し元が管理するDB cursor |
| `master_db` | `phr_master` などのマスタDB名 |
| `exam_facility_id` | 健診機関ID |

### Code Lookup

```python
get_exam_facility_by_code(
    cur,
    *,
    master_db: str,
    exam_facility_code: str,
) -> dict | None
```

#### Input

| 引数 | 内容 |
|---|---|
| `cur` | 呼び出し元が管理するDB cursor |
| `master_db` | `phr_master` などのマスタDB名 |
| `exam_facility_code` | 健診機関コード |

## Return

見つかった場合は以下を返す。

```python
{
    "exam_facility_id": 123,
    "exam_facility_code": "EF0001",
    "exam_facility_name": "サンプル健診センター",
    "exam_facility_display_name": "サンプル健診センター",
    "medical_institution_code": "0110111887",
    "alias_id": 456,
    "src_folder_raw": "受領フォルダ名",
    "dst_folder_norm": "正規化フォルダ名",
}
```

`get_exam_facility_by_id()` と `get_exam_facility_by_code()` では、alias由来ではないため `alias_id`, `src_folder_raw`, `dst_folder_norm` は返却しない、または `None` とする。
呼び出し元で扱いやすいよう、`exam_facility_id`, `exam_facility_code`, `exam_facility_name`, `exam_facility_display_name`, `medical_institution_code` は同じキーで返す。

見つからない場合は `None` を返す。

`01_scan_files.py` が `file_receipts` へ渡す最小値は以下とする。

- `exam_facility_id`
- `exam_facility_code`
- `exam_facility_name`

`file_receipts` へ保持するスナップショットは、追加する `exam_facility_id` と、既存 `facility_code` / `facility_name` を基本とする。
`facility_code` / `facility_name` にはlookupした `exam_facility_code` / `exam_facility_name` をscan時点の値として登録する案とする。

## Lookup SQL Draft

### Folder Alias Lookup SQL

```sql
SELECT
  mfa.alias_id,
  mfa.src_folder_raw,
  mfa.dst_folder_norm,
  ef.exam_facility_id,
  ef.exam_facility_code,
  ef.exam_facility_name,
  ef.exam_facility_display_name,
  ef.medical_institution_code
FROM `phr_master`.`medical_folder_aliases` mfa
INNER JOIN `phr_master`.`exam_facilities` ef
  ON ef.exam_facility_id = mfa.exam_facility_id
WHERE mfa.event_id = %s
  AND mfa.src_folder_raw = %s
  AND mfa.is_active = 1
  AND ef.is_active = 1
LIMIT 1;
```

cross schema FK は張らないが、lookupでは `medical_folder_aliases.exam_facility_id` と `exam_facilities.exam_facility_id` をJOINして整合した行だけを返す。

### ID Lookup SQL

```sql
SELECT
  ef.exam_facility_id,
  ef.exam_facility_code,
  ef.exam_facility_name,
  ef.exam_facility_display_name,
  ef.medical_institution_code
FROM `phr_master`.`exam_facilities` ef
WHERE ef.exam_facility_id = %s
  AND ef.is_active = 1
LIMIT 1;
```

### Code Lookup SQL

```sql
SELECT
  ef.exam_facility_id,
  ef.exam_facility_code,
  ef.exam_facility_name,
  ef.exam_facility_display_name,
  ef.medical_institution_code
FROM `phr_master`.`exam_facilities` ef
WHERE ef.exam_facility_code = %s
  AND ef.is_active = 1
LIMIT 1;
```

## Normalization Policy

初期案では、lookup libは `folder_name.strip()` 程度の軽い空白除去だけを行う。
フォルダ名の業務的な別名吸収、fuzzy match、表記ゆれ推測は行わない。

理由:

- `medical_folder_aliases` は `event_id + src_folder_raw` を正式な照合キーとして持つ。
- 推測一致をlookup libに入れると、スキャン時の誤紐付けリスクが上がる。
- 別名吸収はalias行を追加して明示管理する方が調査しやすい。

## Error / Ambiguity Policy

- `folder_name` が空の場合は `None` を返す、または入力エラーとして扱う。正式挙動は実装前に決める。
- `exam_facility_id` が空または非正数の場合は `None` を返す、または入力エラーとして扱う。正式挙動は実装前に決める。
- `exam_facility_code` が空の場合は `None` を返す、または入力エラーとして扱う。正式挙動は実装前に決める。
- `event_id + src_folder_raw` は一意制約があるため、複数候補は原則発生しない。
- aliasは存在するが `exam_facility_id` が未設定、または紐づく `exam_facilities` が無効の場合は `None` を返す案を基本とする。
- 呼び出し元は `None` の場合、`file_receipts.exam_facility_id` を未設定にするか、スキャンエラーにするかを処理目的に応じて判断する。

## Caller Usage

`01_scan_files.py` では、受領フォルダを判定した後にlookupを呼び、戻り値を `file_receipts` 登録へ渡す。

```python
facility = resolve_exam_facility_by_folder_alias(
    cur,
    master_db=config.master_db,
    event_id=config.event_id,
    folder_name=folder_name,
)

exam_facility_id = facility["exam_facility_id"] if facility else None
facility_code = facility["exam_facility_code"] if facility else None
facility_name = facility["exam_facility_name"] if facility else None
```

`02_02_exam_result_csv_import` では、原則として `file_receipts.exam_facility_id` を使う。
ただし、既存行のバックフィルや検査用途では同じlookupを利用できる。

## Open Points

- `folder_name` 空値を `None` として扱うか、入力エラーにするか。
- `exam_facility_id` / `exam_facility_code` 空値を `None` として扱うか、入力エラーにするか。
- alias未設定のCSVを `DISCOVERED` のまま残すか、`ERROR` にするか。
- `exam_facility_display_name` がある場合、返却する `exam_facility_name` を正式名にするか表示名にするか。
