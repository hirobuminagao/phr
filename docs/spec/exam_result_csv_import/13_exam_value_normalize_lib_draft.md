# Exam Value Normalize Lib Draft

## Status

Draft.

このドキュメントは、CSV健診結果取込で利用する健診結果値normalize共通libの設計案を整理する。
現時点では実装変更は行わず、`02_02_exam_result_csv_import` と後続のXML由来再normalizeで共通利用する前提仕様として扱う。

## Existing Context

既存の `health_exam_result` 設計では、共通libとして `scripts/lib/examination/value_normalizer.py` を作成し、公開APIは `normalize_exam_item_value()` を基本とする方針が記録されている。

また、`namecode` から型・単位などの項目メタデータを取得するlookupは既に存在する。

```text
scripts/lib/db/lookup/exam_item_master.py
```

既存API:

- `get_exam_item(cur, namecode)`
- `get_exam_items(cur, namecodes)`

このlookupは、少なくとも以下を返す。

- `namecode`
- `item_name`
- `data_type`
- `unit`
- `display_unit`
- `ucum_unit`
- `data_type_label`
- `identity_item_code`
- `jun_no`

そのため、`namecode` を渡したら型や単位を返すlookupは新規作成ではなく、既存 `exam_item_master` lookupを利用する案を基本とする。
CSV取込初期実装では、健診機関がCSVに出してきた健診判定をPHR側で再判定しない。
これは法定項目の必須/不足チェックや `check_result` の評価とは別論点である。
健診機関由来の健診判定に関する基準範囲・判定メタデータ、施設別判定マスタ、納品用判定ロジックは後続バージョンで別途検討する。

CD/CO系の結果値名寄せに使う辞書テーブルとして、既存 `dev_phr.norm_variants` が存在する。
旧スクリプトでは `result_code_oid + raw_value_utf8` の完全一致で利用されているが、現時点では `scripts/lib/db/lookup/` 配下の共通lookupとしては存在しない。
そのため、CD/CO系variant lookupは新たに共通lookup化する案を基本とする。

`norm_variants` は旧「紙→Excel→DB 2テーブル直接投入→normalize→export」フローで使われている。
旧フローでは `normalize_item_values.py` が `norm_variants` を参照し、`medi_export_xml.py` がnormalize済み結果からXMLを作成する。
今後はnormalize処理を共通libへ寄せ、CSV取込、XML後続normalize、旧フロー移行後の再normalizeで同じ処理を使う。
`norm_variants` はこの名寄せ用途に限って使われているため、CSV健診結果取込で必要な共通マスタとして `phr_master` 初期DDLに含める。
旧 `dev_phr.norm_variants` の廃止タイミングは今回決めず、CSV取込実装後に参照切替と運用影響を確認してから別途判断する。

## Purpose

健診結果値normalizeは、CSV取込専用実装として閉じ込めず、XML由来値にも再利用できる共通libにする。

目的は以下とする。

- `namecode` とraw値から、正規化値・正規化単位・状態を返す
- `exam_item_master` の型・単位定義を基準にする
- CSV由来の `未実施`, `測定不能`, `未受診` など、型に依存しない非測定値語を共通的に扱う
- `未実施` / `測定不能` / `判定不能` など、entry内の項目結果値として出てくる実施状態・測定可否を、健診機関由来のABC等の健診判定とは分けて扱う
- CSV取込、XML後続normalize、再normalizeで同じ値normalizeを使う
- `exam_item_values` のnormalize系カラムへそのまま反映しやすい戻り値にする

## Placement

配置案:

```text
scripts/lib/examination/value_normalizer.py
```

lookup層との責務分離は以下とする。

- `scripts/lib/db/lookup/exam_item_master.py`
  - `namecode` から型・単位・項目メタデータを取得する
- `scripts/lib/db/lookup/norm_variant.py`
  - `result_code_oid` とraw値からCD/CO系の正規コード候補を取得する
- `scripts/lib/examination/value_normalizer.py`
  - 非測定値語、型、単位、辞書情報を使ってnormalize結果を作る

## Norm Variant Lookup Draft

配置案:

```text
scripts/lib/db/lookup/norm_variant.py
```

主API案:

```python
get_norm_variant(
    cur,
    *,
    master_db: str,
    result_code_oid: str,
    raw_value: str,
) -> dict | None
```

返却案:

```python
{
    "variant_id": 123,
    "result_code_oid": "1.2.392...",
    "raw_value_utf8": "異常なし",
    "normalized_code": "1",
    "code_system": "1.2.392...",
    "display_name": "異常なし",
    "is_canonical": 0,
    "priority": 100,
}
```

SQL案:

```sql
SELECT
  variant_id,
  result_code_oid,
  raw_value_utf8,
  normalized_code,
  code_system,
  display_name,
  is_canonical,
  priority
FROM `{master_db}`.`norm_variants`
WHERE result_code_oid = %s
  AND raw_value_utf8 = %s
  AND is_active = 1
ORDER BY priority, variant_id
LIMIT 1;
```

初期案では `raw_token_norm` は利用しない。
既存方針に合わせ、`result_code_oid + raw_value_utf8` の完全一致を基本とする。
CSV健診結果取込の初期実装では `master_db = "phr_master"` として呼び出す。
`phr_master` へ移設した後は同じlookup APIの `master_db` を差し替えるだけにする。

## Main API Draft

```python
normalize_exam_item_value(
    *,
    namecode: str | None,
    raw_value: str | None,
    raw_unit: str | None = None,
    raw_value_type: str | None = None,
    item_master: dict | None = None,
    norm_variants: dict | None = None,
) -> dict
```

### Input

| 引数 | 内容 |
|---|---|
| `namecode` | 健診項目コード |
| `raw_value` | CSV/XML由来のraw値 |
| `raw_unit` | CSV/XML由来のraw単位 |
| `raw_value_type` | 入力由来の値型。CSVの場合はmapping由来、XMLの場合はXML value type |
| `item_master` | `get_exam_item()` などで取得した項目メタデータ |
| `norm_variants` | CD/CO系などの結果値辞書。`norm_variant` lookupで事前取得したもの |

### Return

`exam_item_values` のnormalize系カラムへ写しやすいdictを返す。

```python
{
    "normalized_value": "123.4",
    "normalized_unit": "mg/dL",
    "reference_lower": "70",
    "reference_upper": "109",
    "normalize_status": "OK",
    "normalize_reason": None,
    "validation_status": "OK",
    "validation_reason": None,
}
```

## Source Reference / Judgement Policy

CSV取込初期実装では、PHR側で健診機関判定を再生成しない。
健診機関がCSVに出してきた下限・上限・判定は、健診機関側が原本として出した情報であり、PHRの共通マスタ基準値や標準判定とは別物として扱う。

- CSV由来の下限・上限は、原本由来情報として `exam_item_values.source_reference_lower` / `source_reference_upper` へ保持する。
- 健診機関がCSVに出してきた検査別判定、カテゴリ総合判定は、初期実装では納品判定やPHR側評価には使わない。
- 法定項目の必須/不足チェックや `check_result` の評価は、この健診機関由来判定とは別に扱う。
- `未実施` / `測定不能` / `判定不能` など、entry内の項目結果値として出てくる実施状態・測定可否も、この健診機関由来判定とは別に扱う。
- これらは `exam_item_values.raw_value` とnormalize状態に残し、ABC等の健診判定カラムや施設別判定マスタへは寄せない。
- CSV由来判定は原本証跡として保持する。最低限、`csv_row_ledger.raw_row_json` から復元できる状態にする。
- `exam_item_values.interpretation_code` / `interpretation_name` はXML由来の標準 `interpretationCode` とは意味が異なるため、健診機関由来の健診判定を初期実装で同一カラムへ寄せない。
- 必要になった場合は、`exam_item_values.source_judgement_raw` などの専用カラム、または施設別判定マスタを後続バージョンで検討する。

後続候補: `phr_master.exam_item_reference_ranges` を別テーブルとして持つ。

```sql
CREATE TABLE `phr_master`.`exam_item_reference_ranges` (
  `exam_item_reference_range_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `namecode` char(17) NOT NULL,
  `sex_code` varchar(16) DEFAULT NULL,
  `age_min` int DEFAULT NULL,
  `age_max` int DEFAULT NULL,
  `exam_facility_id` bigint unsigned DEFAULT NULL,
  `insurer_number` varchar(20) DEFAULT NULL,
  `method_code` varchar(64) DEFAULT NULL,
  `reference_lower` decimal(18,6) DEFAULT NULL,
  `reference_upper` decimal(18,6) DEFAULT NULL,
  `reference_unit` varchar(64) DEFAULT NULL,
  `judgement_rule_code` varchar(64) DEFAULT NULL,
  `valid_from` date DEFAULT NULL,
  `valid_to` date DEFAULT NULL,
  `priority` int NOT NULL DEFAULT 100,
  `is_active` tinyint(1) NOT NULL DEFAULT 1,
  `note` text,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`exam_item_reference_range_id`),
  KEY `idx_exam_item_reference_ranges_namecode` (`namecode`),
  KEY `idx_exam_item_reference_ranges_facility` (`exam_facility_id`),
  KEY `idx_exam_item_reference_ranges_validity` (`valid_from`, `valid_to`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
```

このテーブルは初期DDLには含めない。
将来、PHR側で共通判定を再生成する場合に、性別、年齢、健診年度、健診機関、保険者、検査方法などの差分を吸収するための候補とする。

将来の `judgement_rule_code` 候補:

- `NUMERIC_RANGE`
  - 数値の下限・上限で `LOW` / `NORMAL` / `HIGH` を判定する。
- `CODE_VALUE`
  - CD/COなどの正規化コードから判定する。
- `TEXT_MATCH`
  - テキスト値や所見文言から判定する。
- `NONE`
  - 判定しない。

初期の `normalize_exam_item_value()` は、値の正規化までを責務とし、健診機関由来の健診判定や納品用判定は返さない。

失敗または未処理の場合も例外ではなく、原則として状態付きのdictを返す。

```python
{
    "normalized_value": None,
    "normalized_unit": None,
    "normalize_status": "ERROR",
    "normalize_reason": "INVALID_VALUE_TYPE",
    "validation_status": "INVALID",
    "validation_reason": "INVALID_VALUE_TYPE",
}
```

## Lookup Usage Draft

CSV取込では、行ごと・項目ごとにDBへ問い合わせるのではなく、処理開始時またはformat version単位で必要な `namecode` をまとめて取得する。

```python
items_by_namecode = get_exam_items(cur, target_namecodes)

result = normalize_exam_item_value(
    namecode=namecode,
    raw_value=raw_value,
    raw_unit=raw_unit,
    raw_value_type=raw_value_type,
    item_master=items_by_namecode.get(namecode),
    norm_variants=norm_variants_by_key.get((result_code_oid, raw_value)),
)
```

単品調査や補助処理では `get_exam_item(cur, namecode)` を利用する。

## Initial Normalize Rules

初期案では、過度な旧資産の高度前処理は入れず、以下から始める。

### Non-Measured Raw Values

CSV由来では、検査値欄に数値やコードではなく、測定値ではないが原本上は意味を持つ文字列が入る可能性がある。
初期実装では、完全空セルと非測定値語を分けて扱う。

完全空セル:

- `""`
- 空セル
- 空白だけ

方針:

- CSV取込では `exam_item_values` 行を作らない。
- normalize lib単体に空値が渡された場合は、例外ではなく `normalize_status = SKIPPED`, `normalize_reason = BLANK_RAW_VALUE` を返す。

実施されていないことを示す語:

- `未実施`
- `未受診`
- `実施せず`
- `キャンセル`
- `中止`
- `拒否`
- `対象外`

方針:

- CSV取込では `exam_item_values` 行を作る。
- 元値を `exam_item_values.raw_value` に残す。
- normalize結果は以下を返す。

```python
{
    "normalized_value": None,
    "normalized_unit": None,
    "normalize_status": "SKIPPED",
    "normalize_reason": "RAW_VALUE_NO_RESULT",
    "validation_status": "WARNING",
    "validation_reason": "RAW_VALUE_NO_RESULT",
}
```

測定できなかったことを示す語:

- `測定不能`
- `判定不能`
- `検体不良`
- `採血不可`
- `測定不可`

方針:

- CSV取込では `exam_item_values` 行を作る。
- 元値を `exam_item_values.raw_value` に残す。
- normalize結果は `normalize_status = SKIPPED`, `normalize_reason = RAW_VALUE_UNMEASURABLE`, `validation_status = WARNING` とする。

型に合わない未知文字列:

- 数値項目に `abc`
- 数値項目に `不明`
- 数値項目に `-`
- 数値項目に `※`
- 数値項目に `再検`

方針:

- CSV取込では `exam_item_values` 行を作る。
- 元値を `exam_item_values.raw_value` に残す。
- normalize結果は `normalize_status = ERROR`, `normalize_reason = INVALID_VALUE_TYPE`, `validation_status = INVALID` とする。

意味を持つコード/テキスト:

- `あり`
- `なし`
- `陽性`
- `陰性`
- `+`
- `-`
- `1`
- `2`

方針:

- 共通ノイズ辞書には入れない。
- CD/COなら `norm_variants`、STなら文字列正規化で扱う。

非測定値語はCD/CO系の `norm_variants` とは別に、型に依存しない前処理として共通libで判定する。
ノイズ語の意味が明確にnullFlavorへ対応できる場合は、将来 `nullflavor` 反映も検討する。
ただし初期案では `nullflavor` へ自動写像せず、`exam_item_values.raw_value` に元値を残し、normalize結果側のreasonで分類する。

非測定値語辞書は、初期実装ではYAMLファイルとして管理する。
DBテーブル化は、運用画面や施設別・項目別の管理が必要になった時点で検討する。

配置案:

```text
config/examination/non_result_terms.yml
```

YAMLでは、少なくとも以下の分類を持つ。

- `no_result`: `未実施`, `未受診`, `実施せず`, `キャンセル`, `中止`, `拒否`, `対象外`
- `unmeasurable`: `測定不能`, `判定不能`, `検体不良`, `採血不可`, `測定不可`

`異常なし`, `所見なし`, `あり`, `なし`, `陽性`, `陰性`, `+`, `-` は、項目によって結果値として意味を持つ。
そのため、非測定値語辞書には入れず、CD/COなら `norm_variants`、STなら文字列正規化で扱う。

### Unknown Namecode

`namecode` が空、または `item_master` が取得できない場合:

- `normalize_status = SKIPPED`
- `validation_status = INVALID`
- `validation_reason = UNKNOWN_NAMECODE`

### Blank Raw Value

raw値が完全空の場合、CSV取込では `exam_item_values` 行を作らない。
normalize lib単体に空値が渡された場合は以下を返す。

- `normalized_value = NULL`
- `normalized_unit = NULL`
- `normalize_status = SKIPPED`
- `normalize_reason = BLANK_RAW_VALUE`
- `validation_status = WARNING`
- `validation_reason = BLANK_RAW_VALUE`

### Numeric Value

`item_master.data_type` が数値系の場合:

- 非測定値語に一致した場合は、数値変換を試みず分類済みreasonを返す
- raw値を数値文字列へ変換する
- 全角数字、カンマ、前後空白は軽く正規化する
- 数値変換できない場合は `INVALID_VALUE_TYPE`
- 単位は原則 `item_master.unit` を正とする
- 初期実装で数値系として扱う `data_type` は `PQ`, `INT`, `REAL` とする
- export済み `exam_item_master` では現時点の数値型は `PQ` であり、`INT` / `REAL` は旧 `norm_rules` との互換として受ける
- `raw_unit` があり、`item_master.unit` と一致しない場合は単位変換しない
- 単位不一致は `normalize_status = ERROR`, `normalize_reason = UNIT_MISMATCH`, `validation_status = INVALID` とする
- 単位変換は将来バージョンの拡張対象とする

### Text Value

`item_master.data_type` が `ST` など文字列系の場合:

- 非測定値語に一致した場合は、通常テキストとして正規化せず分類済みreasonを返す
- 前後空白、全角/半角などの基礎正規化を行う
- `normalized_value` へ文字列として返す
- `normalized_unit` は原則 `NULL`

### Code Value

`item_master.data_type` が `CD` / `CO` などコード系の場合:

- 非測定値語に一致した場合は、`norm_variants` を引かず分類済みreasonを返す
- `item_master.result_code_oid` が存在する場合のみ `norm_variants` を利用する案を基本とする
- `norm_variants` は `result_code_oid + raw_value_utf8` の完全一致でraw値を正規コードへ変換する
- 辞書未一致の場合は `NORMALIZE_VARIANT_NOT_FOUND`
- `CD` / `CO` 以外では初期案として `norm_variants` を利用しない
- `CO` で `result_code_oid` がない場合は、初期案では `ERROR` とし、仕様拡張対象にする

### Base Normalize Priority

CSV由来raw値の機械的な前処理は、`identity_hash` と同じ考え方で共通lib側へ寄せる。
DB上の変換ルールテーブルは初期実装では使わない。
処理順は以下とする。

1. 完全空値判定。
2. value base normalize。
3. 非測定値語判定。
4. `item_master.data_type` による型別normalize。
5. 単位チェック。

value base normalizeでは、NFKC、制御文字除去、前後空白除去、空文字の `None` 統一など、意味解釈を伴わない処理だけを行う。
性別、日付、CD/CO値など項目依存の意味解釈は、型別normalizeまたは `norm_variants` で行う。
単位変換や施設由来判定の意味変換は初期実装では行わない。

## CSV Import Integration

`02_02_exam_result_csv_import` では、`csv_exam_result_mapping_rules.target_kind = 'EXAM_ITEM_VALUE'` のruleについて以下を行う。

1. `target_namecode` を集め、`get_exam_items()` で項目メタデータを一括取得する。
2. CSVセルのraw値を取得する。
3. `CD` / `CO` 系で `result_code_oid` がある場合、`norm_variant` lookupで正規コード候補を取得する。
4. `normalize_exam_item_value()` に `namecode`, `raw_value`, `raw_unit`, `raw_value_type`, `item_master`, `norm_variants` を渡す。
5. raw値とnormalize結果を `exam_item_values` に登録する。

CSV取込では、登録時点でnormalize結果まで反映する。
ただし、normalize lib自体はCSV専用ではなく、XML由来の後続normalizeでも使える形にする。
CSV由来の `VALUE` が完全空セルの場合は `exam_item_values` 行を作らない。
下限・上限・判定だけが存在し、`VALUE` が完全空セルの場合も `exam_item_values` 行を作らない。
`未実施` / `キャンセル` / `測定不能` などの非測定値語は完全空ではないため、`exam_item_values.raw_value` に原文を残してnormalize結果のreasonで分類する。

CSV取込初期実装では、CD/CO系の結果値名寄せに `norm_variants` を使う。
`norm_variants` は `phr_master` 初期DDLに含め、CSV取込では `phr_master.norm_variants` を参照する。
`norm_variant` lookupは、単品APIと一括APIの両方を持つ。
CSV取込では、行処理前に対象 `result_code_oid` とraw値の組み合わせを集め、一括APIで事前取得してから `normalize_exam_item_value()` へ渡す。
単品APIは少量処理、テスト、再normalize用の補助入口として残す。
CSV取込スクリプトは、`exam_item_values` 登録時に共通normalize libを同期呼び出しする。

## Future Work

- 非測定値語を将来 `nullflavor` へ写像するか。
