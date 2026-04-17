# identity library

## Overview

このディレクトリは、identity 系処理の共通ライブラリを配置する実装領域である。

対象は以下とする。

- 共通 primitive 処理
- 全項目共通の `base_norm`
- 項目別 `field_norm / match`
- `person_id_custom` / `identity_hash` の builder

本ディレクトリ構造は、以下の設計文書と整合する。

- 親spec: `docs/spec/identity_canonicalization/v1.1.0_identity_layer_commonization.md`
- 子spec: `docs/spec/identity_canonicalization/identity_layers_norm_and_purpose.md`
- 構造spec: `docs/spec/identity_canonicalization/identity_layer_structure.md`
- ADR: `docs/adr/0016-v1.1.0-identity-layer-commonization-and-backfill.md`

---

## Directory Structure

```text
scripts/lib/identity/
├── README.md
├── primitive/
│   ├── normalize.py
│   ├── remove.py
│   ├── convert.py
│   ├── digits.py
│   ├── split.py
│   └── dates.py
│
├── base_norm.py
│
├── field/
│   ├── insurer_number.py
│   ├── insurance_symbol.py
│   ├── insurance_number.py
│   ├── birthdate.py
│   ├── date_field.py
│   ├── name_kana.py
│   ├── name_kanji.py
│   └── gender_code.py
│
└── builder/
    ├── person_id_custom.py
    └── identity_hash.py
```

---

## Responsibility

### primitive/

最小単位の純関数群を置く。

- `normalize.py`: 揺れを揃える処理
- `remove.py`: 不要要素の除去
- `convert.py`: 表現変換
- `digits.py`: 数字専用処理
- `dates.py`: 日付専用処理
- `split.py`: delimiter 指定による単純分割処理

### base_norm.py

primitive を組み合わせた全項目共通の下ごしらえ正規化を置く。

### field/

各項目ごとの意味に基づく処理を置く。

対象:
- insurer_number
- insurance_symbol
- insurance_number
- birthdate
- date_field
- name_kana
- name_kanji
- gender_code


各ファイルでは、主に以下を担当する。

日付系については、既存の `birthdate.py` を維持したまま、用途ベースの共通化先として `date_field.py` を追加してよい。
この場合、import 先を `birthdate.py` から `date_field.py` へ変更するだけでは足りず、呼び出す関数名も用途ベースの関数へ合わせて修正する。
例:
- 旧: `from ...field.birthdate import normalize_birthdate`
- 新: `from ...field.date_field import normalize_date_to_ymd_and_compact`

また、氏名系 field では full 値を parts（family / middle / given）へ分解する用途を持つ関数を追加してよい。
例:
- `normalize_name_kana_full_to_parts`
- `normalize_name_kanji_full_to_parts`

代表的な返却値:
- 単一項目 norm 系
  - `field_norm`
  - `match`
  - 必要に応じた `person_id_custom` / `identity_hash_input` / `export` 用値
- parts 系
  - `full`
  - `family`
  - `middle`
  - `given`

### builder/

完成値の生成を置く。

- `person_id_custom.py`
- `identity_hash.py`

builder は canonical input を受け取り、必須材料不足時は無理に生成しない。

---

## Dependency Rule

依存方向は以下に固定する。

```text
primitive
   ↓
base_norm
   ↓
field
   ↓
builder
```

逆方向依存は禁止とする。

---

## Implementation Rule

- `raw` から直接 builder 用完成値を作らない
- 原則として `raw → base_norm → field_norm → match / canonical input → builder` の流れを守る
- 項目依存の判断は `field/` で行う
- builder は不足時に補完・推測を行わない
- 実装差分が発生した場合は、ADR → spec → 実装の順で整合を取る

---


## Usage (How to use identity library)

入力値（raw を含む）を直接 builder に渡してはいけない。

必ず以下の順序で処理する。

なお、日付系は用途ベース共通関数へ切り替える場合、import 文と関数名の両方を合わせて修正すること。

1. 入力値 → field.normalize_xxx()
2. field の result から canonical 値を取得
3. builder に canonical 値を渡す

例:

```python
from scripts.lib.identity.field.birthdate import normalize_birthdate
from scripts.lib.identity.field.insurance_symbol import normalize_insurance_symbol
from scripts.lib.identity.builder.person_id_custom import build_person_id_custom

birth_res = normalize_birthdate(row["birthdate"])
symbol_res = normalize_insurance_symbol(row["insurance_symbol_raw"])

if not birth_res["ok"]:
    raise Exception(birth_res["reason"])

person_id_res = build_person_id_custom(
    birthdate_match=birth_res["match"],
    insurance_symbol_person_id_custom=symbol_res["person_id_custom"],
)
```

---

## Field Result Structure

field の normalize 関数は、用途に応じて以下のいずれかの形式を返す。

### 1. 単一項目 norm / match 系

代表項目:
- `normalize_birthdate`
- `normalize_date_to_ymd_and_compact`
- `normalize_name_kana_full`
- `normalize_name_kanji_full`

基本項目:
- `ok`: 成功可否
- `missing`: 欠損可否
- `reason`: NG理由
- `raw`: 元値
- `base_norm`: 共通前処理後の値
- `field_norm`: 表示用など
- `match`: 照合用値（canonical input）

例:

```json
{
  "ok": true,
  "match": "19900101",
  "field_norm": "1990-01-01"
}
```

### 2. parts 系

代表項目:
- `normalize_name_kana_full_to_parts`
- `normalize_name_kanji_full_to_parts`

基本項目:
- `ok`: 成功可否
- `missing`: 欠損可否
- `reason`: NG理由
- `raw`: 元値
- `base_norm`: 共通前処理後の値
- `full`: 正規化済み full
- `family`: 姓
- `middle`: 中間
- `given`: 名

例:

```json
{
  "ok": true,
  "full": "ナガオ　ヒロフミ",
  "family": "ナガオ",
  "middle": null,
  "given": "ヒロフミ"
}
```

---

## Important Rules

- 入力値（raw を含む）を直接 builder に渡してはいけない
- builder は正規化を行わない
- field がすべての解釈責務を持つ
- builder は canonical input のみ受け取る
- primitive は単純変換のみに留め、氏名としての解釈（family / middle / given への割当）は field で行う

---

## Input Type Handling (重要)

field 層は raw の値の「型差異」を吸収する責務を持つ。

### 基本方針

- raw は「文字列とは限らない」
- DB / CSV / XML により型が異なることを前提とする
- 型ごとの解釈は field で行う
- generator / builder では型変換を行わない

### 例: birthdate

入力パターン:

- "19900101" (str)
- "1990-01-01" (str)
- "平成010101" (str)
- `datetime.date(1990, 1, 1)` (MySQL DATE)

対応:

- str → base_norm → digits抽出 → parse
- date → field 内で直接 `YYYYMMDD` / `YYYY-MM-DD` に変換

### 実装ルール

- field は `isinstance` により型分岐を行ってよい
- date / int / str など複数型の受け入れを許可する
- base_norm は基本的に str 前提とする
- 非 str 型は base_norm に入れる前に処理する

### NGパターン

- generator で型変換する
- builder で型変換する
- field を通さず raw を builder に渡す

---

## Purpose-based Outputs

field は用途別の値を返す:

- match: 突合用
- person_id_custom: ID生成用
- export: 出力用

用途に応じて適切な値を使用すること

---

## Data Flow

```text
raw
 ↓
base_norm
 ↓
field (match / canonical)
 ↓
builder
```

---

## Notes

- raw は生データであり、正規化や解釈は含まない
- field は raw を受けて正規化し、canonical input を生成する
- builder は canonical input を受けて最終的な ID やハッシュを生成する
- 依存関係は primitive → base_norm → field → builder と一方向に固定する
- builder は field の出力を前提とし、不足時は生成を行わない
- すべての identity 生成はこの流れに従うこと
- split のような汎用的な単純分割は primitive に置き、用途別の parts 解釈は field に置く

---

## Generator (Orchestration Layer)

identity の生成は `generator.py` を使用する。

generator は以下の責務を持つ。

- 必要な入力値（raw / 正規化済み値を含む）を受け取る
- field を通して canonical 値を生成する
- builder に渡す
- person_id_custom / identity_hash と、その生成過程の result を返す

重要:
- generator は I/O を持たない（DB / CSV / ログ出力を行わない）
- あくまで field → builder のオーケストレーションのみ

---

## Generator Functions

### Generator Return Structure

generator 関数は、値だけではなく生成過程の result も dict で返す。

代表的な返却項目:

- `ok`: 全体成功可否
- `reason`: NG理由
- `field_results`: field の処理結果一覧
- `builder_result`: builder の処理結果（単体generatorの場合）
- `person_id_custom`: 生成済み person_id_custom（bundle / identity_hash で返却）
- `identity_hash`: 生成済み identity_hash（bundle で返却）
- `person_id_custom_result`: person_id_custom 側の集約結果
- `identity_hash_result`: identity_hash 側の集約結果

方針:
- generator の利用側は `ok` を必ず確認する
- 値だけでなく `reason` / `field_results` / `builder_result` を使って失敗原因を追跡できる

### generate_person_id_custom

person_id_custom を生成する。

```python
from scripts.lib.identity.generator import generate_person_id_custom

res = generate_person_id_custom(
    birthdate=row["birthdate"],
    insurer_number_raw=row["insurer_number_raw"],
    insurance_symbol_raw=row["insurance_symbol_raw"],
    insurance_number_raw=row["insurance_number_raw"],
)

if not res["ok"]:
    raise Exception(res["reason"])

person_id_custom = res["value"]
builder_result = res["builder_result"]
field_results = res["field_results"]
```

---

### generate_identity_hash

identity_hash を生成する（2モード対応）。

#### パターン1: person_id_custom が既にある場合

```python
from scripts.lib.identity.generator import generate_identity_hash

res = generate_identity_hash(
    person_id_custom=row["person_id_custom"],
    name_kana_full_raw=row["name_kana_full_raw"],
    gender_code=row["gender_code"],
)

if not res["ok"]:
    raise Exception(res["reason"])

identity_hash = res["value"]
builder_result = res["builder_result"]
field_results = res["field_results"]
```

#### パターン2: person_id_custom を内部生成する場合

```python
res = generate_identity_hash(
    birthdate=row["birthdate"],
    insurer_number_raw=row["insurer_number_raw"],
    insurance_symbol_raw=row["insurance_symbol_raw"],
    insurance_number_raw=row["insurance_number_raw"],
    name_kana_full_raw=row["name_kana_full_raw"],
    gender_code=row["gender_code"],
)

if not res["ok"]:
    raise Exception(res["reason"])

identity_hash = res["value"]
person_id_custom = res["person_id_custom"]
person_id_custom_result = res["person_id_custom_result"]
builder_result = res["builder_result"]
field_results = res["field_results"]
```

---

### generate_identity_bundle

person_id_custom と identity_hash をまとめて生成する。

```python
from scripts.lib.identity.generator import generate_identity_bundle

res = generate_identity_bundle(
    birthdate=row["birthdate"],
    insurer_number_raw=row["insurer_number_raw"],
    insurance_symbol_raw=row["insurance_symbol_raw"],
    insurance_number_raw=row["insurance_number_raw"],
    name_kana_full_raw=row["name_kana_full_raw"],
    gender_code=row["gender_code"],
)

if not res["ok"]:
    raise Exception(res["reason"])

person_id_custom = res["person_id_custom"]
identity_hash = res["identity_hash"]
person_id_custom_result = res["person_id_custom_result"]
identity_hash_result = res["identity_hash_result"]
field_results = res["field_results"]
```

---

## Generator Rule

- generator に identity 生成に必要な入力値を渡す
- generator 内で field を通す
- builder は直接呼ばない（例外的ケースを除く）
- すべての identity 生成は generator を経由する
- generator の戻り値の ok を必ずチェックする
- generator の戻り値は dict であり、必要に応じて reason / field_results / builder_result まで確認する

---

## Updated Data Flow

```text
raw
 ↓
generator
 ↓
field (canonical)
 ↓
builder
 ↓
identity
```