# 判定基盤設計（Check Framework Design）

## 目的

本資料は、健診結果チェックにおける判定基盤の構造、および各層の責務を定義する。

制度ごとの判定仕様（則44・特定健診等）は制度別仕様書で管理する。本資料では、則44判定に必要な取得・正規化・判定処理と、制度非依存で共通化できる最小粒度の判定部品の契約を整理する。

---

# 基本方針

- 判定処理は対象者1人、またはXML1件単位で実施する。
- DBアクセス・取得値の正規化・制度固有判定・共通判定処理を分離する。
- 制度固有の条件は法令項目詳細Noごとの判定処理へ実装する。
- 共通ライブラリは制度知識を持たず、最小粒度の処理のみ提供する。
- 第1層から第3層は、現時点では則44専用処理として `scripts/from_medical/script_lib/` 配下へ配置する。
- 第4層のみ、制度非依存の共通libとして `scripts/lib/examination/check/` 配下へ配置する。
- 則44判定全体を `scripts/lib/examination/` 配下の共通libには置かない。
- 共通lib化は将来利用を想定して先行実施せず、制度をまたいで同一契約・同一責務となる実績が確認できたものだけを後から昇格する。
- 旧設計と本設計が衝突した場合は、本設計を優先する。

---

# 全体構成

```
第1層 必要namecode一覧取得
      scripts/from_medical/script_lib/article44_required_namecodes.py
          │
          ▼
第2層 対象者またはXML単位の一括値取得・状態正規化
      scripts/from_medical/script_lib/article44_value_loader.py
          │
          ▼
第3層 則44の法令項目詳細Noごとの判定処理・全項目オーケストレーション
      scripts/from_medical/script_lib/article44_checker.py
          │
          ├── 第4層の最小関数を利用
          ▼
一人分またはXML1件分の Article44Result
（則44各項目の CheckResult を法令項目詳細Noごとに横並びで保持）
```

第4層は、第3層の項目別判定関数から必要に応じて呼び出される最小部品である。
第1層から第4層までを直列に1回だけ通過し、第4層が処理の最終段として結果を返す構造ではない。

---

# 第1層 必要namecode一覧取得

## 責務

- 則44判定で必要なnamecode一覧を取得する。
- 必要namecodeだけでなく、namecodeごとの期待値型を返す。
- 期待値型は判定上の型契約であり、DB実値の型ではない。
- CO対象namecodeは、現バージョンではCDとして返す。
- 健診結果は取得しない。
- DBの `raw_value_type` との一致判定は第2層で行う。
- 現バージョンでは則44に必要なnamecodeを返す。
- 次バージョンで特定健診を追加する際は、則44と特定健診の必要namecodeの和集合を取得する方向とする。

## 配置

```
scripts/from_medical/script_lib/article44_required_namecodes.py
```

## 入力候補

- 判定対象制度
- ルールバージョン

DB・ruleテーブル構造、SQL、最終返却形式は未決とする。

## 出力

```
required_namecodes: tuple[RequiredNamecode, ...]
```

これは想定例であり、最終コンテナ型は未決とする。

`required_namecodes` は単なる文字列一覧ではなく、namecodeごとの期待値型を含む定義一覧とする。

期待値型は、DBに実際に保存されている型ではなく、以下を表す。

- そのnamecodeを判定上どの値型として扱う予定か
- DBレコードが存在しない場合に、どのValue型で `NOT_FOUND` を生成するか
- DBの `raw_value_type` と比較する基準

型定義の想定例は以下とする。

```python
from dataclasses import dataclass
from enum import Enum


class ExpectedValueType(str, Enum):
    PQ = "PQ"
    CD = "CD"
    ST = "ST"


@dataclass(frozen=True)
class RequiredNamecode:
    namecode: str
    expected_value_type: ExpectedValueType
```

COは現バージョンでは `CDValue` として扱うため、期待値型として独立したCOは設けず、CO対象namecodeも `ExpectedValueType.CD` とする。

返却例は以下とする。

```python
required_namecodes = (
    RequiredNamecode(
        namecode="9N001000000000001",
        expected_value_type=ExpectedValueType.PQ,
    ),
    RequiredNamecode(
        namecode="<CDまたはCOのnamecode>",
        expected_value_type=ExpectedValueType.CD,
    ),
    RequiredNamecode(
        namecode="<STのnamecode>",
        expected_value_type=ExpectedValueType.ST,
    ),
)
```

## 現時点

対象制度

- 労働安全衛生規則第44条

## 将来

対象制度

- 則44
- 特定健診
- その他制度

制度追加時は必要namecodeの和集合を返却する。

---

# 第2層 対象者またはXML単位の一括値取得・状態正規化

## 責務

- `RequiredNamecode` の一覧と、対象者またはXMLを特定するキーを受け取る。
- 対象者1人分またはXML1件分の必要namecodeをDBから一括取得する。
- namecode単位でDBへ繰り返し問い合わせない。
- SQL結果は内部的にリストで受け取ってよい。
- 呼び出し元へはnamecodeで直接参照できるdict形式の `ValueMap` を返す。
- `value_type` ごとに判定で使う最小情報へ正規化する。
- 則44の最終判定は行わない。

## 配置

```
scripts/from_medical/script_lib/article44_value_loader.py
```

## 入力

```
required_namecodes: tuple[RequiredNamecode, ...]
対象者キー または XMLキー
DB接続情報または接続済みオブジェクト
取得元テーブル情報
```

第2層は、単なるnamecode一覧ではなく、namecodeと期待値型を持つ定義一覧を受け取る。

DB接続・テーブル指定の最終形式は未決とする。

## 出力

```
ValueMap
```

`ValueMap` の型は以下とする。

```python
ValueMap = dict[str, PQValue | CDValue | STValue]
```

dictのキーはnamecodeとする。

第2層は、`required_namecodes` に含まれるすべてのnamecodeを `ValueMap` のキーとして返す。
DBに該当レコードが存在しない場合も、`ValueMap` からキーを省略しない。

`ValueMap` の値型は、DB実値の型ではなく、required定義の期待値型で決定する。
同じnamecodeは、`NOT_FOUND`・`NULL`・`EMPTY`・`PRESENT`・型不一致のいずれでも同じValue型を返す。
第3層はnamecodeごとのValue型が固定されていることを前提に扱う。

DBにレコードが存在しない場合は、`expected_value_type` に従って対応する値型を以下の状態で返す。

- `value_state = ValueState.NOT_FOUND`
- `is_valid = False`
- 型別の値フィールドは `None`

生成する値型は以下とする。

- `ExpectedValueType.PQ` → `PQValue(value_state=NOT_FOUND, ...)`
- `ExpectedValueType.CD` → `CDValue(value_state=NOT_FOUND, ...)`
- `ExpectedValueType.ST` → `STValue(value_state=NOT_FOUND, ...)`

CO対象namecodeは `ExpectedValueType.CD` として定義されるため、`CDValue(value_state=NOT_FOUND, ...)` を返す。

値には共通外枠の `ValueEntry` を設けず、`PQValue` / `CDValue` / `STValue` を `ValueMap` へ直接格納する。

```python
{
    "<PQのnamecode>": PQValue(...),
    "<CDのnamecode>": CDValue(...),
    "<STのnamecode>": STValue(...),
}
```

各値型にはnamecodeを重複保持しない。

型ごとに必要な情報が異なるため、共通外枠へ無理に集約しない。

この方針の理由は以下とする。

- 型と中身の矛盾を作らない。
- 型ごとに不要な項目を持たせない。
- checker側で余分な外枠を開かない。
- 共通化のための共通化を避ける。

`PQValue` / `CDValue` / `STValue` の詳細フィールドは本資料で定義する。

第3層は、必要namecodeが `ValueMap` に必ず存在することを前提に、添字参照で取得してよい。
第3層で `dict.get()` によるキー不存在判定を通常の欠損判定として使用しない。
キー不存在は、DB欠損ではなくloaderまたはcheckerの実装不整合として扱う。

DBレコードが存在する場合は、期待値型とDBの `raw_value_type` を比較する。
期待値型と実際の型が一致しない場合でも、値は存在するため `value_state=PRESENT` とする。
型不一致の場合は、期待値型に対応するValue型を返し、`is_valid=False` とする。
この場合、`invalid_reason=ValueInvalidReason.TYPE_MISMATCH` とする。
DB原文は対応するrawフィールドへ可能な範囲で保持し、変換後値は `None` とする。

型不一致例の想定は以下とする。

```text
期待値型: PQ
DB raw_value_type: ST
```

この場合は、DB実値の型に合わせて `STValue` を返さず、期待値型に合わせて `PQValue` を返す。

```python
PQValue(
    value_state=ValueState.PRESENT,
    raw_value="<DB原文>",
    numeric_value=None,
    unit="<DB単位またはNone>",
    is_valid=False,
    invalid_reason=ValueInvalidReason.TYPE_MISMATCH,
)
```

これは、`ValueMap` の各namecodeが、第3層のchecker側から見て常に同じValue型を返す契約を維持するためである。

## NOT_FOUNDの返却例

### PQ

```python
PQValue(
    value_state=ValueState.NOT_FOUND,
    raw_value=None,
    numeric_value=None,
    unit=None,
    is_valid=False,
    invalid_reason=None,
)
```

### CDまたはCO

```python
CDValue(
    value_state=ValueState.NOT_FOUND,
    raw_value=None,
    code_value=None,
    is_valid=False,
    invalid_reason=None,
)
```

### ST

```python
STValue(
    value_state=ValueState.NOT_FOUND,
    raw_text=None,
    text=None,
    is_valid=False,
    invalid_reason=None,
)
```

同一namecodeが複数件存在する場合の扱いも未決とする。

---

# 値状態

取得した値は最低限以下を区別する。

|状態|意味|
|---|---|
|NOT_FOUND|レコード自体が存在しない|
|NULL|レコードは存在するがNULL|
|EMPTY|空文字または空白のみ|
|PRESENT|NULL・空文字ではない値が存在する。型として判定処理に利用可能かはis_validで表す。|

`NOT_FOUND`、`NULL`、`EMPTY` は取得時点で同一状態へ潰さず、区別したまま第3層へ渡す。

## NULL

`NULL` は、DBレコードは存在するが、対象となる値カラムがSQL NULLである状態とする。
`NULL` はレコード不存在を表す `NOT_FOUND` とは区別する。

`NULL` の場合は `is_valid=False` とし、型別の変換後値は `None` とする。

例

```python
raw_value is None
```

STの場合は以下とする。

```python
raw_text is None
```

CDの場合は、CDとして使用する `code_value` がSQL NULLである状態を `NULL` とする。

## EMPTY

`EMPTY` は、DBレコードおよび値カラムは存在するが、値が空文字または空白文字だけで構成されている状態とする。
空白文字には、少なくとも半角スペース、全角スペース、改行、CR、LF、タブを含める。

判定用の最低限正規化後に空文字となる場合も `EMPTY` とする。

`EMPTY` の場合は `is_valid=False` とする。
`NULL` と `EMPTY` は第2層で同一状態へ潰さず、第3層へ区別したまま渡す。

例

```text
""
"   "
"　"
"\n\t"
"\r\n"
```

## value_stateとis_valid

- `value_state` は、DB上で値がどのように存在していたかを表す。
- `is_valid` は、各値型として判定処理に利用可能かを表す。
- `invalid_reason` は、`value_state=PRESENT` だが、`is_valid=False` となった理由を表す。
- 値が存在していても型変換や形式検証に失敗する場合があるため、両者は別責務とする。
- 型不正の場合は `value_state=PRESENT` のまま、`is_valid=False` とする。
- `NOT_FOUND / NULL / EMPTY` はすべて `is_valid=False` とする。
- `NOT_FOUND / NULL / EMPTY`、および `PRESENT` かつ `is_valid=True` の場合は `invalid_reason=None` とする。
- `NOT_FOUND / NULL / EMPTY` は `value_state` だけで状態を特定できるため、`invalid_reason` へ重複保持しない。

## ValueInvalidReason

値の存在状態とは別に、`PRESENT` だが型として利用できない理由を表す共通Enumを定義する。

現時点では以下の3種類のみ定義し、理由を過度に細分化しない。

```python
from enum import Enum


class ValueInvalidReason(str, Enum):
    TYPE_MISMATCH = "TYPE_MISMATCH"
    PARSE_ERROR = "PARSE_ERROR"
    FORMAT_ERROR = "FORMAT_ERROR"
```

各値の意味は以下とする。

|理由|意味|
|---|---|
|TYPE_MISMATCH|required定義の期待値型とDBのraw_value_typeが一致しない|
|PARSE_ERROR|期待値型とDB型は一致しているが、値を必要な型へ変換できない。現時点では主にPQのDecimal変換失敗で使用する|
|FORMAT_ERROR|期待値型とDB型は一致しているが、値がその型の最低限の形式要件を満たさない。現時点では主にCDまたはCOの形式不正で使用する|

`invalid_reason` は第2層の正規化・形式検証結果を第3層へ渡すための中間情報であり、最終的な業務reasonは第3層が決定する。

設計理由は以下とする。

- `is_valid=False` だけでは、型不一致・PQ変換不能・CD形式不正を区別できない。
- 第3層が項目別reasonを生成するには、利用不能理由を `ValueMap` 内で保持する必要がある。
- `invalid_reason` は第2層の正規化・形式検証結果を第3層へ渡すための中間情報である。
- 最終的な業務reasonは第3層が決定する。
- 各詳細項目のreasonは最終的に `a44_<法令項目詳細No>_reason` へ保存される。

## 今回の決定例

| DB状態・入力値 | value_state | is_valid |
|---|---|---:|
| レコードなし | NOT_FOUND | False |
| レコードあり、SQL NULL | NULL | False |
| レコードあり、空文字・空白のみ | EMPTY | False |
| 値あり、型として正常 | PRESENT | True |
| 値あり、型変換・形式検証に失敗 | PRESENT | False |

---

# value_typeごとの保持方針

## ST

`STValue` の詳細フィールドは以下とする。

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class STValue:
    value_state: ValueState
    raw_text: str | None
    text: str | None
    is_valid: bool
    invalid_reason: ValueInvalidReason | None
```

各フィールドの意味は以下とする。

### `value_state`

- DB上の値状態を表す。
- `NOT_FOUND / NULL / EMPTY / PRESENT` を使用する。

### `raw_text`

- DBに保存されている未正規化の原文全文。
- 証跡として内容を失わないため、そのまま保持する。

### `text`

- 第3層の検索・判定で使用する判定用文字列。
- `raw_text` に最低限の共通文字列正規化を行った結果を保持する。

現時点の正規化は以下に限定する。

- UnicodeをNFKCへ正規化する。
- CRLFおよびCRをLFへ統一する。
- 改行を半角スペースへ置換する。
- タブを半角スペースへ置換する。
- 連続する空白を1文字へ圧縮する。
- 先頭・末尾の空白を除去する。

以下は行わない。

- 意味の置換
- 同義語への名寄せ
- 固定検索語との一致判定
- 所見有無CDとの整合性判定

### `is_valid`

- STとして判定処理に利用可能かを表す。
- `value_state == ValueState.PRESENT` かつ、正規化後の `text` が空でない場合に `True` とする。
- 検索語に一致しないこと自体は不正ではないため、`is_valid=False` にはしない。

## STValueの値状態決定順

STでは以下の順で値状態を決定する。

1. DBレコードが存在しない場合は `NOT_FOUND`。
2. `raw_text is None` の場合は `NULL`。
3. STの最低限正規化を実施する。
4. 正規化後の `text` が空の場合は `EMPTY`。
5. それ以外は `PRESENT`。
6. `PRESENT` かつ `text` が空でなければ `is_valid=True`。

STの原文が空白・改行・タブだけの場合は、`raw_text` は原文のまま保持し、`text=""`、`value_state=EMPTY`、`is_valid=False` とする。

## STValueの状態例

### 正常値

```python
STValue(
    value_state=ValueState.PRESENT,
    raw_text="胸部X線検査の結果、\n異常所見なし",
    text="胸部X線検査の結果、 異常所見なし",
    is_valid=True,
    invalid_reason=None,
)
```

### 空白・改行のみ

```python
STValue(
    value_state=ValueState.EMPTY,
    raw_text="  \n\t  ",
    text="",
    is_valid=False,
    invalid_reason=None,
)
```

### NULL値

```python
STValue(
    value_state=ValueState.NULL,
    raw_text=None,
    text=None,
    is_valid=False,
    invalid_reason=None,
)
```

### 期待値型ST、DB raw_value_typeがPQまたはCD

```python
STValue(
    value_state=ValueState.PRESENT,
    raw_text="<DB原文>",
    text="<最低限正規化後文字列またはNone>",
    is_valid=False,
    invalid_reason=ValueInvalidReason.TYPE_MISMATCH,
)
```

STは現時点で、非空文字列として成立していれば `PARSE_ERROR` または `FORMAT_ERROR` は使用しない。
検索語に一致しないことも `invalid_reason` にはしない。

## STValueの設計理由

- `raw_text` はDB原文の証跡として保持する。
- `text` は後続checkerで同じ文字列前処理を繰り返さないために保持する。
- 改行、タブ、連続空白を除き、検索語の包含判定を安定させる。
- STの検索語は制度・項目固有なので、`STValue` や第2層へ固定しない。
- 「異常なし」「所見なし」等の検索語は、第3層の法令項目詳細Noごとのcheckerで定義する。
- 所見有無CDとの整合性も第3層で判定する。
- 例として「所見ありなのにSTがない」場合は、第3層でNG系の判定対象とする。
- `STValue` は最低限の文字列整形と利用可能性の保証のみを担当する。

## STValueで今回行わないこと

以下は未決または対象外として残す。

- 「異常なし」「所見なし」等の固定検索語の保持
- 検索語との一致結果の保持
- 同義語への名寄せ
- 所見有無CDとの整合性判定
- 項目別status・reasonの決定
- ST固有エラー理由の保持

---

## CD

`CDValue` の詳細フィールドは以下とする。

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class CDValue:
    value_state: ValueState
    raw_value: str | None
    code_value: str | None
    is_valid: bool
    invalid_reason: ValueInvalidReason | None
```

各フィールドの意味は以下とする。

### `value_state`

- 値の存在状態。
- `NOT_FOUND / NULL / EMPTY / PRESENT`。

### `raw_value`

- XMLから取得した元文字列。

### `code_value`

- XMLまたはDBから取得したCDコード文字列を保持する。
- 形式不正の場合でも、値が存在する場合は元コード文字列を保持する。
- CDとして形式的に利用可能かは `is_valid` で表す。
- `code_value` がNULLまたはEMPTYの場合は、`value_state` で区別する。
- 型は `str` のままとする。

### `is_valid`

- CDとして形式的に有効かどうか。

## CDValueの判定方針

今回対象とする健診結果XMLでは、CD値は数値コードを前提とする。

有効値の例は以下とする。

- `1`
- `2`
- `10`
- `999`

不正値の例は以下とする。

- `01`
- `001`
- `0`
- `A1`
- `1A`
- 空文字

HL7全体では先頭ゼロ付きコードが存在する場合がある。
ただし、本システムでは健診結果XMLを対象とし、今回対象とするCD値は数値コードを前提とするため、先頭ゼロ付きコードは不正値として扱う。

項目ごとの許可コード判定は `CDValue` では行わず、第3層のchecker側で実施する。
`CDValue` は最低限の形式保証のみを担当する。

`raw_value_type="CO"` は、現バージョンでは順序比較を行わない。
そのため、COはCDと同じ形式検証および保持形式を使用する。
ValueMap上ではCO専用の値型を追加せず、`CDValue` として保持する。

```
CD → CDValue
CO → CDValue
```

CO固有の順序比較、優先度判定は現バージョンでは行わない。

## CDValueの値状態決定順

CDおよびCOでは以下の順で値状態を決定する。

1. DBレコードが存在しない場合は `NOT_FOUND`。
2. `code_value is None` の場合は `NULL`。
3. `code_value` の前後空白を除去した結果が空の場合は `EMPTY`。
4. それ以外は `PRESENT`。
5. `PRESENT` の場合のみ、健診結果CDの形式チェックを行う。
6. 形式チェックを通れば `is_valid=True`。
7. 形式不正でも `value_state=PRESENT` のまま `is_valid=False`。

`raw_value` に文字列があっても、CDとして使用する `code_value` がNULLまたはEMPTYの場合は、CDの値状態は `code_value` を基準に決める。

## CDValueの状態例

### 正常値

```python
CDValue(
    value_state=ValueState.PRESENT,
    raw_value="1",
    code_value="1",
    is_valid=True,
    invalid_reason=None,
)
```

### 不正コード（01）

```python
CDValue(
    value_state=ValueState.PRESENT,
    raw_value="01",
    code_value="01",
    is_valid=False,
    invalid_reason=ValueInvalidReason.FORMAT_ERROR,
)
```

### 不正コード（A1）

```python
CDValue(
    value_state=ValueState.PRESENT,
    raw_value="A1",
    code_value="A1",
    is_valid=False,
    invalid_reason=ValueInvalidReason.FORMAT_ERROR,
)
```

### NULL値

```python
CDValue(
    value_state=ValueState.NULL,
    raw_value=None,
    code_value=None,
    is_valid=False,
    invalid_reason=None,
)
```

### 期待値型CD、DB raw_value_typeがPQまたはST

```python
CDValue(
    value_state=ValueState.PRESENT,
    raw_value="<DB原文>",
    code_value="<DBコード値またはNone>",
    is_valid=False,
    invalid_reason=ValueInvalidReason.TYPE_MISMATCH,
)
```

## CDValueの設計理由

- `value_state` は、DB上で値がどのように存在していたかを表す。
- `is_valid` は、CDとして形式的に判定処理へ利用可能かを表す。
- 値が存在していてもCDとして形式的に利用できない場合があるため、両者は別責務とする。
- `code_value` は計算対象ではなくコードであるため、数値型へ変換せず `str` として保持する。
- `001` と `1` のような表現差を同一視しないため、コード値は文字列として扱う。
- 項目ごとの許可コード判定は制度・項目固有のルールであり、`CDValue` ではなく第3層のchecker側で行う。
- `CDValue` はXML台帳1件分の処理内で使う小さな値オブジェクトであり、`is_valid` を独立保持することによる負荷は小さい。

## CDValueで今回行わないこと

以下は未決または対象外として残す。

- コード体系との照合
- 項目ごとの許可コード判定
- コード意味の保持
- HL7コード体系全体への対応
- CO固有の順序比較・優先度判定
- CD固有エラー理由の保持

---

## PQ

`PQValue` の詳細フィールドは以下とする。

```python
from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class PQValue:
    value_state: ValueState
    raw_value: str | None
    numeric_value: Decimal | None
    unit: str | None
    is_valid: bool
    invalid_reason: ValueInvalidReason | None
```

各フィールドの意味は以下とする。

### `value_state`

- DB上の値状態。
- `NOT_FOUND / NULL / EMPTY / PRESENT`。

### `raw_value`

- DBに保存されている未正規化の原文。

### `numeric_value`

- `raw_value` を `Decimal` へ変換できた場合の値。
- 変換不能の場合は `None`。

### `unit`

- DBに保存されている単位。
- 現時点では単位変換や想定単位との一致確認は行わない。

### `is_valid`

- PQとして判定処理に利用可能かを表す。
- 現時点では主に `Decimal` 変換可否で決定する。
- `numeric_value is not None` の代用ではなく、利用可否を明示する独立フィールドとする。

## PQValueの値状態決定順

PQでは以下の順で値状態を決定する。

1. DBレコードが存在しない場合は `NOT_FOUND`。
2. `raw_value is None` の場合は `NULL`。
3. `raw_value` の前後空白を除去した結果が空の場合は `EMPTY`。
4. それ以外は `PRESENT`。
5. `PRESENT` の場合のみ `Decimal` 変換を試みる。
6. Decimal変換できれば `is_valid=True`。
7. Decimal変換できなければ `value_state=PRESENT` のまま `is_valid=False`。

## PQValueの状態例

### 正常値

```python
PQValue(
    value_state=ValueState.PRESENT,
    raw_value="172.5",
    numeric_value=Decimal("172.5"),
    unit="cm",
    is_valid=True,
    invalid_reason=None,
)
```

### 数値変換不能

```python
PQValue(
    value_state=ValueState.PRESENT,
    raw_value="測定不能",
    numeric_value=None,
    unit="cm",
    is_valid=False,
    invalid_reason=ValueInvalidReason.PARSE_ERROR,
)
```

### NULL

```python
PQValue(
    value_state=ValueState.NULL,
    raw_value=None,
    numeric_value=None,
    unit="cm",
    is_valid=False,
    invalid_reason=None,
)
```

### 期待値型PQ、DB raw_value_typeがST

```python
PQValue(
    value_state=ValueState.PRESENT,
    raw_value="<DB原文>",
    numeric_value=None,
    unit="<DB単位またはNone>",
    is_valid=False,
    invalid_reason=ValueInvalidReason.TYPE_MISMATCH,
)
```

## PQValueの設計理由

- `value_state` は、DB上で値がどのように存在していたかを表す。
- `is_valid` は、PQとして判定処理に利用可能かを表す。
- 値が存在していてもDecimalへ変換できない場合があるため、両者は別責務とする。
- 将来、単位不正や業務ルール違反を `is_valid=False` として扱える余地を残す。
- `PQValue` はXML台帳1件分の処理内で使う小さな値オブジェクトであり、`is_valid` を独立保持することによる負荷は小さい。

## PQValueで今回行わないこと

以下は未決または対象外として残す。

- 単位変換
- 想定単位との一致判定
- 数値範囲チェック
- 小数桁数チェック
- PQ固有のエラー理由フィールド

---

# 第2層で行わないこと

- 則44として有効かどうかの判定
- 特定健診として有効かどうかの判定
- 法令項目詳細Noごとのstatus・reason判定
- 制度別総合判定

---

# 第3層 法令項目詳細No判定

## 責務

- 則44の法令項目詳細Noごとの判定を実装する。
- 第2層の `ValueMap` から必要な値・状態を取得する。
- 第4層の最小関数を組み合わせる。
- 項目固有の判定順と条件を実装する。
- `value_state`、`is_valid`、`invalid_reason` を参照して各法令項目詳細Noの `status` と `reason` を決定する。
- 法令項目詳細Noごとの小さな判定関数を配置する。
- 則44の全項目を一巡させるオーケストレーション関数を配置する。
- 一人分またはXML1件分の各 `CheckResult` を、法令項目詳細Noをキーとする横並び結果として返す。

## 配置

```
scripts/from_medical/script_lib/article44_checker.py
```

## 実装単位

`article44_checker.py` は、全項目を1つの巨大関数にしない。
法令項目詳細Noごとの小さな判定関数を作る。

例

```
check_4401001001_medical_history(value_map)
check_4402001001_subjective_symptoms(value_map)
check_4402001002_objective_symptoms(value_map)
check_4403003001_waist(value_map)
check_4408001003_triglycerides(value_map)
check_4409001001_blood_glucose(value_map)
```

TGの関数名は、コード上の可読性のため `triglycerides` 等の意味が分かる英語名を使用してよい。

ただし以下は法令項目詳細Noベースで統一する。

- Article44Resultのキー
- DBカラム名
- 32との紐付け
- checker一覧の識別キー

関数名の英語部分は補助的な可読名であり、正式な識別子は法令項目詳細Noとする。

実際の実装対象一覧・関数名は、32の法令項目詳細Noのうち現バージョンの則44有無判定対象23項目と一致させる。

法令項目詳細Noと判定処理は一対一とする。

現バージョンでは、任意項目である業務歴（4401001002）・喀痰（4404002001）の判定関数は作成しない。

## 全項目オーケストレーション

法令項目詳細Noと項目別関数を対応付けた一覧から順に呼び出す構造とする。

想定形は以下とする。

```python
ARTICLE44_CHECKERS = {
    "4401001001": check_4401001001_medical_history,
    "4402001001": check_4402001001_subjective_symptoms,
    "4402001002": check_4402001002_objective_symptoms,
    "4403003001": check_4403003001_waist,
    "4408001003": check_4408001003_triglycerides,
    "4409001001": check_4409001001_blood_glucose,
}


def check_article44(value_map):
    return {
        detail_no: checker(value_map)
        for detail_no, checker in ARTICLE44_CHECKERS.items()
    }
```

これはインターフェースの想定例であり、型注釈・最終関数名・全項目一覧は未決とする。

全項目オーケストレーション関数の責務は以下とする。

- 32で定義された対象項目のうち、現バージョンの則44有無判定対象23項目を漏れなく一巡する。
- 法令項目詳細Noごとの関数を呼び出す。
- 各関数から `CheckResult` を受け取る。
- 一人分またはXML1件分の `Article44Result` を返す。
- DBへ直接書き込まない。
- 制度総合判定を保存しない。
- 個別判定ロジックをオーケストレーション関数内へ重複実装しない。

## この層へ実装するもの

- 既往歴・自覚症状・他覚症状・心電図の所見有無と所見詳細
- 腹囲の実測・自己測定・BMI条件付き自己申告
- 視力の左右と裸眼・矯正
- 聴力の4項目と会話法へのフォールバック
- 胸部X線の検査結果と複数所見パターン
- 血糖の空腹時血糖・HbA1c・条件付き随時血糖

## この層で行わないこと

- DBアクセス
- 必要namecode一覧の決定
- 生データ取得
- resultテーブルへの記帳
- 則44以外の制度判定

## 返却

各法令項目詳細Noの判定関数は、共通形式の `CheckResult` を返す。

`CheckResult` は、現行72項目方式の `ItemResult` と同じ考え方で扱う。

最終インターフェース例は以下とする。

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class CheckResult:
    status: str
    reason: str | None = None
```

最終クラス名は未決でもよいが、現バージョンでは以下を確定する。

- `status` は文字列とする。
- `reason` は `str | None` とする。
- 新しいstatus体系やreason体系は作らず、現行方式を踏襲する。

`CheckResult.status` は、現行の項目別status体系を踏襲する。

現行項目別statusは以下を使用する。

- `OK`
- `CALCULATED`
- `ALTERNATIVE`
- `MISSING`
- `INVALID`

`WARNING` / `NG` は項目別statusではない。
これらは制度別集約結果および `xml_ledger.check_status` の値であり、`CheckResult.status` では使用しない。

`CheckResult.reason` は以下の方針とする。

- 型は `str | None` とする。
- 正常時は原則 `None` とする。
- 異常時は現行方式と同様に文字列コードを保持する。
- DB保存時は `text` カラムへ保存する。
- 新しいreason型や専用Enumは追加しない。

`ValueInvalidReason` は、そのままDBのreason文字列へ機械的に保存するための最終reasonではない。
`ValueInvalidReason` は `CheckResult.reason` そのものではない。
第2層で保持した `invalid_reason` を、第3層checkerが制度・項目別のreason文字列へ変換するための中間情報として使用する。
第3層が制度・項目固有の文脈を加えて `CheckResult.reason` を決定する。

`CheckResult.reason` は後続で以下のDBカラムへ保存される。

```
a44_<法令項目詳細No>_reason
```

例

```text
ValueInvalidReason.TYPE_MISMATCH
    ↓ 第3層checker
CheckResult(
    status=...,
    reason="EXPECTED_PQ_BUT_ACTUAL_ST",
)
    ↓ 後続保存処理
a44_4403001001_reason
```

全項目オーケストレーション関数は、一人分またはXML1件分の則44判定結果を返す。

想定形は以下とする。

```python
{
    "4401001001": CheckResult(...),
    "4402001001": CheckResult(...),
    "4402001002": CheckResult(...),
    ...
}
```

この一人分またはXML1件分の集合を、本資料では仮に `Article44Result` と呼ぶ。

現行72項目方式では、一人分またはXML1件分の判定結果を `dict[str, ItemResult]` で保持している。
今回も同じ考え方を踏襲し、`Article44Result` は `dict[str, CheckResult]` とする。

違いはキーのみである。

- 現行: `identity_code`
- 今回: 法令項目詳細No

今回の設計は、現行72項目の横持ち判定方式を廃止するものではない。
現行の判定結果保持・横持ち保存方式を踏襲し、判定対象を法令項目詳細Noベース23項目へ差し替えることを目的とする。

`Article44Result` の責務は以下とする。

- 則44の各項目判定を法令項目詳細Noごとに横並びで保持する。
- 現バージョンでは任意項目を含まない23項目の `CheckResult` を保持する。
- 後続のDB記帳へ渡す。
- 後続の則44総合判定へ渡す。
- 必要に応じてCSV出力等へ渡す。

以下は未決として保留する。

- 便利メソッドを持たせるか
- 項目順序をどこで保証するか

---

# 横持ちresultカラム命名規則

則44の項目別判定結果を横持ちresultへ保存する際のカラム名は、以下の規則とする。

```
a44_<法令項目詳細No>_status
a44_<法令項目詳細No>_reason
```

例

```
a44_4401001001_status
a44_4401001001_reason

a44_4402001001_status
a44_4402001001_reason

a44_4411001001_status
a44_4411001001_reason
```

命名理由は以下とする。

- `a44` は労働安全衛生規則第44条の判定であることを示す。
- 法令項目詳細Noをそのまま含め、32の仕様・第3層の判定関数・Article44Result・DBカラムを同じ番号で追跡可能にする。
- 日本語項目名や英語略称をカラム名へ直接入れず、名称変更や表記揺れの影響を避ける。
- `status` と `reason` を各項目で対にして保持する。
- MySQLのカラム名上限64文字に対し、本命名は約21文字であり制限内である。

`TG` についても例外的に `tg` 等の略称をカラム名へ入れず、他項目と同様に法令項目詳細Noベースとする。

## 法令項目詳細Noとカラム対応表

一覧上は法令項目詳細Noとの対応を明確にするため、現バージョンの則44有無判定対象23項目を掲載する。

| 詳細項目 | 法令項目詳細No | statusカラム | reasonカラム |
|---|---:|---|---|
| 既往歴 | 4401001001 | `a44_4401001001_status` | `a44_4401001001_reason` |
| 自覚症状 | 4402001001 | `a44_4402001001_status` | `a44_4402001001_reason` |
| 他覚症状 | 4402001002 | `a44_4402001002_status` | `a44_4402001002_reason` |
| 身長 | 4403001001 | `a44_4403001001_status` | `a44_4403001001_reason` |
| 体重 | 4403002001 | `a44_4403002001_status` | `a44_4403002001_reason` |
| 腹囲 | 4403003001 | `a44_4403003001_status` | `a44_4403003001_reason` |
| 視力 | 4403004001 | `a44_4403004001_status` | `a44_4403004001_reason` |
| 聴力 | 4403005001 | `a44_4403005001_status` | `a44_4403005001_reason` |
| 胸部X線 | 4404001001 | `a44_4404001001_status` | `a44_4404001001_reason` |
| 収縮期血圧 | 4405001001 | `a44_4405001001_status` | `a44_4405001001_reason` |
| 拡張期血圧 | 4405001002 | `a44_4405001002_status` | `a44_4405001002_reason` |
| 血色素量（ヘモグロビン） | 4406001001 | `a44_4406001001_status` | `a44_4406001001_reason` |
| 赤血球数 | 4406001002 | `a44_4406001002_status` | `a44_4406001002_reason` |
| AST | 4407001001 | `a44_4407001001_status` | `a44_4407001001_reason` |
| ALT | 4407001002 | `a44_4407001002_status` | `a44_4407001002_reason` |
| γ-GT | 4407001003 | `a44_4407001003_status` | `a44_4407001003_reason` |
| LDL | 4408001001 | `a44_4408001001_status` | `a44_4408001001_reason` |
| HDL | 4408001002 | `a44_4408001002_status` | `a44_4408001002_reason` |
| TG（中性脂肪） | 4408001003 | `a44_4408001003_status` | `a44_4408001003_reason` |
| 血糖 | 4409001001 | `a44_4409001001_status` | `a44_4409001001_reason` |
| 尿糖 | 4410001001 | `a44_4410001001_status` | `a44_4410001001_reason` |
| 尿蛋白 | 4410001002 | `a44_4410001002_status` | `a44_4410001002_reason` |
| 心電図 | 4411001001 | `a44_4411001001_status` | `a44_4411001001_reason` |

現在の則44有無判定で必須として横持ち判定対象とするのは、任意項目を除く23項目である。

本資料のカラム対応表も、この23項目のみを対象とする。

## 任意項目

現バージョンでは以下を則44有無判定対象外とする。

- 業務歴（4401001002）
- 喀痰（4404002001）

そのため、現バージョンでは以下の扱いとする。

- 横持ち判定対象23項目には含めない。
- 判定関数は実装しない。
- `Article44Result` にも含めない。

ただし、制度要件や仕様変更により対象となる可能性があるため、法令項目詳細No自体は管理対象として残す。

## Article44Resultとの対応

`Article44Result` のキーには法令項目詳細Noを使用する。

現バージョンでは任意項目を含まない23項目の `CheckResult` を保持する。

想定形は以下とする。

```python
{
    "4401001001": CheckResult(...),
    "4402001001": CheckResult(...),
    "4402001002": CheckResult(...),
    ...
}
```

DB保存時は以下の規則で機械的に対応させる。

```
Article44Result["4401001001"].status
    → a44_4401001001_status

Article44Result["4401001001"].reason
    → a44_4401001001_reason
```

この対応により、項目名の日本語・英語変換表を介さず、法令項目詳細Noだけで仕様・処理・保存先を追跡できる。

ただし、`Article44Result` からDBカラムへの実際の変換・保存処理は判定基盤の外側であり、Codexへ任せる接続範囲とする。

---

# 第4層 共通判定ライブラリ

## 責務

- 制度知識を持たない。
- DBアクセスを行わない。
- 最小粒度の処理のみ提供する。

## 配置

```
scripts/lib/examination/check/
├── all.py
├── any.py
├── compare.py
├── finding.py
└── priority.py
```

## 設計方針

最初から巨大なRuleEngineは作らない。

32の項目仕様を確認した結果、複数項目で同じ意味・同じ入力・同じ返却となるものだけを共通化する。

以下のような複合パターン全体は第4層へ置かない。

- `ANY + FINDING(OR)`
- `ALL + FALLBACK`
- 条件付きPRIORITY
- 制度固有のCONDITIONAL

第4層には最小関数だけを置き、第3層の項目別関数で順番に呼び出して組み立てる。

現時点の役割候補は以下とする。

### `any.py`

- 複数候補のうち1件以上に有効値があるかを判定する。

### `all.py`

- 指定した候補すべてに有効値があるかを判定する。

### `compare.py`

- 数値比較またはコード比較を行う。
- 入力、演算子、返却型は未決とする。

### `finding.py`

- 所見有無CDと所見詳細状態を受け取り、最小単位の所見成立状態を返す。
- 制度別の最終 `status`・`reason` は第3層が決定する。

### `priority.py`

- 優先順位付き候補から最初の有効値を選択する。

関数名、入力型、返却型、例外方針は今後1関数ずつ協議するため未決とする。

---

# 複合パターンの組み立て場所

複合パターンの最終的な `status`・`reason` は、第3層の法令項目詳細Noごとの判定関数で決定する。

## 胸部X線の例

```
1. any.pyで胸部X線検査結果の有無を確認する。
2. 検査結果がなければfinding.pyで所見パターン1を確認する。
3. 必要に応じてfinding.pyで所見パターン2を確認する。
4. 複数所見パターンのOR、WARNING優先条件、最終status・reasonは第3層で決定する。
```

## 聴力の例

```
1. all.pyで4項目すべての有無を確認する。
2. ALLが成立しない場合のみ、会話法の有無を確認する。
3. 項目別status・reasonの最終判定は第3層で決定する。
```

---

# 4層を一巡した最終契約

一人分またはXML1件分について、以下の処理を行う。

```
required_namecodes取得
        ↓
ValueMap構築
        ↓
則44各項目関数を実行
        ↓
Article44Resultを返却
```

判定基盤の責務は `Article44Result` を返すところまでとする。

以下は判定基盤の外側とする。

- resultテーブルへの横持ち記帳
- 則44総合判定の保存
- 特定健診総合判定
- CSV出力
- トランザクション管理
- DB upsert

---

# 層間責務

| 層 | 責務 | 配置 |
|---|---|---|
| 第1層 | 何を取得するか決める | `article44_required_namecodes.py` |
| 第2層 | 対象者・XMLの値と状態を一括取得してdictで返す | `article44_value_loader.py` |
| 第3層 | 則44の法令項目詳細Noごとの判定を組み立て、全項目を一巡させて一人分の横並び結果を返す | `article44_checker.py` |
| 第4層 | 制度非依存の最小判定部品を提供する | `scripts/lib/examination/check/` |

---

# 実装責任

## 今回こちらで詳細設計・実装する範囲

- 第2層のValueMap返却契約
- ST / CD / PQの正規化仕様
- 法令項目詳細Noごとの小さな判定関数
- 全項目オーケストレーション関数
- 第4層の最小粒度関数
- CheckResult契約
- Article44Resultの返却契約
- 単体テスト

## 既存システムとの接続時にCodexへ任せる範囲

- 第1層のDB・ruleテーブル接続
- 第2層のSQL・DB取得処理
- 本体スクリプトから4層を一巡させる呼び出し配線
- `Article44Result` から横持ちカラムへの変換
- resultテーブルへの保存
- 制度別総合判定
- DDL / migration / 既存テーブル移行

接続側では、第3層の項目判定や第4層の最小関数を再解釈・再実装しない。
接続側は `Article44Result` を受け取り、保存・総合判定・CSV出力など後続処理へ渡す。

Codex側では以下を行わない。

- 法令項目詳細Noごとの小さな判定関数の再実装
- 全項目オーケストレーション関数の再設計
- 第4層の最小関数の再実装
- 32にない制度条件の追加
- カラム名と法令項目詳細Noの独自変換

---

# 関連資料

```
03_decisions.md
決定事項

05_design_history.md
協議履歴

31_phase7_legal_check_redesign.md
全体構造

32_article44_exam_check_spec.md
則44項目仕様

33_check_framework_design.md
判定基盤仕様
```

---

# 保留事項

- 第1層のDB・ruleテーブル構造
- 第1層の最終返却形式
- RequiredNamecodeの最終クラス名
- ExpectedValueTypeの最終Enum名
- 第1層の最終コンテナ型
- ValueInvalidReasonの最終Enum名
- ValueInvalidReasonからCheckResult.reasonへの変換方針
- CheckResult.reasonの具体的なコード体系
- 同一namecodeが複数件存在する場合の扱い
- method・identity別索引を追加するか
- Article44Resultに便利メソッドを持たせるか
- Article44Resultの項目順序をどこで保証するか
- 業務歴を将来Article44Resultへ追加するか
- 喀痰を将来Article44Resultへ追加するか
- 任意項目を横持ちカラムとして物理作成するか
- statusカラムのDB型
- reasonカラムのDB型・最大長
- Article44Resultから横持ちカラムへの変換方法
- migrationで既存カラムを残す・変更する・廃止する基準
- 第4層各関数の入力・返却・例外仕様

---

# 次回検討

1. CheckResult.reasonの具体的なコード体系を確定する。
2. 同一namecodeが複数件存在する場合の扱いを確定する。
3. Article44Resultの項目順序をどこで保証するか確定する。
4. 第4層の最初の関数を1つ選ぶ。
5. 決定内容を本資料と03へ同期する。
