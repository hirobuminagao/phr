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
- 第1層〜第3層で共有する則44専用の型定義は `scripts/from_medical/script_lib/article44_models.py` へ配置する。
- `article44_models.py` は処理層ではなく、第1層〜第3層間のデータ契約を定義する型モジュールである。
- `article44_models.py` はDBアクセス、値取得、正規化、判定処理、保存処理を持たない。
- 第4層は制度非依存の最小関数だけを置くため、則44専用型を `scripts/lib/examination/check/` へ配置しない。
- 第1層、第2層、第3層は同じ型定義をimportして使用する。
- 各処理ファイル内で同じdataclassやEnumを重複定義しない。
- 則44の23項目に関する具体的な判定ルールは、`article44_checker.py` を正とする。
- DBから判定ルールを動的生成しない。
- DBの `method` を読み取ってRuleEngineのように判定処理を組み立てない。
- 則44用の必要namecode定義は、`exam_item_groups` と `exam_item_group_members` を正とする。
- `exam_item_group_members` はnamecode集合だけでなく、取得時に必要なメタ情報も同一行へ保持する。
- 則44第1層の必要namecode取得では、`exam_item_master` とのJOINを前提としない。
- `exam_item_master` は現時点で必要な情報が完全に揃っている保証がないため、則44第1層の必須参照先にはしない。
- 第1層のDB参照は、則44判定に必要なnamecode群と取得時に必要なメタ情報を一括取得するために使用する。
- DBは「必要な値を取得するための定義」を持ち、Pythonは「取得した値をどう組み合わせて判定するか」を持つ。
- 取得処理を単純化し、処理開始時に必要定義を1回で取得できることを優先する。
- 同じDB定義を対象者ごとに繰り返し取得せず、処理開始時など適切な単位で一度取得して再利用する。
- `exam_item_master` と情報が重複しても許容する。
- 今回の定義件数は限定的であり、二重保持による管理コストより、JOIN回避・定義独立性・取得処理の単純化を優先する。
- 将来マスタ統合を検討する場合も、現バージョンでは `exam_item_group_members` を則44取得定義の正とする。
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

共有型定義
      scripts/from_medical/script_lib/article44_models.py
      第1層〜第3層がimportして使用するデータ契約
```

第4層は、第3層の項目別判定関数から必要に応じて呼び出される最小部品である。
第1層から第4層までを直列に1回だけ通過し、第4層が処理の最終段として結果を返す構造ではない。

---

# 則44共有型定義

第1層〜第3層で共有する則44専用の型定義は、以下へ配置する。

```
scripts/from_medical/script_lib/article44_models.py
```

`article44_models.py` は処理層ではなく、第1層〜第3層間のデータ契約を定義する型モジュールである。
DBアクセス、値取得、正規化、判定処理、保存処理は持たない。
第4層は制度非依存の最小関数だけを置くため、則44専用型を `scripts/lib/examination/check/` へ配置しない。

`article44_models.py` へ配置する型は以下とする。

- `ExpectedValueType`
- `RequiredNamecode`
- `ValueState`
- `ValueInvalidReason`
- `PQValue`
- `CDValue`
- `STValue`
- `CheckResult`
- `ValueMap`
- `Article44Result`

`ExpectedValueType` は、第1層がnamecodeごとの期待値型を返すためのEnumであり、`PQ` / `CD` / `ST` を持つ。
CO対象は現バージョンでは `CD` として扱う。

`RequiredNamecode` は、`namecode` と `expected_value_type` を保持する。

`ValueState` は、`NOT_FOUND` / `NULL` / `EMPTY` / `PRESENT` を持つ。

`ValueInvalidReason` は、`TYPE_MISMATCH` / `PARSE_ERROR` / `FORMAT_ERROR` / `DUPLICATE_NAMECODE` を持つ。

`PQValue` / `CDValue` / `STValue` は、本資料で確定したフィールド構成を使用する。

`CheckResult` は以下の契約とする。

```python
@dataclass(frozen=True)
class CheckResult:
    status: str
    reason: str | None = None
```

`ValueMap` は以下の型エイリアスとする。

```python
ValueMap = dict[str, PQValue | CDValue | STValue]
```

`Article44Result` は以下の型エイリアスとする。

```python
Article44Result = dict[str, CheckResult]
```

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
- 第1層は、DBから対象グループ、グループに所属するnamecode、namecodeの期待値型、method、identity_codeを一括取得する。
- 第1層は、取得したDB定義を `RequiredNamecode(namecode=..., expected_value_type=...)` へ変換して第2層へ渡す。
- 第1層は、checkerの判定順、fallback、status、reasonを決定しない。

DBを正とする情報は以下とする。

- 対象となる項目グループ
- グループに所属するnamecode
- namecodeの期待値型
- method
- identity_code

想定する既存テーブルは以下とする。

- `exam_item_groups`
- `exam_item_group_members`

`exam_item_master` は既存の項目マスタとして引き続き存在する。
ただし、則44第1層の必要namecode取得では必須JOIN先としない。
`exam_item_master` に同じvalue_type、method、identity_code相当の情報が存在しても、則44用group member側への二重保持を許容する。
group member側の値は、その則44用グループで使用する取得定義として管理する。
`exam_item_master` とgroup member間の整合性確認を将来実施する可能性はあるが、今回の取得処理ではJOINして補完しない。
`exam_item_master` に値が無い、または不完全であっても、則44用group member定義だけで第1層が成立する構造とする。

則44用のルール名またはグループコードは、新規追加してよい。
ただし、本資料では具体的なグループコード文字列は確定しない。

DBは「何を取得するか」を管理し、Python checkerは「取得した値をどう判定するか」を管理する。

## 配置

```
scripts/from_medical/script_lib/article44_required_namecodes.py
```

## 入力候補

- 判定対象制度
- ルールバージョン

SQLの詳細、最終返却コンテナ型は未決とする。

第1層は同じDB定義を対象者ごとに繰り返し取得せず、処理開始時など適切な単位で一度取得して再利用する。

## 出力

```
required_namecodes: tuple[RequiredNamecode, ...]
```

`RequiredNamecode` および `ExpectedValueType` は最終名称として確定し、実体は `article44_models.py` へ配置する。
第1層の最終コンテナ型は引き続き未決だが、要素型は `RequiredNamecode` とする。

`required_namecodes` は単なる文字列一覧ではなく、namecodeごとの期待値型を含む定義一覧とする。

期待値型は、DBに実際に保存されている型ではなく、以下を表す。

- そのnamecodeを判定上どの値型として扱う予定か
- DBレコードが存在しない場合に、どのValue型で `NOT_FOUND` を生成するか
- DBの `raw_value_type` と比較する基準

型定義は `article44_models.py` で以下の契約として確定する。

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

`method` と `identity_code` はDBから一括取得する。
ただし、第2層の `ValueMap` 構築には不要なため、現時点では `RequiredNamecode` へ追加しない。

理由は以下とする。

- 第2層が必要とするのは、取得対象namecodeと期待値型だけである。
- methodは判定ルールを動的生成するために使用しない。
- identity_codeはnamecode取得やValue型生成には不要である。
- methodとidentity_codeを `ValueMap` へ伝播させない。
- 不要なメタ情報を第2層・`ValueMap` へ伝播させない。

将来、定義検証・監査・一覧出力などで必要になった場合は、第1層の補助返却情報として別途拡張を検討する。

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

## `exam_item_group_members` の責務

`exam_item_group_members` は、namecodeの集合と取得用メタ情報を保持する。
法令項目詳細No単位のルール構造は持たせず、group member定義から法令項目詳細Noごとのcheckerを動的生成しない。

現行カラムに加え、migrationにより以下のカラムを追加する方針とする。
3カラムはいずれも既存group member行への即時バックフィルを必須にしないため、NULL許容・DEFAULT NULLとする。

- value_type: `varchar(8) DEFAULT NULL`
- method: `varchar(32) DEFAULT NULL`
- identity_code: `varchar(32) DEFAULT NULL`

### namecode

- 第2層がDBから取得する健診結果値のキー。
- 第1層から第2層へ渡す必須情報。

### priority

- SQL取得順を安定させるための並び順。
- 法令項目詳細Noごとの判定順ではない。
- checkerの実行順は `article44_checker.py` の `ARTICLE44_CHECKERS` の定義順を正とする。
- DBのpriorityからchecker順やfallback順を動的生成しない。
- 則44専用group member seedでは、checker内のnamecode登場順に沿って10刻みで採番する。
- 同じ法令項目内では標準経路、代替経路、補助値の順に並べてもよい。
- ただし、判定上の正は常にPython checkerとする。

対応は以下とする。

```text
priority
    = SQL取得順を安定させるため

checker順
    = PythonのARTICLE44_CHECKERSが正
```

則44専用group member seedの採番例は以下とする。

```text
10
20
30
...
```

### value_type

- 当該namecodeに期待するXML値型。
- `PQ` / `CD` / `ST` / `CO` を想定する。
- COは現バージョンではCDとして扱う。
- 第1層で `ExpectedValueType` へ変換する。
- COは現バージョンでは `ExpectedValueType.CD` へ変換する。
- `exam_item_master.xml_value_type` とのJOINで取得せず、group member行から直接取得する。

### method

- namecodeの取得定義・分類・仕様確認・監査に使用するメタ情報。
- `article44_checker.py` の判定ルールを動的生成するためには使用しない。
- `ANY` / `ALL` / `FALLBACK` / `FINDING` 等を保持してもよいが、具体的な組み合わせ、判定順、fallback、status、reasonはPython checkerを正とする。
- `exam_item_master.xml_method_code` とのJOINを前提としない。

### identity_code

- namecodeから導出できる既存の項目識別子。
- 基本的には既存 `exam_item_master.identity_item_code` と同じ同一性項目コードを保持する補助情報。
- 処理時に毎回再計算しなくて済むようDBへ保持している。
- 法令項目詳細Noではない。
- 法令項目詳細Noの代用として使用しない。
- `identity_code` と法令項目詳細Noを同一視・転用しない。
- `exam_item_master.identity_item_code` とのJOINを前提としない。

既存 `exam_item_master.identity_item_code` は `varchar(32)` である。
固定定義上の値は現時点では5文字の英数字だが、既存定義と揃えるため `exam_item_group_members.identity_code` も `varchar(32)` とする。
一部の同一性項目コードは、namecode先頭5桁と完全一致しない既存同一性体系を表すため、単純な先頭5桁再計算だけを正としない。

> `identity_code` と法令項目詳細Noは別の識別体系である。<br>
> `exam_item_group_members.identity_code` へ法令項目詳細Noを格納したり、法令項目詳細Noとの紐付けに転用したりしない。

## 則44専用グループ定義

則44の23checkerが参照する必要namecodeだけを保持する専用グループを追加する方針とする。
既存の72項目グループ、法定健診集計用グループ、特定健診グループとは分離する。

正式なグループ定義は以下とする。

| 項目 | 値 |
|---|---|
| group_code | `v2_2026_ARTICLE44_CHECK_ITEMS` |
| group_name | `2026年版 労働安全衛生規則第44条チェック項目` |
| description | `労働安全衛生規則第44条の23項目判定で必要なnamecode取得定義。判定ルールはarticle44_checker.pyを正とする。` |

既存の `v2_2026_LSIO_Legal_Item` は法定健診判定用グループとして残し、則44の23checker専用取得定義とは混同しない。

則44専用グループのseedは以下へ作成済みである。

```
sql/seed/dev_phr/0015_dev_phr__article44_check_items_v2_2026.sql
```

seed member件数は73件である。
これはArticle44の判定項目数23件とは別であり、複数namecodeを組み合わせて1つの法令項目詳細Noを判定するためである。
`article44_checker.py` の23checkerが参照する一意namecode 73件と完全一致する。
CSV `docs/refactor/health_exam_result/労安法_一般健康診断_項目対応表.csv` の23対象項目の `require_namecodes` 一意73件とも完全一致する。

CSVの `excluded_namecodes` に記載されているLDL計算法除外対象 `3F077000002391901` は、checkerで取得対象として参照していないため、Article44専用group member seedには含めない。
除外判定のための追加取得は現バージョンでは行わず、checkerが参照するnamecodeのみを取得定義とする。

則44専用groupのmember行では、以下を必須入力とする。
DBカラム自体は既存group互換のためNULL許容だが、Article44専用seedではNULLを許容しない。

- namecode
- value_type
- method
- identity_code
- priority

### Article44専用memberのvalue_type

- `PQ` / `CD` / `ST` / `CO` のいずれかとする。
- Python変換時は `CO` を `ExpectedValueType.CD` へ変換する。
- checkerが期待するValue型、CSVの判定内容、既存 `exam_item_master.xml_value_type` を突合して確定する。

### Article44専用memberのmethod

- DB上の取得分類・監査用メタ情報とする。
- checker動的生成には使用しない。
- 実際の値一覧はseed作成時にcheckerの用途を確認して決める。
- 既存 `exam_item_master.xml_method_code` が存在する場合はそれを使用する。
- 既存 `exam_item_master.xml_method_code` がNULLの場合は、CSVの `require_methods` と処理フローからnamecodeに対応するXML method codeを確定する。
- CSVの `require_methods` と既存 `exam_item_master.xml_method_code` が異なる場合は、DBのmethodはXML method codeであるため既存master値を優先する。

### Article44専用memberのidentity_code

- namecode由来の既存識別子とする。
- 法令項目詳細Noではない。
- 原則としてcheckerで使用するnamecodeの既存identity体系に合わせる。
- 既存 `exam_item_master.identity_item_code` を優先して確定する。

## 既存group memberのバックフィル方針

今回のmigrationでは、既存group member行へ追加3カラムをバックフィルしない。
既存72項目処理は従来どおり既存テーブル、JOIN、別定義を使用する。
新しい則44専用groupのseed行だけ、3カラムを必須入力して段階的に利用開始する。

追加3カラムのNULLは「未移行・未定義」を表す。
Article44専用groupではNULLを許容しない。
`article44_required_namecodes.py` は、対象group内で3カラムの必須条件を検証する。

今回のmigrationには以下を追加しない。

- 既存group memberへのUPDATE
- `exam_item_master` からのUPDATE JOIN
- trigger
- CHECK制約
- NOT NULL化
- index追加
- Article44グループseed

## 第1層の取得処理

`article44_required_namecodes.py` の責務は以下とする。

1. 則44用のグループ定義を特定する。
2. `exam_item_group_members` から対象グループの行を一括取得する。
3. 以下を同一テーブルから取得する。
   - namecode
   - value_type
   - method
   - identity_code
   - priority
4. `exam_item_master` とはJOINしない。
5. value_typeを `ExpectedValueType` へ変換する。
6. `RequiredNamecode(namecode, expected_value_type)` を生成する。
7. 同一namecodeが複数定義されている場合は期待値型の整合性を確認して一意化する。
8. checkerの判定順、組み合わせ、fallback、status、reasonは決定しない。
9. DB定義は対象者ごとに取得せず、処理開始時など適切な単位で1回取得して再利用する。

想定SQLの概形は以下とする。
実際のSQL、DB名、プレースホルダ方式は実装時に確定する。

```sql
SELECT
    namecode,
    value_type,
    method,
    identity_code,
    priority
FROM exam_item_group_members
WHERE group_code = %s
ORDER BY priority, namecode
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
`ValueMap` は型エイリアスとして確定し、実体は `article44_models.py` へ配置する。

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

## section_code優先

`exam_item_values` には親CDA section情報を保存する。

```text
section_code
section_code_system
section_name
```

Article44のValueMap生成では、同一namecodeが複数sectionに存在する場合、労働安全衛生法健診結果セクションを表す `section_code='01030'` を優先する。

現行方針は以下とする。

```text
01030が1件
    → 01030を採用

01030が複数件
    → DUPLICATE_NAMECODE

01030が無く、他sectionが1件
    → 互換性のため採用

01030が無く、他sectionが複数件
    → DUPLICATE_NAMECODE
```

この方針により、特定健診セクションやがん検診セクションに同じnamecodeが存在しても、Article44判定では労働安全衛生法健診セクションの値を優先できる。

`section_code` がNULLまたは空の場合は、01030以外のsectionと同じfallback候補として扱う。
ただし、01030が存在する場合はNULL sectionの行を採用しない。

section情報は値の出自を識別するためのimportメタ情報であり、`PQValue` / `CDValue` / `STValue` へは伝播させない。

## interpretationCode保存

`exam_item_values` には、observation直下の `interpretationCode` を保存する。

```text
interpretation_code
interpretation_code_system
interpretation_name
```

`interpretationCode` は、健診医療機関側が付与する高値・低値・正常などの検査結果解釈コードを受け止めるための情報である。

現時点では、Article44の必須項目充足判定には使用しない。
Article44判定は、項目の存在、値型、値状態、制度上の組み合わせを主な判定材料とする。

`interpretationCode` は将来、異常値一覧、受診勧奨、PHR表示、医療機関側判定との比較などで利用する可能性がある。

`interpretationCode` はimport時に保存するXML由来メタ情報であり、現バージョンでは `PQValue` / `CDValue` / `STValue` へは伝播させない。

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
    duplicate_count=None,
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
    duplicate_count=None,
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
    duplicate_count=None,
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
    duplicate_count=None,
)
```

## 同一namecode重複時の返却契約

`ValueMap` は1つの `xml_ledger_id` 単位で構築する。

同一人物の後続版・修正版XMLは別 `xml_ledger_id`、別 `xml_sha256` として履歴保持されるため、本重複判定の対象外とする。

本重複判定の対象は、同一 `xml_ledger_id` 内に同じnamecodeが複数件存在し、かつsection優先後も採用候補を一意に決められない場合とする。

基本方針は以下とする。

- 同一namecodeが複数件存在しても、XML全体・対象者全体の処理は停止しない。
- 先勝ち、後勝ち、任意の1件採用は行わない。
- 複数値をlistとして第3層へ渡すこともしない。
- 当該namecodeは利用不能な値として扱い、関係する法令項目を `INVALID` 判定できるようにする。
- 他のnamecode、他の法令項目の判定は継続する。

第2層の重複検知責務は以下とする。

- DB一括取得結果をnamecodeごとに集約する。
- namecodeごとの取得行から `section_code='01030'` を優先する。
- 01030が1件だけ存在する場合は、その行を採用し、他sectionの同一namecodeは重複扱いしない。
- 01030が複数件存在する場合は重複として検知する。
- 01030が存在せず、他sectionの同一namecodeが1件だけ存在する場合は互換性のため採用する。
- 01030が存在せず、他sectionの同一namecodeが複数件存在する場合は重複として検知する。
- 重複の場合、その採用候補集合の件数を `duplicate_count` へ設定する。
- 重複時は期待値型に対応するValue型を返す。
- `value_state` は `PRESENT` とする。
- `is_valid` は `False` とする。
- `invalid_reason` は `ValueInvalidReason.DUPLICATE_NAMECODE` とする。
- `duplicate_count` は実際の重複件数とする。
- 変換後値は `None` とする。
- raw値、変換後値、unitは採用値を選ばないため `None` とする。
- 重複した複数値のうち、どれか1件を採用しない。
- 当該namecode以外の `ValueMap` 構築は継続する。

重複時にraw値をどれか1件だけ保持すると、その値を採用したように見えるため、現バージョンではrawフィールドも `None` とする。
重複時はDB上に値レコードが存在するため、`value_state=PRESENT` とする。
ただし、複数値のうちどれを採用するか一意に決められないため、raw値および変換後値は `None` とする。
`PRESENT` は採用可能な値が1件あることを意味せず、DB上にNULL・空文字ではない値レコードが存在することを表す。
重複時の利用不能理由は `invalid_reason=ValueInvalidReason.DUPLICATE_NAMECODE`、件数は `duplicate_count` で表す。

PQの重複時返却例は以下とする。

```python
PQValue(
    value_state=ValueState.PRESENT,
    raw_value=None,
    numeric_value=None,
    unit=None,
    is_valid=False,
    invalid_reason=ValueInvalidReason.DUPLICATE_NAMECODE,
    duplicate_count=2,
)
```

CDまたはCOの重複時返却例は以下とする。

```python
CDValue(
    value_state=ValueState.PRESENT,
    raw_value=None,
    code_value=None,
    is_valid=False,
    invalid_reason=ValueInvalidReason.DUPLICATE_NAMECODE,
    duplicate_count=2,
)
```

STの重複時返却例は以下とする。

```python
STValue(
    value_state=ValueState.PRESENT,
    raw_text=None,
    text=None,
    is_valid=False,
    invalid_reason=ValueInvalidReason.DUPLICATE_NAMECODE,
    duplicate_count=2,
)
```

重複件数は、第3層が最終reasonへ含められるようにする必要がある。

重複件数の扱いは以下とする。

- 最終的な `CheckResult.reason` には重複namecodeであることを必ず残す。
- 第2層で `duplicate_count` を保持するため、重複件数をreasonへ含められる。
- 想定reason形式は現行方式に従い、文字列コードまたは `CODE:detail` 形式とする。
- 例: `DUPLICATE_NAMECODE`
- 例: `DUPLICATE_NAMECODE:count=2`

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
- `duplicate_count` は、同一 `xml_ledger_id` 内で同一namecodeが複数件存在した場合の件数を表す。
- 値が存在していても型変換や形式検証に失敗する場合があるため、両者は別責務とする。
- 型不正の場合は `value_state=PRESENT` のまま、`is_valid=False` とする。
- `NOT_FOUND / NULL / EMPTY` はすべて `is_valid=False` とする。
- `NOT_FOUND / NULL / EMPTY`、および `PRESENT` かつ `is_valid=True` の場合は `invalid_reason=None` とする。
- `NOT_FOUND / NULL / EMPTY` は `value_state` だけで状態を特定できるため、`invalid_reason` へ重複保持しない。
- `duplicate_count` は重複時のみ2以上の整数を保持し、重複でない場合は `None` とする。
- `duplicate_count=1` は使用しない。
- `duplicate_count` は、値の存在状態や利用可否そのものを表すフィールドではない。
- `duplicate_count` は、`value_state`、`is_valid`、`invalid_reason` と組み合わせて使用する。

`duplicate_count` の整合性ルールは以下とする。

```text
invalid_reason == ValueInvalidReason.DUPLICATE_NAMECODE
    → duplicate_count is not None
    → duplicate_count >= 2

invalid_reason != ValueInvalidReason.DUPLICATE_NAMECODE
    → duplicate_count is None
```

以下の場合、`duplicate_count=None` とする。

- `NOT_FOUND`
- `NULL`
- `EMPTY`
- `PRESENT` かつ `is_valid=True`
- `TYPE_MISMATCH`
- `PARSE_ERROR`
- `FORMAT_ERROR`

## ValueInvalidReason

値の存在状態とは別に、`PRESENT` だが型として利用できない理由を表す共通Enumを定義する。
`ValueInvalidReason` は最終Enum名として確定し、実体は `article44_models.py` へ配置する。

現時点では以下の4種類のみ定義し、理由を過度に細分化しない。

```python
from enum import Enum


class ValueInvalidReason(str, Enum):
    TYPE_MISMATCH = "TYPE_MISMATCH"
    PARSE_ERROR = "PARSE_ERROR"
    FORMAT_ERROR = "FORMAT_ERROR"
    DUPLICATE_NAMECODE = "DUPLICATE_NAMECODE"
```

各値の意味は以下とする。

|理由|意味|
|---|---|
|TYPE_MISMATCH|required定義の期待値型とDBのraw_value_typeが一致しない|
|PARSE_ERROR|期待値型とDB型は一致しているが、値を必要な型へ変換できない。現時点では主にPQのDecimal変換失敗で使用する|
|FORMAT_ERROR|期待値型とDB型は一致しているが、値がその型の最低限の形式要件を満たさない。現時点では主にCDまたはCOの形式不正で使用する|
|DUPLICATE_NAMECODE|同一xml_ledger_id内で同一namecodeが複数件存在する。どの値を採用すべきか一意に決められないため、当該namecodeを判定へ利用しない|

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
    duplicate_count: int | None
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
    duplicate_count=None,
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
    duplicate_count=None,
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
    duplicate_count=None,
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
    duplicate_count=None,
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
    duplicate_count: int | None
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
    duplicate_count=None,
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
    duplicate_count=None,
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
    duplicate_count=None,
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
    duplicate_count=None,
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
    duplicate_count=None,
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
    duplicate_count: int | None
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
    duplicate_count=None,
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
    duplicate_count=None,
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
    duplicate_count=None,
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
    duplicate_count=None,
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
- 法令項目詳細Noごとの判定対象namecodeの組み合わせ、判定順、ANY / ALLの具体的な組み合わせ、fallback、優先順位、CDとSTの整合性、検索語、数値条件、status、reason、23checkerのオーケストレーションは `article44_checker.py` を正とする。
- DBのmethodやgroup member行を使って、checkerの判定ルールを動的に再構築しない。

法令項目詳細Noは、23checker、Article44Result、横持ちresultカラムを追跡するための正式な識別子である。
法令項目詳細Noと判定ロジックの対応は `article44_checker.py` が持つ。
`exam_item_group_members` に法令項目詳細No単位のルール構造を持たせない。
DBのgroup member定義から、法令項目詳細Noごとのcheckerを動的生成しない。
法令項目詳細NoをDBマスタへ保持する場合は、項目マスタ側の属性として扱う方向とし、group memberの役割とは分離する。
`exam_item_master` に法令項目詳細Noを保持する可能性はあるが、カラム名やDDLは本資料では確定しない。
今回の第1層・第2層の実装に法令項目詳細Noは必須ではない。

同一namecode重複時の判定方針は以下とする。

- `ValueInvalidReason.DUPLICATE_NAMECODE` を検知した場合、関係する法令項目の `status` は `INVALID` とする。
- `reason` には重複namecodeであることを示す文字列を設定する。
- `duplicate_count` を参照して現行reason方式のdetailを生成する。
- 重複件数が取得できるため、重複時reasonは原則として `DUPLICATE_NAMECODE:count=<duplicate_count>` とする。
- `duplicate_count` が想定外に `None` の場合は、実装不整合として扱う。
- ただし、全体停止の例外とするか、detailなしの `DUPLICATE_NAMECODE` へ退避するかは実装時に防御的に決めてよい。
- 通常契約としては、`DUPLICATE_NAMECODE` と `duplicate_count` は必ず対で存在する。
- 重複したnamecodeを利用するすべての法令項目を `INVALID` 対象とする。
- 他の法令項目の判定は継続する。

想定例は以下とする。

```python
CheckResult(
    status="INVALID",
    reason="DUPLICATE_NAMECODE:count=2",
)
```

reasonの最終文字列形式は現行reason方式を踏襲し、新しいreason体系やEnumは追加しない。

上位層との関係は以下とする。

- 第2層は重複を検知しても例外で全体停止しない。
- 第3層は該当法令項目を `INVALID` として `Article44Result` へ含める。
- `Article44Result` は他の正常項目も含めて返す。
- 後続の制度別集約、`xml_ledger.check_status`、XML単位・ZIP単位の集約処理は、`Article44Result` の `INVALID` を材料として `WARNING` または `NG` 等を決定する。
- 上位層の最終集約ルール自体は今回変更しない。

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

実装では `ARTICLE44_CHECKERS` のdict挿入順により、現バージョンの23項目順を保証する。

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

`CheckResult` は最終クラス名として確定し、実体は `article44_models.py` へ配置する。
契約は以下とする。

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class CheckResult:
    status: str
    reason: str | None = None
```

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

32に記載された `WARNING` は、項目別 `CheckResult.status` の値としてそのまま使用しない。
32の `WARNING` 表現は、値の取得方法や成立経路が標準経路ではない、または確認を要する状態を示す仕様上の表現として読む。
第3層checkerでは、条件の意味に応じて現行の項目別statusへ変換する。

- 代替経路で項目が成立した場合は原則 `ALTERNATIVE`。
- 必要値が不足して成立しない場合は `MISSING`。
- 値は存在するが形式不正、型不一致、重複、条件矛盾等で利用できない場合は `INVALID`。
- `WARNING` / `NG` への集約は判定基盤の外側で行う。

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

この一人分またはXML1件分の集合を `Article44Result` と呼ぶ。

現行72項目方式では、一人分またはXML1件分の判定結果を `dict[str, ItemResult]` で保持している。
今回も同じ考え方を踏襲し、`Article44Result` は `dict[str, CheckResult]` の型エイリアスとして確定する。
実体は `article44_models.py` へ配置する。

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
└── finding.py
```

`priority.py` は現時点では実装しない。
優先順位処理は第3層checkerへ明示的に実装する。
同じ入力・同じ意味・同じ返却の優先順位処理が複数項目で確認された場合のみ、後から共通化を検討する。

## 設計方針

最初から巨大なRuleEngineは作らない。

32の項目仕様を確認した結果、複数項目で同じ意味・同じ入力・同じ返却となるものだけを共通化する。

以下のような複合パターン全体は第4層へ置かない。

- `ANY + FINDING(OR)`
- `ALL + FALLBACK`
- 条件付きPRIORITY
- 制度固有のCONDITIONAL

第4層には最小関数だけを置き、第3層の項目別関数で順番に呼び出して組み立てる。

現時点の実装済み関数は以下とする。

### `any.py`

- `has_any_valid(values)`
- `is_valid=True` が1件以上あれば `True`。
- 空Iterableは `False`。
- `ValueMap` や制度知識を持たない。

### `all.py`

- `has_all_valid(values)`
- すべて `is_valid=True` なら `True`。
- 空IterableはPython標準 `all()` と同じく `True`。
- `ValueMap` や制度知識を持たない。

### `compare.py`

- 有効な `numeric_value` を `Decimal` で比較する。
- `is_valid=False` または `numeric_value=None` は `False`。
- 実装済みpublic関数は以下とする。
  - `is_equal`
  - `is_greater_than`
  - `is_greater_than_or_equal`
  - `is_less_than`
  - `is_less_than_or_equal`
  - `is_between`
- コード比較は行わない。

### `finding.py`

- 有効な正規化済み `text` の存在確認・検索語の部分一致を行う。
- 実装済みpublic関数は以下とする。
  - `has_text`
  - `contains_any_keyword`
- `raw_text` は検索しない。
- CDとSTの組み合わせ、最終 `status`・`reason` は第3層で決める。

### `priority.py`

- 現時点では実装しない。
- 優先順位処理は第3層checkerへ明示的に実装する。
- 同じ入力・同じ意味・同じ返却の優先順位処理が複数項目で確認された場合のみ、後から共通化を検討する。

---

# 複合パターンの組み立て場所

複合パターンの最終的な `status`・`reason` は、第3層の法令項目詳細Noごとの判定関数で決定する。

## 胸部X線の例

```
1. 正規の胸部X線検査結果が有効か確認する。
2. 正規の検査結果が有効なら OK。
3. 正規の検査結果が成立しない場合のみ、32で定義された所見パターンを確認する。
4. 所見パターンのいずれかで項目成立と判断できる場合は ALTERNATIVE。
5. 正規結果も所見パターンも存在しない場合は MISSING。
6. 値は存在するが、型不一致、形式不正、重複、所見有無と詳細の矛盾等で利用できない場合は INVALID。
7. WARNING は項目別statusとして返さない。
```

胸部X線の基本対応は以下とする。

```text
正規検査結果で成立
    → OK

正規検査結果なし + 所見パターンで成立
    → ALTERNATIVE

成立材料なし
    → MISSING

値は存在するが利用不能・矛盾
    → INVALID
```

複数の所見パターンがある場合のOR条件、判定順、参照namecodeは32の既存仕様に従う。

## 聴力の例

```
1. 1000Hz右、1000Hz左、4000Hz右、4000Hz左の4項目を確認する。
2. 4項目すべてが有効なら OK。
3. 4項目すべてが成立しない場合のみ、会話法を確認する。
4. 会話法が有効なら ALTERNATIVE。
5. 4項目も会話法も成立しない場合は MISSING。
6. 参照値に TYPE_MISMATCH / PARSE_ERROR / FORMAT_ERROR / DUPLICATE_NAMECODE 等の利用不能理由がある場合は、該当条件に応じて INVALID。
7. WARNING は項目別statusとして返さない。
```

聴力の基本対応は以下とする。

```text
4項目すべて有効
    → OK

4項目不足 + 会話法有効
    → ALTERNATIVE

4項目不足 + 会話法も不成立
    → MISSING

値は存在するが利用不能・重複・型不一致
    → INVALID
```

会話法が存在する場合でも、4項目すべてが有効なら標準経路を優先して `OK` とする。

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

ただし、現行実装では接続側として `03_check_exam_results.py` が `Article44Result` を受け取り、`exam_check_results` の `a44_<法令項目詳細No>_status` / `a44_<法令項目詳細No>_reason` 46列へ展開して保存する。
また、`legal_check_result` / `legal_reason_summary` を生成し、`xml_ledger.check_status` / `xml_ledger.check_reason` へ反映する。

`legal_reason_summary` と `xml_ledger.check_reason` は検索性のため同じ文字列を二重保持する。
形式は以下とする。

```text
<法令項目詳細No>:<日本語項目名>:<reason>
```

例:

```text
4403003001:腹囲:MISSING | 4411001001:心電図:DUPLICATE_NAMECODE:count=2
```

全23項目がOK相当の場合は、`legal_reason_summary` と `xml_ledger.check_reason` はNULLとする。

---

# 層間責務

| 層 | 責務 | 配置 |
|---|---|---|
| 第1層 | 何を取得するか決める | `article44_required_namecodes.py` |
| 第2層 | 対象者・XMLの値と状態を一括取得してdictで返す | `article44_value_loader.py` |
| 第3層 | 則44の法令項目詳細Noごとの判定を組み立て、全項目を一巡させて一人分の横並び結果を返す | `article44_checker.py` |
| 第4層 | 制度非依存の最小判定部品を提供する | `scripts/lib/examination/check/` |

## 責務分離の要約

DBは「何を取得するか」を管理し、Python checkerは「取得した値をどう判定するか」を管理する。

| 管理対象 | 正とする場所 |
|---|---|
| 対象グループ | DB `exam_item_groups` |
| 必要namecode群 | DB `exam_item_group_members` |
| namecodeの期待値型 | DB `exam_item_group_members.value_type` |
| method | DB `exam_item_group_members.method` |
| identity_code | DB `exam_item_group_members.identity_code` |
| 法令項目詳細Noごとの判定ルール | `article44_checker.py` |
| namecodeの組み合わせ・順序・fallback | `article44_checker.py` |
| status / reason | `article44_checker.py` |
| ValueMap構築 | `article44_value_loader.py` |

---

# 実装責任

## 今回こちらで詳細設計・実装する範囲

- 第2層のValueMap返却契約
- 第1層〜第3層で共有する則44専用型の `article44_models.py` への配置
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

現行実装では、上記接続範囲のうち以下は実装済みである。

- 本体スクリプトから4層を一巡させる呼び出し配線。
- `Article44Result` から横持ちカラムへの変換。
- `exam_check_results` へのa44 46列保存。
- Article44法定総合判定。
- `xml_ledger.check_status` / `xml_ledger.check_reason` への反映。

特定健診総合判定は現行Article44ルートでは未接続とし、別フェーズで扱う。

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

34_article44_implementation_status.md
33以降に実装で確定した現在地整理
```

---

# 保留事項

- 同一namecodeに異なるvalue_typeが定義された場合の扱い
- 追加3カラムの将来的なNOT NULL化・CHECK制約追加の要否
- 必要なindexの有無
- `exam_item_master` との将来的な整合性検証方法
- Article44Resultに便利メソッドを持たせるか
- 業務歴を将来Article44Resultへ追加するか
- 喀痰を将来Article44Resultへ追加するか
- 任意項目を横持ちカラムとして物理作成するか
- section_code優先方針の実DB再import後の確認
- interpretationCodeを将来どの判定・表示へ利用するか

---

# 次回検討

1. section情報とinterpretationCodeを含めて実DBを再importする。
2. `03_check_exam_results.py` を再実行し、NG理由を再集計する。
3. `DUPLICATE_NAMECODE` がsection優先により減少するか確認する。
4. 特定健診は別フェーズとして設計範囲を再整理する。
