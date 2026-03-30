# identity layers, norm, and purpose

## 1. Purpose

本specは、identity 系処理におけるレイヤー構造、`norm` の分割方針、`FieldPurpose` の定義、および各主要項目の正規化責務を固定するための詳細specである。

本specの目的は、以下を明確にすることにある。

- identity 系処理をレイヤーごとに責務分離する
- `raw` 以降の値生成を `base_norm` / `field_norm` / purpose 別値として整理する
- `person_id_custom` / `identity_hash` の材料生成をブレなくする
- 欠損や invalid を「例外」ではなく「構造化された結果」として扱う

本specは、親spec `v1.1.0_identity_layer_commonization.md` を受けた設計詳細として扱う。

## 2. Scope

本specの対象は以下とする。

- identity 系のレイヤー構造
- `FieldPurpose` enum の定義
- `base_norm` / `field_norm` / `match` の責務分離
- 主要 identity 項目の field 関数責務
- `PrimitiveStepResult` / `FieldResult` / `BuilderResult` の戻り値構造
- `person_id_custom` / `identity_hash` の材料項目に関わる詳細設計

本specの対象外は以下とする。

- XML/CDA 出力仕様の詳細
- 業務判定ロジックそのもの
- DB 全面再設計
- 実装ファイル配置の最終確定

## 3. Layer Structure

本設計では、処理を責務ごとにレイヤー分割し、「作る」と「選ぶ」を明確に分離する。

- 下位レイヤー: 値を作る（純粋処理、再利用可能、ブレない）
- 上位レイヤー: 何を使うか選ぶ（業務ルール、健保差分、将来拡張）

---

### 3.1 Layer numbering policy

- 数字が小さいほど「低レイヤー（プリミティブ）」
- 数字が大きいほど「高レイヤー（業務ロジック寄り）」
- 下位は上位を知らない（依存は一方向）
- 上位は下位を組み合わせて利用する

依存関係:

```
Layer4 → Layer3 → Layer2 → Layer1
```

---

### 3.2 Layer 1: primitive layer

最小単位の文字列処理・正規化関数群。  
意味を持たない純粋処理のみを担当する。

例:
- NFKC 正規化
- trim（前後空白除去）
- 全角/半角空白の吸収
- 制御文字除去
- 中黒・ハイフン類の統一準備
- 数字抽出（digit）
- 小書き文字の正規化

特徴:
- 入力 → 出力の純粋関数
- 業務知識を持たない
- 再利用性最大

---

### 3.3 Layer 2: field layer

項目単位の処理を担当。  
「どの purpose で値を作るか」に応じて処理を分岐する。

入力:
- raw 値
- FieldPurpose

出力:
- FieldResult（構造化）

内部構造:
- base_norm（共通正規化）
- field_norm（項目固有ロジック）
- purpose 別値生成（match / person_id_custom / export など）

例:
- insurer_number
- insurance_symbol
- insurance_number
- birthdate
- name_kana
- gender_code

特徴:
- 「意味」を持つ最初のレイヤー
- 項目ごとの差分をここに閉じ込める

---

### 3.4 Layer 3: builder layer

複数の FieldResult を組み合わせて「完成品」を作るレイヤー。

対象:
- match セット
- person_id_custom
- identity_hash

例:
- build_match_fields
- build_person_id_custom
- build_identity_hash
- build_identity_bundle

責務:
- 必須項目の存在チェック
- 項目の組み立て
- 欠損時の NG 判定

重要ポリシー:
- 不足している場合は「作らない」
- 代替生成は禁止

---

### 3.5 Layer 4: insurer / profile rule layer

健保・データソースごとのルールを定義するレイヤー。

責務:
- どの項目を使うか
- 欠損時の扱い（スキップ / NG / 別ルート）
- 特殊ケース対応（例: 記号なし健保）

特徴:
- 業務依存
- 将来拡張前提
- 下位レイヤーのロジックは変更しない

例:
- 協会けんぽプロファイル
- 郵政プロファイル

原則:
- 下位は不変
- 上位で吸収

## 4. FieldPurpose

FieldPurpose は、field layer において「その項目に対して何用の値を作るか」を指定するための enum とする。

FieldPurpose 自体は本体DBの業務データとして保持するものではなく、設計上の定義語彙・実装上の契約・処理分岐の固定札として扱う。

本体データ側では、各列名や完成品名によって用途を表現する。  
一方で、設計・実装・監査観点では purpose の定義を明示的に固定する必要があるため、本specでは FieldPurpose を定義メタとして扱う。

---

### 4.1 FieldPurpose definitions

#### Enum definition

```python
from enum import Enum

class FieldPurpose(str, Enum):
    NORM = "norm"
    MATCH = "match"
    PERSON_ID_CUSTOM = "person_id_custom"
    IDENTITY_HASH = "identity_hash"
    EXPORT = "export"
```

#### Definition table

| enum name | value | meaning | main layer | persisted as business data | notes |
|-----------|-------|---------|------------|----------------------------|-------|
| `NORM` | `norm` | raw から後続処理の土台となる正規化値を得る用途 | Layer 2 | No | `base_norm` と `field_norm` を経由する |
| `MATCH` | `match` | 照合・比較・JOIN・検索用の値を得る用途 | Layer 2 / Layer 3 | No | 生成された match 値自体はDB保持対象になりうる |
| `PERSON_ID_CUSTOM` | `person_id_custom` | `person_id_custom` 生成用の値を得る用途 | Layer 2 / Layer 3 | No | 完成品 `person_id_custom` はDB保持対象 |
| `IDENTITY_HASH` | `identity_hash` | `identity_hash` 生成に必要な材料を得る用途 | Layer 2 / Layer 3 | No | 完成品 `identity_hash` はDB保持対象 |
| `EXPORT` | `export` | XML / CSV 等の出力用値を得る用途 | Layer 2 | No | 出力表現は用途に応じて別ルールを取りうる |

#### Design notes

- FieldPurpose は「作る対象」ではなく「作る用途」を表す
- `FieldPurpose` の値そのものを本体DBの列として保存することは前提としない
- ただし、将来的に audit / diagnostic 用テーブルを持つ場合は、処理記録として purpose を保持してよい
- 文字列 typo や表記揺れを避けるため、purpose は enum で固定する

---

### 4.2 Purpose selection policy

各項目関数は、`raw` を起点として、必ず `FieldPurpose` を受け取って処理を行う。

基本フローは以下の通りとする。

`raw` → `base_norm` → `field_norm` → purpose別値

すなわち、`match`、`person_id_custom` 用値、`identity_hash` 用材料、`export` 用値は、原則として `field_norm` を起点に生成する。

#### Selection principles

- 同じ項目でも、purpose が異なれば返す値は異なってよい
- ただし、共通の起点は `field_norm` とする
- `raw` から直接 `match` や `person_id_custom` 用値を作ることは原則避ける
- 例外的な個別処理が必要な場合も、まずは `base_norm` / `field_norm` を通す設計を優先する

#### Usage examples

- `insurance_symbol_field(raw_value, purpose=FieldPurpose.NORM)`
- `insurance_symbol_field(raw_value, purpose=FieldPurpose.MATCH)`
- `insurance_symbol_field(raw_value, purpose=FieldPurpose.PERSON_ID_CUSTOM)`
- `name_kana_field(raw_value, purpose=FieldPurpose.MATCH)`
- `gender_code_field(raw_value, purpose=FieldPurpose.IDENTITY_HASH)`

#### Boundary notes

- Layer 2 は purpose に応じた値を返す
- Layer 3 は Layer 2 の戻り値を組み合わせて完成品を作る
- Layer 4 はどの項目を、どのルールで処理するかを選ぶが、purpose 定義そのものは Layer 2 / Layer 3 の共通契約として固定する

## 5. Normalization Design

### 5.1 raw

`raw` は受領値そのものとする。  
加工や推測を行わず、入力由来の元値として保持する。

原則:
- `raw` は意味変換しない
- 空文字や空白のみの値も、まずは入力として受け取る
- `base_norm` 以降で構造化された正規化を行う

### 5.2 base_norm

`base_norm` は、全項目共通の下ごしらえ正規化とする。  
ここではまだ項目の意味に立ち入らず、「raw を後続処理の土台として扱える状態」に揃えることだけを目的とする。

すなわち、`base_norm` は以下の性質を持つ。

- 全項目共通である
- 業務知識を持たない
- 項目判定をしない
- `field_norm` の前段として必ず通る

`base_norm` の結果は、各項目関数における最初の構造化生成値とする。

#### Responsibility

`base_norm` の責務は、以下のような「意味を持たないノイズ除去・表現揃え」に限定する。

- Unicode 正規化（NFKC）
- 前後空白除去（trim）
- 半角/全角空白の吸収
- 制御文字除去
- 改行・タブ等の整理
- 空値判定の統一

ここでは、以下のような項目依存の判断は行わない。

- 数字項目だから先頭0を削除する
- 氏名カナだから中黒を削除する
- 生年月日だから YYYYMMDD に整える
- 記号だからハイフンを除去する

これらはすべて `field_norm` 以降の責務とする。

#### Processing order

`base_norm` は、原則として以下の順に処理する。

1. `None` 判定
2. 文字列化（必要な場合のみ）
3. Unicode 正規化（NFKC）
4. 制御文字除去
5. 空白類の整理
6. 前後空白除去（trim）
7. 空値判定

#### Input policy

- 入力は原則として `raw` 値とする
- `raw` が `None` の場合は、そのまま空値として扱う
- 文字列以外の型が入力される可能性がある場合は、呼び出し側または `base_norm` 冒頭で安全に文字列化する
- `base_norm` は「壊れた値を救済する」のではなく、「後続で扱える最低限の表現へ揃える」ことを目的とする

#### Output policy

- 出力は `str | None` を基本とする
- 空値と判定した場合は `None` を返す
- 空文字 `""` は `base_norm` 完了時点では残さない
- ここではまだ項目依存の変換は行わないため、値の意味は変えない

#### Empty handling policy

以下は `base_norm` 完了時点で空値として扱う。

- `None`
- `""`
- trim 後に空となる値
- 空白・タブ・改行のみで構成される値
- 制御文字除去と空白整理の結果、実質空となる値

この時点で空と判定された場合、後続の `field_norm` / `match` / `person_id_custom` / `identity_hash` 用材料生成では、原則として `None` 起点で処理する。

#### Primitive composition examples

`base_norm` は Layer 1 の primitive 関数を組み合わせて構成する。

例:
- `to_nfkc`
- `remove_control_chars`
- `normalize_spaces`
- `trim`
- `empty_to_none`

#### Non-goals

`base_norm` では以下を行わない。

- 数字抽出
- 先頭0除去
- 中黒除去
- ハイフン除去
- 小書きカナの統一
- カタカナ化
- 日付フォーマット化
- 項目別の比較ルール適用

これらは項目意味を伴うため、`field_norm` またはそれ以降の責務とする。

#### Result expectations

`base_norm` の結果は、少なくとも以下を満たすことを期待する。

- 文字コード/表記揺れの基本ノイズが抑えられている
- 空値が `None` に統一されている
- `field_norm` 側が項目意味だけに集中できる

#### Design note

`base_norm` は「共通の土台」を作る層であり、「正解の値」を作る層ではない。  
正解に近づける処理は `field_norm` で行う。

### 5.3 field_norm

`field_norm` は、各項目の意味に応じた基本正規化値とする。  
同じ `norm` 層に属していても、適用する具体的ルールは項目ごとに異なってよい。

原則:
- `field_norm` は `base_norm` を起点に作る
- `field_norm` は項目意味に応じた最初の安定表現とする
- `match` / `person_id_custom` / `identity_hash_input` / `export` は原則として `field_norm` を起点に生成する

### 5.4 match

`match` は、照合・比較・JOIN・検索用の値とする。  
`field_norm` が「項目としての基本形」であるのに対し、`match` は「照合専用へ寄せた最終比較形」である。

原則:
- `match` は原則として `field_norm` を起点に生成する
- `match` は照合に不要な表記差を吸収してよい
- `match` は項目によって `field_norm` と同一になってもよい

### 5.5 person_id_custom canonical values

`person_id_custom` 用 canonical 値は、原則として各項目の `match` 値を流用する。  
ただし、項目ごとに `match` とは別定義が必要になった場合は将来分離可能とする。

v1.1.0 での主材料:
- `birthdate_match`
- `insurance_number_match`
- `insurer_number_match`
- `insurance_symbol_match`

### 5.6 identity_hash input values

`identity_hash` の必須材料は以下の3つで固定する。

- `person_id_custom`
- `name_kana_full_match`
- `gender_code_match`

必須材料に `null` / 空値を含めて生成してはならない。  
1つでも欠ける場合、`identity_hash` は未生成とし、不足項目を明示して返す。

### 5.7 export values

`export` は出力先仕様に応じて別ルールを取りうる。  
内部保持の正規値とは切り離し、必要な場面で再フォーマットする。

## 6. Result Structure

本設計では、各レイヤーの戻り値を構造化し、「値・状態・理由」を一体として扱う。

これにより、
- デバッグ容易性
- 欠損追跡
- 監査対応
- builder での判定簡略化

を実現する。

---

### 6.1 PrimitiveStepResult

Layer 1（primitive layer）の戻り値。  
最小単位の処理結果を表す。

```python
class PrimitiveStepResult:
    ok: bool
    value: str | None
    step_name: str
    note: str | None
```

#### 意味

- `ok`: 処理が成立したか
- `value`: 処理後の値
- `step_name`: 実行した処理名（例: nfkc, trim）
- `note`: 補足（例: 制御文字除去あり）

#### ポリシー

- 可能な限り pure function
- 原則として例外は投げず、`ok` で表現

---

### 6.2 FieldResult

Layer 2（field layer）の戻り値。  
項目単位での全処理結果をまとめる。

```python
class FieldResult:
    field_name: str
    raw: str | None
    base_norm: str | None
    field_norm: str | None

    match: str | None
    person_id_custom: str | None
    identity_hash_input: str | None
    export: str | None

    ok: bool
    missing: bool
    reason: str | None
```

#### 意味

- `field_name`: 項目名（例: insurance_symbol）
- `raw`: 入力値
- `base_norm`: 共通正規化後
- `field_norm`: 項目固有正規化後

- `match`: 照合用値
- `person_id_custom`: person_id 用値
- `identity_hash_input`: hash 用材料
- `export`: 出力用値

- `ok`: この項目が有効に処理できたか
- `missing`: 元データが欠損しているか
- `reason`: NG理由 / 補足

#### ポリシー

- raw 以外はすべて生成値
- 欠損時は `missing=True` とし、`reason` を必ず設定
- `ok=False` の場合でも構造は必ず返す

---

### 6.3 BuilderResult

Layer 3（builder layer）の戻り値。  
複数項目を組み合わせた完成品の結果。

```python
class BuilderResult:
    name: str
    value: str | None

    ok: bool
    missing_fields: list[str]
    upstream_missing_fields: list[str]
    reason: str | None
```

#### 意味

- `name`: 生成対象名（例: identity_hash）
- `value`: 完成値

- `ok`: 生成成功可否
- `missing_fields`: 必須不足項目
- `upstream_missing_fields`: 元データ欠損項目
- `reason`: NG理由

#### ポリシー

- 必須項目が1つでも欠けたら `ok=False`
- 無理に生成しない（null埋め禁止）
- missing 情報は必ず上位に伝搬する

---

### 設計原則まとめ

- 「値だけ返す」は禁止
- 常に「値 + 状態 + 理由」を返す
- builder は FieldResult を見て判断するだけにする
- 例外ではなくデータとして状態を扱う

## 7. Field Function Responsibilities

### 7.1 insurer_number

#### field_norm

`insurer_number_field_norm` は、保険者番号として後続処理に利用できる基本正規化値とする。

少なくとも以下を行う。

- 数字以外の文字を除去する
- 半角数字へ寄せる
- 先頭0を削除する（ただし全て0の場合は `0` とする）

例:
- `06139463` → `6139463`
- `00000000` → `0`
- `06-139463` → `6139463`
- `０６１３９４６３` → `6139463`

#### field_norm policy

`insurer_number` は値（数値）として扱う。  
そのため、`insurance_symbol` と異なり、`field_norm` の段階で先頭0を除去してよい。

また、`insurer_number` では文字や区切り記号は識別要素とみなさず、保険者番号本体となる数字のみを残す。

#### match

`insurer_number_match` は、照合・比較・JOIN・検索用の値とする。

v1.1.0 では、`insurer_number_match` は `field_norm` と同一とする。

#### match policy

- `match == field_norm` とする
- 数字以外は保持しない（`field_norm` で除去済み）
- 先頭0は保持しない（`field_norm` で除去済み）
- 結果は常に半角数字のみとする

#### examples

- `06139463` → `6139463`
- `00000000` → `0`
- `06-139463` → `6139463`
- `０６１３９４６３` → `6139463`

#### empty policy

以下の場合、`insurer_number_match` は `None` とする。

- `field_norm` が `None`
- 数字抽出の結果、何も残らない

#### person_id_custom

`insurer_number` は `person_id_custom` の必須材料である。  
Layer 2 では、`insurer_number` に対する `FieldPurpose.PERSON_ID_CUSTOM` の値は、原則として `insurer_number_match` を流用してよい。

すなわち、v1.1.0 では以下を原則とする。

- `person_id_custom == insurer_number_match`

必要になれば将来版で分離可能とするが、現時点では二重定義しない。

#### identity_hash_input

`insurer_number` は `identity_hash` の直接材料ではない。  
`identity_hash` では `person_id_custom` を経由して利用されるため、Layer 2 で `identity_hash_input` 用の専用値は持たない。

#### export

`export` は出力先仕様に応じて別ルールを取りうる。  
v1.1.0 では詳細ルールは固定しないが、少なくとも以下を考慮できるようにしておく。

- 出力先が先頭0付き保険者番号を要求するか
- 固定桁での0埋めが必要か
- 表示用と内部照合用の表現を分ける必要があるか

#### missing policy

以下の場合、`insurer_number` は missing と扱う。

- `raw` が `None`
- `base_norm` 完了時点で空値
- 数字抽出の結果、何も残らない

missing の場合は、少なくとも以下を満たす。

- `ok=False`
- `missing=True`
- `reason` を設定する

#### note

`insurer_number` は `insurance_symbol` と異なり、構造ではなく数値そのものを主とする項目である。  
そのため、`field_norm` と `match` は実質的に同じ値になってよい。

### 7.2 insurance_symbol

#### field_norm

`insurance_symbol_field_norm` は、記号として後続処理に利用できる基本正規化値とする。

少なくとも以下を行う。

- 中黒を除去する
- 括弧（()（））を除去する
- スラッシュ（/／）を除去する
- シャープ（#＃）を除去する
- アスタリスク（*＊）を除去する
- プラス（+＋）を除去する
- 記号（※）を除去する
- ハイフン類 / ダッシュ類 / 長音符類を `-` に統一する（仮統一）
- 先頭0は削除しない

その後、除去対象を取り除いた後の残存文字列に対して文字種判定を行う。

- 英数字とハイフンのみで構成される場合は半角へ寄せる
- 上記以外の文字を1文字でも含む場合は全体を全角へ寄せる

半角判定は、「英数字と `-` のみで構成されるか」を基準とする。

最終的に全角ルートでは、ハイフンは `－` に統一する。

例:
- `123ー01` → `123-01`
- `神-01` → `神－０１`
- `A-01(仮)` → `Ａ－０１仮`

#### match

`insurance_symbol_match` は、照合・比較・JOIN・検索用の値とする。  
記号としての見た目は保持せず、照合に不要な区切り・装飾・0差を極力吸収した最小表現を生成する。

v1.1.0 では、`insurance_symbol_match` は原則として `field_norm` を起点に生成し、少なくとも以下を行う。

- 空白を除去する
- 中黒を除去する
- 括弧（()（））を除去する
- スラッシュ（/／）を除去する
- シャープ（#＃）を除去する
- アスタリスク（*＊）を除去する
- プラス（+＋）を除去する
- 記号（※）を除去する
- ハイフン類 / ダッシュ類 / 長音符類を除去する
- 英数字は半角へ寄せる
- 連続する数字ブロックごとに先頭0を削除する
- 非数字文字は保持する

#### match policy

`insurance_symbol_match` の目的は、同一性照合に不要な表記差をできる限り吸収することにある。  
そのため、`field_norm` では保持していた区切り記号や装飾記号も、`match` では除去対象としてよい。

特に `insurance_symbol_match` では、以下を原則とする。

- ハイフン類は統一ではなく除去する
- 先頭0は全体一括ではなく、数字ブロック単位で削除する
- 数字ブロックは、非数字文字で区切られていた単位ごとに扱う
- 記号として意味を持ちうる非数字文字（例: 漢字、英字、カナ）は保持する

#### examples

- `神ー０１` → `神1`
- `２A１` → `2A1`
- `001-02` → `12`
- `A-001` → `A1`
- `(神)・01` → `神1`
- `＃001※` → `1`

#### empty policy

以下の場合、`insurance_symbol_match` は `None` とする。

- `field_norm` が `None`
- 除去処理の結果、空文字となる
- 数字・文字を含む実質的な記号本体が残らない

#### note

`insurance_symbol_match` は、`insurance_symbol_field_norm` よりも強く照合専用へ寄せた値とする。  
したがって、`field_norm` では保持していた `－` や `０` のような表現差も、`match` では吸収されうる。

### 7.3 insurance_number

#### field_norm

`insurance_number_field_norm` は、番号として後続処理に利用できる基本正規化値とする。

少なくとも以下を行う。

- 数字以外の文字を除去する
- 半角数字へ寄せる
- 先頭0を削除する（ただし全て0の場合は `0` とする）

例:
- `00102` → `102`
- `000` → `0`
- `A001-02` → `102`
- `１２３` → `123`

#### field_norm policy

`insurance_number` は値（数値）として扱う。  
そのため、`insurance_symbol` と異なり、`field_norm` の段階で先頭0を除去してよい。

また、`insurance_number` では文字や区切り記号は識別要素とみなさず、番号本体となる数字のみを残す。

#### match

`insurance_number_match` は、照合・比較・JOIN・検索用の値とする。

v1.1.0 では、`insurance_number_match` は `field_norm` と同一とする。

#### match policy

- `match == field_norm` とする
- 数字以外は保持しない（`field_norm` で除去済み）
- 先頭0は保持しない（`field_norm` で除去済み）
- 結果は常に半角数字のみとする

#### examples

- `00102` → `102`
- `000` → `0`
- `A001-02` → `102`
- `１２３` → `123`

#### empty policy

以下の場合、`insurance_number_match` は `None` とする。

- `field_norm` が `None`
- 数字抽出の結果、何も残らない

#### person_id_custom

`insurance_number` は `person_id_custom` の必須材料である。  
Layer 2 では、`insurance_number` に対する `FieldPurpose.PERSON_ID_CUSTOM` の値は、原則として `insurance_number_match` を流用してよい。

すなわち、v1.1.0 では以下を原則とする。

- `person_id_custom == insurance_number_match`

必要になれば将来版で分離可能とするが、現時点では二重定義しない。

#### identity_hash_input

`insurance_number` は `identity_hash` の直接材料ではない。  
`identity_hash` では `person_id_custom` を経由して利用されるため、Layer 2 で `identity_hash_input` 用の専用値は持たない。

#### export

`export` は出力先仕様に応じて別ルールを取りうる。  
v1.1.0 では詳細ルールは固定しないが、少なくとも以下を考慮できるようにしておく。

- 出力先が先頭0付き番号を要求するか
- 固定桁での0埋めが必要か
- 表示用と内部照合用の表現を分ける必要があるか

#### missing policy

以下の場合、`insurance_number` は missing と扱う。

- `raw` が `None`
- `base_norm` 完了時点で空値
- 数字抽出の結果、何も残らない

missing の場合は、少なくとも以下を満たす。

- `ok=False`
- `missing=True`
- `reason` を設定する

#### note

`insurance_number` は `insurance_symbol` と異なり、構造ではなく数値そのものを主とする項目である。  
そのため、`field_norm` と `match` は実質的に同じ値になってよい。

### 7.4 birthdate

#### field_norm

`birthdate_field_norm` は、生年月日として後続処理に利用できる基本正規化値とする。

v1.1.0 では、`birthdate_field_norm` は原則として西暦の `YYYY-MM-DD` 形式とする。  
ただしこれは表示用文字列ではなく、生年月日として意味が確定した正規形を表す。

少なくとも以下を行う。

- 西暦文字列を解釈する
- 和暦文字列を解釈する
- 元号コード1桁 + 和暦年月日6桁の7桁表現を解釈する
- 不正日付を排除する
- 最終的に西暦 `YYYY-MM-DD` へ統一する

#### accepted input patterns

少なくとも以下を受け入れ対象とする。

- 西暦8桁: `19850102`
- 区切り付き西暦: `1985-01-02`, `1985/1/2`, `1985.1.2`
- 和暦文字列: `昭和60年1月2日`, `平成1年5月3日`, `令和5年10月1日`
- 省略元号文字列: `S60/1/2`, `H01-05-03`, `R5.10.01`
- 元号コード7桁: `GYYMMDD`（ここで `G` はプレースホルダであり、実データ上は 1〜5 の元号コード1桁を表す）

元号コードは以下で固定する。

- `1` = 明治
- `2` = 大正
- `3` = 昭和
- `4` = 平成
- `5` = 令和

例:
- `3600102` → `1985-01-02`
- `4010503` → `1989-05-03`
- `5011001` → `2019-10-01`

#### conversion policy

和暦は入口で吸収し、内部では西暦のみを扱う。  
元号年は以下の式で西暦へ変換する。

- `western_year = era_start_year + era_year - 1`

対応する開始年は以下とする。

- 明治: `1868`
- 大正: `1912`
- 昭和: `1926`
- 平成: `1989`
- 令和: `2019`

また、和暦文字列における `元年` は `1年` と同義として扱う。

#### field_norm policy

`birthdate` は日付の意味を主とする項目である。  
そのため、`field_norm` では比較専用の文字列ではなく、まず西暦日付として意味が安定した形へ正規化する。

v1.1.0 では、`birthdate_field_norm` の表現は `YYYY-MM-DD` を標準とする。  
ただし、将来的に date 型等の別表現へ変更しても、意味としての正規形は同一とみなす。

#### match

`birthdate_match` は、照合・比較・JOIN・検索・ID生成用の値とする。

v1.1.0 では、`birthdate_match` は `birthdate_field_norm` を起点に生成し、`YYYYMMDD` 形式へ変換した8桁半角数字とする。

#### match policy

- `match` は常に `YYYYMMDD` とする
- 区切り文字は含まない
- 年月日はゼロ埋めされた8桁とする
- `field_norm` が `None` の場合は `match` も `None` とする

例:
- `1985-01-02` → `19850102`
- `1989-05-03` → `19890503`
- `2019-10-01` → `20191001`

#### person_id_custom

`birthdate` は `person_id_custom` の必須材料である。  
Layer 2 では、`birthdate` に対する `FieldPurpose.PERSON_ID_CUSTOM` の値は、原則として `birthdate_match` を流用してよい。

すなわち、v1.1.0 では以下を原則とする。

- `person_id_custom == birthdate_match`

必要になれば将来版で分離可能とするが、現時点では二重定義しない。

#### identity_hash_input

`birthdate` は `identity_hash` の直接材料ではない。  
`identity_hash` では `person_id_custom` を経由して利用されるため、Layer 2 で `identity_hash_input` 用の専用値は持たない。

#### export

`export` は出力先仕様に応じて別ルールを取りうる。  
v1.1.0 では少なくとも以下を考慮できるようにしておく。

- `YYYY-MM-DD` を要求するか
- `YYYYMMDD` を要求するか
- date 型相当の値を要求するか
- 和暦表示が必要か

ただし、内部では和暦を保持せず、西暦から必要な表現へ再フォーマットする。

#### empty / invalid policy

以下の場合、`birthdate` は missing または invalid と扱う。

- `raw` が `None`
- `base_norm` 完了時点で空値
- 解釈可能な日付形式に該当しない
- 元号コードが不正
- 和暦年が `00` など不正
- 月日が実在しない

v1.1.0 では `FieldResult` に invalid 専用フラグは持たず、値を採用できないケースは広く `missing=True` で扱い、詳細は `reason` で `invalid_xxx` として明示する。

missing / invalid の場合は、少なくとも以下を満たす。

- `ok=False`
- `missing=True` または `reason` で invalid を明示する
- `reason` を設定する

#### examples

- `19850102` → field_norm: `1985-01-02`, match: `19850102`
- `1985/1/2` → field_norm: `1985-01-02`, match: `19850102`
- `昭和60年1月2日` → field_norm: `1985-01-02`, match: `19850102`
- `H01-05-03` → field_norm: `1989-05-03`, match: `19890503`
- `5011001` → field_norm: `2019-10-01`, match: `20191001`

#### note

`birthdate` は field によって `YYYY-MM-DD` あるいは `YYYYMMDD` を使い分けうるが、意味としては同一の生年月日を表す。  
v1.1.0 では、`field_norm` を `YYYY-MM-DD`、`match` を `YYYYMMDD` とすることで、意味と識別用表現を分離する。

### 7.5 name_kana

本specにおける氏名カナ系の正式命名は以下で固定する。

- `name_kana_full`
- `name_kana_family`
- `name_kana_given`
- `name_kana_middle`

このうち、v1.1.0 で identity 系処理の主対象とするのは `name_kana_full` とする。  
結合された氏名カナは、split 状態に関わらず必ず `full` を付与する。

#### Role in identity processing

`name_kana` は、人物同一性の補助識別に用いる。  
特に `identity_hash` の必須材料としては、`name_kana_full_match` を使用する。

したがって、`name_kana` 系項目では以下を主に生成対象とする。

- `name_kana_full_base_norm`
- `name_kana_full_field_norm`
- `name_kana_full_match`

必要に応じて、family / given / middle に対しても同様の構造を適用できるが、v1.1.0 では `full` を優先する。

#### raw

`raw` は受領値をそのまま保持する。

例:
- `name_kana_full_raw`

ここでは以下を行わない。

- ひらがな→カタカナ変換
- 中黒除去
- 空白除去
- 長音 / ダッシュ整理

#### base_norm

`name_kana` における `base_norm` は、共通下ごしらえとして以下を行う。

- NFKC
- 制御文字除去
- 空白類の整理
- trim
- 空値判定

この段階では、まだ氏名カナ項目としての意味変換は行わない。

#### field_norm

`name_kana` における `field_norm` は、氏名カナとして後続処理に利用できる基本正規化値とする。

`name_kana_full_field_norm` では、少なくとも以下を行う。

- 全角カタカナへ寄せる
- 半角カナを吸収する
- ひらがなをカタカナへ寄せる
- 半角/全角空白を除去する
- 中黒を除去する
- 小書きカナの扱いを固定ルールで正規化する
- 長音符 / ダッシュ類の揺れを固定ルールで吸収する

#### field_norm policy

`field_norm` の目的は、氏名カナとして比較可能な安定した形を得ることにある。  
ただし、ここではまだ用途別の最終値とはみなさない。

すなわち、以下を区別する。

- `field_norm`: 氏名カナとして安定した基本形
- `match`: 照合専用の最終比較形

#### match

`name_kana_full_match` は、照合・比較・`identity_hash` 入力用の値とする。

v1.1.0 では、`name_kana_full_match` は原則として `field_norm` を起点に生成し、少なくとも以下を満たす。

- 全角カタカナである
- 小書きカナは大文字へ正規化されている
- 空白を含まない
- 中黒を含まない
- ハイフン類を含まない
- 長音符を含まない
- 比較に不要な表記差が極力除去されている

実質的に、`name_kana` 系では `match` は `field_norm` をさらに照合専用へ寄せた値とする。  
ただし設計上は責務を分離し、`match` を独立概念として扱う。

特に `name_kana_full_match` では、照合用途を優先し、小書きカナは大文字へ正規化し、ハイフン類・長音符・中黒は除去する。

#### person_id_custom

`name_kana` は `person_id_custom` の必須材料ではない。  
そのため、`name_kana` 系項目は `person_id_custom` 用値を主用途としては持たない。

v1.1.0 では、`name_kana` に対して `FieldPurpose.PERSON_ID_CUSTOM` を受け取るケースは原則想定しない。

#### identity_hash_input

`name_kana` は `identity_hash` の必須材料である。  
ただし、Layer 2 では専用の別値を新規生成するよりも、`name_kana_full_match` を `identity_hash_input` として流用する設計を基本とする。

すなわち、v1.1.0 では以下を原則とする。

- `identity_hash_input == name_kana_full_match`

必要になれば将来版で分離可能とするが、現時点では二重定義しない。

#### export

`export` は、出力先仕様に応じて別ルールを取りうる。  
v1.1.0 では、少なくとも次を考慮できるようにしておく。

- 出力先が空白付きカナを許容するか
- 中黒を維持する必要があるか
- 長音符の表記をどう扱うか

ただし、本specでは `export` の詳細ルールは固定しない。

#### missing policy

以下の場合、`name_kana` は missing と扱う。

- `raw` が `None`
- `base_norm` 完了時点で空値
- `field_norm` の結果、実質空となる

missing の場合は、少なくとも以下を満たす。

- `ok=False`
- `missing=True`
- `reason` を設定する

#### identity policy note

`identity_hash` の必須材料として使用するのは、正式には `name_kana_full_match` とする。  
過去会話で `name_kana_match` と表現していたものは、以後この `name_kana_full_match` を指す。

#### v1.0 実装との差分メモ

v1.0 系実装においては、`name_kana_full_match` 相当の処理で、小書きカナの大文字化・ハイフン類除去・中黒除去は実装済みである一方、長音符除去は未実装であることを確認している。

したがって、v1.1.0 では `name_kana_full_match` の正規定義として長音符を除去することを明示的に採用し、この差分を canonicalization 変更点として扱う。

この差分により、既存 `identity_hash` の一部は旧定義ベースで生成されている可能性があるため、backfill 時には以下を必ず確認対象とする。

- `name_kana_full_match` の再生成
- `identity_hash` の再生成
- 長音符除去差分に起因する hash 変化件数の確認

本差分は見落とし防止のため、v1.1.0 backfill の明示的なチェック項目として扱う。

### 7.6 gender_code

#### field_norm

`gender_code_field_norm` は、性別として後続処理に利用できる基本正規化値とする。

v1.1.0 では、性別はコード値として正規化し、以下のいずれかに統一する。

- `1` = 男性
- `2` = 女性

少なくとも以下を行う。

- 表記ゆれを吸収する
- 日本語・英語・記号表現を正規化する
- 最終的にコード値へ変換する

#### accepted input patterns

少なくとも以下を受け入れ対象とする。

- 数値: `1`, `2`
- 日本語: `男`, `男性`, `男子`, `女`, `女性`, `女子`
- 英語: `M`, `F`, `Male`, `Female`, `man`, `woman`
- 小文字・全角混在: `ｍ`, `ｆ`, `male`, `female`

#### normalization mapping

以下のように正規化する。

| input | field_norm |
|------|------------|
| `1`, `男`, `男性`, `M`, `Male` | `1` |
| `2`, `女`, `女性`, `F`, `Female` | `2` |

上記は v1.1.0 時点での固定マッピングである。運用上新たな同義表現が確認された場合は、意味を曖昧にしない範囲で mapping へ追加してよい。

#### field_norm policy

`gender_code` はカテゴリ（区分値）として扱う。  
そのため、`field_norm` の段階で意味を確定させ、コードへ変換する。

文字列としての保持ではなく、意味を確定した最小単位（`1` / `2`）を採用する。

#### match

`gender_code_match` は、照合・比較・JOIN・検索・`identity_hash` 生成用の値とする。

v1.1.0 では、`gender_code_match` は `field_norm` と同一とする。

#### match policy

- `match == field_norm` とする
- 常に `1` または `2` とする
- それ以外は許容しない
- `field_norm` が `None` の場合は `match` も `None` とする

#### person_id_custom

`gender_code` は `person_id_custom` の材料ではない。  
Layer 2 で `FieldPurpose.PERSON_ID_CUSTOM` 用の専用値は持たない。

#### identity_hash_input

`gender_code` は `identity_hash` の必須材料である。  
Layer 2 では、`gender_code` に対する `FieldPurpose.IDENTITY_HASH` の値は、原則として `gender_code_match` を流用してよい。

すなわち、v1.1.0 では以下を原則とする。

- `identity_hash_input == gender_code_match`

必要になれば将来版で分離可能とするが、現時点では二重定義しない。

#### export

`export` は出力先仕様に応じて別ルールを取りうる。  
v1.1.0 では少なくとも以下を考慮できるようにしておく。

- `1 / 2` コードを要求するか
- `男 / 女` を要求するか
- `M / F` を要求するか

ただし内部では常に `1 / 2` を保持し、外部仕様に応じて変換する。

#### empty / invalid policy

以下の場合、`gender_code` は missing または invalid と扱う。

- `raw` が `None`
- `base_norm` 完了時点で空値
- マッピングに該当しない値（例: `不明`, `その他`）

missing / invalid の場合は、少なくとも以下を満たす。

- `ok=False`
- `missing=True` または `reason` で invalid を明示する
- `reason` を設定する
- `match` または `identity_hash_input` は `None` とする

#### examples

- `男` → field_norm: `1`, match: `1`
- `女性` → field_norm: `2`, match: `2`
- `M` → field_norm: `1`, match: `1`
- `female` → field_norm: `2`, match: `2`
- `不明` → invalid

#### note

`gender_code` は `identity_hash` の構成要素の中で、`person_id_custom` と `name_kana_full_match` を補完する区分値として扱う。  
そのため、曖昧な値を許容せず、確定できない場合は無理に補完しないことを原則とする。

## 8. Builder Responsibilities

### 8.1 build_match_fields

各主要項目について `FieldPurpose.MATCH` を用いて値を生成し、builder で扱いやすいまとまりとして返す。

### 8.2 build_person_id_custom

`build_person_id_custom` は、identity 系の基礎識別値を生成する builder とする。  
本builderは、加入者識別に必要な canonical input を固定順で連結し、`person_id_custom` を生成する責務を持つ。

#### required inputs

`person_id_custom` の必須材料は以下の4つで固定する。

- `birthdate_match`
- `insurance_number_match`
- `insurer_number_match`
- `insurance_symbol_match`

この4つのうち1つでも欠ける場合、`person_id_custom` は生成しない。

#### canonical input policy

`person_id_custom` は raw 値や field_norm を直接連結して生成してはならない。  
必ず以下の canonical 値を用いる。

- `birthdate_match`: Layer 2 `FieldPurpose.MATCH` の値
- `insurance_number_match`: Layer 2 `FieldPurpose.MATCH` の値
- `insurer_number_match`: Layer 2 `FieldPurpose.MATCH` の値
- `insurance_symbol_match`: Layer 2 `FieldPurpose.MATCH` の値

この builder は、各項目の `match` を identity 用 canonical input として採用する。

#### generation rule

v1.1.0 では、`build_person_id_custom` は canonical input を以下の順で `custom_id_gen.generate_id` に渡して生成する前提とする。

入力順:

1. `birthdate_match`
2. `insurance_number_match`
3. `insurer_number_match`
4. `insurance_symbol_match`

この順序は仕様上固定とし、順序を変更してはならない。

#### generation policy

- 必須材料に `None` / 空文字を含めて生成してはならない
- builder 内で補完・推測・代替値生成をしてはならない
- 同一 canonical input からは常に同一 `person_id_custom` が生成されること
- `insurance_symbol_match` / `insurance_number_match` / `insurer_number_match` / `birthdate_match` の定義変更は `person_id_custom` の再生成条件になりうる

#### missing policy

以下の場合、`build_person_id_custom` は未生成とする。

- `birthdate_match` が `None` または空
- `insurance_number_match` が `None` または空
- `insurer_number_match` が `None` または空
- `insurance_symbol_match` が `None` または空

未生成時は少なくとも以下を満たすこと。

- `ok=False`
- `value=None`
- `missing_fields` に不足項目を記録する
- `reason` を設定する

#### upstream missing policy

`person_id_custom` 未生成時は、builder は上流の不足理由を追跡できることが望ましい。  
特に field layer 側で missing / invalid が発生していた場合、builder は `upstream_missing_fields` を通じて元原因を保持できることが望ましい。

例:
- `insurance_symbol_match` 未生成
- 原因が `insurance_symbol_field_norm` 段階で実質空
- `person_id_custom` builder は直接不足に加え upstream 不足も把握可能とする

#### examples

入力:

- `birthdate_match = 19850102`
- `insurance_number_match = 102`
- `insurer_number_match = 6139463`
- `insurance_symbol_match = 神1`

builder への canonical input:

- `birth_yyyymmdd = 19850102`
- `insurance_number = 102`
- `insurer_number = 6139463`
- `insurance_symbol = 神1`

出力:

- `person_id_custom` の完成値

#### implementation note

現行の既存実装では `generate_person_id_custom` ラッパー経由で `custom_id_gen.generate_id` を呼び出している。  
その際、`insurer_number` を 8桁ゼロ埋めして渡す処理が存在する場合でも、v1.1.0 の canonical input 定義としては `insurer_number_match` 自体を正とする。  
ゼロ埋めは generator 側インターフェース都合のフォーマット処理として扱い、identity 設計上の canonical value とは分離して考える。

したがって、v1.1.0 の設計レビューでは generator へ渡す直前のゼロ埋め有無を canonicalization と混同しないこと。

#### backfill note

`person_id_custom` は canonical input の定義変更の影響を受ける。  
特に以下の差分は再生成 backfill 条件になりうる。

- `birthdate_match` の解釈変更（和暦 / 元号コード対応を含む）
- `insurance_number_match` の数字抽出 / 先頭0削除ルール変更
- `insurer_number_match` の数字抽出 / 先頭0削除ルール変更
- `insurance_symbol_match` の canonicalization 変更

backfill 時は少なくとも以下を確認対象とする。

- 4材料の再生成
- `person_id_custom` の再生成
- 旧値 / 新値の差分件数
- 差分原因となった項目別件数

#### note

`person_id_custom` は `identity_hash` の上流に位置する基礎識別値である。  
そのため、`person_id_custom` の canonicalization 品質は downstream の `identity_hash` 品質に直結する。  
v1.1.0 では、`person_id_custom` の品質は `birthdate_match`・`insurance_number_match`・`insurer_number_match`・`insurance_symbol_match` の定義固定によって担保する。

### 8.3 build_identity_hash

`build_identity_hash` は、identity 系の最終識別値を生成する builder とする。  
本builderは、人物同一性の比較を補助するための安定した hash を生成する責務を持つ。

#### required inputs

`identity_hash` の必須材料は以下の3つで固定する。

- `person_id_custom`
- `name_kana_full_match`
- `gender_code_match`

この3つのうち1つでも欠ける場合、`identity_hash` は生成しない。

#### canonical input policy

`identity_hash` は raw 値や field_norm を直接連結して生成してはならない。  
必ず以下の canonical 値を用いる。

- `person_id_custom`: Layer 3 `build_person_id_custom` の完成値
- `name_kana_full_match`: Layer 2 `FieldPurpose.MATCH` の値
- `gender_code_match`: Layer 2 `FieldPurpose.MATCH` の値

特に `gender_code` は raw / field_norm 名で曖昧に扱わず、identity 用入力としては正式に `gender_code_match` を採用する。

#### generation rule

`identity_hash` は、上記 canonical input を以下の順で区切り文字付き連結し、SHA-256 で hash 化した16進文字列とする。

連結順:

1. `person_id_custom`
2. `name_kana_full_match`
3. `gender_code_match`

連結形式:

```text
{person_id_custom}|{name_kana_full_match}|{gender_code_match}
```

hash 方式:

- UTF-8 で encode
- SHA-256 を適用
- 16進文字列（lowercase hex digest）で保持

#### generation policy

- 必須材料に `None` / 空文字を含めて生成してはならない
- builder 内で補完・推測・代替値生成をしてはならない
- `identity_hash` 自体は deterministic でなければならない
- 同一 canonical input からは常に同一 hash が生成されること

#### missing policy

以下の場合、`build_identity_hash` は未生成とする。

- `person_id_custom` が `None` または空
- `name_kana_full_match` が `None` または空
- `gender_code_match` が `None` または空

未生成時は少なくとも以下を満たすこと。

- `ok=False`
- `value=None`
- `missing_fields` に不足項目を記録する
- `reason` を設定する

#### upstream missing policy

`identity_hash` 未生成の理由が `person_id_custom` 未生成に起因する場合、builder は `upstream_missing_fields` を通じて上流不足も追跡できることが望ましい。

例:
- `person_id_custom` 未生成
- その上流原因が `insurance_symbol_match` 欠損
- `identity_hash` builder は、直接不足に加え upstream 不足も把握可能とする

#### examples

入力:

- `person_id_custom = P12345ABCDE`
- `name_kana_full_match = ナガオヒロフミ`
- `gender_code_match = 1`

連結文字列:

```text
P12345ABCDE|ナガオヒロフミ|1
```

出力:

- SHA-256 の lowercase hex digest

#### implementation note

現行の v1.0 / 既存実装では `build_identity_hash` の引数名に `gender_code` が使われている箇所があるが、v1.1.0 の正式specでは `gender_code_match` を canonical input 名として固定する。  
実装側で引数名が旧名のまま残っている場合も、意味上は `gender_code_match` を受け取るものとして統一して扱う。

#### backfill note

`identity_hash` は canonical input の定義変更の影響を受ける。  
特に v1.1.0 では `name_kana_full_match` の定義に長音符除去を追加しているため、既存 `identity_hash` は再生成 backfill 対象となる。

backfill 時は少なくとも以下を確認対象とする。

- `name_kana_full_match` の再生成
- `gender_code_match` の再評価（コード定義との整合確認）
- `person_id_custom` の既存値利用可否
- `identity_hash` の再生成
- 旧 hash / 新 hash の差分件数

#### note

`identity_hash` は人物同一性の補助識別子であり、単独で raw identity を表すものではない。  
そのため、生成材料の canonicalization 品質がそのまま hash 品質に直結する。  
v1.1.0 では、`identity_hash` の品質は `person_id_custom`・`name_kana_full_match`・`gender_code_match` の定義固定によって担保する。

### 8.4 build_identity_bundle

必要に応じて、match / person_id_custom / identity_hash をまとめて構築する親builderを持ってよい。  
ただし個別 builder の責務は維持する。

## 9. Missing / Error Policy

### 9.1 Missing handling principles

- 欠損は例外ではなくデータとして扱う
- `ok=False` / `missing=True` / `reason` で返す
- 無理な補完や推測生成は行わない

### 9.2 No forced generation policy

- 必須材料不足時は `person_id_custom` / `identity_hash` を生成しない
- `null` や空値を混ぜた生成は禁止
- 健保別の事情があっても、下位の共通生成ロジックは崩さない

### 9.3 Upstream missing tracking

builder は、自身の不足項目だけでなく upstream の不足項目も追跡可能とする。  
特に `identity_hash` では、`person_id_custom` 未生成の上流原因を把握できることが望ましい。

## 10. Insurer / Profile Rule Layer

### 10.1 Responsibility boundary

Layer 4 は以下を担当する。

- どの profile を使うか決める
- どの値を採用候補とするか決める
- 既知の欠損傾向や制約を扱う

### 10.2 Common layer boundary

Layer 4 は下位レイヤーのロジックを変更してはならない。  
下位レイヤーは pure / common を維持し、上位レイヤーは選択と制御に徹する。

### 10.3 Future extension policy

協会けんぽ、郵政等の健保差分は Layer 4 の insurer / profile rule layer で吸収する。  
ただし、欠損値を補完して `person_id_custom` や `identity_hash` を救済生成してはならない。

## 11. Notes

- 本specは実装前提の詳細設計である
- 実装時は本specの責務境界を崩さないことを優先する
- 実装差分が発生した場合は、spec を先に更新してからコードへ反映する