


# 09_value_resolution

## ■ 目的
複数の入力カラム（CSV / XML）から、
単一の target カラムへ値を決定するルールを定義する。

本仕様は「どの値を採用するか（意味決定）」を扱うレイヤであり、
単純マッピング（template_mappings）とは責務を分離する。

---

## ■ レイヤー位置

```
input data (CSV / XML)
  ↓
template_mappings（単純マッピング）
  ↓
value_resolution（本仕様）
  ↓
staging / target
```

---

## ■ 空値定義（共通ルール）

以下はすべて「空値」とみなす：

- None
- ""
- 半角スペースのみ
- 全角スペースのみ
- スペース混在のみ

### 判定関数

```python
def is_empty(v):
    return v is None or str(v).strip() == ""
```

※ 本関数は全処理で共通利用する

---

## ■ 基本ルール

### 1. 上書きルール（default）

```
後勝ち（last-write-wins）
ただし空値は上書き不可
```

### 挙動

- 既に値がある場合
  - 新しい値が空 → 無視
  - 新しい値が非空 → 上書き

---

## ■ 値選択ルール（拡張）

### 1. first_valid

```
最初に見つかった非空値を採用
```

例：

```
[本人資格取得日, 家族認定日]
→ 本人が空なら家族
```

---

### 2. last_valid

```
最後に見つかった非空値を採用
```

---

### 3. priority_order

```
優先順位リストに従って選択
```

例：

```
priority = ["本人", "家族"]
```

---

## ■ グループ処理

複数カラムを1単位として扱う

### 例：資格取得日

- 本人資格取得日
- 家族認定日

---

### 定義例

```yaml
group:
  name: qualification_date
  sources:
    - 本人資格取得日
    - 家族認定日
  rule: first_valid
```

---

## ■ 派生列生成

1つの決定結果から複数カラムを生成可能

例：

```
資格取得日 → start_date
資格区分   → qualification_type
```

---

## ■ 実装イメージ

```python
def resolve_value(values, rule):
    non_empty = [v for v in values if not is_empty(v)]

    if not non_empty:
        return None

    if rule == "first_valid":
        return non_empty[0]

    if rule == "last_valid":
        return non_empty[-1]

    raise NotImplementedError
```

---

## ■ 今回実装との関係

現在の実装は以下を採用：

```
- 後勝ち（last-write-wins）
- 空値上書き禁止
```

→ 本仕様における default として固定する

---

## ■ 今後の拡張

- 条件付き選択（例：被保険者区分による分岐）
- 複合条件（Aがある場合のみB採用）
- スコアリング選択

---

## ■ 補足（設計意図）

```
mapping = データの場所を決める
resolution = データの意味を決める
```

本仕様は「意味決定レイヤ」を明確に分離することを目的とする
---

## ■ 漢字氏名の match ルール（固定仕様）

本仕様では、漢字氏名に含まれる「かな混在・表記ゆれ」を吸収し、
照合可能な match 値を生成するルールを定義する。

---

### ■ 対象

- name_kanji_full
- name_kanji_parts（family / middle / given）

---

### ■ 基本方針

```
norm = 表示・保存用（意味を変えない）
match = 照合用（最大限寄せる）
```

👉 漢字の変換・かな寄せは **match 側のみで実施する**

---

### ■ match 生成ルール（順序固定）

入力値に対して以下の順で変換を行う：

1. 漢字正規化辞書を適用
   - identity_kanji_normalization テーブルを使用
   - 例：髙 → 高

2. ひらがな → カタカナ変換

3. 小書きかな → 大文字化
   - 例：ゃ → ヤ、ぃ → イ

4. 記号除去
   - 中黒「・」
   - 記号類（括弧等）

5. ハイフン・長音符除去
   - 「ー」「－」「-」「―」などすべて除去

6. スペース除去
   - 半角 / 全角ともに削除

7. 英数字の全角化

---

### ■ 変換例

#### 入力

```
長尾 　ひろーブミぃ
```

#### norm

```
長尾　ひろーブミぃ
```

#### match

```
長尾ヒロブミイ
```

---

### ■ parts 変換

#### norm_parts

```
family = 長尾
given  = ひろーブミぃ
```

#### match_parts

```
family = 長尾
given  = ヒロブミイ
match_full = 長尾ヒロブミイ
```

---

### ■ 注意事項

- match は **完全一致照合用キー**として使用する
- norm から match は生成可能だが、逆変換は不可
- split（姓・名分解）は norm 側でのみ実施する

---

### ■ 設計意図

```
漢字 = 見た目
かな = 読み
match = 同一人物判定キー
```

本ルールにより、

- ひらがな / カタカナ混在
- 小書きゆれ
- 長音符ゆれ

をすべて吸収し、安定した照合を実現する