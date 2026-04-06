

# Identity Layer Structure (v1.1.0)

## 1. Overview

本ドキュメントは、identity 系処理におけるライブラリ構造と責務分離を定義する。

本構造は以下の spec / ADR と整合する。

- 親spec: `v1.1.0_identity_layer_commonization.md`
- 子spec: `identity_layers_norm_and_purpose.md`
- ADR: `0016-v1.1.0-identity-layer-commonization-and-backfill.md`

本ドキュメントは「実装構造（どこに何を書くか）」を固定する。

---

## 2. Directory Structure

```text
phr/scripts/lib/
└── identity/
    ├── primitive/
    │   ├── normalize.py
    │   ├── remove.py
    │   ├── convert.py
    │   ├── digits.py
    │   └── dates.py
    │
    ├── base_norm.py
    │
    ├── field/
    │   ├── insurer_number.py
    │   ├── insurance_symbol.py
    │   ├── insurance_number.py
    │   ├── birthdate.py
    │   ├── name_kana.py
    │   └── gender_code.py
    │
    └── builder/
        ├── person_id_custom.py
        └── identity_hash.py
```

---

## 3. Layer Responsibility

### 3.1 primitive (Layer 1)

最小単位の処理部品（副作用なし・純関数前提）を提供する。

#### normalize.py
- 揺れを揃える処理
- 例: NFKC, 空白正規化, ハイフン正規化

#### remove.py
- 不要要素の除去（トルツメ）
- 例: 制御文字除去, 中黒除去, 長音符除去, 記号ノイズ除去

#### convert.py
- 表現変換
- 例: ひらがな→カタカナ, 半角/全角変換, 小書きカナ正規化

#### digits.py
- 数字専用処理
- 例: 数字抽出, 先頭0除去, ゼロ埋め

#### dates.py
- 日付専用処理
- 例: 和暦→西暦, 元号コード→西暦, 日付フォーマット変換

---

### 3.2 base_norm (Layer 1.5)

primitive を組み合わせた「全項目共通の下ごしらえ正規化」を担う。

- Unicode正規化
- 空白整理
- 制御文字除去
- 空値判定

※ 項目依存の判断は行わない

---

### 3.3 field (Layer 2)

各項目ごとの意味に基づいた正規化を行う。

- `field_norm` の生成
- `match` の生成

対象:
- insurer_number
- insurance_symbol
- insurance_number
- birthdate
- name_kana
- gender_code

---

### 3.4 builder (Layer 3)

完成値の生成を担う。

- `person_id_custom`
- `identity_hash`

ルール:
- 必須材料が揃わない場合は生成しない
- 入力は canonical 値（原則 match）を使用する

---

## 4. Dependency Flow

依存関係は以下の一方向のみとする。

```text
primitive
   ↓
base_norm
   ↓
field
   ↓
builder
```

逆方向の依存は禁止。

---

## 5. Design Principles

### 5.1 責務分離

- primitive: 操作単位
- base_norm: 共通前処理
- field: 項目意味
- builder: 完成値生成

---

### 5.2 再利用性

primitive は複数 field から再利用される前提で設計する。

---

### 5.3 可読性優先

処理は「何をしているか」が読み取れる順序で記述する。

例:

```python
value = normalize.to_nfkc(raw)
value = remove.remove_control_chars(value)
value = convert.hiragana_to_katakana(value)
value = digits.strip_leading_zeros_keep_zero(value)
```

---

### 5.4 v1.1.0 方針

- canonical input の多くは `match` を流用する
- ただし責務としては builder と分離する

---

## 6. Notes

- 本構造は v1.1.0 の実装基準とする
- 拡張は可能だが、既存責務を崩さないこと
- ファイル分割は必要になった時点で行う（過分割禁止）
- 本構造は identity 共通libの実装基準であり、HIA XML import / HIA dashboard import / subscriber import を含む全パイプラインで共通利用する前提とする
