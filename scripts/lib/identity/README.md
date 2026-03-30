

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

## Responsibility

### primitive/

最小単位の純関数群を置く。

- `normalize.py`: 揺れを揃える処理
- `remove.py`: 不要要素の除去
- `convert.py`: 表現変換
- `digits.py`: 数字専用処理
- `dates.py`: 日付専用処理

### base_norm.py

primitive を組み合わせた全項目共通の下ごしらえ正規化を置く。

### field/

各項目ごとの意味に基づく処理を置く。

対象:
- insurer_number
- insurance_symbol
- insurance_number
- birthdate
- name_kana
- gender_code

各ファイルでは、主に以下を担当する。

- `field_norm`
- `match`
- 必要に応じた `person_id_custom` / `identity_hash_input` / `export` 用値

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

## Notes

- v1.1.0 では canonical input の多くを `match` から流用する
- ただし、責務としては `match` と builder を分離して考える
- 過分割は避け、責務が明確な単位で実装する