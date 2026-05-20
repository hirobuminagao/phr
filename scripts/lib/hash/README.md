# scripts/lib/hash

hash 共通lib。

目的:

```text
canonical compare hash を
共通手順で生成すること。
```

本ディレクトリは:

```text
identity resolve
field normalize
match generation
DB access
```

を責務に持たない。

---

# Current Design Policy

compare hash は:

```text
同一 entity の比較対象値を
軽量に比較するための hash
```

として扱う。

compare hash の目的は:

```text
full compare を完全に無くすこと
```

ではなく、

```text
full compare が必要な候補を高速に絞ること
```

である。

---

# compare hash の基本仕様

compare hash は:

```text
1. values list を受け取る
2. 各値を base_norm に通す
3. delimiter で連結する
4. sha256 を生成する
5. hex digest を返す
```

の固定手順で生成する。

---

# Important Policy

重要:

```text
compare hash は match 値を前提にしない
```

compare hash の標準用途は:

```text
norm 値
```

を hash 化することである。

match 値を hash 化したい場合は:

```text
呼び出し側で match 値を生成し、
その値を compare hash に渡す。
```

compare hash lib 自体は:

```text
match 化
field-specific normalize
identity hash generation
```

を行わない。

---

# Current Files

## compare_hash.py

compare hash helper。

責務:

```text
- values list を受け取る
- base_norm を適用する
- delimiter join を行う
- sha256 hash を返す
```

特徴:

```text
- values 数は固定上限を持つ
- values 順序を維持する
- compare 用 lightweight hash を生成する
- field-specific normalize は行わない
```

現在の制限:

```text
max values count = 16
```

16 を超える場合は:

```text
hash の責務分割がおかしい可能性が高い
```

ため、例外として扱う。

想定用途:

```text
identity_norm_hash
address_hash
other_update_hash
```

非想定用途:

```text
巨大payload hash
JSON blob hash
entity 全項目 hash
```
