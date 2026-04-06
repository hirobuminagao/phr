### 4.1 正規化フロー（v1.1.0 実装ルール）

すべての match 値および identity 生成用 canonical 値は、共有の identity レイヤーを通じて生成されなければなりません。

v1.1.0 では、旧 `common.py` 中心の説明は採用せず、`scripts/lib/identity/` 配下の共通lib構造を正とする。

基本構造は以下の通りとする。

- `primitive/` : 最小単位の文字変換・除去・数字処理
- `base_norm.py` : 項目共通の基礎正規化
- `field/` : 項目ごと・purposeごとの canonical 値生成
- `builder/` : canonical 値を受けて person_id_custom / identity_hash を生成

正規化フローの基本形は以下とする。

```text
アプリケーションスクリプト
    ↓
identity primitive / base_norm / field
    ↓
purpose ごとの canonical 値
    ↓
identity builder
    ↓
person_id_custom / identity_hash
```

実装ルール:

- アプリケーションスクリプトで直接正規化ロジックを複製してはならない
- raw 値から canonical 値を生成する処理は `scripts/lib/identity/` に委譲する
- builder は canonical 値を受け取って組み立てのみを行う
- builder は raw 値の解釈や purpose 選択を責務に含めない

これにより以下の間で整合性を確保する。

- subscribers
- HIA dashboard
- HIA XML import
- medi

なお、raw からどの順で field / builder を呼び出すかという orchestration の責務分離については、v1.1.0 では詳細設計を保留とし、本 README では共通lib構造と builder の責務境界のみを固定する。