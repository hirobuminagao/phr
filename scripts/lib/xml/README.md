

# scripts/lib/xml

XML共通操作を置くディレクトリ。

この配下は、特定保健指導（SHG）や健診などの業務ドメインに依存しない、XMLそのものに対する薄い共通処理のみを扱う。

## 配置方針

- XML Element に対する汎用操作だけを置く
- SHG固有のOID、section、entry、report_code、保健指導区分などは扱わない
- 健診固有の項目コード、CDAセクション判定、健診種別判定などは扱わない
- 業務条件の判定は呼び出し元で行う
- この層は、呼び出し元が指定した対象に対してXML操作だけを行う

## delete.py の責務

`delete.py` は、既存XML要素の削除だけを担当する共通ユーティリティとする。

### 役割

- 呼び出し元から渡された削除対象要素を削除する
- 削除対象要素にぶら下がる子要素もまとめて削除する
- 削除できたかどうかを結果として返す
- 削除できない場合は理由を返す

### 想定入力

- 削除対象の親要素
- 削除対象要素
- 任意の識別ラベル
- 任意の削除理由

### 想定処理

- `parent.remove(target)` による既存要素の削除
- 削除対象が親要素の子であることの確認
- 削除対象または親要素が不足している場合のエラー結果返却

### XMLブロック削除の考え方

`delete.py` は、XML構造上の既存ブロックを parent-child 関係に基づいて削除する。

このとき、削除対象要素の子要素は XMLライブラリ標準動作に従いまとめて削除される。

例:

```text
entry
└ entryRelationship
  └ observation
    └ value
```

上記構造で `entryRelationship` を削除対象にした場合、配下の `observation` や `value` もまとめて削除される。

## 削除対象指定の方針

`delete.py` 自体は、XML業務意味を解釈しない。

そのため、削除対象は呼び出し元が特定する。

例:

- SHG側で `90060` section 内の `code = 1042001060` を持つ `entryRelationship` を特定する
- 呼び出し元が parent Element と target Element を保持する
- `delete.py` はその Element を削除するだけ

## XPath文字列のみで削除しない理由

XPath文字列だけで削除対象を後から再探索すると、以下の問題が起きやすい。

- 同一コードの複数出現
- section構造差異
- namespace差異
- 順序依存
- 想定外の別要素削除

そのため、削除対象探索時点で実際の XML Element と parent Element を保持し、その参照を使って削除する。

### 想定返却

- `deleted`: 削除できたか
- `label`: 呼び出し元が指定した識別ラベル
- `reason`: 呼び出し元が指定した削除理由
- `message`: 削除結果または失敗理由

## delete.py の非責務

`delete.py` は以下を行わない。

- XMLファイルの読み込み
- XMLファイルの保存
- XPath探索
- 削除条件の判定
- SHG固有の判定
- 健診固有の判定
- OID判定
- report_code 判定
- section / entry / observation の意味判定
- 新規XML要素の作成
- XML属性値の更新
- 空になった親要素の自動削除

## delete.py の責務境界

`delete.py` は「削除してよいか」の判断を行わない。

削除してよいかどうかは、各ドメイン側処理が判断する。

例:

- SHG側:
  - report_code 判定
  - 保健指導区分判定
  - observation code 判定
  - 値判定

- 健診側:
  - section種別判定
  - 項目コード判定
  - XML種別判定

`delete.py` は、それら条件判定済みの削除対象に対して削除操作だけを行う。

## 呼び出し元の責務

削除してよい対象かどうかは、呼び出し元が判断する。

例：SHGの特定条件でアウトカム合計ポイントブロックを削除する場合

- `scripts/shg/script_lib/outcome_point_block_fix.py` などのSHG側処理で条件判定する
- 削除対象の親要素と対象要素を特定する
- `scripts/lib/xml/delete.py` に削除対象を渡す
- 必要に応じて呼び出し元がXML保存を行う

## 将来想定

将来的には、必要に応じて以下のような共通XML削除補助を追加する可能性がある。

- empty parent cleanup
- section単位削除補助
- namespace付き探索補助
- location object 共通化
- XML変更履歴保持

ただし、現時点では過剰な共通化を行わず、実利用が確認された最小単位のみを実装対象とする。

## 設計メモ

このディレクトリは、最初から大きなXML共通基盤にしない。

まずは実利用がある最小の共通操作だけを置き、複数ドメインで再利用できる処理が見えた段階で拡張する。