

# medi_~ 健診結果XMLの扱い v1.0.0 精査メモ

## 1. この文書の位置づけ

本書は、v1.0.0時点の `medi_~` 系スクリプトにおける医療機関由来の健診結果XMLの扱いを整理するための暫定specである。

ここで扱うのは、厚生労働省の特定健診XMLフォーマットそのものの仕様整理ではなく、医療機関から受領した健診結果XMLを、労基・法定健診の観点でどのように判定・記録しているかの整理である。

現時点では実装・DDL・マスタ定義の精査が完了していないため、確定仕様ではなく、後続調査のための作業メモとして扱う。

ただし、本書は将来的に、医療機関からXMLを受領した時点で「必要な項目が揃っているか」「XMLとして不備がないか」「再提出依頼や確認アラートを出すべきか」を判断するための検収ルールへ育てていく。

## 2. 対象

- 医療機関から受領する健診結果XML
- `medi_~` 系スクリプト
- 労基・法定健診チェック
- 医療機関XML受領時の不備チェック
- 再提出依頼・確認アラートに使う判定材料
- `LSIO_Legal_Item` グループ定義
- `exam_item_groups`
- `exam_item_group_identity_members`
- `exam_item_group_members`

## 3. 対象外

- 厚生労働省の特定健診XMLフォーマット自体の仕様整理
- 特定保健指導XMLの仕様整理
- 健診値の医学的な正常・異常判定
- 労基署提出用帳票や行政提出形式の確定仕様
- 医療機関への再提出依頼文面そのもの

## 4. v1.0.0時点の理解

医療機関XMLについては、XML内の健診結果項目を抽出し、`namecode` / `identity_item_code` に寄せたうえで、労基・法定健診として必要な項目が存在するかを確認する設計と考えられる。

この判定は、値の医学的妥当性ではなく、項目の存在確認を主目的とする。

つまり、血圧や血糖などの値が正常範囲かどうかではなく、法定健診として必要な検査項目がXML上に存在しているかを確認する。

## 5. 法定健診チェックの扱い

`LSIO_Legal_Item` は、安衛則44条等の定期健康診断における法定項目を表すグループとして扱う。

判定結果は、少なくとも以下のカラム群で保持する想定である。

- `lsio_legal_required_count`
- `lsio_legal_present_count`
- `lsio_legal_is_complete`
- `lsio_legal_missing_methods`
- `lsio_legal_judged_run_id`
- `lsio_legal_judged_at`

一方で、以下のような `judge_*` 系カラムが存在する場合でも、v1.0.0時点で実際の法定健診判定として使用されているかは未精査である。

- `judge_status`
- `is_exam_result`
- `is_legal_exam`
- `judge_score`
- `judge_note`
- `judged_run_id`
- `judged_at`

## 6. required_count / present_count / is_complete の暫定理解

### 6.1 `lsio_legal_required_count`

`LSIO_Legal_Item` に属し、`required_flag = 1` の `identity_item_code` 数を表す。

現時点で確認している定義では、必須項目数は26件と見込まれる。

`required_flag = 0` の項目は、補助・任意項目として扱い、`required_count` には含めない想定である。

### 6.2 `lsio_legal_present_count`

必須 `identity_item_code` のうち、XML内で存在確認できた項目数を表す。

ここでのカウント単位は `namecode` ではなく、最終的には `identity_item_code` 単位である。

1つの `identity_item_code` に複数の `namecode` が紐づく場合、判定対象となる `namecode` のいずれかがXML内で有効に存在すれば、その `identity_item_code` は present とみなす想定である。

### 6.3 `lsio_legal_is_complete`

`lsio_legal_present_count == lsio_legal_required_count` の場合に 1 とする。

それ以外の場合は 0 とする。

`is_complete = 0` は、労基・法定健診として必要な必須項目がすべては揃っていないことを意味する。

ただし、`is_complete = 0` の中には、以下のような状態が混在する。

- 1〜2項目程度の軽微な不足
- 多数項目の不足
- そもそも法定健診XMLではない可能性が高いもの
- XML上に項目はあるが、presence判定条件を満たしていないもの

そのため、業務上の確認では `is_complete` だけでなく、`required_count - present_count` や `lsio_legal_missing_methods` も併せて確認する必要がある。

## 7. `exam_item_groups` の役割

`exam_item_groups` は、健診項目グループの定義を保持するテーブルである。

`LSIO_Legal_Item` は、労基・法定健診項目の判定グループとして扱う。

このテーブルは、判定ロジックそのものではなく、どのグループ定義を用いて判定するかの起点である。

## 8. `exam_item_group_identity_members` の役割

`exam_item_group_identity_members` は、グループに属する `identity_item_code` と、その必須・任意の区分を保持する。

主な役割は以下である。

- `LSIO_Legal_Item` に属する法定健診項目を定義する
- `required_flag` により必須項目かどうかを表す
- `required_presence_namecodes` により、presence判定に使う `namecode` を明示できる
- `presence_value_mode` により、presence判定の方法を指定できる

現時点では、胸部X線のように `identity_item_code` だけでは判定範囲が広すぎる項目について、`required_presence_namecodes` による明示指定が重要と考えられる。

例：胸部X線では、一般の直接・間接の所見有無を表す `namecode` のみを presence 判定対象とし、所見テキスト、撮影日、撮影区分、がん検診系の項目は対象外とする想定である。

## 9. `exam_item_group_members` の役割

`exam_item_group_members` は、グループに所属する具体的な `namecode` と、その役割を保持する。

`role` の暫定理解は以下のとおりである。

| role | 暫定理解 |
| --- | --- |
| `RESULT_VALUE` | 結果値として扱う項目。値の存在により present 判定に寄与する可能性がある。 |
| `PRESENCE_KEY` | 実施有無や所見有無など、存在確認のキーとして扱う項目。 |
| `AUX` | 補助情報。原則として単独では present 判定に使わない想定。 |

ただし、v1.0.0実装において、`role` がどの程度厳密に使われているかは未精査である。

## 10. present判定の暫定モデル

現時点の理解では、present判定は以下のような流れと考えられる。

1. `LSIO_Legal_Item` の `required_flag = 1` の `identity_item_code` を取得する
2. 各 `identity_item_code` について、対応する `namecode` 群を確認する
3. XML内に判定対象の `namecode` が存在し、有効な値またはpresence条件を満たす場合、その `identity_item_code` を present とする
4. present となった `identity_item_code` 数を `lsio_legal_present_count` とする
5. `present_count == required_count` の場合、`lsio_legal_is_complete = 1` とする

ただし、`required_presence_namecodes` が指定されている場合は、その指定が優先される可能性が高い。

## 11. is_complete = 0 の扱い

`lsio_legal_is_complete = 0` の場合、少なくとも労基・法定健診の必須項目がすべて揃っているとは判断できない。

ただし、これを即座に「健診結果XMLとして不正」とは扱わない。

実務上は、以下を確認する必要がある。

- 不足項目数
- 不足している `identity_item_code`
- 不足理由がXML欠損なのか、判定ロジック未対応なのか
- 医療機関XML側のnamecode揺れなのか
- `exam_item_master` 側に必要なnamecodeが不足しているのか
- `role` / `required_presence_namecodes` / `presence_value_mode` の定義不足なのか

将来的には、`is_complete = 0` を単一のNGとして扱うのではなく、以下のような判定レベルに分ける。

| レベル | 暫定名 | 意味 | 想定アクション |
| --- | --- | --- | --- |
| `OK` | 受領可 | 必須項目が揃っている | 通常取り込み |
| `WARN` | 要確認 | 軽微な不足、または補助情報不足。業務判断で受領できる可能性がある | 内部確認、必要に応じて医療機関へ確認 |
| `REVIEW` | 精査対象 | 不足項目が複数あり、法定健診として成立するか判断が必要 | 担当者確認、定義不足の可能性も確認 |
| `REJECT_CANDIDATE` | 再提出候補 | 必須項目の欠落が大きい、またはXML構造上の不備がある | 医療機関へ再提出依頼を検討 |

このレベル判定は、v1.0.0時点では未実装とし、今後のルール拡充対象とする。

## 12. 将来的な受領時チェック方針

医療機関から健診結果XMLを受領した時点で、以下の観点をチェックできるようにする。

### 12.1 XMLファイルとしての基本チェック

- XMLとしてパース可能か
- HL7 CDAとして最低限の構造を持つか
- 想定外の文字化けや不正文字がないか
- 必須ヘッダー情報が取得できるか
- 健診実施日、受診者情報、医療機関情報が取得できるか

### 12.2 人物照合に必要な項目チェック

PHR側で人物を特定するため、以下のような情報が不足していないかを確認する。

- 保険者番号
- 保険証記号
- 保険証番号
- 氏名カナ
- 生年月日
- 性別

この領域の不足は、法定健診項目の不足とは別に、取り込み・照合不能リスクとして扱う。

### 12.3 労基・法定健診項目チェック

`LSIO_Legal_Item` を基準として、労基・法定健診として必要な項目がXML上に存在するかを確認する。

現時点では存在確認を主目的とするが、将来的には以下も区別する。

- 項目自体が存在しない
- 項目は存在するが値が空である
- `NullFlavor` や未実施理由が設定されている
- namecodeが想定外で、既存マスタでは判定できない
- 補助情報だけ存在し、presence判定に使う値が存在しない

### 12.4 再提出・確認アラート方針

チェック結果は、単にDBへ記録するだけでなく、医療機関への確認や再提出依頼の判断材料として使う。

将来的には、以下のような出力を想定する。

- XML単位の判定結果
- 不足している法定健診項目一覧
- 人物照合に必要な項目の不足一覧
- XML構造エラー一覧
- 再提出候補フラグ
- 医療機関確認候補フラグ
- 内部定義不足の可能性フラグ

ただし、再提出依頼を自動送信するのではなく、まずは担当者が確認できるアラート・一覧出力を目標とする。

## 13. 未精査事項

- `medi_~` 系スクリプトの実際の抽出処理
- `RESULT_VALUE` / `PRESENCE_KEY` / `AUX` の実装上の使い分け
- `required_presence_namecodes` が指定された場合の優先順位
- `presence_value_mode` の実装挙動
- `lsio_legal_missing_methods` の実際の格納形式
- `judge_*` 系カラムが完全未使用かどうか
- `required_count` が常に固定値か、定義変更により変動する前提か
- `identity_item_code` と `namecode` の紐づけをどのテーブルから取得しているか
- 医療機関XML内の値が空文字・NullFlavor・未実施理由の場合の扱い
- 法定健診チェック結果を業務判定としてどこまで使うか
- 受領時チェックとして、どの不足を `WARN` / `REVIEW` / `REJECT_CANDIDATE` に分類するか
- 人物照合に必要な項目不足と、法定健診項目不足をどのテーブル・カラムで分けて持つか
- 再提出候補アラートの保存先テーブルを新設するか、既存テーブルに判定結果を追加するか
- 医療機関ごとのnamecode揺れを、マスタ追加で吸収するか、医療機関へ修正依頼するか

## 14. 現時点の注意

このspecは、v1.0.0の実装を後から確認するための棚であり、現時点で確定仕様として扱わない。

特に、労基・法定健診チェックとしての業務判定に使う前に、実装、DDL、マスタ定義、実XMLサンプルを突合して再確認する必要がある。

今後はこの文書を、医療機関XML受領時チェックの仕様書として継続的に更新し、ゆるい存在チェックから、再提出・確認アラートに耐えられる検収ルールへ段階的に拡充する。