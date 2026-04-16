# 05_staging_subscribers_fund

## 目的

本ファイルは、`staging_subscribers_fund` の責務、見えている論点、今後決めるべき事項、およびDDL確認観点を整理するための spec である。

本内容は実装手順そのものではなく、年度更新運用の中で `staging_subscribers_fund` をどのような位置づけで扱うかを明確にすることを目的とする。

## 現時点で見えていること

### 0. 現在の実装状態

現時点の `staging_subscribers_fund` は、実質的にはテーブルが存在するのみであり、運用で想定する取り込み基盤としては未整備である。

- テーブル自体は作成済み
- 本来想定している「受領CSVをマッピングして raw として格納する」処理は未整備
- 既存の取り込みスクリプトは sqlite 版のみであり、今回の前提ではそのまま流用せず、新規設計前提で整理する

このため、本 spec では現行実装を前提に最適化するのではなく、`staging_subscribers_fund` を改めて新規設計する前提で責務と確認観点を整理する。

### 1. 現在の主な責務

- 健保から受領した加入者CSVをテンプレート定義に基づいて staging 用カラムへ格納する
- 取り込み時にテンプレート定義に基づくマッピングを適用する
- HIA登録補助に利用するための基礎データを保持する
- 現状のHIA加入者情報（最新）や `subscribers` との比較土台として利用する
- 照合や比較に必要なキーを生成する
- `subscribers` 補完および年度比較の入力面を提供する

### 2. 加入者マスタそのものではない

`staging_subscribers_fund` は受領データの比較基盤であり、`subscribers` そのものではない。

そのため、現時点では以下は本テーブルの主責務としない。

- 業務上の最終判定結果の保持
- 転籍判定結果の保持
- 年度末状態の保持
- `subscribers` の完成形カラムをここで確定すること

### 3. 今回の運用で重要になる位置づけ

今回の年度更新運用では、`staging_subscribers_fund` は単なる取り込み先ではなく、以下の3用途を支える基盤として扱う。

- 健保受領CSVのテンプレートベース取り込み
- HIA登録補助
- `subscribers` 補完および年度比較の入力面

特に、2025年度最終状態と2026年度受領データの比較前提を整えるため、`staging_subscribers_fund` の責務を明文化する必要がある。

## 今回さらに追加で整理したいこと

今回の論点は、`staging_subscribers_fund` 自体を加入者マスタ化することではなく、受領データの比較基盤としてより明確に整備することである。

そのため、追加で整理したい内容は以下とする。

- norm / match を中心としたカラム設計を明確にする
- `person_id_custom` / `identity_hash` の staging 取り込み時生成方針を明確にする
- `subscribers` 補完に使う入力面として必要なカラムを整理する
- HIA登録補助として必要な最小カラムと、fund共通項目の扱いを整理する

## テンプレートベース取り込みの前提

`staging_subscribers_fund` への取り込みは、健保ごとのテンプレート定義に基づいて行う前提とする。

### 1. 基本方針

- 健保ごとに CSV マッピング用テンプレートを登録する
- テンプレート登録はマニュアルで行う
- 受領CSVは、テンプレート定義に従って `staging_subscribers_fund` の各カラムへマッピングして取り込む
- 取り込み対象CSVは、以下の配置構成に従う

```text
/data/from_fund/import_subscribers_staging/
  ├── input/<insurer_number>/*.csv
  └── archive/<run_id>_<yyyymmdd_hhmmss>/<insurer_number>/*.csv
```

- `input/<insurer_number>/` 配下に配置されたCSVを取り込み対象とする
- CSV自体がエラーの場合は `input/` に残す
- run が正常に走り、読み込み判定まで到達したファイルは `archive/<run_id>_<yyyymmdd_hhmmss>/<insurer_number>/` へ移動する
- `archive/` は実行単位でディレクトリを切り、人間が見て実行日時を判別できるよう日付を含める
- archive への移動処理は取り込みスクリプトへ組み込む
- アーカイブは再現性・監査用途のために保持する
- `archive/` の削除は自動化せず、手動運用とする

### 2. 現在把握しているテンプレート関連テーブル

#### `templates`

健保ごとのテンプレート定義のヘッダ情報を保持する。

現時点の把握内容:
- `fund_id` に紐づく
- `version` を持つ
- `template_type` を持つ
- `target_table` を持つ

#### `template_mappings`

テンプレートごとのマッピング定義を保持する。

現時点の把握内容:
- `fund_id`
- `version`
- `col_order`
- `csv_header`
- `target_column`
- `rule`
- `required`（値必須フラグ）
- `notes`

### 3. 本 spec 上の位置づけ

今回の `staging_subscribers_fund` 設計では、テンプレートテーブルを前提として以下を整理対象に含める。

- `templates` / `template_mappings` を一次入力定義として扱うか
- `target_table=staging_subscribers_fund` のテンプレート運用をどう定義するか
- `rule` の責務を raw / norm / match 生成のどこまでに含めるか
- `required` はテンプレート単位の値必須フラグとして扱う

## raw / norm / match の考え方

### rule の位置づけ（今回の設計整理）

今回の設計では、`rule` は従来のような「汎用的な変換ロジック指定」ではなく、`target_column` の責務に従属する変換指定として再定義する。

### 基本定義

`rule` は、CSV入力値を `target_column` の責務に応じた値へ変換するための限定的な変換指定とする。

- `rule` は `target_column` の種別（norm / match / 補助列）に従属する
- `rule` は単一入力列に対する変換を対象とする
- 1つのCSV列から複数の `target_column` を生成することは許容する

### rule の責務範囲

#### 許容する範囲

- 単一列からの norm 生成
- 単一列からの match 生成
- 単一列からの派生値（分解・補助列）の生成

#### 許容しない範囲

- 複数列をまたぐ値の組み立て
- `person_id_custom` / `identity_hash` の生成
- `subscribers` との照合（`matched_*` 系）
- 業務判定（転籍・資格状態など）

これらはすべてスクリプト側の責務とする。

### rule の分類

#### norm 用 rule

`*_norm` カラムへ値を生成するためのルール。

例:
- `symbol_norm`
- `birth_norm`
- `gender_code_norm`
- `date_or_null`
- `digits_required`

記号に関しては、identity 生成に必要な数字成分を抽出できることを rule 側で担保する。たとえば `神-01` は許容し、数字成分を持たない `神` はエラーとする。

#### match 用 rule

`*_match` カラムへ値を生成するためのルール。

例:
- `insurance_symbol_match`
- `insurance_number_match`
- `name_kana_full_match`
- `name_kanji_full_match`

※ match 系は比較用途であり、norm とは別目的で生成する

#### 補助列用 rule

分解や人手確認補助のための列を生成するルール。

例:
- `symbol_digits`
- `split_family`
- `split_middle`
- `split_given`
- `split_family_kana`
- `split_given_kana`

### 設計上の重要ポイント

- `rule` は独立した変換ロジック体系ではなく、カラム責務に従属する
- カラム名（`*_norm` / `*_match`）だけで値の意味が分かる設計を優先する
- 複雑な条件分岐や業務ロジックは `rule` に持ち込まない
- match ロジックは共通ライブラリで一元管理し、`rule` はその呼び出し指定とする
- `required` は値の存在チェックを担い、identity 生成に必要な形式要件は `rule` 側で担保する

本整理により、テンプレートは「どのCSV列をどの責務カラムへどの種別の rule で入れるか」を定義する役割に限定する。

### raw

本テーブルは raw データの保持を主目的としない。

受領原本そのものの保持は、入力CSV、archive、ETL実行記録で担保する前提とし、`staging_subscribers_fund` では raw を大量に常設カラムとして保持しない。

ただし、出所追跡や行特定に必要な以下の入力起点情報は保持する。

- `src_file`
- `src_row_no`
- `src_line_no`

### norm

`*_norm` は本テーブルにおける主値であり、登録・更新・比較の基準値として扱う。

ここでいう norm は、照合専用の潰し込みではなく、各項目を登録に適した形へ整えた値を指す。

例:
- `insurer_number_norm`
- `insurance_symbol_norm`
- `insurance_number_norm`
- `name_kana_full_norm`
- `name_kanji_full_norm`
- `birth_norm`
- `gender_code_norm`

### match

`*_match` は照合・比較のための補助値であり、比較判定の根拠として用いる。

例:
- `insurance_symbol_match`
※ `insurance_symbol_match` は照合用の列であり、数字抽出を保持する `insurance_symbol_digits` とは別に扱う。
- `insurance_number_match`
- `name_kana_full_match`
- `name_kanji_full_match`

### identity 系

以下を比較基盤として staging 取り込み時点で生成・保持する。

- `person_id_custom`
- `identity_hash`

identity 生成に必要な項目に欠損がある場合は、欠損を明示的にチェックしたうえで `NULL` とする。

## 現時点で確定した設計方針

### 0. テンプレート取り込みの責務範囲

`staging_subscribers_fund` への取り込みにおけるテンプレート定義の責務は、以下で確定とする。

- テンプレート定義は、1つのCSV列から複数の `target_column` を生成する運用を許容する
- `rule` は単一入力列に対する norm / match / 補助列生成までを担う
- テンプレート定義とスクリプト実装の責務境界は、`rule` による単一列変換までをテンプレート側、identity 生成・subscribers 照合・業務判定をスクリプト側とする
- `required` はテンプレート単位の値必須制御として扱う

### 1. 現行DDL確認の位置づけ

現行の `staging_subscribers_fund` DDL は確認済みであり、本 spec では以下を確認済み前提とする。

- norm 系カラム
- match 系カラム
- `person_id_custom`
- 出所追跡用カラム（src系、run系）
- 照合結果保持カラム（matched系）
- identity 生成に必要な入力項目


`identity_hash` は現行DDLに存在しないため、新DDLで追加する前提とする。

現行DDLとの乖離は大きく、実データも未投入であるため、`staging_subscribers_fund` は ALTER ベースではなく DROP + CREATE 前提で再作成する方針とする。

### 2. raw / norm / match の範囲

本テーブルの保持方針は以下で確定とする。

- raw は大量常設しない
- 主値は `*_norm` とする
- 照合用補助値は `*_match` とする
- 出所追跡は `src_*` 系で担保する
- `insurance_symbol_digits` は match とは別に、人手確認・運用補助および person_id_custom 生成前提の数字成分確認列として維持する

### 3. HIA登録補助として必要な最小カラム

HIA登録補助として、少なくとも以下の系統の項目を staging 上で保持する。

- 保険情報（`insurer_number_norm` / `insurance_symbol_norm` / `insurance_number_norm`）
- 氏名情報（`name_kana_full_norm` / `name_kanji_full_norm`）
- 基本属性（`birth_norm` / `gender_code_norm`）
- 会社情報（`received_company_code_norm` / `received_company_name_norm`）
- 続柄情報（`relationship_code_norm` / `relationship_name_norm`）

### 4. subscribers 補完との責務境界

`staging_subscribers_fund` と `subscribers` の責務境界は以下で確定とする。

- `staging_subscribers_fund` は入力面および比較基盤とする
- `subscribers` 補完は別フェーズで行う
- 氏名分解の完成形保持は `subscribers` 側で扱う
- staging 側では比較および補完判断に必要な norm / match / identity / 続柄情報を保持する

## 確定方針

以下を確定方針とする。

- `staging_subscribers_fund` は受領データの比較基盤として扱う
- 健保受領CSVはテンプレート定義に基づいて取り込む
- `staging_subscribers_fund` は raw データの保持を主目的としない
- 保持する主値は norm とし、登録・更新・比較の基準値として扱う
- 照合に必要な項目のみ match を持つ
- raw / norm / match のサフィックスをカラム名へ付与し、値の意味がカラム名だけで判別できるようにする
- `person_id_custom` / `identity_hash` は比較基盤として staging 取り込み時点で生成する
- identity 生成に必要な項目に欠損があるレコードは、欠損を明示的にチェックしたうえで NULL とする
- テンプレート登録はマニュアル運用を前提とする
- 氏名分解や業務上の最終判定は `staging_subscribers_fund` の責務に含めない
- fund共通テーブルであるため、個別健保の受領ヘッダーに存在しないことのみを理由としてテーブル項目を削除しない
- 項目の利用有無は各健保のテンプレート定義によって制御する
- matched_subscriber_id は identity_hash 生成後の subscribers 照合結果として保持する

## 基本方針

### 2.4 match の位置づけ

照合用に match を持つ。
match は比較判定の根拠として用いる。

### 2.5 identity の位置づけ
`person_id_custom` および `identity_hash` は、`staging_subscribers_fund` への取り込み時点で生成する。

本運用では、加入者CSVの取り込み段階で identity 生成に必要な主な材料が揃う前提で扱う。
ただし、identity 生成に必要な項目に欠損がある場合は、欠損のみを明示的にチェックしたうえで、当該レコードの `person_id_custom` / `identity_hash` は `NULL` とする。

### 2.6 会社情報の位置づけ
会社情報は HIA 側で管理する企業情報とは別概念として扱う。

受領CSV由来の会社情報は、受取情報であることが分かる接頭語を用いた `received_*_norm` カラムとして保持する。

保持項目:
- `received_company_code_norm`
- `received_company_name_norm`

`received_company_*_norm` は受領CSV由来の会社情報であり、HIA側で管理する会社コード・会社名とは別概念として扱う。

HIA の加入者登録では事業所コードが必要となるため、受領CSV由来の会社情報は staging 上で保持対象とする。受領CSVに事業所情報が存在しないケースはありうるが、項目自体は fund 共通項目として持つ前提とする。

正規化方針:
- 空白除去は必須とする
- カナを含む場合は全角へ統一する
- それ以外の過剰な正規化は原則行わない

#### 受領ヘッダーとの対応（今回健保）

- `事業所コード` → `received_company_code_norm`
- `事業所` → `received_company_name_norm`

#### HIA 事業所コードとの関係

- `received_company_code_norm` / `received_company_name_norm` は受領CSV由来の会社情報である
- HIA 側で管理する事業所コードとは別項目として扱う
- 加入者登録時には、受領側の会社情報と HIA 側事業所コードの対応付けを行ったうえで使用する

### 2.7 required の位置づけ
`required` は、CSV入力値に対する必須チェック（NULL / 空値不可）を表すフラグとする。

- `required` は値の存在を確認する
- 値の形式チェックや変換は `rule` の責務とする
- identity 生成に必要な形式要件は `rule` 側で担保する

たとえば記号については、`required=1` で値の存在を確認しつつ、`rule` により数字成分が抽出可能であることを要求する。

### 2.7.1 required 適用方針（今回健保）

今回の健保テンプレートにおける `required` は、以下を適用方針として確定する。

#### required=1（値必須とする項目）

- `insurance_symbol_norm`
- `insurance_number_norm`
- `birth_norm`
- `gender_code_norm`
- `name_kana_full_norm`

これらは、加入者識別および比較の最低限成立に必要な項目として扱う。

#### 条件付きで required=1 を検討する項目

- `name_kanji_full_norm`
- `relationship_code_norm`

これらは以下の観点で運用に応じて必須化を検討する。

- `name_kanji_full_norm`: 目視確認および比較補助の重要性が高い場合
- `relationship_code_norm`: 本人 / 本人以外判別を安定させたい場合

#### required=0（任意項目）

上記以外の項目は、値が存在すれば利用するが、欠損を理由に取り込み全体をエラーとはしない。

#### 補足（システム側必須項目）

- `insurer_number_norm` は CSV 由来ではなく外部付与値であるため、テンプレートの `required` ではなくシステム側必須項目として扱う。

#### identity との関係

- `required` はテンプレート単位の値必須制御であり、identity 生成可否とは別概念とする
- identity 構成要素に該当する項目が欠損した場合は、`required` の設定に関わらず identity は生成せず `NULL` とする

## 8. identity生成

以下は `staging_subscribers_fund` への取り込み時点で生成する。

- `person_id_custom`
- `identity_hash`

### identity構成要素（欠損判定対象）

以下の項目を identity の構成要素とし、同時に欠損判定対象項目とする。

- `insurer_number_norm`
- `insurance_symbol_norm`
- `insurance_number_norm`
- `birth_norm`
- `name_kana_full_match`
- `gender_code_norm`

続柄情報（本人 / 本人以外の判別）は identity 構成要素には含めない。続柄は個人同定ではなく資格・関係属性として扱い、別途判別項目として保持・利用する。


### 方針

- identity は staging 取り込み時点で生成する
- 生成に必要な材料は、原則として受領CSVおよび外部付与値から揃う前提で扱う
- identity 生成に必要な項目に欠損がある場合は、その欠損を明示的にチェックする
- 欠損ありのレコードについては、`person_id_custom` / `identity_hash` を `NULL` とする
- 欠損がないレコードについては、共通ルールに基づき identity を生成する
- identity 構成要素がすべて揃う場合は staging 取り込み時点で identity を生成する
- identity_hash 生成後、レコード生成処理の最後に現行 subscribers と照合し、同一 identity が存在する場合は `matched_subscriber_id` へ保持する
- 欠損判定対象項目は以下の identity構成要素とする


## 8.5 本人 / 本人以外判別

本人 / 本人以外の判別は identity とは別軸で扱う。

### 基本方針

- `identity_hash` は個人同定を目的とし、続柄情報は構成要素に含めない
- 本人 / 本人以外の判別は、`relationship_code_norm` または `relationship_name_norm` を用いて別途行う
- 判別に使う主軸は、健保ごとの値体系で本人かどうかが明確に判断できる方を優先する

### 確定方針

- 第一候補は `relationship_code_norm` とする
- コードのみで本人判定が明確でない場合は `relationship_name_norm` を補助的に用いる
- staging では、判別に必要な元情報として `relationship_code_norm` / `relationship_name_norm` を保持する
- 判別ロジックは「コード優先、名称補助」で確定とする

### 設計上の意図

続柄は資格・関係属性であり、個人同定そのものとは別である。
そのため、同一人物で続柄表記やコード体系が変わる可能性を考慮し、identity 構成要素へは含めない。

一方で、年度比較や `subscribers` 補完の際には、本人 / 本人以外の判別は重要な補助情報となるため、別途保持・利用する。




## 8.6 match 項目の分担

比較・照合に必要な match 項目は、`staging_subscribers_fund` と `subscribers` の両方に持つ前提とする。

### 基本方針

- `staging_subscribers_fund` は受領データの入力面・比較面として match を保持する
- `subscribers` は最終保持面・継続参照面として match を保持する
- 役割は異なるが、比較に必要な match 項目は両テーブルで揃える方針とする

### 揃える対象項目

- `name_kana_full_match`
- `name_kanji_full_match`
- `name_kanji_family_match`
- `name_kanji_middle_match`
- `name_kanji_given_match`
- `insurance_symbol_match`
- `insurance_number_match`

### 補足

- `staging_subscribers_fund` では取り込み時点の比較・補完判断のために保持する
- `subscribers` では最終保持面として継続利用するために保持する
- 分担は「どちらか片方だけに持つ」のではなく、「同じ match 項目を両方に持ち、用途で役割を分ける」とする

## 8.7 matched_subscriber_id の位置づけ

`matched_subscriber_id` は、identity_hash 生成後、レコード生成処理の最後に `subscribers` と照合して得た一致先の `subscribers.id` を保持する項目とする。

### 基本方針

- `matched_subscriber_id` は保持する
- 業務上の最終判定結果は保持しない
- 照合結果キャッシュとして扱う

## 9. テンプレート連携

- `rule` に応じて norm / match を生成し、必要な材料が揃う場合は identity も生成する
- 現行 template_mappings の実データでは、1つのCSV列から複数の target_column を生成する定義を許容している
- 項目がテーブルに存在しても、各健保テンプレートで未使用のままとすることを許容する

## 9.5 staging 固有の保持項目

以下の項目は、raw 保持ではなく staging 運用上の追跡・照合のために保持する。

- `src_file`
- `src_row_no`
- `src_line_no`
- `import_run_id`
- `loaded_at`
- `matched_subscriber_id`
- `connect_id_norm`（健保の基幹システム側IDを保持する項目）

一方で、意味が曖昧な状態管理カラムは持たない方針とし、`processed_at` および `matched_checked_at` は削除対象とする。

## 🔥 今回の仕様の一文まとめ

> 本テーブルは raw データの保持を主目的とせず、  
> 正規化された登録基準値（norm）・照合用値（match）・生成可能な identity を保持することで、  
> 加入者情報の登録・比較・更新処理の精度を担保する。

## 関連 spec

- `README.md`
- `01_overview.md`
- `06_subscriber_enrichment.md`
- `docs/spec/hia_export_subscribers_csv/staging_schema.md`
- `docs/spec/hia_export_subscribers_csv/identity_policy.md`
- `docs/spec/hia_export_subscribers_csv/subscriber_apply.md`
- `docs/spec/identity_canonicalization/identity_layers_norm_and_purpose.md`
- `docs/spec/common/db_connection.md`
- テンプレート関連テーブル定義（別途一次情報確認）


## 現行DDLカラムの棚卸し（旧→新方針対応）

本節は、現行 `staging_subscribers_fund` DDL の各カラムを、現在の設計方針に照らしてどのように扱うかを整理するための基礎表である。

区分は以下の意味で用いる。

- `維持`: 現行カラムをそのまま維持する
- `rename`: 命名規則（主に `_norm` / `_match`）へ寄せて引き継ぐ
- `追加`: 現行DDLに存在しないが新DDLで追加する
- `削除`: staging の責務から外す

### 1. 主キー・テンプレート・実行管理

| 現行カラム | 区分 | 新方針 / コメント |
|---|---|---|
| `id` | 維持 | 主キーとして維持 |
| `fund_id` | 維持 | 健保単位の取り込み基盤として維持 |
| `template_ver` | rename | `template_version` へ rename（命名一貫性のため確定） |
| `import_run_id` | 維持 | ETL run / archive と連動するため維持 |
| `created_at` | 維持 | 作成時刻として維持 |
| `loaded_at` | 維持 | `created_at` とズレうる取り込み完了時刻として維持（確定） |

### 2. identity・照合結果

| 現行カラム | 区分 | 新方針 / コメント |
|---|---|---|
| `person_id_custom` | 維持 | staging 取り込み時点で生成・保持 |
| `identity_hash` | 追加 | 現行DDLに存在しないため追加 |
| `matched_subscriber_id` | 維持 | identity_hash 生成後、レコード生成処理の最後に行う subscribers 照合結果として保持 |
| `matched_checked_at` | 削除 | 照合はレコード生成処理の最後に実行するため、`created_at` と実質的に重複しやすく削除対象 |
| `processed_at` | 削除 | 意味が曖昧な状態管理カラムのため削除対象 |

### 3. 氏名（カナ）

| 現行カラム | 区分 | 新方針 / コメント |
|---|---|---|
| `name_kana_full` | rename | `name_kana_full_norm` |
| `name_kana_family` | rename | `name_kana_family_norm` |
| `name_kana_middle` | rename | `name_kana_middle_norm` |
| `name_kana_given` | rename | `name_kana_given_norm` |
| `name_kana_full_match` | 追加 | identity構成要素として追加 |

### 4. 氏名（漢字）

| 現行カラム | 区分 | 新方針 / コメント |
|---|---|---|
| `name_kanji_full` | rename | `name_kanji_full_norm` |
| `name_kanji_family` | rename | `name_kanji_family_norm` |
| `name_kanji_middle` | rename | `name_kanji_middle_norm` |
| `name_kanji_given` | rename | `name_kanji_given_norm` |
| `name_kanji_full_match` | 追加 | 共通ライブラリで生成し、比較判定の根拠として使用 |
| `name_kanji_family_match` | 追加 | 分割後に共通ライブラリを適用して生成 |
| `name_kanji_middle_match` | 追加 | 分割後に共通ライブラリを適用して生成 |
| `name_kanji_given_match` | 追加 | 分割後に共通ライブラリを適用して生成 |

### 5. 基本属性

| 現行カラム | 区分 | 新方針 / コメント |
|---|---|---|
| `gender_code` | rename | `gender_code_norm` |
| `birth` | rename | `birth_norm` |
| `relationship_code` | rename | `relationship_code_norm` |
| `relationship_name` | rename | `relationship_name_norm` |

### 6. 保険情報

| 現行カラム | 区分 | 新方針 / コメント |
|---|---|---|
| `insurer_number` | rename | `insurer_number_norm` |
| `insurance_symbol` | rename | `insurance_symbol_norm` |
| `insurance_symbol_digits` | 維持 | 人手確認・運用補助に加え、person_id_custom 生成前提の数字成分確認列として維持 |
| `insurance_symbol_match` | 追加 | `insurance_symbol_digits` とは別に、照合用の match 列として追加 |
| `insurance_number` | rename | `insurance_number_norm` |
| `insurance_branchnumber` | rename | `insurance_branchnumber_norm` |
| `insurance_number_match` | 追加 | 照合用として追加 |

### 7. 資格・住所・連絡先

| 現行カラム | 区分 | 新方針 / コメント |
|---|---|---|
| `qualification_acquired_date` | rename | `qualification_acquired_date_norm` |
| `qualification_lost_date` | rename | `qualification_lost_date_norm` |
| `postal_code` | rename | `postal_code_norm` |
| `address_line` | rename | `address_line_norm` |
| `building` | rename | `building_norm` |
| `phone` | rename | `phone_norm` |
| `email` | rename | `email_norm` |

### 8. 会社・組織・外部ID

| 現行カラム | 区分 | 新方針 / コメント |
|---|---|---|
| `employer_code` | rename | `received_company_code_norm` へ rename（受領会社コードとして扱うため命名変更） |
| `department_code` | 維持 | 個別健保で未使用でも fund共通項目として維持 |
| `distribution_code` | 維持 | 個別健保で未使用でも fund共通項目として維持 |
| `employee_code` | 維持 | 個別健保で未使用でも fund共通項目として維持 |
| `connect_id` | rename | `connect_id_norm` として保持確定 |
| `received_company_name_norm` | 追加 | 受領CSV由来の会社名として追加確定 |


### 9. 出所追跡

| 現行カラム | 区分 | 新方針 / コメント |
|---|---|---|
| `src_file` | 維持 | 入力起点追跡用として維持 |
| `src_row_no` | 維持 | 行特定用として維持 |
| `src_line_no` | 維持 | 行特定用として維持 |

### 10. この棚卸し表の位置づけ

本表は、新DDLを起こすための基礎表として扱う。最終的な rename / 追加 / 維持 / 削除の確定は、次段のDDL設計で反映する。

次の更新では、本表をもとに以下を詰める。

- `template_mappings.target_column` の更新方針
- 新DDLへの落とし込み

## 現時点の確認結果

### 確認済み

- `staging_subscribers_fund` は現時点ではテーブルのみ存在し、取り込み基盤としては未整備である
- sqlite 版の取り込み実装が存在し、テンプレートベース取り込みの思想自体は既存資産に存在する
- `templates` / `template_mappings` テーブルが存在し、`fund_id + version` 単位でテンプレートを管理している
- `template_mappings` では、1つのCSV列から複数の `target_column` を生成する実データが存在する
- `staging_subscribers_fund` は raw 保持を主目的とせず、norm を主値、match を照合補助値として扱う方針とする
- `person_id_custom` / `identity_hash` は staging 取り込み時点で生成する方針とする
- identity 構成要素および欠損判定対象項目は以下で確定済みとする
  - `insurer_number_norm`
  - `insurance_symbol_norm`
  - `insurance_number_norm`
  - `birth_norm`
  - `name_kana_full_match`
  - `gender_code_norm`
- 本人 / 本人以外の判別は identity とは別軸で扱い、`relationship_code_norm` / `relationship_name_norm` により別途行う方針とする
- 受領CSVの配置構成は `input/<insurer_number>/` と `archive/<run_id>_<yyyymmdd_hhmmss>/<insurer_number>/` を基本とする
- archive への移動処理は取り込みスクリプトへ組み込む方針とする
- `received_company_code_norm` / `received_company_name_norm` は fund 共通項目として保持する方針とする
- 受領CSV由来の会社情報は HIA 側事業所コードとは別概念として扱い、加入者登録時に対応付けて使用する
- `matched_subscriber_id` は identity_hash 生成後、レコード生成処理の最後に行う subscribers 照合結果を保持する照合結果キャッシュとして扱う方針とする
- `matched_checked_at` は `created_at` と実質的に重複しやすいため削除対象とする
- `connect_id_norm` は保持候補ではなく保持確定とする
- `processed_at` は意味が曖昧な状態管理カラムとして削除対象とする
- `insurance_symbol_digits` は人手確認・運用補助に加え、person_id_custom 生成前提の数字成分確認列として維持し、照合用の `insurance_symbol_match` は別列として扱う
- `required` はテンプレート単位の値必須フラグとして扱う方針とする
- `rule` は単一入力列に対する norm / match / 補助列生成までを担う方針とする
- 本人 / 本人以外判別は「コード優先、名称補助」で行う方針とする
- `department_code` / `distribution_code` / `employee_code` は fund 共通項目として維持する方針とする
- `name_kanji_full_match` / `name_kanji_family_match` / `name_kanji_middle_match` / `name_kanji_given_match` は保持する方針とする
- 比較に必要な match 項目は `staging_subscribers_fund` と `subscribers` の両方に持つ前提とする
- `matched_subscriber_id` は照合結果キャッシュとして保持する方針とする
- CSV自体がエラーの場合は `input/` に残し、run が正常に走って読み込み判定まで到達したファイルは `archive/` へ移動する方針とする
- `archive/` は自動削除せず、手動運用とする
- 現行DDLカラムの棚卸し方針（維持 / rename / 追加 / 削除）は確定済み

- `staging_subscribers_fund` は現行DDLとの乖離が大きく、実データも存在しないため、DROP + CREATE 前提で再作成する方針とする

### 未実施

- 本 spec の内容をもとに新DDLへ落とし込む
- 会社 Ubuntu 環境側の現行DDLと最終突合を行う

## このファイルで次に行うこと

次の更新では、本 spec の内容をもとに新DDLへの落とし込みを行う。
