# 05_staging_subscribers_fund

## 目的

本ファイルは、`staging_subscribers_fund` の責務、見えている論点、今後決めるべき事項、およびDDL確認観点を整理するための暫定 spec である。

本内容は実装手順そのものではなく、年度更新運用の中で `staging_subscribers_fund` をどのような位置づけで扱うかを明確にすることを目的とする。

## 現時点で見えていること

### 0. 現在の実装状態

現時点の `staging_subscribers_fund` は、実質的にはテーブルが存在するのみであり、運用で想定する取り込み基盤としては未整備である。

- テーブル自体は作成済み
- 本来想定している「受領CSVをマッピングして raw として格納する」処理は未整備
- 既存の取り込みスクリプトは sqlite 版のみであり、今回の前提ではそのまま流用せず、新規設計前提で整理する

このため、本 spec では現行実装を前提に最適化するのではなく、`staging_subscribers_fund` を改めて新規設計する前提で責務と確認観点を整理する。

### 1. 現在の主な責務

現時点での `staging_subscribers_fund` の責務は、概ね以下と認識する。

- 健保から受領した加入者CSVの必要項目を raw として格納する
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
- 取り込み成功後、対象ファイルは `archive/<run_id>_<yyyymmdd_hhmmss>/<insurer_number>/` へ移動する
- `archive/` は実行単位でディレクトリを切り、人間が見て実行日時を判別できるよう日付を含める
- archive への移動処理は取り込みスクリプトへ組み込む
- アーカイブは再現性・監査用途のために保持する

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
- `required`（現時点では用途未整理）
- `notes`

### 3. 本 spec 上の位置づけ

今回の `staging_subscribers_fund` 設計では、テンプレートテーブルを前提として以下を整理対象に含める。

- `templates` / `template_mappings` を一次入力定義として扱うか
- `target_table=staging_subscribers_fund` のテンプレート運用をどう定義するか
- `rule` の責務を raw / norm / match 生成のどこまでに含めるか
- `required` の意味を明確化するか、別の扱いに整理するか

## raw / norm / match の考え方（現時点の仮置き）

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
- `insurance_number_match`
- `name_kana_full_match`
- `name_kanji_full_match`

### identity 系

以下を比較基盤として staging 取り込み時点で生成・保持する。

- `person_id_custom`
- `identity_hash`

identity 生成に必要な項目に欠損がある場合は、欠損を明示的にチェックしたうえで `NULL` とする。

## 現時点で決め切れていないこと

### 0. テンプレート取り込みの責務範囲

`staging_subscribers_fund` への取り込みにおいて、テンプレート定義をどこまで責務に含めるかは未確定である。

特に以下は整理が必要である。

- テンプレート定義により 1つのCSV列から複数の target_column を生成する運用をどこまで正式仕様として採用するか
- `rule` によって norm / match 生成のどこまでを担うか
- テンプレート定義とスクリプト実装の責務境界をどこで分けるか
- `required` を入力必須制御として使うのか、別用途なのか

### 1. 現行DDLに何があるか

まず、現行の `staging_subscribers_fund` DDL に以下が存在するか確認が必要である。

- norm 系カラム
- match 系カラム
- `person_id_custom`
- `identity_hash`
- 出所追跡用カラム（src系、run系）
- 照合結果保持カラム（matched系）
- identity 生成に必要な入力項目

### 2. raw / norm / match の範囲

どのカラムを raw / norm / match で保持するかは未確定である。

特に以下は整理が必要。

- insurer_number
- insurance_symbol
- insurance_number
- name_kanji_full / family / middle / given
- name_kana_full / family / middle / given
- birth
- gender_code
- relationship_code / relationship_name
- qualification_acquired_date / qualification_lost_date
- received_company_code / received_company_name

### 3. HIA登録補助として必要な最小カラム

本テーブルは HIA登録補助にも利用するため、比較用途だけでなく、HIA登録補助として最低限何が必要かを切り分ける必要がある。

### 4. subscribers 補完との責務境界

`staging_subscribers_fund` を使って `subscribers` の追加カラムを補完することは想定するが、補完結果そのものを `staging_subscribers_fund` に持たせるかどうかは別問題である。

現時点では、以下の方針を仮置きとする。

- `staging_subscribers_fund` は入力面
- `subscribers` 補完は別フェーズ
- 氏名分解などの完成形保持は `subscribers` 側で扱う

## 現時点の仮方針

現時点では、以下を仮方針とする。

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

現時点の候補:
- `received_company_code_norm`
- `received_company_name_norm`

`received_company_*_norm` は受領CSV由来の会社情報であり、HIA側で管理する会社コード・会社名とは別概念として扱う。

正規化方針:
- 空白除去は必須とする
- カナを含む場合は全角へ統一する
- それ以外の過剰な正規化は原則行わない

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

### 方針

- identity は staging 取り込み時点で生成する
- 生成に必要な材料は、原則として受領CSVおよび外部付与値から揃う前提で扱う
- identity 生成に必要な項目に欠損がある場合は、その欠損を明示的にチェックする
- 欠損ありのレコードについては、`person_id_custom` / `identity_hash` を `NULL` とする
- 欠損がないレコードについては、共通ルールに基づき identity を生成する
- identity 構成要素がすべて揃う場合は staging 取り込み時点で identity を生成する
- identity_hash 生成後、現行 subscribers に同一 identity が存在する場合は matched_subscriber_id へ保持する
- 欠損判定対象項目は以下の identity構成要素とする

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
- `matched_checked_at`
- `connect_id_norm`（健保の基幹システム側IDを保持する項目）

一方で、意味が曖昧な状態管理カラムは持たない方針とし、`processed_at` は削除対象とする。

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

## 現時点の確認結果（確認済み / 未確定）

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
- 受領CSVの配置構成は `input/<insurer_number>/` と `archive/<run_id>_<yyyymmdd_hhmmss>/<insurer_number>/` を基本とする
- archive への移動処理は取り込みスクリプトへ組み込む方針とする
- `received_company_*_norm` は受領CSV由来の会社情報であり、HIA管理上の会社情報とは別概念として扱う
- `matched_subscriber_id` は identity_hash 生成後の subscribers 照合結果として保持する方針とする
- `connect_id_norm` は健保の基幹システム側IDとして保持対象とする
- `processed_at` は意味が曖昧な状態管理カラムとして削除対象とする

### 未確定

- `rule` の責務範囲をどこまで認めるか
- `required` の意味を入力列必須・値必須のどちらで扱うか
- 現行DDLカラムの最終棚卸し結果（残す / rename / 再設計 / 削除）
- `received_company_*_norm` の最終カラム設計
- `name_kanji_full_match` / 分割漢字 match の最終カラム設計
- `staging_subscribers_fund` と `subscribers` の match 項目分担
- `connect_id_norm` / `matched_subscriber_id` / `matched_checked_at` の最終カラム設計
- input / archive の削除ポリシー・保持期間

本 spec では、上記の「確認済み」を前提として以降の設計を進め、「未確定」は次の更新で詰める論点として扱う。

## このファイルで次に詰めること

次の更新では、以下を具体化する。

- 現行DDLカラムの棚卸し結果（残す / rename / 再設計 / 削除）
- `rule` の責務範囲
- `required` の意味整理
- `input / archive ディレクトリ運用ルールの詳細（削除ポリシー・保持期間）`
- `subscribers` 補完フェーズへの受け渡し項目
- identity構成要素および欠損判定対象項目（確定済み）
- received_company_*_norm の確定
- name_kanji_full_match / 分割漢字 match の扱い確定
- staging_subscribers_fund と subscribers の match 項目分担
- connect_id_norm / matched_subscriber_id / matched_checked_at の最終カラム設計
- processed_at 削除のDDL反映
