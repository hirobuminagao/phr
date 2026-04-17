# 07_import_staging_subscribers_fund

## 目的

本specは、健保受領CSVを `staging_subscribers_fund` へ取り込む import スクリプトの責務と処理方針を定義する。

本処理の目的は以下とする。

- 健保受領CSVを、テンプレート定義に基づいて `staging_subscribers_fund` の norm / 補助列へ格納する
- `insurer_number_norm` など、CSV外部から与えるべき値をスクリプト側で補完する
- comparison 前段として利用できる staging 基盤を安定して作る

---

## 本specの位置づけ

本specは、`05_staging_subscribers_fund.md` で定義したカラム設計を前提に、import スクリプトがどのように値を生成・投入するかを定義する。

- 05 は「何を持つか」の spec
- 07 は「どう入れるか」の spec

そのため、本specでは DDL 設計そのものは扱わず、取り込み手順・rule 実行方針・外部付与値の扱いを中心に定義する。

---

## スクリプトの責務

本スクリプトの責務は以下とする。

- `input/<insurer_number>/` 配下のCSVを取得し、CSV読込は共通ライブラリ `csv_loader` を利用する
- フォルダ名から `insurer_number` を取得する
- `fund_insurer_number` を参照して `fund_id` を解決する
- `templates` / `template_mappings` から適用テンプレートを解決する
- CSV各列に対して mapping / rule を適用する
- `staging_subscribers_fund` に INSERT する
- `person_id_custom` / `identity_hash` / `matched_subscriber_id` を生成する
- 正常終了時は対象CSVを `archive/` へ移動する

以下は本スクリプトの責務外とする。

- `subscribers` への反映
- 差分判定
- 喪失 / 転籍 / 業務判定

---

## 入力

### 1. CSV配置

取り込み対象CSVは以下の構成で配置される。

```text
/data/from_fund/import_subscribers_staging/
  ├── input/<insurer_number>/*.csv
  └── archive/<run_id>_<yyyymmdd_hhmmss>/<insurer_number>/*.csv
```

- `input/<insurer_number>/` 配下のCSVを対象とする
- `insurer_number` はフォルダ名から取得する

### 1.5 CSVヘッダー処理

CSVのヘッダー処理は、個別実装せず、共通ライブラリ `csv_loader` を利用する。

- 実装配置先は `scripts/lib/csv/csv_loader.py` とする
- BOM除去
- delimiter 処理
- `header_count` / `disp_mode` に応じたヘッダー読込
- `key_headers` / `disp_headers` / `header_index_map` の生成
- 行数カウント（オプション）

これらは `csv_loader` の責務とし、本スクリプト側では再実装しない。

### 2. テンプレート定義

以下を参照して mapping を解決する。

- `templates`
- `template_mappings`

解決キーは以下とする。

- `fund_id`
- `version`

### 3. 外部付与値

CSVに存在しないが staging 生成に必要な値は、スクリプト側で補完する。

代表例:

- `insurer_number_norm`
  - CSVではなく、`input/<insurer_number>/` のフォルダ名から取得した insurer_number を使う
  - `fund_insurer_number` と `fund_id` の対応を前提に解決する

---

## 出力

出力先は `staging_subscribers_fund` とする。

- 主値は `*_norm`
- 照合補助は `*_match`
- 補助列は `insurance_symbol_digits` 等
- 出所追跡は `src_*`
- identity 系は `person_id_custom` / `identity_hash`

---

## 基本フロー

1. `input/<insurer_number>/` から対象CSVを取得する
2. フォルダ名から `insurer_number` を取得する
3. 共通ライブラリ `csv_loader` を使ってCSVを開き、ヘッダー・encoding・row iterator を取得する
4. `fund_insurer_number` から `fund_id` を解決する
5. `templates` から適用テンプレート（`fund_id + version`）を取得する
6. `template_mappings` を読み込む
7. CSVを1行ずつ処理する
8. mapping に従って norm / 補助列を生成する
9. スクリプト側で `insurer_number_norm` を注入する
10. スクリプト側で `name_kana_full_match` / `name_kanji_*_match` / `insurance_*_match` / `relationship_name_match` を生成する
11. スクリプト側で `person_id_custom` / `identity_hash` を生成する
12. `subscribers` を照合し、`matched_subscriber_id` を付与する
13. `staging_subscribers_fund` に INSERT する
14. 正常終了時、CSVを `archive/` へ移動する

---

## rule の実行方針

### 基本方針

- `template_mappings.rule` は単一列変換のみを担う
- rule は norm / 補助列の生成までを担当する
- identity 生成、subscribers 照合、業務判定はスクリプト側で行う
- 変換処理は可能な限り共通ライブラリを優先利用する
- 本体スクリプトに変換ロジックを直接持たせるのは暫定対応に留める
- 共通利用できる変換は `scripts/lib/` 配下へ寄せる
- 個別処理が必要な変換は `scripts/from_fund/` 配下の個別ライブラリへ分離する

### rule で扱うもの

例:

- `symbol_norm`
- `symbol_digits`
- `digits_required`
- `digits_or_null`
- `birth_norm`
- `gender_code_norm`
- `date_or_null`
- `as_is`
- `split_family`
- `split_middle`
- `split_given`
- `split_family_kana`
- `split_middle_kana`
- `split_given_kana`
- `kana_full_no_space`

### rule で扱わないもの

- `insurer_number_norm` の注入
- `name_kana_full_match` の生成
- `name_kanji_*_match` の生成
- `insurance_symbol_match` / `insurance_number_match` の生成
- `relationship_name_match` の生成
- `person_id_custom` / `identity_hash` の生成
- `matched_subscriber_id` の生成

これらはすべてスクリプト側の責務とする。

### 共通ライブラリ参照方針

変換処理の実装にあたっては、既存の共通ライブラリを優先的に確認し、流用可能なものは本体スクリプトへ再実装しない方針とする。

現時点で参照候補とする共通ライブラリ例:

- `scripts/lib/csv/csv_loader.py`
- `scripts/lib/db/lookup/fund.py`
- `scripts/lib/identity/field/name_kana.py`
- `scripts/lib/identity/field/birthdate.py`
- `scripts/lib/identity/field/gender_code.py`

本specでは、特に `name_kana` / `birthdate` の変換について、既存 identity ライブラリの流用可否を確認対象とする。

---

## 氏名処理方針

氏名（カナ）の変換については、`scripts/lib/identity/field/name_kana.py` の `normalize_name_kana_full()` を共通利用候補として確認する。

### 1. 基本方針

氏名（漢字・カナ）の分解は、rule 名ごとに別実装せず、スクリプト側の共通分解関数で処理する。

- rule は「どの要素を返すか」の指定に留める
- 分解ロジック本体は共通関数で一元管理する

### 2. 分解前処理

氏名分解の前に、以下を行う。

1. 全角化
2. 前後の余分な空白除去
3. 半角スペースを全角スペースへ統一
4. 連続スペースを1つに正規化

ただし、実装時は本ロジックを本体へ直接記述する前に、既存の共通ライブラリで同等責務を持つ関数がないかを確認する。

### 3. 分解ルール

氏名は全角スペースで split し、以下の通り割り当てる。

- 一番左を `family`
- 一番右を `given`
- 残りを `middle`

要素数ごとの扱い:

- 1要素
  - `family` = 値全体
  - `middle` = NULL
  - `given` = NULL

- 2要素
  - `family` = 先頭
  - `middle` = NULL
  - `given` = 末尾

- 3要素以上
  - `family` = 先頭
  - `given` = 末尾
  - `middle` = 2番目〜末尾手前を全角スペースで再結合

### 4. 氏名カナ match の生成

- `name_kana_full_match` は必ず `name_kana_full_norm` から生成する
- 分割カナ（family / middle / given）から match を直接生成しない

### 5. フル・分割の優先

- フルのみ存在する場合
  - `name_*_full_norm` を生成し、必要に応じて分解関数で family / middle / given を補完する

- 分割のみ存在する場合
  - family / middle / given を全角スペースで結合して `name_*_full_norm` を生成する

- フル・分割両方存在する場合
  - フルを優先する
  - 分割は補助情報として保持する

---

## 電話番号・メールの扱い

### 電話番号

今回案件では、以下の2列が来る場合がある。

- `個人電話番号`
- `個人携帯電話番号`

どちらも `phone_norm` の候補となるが、優先順位は以下で扱う。

- 携帯電話番号を優先する
- 携帯が空の場合は個人電話番号を採用する

### メール

- `email_norm` は `as_is` を基本とする
- trim は行うが、過剰な正規化は行わない

---

## 続柄の扱い

### 入力

- `relationship_code_norm`
- `relationship_name_norm`

### match 生成

`relationship_name_match` はスクリプト側で生成する。

- `relationship_name_norm` が存在する場合
  - 名称から生成する

- `relationship_name_norm` が存在しない場合でも、`relationship_code_norm` が存在し、かつコード→名称変換ルールが定義されている場合
  - 変換後名称から生成する

- 名称も変換ルールもない場合
  - `relationship_name_match` は生成しない

---

## identity 生成

生年月日の正規化については、`scripts/lib/identity/field/birthdate.py` の `normalize_birthdate()` を共通利用候補として確認する。

### 1. insurer_number_norm

`insurer_number_norm` は CSV からは生成しない。

- `input/<insurer_number>/` のフォルダ名から取得する
- スクリプト側で `insurer_number_norm` へ注入する

### 2. person_id_custom

`person_id_custom` は、必要な構成要素が揃う場合のみスクリプト側で生成する。

### 3. identity_hash

`identity_hash` は、以下が揃う場合のみスクリプト側で生成する。

- `insurer_number_norm`
- `insurance_symbol_norm`
- `insurance_number_norm`
- `birth_norm`
- `name_kana_full_match`
- `gender_code_norm`

欠損がある場合は `NULL` とする。

---

## subscribers 照合

- `identity_hash` 生成後、`subscribers` を照合する
- 一致した場合は `matched_subscriber_id` を保持する
- これは照合結果キャッシュであり、最終業務判定ではない

---

## アーカイブ方針

- CSV自体がエラーの場合は `input/` に残す
- run が正常に走り、読み込み判定まで到達したファイルは `archive/` へ移動する
- `archive/` は自動削除せず、手動運用とする

---

## エラーハンドリング方針

本処理のエラーは、以下の3区分で扱う。

### 1. CSV構造エラー

CSVファイル自体の構造・読込に問題があるケースは、CSV構造エラーとして扱う。

例:

- 文字コード不正
- delimiter 不正
- 必須ヘッダー不足
- template / mapping 解決不可
- 列構造不正
- 対応していない rule が指定されている
- `fund_id` が解決できない

#### 扱い

- 即時停止する
- 当該CSVの取込は中断する
- run status は `failed` とする
- `input/` から `archive/` へは移動しない

---

### 2. オペレーション上の空行・非データ行

Excelエクスポート由来などで発生する、実質的に空の行は、データエラーではなく運用上のノイズとして扱う。

例:

- 全列 `""` の行
- key項目がすべて空の行
- 実質空行

#### 扱い

- INSERT せずにスキップする
- run は継続する
- エラー扱いにはしない
- `skipped_empty_row_count` 等の件数として記録する

---

### 3. データエラー（行単位エラー）

CSV構造は正しいが、特定行の値が要件を満たさない場合は、データエラーとして扱う。

例:

- `required=1` 項目が空
- `digits_required` で数字が得られない
- `birth_norm` が生成できない
- rule適用後に必要値が生成できない

#### 扱い

- 当該行は INSERT しない
- run は継続する
- CSVエラーとして記録するが、Run エラー（即時停止）にはしない
- 行番号・ヘッダー・target_column・rule・raw_value・理由をログへ残す
- 最終 status は `completed_with_errors` とする
- 正常完了 (`success`) にはしない
- 差し戻し対象として扱う

---

## run 終了ステータス

run の終了ステータスは以下の3種類とする。

### success

- CSV構造エラーなし
- データエラーなし
- 空行スキップのみ存在する場合は許容する

### completed_with_errors

- CSV構造エラーなし
- データエラーあり
- 一部行は未取込
- 差し戻し対象

### failed

- CSV構造エラーあり
- 当該CSVは即時停止

---

## ログ・件数管理方針

最低限、以下を件数として保持する方針とする。

- `inserted_row_count`
- `skipped_empty_row_count`
- `row_error_count`

また、データエラーについては最低限以下を記録できるようにする。

- `src_file`
- `src_row_no`
- `csv_header`
- `target_column`
- `rule`
- `raw_value`
- `reason`

## 本specで次に詰めること

- rule 実装関数一覧
- 06139463案件の `template_mappings` 詳細確定
- ログ出力方針
- エラー行の記録方法
