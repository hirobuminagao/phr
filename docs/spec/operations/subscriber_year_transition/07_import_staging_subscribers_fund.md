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

---

## 氏名処理方針

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

以下のケースは、レコード全体エラーまたは項目エラーとして扱う。

- `required=1` 項目が空
- `digits_required` で数字が得られない
- テンプレートが解決できない
- `fund_id` が解決できない
- 対応していない rule が指定されている

---

## 本specで次に詰めること

- rule 実装関数一覧
- 06139463案件の `template_mappings` 詳細確定
- ログ出力方針
- エラー行の記録方法
