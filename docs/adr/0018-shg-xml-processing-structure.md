# ADR-0018: SHG XML 処理の構造分離と配置方針

- Status: Accepted
- Date: 2026-04-08
- Deciders: hiro / ChatGPT
- Related:
  - ADR-0016 v1.1.0 identity layer commonization and backfill
  - ADR-0017 event-centric data model gap
  - docs/spec/shg_result/README.md
  - docs/spec/shg_xml/README.md

## Context

本ADRは、`scripts/shg/check_shg_result_xml.py` を完成させ、特定保健指導結果XMLをCSVでチェック可能にする作業の途中で派生した構造判断を固定するものである。

SHG側の目的は、`shg_result` に保持した健診時の事実データと、特定保健指導結果XMLの内容を同一人物単位で突合し、CSVとして確認できる状態を作ることである。

この作業は、特定保健指導結果XMLをCSVでチェックするという SHG 側タスクを主とし、その実装の中で ADR-0016 で整理した v1.1.0 identity layer commonization を初めて実戦投入した利用シーンである。まず `shg_result` に `person_id_custom` / `identity_hash` を付与できる基盤を整え、その上で SHG XML チェック処理を新スクリプトへ移し替える流れとなった。

SHG結果XMLチェック処理は、旧スクリプト `scripts/tokuho_xml_check/check_tokuho_xml.py` に集約されていた。

旧スクリプトは以下を1ファイルで担っていた。

- XML入力走査
- XMLからの基本情報抽出
- 90030 / 90060 / 90070 等の各種値抽出
- person_id_custom 生成
- SHG結果テーブル参照
- person単位集約
- CSV出力

この結果、以下の問題が生じていた。

- スクリプトが肥大化し、読解・修正・検証が難しい
- XMLから値を取る責務と、それを使う責務が混在している
- SHG XMLチェック処理の中で、新identity基盤（generator / identity_hash）をどこで適用するかが見えにくい
- 将来的なZIP直読化やチェック表化（fase2）に向けた変更点が分離されていない

また、SHG XMLチェックは `shg_result` テーブル設計とは別レイヤーであり、DB仕様とXML処理仕様を分離して管理する必要がある。さらに、CSVチェック処理の実装を進める中で、XMLから値を取る責務をライブラリ側へ外出しした方が安全であると判断した。

## Decision

本ADRは、「特定保健指導結果XMLをCSVでチェックする」という SHG 側タスクに従属する構造判断を扱う。すなわち、チェック処理そのものを直接規定するのではなく、その実装を安全に進めるための配置・責務分離・フェーズ分割を固定する。また、その実装の中で identity ライブラリを最初に適用する利用シーンを明確にする。

SHG XML 処理は、PHR 全体のライブラリ構造の中で以下の方針で分離する。

### 1. SHG XML 抽出ロジックは `scripts/lib/shg/xml/` に配置する

XMLから項目値を取得する責務は、実行スクリプト本体から分離し、`scripts/lib/shg/xml/` 配下へ配置する。

想定する分割単位は以下。

- `basic.py`
  - 基本情報抽出
  - report_code
  - insurer / symbol / number
  - name / gender / birth
  - ticket_no / ticket_exp 等
- `goals.py`
  - 90030 の目標抽出
- `outcomes.py`
  - 90060 の達成状況 / アウトカムポイント抽出
- `measurements.py`
  - 90060 の腹囲 / 体重等の計測値抽出
- `role.py`（必要なら）
  - report_code から initial / final を判定するロジック

### 2. `scripts/shg/check_shg_result_xml.py` は orchestration に寄せる

実行スクリプト本体の責務は以下に限定する。

- 入力引数の受け取り
- XML列挙
- SHG結果テーブル読み込み
- XML抽出関数の呼び出し
- identity生成の呼び出し
- person単位集約
- CSV出力

XML構造そのものの詳細解釈は、できるだけ `scripts/lib/shg/xml/` 側へ寄せる。

### 3. 内部束ねキーは `identity_hash` を正とする

SHG XML チェック処理における内部の人物束ねキーは `identity_hash` を使用する。

- `identity_hash`
  - 内部照合用の主キー
- `person_key`
  - CSV目視確認用として保持
- `person_id_custom`
  - 既存運用との橋渡し用として保持

### 4. フェーズを分離する

#### fase1.0
- 旧スクリプトから新スクリプトへ横移行する
- 入力は展開済みXML前提
- 既存CSV構造をできるだけ維持する
- 新generator / 新shg_result / identity_hash 主軸へ載せ替える

#### fase1.1
- ZIP直読みに変更する
- `DATA/*.xml` を対象とする
- XML単位エラーの扱いは維持する

#### fase2
- チェック表としての比較・判定列を整理する
- ロジックをリファクタリングする

## Consequences

### Positive

- XML抽出ロジックを責務単位で分解できる
- `check_shg_result_xml.py` の可読性が上がる
- `identity_hash` 主軸への移行位置が明確になる
- fase1.0 / fase1.1 / fase2 を安全に分離できる
- XML項目仕様変更時に修正範囲を限定できる

### Negative / Trade-offs

- 旧スクリプトからの移植作業が段階的に必要になる
- ファイル数が増える
- 一時的に旧実装と新実装の両方を見比べながら進める必要がある

## Notes

- `docs/spec/shg_result/` は DB テーブル仕様を扱う
- SHG XML チェック処理の仕様は、別途 `docs/spec/shg_xml/` を新設して管理する
- XMLからの値抽出と、その値をどう利用するかは分離して扱う
- role 判定（例: report_code 21 / 22）も、可能であれば XML処理側の小関数へ外出しする
- 本ADRは、SHG XMLチェック処理を進める中で派生した構造判断を固定するものであり、CSVチェックそのものの詳細仕様は `docs/spec/shg_xml/` 側で管理する
- 本ADRにおいて identity ライブラリは主題ではなく、SHG XMLチェック処理を進める中で最初に適用された基盤として位置づける