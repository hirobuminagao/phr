

# phase4 MHLW分析メモ

## 目的

このディレクトリは、厚生労働省が公開している第4期関連資料を、実装で参照しやすい形に分解・整理するための作業場所とする。

主な目的は以下の3つ。

- 送付用ファイルアーカイブ仕様を整理する
- 健診情報XML仕様を整理する
- 特定保健指導XML仕様を整理する

実装や個別スクリプト修正を先に進めるのではなく、まず厚労省資料を一次情報として整理し、その後に section 実装や check / fix ロジックへ落とし込む前提とする。

## ディレクトリ方針

phase4 配下は、以下の3系統で整理する。

- `common/`
  - 健診 / 特定保健指導のどちらにも共通する内容
  - 主に送付用ファイルアーカイブ仕様、共通ルール、資料目録など
- `health_examination/`
  - 健診情報XMLに関する分析メモとspec化メモ
- `shg/`
  - 特定保健指導XMLに関する分析メモとspec化メモ

## 既存ファイルの扱い

現時点では、phase4 直下に旧整理途中のファイルが残っている。

- `01_document_inventory.md`
- `02_file_archive_spec.md`
- `03_xml_document_spec.md`
- `04_basic_info_spec.md`
- `05_observation_spec.md`
- `06_master_requirements.md`

これらは一旦削除せず、以下の方針で扱う。

- `02_file_archive_spec.md`
  - 共通仕様として `common/` 側へ再整理候補
- `03_xml_document_spec.md`
- `04_basic_info_spec.md`
- `05_observation_spec.md`
  - 健診XML前提で書かれているため、`health_examination/` 側へ再整理候補
- `01_document_inventory.md`
  - 全体の資料目録として残すか、READMEへ統合するか後で判断
- `06_master_requirements.md`
  - 共通マスタ観点として残すか、対象別に分割するか後で判断

つまり、今は「移動や削除を急がず、READMEで整理方針を先に固定する」段階とする。

## 今後の進め方

進め方は以下を基本とする。

1. 厚労省PDF / Excel を一次情報として確認する
2. `common / health_examination / shg` のどこに属するかを決める
3. 章・表・サンプル単位で読みやすいメモへ分解する
4. そのメモを元に repo 内 spec を確定する
5. 最後に section 実装や check / fix ロジックへ反映する

## 当面の優先順位

1. `common/`
   - 送付用ファイルアーカイブ仕様の整理
2. `shg/`
   - 特定保健指導情報ファイル仕様書（5-1A.pdf）の整理
3. `health_examination/`
   - 健診情報ファイル仕様書（3-1A.pdf）の整理

## 注意

- 健診仕様と特定保健指導仕様を混在させない
- 共通化は、送付用ファイルアーカイブ仕様・CDA共通観点・マスタ参照方針に限る
- 実装都合ではなく、まず厚労省資料の記述を基準に整理する