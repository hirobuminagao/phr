

# SHG XML 処理仕様（shg_xml）

## 目的

本ディレクトリは、SHG（特定保健指導）結果XMLの解析・抽出・チェック処理に関する仕様を管理する。

本仕様は、以下の責務を対象とする。

- XMLからの値抽出（basic / goals / outcomes / measurements）
- XML構造の理解と項目マッピング
- スクリプト処理（check_shg_result_xml.py）の設計方針

## スコープ

本specは「XML処理レイヤー」を対象とし、以下は対象外とする。

- DB設計 → `docs/spec/shg_result/`
- identity生成 → `docs/spec/identity_canonicalization/`

## ディレクトリ構成方針

```text
scripts/lib/shg/xml/
  ├ basic.py
  ├ goals.py
  ├ outcomes.py
  ├ measurements.py
  └ role.py（必要に応じて）
```

### 各ファイルの責務

#### basic.py
- XMLの基本情報抽出
- report_code
- insurer / symbol / number
- name / gender / birth
- ticket_no / ticket_exp

#### goals.py
- 90030（初回目標）の抽出

#### outcomes.py
- 90060（最終評価）の抽出
- アウトカム評価
- ポイント

#### measurements.py
- 90060の数値系（腹囲・体重など）

#### role.py
- report_code から initial / final 判定

## 実行スクリプトとの責務分離

### XML層（lib/shg/xml）
- XMLから値を「取得するだけ」
- 業務ロジックは持たない（最小限に留める）

### orchestration層（check_shg_result_xml.py）
- XML列挙
- DB取得
- identity生成
- person単位集約
- CSV出力

## キー設計

| 種別 | 用途 |
|------|------|
| identity_hash | 内部束ねキー（主キー） |
| person_key | CSV確認用 |
| person_id_custom | 既存連携用 |

## フェーズ設計

### fase1.0
- 旧スクリプトからの横移行
- 展開済みXMLを入力とする
- CSV構造を維持

### fase1.1
- ZIP直読みに変更
- DATA/*.xml を対象

### fase2
- チェックロジック整理
- 比較・判定列の強化

## 設計方針

- XML構造に沿って責務を分割する
- 「値取得」と「値の利用」を分離する
- 600行以上の単一スクリプトを避ける
- 将来的な仕様変更に耐える構造とする

## 備考

- 本仕様はADR-0018に基づく
- XML仕様変更時は本ディレクトリで吸収する