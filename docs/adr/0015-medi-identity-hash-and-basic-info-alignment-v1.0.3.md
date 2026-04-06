# ADR-0015: medi / hia 系における identity_hash と基本情報整流化（v1.0.3）

Status: Accepted

---

## 1. Context

PHR基盤において、複数のデータ系統が存在している。

- subscribers（正規加入者マスタ）
- hia_dashboard（CSV由来の状態管理）
- hia_export_zip / xml_event（XML由来の実績系）
- medi 系（xml_ledger 等）

これらはそれぞれ独立した粒度・構造を持っており、  
**同一人物の横断的な突合が困難な状態**であった。

v1.0.2 にて identity の基本方針（person_id_custom + identity_hash）は定義されたが、  
各系統への適用は未完了であった。

当初は、イベントを SQL 上で統合的に生成・管理する案もあったが、

- 既存テーブル構造を大きく変更する必要がある
- 粒度の再定義（人単位 / XML単位）が未整理
- 実装コストと影響範囲が大きい

という理由から、v1.0.3 では以下にスコープを限定することとした。

---

## 2. Decision

### 2.1 v1.0.3 のゴール

v1.0.3 は以下を完了条件とする。

- 各系統において **共通キー（person_id_custom / identity_hash）を整備する**
- 横断突合が可能な状態を作る
- イベントの統合管理（テーブル設計）は次フェーズへ送る

---

### 2.2 既存粒度は壊さない

- 既存テーブルの粒度変更は行わない
- 主キー / unique 制約の思想は維持する
- 人単位 / XML単位の再設計は行わない（保留）

例:
- medi_xml_ledger は引き続き XML単位の台帳として扱う

---

### 2.3 identity 情報は後付けで付与する

各系統に対して以下を追加する。

- `person_id_custom`
- `identity_hash`

方針:

- rawデータから `person_id_custom` を生成
- `identity_hash = person_id_custom + name_kana_match + gender_code`
- subscribers を参照してリアルタイム join は行わない
- 後段の update / backfill により整合を取る

---

### 2.4 スクリプト修正 + backfill を前提とする

実装は以下の手段で行う。

- 既存スクリプトの修正
- 既存データへの backfill 実施

対象:

- subscribers
- hia_dashboard
- hia_export_zip / xml_event
- medi xml_ledger

---

### 2.5 正規化ルールの分散は許容する（暫定）

v1.0.3 では以下は統一しない。

- 正規化ロジックの完全共通化
- ライブラリ化

理由:

- まずは実データ上で identity を通すことを優先するため

---

## 3. Result

以下が達成された。

- 各系統に `identity_hash` が付与された
- 人物単位での横断突合が可能になった
- スクリプトおよび backfill により既存データも整合された

---

## 4. Consequences

### 4.1 良い点

- 既存構造を壊さずに identity を導入できた
- 実運用データでの整合が確認できた
- 次フェーズへの前提が整った

---

### 4.2 課題

以下が顕在化した。

- 正規化ロジックが各所に分散している
- raw / match / hash の責務がコード上で不明確
- backfill と本処理でロジックが重複する
- identity 生成の一貫性保証が弱い

---

## 5. Follow-up

上記課題を受けて、次フェーズでは以下を行う。

- identity 生成・正規化の共通ライブラリ化
- primitive / field / builder レイヤーの整理
- raw / match / hash の責務分離の明確化
- 将来的な event モデルへの接続

これらは ADR-0016（v1.1.0）で扱う。

---

## 6. Summary

v1.0.3 は、

> **イベント統合ではなく、共通キー整備を完了するフェーズ**

として定義し、

- identity_hash を各系統へ後付けで適用
- 既存構造を維持したまま整流化を実現

した。

イベント中心モデルへの移行は次フェーズへ委ねる。