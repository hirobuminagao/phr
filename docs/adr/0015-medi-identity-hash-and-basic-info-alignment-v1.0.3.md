

# ADR-0015: medi系への identity_hash 導入と基本情報表現の整流化（v1.0.3）

## ステータス
Accepted

## コンテキスト
v1.0.2 において、subscribers / hia_dashboard_status / hia_person_years を
`identity_hash` で横断分析できる状態を固定した。

その結果、今後の調査・追跡・出力整合のためには、
医療機関XML由来データ（medi系）にも人物軸を通す必要があることが明確になった。

一方で、現行の v1 運用には以下の制約がある。

- `scripts/work_folder` を原本として管理している
- `scripts/kenshin_list_pydir` は git push → 会社側 pull → 実行環境コピーで運用している
- v1では共通スニペット / 共通libの新設は行わない
- 正規化ロジックは `work_folder` 側を更新し、必要に応じて `kenshin_list_pydir` 側へコピーする
- medi系 ledger の粒度は現時点では最終確定していないため、既存粒度を壊さないことを優先する

---

## 決定事項

### 1. v1.0.3 の目的
v1.0.3 では、medi系の既存粒度を維持したまま、以下を行う。

- medi系へ `identity_hash` を導入する
- 基本情報の持ち方を `raw / norm / match / export` で整流化する
- subscribers / medi / dashboard / person_years / HIA出力 の人物軸を揃える

---

### 2. 既存粒度は壊さない
medi系 ledger / receipt の粒度が人単位か XML単位かは、現時点では保留とする。

したがって v1.0.3 では:

- 既存テーブルの粒度変更は行わない
- 既存の主キー / unique key の思想は維持する
- 追加するのは `identity_hash` と基本情報表現の整流化に限定する

---

### 3. identity_hash は medi系へ後付けで通す
v1.0.3 では、取り込み時に subscribers を同時参照して直接記帳する方式までは採用しない。

代わりに以下の方針を採用する。

- medi系テーブルへ `identity_hash` を追加する
- 既存の人物特定情報を用いて後段で `identity_hash` を付与・更新する
- これにより、既存フローを壊さずに人物横断を可能にする

---

### 4. 基本情報の表現を raw / norm / match / export で統一する
基本情報（氏名、保険証記号、保険証番号等）の表現は、
少なくとも v1.0.3 対象テーブルにおいて、以下の責務で揃える。

- raw: 受領した元値
- norm: 内部処理用の正規化値
- match: 照合用の安定値
- export: 出力仕様に合わせた値

これにより、各工程での意味の揺れを減らす。

---

### 5. v1では共通lib化しない
v1.0.3 では、正規化ロジックや `identity_hash` 生成ロジックについて
新しい共通スニペット / 共通libは作成しない。

方針は以下とする。

- 原本は `scripts/work_folder` 側に置く
- `scripts/kenshin_list_pydir` 側で必要な場合は同一ロジックをコピーする
- 修正が必要な場合は `work_folder` 側を更新し、その後 `kenshin_list_pydir` 側へ反映する

---

## 影響

### v1.0.3 で得られるもの
- medi系にも `identity_hash` が通る
- 医療機関XMLと加入者・dashboard・HIA出力の人物軸が揃う
- v1.1.0 の人寄せサマリテーブル追加の前提が整う

### v1.0.3 でやらないこと
- 新規の人寄せサマリテーブル追加
- event モデルの一般化
- v2 向けディレクトリ再編
- import 時点で subscribers を直接参照する構造変更

---

## 今後

### v1.1.0
- 人寄せ横断サマリテーブルの追加
- identity_hash をキーとした人単位参照の強化

### v2
- ディレクトリ構造の再設計
- app / domain / infra 分離
- 共通lib化
- 業務フロー / 外部I/O / 内部モデルを統一した再構築