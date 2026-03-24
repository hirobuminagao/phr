# ADR-0014: HIA Dashboard と Person Years の突合分析（v1.0.2 固定）

## ステータス
Accepted

## コンテキスト
v1.0.2 において、以下のデータソース間の突合・分析を実施した。

- dev_phr.subscribers
- work_other.hia_dashboard_status
- work_other.hia_person_years

これらを `identity_hash` によって結合し、  
「dashboardに存在するか」「XML（person_years）に存在するか」の差異を分析した。

---

## 決定事項

### 1. identity_hash を基準とした人物突合を採用
- subscribers / dashboard / person_years の横断は identity_hash を唯一のキーとして扱う
- raw値ではなく、match + 正規化済み値に基づく人物同定を前提とする

---

### 2. qualification_lost_date の扱いを明確化
- qualification_lost_date が存在する加入者は「現在の加入者ではない」とみなす
- HIA仕様上、これらの加入者は dashboard に表示されない

---

### 3. 分類ルールの確定

#### A. dashboardあり / XMLあり
- 正常（現役加入者 + 実績あり）

#### B. dashboardあり / XMLなし
- 要確認対象
- ただし、以下は除外する
  - 対象外医療機関

#### C. dashboardなし / XMLあり
- qualification_lost_date がある場合
  - 正常（過去実績の残存）

#### D. dashboardなし / XMLなし
- qualification_lost_date がある場合
  - 正常（非加入者）

---

### 4. 対象外医療機関の扱い
以下の医療機関については、XMLが存在しなくても正常とする：

- 契約医療機関以外
- 医療法人伯鳳会 大阪中央病院（表記揺れ含む）
- メディカルスクエア赤坂
- 有楽町電気ビルクリニック

※ 現時点ではSQL直書きで対応（対処療法）
※ 将来的にはテーブル化する

---

### 5. 分析対象の定義
今後の調査対象は以下に限定する：

- dashboardあり
- XMLなし
- 対象外医療機関ではない
- qualification_lost_date が NULL（現役）

---

## 結果
上記の整理により、

- 「正常なズレ」
- 「仕様由来のズレ」
- 「本当に調査が必要なズレ」

を切り分け可能となった。

---

## 影響

### v1.0.3 への影響
- medi系へ identity_hash を通す必要性が明確化
- 人物軸での横断設計が必須であることが確定

---

## 今後
- v1.0.3 にて
  - medi系への identity_hash 付与
  - raw / norm / match / export の統一
- v1.1.0 にて
  - 人寄せサマリテーブルの導入
