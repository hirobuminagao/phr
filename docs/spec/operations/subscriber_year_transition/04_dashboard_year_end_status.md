# 04_dashboard_year_end_status

## 目的

本ファイルは、加入者年度更新運用における「年度末状態」の定義と、
`hia_dashboard_year_end_status` の責務・役割・記帳ルールを整理するための spec である。

本specでは、新たなロジックを検討するのではなく、
既に議論済みの年度末状態の考え方を明文化し、Step0の仕様として固定することを目的とする。

---

## 年度末状態の定義

2025年度最終状態は、以下の2つをもって定義する。

※本運用において受領する加入者データは、記号100本人以外の最新に限られる場合がある。
このため、年度末状態における加入者基準面は完全な健保全体の最新状態を保証するものではない。

### 1. 加入者基準面（subscribers）

- HIAの加入者最新状態を反映済み
- enrichment（補完）後の状態
- 2026年度比較における基準面として扱う

👉 本運用における「比較基準となる加入者状態」

---

### 2. ダッシュボード年度末状態（hia_dashboard_year_end_status）

- 年度末時点のダッシュボード状態をそのまま記帳
- 年度進捗・結果の評価における母数を固定するための基準として利用する
- 運用状態の履歴として保持する

👉 本運用における「年度末の活動状態」

---

## テーブルの役割整理

### hia_dashboard_year_end_status

#### 役割

- 年度末時点のダッシュボード状態を記録するスナップショットテーブル
- 翌年度比較や判定時の補助情報として利用する

#### 性質

- 履歴テーブル（更新しない・追記のみ）
- 年度単位で固定される

---

## キー設計（方針）

- identity_hash + fiscal_year + insurer_number を主キーとする

補助として以下を保持することを許容する。

- subscribers_id（hia_dashboard_status 側で保持済みの場合）
- hia_subscriber_id（hia_dashboard_status 側で保持済みの場合）

---

## 記帳ルール

### 記帳タイミング

- Step0 実行時に1回のみ記帳する
- 年度末としての記帳実行タイミングはマニュアル指定とする

※ 本処理は scripts/hia/snapshot_hia_dashboard_year_end_status.py にて実行する

---

### 記帳内容

- ダッシュボード運用テーブル（hia_dashboard_status）の現状態をそのまま記録する
- 加工や補正は行わない（事実の保存を優先）
- subscribers や他テーブルを snapshot 時に追加 join して補完しない
- snapshot に必要な補助IDは、事前に hia_dashboard_status 側へ保持された値をそのまま記帳する
- 資格喪失日も、hia_dashboard_status 側へ保持された値をそのまま記帳する

---

## 保持項目方針

`hia_dashboard_year_end_status` は、運用テーブルの完全コピーではなく、年度履歴の追跡と集計に必要な最小項目に絞って保持する。

### 保持対象

#### 1. ID系

- `identity_hash`
- `fiscal_year`
- `insurer_number`
- `person_id_custom`（hia_dashboard_status 上は `subscriber_person_id_custom` を保持し、snapshot では `person_id_custom` として記帳）
- `subscribers_id`（hia_dashboard_status 側で保持済みの場合）
- `hia_subscriber_id`（hia_dashboard_status 側で保持済みの場合）

#### 2. ダッシュボードステータス

- 年度末時点のステータス

#### 3. 健診イベント系

- 健診予約日
- 健診受診日

※ 本specで扱う予約日は健診予約日を指し、保健指導予約日は保持対象に含めない。

#### 4. 医療機関

- 医療機関コード
- 医療機関名

#### 5. 記帳管理

- `snapshot_at`

#### 6. 資格情報

- `qualification_loss_date`

※ 資格喪失日は、年度末時点でダッシュボードCSVに残っていない対象者の扱いを後続で判断するための補助情報として保持する。

### 保持しないもの

- 保健指導の面談日や進捗イベント日
- 運用テーブルの完全コピー
- comparison専用の詳細照合項目一式

---

## 運用テーブルとの関係

### 基本方針

- 運用テーブルと年度末テーブルは明確に役割を分離する
- 補助IDの解決や enrichment は、年度末テーブルではなく運用テーブル側で完了させる
- 年度末テーブルは「固定」の責務に徹し、補完ロジックを持たない
- 資格喪失日も enrichment と同様に、年度末テーブルでは解決せず、運用テーブル側で保持済みの値を固定する

| テーブル | 役割 |
|----------|------|
| ダッシュボード運用テーブル | 現在進行中の状態 |
| hia_dashboard_year_end_status | 年度末の確定状態 |

---

### 年度切替時の処理

Step0 にて以下を行う。

1. 運用テーブルの状態を年度末テーブルへ記帳
2. 記帳完了後、運用テーブルを初期状態へ戻す

---

## 未予約の扱い

### 方針

- 未予約も年度末状態として記帳する
- ただし翌年度比較では優先度を下げる

### 意味

- 履歴としては保持する
- 判定ロジック上は弱いシグナルとして扱う

---

## Step0 の完了条件

以下が満たされた状態をもって、2025年度最終状態の確定とする。

- HIA受領データ（当該タイミングでの最新）が subscribers に反映されている
- hia_dashboard_year_end_status にマニュアル指定した年度末状態が記帳されている
- ダッシュボード運用テーブルが初期状態へ戻されている

---

## 位置づけ

本specは、以下の処理の前提条件となる。

- comparison（03）
- staging（05）
- enrichment（06）

👉 本specにより「比較基準」と「履歴」が確定する

---

## 関連 spec

- 01_overview.md
- 02_operation_steps.md
- 03_comparison_policy.md
- 05_staging_subscribers_fund.md
- 06_subscriber_enrichment.md