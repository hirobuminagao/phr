# Event Table Design v1.1.0

## 1. 目的
本設計は、イベント（健診・特定保健指導）を人単位で一貫して管理するためのデータ構造を定義する。

本モデルでは以下を実現する：
- イベント枠（event）の定義
- 人ごとの現在状態（person_event）の把握
- 実際に発生した事実（event_instance）の履歴管理

---

## 2. 対象範囲
本バージョンでは以下を対象とする：

- 健診イベント（定期健診）
- 医療機関 → 弊社 の結果受領管理
- HIA → 健保 の納品検出管理
- 重複検知およびギャップ検出

---

## 3. テーブル構成
本設計では以下の3テーブルで構成する：

- event（イベント枠）
- person_event（人×イベント状態）
- event_instance（イベント履歴）

---

## 4. event（イベント枠）

### 4.1 概要
イベント枠は、制度・契約に基づく業務単位を表す。

例：
- 06139463 × 2025年度 × 定期健診

### 4.2 主な項目

- event_id
  - イベント枠の内部識別子

- insurer_number
  - 保険者番号

- event_year
  - 対象年度

- event_type
  - イベント種別（健診 / 特定保健指導）

- event_name
  - イベント名称

- start_date
  - 開始日

- end_date
  - 終了日

- submission_deadline
  - 納品締切日

- eligibility_reference_date
  - 対象者判定基準日

- is_active
  - 有効フラグ

---

## 5. person_event（人×イベント状態）

### 5.1 概要
人がイベントに対して現在どのような状態にあるかを管理する。

本テーブルは「現在状態の集約結果」を保持する。

### 5.2 主な項目

- person_event_id
  - 人×イベントの識別子

- event_id
  - 対象イベント

- person_id_custom
  - 個人識別子

- identity_hash
  - 同一人物照合用ハッシュ

---

### 5.3 対象・状態

- is_eligible
  - 対象者フラグ

- result_received_flag
  - 健診結果受領済フラグ

- hia_status_code
  - HIA上の状態コード

---

### 5.4 納品管理

- delivery_target_flag
  - 納品対象フラグ

- delivery_exported_flag
  - 納品済フラグ

- delivery_exported_at
  - 納品日時

---

### 5.5 回数管理（重要）

- result_received_count
  - 医療機関から結果を受領した回数

- delivery_detected_count
  - HIAから納品対象として検出された回数

- last_result_received_at
  - 最後に結果を受領した日時

- last_delivery_detected_at
  - 最後に納品対象として検出された日時

---

### 5.6 ギャップ管理

- gap_flag
  - データ不整合の有無

- gap_reason
  - 不整合理由（例：HIA未反映）

- last_observed_at
  - 最終観測日時

---

## 6. event_instance（イベント履歴）

### 6.1 概要
イベントに関する実際の出来事を時系列で記録する。

1レコード = 1事実（イベント）

---

### 6.2 主な項目

- event_instance_id
  - 履歴識別子

- event_id
  - 対象イベント

- person_id_custom
  - 個人識別子

- identity_hash
  - 同一人物照合用ハッシュ

---

### 6.3 イベント内容

- instance_type
  - 何が起きたか（例：RESULT_XML_RECEIVED, DELIVERY_XML_DETECTED）

- instance_status
  - 処理状態（成功 / 失敗 など）

---

### 6.4 時間

- occurred_at
  - 実際に発生した日時

- observed_at
  - システムが観測した日時

---

### 6.5 ソース情報

- source_system
  - データ発生元（医療機関 / HIA など）

- source_key
  - 元データ識別子（ファイル名等）

---

### 6.6 詳細データ

- payload_json
  - 元データの詳細

- created_at
  - レコード作成日時

---

## 7. 各テーブルの責務

- event
  - 業務枠の定義

- person_event
  - 現在状態の可視化

- event_instance
  - 履歴・証跡の保持

---

## 8. 設計上の重要ポイント

- event は制度単位の枠であり、人に紐づかない
- person_event は現在状態であり、履歴ではない
- event_instance は事実の蓄積であり、回数の根拠となる

特に以下が重要：

- 回数は event_instance の件数から導出される
- person_event はその集約結果を保持する

### 8.1 判定ルールを event に持つ理由

本設計では、以下の判定ルールを event に保持する：

- 資格判定基準日（eligibility_reference_date）
- 年齢計算基準日（age_reference_date）
- 各ルール種別（FIXED_DATE / EXAM_DATE など）

一般的な直感では「受診日時点の年齢・資格」で判定されると考えられがちである。
しかし、実際の健保契約では以下のようなルールが存在する：

- 年度開始日（例：4/1）時点で資格を判定する
- 年度内の固定日（例：11/30）時点で年齢を判定する

この場合、受診日時点の年齢と制度上の年齢が一致しないケースが発生する。

例：
- 受診日：5/20（39歳）
- 年齢基準日：11/30（40歳）
→ 制度上は40歳として扱う

このようなズレを正しく扱うため、
判定ロジックは個人データではなく event 側のルールとして定義する。

これにより以下を実現する：

- 健保ごとのルール差異を吸収可能
- 実装側の「直感による誤判定」を防止
- 判定ロジックの一元管理

---

## 9. 本バージョンで扱わない範囲

- 健診詳細項目（JLAC単位）
- 請求金額
- 特定保健指導の詳細プロセス
- 事業所・部署単位の管理