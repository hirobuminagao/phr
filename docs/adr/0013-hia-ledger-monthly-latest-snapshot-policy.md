

# ADR-0013: HIA Ledger Monthly Latest Snapshot Policy

## Status
Accepted

## Context
HIA 健診結果ZIPは以下の特性を持つ:
- zip_name（ファイル名）は固定（同一月内）
- 同一zip_nameでも中身（XML件数・内容）は日々変化する
- XMLファイル名は同一でも中身（人物）が入れ替わる可能性がある
- 当月は可変、前月以前は固定

従来設計では以下の問題が発生した:
- zip_name / xml_filename に依存した重複判定が破綻
- 同一人物のズレ（JOIN不一致）
- 差し替えZIPによる不整合（古いイベント残存）

## Decision

### 1. hia_import_zips（ZIPヘッダ台帳）
- zip_name を UNIQUE とする
- 同名ZIPは1行で最新状態を保持（UPSERT）
- ZIP再取込時は物理削除せず、同一 zip_name 行を更新する
- zip_sha256 により内容更新を検知

### 2. hia_xml_events（XMLイベント明細）
- XMLファイル名は識別子に使用しない
- 一意キーは以下とする:
  (person_year_id, zip_id, exam_date, facility_code)
- facility_code の NULL は空文字へ正規化
- 論理削除フラグ is_deleted を持つ
- XMLイベントは物理削除せず、is_deleted により状態管理する

#### 更新フロー
1. ZIP再取込時、当該 zip_id 配下の既存レコードを is_deleted=1 にする
2. 今回ZIP内のXMLを処理:
   - 一致レコードがあれば更新し is_deleted=0 に戻す
   - なければ新規INSERT（is_deleted=0）
3. 最終的に is_deleted=1 のまま残ったレコードは「今回ZIPから消えたイベント」とみなす

### 3. hia_person_years（人物年度集約）
- hia_xml_events（is_deleted=0）のみを集約対象とする
- dl_count は「有効イベント件数」とする
- 有効イベントが存在する場合:
  - dl_count >= 1
  - last_seen_* は最新イベントから更新
- 有効イベントが存在しない場合:
  - dl_count = 0
  - last_seen_* = NULL

## Consequences

### 利点
- 同名ZIP差し替えに強い
- XMLファイル名に依存しない安定した識別
- 消えた人／復帰した人を追跡可能
- person_years が常に最新状態を表現できる

### 注意点
- hia_xml_events は物理削除せず論理削除で管理する
- hia_person_years は加算更新ではなく再集計更新とする
- facility_code は必ず正規化（NULL禁止）する

## Migration Notes
- hia_xml_events に以下を追加:
  - is_deleted (TINYINT)
- UNIQUE KEY を以下へ変更:
  - (person_year_id, zip_id, exam_date, facility_code)
- facility_code を NOT NULL + '' 正規化へ変更

## Related
- ADR-0011 hia_dashboard_person_years join
- ADR-0012 identity_hash canonicalization