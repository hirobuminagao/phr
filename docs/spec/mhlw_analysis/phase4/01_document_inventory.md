

# 01 Document Inventory (MHLW specs)

このドキュメントは、厚生労働省（第4期 V08 系）を一次情報（印籠）として扱うための **資料目録**。
以後のルール化・実装・テストは、必ず「どの資料のどの章・図表に基づくか」をここに紐付ける。

---

## 1. 対象スコープ
- 対象:
  - 健診結果XML（CDA）
  - 特定保健指導XML（CDA）
  - 送付用アーカイブ（ZIP/フォルダ/命名）
- 目的: 資料 → ルール整理 → 必要マスタ → OO整理 → 格納仕様 → 入力 → 出力（xml→xml / xml→pdf(+excel)）
- 整理方針:
  - 健診と特定保健指導は、共通のCDA土台を持つが、セクション構造・項目コード・業務ルールが異なるため、specは分離して整理する
  - 送付用ファイルアーカイブ仕様は共通のL0として扱い、その上に健診系 spec と特定保健指導系 spec を分けて積み上げる

---

## 2. 一次情報（Phase4 / V08）

### 2.1 送付用ファイルアーカイブ仕様（L0: ZIP/フォルダ/命名）
- ファイル: `docs/mhlw/phase4_v08/8-1A.pdf`
- タイトル: 送付用ファイルアーカイブ仕様説明書（Ver.4）
- 更新日: 2023-03-31
- 担当範囲（ユーティリティ観点）:
  - ZIPアーカイブ形式、ルートフォルダ構成（DATA/CLAIMS/XSD ほか）
  - ルートフォルダ名規則（提出元/提出先/提出日/同日分割/実施区分）
  - XMLファイル名規則（健診/決済/保健指導/保健指導決済の識別、DATAとCLAIMSの1対1対応）
  - 交換パターン別の必須ファイル定義（表）
- 派生成果物（作成先）:
  - `docs/spec/02_file_archive_spec.md`
  - `docs/spec/mhlw_analysis/phase4/`（図表抜粋・チェック観点メモ）

### 2.2 健診結果XML仕様（L1-L3: CDA/基本情報/健診結果記述）
- ファイル: `docs/mhlw/phase4_v08/3-1A.pdf`
- タイトル: 特定健診情報ファイル仕様説明書（Ver.4）
- 担当範囲（ユーティリティ観点）:
  - ClinicalDocument 構造（ヘッダ、recordTarget、author等）
  - 名前空間、schemaLocation、必須要素/属性
  - 基本情報（保険者番号/記号/番号/氏名/住所/電話 等）
  - 健診結果記述（entry/observation、value型 PQ/CD/CO/TS、nullFlavor 等）
- 派生成果物（作成先）:
  - `docs/spec/03_xml_document_spec.md`
  - `docs/spec/04_basic_info_spec.md`
  - `docs/spec/05_observation_spec.md`

### 2.3 特定保健指導XML仕様（L1-L3: CDA/基本情報/保健指導記述）
- ファイル: `docs/mhlw/phase4_v08/5-1A.pdf`
- タイトル: 特定保健指導情報ファイル仕様説明書（Ver.4）
- 担当範囲（ユーティリティ観点）:
  - ClinicalDocument 構造（ヘッダ、recordTarget、author等）
  - 名前空間、schemaLocation、必須要素/属性
  - 利用者情報、利用券情報、保健指導実施情報
  - 指導共通情報セクション（90010）
  - 指導初回①情報セクション（90020）
  - 指導初回情報セクション（90030）
  - 指導集計情報セクション（90040）
  - 継続支援情報セクション（90050）
  - 中間評価情報セクションおよび最終評価情報セクション（90060）
  - 指導機関情報セクション（90090）
  - 保健指導項目コード、結果コード、value型、nullFlavor 等
- 派生成果物（作成先）:
  - `docs/spec/shg_xml/README.md`
  - `docs/spec/mhlw_analysis/phase4/07_shg_document_structure.md`
  - `docs/spec/mhlw_analysis/phase4/08_shg_header_and_ticket_spec.md`
  - `docs/spec/mhlw_analysis/phase4/09_shg_section_spec.md`
  - `docs/spec/mhlw_analysis/phase4/10_shg_code_and_value_spec.md`

### 2.4 マスタ／コード表（L4: namecode/OID/単位/型の根拠）
- ファイル: `docs/mhlw/phase4_v08/001082795.xlsx`
- タイトル: （コード・マスタ関連 Excel）※暫定
- 期待する役割:
  - namecode（検査項目コード）と項目名・型・単位等の対応
  - OID/コード体系（必要な範囲）
- 派生成果物（作成先）:
  - `docs/spec/06_master_requirements.md`
  - `master/seeds/`（CSV化してseedとして固定する候補）

---

## 3. 仕様化ドキュメント（このrepo内の成果物）

### 3.1 共通 / L0
- `docs/spec/02_file_archive_spec.md` : L0（ZIP/フォルダ/命名/必須ファイル）

### 3.2 健診系
- `docs/spec/03_xml_document_spec.md` : L1（健診CDA文書構造/必須セクション/namespace）
- `docs/spec/04_basic_info_spec.md` : L2（保険者・加入者・連絡先・住所等の基本情報）
- `docs/spec/05_observation_spec.md` : L3（健診結果の記述、value型、nullFlavor、CD/CO/PQ等）

### 3.3 特定保健指導系
- `docs/spec/shg_xml/README.md` : 特定保健指導XMLの総合spec
- `docs/spec/mhlw_analysis/phase4/07_shg_document_structure.md` : 特定保健指導CDA文書構造
- `docs/spec/mhlw_analysis/phase4/08_shg_header_and_ticket_spec.md` : 利用者情報 / 受診券・利用券 / 保健指導実施情報
- `docs/spec/mhlw_analysis/phase4/09_shg_section_spec.md` : 90010〜90090 のセクション別仕様
- `docs/spec/mhlw_analysis/phase4/10_shg_code_and_value_spec.md` : 項目コード / 結果コード / value型 / nullFlavor

### 3.4 マスタ系
- `docs/spec/06_master_requirements.md` : L4（必要マスタ、参照元、seed方針）

---

## 4. 運用ルール（重要）
- 厚労資料（PDF/XSD/Excel）を一次情報として扱う。
- ルール化（Check/Fix）・実装は必ず「根拠（資料名・章・図表）」を紐付ける。
- 局所パッチ（例: `scripts/.../medi_trans_06139463.py`）は **症例（case）** として扱い、仕様ルールの検証用テストベクタとして保存する。
- 健診 spec と特定保健指導 spec を混在させない。共通化できるのは CDA 共通部・送付用アーカイブ・マスタ参照方針に限る。
- 特定保健指導XMLの解析は、まず `5-1A.pdf` を章・表・サンプル単位で分解し、90010 / 90020 / 90030 / 90040 / 90050 / 90060 / 90090 の責務ごとに spec 化してから section 実装へ入る。
