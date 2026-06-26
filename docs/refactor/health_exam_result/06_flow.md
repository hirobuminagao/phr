# health_exam_result v2 初期処理フロー

このドキュメントは、`health_exam_result v2` の初期実装におけるローカルシステム処理フローを整理する。

未着管理、医療機関回答待ち、再提出管理、HIAアップロード後の業務管理は初期実装フローから分離し、別途業務フローとして整理する。

---

## 初期実装スコープ

初期実装では、以下を成立させることを目的とする。

- 受領・編集済みファイルを `file_receipts` に登録する。
- ファイルからXMLを検出し、`xml_ledger` に登録する。
- XML基本情報から `subscribers.id` を解決する。
- XMLに実際に存在した健診値を `item_values` に登録する。
- 法定健診・特定健診・異常値チェックを行い、`exam_check_results` に登録する。
- `xml_ledger` と `file_receipts` に処理結果を集約する。
- HIAアップロード待ち、またはエラーとして状態を整理する。

---

## Mermaid フロー

```mermaid
flowchart TD

%% ===== Start =====
A([開始])

%% ===== File receipt =====
A --> B[02_健診結果（編集）\n投入対象ファイル]
B --> C[01_register_files.py\nfile_receipts 登録]
C --> C1[(health_exam_result.file_receipts)]

%% ===== File type branch =====
C1 --> D{file_type}
D -->|ZIP| E[ZIP展開\nXML検出]
D -->|XML| F[XML検出]
D -->|CSV| G[CSV取込\n将来対応]
G --> G1[初期実装では対象外]

%% ===== XML ledger =====
E --> H[02_import_xml_files.py\nxml_ledger 登録]
F --> H
H --> H1[(health_exam_result.xml_ledger)]

%% ===== XML per person =====
H1 --> I[XML基本情報抽出\n保険者番号・記号・番号・氏名カナ・生年月日・性別・健診日]
I --> J[04_match_subscribers.py\n加入者照合]
J --> J1[(dev_phr.subscribers 参照)]
J --> K[xml_ledger 更新\nsubscriber_id / identity_hash / match_status]

%% ===== Item extraction =====
K --> L[03_extract_item_values.py\n健診項目抽出]
L --> L1[(dev_phr.exam_item_master 参照)]
L --> M[(health_exam_result.item_values)]

%% ===== Checks =====
M --> N[05_check_exam_results.py\n項目単位チェック]
N --> N1[(dev_phr.exam_item_groups 系 参照)]
N --> O[法定健診チェック]
N --> P[特定健診チェック]
N --> Q[異常値チェック]

O --> R[XML/受診者単位チェック集約]
P --> R
Q --> R

R --> S[(health_exam_result.exam_check_results)]
R --> T[xml_ledger 更新\ncheck_status / hia_ready_status / summary]
T --> H1

%% ===== File summary =====
H1 --> U[全XML処理完了後\nfile_receipts サマリー更新]
U --> C1

%% ===== Result =====
U --> V{HIAアップロード可能？}
V -->|Yes| W[HIAアップロード待ち]
V -->|No| X[エラー / 確認対象]

W --> Y([終了])
X --> Y
```

---

## フローとスクリプトの対応

| 処理 | 主担当スクリプト | 主な更新テーブル | 主な参照テーブル |
| --- | --- | --- | --- |
| ファイル検出・登録 | `01_register_files.py` | `health_exam_result.file_receipts` | なし |
| ZIP展開・XML検出・XML台帳登録 | `02_import_xml_files.py` | `health_exam_result.xml_ledger`, `health_exam_result.file_receipts` | なし |
| 健診項目抽出 | `03_extract_item_values.py` | `health_exam_result.item_values`, `health_exam_result.xml_ledger` | `dev_phr.exam_item_master` |
| 加入者照合 | `04_match_subscribers.py` | `health_exam_result.xml_ledger` | `dev_phr.subscribers` |
| 法定・特定・異常値チェック | `05_check_exam_results.py` | `health_exam_result.exam_check_results`, `health_exam_result.xml_ledger` | `dev_phr.exam_item_master`, `dev_phr.exam_item_groups` 系 |
| HIAアップロード待ち整理 | `05_check_exam_results.py` または後続スクリプト | `health_exam_result.xml_ledger`, `health_exam_result.file_receipts` | `health_exam_result.exam_check_results` |

---

## 初期実装では分離する業務フロー

以下は初期処理フローには含めず、別フローとして整理する。

- 医療機関回答待ち
- 再提出受領
- 前回提出との差分比較
- 結果未着チェック
- HIAアップロード実行後の結果反映
- 受領台帳へのサマリー書き戻し運用

---

## 現時点の考え

初期実装では、`file_receipts`、`xml_ledger`、`item_values`、`exam_check_results` を中心に、XML/受診者単位で一気通貫に処理する。

ファイル単位のサマリーは、個別XML処理完了後に `file_receipts` へ集約する。

`zip_receipts` は独立テーブルとしては作成せず、ZIPは `file_receipts.file_type` による処理分岐として扱う。