# health_exam_result v2 初期処理フロー

このドキュメントは、`health_exam_result v2` の初期実装におけるローカルシステム処理フローを整理する。

未着管理、医療機関回答待ち、再提出管理、HIAアップロード後の業務管理は初期実装フローから分離し、別途業務フローとして整理する。

---

## 初期実装スコープ

初期実装では、以下を成立させることを目的とする。

- 設定YAMLの `event_id` から `event.result_root_path` を取得する。
- `medical_folder_aliases` を参照し、イベント配下の医療機関フォルダを探索する。
- 受領・編集済みファイルを `file_receipts` に登録する。
- `zip_sha256` が既存と同一のZIPは処理対象外としてスキップし、ETL実行ログ側でスキップ件数・理由を確認できるようにする。
- ZIPまたはXMLファイルからXMLを検出する。
- XML内容は `xml_sha256` で一意判定し、`xml_ledger` に登録する。
- 物理ファイルとXML内容の対応を `xml_file_links` に登録する。
- 同一 `xml_sha256` のXMLを別ZIP等で再受領した場合は、`xml_ledger` を新規作成せず、`xml_file_links` のみ追加する。
- XML基本情報から `subscribers.id` を解決する。
- XMLに実際に存在した健診値を `exam_item_values` に登録する。
- 法定健診・特定健診・異常値チェックを行い、`exam_check_results` に登録する。
- `xml_ledger` と `file_receipts` に処理結果を集約する。
- `xml_export_status` により、HIAアップロード用XMLとして出力可能かを整理する。

---

## Mermaid フロー

```mermaid
flowchart TD

%% ===== Start =====
A([開始])

%% ===== Event / folder discovery =====
A --> A1[設定YAML読込\nevent_id 取得]
A1 --> A2[(dev_phr.event 参照\nresult_root_path 取得)]
A2 --> A3[(health_exam_result.medical_folder_aliases 参照)]
A3 --> B[02_健診結果（編集）\n投入対象ファイル探索]

%% ===== File receipt =====
B --> C[01_register_files.py\nfile_receipts 登録]
C --> C1[(health_exam_result.file_receipts)]

%% ===== Duplicate ZIP branch =====
C1 --> Z{zip_sha256 既存一致?}
Z -->|Yes| Z1[同一ZIPとして処理スキップ\netl_runs / etl_errors で記録]
Z1 --> U
Z -->|No / ZIP以外| D{file_type}

%% ===== File type branch =====
D -->|ZIP| E[ZIP展開\nXML検出]
D -->|XML| F[XML検出]
D -->|CSV| G[CSV取込\n将来対応]
G --> G1[初期実装では対象外]

%% ===== XML sha branch =====
E --> X[xml_sha256 計算]
F --> X
X --> X1{xml_sha256 既存一致?}

X1 -->|Yes| X2[xml_file_links 追加のみ\nxml_ledger / item抽出 / チェックは再実行しない]
X2 --> X3[(health_exam_result.xml_file_links)]

X1 -->|No| H[02_import_xml_files.py\nxml_ledger 登録]
H --> H1[(health_exam_result.xml_ledger)]
H --> X4[xml_file_links 追加]
X4 --> X3

%% ===== XML per person =====
H1 --> I[XML基本情報抽出\n保険者番号・記号・番号・氏名カナ・生年月日・性別・健診日]
I --> J[04_match_subscribers.py\n加入者照合]
J --> J1[(dev_phr.subscribers 参照)]
J --> K[xml_ledger 更新\nsubscriber_id / identity_hash / match_status]

%% ===== Item extraction =====
K --> L[03_extract_item_values.py\n健診項目抽出]
L --> L1[(dev_phr.exam_item_master 参照)]
L --> M[(health_exam_result.exam_item_values)]

%% ===== Checks =====
M --> N[05_check_exam_results.py\n項目単位チェック]
N --> N1[(dev_phr.exam_item_group_* 系 参照)]
N --> O[法定健診チェック]
N --> P[特定健診チェック]
N --> Q[異常値チェック]

O --> R[XML/受診者単位チェック集約]
P --> R
Q --> R

R --> S[(health_exam_result.exam_check_results)]
R --> T[xml_ledger 更新\ncheck_status / xml_export_status / summary]
T --> H1

%% ===== File summary =====
X3 --> U[全XML処理完了後\nfile_receipts サマリー更新]
H1 --> U
U --> C1

%% ===== Result =====
U --> V{HIAアップロード用XMLとして出力可能?}
V -->|Yes| W[XML出力待ち]
V -->|No| Y[エラー / 確認対象]

W --> END([終了])
Y --> END
```

---

## フローとスクリプトの対応

| 処理 | 主担当スクリプト | 主な更新テーブル | 主な参照テーブル |
| --- | --- | --- | --- |
| イベント・医療機関フォルダ探索 | `01_register_files.py` | なし | `dev_phr.event`, `health_exam_result.medical_folder_aliases` |
| ファイル検出・登録 | `01_register_files.py` | `health_exam_result.file_receipts`, `etl_runs` / `etl_errors` | なし |
| ZIP重複判定 | `01_register_files.py` | `etl_runs` / `etl_errors` | `health_exam_result.file_receipts` |
| ZIP展開・XML検出 | `02_import_xml_files.py` | `health_exam_result.xml_file_links`, `health_exam_result.file_receipts` | `health_exam_result.xml_ledger` |
| XML内容台帳登録 | `02_import_xml_files.py` | `health_exam_result.xml_ledger`, `health_exam_result.xml_file_links` | `health_exam_result.file_receipts` |
| 健診項目抽出 | `03_extract_item_values.py` | `health_exam_result.exam_item_values`, `health_exam_result.xml_ledger` | `dev_phr.exam_item_master` |
| 加入者照合 | `04_match_subscribers.py` | `health_exam_result.xml_ledger` | `dev_phr.subscribers` |
| 法定・特定・異常値チェック | `05_check_exam_results.py` | `health_exam_result.exam_check_results`, `health_exam_result.xml_ledger` | `dev_phr.exam_item_master`, `dev_phr.exam_item_group_*` 系 |
| XML出力可否整理 | `05_check_exam_results.py` または後続スクリプト | `health_exam_result.xml_ledger`, `health_exam_result.file_receipts` | `health_exam_result.exam_check_results` |

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

初期実装では、`file_receipts`、`xml_file_links`、`xml_ledger`、`exam_item_values`、`exam_check_results` を中心に、XML内容単位で一気通貫に処理する。

`file_receipts` は物理ファイル受領台帳、`xml_ledger` はXML内容の一意台帳、`xml_file_links` は物理ファイルとXML内容の対応台帳とする。

同一 `zip_sha256` のZIPは処理対象外としてスキップする。同一ZIPは、共有フォルダ上で `02_健診結果（編集）` へコピーする段階でも上書き確認が発生するため、初期実装では処理が必要かどうかを重視し、ETL実行ログ側でスキップしたことが確認できればよい。

同一 `xml_sha256` のXMLを別ZIP等で再受領した場合、`xml_ledger` は新規作成せず、`xml_file_links` のみ追加する。

ファイル単位のサマリーは、個別XML処理完了後に `file_receipts` へ集約する。

`zip_receipts` は独立テーブルとしては作成せず、ZIPは `file_receipts.file_type` による処理分岐として扱う。