# health_exam_result v2 初期処理フロー

このドキュメントは、`health_exam_result v2` の初期実装におけるローカルシステム処理フローを整理する。

未着管理、医療機関回答待ち、再提出管理、HIAアップロード後の業務管理は初期実装フローから分離し、別途業務フローとして整理する。

---

## 初期実装スコープ

初期実装では、以下を成立させることを目的とする。

- 設定YAMLの `event_id` から `event.result_root_path` を取得する。
- `medical_folder_aliases` を参照し、イベント配下の医療機関フォルダを探索する。
- 受領・編集済みファイルを毎回フルスキャンし、未登録ファイルのみ `file_receipts` に登録する。
- `01_scan_files.py` では `work` へのコピーを行わない。
- 登録済みファイルは処理対象外としてスキップし、ETL実行ログ側でスキップ件数・理由を確認できるようにする。
- ZIPまたはXMLファイルからXMLを検出する。
- XML内容は `xml_sha256` で一意判定し、`xml_ledger` に登録する。
- 物理ファイルとXML内容の対応を `xml_file_links` に登録する。
- 同一 `xml_sha256` のXMLを別ZIP等で再受領した場合は、`xml_ledger` を新規作成せず、`xml_file_links` のみ追加する。
- XML基本情報から `subscribers.id` を解決する。
- XMLに実際に存在した健診値を `exam_item_values` に登録する。
- 法定健診・特定健診・異常値チェックを行い、`exam_check_results` に登録する。
- `xml_ledger` と `file_receipts` に処理結果を集約する。
- `xml_export_status` により、HIAアップロード用XMLの出力状態を整理する。

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
B --> C[01_scan_files.py\nフルスキャン\nworkコピーなし]

%% ===== Duplicate ZIP branch =====
C --> Z{登録済みファイル?}
Z -->|Yes| Z1[処理スキップ\netl_runs / etl_errors で記録]
Z1 --> END
Z -->|No| C1[file_receipts 登録\netl_run_id 保持]
C1 --> C2[(health_exam_result.file_receipts)]
C2 --> D[02_import_xml.py\n処理直前にworkへ一時コピー]

%% ===== File type branch =====
D --> D1{file_type}
D1 -->|ZIP| E[ZIP展開\nXML検出]
D1 -->|XML| F[XML検出]
D1 -->|CSV| G[CSV取込\n将来対応]
G --> G1[初期実装では対象外]
G1 --> WDEL

%% ===== XML sha branch =====
E --> X[xml_sha256 計算]
F --> X
X --> X1{xml_sha256 既存一致?}

X1 -->|Yes| X2[xml_file_links 追加のみ\nxml_ledger / exam_item_values は重複作成しない]
X2 --> X3[(health_exam_result.xml_file_links)]
X2 --> WDEL[work削除\n--keep-work時のみ保持]

X1 -->|No| H[02_import_xml.py\nxml_ledger 登録]
H --> H1[(health_exam_result.xml_ledger)]
H --> X4[xml_file_links 追加]
X4 --> X3

%% ===== XML per person =====
H1 --> I[XML基本情報抽出\n保険者番号・記号・番号・氏名カナ・生年月日・性別・健診日]
I --> J[02_import_xml.py\n加入者照合]
J --> J1[(dev_phr.subscribers 参照)]
J --> K[xml_ledger 更新\nsubscriber_id / identity_hash / match_status]

%% ===== Item extraction =====
K --> L[02_import_xml.py\n健診項目抽出]
L --> L1[(dev_phr.exam_item_master 参照)]
L --> M[(health_exam_result.exam_item_values)]
M --> M1[work削除\n--keep-work時のみ保持]

%% ===== Checks =====
M1 --> N[03_check_exam_results.py\nDB上の値で項目単位チェック\nXML再読込なし]
N --> N1[(dev_phr.exam_item_group_* 系 参照)]
N --> O[法定健診チェック]
N --> P[特定健診チェック]
N --> Q[異常値チェック]

O --> R[XML/受診者単位チェック集約]
P --> R
Q --> R

R --> S[(health_exam_result.exam_check_results)]
R --> T[xml_ledger 更新\ncheck_status / xml_export_status READY等 / summary]
T --> U

%% ===== File summary =====
WDEL --> U[全XML処理完了後\nfile_receipts サマリー更新]

%% ===== Result =====
U --> V{xml_export_status = READY?}
V -->|Yes| W[04_export_hia_xml.py\nHIAアップロード用XML生成]
V -->|No| Y[エラー / 確認対象]

W --> END([終了])
Y --> END
```

---

## フローとスクリプトの対応

| 処理 | 主担当スクリプト | 主な更新テーブル | 主な参照テーブル |
| --- | --- | --- | --- |
| イベント・医療機関フォルダ探索 | `01_scan_files.py` | なし | `dev_phr.event`, `health_exam_result.medical_folder_aliases` |
| ファイル検出・登録 | `01_scan_files.py` | `health_exam_result.file_receipts`, `etl_runs` / `etl_errors` | `health_exam_result.file_receipts` |
| work一時コピー | `02_import_xml.py` | なし | `health_exam_result.file_receipts` |
| ZIP展開・XML検出 | `02_import_xml.py` | `health_exam_result.file_receipts` | `health_exam_result.file_receipts` |
| XML内容台帳登録 | `02_import_xml.py` | `health_exam_result.xml_ledger`, `health_exam_result.xml_file_links` | `health_exam_result.file_receipts`, `health_exam_result.xml_ledger` |
| XML基本情報抽出・加入者照合 | `02_import_xml.py` | `health_exam_result.xml_ledger` | `dev_phr.subscribers` |
| 健診項目抽出 | `02_import_xml.py` | `health_exam_result.exam_item_values`, `health_exam_result.xml_ledger` | `dev_phr.exam_item_master` |
| 法定・特定・異常値チェック | `03_check_exam_results.py` | `health_exam_result.exam_check_results`, `health_exam_result.xml_ledger` | `health_exam_result.exam_item_values`, `dev_phr.exam_item_group_*` 系 |
| XML出力状態整理 | `03_check_exam_results.py` | `health_exam_result.xml_ledger` | `health_exam_result.exam_check_results` |
| HIAアップロード用XML生成 | `04_export_hia_xml.py` | 出力ファイル、`health_exam_result.xml_ledger` | `health_exam_result.xml_ledger`, `health_exam_result.exam_item_values`, `health_exam_result.exam_check_results` |

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

登録済みファイルは処理対象外としてスキップする。同一ZIPは、共有フォルダ上で `02_健診結果（編集）` へコピーする段階でも上書き確認が発生するため、初期実装では処理が必要かどうかを重視し、ETL実行ログ側でスキップしたことが確認できればよい。

同一 `xml_sha256` のXMLを別ZIP等で再受領した場合、`xml_ledger` は新規作成せず、`xml_file_links` のみ追加する。

ファイル単位のサマリーは、個別XML処理完了後に `file_receipts` へ集約する。

`work` 領域は恒久保存領域ではなく、`02_import_xml.py` が処理直前に一時コピー・展開するためだけに使う。通常は処理完了後に削除し、デバッグ時のみ `--keep-work` のような明示オプションで保持する。

`04_export_hia_xml.py` は、医療機関フォルダ配下の `03_健診結果（アップロード）` にRun単位ディレクトリを作成し、`<event.result_root_path>/<医療機関フォルダ>/03_健診結果（アップロード）/yyyymmdd_hhmmss_<run_id>/<xxx.zip>` へ出力する。既存出力ファイルは上書きせず、出力済みファイルの削除・整理は運用側の責務とする。

`file_receipts` は物理ファイル単位、`xml_ledger` はXML内容単位の機械的状態を管理する。人＋イベント単位の最終完了状態は将来の人＋イベント台帳で扱う前提とし、機械的な処理状態と人間の業務確認状態は混在させない。

`zip_receipts` は独立テーブルとしては作成せず、ZIPは `file_receipts.file_type` による処理分岐として扱う。
