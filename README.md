# phr

本リポジトリは、健診データ（主に健診機関から受領するZIP/XML）を取り込み、台帳化・抽出・正規化・判定・出力（将来）までを行うための基盤を管理する。

## 目的
- 受領した健診データ（ZIP/XML/提出CSVなど）を **再現可能な手順** で処理する
- 「いつ・何を・どう処理したか」を **DB上の台帳/ログで追跡可能** にする
- 将来のリファクタ/機能追加を行う際の **基準点（Freeze）** を提供する

## Freeze（基準点 / 前提基盤）
本リポジトリは、運用で一度回してFix済みの状態を「基準点」として固定し、以降の改修はこの基準点との差分として管理する。

- Freeze対象: `scripts/kenshin_list_pydir/` （スクリプト一式 + kenshin_lib）
- Freezeの意味:
  - 既存の挙動を変えない（互換性を壊さない）ことを最優先
  - 仕様/設計意図は docstring と ADR によって明文化する
  - 改修/リファクタは「やる」が、**Freezeの定義（互換性/再現性）を壊さない範囲で進める**

※ Freezeは「開発禁止」ではない。あくまで **前提・基準・比較の軸** を固定する。

### v1.0-freeze（2026-02-12 時点の追加基点）
2026-02-12 に `v1.0-freeze` タグを付与し、`scripts/work_folder/` 系の「現状の意味（契約）」も凍結した。

- 対象: `scripts/work_folder/`
  - import/apply スクリプト（hub/fund）と共通lib
  - `mat/`（person_id_custom 生成仕様: `custom_id_config.json` / `custom_id_mapping.json` / README）
- 前提: v1.0 現状では work_folder が参照する主要テーブルはすべて `dev_phr` スキーマに存在する
- 目的: リファクタではなく「現状の意味の固定」（docstring/README による明文化）
- 位置づけ: `scripts/fund_enrollee_loader/` は SQLite 前提の legacy 系（v1.0 正規運用対象外）

※ タグ確認: `git show --stat v1.0-freeze`

## ディレクトリ構成（重要）
- `scripts/kenshin_list_pydir/`
  - `scripts/` : 実行スクリプト群（手動キック前提のものが中心）
  - `kenshin_lib/` : 共通ライブラリ（DBアクセス、ZIP処理、OID/補助など）
  - `.env` : 環境変数（ローカル運用用。機微情報はコミットしない）
  - `.env.template` : `.env` のひな形（v1.0基準。置換して使用）
- `sql/`
  - `sql/meta/dev_phr/` : dev_phr の初期マスタCSV（core/optional）
  - （work_other等の運用系は必要に応じて追加）

## 主要コンポーネント（kenshin_list_pydir）
### 共有フォルダ観測（受領前段）
共有フォルダをスキャンして「存在するファイル」を台帳化する。

- `scripts/medi_shared_files_scan.py`
  - 共有フォルダを走査して `medi_shared_files` をUPSERTする
  - 探索パターンは原則 ZIP のみ（解凍済みフォルダ地獄対策）

### 共有フォルダファイルの前処理（任意）
- `scripts/medi_shared_files_hash_zip.py`
  - ZIPのsha256算出など（重い処理は段階化）
- `scripts/medi_shared_files_auto_judge.py`
  - ZIP内にXMLがあるか等の軽量判定・台帳反映（manual優先）
- `scripts/medi_shared_files_copy_to_input.py`
  - input側へコピー（運用ステージを進める）


### medi取り込み（DB台帳化・抽出）
- ZIP受領・XML受領・抽出・ログ記録・LSIO判定など（work_other DBが中心）
- `kenshin_lib/medi/*` に処理部品・DBアクセスが集約される

#### v1 実行順（shared→input→receipts→ledger）

`medi_xml_ledger` への記帳は **`scripts/kenshin_list_pydir/scripts/medi_zip_import.py` の `MEDI_IMPORT_MODE=XML_EXTRACT`（内部で `kenshin_lib/medi/xml_extract.py::xml_extract_phase` を実行）** により行う。

1. `scripts/kenshin_list_pydir/scripts/medi_shared_files_scan.py`
   - 共有フォルダを走査し、`medi_shared_files` に **NEW** を積む（観測台帳）
2. `scripts/kenshin_list_pydir/scripts/medi_shared_files_hash_zip.py`
   - `medi_shared_files.sha256` を埋める（hashフェーズ）
3. `scripts/kenshin_list_pydir/scripts/medi_shared_files_auto_judge.py`
   - ZIPを軽く検査して `zip_has_xml` を埋め、`auto_judgement='KENSHIN'` を付与（manualがあれば尊重）
4. `scripts/kenshin_list_pydir/scripts/medi_shared_files_copy_to_input.py`
   - 条件を満たすZIPのみ `MEDI_IMPORT_INPUT_ROOT/<dst_folder_norm>/` にコピーし、`stage_status='INPUT_COPIED'` に進める
5. `scripts/kenshin_list_pydir/scripts/medi_zip_import.py`（`MEDI_IMPORT_MODE=ZIP_IMPORT`）
   - input配下のZIPを走査し、`medi_zip_receipts` をUPSERT
   - `MEDI_IMPORT_XML_ENABLED=true` の場合、ZIP内XMLを棚卸しして `medi_xml_receipts` を基本 **PENDING** で作成（壊れたXMLのみERROR）
6. `scripts/kenshin_list_pydir/scripts/medi_zip_import.py`（`MEDI_IMPORT_MODE=XML_EXTRACT` または `FULL`）
   - `medi_xml_receipts` の `target_status`（既定PENDING）を拾い、抽出フェーズを実行
   - **`medi_xml_ledger` をUPSERT（= ledger記帳の実体）**


#### 6以降（抽出値→正規化/照合用埋め）

7. `scripts/kenshin_list_pydir/scripts/medi_xml_item_extract.py`
   - `medi_xml_receipts.status='OK'` を対象に、XML内の observation/value 等を抽出して **`medi_xml_item_values`（縦持ち）** へUPSERT
   - ここは「健診結果の値系（項目値）」が主。`medi_xml_ledger` への記帳は行わない

8. `scripts/kenshin_list_pydir/scripts/normalize_item_values.py`
   - `medi_xml_item_values` の値を正規化（型/PQ/CD/CO/ST、表記ゆれ、単位、NULL扱いなど）

9. `scripts/kenshin_list_pydir/scripts/normalize_db_update.py`
   - `medi_xml_ledger` 等の **照合用（match系）** を後処理で埋める（例: `insurance_symbol_match`, `insurance_number_match`, `name_kana_match`）

※ 7〜9 は運用上「必要な範囲だけ」手動キックで良い。v1.0-freeze ではロジックの互換性を最優先し、順番のみを固定する。

## DB（概要）
本基盤は MySQL を前提とする。
- `dev_phr` : マスタ・正規化ルール等（GitにCSVで保持）
- `work_other` : 取り込み運用の台帳・ログ（運用で増える）
- 主なテーブル例:
  - 受領/台帳: `medi_zip_receipts`, `medi_xml_receipts`, `medi_xml_ledger`
  - 実行: `medi_import_runs`, `medi_zip_receipt_runs`, `medi_xml_receipt_runs`
  - ログ: `medi_xml_process_logs`
  - 抽出値: `medi_xml_item_values`
  - 判定: `medi_lsio_identity_presence`, `medi_lsio_missing_items`
  - 共有観測: `medi_shared_files`, `medi_shared_folder_aliases`, `medi_zip_passwords`

## 設定（.env / .env.template）
`kenshin_list_pydir/.env` を読み込む（各スクリプトの先頭で `load_dotenv()` している想定）。
機微情報（パスワード等）はコミットしない。

- ひな形: `kenshin_list_pydir/.env.template`（v1.0の基準。**.env を作るときはこれをコピーして置換**）
- 実体: `kenshin_list_pydir/.env`（ローカル運用用。Git管理外）

### 置換ルール（.env.template）
テンプレ内の以下は「必ず自分の環境に置換」する。
- `__FILE_ME__` : 値の入力が必須（未設定のまま動かさない）
- `__PATH_SCANDIR__` / `__PATH_OUTPUT__` : 共有/出力のパス（社内サーバ名が入る場合は機微扱い）

### よくある落とし穴（v1.0時点の仕様を明文化）
- **LOG_LEVEL の重複**: `.env` に同名キーを複数書くと「後勝ち」になり、意図せず DEBUG になることがある。
  - 推奨: `.env` では `LOG_LEVEL` は1回だけにし、調査時はローカルで一時的に上書きする。
- **OS依存パス**: `MEDI_IMPORT_TEMP_ROOT` などが Windows 例のままになりやすい。
  - 例) Windows: `C:/_medi_tmp` / macOS: `/tmp/_medi_tmp`
- **相対パス依存**: `MEDI_IMPORT_INPUT_ROOT=medi_input` や `MEDI_IMPORT_XSD_ROOT=XSD` は相対パス前提。
  - 推奨: `scripts/kenshin_list_pydir/` をカレントとして実行する（=ここを基準に相対解決される）。
- **DB接続キーが複数系統ある**（統合しない。v1.0は現状をFreezeして明示する）:
  - `MEDI_IMPORT_DB_*` : 取込/台帳/ログ（主に `work_other`）
  - `SUBMIT_DB_*` : 提出CSV取込（主に `work_other`）
  - `PHR_MYSQL_*` : マスタ参照（主に `dev_phr`）
  - `MYSQL_*` : 汎用（スクリプトによって参照。READMEで用途を見てから使う）

### 代表的なキー
#### 共有スキャン
- `MEDI_SHARED_ROOT` : 共有フォルダのルート
- `MEDI_SHARED_SCAN_EXTS` : 走査拡張子（推奨: `zip`）
- `MEDI_SHARED_SCAN_LIMIT` : 0=無制限 / >0=件数制限

#### DB接続（例）
- `MEDI_IMPORT_DB_HOST`
- `MEDI_IMPORT_DB_PORT`
- `MEDI_IMPORT_DB_NAME`
- `MEDI_IMPORT_DB_USER`
- `MEDI_IMPORT_DB_PASSWORD`

#### XSD / 入出力
- `MEDI_IMPORT_XSD_ROOT` : XSDディレクトリ（既定: `XSD`）
- `MEDI_IMPORT_XSD_MAIN` : メインXSD（例: `hc08_V08.xsd`）
- `MEDI_IMPORT_INPUT_ROOT` : 取込入力のルート（既定: `medi_input`）

## 実行方針
- 原則「手動キック」前提（運用の安全性と再現性を優先）
- 重要処理は DB に run/log を残す（あとから説明できる形）

## ドキュメント（ADR）
設計判断・凍結点・方針は `docs/adr/` に記録する。

- ADRに残すべき論点（v1.0-freeze で確定したもの）:
  - work_folder のDB座標: 主要テーブルは dev_phr スキーマ
  - mat の配置と変更ポリシー: 変更＝ID仕様変更（v2扱い）
  - legacy（SQLite）ルートの位置づけ: fund_enrollee_loader は正規運用対象外

- ADRは「なぜそうしたか」を残す文書
- READMEは「何があり、どう使うか」を説明する文書

## バージョン/タグ運用（v1.0の付け方）
- v1.0 は「README + ADR を含む Freeze定義が揃ったコミット」に付与する
- タグ付与は以下を推奨:
  1) README/ADR をコミット
  2) `git tag -a v1.0 -m "Baseline freeze: phr/kenshin_list_pydir"`
  3) `git push origin v1.0`


## HIA_fund_ledger_xml（設計メモ / 追加タスクの前提）
HIA から月締めでダウンロードした健診結果 ZIP/XML を、Fund 向け納品のための台帳として扱う追加タスク。
本タスクは `medi_*` 系とは**別フロー**で管理するが、XML 由来の項目名・正規化の考え方は既存の `medi_*` とできるだけ揃える。

### 位置づけ
- 名称: `HIA_fund_ledger_xml`
- 目的:
  - HIA から受領した月締め ZIP/XML を台帳化する
  - 人単位・年度単位で「初回登場」「過去登場」を判定できるようにする
  - 指定月の納品対象について「過去に存在しない人のみ」を再抽出して ZIP 再構成できるようにする
- 方針:
  - `medi_*` とは別の管理単位とする
  - ただし人照合や XML 項目の正規化ロジックは共通化・再利用を前提にする
  - DDL / 実装は**freeze 前提の設計固め後**に作成する

### 現時点の freeze 方針
- まだ DB テーブルは作成しない
- まだ本実装スクリプトは作成しない
- まずは `phr` リポジトリ配下に「設計・前提・判断」を残す
- 設計メモを先に固定し、その後 DDL → スクリプトの順で進める

### 設計の芯（現時点）
- 同一人物判定は以下を前提にする
  - `person_id_custom + name_kana_norm + gender_code + exam_year`
- `person_id_custom` の元データは以下とする
  - 保険者番号
  - 記号
  - 番号
  - 生年月日
- 人台帳は**元の値**と**正規化後の値**の両方を保持する
- XML 読込時も、台帳登録時と**同じ正規化ロジック**で照合する
- 人台帳は `person + exam_year` 単位で保持し、将来は別に健診イベント台帳を追加して年 2 回以上の健診に対応する

### 日時・順序の扱い
- `dl_date` はフォルダ名から取得する業務日付とする
- `dl_date` はスクリプト実行日時とは別物とする
- 初回登場 / 最終登場の判定は `dl_date` と送信回数部分（厚労省ファイル伝送仕様由来）で行う
- `created_at` / `updated_at` は DB 記帳・更新の監査用タイムスタンプとして保持する
- `created_at` は順序判定には使わない

### エラー方針
- `genderCode` 空はエラー
- `exam_date` 無しはエラー
- 上記エラーが 1 件でもある ZIP は**ZIP 単位で未記帳**とする
- エラー内容は ZIP / XML 単位で明確に残し、修正後に再アップロード・再取込する
- 途中まで台帳記帳しない（all-or-nothing）

### 年度の扱い
- 健診年度はカレンダー年ではなく設定値で判定する
- 初期実装では `.env` 等で年度開始日を設定できるようにする
- 例: `2025年度 = 2025-04-01 〜 2026-03-31`

### 追加予定のドキュメント置き場（案）
今後、以下のような設計メモ / ADR / DDL を追加していく前提とする。

```text
/docs/spec/hia_fund_ledger_xml/
  README.md
  flow_overview.md
  identity_and_normalization.md
  error_policy.md
  year_rule.md
  delivery_exclusion_rules.md
/docs/adr/
  0006-hia-fund-ledger-xml-v1-policy.md
/sql/ddl/work_other/
  hia_fund_ledger_xml/
```

### この追加タスクで最初に固めるもの
1. 人照合キー仕様
2. 正規化仕様（元値 / 正規化値 / 照合時の共通関数）
3. ZIP 単位エラー方針
4. 年度判定仕様
5. 人台帳・XML 台帳・ZIP エラー台帳の役割分担