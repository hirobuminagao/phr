# tokuho_xml_check（詳説版）

特定保健指導（CDA/HL7ベース）XMLの **検証・レポート化・初回⇔最終の突合・矛盾検出・プロセス集計** を行うスクリプト一式。  
Windows の VS Code + PowerShell 実行を想定しています。

---

## 目次
- [1. ゴール](#1-ゴール)
- [2. 必要環境](#2-必要環境)
- [3. ディレクトリ構成](#3-ディレクトリ構成)
- [4. セットアップ手順](#4-セットアップ手順)
- [5. 使い方](#5-使い方)
- [6. 出力物の仕様](#6-出力物の仕様)
- [7. 個人キーと擬似ID](#7-個人キーと擬似id)
- [8. 設定ファイルの詳細](#8-設定ファイルの詳細)
- [9. VS Code 連携（任意）](#9-vs-code-連携任意)
- [10. トラブルシュート](#10-トラブルシュート)
- [11. セキュリティ運用の要点](#11-セキュリティ運用の要点)
- [12. 運用Tips・拡張例](#12-運用tips拡張例)
- [付録A. サンプル設定一式](#付録a-サンプル設定一式)
- [付録B. 初回/最終の突合ロジック](#付録b-初回最終の突合ロジック)
- [CSV出力仕様](#csv出力仕様)

---

## 1. ゴール（旧スクリプトでできること）

本スクリプト（旧版）は、特定保健指導XMLを対象に以下を実現します。

### 1.1 XML検証・解析
- CDA/HL7形式XMLを読み込み
- XML構造の妥当性チェック
- 必須項目の存在確認
- エラー内容のログ出力（log.txt / detail.csv）

### 1.2 基本情報抽出
- 保険者番号・記号・番号の抽出
- 氏名カナ・生年月日・性別の取得
- 初回面接日 / 最終面接日の抽出
- 支援レベル（積極的 / 動機付け）の判定

### 1.3 個人識別処理
- person_key の生成（標準ルール）
- person_id（HMAC）の生成
- person_id_custom（Excel互換ID）の生成
- ID生成エラーの記録

### 1.4 初回⇔最終の突合
- 同一人物の初回XMLと最終XMLの紐付け
- フォルダ単位および全体単位での突合判定
- 初回のみ / 最終のみ / 両方ありの判定

### 1.5 アウトカム評価
- 各生活習慣項目（食事・運動・喫煙など）の目標/達成判定
- 腹囲・体重の改善判定
- アウトカムポイント（90060）の集計

### 1.6 矛盾検出
- 「目標なしなのに達成あり」などの論理矛盾検出
- カテゴリ別の矛盾判定（OK/NG）

### 1.7 プロセス評価
- 支援方法（個別・グループ・電話・メール）の集計
- 実施回数・時間の集計
- プロセスポイント（90040 / 90070）の集計

### 1.8 継続評価
- 初回～最終の継続日数算出
- 継続基準（days / calendar）による判定

### 1.9 レポート出力
以下のCSVを自動生成：

- export_shg_report  
  → XML単位の一覧（提出用）

- export_outcome_report  
  → 個人単位の突合・評価・矛盾・プロセス集計

- summary.csv / detail.csv  
  → 検証ログおよび詳細情報

### 1.10 セキュアな外部提供対応
- 擬似ID（HMAC）による匿名化
- カスタムIDとの併記による既存運用との互換性確保
---

## 2. 必要環境
- Python **3.9+**
- ライブラリ：`lxml`（必須）、`pandas`（任意／Excel出力）

```powershell
python -m pip install --upgrade pip
pip install lxml pandas
```

---

## 3. ディレクトリ構成
```
work_folder/
  tokuho_xml_check/
    check_tokuho_xml.py
    lib/
      pseudo_id.py
      custom_id.py
    input/
      <バンドル>/
        DATA/*.xml        ← 解析対象
        XSD/              ← 無視
        ix08_V08.xml      ← 無視
    out/                  ← 詳細出力（summary/detail/log/json）
    export_shg_report/    ← 提出レポート（タイムスタンプ付きCSV）
    export_outcome_report/← 初回/最終・矛盾・プロセス集計（タイムスタンプ付きCSV）
  mat/
    kenshin_item_master.csv
    header_xpath.json
    outcome_process_config.json
    id_config.json
    custom_id_config.json
    custom_id_mapping.json
```

**入力の想定**：`tokuho_xml_check/input/<バンドル>/DATA/*.xml` を再帰的に収集します。  
`XSD/` と `ix08_V08.xml` は対象外です。**「フォルダ名」列** は `<バンドル>` を採用します。

---

## 4. セットアップ手順
1. **Python仮想環境（任意）**
   ```powershell
   cd work_folder
   python -m venv .venv
   . .venv\Scripts\Activate.ps1
   python -m pip install --upgrade pip
   pip install lxml pandas
   ```
2. **設定を編集**
   - `mat/id_config.json` → **salt を必ず自社の秘密値に変更**
   - `mat/header_xpath.json` / `mat/outcome_process_config.json` → 実データのXMLタグに合わせて調整
   - （Excel互換IDを使う場合）`mat/custom_id_mapping.json` を乱数表に合わせて編集
3. **XMLを配置**
   - `tokuho_xml_check/input/<バンドル>/DATA/` に対象XMLを置く

---

## 5. 使い方
### 5.1 コマンド実行
```powershell
cd work_folder
python .\tokuho_xml_check\check_tokuho_xml.py `
  --xml_dir .\tokuho_xml_check\input `
  --master .\mat\kenshin_item_master.csv `
  --outdir .\tokuho_xml_check\out
```
> 単一XMLの場合は `--xml <path-to-xml>` を使用。

### 5.2 実行後の出力場所
- 詳細検証: `tokuho_xml_check/out/`（`summary.csv`, `detail.csv`, `log.txt`, `json/*`）
- 提出レポ: `tokuho_xml_check/export_shg_report/report_YYYYmmdd_HHMMSS.csv`
- 初回/最終: `tokuho_xml_check/export_outcome_report/outcome_process_YYYYmmdd_HHMMSS.csv`

---

## 6. 出力物の仕様
（※詳細は末尾「CSV出力仕様」を参照）

---

## 7. 個人キーと擬似ID
### 7.1 #person_key標準ルール
```
保険者番号(8桁ゼロ埋め)-保険証記号(正規化)-保険証番号(半角数字)-氏名カナ(全角カナ)-生年月日(YYYYMMDD)-性別(1/2)
```
- 保険者番号：数値化→8桁ゼロ埋め
- 保険証記号：**数字のみ→半角**, **数字以外含む→全角化（数字も全角）**
- 保険証番号：半角数字
- 氏名カナ：全角カタカナ（ひらがな→カタカナ、スペース圧縮）
- 生年月日：YYYYMMDD
- 性別：厚労省準拠 `1=男, 2=女`

### 7.2 擬似IDの2系統
- **person_id（HMAC）**：不可逆・安全。`mat/id_config.json` の `salt/length/alphabet` で制御。
- **person_id_custom（Excel互換）**：足し算→掛け算→乱数表置換。`mat/custom_id_config.json` と `mat/custom_id_mapping.json` で制御。

> 運用方針：当面は **両方併記**、将来は `person_id` に一本化。

---

## 8. 設定ファイルの詳細
（略：既存READMEの内容）

---

## 9. VS Code 連携（任意）
（略：既存READMEの内容）

---

## 10. トラブルシュート
（略：既存READMEの内容）

---

## 11. セキュリティ運用の要点
（略：既存READMEの内容）

---

## 12. 運用Tips・拡張例
（略：既存READMEの内容）

---

## 付録A. サンプル設定一式
（略：既存READMEの内容）

---

## 付録B. 初回/最終の突合ロジック
（略：既存READMEの内容）

---

## CSV出力仕様

本ツールは解析結果を以下の2種類のCSVに出力します。

---

### export_shg_report

| 項目 | 意味 |
|------|------|
| folder | XML が置かれていたフォルダ名（対象者バンドル単位） |
| file | XMLファイル名 |
| person_id_custom | 外部ジェネレータで算出したカスタムID |
| person_id_custom_error | カスタムID生成時のエラー内容（正常なら空） |
| report_code | 報告種別コード（21=初回, 22=最終） |
| insurer | 保険者番号 |
| symbol | 記号（被保険者証） |
| number | 番号（被保険者証） |
| birth_raw | XMLに記載の生年月日（元データ） |
| birth | 整形済み生年月日（YYYY-MM-DD形式） |
| initial_date | 初回面接日（90030セクション） |
| final_date | 最終面接日（90060セクション） |
| level_code | 支援レベルコード（1=積極的支援, 2=動機付け支援） |
| level_text | 支援レベル名称 |
| ticket_no | 券番号 |
| ticket_exp | 券有効期限 |
| error | XML読み込み時のエラー内容 |

---

### export_outcome_report

| 項目 | 意味 |
|------|------|
| person_key | 保険者番号＋記号＋番号＋カナ＋生年月日＋性別で作成したキー |
| person_id | person_key（旧仕様では完全一致） |
| person_id_custom | カスタムID |
| person_id_custom_error | カスタムID生成時のエラー内容 |
| db_ticket_no | DBから取得した利用券番号（存在すれば） |
| db_ticket_exp | DBから取得した利用券有効期限 |
| initial_xml | 初回XMLファイル名 |
| final_xml | 最終XMLファイル名 |
| initial_exists | 同一person_keyに初回XMLが存在するか（Yes/No） |
| level_code | 支援レベルコード（1=積極的, 2=動機付け） |
| level_text | 支援レベル名称 |
| initial_date | 初回面接日 |
| final_date | 最終面接日 |
| 継続日数 | 初回～最終の経過日数 |
| 継続判定モード | 判定方法（days/calendar） |
| 継続しきい値 | 判定基準 |
| 継続期間_XML判定 | 継続基準を満たしたか（OK/NG/N/A） |
| initial_same_folder | 同一フォルダに初回XMLがあるか |
| 矛盾(目標なし達成あり) | 目標なしで達成ありの矛盾（Yes/No） |
| conflict_腹囲体重_XML判定 | 腹囲・体重の矛盾判定 |
| conflict_食_XML判定 | 食習慣の矛盾判定 |
| conflict_運動_XML判定 | 運動習慣の矛盾判定 |
| conflict_喫煙_XML判定 | 喫煙習慣の矛盾判定 |
| conflict_休養_XML判定 | 休養習慣の矛盾判定 |
| conflict_その他_XML判定 | その他の矛盾判定 |
| goal_腹囲体重 | 初回目標（腹囲・体重） |
| goal_食 | 初回目標（食習慣） |
| goal_運動 | 初回目標（運動習慣） |
| goal_喫煙 | 初回目標（喫煙習慣） |
| goal_休養 | 初回目標（休養習慣） |
| goal_その他 | 初回目標（その他） |
| achieve_腹囲体重 | 最終達成（腹囲・体重） |
| achieve_腹囲体重_内容 | 改善内容（1cm/1kg, 2cm/2kg等） |
| achieve_食 | 最終達成（食習慣） |
| achieve_運動 | 最終達成（運動習慣） |
| achieve_喫煙 | 最終達成（喫煙習慣） |
| achieve_休養 | 最終達成（休養習慣） |
| achieve_その他 | 最終達成（その他） |
| 健診時_腹囲(cm) | 健診時腹囲（旧は未設定） |
| 最終_腹囲(cm) | 最終腹囲 |
| 健診時_体重(kg) | 健診時体重（旧は未設定） |
| 最終_体重(kg) | 最終体重 |
| final_outcome_summary | 最終達成一覧 |
| initial_goal_summary | 初回目標一覧 |
| outcome_total_points | アウトカムポイント（90060） |
| process_source | プロセス出典（90040 / 90070_evn / none） |
| process_total_points | プロセスポイント合計 |
| process_total_minutes | プロセス時間合計 |
| proc_個別支援(対面)_回数 | 個別支援（対面）回数 |
| proc_個別支援(対面)_分 | 個別支援（対面）時間 |
| proc_個別支援(遠隔)_回数 | 個別支援（遠隔）回数 |
| proc_個別支援(遠隔)_分 | 個別支援（遠隔）時間 |
| proc_グループ支援(対面)_回数 | グループ支援（対面）回数 |
| proc_グループ支援(対面)_分 | グループ支援（対面）時間 |
| proc_グループ支援(遠隔)_回数 | グループ支援（遠隔）回数 |
| proc_グループ支援(遠隔)_分 | グループ支援（遠隔）時間 |
| proc_電話_回数 | 電話支援回数 |
| proc_電話_分 | 電話支援時間 |
| proc_電子メール等_回数 | 電子メール等支援回数 |
| proc_電子メール等_分 | 電子メール等支援時間 |
| grand_total_points | アウトカム＋プロセス合計ポイント |

---

### level_code / level_text のロジック

- 取得元は 90010（保健指導情報）の保健指導区分
- `1020000001` を対象とする
- `level_code`
  - `1` = `積極的支援`
  - `2` = `動機付け支援`
  - `3` = `動機付け支援相当`
- 旧スクリプトでは final XML が存在する場合のみ出力し、final が無い場合は空とする

### initial_date / final_date のロジック

- `initial_date`
  - final XML がある場合は final 側で保持している初回面接日を採用
  - final が無い場合は initial XML 側の初回面接日を採用
- `final_date`
  - final XML がある場合のみ最終面接日を出力
  - final が無い場合は空とする

### process_source のロジック

`process_source` は、プロセス評価の代表取得元を示す。

判定順は以下とする：

| 条件 | process_source |
|------|----------------|
| 90040 にポイントがある | `90040` |
| 90040 は無いが 90070 にポイントがある | `90070_evn` |
| 上記いずれでもない | `none` |

補足：
- 旧スクリプトでは 90040 を優先する
- `90070_evn` は、90040 の代替ソースとして 90070 を使用したことを示す

### process_total_points / process_total_minutes のロジック

- `process_total_points`
  - `process_source = 90040` の場合は `90040._total_points`
  - `process_source = 90070_evn` の場合は `90070._total_points`
  - `process_source = none` の場合は `0`
- `process_total_minutes`
  - `process_source = 90040` の場合は `90040._total_minutes`
  - `process_source = 90070_evn` の場合は `sum(90070.durations_min)`
  - `process_source = none` の場合は `0`

### grand_total_points のロジック

`grand_total_points` は、検算用の合計値として以下で算出する。

- `grand_total_points = outcome_total_points + process_total_points`

補足：
- XML内の集計済み値をそのまま採用する意図ではなく、CSV側で再計算した値を出力する

### goal_* のロジック

カテゴリは以下の 6 つに正規化する。

- 腹囲体重
- 食
- 運動
- 喫煙
- 休養
- その他

初回目標 (`init_goals`) をカテゴリへ寄せた `gmap` を作り、以下で出力する。

- `True` → `目標`
- `False` → `非目標`

### achieve_* のロジック

最終評価 (`final_outs`) を同じ 6 カテゴリへ寄せた `amap` を作り、以下で出力する。

- final XML が無い場合 → 空
- final XML があり `True` → `達成`
- final XML があり `False` → `未`

#### achieve_腹囲体重_内容

腹囲・体重改善コードから以下で出力する。

- `1` → `1cm/1kg`
- `2` → `2cm/2kg`
- それ以外 → `未達成`

### conflict_* のロジック

矛盾判定は「目標なしなのに達成あり」で判定する。

- `conflict = (not goal) and achieve`

カテゴリごとの出力は以下。

- final XML が無い場合 → 空
- conflict = `True` → `NG`
- conflict = `False` → `OK`

#### 矛盾(目標なし達成あり)

- `conflict_*` のどれかが `NG` なら `Yes: 項目一覧`
- 矛盾が無ければ `No`
- final XML が無ければ空

### 継続判定のロジック

`compute_duration_verdict()` により以下を算出する。

- `継続日数`
- `継続判定モード`
- `継続しきい値`
- `継続期間_XML判定`

判定モードは以下の 2 種。

- `days`
- `calendar`

#### days

- `継続日数 >= threshold_days` で判定する
- 例：93日以上

#### calendar

- 初回日付に月数を足した日付以上かで判定する
- 例：3カ月（暦）以上

#### 動機付け支援の扱い

- `level_code == 2`（動機付け支援）の場合
  - `継続期間_XML判定 = N/A(動機付け)`
  - `継続判定モード = ""`
  - `継続しきい値 = ""`

### initial_same_folder のロジック

- final と initial の両方がある場合のみ判定する
- `initial.folder == final.folder`
  - 一致 → `Yes`
  - 不一致 → `No`
- final が無い場合は空とする

### proc_* のロジック

`proc_*` 列は支援方法ごとの回数・時間を表す。

- 代表ソースが 90040 の場合でも、詳細列 `proc_*` は 90070 集計値を使用する
- 対象は以下の支援方法
  - 個別支援（対面）
  - 個別支援（遠隔）
  - グループ支援（対面）
  - グループ支援（遠隔）
  - 電話
  - 電子メール等

#### proc_電子メール等_分

- 厚生労働省定義上、電子メール等には実施時間の定義がない
- そのため、旧列互換を保つ場合でも `0` 固定で扱う

### 初回面談方式_* のロジック

`extract_initial_interview_mode()` により初回面談方式を取得する。

- 初回XMLがある場合
  - `初回面談方式_初回XML_コード`
  - `初回面談方式_初回XML_内容`
- 最終XMLがある場合
  - `初回面談方式_最終XML_コード`
  - `初回面談方式_最終XML_内容`

### カスタムID生成について

本ツールでは個人識別を安定させるために、外部スクリプト `custom_id_gen.py` を呼び出して  
カスタムID（person_id_custom）を生成します。

- `custom_id_gen.py` は `check_tokuho_xml.py` と同じディレクトリに配置してください  
- コマンドライン引数で以下を受け取ります  
  - `--insurer` 保険者番号  
  - `--symbol` 記号  
  - `--insured` 番号  
  - `--birth` 生年月日 (YYYYMMDD)  
  - `--mat` 設定ディレクトリパス  
- 出力:  
  - 通常時 → ID文字列を標準出力  
  - `--trace` オプション → デバッグ情報を標準エラー出力  
  - `--jsonout` オプション → JSON 形式 `{"id":"..."}` を標準出力  

生成したIDは `export_shg_report` および `export_outcome_report` の  
`person_id_custom` 列に出力されます。  
エラーがあった場合は `person_id_custom_error` に内容が記録されます。

---

### 特記事項

- `initial_exists` と `initial_same_folder` は別物  
  - `initial_exists`: **人全体**として初回XMLが存在するか  
  - `initial_same_folder`: **同じフォルダ内**に初回があるか  
- **初回のみ** → 行を出力。`initial_same_folder` は空、矛盾や達成は空。  
- **最終のみ** → 初回が無ければ、最終XMLに記載された目標を利用して矛盾判定。  
- **初回＋最終（同一フォルダ）** → 通常パターン（初回→最終比較）。  
