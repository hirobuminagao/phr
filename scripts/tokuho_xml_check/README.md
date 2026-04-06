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

## 1. ゴール
- **提出用の集約CSV** を自動生成（提出先ごとにヘッダXPath差し替え対応可能）
- **初回⇔最終の突合** と **アウトカム矛盾検出**、**プロセス集計** を自動化
- **#person_key標準ルール** に基づく擬似ID（HMAC）を出力し、外部提供時も安全に集計可能
- 既存Excel運用互換の **独自ID（乱数表ロジック）** も併記出力し移行を円滑化

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
| person_id | person_keyと同じ（将来拡張用の別ID欄） |
| person_id_custom | カスタムID |
| person_id_custom_error | カスタムID生成時のエラー内容 |
| final_xml | 最終XMLファイル名（無ければ空） |
| initial_exists | 同一person_keyに初回XMLが存在するか（Yes/No） |
| level_code | 支援レベルコード（finalが無ければ空） |
| level_text | 支援レベル名称 |
| initial_date | 初回面接日（finalがあればfinal側、無ければinitial側） |
| final_date | 最終面接日（finalがあれば取得、無ければ空） |
| 継続日数 | 初回～最終の経過日数（finalが無ければ空） |
| 継続判定モード | 判定方法（days/calendar） |
| 継続しきい値 | 判定基準（日数 or 月数） |
| 継続期間_XML判定 | 継続基準を満たしたか（OK/NG/N/A） |
| initial_same_folder | 同じフォルダ内に初回XMLがあるか（Yes/No、finalが無ければ空） |
| 矛盾(目標なし達成あり) | 初回で目標無しだが最終で達成ありの矛盾（Yes:項目/No/空） |
| conflict_〇〇_XML判定 | 各カテゴリごとの矛盾判定（OK/NG/空） |
| goal_〇〇 | 初回の目標設定（目標/非目標） |
| achieve_〇〇 | 最終の達成結果（達成/未/空） |
| achieve_腹囲体重_内容 | 腹囲体重の達成内容（1cm/1kg, 2cm/2kg, 未達成） |
| final_outcome_summary | 最終XMLの達成一覧（key:達成/未） |
| initial_goal_summary | 初回XMLの目標一覧（key:目標/非目標） |
| outcome_total_points | 最終アウトカムポイント（90060） |
| process_source | プロセス情報の出典（90040 or 90070_evn or none） |
| process_total_points | プロセスポイント合計 |
| process_total_minutes | プロセス時間合計 |
| proc_xxx | 支援方法ごとの回数・時間（個別/グループ/電話/メール等） |
| grand_total_points | プロセスポイント＋アウトカムポイント合計 |

---

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
