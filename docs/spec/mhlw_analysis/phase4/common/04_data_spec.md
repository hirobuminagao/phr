# 04 DATA Spec

## 1. 目的

DATAフォルダは、送付対象となる実データXMLを格納する領域であり、
健診結果・保健指導・関連情報の実体データを定義する。

本specでは、実装観点で以下を整理する。

- DATA の役割
- ix08 / su08 との関係
- XML構造（CDAベース）
- セクション構造（90030等）

---

## 2. 対象範囲

- DATAフォルダ配下のXMLファイル
- 各種CDA XML（健診結果 / 保健指導等）

---

## 3. 一次情報

- 厚生労働省 `8-1A.pdf`
- 各XMLスキーマ（CDAベース）

---

## 4. DATA の役割

- 個別対象者ごとの実データを格納
- ix08 による参照対象
- su08 の集計対象

---

## 5. 配置場所

```yaml
location:
  root/DATA/
```

---

## 5.1 ファイル命名規則

```yaml
filename:
  pattern: 未確定（要確認）
```

- DATA配下のXMLファイル名は仕様上ルールが存在する可能性がある
- 現時点では ix08 との突合により実体一致を前提とする

```yaml
constraints:
  - ix08.file_name と完全一致すること
```

- 実装上はファイル名そのものよりも「ix08との一致」を優先する

---

## 6. ファイル単位構造

```yaml
DATA:
  - 1 XML = 1対象（個人単位）
```

- ファイル単位で対象者が分離される

---

## 7. 文書構造（概要）

```yaml
ClinicalDocument:
  - header
  - body
```

※ CDA準拠

---

## 8. ヘッダ構造（概要）

```yaml
header:
  id
  recordTarget
  author
  custodian
```

- 対象者情報
- 作成機関情報

---

## 9. ボディ構造（概要）

```yaml
body:
  structuredBody:
    component:
      section:
        - code
        - entry
```

---

## 10. section構造（重要）

```yaml
section:
  code: セクション識別コード
  entry: 実データ
```

```yaml
examples:
  - 90030: 実施内容
  - 24100: 生活習慣改善
  - 24090: 体重・腹囲改善
```

---

## 11. ix08 / su08 との関係

```yaml
relationships:
  ix08:
    - DATA XML を列挙

  su08:
    - DATA XML を集計
```

---

## 12. 実装チェック観点

```yaml
Checks:
  - XMLが存在する
  - XML構造がCDA準拠
  - sectionが存在する
  - section codeが正しい
```

---

## 13. 未整理 / TODO

```yaml
TODO:
  - sectionごとの詳細構造
  - entry内のデータ構造
  - 各コードの厳密仕様確認
```