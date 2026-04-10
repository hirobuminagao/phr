

# 02 ix08 Spec

## 1. 目的

ix08 は送付用ファイルアーカイブに含まれるインデックスファイルであり、
アーカイブ内のXML構成および交換情報を管理するための基礎情報を定義する。

本specでは、実装観点で以下を整理する。

- ix08 の役割
- アーカイブ内における位置付け
- DATA / CLAIMS との関係
- XML構造の概要

---

## 2. 対象範囲

- ix08_V08.xml
- 送付用ファイルアーカイブ内のインデックス構造

---

## 3. 一次情報

- 厚生労働省 `8-1A.pdf`
- ix08 スキーマ定義（ix08_V08.xsd）

---

## 4. ix08 の役割

- アーカイブ内のXMLファイル一覧を管理
- DATAフォルダ内XMLとの整合を担保
- 送付単位のメタ情報を保持

---

## 5. 配置場所とファイル名

```yaml
location:
  root directory

filename:
  ix08_V08.xml
```

---

## 6. 文書構造（詳細）

ix08 は HL7 CDA 準拠の XML構造を持つ。

```yaml
ClinicalDocument:
  id:
    description: 文書識別子

  code:
    description: 文書種別コード

  effectiveTime:
    description: 作成日時

  recordTarget:
    description: 対象単位識別（※ ix08では個人ではなく交換単位）

  author:
    description: 作成者（提出元機関）

  custodian:
    description: 管理者（提出元機関）

  component:
    structuredBody:
      component:
        section:
          code:
            description: セクション種別

          entry:
            file_list:
              - file_name
              - file_type
              - relation
```

---

### 6.1 ヘッダ情報

```yaml
header:
  - id
  - code
  - effectiveTime
  - author
  - custodian
```

- ix08全体の識別情報
- 提出元・作成日時の管理

---

### 6.2 ファイル一覧（最重要）

ix08 のコア機能は、アーカイブ内ファイルの列挙である。

```yaml
file_list:
  - file_name: XMLファイル名
  - file_type: データ種別（健診 / 保健指導 / 決済）
  - relation: DATA / CLAIMS の関係性
```

```yaml
constraints:
  - all DATA files must be listed
  - CLAIMS files must be listed if present
  - no duplicate file_name
```

---

### 6.3 DATAとの対応関係

```yaml
mapping:
  ix08.file_name == DATA.filename
```

- ix08に記載されたファイル名は、DATAフォルダ内の実ファイルと完全一致する必要がある

---

### 6.4 CLAIMSとの対応関係

```yaml
mapping:
  ix08.file_name == CLAIMS.filename
```

- CLAIMSが存在する場合、ix08にも必ず記載される

---

### 6.5 実装観点（拡張）

```yaml
Checks:
  - ClinicalDocument root exists
  - required header fields exist
  - file_list exists
  - all DATA files are referenced
  - CLAIMS files are referenced if present
```

---

## 7. 記載対象

- DATAフォルダ内のXMLファイル
- CLAIMSフォルダ内のXMLファイル（存在する場合）

---

## 8. DATA / CLAIMS との関係

```yaml
constraints:
  - ix08 must reference all DATA XML files
  - ix08 must be consistent with CLAIMS if present
```

---

## 9. 実装チェック観点

```yaml
Checks:
  - ix08 exists
  - ix08 structure is valid XML
  - all DATA files are referenced
  - no missing references
```

---

## 10. 未整理 / TODO

```yaml
TODO:
  - ix08詳細構造解析
  - section単位の分解
  - su08との関係整理
```