

# 03 su08 Spec

## 1. 目的

su08 は送付用ファイルアーカイブに含まれるサマリ情報ファイルであり、
送付単位における集計情報および整合性確認のための情報を定義する。

本specでは、実装観点で以下を整理する。

- su08 の役割
- ix08 との関係
- DATA / CLAIMS との関係
- XML構造の概要

---

## 2. 対象範囲

- su08_V08.xml
- 送付用ファイルアーカイブ内のサマリ情報

---

## 3. 一次情報

- 厚生労働省 `8-1A.pdf`
- su08 スキーマ定義（su08_V08.xsd）

---

## 4. su08 の役割

- 送付単位における件数・集計情報の管理
- ix08 に対する補完情報（インデックスに対するサマリ）
- DATA / CLAIMS 全体の整合性チェック

---

## 5. 配置場所とファイル名

```yaml
location:
  root directory

filename:
  su08_V08.xml
```

---

## 6. 文書構造（概要）

```yaml
su08:
  - header information
  - summary information
  - exchange metadata
```

※ 詳細構造は後続で定義

---

## 7. 記載対象

- DATAフォルダ内のXML全体の集計
- CLAIMSフォルダ内のXML全体の集計（存在する場合）

---

## 8. ix08 / DATA / CLAIMS との関係

```yaml
constraints:
  - su08 must be consistent with ix08
  - su08 summary must match DATA contents
  - su08 summary must match CLAIMS contents if present
```

---

## 9. 実装チェック観点

```yaml
Checks:
  - su08 exists
  - su08 structure is valid XML
  - summary counts match DATA
  - summary counts match CLAIMS if present
```

---

## 10. 未整理 / TODO

```yaml
TODO:
  - su08詳細構造解析
  - 集計項目の特定
  - ix08との突合ロジック整理
```