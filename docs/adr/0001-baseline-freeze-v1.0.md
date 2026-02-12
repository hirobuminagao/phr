# ADR 0001: Baseline Freeze（v1.0）を定義する

- Status: Accepted
- Date: 2026-02-10

## 背景 / 課題
健診データ取り込み基盤（ZIP/XML/提出CSV）について、運用で一度回して修正済みの状態が存在する。
今後のリファクタや機能追加を進めるにあたり、挙動や前提が揺れると「何が変わったか」を追跡できず事故につながる。

## 決定（Decision）
本リポジトリ `phr` の v1.0 は「開発基盤Freeze（基準点）」として定義する。

- Freezeの主対象は `scripts/kenshin_list_pydir/`（実行スクリプト群 + `kenshin_lib`）とする
- Freezeとは「改修禁止」ではなく、以下を固定して差分管理の軸にすること:
  - 既存挙動の再現性（同じ入力なら同じ結果になり、DBログで追跡できる）
  - 前提条件（実行カレント、必要ENV、DB接続系統、入力/出力の位置）
  - 設計意図（docstring と README/ADR により説明可能な状態）

## 理由（Rationale）
- 運用で回った状態を基準点として残すことで、以降の変更を「差分」として明確にできる
- スクリプトが複数のDB接続キーや運用上の前提（相対パス、手動キック）を持つため、暗黙知のままだと再現が崩れる

## 影響（Consequences）
- v1.0 以降の改修は、原則として「何が変わったか」を説明できる形（コミット/ADR/README更新）で行う
- 機微情報はリポジトリに入れない（`.env` は管理外、`.env.template`/`.env.example` で前提のみ共有）
- 破壊的変更（入力仕様やDBスキーマの大幅変更等）は、別バージョンの判断としてADRを追加する

## スコープ（Scope）
- 含む: `scripts/kenshin_list_pydir/` の実行スクリプトと `kenshin_lib`
- 含む: `sql/meta/dev_phr` の初期マスタCSV（再現用）
- 含まない: 実データ（ZIP/XML/CSV）および本番の`.env`（機微情報）

---

## 補足（2026-02-12）: v1.0-freeze タグとの関係

本ADR（0001）は、当初 `scripts/kenshin_list_pydir/` を主対象とした
「健診データ基盤のBaseline Freeze」を定義したものである。

2026-02-12 に付与した `v1.0-freeze` タグでは、これに加えて
`scripts/work_folder/` 系（hub/fund 取込・apply・共通lib・mat）についても
「現状の意味（契約）の固定」を行った。

- work_folder の主要テーブル前提は `dev_phr` スキーマ
- `mat/`（custom_id_config.json / custom_id_mapping.json）はID仕様の一次情報
- `scripts/fund_enrollee_loader/` は SQLite 前提の legacy 系と整理

work_folder 系のFreeze方針および設計判断の詳細は、別ADRとして記録する（0002予定）。

本ADR 0001 は「kenshin_list_pydir を中心とした初期Baseline定義」として有効であり、
v1.0-freeze タグはその拡張基点として位置づける。