r"""
============================================================
PHR work_folder v1.0 — 関係図（現状の意味の固定）

このパッケージは scripts/work_folder/ 配下のスクリプト群の共通ライブラリ。
目的はリファクタではなく「v1.0 現状の契約・責務境界」を明文化して凍結すること。

Directory:
- scripts/work_folder/scripts/  : 実行スクリプト（import/apply）
- scripts/work_folder/lib/      : 共通ライブラリ（normalize / etl / db / errors / id）

------------------------------------------------------------
1) Hub ルート（完成済み）
------------------------------------------------------------
HIA CSV
  → scripts/import_subscribers_to_staging_hub.py
      - CSV → staging_subscribers_hub
      - NormalizeError は etl_errors 記録、行スキップで継続
      - dry-run は INSERT せず rollback（run/err は残る）
  → （別途）apply_subscribers_from_staging_hub.py
      - 【注意】v1.0 現状は apply 名だが import 相当（CSV → staging）
      - subscribers 本表反映（apply）は未実装/未到達

staging_subscribers_hub
  → subscribers（正本）
      - v1.0 現状: 完成済み（別スクリプト/運用で実施）
      - DB座標: subscribers は dev_phr スキーマに存在する（v1.0前提）

------------------------------------------------------------
2) fund ルート（入口のみ完成、差分は未実装）
------------------------------------------------------------
fund CSV
  → scripts/import_subscribers_to_staging_fund.py
      - templates / template_mappings により列マップ
      - mapping → normalize → staging_subscribers_fund
      - ただし「行エラーが1件でもあれば staging を全件 rollback」
      - 成功済み src_file は staging 存在で重複NG、失敗は再投入OK

staging_subscribers_fund
  → （未実装）fund 差分ロジック
  → （未実装）staging → subscribers 反映（apply）

------------------------------------------------------------
2.5) DB座標（v1.0 前提）
------------------------------------------------------------
- 正本（subscribers）: dev_phr.subscribers
- Hub staging        : dev_phr.staging_subscribers_hub（取込）
- fund staging       : dev_phr.staging_subscribers_fund（取込）
- fund template参照  : dev_phr.templates / dev_phr.template_mappings
- fund_id 解決参照   : dev_phr.fund_insurer_numbers / dev_phr.funds
- ETL証跡（run/err） : dev_phr.etl_runs / dev_phr.etl_errors

※v1.0 現状では「ここに列挙したテーブルはすべて dev_phr スキーマ」。環境で dev/stg/prod に切替える場合は、ここを一次情報として更新する

------------------------------------------------------------
3) 共通ライブラリ（依存関係）
------------------------------------------------------------
normalize/*
  - common.py     : 数字/日付/性別/記号など共通のゆれ吸収
  - subscriber.py : 氏名カナ必須の正規化 + person_id_custom生成ラッパー
  - rules.py      : fund向け「列ルール合成」の唯一窓口

custom_id_gen.py
  - person_id_custom 生成（暗号ではない。決定的な固定キー）

errors/normalize.py
  - NormalizeError: 想定内の行エラー（etl_errors 記録・行スキップ前提）

etl/*
  - ddl.py      : etl_runs / etl_errors のDDL存在保証
  - runs.py     : start_run / finish_run（status判定含む）
  - metrics.py  : RunMetrics（rows_seen が進捗の唯一の真実）
  - progress.py : 表示専用（RunMetrics参照のみ）
  - errors.py   : etl_errors 記録 + etl_runs.errors を +1

db/*
  - config.py : env/.env → MySQLParams（接続はしない）
  - mysql.py  : 接続生成のみ（commit/rollback は呼び出し側責務）

------------------------------------------------------------
4) v1.0 の不変条件（要点）
------------------------------------------------------------
- NormalizeError は「想定内」。行スキップ + etl_errors 記録で処理継続
- rows_seen は RunMetrics が保持する唯一の進捗真実（表示側は参照のみ）
- ETL証跡（etl_runs/etl_errors）は conn_log で先commit して残す
- DB commit/rollback の責務は基本的にスクリプト側（libはしない）
============================================================
"""