# System SQL Exports

管理画面から実行環境のマスター状態を取り出すための出力先です。

- migrationではなく、環境間同期用のseed/UPSERT SQLとして扱う。
- テーブル定義変更はここに置かない。
- 出力SQLは原則として `ON DUPLICATE KEY UPDATE` で冪等にする。
- 実行環境で補正したマスター値を、repoに残して他環境へ反映するために使う。

初期対象:

- `phr_master.norm_variants`
