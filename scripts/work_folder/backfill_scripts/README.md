This directory contains one-time migration / backfill scripts.

These scripts are used during schema upgrades and must NOT be executed regularly.

Each script corresponds to a schema change recorded in ADR documents.

Typical upgrade flow:

1. ADR defines the schema change
2. DDL migration is applied to the database
3. A backfill script in this directory is executed once to update existing data
4. Application code begins using the new schema

Important:
- These scripts are **not part of normal system operation**
- They are intended to be executed manually during controlled upgrades
- Each script should clearly indicate the version or change it belongs to (e.g. v1_0_1_*)


データベース接続ルール
------------------------

backfill スクリプトは MySQL データベースへ直接接続する必要がある場合があります。

接続情報（ホスト・ユーザー・パスワード等）は **スクリプト内にハードコードしてはいけません**。
また、これらの情報を **Git リポジトリに保存してはいけません**。

代わりに、スクリプトはローカル設定ファイルから接続情報を読み込みます。

    scripts/work_folder/.env

典型的な設定例：

    PHR_MYSQL_HOST=...
    PHR_MYSQL_PORT=3306
    PHR_MYSQL_USER=...
    PHR_MYSQL_PASSWORD=...
    PHR_MYSQL_DATABASE=work_other

この `.env` ファイルは **Git 管理対象外** とし、各開発環境または運用環境のローカルにのみ存在させます。

ルール：
- スキーマ / テーブル / カラム名はスクリプト内で定義する
- 接続情報は `.env` から読み込む
- backfill スクリプトは `.env` が存在する前提で実行する

この設計により次の利点があります：
- 認証情報がリポジトリにコミットされない
- Mac / Windows / サーバーなど異なる環境でも同じスクリプトが利用できる
- 秘密情報をローカルに保ったまま backfill 作業を再現可能にできる