# PHR使用ユーザー管理

## 目的

健診結果取込・確認・XML出力・HIAアップロード記帳を行う社内ローカル画面の利用者を管理する。

物理MySQLは既存環境と同じものを使うが、業務データとは責務を分けるため、論理DBは `phr_app` とする。

## 初期方針

- ログインIDは社員番号 `employee_no`。
- パスワードは平文保存しない。初期実装は `pbkdf2_sha256` 形式のハッシュを保存する。
- ロールは初期値として `ADMIN` / `EDITOR` / `VIEWER` を用意する。
- `ADMIN` はフルコントロールとして扱う。
- 通常作業の担当可否はロールではなく、個人ごとの作業権限ON/OFFで制御する。
- 個人作業権限は `表示` と `編集・実行` を分ける。
- ロールで何ができるかは固定実装にしない。`app_permissions` と `app_role_permissions` で後から変更できるようにする。ただし出力リスト、XML出力、HIAアップロード等の作業系権限は個人設定を正とする。
- IP制限はユーザー単位で持つ。許可IPが未登録ならIP制限なし、1件以上ある場合は一致するIPのみログイン可。
- 自動ログアウトはadmin設定で管理する。初期値は最大12時間、無操作60分。
- ログイン試行は成功・失敗とも `app_login_attempts` に残す。
- 画面操作や重要な業務操作は `app_audit_logs` に残す。個人情報を含む画面閲覧、今後追加するダウンロードは、誰が・いつ・どの人を見た/出したかを残す。

## DB構成

### `app_users`

PHR画面の利用者本体。

主な項目:

- `employee_no`: 社員番号。ログインID。
- `display_name`: 表示名。
- `department_name`, `email`: 任意の補助情報。
- `password_hash`, `password_hash_algorithm`: パスワード検証用。
- `must_change_password`: 初回変更要求。
- `failed_login_count`, `locked_until`: ログイン制限用。
- `last_login_at`, `last_login_ip`: 最終ログイン記録。
- `is_active`: 利用可否。

### `app_roles`

ロール定義。初期値は仮であり、後続で追加・変更できる。

### `app_permissions`

操作権限定義。画面やAPIではロール名を直接見るのではなく、原則として権限コードを見る。

初期権限:

- `users.view`
- `users.manage`
- `business_settings.view`
- `business_settings.manage`
- `exam_cases.view`
- `exam_cases.edit`
- `export_lists.view`
- `export_lists.edit`
- `xml_export.review`
- `xml_export.official`
- `hia_upload.perform`
- `hia_upload_status.edit`
- `audit.view`

管理カテゴリの扱い:

- `business_settings.view`: イベント設定、健診機関マスタ、健診機関受領フォルダ管理など、業務側の管理カテゴリを参照する。
- `business_settings.manage`: イベント設定、健診機関マスタ、健診機関受領フォルダ管理など、業務側の管理カテゴリを操作する。
- `users.manage`: アカウント管理、権限設定、セキュリティ、監査ログなど、システム管理カテゴリを操作する。
- 初期ロールでは `ADMIN` と `EDITOR` に `business_settings.view` / `business_settings.manage` を付与し、システム管理は `ADMIN` のみとする。

### `app_role_permissions`

ロールと権限の対応。ここを変更すれば、ロールの意味を後から調整できる。

### `app_user_roles`

利用者に付与されたロール。複数ロール付与を許容する。

### `app_user_permissions`

利用者ごとの明示的な作業権限。

用途:

- 出力リスト、XML出力、HIAアップロードなど、現場の担当者ごとにON/OFFしたい作業を管理する。
- ロールで一括付与せず、個人ごとに `表示` と `編集・実行` を分けて管理する。
- `is_allowed = 1` は個人許可、`is_allowed = 0` は個人拒否として扱う。

初期の個人作業権限:

| 作業 | 表示 | 編集・実行 |
|---|---|---|
| 出力リスト | `export_lists.view` | `export_lists.edit` |
| XML出力 | `xml_export.review` | `xml_export.official` |
| HIAアップロード | `hia_upload.perform` | `hia_upload_status.edit` |
| 管理カテゴリ | `business_settings.view` | `business_settings.manage` |

システム管理カテゴリの `users.manage` は、個人作業権限ON/OFFでは扱わず、管理者ロールの責務とする。

権限判定順:

1. `ADMIN` ロールを持つユーザーは全権限を持つ。
2. 個人権限 `app_user_permissions` の明示設定を反映する。
3. ロール権限 `app_role_permissions` を反映する。

### `app_user_allowed_ips`

利用者ごとの許可IP。

初期仕様:

- IP範囲指定は行わない。
- 個別IPのみ登録する。
- 有効な許可IPがないユーザーはIP制限なし。

### `app_sessions`

ログイン成功後のセッション。画面/API実装時に使用する。

初期仕様:

- `session_lifetime_minutes`: セッション最大時間。初期値720分。
- `session_idle_timeout_minutes`: 最終操作から自動ログアウトするまでの分数。初期値60分。0なら無操作ログアウトを無効化。
- 上記は `app_settings` でadminが変更できる。

### `app_login_attempts`

ログイン試行履歴。ログイン不可理由を後から追えるようにする。

### `app_settings`

PHR画面の運用設定。

初期設定:

- `session_lifetime_minutes`: ログインセッションの最大有効時間。
- `session_idle_timeout_minutes`: 無操作ログアウトまでの分数。
- `personal_info_audit_enabled`: 個人情報画面閲覧/ダウンロード監査ログのON/OFF。

### `app_audit_logs`

画面/API操作の監査ログ。出力リスト編集、正式XML出力、HIAアップロード記帳、ユーザー変更などを残す。

個人情報監査:

- 出力リスト詳細など、氏名カナ・記号番号・HIA加入者ID等を含む画面を開いた場合、対象者ごとに `PERSONAL_INFO_VIEW_*` として記録する。
- ダウンロード機能を追加する場合は `PERSONAL_INFO_DOWNLOAD_*` として同じテーブルへ記録する。
- 監査ログの記録ON/OFFは `personal_info_audit_enabled` で制御する。

## 実装ステップ

1. `phr_app` DBとユーザー管理テーブルを作成する。
2. CLIでユーザー登録とログイン確認をできるようにする。
3. FastAPI画面/API実装時に、セッション・権限・IP制限をこのDBへ接続する。
4. アカウント編集画面で個人ごとの作業権限ON/OFFを設定する。
5. セキュリティ設定画面で自動ログアウト、個人情報監査ログON/OFFを変更する。
6. 画面ごとの権限は初期運用後に調整する。

## 適用ファイル

- `sql/ddl/phr_app/0000_phr_app__database.sql`
- `sql/ddl/phr_app/0010_phr_app__user_management.sql`
- `sql/migrations/phr_app/20260807_001_phr_app_create_user_management.sql`
- `sql/migrations/phr_app/20260807_002_phr_app_add_user_approval.sql`
- `sql/migrations/phr_app/20260807_003_phr_app_add_user_permission_overrides.sql`
- `sql/migrations/phr_app/20260807_004_phr_app_add_security_settings.sql`
