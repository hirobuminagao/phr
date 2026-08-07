CREATE DATABASE IF NOT EXISTS `phr_app`
DEFAULT CHARACTER SET utf8mb4
COLLATE utf8mb4_ja_0900_as_cs;

USE `phr_app`;

-- Apply table DDL before this seed migration:
--   sql/ddl/phr_app/0010_phr_app__user_management.sql

INSERT INTO `app_roles` (`role_code`, `role_name`, `description`, `is_system_role`, `is_active`)
VALUES
  ('ADMIN', '管理者', 'ユーザー・権限・出力作業を管理できる初期ロール', 1, 1),
  ('EDITOR', '編集', '健診結果の確認・修正・出力リスト編集を行う初期ロール', 1, 1),
  ('VIEWER', '閲覧', '参照を中心に行う初期ロール', 1, 1)
ON DUPLICATE KEY UPDATE
  `role_name` = VALUES(`role_name`),
  `description` = VALUES(`description`),
  `is_system_role` = VALUES(`is_system_role`),
  `is_active` = VALUES(`is_active`);

INSERT INTO `app_permissions` (`permission_code`, `permission_name`, `permission_group`, `description`, `is_active`)
VALUES
  ('users.view', 'ユーザー参照', 'users', 'PHR使用ユーザーを参照する', 1),
  ('users.manage', 'ユーザー管理', 'users', 'PHR使用ユーザー、ロール、IP制限を登録・変更する', 1),
  ('exam_cases.view', '健診ケース参照', 'health_exam', '人単位の健診状況を参照する', 1),
  ('exam_cases.edit', '健診ケース編集', 'health_exam', '人単位の健診状況や補正情報を変更する', 1),
  ('export_lists.view', '出力リスト参照', 'xml_export', 'HIAアップロード用出力リストを参照する', 1),
  ('export_lists.edit', '出力リスト編集', 'xml_export', 'HIAアップロード用出力リストを作成・編集する', 1),
  ('xml_export.review', 'XML確認出力', 'xml_export', '確認用のXMLを出力する', 1),
  ('xml_export.official', 'XML本番出力', 'xml_export', 'HIAアップロード用の正式XMLを出力する', 1),
  ('hia_upload.perform', 'HIAアップロード作業', 'hia', '出力済みZIPをHIAへアップロードする作業を担当する', 1),
  ('hia_upload_status.edit', 'HIAアップロード状態編集', 'hia', 'HIAアップロード完了・エラー内容を記帳する', 1),
  ('audit.view', '監査ログ参照', 'audit', '操作ログを参照する', 1)
ON DUPLICATE KEY UPDATE
  `permission_name` = VALUES(`permission_name`),
  `permission_group` = VALUES(`permission_group`),
  `description` = VALUES(`description`),
  `is_active` = VALUES(`is_active`);

INSERT INTO `app_role_permissions` (`app_role_id`, `app_permission_id`, `is_allowed`)
SELECT r.`app_role_id`, p.`app_permission_id`, 1
FROM `app_roles` r
JOIN `app_permissions` p
WHERE r.`role_code` = 'ADMIN'
ON DUPLICATE KEY UPDATE `is_allowed` = VALUES(`is_allowed`);

INSERT INTO `app_role_permissions` (`app_role_id`, `app_permission_id`, `is_allowed`)
SELECT r.`app_role_id`, p.`app_permission_id`, 1
FROM `app_roles` r
JOIN `app_permissions` p
  ON p.`permission_code` IN (
    'exam_cases.view',
    'exam_cases.edit'
  )
WHERE r.`role_code` = 'EDITOR'
ON DUPLICATE KEY UPDATE `is_allowed` = VALUES(`is_allowed`);

INSERT INTO `app_role_permissions` (`app_role_id`, `app_permission_id`, `is_allowed`)
SELECT r.`app_role_id`, p.`app_permission_id`, 1
FROM `app_roles` r
JOIN `app_permissions` p
  ON p.`permission_code` IN (
    'exam_cases.view',
    'audit.view'
  )
WHERE r.`role_code` = 'VIEWER'
ON DUPLICATE KEY UPDATE `is_allowed` = VALUES(`is_allowed`);
