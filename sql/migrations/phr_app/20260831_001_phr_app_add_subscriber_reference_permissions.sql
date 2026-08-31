USE `phr_app`;

INSERT INTO `app_permissions` (`permission_code`, `permission_name`, `permission_group`, `description`, `is_active`)
VALUES
  ('subscriber_reference.view', '加入者情報確認', 'subscriber_reference', '加入者検索、通常詳細、イベント別健診状況を参照する', 1),
  ('subscriber_reference.history_view', '加入者履歴確認', 'subscriber_reference', '値を含まない加入者変更履歴と参照履歴を参照する', 1),
  ('subscriber_reference.edit', '加入者情報編集', 'subscriber_reference', '将来の加入者情報編集を許可する。初期版では編集機能なし', 1),
  ('subscriber_reference.pii.full', '加入者個人情報完全表示', 'subscriber_reference', '明示操作時に加入者個人情報を完全表示する', 1),
  ('subscriber_reference.pii.masked', '加入者個人情報マスク表示', 'subscriber_reference', '加入者個人情報をマスクして表示する', 1),
  ('subscriber_reference.pii.hidden', '加入者個人情報非表示', 'subscriber_reference', '加入者個人情報を表示しない', 1)
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
  AND p.`permission_code` LIKE 'subscriber_reference.%'
ON DUPLICATE KEY UPDATE `is_allowed` = VALUES(`is_allowed`);

INSERT INTO `app_role_permissions` (`app_role_id`, `app_permission_id`, `is_allowed`)
SELECT r.`app_role_id`, p.`app_permission_id`, 0
FROM `app_roles` r
JOIN `app_permissions` p
WHERE r.`role_code` <> 'ADMIN'
  AND p.`permission_code` LIKE 'subscriber_reference.%'
ON DUPLICATE KEY UPDATE `is_allowed` = VALUES(`is_allowed`);
