USE `phr_app`;

INSERT INTO `app_roles` (`role_code`, `role_name`, `description`, `is_system_role`, `is_active`)
VALUES
  (
    'FIELD_MANAGER',
    '現場管理者',
    '手入力結果や現場作業の管理を行う。システム管理は行わない',
    0,
    1
  )
ON DUPLICATE KEY UPDATE
  `role_name` = VALUES(`role_name`),
  `description` = VALUES(`description`),
  `is_system_role` = VALUES(`is_system_role`),
  `is_active` = VALUES(`is_active`);

INSERT INTO `app_permissions` (`permission_code`, `permission_name`, `permission_group`, `description`, `is_active`)
VALUES
  (
    'manual_exam_entry.edit',
    '健診結果手入力編集',
    'health_exam',
    '手入力draftを作成・変更・下書き保存する',
    1
  ),
  (
    'manual_exam_entry.manage',
    '健診結果手入力管理',
    'health_exam',
    '手入力draft削除、正式ledger管理、巻き戻しなど現場管理者向け操作を行う',
    1
  )
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
  AND p.`permission_code` IN ('manual_exam_entry.edit', 'manual_exam_entry.manage')
ON DUPLICATE KEY UPDATE `is_allowed` = VALUES(`is_allowed`);

INSERT INTO `app_role_permissions` (`app_role_id`, `app_permission_id`, `is_allowed`)
SELECT r.`app_role_id`, p.`app_permission_id`, 1
FROM `app_roles` r
JOIN `app_permissions` p
  ON p.`permission_code` IN (
    'exam_cases.view',
    'exam_cases.edit',
    'manual_exam_entry.edit',
    'manual_exam_entry.manage'
  )
WHERE r.`role_code` = 'FIELD_MANAGER'
ON DUPLICATE KEY UPDATE `is_allowed` = VALUES(`is_allowed`);

INSERT INTO `app_role_permissions` (`app_role_id`, `app_permission_id`, `is_allowed`)
SELECT r.`app_role_id`, p.`app_permission_id`, 1
FROM `app_roles` r
JOIN `app_permissions` p
WHERE r.`role_code` = 'EDITOR'
  AND p.`permission_code` = 'manual_exam_entry.edit'
ON DUPLICATE KEY UPDATE `is_allowed` = VALUES(`is_allowed`);

INSERT INTO `app_role_permissions` (`app_role_id`, `app_permission_id`, `is_allowed`)
SELECT r.`app_role_id`, p.`app_permission_id`, 0
FROM `app_roles` r
JOIN `app_permissions` p
WHERE r.`role_code` = 'VIEWER'
  AND p.`permission_code` IN ('manual_exam_entry.edit', 'manual_exam_entry.manage')
ON DUPLICATE KEY UPDATE `is_allowed` = VALUES(`is_allowed`);
