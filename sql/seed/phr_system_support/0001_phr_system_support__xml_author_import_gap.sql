INSERT INTO `phr_system_support`.`support_incidents` (
  `incident_id`,
  `incident_key`,
  `title`,
  `incident_type`,
  `status`,
  `description`,
  `created_by`
) VALUES (
  1,
  'INC-20260901-XML-AUTHOR',
  'XML author要素の医師名取込漏れ',
  'IMPLEMENTATION_GAP',
  'OPEN',
  '厚生労働省付属2のauthor要素で表現された医師名を独立健診項目として取り込めていなかった事象。修正前対象を保存し、再取込・case再作成・再出力を追跡する。',
  'SYSTEM_SUPPORT_SETUP'
)
ON DUPLICATE KEY UPDATE
  `title` = VALUES(`title`),
  `description` = VALUES(`description`),
  `updated_at` = CURRENT_TIMESTAMP(3);
