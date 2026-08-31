-- Local-only address samples corresponding to
-- 9999_local_sample_subscribers_for_csv_import.sql.
-- Do not apply to shared or production environments.

INSERT INTO `subscriber_addresses` (
  `subscriber_id`,
  `postal_code`,
  `address_line`,
  `building`,
  `valid_from`,
  `is_current`,
  `source`,
  `prefecture`,
  `city`,
  `prefecture_code`
)
SELECT
  s.`id`,
  sample.`postal_code`,
  sample.`address_line`,
  sample.`building`,
  CURRENT_TIMESTAMP(3),
  1,
  'LOCAL_SAMPLE',
  sample.`prefecture`,
  sample.`city`,
  sample.`prefecture_code`
FROM `subscribers` AS s
INNER JOIN (
  SELECT 'CSV_SAMPLE_001' AS hia_subscriber_id, '060-0001' AS postal_code, '北海道札幌市中央区北一条西1丁目' AS address_line, 'サンプルビル101' AS building, '北海道' AS prefecture, '札幌市中央区' AS city, 1 AS prefecture_code
  UNION ALL SELECT 'CSV_SAMPLE_002', '060-0002', '北海道札幌市中央区北二条西2丁目', NULL, '北海道', '札幌市中央区', 1
  UNION ALL SELECT 'CSV_SAMPLE_003', '060-0003', '北海道札幌市中央区北三条西3丁目', NULL, '北海道', '札幌市中央区', 1
  UNION ALL SELECT 'CSV_SAMPLE_004', '060-0004', '北海道札幌市中央区北四条西4丁目', NULL, '北海道', '札幌市中央区', 1
  UNION ALL SELECT 'CSV_SAMPLE_005', '060-0005', '北海道札幌市中央区北五条西5丁目', NULL, '北海道', '札幌市中央区', 1
  UNION ALL SELECT 'CSV_MURAKAMI_001', '850-0001', '長崎県長崎市尾上町1丁目', NULL, '長崎県', '長崎市', 42
  UNION ALL SELECT 'CSV_MURAKAMI_002', '850-0002', '長崎県長崎市尾上町2丁目', NULL, '長崎県', '長崎市', 42
) AS sample
  ON sample.`hia_subscriber_id` = s.`hia_subscriber_id`
WHERE NOT EXISTS (
  SELECT 1
  FROM `subscriber_addresses` AS existing
  WHERE existing.`subscriber_id` = s.`id`
    AND existing.`is_current` = 1
);
