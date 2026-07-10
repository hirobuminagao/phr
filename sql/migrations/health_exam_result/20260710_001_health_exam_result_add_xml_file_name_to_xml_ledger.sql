ALTER TABLE `health_exam_result`.`xml_ledger`
  ADD COLUMN `xml_file_name` varchar(255) DEFAULT NULL
    AFTER `xml_sha256`,
  ADD KEY `idx_xml_ledger_xml_file_name` (`xml_file_name`);

UPDATE `health_exam_result`.`xml_ledger` xl
JOIN (
  SELECT
    xfl.xml_ledger_id,
    SUBSTRING_INDEX(
      REPLACE(
        COALESCE(xfl.xml_inner_path, fr.relative_path, fr.source_path, fr.file_name),
        '\\',
        '/'
      ),
      '/',
      -1
    ) AS xml_file_name
  FROM `health_exam_result`.`xml_file_links` xfl
  INNER JOIN (
    SELECT xml_ledger_id, MIN(id) AS id
    FROM `health_exam_result`.`xml_file_links`
    GROUP BY xml_ledger_id
  ) first_link
    ON first_link.id = xfl.id
  INNER JOIN `health_exam_result`.`file_receipts` fr
    ON fr.id = xfl.file_receipt_id
) src
  ON src.xml_ledger_id = xl.id
SET xl.xml_file_name = NULLIF(src.xml_file_name, '')
WHERE xl.xml_file_name IS NULL;
