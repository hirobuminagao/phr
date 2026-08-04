ALTER TABLE `health_exam_result`.`exam_ledgers`
  ADD UNIQUE KEY `uq_exam_ledgers_xml_sha256_source` (`source_type`, `xml_sha256`),
  ADD UNIQUE KEY `uq_exam_ledgers_file_row_source` (`source_type`, `file_receipt_id`, `src_row_no`);
