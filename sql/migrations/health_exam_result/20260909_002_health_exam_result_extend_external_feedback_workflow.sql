ALTER TABLE `health_exam_result`.`ops_external_feedback_reports`
  ADD COLUMN `copied_from_report_id` bigint unsigned DEFAULT NULL COMMENT '未解決引き継ぎ元の指摘箱ID' AFTER `fund_delivery_run_id`,
  ADD COLUMN `carried_over_to_report_id` bigint unsigned DEFAULT NULL COMMENT '未解決引き継ぎ先の指摘箱ID' AFTER `copied_from_report_id`,
  ADD KEY `idx_ops_external_feedback_reports_copied_from` (`copied_from_report_id`),
  ADD KEY `idx_ops_external_feedback_reports_carried_over_to` (`carried_over_to_report_id`);

ALTER TABLE `health_exam_result`.`ops_external_feedback_items`
  ADD COLUMN `copied_from_item_id` bigint unsigned DEFAULT NULL COMMENT '未解決引き継ぎ元の対象者ID' AFTER `fund_delivery_member_id`,
  ADD COLUMN `reoutput_xml_export_list_case_id` bigint unsigned DEFAULT NULL COMMENT '解消後に追加した出力リストcase ID' AFTER `copied_from_item_id`,
  ADD KEY `idx_ops_external_feedback_items_copied_from` (`copied_from_item_id`),
  ADD KEY `idx_ops_external_feedback_items_reoutput_case` (`reoutput_xml_export_list_case_id`);

ALTER TABLE `health_exam_result`.`ops_external_feedback_item_details`
  ADD COLUMN `copied_from_detail_id` bigint unsigned DEFAULT NULL COMMENT '未解決引き継ぎ元の指摘項目ID' AFTER `resolution_note`,
  ADD COLUMN `resolved_at` datetime(3) DEFAULT NULL AFTER `copied_from_detail_id`,
  ADD COLUMN `resolved_by` varchar(190) DEFAULT NULL AFTER `resolved_at`,
  ADD KEY `idx_ops_external_feedback_item_details_copied_from` (`copied_from_detail_id`);
