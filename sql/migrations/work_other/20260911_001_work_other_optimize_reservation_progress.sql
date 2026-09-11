ALTER TABLE `work_other`.`reservation_site_records`
  MODIFY COLUMN `hia_member_id` varchar(190) DEFAULT NULL
    COMMENT 'HIA加入者ID。加入者マスタとの照合キー',
  ADD KEY `idx_reservation_site_records_event_status`
    (`event_id`, `reservation_status_raw`, `hia_member_id`),
  ADD KEY `idx_reservation_site_records_event_facility`
    (`event_id`, `exam_facility_id`, `hia_member_id`),
  ADD KEY `idx_reservation_site_records_event_date`
    (`event_id`, `reservation_date`, `hia_member_id`);
