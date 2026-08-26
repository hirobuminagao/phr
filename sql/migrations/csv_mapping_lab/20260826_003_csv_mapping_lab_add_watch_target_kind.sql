ALTER TABLE `csv_mapping_lab`.`csv_mapping_rules`
  DROP CHECK `chk_csv_mapping_rules_target`;

ALTER TABLE `csv_mapping_lab`.`csv_mapping_rules`
  MODIFY COLUMN `target_kind` varchar(64) COLLATE utf8mb4_ja_0900_as_cs NOT NULL
    COMMENT 'LEDGER_FIELD/EXAM_ITEM_VALUE/IGNORE/REVIEW/WATCH',
  MODIFY COLUMN `mapping_strategy` varchar(64) COLLATE utf8mb4_ja_0900_as_cs NOT NULL DEFAULT 'DIRECT'
    COMMENT 'DIRECT/MULTI_COLUMN_JOIN/DERIVED_CODE/METHOD_SELECTION/IGNORE/NEEDS_CONFIRMATION/WATCH_IF_PRESENT',
  ADD CONSTRAINT `chk_csv_mapping_rules_target`
    CHECK (`target_kind` IN ('LEDGER_FIELD', 'EXAM_ITEM_VALUE', 'IGNORE', 'REVIEW', 'WATCH'));
