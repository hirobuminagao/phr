ALTER TABLE `dev_phr`.`event`
  ADD COLUMN `result_root_path` text NULL COMMENT '健診結果ルートフォルダ' AFTER `event_name`;
