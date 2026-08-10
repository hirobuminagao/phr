ALTER TABLE `health_exam_result`.`fund_delivery_lists`
  ADD COLUMN `grouping_mode` varchar(32) NOT NULL DEFAULT 'ALL'
    COMMENT 'ALL/BY_FACILITY. 初期実装はALL'
    AFTER `exam_month`,
  ADD COLUMN `sender_code` varchar(64) NOT NULL DEFAULT '1322100106'
    COMMENT '健保納品ZIPの送信元コード'
    AFTER `grouping_mode`,
  ADD COLUMN `sender_name` varchar(255) DEFAULT NULL
    COMMENT '送信元名称。必要になったら設定値を写す'
    AFTER `sender_code`,
  ADD KEY `idx_fund_delivery_lists_grouping` (`grouping_mode`),
  ADD KEY `idx_fund_delivery_lists_sender` (`sender_code`);

ALTER TABLE `health_exam_result`.`fund_delivery_runs`
  ADD COLUMN `grouping_mode` varchar(32) NOT NULL DEFAULT 'ALL'
    COMMENT 'ALL/BY_FACILITY. 初期実装はALL'
    AFTER `exam_month`,
  ADD COLUMN `sender_code` varchar(64) NOT NULL DEFAULT '1322100106'
    COMMENT '健保納品ZIPの送信元コード'
    AFTER `grouping_mode`,
  ADD COLUMN `sender_name` varchar(255) DEFAULT NULL
    COMMENT '送信元名称。出力時点の設定値'
    AFTER `sender_code`,
  ADD KEY `idx_fund_delivery_runs_grouping` (`grouping_mode`),
  ADD KEY `idx_fund_delivery_runs_sender` (`sender_code`);
