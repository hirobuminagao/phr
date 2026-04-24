ALTER TABLE dev_phr.staging_subscribers_fund
  ADD COLUMN diff_status varchar(50) DEFAULT NULL COMMENT '差分判定結果',
  ADD COLUMN diff_status_method varchar(20) DEFAULT NULL COMMENT '判定手段',
  ADD COLUMN diff_status_reason varchar(255) DEFAULT NULL COMMENT '判定理由';