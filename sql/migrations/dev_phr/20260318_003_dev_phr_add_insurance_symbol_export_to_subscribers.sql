

-- ============================================================
-- dev_phr.subscribers に insurance_symbol_export を追加
-- ============================================================

ALTER TABLE `dev_phr`.`subscribers`
  ADD COLUMN `insurance_symbol_export` varchar(20) DEFAULT NULL
    COMMENT '保険証記号（出力用: 全数字なら半角、非数字を含むなら全体を全角）'
  AFTER `insurance_symbol`;

-- インデックス追加（存在しない場合のみ追加したいが、MySQLではIF NOT EXISTS不可のため前提管理）
ALTER TABLE `dev_phr`.`subscribers`
  ADD INDEX `idx_subscribers_symbol_export` (`insurance_symbol_export`);