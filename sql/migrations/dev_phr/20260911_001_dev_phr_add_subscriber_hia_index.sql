SET @add_subscribers_hia_index_sql = (
  SELECT IF(
    COUNT(*) = 0,
    'ALTER TABLE `dev_phr`.`subscribers` ADD KEY `idx_subscribers_hia_subscriber_id` (`hia_subscriber_id`)',
    'SELECT 1'
  )
  FROM information_schema.statistics
  WHERE table_schema = 'dev_phr'
    AND table_name = 'subscribers'
    AND index_name = 'idx_subscribers_hia_subscriber_id'
);

PREPARE add_subscribers_hia_index_stmt FROM @add_subscribers_hia_index_sql;
EXECUTE add_subscribers_hia_index_stmt;
DEALLOCATE PREPARE add_subscribers_hia_index_stmt;
