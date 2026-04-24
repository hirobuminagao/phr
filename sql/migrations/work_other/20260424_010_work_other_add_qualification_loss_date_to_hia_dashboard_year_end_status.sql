

-- =========================================
-- hia_dashboard_year_end_status に資格喪失日を追加
-- =========================================

ALTER TABLE work_other.hia_dashboard_year_end_status
ADD COLUMN qualification_lost_date DATE NULL COMMENT '資格喪失日'
AFTER exam_date;

-- 既存データのbackfill（hia_dashboard_statusからコピー）
UPDATE work_other.hia_dashboard_year_end_status y
JOIN work_other.hia_dashboard_status s
  ON y.insurer_number = s.insurer_number
 AND y.insurance_symbol_match = s.insurance_symbol_match
 AND y.insurance_number_match = s.insurance_number_match
 AND y.name_match = s.name_match
SET y.qualification_loss_date = s.qualification_loss_date
WHERE y.qualification_loss_date IS NULL;