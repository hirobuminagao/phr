UPDATE dev_phr.exam_item_master
SET
  display_unit = 'ml/min/1.73m2',
  ucum_unit = 'ml/min/1.73m2',
  updated_at = CURRENT_TIMESTAMP(3)
WHERE namecode = '8A065000002391901';
