CREATE DATABASE IF NOT EXISTS `csv_mapping_lab`
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_ja_0900_as_cs;

DELETE FROM `csv_mapping_lab`.`csv_mapping_rules`
WHERE `reason` LIKE 'seed: maruyama 121-240%';

INSERT INTO `csv_mapping_lab`.`csv_mapping_rules` (
  `scope`, `facility_code`, `condition_type`, `column_no_min`, `column_no_max`, `header_pattern`, `normalized_header_pattern`,
  `value_type`, `target_kind`, `target_namecode`, `target_ledger_field`,
  `mapping_strategy`, `confidence`, `reason`, `created_by`, `updated_by`
)
VALUES
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '眼底検査所見１', '眼底検査所見1', 'CODE', 'EXAM_ITEM_VALUE', '9E100160900000049', NULL, 'DIRECT', 0.8500, 'seed: maruyama 121-240 fundus finding text', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '眼底検査所見２', '眼底検査所見2', 'EMPTY', 'IGNORE', NULL, NULL, 'IGNORE', 0.8500, 'seed: maruyama 121-240 empty reserved fundus finding column', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '眼底検査所見３', '眼底検査所見3', 'EMPTY', 'IGNORE', NULL, NULL, 'IGNORE', 0.8500, 'seed: maruyama 121-240 empty reserved fundus finding column', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '眼底検査所見４', '眼底検査所見4', 'EMPTY', 'IGNORE', NULL, NULL, 'IGNORE', 0.8500, 'seed: maruyama 121-240 empty reserved fundus finding column', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '眼底検査所見５', '眼底検査所見5', 'EMPTY', 'IGNORE', NULL, NULL, 'IGNORE', 0.8500, 'seed: maruyama 121-240 empty reserved fundus finding column', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '眼底検査所見疑い１', '眼底検査所見疑い1', 'CODE', 'REVIEW', NULL, NULL, 'NEEDS_CONFIRMATION', 0.7000, 'seed: maruyama 121-240 suspicion qualifier; confirm merge target', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '眼底検査実施理由', '眼底検査実施理由', 'EMPTY', 'IGNORE', NULL, NULL, 'IGNORE', 0.8500, 'seed: maruyama 121-240 empty fundus admin column', 'seed', 'seed'),

  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '服薬１（血圧）', '服薬1(血圧)', 'CODE', 'EXAM_ITEM_VALUE', '9N701000000000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 specific questionnaire', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '服薬２（血糖）', '服薬2(血糖)', 'CODE', 'EXAM_ITEM_VALUE', '9N706000000000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 specific questionnaire', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '服薬３（脂質）', '服薬3(脂質)', 'CODE', 'EXAM_ITEM_VALUE', '9N711000000000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 specific questionnaire', 'seed', 'seed'),
  ('facility', '0110119070', 'header_contains', 121, 240, '_薬剤', '_薬剤', 'EMPTY', 'IGNORE', NULL, NULL, 'IGNORE', 0.8500, 'seed: maruyama 121-240 empty medication note column', 'seed', 'seed'),
  ('facility', '0110119070', 'header_contains', 121, 240, '_服薬理由', '_服薬理由', 'EMPTY', 'IGNORE', NULL, NULL, 'IGNORE', 0.8500, 'seed: maruyama 121-240 empty medication reason column', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '既往歴１（脳血管）', '既往歴1(脳血管)', 'CODE', 'EXAM_ITEM_VALUE', '9N716000000000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 specific questionnaire', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '既往歴２（心血管）', '既往歴2(心血管)', 'CODE', 'EXAM_ITEM_VALUE', '9N721000000000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 specific questionnaire', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '既往歴３（腎不全・人工透析）', '既往歴3(腎不全・人工透析)', 'CODE', 'EXAM_ITEM_VALUE', '9N726000000000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 specific questionnaire', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '貧血', '貧血', 'CODE', 'EXAM_ITEM_VALUE', '9N731000000000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 specific questionnaire', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '２０歳からの体重変化', '20歳からの体重変化', 'CODE', 'EXAM_ITEM_VALUE', '9N741000000000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 specific questionnaire', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '３０分以上の運動習慣', '30分以上の運動習慣', 'CODE', 'EXAM_ITEM_VALUE', '9N746000000000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 specific questionnaire', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '歩行又は身体活動', '歩行又は身体活動', 'CODE', 'EXAM_ITEM_VALUE', '9N751000000000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 specific questionnaire', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '歩行速度', '歩行速度', 'CODE', 'EXAM_ITEM_VALUE', '9N756000000000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 specific questionnaire', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '１年間の体重変化', '1年間の体重変化', 'EMPTY', 'IGNORE', NULL, NULL, 'IGNORE', 0.8500, 'seed: maruyama 121-240 empty specific questionnaire column', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '食べ方１（早食い等）', '食べ方1(早食い等)', 'CODE', 'EXAM_ITEM_VALUE', '9N766000000000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 specific questionnaire', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '食べ方２（就寝前）', '食べ方2(就寝前)', 'CODE', 'EXAM_ITEM_VALUE', '9N771000000000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 specific questionnaire', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '食べ方３（夜食/間食）', '食べ方3(夜食/間食)', 'EMPTY', 'IGNORE', NULL, NULL, 'IGNORE', 0.8500, 'seed: maruyama 121-240 empty snacking questionnaire column', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '食習慣', '食習慣', 'CODE', 'EXAM_ITEM_VALUE', '9N781000000000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 specific questionnaire', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '飲酒', '飲酒', 'CODE', 'EXAM_ITEM_VALUE', '9N786000000000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 specific questionnaire', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '飲酒量', '飲酒量', 'CODE', 'EXAM_ITEM_VALUE', '9N791000000000011', NULL, 'DIRECT', 0.9000, 'seed: maruyama 121-240 specific questionnaire CO value', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '喫煙', '喫煙', 'CODE', 'EXAM_ITEM_VALUE', '9N736000000000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 specific questionnaire', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '睡眠', '睡眠', 'CODE', 'EXAM_ITEM_VALUE', '9N796000000000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 specific questionnaire', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '生活習慣の改善', '生活習慣の改善', 'CODE', 'EXAM_ITEM_VALUE', '9N801000000000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 specific questionnaire', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '保健指導の受診歴', '保健指導の受診歴', 'CODE', 'EXAM_ITEM_VALUE', '9N808000000000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 specific questionnaire', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, 'メタボリック判定', 'メタボリック判定', 'CODE', 'EXAM_ITEM_VALUE', '9N501000000000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 judgement item', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, 'メタボリックコメント', 'メタボリックコメント', 'EMPTY', 'IGNORE', NULL, NULL, 'IGNORE', 0.8500, 'seed: maruyama 121-240 empty comment column', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '保健指導レベル', '保健指導レベル', 'CODE', 'EXAM_ITEM_VALUE', '9N506000000000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 judgement item', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '保健指導コメント', '保健指導コメント', 'EMPTY', 'IGNORE', NULL, NULL, 'IGNORE', 0.8500, 'seed: maruyama 121-240 empty comment column', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '体脂肪率', '体脂肪率', 'EMPTY', 'IGNORE', NULL, NULL, 'IGNORE', 0.8000, 'seed: maruyama 121-240 empty optional display column', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '肥満度所見', '肥満度所見', 'EMPTY', 'IGNORE', NULL, NULL, 'IGNORE', 0.8000, 'seed: maruyama 121-240 empty optional display column', 'seed', 'seed'),

  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '１０００Ｈｚ右', '1000HZ右', 'NUMERIC', 'EXAM_ITEM_VALUE', '9D100163100000001', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 hearing threshold dB raw value', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '４０００Ｈｚ右', '4000HZ右', 'NUMERIC', 'EXAM_ITEM_VALUE', '9D100163200000001', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 hearing threshold dB raw value', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '１０００Ｈｚ左', '1000HZ左', 'NUMERIC', 'EXAM_ITEM_VALUE', '9D100163500000001', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 hearing threshold dB raw value', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '４０００Ｈｚ左', '4000HZ左', 'NUMERIC', 'EXAM_ITEM_VALUE', '9D100163600000001', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 hearing threshold dB raw value', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '聴力検査判定', '聴力検査判定', 'CODE', 'REVIEW', NULL, NULL, 'NEEDS_CONFIRMATION', 0.7000, 'seed: maruyama 121-240 department judgement; not a direct item value', 'seed', 'seed'),
  ('facility', '0110119070', 'header_contains', 121, 240, '所見有無', '所見有無', 'CODE', 'REVIEW', NULL, NULL, 'NEEDS_CONFIRMATION', 0.7000, 'seed: maruyama 121-240 hearing judgement helper; confirm source handling', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '1000Hz右所見有無', '1000HZ右所見有無', 'CODE', 'EXAM_ITEM_VALUE', '9D100163100000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 hearing judgement CD right 1000Hz', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '4000Hz右所見有無', '4000HZ右所見有無', 'CODE', 'EXAM_ITEM_VALUE', '9D100163200000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 hearing judgement CD right 4000Hz', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '1000Hz左所見有無', '1000HZ左所見有無', 'CODE', 'EXAM_ITEM_VALUE', '9D100163500000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 hearing judgement CD left 1000Hz', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '4000Hz左所見有無', '4000HZ左所見有無', 'CODE', 'EXAM_ITEM_VALUE', '9D100163600000011', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 hearing judgement CD left 4000Hz', 'seed', 'seed'),

  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '胸部Ｘ線判定', '胸部X線判定', 'CODE', 'REVIEW', NULL, NULL, 'NEEDS_CONFIRMATION', 0.7000, 'seed: maruyama 121-240 department judgement; not a direct item value', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '胸部Ｘ線１', '胸部X線1', 'CODE', 'EXAM_ITEM_VALUE', '9N206160800000049', NULL, 'DIRECT', 0.8500, 'seed: maruyama 121-240 chest xray finding text', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '胸部Ｘ線２', '胸部X線2', 'CODE', 'EXAM_ITEM_VALUE', '9N206160800000049', NULL, 'DIRECT', 0.8500, 'seed: maruyama 121-240 chest xray finding text', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '胸部Ｘ線３', '胸部X線3', 'CODE', 'EXAM_ITEM_VALUE', '9N206160800000049', NULL, 'DIRECT', 0.8500, 'seed: maruyama 121-240 chest xray finding text', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '胸部Ｘ線４', '胸部X線4', 'EMPTY', 'IGNORE', NULL, NULL, 'IGNORE', 0.8500, 'seed: maruyama 121-240 empty reserved chest xray finding column', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '胸部Ｘ線５', '胸部X線5', 'EMPTY', 'IGNORE', NULL, NULL, 'IGNORE', 0.8500, 'seed: maruyama 121-240 empty reserved chest xray finding column', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '胸部Ｘ線６', '胸部X線6', 'EMPTY', 'IGNORE', NULL, NULL, 'IGNORE', 0.8500, 'seed: maruyama 121-240 empty reserved chest xray finding column', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '胸部Ｘ線２方向', '胸部X線2方向', 'EMPTY', 'IGNORE', NULL, NULL, 'IGNORE', 0.8000, 'seed: maruyama 121-240 empty chest xray helper column', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '胸部ＣＴ判定', '胸部CT判定', 'CODE', 'REVIEW', NULL, NULL, 'NEEDS_CONFIRMATION', 0.7000, 'seed: maruyama 121-240 optional department judgement', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '胸部ＣＴ１', '胸部CT1', 'CODE', 'EXAM_ITEM_VALUE', '9N251160800000049', NULL, 'DIRECT', 0.8000, 'seed: maruyama 121-240 chest CT finding text', 'seed', 'seed'),

  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '視力裸眼(右)', '視力裸眼(右)', 'NUMERIC', 'EXAM_ITEM_VALUE', '9E160162100000001', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 visual acuity', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '視力裸眼(左)', '視力裸眼(左)', 'NUMERIC', 'EXAM_ITEM_VALUE', '9E160162200000001', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 visual acuity', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '視力矯正(右)', '視力矯正(右)', 'NUMERIC', 'EXAM_ITEM_VALUE', '9E160162500000001', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 visual acuity', 'seed', 'seed'),
  ('facility', '0110119070', 'normalized_header_exact', 121, 240, '視力矯正(左)', '視力矯正(左)', 'NUMERIC', 'EXAM_ITEM_VALUE', '9E160162600000001', NULL, 'DIRECT', 0.9500, 'seed: maruyama 121-240 visual acuity', 'seed', 'seed');
