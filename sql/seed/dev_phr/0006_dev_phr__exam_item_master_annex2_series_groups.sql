-- Source: docs/mhlw/phase4_v08/001082795.xlsx
-- Columns: 一連検査グループ識別 / 一連検査グループ関係コード
START TRANSACTION;

UPDATE dev_phr.exam_item_master
SET annex2_series_group_identifier = NULL,
    annex2_series_group_relation_code = NULL;

UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '3C015161002399949', annex2_series_group_relation_code = 'COMP' WHERE namecode = '3C015000002327101';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '3C015161002399949', annex2_series_group_relation_code = 'COMP' WHERE namecode = '3C015000002399901';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '3C015161002399949', annex2_series_group_relation_code = 'RSON' WHERE namecode = '3C015161602399911';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '3C015161002399949', annex2_series_group_relation_code = 'RSON' WHERE namecode = '3C015161002399949';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '3C015161002399949', annex2_series_group_relation_code = 'COMP' WHERE namecode = '8A065000002391901';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '2A020161001930149', annex2_series_group_relation_code = 'COMP' WHERE namecode = '2A040000001930102';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '2A020161001930149', annex2_series_group_relation_code = 'COMP' WHERE namecode = '2A030000001930101';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '2A020161001930149', annex2_series_group_relation_code = 'COMP' WHERE namecode = '2A020000001930101';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '2A020161001930149', annex2_series_group_relation_code = 'RSON' WHERE namecode = '2A020161001930149';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '2A020161001930149', annex2_series_group_relation_code = 'COMP' WHERE namecode = '2A060000001930101';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '2A020161001930149', annex2_series_group_relation_code = 'COMP' WHERE namecode = '2A070000001930101';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '2A020161001930149', annex2_series_group_relation_code = 'COMP' WHERE namecode = '2A080000001930101';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '2A020161001930149', annex2_series_group_relation_code = 'COMP' WHERE namecode = '2A010000001930101';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '2A020161001930149', annex2_series_group_relation_code = 'COMP' WHERE namecode = '2A050000001930101';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9A110161000000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9A110160700000011';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9A110161000000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9A110160800000049';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9A110161000000049', annex2_series_group_relation_code = 'RSON' WHERE namecode = '9A110161600000011';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9A110161000000049', annex2_series_group_relation_code = 'RSON' WHERE namecode = '9A110161000000049';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N211161100000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N201000000000011';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N211161100000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N206160700000011';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N211161100000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N206160800000049';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N211161100000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N211161100000049';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N211161100000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N211161200000049';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N226161100000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N216000000000011';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N226161100000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N221160700000011';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N226161100000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N221160800000049';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N226161100000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N226161100000049';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N226161100000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N226161200000049';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N251161100000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N251000000000011';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N251161100000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N251160700000011';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N251161100000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N251160800000049';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N251161100000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N251161100000049';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N251161100000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N251161200000049';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N256161100000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N256160700000011';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N256161100000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N256160800000049';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N256161100000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N256161100000049';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N256161100000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N256161200000049';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N261161100000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N261160700000011';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N261161100000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N261160800000049';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N261161100000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N261161100000049';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N261161100000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N261161200000049';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9E100161000000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9E100166000000011';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9E100161000000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9E100166100000011';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9E100161000000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9E100166200000011';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9E100161000000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9E100166300000011';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9E100161000000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9E100166600000011';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9E100161000000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9E100166500000011';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9E100161000000049', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9E100160900000049';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9E100161000000049', annex2_series_group_relation_code = 'RSON' WHERE namecode = '9E100161600000011';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9E100161000000049', annex2_series_group_relation_code = 'RSON' WHERE namecode = '9E100161000000049';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N556000000000011', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N556000000000011';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N556000000000011', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N561000000000011';
UPDATE dev_phr.exam_item_master SET annex2_series_group_identifier = '9N556000000000011', annex2_series_group_relation_code = 'COMP' WHERE namecode = '9N566000000000049';

COMMIT;
