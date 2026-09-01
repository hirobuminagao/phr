-- Source: MHLW Annex 2 Ver.4, column "author要素".
START TRANSACTION;

UPDATE dev_phr.exam_item_master
SET annex2_author_item_code = NULL;

UPDATE dev_phr.exam_item_master SET annex2_author_item_code = '9N516000000000049' WHERE namecode = '9N511000000000049';
UPDATE dev_phr.exam_item_master SET annex2_author_item_code = '9N526000000000049' WHERE namecode = '9N521000000000049';
UPDATE dev_phr.exam_item_master SET annex2_author_item_code = '9N536000000000049' WHERE namecode = '9N531000000000049';
UPDATE dev_phr.exam_item_master SET annex2_author_item_code = '9N546000000000049' WHERE namecode = '9N541000000000049';
UPDATE dev_phr.exam_item_master SET annex2_author_item_code = '9N576000000000049' WHERE namecode = '9N571000000000049';
UPDATE dev_phr.exam_item_master SET annex2_author_item_code = '9N586000000000049' WHERE namecode = '9N581161300000011';
UPDATE dev_phr.exam_item_master SET annex2_author_item_code = '9N586000000000049' WHERE namecode = '9N581161400000049';
UPDATE dev_phr.exam_item_master SET annex2_author_item_code = '9N596000000000049' WHERE namecode = '9N591161300000011';
UPDATE dev_phr.exam_item_master SET annex2_author_item_code = '9N596000000000049' WHERE namecode = '9N591161400000049';
UPDATE dev_phr.exam_item_master SET annex2_author_item_code = '9N606000000000049' WHERE namecode = '9N601161300000011';
UPDATE dev_phr.exam_item_master SET annex2_author_item_code = '9N606000000000049' WHERE namecode = '9N601161400000049';
UPDATE dev_phr.exam_item_master SET annex2_author_item_code = '9N616000000000049' WHERE namecode = '9N611161300000011';
UPDATE dev_phr.exam_item_master SET annex2_author_item_code = '9N616000000000049' WHERE namecode = '9N611161400000049';
UPDATE dev_phr.exam_item_master SET annex2_author_item_code = '9N626000000000049' WHERE namecode = '9N621161300000011';
UPDATE dev_phr.exam_item_master SET annex2_author_item_code = '9N626000000000049' WHERE namecode = '9N621161400000049';
UPDATE dev_phr.exam_item_master SET annex2_author_item_code = '9N636000000000049' WHERE namecode = '9N631161300000011';
UPDATE dev_phr.exam_item_master SET annex2_author_item_code = '9N636000000000049' WHERE namecode = '9N631161400000049';
UPDATE dev_phr.exam_item_master SET annex2_author_item_code = '9N646000000000049' WHERE namecode = '9N641000000000049';

COMMIT;
