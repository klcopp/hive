--! qt:transactional
--! qt:replace:/hdfs:.*major_compaction/\.\.\.major_compaction_table/
--! qt:replace:/pfile:.*major_compaction/\.\.\.major_compaction_table/
set hive.mapred.mode=nonstrict;
set hive.merge.tezfiles=true;

-- The point of this test is to make sure that the files and data resulting from query-based compaction are correct.
-- DO NOT CHANGE THE Q.OUT FILES. THERE IS NO REASON THAT WILL JUSTIFY CHANGING THEM, except for ACID file schema 
-- changes or changes to bucketing. IF YOU DO CHANGE THE Q.OUT FILES, please verify that the pre-compaction data matches
-- the post-compaction data EXACTLY, except for the files' directories (pre-compaction data is in a delta directory, 
-- post-compaction data is in a (randomly named) base directory).
-- This is tested on execution engines MR, Tez, LLAP.
 
---------------------------------------------------------------------------------------------------
---- 1. Major compaction, unpartitioned, unbucketed
--
--CREATE TABLE major_compaction(id string, value string) STORED AS ORC TBLPROPERTIES('transactional'='true');
--insert into major_compaction values 
--('1','one'),('2','two'),('3','three'),('4','four'),('5','five'),
--('6','six'),('7','seven'),('8','eight'),('9','nine'),('10','ten'),
--('11','eleven'),('12','twelve'),('13','thirteen'),('14','fourteen'),('15','fifteen'),
--('16','sixteen'),('17','seventeen'),('18','eighteen'),('19','nineteen'),('20','twenty');
--delete from major_compaction where id in ('2', '11', '10');
--delete from major_compaction where id in ('2', '4', '12', '15');
--
---- pre-compaction data
--select ROW__ID, *, INPUT__FILE__NAME from major_compaction;
--
---- simulate QB compaction
--CREATE temporary external table default_tmp_compactor_major_compaction
--(`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint,
--`row` struct<`id` :string, `value` :string>)
--stored as orc
--LOCATION '${system:test.warehouse.dir}/major_compaction/base_0000004_v0000020'
--TBLPROPERTIES ('compactiontable'='true', 'transactional'='false');
--
--set hive.split.grouping.mode=compactor;
--
--INSERT into table default_tmp_compactor_major_compaction
--select validate_acid_sort_order(ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId),
--ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId, ROW__ID.writeId, NAMED_STRUCT('id', id, 'value', value)
--from major_compaction;
--
--set hive.split.grouping.mode=query;
---- end simulate QB compaction
--
---- "post-compaction" data
--select ROW__ID, *, INPUT__FILE__NAME from default_tmp_compactor_major_compaction;
--
--drop table default_tmp_compactor_major_compaction;
--drop table major_compaction;
--dfs -rm -r -f ${system:test.warehouse.dir}/major_compaction;

---------------------------------------------------------------------------------------------------
-- 2. Major compaction, partitioned, bucketed

CREATE TABLE major_compaction_p(id string, value string) partitioned by (p int)
CLUSTERED BY(id) INTO 10 BUCKETS
STORED AS ORC TBLPROPERTIES('transactional'='true');
insert into major_compaction_p partition (p=1) values 
('1','one'),('2','two'),('3','three'),('4','four'),('5','five'),
('6','six'),('7','seven'),('8','eight'),('9','nine'),('10','ten'),
('11','eleven'),('12','twelve'),('13','thirteen'),('14','fourteen'),('15','fifteen'),
('16','sixteen'),('17','seventeen'),('18','eighteen'),('19','nineteen'),('20','twenty');
delete from major_compaction_p where id in ('2', '11', '10');
delete from major_compaction_p where id in ('2', '4', '12', '15');
insert into major_compaction_p values ('21', 'twenty-one', 2); 

-- pre-compaction data --TODO REINSTATE
--select ROW__ID, *, INPUT__FILE__NAME from major_compaction_p;

-- simulate QB compaction
CREATE temporary external table default_tmp_compactor_major_compaction
(`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint,
`row` struct<`id` :string, `value` :string>)
stored as orc
--LOCATION '${system:test.warehouse.dir}/major_compaction_p=1/base_0000004_v0000011'
TBLPROPERTIES ('compactiontable'='true', 'transactional'='false');
--TODO: this dir isn't deleted at the end

set hive.split.grouping.mode=compactor;

--TODO REMVOE
-- this is fine, except in tez, where the bucket issue is popping up:
--select validate_acid_sort_order(ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId),
--ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId, ROW__ID.writeId, NAMED_STRUCT('id', id, 'value', value)
--from major_compaction_p
--where p=1
;
--TODO END REMOVE


INSERT into table default_tmp_compactor_major_compaction
select validate_acid_sort_order(ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId),
ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId, ROW__ID.writeId, NAMED_STRUCT('id', id, 'value', value)
from major_compaction_p
;
--where p=1;

--TODO REMVOE
select * from default_tmp_compactor_major_compaction;
select count(*) from default_tmp_compactor_major_compaction;
--TODO end REMVOE


set hive.split.grouping.mode=query;
-- end simulate QB compaction

--TODO REMVOE
select * from default_tmp_compactor_major_compaction;
--TODO end REMVOE

----TODO remove theseQ!
--
--
--select validate_acid_sort_order(ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId),
--ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId, ROW__ID.writeId, NAMED_STRUCT('id', id, 'value', value)
--from major_compaction_p
--where p=1;
--
--select validate_acid_sort_order(ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId),
--ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId, ROW__ID.writeId, NAMED_STRUCT('id', id, 'value', value)
--from major_compaction_p;

-- "post-compaction" data
select ROW__ID, *, INPUT__FILE__NAME from default_tmp_compactor_major_compaction;

drop table default_tmp_compactor_major_compaction;
drop table major_compaction_p;
dfs -rm -r -f ${system:test.warehouse.dir}/major_compaction_p;
