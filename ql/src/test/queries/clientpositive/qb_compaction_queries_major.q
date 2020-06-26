set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.compactor.crud.query.based=true;
set hive.merge.tezfiles=true; -- we want to check merge tasks

-- The point of this test is to make sure that the files and data resulting from query-based compaction are correct.
-- DO NOT CHANGE THE Q.OUT FILES. THERE IS NO REASON THAT WILL JUSTIFY CHANGING THEM, except for ACID file schema 
-- changes or changes to bucketing. IF YOU DO CHANGE THE Q.OUT FILES, please verify that the pre-compaction data matches
-- the post-compaction data EXACTLY, except for the files' directories (pre-compaction data is in a delta directory, 
-- post-compaction data is in a base directory.
 

CREATE TABLE test_update_bucketed(id string, value string) CLUSTERED BY(id) INTO 10 BUCKETS STORED AS ORC TBLPROPERTIES('transactional'='true');
insert into test_update_bucketed values 
('1','one'),('2','two'),('3','three'),('4','four'),('5','five'),
('6','six'),('7','seven'),('8','eight'),('9','nine'),('10','ten'),
('11','eleven'),('12','twelve'),('13','thirteen'),('14','fourteen'),('15','fifteen'),
('16','sixteen'),('17','seventeen'),('18','eighteen'),('19','nineteen'),('20','twenty');
delete from test_update_bucketed where id in ('2', '11', '10');
delete from test_update_bucketed where id in ('2', '4', '12', '15');

-- pre-compaction data
select ROW__ID, *, INPUT__FILE__NAME from test_update_bucketed;

-- simulate QB compaction
CREATE temporary external table default_tmp_compactor_major_compaction
(`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint,
`row` struct<`id` :string, `value` :string>)
stored as orc LOCATION '${system:test.tmp.dir}/test_update_bucketed/base_0000004_v0000020'
TBLPROPERTIES ('compactiontable'='true', 'transactional'='false');

set hive.split.grouping.mode=compactor;

INSERT into table default_tmp_compactor_major_compaction
select validate_acid_sort_order(ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId),
ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId, ROW__ID.writeId, NAMED_STRUCT('id', id, 'value', value)
from default.test_update_bucketed;

set hive.split.grouping.mode=query;
-- end simulate QB compaction

-- "post-compaction" data
select ROW__ID, *, INPUT__FILE__NAME from default_tmp_compactor_major_compaction;

drop table default_tmp_compactor_major_compaction;
drop table test_update_bucketed;



----major, unpartitioned
--create table major_compaction (a int, b int) stored as orc tblproperties ("transactional"="true");
--insert into major_compaction values (3, 3), (4, 4);
--delete from major_compaction where a < 4;
--insert into major_compaction values (5, 5), (6, 6);
--
--CREATE temporary external table default_tmp_compactor_major_compaction
--(`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint,
--`row` struct<`a` :int, `b` :int>)
--stored as orc LOCATION '${system:test.tmp.dir}/major_compaction/base_0000004_v0000020'
--TBLPROPERTIES ('compactiontable'='true', 'transactional'='false');
--
--
--INSERT into table default_tmp_compactor_major_compaction
--select validate_acid_sort_order(ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId),
--ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId, ROW__ID.writeId, NAMED_STRUCT('a', a, 'b', b)
--from default.major_compaction;
--
--select * from default_tmp_compactor_major_compaction;
--
--DROP table if exists default_tmp_compactor_major_compaction;
--DROP table major_compaction;
--
---- major, partitioned
--create table major_compaction_p (a int, b int) partitioned by (p int) stored as orc tblproperties 
--("transactional"="true");
--insert into major_compaction_p values (3, 3, 1), (4, 4, 1);
--delete from major_compaction_p where a < 4;
--insert into major_compaction_p values (5, 5, 1), (6, 6, 1);
--
--CREATE temporary external table default_tmp_compactor_major_compaction_p
--(`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint, `row` 
--struct<`a` :int, `b` :int, `p` :int>)
--stored as orc LOCATION '${system:test.tmp.dir}/major_compaction_p/p=1/base_0000004_v0000011'
--TBLPROPERTIES ('compactiontable'='true', 'transactional'='false');
--
--explain
--INSERT into table default_tmp_compactor_major_compaction_p
--select validate_acid_sort_order(ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId),
--ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId, ROW__ID.writeId, NAMED_STRUCT('a', a, 'b', b, 'p', p)
--from default.major_compaction_p where `p`=1;
--
--INSERT into table default_tmp_compactor_major_compaction_p
--select validate_acid_sort_order(ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId),
--ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId, ROW__ID.writeId, NAMED_STRUCT('a', a, 'b', b, 'p', p)
--from default.major_compaction_p where `p`=1;
--
--select * from default_tmp_compactor_major_compaction_p;
--
--DROP table if exists default_tmp_compactor_major_compaction_p;
--DROP table major_compaction_p;
