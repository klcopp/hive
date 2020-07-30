--! qt:transactional
--! qt:replace:/hdfs:.*major_compaction/\.\.\.major_compaction_table/
set hive.mapred.mode=nonstrict;
set hive.merge.tezfiles=true;

-- Feel free to change this.
-- The point of this test is to show, at a glance, if there are any changes in compilation that might have caused
-- an issue in query-based compaction. Contains only DDL and explain plans.

-- Major compaction, unpartitioned, bucketed

CREATE TABLE major_compaction(id string, value string) 
CLUSTERED BY(id) INTO 10 BUCKETS
STORED AS ORC TBLPROPERTIES('transactional'='true');
insert into major_compaction values 
('1','one'),('2','two'),('3','three'),('4','four'),('5','five'),
('6','six'),('7','seven'),('8','eight'),('9','nine'),('10','ten'),
('11','eleven'),('12','twelve'),('13','thirteen'),('14','fourteen'),('15','fifteen'),
('16','sixteen'),('17','seventeen'),('18','eighteen'),('19','nineteen'),('20','twenty');
delete from major_compaction where id in ('2', '11', '10');
delete from major_compaction where id in ('2', '4', '12', '15');

CREATE temporary external table default_tmp_compactor_major_compaction
(`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint,
`row` struct<`id` :string, `value` :string>)
stored as orc
LOCATION '${system:test.warehouse.dir}/major_compaction/base_0000004_v0000020'
TBLPROPERTIES ('compactiontable'='true', 'transactional'='false');

set hive.split.grouping.mode=compactor;

--compaction query
EXPLAIN
INSERT into table default_tmp_compactor_major_compaction
select validate_acid_sort_order(ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId),
ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId, ROW__ID.writeId, NAMED_STRUCT('id', id, 'value', value)
from default.major_compaction;

set hive.split.grouping.mode=query;

drop table default_tmp_compactor_major_compaction;
drop table major_compaction;