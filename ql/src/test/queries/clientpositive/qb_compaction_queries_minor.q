set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

--minor compaction, unpartitioned

create table minor_compaction (a int, b int) stored as orc tblproperties ("transactional"="true");
insert into minor_compaction values (3, 3), (4, 4);
delete from minor_compaction where a < 4;
insert into minor_compaction values (5, 5), (6, 6);
delete from minor_compaction where a > 5;

CREATE temporary external table delta_default_tmp_compactor_minor_compaction(`operation` int, `originalTransaction` bigint,
`bucket` int, `rowId` bigint, `currentTransaction` bigint, `row` struct<`a` :string, `b` :int>)
PARTITIONED BY (`file_name` STRING)  stored as orc 
TBLPROPERTIES ('compactiontable'='true', 'transactional'='false');

ALTER table delta_default_tmp_compactor_minor_compaction add
partition (file_name='delta_0000001_0000001_0000') location '${system:test.tmp.dir}/minor_compaction/delta_0000001_0000001_0000'
partition (file_name='delta_0000003_0000003_0000') location '${system:test.tmp.dir}/minor_compaction/delta_0000002_0000002_0000';

CREATE temporary external table delta_default_tmp_compactor_minor_compaction_result(`operation` int, `originalTransaction` bigint,
`bucket` int, `rowId` bigint, `currentTransaction` bigint, `row` struct<`a` :string, `b` :int>)
clustered by (`bucket`) sorted by (`bucket`, `originalTransaction`, `rowId`) into 1 buckets stored as orc
LOCATION '${system:test.tmp.dir}/warehouse/minor_compaction/delta_0000001_0000005_v0000009'
TBLPROPERTIES ('compactiontable'='true', 'bucketing_version'='2', 'transactional'='false');


CREATE temporary external table delete_delta_default_tmp_compactor_minor_compaction(`operation` int, `originalTransaction` bigint, 
`bucket` int, `rowId` bigint, `currentTransaction` bigint, `row` struct<`a` :string, `b` :int>)  PARTITIONED BY 
(`file_name` STRING)  stored as orc TBLPROPERTIES ('compactiontable'='true', 'transactional'='false');

ALTER table delete_delta_default_tmp_compactor_minor_compaction add 
partition (file_name='delete_delta_0000003_0000003_0000') location '${system:test.tmp.dir}/minor_compaction/delete_delta_0000003_0000003_0000'
partition (file_name='delete_delta_0000005_0000005_0000') location '${system:test.tmp.dir}/minor_compaction/delete_delta_0000005_0000005_0000';

CREATE temporary external table delete_delta_default_tmp_compactor_minor_compaction_result(`operation` int, 
`originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint, `row` struct<`a` :string, `b` :int>)
clustered by (`bucket`) sorted by (`bucket`, `originalTransaction`, `rowId`) into 1 buckets stored as orc 
LOCATION '${system:test.tmp.dir}/minor_compaction/delete_delta_0000001_0000005_v0000009'
TBLPROPERTIES ('compactiontable'='true', 'bucketing_version'='2', 'transactional'='false');

explain
INSERT into table delta_default_tmp_compactor_minor_compaction_result select `operation`, `originalTransaction`, `bucket`, 
`rowId`, `currentTransaction`, `row` from delta_default_tmp_compactor_minor_compaction;

INSERT into table delta_default_tmp_compactor_minor_compaction_result select `operation`, `originalTransaction`, `bucket`, 
`rowId`, `currentTransaction`, `row` from delta_default_tmp_compactor_minor_compaction;

explain
INSERT into table delete_delta_default_tmp_compactor_minor_compaction_result select `operation`, `originalTransaction`, 
`bucket`, `rowId`, `currentTransaction`, `row` from delete_delta_default_tmp_compactor_minor_compaction;

INSERT into table delete_delta_default_tmp_compactor_minor_compaction_result select `operation`, `originalTransaction`, 
`bucket`, `rowId`, `currentTransaction`, `row` from delete_delta_default_tmp_compactor_minor_compaction;

select * from delta_default_tmp_compactor_minor_compaction_result;
select * from delete_delta_default_tmp_compactor_minor_compaction_result;

DROP table if exists delta_default_tmp_compactor_minor_compaction_result;
DROP table if exists delta_default_tmp_compactor_minor_compaction;
DROP table if exists delete_delta_default_tmp_compactor_minor_compaction_result;
DROP table if exists delete_delta_default_tmp_compactor_minor_compaction_result;

--minor, partitioned todo


