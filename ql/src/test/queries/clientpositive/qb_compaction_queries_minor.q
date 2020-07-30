--! qt:transactional


set hive.mapred.mode=nonstrict;
set hive.merge.tezfiles=true;

-- The point of this test is to make sure that the files and data resulting from query-based compaction are correct.
-- DO NOT CHANGE THE Q.OUT FILES. THERE IS NO REASON THAT WILL JUSTIFY CHANGING THEM, except for ACID file schema 
-- changes or changes to bucketing. IF YOU DO CHANGE THE Q.OUT FILES, please verify that the pre-compaction data matches
-- the post-compaction data EXACTLY, except for the files' directories (pre-compaction data is in a delta directory, 
-- post-compaction data is in a base directory.
-- This is tested on engines MR, Tez, LLAP.

--minor compaction, unpartitioned

create table minor_compaction (a int, b int) stored as orc tblproperties ("transactional"="true");
insert into minor_compaction values (3, 3), (4, 4), (5, 5), (6, 6);
update table minor_compaction set a=7 where a<5;
update table minor_compaction set b=8 where b>5;

CREATE temporary external table delta_default_tmp_compactor_minor_compaction(`operation` int, `originalTransaction` bigint,
`bucket` int, `rowId` bigint, `currentTransaction` bigint, `row` struct<`a` :string, `b` :int>)
PARTITIONED BY (`file_name` STRING)  stored as orc 
TBLPROPERTIES ('compactiontable'='true', 'transactional'='false');

ALTER table delta_default_tmp_compactor_minor_compaction add
partition (file_name='delta_0000001_0000001_0000') location '${system:test.tmp.dir}/minor_compaction/delta_0000001_0000001_0000'
partition (file_name='delta_0000002_0000002_0000') location '${system:test.tmp.dir}/minor_compaction/delta_0000002_0000002_0000';
partition (file_name='delta_0000003_0000003_0000') location '${system:test.tmp.dir}/minor_compaction/delta_0000003_0000003_0000';

CREATE temporary external table delta_default_tmp_compactor_minor_compaction_result(`operation` int, `originalTransaction` bigint,
`bucket` int, `rowId` bigint, `currentTransaction` bigint, `row` struct<`a` :string, `b` :int>)
clustered by (`bucket`) sorted by (`bucket`, `originalTransaction`, `rowId`) into 1 buckets stored as orc
LOCATION '${system:test.tmp.dir}/warehouse/minor_compaction/delta_0000001_0000005_v0000009'
TBLPROPERTIES ('compactiontable'='true', 'bucketing_version'='2', 'transactional'='false');


CREATE temporary external table delete_delta_default_tmp_compactor_minor_compaction(`operation` int, `originalTransaction` bigint, 
`bucket` int, `rowId` bigint, `currentTransaction` bigint, `row` struct<`a` :string, `b` :int>)  PARTITIONED BY 
(`file_name` STRING)  stored as orc TBLPROPERTIES ('compactiontable'='true', 'transactional'='false');

ALTER table delete_delta_default_tmp_compactor_minor_compaction add 
partition (file_name='delete_delta_0000002_0000002_0000') location '${system:test.tmp.dir}/minor_compaction/delete_delta_0000002_0000002_0000'
partition (file_name='delete_delta_0000003_0000003_0000') location '${system:test.tmp.dir}/minor_compaction/delete_delta_0000003_0000003_0000';

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

DROP table if exists delta_default_tmp_compactor_minor_compaction;
DROP table if exists delete_delta_default_tmp_compactor_minor_compaction;
DROP table if exists delta_default_tmp_compactor_minor_compaction_result;
DROP table if exists delete_delta_default_tmp_compactor_minor_compaction_result;

--minor, partitioned with buckets

create table minor_compaction (a int, b int) clustered by (a) into 4 buckets stored as orc tblproperties 
("transactional"="true");
insert into minor_compaction values (3, 3), (4, 4);
delete from minor_compaction where a < 4;
insert into minor_compaction values (5, 5), (6, 6);
delete from minor_compaction where a > 5;

CREATE temporary external table delta_default_tmp_compactor_minor_compaction(`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint, `row` struct<`a` :string, `b` :int>)  PARTITIONED BY (`file_name` STRING)  stored as orc TBLPROPERTIES ('compactiontable'='true', 'transactional'='false')
ALTER table delta_default_tmp_compactor_minor_compaction add partition (file_name='delta_0000001_0000001_0000') location 'file:/Users/karencoppage/upstream/hive/itests/hive-unit/target/tmp/org.apache.hadoop.hive.ql.txn.compactor.TestCrudCompactorOnTez-1592410213728_36081077/warehouse/testminorcompaction/ds=today/delta_0000001_0000001_0000' partition (file_name='delta_0000002_0000002_0000') location 'file:/Users/karencoppage/upstream/hive/itests/hive-unit/target/tmp/org.apache.hadoop.hive.ql.txn.compactor.TestCrudCompactorOnTez-1592410213728_36081077/warehouse/testminorcompaction/ds=today/delta_0000002_0000002_0000' partition (file_name='delta_0000004_0000004_0000') location 'file:/Users/karencoppage/upstream/hive/itests/hive-unit/target/tmp/org.apache.hadoop.hive.ql.txn.compactor.TestCrudCompactorOnTez-1592410213728_36081077/warehouse/testminorcompaction/ds=today/delta_0000004_0000004_0000' 
CREATE temporary external table delta_default_tmp_compactor_minor_compaction_result(`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint, `row` struct<`a` :string, `b` :int>)  clustered by (`bucket`) sorted by (`bucket`, `originalTransaction`, `rowId`) into 2 buckets stored as orc LOCATION 'file:/Users/karencoppage/upstream/hive/itests/hive-unit/target/tmp/org.apache.hadoop.hive.ql.txn.compactor.TestCrudCompactorOnTez-1592410213728_36081077/warehouse/testminorcompaction/ds=today/delta_0000001_0000005_v0000009' TBLPROPERTIES ('compactiontable'='true', 'bucketing_version'='2', 'transactional'='false')
CREATE temporary external table delete_delta_default_tmp_compactor_minor_compaction(`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint, `row` struct<`a` :string, `b` :int>)  PARTITIONED BY (`file_name` STRING)  stored as orc TBLPROPERTIES ('compactiontable'='true', 'transactional'='false')
ALTER table delete_delta_default_tmp_compactor_minor_compaction add partition (file_name='delete_delta_0000003_0000003_0000') location 'file:/Users/karencoppage/upstream/hive/itests/hive-unit/target/tmp/org.apache.hadoop.hive.ql.txn.compactor.TestCrudCompactorOnTez-1592410213728_36081077/warehouse/testminorcompaction/ds=today/delete_delta_0000003_0000003_0000' partition (file_name='delete_delta_0000005_0000005_0000') location 'file:/Users/karencoppage/upstream/hive/itests/hive-unit/target/tmp/org.apache.hadoop.hive.ql.txn.compactor.TestCrudCompactorOnTez-1592410213728_36081077/warehouse/testminorcompaction/ds=today/delete_delta_0000005_0000005_0000' 
CREATE temporary external table delete_delta_default_tmp_compactor_minor_compaction_result(`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint, `row` struct<`a` :string, `b` :int>)  clustered by (`bucket`) sorted by (`bucket`, `originalTransaction`, `rowId`) into 2 buckets stored as orc LOCATION 'file:/Users/karencoppage/upstream/hive/itests/hive-unit/target/tmp/org.apache.hadoop.hive.ql.txn.compactor.TestCrudCompactorOnTez-1592410213728_36081077/warehouse/testminorcompaction/ds=today/delete_delta_0000001_0000005_v0000009' TBLPROPERTIES ('compactiontable'='true', 'bucketing_version'='2', 'transactional'='false')

INSERT into table delta_default_tmp_compactor_minor_compaction_result select `operation`, `originalTransaction`, `bucket`, `rowId`, `currentTransaction`, `row` from delta_default_tmp_compactor_minor_compaction
INSERT into table delete_delta_default_tmp_compactor_minor_compaction_result select `operation`, `originalTransaction`, `bucket`, `rowId`, `currentTransaction`, `row` from delete_delta_default_tmp_compactor_minor_compaction

DROP table if exists delta_default_tmp_compactor_minor_compaction;
DROP table if exists delete_delta_default_tmp_compactor_minor_compaction;
DROP table if exists delta_default_tmp_compactor_minor_compaction_result;
DROP table if exists delete_delta_default_tmp_compactor_minor_compaction_result;
DROP table if exists minor_compaction_partitioned;
