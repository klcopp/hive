set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.compactor.crud.query.based=true;
set hive.merge.tezfiles=true;
--frogmethod todo check these^

--major, unpartitioned
create table major_compaction (a int, b int) stored as orc tblproperties ("transactional"="true");
insert into major_compaction values (3, 3), (4, 4);
delete from major_compaction where a < 4;
insert into major_compaction values (5, 5), (6, 6);

CREATE temporary external table default_tmp_compactor_major_compaction
(`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint,
`row` struct<`a` :int, `b` :int>)
stored as orc LOCATION '${system:test.tmp.dir}/major_compaction/base_0000004_v0000020'
TBLPROPERTIES ('compactiontable'='true', 'transactional'='false');

explain
INSERT into table default_tmp_compactor_major_compaction
select validate_acid_sort_order(ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId),
ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId, ROW__ID.writeId, NAMED_STRUCT('a', a, 'b', b)
from default.major_compaction;

DROP table if exists default_tmp_compactor_major_compaction;
DROP table major_compaction;