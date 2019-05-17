drop table if exists timestamps;
drop table if exists timestampLocalTzs;
drop table if exists dates;
drop table if exists strings;
drop table if exists chars;
drop table if exists varchars;

--non-vectorized
set hive.vectorized.execution.enabled=false;
set hive.fetch.task.conversion=none;

create table timestamps (t timestamp) stored as parquet;
insert into timestamps values
("2020-02-03"),
("1969-12-31 23:59:59.999999999")
;
from timestamps select cast (t as string      format "yyyy hh24...PM");
from timestamps select cast (t as char(11)    format "yyyy hh24...PM"); -- will be truncated
from timestamps select cast (t as varchar(11) format "yyyy hh24...PM"); -- will be truncated

create table dates (d date) stored as parquet;
insert into dates values
("2020-02-03"),
("1969-12-31")
;
from timestamps select cast (t as string      format "yyyy");
from timestamps select cast (t as char(10)    format "yyyy");
from timestamps select cast (t as varchar(10) format "yyyy");

create table timestampLocalTzs (t timestamp with local time zone);
insert into timestampLocalTzs values
("2020-02-03 America/New_York"),
("1969-12-31 23:59:59.999999999 Europe/Rome")
;

from timestampLocalTzs select cast (t as string      format "yyyy hh24 tzh:tzm");
from timestampLocalTzs select cast (t as char(10)    format "yyyy hh24 tzh:tzm");
from timestampLocalTzs select cast (t as varchar(10) format "yyyy hh24 tzh:tzm");

create table strings  (s string)      stored as parquet;
create table varchars (s varchar(11)) stored as parquet;
create table chars    (s char(11))    stored as parquet;
insert into strings values
("20 / 2 / 3"),
("1969 12 31")
;
insert into varchars select * from strings;
insert into chars select * from strings;

from strings    select cast (s as timestamp                      format "yyyy.mm.dd");
from strings    select cast (s as date                           format "yyyy.mm.dd");
from varchars   select cast (s as timestamp                      format "yyyy.mm.dd");
from varchars   select cast (s as date                           format "yyyy.mm.dd");
from chars      select cast (s as timestamp                      format "yyyy.mm.dd");
from chars      select cast (s as date                           format "yyyy.mm.dd");
from strings    select cast (s as timestamp with local time zone format "yyyy.mm.dd");


--correct descriptions
explain from strings    select cast (s as timestamp                      format "yyy.mm.dd");
explain from strings    select cast (s as date                           format "yyy.mm.dd");
explain from strings    select cast (s as timestamp with local time zone format "yyyy.mm.dd");
explain from timestamps select cast (t as string                         format "yyyy");
explain from timestamps select cast (t as varchar(12)                    format "yyyy");


--vectorized (no timestamp with local time zone here)
set hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

from timestamps select cast (t as string      format "yyyy");
from dates      select cast (d as string      format "yyyy");
from timestamps select cast (t as varchar(11) format "yyyy");
from dates      select cast (d as varchar(11) format "yyyy");
from timestamps select cast (t as char(11)    format "yyyy");
from dates      select cast (d as char(11)    format "yyyy");
from strings    select cast (s as timestamp   format "yyyy.mm.dd");
from varchars   select cast (s as timestamp   format "yyyy.mm.dd");
from chars      select cast (s as timestamp   format "yyyy.mm.dd");
from strings    select cast (s as date        format "yyyy.mm.dd");
from varchars   select cast (s as date        format "yyyy.mm.dd");
from chars      select cast (s as date        format "yyyy.mm.dd");
