drop table if exists timestamps;
drop table if exists timestampLocalTzs;
drop table if exists dates;
drop table if exists strings;

set hive.use.sql.datetime.formats=true;

--non-vectorized
set hive.vectorized.execution.enabled=false; --frogmethod see below
set hive.fetch.task.conversion=none; --frogmethod do you want mapreduce or not

create table timestamps (t timestamp) stored as parquet;
insert into timestamps values
("2019-01-01"),
("1969-12-31 23:59:59.999999999")
;
from timestamps select cast (t as string format "yyyy");


create table dates (d date) stored as parquet;
insert into dates values
("2019-01-01"),
("1969-12-31")
;
from timestamps select cast (t as string format "yyyy");


--todo frogmethod uncomment after implementation
--create table timestampLocalTzs (t timestamp with local time zone);
--insert into timestamps values
--("2019-01-01 America/New_York"),
--("1969-12-31 23:59:59.999999999 Europe/Rome")
--;
--from timestampLocalTzs select cast (t as string format "yyyy");
--from timestampLocalTzs select cast (t as string format "hh"); -- todo change to hh24 maybe


create table strings (s string) stored as parquet;
insert into strings values
("2019"),
("1969")
;
from strings    select cast (s as timestamp                      format "yyyy");
from strings    select cast (s as date                           format "yyyy");
--from strings    select cast (s as timestamp with local time zone format "yyyy"); //frogmethod


--correct descriptions
explain
from strings    select cast (s as timestamp                      format "yyyy");
explain
from strings    select cast (s as date                           format "yyyy");
--explain
--from strings    select cast (s as timestamp with local time zone format "yyyy"); //frogmethod
explain
from timestamps select cast (t as string                         format "yyyy");


--vectorized
set hive.fetch.task.conversion=none;
set hive.vectorized.execution.enabled=true;

--from timestamps select cast (t as string    format "yyyy"); todo frogmethod uncomment after fixing 
--from dates      select cast (d as string    format "yyyy");
from strings    select cast (s as timestamp format "yyyy");
from strings    select cast (s as date      format "yyyy");
