create database case_study;

use case_study;




drop table if exists click_stream_data;

create table click_stream_data(UserID string, Location string, SessionID string, URL string, PaymentMethod string, LogDate string, LogTime string)
ROW FORMAT SERDE
  'org.apache.hive.hcatalog.data.JsonSerDe';
  
show tables;

load data local inpath '/home/hadoop/case-study/UserData.json' into table click_stream_data;



drop table if exists most_active_user;

create table most_active_user(UserID string, Total int) row format delimited fields terminated by',' location '/home/hadoop/case-study/outputs/most_active_user';

insert into most_active_user select UserID, count(UserID) as most_active from click_stream_data group by UserID order by most_active desc;


SET mapreduce.framework.name=local;




--drop table if exists click_stream_partitioned_data;
--create external table click_stream_partitioned_data(UserID string, SessionID string, URL string, PaymentMethod string, LogDate string, LogTime string)
--partitioned by (Location string)
--ROW FORMAT SERDE
--  'org.apache.hive.hcatalog.data.JsonSerDe';
  
--SET hive.exec.dynamic.partition = true;
--SET hive.exec.dynamic.partition.mode = nonstrict;  


--insert into click_stream_partitioned_data partition (Location) select * from click_stream_data limit 1000;




--drop table if exists location_wise_analysis;
--create table location_wise_analysis(Location string, No_Of_Users int) row format delimited fields terminated by',' location '/home/hadoop/case-study/outputs/location_wise_analysis';
--insert into location_wise_analysis select Location, count(UserID) as location_count from click_stream_data group by Location;



--drop table if exists day_wise_count;
--create table day_wise_count(LogDate string, No_Of_Users int) row format delimited fields terminated by',' location '/home/hadoop/case-study/outputs/day_wise_count';
--insert into day_wise_count select LogDate, count(UserID) from click_stream_partitioned_data group by LogDate;










SET hive.auto.convert.join = false;
SET mapreduce.map.memory.mb=8192;
SET mapreduce.reduce.memory.mb=8192;
SET hive.exec.max.dynamic.partitions = 10000;
SET hive.exec.max.dynamic.partitions.pernode = 1000;




