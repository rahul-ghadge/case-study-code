use hive_input_output;

drop table if exists click_stream_json;
create table click_stream_json(
UserID string, User_Name string, SessionID string, URL string, Action string, LogTime string, PaymentMethod string, LogDate string, Location string)
ROW FORMAT SERDE
  'org.apache.hive.hcatalog.data.JsonSerDe';

load data local inpath'/home/suraj/Data.json' into table click_stream_json;

SET hive.exec.dynamic.partition = true;SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.auto.convert.join = false;SET mapreduce.map.memory.mb=8192;SET mapreduce.reduce.memory.mb=8192;

SET hive.exec.max.dynamic.partitions = 10000;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET mapreduce.framework.name=local;

--drop table if exists click_stream_par_json;
--create external table click_stream_par_json(
--UserID string, User_Name string, SessionID string, URL string, Action string, LogTime string, PaymentMethod string, LogDate string) 
--partitioned by (Location string)
--ROW FORMAT SERDE
--  'org.apache.hive.hcatalog.data.JsonSerDe';

drop table if exists Most_Active_User;

create table Most_Active_User(User_Name string, Total int) row format delimited fields terminated by',' location '/hive_output/Most_Active_User';

insert into Most_Active_User select User_Name, count(User_Name) as most_active from click_stream_json group by User_Name order by most_active desc;

  
--insert into click_stream_par_json partition (Location) select * from click_stream_json;

--drop table if exists Location_Analysis;

--create table Location_Analysis(Location string, No_Of_Users int) row format delimited fields terminated by',' location '/hive_output/Location_Analysis';

--insert into Location_Analysis select Location, count(UserID) as location_count from click_stream_json group by Location;

--drop table if exists Day_Wise_Analysis;

--create table Day_Wise_Analysis(Log_Date string, No_Of_Users int) row format delimited fields terminated by',' location '/hive_output/Day_Wise_Analysis';

--insert into Day_Wise_Analysis select LogDate, count(UserID) from click_stream_par_json group by LogDate;


