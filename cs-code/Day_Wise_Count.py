
import pyspark
import pandas
from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local[1]").appName("Test").enableHiveSupport().getOrCreate()


df=spark.sql("select * from case_study.click_stream_data")

# df=spark.sql("select * from case_study.click_stream_partitioned_data")

# df=spark.read.option("header","true").json("hdfs://0.0.0.0:9000/user/hive/warehouse/case_study.db/click_stream_data/UserData.json")

df.show()

df.createOrReplaceTempView("day_wise_count")

df1=spark.sql("select LogDate, count(UserID) as day_wise_user_count from day_wise_count group by LogDate order by day_wise_user_count desc")

df1.show()

df1.write.mode('overwrite').format('csv').option('header','true').saveAsTable("spark_output.day_wise_count")

df1.toPandas().to_excel('home/hadoop/case-study/outputs/day_wise_count.xlsx', sheet_name = 'Sheet1', index = False)
