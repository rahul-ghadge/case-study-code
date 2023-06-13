
import pyspark
import pandas
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[1]").appName("Test").enableHiveSupport().getOrCreate()

df=spark.sql("select * from case_study.click_stream_data")

# df=spark.read.option("header","true").json("hdfs://0.0.0.0:9000/user/hive/warehouse/case_study.db/click_stream_data/UserData.json")

df.show()


df1=spark.sql("select Location, count(UserID) as location_count from case_study.click_stream_data group by Location")

df1.show()

df1.write.mode('overwrite').format('csv').option('header','true').saveAsTable('spark_output.location_wise_analysis')

df1.toPandas().to_excel('/home/hadoop/case-study/outputs/location_wise_analysis.xlsx', sheet_name = 'Sheet1', index = False)

