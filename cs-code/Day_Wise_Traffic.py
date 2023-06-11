
import pyspark
from pyspark.sql import SparkSession

import pandas

#pip install openpyxl

spark = SparkSession.builder.master("local[1]").appName("Test").enableHiveSupport().getOrCreate()

df=spark.sql("select * from hive_input_output.click_stream_json")

#df=spark.read.option("header","true").json("hdfs://0.0.0.0:9000/user/hive/warehouse/hive_input_output.db/click_stream_json/Data.json")

df.show()

df.createOrReplaceTempView("Day_Wise")

df1=spark.sql("select LogDate, count(UserID) as Day_Wise from Day_Wise group by LogDate order by Day_Wise desc")


df1.show()

df1.write.mode('overwrite').format('csv').option('header','true').saveAsTable("spark_output.Day_Wise_Analysis")


df1.toPandas().to_excel('/home/suraj/Output/Day_Wise_Analysis.xlsx', sheet_name = 'Sheet1', index = False)
