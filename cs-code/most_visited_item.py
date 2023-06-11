

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("Test").enableHiveSupport().getOrCreate()

import pandas

#pip install openpyxl

df=spark.sql("select * from hive_input_output.click_stream_json")

#df=spark.read.option("header","true").json("hdfs://0.0.0.0:9000/user/hive/warehouse/hive_input_output.db/click_stream_json/Data.json")

df.show()

from pyspark.sql.functions import *

df1 = df.withColumn("item", split(df['URL'], 'item.').getItem(1))

df1.show()

df1.createOrReplaceTempView("item")

df2=spark.sql("select item, count(item) as most_visited from item group by item order by most_visited desc")

df2.show()

df2.write.mode("overwrite").format('csv').option('header','true').saveAsTable("spark_output.Most_Visited_Item1")

df2.toPandas().to_excel('/home/suraj/Output/Most_Visited_Item.xlsx', sheet_name = 'Sheet1', index = False)


