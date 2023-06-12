
import pyspark
from pyspark.sql import SparkSession


import pandas

#pip install openpyxl


spark = SparkSession.builder.master("local[1]").appName("Test").enableHiveSupport().getOrCreate()

df=spark.sql("select * from hive_input_output.click_stream_json")

#df=spark.read.option("header","true").json("hdfs://0.0.0.0:9000/user/hive/warehouse/hive_input_output.db/click_stream_json/Data.json")

df.show()

from pyspark.sql.functions import *


df1 = df.withColumn("item", split(df['URL'], 'item.').getItem(1))


df1.show()


df1.createOrReplaceTempView("User_Purchase")

df2=spark.sql("select User_Name, count(item) as user_purchased from User_Purchase where Action='purchase' group by User_Name")

df2.show()


df2.write.mode('overwrite').format('csv').option('header','true').saveAsTable("spark_output.User_Purchased")


df2.toPandas().to_excel('/home/suraj/Output/User_Purchased.xlsx', sheet_name = 'Sheet1', index = False)
