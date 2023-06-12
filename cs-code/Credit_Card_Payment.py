


import pyspark
from pyspark.sql import SparkSession



spark = SparkSession.builder.master("local[1]").appName("Test").enableHiveSupport().getOrCreate()




from pyspark.sql.functions import *

import pandas

#pip install openpyxl

df=spark.sql("select * from hive_input_output.click_stream_json")

#df=spark.read.option("header","true").json("hdfs://0.0.0.0:9000/user/hive/warehouse/hive_input_output.db/click_stream_json1/Data.json")
df1 = df.withColumn("item", split(df['URL'], 'item.').getItem(1))

df1.show()

df1.createOrReplaceTempView("credit_card_payment")

df2=spark.sql("select User_Name, count(User_Name) as credit_card_Payment from credit_card_payment where PaymentMethod ='Credit card' group by User_Name")

df2.show()

df2.write.mode('overwrite').format('csv').option('header','true').saveAsTable('spark_output.credit_card_payment')

df2.toPandas().to_excel('/home/suraj/Output/credit_card_payment.xlsx', sheet_name = 'Sheet1', index = False)
