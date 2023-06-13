
import pyspark
import pandas
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.master("local[1]").appName("Test").enableHiveSupport().getOrCreate()


df=spark.sql("select * from case_study.click_stream_data")

# df=spark.read.option("header","true").json("hdfs://0.0.0.0:9000/user/hive/warehouse/case_study.db/click_stream_data/UserData.json")

df1 = df.withColumn("item", split(df['URL'], 'item').getItem(1))

df1.show()

df1.createOrReplaceTempView("credit_card_payment")

df2=spark.sql("select UserId, count(UserId) as credit_card_Payment from credit_card_payment where PaymentMethod ='Credit-Card' group by UserId")

df2.show()

df2.write.mode('overwrite').format('csv').option('header','true').saveAsTable('spark_output.credit_card_payment')

df2.toPandas().to_excel('/home/hadoop/case-study/outputs/credit_card_payment.xlsx', sheet_name = 'Sheet1', index = False)
