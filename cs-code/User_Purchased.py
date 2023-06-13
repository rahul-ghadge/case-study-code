
import pyspark
import pandas
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.master("local[1]").appName("Test").enableHiveSupport().getOrCreate()


df=spark.sql("select * from case_study.click_stream_data")

# df=spark.read.option("header","true").json("hdfs://0.0.0.0:9000/user/hive/warehouse/case_study.db/click_stream_data/UserData.json")

df.show()

df1 = df.withColumn("item", split(df['URL'], 'item').getItem(1))

df1.show()

df1.createOrReplaceTempView("user_purchase")

df2=spark.sql("select UserId, count(item) as user_purchased from user_purchase where url LIKE concat('%', 'Purchase', '%') group by UserId")

df2.show()

df2.write.mode('overwrite').format('csv').option('header','true').saveAsTable("spark_output.User_Purchased")

df2.toPandas().to_excel('/home/hadoop/case-study/outputs/User_Purchased.xlsx', sheet_name = 'Sheet1', index = False)
