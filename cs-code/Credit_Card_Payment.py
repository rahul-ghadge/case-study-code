#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import SparkSession


# In[2]:


spark = SparkSession.builder.master("local[1]").appName("Test").enableHiveSupport().getOrCreate()


# In[24]:


from pyspark.sql.functions import *


# In[25]:


import pandas


# In[28]:


#pip install openpyxl


# In[23]:


df=spark.sql("select * from hive_input_output.click_stream_json")


# In[10]:


#df=spark.read.option("header","true").json("hdfs://0.0.0.0:9000/user/hive/warehouse/hive_input_output.db/click_stream_json1/Data.json")


# In[12]:


df1 = df.withColumn("item", split(df['URL'], 'item.').getItem(1))


# In[13]:


df1.show()


# In[14]:


df1.createOrReplaceTempView("credit_card_payment")


# In[15]:


df2=spark.sql("select User_Name, count(User_Name) as credit_card_Payment from credit_card_payment where PaymentMethod ='Credit card' group by User_Name")


# In[16]:


df2.show()


# In[17]:


df2.write.mode('overwrite').format('csv').option('header','true').saveAsTable('spark_output.credit_card_payment')


# In[29]:


df2.toPandas().to_excel('/home/suraj/Output/credit_card_payment.xlsx', sheet_name = 'Sheet1', index = False)


# In[ ]:




